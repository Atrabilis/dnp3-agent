package master

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/atrabilis/dnp3-agent/dnp3"
)

// IINNames maps IIN bits to readable names.
var IINNames = []struct {
	Bit  uint16
	Name string
}{
	{dnp3.IINClass1Events, "class1_events"}, {dnp3.IINClass2Events, "class2_events"},
	{dnp3.IINClass3Events, "class3_events"}, {dnp3.IINNeedTime, "need_time"},
	{dnp3.IINLocalControl, "local_control"}, {dnp3.IINDeviceTrouble, "device_trouble"},
	{dnp3.IINDeviceRestart, "device_restart"}, {dnp3.IINFuncNotSupported, "function_not_supported"},
	{dnp3.IINObjectUnknown, "object_unknown"}, {dnp3.IINParameterError, "parameter_error"},
	{dnp3.IINEventBufferOverflow, "event_buffer_overflow"}, {dnp3.IINAlreadyExecuting, "already_executing"},
	{dnp3.IINConfigCorrupt, "config_corrupt"},
}

// ReadStatic connects, reads class 0 (the current state of every point) once and disconnects. It is read-only: no
// confirmation is sent (a class 0 response does not ask for one) and nothing is written. It returns one row per
// point plus a summary row with the IIN.
func ReadStatic(ctx context.Context, o Options) ([]map[string]any, error) {
	return ReadOnce(ctx, o, 0)
}

// ReadOnce connects, sends one READ of the given classes and disconnects after the last fragment. No confirmation
// is ever sent: events read this way stay in the outstation and are sent again to the next master that polls.
func ReadOnce(ctx context.Context, o Options, classes ...int) ([]map[string]any, error) {
	o.defaults()
	ss := &session{o: o} // only for point names and scaling
	d := net.Dialer{Timeout: o.Timeout}
	conn, err := d.DialContext(ctx, "tcp", net.JoinHostPort(o.Host, strconv.Itoa(o.Port)))
	if err != nil {
		return nil, err
	}
	defer func() { _ = conn.Close() }()

	segs, _ := dnp3.Segment(dnp3.ReadClasses(0, classes...), 0)
	for _, s := range segs {
		f, err := dnp3.EncodeFrame(dnp3.LinkUnconfirmedUserData, true, o.Outstation, o.Master, s)
		if err != nil {
			return nil, err
		}
		if _, err := conn.Write(f); err != nil {
			return nil, err
		}
	}

	var rows []map[string]any
	var iin uint16
	var buf []byte
	var rs dnp3.Reassembler
	tmp := make([]byte, 4096)
	deadline := time.Now().Add(o.Timeout)
	for {
		_ = conn.SetReadDeadline(deadline)
		n, err := conn.Read(tmp)
		if err != nil {
			return rows, fmt.Errorf("reading class 0: %w", err)
		}
		buf = append(buf, tmp[:n]...)
		for {
			i := indexSync(buf)
			if i < 0 || len(buf[i:]) < 3 {
				break
			}
			buf = buf[i:]
			total, ferr := dnp3.FrameLen(buf)
			if ferr != nil {
				buf = buf[2:]
				continue
			}
			if len(buf) < total {
				break
			}
			fr, derr := dnp3.DecodeFrame(buf[:total])
			buf = buf[total:]
			if o.Debug {
				o.Logf("dnp3 debug: frame ctl=%02x src=%d dst=%d len=%d err=%v", fr.Control, fr.Src, fr.Dest, len(fr.Data), derr)
			}
			if derr != nil || fr.FromMaster() || fr.Function() != dnp3.LinkUnconfirmedUserData {
				continue
			}
			if fr.Dest != o.Master {
				o.Logf("dnp3: frame for master address %d (ours is %d): the outstation's configured master address", fr.Dest, o.Master)
				continue
			}
			if fr.Src != o.Outstation {
				return rows, fmt.Errorf("answer from outstation %d, expected %d", fr.Src, o.Outstation)
			}
			apdu, aerr := rs.Add(fr.Data)
			if o.Debug && aerr != nil {
				o.Logf("dnp3 debug: transport: %v", aerr)
			}
			if apdu == nil {
				continue
			}
			if o.Debug {
				o.Logf("dnp3 debug: fragment %d bytes, header % x", len(apdu), apdu[:min(4, len(apdu))])
			}
			r, perr := dnp3.ParseResponse(apdu)
			if perr != nil {
				return rows, perr
			}
			iin = r.IIN
			for _, p := range r.Points {
				ref := ss.ref(p)
				val := any(p.Value == 1)
				if p.Double {
					val = p.Value
				}
				row := map[string]any{"type": "point", "ref": ref, "group": p.Group, "variation": p.Variation,
					"index": p.Index, "value": val, "flags": fmt.Sprintf("%02x", p.Flags), "event": p.Event}
				if p.Time != nil {
					row["time"] = p.Time.Format("2006-01-02T15:04:05.000Z07:00")
					row["time_unsynced"] = p.TimeUnsynced || r.IIN&dnp3.IINNeedTime != 0
				}
				rows = append(rows, row)
			}
			for _, m := range r.Measures {
				row := map[string]any{"type": "measure", "kind": m.Kind, "ref": ss.measureRef(m), "group": m.Group,
					"variation": m.Variation, "index": m.Index, "raw": m.Value, "value": ss.scaled(m), "event": m.Event,
					"frozen": m.Frozen}
				if m.HasFlags {
					row["flags"] = fmt.Sprintf("%02x", m.Flags)
				}
				if m.Time != nil {
					row["time"] = m.Time.Format("2006-01-02T15:04:05.000Z07:00")
				}
				rows = append(rows, row)
			}
			// A response in several fragments that asks for confirmation: the outstation sends the next fragment only
			// after it. A read-only check never confirms, so it stops here.
			pending := !r.FIN() && r.NeedsConfirm()
			if r.FIN() || pending {
				var flags []string
				for _, b := range IINNames {
					if iin&b.Bit != 0 {
						flags = append(flags, b.Name)
					}
				}
				rows = append(rows, map[string]any{"type": "summary", "points": len(rows), "skipped_objects": r.Skipped,
					"iin": fmt.Sprintf("%04x", iin), "iin_flags": flags, "confirm_requested": r.NeedsConfirm(),
					"more_fragments_pending": pending, "note": "nothing was confirmed or written"})
				return rows, nil
			}
		}
	}
}
