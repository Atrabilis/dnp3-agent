// Package master is a polling DNP3 master over TCP that records binary and double-bit input events. It implements
// listen.Source of github.com/atrabilis/relay-events: every response fragment becomes one report line, and the
// outstation is sent the application confirmation for a fragment only after the supervisor has stored it, so events
// stay in the outstation's buffer until they are safe.
//
// It can also take periodic snapshots of the current state (class 0: binary inputs, analog inputs and counters) for
// telemetry; those go to a callback, not to the event table. Analog and counter events are confirmed and dropped:
// the snapshots carry the measurements.
//
// By default it never writes to the outstation: no time synchronization, no clearing of the restart indication, no
// controls. A session reads class 1, 2 and 3 events, then class 0 (current state), then polls events periodically.
package master

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/atrabilis/dnp3-agent/dnp3"
	"github.com/atrabilis/relay-events/listen"
)

// Options for one outstation.
type Options struct {
	Host       string
	Port       int    // default 20000
	Outstation uint16 // outstation link address
	Master     uint16 // our link address

	Timeout           time.Duration // response timeout (default 5s)
	PollInterval      time.Duration // event poll period (default 2s)
	IntegrityInterval time.Duration // class 0 read period after the start (default 1h; 0 = only at session start)
	Heartbeat         time.Duration // heartbeat line period (default 30s)

	// TelemetryInterval is the period of the class 0 snapshots passed to OnTelemetry (0 = only the class 0 reads the
	// session already does: at the start and every IntegrityInterval). Telemetry reads never reach the event table.
	TelemetryInterval time.Duration
	// OnTelemetry receives every class 0 snapshot. It is called from the session goroutine and must not block.
	OnTelemetry func(Telemetry)

	// Names of binary inputs and double-bit inputs by index, used as the stored reference (e.g. "SigOpenOc1F").
	// Points without a name are stored as "BI<index>" / "DBI<index>".
	BinaryNames map[uint32]string
	DoubleNames map[uint32]string
	// Names and scale factors of analog inputs and counters, for the telemetry snapshots. Points without a name are
	// "AI<index>" / "C<index>"; the value sent by the outstation is multiplied by the scale (default 1).
	AnalogNames  map[uint32]string
	AnalogScale  map[uint32]float64
	CounterNames map[uint32]string

	Logf  func(format string, args ...any)
	Debug bool // log every received frame (read-only checks)
}

func (o *Options) defaults() {
	if o.Port == 0 {
		o.Port = 20000
	}
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Second
	}
	if o.PollInterval <= 0 {
		o.PollInterval = 2 * time.Second
	}
	if o.IntegrityInterval < 0 {
		o.IntegrityInterval = 0
	}
	if o.Heartbeat <= 0 {
		o.Heartbeat = 30 * time.Second
	}
	if o.Logf == nil {
		o.Logf = func(string, ...any) {}
	}
}

// SourceName is how the outstation appears in the event table ("source" column).
func (o Options) SourceName() string { return "dnp3:" + strconv.Itoa(int(o.Outstation)) }

// Source returns a listen.Source for the outstation.
func Source(o Options) listen.Source {
	o.defaults()
	return &source{o: o}
}

type source struct{ o Options }

// Start connects and runs one session in the background.
func (s *source) Start(ctx context.Context, _ listen.State) (listen.Session, error) {
	ss := &session{o: s.o, items: make(chan listen.Item), stop: make(chan struct{}), exit: listen.ExitOK}
	ss.wg.Add(1)
	go ss.run(ctx)
	return ss, nil
}

type session struct {
	o     Options
	items chan listen.Item
	stop  chan struct{}
	once  sync.Once
	wg    sync.WaitGroup

	mu   sync.Mutex
	conn net.Conn
	exit int

	appSeq, tpSeq uint8
	frags         chan fragment
	reports       int64
	snap          *Telemetry // class 0 snapshot being assembled (nil outside a class 0 read)

	loggedRestart, loggedNeedTime bool // IIN bits already logged in this session
}

type fragment struct {
	apdu []byte
	err  error
}

func (s *session) Items() <-chan listen.Item { return s.items }
func (s *session) Stop()                     { s.once.Do(func() { close(s.stop) }) }

func (s *session) Kill() {
	s.Stop()
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
}

func (s *session) Wait() int {
	s.wg.Wait()
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.exit
}

// errStopped means Stop was called: the session ends cleanly (exit 0).
var errStopped = errors.New("stopped")

type exitError struct {
	code   int
	reason string
	err    error
}

func (e *exitError) Error() string { return fmt.Sprintf("%s: %v", e.reason, e.err) }

func (s *session) run(ctx context.Context) {
	defer s.wg.Done()
	defer close(s.items)
	err := s.loop(ctx)
	code, reason := listen.ExitOK, "stopped"
	var ee *exitError
	switch {
	case errors.Is(err, errStopped) || err == nil:
	case errors.As(err, &ee):
		code, reason = ee.code, ee.reason
		s.o.Logf("dnp3 %s:%d: %v", s.o.Host, s.o.Port, err)
	default:
		code, reason = listen.ExitLost, "connection_lost"
		s.o.Logf("dnp3 %s:%d: %v", s.o.Host, s.o.Port, err)
	}
	s.mu.Lock()
	s.exit = code
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.mu.Unlock()
	end, _ := json.Marshal(map[string]any{"type": "end", "reason": reason, "reports": s.reports, "exit": code})
	_ = s.emitRaw(end, nil) // best effort: the session is ending either way
}

func (s *session) loop(ctx context.Context) error {
	addr := net.JoinHostPort(s.o.Host, strconv.Itoa(s.o.Port))
	d := net.Dialer{Timeout: s.o.Timeout}
	conn, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return &exitError{code: listen.ExitUsage, reason: "connect_failed", err: err}
	}
	s.mu.Lock()
	s.conn = conn
	s.mu.Unlock()
	s.frags = make(chan fragment, 16)
	go s.readLoop(conn)

	hello, _ := json.Marshal(map[string]any{"type": "hello", "source": s.o.SourceName(), "protocol": "dnp3",
		"host": s.o.Host, "port": s.o.Port, "outstation": s.o.Outstation, "master": s.o.Master})
	if err := s.emitRaw(hello, nil); err != nil {
		return err
	}
	enabled, _ := json.Marshal(map[string]any{"type": "enabled", "at": stamp(time.Now())})
	if err := s.emitRaw(enabled, nil); err != nil {
		return err
	}

	// Start of session: whatever the outstation buffered while nobody polled, then the current state.
	if err := s.poll(ctx, []int{1, 2, 3}, "event"); err != nil {
		return err
	}
	if err := s.poll(ctx, []int{0}, "static"); err != nil {
		return err
	}
	lastIntegrity, lastBeat, lastTelemetry := time.Now(), time.Now(), time.Now()
	tick := time.NewTicker(s.o.PollInterval)
	defer tick.Stop()
	for {
		select {
		case <-s.stop:
			return errStopped
		case <-ctx.Done():
			return errStopped
		case f := <-s.frags: // unsolicited responses between polls
			if f.err != nil {
				return f.err
			}
			if err := s.handleUnsolicited(f.apdu); err != nil {
				return err
			}
		case <-tick.C:
			if err := s.poll(ctx, []int{1, 2, 3}, "event"); err != nil {
				return err
			}
			if s.o.IntegrityInterval > 0 && time.Since(lastIntegrity) >= s.o.IntegrityInterval {
				if err := s.poll(ctx, []int{0}, "static"); err != nil {
					return err
				}
				lastIntegrity, lastTelemetry = time.Now(), time.Now()
			}
			if s.o.TelemetryInterval > 0 && s.o.OnTelemetry != nil && time.Since(lastTelemetry) >= s.o.TelemetryInterval {
				if err := s.poll(ctx, []int{0}, reasonTelemetry); err != nil {
					return err
				}
				lastTelemetry = time.Now()
			}
			if time.Since(lastBeat) >= s.o.Heartbeat {
				hb, _ := json.Marshal(map[string]any{"type": "heartbeat", "at": stamp(time.Now()), "reports": s.reports})
				if err := s.emitRaw(hb, nil); err != nil {
					return err
				}
				lastBeat = time.Now()
			}
		}
	}
}

// reasonTelemetry marks a class 0 read made only for a telemetry snapshot: its points are not stored as events.
const reasonTelemetry = "telemetry"

// poll sends one READ and handles every fragment of the response. A class 0 read also produces a telemetry
// snapshot when OnTelemetry is set.
func (s *session) poll(ctx context.Context, classes []int, reason string) error {
	if s.o.OnTelemetry != nil && len(classes) == 1 && classes[0] == 0 {
		s.snap = newTelemetry()
		defer func() { s.snap = nil }()
	}
	seq := s.appSeq
	s.appSeq = (s.appSeq + 1) & 0x0F
	if err := s.send(dnp3.ReadClasses(seq, classes...)); err != nil {
		return err
	}
	expect := seq
	for {
		apdu, err := s.await(ctx)
		if err != nil {
			return err
		}
		r, err := dnp3.ParseResponse(apdu)
		if err != nil {
			return fmt.Errorf("decoding response: %w", err)
		}
		if r.Unsolicited() {
			if err := s.handle(r, "event"); err != nil {
				return err
			}
			continue
		}
		if r.Seq() != expect {
			s.o.Logf("dnp3: response with sequence %d, expected %d; ignored", r.Seq(), expect)
			continue
		}
		if r.IIN&(dnp3.IINFuncNotSupported|dnp3.IINObjectUnknown|dnp3.IINParameterError) != 0 {
			return &exitError{code: listen.ExitMismatch, reason: "request_rejected",
				err: fmt.Errorf("outstation rejected the class read (IIN %04x)", r.IIN)}
		}
		if s.snap != nil {
			s.snap.add(s, r)
		}
		if err := s.handle(r, reason); err != nil {
			return err
		}
		if r.FIN() {
			if s.snap != nil {
				s.snap.At = time.Now().UTC()
				s.o.OnTelemetry(*s.snap)
			}
			return nil
		}
		expect = (expect + 1) & 0x0F
	}
}

func (s *session) handleUnsolicited(apdu []byte) error {
	r, err := dnp3.ParseResponse(apdu)
	if err != nil {
		return fmt.Errorf("decoding unsolicited response: %w", err)
	}
	return s.handle(r, "event")
}

// handle turns one fragment into a report line and confirms it once stored (when the outstation asked for it).
func (s *session) handle(r dnp3.Response, reason string) error {
	// The restart and need-time bits stay set until a master clears them (a write this agent does not do), so they
	// are logged once per session, not on every poll.
	if r.IIN&dnp3.IINDeviceRestart != 0 && !s.loggedRestart {
		s.loggedRestart = true
		s.o.Logf("dnp3: outstation reports a restart (IIN1.7); the bit stays set until a master clears it")
	}
	if r.IIN&dnp3.IINNeedTime != 0 && !s.loggedNeedTime {
		s.loggedNeedTime = true
		s.o.Logf("dnp3: outstation asks for time (IIN1.4): its event time stamps are stored as not synchronized")
	}
	ovfl := r.IIN&dnp3.IINEventBufferOverflow != 0
	if reason == reasonTelemetry || (len(r.Points) == 0 && !ovfl) {
		// Telemetry snapshots and fragments with only analog or counter events are not stored as events.
		if r.NeedsConfirm() {
			return s.send(dnp3.Confirm(r.Seq(), r.Unsolicited()))
		}
		return nil
	}
	line := s.reportLine(r, reason, ovfl)
	ok, err := s.emit(line)
	if err != nil {
		return err
	}
	if !ok {
		return errStopped // not stored: no confirmation, the outstation keeps the events for the next session
	}
	s.reports++
	if r.NeedsConfirm() {
		return s.send(dnp3.Confirm(r.Seq(), r.Unsolicited()))
	}
	return nil
}

func (s *session) reportLine(r dnp3.Response, reason string, ovfl bool) *listen.Line {
	seq := int64(r.Seq())
	l := &listen.Line{Type: "report", ReceivedAt: stamp(time.Now()), SeqNum: &seq, BufOvfl: ovfl}
	needTime := r.IIN&dnp3.IINNeedTime != 0
	for i, p := range r.Points {
		ref := s.ref(p)
		v := map[string]any{"q": fmt.Sprintf("%02x", p.Flags), "index": p.Index}
		if p.Double {
			v["stVal"] = p.Value
		} else {
			v["stVal"] = p.Value == 1
		}
		if p.Time != nil {
			tq := 0
			if needTime || p.TimeUnsynced {
				tq = listen.TQClockNotSynchronized
			}
			v["t"] = map[string]any{"ts": stamp(*p.Time), "tq": tq}
		}
		raw, _ := json.Marshal(v)
		rsn := reason
		if !p.Event {
			rsn = "static"
		}
		l.Values = append(l.Values, listen.ValueEntry{Index: i, Ref: ref, Reasons: []string{rsn}, Value: raw})
	}
	raw, _ := json.Marshal(map[string]any{"type": "report", "seq_num": seq, "points": len(r.Points), "buf_ovfl": ovfl, "iin": r.IIN})
	l.Raw = raw
	return l
}

// measureRef is the telemetry name of an analog input or counter.
func (s *session) measureRef(m dnp3.Measurement) string {
	if m.Kind == dnp3.KindAnalog {
		if n, ok := s.o.AnalogNames[m.Index]; ok && n != "" {
			return n
		}
		return "AI" + strconv.Itoa(int(m.Index))
	}
	if n, ok := s.o.CounterNames[m.Index]; ok && n != "" {
		return n
	}
	return "C" + strconv.Itoa(int(m.Index))
}

// scaled applies the configured scale factor of an analog input.
func (s *session) scaled(m dnp3.Measurement) float64 {
	if m.Kind == dnp3.KindAnalog {
		if f, ok := s.o.AnalogScale[m.Index]; ok && f != 0 {
			return m.Value * f
		}
	}
	return m.Value
}

func (s *session) ref(p dnp3.Point) string {
	if p.Double {
		if n, ok := s.o.DoubleNames[p.Index]; ok && n != "" {
			return n
		}
		return "DBI" + strconv.Itoa(int(p.Index))
	}
	if n, ok := s.o.BinaryNames[p.Index]; ok && n != "" {
		return n
	}
	return "BI" + strconv.Itoa(int(p.Index))
}

// emitRaw sends a non-report line (parsed from JSON) without waiting for an acknowledgement.
func (s *session) emitRaw(b []byte, _ func(bool)) error {
	l, err := listen.ParseLine(b)
	if err != nil {
		return err
	}
	select {
	case s.items <- listen.Item{Line: l}:
		return nil
	case <-s.stop:
		if l.Type == "end" {
			// The supervisor still drains after Stop; give it the end line without blocking forever.
			select {
			case s.items <- listen.Item{Line: l}:
			case <-time.After(time.Second):
			}
		}
		return errStopped
	}
}

// emit sends a report line and waits for the supervisor's acknowledgement.
func (s *session) emit(l *listen.Line) (bool, error) {
	done := make(chan bool, 1)
	select {
	case s.items <- listen.Item{Line: l, Ack: func(ok bool) { done <- ok }}:
	case <-s.stop:
		return false, errStopped
	}
	select {
	case ok := <-done:
		return ok, nil
	case <-s.stop:
		return false, errStopped
	}
}

// send writes one application fragment (split in transport segments and link frames).
func (s *session) send(apdu []byte) error {
	segs, next := dnp3.Segment(apdu, s.tpSeq)
	s.tpSeq = next
	for _, seg := range segs {
		f, err := dnp3.EncodeFrame(dnp3.LinkUnconfirmedUserData, true, s.o.Outstation, s.o.Master, seg)
		if err != nil {
			return err
		}
		if err := s.write(f); err != nil {
			return err
		}
	}
	return nil
}

func (s *session) write(b []byte) error {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	_ = c.SetWriteDeadline(time.Now().Add(s.o.Timeout))
	_, err := c.Write(b)
	return err
}

// await waits for the next application fragment from the outstation.
func (s *session) await(ctx context.Context) ([]byte, error) {
	select {
	case f := <-s.frags:
		return f.apdu, f.err
	case <-time.After(s.o.Timeout):
		return nil, fmt.Errorf("no response within %s", s.o.Timeout)
	case <-s.stop:
		return nil, errStopped
	case <-ctx.Done():
		return nil, errStopped
	}
}

// readLoop decodes link frames from the connection, answers link status requests and reassembles fragments.
func (s *session) readLoop(conn net.Conn) {
	var buf []byte
	var rs dnp3.Reassembler
	warnedDest := false
	tmp := make([]byte, 4096)
	push := func(f fragment) bool {
		select {
		case s.frags <- f:
			return true
		case <-s.stop:
			return false
		}
	}
	for {
		n, err := conn.Read(tmp)
		if n > 0 {
			buf = append(buf, tmp[:n]...)
		}
		for {
			i := indexSync(buf)
			if i < 0 {
				if len(buf) > 0 && buf[len(buf)-1] == 0x05 {
					buf = buf[len(buf)-1:]
				} else {
					buf = buf[:0]
				}
				break
			}
			buf = buf[i:]
			if len(buf) < 3 {
				break
			}
			total, ferr := dnp3.FrameLen(buf)
			if ferr != nil {
				buf = buf[2:]
				continue
			}
			if len(buf) < total {
				break
			}
			raw := buf[:total]
			buf = buf[total:]
			fr, derr := dnp3.DecodeFrame(raw)
			if derr != nil {
				s.o.Logf("dnp3: %v; frame dropped", derr)
				continue
			}
			if fr.FromMaster() {
				continue
			}
			if fr.Dest != s.o.Master {
				// Frames for another master address: the outstation's own messages (link status requests, unsolicited
				// responses) go to the master address configured in it, which may differ from ours.
				if !warnedDest {
					warnedDest = true
					s.o.Logf("dnp3: outstation sends frames to master address %d, not %d; set master: %d in the config",
						fr.Dest, s.o.Master, fr.Dest)
				}
				continue
			}
			if fr.Src != s.o.Outstation {
				if !push(fragment{err: &exitError{code: listen.ExitMismatch, reason: "address_mismatch",
					err: fmt.Errorf("answer from outstation %d, expected %d", fr.Src, s.o.Outstation)}}) {
					return
				}
				continue
			}
			switch fr.Function() {
			case dnp3.LinkRequestStatus:
				if b, e := dnp3.EncodeFrame(dnp3.LinkStatusOfLink, false, s.o.Outstation, s.o.Master, nil); e == nil {
					_ = s.write(b)
				}
				continue
			case dnp3.LinkUnconfirmedUserData:
			default:
				continue
			}
			apdu, aerr := rs.Add(fr.Data)
			if aerr != nil {
				s.o.Logf("dnp3: %v", aerr)
				continue
			}
			if apdu != nil && !push(fragment{apdu: apdu}) {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) && !errors.Is(err, net.ErrClosed) {
				s.o.Logf("dnp3: read: %v", err)
			}
			push(fragment{err: fmt.Errorf("connection closed: %w", err)})
			return
		}
	}
}

func indexSync(b []byte) int {
	for i := 0; i+1 < len(b); i++ {
		if b[i] == 0x05 && b[i+1] == 0x64 {
			return i
		}
	}
	return -1
}

func stamp(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z07:00") }
