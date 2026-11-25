package dnp3

import (
	"bytes"
	"encoding/hex"
	"math"
	"testing"
	"time"
)

func mustHex(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestCRCKnownFrame(t *testing.T) {
	// Link "reset link states" header from master 0 to outstation 1, as in the DNP3 primer: 05 64 05 C0 01 00 00 04 E9 21.
	hdr := mustHex(t, "056405c001000004")
	if got := CRC(hdr); got != 0x21e9 {
		t.Fatalf("CRC = %04x, want 21e9", got)
	}
}

func TestFrameRoundTripAndCRCErrors(t *testing.T) {
	data := bytes.Repeat([]byte{0xAB}, 40) // three data blocks: 16 + 16 + 8
	b, err := EncodeFrame(LinkUnconfirmedUserData, true, 3, 1, data)
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := FrameLen(b); n != len(b) {
		t.Fatalf("FrameLen = %d, len = %d", n, len(b))
	}
	f, err := DecodeFrame(b)
	if err != nil || f.Dest != 3 || f.Src != 1 || f.Function() != LinkUnconfirmedUserData || !f.FromMaster() || !bytes.Equal(f.Data, data) {
		t.Fatalf("decoded %+v, %v", f, err)
	}
	bad := append([]byte(nil), b...)
	bad[12] ^= 0xFF // inside the first data block
	if _, err := DecodeFrame(bad); err == nil {
		t.Error("corrupted data block accepted")
	}
	bad = append([]byte(nil), b...)
	bad[4] ^= 0x01 // destination, covered by the header CRC
	if _, err := DecodeFrame(bad); err == nil {
		t.Error("corrupted header accepted")
	}
	if _, err := EncodeFrame(LinkUnconfirmedUserData, true, 3, 1, make([]byte, 251)); err == nil {
		t.Error("oversized user data accepted")
	}
}

func TestSegmentAndReassemble(t *testing.T) {
	apdu := make([]byte, 600)
	for i := range apdu {
		apdu[i] = byte(i)
	}
	segs, next := Segment(apdu, 62)
	if len(segs) != 3 || next != 1 { // 249 + 249 + 102; sequence wraps 62, 63, 0 -> next 1
		t.Fatalf("segments %d next %d", len(segs), next)
	}
	var r Reassembler
	var out []byte
	for i, s := range segs {
		got, err := r.Add(s)
		if err != nil {
			t.Fatal(err)
		}
		if i < 2 && got != nil {
			t.Fatal("fragment returned before FIN")
		}
		out = got
	}
	if !bytes.Equal(out, apdu) {
		t.Fatal("reassembled fragment differs")
	}
	// A middle segment out of sequence discards the fragment.
	var r2 Reassembler
	_, _ = r2.Add(segs[0])
	if _, err := r2.Add(segs[2]); err == nil {
		t.Error("out-of-sequence segment accepted")
	}
	one, _ := Segment([]byte{0xC0, 0x01}, 5)
	if len(one) != 1 || one[0][0] != tpFIR|tpFIN|5 {
		t.Errorf("single segment header %x", one[0][0])
	}
}

func TestRequests(t *testing.T) {
	if got := hex.EncodeToString(ReadClasses(3, 1, 2, 3)); got != "c301"+"3c0206"+"3c0306"+"3c0406" {
		t.Errorf("class 1-3 read = %s", got)
	}
	if got := hex.EncodeToString(ReadClasses(0, 0)); got != "c001"+"3c0106" {
		t.Errorf("class 0 read = %s", got)
	}
	if got := hex.EncodeToString(Confirm(7, false)); got != "c700" {
		t.Errorf("confirm = %s", got)
	}
	if got := hex.EncodeToString(Confirm(2, true)); got != "d200" {
		t.Errorf("unsolicited confirm = %s", got)
	}
}

func ms48(t time.Time) []byte {
	ms := uint64(t.UnixMilli())
	return []byte{byte(ms), byte(ms >> 8), byte(ms >> 16), byte(ms >> 24), byte(ms >> 32), byte(ms >> 40)}
}

func TestParseEventsWithAbsoluteTime(t *testing.T) {
	t0 := time.Date(2026, 11, 23, 14, 0, 0, 123e6, time.UTC)
	// Response, FIR FIN CON seq 4, IIN: class 1 events pending + need time; IIN2: event buffer overflow.
	apdu := []byte{0xE4, 0x81, 0x12, 0x08}
	// g2v2, qualifier 0x28 (2-byte index prefix, 2-byte count), 2 events: index 6 on (flags 0x81), index 10 off.
	apdu = append(apdu, 2, 2, 0x28, 2, 0)
	apdu = append(apdu, 6, 0, 0x81)
	apdu = append(apdu, ms48(t0)...)
	apdu = append(apdu, 10, 0, 0x01)
	apdu = append(apdu, ms48(t0.Add(85*time.Millisecond))...)
	r, err := ParseResponse(apdu)
	if err != nil {
		t.Fatal(err)
	}
	if !r.NeedsConfirm() || r.Seq() != 4 || !r.FIN() || r.Unsolicited() {
		t.Errorf("control %02x", r.Control)
	}
	if r.IIN&IINNeedTime == 0 || r.IIN&IINClass1Events == 0 || r.IIN&IINEventBufferOverflow == 0 {
		t.Errorf("IIN %04x", r.IIN)
	}
	if len(r.Points) != 2 {
		t.Fatalf("points %+v", r.Points)
	}
	p0, p1 := r.Points[0], r.Points[1]
	if !p0.Event || p0.Index != 6 || p0.Value != 1 || p0.Flags != 0x01 || !p0.Time.Equal(t0) {
		t.Errorf("event 0: %+v %v", p0, p0.Time)
	}
	if p1.Index != 10 || p1.Value != 0 || !p1.Time.Equal(t0.Add(85*time.Millisecond)) {
		t.Errorf("event 1: %+v %v", p1, p1.Time)
	}
}

func TestParseRelativeTimeStaticAndSkipped(t *testing.T) {
	cto := time.Date(2026, 11, 23, 14, 0, 0, 0, time.UTC)
	apdu := []byte{0xC0, 0x81, 0x00, 0x00}
	apdu = append(apdu, 51, 2, 0x07, 1) // CTO, unsynchronized, count 1
	apdu = append(apdu, ms48(cto)...)
	apdu = append(apdu, 2, 3, 0x17, 1, 5, 0x81, 250, 0)           // g2v3, 1-byte prefix, index 5 on at +250 ms
	apdu = append(apdu, 10, 2, 0x00, 0, 0, 0x01)                  // a binary output status, skipped
	apdu = append(apdu, 1, 2, 0x01, 0, 0, 2, 0, 0x81, 0x01, 0x80) // g1v2 start-stop 0..2
	apdu = append(apdu, 3, 2, 0x00, 7, 7, 0x81)                   // g3v2 index 7: double-bit on (state 2 in bits 6-7) -> 0x81 = 10 000001
	apdu = append(apdu, 1, 1, 0x00, 0, 9, 0x05, 0x02)             // g1v1 packed: indexes 0..9, bits 1,0,1,0,0,0,0,0,0,1
	r, err := ParseResponse(apdu)
	if err != nil {
		t.Fatal(err)
	}
	if r.Skipped != 1 {
		t.Errorf("skipped %d", r.Skipped)
	}
	ev := r.Points[0]
	if !ev.Event || ev.Index != 5 || !ev.Time.Equal(cto.Add(250*time.Millisecond)) || !ev.TimeUnsynced {
		t.Errorf("relative-time event: %+v %v", ev, ev.Time)
	}
	static := r.Points[1:4]
	for i, want := range []uint8{1, 0, 1} {
		if static[i].Event || static[i].Index != uint32(i) || static[i].Value != want {
			t.Errorf("static %d: %+v", i, static[i])
		}
	}
	if static[1].Flags != 0x01 {
		t.Errorf("flags without the state bit: %02x", static[1].Flags)
	}
	dbl := r.Points[4]
	if !dbl.Double || dbl.Index != 7 || dbl.Value != 2 {
		t.Errorf("double-bit: %+v", dbl)
	}
	packed := r.Points[5:]
	if len(packed) != 10 || packed[0].Value != 1 || packed[1].Value != 0 || packed[2].Value != 1 || packed[9].Value != 1 {
		t.Errorf("packed: %+v", packed)
	}
}

func TestParseMeasurements(t *testing.T) {
	at := time.Date(2026, 11, 25, 15, 30, 0, 125e6, time.UTC)
	le32 := func(v uint32) []byte { return []byte{byte(v), byte(v >> 8), byte(v >> 16), byte(v >> 24)} }
	apdu := []byte{0xC0, 0x81, 0x00, 0x00}
	// g30v1 (32-bit with flag, as the recloser's point map sends them), indexes 0..1: 123 and -7.
	apdu = append(apdu, 30, 1, 0x00, 0, 1)
	apdu = append(apdu, 0x01)
	apdu = append(apdu, le32(123)...)
	apdu = append(apdu, 0x01)
	apdu = append(apdu, le32(uint32(0xFFFFFFF9))...)
	// g30v5 (float with flag), index 10: 49.98.
	apdu = append(apdu, 30, 5, 0x00, 10, 10, 0x01)
	apdu = append(apdu, le32(math.Float32bits(49.98))...)
	// g30v4 (16-bit without flag), index 12: -1.
	apdu = append(apdu, 30, 4, 0x00, 12, 12, 0xFF, 0xFF)
	// g32v3 (32-bit event with time), 1-byte prefix, index 3: 23100 at `at`.
	apdu = append(apdu, 32, 3, 0x17, 1, 3, 0x01)
	apdu = append(apdu, le32(23100)...)
	apdu = append(apdu, ms48(at)...)
	// g20v1 (32-bit counter with flag), index 6: 4000000000 (unsigned).
	apdu = append(apdu, 20, 1, 0x00, 6, 6, 0x01)
	apdu = append(apdu, le32(4000000000)...)
	// g20v5 (32-bit counter without flag), index 7: 12.
	apdu = append(apdu, 20, 5, 0x00, 7, 7)
	apdu = append(apdu, le32(12)...)
	// g22v1 (counter event without time), 2-byte prefix, index 300: 9.
	apdu = append(apdu, 22, 1, 0x28, 1, 0, 0x2C, 0x01, 0x01)
	apdu = append(apdu, le32(9)...)
	// g21v1 (frozen counter with flag), index 2: 5.
	apdu = append(apdu, 21, 1, 0x00, 2, 2, 0x01)
	apdu = append(apdu, le32(5)...)
	// g20v3 (delta counter, obsolete): parsed and skipped.
	apdu = append(apdu, 20, 3, 0x00, 9, 9, 0x01)
	apdu = append(apdu, le32(1)...)
	r, err := ParseResponse(apdu)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Points) != 0 || r.Skipped != 1 || len(r.Measures) != 9 {
		t.Fatalf("points %d skipped %d measures %d", len(r.Points), r.Skipped, len(r.Measures))
	}
	type want struct {
		kind   string
		index  uint32
		value  float64
		event  bool
		flags  bool
		frozen bool
	}
	wants := []want{
		{KindAnalog, 0, 123, false, true, false}, {KindAnalog, 1, -7, false, true, false},
		{KindAnalog, 10, float64(float32(49.98)), false, true, false}, {KindAnalog, 12, -1, false, false, false},
		{KindAnalog, 3, 23100, true, true, false}, {KindCounter, 6, 4000000000, false, true, false},
		{KindCounter, 7, 12, false, false, false}, {KindCounter, 300, 9, true, true, false},
		{KindCounter, 2, 5, false, true, true},
	}
	for i, w := range wants {
		m := r.Measures[i]
		if m.Kind != w.kind || m.Index != w.index || m.Value != w.value || m.Event != w.event || m.HasFlags != w.flags || m.Frozen != w.frozen {
			t.Errorf("measure %d: %+v, want %+v", i, m, w)
		}
	}
	if ev := r.Measures[4]; ev.Time == nil || !ev.Time.Equal(at) {
		t.Errorf("analog event time: %v", ev.Time)
	}
}

func TestParseErrors(t *testing.T) {
	for name, apdu := range map[string][]byte{
		"short":             {0xC0, 0x81},
		"not a response":    {0xC0, 0x01, 0, 0},
		"truncated data":    {0xC0, 0x81, 0, 0, 2, 2, 0x28, 1, 0, 6, 0, 0x81, 1, 2},
		"unknown object":    {0xC0, 0x81, 0, 0, 99, 1, 0x07, 1, 0},
		"bad qualifier":     {0xC0, 0x81, 0, 0, 2, 2, 0x0F, 1},
		"stop before start": {0xC0, 0x81, 0, 0, 1, 2, 0x00, 5, 2, 0x81},
	} {
		if _, err := ParseResponse(apdu); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
