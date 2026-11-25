package dnp3

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

// Application control bits.
const (
	AppFIR = 0x80
	AppFIN = 0x40
	AppCON = 0x20
	AppUNS = 0x10
)

// Application function codes used by a polling master.
const (
	FuncConfirm             = 0x00
	FuncRead                = 0x01
	FuncWrite               = 0x02
	FuncResponse            = 0x81
	FuncUnsolicitedResponse = 0x82
)

// IIN bits (IIN1 in the low byte, IIN2 in the high byte).
const (
	IINClass1Events        = 0x0002
	IINClass2Events        = 0x0004
	IINClass3Events        = 0x0008
	IINNeedTime            = 0x0010
	IINLocalControl        = 0x0020
	IINDeviceTrouble       = 0x0040
	IINDeviceRestart       = 0x0080
	IINFuncNotSupported    = 0x0100
	IINObjectUnknown       = 0x0200
	IINParameterError      = 0x0400
	IINEventBufferOverflow = 0x0800
	IINAlreadyExecuting    = 0x1000
	IINConfigCorrupt       = 0x2000
)

// ReadClasses builds a READ request for the given classes (0 = static data, 1-3 = events), all objects (qualifier 6).
func ReadClasses(seq uint8, classes ...int) []byte {
	apdu := []byte{AppFIR | AppFIN | seq&0x0F, FuncRead}
	for _, c := range classes {
		apdu = append(apdu, 60, byte(c+1), 0x06) // group 60 variation 1 = class 0, 2..4 = classes 1..3
	}
	return apdu
}

// Confirm builds an application confirmation for a response fragment with sequence seq.
func Confirm(seq uint8, unsolicited bool) []byte {
	ac := byte(AppFIR | AppFIN | seq&0x0F)
	if unsolicited {
		ac |= AppUNS
	}
	return []byte{ac, FuncConfirm}
}

// Response is a decoded response fragment.
type Response struct {
	Control  uint8
	Function uint8
	IIN      uint16
	Points   []Point       // binary and double-bit inputs (the event table)
	Measures []Measurement // analog inputs and counters (telemetry)
	Skipped  int           // objects of groups the master does not record (binary outputs, ...), parsed only to skip them
}

// Seq returns the application sequence number.
func (r Response) Seq() uint8 { return r.Control & 0x0F }

// NeedsConfirm reports whether the outstation asked for an application confirmation.
func (r Response) NeedsConfirm() bool { return r.Control&AppCON != 0 }

// Unsolicited reports whether this is an unsolicited response.
func (r Response) Unsolicited() bool { return r.Control&AppUNS != 0 }

// FIN reports whether this is the last fragment of the response.
func (r Response) FIN() bool { return r.Control&AppFIN != 0 }

// Point is one binary or double-bit input value: static (current state) or event (a change with its time).
type Point struct {
	Group, Variation uint8
	Index            uint32
	Event            bool
	Double           bool       // double-bit input (0 intermediate, 1 off, 2 on, 3 indeterminate)
	Value            uint8      // 0/1 for binary inputs, 0..3 for double-bit inputs
	Flags            uint8      // online, restart, comm lost, forced, chatter (state bits removed)
	Time             *time.Time // events with time
	TimeUnsynced     bool       // the time came with an unsynchronized common time of occurrence (g51v2)
}

// Measurement kinds.
const (
	KindAnalog  = "AI"
	KindCounter = "Counter"
)

// Measurement is one analog input or counter value: static (g30, g20, g21) or event (g32, g22).
type Measurement struct {
	Group, Variation uint8
	Kind             string // KindAnalog or KindCounter
	Index            uint32
	Event            bool
	Frozen           bool       // frozen counter (g21, g23)
	Value            float64    // as sent: integers are converted, no scaling
	Flags            uint8      // online, restart, comm lost, remote forced, local forced, over-range, ... (0 when the variation has no flags)
	HasFlags         bool       // false for the "without flag" variations
	Time             *time.Time // events and frozen counters with time
	TimeUnsynced     bool
}

// ParseResponse decodes a response or unsolicited response fragment.
func ParseResponse(apdu []byte) (Response, error) {
	if len(apdu) < 4 {
		return Response{}, errors.New("response shorter than its header")
	}
	r := Response{Control: apdu[0], Function: apdu[1], IIN: uint16(apdu[2]) | uint16(apdu[3])<<8}
	if r.Function != FuncResponse && r.Function != FuncUnsolicitedResponse {
		return r, fmt.Errorf("unexpected application function 0x%02x", r.Function)
	}
	p := newParser(apdu[4:])
	var cto *time.Time
	ctoUnsynced := false
	for !p.done() {
		g, v, q, err := p.header()
		if err != nil {
			return r, err
		}
		idx, count, prefixed, err := p.rangeOf(q)
		if err != nil {
			return r, fmt.Errorf("g%dv%d: %w", g, v, err)
		}
		switch {
		case (g == 1 && v == 1) || (g == 3 && v == 1) || (g == 10 && v == 1) || (g == 80 && v == 1):
			// Packed bits (2 bits per point for double-bit inputs), only with a start-stop range.
			bits := 1
			if g == 3 {
				bits = 2
			}
			if prefixed {
				return r, fmt.Errorf("g%dv%d with an index prefix is not valid", g, v)
			}
			raw, err := p.take((count*bits + 7) / 8)
			if err != nil {
				return r, err
			}
			if g == 1 || g == 3 {
				for i := 0; i < count; i++ {
					val := (raw[i*bits/8] >> (i * bits % 8)) & byte(1<<bits-1)
					r.Points = append(r.Points, Point{Group: g, Variation: v, Index: idx + uint32(i), Double: g == 3,
						Value: val, Flags: 0x01})
				}
			} else {
				r.Skipped += count
			}
		case g == 51 && (v == 1 || v == 2):
			b, err := p.take(6 * count)
			if err != nil {
				return r, err
			}
			t := dnpTime(b[len(b)-6:])
			cto, ctoUnsynced = &t, v == 2
		default:
			size, ok := objectSize(g, v)
			if !ok {
				return r, fmt.Errorf("g%dv%d: object size unknown, cannot skip it", g, v)
			}
			for i := 0; i < count; i++ {
				index := idx + uint32(i)
				if prefixed {
					if index, err = p.prefix(q); err != nil {
						return r, err
					}
				}
				b, err := p.take(size)
				if err != nil {
					return r, err
				}
				if pt, isPoint := decodePoint(g, v, index, b, cto, ctoUnsynced); isPoint {
					r.Points = append(r.Points, pt)
				} else if m, isMeasure := decodeMeasurement(g, v, index, b); isMeasure {
					r.Measures = append(r.Measures, m)
				} else {
					r.Skipped++
				}
			}
		}
	}
	return r, nil
}

// decodePoint decodes binary input (g1v2, g2v1-3) and double-bit input (g3v2, g4v1-3) objects.
func decodePoint(g, v uint8, index uint32, b []byte, cto *time.Time, ctoUnsynced bool) (Point, bool) {
	pt := Point{Group: g, Variation: v, Index: index}
	switch g {
	case 1, 2:
		pt.Value, pt.Flags = b[0]>>7&1, b[0]&0x7F
	case 3, 4:
		pt.Double, pt.Value, pt.Flags = true, b[0]>>6&3, b[0]&0x3F
	default:
		return pt, false
	}
	if g == 2 || g == 4 {
		pt.Event = true
		switch v {
		case 2:
			t := dnpTime(b[1:7])
			pt.Time = &t
		case 3:
			if cto != nil {
				t := cto.Add(time.Duration(binary.LittleEndian.Uint16(b[1:3])) * time.Millisecond)
				pt.Time, pt.TimeUnsynced = &t, ctoUnsynced
			}
		}
	}
	return pt, true
}

// decodeMeasurement decodes analog inputs (g30, g32) and counters (g20, g21, g22, g23). Delta counter variations
// (obsolete) are not decoded.
func decodeMeasurement(g, v uint8, index uint32, b []byte) (Measurement, bool) {
	m := Measurement{Group: g, Variation: v, Index: index}
	i32 := func(o int) float64 { return float64(int32(binary.LittleEndian.Uint32(b[o:]))) }
	i16 := func(o int) float64 { return float64(int16(binary.LittleEndian.Uint16(b[o:]))) }
	u32 := func(o int) float64 { return float64(binary.LittleEndian.Uint32(b[o:])) }
	u16 := func(o int) float64 { return float64(binary.LittleEndian.Uint16(b[o:])) }
	f32 := func(o int) float64 { return float64(math.Float32frombits(binary.LittleEndian.Uint32(b[o:]))) }
	f64 := func(o int) float64 { return math.Float64frombits(binary.LittleEndian.Uint64(b[o:])) }
	withTime := func(o int) {
		t := dnpTime(b[o : o+6])
		m.Time = &t
	}
	flag := func() { m.Flags, m.HasFlags = b[0], true }
	switch g {
	case 30, 32: // analog input, analog input event
		m.Kind, m.Event = KindAnalog, g == 32
		switch {
		case v == 1:
			flag()
			m.Value = i32(1)
		case v == 2:
			flag()
			m.Value = i16(1)
		case g == 30 && v == 3:
			m.Value = i32(0)
		case g == 30 && v == 4:
			m.Value = i16(0)
		case g == 30 && v == 5, g == 32 && v == 5:
			flag()
			m.Value = f32(1)
		case g == 30 && v == 6, g == 32 && v == 6:
			flag()
			m.Value = f64(1)
		case g == 32 && v == 3:
			flag()
			m.Value = i32(1)
			withTime(5)
		case g == 32 && v == 4:
			flag()
			m.Value = i16(1)
			withTime(3)
		case g == 32 && v == 7:
			flag()
			m.Value = f32(1)
			withTime(5)
		case g == 32 && v == 8:
			flag()
			m.Value = f64(1)
			withTime(9)
		default:
			return m, false
		}
	case 20, 22: // counter, counter event (variations 3, 4, 7 and 8 are delta counters)
		m.Kind, m.Event = KindCounter, g == 22
		switch {
		case v == 1:
			flag()
			m.Value = u32(1)
		case v == 2:
			flag()
			m.Value = u16(1)
		case g == 20 && v == 5:
			m.Value = u32(0)
		case g == 20 && v == 6:
			m.Value = u16(0)
		case g == 22 && v == 5:
			flag()
			m.Value = u32(1)
			withTime(5)
		case g == 22 && v == 6:
			flag()
			m.Value = u16(1)
			withTime(3)
		default:
			return m, false
		}
	case 21, 23: // frozen counter, frozen counter event
		m.Kind, m.Event, m.Frozen = KindCounter, g == 23, true
		switch {
		case v == 1:
			flag()
			m.Value = u32(1)
		case v == 2:
			flag()
			m.Value = u16(1)
		case v == 5:
			flag()
			m.Value = u32(1)
			withTime(5)
		case v == 6:
			flag()
			m.Value = u16(1)
			withTime(3)
		case g == 21 && v == 9:
			m.Value = u32(0)
		case g == 21 && v == 10:
			m.Value = u16(0)
		default:
			return m, false
		}
	default:
		return m, false
	}
	return m, true
}

// dnpTime decodes a 48-bit count of milliseconds since 1970-01-01 UTC.
func dnpTime(b []byte) time.Time {
	ms := uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 | uint64(b[4])<<32 | uint64(b[5])<<40
	return time.UnixMilli(int64(ms)).UTC()
}

// objectSize is the size of one object (without index prefix) for the fixed-size groups a class poll can return.
func objectSize(g, v uint8) (int, bool) {
	sizes := map[uint8][]int{ // index = variation; 0 = not defined
		1:  {0, 0, 1},
		2:  {0, 1, 7, 3},
		3:  {0, 0, 1},
		4:  {0, 1, 7, 3},
		10: {0, 0, 1},
		11: {0, 1, 7},
		20: {0, 5, 3, 5, 3, 4, 2, 4, 2},
		21: {0, 5, 3, 5, 3, 11, 9, 11, 9, 4, 2, 4, 2},
		22: {0, 5, 3, 5, 3, 11, 9, 11, 9},
		23: {0, 5, 3, 5, 3, 11, 9, 11, 9},
		30: {0, 5, 3, 4, 2, 5, 9},
		31: {0, 5, 3, 11, 9, 4, 2, 5, 9},
		32: {0, 5, 3, 11, 9, 5, 9, 11, 15},
		33: {0, 5, 3, 11, 9, 5, 9, 11, 15},
		40: {0, 5, 3, 5, 9},
		42: {0, 5, 3, 11, 9, 5, 9, 11, 15},
		50: {0, 6},
		52: {0, 2, 2},
	}
	s, ok := sizes[g]
	if !ok || int(v) >= len(s) || s[v] == 0 {
		return 0, false
	}
	return s[v], true
}

type parser struct {
	b []byte
	p int
}

func newParser(b []byte) *parser { return &parser{b: b} }
func (p *parser) done() bool     { return p.p >= len(p.b) }

func (p *parser) take(n int) ([]byte, error) {
	if n < 0 || p.p+n > len(p.b) {
		return nil, errors.New("object data truncated")
	}
	b := p.b[p.p : p.p+n]
	p.p += n
	return b, nil
}

func (p *parser) header() (g, v, q uint8, err error) {
	b, err := p.take(3)
	if err != nil {
		return 0, 0, 0, errors.New("object header truncated")
	}
	return b[0], b[1], b[2], nil
}

// rangeOf reads the range field of qualifier q: start index and count, and whether each object has an index prefix.
func (p *parser) rangeOf(q uint8) (start uint32, count int, prefixed bool, err error) {
	prefix, rng := q>>4&0x7, q&0x0F
	switch rng {
	case 0x0, 0x1, 0x2: // start-stop with 1, 2 or 4 octets
		n := 1 << rng
		b, err := p.take(2 * n)
		if err != nil {
			return 0, 0, false, err
		}
		a, z := uint32(leUint(b[:n])), uint32(leUint(b[n:]))
		if z < a {
			return 0, 0, false, fmt.Errorf("stop %d before start %d", z, a)
		}
		return a, int(z-a) + 1, prefix != 0, nil
	case 0x7, 0x8, 0x9: // count with 1, 2 or 4 octets
		n := 1 << (rng - 7)
		b, err := p.take(n)
		if err != nil {
			return 0, 0, false, err
		}
		return 0, int(leUint(b)), prefix != 0, nil
	case 0x6:
		return 0, 0, false, nil
	default:
		return 0, 0, false, fmt.Errorf("qualifier 0x%02x not supported", q)
	}
}

// prefix reads one index prefix (qualifier prefix codes 1, 2 and 3: 1, 2 or 4 octets).
func (p *parser) prefix(q uint8) (uint32, error) {
	code := q >> 4 & 0x7
	if code < 1 || code > 3 {
		return 0, fmt.Errorf("prefix code %d not supported", code)
	}
	b, err := p.take(1 << (code - 1))
	if err != nil {
		return 0, err
	}
	return uint32(leUint(b)), nil
}

func leUint(b []byte) uint64 {
	var v uint64
	for i := len(b) - 1; i >= 0; i-- {
		v = v<<8 | uint64(b[i])
	}
	return v
}
