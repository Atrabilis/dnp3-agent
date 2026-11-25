package master

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/atrabilis/dnp3-agent/dnp3"
)

// fakeOutstation is a minimal DNP3 outstation for tests. It answers class reads, keeps events until the master
// confirms the fragment that carried them, and can be told to misbehave.
type fakeOutstation struct {
	t         *testing.T
	ln        net.Listener
	addr      uint16 // outstation address
	master    uint16
	mu        sync.Mutex
	events    []fakeEvent // pending events (removed on confirm)
	static    map[uint32]bool
	analogs   map[uint32]int32  // static analog inputs (g30v1), indexes 0..n-1
	counters  map[uint32]uint32 // static counters (g20v1), indexes 0..n-1
	analogEv  int               // pending analog events (g32v3), removed when the fragment that carried them is confirmed
	analogOut map[uint8]int     // analog events sent in the fragment with this sequence
	class0    int               // class 0 reads received
	iin       uint16            // extra IIN bits in every response
	srcAddr   uint16            // address used in responses (default addr): set to test address mismatch
	silent    bool              // never answer
	maxPerFr  int               // events per fragment (multi-fragment responses when smaller than the events)
	confirms  int
	reads     int
	conns     []net.Conn
	sentOut   map[uint8][]fakeEvent // events sent in the fragment with this sequence, awaiting confirm
	linkReq   bool                  // send a link status request after the first read
	linkReqTo uint16                // destination of that request (default: the master)
	linkResp  chan struct{}
}

type fakeEvent struct {
	index uint32
	on    bool
	at    time.Time
}

func newFakeOutstation(t *testing.T, addr, master uint16) *fakeOutstation {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeOutstation{t: t, ln: ln, addr: addr, master: master, srcAddr: addr, static: map[uint32]bool{},
		analogs: map[uint32]int32{}, counters: map[uint32]uint32{}, analogOut: map[uint8]int{},
		maxPerFr: 100, sentOut: map[uint8][]fakeEvent{}, linkResp: make(chan struct{}, 1)}
	go f.accept()
	t.Cleanup(func() {
		_ = ln.Close()
		f.mu.Lock()
		for _, c := range f.conns {
			_ = c.Close()
		}
		f.mu.Unlock()
	})
	return f
}

func (f *fakeOutstation) port() int { return f.ln.Addr().(*net.TCPAddr).Port }

func (f *fakeOutstation) addEvent(index uint32, on bool, at time.Time) {
	f.mu.Lock()
	f.events = append(f.events, fakeEvent{index, on, at})
	f.mu.Unlock()
}

func (f *fakeOutstation) pending() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func (f *fakeOutstation) dropConnections() {
	f.mu.Lock()
	for _, c := range f.conns {
		_ = c.Close()
	}
	f.conns = nil
	f.mu.Unlock()
}

func (f *fakeOutstation) accept() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		f.mu.Lock()
		f.conns = append(f.conns, c)
		f.sentOut = map[uint8][]fakeEvent{} // unconfirmed fragments of an old connection are sent again
		f.analogOut = map[uint8]int{}
		f.mu.Unlock()
		go f.serve(c)
	}
}

func (f *fakeOutstation) serve(c net.Conn) {
	var buf []byte
	var rs dnp3.Reassembler
	var appSeq, tpSeq uint8
	tmp := make([]byte, 2048)
	send := func(apdu []byte, function uint8, primary bool) {
		segs, next := dnp3.Segment(apdu, tpSeq)
		tpSeq = next
		if apdu == nil {
			segs = [][]byte{nil}
		}
		for _, s := range segs {
			fr, _ := dnp3.EncodeFrame(function, primary, f.master, f.srcAddr, s)
			fr[3] &^= 0x80 // DIR off: from the outstation
			// Recompute the header CRC after changing the control field.
			binary.LittleEndian.PutUint16(fr[8:], dnp3.CRC(fr[:8]))
			_, _ = c.Write(fr)
		}
	}
	for {
		n, err := c.Read(tmp)
		if err != nil {
			return
		}
		buf = append(buf, tmp[:n]...)
		for len(buf) >= 10 {
			total, ferr := dnp3.FrameLen(buf)
			if ferr != nil || len(buf) < total {
				break
			}
			fr, derr := dnp3.DecodeFrame(buf[:total])
			buf = buf[total:]
			if derr != nil {
				f.t.Errorf("outstation got a bad frame: %v", derr)
				continue
			}
			if fr.Function() == dnp3.LinkStatusOfLink {
				f.linkResp <- struct{}{}
				continue
			}
			apdu, _ := rs.Add(fr.Data)
			if apdu == nil {
				continue
			}
			f.mu.Lock()
			silent := f.silent
			f.mu.Unlock()
			if silent {
				continue
			}
			switch apdu[1] {
			case dnp3.FuncConfirm:
				f.mu.Lock()
				f.confirms++
				seq := apdu[0] & 0x0F
				for _, e := range f.sentOut[seq] {
					for i, p := range f.events {
						if p == e {
							f.events = append(f.events[:i], f.events[i+1:]...)
							break
						}
					}
				}
				delete(f.sentOut, seq)
				f.analogEv -= f.analogOut[seq]
				delete(f.analogOut, seq)
				f.mu.Unlock()
			case dnp3.FuncRead:
				f.mu.Lock()
				f.reads++
				linkReq := f.linkReq && f.reads == 1
				f.mu.Unlock()
				for _, fragApdu := range f.answer(apdu, &appSeq) {
					send(fragApdu, dnp3.LinkUnconfirmedUserData, true)
				}
				if linkReq {
					f.mu.Lock()
					to := f.linkReqTo
					f.mu.Unlock()
					if to == 0 {
						send(nil, dnp3.LinkRequestStatus, true)
					} else {
						fr, _ := dnp3.EncodeFrame(dnp3.LinkRequestStatus, true, to, f.srcAddr, nil)
						fr[3] &^= 0x80
						binary.LittleEndian.PutUint16(fr[8:], dnp3.CRC(fr[:8]))
						_, _ = c.Write(fr)
					}
				}
			}
		}
	}
}

// answer builds the response fragments to a READ: class 0 static, or classes 1-3 events (CON set, kept until
// confirmed).
func (f *fakeOutstation) answer(req []byte, appSeq *uint8) [][]byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	reqSeq := req[0] & 0x0F
	class0 := len(req) >= 5 && req[3] == 1
	iin := f.iin
	if len(f.events) > 0 {
		iin |= dnp3.IINClass1Events
	}
	if class0 {
		f.class0++
		apdu := []byte{dnp3.AppFIR | dnp3.AppFIN | reqSeq, dnp3.FuncResponse, byte(iin), byte(iin >> 8)}
		if len(f.analogs) > 0 {
			apdu = append(apdu, 30, 1, 0x00, 0, byte(len(f.analogs)-1))
			for i := 0; i < len(f.analogs); i++ {
				v := uint32(f.analogs[uint32(i)])
				apdu = append(apdu, 0x01, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
			}
		}
		if len(f.counters) > 0 {
			apdu = append(apdu, 20, 1, 0x00, 0, byte(len(f.counters)-1))
			for i := 0; i < len(f.counters); i++ {
				v := f.counters[uint32(i)]
				apdu = append(apdu, 0x01, byte(v), byte(v>>8), byte(v>>16), byte(v>>24))
			}
		}
		if len(f.counters) > 0 {
			// Frozen counters with the same indexes, never frozen (value 0, flags online+restart+discontinuity), as a
			// real recloser sends them.
			apdu = append(apdu, 21, 1, 0x00, 0, byte(len(f.counters)-1))
			for i := 0; i < len(f.counters); i++ {
				apdu = append(apdu, 0x43, 0, 0, 0, 0)
			}
		}
		if len(f.static) > 0 {
			apdu = append(apdu, 1, 2, 0x00, 0, byte(len(f.static)-1))
			for i := 0; i < len(f.static); i++ {
				b := byte(0x01)
				if f.static[uint32(i)] {
					b |= 0x80
				}
				apdu = append(apdu, b)
			}
		}
		return [][]byte{apdu}
	}
	// Events not yet sent in an unconfirmed fragment.
	var todo []fakeEvent
	sent := map[fakeEvent]bool{}
	for _, list := range f.sentOut {
		for _, e := range list {
			sent[e] = true
		}
	}
	for _, e := range f.events {
		if !sent[e] {
			todo = append(todo, e)
		}
	}
	analogNow := 0
	if len(f.analogOut) == 0 {
		analogNow = f.analogEv
	}
	if len(todo) == 0 && analogNow == 0 {
		return [][]byte{{dnp3.AppFIR | dnp3.AppFIN | reqSeq, dnp3.FuncResponse, byte(iin), byte(iin >> 8)}}
	}
	if len(todo) == 0 {
		// Only analog events: one fragment that asks for confirmation.
		apdu := []byte{dnp3.AppFIR | dnp3.AppFIN | dnp3.AppCON | reqSeq, dnp3.FuncResponse, byte(iin), byte(iin >> 8)}
		apdu = append(apdu, f.analogEvents(analogNow)...)
		f.analogOut[reqSeq] = analogNow
		*appSeq = (reqSeq + 1) & 0x0F
		return [][]byte{apdu}
	}
	var out [][]byte
	seq := reqSeq
	for i := 0; i < len(todo); i += f.maxPerFr {
		chunk := todo[i:min(i+f.maxPerFr, len(todo))]
		ac := byte(dnp3.AppCON) | seq
		if i == 0 {
			ac |= dnp3.AppFIR
		}
		if i+f.maxPerFr >= len(todo) {
			ac |= dnp3.AppFIN
		}
		apdu := []byte{ac, dnp3.FuncResponse, byte(iin), byte(iin >> 8), 2, 2, 0x28, byte(len(chunk)), 0}
		for _, e := range chunk {
			flags := byte(0x01)
			if e.on {
				flags |= 0x80
			}
			apdu = append(apdu, byte(e.index), byte(e.index>>8), flags)
			ms := uint64(e.at.UnixMilli())
			for k := 0; k < 6; k++ {
				apdu = append(apdu, byte(ms>>(8*k)))
			}
		}
		if i == 0 && analogNow > 0 {
			apdu = append(apdu, f.analogEvents(analogNow)...)
			f.analogOut[seq] = analogNow
		}
		f.sentOut[seq] = append([]fakeEvent(nil), chunk...)
		out = append(out, apdu)
		seq = (seq + 1) & 0x0F
	}
	*appSeq = seq
	return out
}

// counts returns how many reads and confirmations the outstation received.
func (f *fakeOutstation) counts() (reads, confirms int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.reads, f.confirms
}

// analogEvents encodes n analog input events (g32v3, 1-byte index prefix) for index 0 with a fixed value and time.
func (f *fakeOutstation) analogEvents(n int) []byte {
	b := []byte{32, 3, 0x17, byte(n)}
	at := uint64(time.Date(2026, 11, 25, 12, 0, 0, 0, time.UTC).UnixMilli())
	for i := 0; i < n; i++ {
		b = append(b, 0, 0x01, 100, 0, 0, 0)
		for k := 0; k < 6; k++ {
			b = append(b, byte(at>>(8*k)))
		}
	}
	return b
}

// analogPending returns the analog events not yet confirmed and the class 0 reads received.
func (f *fakeOutstation) analogPending() (pending, class0 int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.analogEv, f.class0
}
