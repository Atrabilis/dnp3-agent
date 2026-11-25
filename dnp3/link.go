// Package dnp3 encodes and decodes the DNP3 (IEEE 1815) layers a polling master needs: link frames (FT3 with CRC),
// transport segments and application fragments, and the object groups a class poll can return.
package dnp3

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// CRC computes the DNP3 CRC-16 (polynomial 0x3D65, reflected 0xA6BC, inverted output).
func CRC(b []byte) uint16 {
	var crc uint16
	for _, c := range b {
		crc ^= uint16(c)
		for i := 0; i < 8; i++ {
			if crc&1 != 0 {
				crc = crc>>1 ^ 0xA6BC
			} else {
				crc >>= 1
			}
		}
	}
	return ^crc
}

// Link layer control field bits and the functions a master uses.
const (
	linkDIR = 0x80 // set on frames from the master
	linkPRM = 0x40 // primary (initiating) frame

	LinkUnconfirmedUserData = 0x04 // primary function: user data, no link confirmation
	LinkRequestStatus       = 0x09 // primary function: request link status (keep-alive)
	LinkStatusOfLink        = 0x0B // secondary function: answer to request link status

	maxLinkUserData = 250 // user data octets per link frame
)

// Frame is one decoded link frame.
type Frame struct {
	Control uint8
	Dest    uint16
	Src     uint16
	Data    []byte // user data, CRCs removed
}

// Function returns the link function code.
func (f Frame) Function() uint8 { return f.Control & 0x0F }

// FromMaster reports whether the DIR bit is set.
func (f Frame) FromMaster() bool { return f.Control&linkDIR != 0 }

// EncodeFrame builds a link frame from the master (DIR set). Primary frames carry PRM.
func EncodeFrame(function uint8, primary bool, dest, src uint16, data []byte) ([]byte, error) {
	if len(data) > maxLinkUserData {
		return nil, fmt.Errorf("link user data of %d octets (max %d)", len(data), maxLinkUserData)
	}
	ctl := linkDIR | function&0x0F
	if primary {
		ctl |= linkPRM
	}
	hdr := make([]byte, 8, 10+len(data)+2*((len(data)+15)/16))
	hdr[0], hdr[1], hdr[2], hdr[3] = 0x05, 0x64, byte(5+len(data)), ctl
	binary.LittleEndian.PutUint16(hdr[4:], dest)
	binary.LittleEndian.PutUint16(hdr[6:], src)
	out := binary.LittleEndian.AppendUint16(hdr, CRC(hdr))
	for i := 0; i < len(data); i += 16 {
		blk := data[i:min(i+16, len(data))]
		out = append(out, blk...)
		out = binary.LittleEndian.AppendUint16(out, CRC(blk))
	}
	return out, nil
}

// ErrCRC means a frame failed its CRC check.
var ErrCRC = errors.New("DNP3 link CRC error")

// FrameLen returns the total length of the frame starting at b (which must begin with 0x05 0x64 and hold at least
// the 3-byte start of the header), or an error for an invalid length field.
func FrameLen(b []byte) (int, error) {
	if len(b) < 3 || b[0] != 0x05 || b[1] != 0x64 {
		return 0, errors.New("not at a DNP3 frame start")
	}
	n := int(b[2])
	if n < 5 {
		return 0, fmt.Errorf("invalid link length %d", n)
	}
	user := n - 5
	return 10 + user + 2*((user+15)/16), nil
}

// DecodeFrame decodes one complete frame (as long as FrameLen says), checking every CRC.
func DecodeFrame(b []byte) (Frame, error) {
	total, err := FrameLen(b)
	if err != nil {
		return Frame{}, err
	}
	if len(b) < total {
		return Frame{}, errors.New("short frame")
	}
	if CRC(b[:8]) != binary.LittleEndian.Uint16(b[8:10]) {
		return Frame{}, fmt.Errorf("%w (header)", ErrCRC)
	}
	f := Frame{Control: b[3], Dest: binary.LittleEndian.Uint16(b[4:]), Src: binary.LittleEndian.Uint16(b[6:])}
	user := int(b[2]) - 5
	p := 10
	for user > 0 {
		n := min(16, user)
		blk := b[p : p+n]
		if CRC(blk) != binary.LittleEndian.Uint16(b[p+n:]) {
			return Frame{}, fmt.Errorf("%w (data block)", ErrCRC)
		}
		f.Data = append(f.Data, blk...)
		p += n + 2
		user -= n
	}
	return f, nil
}

// Transport header bits.
const (
	tpFIN = 0x80
	tpFIR = 0x40
)

// Segment splits an application fragment into transport segments (one per link frame), starting at sequence seq.
// It returns the segments and the next sequence number.
func Segment(apdu []byte, seq uint8) ([][]byte, uint8) {
	const maxSeg = maxLinkUserData - 1
	var segs [][]byte
	for i := 0; i == 0 || i < len(apdu); i += maxSeg {
		chunk := apdu[i:min(i+maxSeg, len(apdu))]
		h := seq & 0x3F
		if i == 0 {
			h |= tpFIR
		}
		if i+maxSeg >= len(apdu) {
			h |= tpFIN
		}
		segs = append(segs, append([]byte{h}, chunk...))
		seq = (seq + 1) & 0x3F
	}
	return segs, seq
}

// Reassembler rebuilds application fragments from transport segments.
type Reassembler struct {
	buf     []byte
	active  bool
	nextSeq uint8
}

// Add takes one segment and returns a complete fragment when its FIN segment arrives. Out-of-sequence or orphan
// segments discard what was collected (the outstation will repeat or the master will re-request).
func (r *Reassembler) Add(seg []byte) ([]byte, error) {
	if len(seg) < 1 {
		return nil, errors.New("empty transport segment")
	}
	h, body := seg[0], seg[1:]
	seq := h & 0x3F
	if h&tpFIR != 0 {
		r.buf, r.active, r.nextSeq = append(r.buf[:0], body...), true, (seq+1)&0x3F
	} else {
		if !r.active || seq != r.nextSeq {
			r.active = false
			return nil, fmt.Errorf("transport segment out of sequence (got %d)", seq)
		}
		r.buf = append(r.buf, body...)
		r.nextSeq = (seq + 1) & 0x3F
	}
	if h&tpFIN != 0 {
		r.active = false
		return append([]byte(nil), r.buf...), nil
	}
	return nil, nil
}
