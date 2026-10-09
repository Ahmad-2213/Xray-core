//go:build muxtrace

package mux

// Content fingerprints for resume diagnosis. Tag-gated: production
// builds (without muxtrace) link the no-op stubs in trace_notrace.go
// instead, so the hot path pays one call at most. Counts already agree
// in the failing windows, so the next discriminator is content: sender
// tuples at retain time vs receiver tuples at admit time.

import (
	"context"
	"hash/crc32"
	"sync"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
)

// fpTuple pins one counted frame: its cumulative number, session, the
// session's byte offset before it, length and payload CRC.
type fpTuple struct {
	seq uint64
	sid uint16
	off uint64
	ln  int
	crc uint32
}

const fpRingCap = 2048

var (
	traceMu       sync.Mutex
	traceSendRing []fpTuple
	traceSendOff  = map[*carrierGate]map[uint16]uint64{}
	traceRecvRing []fpTuple
	traceRecvOff  = map[*Session]uint64{}
)

func fpAppendRing(ring []fpTuple, t fpTuple) []fpTuple {
	ring = append(ring, t)
	if len(ring) > fpRingCap {
		ring = append([]fpTuple(nil), ring[len(ring)-fpRingCap:]...)
	}
	return ring
}

// traceRetain records a sender-side counted frame at store time.
func traceRetain(g *carrierGate, seq uint64, sid uint16, raw []byte) {
	traceMu.Lock()
	defer traceMu.Unlock()
	m := traceSendOff[g]
	if m == nil {
		m = map[uint16]uint64{}
		traceSendOff[g] = m
	}
	off := m[sid]
	m[sid] = off + uint64(len(raw))
	traceSendRing = fpAppendRing(traceSendRing, fpTuple{seq, sid, off, len(raw), crc32.ChecksumIEEE(raw)})
}

// traceAdmit records a receiver-side counted frame at admit time. s may
// be nil (unknown session); mb may be nil (already consumed, e.g. End).
func traceAdmit(s *Session, n uint64, sid uint16, mb buf.MultiBuffer) {
	ln, crc := 0, uint32(0)
	if mb != nil {
		h := crc32.NewIEEE()
		for _, b := range mb {
			by := b.Bytes()
			ln += len(by)
			h.Write(by)
		}
		crc = h.Sum32()
	}
	traceMu.Lock()
	defer traceMu.Unlock()
	var off uint64
	if s != nil {
		off = traceRecvOff[s]
		traceRecvOff[s] = off + uint64(ln)
	}
	traceRecvRing = fpAppendRing(traceRecvRing, fpTuple{n, sid, off, ln, crc})
}

// traceLogRecv dumps the last 16 admitted tuples with a prefix and the
// snapshot value the worker sent (snapRx).
func traceLogRecv(prefix string, snapRx uint64) {
	traceMu.Lock()
	defer traceMu.Unlock()
	errors.LogInfo(context.Background(), "mux trace: ", prefix, " snapRx ", snapRx, " recvRing ", len(traceRecvRing))
	start := 0
	if len(traceRecvRing) > 16 {
		start = len(traceRecvRing) - 16
	}
	for _, t := range traceRecvRing[start:] {
		errors.LogInfo(context.Background(), "mux trace: ", prefix, " recv seq ", t.seq, " sid ", t.sid, " off ", t.off, " len ", t.ln, " crc ", t.crc)
	}
}

// traceLogFlush records the first 8 seqs a flush actually wrote.
func traceLogFlush(id uint64, label string, seqs []uint64) {
	n := len(seqs)
	if n > 8 {
		seqs = seqs[:8]
	}
	errors.LogInfo(context.Background(), "mux trace: flush#", id, " ", label, " flushed ", n, " seqs ", seqs)
}

// traceLogSendWindow dumps retained sender tuples around peerRx and is
// called wherever a flush decision is made.
func traceLogSendWindow(prefix string, peerRx uint64) {
	traceMu.Lock()
	defer traceMu.Unlock()
	errors.LogInfo(context.Background(), "mux trace: ", prefix, " peerRx ", peerRx, " sendRing ", len(traceSendRing))
	for _, t := range traceSendRing {
		if t.seq+4 >= peerRx && t.seq <= peerRx+8 {
			errors.LogInfo(context.Background(), "mux trace: ", prefix, " send seq ", t.seq, " sid ", t.sid, " off ", t.off, " len ", t.ln, " crc ", t.crc)
		}
	}
}
