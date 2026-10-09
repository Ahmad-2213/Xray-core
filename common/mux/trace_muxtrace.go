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
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
)

// fpTuple pins one counted frame: its cumulative number, session, the
// session's byte offset before it, length, payload CRC and direction
// ("up" client→server, "down" server→client).
type fpTuple struct {
	seq uint64
	sid uint16
	off uint64
	ln  int
	crc uint32
	dir string
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

// traceRetain records a sender-side counted frame at store time. ln/crc
// cover the payload exactly as the production chain sees it (post-meta
// bytes), so sender and receiver tuples are directly comparable.
func traceRetain(g *carrierGate, seq uint64, sid uint16, ln int, crc uint32) {
	traceMu.Lock()
	defer traceMu.Unlock()
	m := traceSendOff[g]
	if m == nil {
		m = map[uint16]uint64{}
		traceSendOff[g] = m
	}
	off := m[sid]
	m[sid] = off + uint64(ln)
	traceSendRing = fpAppendRing(traceSendRing, fpTuple{seq, sid, off, ln, crc, g.dir})
}

// traceAdmit records a receiver-side counted frame at admit time. s may
// be nil (unknown session); mb may be nil (already consumed, e.g. End).
// dir is "down" on the client, "up" on the server.
func traceAdmit(s *Session, n uint64, sid uint16, mb buf.MultiBuffer, dir string) {
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
	traceRecvRing = fpAppendRing(traceRecvRing, fpTuple{n, sid, off, ln, crc, dir})
}

// traceReset clears rings and offsets. Called from HsResetForTest so
// each test starts with clean diagnostic state (rings are process-global
// and would otherwise mix tuples across tests sharing counter values).
func traceReset() {
	traceMu.Lock()
	defer traceMu.Unlock()
	traceSendRing = nil
	traceRecvRing = nil
	traceSendOff = map[*carrierGate]map[uint16]uint64{}
	traceRecvOff = map[*Session]uint64{}
}

// traceLogRecv dumps the admitted tuples with a prefix and the
// snapshot value the worker sent (snapRx). Full dump (no 16-cap):
// used to diff whole chains across a drift.
func traceLogRecv(prefix string, snapRx uint64) {
	traceMu.Lock()
	defer traceMu.Unlock()
	errors.LogInfo(context.Background(), "mux trace: ", prefix, " snapRx ", snapRx, " recvRing ", len(traceRecvRing))
	for _, t := range traceRecvRing {
		errors.LogInfo(context.Background(), "mux trace: ", prefix, " recv seq ", t.seq, " sid ", t.sid, " off ", t.off, " len ", t.ln, " crc ", t.crc, " dir ", t.dir)
	}
}

// traceDumpAdopt appends both rings to a file (console wrapping mangles
// long log lines, so file output is the only faithful channel). Append
// mode: every adopt adds an episode block, so multi-episode runs keep
// full history instead of overwriting.
func traceDumpAdopt(path string, snapRx, peerRx uint64) {
	traceMu.Lock()
	defer traceMu.Unlock()
	var sb strings.Builder
	sb.WriteString("=== adopt snapRx ")
	sb.WriteString(strconv.FormatUint(snapRx, 10))
	sb.WriteString(" peerRx ")
	sb.WriteString(strconv.FormatUint(peerRx, 10))
	sb.WriteString("\n")
	for _, t := range traceRecvRing {
		sb.WriteString("R ")
		writeTuple(&sb, t)
	}
	for _, t := range traceSendRing {
		sb.WriteString("S ")
		writeTuple(&sb, t)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()
	f.WriteString(sb.String())
}

func writeTuple(sb *strings.Builder, t fpTuple) {
	sb.WriteString(strconv.FormatUint(t.seq, 10))
	sb.WriteByte(' ')
	sb.WriteString(strconv.Itoa(int(t.sid)))
	sb.WriteByte(' ')
	sb.WriteString(strconv.FormatUint(t.off, 10))
	sb.WriteByte(' ')
	sb.WriteString(strconv.Itoa(t.ln))
	sb.WriteByte(' ')
	sb.WriteString(strconv.FormatUint(uint64(t.crc), 10))
	sb.WriteByte(' ')
	sb.WriteString(t.dir)
	sb.WriteByte('\n')
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
// called wherever a flush decision is made. Full window (no ±8 filter)
// for whole-chain diffs.
func traceLogSendWindow(prefix string, peerRx uint64) {
	traceMu.Lock()
	defer traceMu.Unlock()
	errors.LogInfo(context.Background(), "mux trace: ", prefix, " peerRx ", peerRx, " sendRing ", len(traceSendRing))
	for _, t := range traceSendRing {
		errors.LogInfo(context.Background(), "mux trace: ", prefix, " send seq ", t.seq, " sid ", t.sid, " off ", t.off, " len ", t.ln, " crc ", t.crc, " dir ", t.dir)
	}
}
