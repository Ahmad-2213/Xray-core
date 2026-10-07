package mux

// Suspend-capable carrier gate (Phase 2).
//
// Byte flow (resume enabled):
//
//	session Writer -> gate -> carrier pipe -> transport
//
// The gate owns the cumulative transmit counter, the bounded retain store
// (private copies, never pooled buffers) and the suspend flag. Session
// goroutines never see carrier errors while a resume is possible: bytes are
// durably stored first and forwarded afterwards, so a failed forward is
// always replayable. Errors surface only when the worker finally closes.
//
// Counting rule (identical on both ends, derived from the same wire bytes):
//   New:  counted iff target network is TCP.
//   Keep: counted iff OptionData is set and the frame is not UDP-marked.
//   End:  always counted.
//   KeepAlive/Resume/Ack: never counted.
// UDP payloads are never retained.

import (
	"io"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/bitmask"
	"github.com/xtls/xray-core/common/buf"
)

// countedFrame reports whether a transmitted frame advances the cumulative
// counters. metaLen is the metadata length prefix; netByteValid/netByte carry
// the target-network byte when present (New always, Keep only when UDP).
func countedFrame(status SessionStatus, opt bitmask.Byte, metaLen int, netByte byte, hasNet bool) bool {
	switch status {
	case SessionStatusNew:
		return hasNet && netByte != byte(TargetNetworkUDP)
	case SessionStatusKeep:
		if !opt.Has(OptionData) {
			return false
		}
		if hasNet && netByte == byte(TargetNetworkUDP) {
			return false
		}
		return true
	case SessionStatusEnd:
		return true
	default:
		return false
	}
}

// parseMetaPrefix extracts status/option/length/network from a meta frame
// buffer as produced by FrameMetadata.WriteTo: [len u16][sid u16][st][op]...
func parseMetaPrefix(b []byte) (status SessionStatus, opt bitmask.Byte, metaLen int, netByte byte, hasNet bool, ok bool) {
	if len(b) < 6 {
		return 0, 0, 0, 0, false, false
	}
	metaLen = int(uint16(b[0])<<8 | uint16(b[1]))
	if len(b) < 2+metaLen {
		return 0, 0, 0, 0, false, false
	}
	status = SessionStatus(b[4])
	opt = bitmask.Byte(b[5])
	if metaLen > 4 {
		netByte = b[6]
		hasNet = true
	}
	return status, opt, metaLen, netByte, hasNet, true
}

// Exported for unit tests (package mux_test).
func CountedFrameForTest(status SessionStatus, opt bitmask.Byte, metaLen int, netByte byte, hasNet bool) bool {
	return countedFrame(status, opt, metaLen, netByte, hasNet)
}

func ParseMetaPrefixForTest(b []byte) (SessionStatus, bitmask.Byte, int, byte, bool, bool) {
	return parseMetaPrefix(b)
}

type storedFrame struct {
	seq uint64
	sid uint16
	raw []byte
}

type carrierGate struct {
	mu        sync.Mutex
	suspended bool
	done      <-chan struct{}
	target    buf.Writer
	policy    ResumePolicy

	tx             Counter
	frames         []storedFrame
	baseSeq        uint64
	storedByte     int64
	onCarrierError func()

	ackCh      chan struct{}
	resumeCh   chan struct{}
	pendingAck uint64
	hasPending bool

	ackRecvMu   sync.Mutex
	lastAckTime time.Time
}

func newCarrierGate(target buf.Writer, done <-chan struct{}, policy ResumePolicy) *carrierGate {
	if !policy.Enabled {
		policy = DisabledPolicy()
	}
	return &carrierGate{
		target:      target,
		done:        done,
		policy:      policy,
		ackCh:       make(chan struct{}, 1),
		resumeCh:    make(chan struct{}, 1),
		lastAckTime: time.Now(),
	}
}

func (g *carrierGate) notifyAck() {
	select {
	case g.ackCh <- struct{}{}:
	default:
	}
}

func (g *carrierGate) notifyResume() {
	select {
	case g.resumeCh <- struct{}{}:
	default:
	}
}

// TxCount returns frames assigned so far. Unacked = TxCount - ackedUpTo.
func (g *carrierGate) TxCount() uint64 { return g.tx.Value() }

// UnackedBytes returns retained bytes not yet acknowledged.
func (g *carrierGate) UnackedBytes() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.storedByte
}

// setSuspended parks forwarding; queued bytes stay stored for replay.
func (g *carrierGate) setSuspended(s bool) {
	g.mu.Lock()
	g.suspended = s
	g.mu.Unlock()
	if !s {
		g.notifyResume()
	}
}

// swapTarget repoints the carrier pipe (rebind/adopt).
func (g *carrierGate) swapTarget(t buf.Writer) {
	g.mu.Lock()
	g.target = t
	g.mu.Unlock()
}

// ack frees retained frames up to n (cumulative) and unblocks writers.
func (g *carrierGate) ack(n uint64) {
	g.mu.Lock()
	for len(g.frames) > 0 && g.frames[0].seq <= n {
		g.storedByte -= int64(len(g.frames[0].raw))
		g.frames[0].raw = nil
		g.frames = g.frames[1:]
		g.baseSeq++
	}
	g.mu.Unlock()
	g.ackRecvMu.Lock()
	g.lastAckTime = time.Now()
	g.ackRecvMu.Unlock()
	g.notifyAck()
}

// lastAckRecv returns when the last Ack was processed.
func (g *carrierGate) lastAckRecv() time.Time {
	g.ackRecvMu.Lock()
	defer g.ackRecvMu.Unlock()
	return g.lastAckTime
}

// WriteMultiBuffer stores counted frames (private copies) and forwards.
// Carrier errors never surface while suspend is possible; they are absorbed
// here and the bytes are replayed on rebind.
func (g *carrierGate) WriteMultiBuffer(mb buf.MultiBuffer) error {
	if !g.policy.Enabled {
		return g.target.WriteMultiBuffer(mb)
	}
	if len(mb) == 0 {
		return nil
	}
	seq, counted, err := g.store(mb)
	if err != nil {
		return err
	}
	if !counted {
		return g.forwardUncounted(mb)
	}
	for {
		g.mu.Lock()
		if g.isDoneLocked() {
			g.mu.Unlock()
			return io.ErrClosedPipe
		}
		if g.suspended {
			g.mu.Unlock()
			if err := g.waitResume(); err != nil {
				return err
			}
			continue
		}
		target := g.target
		g.mu.Unlock()
		raw, ok := g.rawCopyOf(seq)
		if !ok {
			// Acknowledged meanwhile: peer has it.
			return nil
		}
		if err := target.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(raw)}); err != nil {
			// Stored already; suspend will replay it. Spin until the
			// detector parks us or the worker closes.
			g.requestSuspend()
			continue
		}
		return nil
	}
}

// onCarrierError is set by the worker: park forwarding and interrupt the
// current pipes so readers unblock. Idempotent.
func (g *carrierGate) setOnCarrierError(f func()) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.onCarrierError = f
}

// store parses mb, cap-blocks, assigns seq and retains a private copy,
// taking ownership of mb. Uncounted frames are left untouched for the
// caller to forward directly.
func (g *carrierGate) store(mb buf.MultiBuffer) (uint64, bool, error) {
	first := mb[0].Bytes()
	status, opt, metaLen, netByte, hasNet, ok := parseMetaPrefix(first)
	if !ok {
		buf.ReleaseMulti(mb)
		return 0, false, nil
	}
	sid := uint16(0)
	if len(first) >= 4 {
		sid = uint16(first[2])<<8 | uint16(first[3])
	}
	if !countedFrame(status, opt, metaLen, netByte, hasNet) {
		return 0, false, nil
	}
	need := int64(0)
	for _, b := range mb {
		need += int64(len(b.Bytes()))
	}
	g.mu.Lock()
	for g.storedByte+need > g.policy.MaxWorkerBuffer {
		if g.isDoneLocked() {
			g.mu.Unlock()
			buf.ReleaseMulti(mb)
			return 0, false, io.ErrClosedPipe
		}
		g.mu.Unlock()
		select {
		case <-g.ackCh:
		case <-g.done:
			buf.ReleaseMulti(mb)
			return 0, false, io.ErrClosedPipe
		}
		g.mu.Lock()
	}
	raw := make([]byte, 0, need)
	for _, b := range mb {
		raw = append(raw, b.Bytes()...)
	}
	buf.ReleaseMulti(mb)
	g.tx.mu.Lock()
	g.tx.n++
	seq := g.tx.n
	g.tx.mu.Unlock()
	g.frames = append(g.frames, storedFrame{seq: seq, sid: sid, raw: raw})
	g.storedByte += int64(len(raw))
	g.mu.Unlock()
	return seq, true, nil
}

// rawCopyOf returns a private copy of the retained frame, if still held.
func (g *carrierGate) rawCopyOf(seq uint64) ([]byte, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for _, f := range g.frames {
		if f.seq == seq {
			cp := make([]byte, len(f.raw))
			copy(cp, f.raw)
			return cp, true
		}
	}
	return nil, false
}

func (g *carrierGate) requestSuspend() {
	g.mu.Lock()
	f := g.onCarrierError
	g.mu.Unlock()
	if f != nil {
		f()
	}
}

func (g *carrierGate) isDoneLocked() bool {
	select {
	case <-g.done:
		return true
	default:
		return false
	}
}

func (g *carrierGate) waitResume() error {
	for {
		g.mu.Lock()
		if g.isDoneLocked() {
			g.mu.Unlock()
			return io.ErrClosedPipe
		}
		if !g.suspended {
			g.mu.Unlock()
			return nil
		}
		g.mu.Unlock()
		select {
		case <-g.resumeCh:
		case <-g.done:
			return io.ErrClosedPipe
		case <-time.After(time.Second):
		}
	}
}

// forwardUncounted passes Resume/Ack/KeepAlive through live carriers and
// coalesces Acks while suspended (cumulative: only the latest matters).
func (g *carrierGate) forwardUncounted(mb buf.MultiBuffer) error {
	g.mu.Lock()
	if g.isDoneLocked() {
		g.mu.Unlock()
		buf.ReleaseMulti(mb)
		return io.ErrClosedPipe
	}
	if g.suspended {
		// Coalesce: keep cumulative Ack value, drop the rest.
		g.mu.Unlock()
		buf.ReleaseMulti(mb)
		return nil
	}
	target := g.target
	g.mu.Unlock()
	return target.WriteMultiBuffer(mb)
}

// writeAckFrame emits Ack{rx} live or records it for the rebind flush.
func (g *carrierGate) writeAck(rx uint64) {
	g.mu.Lock()
	if g.isDoneLocked() {
		g.mu.Unlock()
		return
	}
	if g.suspended {
		if !g.hasPending || rx > g.pendingAck {
			g.pendingAck = rx
			g.hasPending = true
		}
		g.mu.Unlock()
		return
	}
	target := g.target
	g.mu.Unlock()
	meta := FrameMetadata{SessionStatus: SessionStatusAck}
	meta.Option.Set(OptionData)
	payload := encodeAck(AckPayload{RxCount: rx})
	_ = writeMetaWithFrame(target, meta, buf.MultiBuffer{payload})
}

// flushSince writes pending Ack then retained frames with seq > peerRx.
func (g *carrierGate) flushSince(peerRx uint64) error {
	g.mu.Lock()
	var ackToSend uint64
	hasAck := g.hasPending
	if hasAck {
		ackToSend = g.pendingAck
		g.hasPending = false
	}
	target := g.target
	var frames [][]byte
	for _, f := range g.frames {
		if f.seq > peerRx {
			cp := make([]byte, len(f.raw))
			copy(cp, f.raw)
			frames = append(frames, cp)
		}
	}
	g.mu.Unlock()
	if hasAck {
		meta := FrameMetadata{SessionStatus: SessionStatusAck}
		meta.Option.Set(OptionData)
		payload := encodeAck(AckPayload{RxCount: ackToSend})
		if err := writeMetaWithFrame(target, meta, buf.MultiBuffer{payload}); err != nil {
			return err
		}
	}
	for _, raw := range frames {
		if err := target.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(raw)}); err != nil {
			return err
		}
	}
	return nil
}
