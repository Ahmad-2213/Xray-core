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
	"context"
	"hash/crc32"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common/bitmask"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
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

type storedFrame struct {
	seq      uint64
	sid      uint16
	raw      []byte
	storedAt time.Time
	// h chains this frame into the cumulative content hash (see
	// chainFP): h == chain(tip-before, seq, sid, payload). Freed with
	// the frame; the base checkpoint below covers older seqs.
	h uint64
}

type carrierGate struct {
	mu        sync.Mutex
	suspended bool
	done      <-chan struct{}
	target    buf.Writer
	policy    ResumePolicy

	// sendMu serializes store+forward so wire order always matches seq
	// order. Without it two session writers could put frames on the wire
	// in a different order than their seqs, and the peer (which counts by
	// arrival) would permanently diverge after a cut. Ack accounting and
	// flush never take sendMu, so they can't deadlock a blocked sender.
	sendMu sync.Mutex

	tx             Counter
	frames         []storedFrame
	baseSeq        uint64
	storedByte     int64
	streamBytes    map[uint16]int64
	// lastFlushMax/lastFlushEpoch record the newest completed flush:
	// live writers skip only seqs it covered on the still-current
	// target. Stale-epoch marks never suppress (their carrier may have
	// died with the bytes); the superseding flush re-covers via peerRx.
	lastFlushMax   uint64
	lastFlushEpoch uint64
	// hashTip/hashTipSeq chain the latest retained frame; hashBase/
	// hashBaseSeq checkpoint the newest freed one. Together they verify
	// a peer's (RxCount, Hash) tip against sent history: exact hit on a
	// retained frame or the base, fail-closed drift close on mismatch.
	hashTip     uint64
	hashTipSeq  uint64
	hashBase    uint64
	hashBaseSeq uint64
	onCarrierError func()

	// flushMu serializes rebind flushes: overlapping adopts on one gate
	// would double-forward the same suffix (frame-aligned duplication
	// under churn). Live writers hold the same mutex across their
	// coverage check and write, so check and write are atomic against a
	// flush in either order.
	flushMu sync.Mutex
	// targetEpoch bumps on every swapTarget. A flush aborts when its
	// epoch goes stale so a still-blocked previous-episode goroutine
	// can't write into the new target while the next flush runs.
	targetEpoch atomic.Uint64
	// activeFlush counts flushes currently executing (0 or 1 by the
	// mutex; the WARN is a canary for future regressions).
	activeFlush atomic.Int64
	// flushID numbers flushes for trace correlation.
	flushID atomic.Uint64
	// label is the worker's short token for trace lines, set by the
	// worker once known (gates outlive any single worker).
	label atomic.Value // string
	// dir marks the payload direction this gate carries ("up" for the
	// client gate, "down" for the server gate), set once before traffic.
	// Diagnostic only (muxtrace attribution).
	dir string

	ackCh      chan struct{}
	resumeCh   chan struct{}
	pendingAck  uint64
	pendingHash uint64
	hasPending  bool

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
		streamBytes: make(map[uint16]int64),
		ackCh:       make(chan struct{}),
		resumeCh:    make(chan struct{}),
		lastAckTime: time.Now(),
	}
}

// notifyAck wakes cap-blocked senders. Broadcast (close-and-replace) so
// every waiter observes it; a capacity-1 send could be consumed by one
// waiter while others sleep past their window.
func (g *carrierGate) notifyAck() {
	g.mu.Lock()
	close(g.ackCh)
	g.ackCh = make(chan struct{})
	g.mu.Unlock()
}

// notifyResume wakes writers parked in waitResume. Broadcast for the same
// reason as notifyAck.
func (g *carrierGate) notifyResume() {
	g.mu.Lock()
	close(g.resumeCh)
	g.resumeCh = make(chan struct{})
	g.mu.Unlock()
}

// swapDone repoints the lifetime channel (rebind adopts the gate onto a
// token-scoped done). Callers must ensure no waiter selects on a stale
// channel: all wait loops below re-capture under mu every iteration.
func (g *carrierGate) swapDone(done <-chan struct{}) {
	g.mu.Lock()
	g.done = done
	g.mu.Unlock()
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

// swapTarget repoints the carrier pipe (rebind/adopt) and retires any
// flush still aimed at the old target: it aborts on the epoch check.
func (g *carrierGate) swapTarget(t buf.Writer) {
	g.mu.Lock()
	g.target = t
	g.mu.Unlock()
	g.targetEpoch.Add(1)
}

// gateLabel returns the worker token label for trace lines.
func (g *carrierGate) gateLabel() string {
	if v := g.label.Load(); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return "?"
}

// SetLabel tags the gate with the owning worker's short token.
func (g *carrierGate) SetLabel(s string) {
	g.label.Store(s)
}

// ack frees retained frames up to n (cumulative) and unblocks writers.
func (g *carrierGate) ack(n uint64) {
	g.mu.Lock()
	for len(g.frames) > 0 && g.frames[0].seq <= n {
		g.hashBase, g.hashBaseSeq = g.frames[0].h, g.frames[0].seq
		g.storedByte -= int64(len(g.frames[0].raw))
		g.streamBytes[g.frames[0].sid] -= int64(len(g.frames[0].raw))
		if g.streamBytes[g.frames[0].sid] <= 0 {
			delete(g.streamBytes, g.frames[0].sid)
		}
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

// verifyRxHash checks a peer's (rx, hash) tip against sent history: an
// exact hit on a retained frame or the base checkpoint. ok=false means
// outside the window (check skipped, fail open); drift=true on mismatch
// (fail closed: the streams have diverged). local returns our hash at rx
// (or 0 when outside the window) for the drift log.
func (g *carrierGate) verifyRxHash(rx, h uint64) (ok bool, drift bool, local uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if rx == g.hashBaseSeq {
		return true, h != g.hashBase, g.hashBase
	}
	for _, f := range g.frames {
		if f.seq == rx {
			return true, f.h != h, f.h
		}
		if f.seq > rx {
			break
		}
	}
	return false, false, 0
}

// unackedAge returns how long the oldest retained frame has waited for
// its ack, or 0 when nothing is retained.
func (g *carrierGate) unackedAge() time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.frames) == 0 {
		return 0
	}
	return time.Since(g.frames[0].storedAt)
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
	return g.writeCounted(mb)
}

// parse extracts frame identity without locking or taking ownership.
func (g *carrierGate) parse(mb buf.MultiBuffer) (SessionStatus, bitmask.Byte, int, byte, bool, bool) {
	return parseMetaPrefix(mb[0].Bytes())
}

// writeCounted runs the ordered sender: seq assignment and wire order must
// match, otherwise the peer (which counts by arrival) diverges after a cut
// and the replay math silently drops frames.
func (g *carrierGate) writeCounted(mb buf.MultiBuffer) error {
	status, opt, metaLen, netByte, hasNet, ok := g.parse(mb)
	if !ok {
		return g.forwardUncounted(mb)
	}
	sid := uint16(0)
	if first := mb[0].Bytes(); len(first) >= 4 {
		sid = uint16(first[2])<<8 | uint16(first[3])
	}
	if !countedFrame(status, opt, metaLen, netByte, hasNet) {
		return g.forwardUncounted(mb)
	}
	var need int64
	for _, b := range mb {
		need += int64(len(b.Bytes()))
	}
	// Reserve credit before taking sendMu: a cap-blocked bulk stream must
	// not stall other sessions' senders.
	for {
		if err := g.reserve(need, sid); err != nil {
			buf.ReleaseMulti(mb)
			return err
		}
		g.sendMu.Lock()
		seq, retry, err := g.retain(mb, need, sid)
		if err != nil {
			g.sendMu.Unlock()
			return err
		}
		if !retry {
			defer g.sendMu.Unlock()
			return g.forwardRetained(seq)
		}
		// Lost a race after reserve (mb untouched): release sendMu and
		// re-reserve. Caps only loosen via ack, which never takes
		// sendMu, so this terminates.
		g.sendMu.Unlock()
	}
}

// forwardRetained forwards one retained frame, riding out suspension and
// carrier errors. Caller holds sendMu, released on return.
func (g *carrierGate) forwardRetained(seq uint64) error {
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
		g.mu.Unlock()
		// Serialize the coverage check and the write against a
		// concurrent rebind flush: otherwise both can cover the same
		// seq (frame-aligned duplication under churn). The flush takes
		// the same mutex, so check and write are atomic against it.
		// No deadlock: flushes never take sendMu (held by our caller),
		// and waits happen outside both mutexes.
		g.flushMu.Lock()
		g.mu.Lock()
		if g.isDoneLocked() {
			g.mu.Unlock()
			g.flushMu.Unlock()
			return io.ErrClosedPipe
		}
		if g.suspended {
			g.mu.Unlock()
			g.flushMu.Unlock()
			if err := g.waitResume(); err != nil {
				return err
			}
			continue
		}
		// Skip only if the current flush already covered this seq on
		// the current target epoch: a mark from an older epoch is
		// stale (its carrier may have died with the bytes), and the
		// superseding flush re-covers the tail via peerRx truth.
		// Inline reads: g.mu is held, and helpers would self-deadlock.
		if seq <= g.lastFlushMax && g.lastFlushEpoch == g.targetEpoch.Load() {
			g.mu.Unlock()
			g.flushMu.Unlock()
			return nil
		}
		target := g.target
		epoch := g.targetEpoch.Load()
		g.mu.Unlock()
		raw, ok := g.rawCopyOf(seq)
		if !ok {
			// Acknowledged meanwhile: peer has it.
			g.flushMu.Unlock()
			return nil
		}
		if err := target.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(raw)}); err != nil {
			// Stored already; suspend will replay it. Spin until the
			// detector parks us or the worker closes.
			g.flushMu.Unlock()
			g.requestSuspend()
			continue
		}
		traceWriteCount(epoch, seq)
		// Deliberately no coverage mark here: only the peer's rx decides
		// replay ranges (see flushSince), and marks conflate "written"
		// with "received". A live-sent seq stays replayable until acked.
		g.flushMu.Unlock()
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

// reserve blocks until need bytes fit the worker and per-stream caps.
// Runs without sendMu so a cap-blocked bulk stream never stalls other
// sessions' senders; retain re-verifies under sendMu afterwards.
func (g *carrierGate) reserve(need int64, sid uint16) error {
	g.mu.Lock()
	for g.storedByte+need > g.policy.MaxWorkerBuffer || g.streamBytes[sid]+need > g.policy.MaxStreamBuffer {
		if g.isDoneLocked() {
			g.mu.Unlock()
			return io.ErrClosedPipe
		}
		ackCh := g.ackCh
		done := g.done
		g.mu.Unlock()
		select {
		case <-ackCh:
		case <-done:
			return io.ErrClosedPipe
		}
		g.mu.Lock()
	}
	g.mu.Unlock()
	return nil
}

// retain stores a private copy and assigns its seq, consuming mb.
// Caller holds sendMu. Caps were reserved beforehand and are re-verified
// here; on a lost race it returns retry=true with mb untouched.
func (g *carrierGate) retain(mb buf.MultiBuffer, need int64, sid uint16) (seq uint64, retry bool, err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.isDoneLocked() {
		buf.ReleaseMulti(mb)
		return 0, false, io.ErrClosedPipe
	}
	if g.storedByte+need > g.policy.MaxWorkerBuffer || g.streamBytes[sid]+need > g.policy.MaxStreamBuffer {
		return 0, true, nil
	}
	raw := make([]byte, 0, need)
	// Digest the payload (every buffer after mb[0]) BEFORE release:
	// mb[0] is metadata plus the u16 length prefix, which the peer's
	// chunk reader strips, so chaining it would diverge. Reading after
	// ReleaseMulti is use-after-recycle (nil deref or silent garbage).
	var ln int
	hh := crc32.NewIEEE()
	for i, b := range mb {
		by := b.Bytes()
		raw = append(raw, by...)
		if i > 0 {
			ln += len(by)
			hh.Write(by)
		}
	}
	buf.ReleaseMulti(mb)
	g.tx.mu.Lock()
	g.tx.n++
	seq = g.tx.n
	g.tx.mu.Unlock()
	h := chainFP(g.hashTip, seq, sid, ln, hh.Sum32())
	g.hashTip, g.hashTipSeq = h, seq
	g.frames = append(g.frames, storedFrame{seq: seq, sid: sid, raw: raw, storedAt: time.Now(), h: h})
	g.storedByte += int64(len(raw))
	g.streamBytes[sid] += int64(len(raw))
	traceRetain(g, seq, sid, ln, hh.Sum32())
	return seq, false, nil
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
		resumeCh := g.resumeCh
		done := g.done
		g.mu.Unlock()
		select {
		case <-resumeCh:
		case <-done:
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

// writeAckFrame emits Ack{rx, hash} live or records it for the rebind
// flush. Count and tip travel together (same instant) so the sender
// never verifies a count against a tip that has moved past it.
func (g *carrierGate) writeAck(rx, h uint64) {
	g.mu.Lock()
	if g.isDoneLocked() {
		g.mu.Unlock()
		return
	}
	if g.suspended {
		if !g.hasPending || rx > g.pendingAck {
			g.pendingAck = rx
			g.pendingHash = h
			g.hasPending = true
		}
		g.mu.Unlock()
		return
	}
	target := g.target
	g.mu.Unlock()
	meta := FrameMetadata{SessionStatus: SessionStatusAck}
	meta.Option.Set(OptionData)
	payload := encodeAck(AckPayload{RxCount: rx, Hash: h, HasHash: true})
	_ = writeMetaWithFrame(target, meta, buf.MultiBuffer{payload})
}

// flushSince writes pending Ack then retained frames with seq > peerRx.
// Covers exactly the unacked suffix; records the high-water mark so live
// writers waking from suspend don't re-forward what was just replayed.
// Serialized: overlapping adopts on one gate must not double-forward.
// Aborts (partial mark) when the target epoch moves underneath it, so a
// still-blocked previous-episode flush can't write into the new target
// while the next flush runs; the superseding flush re-covers the tail.
func (g *carrierGate) flushSince(peerRx uint64) error {
	g.flushMu.Lock()
	defer g.flushMu.Unlock()
	if n := g.activeFlush.Add(1); n > 1 {
		errors.LogWarning(context.Background(), "mux resume: concurrent flush execution detected (canary)")
	}
	defer g.activeFlush.Add(-1)
	id := g.flushID.Add(1)
	epoch := g.targetEpoch.Load()
	g.mu.Lock()
	var ackToSend, ackHashToSend uint64
	hasAck := g.hasPending
	if hasAck {
		ackToSend = g.pendingAck
		ackHashToSend = g.pendingHash
		g.hasPending = false
	}
	target := g.target
	low := peerRx
	var frames [][]byte
	var fseqs []uint64
	maxSent := low
	for _, f := range g.frames {
		if f.seq > low {
			cp := make([]byte, len(f.raw))
			copy(cp, f.raw)
			frames = append(frames, cp)
			fseqs = append(fseqs, f.seq)
			if f.seq > maxSent {
				maxSent = f.seq
			}
		}
	}
	g.mu.Unlock()
	errors.LogInfo(context.Background(), "mux resume: flush#", id, " ", g.gateLabel(), " peerRx ", peerRx, " fromSeq ", low+1, " toSeq ", maxSent, " frames ", len(frames))
	traceLogFlush(id, g.gateLabel(), fseqs)
	if hasAck {
		meta := FrameMetadata{SessionStatus: SessionStatusAck}
		meta.Option.Set(OptionData)
		payload := encodeAck(AckPayload{RxCount: ackToSend, Hash: ackHashToSend, HasHash: true})
		if err := writeMetaWithFrame(target, meta, buf.MultiBuffer{payload}); err != nil {
			return err
		}
	}
	for i, raw := range frames {
		if g.targetEpoch.Load() != epoch {
			break
		}
		if err := target.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(raw)}); err != nil {
			return err
		}
		traceWriteCount(epoch, fseqs[i])
	}
	g.mu.Lock()
	g.lastFlushMax, g.lastFlushEpoch = maxSent, epoch
	g.mu.Unlock()
	return nil
}
