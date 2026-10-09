package mux

// Mux session resume (private fork): carrier-independent logical session.
//
// Design (see issue XTLS/Xray-core#7097, closed upstream as not planned):
//   - The carrier (WS/XHTTP/gRPC/...) is an ordered reliable byte stream.
//     Both sides maintain implicit cumulative frame counters; only Ack and
//     Resume carry numbers on the wire. v1 frame metadata bytes are untouched.
//   - Counted statuses: New, Keep, End. Not counted: KeepAlive, Resume, Ack.
//   - On carrier loss the worker suspends (no FIN upstream: caller simply
//     stops calling Close/Interrupt on session pipes) and redials through the
//     worker's own dialer. Bounded retain + replay, tombstones for resets.
//   - Disabled by default (zero-cost flag-off). Opt-in per outbound tag via
//     infra/conf "muxResume" key. Version negotiation is in-band on
//     v1.mux.cool (RequestCommandMux on every protocol): the client
//     announces Resume first, old peers fail the unknown status fast with
//     zero dials, and the server sniffs it. No second magic hostname
//     (which would go out as plain TCP, bypassing Vision, and leak the
//     announce to whoever answers that name).
//
// UDP payloads are never retained (slots only, replayed as tombstones).
//
// Lock order (must stay consistent): rsMu → hsMu → SessionManager locks →
// carrierGate mu / pipeMu. Nothing acquires them in reverse: gate and pipe
// code never take rsMu/hsMu/manager locks; Session.Close takes only its
// parent manager lock plus its own pmu; SessionManager methods are leaves.

import (
	"crypto/rand"
	"sync"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
)

// Session status extensions for resume. v1 statuses 0x01-0x04 are untouched.
const (
	SessionStatusResume SessionStatus = 0x05
	SessionStatusAck    SessionStatus = 0x06
)

// DiscardTombstone marks a reset stream's retained sequence slots whose
// payload was dropped. The slot count is preserved so cumulative counters
// stay aligned; the peer replays a discard instead of data.
const DiscardTombstone = true

// ResumePolicy configures opt-in session resume. Zero value = disabled.
type ResumePolicy struct {
	Enabled         bool
	SuspendTimeout  time.Duration
	MaxStreamBuffer int64
	MaxWorkerBuffer int64
	AckEveryMs      int64
	RedialDelays    []time.Duration
	NoV2CacheTTL    time.Duration
	AckTimeout      time.Duration
}

// DefaultResumePolicy returns the reviewed defaults: short hold, fast redial.
// 10s suspend covers typical WS/TLS redial (<2s with session resumption +
// testpre pool) plus replay; longer holds only pin FDs/memory.
func DefaultResumePolicy() ResumePolicy {
	return ResumePolicy{
		Enabled:         true,
		SuspendTimeout:  10 * time.Second,
		MaxStreamBuffer: 256 * 1024,
		MaxWorkerBuffer: 4 * 1024 * 1024,
		AckEveryMs:      500,
		RedialDelays:    []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second},
		NoV2CacheTTL:    10 * time.Minute,
		AckTimeout:      4 * time.Second,
	}
}

// DisabledPolicy is the zero-cost default: current fail-fast behavior.
func DisabledPolicy() ResumePolicy {
	return ResumePolicy{Enabled: false}
}

var (
	resumeRegistryMu sync.RWMutex
	resumeByTag      = make(map[string]ResumePolicy)
)

// RegisterResumePolicy registers an opt-in policy for an outbound tag.
// Called from infra/conf when "muxResume" is present. Tags must be
// explicit: an empty tag is ignored (never a global default), so one
// untagged outbound can't silently enable resume for every mux outbound
// in the process. Outbounds added outside conf (e.g. gRPC API) stay
// disabled — fail-closed.
func RegisterResumePolicy(tag string, p ResumePolicy) {
	if tag == "" {
		return
	}
	resumeRegistryMu.Lock()
	defer resumeRegistryMu.Unlock()
	resumeByTag[tag] = p
}

// UnregisterResumePolicy drops the policy for an outbound tag (e.g. on
// outbound removal through the API), so a later outbound reusing the tag
// can't inherit a stale policy.
func UnregisterResumePolicy(tag string) {
	if tag == "" {
		return
	}
	resumeRegistryMu.Lock()
	defer resumeRegistryMu.Unlock()
	delete(resumeByTag, tag)
}

// LookupResumePolicy returns the policy for an outbound tag, or disabled
// when the tag was never registered.
func LookupResumePolicy(tag string) ResumePolicy {
	resumeRegistryMu.RLock()
	defer resumeRegistryMu.RUnlock()
	if p, ok := resumeByTag[tag]; ok {
		return p
	}
	return DisabledPolicy()
}

// tokenString renders a short non-secret prefix for logs (never the full
// token: it authorizes resume).
func tokenString(t [16]byte) string {
	const hex = "0123456789abcdef"
	var b [8]byte
	for i := 0; i < 4; i++ {
		b[i*2] = hex[t[i]>>4]
		b[i*2+1] = hex[t[i]&0x0f]
	}
	return string(b[:])
}

func (m *ClientWorker) tokenString() string { return tokenString(m.token) }

func (w *ServerWorker) tokenString() string { return tokenString(w.resumeToken) }

// validateRebind guards adoption of a parked worker: epochs must advance
// (replays/duplicates rejected), the peer may not claim more than we sent,
// and the rebind must carry the same user the park was bound to.
func validateRebind(entryTx, entryEpoch uint64, entryUser string, rp ResumePayload, user string) error {
	if entryUser != "" && user != "" && entryUser != user {
		return errors.New("resume user mismatch")
	}
	if rp.Epoch <= entryEpoch {
		return errors.New("stale resume epoch")
	}
	if rp.RxCount > entryTx {
		return errors.New("resume count beyond sent")
	}
	return nil
}

// halfOpenTripped fires the unilateral half-open detector: bytes retained
// (unacked) while no Ack arrived for longer than the timeout means the read
// side is stalled even though writes succeed. All three must hold: fresh
// acks veto the trip (slow bulk with flowing acks is healthy), and young
// unacked data vetoes it (a send after silence hasn't had time to be acked).
func halfOpenTripped(unacked int64, idle, age, timeout time.Duration) bool {
	return unacked > 0 && idle > timeout && age > timeout
}

// NewToken generates an unguessable 128-bit resume token. It must be bound
// by the caller to the authenticated (VLESS) user session: the server only
// honors Resume when the new carrier carries the same user credentials.
func NewToken() ([16]byte, error) {
	var t [16]byte
	_, err := rand.Read(t[:])
	return t, err
}

// CountedStatus reports whether a frame advances the cumulative counters.
func CountedStatus(s SessionStatus) bool {
	switch s {
	case SessionStatusNew, SessionStatusKeep, SessionStatusEnd:
		return true
	default:
		return false
	}
}

// rxState is the receive-side cumulative frame counter with a suspend
// seal, a reader generation and the content tip. admit must be called
// with a FULLY READ frame before delivery: while unsealed and
// same-generation it counts (chaining the tip) and returns true;
// once sealed, or for a frame from an older reader generation, it
// returns false and the caller drops the frame uncounted — the peer
// will replay it after rebind. The generation closes the stale-reader
// hole: a reader blocked mid-delivery keeps parsing its already-buffered
// frames after the swap unseals for the new carrier, and without the
// generation those frames would count, deliver, and then arrive again
// via replay (duplication). Counting before delivery is safe because
// the whole frame is already in memory; delivery can then only fail via
// session/worker close, which needs no replay.
type rxState struct {
	mu     sync.Mutex
	n      uint64
	sealed bool
	gen    uint64
	tip    uint64
}

func (r *rxState) admit(gen uint64, counted bool, sid uint16, mb buf.MultiBuffer) (uint64, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.sealed || gen != r.gen {
		if gen != r.gen {
			traceAdmitGenMismatch(gen, r.gen, r.sealed)
		}
		return r.n, false
	}
	if counted {
		r.n++
		ln, crc := fpDigest(mb)
		r.tip = chainFP(r.tip, r.n, sid, ln, crc)
	}
	return r.n, true
}

// seal stops further counting; sealAndSnapshot seals and returns the
// exact delivered set the peer must replay past. Idempotent.
func (r *rxState) seal() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = true
}

func (r *rxState) sealAndSnapshot() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = true
	return r.n
}

// tipSnapshot returns the count and content tip together, atomically,
// without sealing: for acks, which must never mix a count with a tip
// that has already moved past it.
func (r *rxState) tipSnapshot() (uint64, uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n, r.tip
}

// resume adopts another worker's baseline (count, tip and generation
// handling stay with the caller) and reopens counting.
func (r *rxState) resume(n uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n = n
	r.sealed = false
}

// restore adopts a full handover baseline: count and content tip from
// the parked entry, reopened for the new generation.
func (r *rxState) restore(n, tip uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.n = n
	r.tip = tip
	r.sealed = false
}

// newGeneration retires the old reader: bumps the generation and
// unseals atomically, so frames the stale reader already buffered can
// never admit again.
func (r *rxState) newGeneration() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.gen++
	r.sealed = false
	return r.gen
}

// unseal reopens counting at the current value. Must hold no other
// assumption: unlike resume(Value()), it cannot lose a concurrent admit.
func (r *rxState) unseal() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sealed = false
}

func (r *rxState) Value() uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// Counter tracks one direction of the cumulative frame count.
type Counter struct {
	mu sync.Mutex
	n  uint64
}

func (c *Counter) Next() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n++
	return c.n
}

func (c *Counter) Value() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

// Set overwrites the counter, used when adopting another worker's baseline.
func (c *Counter) Set(v uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n = v
}
