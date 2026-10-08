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
