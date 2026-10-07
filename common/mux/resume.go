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
//     infra/conf "muxResume" key. Old peers see unknown statuses and fail
//     fast (current behavior); v2 workers are selected via v2.mux.cool magic
//     target with TTL-cached fallback to v1.
//
// UDP payloads are never retained (slots only, replayed as tombstones).

import (
	"crypto/rand"
	"sync"
	"time"
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
	AckEveryBytes   int64
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
		AckEveryBytes:   32 * 1024,
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
	resumeDefault    = DisabledPolicy()
)

// RegisterResumePolicy registers an opt-in policy for an outbound tag.
// Called from infra/conf when "muxResume" is present. Empty tag sets default.
func RegisterResumePolicy(tag string, p ResumePolicy) {
	resumeRegistryMu.Lock()
	defer resumeRegistryMu.Unlock()
	if tag == "" {
		resumeDefault = p
		return
	}
	resumeByTag[tag] = p
}

// LookupResumePolicy returns the policy for an outbound tag, or the default.
func LookupResumePolicy(tag string) ResumePolicy {
	resumeRegistryMu.RLock()
	defer resumeRegistryMu.RUnlock()
	if p, ok := resumeByTag[tag]; ok {
		return p
	}
	return resumeDefault
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
