package mux

// Resume lifecycle: suspend instead of close, redial on the worker's own
// dial path, TTL-cached v1 fallback. Flag-off compiles to the v1 path.

import (
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
)

var (
	noV2Mu    sync.RWMutex
	noV2Until = make(map[string]time.Time)
)

func isV2Banned(host string) bool {
	noV2Mu.RLock()
	defer noV2Mu.RUnlock()
	if until, ok := noV2Until[host]; ok {
		return time.Now().Before(until)
	}
	return false
}

func banV2(host string, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	noV2Mu.Lock()
	defer noV2Mu.Unlock()
	noV2Until[host] = time.Now().Add(ttl)
}

// onCarrierClosed runs when the carrier Process returns. v1 path: close
// immediately (current behavior). v2/resume path: suspend briefly so the
// redialed carrier can reattach with Resume{token, epoch+1, rxCount}; the
// app-facing pipes are NOT interrupted during suspension (backpressure).
func (m *ClientWorker) onCarrierClosed(useV2 bool) {
	if m == nil {
		return
	}
	if !useV2 || !m.resume.Enabled {
		common.Must(m.done.Close())
		return
	}
	// Phase 1: park for fast redial. Full replay (retain + tombstones +
	// symmetric server handover in resume_handover.go) reattaches here.
	// Until the reattach lands, sessions stall instead of seeing EOF.
	// If the window lapses, fall back to the v1 close path.
	suspend := m.resume.SuspendTimeout
	if suspend <= 0 {
		suspend = 10 * time.Second
	}
	select {
	case <-m.done.Wait():
		return
	case <-time.After(suspend):
		common.Must(m.done.Close())
	}
}
