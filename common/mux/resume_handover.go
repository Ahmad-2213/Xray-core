package mux

// Server-side suspend handover for mux resume. Triggered solely by the
// client's Resume request; there is no server flag. Entries are capped by
// count and a short expiry so a dead client cannot pin memory.

import (
	"sync"
	"time"
)

type suspendedWorker struct {
	manager *SessionManager
	gate    *carrierGate
	tx      uint64
	rx      uint64
	epoch   uint64
	user    string
	expires time.Time
	born    time.Time
	done    *tokenDone
	// quiesced closes when the parking worker's run loop exits. Nil
	// for test-built entries; adopters wait on it with a timeout.
	quiesced <-chan struct{}
}

// tokenDone is the token-scoped lifetime channel. The gate adopted onto a
// rebind references this, never a single worker's done: the parking worker
// may be reaped by its monitor while adopted sessions are still live.
// Close-once under mutex; shared by the parking worker, the entry, and
// the adopting worker.
type tokenDone struct {
	mu     sync.Mutex
	ch     chan struct{}
	closed bool
}

func newTokenDone() *tokenDone {
	return &tokenDone{ch: make(chan struct{})}
}

func (t *tokenDone) wait() <-chan struct{} {
	return t.ch
}

func (t *tokenDone) close() {
	if t == nil {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		t.closed = true
		close(t.ch)
	}
}

// closeEntry releases everything a parked entry holds. Tolerates test
// entries with nil manager/done.
func closeEntry(e *suspendedWorker) {
	if e == nil {
		return
	}
	if e.manager != nil {
		e.manager.Close()
	}
	if e.done != nil {
		e.done.close()
	}
}

var (
	hsMu      sync.Mutex
	hsEntries = make(map[[16]byte]*suspendedWorker)
)

const (
	hsMaxEntries = 1024
	hsTTL        = 10 * time.Second
	// hsMaxLifetime caps total park time across validation-failure
	// refreshes: repeated failures must not pin an entry indefinitely.
	hsMaxLifetime = 60 * time.Second
	// hsPerUserQuota bounds one user's parked entries so a single user
	// can't evict everyone else's via the oldest-first shedding below.
	hsPerUserQuota = 256
)

func hsPut(token [16]byte, e *suspendedWorker) {
	hsMu.Lock()
	defer hsMu.Unlock()
	// Per-user quota first: shed this user's oldest so one user can
	// never evict another's parks through the global cap below.
	for {
		var oldest [16]byte
		var oldestExp time.Time
		mine, first := 0, true
		for k, v := range hsEntries {
			if v.user != e.user || k == token {
				continue
			}
			mine++
			if first || v.expires.Before(oldestExp) {
				oldest, oldestExp, first = k, v.expires, false
			}
		}
		if mine < hsPerUserQuota {
			break
		}
		closeEntry(hsEntries[oldest])
		delete(hsEntries, oldest)
	}
	if len(hsEntries) >= hsMaxEntries {
		now := time.Now()
		for k, v := range hsEntries {
			if now.After(v.expires) {
				closeEntry(v)
				delete(hsEntries, k)
			}
		}
		if len(hsEntries) >= hsMaxEntries {
			// Still full: shed from the largest holder rather than the
			// globally oldest, so a few heavy users can't evict
			// everyone else's parks.
			counts := make(map[string]int, 8)
			for _, v := range hsEntries {
				counts[v.user]++
			}
			var top string
			topN, first := 0, true
			for u, n := range counts {
				if first || n > topN {
					top, topN, first = u, n, false
				}
			}
			var oldest [16]byte
			var oldestExp time.Time
			ofirst := true
			for k, v := range hsEntries {
				if v.user != top {
					continue
				}
				if ofirst || v.expires.Before(oldestExp) {
					oldest, oldestExp, ofirst = k, v.expires, false
				}
			}
			if !ofirst {
				closeEntry(hsEntries[oldest])
				delete(hsEntries, oldest)
			}
		}
		if len(hsEntries) >= hsMaxEntries {
			closeEntry(e)
			return
		}
	}
	if old, ok := hsEntries[token]; ok {
		// Same token re-parked (overlapping carriers): the older park
		// can never rebind again, close it instead of leaking.
		closeEntry(old)
	}
	if e.born.IsZero() {
		e.born = time.Now()
	}
	e.expires = time.Now().Add(hsTTL)
	hsEntries[token] = e
	lazyJanitor()
}

func hsTake(token [16]byte) (*suspendedWorker, bool) {
	hsMu.Lock()
	defer hsMu.Unlock()
	e, ok := hsEntries[token]
	if !ok {
		return nil, false
	}
	delete(hsEntries, token)
	if time.Now().After(e.expires) {
		closeEntry(e)
		return nil, false
	}
	return e, true
}

// hsPeek looks up without consuming, so a failed validation keeps the
// entry for a later retry with a greater epoch.
func hsPeek(token [16]byte) (*suspendedWorker, bool) {
	hsMu.Lock()
	defer hsMu.Unlock()
	e, ok := hsEntries[token]
	if !ok || time.Now().After(e.expires) {
		return nil, false
	}
	return e, true
}

// hsAdopt validates and consumes an entry under a single lock, so two
// concurrent rebinds for the same token can't both pass validation. The
// validate closure runs under the table lock and must not touch the table.
// On validation error the entry is put back with refreshed TTL (a later
// retry with a greater epoch can still adopt it). Unknown or expired
// tokens yield (nil, false, nil); expired entries are closed, never leaked.
func hsAdopt(token [16]byte, validate func(*suspendedWorker) error) (*suspendedWorker, bool, error) {
	hsMu.Lock()
	defer hsMu.Unlock()
	e, ok := hsEntries[token]
	if !ok {
		return nil, false, nil
	}
	if time.Now().After(e.expires) {
		closeEntry(e)
		delete(hsEntries, token)
		return nil, false, nil
	}
	if err := validate(e); err != nil {
		// Refresh, but never past the lifetime cap: repeated failures
		// must not pin an entry indefinitely.
		e.expires = time.Now().Add(hsTTL)
		if max := e.born.Add(hsMaxLifetime); !e.born.IsZero() && e.expires.After(max) {
			e.expires = max
		}
		return nil, true, err
	}
	delete(hsEntries, token)
	return e, true, nil
}

// sweepExpired closes and drops expired parks. Runs periodically (janitor)
// so dead clients can't pin sessions, gates, or outbound conns.
func sweepExpired() {
	hsMu.Lock()
	defer hsMu.Unlock()
	now := time.Now()
	for k, v := range hsEntries {
		if now.After(v.expires) {
			closeEntry(v)
			delete(hsEntries, k)
		}
	}
}

var janitorOnce sync.Once

// liveReg tracks live server workers by resume token so a rebind racing
// a park can force-park the stale carrier instead of taking an epoch-0
// negative and waiting a full redial cycle: during a half-open stall the
// client redials while the server still sees the old carrier as live, so
// the first redial finds nothing parked. Force-park moves the sessions
// into the handover table and the same redial adopts them at once.
// Rule: never hold liveRegMu while acquiring a worker rsMu (register and
// unregister take liveRegMu leaf-style; find snapshots the pointer and
// releases before touching the worker).
var (
	liveRegMu sync.Mutex
	liveReg   = make(map[[16]byte]*ServerWorker)
)

func liveRegPut(token [16]byte, w *ServerWorker) {
	liveRegMu.Lock()
	defer liveRegMu.Unlock()
	liveReg[token] = w
}

func liveRegFind(token [16]byte) *ServerWorker {
	liveRegMu.Lock()
	defer liveRegMu.Unlock()
	return liveReg[token]
}

// liveRegRemove unregisters only if the mapping still points at w: a
// newer worker reusing the token must survive an older worker's exit.
func liveRegRemove(token [16]byte, w *ServerWorker) {
	liveRegMu.Lock()
	defer liveRegMu.Unlock()
	if liveReg[token] == w {
		delete(liveReg, token)
	}
}

// lazyJanitor starts the expiry sweeper on first park (no eager init, so
// importing common/mux with resume disabled costs no goroutine). Expiry is
// otherwise lazy (checked on take/peek), which alone could pin dead entries
// when no new traffic arrives.
func lazyJanitor() {
	janitorOnce.Do(func() {
		go func() {
			t := time.NewTicker(5 * time.Second)
			defer t.Stop()
			for range t.C {
				sweepExpired()
			}
		}()
	})
}

// hsPutBack restores an entry after failed validation so a later retry
// with a greater epoch can still adopt it.
func hsPutBack(token [16]byte, e *suspendedWorker) {
	hsMu.Lock()
	defer hsMu.Unlock()
	if len(hsEntries) >= hsMaxEntries {
		closeEntry(e)
		return
	}
	if old, ok := hsEntries[token]; ok {
		closeEntry(old)
	}
	e.expires = time.Now().Add(hsTTL)
	hsEntries[token] = e
}
