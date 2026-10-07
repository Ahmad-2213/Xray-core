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
	done    *tokenDone
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
)

func hsPut(token [16]byte, e *suspendedWorker) {
	hsMu.Lock()
	defer hsMu.Unlock()
	if len(hsEntries) >= hsMaxEntries {
		now := time.Now()
		for k, v := range hsEntries {
			if now.After(v.expires) {
				closeEntry(v)
				delete(hsEntries, k)
			}
		}
		if len(hsEntries) >= hsMaxEntries {
			// Still full: evict the oldest (closest to expiry) rather
			// than leaking the parked sessions. Newest entries are
			// the most likely to rebind.
			var oldest [16]byte
			var oldestExp time.Time
			first := true
			for k, v := range hsEntries {
				if first || v.expires.Before(oldestExp) {
					oldest, oldestExp, first = k, v.expires, false
				}
			}
			if !first {
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
	e.expires = time.Now().Add(hsTTL)
	hsEntries[token] = e
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
		e.expires = time.Now().Add(hsTTL)
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

func init() {
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for range t.C {
			sweepExpired()
		}
	}()
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

// Test hooks (package mux_test). Tokens are the only shared key; entries
// stay unexported. Reset first: the table is process-global.
func HsResetForTest() {
	hsMu.Lock()
	defer hsMu.Unlock()
	hsEntries = make(map[[16]byte]*suspendedWorker)
}

func HsLenForTest() int {
	hsMu.Lock()
	defer hsMu.Unlock()
	return len(hsEntries)
}

func HsPutForTest(token [16]byte, tx, rx, epoch uint64, user string) {
	hsPut(token, &suspendedWorker{tx: tx, rx: rx, epoch: epoch, user: user})
}

func HsPeekForTest(token [16]byte) (tx, rx, epoch uint64, user string, ok bool) {
	e, ok := hsPeek(token)
	if !ok {
		return 0, 0, 0, "", false
	}
	return e.tx, e.rx, e.epoch, e.user, true
}

func HsTakeForTest(token [16]byte) (tx, rx, epoch uint64, user string, ok bool) {
	e, ok := hsTake(token)
	if !ok {
		return 0, 0, 0, "", false
	}
	return e.tx, e.rx, e.epoch, e.user, true
}

func HsPutBackForTest(token [16]byte, tx, rx, epoch uint64, user string) {
	hsPutBack(token, &suspendedWorker{tx: tx, rx: rx, epoch: epoch, user: user})
}
