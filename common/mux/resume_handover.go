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
				delete(hsEntries, k)
			}
		}
		if len(hsEntries) >= hsMaxEntries {
			return
		}
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

// hsPutBack restores an entry after failed validation so a later retry
// with a greater epoch can still adopt it.
func hsPutBack(token [16]byte, e *suspendedWorker) {
	hsMu.Lock()
	defer hsMu.Unlock()
	if len(hsEntries) >= hsMaxEntries {
		return
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
