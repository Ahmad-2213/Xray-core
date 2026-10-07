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
