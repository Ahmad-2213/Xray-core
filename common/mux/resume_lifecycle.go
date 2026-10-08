package mux

// Resume lifecycle: suspend instead of close, rebind on the worker's own
// dial path, TTL-cached v1 fallback. Flag-off compiles to the v1 path.

import (
	"context"
	goerrors "errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/pipe"
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

// v2BanKey scopes the v2 fallback ban to one outbound tag + server. Keying
// by the magic hostname alone would disable resume for every outbound
// after a single server's handshake failure.
func v2BanKey(tag string, target net.Address) string {
	return tag + "\x00" + target.String()
}

func banV2(host string, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	noV2Mu.Lock()
	defer noV2Mu.Unlock()
	noV2Until[host] = time.Now().Add(ttl)
}

var (
	v2FailMu sync.Mutex
	v2Fails  = make(map[string]int)
)

// recordV2Fail counts consecutive instant-death handshakes per ban key.
// A single transient dial failure must not ban v2 (that failure is what
// resume exists for); only a streak proves an old/non-v2 server.
func recordV2Fail(key string) int {
	v2FailMu.Lock()
	defer v2FailMu.Unlock()
	v2Fails[key]++
	return v2Fails[key]
}

func clearV2Fails(key string) {
	v2FailMu.Lock()
	defer v2FailMu.Unlock()
	delete(v2Fails, key)
}

// banTrackingDialer records whether any dial succeeded. Only post-connect
// instant deaths count toward the v2 ban: a dial failure during a flap
// looks identical (tx==0 && rx==0) but is exactly what resume is for.
type banTrackingDialer struct {
	internet.Dialer
	connected atomic.Bool
}

func (d *banTrackingDialer) Dial(ctx context.Context, dest net.Destination) (stat.Connection, error) {
	conn, err := d.Dialer.Dial(ctx, dest)
	if err == nil {
		d.connected.Store(true)
	}
	return conn, err
}

func (d *banTrackingDialer) Connected() bool {
	if d == nil {
		return true
	}
	return d.connected.Load()
}

// shouldFallbackV1 detects a peer that never spoke v2: the dial
// succeeded but no Resume reply arrived before death inside the handshake
// window. Frame counters can't prove this — the first payload is stored
// within ~100ms while the rejection travels a real path — but a v2 server
// answers every announce immediately, so a missing reply is the evidence.
func shouldFallbackV1(connected, sawReply bool, age time.Duration) bool {
	return connected && !sawReply && age < 5*time.Second
}

// out returns the stable session-write target: the gate when resume is
// enabled, the raw carrier pipe otherwise.
func (m *ClientWorker) out() buf.Writer {
	if m.gate != nil {
		return m.gate
	}
	return m.link.Writer
}

// IsSuspended reports whether the worker is parked awaiting rebind.
// Suspended workers read as full so new streams get a fresh worker.
func (m *ClientWorker) IsSuspended() bool {
	if m.gate == nil {
		return false
	}
	m.gate.mu.Lock()
	defer m.gate.mu.Unlock()
	return m.gate.suspended
}

// attachCarrier records the live pipe ends and points the gate at them.
func (m *ClientWorker) attachCarrier(upW buf.Writer, downR buf.Reader) {
	if m.gate != nil {
		m.gate.swapTarget(upW)
	}
	m.pipeMu.Lock()
	m.upPipe = upW
	m.downPipe = downR
	m.downReader = &buf.BufferedReader{Reader: downR}
	m.pipeMu.Unlock()
}

// interruptPipes unblocks loops parked on dead pipes. Retained bytes are
// already stored, so discards are replayable; session goroutines wait in
// the gate instead of failing.
func (m *ClientWorker) interruptPipes() {
	m.pipeMu.Lock()
	up, down := m.upPipe, m.downPipe
	m.pipeMu.Unlock()
	common.Interrupt(up)
	common.Interrupt(down)
}

// currentDownReader returns the live downlink reader for fetchOutput.
func (m *ClientWorker) currentDownReader() *buf.BufferedReader {
	m.pipeMu.Lock()
	defer m.pipeMu.Unlock()
	return m.downReader
}

// serveCarrier pumps one carrier, then suspends+rebinds or closes.
func (m *ClientWorker) serveCarrier(p proxy.Outbound, d internet.Dialer, uplinkReader buf.Reader, downlinkWriter buf.Writer, target net.Address, useV2 bool) {
	outbounds := []*session.Outbound{{
		Target: net.TCPDestination(target, muxCoolPort),
	}}
	ctx := session.ContextWithOutbounds(context.Background(), outbounds)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	pumpDone := make(chan error, 1)
	go func() { pumpDone <- p.Process(ctx, &transport.Link{Reader: uplinkReader, Writer: downlinkWriter}, d) }()
	errP := <-pumpDone
	if errP != nil {
		errC := errors.Cause(errP)
		if !(goerrors.Is(errC, io.EOF) || goerrors.Is(errC, io.ErrClosedPipe) || goerrors.Is(errC, context.Canceled)) {
			errors.LogInfoInner(ctx, errP, "failed to handler mux client connection")
		}
	}

	if !useV2 || !m.resume.Enabled {
		common.Must(m.done.Close())
		return
	}
	// Instant-death handshake failure (e.g. old server): fall back so
	// fresh workers use v1 instead of suspending pointlessly. Ban v2
	// only on a streak of post-connect deaths: a dial failure during a
	// flap must never ban the version resume needs.
	key := m.banKey
	if key == "" {
		key = muxCoolAddress.String()
	}
	if shouldFallbackV1(m.dialProbe.Connected(), m.sawResumeReply.Load(), time.Since(m.createdAt)) {
		if m.dialProbe.Connected() && recordV2Fail(key) >= 3 {
			banV2(key, m.resume.NoV2CacheTTL)
			// Reset the streak with the ban: v1 workers carry no
			// counted traffic, so nothing would ever clear it and the
			// first post-TTL failure would re-ban instantly.
			clearV2Fails(key)
		}
		common.Must(m.done.Close())
		return
	}
	if !m.dialProbe.Connected() {
		// Never got a carrier: fail fast like v1. Retrying here would
		// pile suspended workers and dial storms during an outage, and
		// there is no server-side state worth waiting for — the next
		// dispatch creates a fresh worker. Live sessions (connected at
		// least once) still suspend and redial below.
		common.Must(m.done.Close())
		return
	}
	clearV2Fails(key)
	if m.sessionManager.Size() == 0 {
		common.Must(m.done.Close())
		return
	}
	m.enterSuspend()
	m.startSupervisor(p, d, target)
}

// startSupervisor runs redial attempts until resume or suspend timeout.
// CAS-guarded: at most one supervisor per worker. On exit it re-checks:
// without that, a concurrent startSupervisor could fail its CAS against
// the stale flag and leave the worker suspended with no supervisor.
func (m *ClientWorker) startSupervisor(p proxy.Outbound, d internet.Dialer, target net.Address) {
	if !m.supervising.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer func() {
			m.supervising.Store(false)
			if !m.done.Done() && m.IsSuspended() && !time.Now().After(m.suspendDeadline()) {
				m.startSupervisor(p, d, target)
			}
		}()
		delays := m.resume.RedialDelays
		if len(delays) == 0 {
			delays = []time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second}
		}
		di := 0
		for {
			if m.done.Done() || !m.IsSuspended() {
				return
			}
			if time.Now().After(m.suspendDeadline()) {
				common.Must(m.done.Close())
				return
			}
			dl := delays[di%len(delays)]
			di++
			select {
			case <-m.done.Wait():
				return
			case <-time.After(dl):
			}
			m.redialAttempt(p, d, target)
		}
	}()
}

// redialAttempt dials a fresh carrier for the SAME worker: swap pipes,
// send Resume, run the pump, and wait for the reply, death or timeout.
func (m *ClientWorker) redialAttempt(p proxy.Outbound, d internet.Dialer, target net.Address) {
	opts := []pipe.Option{pipe.WithSizeLimit(64 * 1024)}
	uplinkReader, upLinkWriter := pipe.New(opts...)
	downlinkReader, downlinkWriter := pipe.New(opts...)

	m.epoch.Add(1)
	myEpoch := m.epoch.Load()
	errors.LogInfo(context.Background(), "mux resume: redial token ", m.tokenString(), " epoch ", myEpoch)
	resume := ResumePayload{Token: m.token, Epoch: myEpoch, RxCount: m.rx.Value()}
	meta := FrameMetadata{SessionStatus: SessionStatusResume}
	meta.Option.Set(OptionData)
	payload := encodeResume(resume)
	if err := writeMetaWithFrame(upLinkWriter, meta, buf.MultiBuffer{payload}); err != nil {
		return
	}
	m.attachCarrierSwap(upLinkWriter, downlinkReader)
	outbounds := []*session.Outbound{{
		Target: net.TCPDestination(target, muxCoolPort),
	}}
	ctx := session.ContextWithOutbounds(context.Background(), outbounds)
	ctx, cancel := context.WithCancel(ctx)
	pumpDone := make(chan error, 1)
	go func() {
		defer cancel()
		pumpDone <- p.Process(ctx, &transport.Link{Reader: uplinkReader, Writer: downlinkWriter}, d)
		if m.done.Done() {
			return
		}
		if m.sessionManager.Size() == 0 {
			common.Must(m.done.Close())
			return
		}
		if m.epoch.Load() != myEpoch {
			// A newer attempt took over (or finished): this tail is
			// stale and must not suspend a healthy worker or touch
			// pipes it no longer owns.
			return
		}
		m.enterSuspend()
		m.startSupervisor(p, d, target)
	}()
	select {
	case <-pumpDone:
	case <-m.done.Wait():
	case <-time.After(8 * time.Second):
	}
}

// attachCarrierSwap swaps pipes on rebind: interrupt the dead ends (stored
// bytes make discards replayable), install the new ones, and bump the pipe
// generation so waitRebind wakes for the new downlink.
func (m *ClientWorker) attachCarrierSwap(upW buf.Writer, downR buf.Reader) {
	m.pipeMu.Lock()
	oldUp, oldDown := m.upPipe, m.downPipe
	m.upPipe = upW
	m.downPipe = downR
	m.downReader = &buf.BufferedReader{Reader: downR}
	m.pipeMu.Unlock()
	m.pipeGen.Add(1)
	common.Interrupt(oldUp)
	common.Interrupt(oldDown)
	if m.gate != nil {
		m.gate.swapTarget(upW)
	}
}

// enterSuspend parks forwarding once; records the deadline on first entry.
func (m *ClientWorker) enterSuspend() {
	m.suspMu.Lock()
	if m.suspendEnd.IsZero() {
		timeout := m.resume.SuspendTimeout
		if timeout <= 0 {
			timeout = 10 * time.Second
		}
		m.suspendEnd = time.Now().Add(timeout)
	}
	m.suspMu.Unlock()
	if m.gate != nil {
		m.gate.setSuspended(true)
	}
	errors.LogInfo(context.Background(), "mux resume: suspend worker token ", m.tokenString(), " sessions ", m.sessionManager.Size())
	m.interruptPipes()
}

// suspendDeadline returns the current episode deadline.
func (m *ClientWorker) suspendDeadline() time.Time {
	m.suspMu.Lock()
	defer m.suspMu.Unlock()
	if m.suspendEnd.IsZero() {
		return time.Now().Add(10 * time.Second)
	}
	return m.suspendEnd
}

// clearSuspended resumes forwarding after a successful rebind.
func (m *ClientWorker) clearSuspended() {
	m.suspMu.Lock()
	m.suspendEnd = time.Time{}
	m.suspMu.Unlock()
	if m.gate != nil {
		m.gate.setSuspended(false)
	}
}

// watchHalfOpen forces suspend when bytes sit unacked past AckTimeout
// (write side alive, read side stalled): acks can't arrive, so redial.
func (m *ClientWorker) watchHalfOpen(p proxy.Outbound, d internet.Dialer, target net.Address) {
	t := time.NewTicker(2 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-m.done.Wait():
			return
		case <-t.C:
		}
		if m.gate == nil || m.IsSuspended() || m.done.Done() {
			continue
		}
		timeout := m.resume.AckTimeout
		if timeout <= 0 {
			timeout = 4 * time.Second
		}
		// Trip only when acks are stale AND the oldest unacked frame is
		// old: fresh acks veto (slow bulk is healthy), and young data
		// vetoes (a send after silence hasn't had time to be acked).
		idle := time.Since(m.gate.lastAckRecv())
		if halfOpenTripped(m.gate.UnackedBytes(), idle, m.gate.unackedAge(), timeout) {
			m.enterSuspend()
			m.startSupervisor(p, d, target)
		}
	}
}
