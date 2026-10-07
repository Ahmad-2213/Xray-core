package mux

// Resume lifecycle: suspend instead of close, rebind on the worker's own
// dial path, TTL-cached v1 fallback. Flag-off compiles to the v1 path.

import (
	"context"
	goerrors "errors"
	"io"
	"sync"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
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

func banV2(host string, ttl time.Duration) {
	if ttl <= 0 {
		ttl = 10 * time.Minute
	}
	noV2Mu.Lock()
	defer noV2Mu.Unlock()
	noV2Until[host] = time.Now().Add(ttl)
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
	// Instant-death handshake failure (e.g. old server): ban v2 so fresh
	// workers fall back, instead of suspending pointlessly.
	if m.gate.TxCount() == 0 && m.rx.Value() == 0 && time.Since(m.createdAt) < 5*time.Second {
		banV2(muxCoolAddressV2.String(), m.resume.NoV2CacheTTL)
		common.Must(m.done.Close())
		return
	}
	if m.sessionManager.Size() == 0 {
		common.Must(m.done.Close())
		return
	}
	m.enterSuspend()
	m.startSupervisor(p, d, target)
}

// startSupervisor runs redial attempts until resume or suspend timeout.
// CAS-guarded: at most one supervisor per worker.
func (m *ClientWorker) startSupervisor(p proxy.Outbound, d internet.Dialer, target net.Address) {
	if !m.supervising.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.supervising.Store(false)
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

	m.epoch++
	resume := ResumePayload{Token: m.token, Epoch: m.epoch, RxCount: m.rx.Value()}
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
// bytes make discards replayable), install the new ones.
func (m *ClientWorker) attachCarrierSwap(upW buf.Writer, downR buf.Reader) {
	m.pipeMu.Lock()
	oldUp, oldDown := m.upPipe, m.downPipe
	m.upPipe = upW
	m.downPipe = downR
	m.downReader = &buf.BufferedReader{Reader: downR}
	m.pipeMu.Unlock()
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
		if m.gate.UnackedBytes() > 0 && time.Since(m.gate.lastAckRecv()) > timeout {
			m.enterSuspend()
			m.startSupervisor(p, d, target)
		}
	}
}
