package mux

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
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/common/task"
	"github.com/xtls/xray-core/common/xudp"
	"github.com/xtls/xray-core/proxy"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/pipe"
)

type ClientManager struct {
	Enabled bool // whether mux is enabled from user config
	Picker  WorkerPicker
}

func (m *ClientManager) Dispatch(ctx context.Context, link *transport.Link) error {
	for i := 0; i < 16; i++ {
		worker, err := m.Picker.PickAvailable()
		if err != nil {
			return err
		}
		if worker.Dispatch(ctx, link) {
			return nil
		}
	}

	return errors.New("unable to find an available mux client")
}

type WorkerPicker interface {
	PickAvailable() (*ClientWorker, error)
}

type IncrementalWorkerPicker struct {
	Factory ClientWorkerFactory

	access      sync.Mutex
	workers     []*ClientWorker
	cleanupTask *task.Periodic
}

func (p *IncrementalWorkerPicker) cleanupFunc() error {
	p.access.Lock()
	defer p.access.Unlock()

	if len(p.workers) == 0 {
		return errors.New("no worker")
	}

	p.cleanup()
	return nil
}

func (p *IncrementalWorkerPicker) cleanup() {
	var activeWorkers []*ClientWorker
	for _, w := range p.workers {
		if !w.Closed() {
			activeWorkers = append(activeWorkers, w)
		}
	}
	p.workers = activeWorkers
}

func (p *IncrementalWorkerPicker) findAvailable() int {
	for idx, w := range p.workers {
		if !w.IsFull() && !w.IsSuspended() {
			return idx
		}
	}

	return -1
}

func (p *IncrementalWorkerPicker) pickInternal() (*ClientWorker, bool, error) {
	p.access.Lock()
	defer p.access.Unlock()

	idx := p.findAvailable()
	if idx >= 0 {
		n := len(p.workers)
		if n > 1 && idx != n-1 {
			p.workers[n-1], p.workers[idx] = p.workers[idx], p.workers[n-1]
		}
		return p.workers[idx], false, nil
	}

	p.cleanup()

	worker, err := p.Factory.Create()
	if err != nil {
		return nil, false, err
	}
	p.workers = append(p.workers, worker)

	if p.cleanupTask == nil {
		p.cleanupTask = &task.Periodic{
			Interval: time.Second * 30,
			Execute:  p.cleanupFunc,
		}
	}

	return worker, true, nil
}

func (p *IncrementalWorkerPicker) PickAvailable() (*ClientWorker, error) {
	worker, start, err := p.pickInternal()
	if start {
		common.Must(p.cleanupTask.Start())
	}

	return worker, err
}

type ClientWorkerFactory interface {
	Create() (*ClientWorker, error)
}

type DialingWorkerFactory struct {
	Proxy       proxy.Outbound
	Dialer      internet.Dialer
	Strategy    ClientStrategy
	Resume      ResumePolicy
	OutboundTag string
}

func (f *DialingWorkerFactory) Create() (*ClientWorker, error) {
	opts := []pipe.Option{pipe.WithSizeLimit(64 * 1024)}
	uplinkReader, upLinkWriter := pipe.New(opts...)
	downlinkReader, downlinkWriter := pipe.New(opts...)

	c, err := NewClientWorkerWithResume(transport.Link{
		Reader: downlinkReader,
		Writer: upLinkWriter,
	}, f.Strategy, f.Resume)
	if err != nil {
		return nil, err
	}

	// Version negotiation is in-band on v1.mux.cool (RequestCommandMux on
	// every protocol, Vision flow preserved): a v2 worker announces its
	// resume token as the first frame, and the server sniffs it. Old
	// servers fail the unknown status fast with zero dials, which is also
	// the reliable fallback signal. A separate v2 magic hostname would go
	// out as plain TCP (Vision bypass) and leak the announce to whoever
	// answers that name on old servers.
	useV2 := f.Resume.Enabled && !isV2Banned(v2BanKey(f.OutboundTag, muxCoolAddress))
	target := muxCoolAddress
	c.useV2 = useV2
	c.banKey = v2BanKey(f.OutboundTag, muxCoolAddress)
	c.redialP = f.Proxy
	c.redialD = f.Dialer
	c.redialAddr = target
	// The initial carrier dials through a probe: only a post-connect
	// instant death counts toward the v2 ban (see serveCarrier).
	probe := &banTrackingDialer{Dialer: f.Dialer}
	c.dialProbe = probe
	if c.gate != nil {
		c.gate.setOnCarrierError(func() {
			c.enterSuspend()
			c.startSupervisor(c.redialP, c.redialD, c.redialAddr)
		})
	}

	c.attachCarrier(upLinkWriter, downlinkReader)

	if useV2 {
		// Announce the resume token as the first frame so a later Resume
		// on a fresh carrier can rebind this worker's sessions server-side.
		// Uncounted: bypasses the gate, direct to the pipe.
		meta := FrameMetadata{SessionStatus: SessionStatusResume}
		meta.Option.Set(OptionData)
		payload := encodeResume(ResumePayload{Token: c.token, Epoch: c.epoch.Load(), RxCount: 0})
		if err := writeMetaWithFrame(upLinkWriter, meta, buf.MultiBuffer{payload}); err != nil {
			useV2 = false
			c.useV2 = false
		}
	}

	go c.serveCarrier(f.Proxy, probe, uplinkReader, downlinkWriter, target, useV2)

	if useV2 {
		go c.watchHalfOpen(f.Proxy, f.Dialer, target)
	}

	return c, nil
}

type ClientStrategy struct {
	MaxConcurrency uint32
	MaxConnection  uint32
}

type ClientWorker struct {
	sessionManager *SessionManager
	link           transport.Link
	done           *done.Instance
	timer          *time.Ticker
	strategy       ClientStrategy
	resume         ResumePolicy
	token          [16]byte
	epoch          atomic.Uint64
	rx             Counter
	ackMu          ackState
	// Phase 2 suspend/resume state. gate is nil unless resume is enabled.
	gate        *carrierGate
	useV2       bool
	createdAt   time.Time
	supervising atomic.Bool
	redialP     proxy.Outbound
	redialD     internet.Dialer
	redialAddr  net.Address

	pipeMu     sync.Mutex
	downReader *buf.BufferedReader
	downPipe   buf.Reader
	upPipe     buf.Writer
	// pipeGen bumps on every carrier swap so waitRebind wakes for the new
	// downlink (which carries the Resume reply that clears the suspend).
	pipeGen atomic.Uint64
	// banKey scopes the v2 fallback ban to this outbound tag + server.
	banKey string
	// sawResumeReply records any valid Resume reply (either epoch): a v2
	// server answers every announce immediately, so its absence at death
	// is the old-server evidence the fallback predicate needs. Frame
	// counters can't serve: the first payload is stored within ~100ms
	// while a real-path rejection is still traveling.
	sawResumeReply atomic.Bool
	// dialProbe records whether the initial carrier dial succeeded; only
	// post-connect deaths count toward the v2 ban (see serveCarrier).
	dialProbe  *banTrackingDialer
	suspMu     sync.Mutex
	suspendEnd time.Time
}

type ackState struct {
	mu       sync.Mutex
	lastSent uint64
	lastTime time.Time
	pending  bool
}

var (
	muxCoolAddress = net.DomainAddress("v1.mux.cool")
	muxCoolPort    = net.Port(9527)
)

// NewClientWorker creates a new mux.Client.
func NewClientWorker(stream transport.Link, s ClientStrategy) (*ClientWorker, error) {
	return NewClientWorkerWithResume(stream, s, DisabledPolicy())
}

// NewClientWorkerWithResume creates a mux client with opt-in resume policy.
// Flag-off (DisabledPolicy) follows the exact v1 path with zero extra cost.
func NewClientWorkerWithResume(stream transport.Link, s ClientStrategy, policy ResumePolicy) (*ClientWorker, error) {
	c := &ClientWorker{
		sessionManager: NewSessionManager(),
		link:           stream,
		done:           done.New(),
		timer:          time.NewTicker(time.Second * 16),
		strategy:       s,
		resume:         policy,
		createdAt:      time.Now(),
	}
	c.epoch.Store(uint64(time.Now().UnixNano()))
	if policy.Enabled {
		if t, err := NewToken(); err == nil {
			c.token = t
		}
		c.gate = newCarrierGate(stream.Writer, c.done.Wait(), policy)
	}
	c.attachCarrier(stream.Writer, stream.Reader)

	go c.fetchOutput()
	go c.monitor()

	return c, nil
}

func (m *ClientWorker) TotalConnections() uint32 {
	return uint32(m.sessionManager.Count())
}

func (m *ClientWorker) ActiveConnections() uint32 {
	return uint32(m.sessionManager.Size())
}

// Closed returns true if this Client is closed.
func (m *ClientWorker) Closed() bool {
	return m.done.Done()
}

func (m *ClientWorker) WaitClosed() <-chan struct{} {
	return m.done.Wait()
}

func (m *ClientWorker) Close() error {
	return m.done.Close()
}

func (m *ClientWorker) monitor() {
	defer m.timer.Stop()

	for {
		checkSize := m.sessionManager.Size()
		checkCount := m.sessionManager.Count()
		select {
		case <-m.done.Wait():
			m.sessionManager.Close()
			common.Interrupt(m.link.Writer)
			common.Interrupt(m.link.Reader)
			return
		case <-m.timer.C:
			if m.sessionManager.CloseIfNoSessionAndIdle(checkSize, checkCount) {
				common.Must(m.done.Close())
			}
		}
	}
}

func writeFirstPayload(reader buf.Reader, writer *Writer) error {
	err := buf.CopyOnceTimeout(reader, writer, time.Millisecond*100)
	if err == buf.ErrNotTimeoutReader || err == buf.ErrReadTimeout {
		return writer.WriteMultiBuffer(buf.MultiBuffer{})
	}

	if err != nil {
		return err
	}

	return nil
}

func fetchInput(ctx context.Context, s *Session, output buf.Writer) {
	outbounds := session.OutboundsFromContext(ctx)
	ob := outbounds[len(outbounds)-1]
	transferType := protocol.TransferTypeStream
	if ob.Target.Network == net.Network_UDP {
		transferType = protocol.TransferTypePacket
	}
	s.transferType = transferType
	var inbound *session.Inbound
	if session.IsReverseMuxFromContext(ctx) {
		inbound = session.InboundFromContext(ctx)
	}
	writer := NewWriter(s.ID, ob.Target, output, transferType, xudp.GetGlobalID(ctx), inbound)
	defer s.Close(false)
	defer writer.Close()

	errors.LogInfo(ctx, "dispatching request to ", ob.Target)
	if err := writeFirstPayload(s.input, writer); err != nil {
		errors.LogInfoInner(ctx, err, "failed to write first payload")
		writer.hasError = true
		return
	}

	if err := buf.Copy(s.input, writer); err != nil {
		errors.LogInfoInner(ctx, err, "failed to fetch all input")
		writer.hasError = true
		return
	}
}

func (m *ClientWorker) IsClosing() bool {
	sm := m.sessionManager
	if m.strategy.MaxConnection > 0 && sm.Count() >= int(m.strategy.MaxConnection) {
		return true
	}
	return false
}

// IsFull returns true if this ClientWorker is unable to accept more connections.
// it might be because it is closing, or the number of connections has reached the limit.
func (m *ClientWorker) IsFull() bool {
	if m.IsClosing() || m.Closed() {
		return true
	}

	sm := m.sessionManager
	if m.strategy.MaxConcurrency > 0 && sm.Size() >= int(m.strategy.MaxConcurrency) {
		return true
	}
	return false
}

func (m *ClientWorker) Dispatch(ctx context.Context, link *transport.Link) bool {
	if m.IsFull() || m.IsSuspended() {
		return false
	}

	sm := m.sessionManager
	s := sm.Allocate(&m.strategy, link.Reader, link.Writer)
	if s == nil {
		return false
	}
	go fetchInput(ctx, s, m.out())
	if _, ok := link.Reader.(*pipe.Reader); !ok {
		select {
		case <-ctx.Done():
		case <-s.done.Wait():
		}
	}
	return true
}

func (m *ClientWorker) handleStatueKeepAlive(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if meta.Option.Has(OptionData) {
		return buf.Copy(NewStreamReader(reader), buf.Discard)
	}
	return nil
}

func (m *ClientWorker) handleStatusNew(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if meta.Option.Has(OptionData) {
		return buf.Copy(NewStreamReader(reader), buf.Discard)
	}
	return nil
}

func (m *ClientWorker) handleStatusKeep(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !meta.Option.Has(OptionData) {
		return nil
	}

	// Count only what was actually delivered: the sender counts at store
	// time, so a frame cut mid-payload must stay out of rx or the sender
	// will never replay it (silent loss). A downstream write error means
	// the carrier delivered fully — still count. Mirrors the server.
	count := m.resume.Enabled && meta.Target.Network != net.Network_UDP
	s, found := m.sessionManager.Get(meta.SessionID)
	if !found {
		// Notify remote peer to close this session. Routed via out() so
		// the End is counted and retained (and hits the live carrier
		// after a swap). Async: the gate may be suspended and must
		// never stall the reader loop.
		sid := meta.SessionID
		out := m.out()
		go func() {
			closingWriter := NewResponseWriter(sid, out, protocol.TransferTypeStream)
			closingWriter.Close()
		}()

		if err := buf.Copy(NewStreamReader(reader), buf.Discard); err != nil {
			return &frameReadError{err}
		}
		if count {
			m.rx.Next()
			m.maybeSendAck()
		}
		return nil
	}

	rr := s.NewReader(reader, &meta.Target)
	if !m.resume.Enabled {
		// Flag-off: original upstream streaming delivery — unbounded,
		// byte-identical v1 path.
		err := buf.Copy(rr, s.output)
		if err != nil && buf.IsWriteError(err) {
			errors.LogInfoInner(context.Background(), err, "failed to write to downstream. closing session ", s.ID)
			s.Close(false)
			return buf.Copy(rr, buf.Discard)
		}
		return err
	}
	mb, rerr := readFullFrame(rr)
	if rerr != nil {
		if rerr == errFrameTooLarge {
			return rerr
		}
		return &frameReadError{rerr}
	}
	werr := s.output.WriteMultiBuffer(mb)
	if count {
		m.rx.Next()
		m.maybeSendAck()
	}
	if werr != nil {
		errors.LogInfoInner(context.Background(), werr, "failed to write to downstream. closing session ", s.ID)
		s.Close(false)
		if derr := buf.Copy(rr, buf.Discard); derr != nil {
			return &frameReadError{derr}
		}
		return nil
	}

	return nil
}

func (m *ClientWorker) handleStatusEnd(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if s, found := m.sessionManager.Get(meta.SessionID); found {
		s.Close(false)
	}
	count := m.resume.Enabled
	if meta.Option.Has(OptionData) {
		if err := buf.Copy(NewStreamReader(reader), buf.Discard); err != nil {
			return &frameReadError{err}
		}
	}
	if count {
		m.rx.Next()
		m.maybeSendAck()
	}
	return nil
}

// handleStatusResume processes the server Resume reply on a reattached
// carrier: validate, replay retained frames past the server's rx, resume.
func (m *ClientWorker) handleStatusResume(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !m.resume.Enabled {
		return errors.New("unexpected resume status")
	}
	if !meta.Option.Has(OptionData) {
		return nil
	}
	mb, err := readFullFrame(NewStreamReader(reader))
	if err != nil {
		if err == errFrameTooLarge {
			return err
		}
		return &frameReadError{err}
	}
	defer buf.ReleaseMulti(mb)
	var raw []byte
	for _, b := range mb {
		raw = append(raw, b.Bytes()...)
	}
	if len(raw) < resumePayloadLen {
		return errors.New("short resume payload")
	}
	rp, err := decodeResume(raw)
	if err != nil {
		return err
	}
	if rp.Token != m.token {
		return errors.New("resume token mismatch")
	}
	m.sawResumeReply.Store(true)
	// Any valid Resume reply proves v2 on this tag: clear the fallback
	// streak (a negative epoch-0 reply counts too — only v2 speaks it).
	clearV2Fails(m.banKey)
	if rp.Epoch == 0 {
		// Negative reply: the server has nothing parked (fresh carrier
		// raced a park, or a previous park expired). Stay suspended and
		// keep redialing on cadence; do NOT resume, flush, or clear.
		return nil
	}
	flushed := 0
	go func() {
		if m.gate != nil {
			if rp.RxCount > m.gate.TxCount() {
				errors.LogInfoInner(context.Background(), errors.New("resume count beyond sent"), "mux resume: rebind flush failed")
				return
			}
			sent := m.gate.TxCount()
			if err := m.gate.flushSince(rp.RxCount); err != nil {
				errors.LogInfoInner(context.Background(), err, "mux resume: rebind flush failed")
				return
			}
			// Free what the peer confirms, mirroring the server side.
			m.gate.ack(rp.RxCount)
			if sent > rp.RxCount {
				flushed = int(sent - rp.RxCount)
			}
		}
		m.clearSuspended()
		errors.LogInfo(context.Background(), "mux resume: reattached token ", m.tokenString(), " replayed ", flushed, " frames")
	}()
	// Flush runs off the reader loop (bidirectional unacked data above
	// the pipe buffers would deadlock two inline flushes). The carrier
	// stays readable meanwhile.
	return nil
}

// handleStatusAck frees retained frames up to the peer's rx count.
func (m *ClientWorker) handleStatusAck(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !m.resume.Enabled {
		return errors.New("unexpected ack status")
	}
	if !meta.Option.Has(OptionData) {
		return nil
	}
	mb, err := readFullFrame(NewStreamReader(reader))
	if err != nil {
		if err == errFrameTooLarge {
			return err
		}
		return &frameReadError{err}
	}
	defer buf.ReleaseMulti(mb)
	var raw []byte
	for _, b := range mb {
		raw = append(raw, b.Bytes()...)
	}
	if len(raw) < ackPayloadLen {
		return errors.New("short ack payload")
	}
	ap, err := decodeAck(raw)
	if err != nil {
		return err
	}
	if m.gate != nil {
		m.gate.ack(ap.RxCount)
	}
	return nil
}

// maybeSendAck emits Ack{rxCount} at most every AckEveryMs when rx advanced.
// A trailing ack is scheduled when rate-limited with unacked rx, so a
// burst's last frame is never left unacked (see server maybeSendAck).
func (m *ClientWorker) maybeSendAck() {
	if !m.resume.Enabled || m.gate == nil {
		return
	}
	intervalMs := m.resume.AckEveryMs
	if intervalMs <= 0 {
		intervalMs = 500
	}
	interval := time.Duration(intervalMs) * time.Millisecond
	now := time.Now()
	m.ackMu.mu.Lock()
	count := m.rx.Value()
	advanced := count != m.ackMu.lastSent
	// Frame-count trigger: bulk traffic must not wait out the whole
	// timer per window (see server maybeSendAck).
	if advanced && (count-m.ackMu.lastSent >= 8 || now.Sub(m.ackMu.lastTime) >= interval) {
		m.ackMu.lastSent = count
		m.ackMu.lastTime = now
		m.ackMu.mu.Unlock()
		m.gate.writeAck(count)
		return
	}
	if advanced && !m.ackMu.pending {
		m.ackMu.pending = true
		wait := interval - now.Sub(m.ackMu.lastTime)
		m.ackMu.mu.Unlock()
		time.AfterFunc(wait, m.sendTrailingAck)
		return
	}
	m.ackMu.mu.Unlock()
}

// sendTrailingAck emits the delayed ack scheduled by maybeSendAck.
func (m *ClientWorker) sendTrailingAck() {
	m.ackMu.mu.Lock()
	m.ackMu.pending = false
	count := m.rx.Value()
	if count == m.ackMu.lastSent {
		m.ackMu.mu.Unlock()
		return
	}
	m.ackMu.lastSent = count
	m.ackMu.lastTime = time.Now()
	m.ackMu.mu.Unlock()
	if m.gate != nil {
		m.gate.writeAck(count)
	}
}

// currentDownReaderGen returns the live downlink reader together with its
// pipe generation, atomically: a swap between separate calls could hand
// fetchOutput a reader that no longer matches the generation it compares.
func (m *ClientWorker) currentDownReaderGen() (*buf.BufferedReader, uint64) {
	m.pipeMu.Lock()
	defer m.pipeMu.Unlock()
	return m.downReader, m.pipeGen.Load()
}

func (m *ClientWorker) fetchOutput() {
	defer func() {
		common.Must(m.done.Close())
	}()

	for {
		reader, gen := m.currentDownReaderGen()
		if m.readLoop(reader) {
			return
		}
		// Carrier read failed. v1 path (or closed worker): fail fast.
		if m.gate == nil || m.done.Done() {
			return
		}
		// A swap landed while reading: the new downlink (and its Resume
		// reply) is already installed — read it immediately instead of
		// waiting out a poll with a stale generation baseline.
		if m.pipeGen.Load() != gen {
			continue
		}
		// Suspended: park until rebind swaps in a fresh reader, the
		// worker closes, or the episode times out.
		if !m.waitRebind(gen) {
			return
		}
	}
}

// frameReadError marks bytes that never arrived: the carrier died
// mid-frame. Unlike decode/protocol errors (corrupt data on a live
// carrier — fail fast), these must park and wait: the downlink can error
// before suspension is recorded, and only a rebind clears it.
type frameReadError struct{ err error }

func (e *frameReadError) Error() string { return e.err.Error() }
func (e *frameReadError) Unwrap() error { return e.err }

// readLoop processes frames until the carrier errors. True = worker done.
func (m *ClientWorker) readLoop(reader *buf.BufferedReader) bool {
	var meta FrameMetadata
	for {
		err := meta.Unmarshal(reader, false)
		if err != nil {
			if errors.Cause(err) != io.EOF {
				errors.LogInfoInner(context.Background(), err, "failed to read metadata")
			}
			return false
		}

		switch meta.SessionStatus {
		case SessionStatusKeepAlive:
			err = m.handleStatueKeepAlive(&meta, reader)
		case SessionStatusEnd:
			err = m.handleStatusEnd(&meta, reader)
		case SessionStatusNew:
			err = m.handleStatusNew(&meta, reader)
		case SessionStatusKeep:
			err = m.handleStatusKeep(&meta, reader)
		case SessionStatusResume:
			err = m.handleStatusResume(&meta, reader)
		case SessionStatusAck:
			err = m.handleStatusAck(&meta, reader)
		default:
			status := meta.SessionStatus
			errors.LogError(context.Background(), "unknown status: ", status)
			return true
		}

		if err != nil {
			// Transport loss parks even before suspension is recorded
			// (the downlink can error first); decode and protocol
			// errors still fail fast.
			var fre *frameReadError
			if goerrors.As(err, &fre) {
				return false
			}
			errors.LogInfoInner(context.Background(), err, "failed to process data")
			return true
		}
	}
}

// waitRebind parks fetchOutput until a rebind swaps the reader, the worker
// closes, or the suspend episode times out. False = give up. gen is the
// pipe generation the caller just read: a swap between the caller's check
// and this capture would otherwise miss one reply and cost that attempt.
// The generation check is the point: attachCarrierSwap bumps pipeGen on
// every rebind, so fetchOutput picks up the new downlink and reads the
// server's Resume reply (which clears the suspend). Waiting on
// !IsSuspended alone would deadlock, since only the reply reader can
// clear it.
func (m *ClientWorker) waitRebind(gen uint64) bool {
	for {
		if m.done.Done() {
			return false
		}
		if !m.IsSuspended() || m.pipeGen.Load() != gen {
			return true
		}
		if time.Now().After(m.suspendDeadline()) {
			return false
		}
		select {
		case <-m.done.Wait():
			return false
		case <-time.After(500 * time.Millisecond):
		}
	}
}
