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

	useV2 := f.Resume.Enabled && !isV2Banned(muxCoolAddressV2.String())
	target := muxCoolAddress
	if useV2 {
		target = muxCoolAddressV2
	}
	c.useV2 = useV2
	c.redialP = f.Proxy
	c.redialD = f.Dialer
	c.redialAddr = target
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
		payload := encodeResume(ResumePayload{Token: c.token, Epoch: c.epoch, RxCount: 0})
		if err := writeMetaWithFrame(upLinkWriter, meta, buf.MultiBuffer{payload}); err != nil {
			useV2 = false
			target = muxCoolAddress
			c.useV2 = false
		}
	}

	go c.serveCarrier(f.Proxy, f.Dialer, uplinkReader, downlinkWriter, target, useV2)

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
	epoch          uint64
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
	suspMu     sync.Mutex
	suspendEnd time.Time
}

type ackState struct {
	mu       sync.Mutex
	lastSent uint64
	lastTime time.Time
}

var (
	muxCoolAddress   = net.DomainAddress("v1.mux.cool")
	muxCoolAddressV2 = net.DomainAddress("v2.mux.cool")
	muxCoolPort      = net.Port(9527)
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
		epoch:          uint64(time.Now().UnixNano()),
		createdAt:      time.Now(),
	}
	if policy.Enabled {
		if t, err := NewToken(); err == nil {
			c.token = t
		}
		c.gate = newCarrierGate(stream.Writer, c.done.Wait(), policy)
	}

	go c.fetchOutput()
	go c.monitor()
	if policy.Enabled {
		go c.watchHalfOpen()
	}

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

	s, found := m.sessionManager.Get(meta.SessionID)
	if !found {
		// Notify remote peer to close this session.
		closingWriter := NewResponseWriter(meta.SessionID, m.link.Writer, protocol.TransferTypeStream)
		closingWriter.Close()

		return buf.Copy(NewStreamReader(reader), buf.Discard)
	}

	rr := s.NewReader(reader, &meta.Target)
	err := buf.Copy(rr, s.output)
	if m.resume.Enabled && meta.Target.Network != net.Network_UDP {
		m.rx.Next()
		m.maybeSendAck()
	}
	if err != nil && buf.IsWriteError(err) {
		errors.LogInfoInner(context.Background(), err, "failed to write to downstream. closing session ", s.ID)
		s.Close(false)
		return buf.Copy(rr, buf.Discard)
	}

	return err
}

func (m *ClientWorker) handleStatusEnd(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if s, found := m.sessionManager.Get(meta.SessionID); found {
		s.Close(false)
	}
	if m.resume.Enabled {
		m.rx.Next()
		m.maybeSendAck()
	}
	if meta.Option.Has(OptionData) {
		return buf.Copy(NewStreamReader(reader), buf.Discard)
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
	mb, err := NewStreamReader(reader).ReadMultiBuffer()
	if err != nil {
		return err
	}
	defer buf.ReleaseMulti(mb)
	if len(mb) == 0 || len(mb[0].Bytes()) < resumePayloadLen {
		return errors.New("short resume payload")
	}
	rp, err := decodeResume(mb[0].Bytes())
	if err != nil {
		return err
	}
	if rp.Token != m.token {
		return errors.New("resume token mismatch")
	}
	if m.gate != nil {
		if err := m.gate.flushSince(rp.RxCount); err != nil {
			return err
		}
	}
	m.clearSuspended()
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
	mb, err := NewStreamReader(reader).ReadMultiBuffer()
	if err != nil {
		return err
	}
	defer buf.ReleaseMulti(mb)
	if len(mb) == 0 || len(mb[0].Bytes()) < ackPayloadLen {
		return errors.New("short ack payload")
	}
	ap, err := decodeAck(mb[0].Bytes())
	if err != nil {
		return err
	}
	if m.gate != nil {
		m.gate.ack(ap.RxCount)
	}
	return nil
}

// maybeSendAck emits Ack{rxCount} at most every AckEveryMs when rx advanced.
func (m *ClientWorker) maybeSendAck() {
	if !m.resume.Enabled || m.gate == nil {
		return
	}
	interval := m.resume.AckEveryMs
	if interval <= 0 {
		interval = 500
	}
	now := time.Now()
	m.ackMu.mu.Lock()
	if now.Sub(m.ackMu.lastTime) < time.Duration(interval)*time.Millisecond {
		m.ackMu.mu.Unlock()
		return
	}
	count := m.rx.Value()
	if count == m.ackMu.lastSent {
		m.ackMu.mu.Unlock()
		return
	}
	m.ackMu.lastSent = count
	m.ackMu.lastTime = now
	m.ackMu.mu.Unlock()

	m.gate.writeAck(count)
}

func (m *ClientWorker) fetchOutput() {
	defer func() {
		common.Must(m.done.Close())
	}()

	for {
		reader := m.currentDownReader()
		if m.readLoop(reader) {
			return
		}
		// Carrier read failed. v1 path (or closed worker): fail fast.
		if m.gate == nil || m.done.Done() {
			return
		}
		// Suspended: park until rebind swaps in a fresh reader, the
		// worker closes, or the episode times out.
		if !m.waitRebind() {
			return
		}
	}
}

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
			errors.LogInfoInner(context.Background(), err, "failed to process data")
			return true
		}
	}
}

// waitRebind parks fetchOutput until a rebind swaps the reader, the worker
// closes, or the suspend episode times out. False = give up.
func (m *ClientWorker) waitRebind() bool {
	for {
		if m.done.Done() {
			return false
		}
		if !m.IsSuspended() {
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
