package mux

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
		if !w.IsFull() {
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

	if useV2 {
		// Announce the resume token as the first frame so a later Resume
		// on a fresh carrier can rebind this worker's sessions server-side.
		meta := FrameMetadata{SessionStatus: SessionStatusResume}
		meta.Option.Set(OptionData)
		payload := encodeResume(ResumePayload{Token: c.token, Epoch: c.epoch, RxCount: 0})
		if err := writeMetaWithFrame(upLinkWriter, meta, buf.MultiBuffer{payload}); err != nil {
			// WriteMultiBuffer owns the buffer on all paths; just fall back.
			useV2 = false
			target = muxCoolAddress
		}
	}

	go func(p proxy.Outbound, d internet.Dialer, c *ClientWorker) {
		outbounds := []*session.Outbound{{
			Target: net.TCPDestination(target, muxCoolPort),
		}}
		ctx := session.ContextWithOutbounds(context.Background(), outbounds)
		ctx, cancel := context.WithCancel(ctx)

		if errP := p.Process(ctx, &transport.Link{Reader: uplinkReader, Writer: downlinkWriter}, d); errP != nil {
			errC := errors.Cause(errP)
			if !(goerrors.Is(errC, io.EOF) || goerrors.Is(errC, io.ErrClosedPipe) || goerrors.Is(errC, context.Canceled)) {
				errors.LogInfoInner(ctx, errP, "failed to handler mux client connection")
			}
		}
		c.onCarrierClosed(useV2)
		cancel()
	}(f.Proxy, f.Dialer, c)

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
	tx             Counter
	rx             Counter
	ackMu          ackState
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
	}
	if policy.Enabled {
		if t, err := NewToken(); err == nil {
			c.token = t
		}
	}

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
	writer.counter = s.tx
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
	if m.IsFull() {
		return false
	}

	sm := m.sessionManager
	s := sm.Allocate(&m.strategy, link.Reader, link.Writer)
	if s == nil {
		return false
	}
	if m.resume.Enabled {
		s.tx = &m.tx
	}
	go fetchInput(ctx, s, m.link.Writer)
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
	if m.resume.Enabled {
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

// handleStatusResume processes a server Resume response on a reattached
// carrier. Phase 1 parses and validates the echoed count; full replay from
// the retained queue lands with the sender-side retain store.
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
	m.rx.Next()
	return nil
}

// handleStatusAck frees retained frames up to the peer's rx count.
// Phase 1 has no retain store yet, so this only advances accounting.
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
	buf.ReleaseMulti(mb)
	return nil
}

// maybeSendAck emits Ack{rxCount} at most every AckEveryMs when rx advanced.
func (m *ClientWorker) maybeSendAck() {
	if !m.resume.Enabled {
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

	meta := FrameMetadata{SessionStatus: SessionStatusAck}
	meta.Option.Set(OptionData)
	// Ownership transfers to WriteMultiBuffer (releases on all paths).
	payload := encodeAck(AckPayload{RxCount: count})
	_ = writeMetaWithFrame(m.link.Writer, meta, buf.MultiBuffer{payload})
}

func (m *ClientWorker) fetchOutput() {
	defer func() {
		common.Must(m.done.Close())
	}()

	reader := &buf.BufferedReader{Reader: m.link.Reader}

	var meta FrameMetadata
	for {
		err := meta.Unmarshal(reader, false)
		if err != nil {
			if errors.Cause(err) != io.EOF {
				errors.LogInfoInner(context.Background(), err, "failed to read metadata")
			}
			break
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
			return
		}

		if err != nil {
			errors.LogInfoInner(context.Background(), err, "failed to process data")
			return
		}
	}
}
