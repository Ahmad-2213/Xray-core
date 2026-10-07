package mux

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/log"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/protocol"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/common/signal/done"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/pipe"
)

type Server struct {
	dispatcher routing.Dispatcher
}

// NewServer creates a new mux.Server.
func NewServer(ctx context.Context) *Server {
	s := &Server{}
	core.RequireFeatures(ctx, func(d routing.Dispatcher) {
		s.dispatcher = d
	})
	return s
}

// Type implements common.HasType.
func (s *Server) Type() interface{} {
	return s.dispatcher.Type()
}

// Dispatch implements routing.Dispatcher
func (s *Server) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	if dest.Address != muxCoolAddress && dest.Address != muxCoolAddressV2 {
		return s.dispatcher.Dispatch(ctx, dest)
	}

	opts := pipe.OptionsFromContext(ctx)
	uplinkReader, uplinkWriter := pipe.New(opts...)
	downlinkReader, downlinkWriter := pipe.New(opts...)

	_, err := NewServerWorker(ctx, s.dispatcher, &transport.Link{
		Reader: uplinkReader,
		Writer: downlinkWriter,
	})
	if err != nil {
		return nil, err
	}

	return &transport.Link{Reader: downlinkReader, Writer: uplinkWriter}, nil
}

// DispatchLink implements routing.Dispatcher
func (s *Server) DispatchLink(ctx context.Context, dest net.Destination, link *transport.Link) error {
	if dest.Address != muxCoolAddress && dest.Address != muxCoolAddressV2 {
		return s.dispatcher.DispatchLink(ctx, dest, link)
	}
	worker, err := NewServerWorker(ctx, s.dispatcher, link)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
	case <-worker.done.Wait():
	}
	return nil
}

// Start implements common.Runnable.
func (s *Server) Start() error {
	return nil
}

// Close implements common.Closable.
func (s *Server) Close() error {
	return nil
}

type ServerWorker struct {
	dispatcher routing.Dispatcher
	link       *transport.Link
	// sessionManager and gate are swapped by park/adopt while monitor and
	// handlers read them: atomic pointers so the swap is race-free.
	sessionManager atomic.Pointer[SessionManager]
	gate           atomic.Pointer[carrierGate]
	done           *done.Instance
	timer          *time.Ticker
	tx             Counter
	rx             Counter
	resumeToken    [16]byte
	resumeHasToken bool
	resumeUser     string
	resumeEpoch    uint64
	// resumeDone is the token-scoped lifetime for the gate. The parking
	// worker may be reaped by its monitor while adopted sessions are
	// still live, so the gate must never reference a worker's done.
	// Shared with the handover entry; closed once on worker close or
	// park expiry.
	resumeDone *tokenDone
	// localUser binds rebinds to the VLESS user of this worker,
	// captured at construction (run/adopt run on one goroutine after).
	localUser string
	ackMu     ackState
}

func NewServerWorker(ctx context.Context, d routing.Dispatcher, link *transport.Link) (*ServerWorker, error) {
	worker := &ServerWorker{
		dispatcher: d,
		link:       link,
		done:       done.New(),
		timer:      time.NewTicker(60 * time.Second),
	}
	worker.sessionManager.Store(NewSessionManager())
	if ib := session.InboundFromContext(ctx); ib != nil && ib.User != nil {
		worker.localUser = ib.User.Email
	}
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		inbound.CanSpliceCopy = 3
	}
	go worker.run(ctx)
	go worker.monitor()
	return worker, nil
}

func handle(ctx context.Context, s *Session, output buf.Writer) {
	writer := NewResponseWriter(s.ID, output, s.transferType)
	if err := buf.Copy(s.input, writer); err != nil {
		errors.LogInfoInner(ctx, err, "session ", s.ID, " ends.")
		writer.hasError = true
	}

	writer.Close()
	s.Close(false)
}

// out returns the stable response-write target: the gate once a v2
// Resume was seen, the raw carrier pipe otherwise.
func (w *ServerWorker) out() buf.Writer {
	if g := w.gate.Load(); g != nil {
		return g
	}
	return w.link.Writer
}

func (w *ServerWorker) monitor() {
	defer w.timer.Stop()

	for {
		sm := w.sessionManager.Load()
		checkSize := sm.Size()
		checkCount := sm.Count()
		select {
		case <-w.done.Wait():
			sm.Close()
			common.Interrupt(w.link.Writer)
			common.Interrupt(w.link.Reader)
			return
		case <-w.timer.C:
			if sm.CloseIfNoSessionAndIdle(checkSize, checkCount) {
				common.Must(w.done.Close())
			}
		}
	}
}

func (w *ServerWorker) ActiveConnections() uint32 {
	return uint32(w.sessionManager.Load().Size())
}

func (w *ServerWorker) Closed() bool {
	return w.done.Done()
}

func (w *ServerWorker) WaitClosed() <-chan struct{} {
	return w.done.Wait()
}

func (w *ServerWorker) Close() error {
	return w.done.Close()
}

func (w *ServerWorker) handleStatusKeepAlive(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if meta.Option.Has(OptionData) {
		return buf.Copy(NewStreamReader(reader), buf.Discard)
	}
	return nil
}

func (w *ServerWorker) handleStatusNew(ctx context.Context, meta *FrameMetadata, reader *buf.BufferedReader) error {
	ctx = session.SubContextFromMuxInbound(ctx)
	if meta.Inbound != nil && meta.Inbound.Source.IsValid() && meta.Inbound.Local.IsValid() {
		if inbound := session.InboundFromContext(ctx); inbound != nil {
			newInbound := *inbound
			newInbound.Source = meta.Inbound.Source
			newInbound.Local = meta.Inbound.Local
			ctx = session.ContextWithInbound(ctx, &newInbound)
		}
	}
	errors.LogInfo(ctx, "received request for ", meta.Target)
	{
		msg := &log.AccessMessage{
			To:     meta.Target,
			Status: log.AccessAccepted,
			Reason: "",
		}
		if inbound := session.InboundFromContext(ctx); inbound != nil && inbound.Source.IsValid() {
			msg.From = inbound.Source
			msg.Email = inbound.User.Email
		}
		ctx = log.ContextWithAccessMessage(ctx, msg)
	}

	if network := session.AllowedNetworkFromContext(ctx); network != net.Network_Unknown {
		if meta.Target.Network != network {
			return errors.New("unexpected network ", meta.Target.Network) // it will break the whole Mux connection
		}
	}

	if meta.GlobalID != [8]byte{} { // MUST ignore empty Global ID
		mb, err := NewPacketReader(reader, &meta.Target).ReadMultiBuffer()
		if err != nil {
			return err
		}
		XUDPManager.Lock()
		x := XUDPManager.Map[meta.GlobalID]
		if x == nil {
			x = &XUDP{GlobalID: meta.GlobalID}
			XUDPManager.Map[meta.GlobalID] = x
			XUDPManager.Unlock()
		} else {
			if x.Status == Initializing { // nearly impossible
				XUDPManager.Unlock()
				errors.LogWarningInner(ctx, errors.New("conflict"), "XUDP hit ", meta.GlobalID)
				// It's not a good idea to return an err here, so just let client wait.
				// Client will receive an End frame after sending a Keep frame.
				return nil
			}
			x.Status = Initializing
			XUDPManager.Unlock()
			x.Mux.Close(false) // detach from previous Mux
			b := buf.New()
			b.Write(mb[0].Bytes())
			b.UDP = mb[0].UDP
			if err = x.Mux.output.WriteMultiBuffer(mb); err != nil {
				x.Interrupt()
				mb = buf.MultiBuffer{b}
			} else {
				b.Release()
				mb = nil
			}
			errors.LogInfoInner(ctx, err, "XUDP hit ", meta.GlobalID)
		}
		if mb != nil {
			ctx = session.ContextWithTimeoutOnly(ctx, true)
			// Actually, it won't return an error in Xray-core's implementations.
			link, err := w.dispatcher.Dispatch(ctx, meta.Target)
			if err != nil {
				XUDPManager.Lock()
				delete(XUDPManager.Map, x.GlobalID)
				XUDPManager.Unlock()
				err = errors.New("XUDP new ", meta.GlobalID).Base(errors.New("failed to dispatch request to ", meta.Target).Base(err))
				return err // it will break the whole Mux connection
			}
			link.Writer.WriteMultiBuffer(mb) // it's meaningless to test a new pipe
			x.Mux = &Session{
				input:  link.Reader,
				output: link.Writer,
			}
			errors.LogInfoInner(ctx, err, "XUDP new ", meta.GlobalID)
		}
		x.Mux = &Session{
			input:        x.Mux.input,
			output:       x.Mux.output,
			parent:       w.sessionManager.Load(),
			ID:           meta.SessionID,
			transferType: protocol.TransferTypePacket,
			XUDP:         x,
		}
		x.Status = Active
		if !w.sessionManager.Load().Add(x.Mux) {
			x.Mux.Close(false)
			return errors.New("failed to add new session")
		}
		go handle(ctx, x.Mux, w.out())
		return nil
	}

	link, cancel, err := w.dispatchLink(ctx, meta.Target)
	if err != nil {
		if meta.Option.Has(OptionData) {
			buf.Copy(NewStreamReader(reader), buf.Discard)
		}
		return errors.New("failed to dispatch request.").Base(err)
	}
	s := &Session{
		input:        link.Reader,
		output:       link.Writer,
		parent:       w.sessionManager.Load(),
		ID:           meta.SessionID,
		transferType: protocol.TransferTypeStream,
		cancel:       cancel,
	}
	if meta.Target.Network == net.Network_UDP {
		s.transferType = protocol.TransferTypePacket
	}
	if !w.sessionManager.Load().Add(s) {
		s.Close(false)
		return errors.New("failed to add new session")
	}
	w.countRx(meta)
	go handle(ctx, s, w.out())
	if !meta.Option.Has(OptionData) {
		return nil
	}

	rr := s.NewReader(reader, &meta.Target)
	err = buf.Copy(rr, s.output)

	if err != nil && buf.IsWriteError(err) {
		s.Close(false)
		return buf.Copy(rr, buf.Discard)
	}
	return err
}

func (w *ServerWorker) handleStatusKeep(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !meta.Option.Has(OptionData) {
		return nil
	}
	w.countRx(meta)

	s, found := w.sessionManager.Load().Get(meta.SessionID)
	if !found {
		// Notify remote peer to close this session. Routed via out() so
		// the End is counted and retained like any other frame (and hits
		// the live carrier after a swap, not the dead original pipe).
		// Async: the gate may be suspended (half-open park) and must
		// never stall the reader loop.
		sid := meta.SessionID
		out := w.out()
		go func() {
			closingWriter := NewResponseWriter(sid, out, protocol.TransferTypeStream)
			closingWriter.Close()
		}()

		return buf.Copy(NewStreamReader(reader), buf.Discard)
	}

	rr := s.NewReader(reader, &meta.Target)
	err := buf.Copy(rr, s.output)

	if err != nil && buf.IsWriteError(err) {
		errors.LogInfoInner(context.Background(), err, "failed to write to downstream writer. closing session ", s.ID)
		s.Close(false)
		return buf.Copy(rr, buf.Discard)
	}

	return err
}

func (w *ServerWorker) handleStatusEnd(meta *FrameMetadata, reader *buf.BufferedReader) error {
	w.countRx(meta)
	if s, found := w.sessionManager.Load().Get(meta.SessionID); found {
		s.Close(false)
	}
	if meta.Option.Has(OptionData) {
		return buf.Copy(NewStreamReader(reader), buf.Discard)
	}
	return nil
}

func (w *ServerWorker) handleFrame(ctx context.Context, reader *buf.BufferedReader) error {
	var meta FrameMetadata
	err := meta.Unmarshal(reader, session.IsReverseMuxFromContext(ctx))
	if err != nil {
		return errors.New("failed to read metadata").Base(err)
	}

	switch meta.SessionStatus {
	case SessionStatusKeepAlive:
		err = w.handleStatusKeepAlive(&meta, reader)
	case SessionStatusEnd:
		err = w.handleStatusEnd(&meta, reader)
	case SessionStatusNew:
		err = w.handleStatusNew(session.ContextWithIsReverseMux(ctx, false), &meta, reader)
	case SessionStatusKeep:
		err = w.handleStatusKeep(&meta, reader)
	case SessionStatusResume:
		err = w.handleStatusResume(&meta, reader)
	case SessionStatusAck:
		err = w.handleStatusAck(&meta, reader)
	default:
		status := meta.SessionStatus
		return errors.New("unknown status: ", status)
	}

	if err != nil {
		return errors.New("failed to process data").Base(err)
	}
	return nil
}

func (w *ServerWorker) run(ctx context.Context) {
	parked := false
	defer func() {
		if !parked {
			common.Must(w.done.Close())
		}
		// A parked worker's sessions outlive it (adopt or janitor owns
		// them now); anything else dies with the worker.
		if td := w.resumeDone; td != nil && !parked {
			td.close()
		}
	}()

	reader := &buf.BufferedReader{Reader: w.link.Reader}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			err := w.handleFrame(ctx, reader)
			if err != nil {
				if w.parkForResume(ctx) {
					parked = true
					return
				}
				if errors.Cause(err) != io.EOF {
					errors.LogInfoInner(ctx, err, "unexpected EOF")
				}
				return
			}
		}
	}
}

// dispatchLink dispatches downstream with a carrier-detached context once
// a v2 token is known: values (inbound, routing flags, access log) are
// preserved via WithoutCancel, but carrier death no longer cancels parked
// sessions' outbounds. The cancel is stored on the Session and invoked on
// Close, so closed sessions never leak. v1 path keeps the carrier ctx.
func (w *ServerWorker) dispatchLink(ctx context.Context, dest net.Destination) (*transport.Link, context.CancelFunc, error) {
	dctx := ctx
	var cancel context.CancelFunc
	if w.resumeHasToken {
		dctx, cancel = context.WithCancel(context.WithoutCancel(ctx))
	}
	link, err := w.dispatcher.Dispatch(dctx, dest)
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return nil, nil, err
	}
	return link, cancel, nil
}

// parkForResume suspends instead of closing when a v2 token is known:
// the session table is handed to the handover map for 10s so a fresh
// carrier Resume can adopt it. The manager is swapped so the monitor
// closes nothing; pipes are interrupted (retained bytes replay later).
// The gate is moved onto the token-scoped done first: the parking worker
// may be reaped by its monitor while adopted sessions are still live.
// Returns true when parked.
func (w *ServerWorker) parkForResume(ctx context.Context) bool {
	if !w.resumeHasToken {
		return false
	}
	sm := w.sessionManager.Load()
	if sm.Size() == 0 {
		return false
	}
	user := w.resumeUser
	if user == "" {
		user = w.localUser
	}
	if w.resumeDone == nil {
		w.resumeDone = newTokenDone()
	}
	if g := w.gate.Load(); g != nil {
		g.setSuspended(true)
		g.swapDone(w.resumeDone.wait())
	}
	nSessions := sm.Size()
	hsPut(w.resumeToken, &suspendedWorker{
		manager: sm,
		gate:    w.gate.Load(),
		tx:      w.gateTx(),
		rx:      w.rx.Value(),
		epoch:   w.resumeEpoch,
		user:    user,
		done:    w.resumeDone,
	})
	w.sessionManager.Store(NewSessionManager())
	common.Interrupt(w.link.Writer)
	common.Interrupt(w.link.Reader)
	errors.LogInfo(ctx, "mux resume: parked ", nSessions, " sessions token ", w.tokenString(), " epoch ", w.resumeEpoch, " for rebind")
	return true
}

// gateTx returns transmitted frames (0 when no gate yet).
func (w *ServerWorker) gateTx() uint64 {
	if g := w.gate.Load(); g != nil {
		return g.TxCount()
	}
	return 0
}
