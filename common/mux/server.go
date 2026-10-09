package mux

import (
	"context"
	"io"
	"sync"
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
	// Only v1.mux.cool is magic: version negotiation is in-band via the
	// first frame (Resume announce), so every protocol keeps its Mux
	// command path. v2.mux.cool is retired (it went out as plain TCP).
	if dest.Address != muxCoolAddress {
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
	if dest.Address != muxCoolAddress {
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
	rx             rxState
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
	// rsMu guards the resume fields below: parkForResume runs on the run
	// loop, the gate error callback, and the half-open watcher, while
	// handleStatusResume runs on the run loop. Without it concurrent
	// parks double-store (killing the park via close-on-overwrite) and
	// race the adopt assignments.
	rsMu sync.Mutex
	// parkOnce ensures exactly one park per worker: concurrent triggers
	// (read error + write error + watchdog on a busy death) must not
	// double-store the same table.
	parkOnce atomic.Bool
	// localUser binds rebinds to the VLESS user of this worker,
	// captured at construction (run/adopt run on one goroutine after).
	localUser string
	ackMu     ackState
	// suppressedEnds counts reactive Ends withheld for unknown sessions
	// once a v2 token is known (see handleStatusKeep).
	suppressedEnds atomic.Uint64
	// quiesced closes when this worker's run loop exits. A park hands it
	// to the handover entry so the adopter can wait out late deliveries
	// from the old carrier before replaying.
	quiesced chan struct{}
}

func NewServerWorker(ctx context.Context, d routing.Dispatcher, link *transport.Link) (*ServerWorker, error) {
	worker := &ServerWorker{
		dispatcher: d,
		link:       link,
		done:       done.New(),
		timer:      time.NewTicker(60 * time.Second),
		quiesced:   make(chan struct{}),
	}
	worker.sessionManager.Store(NewSessionManager())
	worker.localUser = boundIdentity(ctx)
	if inbound := session.InboundFromContext(ctx); inbound != nil {
		inbound.CanSpliceCopy = 3
	}
	go worker.run(ctx)
	go worker.monitor()
	return worker, nil
}

// newPayloadDelay injects latency before the server reads a New frame's
// first payload (test-only hook, set via NewPayloadDelayForTest). Lets a
// kill land deterministically inside the New window to prove a cut there
// survives via replay.
var newPayloadDelay atomic.Uint64 // nanoseconds

// boundIdentity binds rebinds to the authenticated user behind this
// worker: email when set, else the account proto string. Empty only for
// anonymous inbounds, which are unbindable by necessity (documented).
func boundIdentity(ctx context.Context) string {
	ib := session.InboundFromContext(ctx)
	if ib == nil || ib.User == nil {
		return ""
	}
	if ib.User.Email != "" {
		return ib.User.Email
	}
	if s, ok := ib.User.Account.ToProto().(interface{ String() string }); ok {
		return s.String()
	}
	return ""
}

func handle(ctx context.Context, s *Session, output buf.Writer) {
	writer := NewResponseWriter(s.ID, output, s.loadTransferType())
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
			XUDP:         x,
		}
		x.Mux.storeTransferType(protocol.TransferTypePacket)
		x.Status = Active
		if !w.sessionManager.Load().Add(x.Mux) {
			x.Mux.Close(false)
			return errors.New("failed to add new session")
		}
		go handle(ctx, x.Mux, w.out())
		return nil
	}

	// A replayed New for an unborn ID is the second half of a cut inside
	// the first payload (kept uncounted and parked): deliver to the
	// existing session instead of colliding. IDs are never reused within
	// a worker, so a New for a born ID is a genuine duplicate. Checked
	// before dispatch so replays never redial downstream. Resume-only:
	// flag-off duplicates follow the upstream overwrite path below.
	if w.resumeHasToken {
		if existing, ok := w.sessionManager.Load().Get(meta.SessionID); ok {
			if !existing.unborn {
				return errors.New("duplicate New for live session")
			}
			rr := existing.NewReader(reader, &meta.Target)
			mb, rerr := readFullFrame(rr)
			if rerr != nil {
				return rerr
			}
			// Admit before delivery (see handleStatusKeep): a sealed
			// replay cut drops here uncounted instead of duplicating
			// past the park snapshot.
			if !w.rx.admit(meta.Target.Network == net.Network_TCP) {
				buf.ReleaseMulti(mb)
				return nil
			}
			werr := existing.output.WriteMultiBuffer(mb)
			existing.unborn = false
			w.maybeSendAck()
			if werr != nil {
				existing.Close(false)
				return buf.Copy(rr, buf.Discard)
			}
			return nil
		}
	}
	link, cancel, err := w.dispatchLink(ctx, meta.Target)
	if err != nil {
		if meta.Option.Has(OptionData) {
			buf.Copy(NewStreamReader(reader), buf.Discard)
		}
		return errors.New("failed to dispatch request.").Base(err)
	}
	s := &Session{
		input:  link.Reader,
		output: link.Writer,
		parent: w.sessionManager.Load(),
		ID:     meta.SessionID,
		cancel: cancel,
		// Only a payload-carrying New on a resume carrier can be cut
		// mid-delivery: a payload-less New is complete at its meta, and
		// flag-off sessions never take the idempotent replay branch, so
		// both stay born from birth.
		unborn: w.resumeHasToken && meta.Option.Has(OptionData),
	}
	s.storeTransferType(protocol.TransferTypeStream)
	if meta.Target.Network == net.Network_UDP {
		s.storeTransferType(protocol.TransferTypePacket)
	}
	if !w.sessionManager.Load().Add(s) {
		s.Close(false)
		return errors.New("failed to add new session")
	}
	go handle(ctx, s, w.out())
	if !meta.Option.Has(OptionData) {
		if w.rx.admit(meta.Target.Network == net.Network_TCP) {
			w.maybeSendAck()
		}
		return nil
	}

	// Read the whole chunk before delivering (see readFullFrame): no
	// partial delivery, no duplication on replay.
	if d := newPayloadDelay.Load(); d > 0 {
		time.Sleep(time.Duration(d))
	}
	rr := s.NewReader(reader, &meta.Target)
	if !w.resumeHasToken {
		// Flag-off: original upstream streaming delivery — unbounded,
		// byte-identical v1 path.
		err = buf.Copy(rr, s.output)
		if err != nil && buf.IsWriteError(err) {
			s.Close(false)
			return buf.Copy(rr, buf.Discard)
		}
		return err
	}
	mb, rerr := readFullFrame(rr)
	if rerr != nil {
		// Never delivered and never counted: the session stays tabled
		// but unborn, so the rebind replay lands on the idempotent
		// branch above instead of colliding. If no rebind comes, the
		// park janitor reaps it.
		return rerr
	}
	// Admit before delivery: a sealed first payload drops here uncounted
	// (staying unborn for the idempotent replay branch) instead of
	// duplicating past the park snapshot.
	if !w.rx.admit(meta.Target.Network == net.Network_TCP) {
		buf.ReleaseMulti(mb)
		return nil
	}
	werr := s.output.WriteMultiBuffer(mb)
	// Carrier delivered fully; a downstream write error is local, so the
	// frame still counts (the sender counted it at store time) — already
	// admitted above.
	s.unborn = false
	w.maybeSendAck()
	if werr != nil {
		s.Close(false)
		return buf.Copy(rr, buf.Discard)
	}
	return nil
}

func (w *ServerWorker) handleStatusKeep(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !meta.Option.Has(OptionData) {
		return nil
	}
	// Count only what was actually delivered (see client
	// handleStatusKeep): a frame cut mid-payload must stay out of rx.
	// Admit-before-delivery: the frame is fully read, so counting now is
	// safe, and a seal (park snapshot) drops it for replay instead of
	// letting it slip past the snapshot and duplicate on rebind.
	count := meta.Target.Network != net.Network_UDP
	s, found := w.sessionManager.Load().Get(meta.SessionID)
	if !found {
		// Resume-safe: do NOT answer an unknown Keep with an End (see
		// client handleStatusKeep). After parkForResume the table is
		// empty by design while sessions wait in the handover map; an
		// End here would be retained/replayed and kill the rebind.
		// Discard + count so the sender still advances past it.
		// Genuinely-unknown sessions now linger on the sender until the
		// app closes them; each suppression is counted and logged.
		if !w.resumeHasToken {
			// v1 parity: notify remote peer to close this session.
			// Routed via out() so it hits the live carrier after a
			// swap, not the dead original pipe. Async: the gate may
			// be suspended (half-open park) and must never stall the
			// reader loop.
			sid := meta.SessionID
			out := w.out()
			go func() {
				closingWriter := NewResponseWriter(sid, out, protocol.TransferTypeStream)
				closingWriter.Close()
			}()
		} else {
			n := w.suppressedEnds.Add(1)
			errors.LogInfo(context.Background(), "mux resume: suppressed reactive End for unknown session ", meta.SessionID, " total ", n)
		}

		if err := buf.Copy(NewStreamReader(reader), buf.Discard); err != nil {
			return err
		}
		if w.rx.admit(count) {
			w.maybeSendAck()
		}
		return nil
	}

	rr := s.NewReader(reader, &meta.Target)
	if !w.resumeHasToken {
		// Flag-off: original upstream streaming delivery — unbounded,
		// byte-identical v1 path.
		err := buf.Copy(rr, s.output)
		if err != nil && buf.IsWriteError(err) {
			errors.LogInfoInner(context.Background(), err, "failed to write to downstream writer. closing session ", s.ID)
			s.Close(false)
			return buf.Copy(rr, buf.Discard)
		}
		if err == nil && count && w.rx.admit(true) {
			w.maybeSendAck()
		}
		return err
	}
	mb, rerr := readFullFrame(rr)
	if rerr != nil {
		return rerr
	}
	// Sealed mid-park: drop uncounted; the peer replays it after rebind.
	if !w.rx.admit(count) {
		buf.ReleaseMulti(mb)
		return nil
	}
	werr := s.output.WriteMultiBuffer(mb)
	w.maybeSendAck()

	if werr != nil {
		errors.LogInfoInner(context.Background(), werr, "failed to write to downstream writer. closing session ", s.ID)
		s.Close(false)
		return buf.Copy(rr, buf.Discard)
	}

	return nil
}

func (w *ServerWorker) handleStatusEnd(meta *FrameMetadata, reader *buf.BufferedReader) error {
	if !w.resumeHasToken {
		// Flag-off: original upstream close-first path.
		if s, found := w.sessionManager.Load().Get(meta.SessionID); found {
			s.Close(false)
		}
		if meta.Option.Has(OptionData) {
			if err := buf.Copy(NewStreamReader(reader), buf.Discard); err != nil {
				return err
			}
		}
		return nil
	}
	// Resume path: consume, then admit. A sealed End belongs to a park
	// in progress: leave it uncounted (and the session open) so the
	// rebind replays it.
	if meta.Option.Has(OptionData) {
		if err := buf.Copy(NewStreamReader(reader), buf.Discard); err != nil {
			return err
		}
	}
	if !w.rx.admit(true) {
		return nil
	}
	if s, found := w.sessionManager.Load().Get(meta.SessionID); found {
		s.Close(false)
	}
	w.maybeSendAck()
	return nil
}

func (w *ServerWorker) handleFrame(ctx context.Context, reader *buf.BufferedReader) error {
	var meta FrameMetadata
	err := meta.Unmarshal(reader, session.IsReverseMuxFromContext(ctx))
	if err != nil {
		return errors.New("failed to read metadata").Base(err)
	}
	return w.handleMeta(ctx, reader, &meta)
}

func (w *ServerWorker) handleMeta(ctx context.Context, reader *buf.BufferedReader, meta *FrameMetadata) error {
	var err error
	switch meta.SessionStatus {
	case SessionStatusKeepAlive:
		err = w.handleStatusKeepAlive(meta, reader)
	case SessionStatusEnd:
		err = w.handleStatusEnd(meta, reader)
	case SessionStatusNew:
		err = w.handleStatusNew(session.ContextWithIsReverseMux(ctx, false), meta, reader)
	case SessionStatusKeep:
		err = w.handleStatusKeep(meta, reader)
	case SessionStatusResume:
		err = w.handleStatusResume(meta, reader)
	case SessionStatusAck:
		err = w.handleStatusAck(meta, reader)
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
		// The run loop is over: no more deliveries from the old
		// carrier. Adopters waiting on quiesced may proceed.
		close(w.quiesced)
		if !parked {
			common.Must(w.done.Close())
		}
		// A parked worker's sessions outlive it (adopt or janitor owns
		// them now); anything else dies with the worker. The parkOnce
		// re-check covers a park that landed after this run's own call
		// returned false: the shared done must survive that too.
		w.rsMu.Lock()
		td := w.resumeDone
		parkedOnce := w.parkOnce.Load()
		token := w.resumeToken
		hasToken := w.resumeHasToken
		w.rsMu.Unlock()
		if td != nil && !parked && !parkedOnce {
			td.close()
		}
		if hasToken {
			// Release the live mapping only if it still points here:
			// a newer carrier adopting the same token must survive.
			liveRegRemove(token, w)
		}
	}()

	reader := &buf.BufferedReader{Reader: w.link.Reader}

	// In-band version sniff: a Resume first frame marks a v2 carrier
	// (announce or rebind); anything else is v1 and flows through the
	// normal switch. Old servers fail the Resume frame fast with zero
	// dials — that fast failure, not a magic hostname, is the v2 signal
	// and the fallback trigger.
	var first FrameMetadata
	ferr := first.Unmarshal(reader, session.IsReverseMuxFromContext(ctx))
	if ferr != nil {
		ferr = errors.New("failed to read metadata").Base(ferr)
	} else if first.SessionStatus == SessionStatusResume && first.Option.Has(OptionData) {
		ferr = w.handleStatusResume(&first, reader)
	} else {
		ferr = w.handleMeta(ctx, reader, &first)
	}
	if ferr != nil {
		if w.parkForResume(ctx) {
			parked = true
			return
		}
		if errors.Cause(ferr) != io.EOF {
			errors.LogInfoInner(ctx, ferr, "unexpected EOF")
		}
		return
	}

	for {
		select {
		case <-ctx.Done():
			// Carrier context canceled: try to park first — detached
			// sessions can still survive on a rebind.
			if w.parkForResume(ctx) {
				parked = true
			}
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
	// Another goroutine already parked this worker (read error + write
	// error + watchdog on a busy death): the park happened, so report
	// parked — especially run(), which must NOT close the shared done.
	// Checked before the Size gate: the winner swaps in an empty manager,
	// so a loser would otherwise exit false on Size()==0.
	if w.parkOnce.Load() {
		return true
	}
	w.rsMu.Lock()
	defer w.rsMu.Unlock()
	if !w.resumeHasToken {
		return false
	}
	sm := w.sessionManager.Load()
	if sm.Size() == 0 {
		return false
	}
	// Exactly one park per worker; re-check under the lock against a
	// park that landed between the Load above and here.
	if !w.parkOnce.CompareAndSwap(false, true) {
		return true
	}
	// Seal BEFORE the manager swap and snapshot the exact delivered set:
	// any frame the old run loop admits after this point drops uncounted
	// (replayed after rebind) instead of slipping past the snapshot and
	// duplicating on replay. Sealing only after the abort checks above,
	// so a refused park never stalls counting.
	snap := w.rx.sealAndSnapshot()
	user := w.resumeUser
	if user == "" {
		user = w.localUser
	}
	// Fresh tokenDone per park: the previous park's done may still be
	// owned by an entry (adopted or expired), which closes it itself.
	w.resumeDone = newTokenDone()
	if g := w.gate.Load(); g != nil {
		g.setSuspended(true)
		g.swapDone(w.resumeDone.wait())
	}
	nSessions := sm.Size()
	hsPut(w.resumeToken, &suspendedWorker{
		manager: sm,
		gate:    w.gate.Load(),
		tx:      w.gateTx(),
		rx:      snap,
		epoch:   w.resumeEpoch,
		user:    user,
		done:    w.resumeDone,
		// Closed by this worker's run loop defer when it exits; the
		// adopter waits on it so late deliveries can't race the replay.
		quiesced: w.quiesced,
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
