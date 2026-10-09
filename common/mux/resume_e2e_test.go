package mux_test

// Live client↔server resume harness: a real ClientWorker (via
// DialingWorkerFactory, the production path) talks to real ServerWorkers
// over a killable bridged carrier. Kills exercise the full death →
// suspend → redial → adopt → replay cycle; assertions are end-to-end
// (same app socket survives, byte-exact echo), so they fail if any link
// in the chain regresses.

import (
	"bytes"
	"context"
	"errors"
	stdnet "net"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/net"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/common/session"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/transport"
	"github.com/xtls/xray-core/transport/internet"
	"github.com/xtls/xray-core/transport/internet/stat"
	"github.com/xtls/xray-core/transport/pipe"
)

// echoDispatcher returns links that echo everything back. The echo dies
// with ctx, modeling freedom: pre-detachCtx code cancels the ctx on
// carrier death, so an echo that stops after a park proves the A4
// regression (and one that survives proves the fix).
type echoDispatcher struct{}

func (echoDispatcher) Type() interface{} { return routing.DispatcherType() }
func (echoDispatcher) Start() error      { return nil }
func (echoDispatcher) Close() error      { return nil }

func (echoDispatcher) Dispatch(ctx context.Context, dest net.Destination) (*transport.Link, error) {
	upR, upW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	downR, downW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	go func() {
		_ = buf.Copy(upR, downW)
	}()
	go func() {
		<-ctx.Done()
		upR.Interrupt()
		_ = upW.Close()
		downR.Interrupt()
		_ = downW.Close()
	}()
	return &transport.Link{Reader: downR, Writer: upW}, nil
}

func (echoDispatcher) DispatchLink(ctx context.Context, dest net.Destination, link *transport.Link) error {
	return errors.New("echoDispatcher: DispatchLink unused")
}

type stubDialer struct{}

func (stubDialer) Dial(ctx context.Context, dest net.Destination) (stat.Connection, error) {
	return nil, errors.New("stubDialer: no real dial in test")
}

// stubDialerOK models a successful transport dial (TCP+TLS to the server):
// the handshake that follows may still die instantly (old server).
type stubDialerOK struct{ stubDialer }

func (stubDialerOK) Dial(ctx context.Context, dest net.Destination) (stat.Connection, error) {
	c1, c2 := stdnet.Pipe()
	_ = c2.Close()
	return c1, nil
}

// flapDialer fails the next n dials, then succeeds: a transport flap.
// Unlike post-connect deaths, dial failures must suspend (not close),
// so sessions survive short outages.
type flapDialer struct {
	mu       sync.Mutex
	failLeft int
	stubDialerOK
}

func (d *flapDialer) Dial(ctx context.Context, dest net.Destination) (stat.Connection, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.failLeft > 0 {
		d.failLeft--
		return nil, errors.New("flap: dial failed")
	}
	return d.stubDialerOK.Dial(ctx, dest)
}

func (d *flapDialer) arm(n int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.failLeft = n
}

func (stubDialer) DestIpAddress() net.IP { return nil }

func (stubDialer) SetOutboundGateway(ctx context.Context, ob *session.Outbound) {}

// carrierHarness bridges the client carrier to a fresh ServerWorker per
// Process call (the production inbound path in miniature). The bridge is
// killable: killing breaks reads and writes on both ends at once, which
// is exactly the busy-death N1 scenario.
type carrierHarness struct {
	mu       sync.Mutex
	disp     routing.Dispatcher
	failLeft int
	kill     func()
	servers  []*mux.ServerWorker
}

func (h *carrierHarness) Process(ctx context.Context, link *transport.Link, d internet.Dialer) error {
	h.mu.Lock()
	if h.failLeft > 0 {
		h.failLeft--
		h.mu.Unlock()
		return errors.New("injected dial failure")
	}
	h.mu.Unlock()
	// Model the transport dial like a real outbound: it determines
	// whether a later instant death counts toward the v2 ban (dial
	// failures during a flap must not).
	conn, err := d.Dial(ctx, net.TCPDestination(net.DomainAddress("v1.mux.cool"), 9527))
	if err != nil {
		return err
	}
	defer conn.Close()
	h.mu.Lock()
	upR, upW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	downR, downW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	srv, err := mux.NewServerWorker(ctx, h.disp, &transport.Link{Reader: upR, Writer: downW})
	if err != nil {
		h.mu.Unlock()
		return err
	}
	h.servers = append(h.servers, srv)
	done := make(chan struct{})
	var once sync.Once
	stop := func() {
		once.Do(func() {
			close(done)
			upR.Interrupt()
			_ = upW.Close()
			downR.Interrupt()
			_ = downW.Close()
		})
	}
	h.kill = stop
	h.mu.Unlock()
	go func() {
		_ = fragmentPump(link.Reader, upW)
		stop()
	}()
	go func() {
		_ = fragmentPump(downR, link.Writer)
		stop()
	}()
	select {
	case <-ctx.Done():
		stop()
		return ctx.Err()
	case <-done:
		return errors.New("carrier killed")
	}
}

// fragmentPump copies src to dst shattering every buffer into 1-3 byte
// pieces, modeling carriers that split frames across reads (WS/TLS/TCP
// all do). A bridge preserving write boundaries always hands the reader
// whole frames, so byte-exact tests could never catch reassembly bugs.
func fragmentPump(src buf.Reader, dst buf.Writer) error {
	for {
		mb, err := src.ReadMultiBuffer()
		if err != nil {
			buf.ReleaseMulti(mb)
			return err
		}
		for _, b := range mb {
			raw := b.Bytes()
			for len(raw) > 0 {
				n := 1 + int(raw[0])%3
				if n > len(raw) {
					n = len(raw)
				}
				cp := append([]byte(nil), raw[:n]...)
				if werr := dst.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(cp)}); werr != nil {
					buf.ReleaseMulti(mb)
					return werr
				}
				raw = raw[n:]
			}
		}
		buf.ReleaseMulti(mb)
	}
}

func (h *carrierHarness) killCurrent() {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.kill != nil {
		h.kill()
	}
}

// armFailures makes the next n Process calls fail. Armed mid-test so
// failures hit redials (with a park waiting) rather than the initial
// dial (after which nothing exists to adopt).
func (h *carrierHarness) armFailures(n int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.failLeft = n
}

func (h *carrierHarness) closeAll() {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, s := range h.servers {
		_ = s.Close()
	}
}

type e2eFixture struct {
	worker *mux.ClientWorker
	appUpW buf.Writer
	appDnR *pipe.Reader
	cancel context.CancelFunc
}

func newE2EFixture(t *testing.T, h *carrierHarness, dialer internet.Dialer, policy mux.ResumePolicy) *e2eFixture {
	t.Helper()
	factory := &mux.DialingWorkerFactory{
		Proxy:       h,
		Dialer:      dialer,
		Strategy:    mux.ClientStrategy{MaxConcurrency: 8},
		Resume:      policy,
		OutboundTag: "e2e-test",
	}
	worker, err := factory.Create()
	if err != nil {
		t.Fatal("factory.Create failed:", err)
	}
	upR, upW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	downR, downW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{
		{Target: net.TCPDestination(net.DomainAddress("example.com"), 80)},
	})
	if !worker.Dispatch(ctx, &transport.Link{Reader: upR, Writer: downW}) {
		t.Fatal("worker.Dispatch rejected the session")
	}
	return &e2eFixture{worker: worker, appUpW: upW, appDnR: downR}
}

func testResumePolicy() mux.ResumePolicy {
	p := mux.DefaultResumePolicy()
	// Generous episode for slow CI: functional behavior is identical,
	// only the give-up deadline moves.
	p.SuspendTimeout = 30 * time.Second
	p.RedialDelays = []time.Duration{50 * time.Millisecond, 100 * time.Millisecond, 200 * time.Millisecond}
	return p
}

func writeChunk(t *testing.T, w buf.Writer, payload []byte) {
	t.Helper()
	if err := w.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)}); err != nil {
		t.Fatal("app write failed:", err)
	}
}

func readExactly(t *testing.T, r *pipe.Reader, n int, timeout time.Duration) []byte {
	t.Helper()
	var acc []byte
	deadline := time.Now().Add(timeout)
	for len(acc) < n {
		mb, err := r.ReadMultiBufferTimeout(time.Until(deadline))
		if err != nil {
			t.Fatalf("app read failed after %d/%d bytes: %v", len(acc), n, err)
		}
		for _, b := range mb {
			acc = append(acc, b.Bytes()...)
		}
		buf.ReleaseMulti(mb)
	}
	return acc
}

func TestResumeE2ERebindSurvives(t *testing.T) {
	h := &carrierHarness{disp: echoDispatcher{}}
	fx := newE2EFixture(t, h, stubDialerOK{}, testResumePolicy())
	defer fx.worker.Close()
	defer h.closeAll()
	defer mux.HsResetForTest()

	ping1 := []byte("ping-before-kill")
	writeChunk(t, fx.appUpW, ping1)
	if got := readExactly(t, fx.appDnR, len(ping1), testTimeout(10*time.Second)); !bytes.Equal(got, ping1) {
		t.Fatalf("pre-kill echo mismatch: %q", got)
	}

	// Busy death: reads and writes fail together (the N1 scenario).
	h.killCurrent()

	ping2 := []byte("ping-after-rebind-same-socket")
	writeChunk(t, fx.appUpW, ping2)
	if got := readExactly(t, fx.appDnR, len(ping2), testTimeout(20*time.Second)); !bytes.Equal(got, ping2) {
		t.Fatalf("post-rebind echo mismatch: %q", got)
	}
}

func TestResumeE2EFailedRedialsThenSuccess(t *testing.T) {
	h := &carrierHarness{disp: echoDispatcher{}}
	fx := newE2EFixture(t, h, stubDialerOK{}, testResumePolicy())
	defer fx.worker.Close()
	defer h.closeAll()
	defer mux.HsResetForTest()

	writeChunk(t, fx.appUpW, []byte("warmup"))
	_ = readExactly(t, fx.appDnR, len("warmup"), testTimeout(10*time.Second))

	// Fail the next two redials only: a park is waiting, so the third
	// redial must still adopt and resume.
	h.armFailures(2)
	h.killCurrent()
	ping := []byte("ping-after-two-failed-redials")
	writeChunk(t, fx.appUpW, ping)
	if got := readExactly(t, fx.appDnR, len(ping), testTimeout(25*time.Second)); !bytes.Equal(got, ping) {
		t.Fatalf("echo after failed redials mismatch: %q", got)
	}
}

func TestResumeE2EMidPayloadByteExact(t *testing.T) {
	h := &carrierHarness{disp: echoDispatcher{}}
	fx := newE2EFixture(t, h, stubDialerOK{}, testResumePolicy())
	defer fx.worker.Close()
	defer h.closeAll()
	defer mux.HsResetForTest()

	const chunks = 64
	const chunkSize = 16 * 1024
	var sent []byte
	for i := 0; i < chunks; i++ {
		c := bytes.Repeat([]byte{byte(i)}, chunkSize)
		sent = append(sent, c...)
	}
	total := len(sent)

	// Throttled drain (5ms per read, ≤64KB per pipe read): draining 1MB
	// takes ≥80ms on any machine, so killing at 50ms deterministically
	// lands mid-transfer with data in flight. The reader drains for the
	// whole test — stalling it would seize the pipeline (and correctly
	// trip the half-open watchdog).
	var echoed []byte
	var echoMu sync.Mutex
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			echoMu.Lock()
			n := len(echoed)
			echoMu.Unlock()
			if n >= total {
				return
			}
			mb, err := fx.appDnR.ReadMultiBufferTimeout(testTimeout(15 * time.Second))
			if err != nil {
				return
			}
			time.Sleep(5 * time.Millisecond)
			for _, b := range mb {
				echoMu.Lock()
				echoed = append(echoed, b.Bytes()...)
				echoMu.Unlock()
			}
			buf.ReleaseMulti(mb)
		}
	}()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; i < chunks; i++ {
			if err := fx.appUpW.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(sent[i*chunkSize : (i+1)*chunkSize])}); err != nil {
				return
			}
		}
	}()
	// Kill when the receiver has real bytes (deterministic mid-flight
	// cut with data in flight), not after a fixed sleep: under loaded
	// or instrumented runtimes 50ms may land before the first byte.
	deadline := time.Now().Add(testTimeout(15 * time.Second))
	for {
		echoMu.Lock()
		n := len(echoed)
		echoMu.Unlock()
		if n >= 256*1024 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("receiver stalled before kill threshold (%d/%d)", n, total)
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.killCurrent()
	echoMu.Lock()
	atKill := len(echoed)
	echoMu.Unlock()
	joinDone := make(chan struct{})
	go func() {
		<-writerDone
		<-readerDone
		close(joinDone)
	}()
	select {
	case <-joinDone:
	case <-time.After(testTimeout(60 * time.Second)):
		t.Fatal("mid-payload flow stalled (writer or drain stuck)")
	}
	echoMu.Lock()
	final := echoed
	echoMu.Unlock()
	if atKill >= total {
		t.Fatalf("kill landed after full echo (%d/%d): cut was not mid-flight", atKill, total)
	}
	if !bytes.Equal(final, sent) {
		off := -1
		for i := range final {
			if i >= len(sent) || final[i] != sent[i] {
				off = i
				break
			}
		}
		t.Fatalf("byte-exact mismatch: got %d bytes, want %d, first diff at %d", len(final), total, off)
	}
}

// TestResumeE2EDelayedDeliveryByteExact kills while a fully-read frame
// waits on slow delivery, then drains with a silence window. Pre-seal
// code snapshots k-1 and replays the delivered frame (duplication, so
// extra bytes arrive); sealed code admits before the delay, so the
// snapshot covers it and the rebind skips it. Single small payload: the
// only bytes that may ever arrive are the echo itself, so any surplus
// byte is proof of duplication.
func TestResumeE2EDelayedDeliveryByteExact(t *testing.T) {
	mux.DeliverDelayForTest(1500 * time.Millisecond)
	defer mux.DeliverDelayForTest(0)
	h := &carrierHarness{disp: echoDispatcher{}}
	fx := newE2EFixture(t, h, stubDialerOK{}, testResumePolicy())
	defer fx.worker.Close()
	defer h.closeAll()
	defer mux.HsResetForTest()

	payload := bytes.Repeat([]byte{0xCD}, 8*1024)
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- fx.appUpW.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)})
	}()
	// Kill exactly while a frame is blocked inside delayed delivery (not
	// after a fixed sleep, which races frame arrival and flakes).
	deadline := time.Now().Add(testTimeout(15 * time.Second))
	for mux.DeliverBlockedForTest() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no frame entered delayed delivery before kill window")
		}
		time.Sleep(10 * time.Millisecond)
	}
	h.killCurrent()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal("app write failed:", err)
		}
	case <-time.After(testTimeout(60 * time.Second)):
		t.Fatal("app write stalled")
	}
	if got := readExactly(t, fx.appDnR, len(payload), testTimeout(60*time.Second)); !bytes.Equal(got, payload) {
		t.Fatalf("byte-exact mismatch: got %d bytes, want %d", len(got), len(payload))
	}
	// Silence window: nothing else may ever arrive. A replayed duplicate
	// lands here within ~2s (redial cadence); data means duplication.
	mb, err := fx.appDnR.ReadMultiBufferTimeout(5 * time.Second)
	if err == nil {
		n := 0
		for _, b := range mb {
			n += len(b.Bytes())
		}
		buf.ReleaseMulti(mb)
		t.Fatalf("surplus %d bytes after exact echo: duplicated replay", n)
	}
}

func TestResumeE2EBulkCompletes(t *testing.T) {
	h := &carrierHarness{disp: echoDispatcher{}}
	fx := newE2EFixture(t, h, stubDialerOK{}, testResumePolicy())
	defer fx.worker.Close()
	defer h.closeAll()
	defer mux.HsResetForTest()

	// 512KB exceeds the 256KB per-stream window: completing it proves
	// cap-block plus ack cycles keep flowing (fragmentation makes every
	// byte expensive, so this stays small on purpose).
	const total = 512 * 1024
	big := bytes.Repeat([]byte{0xAB}, total)
	done := make(chan error, 1)
	go func() {
		done <- fx.appUpW.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(big)})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("bulk write failed:", err)
		}
	case <-time.After(testTimeout(60 * time.Second)):
		t.Fatal("bulk write stalled (window/ack liveness)")
	}
	if got := readExactly(t, fx.appDnR, total, testTimeout(60*time.Second)); !bytes.Equal(got, big) {
		t.Fatal("bulk echo mismatch")
	}
}

func TestResumeTruncatedFrameParks(t *testing.T) {
	downR, downW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	upR, upW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	worker, err := mux.NewClientWorkerWithResume(
		transport.Link{Reader: downR, Writer: upW},
		mux.ClientStrategy{MaxConcurrency: 8},
		mux.DefaultResumePolicy(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	defer mux.HsResetForTest()

	appUpR, _ := pipe.New(pipe.WithSizeLimit(64 * 1024))
	_, appDnW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{
		{Target: net.TCPDestination(net.DomainAddress("example.com"), 80)},
	})
	if !worker.Dispatch(ctx, &transport.Link{Reader: appUpR, Writer: appDnW}) {
		t.Fatal("dispatch rejected")
	}

	// Truncated Keep for the live session: meta + size prefix claiming
	// 100 bytes, only 10 delivered, then the carrier dies. Pre-fix this
	// killed the worker (handler error); it must park (stay alive).
	meta := mux.FrameMetadata{SessionID: 1, SessionStatus: mux.SessionStatusKeep, Option: mux.OptionData}
	b := buf.New()
	if err := meta.WriteTo(b); err != nil {
		t.Fatal(err)
	}
	pay := buf.New()
	if _, err := serial.WriteUint16(pay, 100); err != nil {
		t.Fatal(err)
	}
	if _, err := pay.Write([]byte("0123456789")); err != nil {
		t.Fatal(err)
	}
	if err := downW.WriteMultiBuffer(buf.MultiBuffer{b, pay}); err != nil {
		t.Fatal(err)
	}
	_ = downW.Close()
	upR.Interrupt()
	time.Sleep(300 * time.Millisecond)
	if worker.Closed() {
		t.Fatal("truncated frame killed the worker; must park instead")
	}
}

func TestResumeE2ENewPayloadCutSurvives(t *testing.T) {
	// Hold the server inside New handling so the kill lands
	// deterministically inside the first-payload window: the replayed
	// New must land on the idempotent branch, not collide.
	mux.NewPayloadDelayForTest(300 * time.Millisecond)
	defer mux.NewPayloadDelayForTest(0)
	h := &carrierHarness{disp: echoDispatcher{}}
	fx := newE2EFixture(t, h, stubDialerOK{}, testResumePolicy())
	defer fx.worker.Close()
	defer h.closeAll()
	defer mux.HsResetForTest()

	payload := []byte("new-session-first-payload")
	writeDone := make(chan error, 1)
	go func() {
		writeDone <- fx.appUpW.WriteMultiBuffer(buf.MultiBuffer{buf.FromBytes(payload)})
	}()
	time.Sleep(150 * time.Millisecond)
	h.killCurrent()
	select {
	case err := <-writeDone:
		if err != nil {
			t.Fatal("app write failed:", err)
		}
	case <-time.After(testTimeout(30 * time.Second)):
		t.Fatal("app write stalled")
	}
	if got := readExactly(t, fx.appDnR, len(payload), testTimeout(25*time.Second)); !bytes.Equal(got, payload) {
		t.Fatalf("new-cut echo mismatch: %q", got)
	}
}

// oldServer mimics a pre-fork server: the first Resume frame fails fast
// with an unknown status and zero dials. The client must fall back
// (close promptly) instead of suspending for the whole episode.
type oldServer struct{}

func (oldServer) Process(ctx context.Context, link *transport.Link, d internet.Dialer) error {
	conn, err := d.Dial(ctx, net.TCPDestination(net.DomainAddress("v1.mux.cool"), 9527))
	if err != nil {
		return err
	}
	defer conn.Close()
	mb, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		return err
	}
	defer buf.ReleaseMulti(mb)
	if len(mb) == 0 || len(mb[0].Bytes()) < 6 {
		return errors.New("old server: short meta")
	}
	if mb[0].Bytes()[4] == byte(mux.SessionStatusResume) {
		return errors.New("old server: unknown status")
	}
	return errors.New("old server: expected Resume first")
}

func TestResumeE2EDialFlapSurvives(t *testing.T) {
	fd := &flapDialer{}
	h := &carrierHarness{disp: echoDispatcher{}}
	fx := newE2EFixture(t, h, fd, testResumePolicy())
	defer fx.worker.Close()
	defer h.closeAll()
	defer mux.HsResetForTest()

	writeChunk(t, fx.appUpW, []byte("before-flap"))
	if got := readExactly(t, fx.appDnR, len("before-flap"), testTimeout(10*time.Second)); !bytes.Equal(got, []byte("before-flap")) {
		t.Fatalf("pre-flap echo mismatch: %q", got)
	}
	// Flap the next 4 dials mid-episode, then recover: dial failures
	// must suspend (not close), and the session must survive.
	fd.arm(4)
	h.killCurrent()
	ping := []byte("ping-after-flap")
	writeChunk(t, fx.appUpW, ping)
	if got := readExactly(t, fx.appDnR, len(ping), testTimeout(25*time.Second)); !bytes.Equal(got, ping) {
		t.Fatalf("post-flap echo mismatch: %q", got)
	}
	if fx.worker.Closed() {
		t.Fatal("worker died through a survivable flap")
	}
}

func TestResumeE2ENeverConnectedFailsFast(t *testing.T) {
	// The very first dial fails with an app session already waiting: no
	// carrier ever existed, so there is nothing to resume — fail fast
	// like v1 instead of suspending (no dial storm, no 10s hang per new
	// connection during an outage).
	fd := &flapDialer{}
	fd.arm(1000)
	h := &carrierHarness{disp: echoDispatcher{}}
	policy := testResumePolicy()
	factory := &mux.DialingWorkerFactory{
		Proxy:       h,
		Dialer:      fd,
		Strategy:    mux.ClientStrategy{MaxConcurrency: 8},
		Resume:      policy,
		OutboundTag: "neverconnected-test",
	}
	worker, err := factory.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	defer h.closeAll()

	appUpR, _ := pipe.New(pipe.WithSizeLimit(64 * 1024))
	_, appDnW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{
		{Target: net.TCPDestination(net.DomainAddress("example.com"), 80)},
	})
	worker.Dispatch(ctx, &transport.Link{Reader: appUpR, Writer: appDnW})

	deadline := time.Now().Add(5 * time.Second)
	for !worker.Closed() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !worker.Closed() {
		t.Fatal("never-connected worker neither failed fast nor closed")
	}
	if worker.IsSuspended() {
		t.Fatal("never-connected dial failure suspended instead of failing fast")
	}
}

func TestResumeE2EOldServerFallsBackFast(t *testing.T) {
	policy := testResumePolicy()
	factory := &mux.DialingWorkerFactory{
		Proxy:       oldServer{},
		Dialer:      stubDialerOK{},
		Strategy:    mux.ClientStrategy{MaxConcurrency: 8},
		Resume:      policy,
		OutboundTag: "oldserver-test",
	}
	worker, err := factory.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	defer mux.ClearV2FailsForTest(mux.V2BanKeyForTest("oldserver-test", net.DomainAddress("v1.mux.cool")))

	appUpR, _ := pipe.New(pipe.WithSizeLimit(64 * 1024))
	_, appDnW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{
		{Target: net.TCPDestination(net.DomainAddress("example.com"), 80)},
	})
	worker.Dispatch(ctx, &transport.Link{Reader: appUpR, Writer: appDnW})

	// Instant-death handshake with zero traffic: fallback closes the
	// worker instead of parking it for the episode.
	deadline := time.Now().Add(5 * time.Second)
	for !worker.Closed() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !worker.Closed() {
		t.Fatal("old-server rejection neither fell back nor closed")
	}
	if worker.IsSuspended() {
		t.Fatal("instant-death handshake suspended instead of falling back")
	}
}

// slowOldServer models a pre-fork server over a real path: the transport
// dial succeeds, then the rejection takes ~300ms to travel back. App data
// is already queued by then (tx==1), so a tx==0 predicate would suspend
// till timeout; the reply-evidence predicate must still fall back fast.
type slowOldServer struct {
	delay time.Duration
}

func (s slowOldServer) Process(ctx context.Context, link *transport.Link, d internet.Dialer) error {
	conn, err := d.Dial(ctx, net.TCPDestination(net.DomainAddress("old.example"), 443))
	if err != nil {
		return err
	}
	defer conn.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(s.delay):
	}
	mb, err := link.Reader.ReadMultiBuffer()
	if err != nil {
		return err
	}
	defer buf.ReleaseMulti(mb)
	if len(mb) == 0 || len(mb[0].Bytes()) < 6 {
		return errors.New("old server: short meta")
	}
	if mb[0].Bytes()[4] == byte(mux.SessionStatusResume) {
		return errors.New("old server: unknown status")
	}
	return errors.New("old server: expected Resume first")
}

func TestResumeE2ESlowOldServerFallsBack(t *testing.T) {
	policy := testResumePolicy()
	factory := &mux.DialingWorkerFactory{
		Proxy:       slowOldServer{delay: 300 * time.Millisecond},
		Dialer:      stubDialerOK{},
		Strategy:    mux.ClientStrategy{MaxConcurrency: 8},
		Resume:      policy,
		OutboundTag: "slowoldserver-test",
	}
	worker, err := factory.Create()
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Close()
	defer mux.ClearV2FailsForTest(mux.V2BanKeyForTest("slowoldserver-test", net.DomainAddress("v1.mux.cool")))

	appUpR, appUpW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	_, appDnW := pipe.New(pipe.WithSizeLimit(64 * 1024))
	// Queue app data before dispatch: the New frame is stored (tx==1)
	// long before the 300ms rejection lands.
	writeChunk(t, appUpW, []byte("early-app-data"))
	ctx := session.ContextWithOutbounds(context.Background(), []*session.Outbound{
		{Target: net.TCPDestination(net.DomainAddress("example.com"), 80)},
	})
	worker.Dispatch(ctx, &transport.Link{Reader: appUpR, Writer: appDnW})

	deadline := time.Now().Add(5 * time.Second)
	for !worker.Closed() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !worker.Closed() {
		t.Fatal("slow old-server rejection neither fell back nor closed")
	}
	if worker.IsSuspended() {
		t.Fatal("slow rejection suspended instead of falling back")
	}
}
