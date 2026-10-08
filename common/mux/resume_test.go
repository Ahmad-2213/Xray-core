package mux_test

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/bitmask"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/mux"
	"github.com/xtls/xray-core/common/net"
)

func TestResumePolicyDefaults(t *testing.T) {
	p := mux.DefaultResumePolicy()
	if !p.Enabled || p.SuspendTimeout != 10*time.Second {
		t.Fatalf("bad defaults: %+v", p)
	}
	if p.MaxStreamBuffer != 256*1024 || p.MaxWorkerBuffer != 4*1024*1024 {
		t.Fatalf("bad caps: %+v", p)
	}
	if got := mux.DisabledPolicy(); got.Enabled {
		t.Fatal("disabled policy must be flag-off")
	}
}

func TestCountedStatus(t *testing.T) {
	if !mux.CountedStatus(mux.SessionStatusNew) || !mux.CountedStatus(mux.SessionStatusKeep) || !mux.CountedStatus(mux.SessionStatusEnd) {
		t.Fatal("New/Keep/End must be counted")
	}
	if mux.CountedStatus(mux.SessionStatusKeepAlive) || mux.CountedStatus(mux.SessionStatusResume) || mux.CountedStatus(mux.SessionStatusAck) {
		t.Fatal("KeepAlive/Resume/Ack must not be counted")
	}
}

func TestResumeRegistry(t *testing.T) {
	mux.RegisterResumePolicy("test-tag-1", mux.DefaultResumePolicy())
	if got := mux.LookupResumePolicy("test-tag-1"); !got.Enabled {
		t.Fatal("registry lookup failed")
	}
	if got := mux.LookupResumePolicy("missing-tag"); got.Enabled {
		t.Fatal("default must stay disabled")
	}
}

func TestResumeFrameRejectsShort(t *testing.T) {
	if _, err := mux.DecodeResumeForTest([]byte{1, 2, 3}); err == nil {
		t.Fatal("short resume payload must be rejected")
	}
	if _, err := mux.DecodeAckForTest([]byte{1}); err == nil {
		t.Fatal("short ack payload must be rejected")
	}
}

func TestResumeRoundTrip(t *testing.T) {
	rp := mux.ResumePayload{Epoch: 1791393943700713701, RxCount: 42}
	for i := range rp.Token {
		rp.Token[i] = byte(i + 1)
	}
	enc := mux.EncodeResumeForTest(rp)
	raw := append([]byte(nil), enc.Bytes()...)
	enc.Release()
	got, err := mux.DecodeResumeForTest(raw)
	if err != nil {
		t.Fatal("round trip decode failed:", err)
	}
	if got != rp {
		t.Fatalf("round trip mismatch: %+v != %+v", got, rp)
	}

	ap := mux.AckPayload{RxCount: 99}
	enca := mux.EncodeAckForTest(ap)
	rawa := append([]byte(nil), enca.Bytes()...)
	enca.Release()
	gota, err := mux.DecodeAckForTest(rawa)
	if err != nil {
		t.Fatal("ack round trip decode failed:", err)
	}
	if gota != ap {
		t.Fatalf("ack round trip mismatch: %+v != %+v", gota, ap)
	}
}

func TestResumeDecodeIgnoresTrailingBytes(t *testing.T) {
	rp := mux.ResumePayload{Epoch: 7, RxCount: 3}
	enc := mux.EncodeResumeForTest(rp)
	raw := append(append([]byte(nil), enc.Bytes()...), 0xde, 0xad, 0xbe, 0xef)
	enc.Release()
	got, err := mux.DecodeResumeForTest(raw)
	if err != nil {
		t.Fatal("trailing bytes must be tolerated:", err)
	}
	if got != rp {
		t.Fatalf("trailing-bytes mismatch: %+v != %+v", got, rp)
	}
}

func FuzzDecodeResume(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1, 2, 3})
	f.Add(make([]byte, 32))
	f.Add(make([]byte, 64))
	f.Fuzz(func(t *testing.T, p []byte) {
		_, err := mux.DecodeResumeForTest(p)
		if len(p) < 32 && err == nil {
			t.Fatal("short resume payload accepted")
		}
		if len(p) >= 32 && err != nil {
			t.Fatal("valid resume payload rejected:", err)
		}
	})
}

func FuzzDecodeAck(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte{1})
	f.Add(make([]byte, 8))
	f.Add(make([]byte, 24))
	f.Fuzz(func(t *testing.T, p []byte) {
		_, err := mux.DecodeAckForTest(p)
		if len(p) < 8 && err == nil {
			t.Fatal("short ack payload accepted")
		}
		if len(p) >= 8 && err != nil {
			t.Fatal("valid ack payload rejected:", err)
		}
	})
}

func TestCountedFrameMatrix(t *testing.T) {
	const (
		tcp = byte(mux.TargetNetworkTCP)
		udp = byte(mux.TargetNetworkUDP)
	)
	cases := []struct {
		name    string
		status  mux.SessionStatus
		opt     bitmask.Byte
		metaLen int
		netByte byte
		hasNet  bool
		want    bool
	}{
		{"New/TCP counted", mux.SessionStatusNew, 0, 8, tcp, true, true},
		{"New/UDP skipped", mux.SessionStatusNew, 0, 8, udp, true, false},
		{"New/no-net skipped", mux.SessionStatusNew, 0, 4, 0, false, false},
		{"Keep/Data counted", mux.SessionStatusKeep, 1, 4, 0, false, true},
		{"Keep/Data/TCP counted", mux.SessionStatusKeep, 1, 8, tcp, true, true},
		{"Keep/Data/UDP skipped", mux.SessionStatusKeep, 1, 8, udp, true, false},
		{"Keep/no-Data skipped", mux.SessionStatusKeep, 0, 4, 0, false, false},
		{"End always counted", mux.SessionStatusEnd, 0, 4, 0, false, true},
		{"KeepAlive never", mux.SessionStatusKeepAlive, 1, 4, 0, false, false},
		{"Resume never", mux.SessionStatusResume, 1, 4, 0, false, false},
		{"Ack never", mux.SessionStatusAck, 1, 4, 0, false, false},
	}
	for _, c := range cases {
		if got := mux.CountedFrameForTest(c.status, c.opt, c.metaLen, c.netByte, c.hasNet); got != c.want {
			t.Errorf("%s: got %v want %v", c.name, got, c.want)
		}
	}
}

func TestParseMetaPrefixEdges(t *testing.T) {
	if _, _, _, _, _, ok := mux.ParseMetaPrefixForTest(nil); ok {
		t.Fatal("empty buffer must fail")
	}
	if _, _, _, _, _, ok := mux.ParseMetaPrefixForTest([]byte{0, 4, 0, 1, 2}); ok {
		t.Fatal("5-byte buffer must fail")
	}
	if _, _, _, _, _, ok := mux.ParseMetaPrefixForTest([]byte{0, 10, 0, 1, 2, 1, 1, 1}); ok {
		t.Fatal("metaLen overrun must fail")
	}
	st, opt, metaLen, _, hasNet, ok := mux.ParseMetaPrefixForTest([]byte{0, 4, 0, 9, byte(mux.SessionStatusNew), 1})
	if !ok || st != mux.SessionStatusNew || opt != mux.OptionData || metaLen != 4 || hasNet {
		t.Fatalf("minimal meta misparsed: %v %v %d %v %v", st, opt, metaLen, hasNet, ok)
	}
	_, _, _, netByte, hasNet, ok := mux.ParseMetaPrefixForTest([]byte{0, 5, 0, 9, byte(mux.SessionStatusNew), 1, byte(mux.TargetNetworkTCP)})
	if !ok || !hasNet || netByte != byte(mux.TargetNetworkTCP) {
		t.Fatalf("network byte misparsed: %v %v %v", netByte, hasNet, ok)
	}
}

func resumeTokenForTest(fill byte) [16]byte {
	var t [16]byte
	for i := range t {
		t[i] = fill + byte(i)
	}
	return t
}

func TestHandoverLifecycle(t *testing.T) {
	mux.HsResetForTest()
	defer mux.HsResetForTest()
	if n := mux.HsLenForTest(); n != 0 {
		t.Fatalf("table not empty after reset: %d", n)
	}

	tok := resumeTokenForTest(0xA0)
	if _, _, _, _, ok := mux.HsTakeForTest(tok); ok {
		t.Fatal("unknown token take must fail")
	}
	mux.HsPutForTest(tok, 10, 7, 5, "u1")
	if n := mux.HsLenForTest(); n != 1 {
		t.Fatalf("put not stored: %d", n)
	}
	if tx, rx, epoch, user, ok := mux.HsPeekForTest(tok); !ok || tx != 10 || rx != 7 || epoch != 5 || user != "u1" {
		t.Fatalf("peek mismatch: %d %d %d %q %v", tx, rx, epoch, user, ok)
	}
	if tx, rx, epoch, user, ok := mux.HsTakeForTest(tok); !ok || tx != 10 || rx != 7 || epoch != 5 || user != "u1" {
		t.Fatalf("take mismatch: %d %d %d %q %v", tx, rx, epoch, user, ok)
	}
	if _, _, _, _, ok := mux.HsTakeForTest(tok); ok {
		t.Fatal("double take must fail (entry consumed)")
	}
	mux.HsPutBackForTest(tok, 10, 7, 5, "u1")
	if _, _, _, _, ok := mux.HsPeekForTest(tok); !ok {
		t.Fatal("put-back must restore the entry")
	}
}

func TestHandoverCapacity(t *testing.T) {
	mux.HsResetForTest()
	defer mux.HsResetForTest()
	var tok [16]byte
	for i := 0; i < 1100; i++ {
		tok[0] = byte(i)
		tok[1] = byte(i >> 8)
		mux.HsPutForTest(tok, uint64(i), 0, 1, "")
	}
	if n := mux.HsLenForTest(); n > 1024 {
		t.Fatalf("table exceeded cap: %d", n)
	}
}

func TestValidateRebindMatrix(t *testing.T) {
	base := mux.ResumePayload{Epoch: 6, RxCount: 10}
	if err := mux.ValidateRebindForTest(10, 5, "a", base, "a"); err != nil {
		t.Fatal("valid rebind rejected:", err)
	}
	if err := mux.ValidateRebindForTest(0, 0, "", mux.ResumePayload{Epoch: 1}, ""); err != nil {
		t.Fatal("empty-user rebind rejected:", err)
	}
	if err := mux.ValidateRebindForTest(10, 5, "a", mux.ResumePayload{Epoch: 5, RxCount: 1}, "a"); err == nil {
		t.Fatal("equal epoch must be stale")
	} else if !strings.Contains(err.Error(), "stale") {
		t.Fatal("wrong error for stale epoch:", err)
	}
	if err := mux.ValidateRebindForTest(10, 5, "a", mux.ResumePayload{Epoch: 4, RxCount: 1}, "a"); err == nil {
		t.Fatal("older epoch must be stale")
	}
	if err := mux.ValidateRebindForTest(10, 5, "a", mux.ResumePayload{Epoch: 6, RxCount: 11}, "a"); err == nil {
		t.Fatal("count beyond sent must be rejected")
	} else if !strings.Contains(err.Error(), "beyond") {
		t.Fatal("wrong error for count beyond sent:", err)
	}
	if err := mux.ValidateRebindForTest(10, 5, "a", base, "b"); err == nil {
		t.Fatal("user mismatch must be rejected")
	} else if !strings.Contains(err.Error(), "mismatch") {
		t.Fatal("wrong error for user mismatch:", err)
	}
}

func TestHalfOpenTrippedMatrix(t *testing.T) {
	if mux.HalfOpenTrippedForTest(0, 10*time.Second, 10*time.Second, 4*time.Second) {
		t.Fatal("no unacked bytes must not trip")
	}
	if !mux.HalfOpenTrippedForTest(100, 5*time.Second, 5*time.Second, 4*time.Second) {
		t.Fatal("stalled acks with old unacked data must trip")
	}
	if mux.HalfOpenTrippedForTest(100, 4*time.Second, 5*time.Second, 4*time.Second) {
		t.Fatal("trip must be strictly past the timeout")
	}
	if mux.HalfOpenTrippedForTest(100, 5*time.Second, 4*time.Second, 4*time.Second) {
		t.Fatal("age must be strictly past the timeout")
	}
	if mux.HalfOpenTrippedForTest(100, 5*time.Second, 0, 4*time.Second) {
		t.Fatal("young unacked data must veto (send after silence)")
	}
	if mux.HalfOpenTrippedForTest(100, 0, 5*time.Second, 4*time.Second) {
		t.Fatal("fresh acks must veto (slow bulk is healthy)")
	}
}

// ---- Phase 3: gate fault injection over a scripted carrier ----

var errTestCarrierDead = errors.New("test carrier dead")

type flakyCarrierForTest struct {
	mu       sync.Mutex
	failLeft int
	frames   [][]byte
}

func (w *flakyCarrierForTest) WriteMultiBuffer(mb buf.MultiBuffer) error {
	var raw []byte
	for _, b := range mb {
		raw = append(raw, b.Bytes()...)
	}
	buf.ReleaseMulti(mb)
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.failLeft > 0 {
		w.failLeft--
		return errTestCarrierDead
	}
	w.frames = append(w.frames, raw)
	return nil
}

func (w *flakyCarrierForTest) recorded() [][]byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([][]byte(nil), w.frames...)
}

func countedFrameBytesForTest(t *testing.T, sid uint16, status mux.SessionStatus, payload string) buf.MultiBuffer {
	t.Helper()
	meta := mux.FrameMetadata{SessionID: sid, SessionStatus: status, Option: mux.OptionData}
	if status == mux.SessionStatusNew {
		meta.Target = net.TCPDestination(net.DomainAddress("example.com"), 80)
	}
	b := buf.New()
	if err := meta.WriteTo(b); err != nil {
		t.Fatal("meta WriteTo failed:", err)
	}
	if payload == "" {
		return buf.MultiBuffer{b}
	}
	p := buf.New()
	if _, err := p.Write([]byte(payload)); err != nil {
		t.Fatal("payload write failed:", err)
	}
	return buf.MultiBuffer{b, p}
}

func TestGateTransientLossRetriedInOrder(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{failLeft: 3}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	for i := 1; i <= 5; i++ {
		mb := countedFrameBytesForTest(t, uint16(i), mux.SessionStatusNew, fmt.Sprintf("msg-%d", i))
		if err := g.WriteMultiBuffer(mb); err != nil {
			t.Fatalf("frame %d not absorbed: %v", i, err)
		}
	}
	if got := g.TxCount(); got != 5 {
		t.Fatalf("TxCount = %d, want 5", got)
	}
	frames := carrier.recorded()
	if len(frames) != 5 {
		t.Fatalf("recorded %d frames, want exactly 5 (no loss, no dup)", len(frames))
	}
	for i, f := range frames {
		if want := fmt.Sprintf("msg-%d", i+1); !bytes.Contains(f, []byte(want)) {
			t.Fatalf("frame %d out of order or corrupt: %q", i, f)
		}
	}
	if g.UnackedBytes() == 0 {
		t.Fatal("retained bytes must stay until acked")
	}
	mux.GateAckForTest(g, 5)
	if g.UnackedBytes() != 0 {
		t.Fatal("full ack must free all retained bytes")
	}
}

func TestGateSuspendBlocksUntilResume(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	mux.GateSuspendForTest(g, true)
	res := make(chan error, 1)
	go func() {
		res <- g.WriteMultiBuffer(countedFrameBytesForTest(t, 1, mux.SessionStatusNew, "held"))
	}()
	select {
	case err := <-res:
		t.Fatalf("write passed while suspended: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	if len(carrier.recorded()) != 0 {
		t.Fatal("nothing may forward while suspended")
	}
	mux.GateSuspendForTest(g, false)
	select {
	case err := <-res:
		if err != nil {
			t.Fatal("write failed after resume:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("write stuck after resume")
	}
	frames := carrier.recorded()
	if len(frames) != 1 || !bytes.Contains(frames[0], []byte("held")) {
		t.Fatalf("resumed write lost or duplicated: %q", frames)
	}
	mux.GateAckForTest(g, 1)
	if g.UnackedBytes() != 0 {
		t.Fatal("ack must free the resumed frame")
	}
}

func TestGateAckFreesAndFlushReplaysSuffix(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	for i := 1; i <= 3; i++ {
		mb := countedFrameBytesForTest(t, uint16(i), mux.SessionStatusKeep, fmt.Sprintf("k%d", i))
		if err := g.WriteMultiBuffer(mb); err != nil {
			t.Fatalf("frame %d not absorbed: %v", i, err)
		}
	}
	before := g.UnackedBytes()
	if before == 0 {
		t.Fatal("nothing retained")
	}
	mux.GateAckForTest(g, 2)
	after := g.UnackedBytes()
	if after == 0 || after >= before {
		t.Fatalf("partial ack must free a prefix: before %d after %d", before, after)
	}
	rebind := &flakyCarrierForTest{}
	mux.GateSwapTargetForTest(g, rebind)
	if err := mux.GateFlushSinceForTest(g, 2); err != nil {
		t.Fatal("flush failed:", err)
	}
	frames := rebind.recorded()
	if len(frames) != 1 || !bytes.Contains(frames[0], []byte("k3")) {
		t.Fatalf("rebind must replay exactly the unacked suffix: %q", frames)
	}
}

func TestGateCapBlocksUntilDone(t *testing.T) {
	policy := mux.DefaultResumePolicy()
	policy.MaxWorkerBuffer = 16
	done := make(chan struct{})
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, policy)
	res := make(chan error, 1)
	go func() {
		mb := countedFrameBytesForTest(t, 1, mux.SessionStatusNew, "this-payload-far-exceeds-sixteen-bytes")
		res <- g.WriteMultiBuffer(mb)
	}()
	select {
	case err := <-res:
		t.Fatalf("cap-blocked write returned early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(done)
	select {
	case err := <-res:
		if err == nil {
			t.Fatal("closed worker must surface an error, not success")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cap-blocked write ignored worker close")
	}
}

func TestGateAckCoalescedWhileSuspended(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	mux.GateSuspendForTest(g, true)
	mux.GateWriteAckForTest(g, 3)
	mux.GateWriteAckForTest(g, 7)
	mux.GateWriteAckForTest(g, 6)
	if err := mux.GateFlushSinceForTest(g, 99); err != nil {
		t.Fatal("flush failed:", err)
	}
	var all []byte
	for _, f := range carrier.recorded() {
		all = append(all, f...)
	}
	if len(all) < 8 {
		t.Fatalf("coalesced ack missing: %d bytes", len(all))
	}
	if got := binary.BigEndian.Uint64(all[len(all)-8:]); got != 7 {
		t.Fatalf("coalesced ack = %d, want latest (7)", got)
	}
}

func TestGateConcurrentWritersExactOnce(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{failLeft: 20}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	const writers = 8
	const perWriter = 25
	var wg sync.WaitGroup
	errs := make(chan error, writers*perWriter)
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				mb := countedFrameBytesForTest(t, uint16(w), mux.SessionStatusNew, fmt.Sprintf("w%d-f%d", w, i))
				if err := g.WriteMultiBuffer(mb); err != nil {
					errs <- err
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal("concurrent write failed:", err)
	}
	if got := g.TxCount(); got != writers*perWriter {
		t.Fatalf("TxCount = %d, want %d", got, writers*perWriter)
	}
	// Ordered sender: every stored frame must hit the wire exactly once,
	// even with concurrent writers and injected carrier failures.
	seen := make(map[string]int)
	for _, f := range carrier.recorded() {
		seen[string(f)]++
	}
	if len(seen) != writers*perWriter {
		t.Fatalf("recorded %d distinct frames, want %d", len(seen), writers*perWriter)
	}
	for payload, n := range seen {
		if n != 1 {
			t.Fatalf("frame %q delivered %d times", payload, n)
		}
	}
	mux.GateAckForTest(g, writers*perWriter)
	if g.UnackedBytes() != 0 {
		t.Fatal("full ack must free everything")
	}
}

func TestGateGarbageFrameIgnored(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	garbage := buf.New()
	if _, err := garbage.Write([]byte{0x01}); err != nil {
		t.Fatal(err)
	}
	// Parse failure must not release-then-forward the caller's buffer:
	// no crash, no retention, and the gate keeps working afterwards.
	if err := g.WriteMultiBuffer(buf.MultiBuffer{garbage}); err != nil {
		t.Fatal("garbage frame must be ignored, not error:", err)
	}
	if g.TxCount() != 0 || g.UnackedBytes() != 0 {
		t.Fatal("garbage must not be counted or retained")
	}
	mb := countedFrameBytesForTest(t, 1, mux.SessionStatusNew, "after-garbage")
	if err := g.WriteMultiBuffer(mb); err != nil {
		t.Fatal("valid frame after garbage failed:", err)
	}
	// Uncounted garbage is forwarded (1) plus the valid frame (1).
	frames := carrier.recorded()
	if g.TxCount() != 1 || len(frames) != 2 || !bytes.Contains(frames[1], []byte("after-garbage")) {
		t.Fatal("gate stuck after garbage frame")
	}
}

func TestGateAckAfterFlushFreesAll(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	for i := 1; i <= 3; i++ {
		mb := countedFrameBytesForTest(t, uint16(i), mux.SessionStatusKeep, fmt.Sprintf("q%d", i))
		if err := g.WriteMultiBuffer(mb); err != nil {
			t.Fatal(err)
		}
	}
	rebind := &flakyCarrierForTest{}
	mux.GateSwapTargetForTest(g, rebind)
	if err := mux.GateFlushSinceForTest(g, 0); err != nil {
		t.Fatal("flush failed:", err)
	}
	if len(rebind.recorded()) != 3 {
		t.Fatalf("flush must replay all 3, got %d", len(rebind.recorded()))
	}
	// The rebind discipline: free what the peer confirms right after
	// flushing, or retention pins memory and the half-open detector
	// re-trips on the fresh carrier.
	mux.GateAckForTest(g, 3)
	if g.UnackedBytes() != 0 {
		t.Fatal("ack-after-flush must free all retained bytes")
	}
}

func TestGateReserveDoesNotStallOthers(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	policy := mux.DefaultResumePolicy()
	policy.MaxWorkerBuffer = 64 * 1024 * 1024
	policy.MaxStreamBuffer = 256
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, policy)
	bulkMB := countedFrameBytesForTest(t, 1, mux.SessionStatusNew, strings.Repeat("B", 1024))
	smallMB := countedFrameBytesForTest(t, 2, mux.SessionStatusNew, "s")
	// Bulk frame exceeds the per-stream window: reserve blocks (no acks).
	bulkDone := make(chan error, 1)
	go func() { bulkDone <- g.WriteMultiBuffer(bulkMB) }()
	select {
	case err := <-bulkDone:
		t.Fatalf("bulk write returned early: %v", err)
	case <-time.After(100 * time.Millisecond):
	}
	// A small stream on another sid must still complete promptly: the
	// cap-block waits without holding the ordered sender.
	smallDone := make(chan error, 1)
	go func() { smallDone <- g.WriteMultiBuffer(smallMB) }()
	select {
	case err := <-smallDone:
		if err != nil {
			t.Fatal("small stream stalled behind bulk reserve:", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("small stream head-of-line blocked by bulk cap-block")
	}
}

func TestGateUnackedAge(t *testing.T) {
	done := make(chan struct{})
	defer close(done)
	carrier := &flakyCarrierForTest{}
	g := mux.NewGateForTest(carrier, done, mux.DefaultResumePolicy())
	if age := mux.GateUnackedAgeForTest(g); age != 0 {
		t.Fatalf("empty gate age = %v, want 0", age)
	}
	mb := countedFrameBytesForTest(t, 1, mux.SessionStatusNew, "aged")
	if err := g.WriteMultiBuffer(mb); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if age := mux.GateUnackedAgeForTest(g); age < 20*time.Millisecond {
		t.Fatalf("retained frame age = %v, want >= 20ms", age)
	}
	mux.GateAckForTest(g, 1)
	if age := mux.GateUnackedAgeForTest(g); age != 0 {
		t.Fatalf("acked gate age = %v, want 0", age)
	}
}

func TestV2FailConsecutive(t *testing.T) {
	key := "failkey-test.invalid"
	mux.ClearV2FailsForTest(key)
	if n := mux.RecordV2FailForTest(key); n != 1 {
		t.Fatalf("first fail = %d, want 1", n)
	}
	if n := mux.RecordV2FailForTest(key); n != 2 {
		t.Fatalf("second fail = %d, want 2", n)
	}
	mux.ClearV2FailsForTest(key)
	if n := mux.RecordV2FailForTest(key); n != 1 {
		t.Fatalf("fail after clear = %d, want 1", n)
	}
	mux.ClearV2FailsForTest(key)
}

// dribbleReaderForTest scripts partial reads: chunks go out piece-wise,
// then either clean EOF (full frame delivered in dribbles) or a cut.
type dribbleReaderForTest struct {
	chunks [][]byte
	cut    bool
	calls  int
}

func (r *dribbleReaderForTest) ReadMultiBuffer() (buf.MultiBuffer, error) {
	if r.calls >= len(r.chunks) {
		if r.cut {
			return nil, errors.New("carrier cut")
		}
		return nil, io.EOF
	}
	b := buf.FromBytes(r.chunks[r.calls])
	r.calls++
	return buf.MultiBuffer{b}, nil
}

func collectBytes(mb buf.MultiBuffer) []byte {
	var out []byte
	for _, b := range mb {
		out = append(out, b.Bytes()...)
	}
	buf.ReleaseMulti(mb)
	return out
}

func TestReadFullFrameDribbles(t *testing.T) {
	mb, err := mux.ReadFullFrameForTest(&dribbleReaderForTest{chunks: [][]byte{[]byte("he"), []byte("llo"), []byte("!")}})
	if err != nil {
		t.Fatal("dribbled frame must assemble:", err)
	}
	if got := collectBytes(mb); string(got) != "hello!" {
		t.Fatalf("assembled %q, want %q", got, "hello!")
	}
}

func TestReadFullFrameCutDiscardsPrefix(t *testing.T) {
	// Carrier dies mid-payload after delivering a prefix: nothing may
	// come out (the sender replays the whole frame; a delivered prefix
	// would duplicate it).
	_, err := mux.ReadFullFrameForTest(&dribbleReaderForTest{chunks: [][]byte{[]byte("he")}, cut: true})
	if err == nil {
		t.Fatal("cut frame must error, not deliver a prefix")
	}
}

func TestReadFullFrameEmpty(t *testing.T) {
	mb, err := mux.ReadFullFrameForTest(&dribbleReaderForTest{})
	if err != nil {
		t.Fatal("empty chunk EOF must not error:", err)
	}
	if got := collectBytes(mb); len(got) != 0 {
		t.Fatalf("empty frame gave %d bytes", len(got))
	}
}

func TestReadFullFrameOversizeFails(t *testing.T) {
	// A corrupt/hostile size prefix must fail, never accumulate
	// unboundedly — and never park as transport loss.
	big := make([]byte, 9000)
	for i := range big {
		big[i] = byte(i)
	}
	if _, err := mux.ReadFullFrameForTest(&dribbleReaderForTest{chunks: [][]byte{big}}); err == nil {
		t.Fatal("oversize frame must fail")
	}
}

// ---- Phase 3: v1/v2 interop matrix ----

func TestV2StatusesDoNotAliasV1(t *testing.T) {
	if byte(mux.SessionStatusResume) != 0x05 || byte(mux.SessionStatusAck) != 0x06 {
		t.Fatal("v2 statuses must stay 0x05/0x06")
	}
	for _, s := range []mux.SessionStatus{mux.SessionStatusNew, mux.SessionStatusKeep, mux.SessionStatusEnd, mux.SessionStatusKeepAlive} {
		if s == mux.SessionStatusResume || s == mux.SessionStatusAck {
			t.Fatalf("v1 status %02x aliases a v2 status", byte(s))
		}
	}
	// v1 parsers read v2 metas structurally (fail-fast happens at dispatch,
	// never at parse): sid/status/option survive a WriteTo round trip.
	for _, status := range []mux.SessionStatus{mux.SessionStatusResume, mux.SessionStatusAck} {
		meta := mux.FrameMetadata{SessionID: 9, SessionStatus: status, Option: mux.OptionData}
		b := buf.New()
		if err := meta.WriteTo(b); err != nil {
			t.Fatal("v2 meta WriteTo failed:", err)
		}
		raw := append([]byte(nil), b.Bytes()...)
		b.Release()
		nb := buf.New()
		if _, err := nb.Write(raw[2:]); err != nil {
			t.Fatal("buffer fill failed:", err)
		}
		var back mux.FrameMetadata
		if err := back.UnmarshalFromBuffer(nb, false); err != nil {
			t.Fatal("v1 parser must read v2 meta structurally:", err)
		}
		nb.Release()
		if back.SessionID != 9 || back.SessionStatus != status || back.Option != mux.OptionData {
			t.Fatalf("v2 meta corrupted: %+v", back)
		}
	}
}

func TestV2BanTTL(t *testing.T) {
	host := "v2ban-test.invalid"
	if mux.IsV2BannedForTest(host) {
		t.Fatal("fresh host must not be banned")
	}
	mux.BanV2ForTest(host, 50*time.Millisecond)
	if !mux.IsV2BannedForTest(host) {
		t.Fatal("ban must apply")
	}
	if mux.IsV2BannedForTest("other.invalid") {
		t.Fatal("ban must be per-host")
	}
	time.Sleep(60 * time.Millisecond)
	if mux.IsV2BannedForTest(host) {
		t.Fatal("ban must expire")
	}
}

func TestV2BanKeyScopesPerTag(t *testing.T) {
	addr := net.DomainAddress("v1.mux.cool")
	k1 := mux.V2BanKeyForTest("ws-out", addr)
	k2 := mux.V2BanKeyForTest("tg-out", addr)
	if k1 == k2 {
		t.Fatal("ban key must differ per outbound tag")
	}
	mux.BanV2ForTest(k1, time.Minute)
	if !mux.IsV2BannedForTest(k1) {
		t.Fatal("ban must apply")
	}
	if mux.IsV2BannedForTest(k2) {
		t.Fatal("one outbound's ban must not affect another")
	}
}

func TestFallbackV1Matrix(t *testing.T) {
	if !mux.ShouldFallbackV1ForTest(0, 0, time.Second) {
		t.Fatal("instant-death handshake must fall back to v1")
	}
	if mux.ShouldFallbackV1ForTest(0, 0, 6*time.Second) {
		t.Fatal("old carrier must not fall back")
	}
	if mux.ShouldFallbackV1ForTest(1, 0, time.Second) {
		t.Fatal("frames sent means v2 works")
	}
	if mux.ShouldFallbackV1ForTest(0, 5, time.Second) {
		t.Fatal("frames received means v2 works")
	}
}
