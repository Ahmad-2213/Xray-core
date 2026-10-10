package mux

// Test hooks for package mux_test, in the Go export_test.go idiom: this
// file compiles into the test binary only, keeping production files free
// of test scaffolding (except the newPayloadDelay variable itself, which
// lives in server.go next to its reader so production builds compile;
// only its setter is here).

import (
	"time"

	"github.com/xtls/xray-core/common/bitmask"
	"github.com/xtls/xray-core/common/buf"
	"github.com/xtls/xray-core/common/net"
)

// NewPayloadDelayForTest sets the injected New-payload delay.
func NewPayloadDelayForTest(d time.Duration) {
	newPayloadDelay.Store(uint64(d.Nanoseconds()))
}

// DeliverDelayForTest sets the injected client downlink delivery delay.
func DeliverDelayForTest(d time.Duration) {
	deliverDelay.Store(uint64(d.Nanoseconds()))
	deliverInDelay.Store(0)
	deliverCycles.Store(0)
}

// DeliverBlockedForTest reports how many frames are currently inside the
// delivery delay.
func DeliverBlockedForTest() int64 {
	return deliverInDelay.Load()
}

// DeliverCyclesForTest counts entered delivery delays (sleep cycles).
func DeliverCyclesForTest() int64 {
	return deliverCycles.Load()
}

func DecodeResumeForTest(p []byte) (ResumePayload, error) {
	return decodeResume(p)
}
func DecodeAckForTest(p []byte) (AckPayload, error) { return decodeAck(p) }
func EncodeResumeForTest(p ResumePayload) *buf.Buffer {
	return encodeResume(p)
}
func EncodeAckForTest(p AckPayload) *buf.Buffer { return encodeAck(p) }
func ValidateRebindForTest(entryTx, entryEpoch uint64, entryUser string, rp ResumePayload, user string) error {
	return validateRebind(entryTx, entryEpoch, entryUser, rp, user)
}

func HalfOpenTrippedForTest(unacked int64, idle, age, timeout time.Duration) bool {
	return halfOpenTripped(unacked, idle, age, timeout)
}

func CountedFrameForTest(status SessionStatus, opt bitmask.Byte, metaLen int, netByte byte, hasNet bool) bool {
	return countedFrame(status, opt, metaLen, netByte, hasNet)
}

func ParseMetaPrefixForTest(b []byte) (SessionStatus, bitmask.Byte, int, byte, bool, bool) {
	return parseMetaPrefix(b)
}

func NewGateForTest(target buf.Writer, done <-chan struct{}, policy ResumePolicy) *carrierGate {
	return newCarrierGate(target, done, policy)
}

func GateSuspendForTest(g *carrierGate, suspended bool) { g.setSuspended(suspended) }

func GateSwapTargetForTest(g *carrierGate, target buf.Writer) { g.swapTarget(target) }

func GateAckForTest(g *carrierGate, n uint64) { g.ack(n) }

func GateVerifyRxHashForTest(g *carrierGate, rx, h uint64) (bool, bool) {
	ok, drift, _ := g.verifyRxHash(rx, h)
	return ok, drift
}

func GateHashStateForTest(g *carrierGate) (tipSeq, tip, baseSeq, base uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.hashTipSeq, g.hashTip, g.hashBaseSeq, g.hashBase
}

func GateFlushSinceForTest(g *carrierGate, peerRx uint64) error { return g.flushSince(peerRx) }

func GateWriteAckForTest(g *carrierGate, rx, h uint64) { g.writeAck(rx, h) }

func GateUnackedAgeForTest(g *carrierGate) time.Duration { return g.unackedAge() }

// Test hooks for the handover table. Tokens are the only shared key;
// entries stay unexported. Reset first: the table is process-global.
func HsResetForTest() {
	hsMu.Lock()
	defer hsMu.Unlock()
	hsEntries = make(map[[16]byte]*suspendedWorker)
	traceReset()
}

// RxStateForTest exposes admit/seal/generation for the deterministic
// guard unit test below (white-box, no timing involved).
type RxStateForTest = rxState

func NewRxStateForTest() *RxStateForTest {
	return &RxStateForTest{}
}

func RxAdmitForTest(r *RxStateForTest, gen uint64, counted bool, sid uint16, mb buf.MultiBuffer) (uint64, bool) {
	return r.admit(gen, counted, sid, mb)
}

func RxSealForTest(r *RxStateForTest) {
	r.seal()
}

func RxNewGenerationForTest(r *RxStateForTest) uint64 {
	return r.newGeneration()
}

func WorkerReadLoopForTest(w *ClientWorker, r *buf.BufferedReader, gen uint64) bool {
	return w.readLoop(r, gen)
}

func WorkerCurrentGenForTest(w *ClientWorker) uint64 {
	return w.currentGen()
}

func WorkerRxValueForTest(w *ClientWorker) uint64 {
	return w.rx.Value()
}

func HsLenForTest() int {
	hsMu.Lock()
	defer hsMu.Unlock()
	return len(hsEntries)
}

func HsPutForTest(token [16]byte, tx, rx, epoch uint64, user string) {
	hsPut(token, &suspendedWorker{tx: tx, rx: rx, epoch: epoch, user: user})
}

func HsPeekForTest(token [16]byte) (tx, rx, epoch uint64, user string, ok bool) {
	e, ok := hsPeek(token)
	if !ok {
		return 0, 0, 0, "", false
	}
	return e.tx, e.rx, e.epoch, e.user, true
}

func HsTakeForTest(token [16]byte) (tx, rx, epoch uint64, user string, ok bool) {
	e, ok := hsTake(token)
	if !ok {
		return 0, 0, 0, "", false
	}
	return e.tx, e.rx, e.epoch, e.user, true
}

func HsPutBackForTest(token [16]byte, tx, rx, epoch uint64, user string) {
	hsPutBack(token, &suspendedWorker{tx: tx, rx: rx, epoch: epoch, user: user})
}

func BanV2ForTest(host string, ttl time.Duration) { banV2(host, ttl) }

func IsV2BannedForTest(host string) bool { return isV2Banned(host) }

func RecordV2FailForTest(key string) int { return recordV2Fail(key) }

func ClearV2FailsForTest(key string) { clearV2Fails(key) }

func V2BanKeyForTest(tag string, target net.Address) string { return v2BanKey(tag, target) }

func ShouldFallbackV1ForTest(connected, sawReply bool, age time.Duration) bool {
	return shouldFallbackV1(connected, sawReply, age)
}
