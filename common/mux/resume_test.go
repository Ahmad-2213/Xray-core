package mux_test

import (
	"strings"
	"testing"
	"time"

	"github.com/xtls/xray-core/common/bitmask"
	"github.com/xtls/xray-core/common/mux"
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
	if mux.HalfOpenTrippedForTest(0, 10*time.Second, 4*time.Second) {
		t.Fatal("no unacked bytes must not trip")
	}
	if !mux.HalfOpenTrippedForTest(100, 5*time.Second, 4*time.Second) {
		t.Fatal("stalled acks must trip")
	}
	if mux.HalfOpenTrippedForTest(100, 4*time.Second, 4*time.Second) {
		t.Fatal("trip must be strictly past the timeout")
	}
	if mux.HalfOpenTrippedForTest(100, 0, 4*time.Second) {
		t.Fatal("fresh acks must not trip")
	}
}
