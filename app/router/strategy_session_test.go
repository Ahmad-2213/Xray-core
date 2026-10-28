package router

import (
	gonet "net"
	"testing"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/features/routing"
)

type fakeRouteCtx struct {
	routing.Context
	src    string
	domain string
}

func (c fakeRouteCtx) GetSourceIPs() []gonet.IP {
	if c.src == "" {
		return nil
	}
	return []gonet.IP{gonet.ParseIP(c.src)}
}

func (c fakeRouteCtx) GetTargetDomain() string { return c.domain }

func (c fakeRouteCtx) GetTargetIPs() []gonet.IP { return nil }

func newTestSession(t *testing.T, settings string) *SessionRoundRobinStrategy {
	t.Helper()
	s, err := NewSessionRoundRobinStrategy("", serial.ToTypedMessage(wrapperspb.String(settings)))
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func pick(t *testing.T, s *SessionRoundRobinStrategy, ctx fakeRouteCtx, tags []string) string {
	t.Helper()
	return s.PickOutboundWithContext(ctx, tags)
}

func TestSessionRoundRobinAffinity(t *testing.T) {
	s := newTestSession(t, `{}`)
	tags := []string{"A", "B", "C"}
	steps := []struct {
		src, domain, want string
	}{
		{"1.1.1.1", "www.a.com", "A"},
		{"1.1.1.1", "cdn.a.com", "A"},
		{"1.1.1.1", "b.org", "B"},
		{"1.1.1.1", "c.net", "C"},
		{"1.1.1.1", "d.io", "A"},
		{"1.1.1.1", "www.a.com", "A"},
		{"2.2.2.2", "www.a.com", "B"},
	}
	for i, st := range steps {
		if got := pick(t, s, fakeRouteCtx{src: st.src, domain: st.domain}, tags); got != st.want {
			t.Fatalf("step %d (%s,%s): want %s got %s", i, st.src, st.domain, st.want, got)
		}
	}
}

func TestSessionRoundRobinTTLExpiry(t *testing.T) {
	s := newTestSession(t, `{}`)
	tags := []string{"A", "B", "C"}
	if got := pick(t, s, fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}, tags); got != "A" {
		t.Fatalf("want A got %s", got)
	}
	if got := pick(t, s, fakeRouteCtx{src: "1.1.1.1", domain: "b.com"}, tags); got != "B" {
		t.Fatalf("want B got %s", got)
	}
	s.mu.Lock()
	for _, e := range s.sessions {
		e.expires = time.Now().Add(-time.Second)
	}
	s.mu.Unlock()
	if got := pick(t, s, fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}, tags); got != "C" {
		t.Fatalf("want C after expiry got %s", got)
	}
}

func TestSessionRoundRobinMissingTag(t *testing.T) {
	s := newTestSession(t, `{}`)
	full := []string{"A", "B", "C"}
	first := pick(t, s, fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}, full)
	if first != "A" {
		t.Fatalf("want A got %s", first)
	}
	narrow := []string{"B", "C"}
	less := pick(t, s, fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}, narrow)
	if less == "A" {
		t.Fatal("dead tag A must not be returned")
	}
	if again := pick(t, s, fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}, narrow); again != less {
		t.Fatalf("repeat must return same value: %s then %s", less, again)
	}
}

func TestSessionRoundRobinTraffic(t *testing.T) {
	s := newTestSession(t, `{"key":"none","balance":"traffic","weights":{"C":4}}`)
	used := map[string]int64{"A": 3000000, "B": 2000000, "C": 6000000}
	s.usedFn = func(tag string) (int64, bool) {
		return used[tag], true
	}
	tags := []string{"A", "B", "C"}
	if got := pick(t, s, fakeRouteCtx{}, tags); got != "C" {
		t.Fatalf("want C (lowest score 1.5e6) got %s", got)
	}
	used["C"] = 20000000
	if got := pick(t, s, fakeRouteCtx{}, tags); got != "B" {
		t.Fatalf("want B got %s", got)
	}
}

func TestSessionSettings(t *testing.T) {
	c, err := ParseSessionSettings("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Key != "domain" || c.Balance != "count" || c.Traffic != "both" || c.TTLSec != 300 {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	bad := []string{`{"key":"x"}`, `{"balance":"y"}`, `{"traffic":"z"}`, `{"weights":{"A":0}}`, `{"nope":1}`}
	for _, raw := range bad {
		if _, err := ParseSessionSettings(raw); err == nil {
			t.Fatalf("expected error for %s", raw)
		}
	}
}

func TestSessionStallEjection(t *testing.T) {
	s := newTestSession(t, `{"key":"none","balance":"traffic"}`)
	used := map[string]int64{"A": 5000000, "B": 5000000, "C": 0}
	s.usedFn = func(tag string) (int64, bool) {
		return used[tag], true
	}
	tags := []string{"A", "B", "C"}
	ctx := fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}
	for i := 0; i < 3; i++ {
		if got := s.PickOutboundWithContext(ctx, tags); got != "C" {
			t.Fatalf("pick %d: want C got %s", i, got)
		}
	}
	s.mu.Lock()
	s.stall["C"].since = time.Now().Add(-30 * time.Second)
	s.mu.Unlock()
	if got := s.PickOutboundWithContext(ctx, tags); got == "C" {
		t.Fatal("stalled C must be ejected")
	}
	s.mu.Lock()
	s.stall["C"].until = time.Now().Add(-time.Second)
	s.mu.Unlock()
	if got := s.PickOutboundWithContext(ctx, tags); got != "C" {
		t.Fatalf("want C after hold expiry got %s", got)
	}
}

func TestSessionStallUnpinsDomain(t *testing.T) {
	s := newTestSession(t, `{"key":"domain","balance":"traffic"}`)
	used := map[string]int64{"A": 5000000, "B": 5000000, "C": 0}
	s.usedFn = func(tag string) (int64, bool) {
		return used[tag], true
	}
	tags := []string{"A", "B", "C"}
	ctx := fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}
	for i := 0; i < 3; i++ {
		if got := s.PickOutboundWithContext(ctx, tags); got != "C" {
			t.Fatalf("pick %d: want C got %s", i, got)
		}
	}
	s.mu.Lock()
	s.stall["C"].since = time.Now().Add(-30 * time.Second)
	s.mu.Unlock()
	if got := s.PickOutboundWithContext(ctx, tags); got == "C" {
		t.Fatal("held C must not stay pinned to the session")
	}
}

func TestSessionStallProgressClears(t *testing.T) {
	s := newTestSession(t, `{"key":"none","balance":"traffic"}`)
	used := map[string]int64{"A": 5000000, "B": 5000000, "C": 0}
	s.usedFn = func(tag string) (int64, bool) {
		return used[tag], true
	}
	tags := []string{"A", "B", "C"}
	for i := 0; i < 6; i++ {
		used["C"] += 1000
		s.PickOutboundWithContext(fakeRouteCtx{src: "1.1.1.1", domain: "a.com"}, tags)
	}
	s.mu.Lock()
	held := !s.stall["C"].until.IsZero()
	s.mu.Unlock()
	if held {
		t.Fatal("progressing outbound must not be held")
	}
}
