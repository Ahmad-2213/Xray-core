package router

import (
	"context"
	"encoding/json"
	"math"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/publicsuffix"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/common/serial"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/routing"
	"github.com/xtls/xray-core/features/stats"
)

const (
	sessionHerdBytes     = 256 * 1024
	sessionDefaultTTLSec = 300
	sessionDefaultMax    = 100000
)

const (
	sessionStallAssigns = 3
	sessionStallAfter   = 20 * time.Second
	sessionStallHold    = 120 * time.Second
)

type SessionSettings struct {
	Key        string             `json:"key"`
	TTLSec     int                `json:"ttlSec"`
	Balance    string             `json:"balance"`
	Traffic    string             `json:"traffic"`
	Weights    map[string]float64 `json:"weights"`
	MaxEntries int                `json:"maxEntries"`
}

func ParseSessionSettings(raw string) (*SessionSettings, error) {
	c := &SessionSettings{}
	if strings.TrimSpace(raw) != "" {
		dec := json.NewDecoder(strings.NewReader(raw))
		dec.DisallowUnknownFields()
		if err := dec.Decode(c); err != nil {
			return nil, errors.New("invalid sessionRoundRobin settings").Base(err)
		}
	}
	c.Key = strings.ToLower(c.Key)
	switch c.Key {
	case "":
		c.Key = "domain"
	case "none", "source", "domain", "host":
	default:
		return nil, errors.New("unknown sessionRoundRobin key: ", c.Key)
	}
	c.Balance = strings.ToLower(c.Balance)
	switch c.Balance {
	case "", "count":
		c.Balance = "count"
	case "traffic":
	default:
		return nil, errors.New("unknown sessionRoundRobin balance: ", c.Balance)
	}
	c.Traffic = strings.ToLower(c.Traffic)
	switch c.Traffic {
	case "":
		c.Traffic = "both"
	case "both", "up", "down":
	default:
		return nil, errors.New("unknown sessionRoundRobin traffic: ", c.Traffic)
	}
	if c.TTLSec <= 0 {
		c.TTLSec = sessionDefaultTTLSec
	}
	if c.MaxEntries <= 0 {
		c.MaxEntries = sessionDefaultMax
	}
	for tag, w := range c.Weights {
		if w <= 0 {
			return nil, errors.New("sessionRoundRobin weight must be positive: ", tag)
		}
	}
	return c, nil
}

type sessionEntry struct {
	tag     string
	expires time.Time
}

type stallState struct {
	since    time.Time
	assigned int
	until    time.Time
	lastUsed int64
}

type statsHolder struct{ m stats.Manager }

type SessionRoundRobinStrategy struct {
	rr  *RoundRobinStrategy
	cfg *SessionSettings
	ttl time.Duration

	mu        sync.Mutex
	rot       int
	sessions  map[string]*sessionEntry
	lastPurge time.Time
	virtual   map[string]float64
	lastReal  map[string]int64

	statsMgr atomic.Pointer[statsHolder]
	warned   atomic.Bool
	usedFn   func(tag string) (int64, bool)
	stall    map[string]*stallState
}

func NewSessionRoundRobinStrategy(fallbackTag string, ts *serial.TypedMessage) (*SessionRoundRobinStrategy, error) {
	raw := ""
	if ts != nil {
		i, err := ts.GetInstance()
		if err != nil {
			return nil, err
		}
		sv, ok := i.(*wrapperspb.StringValue)
		if !ok {
			return nil, errors.New("not a StringValue")
		}
		raw = sv.Value
	}
	cfg, err := ParseSessionSettings(raw)
	if err != nil {
		return nil, err
	}
	s := &SessionRoundRobinStrategy{
		rr:       &RoundRobinStrategy{FallbackTag: fallbackTag},
		cfg:      cfg,
		ttl:      time.Duration(cfg.TTLSec) * time.Second,
		sessions: make(map[string]*sessionEntry),
		virtual:  make(map[string]float64),
		lastReal: make(map[string]int64),
		stall:    make(map[string]*stallState),
	}
	s.usedFn = s.usedFromStats
	return s, nil
}

func (s *SessionRoundRobinStrategy) InjectContext(ctx context.Context) {
	s.rr.InjectContext(ctx)
	if s.cfg.Balance == "traffic" {
		_ = core.OptionalFeatures(ctx, func(m stats.Manager) {
			s.statsMgr.Store(&statsHolder{m: m})
		})
	}
}

func (s *SessionRoundRobinStrategy) GetPrincipleTarget(tags []string) []string {
	return tags
}

func (s *SessionRoundRobinStrategy) PickOutbound(tags []string) string {
	alive := s.rr.aliveTags(tags)
	if len(alive) == 0 {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	s.observeLocked(alive, now)
	alive = s.withoutHeldLocked(alive, now)
	tag := s.pickLocked(alive)
	s.noteAssignLocked(tag, now)
	return tag
}

func (s *SessionRoundRobinStrategy) PickOutboundWithContext(ctx routing.Context, tags []string) string {
	alive := s.rr.aliveTags(tags)
	if len(alive) == 0 {
		return ""
	}
	key := s.sessionKey(ctx)
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observeLocked(alive, now)
	alive = s.withoutHeldLocked(alive, now)
	if key == "" {
		tag := s.pickLocked(alive)
		s.noteAssignLocked(tag, now)
		return tag
	}
	s.purgeLocked(now)
	if e, ok := s.sessions[key]; ok && e.expires.After(now) && containsTag(alive, e.tag) {
		e.expires = now.Add(s.ttl)
		s.noteAssignLocked(e.tag, now)
		return e.tag
	}
	tag := s.pickLocked(alive)
	s.noteAssignLocked(tag, now)
	s.sessions[key] = &sessionEntry{tag: tag, expires: now.Add(s.ttl)}
	return tag
}

func containsTag(tags []string, tag string) bool {
	for _, t := range tags {
		if t == tag {
			return true
		}
	}
	return false
}

func (s *SessionRoundRobinStrategy) purgeLocked(now time.Time) {
	if len(s.sessions) < s.cfg.MaxEntries && now.Sub(s.lastPurge) < time.Minute {
		return
	}
	s.lastPurge = now
	for k, e := range s.sessions {
		if !e.expires.After(now) {
			delete(s.sessions, k)
		}
	}
	if len(s.sessions) >= s.cfg.MaxEntries {
		s.sessions = make(map[string]*sessionEntry)
	}
}

func (s *SessionRoundRobinStrategy) sessionKey(ctx routing.Context) string {
	if s.cfg.Key == "none" {
		return ""
	}
	src := ""
	if ips := ctx.GetSourceIPs(); len(ips) > 0 {
		src = ips[0].String()
	}
	if s.cfg.Key == "source" {
		return src
	}
	host := sessionTargetHost(ctx)
	if src == "" && host == "" {
		return ""
	}
	if s.cfg.Key == "domain" {
		host = sessionBaseDomain(host)
	}
	return src + "|" + host
}

func sessionTargetHost(ctx routing.Context) string {
	if d := ctx.GetTargetDomain(); d != "" {
		return strings.ToLower(d)
	}
	if ips := ctx.GetTargetIPs(); len(ips) > 0 {
		return ips[0].String()
	}
	return ""
}

func sessionBaseDomain(host string) string {
	if host == "" {
		return host
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return host
	}
	if d, err := publicsuffix.EffectiveTLDPlusOne(host); err == nil {
		return d
	}
	return host
}

func (s *SessionRoundRobinStrategy) weight(tag string) float64 {
	if w, ok := s.cfg.Weights[tag]; ok && w > 0 {
		return w
	}
	return 1
}

func (s *SessionRoundRobinStrategy) usedFromStats(tag string) (int64, bool) {
	h := s.statsMgr.Load()
	if h == nil || h.m == nil {
		return 0, false
	}
	var total int64
	found := false
	if s.cfg.Traffic != "down" {
		if c := h.m.GetCounter("outbound>>>" + tag + ">>>traffic>>>uplink"); c != nil {
			total += c.Value()
			found = true
		}
	}
	if s.cfg.Traffic != "up" {
		if c := h.m.GetCounter("outbound>>>" + tag + ">>>traffic>>>downlink"); c != nil {
			total += c.Value()
			found = true
		}
	}
	return total, found
}

func (s *SessionRoundRobinStrategy) pickLocked(tags []string) string {
	n := len(tags)
	start := s.rot % n
	s.rot = start + 1
	if s.cfg.Balance != "traffic" {
		return tags[start]
	}
	best := tags[start]
	bestScore := math.MaxFloat64
	seen := false
	for i := 0; i < n; i++ {
		tag := tags[(start+i)%n]
		used, ok := s.usedFn(tag)
		if ok {
			seen = true
		}
		if last := s.lastReal[tag]; used > last {
			s.virtual[tag] -= float64(used - last)
			if s.virtual[tag] < 0 {
				s.virtual[tag] = 0
			}
		}
		s.lastReal[tag] = used
		score := (float64(used) + s.virtual[tag]) / s.weight(tag)
		if score < bestScore {
			best, bestScore = tag, score
		}
	}
	if !seen && s.warned.CompareAndSwap(false, true) {
		errors.LogWarning(context.Background(), "sessionRoundRobin traffic balance: no outbound stats counters found; enable stats and policy.system.statsOutboundUplink/statsOutboundDownlink, and give outbounds tags")
	}
	s.virtual[best] += sessionHerdBytes
	return best
}

func (s *SessionRoundRobinStrategy) stallOf(tag string) *stallState {
	st := s.stall[tag]
	if st == nil {
		st = &stallState{}
		s.stall[tag] = st
	}
	return st
}

func (s *SessionRoundRobinStrategy) observeLocked(tags []string, now time.Time) {
	if s.cfg.Balance != "traffic" {
		return
	}
	for _, tag := range tags {
		used, ok := s.usedFn(tag)
		if !ok {
			continue
		}
		st := s.stallOf(tag)
		if used > st.lastUsed {
			st.lastUsed = used
			st.assigned = 0
			st.until = time.Time{}
			continue
		}
		if st.until.IsZero() && st.assigned >= sessionStallAssigns && now.Sub(st.since) >= sessionStallAfter {
			st.until = now.Add(sessionStallHold)
			errors.LogWarning(context.Background(), "sessionRoundRobin: outbound ", tag, " carried no traffic for ", st.assigned, " new connections, skipping it for ", sessionStallHold)
		} else if !st.until.IsZero() && now.After(st.until) {
			st.until = time.Time{}
			st.assigned = 0
		}
	}
}

func (s *SessionRoundRobinStrategy) withoutHeldLocked(tags []string, now time.Time) []string {
	out := make([]string, 0, len(tags))
	for _, tag := range tags {
		if st := s.stall[tag]; st != nil && !st.until.IsZero() && now.Before(st.until) {
			continue
		}
		out = append(out, tag)
	}
	if len(out) == 0 {
		return tags
	}
	return out
}

func (s *SessionRoundRobinStrategy) noteAssignLocked(tag string, now time.Time) {
	if s.cfg.Balance != "traffic" {
		return
	}
	st := s.stallOf(tag)
	if st.assigned == 0 {
		st.since = now
	}
	st.assigned++
}
