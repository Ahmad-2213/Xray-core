package router

import (
	"context"
	sync "sync"

	"github.com/xtls/xray-core/app/observatory"
	"github.com/xtls/xray-core/common"
	"github.com/xtls/xray-core/common/errors"
	"github.com/xtls/xray-core/core"
	"github.com/xtls/xray-core/features/extension"
	"github.com/xtls/xray-core/features/outbound"
	"github.com/xtls/xray-core/features/routing"
)

type BalancingStrategy interface {
	PickOutbound([]string) string
}

type BalancingPrincipleTarget interface {
	GetPrincipleTarget([]string) []string
}

type ContextBalancingStrategy interface {
	PickOutboundWithContext(ctx routing.Context, tags []string) string
}

type RoundRobinStrategy struct {
	FallbackTag string

	ctx         context.Context
	observatory extension.Observatory
	mu          sync.Mutex
	index       int
}

func (s *RoundRobinStrategy) InjectContext(ctx context.Context) {
	s.ctx = ctx
	if len(s.FallbackTag) > 0 {
		common.Must(core.RequireFeatures(s.ctx, func(observatory extension.Observatory) error {
			s.observatory = observatory
			return nil
		}))
	}
}

func (s *RoundRobinStrategy) GetPrincipleTarget(strings []string) []string {
	return strings
}

func (s *RoundRobinStrategy) aliveTags(tags []string) []string {
	if s.observatory == nil {
		return tags
	}
	observeReport, err := s.observatory.GetObservation(s.ctx)
	if err != nil {
		return tags
	}
	result, ok := observeReport.(*observatory.ObservationResult)
	if !ok {
		return tags
	}
	statusMap := make(map[string]*observatory.OutboundStatus, len(result.Status))
	for _, st := range result.Status {
		statusMap[st.OutboundTag] = st
	}
	alive := make([]string, 0, len(tags))
	for _, candidate := range tags {
		if st, found := statusMap[candidate]; found && !st.Alive {
			continue
		}
		alive = append(alive, candidate)
	}
	return alive
}

func (s *RoundRobinStrategy) PickOutbound(tags []string) string {
	tags = s.aliveTags(tags)
	n := len(tags)
	if n == 0 {
		return ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tag := tags[s.index%n]
	s.index = (s.index + 1) % n
	return tag
}

type Balancer struct {
	selectors   []string
	strategy    BalancingStrategy
	ohm         outbound.Manager
	fallbackTag string

	override override
}

func (b *Balancer) PickOutbound() (string, error) {
	return b.PickOutboundWithContext(nil)
}

func (b *Balancer) PickOutboundWithContext(ctx routing.Context) (string, error) {
	candidates, err := b.SelectOutbounds()
	if err != nil {
		if b.fallbackTag != "" {
			errors.LogInfo(context.Background(), "fallback to [", b.fallbackTag, "], due to error: ", err)
			return b.fallbackTag, nil
		}
		return "", err
	}
	var tag string
	if o := b.override.Get(); o != "" {
		tag = o
	} else if cs, ok := b.strategy.(ContextBalancingStrategy); ok && ctx != nil {
		tag = cs.PickOutboundWithContext(ctx, candidates)
	} else {
		tag = b.strategy.PickOutbound(candidates)
	}
	if tag == "" {
		if b.fallbackTag != "" {
			errors.LogInfo(context.Background(), "fallback to [", b.fallbackTag, "], due to empty tag returned")
			return b.fallbackTag, nil
		}
		// will use default handler
		return "", errors.New("balancing strategy returns empty tag")
	}
	return tag, nil
}

func (b *Balancer) InjectContext(ctx context.Context) {
	if contextReceiver, ok := b.strategy.(extension.ContextReceiver); ok {
		contextReceiver.InjectContext(ctx)
	}
}

// SelectOutbounds select outbounds with selectors of the Balancer
func (b *Balancer) SelectOutbounds() ([]string, error) {
	hs, ok := b.ohm.(outbound.HandlerSelector)
	if !ok {
		return nil, errors.New("outbound.Manager is not a HandlerSelector")
	}
	tags := hs.Select(b.selectors)
	return tags, nil
}

// GetPrincipleTarget implements routing.BalancerPrincipleTarget
func (r *Router) GetPrincipleTarget(tag string) ([]string, error) {
	if b, ok := (*r.balancers.Load())[tag]; ok {
		if s, ok := b.strategy.(BalancingPrincipleTarget); ok {
			candidates, err := b.SelectOutbounds()
			if err != nil {
				return nil, errors.New("unable to select outbounds").Base(err)
			}
			return s.GetPrincipleTarget(candidates), nil
		}
		return nil, errors.New("unsupported GetPrincipleTarget")
	}
	return nil, errors.New("cannot find tag")
}

// SetOverrideTarget implements routing.BalancerOverrider
func (r *Router) SetOverrideTarget(tag, target string) error {
	if b, ok := (*r.balancers.Load())[tag]; ok {
		b.override.Put(target)
		return nil
	}
	return errors.New("cannot find tag")
}

// GetOverrideTarget implements routing.BalancerOverrider
func (r *Router) GetOverrideTarget(tag string) (string, error) {
	if b, ok := (*r.balancers.Load())[tag]; ok {
		return b.override.Get(), nil
	}
	return "", errors.New("cannot find tag")
}
