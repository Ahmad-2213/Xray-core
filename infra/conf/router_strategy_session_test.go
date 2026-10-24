package conf_test

import (
	"encoding/json"
	"testing"

	"github.com/xtls/xray-core/app/router"
	. "github.com/xtls/xray-core/infra/conf"
)

func buildSessionRule(t *testing.T, raw string) (*router.BalancingRule, error) {
	t.Helper()
	var c BalancingRule
	if err := json.Unmarshal([]byte(raw), &c); err != nil {
		t.Fatal(err)
	}
	return c.Build()
}

func TestBalancingRuleSessionMode(t *testing.T) {
	r, err := buildSessionRule(t, `{"tag":"b","selector":["p-"],"strategy":{"type":"roundRobin","mode":"session"}}`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Strategy != "sessionroundrobin" {
		t.Fatalf("strategy must be sessionroundrobin, got %q", r.Strategy)
	}

	r, err = buildSessionRule(t, `{"tag":"b","selector":["p-"],"strategy":{"type":"sessionRoundRobin","settings":{"key":"source","balance":"traffic"}}}`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Strategy != "sessionroundrobin" {
		t.Fatalf("strategy must be sessionroundrobin, got %q", r.Strategy)
	}
	if r.StrategySettings == nil {
		t.Fatal("expected non-nil StrategySettings")
	}

	r, err = buildSessionRule(t, `{"tag":"b","selector":["p-"],"strategy":{"type":"roundRobin"}}`)
	if err != nil {
		t.Fatal(err)
	}
	if r.Strategy != "roundrobin" {
		t.Fatalf("strategy must be roundrobin, got %q", r.Strategy)
	}

	if _, err = buildSessionRule(t, `{"tag":"b","selector":["p-"],"strategy":{"type":"random","mode":"session"}}`); err == nil {
		t.Fatal("expected error for mode session on random")
	}

	if _, err = buildSessionRule(t, `{"tag":"b","selector":["p-"],"strategy":{"type":"roundRobin","mode":"bogus"}}`); err == nil {
		t.Fatal("expected error for unknown mode")
	}

	if _, err = buildSessionRule(t, `{"tag":"b","selector":["p-"],"strategy":{"type":"sessionRoundRobin","settings":{"key":"bad"}}}`); err == nil {
		t.Fatal("expected error for bad session key")
	}
}
