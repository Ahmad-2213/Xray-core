package conf_test

import (
	"encoding/json"
	"strings"
	"testing"

	. "github.com/xtls/xray-core/infra/conf"
)

func TestMuxResumeConfigDefaults(t *testing.T) {
	c := &MuxResumeConfig{Enabled: true}
	p := c.ToPolicy()
	if !p.Enabled {
		t.Fatal("enabled policy must be enabled")
	}
	if p.SuspendTimeout.Seconds() != 10 {
		t.Fatalf("default suspend timeout, got %v", p.SuspendTimeout)
	}
	if p.MaxStreamBuffer != 256*1024 || p.MaxWorkerBuffer != 4*1024*1024 {
		t.Fatal("default buffer caps")
	}
	if d := (&MuxResumeConfig{}).ToPolicy(); d.Enabled {
		t.Fatal("disabled config must stay disabled")
	}
}

func TestMuxResumeRequiresTag(t *testing.T) {
	c := &OutboundDetourConfig{Protocol: "freedom", MuxResume: &MuxResumeConfig{Enabled: true}}
	if _, err := c.Build(); err == nil || !strings.Contains(err.Error(), `"tag"`) {
		t.Fatalf("tag-less muxResume must fail loudly, got %v", err)
	}
}

func TestMuxResumeRejectsVision(t *testing.T) {
	raw := json.RawMessage(`{"address":"example.com","port":443,"id":"27848739-7e62-4138-9fd3-098a63964b6b","flow":"xtls-rprx-vision","encryption":"none"}`)
	c := &OutboundDetourConfig{Tag: "v", Protocol: "vless", Settings: &raw, MuxResume: &MuxResumeConfig{Enabled: true}}
	if _, err := c.Build(); err == nil || !strings.Contains(err.Error(), "vision") {
		t.Fatalf("vision+muxResume must fail loudly, got %v", err)
	}
}

func TestVLessVisionFlowDetect(t *testing.T) {
	if (&VLessOutboundConfig{Flow: "xtls-rprx-vision"}).HasVisionFlow() != true {
		t.Fatal("simplified-style vision not detected")
	}
	raw := json.RawMessage(`{"flow":"xtls-rprx-vision"}`)
	vc := &VLessOutboundConfig{Vnext: []*VLessOutboundVnext{{Users: []json.RawMessage{raw}}}}
	if !vc.HasVisionFlow() {
		t.Fatal("vnext-style vision not detected")
	}
	if (&VLessOutboundConfig{}).HasVisionFlow() {
		t.Fatal("empty config must not report vision")
	}
}
