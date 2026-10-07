package conf_test

import (
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
