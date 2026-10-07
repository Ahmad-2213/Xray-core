package mux_test

import (
	"testing"
	"time"

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
