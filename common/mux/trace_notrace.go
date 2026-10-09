//go:build !muxtrace

package mux

// No-op trace stubs: production builds pay at most one call per site.

import (
	"github.com/xtls/xray-core/common/buf"
)

func traceRetain(g *carrierGate, seq uint64, sid uint16, ln int, crc uint32) {}

func traceAdmit(s *Session, n uint64, sid uint16, mb buf.MultiBuffer, dir string) {}

func traceAdmitGenMismatch(gen, curGen uint64, sealed bool) {}

func traceLogRecv(prefix string, snapRx uint64) {}

func traceDumpAdopt(path string, snapRx, peerRx uint64) {}

func traceLogSendWindow(prefix string, peerRx uint64) {}

func traceLogFlush(id uint64, label string, seqs []uint64) {}

func traceWriteCount(epoch, seq uint64) {}

func traceReset() {}
