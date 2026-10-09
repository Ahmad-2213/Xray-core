//go:build race

package mux_test

import "time"

// testTimeoutScale stretches fixed test deadlines under the race
// detector: instrumentation collapses throughput (1-3 byte fragment pump
// × race overhead), so wall-clock budgets must grow while the logic and
// payloads under test stay identical.
const testTimeoutScale = 6

func testTimeout(d time.Duration) time.Duration { return d * testTimeoutScale }
