//go:build !race

package mux_test

import "time"

// testTimeoutScale is 1 without the race detector: deadlines as written.
const testTimeoutScale = 1

func testTimeout(d time.Duration) time.Duration { return d * testTimeoutScale }
