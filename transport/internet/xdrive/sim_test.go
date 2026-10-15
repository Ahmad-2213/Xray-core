package xdrive

// A latency simulator for the Google Drive backend.
//
// The live measurements against the real API give the numbers this file
// hard-codes: a write costs about 1.5 s, a newly written file only becomes
// visible to files.list about 450 ms later, and a listing costs about 420 ms.
// Feeding those through the real walWriter and walReader shows how much of
// the end-to-end delay is the transport's own queueing and how much is Drive.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Measured against the live API from this host.
const (
	// A write small enough for the inline path is a plain metadata POST and is
	// much cheaper than a multipart upload; a bulk segment is multipart.
	simPutInlineLatency  = 880 * time.Millisecond
	simPutMediaLatency   = 1550 * time.Millisecond
	simGetLatency        = 450 * time.Millisecond
	simListLatency       = 420 * time.Millisecond
	simIndexLag          = 450 * time.Millisecond
	simDeleteLatency     = 250 * time.Millisecond

	// Drive enforces a per-user request cap well below what the published
	// quota-unit table implies, and 403s with no useful detail when it trips.
	// The account here sustains about 15 requests per second.
	simRequestsPerSecond = 15.0
	simBurstAllowance    = 24
)

type simObject struct {
	base      string
	data      []byte
	visibleAt time.Time
}

type simDrive struct {
	mu      sync.Mutex
	objects map[string]simObject

	inlineLimit int
	probeWindow int

	// shared request budget, modelling Drive's per-user cap
	tokens float64
	last   time.Time

	puts    int64
	gets    int64
	lists   int64
	deletes int64
	bytes   int64
	throttled int64
}

func newSimDrive() *simDrive {
	return &simDrive{
		objects:     map[string]simObject{},
		inlineLimit: driveInlineLimit,
		probeWindow: probeWindowWidth,
		tokens:      simBurstAllowance,
		last:        time.Now(),
	}
}

// take blocks until the shared request budget allows one more call.
func (s *simDrive) take() {
	for {
		s.mu.Lock()
		now := time.Now()
		s.tokens += now.Sub(s.last).Seconds() * simRequestsPerSecond
		if s.tokens > simBurstAllowance {
			s.tokens = simBurstAllowance
		}
		s.last = now
		if s.tokens >= 1 {
			s.tokens--
			s.mu.Unlock()
			return
		}
		wait := time.Duration((1 - s.tokens) / simRequestsPerSecond * float64(time.Second))
		s.mu.Unlock()
		if wait < time.Millisecond {
			wait = time.Millisecond
		}
		atomic.AddInt64(&s.throttled, 1)
		time.Sleep(wait)
	}
}

func (s *simDrive) stats() (puts, gets, lists, deletes, bytes int64) {
	return atomic.LoadInt64(&s.puts), atomic.LoadInt64(&s.gets),
		atomic.LoadInt64(&s.lists), atomic.LoadInt64(&s.deletes),
		atomic.LoadInt64(&s.bytes)
}

func (s *simDrive) Put(ctx context.Context, name string, data []byte) error {
	atomic.AddInt64(&s.puts, 1)
	s.take()
	latency := simPutMediaLatency
	if len(data) <= s.inlineLimit {
		latency = simPutInlineLatency
	}
	if !sleepCtx(ctx, latency) {
		return ctx.Err()
	}
	s.mu.Lock()
	s.objects[flatten(name)] = simObject{
		base:      name[strings.LastIndexByte(name, '/')+1:],
		data:      data,
		visibleAt: time.Now().Add(simIndexLag),
	}
	s.mu.Unlock()
	return nil
}

func (s *simDrive) Get(ctx context.Context, name string) ([]byte, error) {
	atomic.AddInt64(&s.gets, 1)
	s.take()
	if !sleepCtx(ctx, simGetLatency) {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	object, ok := s.objects[flatten(name)]
	s.mu.Unlock()
	if !ok || time.Now().Before(object.visibleAt) {
		return nil, errNotFound
	}
	atomic.AddInt64(&s.bytes, int64(len(object.data)))
	return object.data, nil
}

func (s *simDrive) Delete(ctx context.Context, name string) error {
	atomic.AddInt64(&s.deletes, 1)
	s.take()
	if !sleepCtx(ctx, simDeleteLatency) {
		return ctx.Err()
	}
	s.mu.Lock()
	delete(s.objects, flatten(name))
	s.mu.Unlock()
	return nil
}

func (s *simDrive) List(ctx context.Context, prefix string) ([]Entry, error) {
	atomic.AddInt64(&s.lists, 1)
	s.take()
	if !sleepCtx(ctx, simListLatency) {
		return nil, ctx.Err()
	}
	flat := flatten(prefix) + flatSeparator
	all := s.visibleEntries(flat, -1)
	out := make([]Entry, 0, len(all))
	for _, entry := range all {
		rest := strings.TrimPrefix(entry.Name, flat)
		if rest == "" {
			continue
		}
		if cut := strings.Index(rest, flatSeparator); cut >= 0 {
			rest = rest[:cut]
		}
		out = append(out, Entry{Name: rest})
	}
	return out, nil
}

// probe mirrors driveStorage.probe: one listing that covers a forward window
// of sequence numbers, carrying the payload inline when it is small enough.
func (s *simDrive) probe(ctx context.Context, prefix string, seq int64) ([]Entry, error) {
	atomic.AddInt64(&s.lists, 1)
	s.take()
	if !sleepCtx(ctx, simListLatency) {
		return nil, ctx.Err()
	}
	entries := s.visibleEntries(flatten(prefix), seq)
	if len(entries) == 0 {
		return nil, errNotFound
	}
	return entries, nil
}

// visibleEntries returns everything under a flat prefix that Drive has already
// indexed. When fromSeq is >= 0 only names at or after that sequence count,
// which is what the reader's window asks for.
func (s *simDrive) visibleEntries(flat string, fromSeq int64) []Entry {
	now := time.Now()
	s.mu.Lock()
	found := make([]Entry, 0, 32)
	for name, object := range s.objects {
		if !strings.HasPrefix(name, flat) {
			continue
		}
		if now.Before(object.visibleAt) {
			continue
		}
		entry := Entry{Name: object.base}
		if len(object.data) > 0 && len(object.data) <= s.inlineLimit {
			entry.Inline = object.data
		}
		found = append(found, entry)
	}
	s.mu.Unlock()

	if fromSeq < 0 {
		return found
	}
	out := found[:0]
	for _, entry := range found {
		if n, ok := parseEntry(entry.Name); ok && n >= fromSeq {
			out = append(out, entry)
		}
	}
	if len(out) > s.probeWindow {
		sort.Slice(out, func(i, j int) bool {
			a, _ := parseEntry(out[i].Name)
			b, _ := parseEntry(out[j].Name)
			return a < b
		})
		out = out[:s.probeWindow]
	}
	return out
}

func (s *simDrive) Close() error { return nil }

func sleepCtx(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// simPair is one xdrive session seen from both ends, like a client Conn and
// the matching server Conn sitting on opposite sides of the same Drive folder.
type simPair struct {
	client  *Conn
	server  *Conn
	cancel  context.CancelFunc
	drive   *simDrive
}

func newSimPair(t *testing.T, p params, tune func(*simDrive)) *simPair {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	drive := newSimDrive()
	if tune != nil {
		tune(drive)
	}
	client := newConn(ctx, drive, uplinkPrefix("s"), downlinkPrefix("s"), p, nil)
	server := newConn(ctx, drive, downlinkPrefix("s"), uplinkPrefix("s"), p, nil)
	return &simPair{client: client, server: server, cancel: cancel, drive: drive}
}

func (pair *simPair) Close() {
	pair.cancel()
	pair.client.Close()
	pair.server.Close()
}

func simParams(segmentBytes, concurrency int, flush, poll time.Duration) params {
	return params{
		segmentBytes:    segmentBytes,
		flushInterval:   flush,
		minPollInterval: poll,
		maxPollInterval: 2 * poll,
		eagerWindow:     2 * time.Second,
		holeTimeout:     30 * time.Second,
		sessionTTL:      time.Minute,
		concurrency:     concurrency,
		growth:          1,
	}
}

// readN pulls exactly n bytes from a Conn.
func readN(t *testing.T, c *Conn, n int) error {
	t.Helper()
	got := 0
	buf := make([]byte, 32*1024)
	for got < n {
		want := n - got
		if want > len(buf) {
			want = len(buf)
		}
		m, err := c.Read(buf[:want])
		got += m
		if err != nil {
			return fmt.Errorf("read %d/%d: %w", got, n, err)
		}
	}
	return nil
}

// TestSimIdleRoundTrip measures the delay of one request/response exchange on
// an otherwise empty session, with and without the media download step.
func TestSimIdleRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name  string
		bytes int
	}{
		{"interactive 200 B", 200},
		{"small 4 KiB", 4096},
		{"bulk 256 KiB", 262144},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pair := newSimPair(t, simParams(262144, 28, 20*time.Millisecond, 20*time.Millisecond), nil)
			defer pair.Close()

			request := make([]byte, tc.bytes)
			for i := range request {
				request[i] = byte(i)
			}

			start := time.Now()
			go func() { pair.client.Write(request) }()

			// Server side receives the request.
			if err := readN(t, pair.server, tc.bytes); err != nil {
				t.Fatal(err)
			}
			up := time.Since(start)

			// And answers it.
			go func() { pair.server.Write(request) }()
			if err := readN(t, pair.client, tc.bytes); err != nil {
				t.Fatal(err)
			}
			down := time.Since(start)

			puts, gets, lists, _, _ := pair.drive.stats()
			t.Logf("%-18s one way %5.2fs   round trip %5.2fs   (Put=%d Get=%d List=%d)",
				tc.name, up.Seconds(), down.Seconds(), puts, gets, lists)
		})
	}
}

// TestSimHeadOfLine is the case the user hit: a bulk download owns the pipe,
// then a small request arrives. This measures how long the small request waits.
func TestSimHeadOfLine(t *testing.T) {
	for _, tc := range []struct {
		name     string
		segment  int
		conc     int
		probeWin int
	}{
		{"16 KiB x 28 (ramped)", 16384, 28, 64},
		{"256 KiB x 28", 262144, 28, 64},
		{"256 KiB x 28, probe 16", 262144, 28, 16},
		{"256 KiB x 8, probe 16", 262144, 8, 16},
		{"1 MiB x 32, probe 16", 1048576, 32, 16},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pair := newSimPair(t, simParams(tc.segment, tc.conc, 20*time.Millisecond, 20*time.Millisecond),
				func(d *simDrive) { d.probeWindow = tc.probeWin })
			defer pair.Close()

			// The server pushes a long download at the client.
			downloadDone := make(chan struct{})
			go func() {
				defer close(downloadDone)
				chunk := make([]byte, tc.segment)
				for i := 0; i < 40; i++ {
					pair.server.Write(chunk)
				}
			}()

			// Let the download own the pipe for a while.
			time.Sleep(6 * time.Second)

			// Now an interactive request comes back the other way.
			start := time.Now()
			small := []byte("GET /hls/seg1.m3u8 HTTP/1.1\r\n\r\n")
			go func() { pair.client.Write(small) }()
			if err := readN(t, pair.server, len(small)); err != nil {
				t.Fatal(err)
			}
			interactive := time.Since(start)

			puts, gets, lists, _, mb := pair.drive.stats()
			t.Logf("%-22s interactive delivered in %6.2fs   Put=%d Get=%d List=%d moved=%d KB",
				tc.name, interactive.Seconds(), puts, gets, lists, mb/1024)
		})
	}
}

// TestSimSustained shows whether the pipe keeps moving or stalls in bursts.
func TestSimSustained(t *testing.T) {
	pair := newSimPair(t, simParams(262144, 28, 20*time.Millisecond, 20*time.Millisecond), nil)
	defer pair.Close()

	total := 4 << 20
	start := time.Now()
	go func() {
		chunk := make([]byte, 262144)
		written := 0
		for written < total {
			n, err := pair.server.Write(chunk)
			if err != nil {
				return
			}
			written += n
		}
	}()

	buf := make([]byte, 64*1024)
	got := 0
	stalls := 0
	last := time.Now()
	samples := 0
	for got < total {
		n, err := pair.client.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		got += n
		if d := time.Since(last); d > 500*time.Millisecond {
			stalls++
		}
		last = time.Now()
		samples++
	}
	elapsed := time.Since(start)
	puts, gets, lists, deletes, _ := pair.drive.stats()
	t.Logf("4 MiB in %5.1fs = %4.0f KB/s | stalls>500ms=%d over %d reads | Put=%d Get=%d List=%d Delete=%d | req/segment=%.1f | throttled=%d",
		elapsed.Seconds(), float64(total)/elapsed.Seconds()/1024, stalls, samples,
		puts, gets, lists, deletes, float64(puts+gets+lists+deletes)/16.0,
		atomic.LoadInt64(&pair.drive.throttled))
}

// TestSimBurstPattern samples arrival times at the client to show whether the
// delivery is smooth or arrives in clusters.
func TestSimBurstPattern(t *testing.T) {
	pair := newSimPair(t, simParams(262144, 28, 20*time.Millisecond, 20*time.Millisecond), nil)
	defer pair.Close()

	go func() {
		chunk := make([]byte, 262144)
		for i := 0; i < 60; i++ {
			pair.server.Write(chunk)
		}
	}()

	buf := make([]byte, 64*1024)
	start := time.Now()
	var stamps []time.Duration
	got := 0
	for got < 2<<20 && time.Since(start) < 60*time.Second {
		n, err := pair.client.Read(buf)
		if err != nil {
			t.Fatal(err)
		}
		got += n
		stamps = append(stamps, time.Since(start))
	}

	gaps := make([]time.Duration, 0, len(stamps))
	for i := 1; i < len(stamps); i++ {
		gaps = append(gaps, stamps[i]-stamps[i-1])
	}
	var maxGap, total time.Duration
	for _, g := range gaps {
		total += g
		if g > maxGap {
			maxGap = g
		}
	}
	if len(stamps) > 1 {
		meanGap := (total / time.Duration(len(gaps))).Milliseconds()
		t.Logf("reads=%d over %s | mean gap %5dms | max gap %5.2fs",
			len(stamps), total.Round(time.Millisecond), meanGap, maxGap.Seconds())
		first := []string{}
		for i := 0; i < len(stamps) && i < 12; i++ {
			first = append(first, stamps[i].Round(10*time.Millisecond).String())
		}
		t.Logf("first arrival times: %v", first)
	}
}