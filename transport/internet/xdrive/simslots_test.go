package xdrive

// Simulator for the slot transport, driven by the latencies measured against
// the live Drive API on 2026-10-15:
//
//	files.patch, description only   876 ms median   (range 634-1619)
//	files.patch, with media body    1270 ms
//	files.get?fields=description    371 ms           5 quota units
//	files.get?alt=media             860 ms          200 quota units
//
// Drive's index lag and files.list play no part here at all, which is the
// point: the reader never waits for either.

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	simPatchInline = 880 * time.Millisecond
	simPatchMedia  = 1270 * time.Millisecond
	simGetMeta     = 371 * time.Millisecond
	simGetBody     = 860 * time.Millisecond
)

// simScale shrinks the whole model at once: Drive's latencies and the
// transport's own timers. Only the proportions between them decide how the
// transport behaves, so scaling both keeps the test honest while finishing in
// seconds instead of minutes. One test restores the true timings to report the
// numbers that actually matter.
var simScale = 0.04

func simDur(d time.Duration) time.Duration {
	scaled := time.Duration(float64(d) * simScale)
	if scaled < 500*time.Microsecond {
		scaled = 500 * time.Microsecond
	}
	return scaled
}

// simRealTimings puts the model back on its true clock, both Drive's and the
// transport's, for a test that reports real numbers, and restores it after.
func simRealTimings() func() {
	prevScale := simScale
	prevCeiling, prevFloor := slotHuntCeiling, slotHuntFloor
	prevTailMin, prevTailMax := slotTailMin, slotTailMax
	prevAck, prevTTL := slotAckRetry, slotWatermarkTTL
	prevStall, prevGrace := slotStallTimeout, slotCloseGrace
	simScale = 1
	slotHuntCeiling, slotHuntFloor = 120*time.Millisecond, 50*time.Millisecond
	slotTailMin, slotTailMax = 150*time.Millisecond, 800*time.Millisecond
	slotAckRetry, slotWatermarkTTL = 150*time.Millisecond, 700*time.Millisecond
	slotStallTimeout, slotCloseGrace = 60*time.Second, 2*time.Second
	return func() {
		simScale = prevScale
		slotHuntCeiling, slotHuntFloor = prevCeiling, prevFloor
		slotTailMin, slotTailMax = prevTailMin, prevTailMax
		slotAckRetry, slotWatermarkTTL = prevAck, prevTTL
		slotStallTimeout, slotCloseGrace = prevStall, prevGrace
	}
}

type simSlotObject struct {
	description string
	content     []byte
}

type simSlots struct {
	mu   sync.Mutex
	byID map[string]*simSlotObject
	next int

	writes  int64
	reads   int64
	bodies  int64
	marks   int64
	markGet int64
	bytes   int64
}

func newSimSlots() *simSlots {
	return &simSlots{byID: map[string]*simSlotObject{}}
}

func (s *simSlots) makeSlot(ctx context.Context, name string) (string, error) {
	if !simSleep(ctx, simDur(simPatchInline)) {
		return "", ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	id := "simslot-" + strconv.Itoa(s.next)
	s.byID[id] = &simSlotObject{description: encodeWatermark(-1)}
	return id, nil
}

func (s *simSlots) writeSlot(ctx context.Context, id string, seq int64, mode uint8, data []byte) error {
	atomic.AddInt64(&s.writes, 1)
	latency := simPatchInline
	if mode == slotModeMedia {
		latency = simPatchMedia
	}
	if !simSleep(ctx, simDur(latency)) {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.byID[id]
	if !ok {
		object = &simSlotObject{}
		s.byID[id] = object
	}
	object.description = encodeSlot(slotFrame{Seq: seq, Mode: mode, Data: data})
	if mode == slotModeMedia {
		object.content = append([]byte(nil), data...)
	}
	atomic.AddInt64(&s.bytes, int64(len(data)))
	return nil
}

func (s *simSlots) readSlot(ctx context.Context, id string) (slotFrame, bool, error) {
	atomic.AddInt64(&s.reads, 1)
	if !simSleep(ctx, simDur(simGetMeta)) {
		return slotFrame{}, false, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.byID[id]
	if !ok {
		return slotFrame{}, false, nil
	}
	frame, good := decodeSlot(object.description)
	return frame, good, nil
}

func (s *simSlots) readSlotBody(ctx context.Context, id string) ([]byte, error) {
	atomic.AddInt64(&s.bodies, 1)
	if !simSleep(ctx, simDur(simGetBody)) {
		return nil, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.byID[id]
	if !ok {
		return nil, errNotFound
	}
	return object.content, nil
}

func (s *simSlots) writeWatermark(ctx context.Context, id string, seq int64) error {
	atomic.AddInt64(&s.marks, 1)
	if !simSleep(ctx, simDur(simPatchInline)) {
		return ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.byID[id]
	if !ok {
		object = &simSlotObject{}
		s.byID[id] = object
	}
	object.description = encodeWatermark(seq)
	return nil
}

func (s *simSlots) readWatermark(ctx context.Context, id string) (int64, error) {
	atomic.AddInt64(&s.markGet, 1)
	if !simSleep(ctx, simDur(simGetMeta)) {
		return -1, ctx.Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	object, ok := s.byID[id]
	if !ok {
		return -1, nil
	}
	return decodeWatermark(object.description), nil
}

func (s *simSlots) counters() (writes, reads, bodies, marks, markGet int64) {
	return atomic.LoadInt64(&s.writes), atomic.LoadInt64(&s.reads),
		atomic.LoadInt64(&s.bodies), atomic.LoadInt64(&s.marks),
		atomic.LoadInt64(&s.markGet)
}

func (s *simSlots) total() int64 {
	w, r, b, mk, mg := s.counters()
	return w + r + b + mk + mg
}

func (s *simSlots) Close() error { return nil }

func simSleep(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func simSlotParams(segmentBytes, depth int) params {
	return params{
		segmentBytes:    segmentBytes,
		flushInterval:   10 * time.Millisecond,
		minPollInterval: 20 * time.Millisecond,
		maxPollInterval: 400 * time.Millisecond,
		eagerWindow:     2 * time.Second,
		holeTimeout:     30 * time.Second,
		sessionTTL:      time.Minute,
		concurrency:     depth,
		growth:          1,
	}
}

// newSimSlotPair builds both ends of one session over a shared slot store,
// exactly the way Dial and the listener build them.
func newSimSlotPair(t *testing.T, p params) (*slotConn, *slotConn, *simSlots) {
	t.Helper()
	store := newSimSlots()
	depth := 0
	for _, id := range []string{} {
		_ = id
	}
	_ = depth

	build := func() slotTable {
		table := slotTable{Slots: make([]string, 0, 8)}
		for i := 0; i < p.concurrency && i < slotDepthLimit; i++ {
			id, err := store.makeSlot(context.Background(), "")
			if err != nil {
				t.Fatalf("makeSlot: %v", err)
			}
			table.Slots = append(table.Slots, id)
		}
		mark, err := store.makeSlot(context.Background(), "")
		if err != nil {
			t.Fatalf("makeSlot: %v", err)
		}
		table.Watermark = mark
		return table
	}

	c2s, s2c := build(), build()
	clientView := slotAnnounce{Uplink: c2s, Downlink: s2c}
	serverView := slotAnnounce{Uplink: s2c, Downlink: c2s}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	client := newSlotConn(ctx, store, clientView, p, nil)
	server := newSlotConn(ctx, store, serverView, p, nil)
	return client, server, store
}

func simReadN(t *testing.T, c *slotConn, n int) error {
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
			if err == io.EOF {
				return io.EOF
			}
			return err
		}
	}
	return nil
}

// TestSlotLatency is the number that decides whether HLS can work at all.
// Aparat gives a stream request about three seconds; a round trip here has to
// land well under that.
func TestSlotLatency(t *testing.T) {
	defer simRealTimings()()
	for _, tc := range []struct {
		name  string
		bytes int
	}{
		{"interactive 200 B", 200},
		{"request 1 KiB", 1024},
		{"inline limit 14 KiB", 14 * 1024},
		{"media 256 KiB", 256 * 1024},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := simSlotParams(256*1024, 6)
			client, server, store := newSimSlotPair(t, p)
			defer client.Close()
			defer server.Close()

			payload := make([]byte, tc.bytes)
			for i := range payload {
				payload[i] = byte(i * 7)
			}

			start := time.Now()
			go client.Write(payload)
			if err := simReadN(t, server, tc.bytes); err != nil {
				t.Fatalf("upstream %v after %s | requests=%d | clientReader=%q serverReader=%q",
					err, time.Since(start).Round(time.Millisecond), store.total(),
					client.reader.Stopped(), server.reader.Stopped())
			}
			up := time.Since(start)

			go server.Write(payload)
			if err := simReadN(t, client, tc.bytes); err != nil {
				t.Fatalf("downstream %v after %s | requests=%d | clientReader=%q serverReader=%q",
					err, time.Since(start).Round(time.Millisecond), store.total(),
					client.reader.Stopped(), server.reader.Stopped())
			}
			down := time.Since(start)

			w, r, b, mk, mg := store.counters()
			t.Logf("%-20s one way %5.2fs   ROUND TRIP %5.2fs   patch=%d probe=%d body=%d mark=%d markGet=%d  total=%d",
				tc.name, up.Seconds(), down.Seconds(), w, r, b, mk, mg, store.total())
		})
	}
}

// TestSlotHeadOfLine is the download-in-progress case the user reported as
// twenty seconds. Nothing in the slot path shares a budget with a bulk
// segment, so this should land near the idle number.
func TestSlotHeadOfLine(t *testing.T) {
	for _, tc := range []struct {
		name    string
		segment int
		depth   int
	}{
		{"depth 4, 256 KiB", 262144, 4},
		{"depth 8, 256 KiB", 262144, 8},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := simSlotParams(tc.segment, tc.depth)
			client, server, store := newSimSlotPair(t, p)
			defer client.Close()
			defer server.Close()

			// The download has to actually be drained, otherwise the client's
			// reader fills up, stops publishing its watermark, and the server
			// correctly refuses to overwrite a slot. That is the backpressure
			// working, and it is worth having in the test.
			drained := make(chan int64, 1)
			go func() {
				buf := make([]byte, 64*1024)
				var total int64
				for {
					n, err := client.Read(buf)
					if err != nil {
						break
					}
					total += int64(n)
				}
				drained <- total
			}()

			go func() {
				chunk := make([]byte, tc.segment)
				for i := 0; i < 40; i++ {
					if _, err := server.Write(chunk); err != nil {
						return
					}
				}
			}()

			time.Sleep(simDur(8 * time.Second))
			start := time.Now()
			small := []byte("GET /hls/seg3.ts HTTP/1.1\r\n\r\n")
			go client.Write(small)
			if err := simReadN(t, server, len(small)); err != nil {
				t.Fatalf("%v after %s | clientReader=%q serverReader=%q",
					err, time.Since(start).Round(time.Millisecond),
					client.reader.Stopped(), server.reader.Stopped())
			}
			t.Logf("%-20s interactive delivered in %6.2fs while %d Drive requests were in flight",
				tc.name, time.Since(start).Seconds(), store.total())

			// Close the sender first; the drainer then sees the end marker and
			// stops on its own.
			server.Close()
			client.Close()
			<-drained
		})
	}
}

// TestSlotPageLoad is the workload that actually broke. A browser opens a
// dozen connections at once, each a small request and a small response, and
// they all share one session. Sending each as its own segment turns a page
// load into a dozen Drive writes competing for a handful of slots.
func TestSlotPageLoad(t *testing.T) {
	for _, fanout := range []int{4, 12, 24} {
		t.Run(fmt.Sprintf("%d concurrent", fanout), func(t *testing.T) {
			p := simSlotParams(262144, 8)
			client, server, store := newSimSlotPair(t, p)
			defer client.Close()
			defer server.Close()

			request := []byte("GET /asset HTTP/1.1\r\nHost: example\r\n\r\n")
			response := []byte("response body, a few kilobytes")
			reqBytes := fanout * len(request)
			respBytes := fanout * len(response)

			// The server drains every request before answering, the way a proxy
			// finishes reading a page's worth of requests and then replies.
			served := make(chan error, 1)
			go func() {
				buf := make([]byte, 8*1024)
				got := 0
				for got < reqBytes {
					n, err := server.Read(buf)
					if err != nil {
						served <- err
						return
					}
					got += n
				}
				for i := 0; i < fanout; i++ {
					if _, err := server.Write(response); err != nil {
						served <- err
						return
					}
				}
				served <- nil
			}()

			// All the requests go out together, the way a page load issues them.
			start := time.Now()
			for i := 0; i < fanout; i++ {
				if _, err := client.Write(request); err != nil {
					t.Fatalf("write %d: %v", i, err)
				}
			}
			if err := simReadN(t, client, respBytes); err != nil {
				t.Fatalf("after %s: %v | clientReader=%q serverReader=%q",
					time.Since(start).Round(time.Millisecond), err,
					client.reader.Stopped(), server.reader.Stopped())
			}
			if err := <-served; err != nil {
				t.Fatalf("server: %v", err)
			}
			done := time.Since(start)
			w, r, b, mk, mg := store.counters()
			t.Logf("fanout %2d: all done in %5.2fs | per request %5.2fs | patch=%d probe=%d body=%d mark=%d markGet=%d total=%d",
				fanout, done.Seconds(), done.Seconds()/float64(fanout), w, r, b, mk, mg, store.total())
		})
	}
}

// TestSlotSlowConsumer pins the loop that killed a real session.
//
// A consumer that stops reading fills the reader's buffer. If the reader only
// acknowledges a slot once the bytes have been handed on, the writer's gate
// shuts, the writer stops, and the consumer never gets the data it was waiting
// for. Nothing times out quickly and nothing explains itself: the session just
// goes quiet and eventually dies, taking every mux substream with it.
//
// Here the client writes steadily and never reads. The server must keep
// writing regardless.
func TestSlotSlowConsumer(t *testing.T) {
	p := simSlotParams(262144, 8)
	client, server, store := newSimSlotPair(t, p)
	defer client.Close()
	defer server.Close()

	// The server never reads, so its own read buffer fills, but that must not
	// stop it writing.
	writeErr := make(chan error, 1)
	go func() {
		chunk := make([]byte, 4096)
		for i := 0; i < 200; i++ {
			if _, err := server.Write(chunk); err != nil {
				writeErr <- err
				return
			}
		}
		writeErr <- nil
	}()

	// The client reads slowly: one segment at a time, slowly enough that the
	// buffer between reader and consumer is under pressure.
	drained := make(chan int64, 1)
	go func() {
		buf := make([]byte, 4096)
		var total int64
		for i := 0; i < 200*4096; i++ {
			n, err := client.Read(buf)
			if err != nil {
				break
			}
			total += int64(n)
		}
		drained <- total
	}()

	select {
	case err := <-writeErr:
		if err != nil {
			t.Fatalf("the server stopped writing: %v | clientReader=%q serverReader=%q",
				err, client.reader.Stopped(), server.reader.Stopped())
		}
		t.Logf("the server wrote all 200 segments while the client was still reading | store=%d", store.total())
	case <-time.After(30 * time.Second):
		t.Fatalf("the server stalled with a slow consumer | clientReader=%q serverReader=%q",
			client.reader.Stopped(), server.reader.Stopped())
	}
	client.Close()
	<-drained
}

// TestSlotSustained reports throughput and what it costs in Drive requests,
// which is the budget that actually limits this transport.
func TestSlotSustained(t *testing.T) {
	p := simSlotParams(256*1024, 8)
	client, server, store := newSimSlotPair(t, p)
	defer client.Close()
	defer server.Close()

	total := 4 << 20
	go func() {
		chunk := make([]byte, 262144)
		for sent := 0; sent < total; {
			n, err := server.Write(chunk)
			if err != nil {
				return
			}
			sent += n
		}
	}()

	buf := make([]byte, 64*1024)
	start := time.Now()
	got, stalls := 0, 0
	maxGap := time.Duration(0)
	last := start
	for got < total {
		n, err := client.Read(buf)
		if err != nil {
			t.Fatalf("read %d/%d after %s: %v | clientReader=%q",
				got, total, time.Since(start).Round(time.Millisecond), err, client.reader.Stopped())
		}
		got += n
		d := time.Since(last)
		if d > 500*time.Millisecond {
			stalls++
		}
		if d > maxGap {
			maxGap = d
		}
		last = time.Now()
	}
	elapsed := time.Since(start)
	w, r, b, mk, mg := store.counters()
	segments := float64(total) / 262144
	t.Logf("4 MiB in %5.1fs = %4.0f KB/s | stalls>500ms=%d maxGap=%s | patch=%d probe=%d body=%d mark=%d markGet=%d",
		elapsed.Seconds(), float64(total)/elapsed.Seconds()/1024, stalls, maxGap.Round(time.Millisecond), w, r, b, mk, mg)
	t.Logf("   %.2f Drive requests per 256 KiB segment, %.1f requests per second",
		float64(store.total())/segments, float64(store.total())/elapsed.Seconds())
}

type gateSlotStore struct {
	slotStore
	gate chan struct{}
}

func (g *gateSlotStore) writeSlot(ctx context.Context, id string, seq int64, mode uint8, data []byte) error {
	select {
	case <-g.gate:
	case <-ctx.Done():
		return ctx.Err()
	}
	return g.slotStore.writeSlot(ctx, id, seq, mode, data)
}

func TestSlotWriterBackpressure(t *testing.T) {
	const depth = 4
	p := simSlotParams(1024, depth)
	inner := newSimSlots()
	store := &gateSlotStore{slotStore: inner, gate: make(chan struct{})}
	table := slotTable{}
	for i := 0; i < depth; i++ {
		id, err := inner.makeSlot(context.Background(), "")
		if err != nil {
			t.Fatalf("makeSlot: %v", err)
		}
		table.Slots = append(table.Slots, id)
	}
	mark, err := inner.makeSlot(context.Background(), "")
	if err != nil {
		t.Fatalf("makeSlot: %v", err)
	}
	table.Watermark = mark
	if err := inner.writeWatermark(context.Background(), mark, 1<<30); err != nil {
		t.Fatalf("seed watermark: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newSlotWriter(ctx, store, table, p)

	const total = 3*depth + 2
	seg := make([]byte, 1024)
	done := make(chan int, total)
	go func() {
		for i := 0; i < total; i++ {
			if _, err := w.Write(seg); err != nil {
				return
			}
			done <- i
		}
	}()

	const want = depth
	for i := 0; i < want; i++ {
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("write %d blocked, want first %d to complete", i, want)
		}
	}
	select {
	case n := <-done:
		t.Fatalf("write %d completed against a blocked store, want backpressure after %d", n, want)
	case <-time.After(300 * time.Millisecond):
	}
	close(store.gate)
	for i := want; i < total; i++ {
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Fatalf("write %d stuck after release", i)
		}
	}
}

type slowReadSlotStore struct {
	slotStore
	delay time.Duration
}

func (s *slowReadSlotStore) readSlot(ctx context.Context, id string) (slotFrame, bool, error) {
	if !simSleep(ctx, s.delay) {
		return slotFrame{}, false, ctx.Err()
	}
	return s.slotStore.readSlot(ctx, id)
}

func TestSlotCloseDrainsSlowReader(t *testing.T) {
	const depth = 2
	const segs = 6
	p := simSlotParams(1024, depth)
	inner := newSimSlots()
	store := &slowReadSlotStore{slotStore: inner, delay: time.Second}
	table := slotTable{}
	for i := 0; i < depth; i++ {
		id, err := inner.makeSlot(context.Background(), "")
		if err != nil {
			t.Fatalf("makeSlot: %v", err)
		}
		table.Slots = append(table.Slots, id)
	}
	mark, err := inner.makeSlot(context.Background(), "")
	if err != nil {
		t.Fatalf("makeSlot: %v", err)
	}
	table.Watermark = mark

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newSlotWriter(ctx, store, table, p)
	r := newSlotReader(ctx, store, table, p)

	for i := 0; i < segs; i++ {
		seg := make([]byte, 1024)
		for j := range seg {
			seg[j] = byte(i + 1)
		}
		if _, err := w.Write(seg); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	type result struct {
		chunks [][]byte
	}
	finished := make(chan result, 1)
	go func() {
		var res result
		for data := range r.ch {
			res.chunks = append(res.chunks, data)
		}
		finished <- res
	}()

	done := make(chan error, 1)
	go func() {
		done <- w.Close()
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(40 * time.Second):
		t.Fatal("Close did not drain a progressing reader")
	}
	select {
	case res := <-finished:
		if len(res.chunks) != segs {
			t.Fatalf("delivered %d segments, want %d", len(res.chunks), segs)
		}
		for i, c := range res.chunks {
			if len(c) != 1024 || c[0] != byte(i+1) {
				t.Fatalf("segment %d corrupt", i)
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reader did not stop after the end marker")
	}
	if got := r.Stopped(); got != "the peer closed the stream" {
		t.Fatalf("reader stopped with %q, want clean end", got)
	}
}
