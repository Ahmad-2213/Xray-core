package xdrive

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestDeleteAverageStartsAtFirstSample(t *testing.T) {
	s := &driveStorage{}
	s.noteDelete(100 * time.Millisecond)
	if s.delN != 1 {
		t.Fatalf("samples = %d, want 1", s.delN)
	}
	if s.delAvg != 100*time.Millisecond {
		t.Fatalf("average = %v, want 100ms (first sample must not be halved)", s.delAvg)
	}
}

func TestDeleteBaselineTracksMinimum(t *testing.T) {
	s := &driveStorage{}
	s.noteDelete(time.Millisecond)
	s.noteDelete(time.Millisecond)
	if s.shouldGrow() {
		t.Fatal("grew with only 2 samples")
	}
	s.noteDelete(time.Millisecond)
	if s.delBase != time.Millisecond {
		t.Fatalf("baseline = %v, want 1ms after 3 samples", s.delBase)
	}
	if !s.shouldGrow() {
		t.Fatal("healthy average should grow")
	}
	s.noteDelete(10 * time.Millisecond)
	if s.shouldGrow() {
		t.Fatal("grew while average exceeded 1.5x baseline")
	}
	for i := 0; i < 20 && !s.shouldGrow(); i++ {
		s.noteDelete(0)
	}
	if !s.shouldGrow() {
		t.Fatal("growth never resumed after latency recovered")
	}
}

func TestDeleteNotFoundNotSampled(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, "sessions/ghost", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	drive.mu.Lock()
	for id, f := range drive.files {
		if f.name == "sessions~ghost" {
			delete(drive.files, id)
		}
	}
	drive.mu.Unlock()

	if err := storage.Delete(ctx, "sessions/ghost"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if storage.delN != 0 {
		t.Fatalf("404 delete sampled, samples = %d", storage.delN)
	}

	if err := storage.Put(ctx, "sessions/real", []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := storage.Delete(ctx, "sessions/real"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if storage.delN != 1 {
		t.Fatalf("samples = %d, want 1", storage.delN)
	}
}

func TestGrowthNeedsFullSegment(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		storage.noteDelete(time.Millisecond)
	}

	w := newWALWriter(ctx, storage, "streams/grow", params{
		segmentBytes:  1024,
		flushInterval: 50 * time.Millisecond,
		concurrency:   2,
		growth:        2,
	})
	defer w.wg.Wait()

	if _, err := w.Write(make([]byte, 100)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	time.Sleep(150 * time.Millisecond)
	if w.segBytes != 1024 {
		t.Fatalf("partial flush grew to %d", w.segBytes)
	}

	if _, err := w.Write(make([]byte, 2000)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if w.segBytes != 2048 {
		t.Fatalf("full flush left segBytes at %d, want 2048", w.segBytes)
	}
}

func TestGrowthStopsWhenSlow(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		storage.noteDelete(time.Millisecond)
	}

	w := newWALWriter(ctx, storage, "streams/slow", params{
		segmentBytes:  1024,
		flushInterval: time.Hour,
		concurrency:   2,
		growth:        2,
	})
	defer w.wg.Wait()

	if _, err := w.Write(make([]byte, 2000)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if w.segBytes != 2048 {
		t.Fatalf("segBytes = %d, want 2048", w.segBytes)
	}
	for i := 0; i < 3; i++ {
		storage.noteDelete(100 * time.Millisecond)
	}
	if _, err := w.Write(make([]byte, 3000)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if w.segBytes != 2048 {
		t.Fatalf("grew to %d while delete latency was high", w.segBytes)
	}
}

func TestGrowthCapHolds(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		storage.noteDelete(time.Millisecond)
	}

	w := newWALWriter(ctx, storage, "streams/cap", params{
		segmentBytes:  1024,
		flushInterval: time.Hour,
		concurrency:   2,
		growth:        5000,
	})
	defer w.wg.Wait()

	if _, err := w.Write(make([]byte, 2000)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if w.segBytes != maxAdaptiveSegmentBytes {
		t.Fatalf("segBytes = %d, want cap %d", w.segBytes, maxAdaptiveSegmentBytes)
	}
}

func TestGrowthNeverBelowConfigured(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		storage.noteDelete(time.Millisecond)
	}

	const configured = 8 * 1024 * 1024
	w := newWALWriter(ctx, storage, "streams/big", params{
		segmentBytes:  configured,
		flushInterval: time.Hour,
		concurrency:   2,
		growth:        2,
	})
	defer w.wg.Wait()

	if _, err := w.Write(make([]byte, configured+1)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if w.segBytes != configured {
		t.Fatalf("segBytes = %d, want configured %d", w.segBytes, configured)
	}
}

func TestGrowthFactorOneDisables(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for i := 0; i < 3; i++ {
		storage.noteDelete(time.Millisecond)
	}

	w := newWALWriter(ctx, storage, "streams/fixed", params{
		segmentBytes:  1024,
		flushInterval: time.Hour,
		concurrency:   2,
		growth:        1,
	})
	defer w.wg.Wait()

	if _, err := w.Write(make([]byte, 2000)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if w.segBytes != 1024 {
		t.Fatalf("factor 1 grew to %d", w.segBytes)
	}
}

func manualReader(storage Storage, prefix string, p params) *walReader {
	return &walReader{
		ctx:      context.Background(),
		storage:  storage,
		prefix:   prefix,
		params:   p,
		ch:       make(chan []byte, 64),
		discards: make(chan string, 64),
		wake:     make(chan struct{}, 1),
	}
}

func driveCounts(d *fakeDrive) (orLists, containsLists int) {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.orLists, d.containsLists
}

func TestProbeIdleIssuesOneQuery(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	r := manualReader(storage, "streams/idle", params{concurrency: 8, holeTimeout: 100 * time.Millisecond})

	for i := 0; i < 20; i++ {
		if _, _, err := r.poll(); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	if orLists, containsLists := driveCounts(drive); orLists != 20 || containsLists != 0 {
		t.Fatalf("idle polls issued %d probes and %d full lists, want 20 and 0", orLists, containsLists)
	}

	time.Sleep(1200 * time.Millisecond)
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if orLists, containsLists := driveCounts(drive); orLists != 21 || containsLists != 1 {
		t.Fatalf("after 1s got %d probes and %d full lists, want 21 and 1", orLists, containsLists)
	}

	for i := 0; i < 5; i++ {
		if _, _, err := r.poll(); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	if orLists, containsLists := driveCounts(drive); orLists != 26 || containsLists != 1 {
		t.Fatalf("safety window reset failed: %d probes and %d full lists, want 26 and 1", orLists, containsLists)
	}
}

func TestProbeSeesEnd(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/e", 0, endSuffix), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}

	r := manualReader(storage, "streams/e", params{concurrency: 8, holeTimeout: 100 * time.Millisecond})
	advanced, eof, err := r.poll()
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if !eof || advanced {
		t.Fatalf("poll returned advanced=%v eof=%v, want false true", advanced, eof)
	}
}

func TestProbeSeesErr(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/f", 0, errSuffix), nil); err != nil {
		t.Fatalf("Put: %v", err)
	}

	r := manualReader(storage, "streams/f", params{concurrency: 8, holeTimeout: 100 * time.Millisecond})
	_, _, err := r.poll()
	if err == nil || !strings.Contains(err.Error(), "could not store") {
		t.Fatalf("poll returned %v, want peer-store error", err)
	}
}

func TestHoleDetectionStillFires(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	for _, seq := range []int64{1, 2, 3} {
		if err := storage.Put(ctx, objectName("streams/h", seq, segSuffix), []byte("x")); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	r := manualReader(storage, "streams/h", params{concurrency: 8, holeTimeout: 100 * time.Millisecond})
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if _, containsLists := driveCounts(drive); containsLists != 0 {
		t.Fatalf("first idle poll ran a full list")
	}

	time.Sleep(1200 * time.Millisecond)
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}

	time.Sleep(1200 * time.Millisecond)
	_, _, err := r.poll()
	if err == nil || !strings.Contains(err.Error(), "never arrived") {
		t.Fatalf("poll returned %v, want hole error", err)
	}
}

type stubStorage struct {
	mu          sync.Mutex
	data        map[string][]byte
	lists       [][]string
	listCalls   int
	fail        map[string]error
	delay       map[string]time.Duration
	defDelay    time.Duration
	starts      map[string]time.Time
	ends        map[string]time.Time
	inflight    int
	maxInflight int
	startedGets int
}

func (s *stubStorage) Put(_ context.Context, name string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[name] = cp
	return nil
}

func (s *stubStorage) Get(ctx context.Context, name string) ([]byte, error) {
	s.mu.Lock()
	if s.starts == nil {
		s.starts = make(map[string]time.Time)
		s.ends = make(map[string]time.Time)
	}
	s.starts[name] = time.Now()
	s.inflight++
	if s.inflight > s.maxInflight {
		s.maxInflight = s.inflight
	}
	s.startedGets++
	d := s.defDelay
	if v, ok := s.delay[name]; ok {
		d = v
	}
	err := s.fail[name]
	s.mu.Unlock()

	select {
	case <-time.After(d):
	case <-ctx.Done():
		s.mu.Lock()
		s.inflight--
		s.ends[name] = time.Now()
		s.mu.Unlock()
		return nil, ctx.Err()
	}

	s.mu.Lock()
	s.inflight--
	s.ends[name] = time.Now()
	cp := append([]byte(nil), s.data[name]...)
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return cp, nil
}

func (s *stubStorage) Delete(_ context.Context, _ string) error {
	return nil
}

func (s *stubStorage) List(_ context.Context, _ string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listCalls++
	names := s.lists[len(s.lists)-1]
	if s.listCalls <= len(s.lists) {
		names = s.lists[s.listCalls-1]
	}
	entries := make([]Entry, 0, len(names))
	for _, name := range names {
		entries = append(entries, Entry{Name: name})
	}
	return entries, nil
}

func (s *stubStorage) Close() error {
	return nil
}

func (s *stubStorage) snapshot() (starts, ends map[string]time.Time, maxInflight, startedGets int, listCalls int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	starts = make(map[string]time.Time, len(s.starts))
	for k, v := range s.starts {
		starts[k] = v
	}
	ends = make(map[string]time.Time, len(s.ends))
	for k, v := range s.ends {
		ends[k] = v
	}
	return starts, ends, s.maxInflight, s.startedGets, s.listCalls
}

func (s *stubStorage) inflightNow() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.inflight
}

func stubNames(prefix string, n int) (full []string, rel []string) {
	for i := 0; i < n; i++ {
		rel = append(rel, objectName("", int64(i), segSuffix)[1:])
		full = append(full, prefix+"/"+rel[i])
	}
	return full, rel
}

func drainDiscards(r *walReader) []string {
	var out []string
	for {
		select {
		case name := <-r.discards:
			out = append(out, name)
		default:
			return out
		}
	}
}

func drainReader(t *testing.T, r *walReader) [][]byte {
	t.Helper()
	var out [][]byte
	for {
		select {
		case chunk := <-r.ch:
			out = append(out, chunk)
		default:
			return out
		}
	}
}

func TestWindowDeliversInOrder(t *testing.T) {
	const n = 16
	prefix := "streams/window"
	full, rel := stubNames(prefix, n)
	s := &stubStorage{data: make(map[string][]byte), delay: make(map[string]time.Duration), defDelay: 5 * time.Millisecond}
	for i, name := range full {
		s.data[name] = []byte{byte(i)}
		s.delay[name] = time.Duration(n-i) * 5 * time.Millisecond
	}
	s.lists = [][]string{rel}
	r := manualReader(s, prefix, params{concurrency: 4, holeTimeout: time.Second})

	advanced, eof, err := r.poll()
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if eof || !advanced {
		t.Fatalf("poll returned advanced=%v eof=%v", advanced, eof)
	}
	got := drainReader(t, r)
	if len(got) != n {
		t.Fatalf("delivered %d chunks, want %d", len(got), n)
	}
	for i, chunk := range got {
		if len(chunk) != 1 || chunk[0] != byte(i) {
			t.Fatalf("chunk %d out of order: %v", i, chunk)
		}
	}
	_, _, maxInflight, _, _ := s.snapshot()
	if maxInflight > 4 {
		t.Fatalf("max in-flight GETs = %d, want <= 4", maxInflight)
	}
	discarded := drainDiscards(r)
	if len(discarded) != n {
		t.Fatalf("discarded %d segments, want %d", len(discarded), n)
	}
	for i, name := range discarded {
		if name != full[i] {
			t.Fatalf("discard %d = %q, want %q (discard must follow delivery order)", i, name, full[i])
		}
	}
}

func TestWindowErrorStopsAtHead(t *testing.T) {
	const n = 8
	prefix := "streams/errhead"
	full, rel := stubNames(prefix, n)
	s := &stubStorage{data: make(map[string][]byte), defDelay: 5 * time.Millisecond}
	for i, name := range full {
		s.data[name] = []byte{byte(i)}
	}
	boom := errTestBoom
	s.setFail(full[5], boom)
	s.lists = [][]string{rel}
	r := manualReader(s, prefix, params{concurrency: 4, holeTimeout: time.Second})

	_, _, err := r.poll()
	if err != boom {
		t.Fatalf("poll returned %v, want the injected failure", err)
	}
	if r.seq != 5 {
		t.Fatalf("seq = %d, want 5 (failed segment must not advance)", r.seq)
	}
	got := drainReader(t, r)
	if len(got) != 5 {
		t.Fatalf("delivered %d chunks before failure, want 5", len(got))
	}
	discarded := drainDiscards(r)
	if len(discarded) != 5 {
		t.Fatalf("discarded %d segments, want exactly the 5 delivered ones", len(discarded))
	}
	for i, name := range discarded {
		if name != full[i] {
			t.Fatalf("discard %d = %q, want %q", i, name, full[i])
		}
	}
}

var errTestBoom testError = "injected Get failure"

type testError string

func (e testError) Error() string {
	return string(e)
}

func (s *stubStorage) setFail(name string, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail == nil {
		s.fail = make(map[string]error)
	}
	s.fail[name] = err
}

func (s *stubStorage) clearFail(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.fail, name)
}

func TestWindowNotFoundDropsWithoutLoss(t *testing.T) {
	const n = 4
	prefix := "streams/missing"
	full, rel := stubNames(prefix, n)
	s := &stubStorage{data: make(map[string][]byte), fail: make(map[string]error), defDelay: time.Millisecond}
	for i, name := range full {
		s.data[name] = []byte{byte(i)}
	}
	s.fail[full[0]] = errNotFound
	s.lists = [][]string{rel}
	r := manualReader(s, prefix, params{concurrency: 4, holeTimeout: time.Second})

	advanced, eof, err := r.poll()
	if err != nil || eof || advanced {
		t.Fatalf("poll returned advanced=%v eof=%v err=%v, want clean retry", advanced, eof, err)
	}
	if r.seq != 0 {
		t.Fatalf("seq = %d, want 0", r.seq)
	}
	if got := drainReader(t, r); len(got) != 0 {
		t.Fatalf("delivered %d chunks, want none", len(got))
	}

	s.clearFail(full[0])
	advanced, eof, err = r.poll()
	if err != nil || eof || !advanced {
		t.Fatalf("second poll returned advanced=%v eof=%v err=%v", advanced, eof, err)
	}
	if got := drainReader(t, r); len(got) != n {
		t.Fatalf("redelivered %d chunks, want %d", len(got), n)
	}
}

func TestWindowRespectsConcurrency(t *testing.T) {
	const n = 20
	prefix := "streams/cap"
	full, rel := stubNames(prefix, n)
	s := &stubStorage{data: make(map[string][]byte), defDelay: 30 * time.Millisecond}
	for i, name := range full {
		s.data[name] = []byte{byte(i)}
	}
	s.lists = [][]string{rel}
	r := manualReader(s, prefix, params{concurrency: 4, holeTimeout: time.Second})

	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	_, _, maxInflight, _, _ := s.snapshot()
	if maxInflight > 4 {
		t.Fatalf("max in-flight GETs = %d, want <= 4", maxInflight)
	}
	if got := drainReader(t, r); len(got) != n {
		t.Fatalf("delivered %d chunks, want %d", len(got), n)
	}
}

func TestWindowBackpressureBounds(t *testing.T) {
	const n = 8
	prefix := "streams/pressure"
	full, rel := stubNames(prefix, n)
	s := &stubStorage{data: make(map[string][]byte), defDelay: 30 * time.Millisecond}
	for i, name := range full {
		s.data[name] = []byte{byte(i)}
	}
	s.lists = [][]string{rel}
	ctx, cancel := context.WithCancel(context.Background())
	r := &walReader{
		ctx:      ctx,
		storage:  s,
		prefix:   prefix,
		params:   params{concurrency: 4, holeTimeout: time.Second},
		ch:       make(chan []byte),
		discards: make(chan string, 64),
		wake:     make(chan struct{}, 1),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = r.poll()
	}()
	time.Sleep(300 * time.Millisecond)
	_, _, _, startedGets, _ := s.snapshot()
	if startedGets != 4 {
		t.Fatalf("started %d GETs with no consumer, want exactly the 4 window slots", startedGets)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not return after cancel")
	}
}

func TestWindowCtxLeavesNoGoroutine(t *testing.T) {
	const n = 8
	prefix := "streams/noleak"
	full, rel := stubNames(prefix, n)
	s := &stubStorage{data: make(map[string][]byte), defDelay: 500 * time.Millisecond}
	for i, name := range full {
		s.data[name] = []byte{byte(i)}
	}
	s.lists = [][]string{rel}
	ctx, cancel := context.WithCancel(context.Background())
	r := &walReader{
		ctx:      ctx,
		storage:  s,
		prefix:   prefix,
		params:   params{concurrency: 4, holeTimeout: time.Second},
		ch:       make(chan []byte, 64),
		discards: make(chan string, 64),
		wake:     make(chan struct{}, 1),
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _, _ = r.poll()
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("poll did not return after cancel")
	}
	waitFor(t, "in-flight GETs to finish", func() bool {
		return s.inflightNow() == 0
	})
}

func TestWindowOverlapTiming(t *testing.T) {
	prefix := "streams/overlap"
	full, rel := stubNames(prefix, 8)
	s := &stubStorage{data: make(map[string][]byte), delay: make(map[string]time.Duration), defDelay: 20 * time.Millisecond}
	for i, name := range full {
		s.data[name] = []byte{byte(i)}
	}
	s.delay[full[3]] = 400 * time.Millisecond
	s.lists = [][]string{rel[:4], rel}
	r := manualReader(s, prefix, params{concurrency: 4, holeTimeout: time.Second})

	start := time.Now()
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	elapsed := time.Since(start)
	if got := drainReader(t, r); len(got) != 8 {
		t.Fatalf("delivered %d chunks, want 8", len(got))
	}
	starts, ends, _, _, _ := s.snapshot()
	for _, i := range []int{4, 5, 6} {
		if !starts[full[i]].Before(ends[full[3]]) {
			t.Fatalf("GET(%d) started at %v, not before GET(3) finished at %v",
				i, starts[full[i]], ends[full[3]])
		}
	}
	if elapsed > 800*time.Millisecond {
		t.Fatalf("8 segments took %v, want well under 800ms", elapsed)
	}
}

func TestWindowBacklogDrainsPipelined(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	const n = 20
	prefix := "streams/backlog"
	for i := 0; i < n; i++ {
		payload := make([]byte, driveInlineLimit+1)
		payload[0] = byte(i)
		if err := storage.Put(ctx, objectName(prefix, int64(i), segSuffix), payload); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	r := manualReader(storage, prefix, params{concurrency: 8, holeTimeout: time.Second})
	advanced, eof, err := r.poll()
	if err != nil {
		t.Fatalf("poll: %v", err)
	}
	if eof || !advanced {
		t.Fatalf("poll returned advanced=%v eof=%v", advanced, eof)
	}
	got := drainReader(t, r)
	if len(got) != n {
		t.Fatalf("delivered %d chunks, want %d", len(got), n)
	}
	for i, chunk := range got {
		if len(chunk) != driveInlineLimit+1 || chunk[0] != byte(i) {
			t.Fatalf("chunk %d corrupted or out of order", i)
		}
	}
	orLists, _ := driveCounts(drive)
	if orLists != 1 {
		t.Fatalf("backlog drain issued %d probes, want exactly the initial 1", orLists)
	}
}
