package xdrive

import (
	"bytes"
	"context"
	"io"
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

	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	drive.mu.Lock()
	probeQuery := drive.lastQuery
	drive.mu.Unlock()
	if !strings.Contains(probeQuery, "' in parents and trashed = false and (name = '") {
		t.Fatalf("probe query missing parenthesised group: %q", probeQuery)
	}

	for i := 0; i < 19; i++ {
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

func TestProbeHotMissesSkipSafetyNet(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/hot", 0, segSuffix), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	r := manualReader(storage, "streams/hot", params{concurrency: 8, holeTimeout: 100 * time.Millisecond})
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	baseOr, baseContains := driveCounts(drive)

	for i := 0; i < 20; i++ {
		if _, _, err := r.poll(); err != nil {
			t.Fatalf("poll: %v", err)
		}
	}
	orLists, containsLists := driveCounts(drive)
	if orLists-baseOr != 20 || containsLists != baseContains {
		t.Fatalf("hot gap issued %d probes and %d full lists, want 20 and %d",
			orLists-baseOr, containsLists-baseContains, baseContains)
	}

	time.Sleep(2500 * time.Millisecond)
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	time.Sleep(1200 * time.Millisecond)
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	orLists, containsLists = driveCounts(drive)
	if orLists-baseOr != 22 || containsLists != baseContains+1 {
		t.Fatalf("cold gap issued %d probes and %d full lists, want 22 and %d",
			orLists-baseOr, containsLists-baseContains, baseContains+1)
	}
}

func TestProbeHotWindowFollowsEagerWindow(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/hotwin", 0, segSuffix), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	r := manualReader(storage, "streams/hotwin", params{concurrency: 8, holeTimeout: 100 * time.Millisecond})
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	r.eagerWindow = 3 * time.Second

	time.Sleep(2500 * time.Millisecond)
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	time.Sleep(1100 * time.Millisecond)
	if _, _, err := r.poll(); err != nil {
		t.Fatalf("poll: %v", err)
	}
	if _, containsLists := driveCounts(drive); containsLists != 0 {
		t.Fatalf("hot probes ran %d full lists, want 0", containsLists)
	}
}

func TestProbeHedgeBeatsSlowQuery(t *testing.T) {
	drive := newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	if err := storage.Put(ctx, objectName("streams/hedge", 0, segSuffix), []byte("x")); err != nil {
		t.Fatalf("Put: %v", err)
	}

	drive.mu.Lock()
	drive.listDelay = 1500 * time.Millisecond
	drive.mu.Unlock()

	start := time.Now()
	entries, err := storage.probe(ctx, "streams/hedge", 0)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("probe returned %d entries, want 1", len(entries))
	}
	if d := time.Since(start); d >= 1300*time.Millisecond {
		t.Fatalf("probe took %v, want under 1.3s", d)
	}
	waitFor(t, "hedged query to land", func() bool {
		orLists, _ := driveCounts(drive)
		return orLists == 2
	})
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

func TestProbeWindow(t *testing.T) {
	_ = newFakeDrive(t)
	storage := newDriveBackend(t)
	ctx := context.Background()

	const n = 10
	for i := 0; i < n; i++ {
		if err := storage.Put(ctx, objectName("streams/w", int64(i), segSuffix), []byte{byte(i)}); err != nil {
			t.Fatalf("Put: %v", err)
		}
	}

	entries, err := storage.probe(ctx, "streams/w", 0)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if len(entries) != n {
		t.Fatalf("probe returned %d entries, want %d", len(entries), n)
	}
	for i, entry := range entries {
		if want := objectName("streams/w", int64(i), segSuffix)[len("streams/w/"):]; entry.Name != want {
			t.Fatalf("entry %d = %q, want %q", i, entry.Name, want)
		}
	}

	if _, err := storage.probe(ctx, "streams/empty", 0); err != errNotFound {
		t.Fatalf("probe returned %v, want errNotFound", err)
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
		t.Fatalf("hole discovery ran a full list")
	}

	time.Sleep(300 * time.Millisecond)
	_, _, err := r.poll()
	if err == nil || !strings.Contains(err.Error(), "never arrived") {
		t.Fatalf("poll returned %v, want hole error", err)
	}
	if _, containsLists := driveCounts(drive); containsLists != 0 {
		t.Fatalf("hole detection needed a full list")
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
	putStarts   map[string]time.Time
	putCalls    int
}

func (s *stubStorage) Put(ctx context.Context, name string, data []byte) error {
	s.mu.Lock()
	if s.putStarts == nil {
		s.putStarts = make(map[string]time.Time)
	}
	s.putStarts[name] = time.Now()
	s.putCalls++
	s.mu.Unlock()

	select {
	case <-time.After(200 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[name] = cp
	return nil
}

func (s *stubStorage) putStats() (map[string]time.Time, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	starts := make(map[string]time.Time, len(s.putStarts))
	for k, v := range s.putStarts {
		starts[k] = v
	}
	return starts, s.putCalls
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

var errProbeBoom testError = "injected probe failure"

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
	starts, ends, _, _, listCalls := s.snapshot()
	t.Logf("listCalls=%d elapsed=%v", listCalls, elapsed)
	gap := lookaheadMinGap
	if maxListCalls := int(elapsed/gap) + 3; listCalls > maxListCalls {
		t.Fatalf("listCalls=%d exceeds elapsed/gap+3=%d", listCalls, maxListCalls)
	}
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
	orLists, containsLists := driveCounts(drive)
	if orLists < 2 || orLists > 4 || containsLists != 0 {
		t.Fatalf("backlog drain issued %d probes and %d full lists, want 2-4 probes and 0 full lists", orLists, containsLists)
	}
}

func TestPartialCapBoundsFlushes(t *testing.T) {
	const prefix = "streams/partial"
	const total = 200
	run := func() (int, []byte) {
		s := &stubStorage{data: make(map[string][]byte)}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		w := newWALWriter(ctx, s, prefix, params{
			segmentBytes:  1 << 20,
			flushInterval: 45 * time.Millisecond,
			concurrency:   8,
		})
		tick := time.NewTicker(5 * time.Millisecond)
		defer tick.Stop()
		for i := 0; i < total; i++ {
			<-tick.C
			if _, err := w.Write([]byte{byte(i)}); err != nil {
				t.Fatalf("Write: %v", err)
			}
		}
		if err := w.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		_, puts := s.putStats()
		var got []byte
		for seq := 0; ; seq++ {
			chunk, ok := s.data[objectName(prefix, int64(seq), segSuffix)]
			if !ok {
				break
			}
			got = append(got, chunk...)
		}
		return puts, got
	}
	check := func(got []byte) {
		t.Helper()
		if len(got) != total {
			t.Fatalf("reassembled %d bytes, want %d", len(got), total)
		}
		for i := range got {
			if got[i] != byte(i) {
				t.Fatalf("byte %d = %d, want %d", i, got[i], byte(i))
			}
		}
	}

	putsCapped, gotCapped := run()
	check(gotCapped)
	if putsCapped > 14 {
		t.Fatalf("capped run issued %d Puts, want <= 14", putsCapped)
	}

	oldCap := maxPartialInflight
	maxPartialInflight = 1 << 30
	defer func() { maxPartialInflight = oldCap }()
	putsHuge, gotHuge := run()
	check(gotHuge)

	t.Logf("puts with cap=%d: %d, without cap: %d", oldCap, putsCapped, putsHuge)
}

type growStub struct {
	*stubStorage
}

func (g *growStub) shouldGrow() bool {
	return true
}

func TestGrowingFullSegmentsBypassCap(t *testing.T) {
	const prefix = "streams/growfull"
	interval := 20 * time.Millisecond
	base := &stubStorage{data: make(map[string][]byte)}
	s := &growStub{base}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newWALWriter(ctx, s, prefix, params{
		segmentBytes:  1024,
		flushInterval: interval,
		concurrency:   8,
		growth:        2,
	})
	if _, err := w.Write(make([]byte, 1024+2048+4096+8192)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := w.Write([]byte{0xBB}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	start := time.Now()
	trail := objectName(prefix, 4, segSuffix)
	for {
		starts, _ := base.putStats()
		if ts, ok := starts[trail]; ok {
			if d := ts.Sub(start); d > 2*interval {
				t.Fatalf("trailing Put started after %v, want within %v", d, 2*interval)
			}
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("trailing Put never started")
		}
		time.Sleep(time.Millisecond)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	w.mu.Lock()
	partial := w.partial
	w.mu.Unlock()
	if partial != 0 {
		t.Fatalf("w.partial = %d, want 0 after Close", partial)
	}
}

func TestPartialCapSkipsFullSegments(t *testing.T) {
	const prefix = "streams/trailing"
	const segBytes = 1024
	const full = 32
	interval := 20 * time.Millisecond
	s := &stubStorage{data: make(map[string][]byte)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w := newWALWriter(ctx, s, prefix, params{
		segmentBytes:  segBytes,
		flushInterval: interval,
		concurrency:   40,
	})
	bulk := make([]byte, full*segBytes)
	for i := range bulk {
		bulk[i] = byte(i)
	}
	if _, err := w.Write(bulk); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if _, err := w.Write([]byte{0xAA}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	start := time.Now()
	trail := objectName(prefix, full, segSuffix)
	for {
		starts, _ := s.putStats()
		if ts, ok := starts[trail]; ok {
			if d := ts.Sub(start); d > 2*interval {
				t.Fatalf("trailing Put started after %v, want within %v", d, 2*interval)
			}
			break
		}
		if time.Since(start) > 5*time.Second {
			t.Fatal("trailing Put never started")
		}
		time.Sleep(time.Millisecond)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

type probeFlapStorage struct {
	mu        sync.Mutex
	failUntil time.Time
	failErr   error
	entries   []Entry
	data      map[string][]byte
}

func (s *probeFlapStorage) probe(_ context.Context, _ string, _ int64) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if time.Now().Before(s.failUntil) {
		return nil, s.failErr
	}
	return s.entries, nil
}

func (s *probeFlapStorage) Put(_ context.Context, name string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[name] = cp
	return nil
}

func (s *probeFlapStorage) Get(_ context.Context, name string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cp, ok := s.data[name]; ok {
		return append([]byte(nil), cp...), nil
	}
	return nil, errNotFound
}

func (s *probeFlapStorage) Delete(_ context.Context, _ string) error {
	return nil
}

func (s *probeFlapStorage) List(_ context.Context, _ string) ([]Entry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.entries, nil
}

func (s *probeFlapStorage) Close() error {
	return nil
}

func TestReaderRidesOutTransientProbeFailure(t *testing.T) {
	prefix := "streams/flap"
	_, rel := stubNames(prefix, 4)
	var entries []Entry
	var want []byte
	for i, name := range rel {
		chunk := []byte{byte(i), byte(i + 1)}
		want = append(want, chunk...)
		entries = append(entries, Entry{Name: name, Inline: chunk})
	}
	entries = append(entries, Entry{Name: objectName("", 4, endSuffix)[1:]})
	s := &probeFlapStorage{
		failUntil: time.Now().Add(3 * time.Second),
		failErr:   errProbeBoom,
		entries:   entries,
		data:      make(map[string][]byte),
	}
	p := params{concurrency: 4, minPollInterval: 5 * time.Millisecond, maxPollInterval: 20 * time.Millisecond, eagerWindow: time.Second, holeTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	r := newWALReader(ctx, s, prefix, p)
	var got []byte
	for chunk := range r.ch {
		got = append(got, chunk...)
	}
	if d := time.Since(start); d < 2500*time.Millisecond {
		t.Fatalf("reader finished in %v, want it to ride out the 3s outage", d)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("delivered %v, want %v", got, want)
	}
	if err := r.Err(); err != io.EOF {
		t.Fatalf("Err returned %v, want EOF", err)
	}
}

func TestReaderGivesUpAfterGracePeriod(t *testing.T) {
	previousGrace := readerErrorGracePeriod
	readerErrorGracePeriod = 300 * time.Millisecond
	defer func() { readerErrorGracePeriod = previousGrace }()

	s := &probeFlapStorage{
		failUntil: time.Now().Add(time.Hour),
		failErr:   errProbeBoom,
		data:      make(map[string][]byte),
	}
	p := params{concurrency: 4, minPollInterval: 5 * time.Millisecond, maxPollInterval: 20 * time.Millisecond, eagerWindow: time.Second, holeTimeout: time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	r := newWALReader(ctx, s, "streams/grace", p)
	n := 0
	for range r.ch {
		n++
	}
	if n != 0 {
		t.Fatalf("delivered %d chunks during a persistent outage", n)
	}
	if err := r.Err(); err != errProbeBoom {
		t.Fatalf("Err returned %v, want the probe failure", err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("reader took %v to give up", d)
	}
}
