package xdrive

import (
	"context"
	"strings"
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
