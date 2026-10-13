package xdrive

import (
	"context"
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
