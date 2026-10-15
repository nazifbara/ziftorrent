package peer

import (
	"sync"
	"testing"
	"time"
)

func TestRTTTrackerDefaultTimeout(t *testing.T) {
	r := newRTTTracker(10)
	got := r.Timeout()
	want := 8 * time.Second
	if got != want {
		t.Fatalf("expected default timeout %v, got %v", want, got)
	}
}

func TestRTTTrackerIgnoresNonPositiveSamples(t *testing.T) {
	r := newRTTTracker(10)
	r.Record(0)
	r.Record(-5 * time.Millisecond)
	got := r.Timeout()
	want := 8 * time.Second
	if got != want {
		t.Fatalf("expected default timeout after ignored samples, got %v", got)
	}
}

func TestRTTTrackerAverageWithinBounds(t *testing.T) {
	r := newRTTTracker(10)
	r.Record(1 * time.Second)
	r.Record(2 * time.Second)
	// avg = 1.5s, timeout = avg*4 = 6s, within [2s, 20s]
	got := r.Timeout()
	want := 6 * time.Second
	if got != want {
		t.Fatalf("expected timeout %v, got %v", want, got)
	}
}

func TestRTTTrackerEvictsOldestSampleBeyondMaxLen(t *testing.T) {
	r := newRTTTracker(2)
	r.Record(1 * time.Second)
	r.Record(2 * time.Second)
	r.Record(3 * time.Second)
	// oldest (1s) should have been evicted, leaving [2s, 3s] -> avg 2.5s -> timeout 10s
	got := r.Timeout()
	want := 10 * time.Second
	if got != want {
		t.Fatalf("expected timeout %v after eviction, got %v", want, got)
	}
	if len(r.samples) != 2 {
		t.Fatalf("expected 2 retained samples, got %d", len(r.samples))
	}
}

func TestRTTTrackerClampsLowTimeout(t *testing.T) {
	r := newRTTTracker(10)
	r.Record(10 * time.Millisecond)
	got := r.Timeout()
	want := 2 * time.Second
	if got != want {
		t.Fatalf("expected clamped minimum timeout %v, got %v", want, got)
	}
}

func TestRTTTrackerClampsHighTimeout(t *testing.T) {
	r := newRTTTracker(10)
	r.Record(10 * time.Second)
	got := r.Timeout()
	want := 10 * time.Second
	if got != want {
		t.Fatalf("expected clamped maximum timeout %v, got %v", want, got)
	}
}

func TestRTTTrackerConcurrentRecordIsSafe(t *testing.T) {
	r := newRTTTracker(50)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			r.Record(time.Duration(n+1) * time.Millisecond)
		}(i)
	}
	wg.Wait()
	// Just verifying no race/panic and a sane result is produced.
	// Run with `go test -race` to actually exercise the concurrency guarantee.
	if got := r.Timeout(); got <= 0 {
		t.Fatalf("expected a positive timeout, got %v", got)
	}
}
