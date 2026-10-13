package peer

import (
	"sync"
	"time"
)

type rttTracker struct {
	mu      sync.Mutex
	samples []time.Duration
	sum     time.Duration
	maxLen  int
}

func newRTTTracker(maxLen int) *rttTracker {
	return &rttTracker{maxLen: maxLen}
}

func (r *rttTracker) Record(sample time.Duration) {
	if sample <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.samples = append(r.samples, sample)
	r.sum += sample
	if len(r.samples) > r.maxLen {
		r.sum -= r.samples[0]
		r.samples = r.samples[1:]
	}
}

func (r *rttTracker) Timeout() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.samples) == 0 {
		return 8 * time.Second
	}
	avg := r.sum / time.Duration(len(r.samples))
	t := avg * 4
	t = max(t, 2*time.Second)
	t = min(t, 10*time.Second)
	return t
}
