package peer

import (
	"context"
	"testing"
	"time"
)

// fullPeer returns a peer whose request slots are all held, so AcquireSlot can
// only return through its cancellation cases.
func fullPeer(t *testing.T) *Peer {
	t.Helper()
	p := New(nil, nil, 1)
	for range maxInflightRequest {
		if !p.AcquireSlot(context.Background()) {
			t.Fatal("failed to fill request slots")
		}
	}
	return p
}

func assertAcquireFailsWithoutReleasing(t *testing.T, p *Peer, ctx context.Context) {
	t.Helper()
	result := make(chan bool, 1)
	go func() { result <- p.AcquireSlot(ctx) }()
	select {
	case ok := <-result:
		if ok {
			t.Fatal("expected AcquireSlot to fail when it cannot get a slot")
		}
	case <-time.After(time.Second):
		t.Fatal("AcquireSlot blocked instead of returning")
	}
	if got := p.InflightRequests(); got != maxInflightRequest {
		t.Fatalf("expected a failed AcquireSlot to leave %d slots held, got %d", maxInflightRequest, got)
	}
}

func TestAcquireSlotOnCancelDoesNotReleaseHeldSlot(t *testing.T) {
	p := fullPeer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	assertAcquireFailsWithoutReleasing(t, p, ctx)
}

func TestAcquireSlotOnPeerDoneDoesNotReleaseHeldSlot(t *testing.T) {
	p := fullPeer(t)
	p.markDone()

	assertAcquireFailsWithoutReleasing(t, p, context.Background())
}
