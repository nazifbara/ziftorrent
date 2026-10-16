package peer

import (
	"context"
	"testing"
	"time"
)

// startWaitUnchoked runs WaitUnchoked in the background and returns its result.
func startWaitUnchoked(p *Peer, ctx context.Context) <-chan bool {
	result := make(chan bool, 1)
	go func() { result <- p.WaitUnchoked(ctx) }()
	return result
}

func expectWaitResult(t *testing.T, result <-chan bool, want bool) {
	t.Helper()
	select {
	case got := <-result:
		if got != want {
			t.Fatalf("expected WaitUnchoked to return %v, got %v", want, got)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitUnchoked did not return")
	}
}

func expectStillWaiting(t *testing.T, result <-chan bool) {
	t.Helper()
	select {
	case got := <-result:
		t.Fatalf("expected WaitUnchoked to keep waiting, it returned %v", got)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestNewPeerStartsChoked(t *testing.T) {
	p := New(nil, nil, 1)
	p.SetBitfield([]bool{true})

	if p.CanRequest(0) {
		t.Fatal("expected a new peer to start choked")
	}
	expectStillWaiting(t, startWaitUnchoked(p, context.Background()))
}

func TestWaitUnchokedReturnsOnUnchoke(t *testing.T) {
	p := New(nil, nil, 1)
	result := startWaitUnchoked(p, context.Background())
	expectStillWaiting(t, result)

	p.SetChoked(false)

	expectWaitResult(t, result, true)
}

func TestWaitUnchokedReturnsImmediatelyWhenUnchoked(t *testing.T) {
	p := New(nil, nil, 1)
	p.SetChoked(false)

	expectWaitResult(t, startWaitUnchoked(p, context.Background()), true)
}

func TestWaitUnchokedReturnsFalseOnCancel(t *testing.T) {
	p := New(nil, nil, 1)
	ctx, cancel := context.WithCancel(context.Background())
	result := startWaitUnchoked(p, ctx)

	cancel()

	expectWaitResult(t, result, false)
}

func TestWaitUnchokedReturnsFalseWhenPeerDone(t *testing.T) {
	p := New(nil, nil, 1)
	result := startWaitUnchoked(p, context.Background())

	p.markDone()

	expectWaitResult(t, result, false)
}

func TestUnchokeTwiceDoesNotPanic(t *testing.T) {
	p := New(nil, nil, 1)
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("expected a repeated unchoke to be ignored, got panic: %v", r)
		}
	}()

	p.SetChoked(false)
	p.SetChoked(false)
}

func TestRepeatedChokeDoesNotStrandWaiter(t *testing.T) {
	p := New(nil, nil, 1)
	p.SetChoked(false)
	p.SetChoked(true)
	result := startWaitUnchoked(p, context.Background())
	expectStillWaiting(t, result)

	// A second choke must not swap out the channel the waiter is blocked on.
	p.SetChoked(true)
	p.SetChoked(false)

	expectWaitResult(t, result, true)
}

func TestChokeAfterUnchokeBlocksNewWaiters(t *testing.T) {
	p := New(nil, nil, 1)
	p.SetChoked(false)
	p.SetChoked(true)

	if p.CanRequest(0) {
		t.Fatal("expected the peer to be choked again")
	}
	expectStillWaiting(t, startWaitUnchoked(p, context.Background()))
}
