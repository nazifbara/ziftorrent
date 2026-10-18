package download

import (
	"net"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/wire"
)

// newChokedWorkerSetup returns a download of 10 pieces (20 blocks) and a
// choked peer that has all of them and a writable connection.
func newChokedWorkerSetup(t *testing.T) (*Download, *peer.Peer) {
	t.Helper()
	d := newTestDownload(t, 10)
	stopPendingTimers(t, d)
	p := newInflightPeer(t, d, newConn(t))
	p.SetChoked(true)
	return d, p
}

func TestPeerWorkerSendsNothingWhileChoked(t *testing.T) {
	d, p := newChokedWorkerSetup(t)
	exited := startPeerWorker(d, p)

	time.Sleep(100 * time.Millisecond)

	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected no requests to a choked peer, got %d pending", got)
	}
	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected no slots held while choked, got %d", got)
	}

	d.cancel()
	expectExit(t, exited, "peer worker kept waiting for an unchoke after cancel")
}

func TestPeerWorkerSendsRequestsAfterUnchoke(t *testing.T) {
	d, p := newChokedWorkerSetup(t)
	exited := startPeerWorker(d, p)
	t.Cleanup(func() {
		d.cancel()
		<-exited
	})

	p.SetChoked(false)

	waitFor(t, func() bool { return pendingCount(d) == 15 })
	if got := pendingCount(d); got != 15 {
		t.Fatalf("expected the worker to fill all 15 slots after an unchoke, got %d pending", got)
	}
	if got := p.InflightRequests(); got != 15 {
		t.Fatalf("expected 15 slots held, got %d", got)
	}
}

func choke(t *testing.T, d *Download, p *peer.Peer) {
	t.Helper()
	within(t, func() { d.onMessage(p, wire.BuildChoke()) })
}

func TestChokeDropsPendingRequests(t *testing.T) {
	d := newTestDownload(t, 2)
	p := newInflightPeer(t, d, nil)
	for range 3 {
		addPending(t, d, p)
	}

	choke(t, d, p)

	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected a choke to drop the peer's pending requests, %d left", got)
	}
	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected a choke to release the peer's slots, %d held", got)
	}
	other := newInflightPeer(t, d, nil)
	for range 4 {
		pickFor(t, d, other)
	}
}

func TestChokeStopsDroppedRequestTimers(t *testing.T) {
	d := newTestDownload(t, 1)
	p := newInflightPeer(t, d, nil)
	key := addPending(t, d, p)
	d.mu.Lock()
	timer := d.pending[key].timer
	d.mu.Unlock()

	choke(t, d, p)

	if timer.Stop() {
		t.Fatal("expected the dropped request's timer to be stopped already")
	}
}

func TestRepeatedChokesWithPendingRequestsDoNotClosePeer(t *testing.T) {
	d := newTestDownload(t, 2)
	p := newInflightPeer(t, d, nil)

	for range peerFailureLimit + 1 {
		p.SetChoked(false)
		for range 3 {
			addPending(t, d, p)
		}
		choke(t, d, p)
	}

	if isClosed(p) {
		t.Fatal("expected chokes not to count as failures")
	}
	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected every slot to be released, %d held", got)
	}
}

func TestChokeOnlyDropsChokingPeerRequests(t *testing.T) {
	d := newTestDownload(t, 2)
	a := newInflightPeer(t, d, nil)
	b := newInflightPeer(t, d, nil)
	addPending(t, d, a)
	addPending(t, d, a)
	bKeys := []pendingKey{addPending(t, d, b), addPending(t, d, b)}

	choke(t, d, a)

	if got := b.InflightRequests(); got != 2 {
		t.Fatalf("expected the other peer to keep its 2 slots, got %d", got)
	}
	if got := pendingCount(d); got != 2 {
		t.Fatalf("expected only the other peer's 2 requests to remain, got %d", got)
	}
	for _, key := range bKeys {
		if !hasPending(d, key) {
			t.Fatalf("expected the other peer's request %+v to stay pending", key)
		}
	}
}

func TestChokeDistinguishesPeersWithSameAddress(t *testing.T) {
	d := newTestDownload(t, 1)
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6881}
	// e.g. a reconnect while the old connection still has requests pending.
	old := peer.New(nil, addr, 1)
	current := peer.New(nil, addr, 1)
	for _, p := range []*peer.Peer{old, current} {
		p.SetChoked(false)
		d.picker.SetBitfield(p, []bool{true})
	}
	addPending(t, d, old)
	addPending(t, d, current)

	choke(t, d, current)

	if got := old.InflightRequests(); got != 1 {
		t.Fatalf("expected the other connection to keep its slot, got %d", got)
	}
	if got := current.InflightRequests(); got != 0 {
		t.Fatalf("expected the choking connection to release its slot, got %d", got)
	}
	if got := pendingCount(d); got != 1 {
		t.Fatalf("expected the other connection's request to stay pending, got %d pending", got)
	}
}

func TestTimeoutAfterChokeIsIgnored(t *testing.T) {
	d := newTestDownload(t, 1)
	p := newInflightPeer(t, d, nil)
	key := addPending(t, d, p)

	choke(t, d, p)
	for range peerFailureLimit + 1 {
		within(t, func() { d.onTimeout(key) })
	}

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected no double release, %d slots held", got)
	}
	if isClosed(p) {
		t.Fatal("expected timeouts of dropped requests not to count as failures")
	}
}

func TestChokeWithoutPendingRequestsIsNoop(t *testing.T) {
	d := newTestDownload(t, 1)
	a := newInflightPeer(t, d, nil)
	b := newInflightPeer(t, d, nil)
	addPending(t, d, b)

	choke(t, d, a)
	choke(t, d, a)

	if got := pendingCount(d); got != 1 {
		t.Fatalf("expected the other peer's request to stay pending, got %d", got)
	}
}
