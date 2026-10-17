package download

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
)

// newInflightPeer returns a peer that is unchoked and has piece 0, so
// CanRequest(0) only depends on its choke state.
func newInflightPeer(t *testing.T, conn *net.TCPConn) *peer.Peer {
	t.Helper()
	p := peer.New(conn, nil, 1)
	p.SetBitfield([]bool{true})
	p.SetChoked(false)
	return p
}

// newSentPeer returns a peer holding one slot, as if it had just been sent a
// request.
func newSentPeer(t *testing.T) *peer.Peer {
	t.Helper()
	p := newInflightPeer(t, nil)
	acquireSlot(t, p)
	return p
}

func acquireSlot(t *testing.T, p *peer.Peer) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if !p.AcquireSlot(ctx) {
		t.Fatal("failed to acquire a request slot; all slots are held")
	}
}

// within fails the test if fn does not return in time. ReleaseSlot blocks when
// no slot is held, so a double release shows up as a hang.
func within(t *testing.T, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		fn()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("call blocked; a slot was probably released without being held")
	}
}

// tcpConnPair returns the client side of a loopback TCP connection; the server
// side is drained so writes do not block.
func tcpConnPair(t *testing.T) *net.TCPConn {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		server, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { server.Close() })
		buf := make([]byte, 1024)
		for {
			if _, err := server.Read(buf); err != nil {
				return
			}
		}
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn.(*net.TCPConn)
}

func stopPendingTimers(t *testing.T, d *Download) {
	t.Helper()
	t.Cleanup(func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		for _, pr := range d.pending {
			if pr.timer != nil {
				pr.timer.Stop()
			}
		}
	})
}

func TestTryRequestPeerKeepsSlotForSentRequest(t *testing.T) {
	d := newQueueDownload(t, 1)
	stopPendingTimers(t, d)
	p := newInflightPeer(t, tcpConnPair(t))
	acquireSlot(t, p)

	within(t, func() { d.tryRequestPeer(p, &job{index: 0, begin: 0}) })

	if got := p.InflightRequests(); got != 1 {
		t.Fatalf("expected 1 in-flight request after a successful send, got %d", got)
	}
}

func TestTryRequestPeerReleasesSlotOnFailedWrite(t *testing.T) {
	d := newQueueDownload(t, 1)
	conn := tcpConnPair(t)
	conn.Close()
	p := newInflightPeer(t, conn)
	acquireSlot(t, p)

	within(t, func() { d.tryRequestPeer(p, &job{index: 0, begin: 0}) })

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected a failed write to release its slot, got %d in flight", got)
	}
	if len(d.pending) != 0 {
		t.Fatalf("expected no pending request after a failed write, got %d", len(d.pending))
	}
}

func TestTryRequestPeerReleasesSlotOnRefusedRequest(t *testing.T) {
	d := newQueueDownload(t, 1)
	p := newInflightPeer(t, tcpConnPair(t))
	p.SetChoked(true)
	acquireSlot(t, p)

	within(t, func() { d.tryRequestPeer(p, &job{index: 0, begin: 0}) })

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected a request refused by CanRequest to release its slot, got %d in flight", got)
	}
}

func TestBlockReceivedReleasesInflightRequest(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newSentPeer(t)
	c.pending[pendingKey(0, 0)].peer = p

	within(t, func() { c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5])) })

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected the received block to release its request, got %d in flight", got)
	}
}

func TestTimeoutReleasesInflightRequest(t *testing.T) {
	d := newQueueDownload(t, 1)
	p := newSentPeer(t)
	j := &job{index: 0, begin: 0}
	key := pendingKey(j.index, j.begin)
	d.pending[key] = &pendingRequest{job: j, peer: p, timer: time.NewTimer(time.Hour)}

	within(t, func() { d.onTimeout(key, j) })

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected the timeout to release its request, got %d in flight", got)
	}
}

func TestLateBlockAfterTimeoutDoesNotReleaseTwice(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newSentPeer(t)
	key := pendingKey(0, 0)
	c.pending[key].peer = p

	within(t, func() {
		c.onTimeout(key, c.pending[key].job)
		c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5]))
	})

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected 0 in flight after a timeout and a late block, got %d", got)
	}
}

func TestTimeoutAfterBlockDoesNotReleaseTwice(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newSentPeer(t)
	key := pendingKey(0, 0)
	j := c.pending[key].job
	c.pending[key].peer = p

	within(t, func() {
		c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5]))
		c.onTimeout(key, j)
	})

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected 0 in flight after a block and a stale timeout, got %d", got)
	}
}

func TestLateBlockFromPreviousPeerReleasesCurrentOwner(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	a := newSentPeer(t)
	key := pendingKey(0, 0)
	c.pending[key].peer = a

	// a's request times out and the job is handed to b.
	j := c.pending[key].job
	within(t, func() { c.onTimeout(key, j) })
	b := newSentPeer(t)
	c.pending[key] = &pendingRequest{job: j, peer: b, timer: time.NewTimer(time.Hour)}

	// a's block shows up late and satisfies b's pending entry.
	within(t, func() { c.onBlockReceived(a, blockMessage(t, 0, 0, content[:5])) })

	if got := a.InflightRequests(); got != 0 {
		t.Fatalf("expected the previous peer to stay at 0 in flight, got %d", got)
	}
	if got := b.InflightRequests(); got != 0 {
		t.Fatalf("expected the current owner's request to be released, got %d in flight", got)
	}
}

func TestUnsolicitedBlockDoesNotReleaseInflightRequest(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newSentPeer(t)
	delete(c.pending, pendingKey(0, 0))

	within(t, func() { c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5])) })

	if got := p.InflightRequests(); got != 1 {
		t.Fatalf("expected an unrequested block not to release anything, got %d in flight", got)
	}
}

// startPeerWorker runs runPeerWorker and returns a channel closed when it exits.
func startPeerWorker(d *Download, p *peer.Peer) <-chan struct{} {
	exited := make(chan struct{})
	go func() {
		d.runPeerWorker(p)
		close(exited)
	}()
	return exited
}

func TestPeerWorkerExitsOnCancelWithEmptyQueue(t *testing.T) {
	d := newQueueDownload(t, 1)
	p := newInflightPeer(t, tcpConnPair(t))
	exited := startPeerWorker(d, p)

	d.cancel()

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("peer worker kept waiting on an empty queue after cancel")
	}
}

func TestPeerWorkerRequeuesAddressWhenPeerCloses(t *testing.T) {
	d := newQueueDownload(t, 1)
	d.addrQueue = make(chan *net.TCPAddr, 1)
	conn := tcpConnPair(t)
	addr := conn.RemoteAddr().(*net.TCPAddr)
	p := peer.New(conn, addr, 1)
	exited := startPeerWorker(d, p)

	p.Close()

	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("peer worker kept waiting on an empty queue after the peer closed")
	}
	select {
	case got := <-d.addrQueue:
		if !p.MatchesAddress(got) {
			t.Fatalf("expected %v to be requeued, got %v", addr, got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected the closed peer's address to be requeued")
	}
}
