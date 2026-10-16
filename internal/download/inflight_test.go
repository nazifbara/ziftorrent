package download

import (
	"net"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
)

// newInflightPeer returns a peer that is unchoked and has piece 0, so
// CanRequest(0) only depends on its in-flight request count.
func newInflightPeer(t *testing.T, conn *net.TCPConn) *peer.Peer {
	t.Helper()
	p := peer.New(conn, nil, 1)
	p.SetBitfield([]bool{true})
	return p
}

// inflight infers the peer's in-flight request count from how many more
// requests CanRequest admits before hitting the cap, then restores it.
func inflight(t *testing.T, p *peer.Peer) int {
	t.Helper()
	const capacity = 15
	free := 0
	for p.CanRequest(0) {
		p.RecordRequest(1)
		free++
		if free > 10*capacity {
			t.Fatalf("in-flight count never reached the cap; it has drifted far below zero")
		}
	}
	p.RecordRequest(int32(-free))
	return capacity - free
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

func TestTryRequestPeerCountsSentRequest(t *testing.T) {
	d := newQueueDownload(t, 1)
	stopPendingTimers(t, d)
	p := newInflightPeer(t, tcpConnPair(t))

	d.tryRequestPeer(p, &job{index: 0, begin: 0})

	if got := inflight(t, p); got != 1 {
		t.Fatalf("expected 1 in-flight request after a successful send, got %d", got)
	}
}

func TestTryRequestPeerDoesNotCountFailedWrite(t *testing.T) {
	d := newQueueDownload(t, 1)
	conn := tcpConnPair(t)
	conn.Close()
	p := newInflightPeer(t, conn)

	d.tryRequestPeer(p, &job{index: 0, begin: 0})

	if got := inflight(t, p); got != 0 {
		t.Fatalf("expected a failed write not to count as in flight, got %d", got)
	}
	if len(d.pending) != 0 {
		t.Fatalf("expected no pending request after a failed write, got %d", len(d.pending))
	}
}

func TestTryRequestPeerDoesNotCountRefusedRequest(t *testing.T) {
	d := newQueueDownload(t, 1)
	p := newInflightPeer(t, tcpConnPair(t))
	p.SetChoked(true)

	d.tryRequestPeer(p, &job{index: 0, begin: 0})

	p.SetChoked(false)
	if got := inflight(t, p); got != 0 {
		t.Fatalf("expected a request refused by CanRequest not to count, got %d", got)
	}
}

func TestBlockReceivedReleasesInflightRequest(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, nil)
	c.pending[pendingKey(0, 0)].peer = p
	p.RecordRequest(1)

	c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5]))

	if got := inflight(t, p); got != 0 {
		t.Fatalf("expected the received block to release its request, got %d in flight", got)
	}
}

func TestTimeoutReleasesInflightRequest(t *testing.T) {
	d := newQueueDownload(t, 1)
	p := newInflightPeer(t, nil)
	j := &job{index: 0, begin: 0}
	key := pendingKey(j.index, j.begin)
	d.pending[key] = &pendingRequest{job: j, peer: p, timer: time.NewTimer(time.Hour)}
	p.RecordRequest(1)

	d.onTimeout(key, j)

	if got := inflight(t, p); got != 0 {
		t.Fatalf("expected the timeout to release its request, got %d in flight", got)
	}
}

func TestLateBlockAfterTimeoutDoesNotReleaseTwice(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, nil)
	key := pendingKey(0, 0)
	c.pending[key].peer = p
	p.RecordRequest(1)

	c.onTimeout(key, c.pending[key].job)
	c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5]))

	if got := inflight(t, p); got != 0 {
		t.Fatalf("expected 0 in flight after a timeout and a late block, got %d", got)
	}
}

func TestTimeoutAfterBlockDoesNotReleaseTwice(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, nil)
	key := pendingKey(0, 0)
	j := c.pending[key].job
	c.pending[key].peer = p
	p.RecordRequest(1)

	c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5]))
	c.onTimeout(key, j)

	if got := inflight(t, p); got != 0 {
		t.Fatalf("expected 0 in flight after a block and a stale timeout, got %d", got)
	}
}

func TestLateBlockFromPreviousPeerReleasesCurrentOwner(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	a := newInflightPeer(t, nil)
	b := newInflightPeer(t, nil)
	key := pendingKey(0, 0)
	c.pending[key].peer = a
	a.RecordRequest(1)

	// a's request times out and the job is handed to b.
	j := c.pending[key].job
	c.onTimeout(key, j)
	c.pending[key] = &pendingRequest{job: j, peer: b, timer: time.NewTimer(time.Hour)}
	b.RecordRequest(1)

	// a's block shows up late and satisfies b's pending entry.
	c.onBlockReceived(a, blockMessage(t, 0, 0, content[:5]))

	if got := inflight(t, a); got != 0 {
		t.Fatalf("expected the previous peer to stay at 0 in flight, got %d", got)
	}
	if got := inflight(t, b); got != 0 {
		t.Fatalf("expected the current owner's request to be released, got %d in flight", got)
	}
}

func TestUnsolicitedBlockDoesNotReleaseInflightRequest(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, nil)
	delete(c.pending, pendingKey(0, 0))
	p.RecordRequest(1)

	c.onBlockReceived(p, blockMessage(t, 0, 0, content[:5]))

	if got := inflight(t, p); got != 1 {
		t.Fatalf("expected an unrequested block not to release anything, got %d in flight", got)
	}
}
