package download

import (
	"fmt"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
)

// peerFailureLimit is the number of consecutive failures a peer survives;
// the next one closes it.
const peerFailureLimit = 5

func isClosed(p *peer.Peer) bool {
	select {
	case <-p.Done():
		return true
	default:
		return false
	}
}

// timeOutRequest registers a request sent to p and lets it time out. Each call
// uses a fresh key so it never collides with real blocks.
func timeOutRequest(t *testing.T, d *Download, p *peer.Peer) {
	t.Helper()
	acquireSlot(t, p)
	d.mu.Lock()
	n := len(d.pending)
	d.mu.Unlock()
	j := &job{index: 1000, begin: uint32(n)}
	key := fmt.Sprintf("timeout:%p:%d:%d", p, n, time.Now().UnixNano())
	d.mu.Lock()
	d.pending[key] = &pendingRequest{job: j, peer: p, timer: time.NewTimer(time.Hour)}
	d.mu.Unlock()
	within(t, func() { d.onTimeout(key, j) })
}

// newFailureTestClient is a block test client whose queue can absorb every
// requeued job without spawning goroutines.
func newFailureTestClient(t *testing.T, content []byte) *Download {
	t.Helper()
	c := newBlockTestClient(t, content)
	c.queue = make(chan *job, 64)
	return c
}

func TestRepeatedTimeoutsCloseStallingPeer(t *testing.T) {
	d := newFailureTestClient(t, []byte("0123456789"))
	p := newInflightPeer(t, nil)

	for range peerFailureLimit {
		timeOutRequest(t, d, p)
	}
	if isClosed(p) {
		t.Fatalf("expected the peer to survive %d timeouts", peerFailureLimit)
	}

	timeOutRequest(t, d, p)
	if !isClosed(p) {
		t.Fatalf("expected the peer to be closed after %d timeouts", peerFailureLimit+1)
	}
}

func TestStaleTimeoutIsNotAFailure(t *testing.T) {
	content := []byte("0123456789")
	d := newFailureTestClient(t, content)
	p := newSentPeer(t)
	key := pendingKey(0, 0)
	j := d.pending[key].job
	d.pending[key].peer = p

	within(t, func() { d.onBlockReceived(p, blockMessage(t, 0, 0, content[:5])) })
	for range peerFailureLimit + 1 {
		within(t, func() { d.onTimeout(key, j) })
	}

	if isClosed(p) {
		t.Fatal("expected timeouts for an already received block not to count as failures")
	}
}

func TestReceivedBlockResetsTimeoutFailures(t *testing.T) {
	content := []byte("0123456789")
	d := newFailureTestClient(t, content)
	p := newInflightPeer(t, nil)

	for range peerFailureLimit {
		timeOutRequest(t, d, p)
	}
	acquireSlot(t, p)
	d.pending[pendingKey(0, 0)].peer = p
	within(t, func() { d.onBlockReceived(p, blockMessage(t, 0, 0, content[:5])) })
	for range peerFailureLimit {
		timeOutRequest(t, d, p)
	}

	if isClosed(p) {
		t.Fatal("expected a received block to reset the peer's timeout failures")
	}
}

func TestLateBlockDoesNotResetPreviousPeerFailures(t *testing.T) {
	content := []byte("0123456789")
	d := newFailureTestClient(t, content)
	a := newInflightPeer(t, nil)

	for range peerFailureLimit {
		timeOutRequest(t, d, a)
	}
	// The block is now owned by b; a's late copy satisfies b's request.
	d.pending[pendingKey(0, 0)].peer = newSentPeer(t)
	within(t, func() { d.onBlockReceived(a, blockMessage(t, 0, 0, content[:5])) })
	timeOutRequest(t, d, a)

	if !isClosed(a) {
		t.Fatal("expected a block answering another peer's request not to reset a's failures")
	}
}

func TestTimeoutFailureCountsAgainstRequestOwner(t *testing.T) {
	d := newFailureTestClient(t, []byte("0123456789"))
	owner := newInflightPeer(t, nil)
	other := newInflightPeer(t, nil)

	for range peerFailureLimit + 1 {
		timeOutRequest(t, d, owner)
	}

	if !isClosed(owner) {
		t.Fatal("expected the peer that owned the timed-out requests to be closed")
	}
	if isClosed(other) {
		t.Fatal("expected an unrelated peer not to be affected by another peer's timeouts")
	}
}
