package download

import (
	"testing"

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

// timeOutRequest sends p a request and lets it time out.
func timeOutRequest(t *testing.T, d *Download, p *peer.Peer) {
	t.Helper()
	key := addPending(t, d, p)
	within(t, func() { d.onTimeout(key) })
}

func TestRepeatedTimeoutsCloseStallingPeer(t *testing.T) {
	d := newTestDownload(t, 1)
	p := newInflightPeer(t, d, nil)

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
	d := newBlockTestClient(t, content)
	p := newInflightPeer(t, d, nil)
	key := addPending(t, d, p)

	within(t, func() { d.onBlockReceived(p, blockMessage(t, 0, 0, content[:5])) })
	for range peerFailureLimit + 1 {
		within(t, func() { d.onTimeout(key) })
	}

	if isClosed(p) {
		t.Fatal("expected timeouts for an already received block not to count as failures")
	}
}

func TestReceivedBlockResetsTimeoutFailures(t *testing.T) {
	content := []byte("0123456789")
	d := newBlockTestClient(t, content)
	p := newInflightPeer(t, d, nil)

	for range peerFailureLimit {
		timeOutRequest(t, d, p)
	}
	key := addPending(t, d, p)
	within(t, func() { d.onBlockReceived(p, blockMessage(t, 0, uint32(key.begin), content[key.begin:key.begin+5])) })
	for range peerFailureLimit {
		timeOutRequest(t, d, p)
	}

	if isClosed(p) {
		t.Fatal("expected a received block to reset the peer's timeout failures")
	}
}

func TestLateBlockDoesNotResetPreviousPeerFailures(t *testing.T) {
	content := []byte("0123456789")
	d := newBlockTestClient(t, content)
	a := newInflightPeer(t, d, nil)

	for range peerFailureLimit {
		timeOutRequest(t, d, a)
	}
	// The block is now requested from b; a's late copy answers b's request.
	b := newInflightPeer(t, d, newConn(t))
	key := addPending(t, d, b)
	within(t, func() { d.onBlockReceived(a, blockMessage(t, 0, uint32(key.begin), content[key.begin:key.begin+5])) })
	timeOutRequest(t, d, a)

	if !isClosed(a) {
		t.Fatal("expected a block answering another peer's request not to reset a's failures")
	}
}

func TestTimeoutFailureCountsAgainstRequestOwner(t *testing.T) {
	d := newTestDownload(t, 1)
	owner := newInflightPeer(t, d, nil)
	other := newInflightPeer(t, d, nil)

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
