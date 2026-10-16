package download

import (
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/peer"
)

// newChokedWorkerSetup returns a download with n queued jobs for piece 0 and a
// choked peer that has piece 0 and a writable connection.
func newChokedWorkerSetup(t *testing.T, n int) (*Download, *peer.Peer) {
	t.Helper()
	d := newQueueDownload(t, n)
	stopPendingTimers(t, d)
	for i := range n {
		d.enqueue(&job{index: 0, begin: uint32(i) * config.BlockSize})
	}
	p := peer.New(tcpConnPair(t), nil, 1)
	p.SetBitfield([]bool{true})
	return d, p
}

func pendingCount(d *Download) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

func TestPeerWorkerTakesNoJobsWhileChoked(t *testing.T) {
	d, p := newChokedWorkerSetup(t, 20)
	exited := startPeerWorker(d, p)

	time.Sleep(100 * time.Millisecond)

	if got := len(d.queue); got != 20 {
		t.Fatalf("expected a choked peer's worker to leave the queue alone, %d of 20 jobs left", got)
	}
	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected no requests to a choked peer, got %d pending", got)
	}

	d.cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("peer worker kept waiting for an unchoke after cancel")
	}
}

func TestPeerWorkerSendsRequestsAfterUnchoke(t *testing.T) {
	d, p := newChokedWorkerSetup(t, 20)
	exited := startPeerWorker(d, p)
	t.Cleanup(func() {
		d.cancel()
		<-exited
	})

	p.SetChoked(false)

	deadline := time.Now().Add(time.Second)
	for pendingCount(d) < 15 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := pendingCount(d); got != 15 {
		t.Fatalf("expected the worker to fill all 15 slots after an unchoke, got %d pending", got)
	}
	if got := p.InflightRequests(); got != 15 {
		t.Fatalf("expected 15 slots held, got %d", got)
	}
	if got := len(d.queue); got != 5 {
		t.Fatalf("expected 5 jobs left in the queue, got %d", got)
	}
}
