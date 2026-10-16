package download

import (
	"net"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/wire"
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

// addPending registers a request sent to p for (index, begin), holding one of
// p's slots, and returns its job.
func addPending(t *testing.T, d *Download, p *peer.Peer, index, begin uint32) *job {
	t.Helper()
	acquireSlot(t, p)
	j := &job{index: index, begin: begin}
	d.mu.Lock()
	d.pending[pendingKey(index, begin)] = &pendingRequest{job: j, peer: p, timer: time.NewTimer(time.Hour)}
	d.mu.Unlock()
	return j
}

func choke(t *testing.T, d *Download, p *peer.Peer) {
	t.Helper()
	within(t, func() { d.onMessage(p, wire.BuildChoke()) })
}

// drainQueue returns every job currently in the queue.
func drainQueue(d *Download) []*job {
	var jobs []*job
	for {
		select {
		case j := <-d.queue:
			jobs = append(jobs, j)
		default:
			return jobs
		}
	}
}

func TestChokeDropsPendingRequests(t *testing.T) {
	d := newQueueDownload(t, 16)
	p := newInflightPeer(t, nil)
	want := map[*job]bool{}
	for i := range uint32(3) {
		want[addPending(t, d, p, 0, i*config.BlockSize)] = true
	}

	choke(t, d, p)

	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected a choke to drop the peer's pending requests, %d left", got)
	}
	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected a choke to release the peer's slots, %d held", got)
	}
	requeued := drainQueue(d)
	if len(requeued) != len(want) {
		t.Fatalf("expected %d jobs requeued, got %d", len(want), len(requeued))
	}
	for _, j := range requeued {
		if !want[j] {
			t.Fatalf("unexpected job requeued: %v", j)
		}
		delete(want, j)
	}
}

func TestChokeStopsDroppedRequestTimers(t *testing.T) {
	d := newQueueDownload(t, 16)
	p := newInflightPeer(t, nil)
	addPending(t, d, p, 0, 0)
	d.mu.Lock()
	timer := d.pending[pendingKey(0, 0)].timer
	d.mu.Unlock()

	choke(t, d, p)

	if timer.Stop() {
		t.Fatal("expected the dropped request's timer to be stopped already")
	}
}

func TestRepeatedChokesWithPendingRequestsDoNotClosePeer(t *testing.T) {
	d := newQueueDownload(t, 64)
	p := newInflightPeer(t, nil)

	for round := range uint32(peerFailureLimit + 1) {
		p.SetChoked(false)
		for i := range uint32(3) {
			addPending(t, d, p, round, i*config.BlockSize)
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
	d := newQueueDownload(t, 16)
	a := newInflightPeer(t, nil)
	b := newInflightPeer(t, nil)
	addPending(t, d, a, 0, 0)
	addPending(t, d, a, 0, config.BlockSize)
	bJobs := []*job{
		addPending(t, d, b, 1, 0),
		addPending(t, d, b, 1, config.BlockSize),
	}

	choke(t, d, a)

	if got := b.InflightRequests(); got != 2 {
		t.Fatalf("expected the other peer to keep its 2 slots, got %d", got)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.pending) != 2 {
		t.Fatalf("expected only the other peer's 2 requests to remain, got %d", len(d.pending))
	}
	for _, j := range bJobs {
		pr, ok := d.pending[pendingKey(j.index, j.begin)]
		if !ok || pr.peer != b {
			t.Fatalf("expected the other peer's request %v to stay pending", j)
		}
	}
}

func TestChokeDistinguishesPeersWithSameAddress(t *testing.T) {
	d := newQueueDownload(t, 16)
	addr := &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 6881}
	// e.g. a reconnect while the old connection still has requests pending.
	old := peer.New(nil, addr, 1)
	old.SetChoked(false)
	current := peer.New(nil, addr, 1)
	current.SetChoked(false)
	addPending(t, d, old, 0, 0)
	addPending(t, d, current, 0, config.BlockSize)

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
	d := newQueueDownload(t, 16)
	p := newInflightPeer(t, nil)
	j := addPending(t, d, p, 0, 0)

	choke(t, d, p)
	for range peerFailureLimit + 1 {
		within(t, func() { d.onTimeout(pendingKey(j.index, j.begin), j) })
	}

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected no double release, %d slots held", got)
	}
	if got := len(drainQueue(d)); got != 1 {
		t.Fatalf("expected the job to be requeued once, got %d", got)
	}
	if isClosed(p) {
		t.Fatal("expected timeouts of dropped requests not to count as failures")
	}
}

func TestChokeWithoutPendingRequestsIsNoop(t *testing.T) {
	d := newQueueDownload(t, 16)
	a := newInflightPeer(t, nil)
	b := newInflightPeer(t, nil)
	addPending(t, d, b, 0, 0)

	choke(t, d, a)
	choke(t, d, a)

	if got := pendingCount(d); got != 1 {
		t.Fatalf("expected the other peer's request to stay pending, got %d", got)
	}
	if got := len(d.queue); got != 0 {
		t.Fatalf("expected nothing requeued, got %d jobs", got)
	}
}
