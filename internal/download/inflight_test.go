package download

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/picker"
	"github.com/nazifbara/ziftorrent/internal/torrent"
)

// newTestDownload returns a download of n pieces of 2 blocks each, with no
// files behind it.
func newTestDownload(t *testing.T, n int) *Download {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	pieceSize := 2 * config.BlockSize
	return &Download{
		picker:  picker.New(n, pieceSize, pieceSize, config.BlockSize),
		pending: make(map[pendingKey]*pendingRequest),
		ctx:     ctx,
		cancel:  cancel,
		torrent: &torrent.Torrent{NumOfPieces: uint64(n), PieceSize: uint64(pieceSize), FinalPieceSize: uint64(pieceSize)},
		config:  &config.Config{BlockSize: config.BlockSize},
	}
}

// newInflightPeer returns an unchoked peer that has every piece of d.
func newInflightPeer(t *testing.T, d *Download, conn *net.TCPConn) *peer.Peer {
	t.Helper()
	n := int(d.torrent.NumOfPieces)
	p := peer.New(conn, nil, uint64(n))
	all := make([]bool, n)
	for i := range all {
		all[i] = true
	}
	p.SetBitfield(all)
	d.picker.SetBitfield(p, all)
	p.SetChoked(false)
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

// addPending picks a block for p and registers it as sent, holding one of
// p's slots, as tryRequestPeer would. The timer never fires on its own.
func addPending(t *testing.T, d *Download, p *peer.Peer) pendingKey {
	t.Helper()
	acquireSlot(t, p)
	req, ok := d.picker.Pick(p)
	if !ok {
		t.Fatal("expected the picker to have a block for the peer")
	}
	key := keyFor(p, req)
	d.mu.Lock()
	d.pending[key] = &pendingRequest{req: req, sentAt: time.Now(), timer: time.NewTimer(time.Hour)}
	d.mu.Unlock()
	return key
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

// tcpConnPair returns the client side of a loopback TCP connection. What
// the client writes is sent to the returned channel, so writes do not block.
func tcpConnPair(t *testing.T) (*net.TCPConn, <-chan []byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })
	received := make(chan []byte, 64)
	go func() {
		server, err := ln.Accept()
		if err != nil {
			return
		}
		t.Cleanup(func() { server.Close() })
		for {
			buf := make([]byte, 1024)
			n, err := server.Read(buf)
			if err != nil {
				return
			}
			select {
			case received <- buf[:n]:
			default:
			}
		}
	}()
	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn.(*net.TCPConn), received
}

func newConn(t *testing.T) *net.TCPConn {
	t.Helper()
	conn, _ := tcpConnPair(t)
	return conn
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

func pendingCount(d *Download) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.pending)
}

func hasPending(d *Download, key pendingKey) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	_, ok := d.pending[key]
	return ok
}

func pickFor(t *testing.T, d *Download, p *peer.Peer) picker.Request {
	t.Helper()
	req, ok := d.picker.Pick(p)
	if !ok {
		t.Fatal("expected the picker to have a block for the peer")
	}
	return req
}

func TestTryRequestPeerKeepsSlotForSentRequest(t *testing.T) {
	d := newTestDownload(t, 1)
	stopPendingTimers(t, d)
	p := newInflightPeer(t, d, newConn(t))
	acquireSlot(t, p)
	req := pickFor(t, d, p)

	within(t, func() { d.tryRequestPeer(p, req) })

	if got := p.InflightRequests(); got != 1 {
		t.Fatalf("expected 1 in-flight request after a successful send, got %d", got)
	}
	if !hasPending(d, keyFor(p, req)) {
		t.Fatal("expected the sent request to be pending")
	}
}

func TestTryRequestPeerReleasesSlotOnFailedWrite(t *testing.T) {
	d := newTestDownload(t, 1)
	conn := newConn(t)
	conn.Close()
	p := newInflightPeer(t, d, conn)
	acquireSlot(t, p)
	req := pickFor(t, d, p)

	within(t, func() { d.tryRequestPeer(p, req) })

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected a failed write to release its slot, got %d in flight", got)
	}
	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected no pending request after a failed write, got %d", got)
	}
	if again := pickFor(t, d, p); again != req {
		t.Fatalf("expected the block %+v back in the picker, got %+v", req, again)
	}
}

func TestTryRequestPeerReleasesSlotWhenChoked(t *testing.T) {
	d := newTestDownload(t, 1)
	p := newInflightPeer(t, d, newConn(t))
	acquireSlot(t, p)
	req := pickFor(t, d, p)
	p.SetChoked(true)

	within(t, func() { d.tryRequestPeer(p, req) })

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected a request to a choked peer to release its slot, got %d in flight", got)
	}
	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected nothing sent to a choked peer, got %d pending", got)
	}
}

func TestTimeoutReleasesInflightRequest(t *testing.T) {
	d := newTestDownload(t, 1)
	p := newInflightPeer(t, d, nil)
	key := addPending(t, d, p)

	within(t, func() { d.onTimeout(key) })

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected the timeout to release its request, got %d in flight", got)
	}
	if hasPending(d, key) {
		t.Fatal("expected the pending request to be removed")
	}
}

func TestTimeoutHandsBlockToAnotherPeer(t *testing.T) {
	d := newTestDownload(t, 1)
	a := newInflightPeer(t, d, nil)
	first := addPending(t, d, a)
	addPending(t, d, a)
	within(t, func() { d.onTimeout(first) })

	b := newInflightPeer(t, d, nil)
	if req := pickFor(t, d, b); req.Index != first.index || req.Begin != first.begin {
		t.Fatalf("expected the timed-out block to be picked again, got %+v", req)
	}
}

func TestRepeatedTimeoutIsIgnored(t *testing.T) {
	d := newTestDownload(t, 1)
	p := newInflightPeer(t, d, nil)
	key := addPending(t, d, p)

	within(t, func() {
		d.onTimeout(key)
		d.onTimeout(key)
	})

	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected a single release, %d slots held", got)
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

func expectExit(t *testing.T, exited <-chan struct{}, msg string) {
	t.Helper()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal(msg)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
}

func TestPeerWorkerExitsOnCancelWithNothingToPick(t *testing.T) {
	d := newTestDownload(t, 1)
	p := peer.New(newConn(t), nil, 1)
	p.SetChoked(false)
	exited := startPeerWorker(d, p)

	d.cancel()

	expectExit(t, exited, "peer worker kept waiting on the picker after cancel")
}

func TestPeerWorkerRequeuesAddressWhenPeerCloses(t *testing.T) {
	d := newTestDownload(t, 1)
	d.addrQueue = make(chan *net.TCPAddr, 1)
	conn := newConn(t)
	addr := conn.RemoteAddr().(*net.TCPAddr)
	p := peer.New(conn, addr, 1)
	exited := startPeerWorker(d, p)

	p.Close()

	expectExit(t, exited, "peer worker kept waiting after the peer closed")
	select {
	case got := <-d.addrQueue:
		if !p.MatchesAddress(got) {
			t.Fatalf("expected %v to be requeued, got %v", addr, got)
		}
	case <-time.After(time.Second):
		t.Fatal("expected the closed peer's address to be requeued")
	}
}

func TestPeerWorkerReleasesRequestsWhenPeerCloses(t *testing.T) {
	d := newTestDownload(t, 1)
	stopPendingTimers(t, d)
	d.addrQueue = make(chan *net.TCPAddr, 1)
	conn := newConn(t)
	p := newInflightPeer(t, d, conn)
	exited := startPeerWorker(d, p)
	waitFor(t, func() bool { return pendingCount(d) == 2 })

	p.Close()

	expectExit(t, exited, "peer worker kept running after the peer closed")
	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected the closed peer's requests to be dropped, %d pending", got)
	}
	other := newInflightPeer(t, d, nil)
	pickFor(t, d, other)
	pickFor(t, d, other)
}

func TestPeerWorkerWaitsForUsefulPiece(t *testing.T) {
	d := newTestDownload(t, 2)
	stopPendingTimers(t, d)
	p := peer.New(newConn(t), nil, 2)
	p.SetChoked(false)
	exited := startPeerWorker(d, p)
	t.Cleanup(func() {
		d.cancel()
		<-exited
	})

	time.Sleep(50 * time.Millisecond)
	if got := pendingCount(d); got != 0 {
		t.Fatalf("expected nothing requested from a peer with no pieces, got %d pending", got)
	}

	d.onMessage(p, haveMessage(1))

	waitFor(t, func() bool { return pendingCount(d) == 2 })
	if got := pendingCount(d); got != 2 {
		t.Fatalf("expected both blocks of the new piece to be requested, got %d pending", got)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	for key := range d.pending {
		if key.index != 1 {
			t.Fatalf("expected only piece 1 to be requested, got %+v", key)
		}
	}
}
