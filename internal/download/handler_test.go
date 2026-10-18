package download

import (
	"bytes"
	"context"
	"crypto/sha1"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/picker"
	"github.com/nazifbara/ziftorrent/internal/torrent"
	"github.com/nazifbara/ziftorrent/internal/wire"
)

func newFileState(t *testing.T, begin, size int64) *fileState {
	t.Helper()
	dir := t.TempDir()
	f, err := os.Create(filepath.Join(dir, "piece.bin"))
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	t.Cleanup(func() { f.Close() })
	// Pre-size the file so WriteAt has real space to write into.
	if err := f.Truncate(size); err != nil {
		t.Fatalf("failed to truncate test file: %v", err)
	}
	return &fileState{begin: begin, size: size, file: f, numOfBlocks: 1}
}

func newDownloadTestClient(t *testing.T, filesGrid []*fileState, contentSize uint64) *Download {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Download{
		files:     filesGrid,
		torrent:   &torrent.Torrent{ContentSize: contentSize},
		config:    &config.Config{BlockSize: config.BlockSize},
		done:      make(chan struct{}),
		startedAt: time.Now(),
		ctx:       ctx,
		cancel:    cancel,
	}
}

func TestWriteAtGlobalWithinSingleFile(t *testing.T) {
	fs := newFileState(t, 0, 20)
	c := newDownloadTestClient(t, []*fileState{fs}, 100)

	data := []byte("hello world")
	if err := c.writeAtGlobal(5, data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := make([]byte, len(data))
	if _, err := fs.file.ReadAt(got, 5); err != nil {
		t.Fatalf("failed to read back written data: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("expected %q at offset 5, got %q", data, got)
	}
	if fs.blocksWrote.Load() != 1 {
		t.Fatalf("expected blocksWrote to be incremented to 1, got %d", fs.blocksWrote.Load())
	}
}

func TestWriteAtGlobalSpansTwoFiles(t *testing.T) {
	fsA := newFileState(t, 0, 10)  // logical bytes [0,10)
	fsB := newFileState(t, 10, 20) // logical bytes [10,30)
	c := newDownloadTestClient(t, []*fileState{fsA, fsB}, 100)

	data := bytes.Repeat([]byte{0xAB}, 15) // begin=5 .. end=20 -- straddles the boundary at 10
	if err := c.writeAtGlobal(5, data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	gotA := make([]byte, 5) // bytes [5,10) belong to file A
	if _, err := fsA.file.ReadAt(gotA, 5); err != nil {
		t.Fatalf("failed to read back from file A: %v", err)
	}
	if !bytes.Equal(gotA, data[:5]) {
		t.Fatalf("file A mismatch: got %v, want %v", gotA, data[:5])
	}

	gotB := make([]byte, 10) // bytes [10,20) belong to file B, at offset [0,10) within it
	if _, err := fsB.file.ReadAt(gotB, 0); err != nil {
		t.Fatalf("failed to read back from file B: %v", err)
	}
	if !bytes.Equal(gotB, data[5:]) {
		t.Fatalf("file B mismatch: got %v, want %v", gotB, data[5:])
	}
}

func TestWriteAtGlobalOffsetOutOfRange(t *testing.T) {
	fs := newFileState(t, 0, 10)
	c := newDownloadTestClient(t, []*fileState{fs}, 100)

	err := c.writeAtGlobal(9999, []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected an error for an out-of-range offset, got nil")
	}
}

func TestWriteAtGlobalConcurrentWritesAreSafe(t *testing.T) {
	fs := newFileState(t, 0, 800)
	c := newDownloadTestClient(t, []*fileState{fs}, 100000)

	var wg sync.WaitGroup
	var errs atomic.Int32
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			block := bytes.Repeat([]byte{byte(i)}, 10)
			if err := c.writeAtGlobal(int64(i*10), block); err != nil {
				errs.Add(1)
			}
		}(i)
	}
	wg.Wait()
	if errs.Load() != 0 {
		t.Fatalf("expected all concurrent writes to succeed, got %d errors", errs.Load())
	}
}

func TestReadPieceSpansFiles(t *testing.T) {
	// 25 bytes of content split over three files, cut into pieces of 10 bytes:
	// piece 0 = [0,10), piece 1 = [10,20), piece 2 = [20,25) (short last piece).
	fsA := newFileState(t, 0, 4)   // logical bytes [0,4)
	fsB := newFileState(t, 4, 8)   // logical bytes [4,12)
	fsC := newFileState(t, 12, 13) // logical bytes [12,25)
	c := newDownloadTestClient(t, []*fileState{fsA, fsB, fsC}, 25)
	c.torrent.PieceSize = 10
	c.picker = picker.New(3, 10, 5, 5)

	content := make([]byte, 25)
	for i := range content {
		content[i] = byte(i + 1)
	}
	if err := c.writeAtGlobal(0, content); err != nil {
		t.Fatalf("failed to write content: %v", err)
	}

	for i, want := range [][]byte{content[0:10], content[10:20], content[20:25]} {
		got, err := c.readPiece(i)
		if err != nil {
			t.Fatalf("piece %d: unexpected error: %v", i, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("piece %d: got %v, want %v", i, got, want)
		}
	}
}

func TestReadPieceFailsWhenFileIsShorterThanPiece(t *testing.T) {
	fs := newFileState(t, 0, 10)
	c := newDownloadTestClient(t, []*fileState{fs}, 10)
	c.torrent.PieceSize = 10
	c.picker = picker.New(1, 10, 10, 5)
	if err := fs.file.Truncate(6); err != nil {
		t.Fatalf("failed to truncate test file: %v", err)
	}

	if _, err := c.readPiece(0); err == nil {
		t.Fatal("expected an error when the piece is not fully on disk, got nil")
	}
}

// newBlockTestClient builds a single-file download of one 10-byte piece made
// of two 5-byte blocks.
func newBlockTestClient(t *testing.T, content []byte) *Download {
	t.Helper()
	// SaveProgress writes under the home directory; keep it out of the real one.
	t.Setenv("HOME", t.TempDir())
	cfg, err := config.Load(make([]byte, 20), "dest")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	cfg.BlockSize = 5

	c := newDownloadTestClient(t, []*fileState{newFileState(t, 0, 10)}, 10)
	c.config = cfg
	// Let background progress saves finish before the temp home is removed.
	t.Cleanup(c.wg.Wait)
	c.torrent.NumOfPieces = 1
	c.torrent.PieceSize = 10
	c.torrent.FinalPieceSize = 10
	c.torrent.PieceHashes = [][20]byte{sha1.Sum(content)}
	c.picker = picker.New(1, 10, 10, 5)
	c.pending = make(map[pendingKey]*pendingRequest)
	return c
}

func blockMessage(t *testing.T, index, begin uint32, block []byte) wire.Message {
	t.Helper()
	msg, err := wire.ParseMessage(wire.BuildPiece(index, begin, block))
	if err != nil {
		t.Fatalf("failed to build block message: %v", err)
	}
	return msg
}

func haveMessage(index uint32) []byte { return wire.BuildHave(index) }

// sendBlock delivers, from p, the block of content that key refers to.
func sendBlock(t *testing.T, d *Download, p *peer.Peer, content []byte, key pendingKey) {
	t.Helper()
	within(t, func() {
		d.onBlockReceived(p, blockMessage(t, uint32(key.index), uint32(key.begin), content[key.begin:key.begin+5]))
	})
}

func isDone(d *Download) bool {
	select {
	case <-d.Done():
		return true
	default:
		return false
	}
}

func TestOnBlockReceivedVerifiesPieceFromDisk(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, c, nil)
	first, second := addPending(t, c, p), addPending(t, c, p)

	sendBlock(t, c, p, content, first)
	if isDone(c) {
		t.Fatal("download finished before the last block arrived")
	}
	sendBlock(t, c, p, content, second)

	if !isDone(c) {
		t.Fatal("expected the download to finish once the only piece is verified")
	}
	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected both requests released, %d in flight", got)
	}
	got, err := c.readPiece(0)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("unexpected piece on disk: got %q err=%v", got, err)
	}
}

func TestOnBlockReceivedResetsInvalidPiece(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, c, nil)
	first, second := addPending(t, c, p), addPending(t, c, p)

	sendBlock(t, c, p, content, first)
	sendBlock(t, c, p, []byte("XXXXXXXXXX"), second)

	if isDone(c) {
		t.Fatal("download must not finish with a corrupt piece")
	}
	if c.totalDownloaded.Load() != 0 {
		t.Fatalf("expected piece progress to be reset, downloaded=%d", c.totalDownloaded.Load())
	}
	pickFor(t, c, p)
	pickFor(t, c, p)
}

func TestLateBlockAfterTimeoutIsKept(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, c, nil)
	key := addPending(t, c, p)
	within(t, func() { c.onTimeout(key) })

	sendBlock(t, c, p, content, key)

	if got := c.totalDownloaded.Load(); got != 5 {
		t.Fatalf("expected the late block to be counted, downloaded=%d", got)
	}
	if got := p.InflightRequests(); got != 0 {
		t.Fatalf("expected no double release, %d in flight", got)
	}
	if req := pickFor(t, c, p); req.Begin == key.begin {
		t.Fatal("expected the late block not to be requested again")
	}
}

func TestDuplicateBlockIsCountedOnce(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, c, nil)
	key := addPending(t, c, p)

	sendBlock(t, c, p, content, key)
	sendBlock(t, c, p, content, key)

	if got := c.totalDownloaded.Load(); got != 5 {
		t.Fatalf("expected the duplicate not to be counted, downloaded=%d", got)
	}
}

func TestLateBlockFromPreviousPeerReleasesCurrentOwner(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	a := newInflightPeer(t, c, nil)
	key := addPending(t, c, a)
	// a's request times out and the block is handed to b.
	within(t, func() { c.onTimeout(key) })
	b := newInflightPeer(t, c, newConn(t))
	bKey := addPending(t, c, b)
	if bKey.begin != key.begin {
		t.Fatalf("expected b to get the timed-out block, got %+v", bKey)
	}

	// a's block shows up late and settles b's request.
	sendBlock(t, c, a, content, key)

	if got := a.InflightRequests(); got != 0 {
		t.Fatalf("expected the previous peer to stay at 0 in flight, got %d", got)
	}
	if got := b.InflightRequests(); got != 0 {
		t.Fatalf("expected the current owner's request to be released, got %d in flight", got)
	}
}

func TestUnrequestedBlockDoesNotReleaseOtherRequests(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	p := newInflightPeer(t, c, nil)
	key := addPending(t, c, p)
	other := pendingKey{index: 0, begin: 5 - key.begin, peer: p}

	sendBlock(t, c, p, content, other)

	if got := p.InflightRequests(); got != 1 {
		t.Fatalf("expected an unrequested block not to release anything, got %d in flight", got)
	}
	if !hasPending(c, key) {
		t.Fatal("expected the real request to stay pending")
	}
}

func TestEndgameFastPeerFinishesWithoutWaitingForStalledPeer(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)
	stopPendingTimers(t, c)
	stalledConn, stalledGot := tcpConnPair(t)
	stalled := newInflightPeer(t, c, stalledConn)
	addPending(t, c, stalled)
	addPending(t, c, stalled)

	// Every block is requested: the fast peer gets duplicates.
	fast := newInflightPeer(t, c, nil)
	first, second := addPending(t, c, fast), addPending(t, c, fast)
	sendBlock(t, c, fast, content, first)
	sendBlock(t, c, fast, content, second)

	if !isDone(c) {
		t.Fatal("expected the fast peer's copies to finish the download")
	}
	if got := stalled.InflightRequests(); got != 0 {
		t.Fatalf("expected the stalled peer's requests to be released, %d in flight", got)
	}
	if got := pendingCount(c); got != 0 {
		t.Fatalf("expected nothing left pending, got %d", got)
	}
	var sent []byte
	deadline := time.After(time.Second)
	for len(sent) < 2*17 {
		select {
		case b := <-stalledGot:
			sent = append(sent, b...)
		case <-deadline:
			t.Fatalf("expected 2 cancel messages sent to the stalled peer, got %v", sent)
		}
	}
	for i := 0; i < len(sent); i += 17 {
		if msg, err := wire.ParseMessage(sent[i : i+17]); err != nil || msg.ID != 8 {
			t.Fatalf("expected a cancel message, got %v (err=%v)", sent[i:i+17], err)
		}
	}
}
