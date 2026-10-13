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
	"github.com/nazifbara/ziftorrent/internal/piece"
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
	if fs.blocksWrote != 1 {
		t.Fatalf("expected blocksWrote to be incremented to 1, got %d", fs.blocksWrote)
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
	c.pieces = []*piece.State{piece.New(10, 5), piece.New(10, 5), piece.New(5, 5)}

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
	c.pieces = []*piece.State{piece.New(10, 5)}
	if err := fs.file.Truncate(6); err != nil {
		t.Fatalf("failed to truncate test file: %v", err)
	}

	if _, err := c.readPiece(0); err == nil {
		t.Fatal("expected an error when the piece is not fully on disk, got nil")
	}
}

// newBlockTestClient builds a single-file download of one 10-byte piece made
// of two 5-byte blocks, with both blocks already requested.
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
	c.torrent.PieceSize = 10
	c.torrent.PieceHashes = [][20]byte{sha1.Sum(content)}
	c.pieces = []*piece.State{piece.New(10, 5)}
	c.queue = make(chan *job, 2)
	c.pending = make(map[string]*pendingRequest)
	for _, begin := range []uint32{0, 5} {
		c.pending[pendingKey(0, begin)] = &pendingRequest{
			job:   &job{index: 0, begin: begin},
			peer:  &peer.Peer{},
			timer: time.NewTimer(time.Hour),
		}
	}
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

func TestOnBlockReceivedVerifiesPieceFromDisk(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)

	c.onBlockReceived(nil, blockMessage(t, 0, 0, content[:5]))
	select {
	case <-c.Done():
		t.Fatal("download finished before the last block arrived")
	default:
	}
	c.onBlockReceived(nil, blockMessage(t, 0, 5, content[5:]))

	select {
	case <-c.Done():
	default:
		t.Fatal("expected the download to finish once the only piece is verified")
	}
	if len(c.queue) != 0 {
		t.Fatalf("expected a valid piece not to be re-queued, got %d jobs", len(c.queue))
	}
	got, err := c.readPiece(0)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("unexpected piece on disk: got %q err=%v", got, err)
	}
}

func TestOnBlockReceivedRequeuesInvalidPiece(t *testing.T) {
	content := []byte("0123456789")
	c := newBlockTestClient(t, content)

	c.onBlockReceived(nil, blockMessage(t, 0, 0, content[:5]))
	c.onBlockReceived(nil, blockMessage(t, 0, 5, []byte("XXXXX")))

	select {
	case <-c.Done():
		t.Fatal("download must not finish with a corrupt piece")
	default:
	}
	if len(c.queue) != 2 {
		t.Fatalf("expected both blocks to be re-queued, got %d jobs", len(c.queue))
	}
	if c.pieces[0].BlockRead() != 0 || c.totalDownloaded.Load() != 0 {
		t.Fatalf("expected piece progress to be reset: read=%d downloaded=%d", c.pieces[0].BlockRead(), c.totalDownloaded.Load())
	}
}
