package download

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/torrent"
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

func TestWriteAtGlobalClosesDoneChannelWhenComplete(t *testing.T) {
	fs := newFileState(t, 0, 5)
	c := newDownloadTestClient(t, []*fileState{fs}, 5) // ContentSize == what we're about to write

	data := []byte{1, 2, 3, 4, 5}
	// Simulate the totalDownloaded accounting that onBlockReceived does before
	// calling writeToFile/writeAtGlobal.
	c.totalDownloaded.Store(int64(len(data)))
	if err := c.writeAtGlobal(0, data); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	select {
	case <-c.Done():
	default:
		t.Fatal("expected c.done to be closed once totalDownloaded reaches ContentSize")
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
