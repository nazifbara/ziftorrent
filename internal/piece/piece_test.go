package piece

import (
	"testing"
)

func TestAddBlockCompletesPiece(t *testing.T) {
	p := New(4, 2)
	if done, err := p.AddBlock(0, []byte{1, 2}); err != nil || done {
		t.Fatalf("first block: done=%v err=%v", done, err)
	}
	done, err := p.AddBlock(2, []byte{3, 4})
	if err != nil || !done {
		t.Fatalf("second block: done=%v err=%v", done, err)
	}
	if !p.IsDone() {
		t.Fatal("expected piece to report done")
	}
}

func TestAddBlockRejectsOutOfBounds(t *testing.T) {
	p := New(4, 2)
	if _, err := p.AddBlock(3, []byte{1, 2}); err == nil {
		t.Fatal("expected an error for a block past the piece boundary")
	}
	if p.BlockRead() != 0 {
		t.Fatalf("expected rejected block not to be counted, got %d", p.BlockRead())
	}
}

func TestResetClearsProgress(t *testing.T) {
	p := New(32768, 16384)
	p.AddBlock(0, make([]byte, 16384))
	p.AddBlock(16384, make([]byte, 16384))
	p.Reset()
	if p.BlockRead() != 0 || p.IsDone() || p.Size() != 32768 {
		t.Fatalf("unexpected reset state: read=%d done=%v size=%d", p.BlockRead(), p.IsDone(), p.Size())
	}
}

func TestSyncWithOffsetSkipsBlocksAlreadyRead(t *testing.T) {
	p := New(49152, 16384)
	p.SyncWithOffset(32768)
	if p.BlockRead() != 2 {
		t.Fatalf("expected 2 blocks read after syncing to offset 32768, got %d", p.BlockRead())
	}
	done, err := p.AddBlock(32768, make([]byte, 16384))
	if err != nil || !done {
		t.Fatalf("last block after sync: done=%v err=%v", done, err)
	}
}
