package piece

import (
	"bytes"
	"testing"
)

func TestAddBlockCompletesPiece(t *testing.T) {
	p := New(4, 2)
	if done, _, err := p.AddBlock(0, []byte{1, 2}); err != nil || done {
		t.Fatalf("first block: done=%v err=%v", done, err)
	}
	done, data, err := p.AddBlock(2, []byte{3, 4})
	if err != nil || !done {
		t.Fatalf("second block: done=%v err=%v", done, err)
	}
	if !bytes.Equal(data, []byte{1, 2, 3, 4}) {
		t.Fatalf("unexpected piece bytes: %v", data)
	}
	if !p.IsDone() {
		t.Fatal("expected piece to report done")
	}
}

func TestAddBlockRejectsOutOfBounds(t *testing.T) {
	p := New(4, 2)
	if _, _, err := p.AddBlock(3, []byte{1, 2}); err == nil {
		t.Fatal("expected an error for a block past the piece boundary")
	}
}

func TestResetRestoresPieceSizedBuffer(t *testing.T) {
	p := New(32768, 16384)
	p.AddBlock(0, make([]byte, 16384))
	p.AddBlock(16384, make([]byte, 16384))
	p.Reset()
	if len(p.bytes) != 32768 || p.blocksRead != 0 || p.IsDone() {
		t.Fatalf("unexpected reset state: len=%d read=%d done=%v", len(p.bytes), p.blocksRead, p.IsDone())
	}
}
