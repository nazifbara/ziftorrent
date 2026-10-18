package piece

import (
	"testing"

	"github.com/nazifbara/ziftorrent/internal/peer"
)

func TestNewSplitsPieceIntoBlocks(t *testing.T) {
	p := New(3, 10, 4)
	if p.BlockCount() != 3 || p.Missing() != 3 || p.Started() || p.IsDone() {
		t.Fatalf("unexpected new piece: blocks=%d missing=%d", p.BlockCount(), p.Missing())
	}
	for i, want := range []Block{{Begin: 0, Length: 4}, {Begin: 4, Length: 4}, {Begin: 8, Length: 2}} {
		if b := p.Block(i); b.Begin != want.Begin || b.Length != want.Length {
			t.Fatalf("block %d: got begin=%d length=%d, want %+v", i, b.Begin, b.Length, want)
		}
	}
}

func TestBlockAtRejectsUnalignedOffsets(t *testing.T) {
	p := New(0, 10, 4)
	for begin, want := range map[int]int{0: 0, 4: 1, 8: 2, 2: -1, 12: -1, -4: -1} {
		if got := p.BlockAt(begin); got != want {
			t.Errorf("BlockAt(%d): got %d, want %d", begin, got, want)
		}
	}
}

func TestReceiveCompletesPieceOnce(t *testing.T) {
	p := New(0, 4, 2)
	if _, fresh := p.Receive(0); !fresh || p.IsDone() {
		t.Fatalf("first block: fresh=%v done=%v", fresh, p.IsDone())
	}
	if _, fresh := p.Receive(0); fresh {
		t.Fatal("expected the same block received twice to be a duplicate")
	}
	if p.IsDone() {
		t.Fatal("a duplicate block must not complete the piece")
	}
	if _, fresh := p.Receive(1); !fresh || !p.IsDone() {
		t.Fatalf("second block: fresh=%v done=%v", fresh, p.IsDone())
	}
}

func TestReleaseReturnsBlockToMissingOnceNobodyHasIt(t *testing.T) {
	a, b := new(peer.Peer), new(peer.Peer)
	p := New(0, 4, 2)
	p.Request(0, a)
	p.Request(0, b)
	if p.Missing() != 1 {
		t.Fatalf("expected 1 missing block, got %d", p.Missing())
	}
	p.Release(0, a)
	if p.Block(0).Status != RequestedBlock {
		t.Fatal("expected the block to stay requested while b has it")
	}
	p.Release(0, b)
	if p.Block(0).Status != MissingBlock || p.Missing() != 2 {
		t.Fatalf("expected the block back to missing: status=%v missing=%d", p.Block(0).Status, p.Missing())
	}
}

func TestReceiveReturnsRequesters(t *testing.T) {
	a, b := new(peer.Peer), new(peer.Peer)
	p := New(0, 4, 2)
	p.Request(0, a)
	p.Request(0, b)
	requesters, fresh := p.Receive(0)
	if !fresh || len(requesters) != 2 || len(p.Block(0).Requesters) != 0 {
		t.Fatalf("unexpected receive: fresh=%v requesters=%d", fresh, len(requesters))
	}
}

func TestResetClearsProgress(t *testing.T) {
	p := New(0, 4, 2)
	p.Receive(0)
	p.Receive(1)
	p.Reset()
	if p.IsDone() || p.Started() || p.Missing() != 2 {
		t.Fatalf("unexpected reset state: done=%v missing=%d", p.IsDone(), p.Missing())
	}
}
