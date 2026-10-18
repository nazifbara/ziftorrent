package picker

import (
	"testing"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/piece"
)

// newTestPicker returns a picker of n pieces of 2 blocks each (4 bytes,
// blocks of 2), with random first pieces turned off.
func newTestPicker(n int) *Picker {
	pk := New(n, 4, 4, 2)
	pk.randomFirstPieces = 0
	return pk
}

func bits(n int, set ...int) []bool {
	b := make([]bool, n)
	for _, i := range set {
		b[i] = true
	}
	return b
}

func mustPick(t *testing.T, pk *Picker, p *peer.Peer) Request {
	t.Helper()
	req, ok := pk.Pick(p)
	if !ok {
		t.Fatal("expected a block to be picked")
	}
	return req
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestPickOnlyReturnsPiecesThePeerHas(t *testing.T) {
	pk := newTestPicker(4)
	p := new(peer.Peer)
	pk.SetBitfield(p, bits(4, 2))

	for range 2 {
		if req := mustPick(t, pk, p); req.Index != 2 {
			t.Fatalf("expected piece 2, got %+v", req)
		}
	}
	if _, ok := pk.Pick(p); ok {
		t.Fatal("expected nothing left to pick")
	}
}

func TestPickFailsForPeerWithNoPieces(t *testing.T) {
	pk := newTestPicker(4)
	p := new(peer.Peer)
	if _, ok := pk.Pick(p); ok {
		t.Fatal("expected an unknown peer to get nothing")
	}
	pk.SetBitfield(p, bits(4))
	if _, ok := pk.Pick(p); ok {
		t.Fatal("expected a peer with no pieces to get nothing")
	}
}

func TestPickReturnsBlockLength(t *testing.T) {
	pk := New(2, 4, 3, 2)
	p := new(peer.Peer)
	pk.SetBitfield(p, bits(2, 1))
	mustPick(t, pk, p)
	if req := mustPick(t, pk, p); req.Index != 1 || req.Begin != 2 || req.Length != 1 {
		t.Fatalf("expected the short last block of the final piece, got %+v", req)
	}
}

func TestPickPrefersStartedPiece(t *testing.T) {
	pk := newTestPicker(4)
	a, b := new(peer.Peer), new(peer.Peer)
	pk.SetBitfield(a, bits(4, 3))
	pk.SetBitfield(b, bits(4, 0, 1, 2, 3))
	mustPick(t, pk, a)

	// Piece 3 is the most common, but it is already started.
	if req := mustPick(t, pk, b); req.Index != 3 {
		t.Fatalf("expected the started piece 3, got %+v", req)
	}
}

func TestPickRarestFirst(t *testing.T) {
	pk := newTestPicker(3)
	a, b, c := new(peer.Peer), new(peer.Peer), new(peer.Peer)
	pk.SetBitfield(a, bits(3, 0, 1, 2))
	pk.SetBitfield(b, bits(3, 0, 2))
	pk.SetBitfield(c, bits(3, 0))

	if req := mustPick(t, pk, a); req.Index != 1 {
		t.Fatalf("expected the rarest piece 1, got %+v", req)
	}
}

func TestAvailabilityFollowsHaveAndRemovePeer(t *testing.T) {
	pk := newTestPicker(2)
	a, b := new(peer.Peer), new(peer.Peer)
	pk.SetBitfield(a, bits(2, 0, 1))
	pk.Have(b, 0)
	pk.Have(b, 0) // a repeated have counts once
	if pk.availability[0] != 2 || pk.availability[1] != 1 {
		t.Fatalf("unexpected availability: %v", pk.availability)
	}

	pk.RemovePeer(a)
	if pk.availability[0] != 1 || pk.availability[1] != 0 {
		t.Fatalf("expected a's pieces to be removed: %v", pk.availability)
	}
}

func TestHaveSignalsChange(t *testing.T) {
	pk := newTestPicker(2)
	p := new(peer.Peer)
	changed := pk.Changed()
	pk.Have(p, 1)
	if !closed(changed) {
		t.Fatal("expected a new piece to signal a change")
	}
	if req := mustPick(t, pk, p); req.Index != 1 {
		t.Fatalf("expected piece 1, got %+v", req)
	}
}

func TestPickBreaksTiesRandomly(t *testing.T) {
	seen := map[int]bool{}
	for range 50 {
		pk := newTestPicker(4)
		p := new(peer.Peer)
		pk.SetBitfield(p, bits(4, 0, 1, 2, 3))
		seen[mustPick(t, pk, p).Index] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected ties to be broken at random, always got %v", seen)
	}
}

func TestPickIsRandomBeforeFirstPiecesAreDone(t *testing.T) {
	seen := map[int]bool{}
	for range 50 {
		pk := New(4, 4, 4, 2)
		a, b := new(peer.Peer), new(peer.Peer)
		pk.SetBitfield(a, bits(4, 0, 1, 2, 3))
		pk.SetBitfield(b, bits(4, 1, 2, 3))
		seen[mustPick(t, pk, a).Index] = true
	}
	if len(seen) < 2 {
		t.Fatalf("expected random picks before the first pieces are done, always got %v", seen)
	}
}

func TestReleaseMakesBlockAvailableAgain(t *testing.T) {
	pk := newTestPicker(1)
	a, b := new(peer.Peer), new(peer.Peer)
	pk.SetBitfield(a, bits(1, 0))
	pk.SetBitfield(b, bits(1, 0))
	first := mustPick(t, pk, a)
	mustPick(t, pk, a)

	changed := pk.Changed()
	pk.Release(first.Index, first.Begin, a)
	if !closed(changed) {
		t.Fatal("expected a release to signal a change")
	}
	if req := mustPick(t, pk, b); req != first {
		t.Fatalf("expected the released block %+v, got %+v", first, req)
	}
}

func TestReceivedCompletesPiece(t *testing.T) {
	pk := newTestPicker(1)
	p := new(peer.Peer)
	pk.SetBitfield(p, bits(1, 0))
	r1, r2 := mustPick(t, pk, p), mustPick(t, pk, p)

	requesters, done, fresh := pk.Received(r1.Index, r1.Begin)
	if !fresh || done || len(requesters) != 1 || requesters[0] != p {
		t.Fatalf("first block: requesters=%v done=%v fresh=%v", requesters, done, fresh)
	}
	if _, _, fresh := pk.Received(r1.Index, r1.Begin); fresh {
		t.Fatal("expected a duplicate block not to be fresh")
	}
	if pk.Wanted(r1.Index, r1.Begin, r1.Length) {
		t.Fatal("expected a received block not to be wanted")
	}
	if _, done, _ := pk.Received(r2.Index, r2.Begin); !done || !pk.Complete() {
		t.Fatal("expected the last block to complete the piece and the download")
	}
}

func TestReceivedKeepsLateBlock(t *testing.T) {
	pk := newTestPicker(1)
	p := new(peer.Peer)
	pk.SetBitfield(p, bits(1, 0))
	req := mustPick(t, pk, p)
	pk.Release(req.Index, req.Begin, p) // timed out

	if !pk.Wanted(req.Index, req.Begin, req.Length) {
		t.Fatal("expected a timed-out block to still be wanted")
	}
	if requesters, _, fresh := pk.Received(req.Index, req.Begin); !fresh || len(requesters) != 0 {
		t.Fatalf("expected the late block to count: fresh=%v requesters=%v", fresh, requesters)
	}
	if req2 := mustPick(t, pk, p); req2.Begin == req.Begin {
		t.Fatal("expected the received block not to be picked again")
	}
}

func TestWantedRejectsWrongLength(t *testing.T) {
	pk := New(1, 3, 3, 2)
	if pk.Wanted(0, 2, 2) || !pk.Wanted(0, 2, 1) || pk.Wanted(0, 1, 1) || pk.Wanted(1, 0, 2) {
		t.Fatal("expected only exact blocks to be wanted")
	}
}

func TestPieceFailedResetsPiece(t *testing.T) {
	pk := newTestPicker(1)
	p := new(peer.Peer)
	pk.SetBitfield(p, bits(1, 0))
	pk.Received(0, 0)
	pk.Received(0, 2)

	pk.PieceFailed(0)
	if pk.Complete() || pk.donePieces != 0 || pk.missing != 2 {
		t.Fatalf("expected the piece reset: done=%d missing=%d", pk.donePieces, pk.missing)
	}
	mustPick(t, pk, p)
	mustPick(t, pk, p)
}

func TestSyncWithProgressMarksWholeBlocks(t *testing.T) {
	pk := newTestPicker(3)
	// Bytes [0,7): piece 0 whole, and the first block plus one byte of piece 1.
	got := pk.SyncWithProgress([]config.ProgressRange{{Start: 0, End: 7}}, 4)
	if got != 6 {
		t.Fatalf("expected 6 bytes of whole blocks, got %d", got)
	}
	if pk.donePieces != 1 || pk.missing != 3 {
		t.Fatalf("unexpected state: done=%d missing=%d", pk.donePieces, pk.missing)
	}
	if pk.pieces[1].Block(1).Status != piece.MissingBlock {
		t.Fatal("expected the partly written block to stay missing")
	}
}

func TestNoEndgameWhileUnrequestedBlocksRemain(t *testing.T) {
	pk := newTestPicker(2)
	a, b, c := new(peer.Peer), new(peer.Peer), new(peer.Peer)
	pk.SetBitfield(a, bits(2, 0))
	pk.SetBitfield(b, bits(2, 0))
	pk.SetBitfield(c, bits(2, 1))
	mustPick(t, pk, a)
	mustPick(t, pk, a)

	// Piece 1 (only c has it) still has missing blocks, so b may not
	// duplicate piece 0.
	if _, ok := pk.Pick(b); ok {
		t.Fatal("expected no duplicate request before endgame")
	}
}

func TestEndgameDuplicatesRequests(t *testing.T) {
	pk := newTestPicker(1)
	a, b, c := new(peer.Peer), new(peer.Peer), new(peer.Peer)
	for _, p := range []*peer.Peer{a, b, c} {
		pk.SetBitfield(p, bits(1, 0))
	}
	mustPick(t, pk, a)
	changed := pk.Changed()
	mustPick(t, pk, a)
	if !closed(changed) {
		t.Fatal("expected entering endgame to signal a change")
	}

	if _, ok := pk.Pick(a); ok {
		t.Fatal("expected a peer never to get the same block twice")
	}
	b1, b2 := mustPick(t, pk, b), mustPick(t, pk, b)
	if b1 == b2 {
		t.Fatal("expected b to get two different blocks")
	}
	if _, ok := pk.Pick(c); ok {
		t.Fatal("expected duplicates to be capped at 2 requesters")
	}

	requesters, _, fresh := pk.Received(b1.Index, b1.Begin)
	if !fresh || len(requesters) != 2 {
		t.Fatalf("expected both requesters returned, got %v", requesters)
	}
}

func TestEndgamePrefersFewestRequesters(t *testing.T) {
	pk := newTestPicker(1)
	pk.maxDuplicates = 3
	a, b, c := new(peer.Peer), new(peer.Peer), new(peer.Peer)
	for _, p := range []*peer.Peer{a, b, c} {
		pk.SetBitfield(p, bits(1, 0))
	}
	mustPick(t, pk, a)
	mustPick(t, pk, a)
	first := mustPick(t, pk, b)

	if req := mustPick(t, pk, c); req == first {
		t.Fatalf("expected c to get the block with one requester, got %+v", req)
	}
}
