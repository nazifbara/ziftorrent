package picker

import (
	"math/rand/v2"
	"sync"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/piece"
)

const (
	// Pieces picked at random, before switching to rarest-first.
	defaultRandomFirstPieces = 4
	// Peers one block can be requested from at once in endgame.
	defaultMaxDuplicates = 2
)

type Request struct {
	Index  int
	Begin  int
	Length int
}

// Picker tracks every block of the torrent and decides which one each peer
// is asked for next.
type Picker struct {
	mu           sync.Mutex
	pieces       []*piece.Piece
	availability []int
	peers        map[*peer.Peer][]bool
	// missing counts blocks neither requested nor received. Endgame starts
	// when it reaches 0.
	missing    int
	donePieces int
	changed    chan struct{}

	randomFirstPieces int
	maxDuplicates     int
}

func New(numOfPieces, pieceSize, finalPieceSize, blockSize int) *Picker {
	pieces := make([]*piece.Piece, numOfPieces)
	missing := 0
	for i := range pieces {
		size := pieceSize
		if i == numOfPieces-1 {
			size = finalPieceSize
		}
		pieces[i] = piece.New(i, size, blockSize)
		missing += pieces[i].BlockCount()
	}
	return &Picker{
		pieces:            pieces,
		availability:      make([]int, numOfPieces),
		peers:             make(map[*peer.Peer][]bool),
		missing:           missing,
		changed:           make(chan struct{}),
		randomFirstPieces: defaultRandomFirstPieces,
		maxDuplicates:     defaultMaxDuplicates,
	}
}

func (pk *Picker) PieceSize(index int) int { return pk.pieces[index].Size() }

// Changed returns a channel closed on the next change that can make Pick
// succeed. Take it before calling Pick so no change is missed.
func (pk *Picker) Changed() <-chan struct{} {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	return pk.changed
}

func (pk *Picker) notify() {
	close(pk.changed)
	pk.changed = make(chan struct{})
}

// update runs fn on pc and keeps pk.missing in sync with it.
func (pk *Picker) update(pc *piece.Piece, fn func()) {
	before := pc.Missing()
	fn()
	pk.missing += pc.Missing() - before
}

func (pk *Picker) bitfield(p *peer.Peer) []bool {
	has, ok := pk.peers[p]
	if !ok {
		has = make([]bool, len(pk.pieces))
		pk.peers[p] = has
	}
	return has
}

// SetBitfield records the pieces p has.
func (pk *Picker) SetBitfield(p *peer.Peer, bitfield []bool) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	has := pk.bitfield(p)
	added := false
	for i := range has {
		value := i < len(bitfield) && bitfield[i]
		if value == has[i] {
			continue
		}
		has[i] = value
		if value {
			pk.availability[i]++
			added = true
		} else {
			pk.availability[i]--
		}
	}
	if added {
		pk.notify()
	}
}

// Have records that p now has piece index.
func (pk *Picker) Have(p *peer.Peer, index int) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	has := pk.bitfield(p)
	if index < 0 || index >= len(has) || has[index] {
		return
	}
	has[index] = true
	pk.availability[index]++
	pk.notify()
}

// RemovePeer forgets p's pieces. Its pending requests must be released
// separately.
func (pk *Picker) RemovePeer(p *peer.Peer) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	for i, value := range pk.peers[p] {
		if value {
			pk.availability[i]--
		}
	}
	delete(pk.peers, p)
}

// Pick returns a block p has that is still needed, and records it as
// requested from p. ok is false when p has nothing to offer right now.
func (pk *Picker) Pick(p *peer.Peer) (req Request, ok bool) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	has := pk.peers[p]
	if has == nil {
		return Request{}, false
	}

	pc := pk.choosePiece(has, true)
	if pc == nil {
		pc = pk.choosePiece(has, false)
	}
	if pc == nil {
		return pk.pickDuplicate(p, has)
	}
	for i := range pc.BlockCount() {
		if pc.Block(i).Status == piece.MissingBlock {
			pk.update(pc, func() { pc.Request(i, p) })
			if pk.missing == 0 {
				// Endgame starts: waiting peers can now pick duplicates.
				pk.notify()
			}
			return newRequest(pc, i), true
		}
	}
	return Request{}, false
}

// choosePiece returns a piece p has with missing blocks, among those already
// started or not. It picks the rarest one, breaking ties at random, or any one
// at random while fewer than randomFirstPieces pieces are done.
func (pk *Picker) choosePiece(has []bool, started bool) *piece.Piece {
	random := !started && pk.donePieces < pk.randomFirstPieces
	var chosen *piece.Piece
	best, ties := 0, 0
	for i, pc := range pk.pieces {
		if !has[i] || pc.Missing() == 0 || pc.Started() != started {
			continue
		}
		rarity := 0
		if !random {
			rarity = pk.availability[i]
		}
		switch {
		case chosen == nil || rarity < best:
			chosen, best, ties = pc, rarity, 1
		case rarity == best:
			ties++
			// Reservoir sampling: each tie ends up chosen with chance 1/ties.
			if rand.IntN(ties) == 0 {
				chosen = pc
			}
		}
	}
	return chosen
}

// pickDuplicate is endgame: once every missing block is requested, p may be
// asked for one already requested from other peers. It picks the block with
// the fewest requesters, never one p already has requested.
func (pk *Picker) pickDuplicate(p *peer.Peer, has []bool) (Request, bool) {
	if pk.missing != 0 {
		return Request{}, false
	}
	var chosen *piece.Piece
	chosenBlock := -1
	fewest := pk.maxDuplicates
	for i, pc := range pk.pieces {
		if !has[i] || pc.IsDone() {
			continue
		}
		for j := range pc.BlockCount() {
			b := pc.Block(j)
			if b.Status != piece.RequestedBlock || len(b.Requesters) >= fewest || requestedFrom(b, p) {
				continue
			}
			chosen, chosenBlock, fewest = pc, j, len(b.Requesters)
		}
	}
	if chosen == nil {
		return Request{}, false
	}
	chosen.Request(chosenBlock, p)
	return newRequest(chosen, chosenBlock), true
}

func requestedFrom(b *piece.Block, p *peer.Peer) bool {
	for _, r := range b.Requesters {
		if r == p {
			return true
		}
	}
	return false
}

func newRequest(pc *piece.Piece, i int) Request {
	b := pc.Block(i)
	return Request{Index: pc.Index(), Begin: b.Begin, Length: b.Length}
}

// block returns the piece and block index for (index, begin), or ok=false
// when they don't match a block.
func (pk *Picker) block(index, begin int) (*piece.Piece, int, bool) {
	if index < 0 || index >= len(pk.pieces) {
		return nil, 0, false
	}
	pc := pk.pieces[index]
	i := pc.BlockAt(begin)
	return pc, i, i >= 0
}

// Release takes back a block requested from p, after a timeout, a choke or a
// disconnect. The block can then be picked again.
func (pk *Picker) Release(index, begin int, p *peer.Peer) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	pc, i, ok := pk.block(index, begin)
	if !ok {
		return
	}
	before := pk.missing
	pk.update(pc, func() { pc.Release(i, p) })
	if pk.missing != before {
		pk.notify()
	}
}

// Wanted reports whether a block of that position and length is still
// needed.
func (pk *Picker) Wanted(index, begin, length int) bool {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	pc, i, ok := pk.block(index, begin)
	return ok && pc.Block(i).Length == length && pc.Block(i).Status != piece.ReceivedBlock
}

// Received marks a block as received, whoever sent it. It returns every peer
// the block was requested from, so their requests can be settled and
// cancelled. fresh is false for a duplicate or an unknown block. pieceDone
// reports that this block completed its piece, which then needs its hash
// checked.
func (pk *Picker) Received(index, begin int) (requesters []*peer.Peer, pieceDone, fresh bool) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	pc, i, ok := pk.block(index, begin)
	if !ok {
		return nil, false, false
	}
	pk.update(pc, func() { requesters, fresh = pc.Receive(i) })
	if !fresh {
		return nil, false, false
	}
	if pc.IsDone() {
		pk.donePieces++
		pieceDone = true
	}
	return requesters, pieceDone, true
}

// PieceFailed puts every block of a piece that failed its hash check back
// to missing.
func (pk *Picker) PieceFailed(index int) {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	if index < 0 || index >= len(pk.pieces) {
		return
	}
	pc := pk.pieces[index]
	if pc.IsDone() {
		pk.donePieces--
	}
	pk.update(pc, pc.Reset)
	pk.notify()
}

// Complete reports whether every piece is done.
func (pk *Picker) Complete() bool {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	return pk.donePieces == len(pk.pieces)
}

// SyncWithProgress marks as received every block that lies entirely inside a
// saved progress range, and returns how many bytes that covers.
func (pk *Picker) SyncWithProgress(progress []config.ProgressRange, pieceSize int) int {
	pk.mu.Lock()
	defer pk.mu.Unlock()
	received := 0
	for _, pg := range progress {
		first := max(pg.Start/pieceSize, 0)
		last := min((pg.End-1)/pieceSize, len(pk.pieces)-1)
		for index := first; index <= last; index++ {
			pc := pk.pieces[index]
			wasDone := pc.IsDone()
			for i := range pc.BlockCount() {
				b := pc.Block(i)
				start := index*pieceSize + b.Begin
				if start < pg.Start || start+b.Length > pg.End {
					continue
				}
				var fresh bool
				pk.update(pc, func() { _, fresh = pc.Receive(i) })
				if fresh {
					received += b.Length
				}
			}
			if !wasDone && pc.IsDone() {
				pk.donePieces++
			}
		}
	}
	return received
}
