package piece

import (
	"slices"

	"github.com/nazifbara/ziftorrent/internal/peer"
)

type BlockStatus int

const (
	MissingBlock BlockStatus = iota
	RequestedBlock
	ReceivedBlock
)

type Block struct {
	Begin      int
	Length     int
	Status     BlockStatus
	Requesters []*peer.Peer
}

// Piece tracks the state of each block of one piece. It is not safe for
// concurrent use: the picker guards every piece with its own mutex.
type Piece struct {
	index    int
	size     int
	blocks   []Block
	missing  int
	received int
}

func New(index, size, blockSize int) *Piece {
	numOfBlocks := size / blockSize
	if size%blockSize != 0 {
		numOfBlocks++
	}
	blocks := make([]Block, numOfBlocks)
	for i := range blocks {
		begin := i * blockSize
		blocks[i] = Block{Begin: begin, Length: min(blockSize, size-begin)}
	}
	return &Piece{index: index, size: size, blocks: blocks, missing: numOfBlocks}
}

func (p *Piece) Index() int { return p.index }

func (p *Piece) Size() int { return p.size }

func (p *Piece) BlockCount() int { return len(p.blocks) }

func (p *Piece) Block(i int) *Block { return &p.blocks[i] }

// BlockAt returns the index of the block starting at begin, or -1 when begin
// is not the start of a block.
func (p *Piece) BlockAt(begin int) int {
	if begin < 0 || len(p.blocks) == 0 || begin%p.blocks[0].Length != 0 {
		return -1
	}
	i := begin / p.blocks[0].Length
	if i >= len(p.blocks) {
		return -1
	}
	return i
}

// Missing reports how many blocks are neither requested nor received.
func (p *Piece) Missing() int { return p.missing }

// Started reports whether any block is requested or received.
func (p *Piece) Started() bool { return p.missing < len(p.blocks) }

func (p *Piece) IsDone() bool { return p.received == len(p.blocks) }

// Request records that block i was requested from a peer.
func (p *Piece) Request(i int, from *peer.Peer) {
	b := &p.blocks[i]
	if b.Status == MissingBlock {
		b.Status = RequestedBlock
		p.missing--
	}
	b.Requesters = append(b.Requesters, from)
}

// Release removes a peer from the requesters of block i. The block goes back
// to missing once nobody has it requested.
func (p *Piece) Release(i int, from *peer.Peer) {
	b := &p.blocks[i]
	b.Requesters = slices.DeleteFunc(b.Requesters, func(r *peer.Peer) bool { return r == from })
	if b.Status == RequestedBlock && len(b.Requesters) == 0 {
		b.Status = MissingBlock
		p.missing++
	}
}

// Receive marks block i as received and returns the peers it was requested
// from. fresh is false when the block had already been received.
func (p *Piece) Receive(i int) (requesters []*peer.Peer, fresh bool) {
	b := &p.blocks[i]
	if b.Status == ReceivedBlock {
		return nil, false
	}
	if b.Status == MissingBlock {
		p.missing--
	}
	b.Status = ReceivedBlock
	p.received++
	requesters = b.Requesters
	b.Requesters = nil
	return requesters, true
}

// Reset puts every block back to missing, e.g. after a failed hash check.
func (p *Piece) Reset() {
	for i := range p.blocks {
		p.blocks[i].Status = MissingBlock
		p.blocks[i].Requesters = nil
	}
	p.missing = len(p.blocks)
	p.received = 0
}
