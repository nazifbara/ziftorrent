package piece

import (
	"fmt"
	"sync"

	"github.com/nazifbara/ziftorrent/internal/config"
)

type State struct {
	mu          sync.Mutex
	blocksRead  int
	numOfBlocks int
	pieceSize   uint64
	done        bool
}

func New(size uint64, blockSize uint32) *State {
	blocks := int(size / uint64(blockSize))
	if size%uint64(blockSize) != 0 {
		blocks++
	}
	return &State{pieceSize: size, numOfBlocks: blocks}
}

func (p *State) SyncWithOffset(offset uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocksRead = int(offset / config.BlockSize)
}

func (p *State) BlockRead() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.blocksRead
}

func (p *State) BlockCount() int {
	return p.numOfBlocks
}

func (p *State) IsDone() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done
}

func (p *State) AddBlock(begin uint32, block []byte) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	end := int(begin) + len(block)
	if end > int(p.pieceSize) {
		return false, fmt.Errorf("block exceeds piece boundary")
	}
	p.blocksRead++
	p.done = p.blocksRead == p.numOfBlocks
	if !p.done {
		return false, nil
	}
	return true, nil
}

func (p *State) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocksRead = 0
	p.done = false
}

func (p *State) Size() uint64 {
	return p.pieceSize
}
