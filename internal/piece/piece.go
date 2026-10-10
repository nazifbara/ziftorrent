package piece

import (
	"fmt"
	"sync"
)

type State struct {
	mu          sync.Mutex
	bytes       []byte
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
	return &State{bytes: make([]byte, size), pieceSize: size, numOfBlocks: blocks}
}

func (p *State) BlockCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.numOfBlocks
}

func (p *State) IsDone() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.done
}

func (p *State) AddBlock(begin uint32, block []byte) (bool, []byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	end := int(begin) + len(block)
	if end > len(p.bytes) {
		return false, nil, fmt.Errorf("block exceeds piece boundary")
	}
	copy(p.bytes[begin:end], block)
	p.blocksRead++
	p.done = p.blocksRead == p.numOfBlocks
	if !p.done {
		return false, nil, nil
	}
	return true, append([]byte(nil), p.bytes...), nil
}

func (p *State) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.blocksRead = 0
	p.bytes = make([]byte, p.pieceSize)
	p.done = false
}

func (p *State) Release() {
	p.mu.Lock()
	p.bytes = nil
	p.mu.Unlock()
}

func (p *State) Size() uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.pieceSize
}
