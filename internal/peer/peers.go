package peer

import (
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type Peer struct {
	conn         *net.TCPConn
	addr         *net.TCPAddr
	choked       bool
	bitfield     []bool
	done         chan struct{}
	failureCount atomic.Int32
	closeOnce    sync.Once
	rtt          *rttTracker
	mu           sync.Mutex
}

func New(conn *net.TCPConn, addr *net.TCPAddr, pieceCount uint64) *Peer {
	return &Peer{conn: conn, addr: addr, bitfield: make([]bool, pieceCount), done: make(chan struct{}), rtt: newRTTTracker(100)}
}

func (p *Peer) Read(b []byte) (int, error) { return p.conn.Read(b) }

func (p *Peer) Write(b []byte) (int, error) { return p.conn.Write(b) }

func (p *Peer) Close() error { return p.conn.Close() }

func (p *Peer) Done() <-chan struct{} { return p.done }

func (p *Peer) Address() *net.TCPAddr {
	if p == nil || p.addr == nil {
		return nil
	}
	return &net.TCPAddr{IP: append(net.IP(nil), p.addr.IP...), Port: p.addr.Port, Zone: p.addr.Zone}
}

func (p *Peer) MatchesAddress(addr *net.TCPAddr) bool {
	return p != nil && p.addr != nil && addr != nil && p.addr.IP.Equal(addr.IP) && p.addr.Port == addr.Port
}

func (p *Peer) CanRequest(pieceIndex uint32) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return !p.choked && int(pieceIndex) < len(p.bitfield) && p.bitfield[pieceIndex]
}

func (p *Peer) SetChoked(choked bool) {
	p.mu.Lock()
	p.choked = choked
	p.mu.Unlock()
}

func (p *Peer) SetHave(pieceIndex uint32) {
	p.mu.Lock()
	if int(pieceIndex) < len(p.bitfield) {
		p.bitfield[pieceIndex] = true
	}
	p.mu.Unlock()
}

func (p *Peer) SetBitfield(bitfield []bool) {
	p.mu.Lock()
	p.bitfield = append(p.bitfield[:0], bitfield...)
	p.mu.Unlock()
}

func (p *Peer) RecordRTT(sample time.Duration) { p.rtt.Record(sample) }

func (p *Peer) RequestTimeout() time.Duration { return p.rtt.Timeout() }

func (p *Peer) RecordFailure() {
	p.failureCount.Add(1)
	if p.failureCount.Load() > 5 {
		p.markDone()
		return
	}
}

func (p *Peer) markDone() {
	p.closeOnce.Do(func() {
		if p.done != nil {
			close(p.done)
		}
	})
}
