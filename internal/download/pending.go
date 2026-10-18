package download

import (
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/picker"
)

// pendingKey identifies one request to one peer. In endgame the same block
// can be pending with several peers.
type pendingKey struct {
	index int
	begin int
	peer  *peer.Peer
}

type pendingRequest struct {
	req    picker.Request
	timer  *time.Timer
	sentAt time.Time
}

func keyFor(p *peer.Peer, req picker.Request) pendingKey {
	return pendingKey{index: req.Index, begin: req.Begin, peer: p}
}

// takePending removes the pending entry for key and reports whether it was
// there. Whoever removes an entry releases its slot, exactly once.
func (d *Download) takePending(key pendingKey) (*pendingRequest, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()
	pr, ok := d.pending[key]
	if ok {
		delete(d.pending, key)
		pr.timer.Stop()
	}
	return pr, ok
}

// dropPendingFor settles every request pending with p, on choke or
// disconnect, and hands the blocks back to the picker. It doesn't count as
// a failure.
func (d *Download) dropPendingFor(p *peer.Peer) {
	d.mu.Lock()
	var dropped []*pendingRequest
	for key, pr := range d.pending {
		if key.peer == p {
			pr.timer.Stop()
			dropped = append(dropped, pr)
			delete(d.pending, key)
		}
	}
	d.mu.Unlock()
	for _, pr := range dropped {
		p.ReleaseSlot()
		d.picker.Release(pr.req.Index, pr.req.Begin, p)
	}
}

func (d *Download) onTimeout(key pendingKey) {
	pr, ok := d.takePending(key)
	if !ok {
		return
	}
	key.peer.ReleaseSlot()
	key.peer.RecordFailure()
	d.picker.Release(pr.req.Index, pr.req.Begin, key.peer)
}

// removePeer forgets a peer whose connection is gone.
func (d *Download) removePeer(p *peer.Peer) {
	d.dropPendingFor(p)
	d.picker.RemovePeer(p)
}
