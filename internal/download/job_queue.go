package download

import (
	"fmt"
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
)

type job struct {
	index uint32
	begin uint32
}

type pendingRequest struct {
	job    *job
	peer   *peer.Peer
	timer  *time.Timer
	sentAt time.Time
}

func (j *job) String() string {
	return fmt.Sprintf("job{index:%d, begin:%d}", j.index, j.begin)
}

func pendingKey(index, begin uint32) string {
	return fmt.Sprintf("%d:%d", index, begin)
}

func (d *Download) enqueue(job *job) {
	select {
	case d.queue <- job:
	case <-d.ctx.Done():
	}
}

func (d *Download) requeue(job *job) {
	select {
	case d.queue <- job:
	case <-d.ctx.Done():
	default:
		go func() {
			select {
			case d.queue <- job:
			case <-d.ctx.Done():
			}
		}()
	}
}

func (d *Download) onTimeout(key string, job *job) {
	d.mu.Lock()
	_, stillPending := d.pending[key]
	delete(d.pending, key)
	d.mu.Unlock()
	if stillPending {
		d.requeue(job)
	}
}

func (d *Download) addPieceJobsBetween(startIndex, endIndex uint32, startOffset, endOffset uint64) {
	d.pieces[startIndex].SyncWithOffset(uint32(startOffset) - startIndex)
	d.pieces[endIndex].SyncWithOffset(uint32(endOffset) - endIndex)
	for i := startIndex; i <= endIndex; i++ {
		d.addPieceJob(i)
	}
}

func (d *Download) addPieceJob(pieceIndex uint32) {
	pieceState := d.pieces[pieceIndex]
	for i := pieceState.BlockRead(); i < pieceState.BlockCount(); i++ {
		offset := uint32(i) * d.config.BlockSize
		d.addToQueue(pieceIndex, offset)
	}
}

func (d *Download) addToQueue(pieceIndex uint32, offset uint32) {
	pieceState := d.pieces[pieceIndex]
	if pieceState.IsDone() {
		return
	}
	d.enqueue(&job{index: pieceIndex, begin: offset})
}
