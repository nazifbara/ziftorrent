package download

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/utils"
	"github.com/nazifbara/ziftorrent/internal/wire"
)

func (d *Download) tryRequestPeer(p *peer.Peer, job *job) {
	if !p.CanRequest(job.index) {
		d.requeue(job)
		return
	}

	req := wire.BuildRequest(job.index, job.begin, d.torrent.BlockLength(job.index, job.begin))
	sentAt := time.Now()
	if _, err := p.Write(req); err != nil {
		d.requeue(job)
		p.RecordFailure()
		return
	}

	timeout := p.RequestTimeout()
	key := pendingKey(job.index, job.begin)

	d.mu.Lock()
	d.pending[key] = &pendingRequest{
		job:    job,
		peer:   p,
		sentAt: sentAt,
		timer: time.AfterFunc(timeout, func() {
			d.onTimeout(key, job)
		}),
	}
	d.mu.Unlock()
}

func (d *Download) runPeerWorker(p *peer.Peer) {
	d.wg.Add(1)
	defer d.wg.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case <-p.Done():
			d.requeueAddress(p.Address())
			return
		case job, ok := <-d.queue:
			if !ok {
				return
			}
			d.tryRequestPeer(p, job)
		}
	}
}

func (d *Download) readPeerMessages(p *peer.Peer) {
	d.wg.Add(1)
	defer d.wg.Done()

	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-d.ctx.Done():
			p.Close()
		case <-stopWatch:
		}
	}()

	var buffer []byte
	for {
		select {
		case <-d.ctx.Done():
			return
		default:
		}

		chunk := make([]byte, 1024)
		n, err := p.Read(chunk)
		if err != nil {
			p.RecordFailure()
			return
		}

		buffer = append(buffer, chunk[:n]...)

		for {
			if len(buffer) < 4 {
				break
			}

			length := int(binary.BigEndian.Uint32(buffer[:4]))
			if length == 0 {
				buffer = buffer[4:]
				continue
			}

			total := 4 + length
			if len(buffer) < total {
				break
			}

			msg := buffer[:total]
			buffer = buffer[total:]

			d.onMessage(p, msg)
		}
		time.Sleep(time.Microsecond)
	}
}

func (d *Download) onMessage(p *peer.Peer, msg []byte) {
	message, err := wire.ParseMessage(msg)
	if err != nil {
		return
	}
	switch message.ID {
	case 0:
		p.SetChoked(true)
	case 1:
		p.SetChoked(false)
	case 4:
		d.onHave(p, message)
	case 5:
		d.onBitfield(p, message)
	case 7:
		d.onBlockReceived(p, message)
	}
}

func (d *Download) onHave(p *peer.Peer, msg wire.Message) {
	payload, err := wire.ParseHavePayload(msg.Payload)
	if err != nil {
		return
	}
	p.SetHave(payload.Index)
}

func (d *Download) onBitfield(p *peer.Peer, msg wire.Message) {
	payload := wire.ParseBitfieldPayload(msg.Payload)
	p.SetBitfield(payload.Bitfield)
	go d.runPeerWorker(p)
}

func (d *Download) onBlockReceived(p *peer.Peer, msg wire.Message) {
	payload, err := wire.ParseBlockPayload(msg.Payload)
	if err != nil {
		return
	}
	key := pendingKey(payload.Index, payload.Begin)

	d.mu.Lock()
	pr, ok := d.pending[key]
	if ok {
		delete(d.pending, key)
	}
	d.mu.Unlock()

	if !ok {
		return
	}

	pr.timer.Stop()
	if pr.peer == p {
		p.RecordRTT(time.Since(pr.sentAt))
	}

	if int(payload.Index) >= len(d.pieces) {
		return
	}
	pieceState := d.pieces[payload.Index]
	pieceDone, pieceBytes, err := pieceState.AddBlock(payload.Begin, payload.Block)
	if err != nil {
		return
	}
	d.totalDownloaded.Add(int64(len(payload.Block)))
	fmt.Printf("🍰 Progress %s / %s\n", utils.StringByteSize(d.totalDownloaded.Load()), utils.StringByteSize(int64(d.torrent.ContentSize)))
	if pieceDone {
		isValid := int(payload.Index) < len(d.torrent.PieceHashes) && sha1.Sum(pieceBytes) == d.torrent.PieceHashes[payload.Index]
		if !isValid {
			fmt.Printf("⚠️ Re-downloading piece %d due to invalid\n", payload.Index)
			pieceState.Reset()
			d.totalDownloaded.Add(-int64(pieceState.Size()))
			d.addPieceJob(pr.job.index)
			return
		}
		pieceState.Release()
	}
	go func(data []byte) {
		if err := d.writeToFile(payload.Index, payload.Begin, data); err != nil {
			fmt.Println("❌ Couldn't write to file")
			d.finish()
		}
		if err := d.config.SaveProgress(); err != nil {
			fmt.Println("❌ Couldn't save download progress")
		}
	}(payload.Block)
}

func (d *Download) writeToFile(pieceIndex, pieceBegin uint32, block []byte) error {
	return d.writeAtGlobal(utils.GlobalBegin(pieceIndex, pieceBegin, d.torrent.PieceSize), block)
}

func (d *Download) writeAtGlobal(globalBegin int64, data []byte) error {
	for len(data) > 0 {
		index := slices.IndexFunc(d.files, func(f *fileState) bool {
			return f.begin <= globalBegin && globalBegin < f.begin+f.size
		})
		if index == -1 {
			return fmt.Errorf("no file found for global offset %d", globalBegin)
		}
		n, err := d.files[index].writeAt(globalBegin, data)
		if err != nil {
			return err
		}
		d.config.RecordProgress(d.Key(), globalBegin, uint32(n))
		if d.totalDownloaded.Load() == int64(d.torrent.ContentSize) {
			fmt.Printf("🔥 Download completed in %v\n", time.Since(d.startedAt))
			d.finish()
		}
		data = data[n:]
		globalBegin += n
	}
	return nil
}

// writeAt writes as much of data as fits in this file, starting at the
// torrent-wide offset globalBegin, and returns how many bytes it consumed.
func (f *fileState) writeAt(globalBegin int64, data []byte) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	offset := globalBegin - f.begin
	n := min(int64(len(data)), f.size-offset)
	if _, err := f.file.WriteAt(data[:n], offset); err != nil {
		return 0, err
	}
	f.blocksWrote++
	if f.blocksWrote == f.numOfBlocks {
		fmt.Printf("✅ %s completed\n", f.name)
	}
	return n, nil
}

// finish closes the done channel exactly once.
func (d *Download) finish() {
	d.doneOnce.Do(func() { close(d.done) })
}
