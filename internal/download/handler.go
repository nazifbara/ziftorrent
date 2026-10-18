package download

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"slices"
	"time"

	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/picker"
	"github.com/nazifbara/ziftorrent/internal/utils"
	"github.com/nazifbara/ziftorrent/internal/wire"
)

// tryRequestPeer sends req to p. The caller holds one of p's slots, and the
// picker has req recorded as requested from p.
func (d *Download) tryRequestPeer(p *peer.Peer, req picker.Request) {
	if !p.CanRequest(uint32(req.Index)) {
		// Choked since the wait: put the block back.
		d.picker.Release(req.Index, req.Begin, p)
		p.ReleaseSlot()
		return
	}
	key := keyFor(p, req)
	// Register before sending, so the reply can't arrive before its entry.
	d.mu.Lock()
	d.pending[key] = &pendingRequest{
		req:    req,
		sentAt: time.Now(),
		timer:  time.AfterFunc(p.RequestTimeout(), func() { d.onTimeout(key) }),
	}
	d.mu.Unlock()

	if _, err := p.Write(wire.BuildRequest(uint32(req.Index), uint32(req.Begin), uint32(req.Length))); err != nil {
		if _, ok := d.takePending(key); ok {
			p.ReleaseSlot()
			p.RecordFailure()
			d.picker.Release(req.Index, req.Begin, p)
		}
	}
}

// pick waits until the picker has a block for p. ok is false when the
// download or the peer stopped first.
func (d *Download) pick(p *peer.Peer) (req picker.Request, ok bool) {
	for {
		changed := d.picker.Changed()
		if req, ok := d.picker.Pick(p); ok {
			return req, true
		}
		select {
		case <-changed:
		case <-d.ctx.Done():
			return picker.Request{}, false
		case <-p.Done():
			return picker.Request{}, false
		}
	}
}

func (d *Download) runPeerWorker(p *peer.Peer) {
	for p.WaitUnchoked(d.ctx) && p.AcquireSlot(d.ctx) {
		req, ok := d.pick(p)
		if !ok {
			p.ReleaseSlot()
			break
		}
		d.tryRequestPeer(p, req)
	}
	if d.ctx.Err() == nil {
		d.removePeer(p)
		d.requeueAddress(p.Address())
	}
}

const maxMessageLength = 1 << 20

// readPeerMessages must be started with d.wg.Go so Stop waits for it.
func (d *Download) readPeerMessages(p *peer.Peer) {
	// Unblock the pending Read on shutdown, and drop the peer once its
	// connection is unusable so its worker stops pulling jobs.
	defer p.Close()
	stopWatch := make(chan struct{})
	defer close(stopWatch)
	go func() {
		select {
		case <-d.ctx.Done():
			p.Close()
		case <-stopWatch:
		}
	}()

	chunk := make([]byte, 32*1024)
	var buffer []byte
	for {
		n, err := p.Read(chunk)
		if err != nil {
			return
		}
		buffer = append(buffer, chunk[:n]...)

		for len(buffer) >= 4 {
			length := int(binary.BigEndian.Uint32(buffer[:4]))
			if length > maxMessageLength {
				return
			}
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

		buffer = append(buffer[:0:0], buffer...)
	}
}

func (d *Download) KeepPeerAlive(p *peer.Peer) {
	ticker := time.NewTicker(keepAliveInterval)
	defer ticker.Stop()
	keepAlive := wire.BuildKeepAlive()
	for {
		select {
		case <-ticker.C:
			if p.Choked() {
				if _, err := p.Write(keepAlive); err != nil {
					p.Close()
					return
				}
			}
		case <-d.ctx.Done():
			return
		case <-p.Done():
			return
		}
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
		d.dropPendingFor(p)
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
	d.picker.Have(p, int(payload.Index))
}

func (d *Download) onBitfield(p *peer.Peer, msg wire.Message) {
	payload := wire.ParseBitfieldPayload(msg.Payload)
	p.SetBitfield(payload.Bitfield)
	d.picker.SetBitfield(p, payload.Bitfield)
}

func (d *Download) onBlockReceived(p *peer.Peer, msg wire.Message) {
	payload, err := wire.ParseBlockPayload(msg.Payload)
	if err != nil {
		return
	}
	index, begin, length := int(payload.Index), int(payload.Begin), len(payload.Block)
	// A block nobody needs anymore (a duplicate, or one that was never
	// requested) is dropped. A late block after a timeout is still kept.
	if !d.picker.Wanted(index, begin, length) {
		d.settleRequest(p, pendingKey{index: index, begin: begin, peer: p})
		return
	}
	// Write before counting the block: once the picker reports the piece as
	// done, every one of its blocks is on disk and readPiece can verify it.
	if err := d.writeToFile(payload.Index, payload.Begin, payload.Block); err != nil {
		fmt.Println("❌ Couldn't write to file")
		d.finish()
		return
	}

	requesters, pieceDone, fresh := d.picker.Received(index, begin)
	if !fresh {
		// Another copy got there first.
		d.settleRequest(p, pendingKey{index: index, begin: begin, peer: p})
		return
	}
	for _, r := range requesters {
		d.settleRequest(p, pendingKey{index: index, begin: begin, peer: r})
	}
	d.totalDownloaded.Add(int64(length))
	if pieceDone {
		pieceBytes, err := d.readPiece(index)
		isValid := err == nil && index < len(d.torrent.PieceHashes) && sha1.Sum(pieceBytes) == d.torrent.PieceHashes[index]
		if !isValid {
			fmt.Printf("⚠️ Re-downloading piece %d due to invalid hash\n", index)
			d.totalDownloaded.Add(-int64(d.picker.PieceSize(index)))
			d.picker.PieceFailed(index)
			return
		}
	}
	if d.picker.Complete() {
		fmt.Printf("🔥 Download completed in %v\n", time.Since(d.startedAt))
		d.finish()
	}
}

// settleRequest removes the request for key once its block arrived from
// sender. A request to another peer is cancelled, since its copy is no
// longer needed.
func (d *Download) settleRequest(sender *peer.Peer, key pendingKey) {
	pr, ok := d.takePending(key)
	if !ok {
		return
	}
	key.peer.ReleaseSlot()
	if key.peer == sender {
		sender.RecordRTT(time.Since(pr.sentAt))
		return
	}
	key.peer.Write(wire.BuildCancel(uint32(pr.req.Index), uint32(pr.req.Begin), uint32(pr.req.Length)))
}

func (d *Download) writeToFile(pieceIndex, pieceBegin uint32, block []byte) error {
	return d.writeAtGlobal(utils.GlobalBegin(pieceIndex, pieceBegin, d.torrent.PieceSize), block)
}

// readPiece reads a whole piece back from disk. A piece is a contiguous range
// of the torrent-wide byte stream, so it may start in the middle of one file
// and continue into the following ones.
func (d *Download) readPiece(pieceIndex int) ([]byte, error) {
	piece := make([]byte, d.picker.PieceSize(pieceIndex))
	globalBegin := utils.GlobalBegin(uint32(pieceIndex), 0, d.torrent.PieceSize)
	for buf := piece; len(buf) > 0; {
		index := slices.IndexFunc(d.files, func(f *fileState) bool {
			return f.begin <= globalBegin && globalBegin < f.begin+f.size
		})
		if index == -1 {
			return nil, fmt.Errorf("no file found for global offset %d", globalBegin)
		}
		n, err := d.files[index].readAt(globalBegin, buf)
		if err != nil {
			return nil, err
		}
		buf = buf[n:]
		globalBegin += n
	}
	return piece, nil
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
		d.config.RecordProgress(int(globalBegin), int(n))
		data = data[n:]
		globalBegin += n
	}
	return nil
}

// writeAt writes as much of data as fits in this file, starting at the
// torrent-wide offset globalBegin, and returns how many bytes it consumed.
func (f *fileState) writeAt(globalBegin int64, data []byte) (int64, error) {
	offset := globalBegin - f.begin
	n := min(int64(len(data)), f.size-offset)
	if _, err := f.file.WriteAt(data[:n], offset); err != nil {
		return 0, err
	}
	f.blocksWrote.Add(1)
	if int(f.blocksWrote.Load()) == f.numOfBlocks {
		fmt.Printf("✅ %s completed\n", f.name)
	}
	return n, nil
}

// readAt fills as much of buf as this file holds, starting at the
// torrent-wide offset globalBegin, and returns how many bytes it read.
func (f *fileState) readAt(globalBegin int64, buf []byte) (int64, error) {
	offset := globalBegin - f.begin
	n := min(int64(len(buf)), f.size-offset)
	if _, err := f.file.ReadAt(buf[:n], offset); err != nil {
		return 0, err
	}
	return n, nil
}

// finish closes the done channel exactly once.
func (d *Download) finish() {
	d.doneOnce.Do(func() { close(d.done) })
}
