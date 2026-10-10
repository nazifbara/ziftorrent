package download

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/piece"
	"github.com/nazifbara/ziftorrent/internal/torrent"
	"github.com/nazifbara/ziftorrent/internal/tracker"
	"github.com/nazifbara/ziftorrent/internal/wire"
)

type fileState struct {
	begin       int64
	size        int64
	file        *os.File
	blocksWrote int
	numOfBlocks int
	name        string
	mu          sync.Mutex
}

type Download struct {
	mu              sync.Mutex
	torrent         *torrent.Torrent
	queue           chan *job
	addrQueue       chan *net.TCPAddr
	pending         map[string]*pendingRequest
	pieces          []*piece.State
	startedAt       time.Time
	totalDownloaded atomic.Int64
	done            chan struct{}
	doneOnce        sync.Once
	tracker         *tracker.Tracker
	files           []*fileState
	ctx             context.Context
	cancel          context.CancelFunc
	wg              sync.WaitGroup
	filesOnce       sync.Once
}

func New(t *torrent.Torrent) *Download {
	if t == nil {
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	pieces := make([]*piece.State, t.NumOfPieces)
	for i := range pieces {
		size := t.PieceSize
		if i == len(pieces)-1 {
			size = t.FinalPieceSize
		}
		pieces[i] = piece.New(size, config.BlockSize)
	}
	return &Download{
		torrent: t,
		pieces:  pieces,
		queue:   make(chan *job, t.NumOfBlocks),
		pending: make(map[string]*pendingRequest),
		ctx:     ctx,
		cancel:  cancel,
		files:   make([]*fileState, len(t.Files)),
		done:    make(chan struct{}),
	}
}

func createFileFromPath(root string, path []string) (*os.File, error) {
	if len(path) == 0 {
		return nil, fmt.Errorf("path must not be empty")
	}
	current := root
	for i, p := range path {
		if current == "" {
			current = p
		} else {
			current = filepath.Join(current, p)
		}
		if i != len(path)-1 {
			if err := os.Mkdir(current, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
				return nil, err
			}
			continue
		}
		return os.OpenFile(current, os.O_WRONLY|os.O_CREATE, 0o666)
	}
	return nil, fmt.Errorf("unreachable")
}

func (d *Download) Start(trackerURL string) error {
	announceList := d.torrent.AnnounceList
	if len(announceList) == 0 && d.torrent.Announce != "" {
		announceList = [][]string{{d.torrent.Announce}}
	}
	if trackerURL != "" {
		announceList = [][]string{{trackerURL}}
	}

	parentFolder := ""
	if len(d.torrent.Files) > 1 {
		if err := os.Mkdir(d.torrent.Name, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		parentFolder = d.torrent.Name
	}

	var fileBegin int64
	for i, f := range d.torrent.Files {
		file, err := createFileFromPath(parentFolder, f.Path)
		if err != nil {
			return err
		}
		if i > 0 {
			fileBegin += d.torrent.Files[i-1].Length
		}
		blocks := f.Length / int64(config.BlockSize)
		if f.Length%int64(config.BlockSize) != 0 {
			blocks++
		}
		d.files[i] = &fileState{begin: fileBegin, size: f.Length, file: file, numOfBlocks: int(blocks), name: f.Path[len(f.Path)-1]}
	}

	foundTracker, err := tracker.Find(announceList, d.torrent)
	if err != nil {
		return err
	}
	d.startedAt = time.Now()
	d.tracker = foundTracker
	fmt.Println("⬇️ Downloading...")
	return d.download()
}

func (d *Download) download() error {
	addresses := d.tracker.PeerAddresses()
	d.addrQueue = make(chan *net.TCPAddr, len(addresses))
	for _, address := range addresses {
		d.addrQueue <- address
	}
	for i := range d.pieces {
		d.addPieceJob(uint32(i))
	}

	go func() {
		for {
			select {
			case <-d.ctx.Done():
				return
			case <-d.done:
				return
			case address, ok := <-d.addrQueue:
				if !ok {
					return
				}
				p, err := d.connectToPeer(address)
				if err != nil {
					d.requeueAddress(address)
					continue
				}
				if err := peer.Handshake(p, d.torrent.InfoHash); err != nil {
					p.Close()
					d.requeueAddress(address)
					continue
				}
				if _, err := p.Write(wire.BuildInterested()); err != nil {
					p.Close()
					d.requeueAddress(address)
					continue
				}
				go d.readPeerMessages(p)
			}
		}
	}()

	<-d.done
	d.Stop()
	for _, p := range d.tracker.Peers() {
		if p != nil {
			p.Close()
		}
	}
	return nil
}

func (d *Download) Stop() {
	d.cancel()
	d.wg.Wait()
	d.closeFiles()
}

func (d *Download) Done() <-chan struct{} { return d.done }

func (d *Download) Progress() (downloaded, total int64) {
	return d.totalDownloaded.Load(), int64(d.torrent.ContentSize)
}

func (d *Download) connectToPeer(address *net.TCPAddr) (*peer.Peer, error) {
	if address == nil {
		return nil, errors.New("address is nil")
	}
	if existing := d.tracker.FindPeer(address); existing != nil {
		return existing, nil
	}
	conn, err := net.DialTimeout("tcp", address.String(), 300*time.Millisecond)
	if err != nil {
		return nil, err
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("connection is not *net.TCPConn: %T", conn)
	}
	p := peer.New(tcpConn, address, d.torrent.NumOfPieces)
	d.tracker.AddPeer(p)
	return p, nil
}

func (d *Download) requeueAddress(address *net.TCPAddr) {
	select {
	case d.addrQueue <- address:
	case <-d.ctx.Done():
	default:
	}
}

func (d *Download) closeFiles() {
	d.filesOnce.Do(func() {
		for _, f := range d.files {
			if f != nil && f.file != nil {
				f.file.Close()
			}
		}
	})
}
