package download

import (
	"context"
	"testing"
	"time"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/piece"
	"github.com/nazifbara/ziftorrent/internal/torrent"
)

func newQueueDownload(t *testing.T, queueCap int) *Download {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Download{
		queue:   make(chan *job, queueCap),
		pending: make(map[string]*pendingRequest),
		ctx:     ctx,
		cancel:  cancel,
		torrent: &torrent.Torrent{},
		config: &config.Config{
			BlockSize: config.BlockSize,
		},
	}
}

func TestRequeueDoesNotBlockWhenQueueIsFull(t *testing.T) {
	d := newQueueDownload(t, 1)
	d.enqueue(&job{index: 0})

	done := make(chan struct{})
	go func() {
		d.requeue(&job{index: 1})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("requeue blocked while the queue was full")
	}
}

func TestTimeoutRequeuesPendingJob(t *testing.T) {
	d := newQueueDownload(t, 1)
	j := &job{index: 2, begin: 100}
	key := pendingKey(j.index, j.begin)
	d.pending[key] = &pendingRequest{job: j}

	d.onTimeout(key, j)
	if _, ok := d.pending[key]; ok {
		t.Fatal("expected the pending request to be removed")
	}
	if got := <-d.queue; got != j {
		t.Fatal("expected the timed-out job to be requeued")
	}
}

func TestAddPieceJobEnqueuesEveryBlock(t *testing.T) {
	d := newQueueDownload(t, 3)
	d.pieces = []*piece.State{piece.New(3*16384, 16384)}

	d.addPieceJob(0)
	for want := 0; want < 3; want++ {
		j := <-d.queue
		if j.index != 0 || j.begin != uint32(want)*16384 {
			t.Fatalf("unexpected job: %v", j)
		}
	}
}
