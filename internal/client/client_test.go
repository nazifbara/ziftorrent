package client

import (
	"testing"

	"github.com/nazifbara/ziftorrent/internal/torrent"
)

func TestNewDownloadRegistersTransfer(t *testing.T) {
	c := NewClient()
	t.Cleanup(c.Stop)

	d := c.NewDownload(&torrent.Torrent{NumOfPieces: 1, PieceSize: 1, FinalPieceSize: 1, NumOfBlocks: 1})
	if d == nil {
		t.Fatal("expected a download")
	}
	if c.DownloadCount() != 1 {
		t.Fatal("expected client to retain the created download")
	}
}

func TestNewDownloadRejectsNilTorrent(t *testing.T) {
	c := NewClient()
	t.Cleanup(c.Stop)
	if got := c.NewDownload(nil); got != nil {
		t.Fatalf("expected nil download, got %#v", got)
	}
}
