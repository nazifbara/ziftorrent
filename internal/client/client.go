package client

import (
	"sync"

	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/download"
	"github.com/nazifbara/ziftorrent/internal/torrent"
)

type Client struct {
	mu        sync.Mutex
	downloads []*download.Download
	config    *config.Config
}

func NewClient(c *config.Config) *Client {
	return &Client{
		config: c,
	}
}

func (c *Client) NewDownload(torrent *torrent.Torrent) *download.Download {
	d := download.New(torrent, c.config)
	if d == nil {
		return nil
	}
	c.mu.Lock()
	c.downloads = append(c.downloads, d)
	c.mu.Unlock()
	return d
}

func (c *Client) Stop() {
	c.mu.Lock()
	downloads := append([]*download.Download(nil), c.downloads...)
	c.mu.Unlock()
	for _, d := range downloads {
		d.Stop()
	}
}

func (c *Client) DownloadCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.downloads)
}
