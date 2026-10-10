package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/nazifbara/ziftorrent/internal/client"
	"github.com/nazifbara/ziftorrent/internal/config"
	"github.com/nazifbara/ziftorrent/internal/torrent"
)

func main() {
	torrentPath := flag.String("f", "", "path to the torrent file")
	trackerURL := flag.String("t", "", "tracker url")
	flag.Parse()
	code, err := run(*torrentPath, *trackerURL)
	if err != nil {
		fmt.Println(err.Error())
	}
	os.Exit(code)
}

func run(torrentPath, trackerURL string) (int, error) {
	if torrentPath == "" {
		return 1, errors.New("path to torrent file not provided")
	}
	t, err := torrent.Open(torrentPath)
	if err != nil {
		return 1, err
	}
	c, err := config.Load()
	if err != nil || c == nil {
		return 1, errors.New("config not load")
	}
	tc := client.NewClient(c)
	d := tc.NewDownload(t)
	var clientErr error
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	go func() {
		clientErr = d.Start(trackerURL)
		cancel()
	}()
	<-ctx.Done()
	// Stop cancels every download and closes the files it opened.
	tc.Stop()
	if clientErr != nil {
		return 1, clientErr
	}
	return 0, nil
}
