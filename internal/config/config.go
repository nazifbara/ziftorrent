package config

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
)

const (
	BlockSize  uint32 = 16384
	configPath        = ".config/ziftorrent/"
)

type Progress struct {
	RemainingJobs map[string][]struct {
		Index uint32 `json:"index"`
		Begin uint32 `json:"begin"`
	} `json:"remaining_jobs"`
}

type Config struct {
	mux          sync.Mutex
	ProgressFile *os.File
	BlockSize    uint32
}

func Load() (*Config, error) {
	progressFile, err := OpenCreateProgressFile()
	if err != nil {
		return nil, err
	}
	return &Config{
		ProgressFile: progressFile,
		BlockSize:    BlockSize,
	}, err
}

func OpenCreateProgressFile() (*os.File, error) {
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	configDirPath := filepath.Join(homePath, configPath)
	if err := os.MkdirAll(configDirPath, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return os.OpenFile(filepath.Join(configDirPath, ".progress"), os.O_WRONLY|os.O_CREATE, 0o666)
}
