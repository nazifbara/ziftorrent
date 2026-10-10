package config

import (
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	BlockSize  uint32 = 16384
	configPath        = ".config/ziftorrent/"
)

type PendingItem struct {
	Index uint32
	Begin uint32
}

type Config struct {
	mux            sync.Mutex
	pendingMap     map[string][]PendingItem
	pendingMapFile *os.File
	BlockSize      uint32
}

func (c *Config) AddPending(key string, item PendingItem) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.pendingMap == nil {
		c.pendingMap = make(map[string][]PendingItem)
	}
	c.pendingMap[key] = append(c.pendingMap[key], item)
}

func (c *Config) Save() error {
	c.mux.Lock()
	defer c.mux.Unlock()
	encoder := json.NewEncoder(c.pendingMapFile)
	return encoder.Encode(c.pendingMap)
}

func Load() (*Config, error) {
	pendingMapFile, err := OpenCreatePendingMapFile()
	if err != nil {
		return nil, err
	}
	var pendingMap map[string][]PendingItem
	decoder := json.NewDecoder(pendingMapFile)
	if err := decoder.Decode(&pendingMap); err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	if pendingMap == nil {
		pendingMap = make(map[string][]PendingItem)
	}
	return &Config{
		pendingMapFile: pendingMapFile,
		BlockSize:      BlockSize,
		pendingMap:     pendingMap,
	}, err
}

func OpenCreatePendingMapFile() (*os.File, error) {
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	configDirPath := filepath.Join(homePath, configPath)
	if err := os.MkdirAll(configDirPath, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	return os.OpenFile(filepath.Join(configDirPath, ".pending_map"), os.O_RDWR|os.O_CREATE, 0o644)
}
