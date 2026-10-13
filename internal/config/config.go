package config

import (
	"cmp"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

const (
	BlockSize        uint32 = 16384
	progressFileName        = "progress"
)

type ProgressRange struct {
	Start int64
	End   int64
}

type Config struct {
	mux       sync.Mutex
	progress  map[string][]ProgressRange
	dirPath   string
	BlockSize uint32
}

func (c *Config) GetProgress(key string) []ProgressRange {
	return c.progress[key]
}

func (c *Config) RecordProgress(key string, globalBegin int64, blockSize uint32) {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.progress == nil {
		c.progress = make(map[string][]ProgressRange)
	}

	newItem := ProgressRange{
		Start: globalBegin,
		End:   globalBegin + int64(blockSize),
	}

	entry := c.progress[key]

	// Find the correct insertion index to keep the slice sorted by Start
	idx, _ := slices.BinarySearchFunc(entry, newItem.Start, func(item ProgressRange, target int64) int {
		return cmp.Compare(item.Start, target)
	})

	// Insert the new item at the sorted position
	entry = slices.Insert(entry, idx, newItem)

	// Sweep through and merge any overlapping or touching ranges
	merged := make([]ProgressRange, 0, len(entry))
	for _, current := range entry {
		if len(merged) == 0 {
			merged = append(merged, current)
		} else {
			last := &merged[len(merged)-1]

			// If current range overlaps or touches the previous one (e.g. End >= Start)
			if current.Start <= last.End {
				// Extend the last range's end if the current one goes further
				if current.End > last.End {
					last.End = current.End
				}
			} else {
				// No overlap, add as a new separate range
				merged = append(merged, current)
			}
		}
	}

	c.progress[key] = merged
}

func (c *Config) SaveProgress() error {
	c.mux.Lock()
	defer c.mux.Unlock()

	tmpFile, err := os.CreateTemp(c.dirPath, "progress-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmpFile.Name()

	defer func() {
		tmpFile.Close()
		os.Remove(tmpName)
	}()

	encoder := json.NewEncoder(tmpFile)
	if err := encoder.Encode(c.progress); err != nil {
		return err
	}
	return os.Rename(tmpName, c.progressFileName())
}

func (c *Config) progressFileName() string {
	return filepath.Join(c.dirPath, progressFileName)
}

func Load() (*Config, error) {
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	configDirPath := filepath.Join(homePath, ".config/ziftorrent")
	if err := os.MkdirAll(configDirPath, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	progressFile, err := os.OpenFile(filepath.Join(configDirPath, "progress"), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	var progress map[string][]ProgressRange
	decoder := json.NewDecoder(progressFile)
	if err := decoder.Decode(&progress); err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	}
	if progress == nil {
		progress = make(map[string][]ProgressRange)
	}

	return &Config{
		BlockSize: BlockSize,
		progress:  progress,
		dirPath:   configDirPath,
	}, err
}
