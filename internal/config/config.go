package config

import (
	"cmp"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

const (
	BlockSize        int = 16384
	progressFileName     = "progress"
)

type ProgressRange struct {
	Start int
	End   int
}

type Config struct {
	mux            sync.Mutex
	progressRanges []ProgressRange
	dirPath        string
	BlockSize      int
}

func (c *Config) GetProgressRanges() []ProgressRange {
	return c.progressRanges
}

func (c *Config) RecordProgress(globalBegin int, blockSize int) {
	c.mux.Lock()
	defer c.mux.Unlock()

	newItem := ProgressRange{
		Start: globalBegin,
		End:   globalBegin + blockSize,
	}

	ranges := c.progressRanges

	// Find the correct insertion index to keep the slice sorted by Start
	idx, _ := slices.BinarySearchFunc(ranges, newItem.Start, func(r ProgressRange, target int) int {
		return cmp.Compare(r.Start, target)
	})

	// Insert the new item at the sorted position
	ranges = slices.Insert(ranges, idx, newItem)

	// Sweep through and merge any overlapping or touching ranges
	merged := make([]ProgressRange, 0, len(ranges))
	for _, current := range ranges {
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

	c.progressRanges = merged
}

func (c *Config) SaveProgress() error {
	c.mux.Lock()
	ranges := slices.Clone(c.progressRanges)
	c.mux.Unlock()

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
	if err := encoder.Encode(ranges); err != nil {
		return err
	}

	return os.Rename(tmpName, c.progressFileName())
}

func (c *Config) progressFileName() string {
	return filepath.Join(c.dirPath, progressFileName)
}

func Load(infohash []byte, destination string) (*Config, error) {
	homePath, err := os.UserHomeDir()
	if err != nil {
		return nil, err
	}
	infohashStr := hex.EncodeToString(infohash)
	downloadKey := infohashStr + "|" + destination
	keyHash := sha1.Sum([]byte(downloadKey))
	keyHashStr := hex.EncodeToString(keyHash[:])
	configDirPath := filepath.Join(homePath, ".config/ziftorrent", keyHashStr)
	if err := os.MkdirAll(configDirPath, 0o755); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, err
	}
	progressFile, err := os.OpenFile(filepath.Join(configDirPath, progressFileName), os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	defer progressFile.Close()
	var progressRanges []ProgressRange
	decoder := json.NewDecoder(progressFile)
	if err := decoder.Decode(&progressRanges); err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}
	}

	return &Config{
		BlockSize:      BlockSize,
		progressRanges: progressRanges,
		dirPath:        configDirPath,
	}, err
}
