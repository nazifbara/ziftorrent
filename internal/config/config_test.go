package config

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
)

func TestSavedProgressIsLoadedBack(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	infohash := make([]byte, 20)

	c, err := Load(infohash, "/downloads/a")
	if err != nil {
		t.Fatalf("failed to load config: %v", err)
	}
	c.RecordProgress(0, 10)
	c.RecordProgress(10, 10)
	c.RecordProgress(40, 10)
	if err := c.SaveProgress(); err != nil {
		t.Fatalf("failed to save progress: %v", err)
	}

	reloaded, err := Load(infohash, "/downloads/a")
	if err != nil {
		t.Fatalf("failed to reload config: %v", err)
	}
	want := []ProgressRange{{Start: 0, End: 20}, {Start: 40, End: 50}}
	if got := reloaded.GetProgressRanges(); !slices.Equal(got, want) {
		t.Fatalf("unexpected ranges after reload: got %v, want %v", got, want)
	}

	other, err := Load(infohash, "/downloads/b")
	if err != nil {
		t.Fatalf("failed to load config for another destination: %v", err)
	}
	if got := other.GetProgressRanges(); len(got) != 0 {
		t.Fatalf("expected no progress for another destination, got %v", got)
	}

	// Each download gets a single directory directly under the config root.
	entries, err := os.ReadDir(filepath.Join(home, ".config/ziftorrent"))
	if err != nil {
		t.Fatalf("failed to read config root: %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("expected one config directory per download, got %d entries", len(entries))
	}
	for _, e := range entries {
		if _, err := os.Stat(filepath.Join(home, ".config/ziftorrent", e.Name(), progressFileName)); err != nil {
			t.Fatalf("expected a progress file directly in %s: %v", e.Name(), err)
		}
	}
}
