package torrent

import (
	"bytes"
	"crypto/sha1"
	"os"
	"path/filepath"
	"testing"

	bencode "github.com/jackpal/bencode-go"
)

func repeatHash(b byte) [20]byte {
	var h [20]byte
	for i := range h {
		h[i] = b
	}
	return h
}

func TestToTorrentSingleFile(t *testing.T) {
	h1 := repeatHash(0x01)
	h2 := repeatHash(0x02)
	info := infoDict{
		Name:        "test.txt",
		PieceLength: 10,
		Pieces:      string(h1[:]) + string(h2[:]),
		Length:      15,
	}
	bt := bencodeTorrent{
		Announce: "udp://tracker.example.com:80",
		Info:     info,
	}

	torrent, err := bt.toTorrent()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if torrent.Name != "test.txt" {
		t.Fatalf("expected name %q, got %q", "test.txt", torrent.Name)
	}
	if torrent.ContentSize != 15 {
		t.Fatalf("expected content size 15, got %d", torrent.ContentSize)
	}
	if torrent.NumOfPieces != 2 {
		t.Fatalf("expected 2 pieces, got %d", torrent.NumOfPieces)
	}
	if torrent.PieceSize != 10 {
		t.Fatalf("expected piece size 10, got %d", torrent.PieceSize)
	}
	if torrent.FinalPieceSize != 5 {
		t.Fatalf("expected final piece size 5 (15%%10), got %d", torrent.FinalPieceSize)
	}
	if len(torrent.Files) != 1 {
		t.Fatalf("expected synthesized single file entry, got %d files", len(torrent.Files))
	}
	if torrent.Files[0].Length != 15 || torrent.Files[0].Path[0] != "test.txt" {
		t.Fatalf("unexpected synthesized file entry: %+v", torrent.Files[0])
	}
	// content size 15 is below one full block (16384): rounds up to 1 block.
	if torrent.NumOfBlocks != 1 {
		t.Fatalf("expected 1 block, got %d", torrent.NumOfBlocks)
	}
	if len(torrent.PieceHashes) != 2 {
		t.Fatalf("expected 2 piece hashes, got %d", len(torrent.PieceHashes))
	}
	if torrent.PieceHashes[0] != h1 || torrent.PieceHashes[1] != h2 {
		t.Fatal("piece hashes mismatch")
	}

	// info hash must match an independent SHA-1 of the bencoded info dict
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, info); err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}
	wantInfoHash := sha1.Sum(buf.Bytes())
	if torrent.InfoHash != wantInfoHash {
		t.Fatalf("info hash mismatch: got %x, want %x", torrent.InfoHash, wantInfoHash)
	}
}

func TestToTorrentMultiFile(t *testing.T) {
	h1 := repeatHash(0x03)
	h2 := repeatHash(0x04)
	info := infoDict{
		Name:        "multi",
		PieceLength: 10,
		Pieces:      string(h1[:]) + string(h2[:]),
		Files: []fileDict{
			{Length: 5, Path: []string{"a.txt"}},
			{Length: 10, Path: []string{"sub", "b.txt"}},
		},
	}
	bt := bencodeTorrent{Info: info}

	torrent, err := bt.toTorrent()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if torrent.ContentSize != 15 {
		t.Fatalf("expected content size 15, got %d", torrent.ContentSize)
	}
	if len(torrent.Files) != 2 {
		t.Fatalf("expected original 2 files preserved, got %d", len(torrent.Files))
	}
	if torrent.Files[1].Path[0] != "sub" || torrent.Files[1].Path[1] != "b.txt" {
		t.Fatalf("unexpected file path: %+v", torrent.Files[1].Path)
	}
}

func TestGetPieceHashesMalformed(t *testing.T) {
	info := infoDict{Pieces: "not-a-multiple-of-20-bytes"}
	_, err := getPieceHashes(info)
	if err == nil {
		t.Fatal("expected error for malformed pieces string, got nil")
	}
}

func TestGetPieceHashesValid(t *testing.T) {
	h1 := repeatHash(0xAA)
	info := infoDict{Pieces: string(h1[:])}
	hashes, err := getPieceHashes(info)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(hashes) != 1 || hashes[0] != h1 {
		t.Fatalf("unexpected hashes: %+v", hashes)
	}
}

func TestOpenTorrentMissingFile(t *testing.T) {
	_, err := Open(filepath.Join(t.TempDir(), "does-not-exist.torrent"))
	if err == nil {
		t.Fatal("expected error opening a missing file, got nil")
	}
}

func TestOpenTorrentMalformedBencode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.torrent")
	if err := os.WriteFile(path, []byte("not bencode data"), 0o644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}
	_, err := Open(path)
	if err == nil || err.Error() != "malformed torrent bencode" {
		t.Fatalf("expected malformed bencode error, got %v", err)
	}
}

func TestOpenTorrentValid(t *testing.T) {
	h1 := repeatHash(0x05)
	bt := bencodeTorrent{
		Announce: "udp://tracker.example.com:80",
		Info: infoDict{
			Name:        "file.bin",
			PieceLength: 4,
			Pieces:      string(h1[:]),
			Length:      4,
		},
	}
	path := filepath.Join(t.TempDir(), "valid.torrent")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	if err := bencode.Marshal(f, bt); err != nil {
		f.Close()
		t.Fatalf("failed to marshal test torrent: %v", err)
	}
	f.Close()

	torrent, err := Open(path)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if torrent.Name != "file.bin" {
		t.Fatalf("expected name %q, got %q", "file.bin", torrent.Name)
	}
	if torrent.ContentSize != 4 {
		t.Fatalf("expected content size 4, got %d", torrent.ContentSize)
	}
}

func TestOpenTorrentReturnsToTorrentError(t *testing.T) {
	bt := bencodeTorrent{
		Info: infoDict{
			Name:   "broken",
			Pieces: "too-short", // not a multiple of 20 -> getPieceHashes fails
			Length: 10,
		},
	}
	path := filepath.Join(t.TempDir(), "broken.torrent")
	f, err := os.Create(path)
	if err != nil {
		t.Fatalf("failed to create test file: %v", err)
	}
	if err := bencode.Marshal(f, bt); err != nil {
		f.Close()
		t.Fatalf("failed to marshal test torrent: %v", err)
	}
	f.Close()

	_, err = Open(path)
	if err == nil {
		t.Fatal("expected malformed pieces error")
	}
}
