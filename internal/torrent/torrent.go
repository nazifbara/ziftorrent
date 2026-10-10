package torrent

import (
	"bytes"
	"crypto/sha1"
	"errors"
	"fmt"
	"os"

	"github.com/jackpal/bencode-go"
	"github.com/nazifbara/ziftorrent/internal/config"
)

// bencodeTorrent mirrors the on-disk .torrent layout. It stays private so the
// bencode tags and raw encoding never leak into the rest of the program;
// callers only ever see the parsed Torrent.
type bencodeTorrent struct {
	Announce     string     `bencode:"announce"`
	AnnounceList [][]string `bencode:"announce-list,omitempty"`
	Comment      string     `bencode:"comment,omitempty"`
	CreatedBy    string     `bencode:"created by,omitempty"`
	CreationDate uint64     `bencode:"creation date,omitempty"`
	Info         infoDict   `bencode:"info"`
}

func (bt *bencodeTorrent) toTorrent() (Torrent, error) {
	var buf bytes.Buffer
	if err := bencode.Marshal(&buf, bt.Info); err != nil {
		return Torrent{}, err
	}
	infoHash := sha1.Sum(buf.Bytes())
	pieceHashes, err := getPieceHashes(bt.Info)
	if err != nil {
		return Torrent{}, err
	}
	contentSize := 0
	if len(bt.Info.Files) == 0 {
		contentSize = int(bt.Info.Length)
		bt.Info.Files = []fileDict{{Length: int64(contentSize), Path: []string{bt.Info.Name}}}
	} else {
		for _, file := range bt.Info.Files {
			contentSize += int(file.Length)
		}
	}
	finalPieceSize := int(contentSize) % int(bt.Info.PieceLength)
	if contentSize > 0 && finalPieceSize == 0 {
		finalPieceSize = int(bt.Info.PieceLength)
	}
	numOfBlocks := contentSize / int(config.BlockSize)
	if contentSize%int(config.BlockSize) != 0 {
		numOfBlocks++
	}
	files := make([]File, len(bt.Info.Files))
	for i, f := range bt.Info.Files {
		files[i] = File{Length: f.Length, Path: append([]string(nil), f.Path...)}
	}
	return Torrent{
		Name:           bt.Info.Name,
		Announce:       bt.Announce,
		InfoHash:       infoHash,
		NumOfPieces:    uint64(len(bt.Info.Pieces) / 20),
		ContentSize:    uint64(contentSize),
		AnnounceList:   bt.AnnounceList,
		PieceHashes:    pieceHashes,
		PieceSize:      bt.Info.PieceLength,
		Files:          files,
		NumOfBlocks:    uint64(numOfBlocks),
		FinalPieceSize: uint64(finalPieceSize),
	}, nil
}

// infoDict represents the "info" dictionary within the torrent metainfo.
type infoDict struct {
	Name        string     `bencode:"name"`
	PieceLength uint64     `bencode:"piece length"`
	Pieces      string     `bencode:"pieces"`
	Files       []fileDict `bencode:"files,omitempty"`
	Length      uint64     `bencode:"length,omitempty"` // For single-file torrents
}

// fileDict represents an individual file entry in multi-file torrents.
type fileDict struct {
	Length int64    `bencode:"length"`
	Path   []string `bencode:"path"`
}

// File is one file inside a torrent, in the order its bytes appear in the
// torrent's contiguous content.
type File struct {
	Length int64
	Path   []string
}

// Torrent is the parsed, read-only metadata of a .torrent file.
type Torrent struct {
	Announce       string
	InfoHash       [20]byte
	PieceHashes    [][20]byte
	PieceSize      uint64
	FinalPieceSize uint64
	NumOfPieces    uint64
	NumOfBlocks    uint64
	ContentSize    uint64
	Files          []File
	Name           string
	AnnounceList   [][]string
}

// BlockLength returns the length of the block that starts at begin inside
// piece pieceIdx: a full config.BlockSize, or whatever is left of the piece.
func (t *Torrent) BlockLength(pieceIdx, begin uint32) uint32 {
	pieceSize := uint32(t.PieceSize)
	if pieceIdx == uint32(t.NumOfPieces-1) {
		pieceSize = uint32(t.FinalPieceSize)
	}
	return min(pieceSize-begin, config.BlockSize)
}

// Open reads and parses the .torrent file at filePath.
func Open(filePath string) (*Torrent, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var bt bencodeTorrent
	err = bencode.Unmarshal(f, &bt)
	if err != nil {
		return nil, errors.New("malformed torrent bencode")
	}
	torrent, err := bt.toTorrent()
	if err != nil {
		return nil, err
	}
	return &torrent, nil
}

func getPieceHashes(info infoDict) ([][20]byte, error) {
	// A SHA-1 hash is exactly 20 bytes long
	const hashSize = 20

	// Quick validation: the total length must be a multiple of 20
	if len(info.Pieces)%hashSize != 0 {
		return nil, fmt.Errorf("malformed pieces string: length %d is not a multiple of %d", len(info.Pieces), hashSize)
	}

	numPieces := len(info.Pieces) / hashSize
	hashes := make([][20]byte, 0, numPieces)

	// Loop through the string 20 bytes at a time
	for i := 0; i < len(info.Pieces); i += hashSize {
		var hash [20]byte

		// Copy 20 bytes from the string into the fixed-size array
		copy(hash[:], info.Pieces[i:i+hashSize])

		hashes = append(hashes, hash)
	}

	return hashes, nil
}
