package peer

import (
	"bytes"
	"errors"
	"io"

	"github.com/nazifbara/ziftorrent/internal/wire"
)

const handshakeLen = 68

var (
	errBadProtocol = errors.New("peer: handshake is not BitTorrent protocol")
	errInfoHash    = errors.New("peer: handshake info hash mismatch")
)

// Handshake performs the BitTorrent handshake over rw and checks that the
// remote side is serving the torrent identified by infoHash.
// It takes an io.ReadWriter rather than *Peer so it stays independent of
// connection details and is easy to test with an in-memory pipe.
func Handshake(rw io.ReadWriter, infoHash [20]byte) error {
	if rw == nil {
		return errors.New("peer: nil connection")
	}
	if _, err := rw.Write(wire.BuildHandShake(infoHash[:])); err != nil {
		return err
	}
	resp := make([]byte, handshakeLen)
	if _, err := io.ReadFull(rw, resp); err != nil {
		return err
	}
	return checkHandshake(resp, infoHash)
}

// checkHandshake validates a raw handshake reply:
// <pstrlen=19><"BitTorrent protocol"><8 reserved><20 info hash><20 peer id>.
func checkHandshake(resp []byte, infoHash [20]byte) error {
	if len(resp) != handshakeLen || resp[0] != 19 || string(resp[1:20]) != "BitTorrent protocol" {
		return errBadProtocol
	}
	if !bytes.Equal(resp[28:48], infoHash[:]) {
		return errInfoHash
	}
	return nil
}
