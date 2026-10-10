package peer

import (
	"errors"
	"net"
	"testing"

	"github.com/nazifbara/ziftorrent/internal/wire"
)

func TestHandshakeAcceptsMatchingInfoHash(t *testing.T) {
	hash := [20]byte{1, 2, 3}
	client, remote := net.Pipe()
	defer client.Close()
	defer remote.Close()

	go func() {
		buf := make([]byte, handshakeLen)
		remote.Read(buf)
		remote.Write(wire.BuildHandShake(hash[:]))
	}()

	if err := Handshake(client, hash); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCheckHandshakeRejectsWrongInfoHash(t *testing.T) {
	resp := wire.BuildHandShake(make([]byte, 20))
	if err := checkHandshake(resp, [20]byte{9}); !errors.Is(err, errInfoHash) {
		t.Fatalf("expected errInfoHash, got %v", err)
	}
}

func TestCheckHandshakeRejectsOtherProtocol(t *testing.T) {
	resp := wire.BuildHandShake(make([]byte, 20))
	resp[1] = 'X'
	if err := checkHandshake(resp, [20]byte{}); !errors.Is(err, errBadProtocol) {
		t.Fatalf("expected errBadProtocol, got %v", err)
	}
}
