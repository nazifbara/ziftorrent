package tracker

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/nazifbara/ziftorrent/internal/torrent"
)

func TestBuildConnReq(t *testing.T) {
	got := buildConnReq()
	if len(got) != 16 {
		t.Fatalf("expected length 16, got %d", len(got))
	}
	wantConnID := uint64(0x041727101980)
	if binary.BigEndian.Uint64(got[0:8]) != wantConnID {
		t.Fatalf("expected connection id %x, got %x", wantConnID, binary.BigEndian.Uint64(got[0:8]))
	}
	if binary.BigEndian.Uint32(got[8:12]) != 0 {
		t.Fatalf("expected connect action 0, got %d", binary.BigEndian.Uint32(got[8:12]))
	}
}

func TestParseConnResp(t *testing.T) {
	resp := make([]byte, 16)
	binary.BigEndian.PutUint32(resp[0:4], 0)
	binary.BigEndian.PutUint32(resp[4:8], 12345)
	copy(resp[8:16], []byte{1, 2, 3, 4, 5, 6, 7, 8})

	got := parseConnResp(resp)
	if binary.BigEndian.Uint32(got.action) != 0 {
		t.Fatalf("expected action 0, got %v", got.action)
	}
	if binary.BigEndian.Uint32(got.transactionID) != 12345 {
		t.Fatalf("expected transaction id 12345, got %v", got.transactionID)
	}
	wantConnID := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	for i := range wantConnID {
		if got.connectionID[i] != wantConnID[i] {
			t.Fatalf("connection id mismatch: got %v, want %v", got.connectionID, wantConnID)
		}
	}
}

func TestParseAnnounceResp(t *testing.T) {
	resp := make([]byte, 20)
	binary.BigEndian.PutUint32(resp[0:4], 1)     // action
	binary.BigEndian.PutUint32(resp[4:8], 999)   // transaction id
	binary.BigEndian.PutUint32(resp[8:12], 1800) // interval
	binary.BigEndian.PutUint32(resp[12:16], 3)   // leechers
	binary.BigEndian.PutUint32(resp[16:20], 7)   // seeders

	peer1 := []byte{192, 168, 1, 1, 0x1A, 0xE1} // 192.168.1.1:6881
	peer2 := []byte{10, 0, 0, 5, 0x27, 0x0F}    // 10.0.0.5:9999
	resp = append(resp, peer1...)
	resp = append(resp, peer2...)

	got, err := parseAnnounceResp(resp)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.leechers != 3 {
		t.Fatalf("expected 3 leechers, got %d", got.leechers)
	}
	if got.seeders != 7 {
		t.Fatalf("expected 7 seeders, got %d", got.seeders)
	}
	if len(got.peerAddresses) != 2 {
		t.Fatalf("expected 2 peer addresses, got %d", len(got.peerAddresses))
	}
	if got.peerAddresses[0].IP.String() != "192.168.1.1" || got.peerAddresses[0].Port != 6881 {
		t.Fatalf("unexpected first peer: %+v", got.peerAddresses[0])
	}
	if got.peerAddresses[1].IP.String() != "10.0.0.5" || got.peerAddresses[1].Port != 9999 {
		t.Fatalf("unexpected second peer: %+v", got.peerAddresses[1])
	}
}

func TestBuildAnnounceReq(t *testing.T) {
	conn, err := net.ListenUDP("udp", &net.UDPAddr{})
	if err != nil {
		t.Fatalf("failed to open local UDP socket: %v", err)
	}
	defer conn.Close()

	var infoHash [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i + 1)
	}
	tor := torrent.Torrent{InfoHash: infoHash, ContentSize: 42}

	connID := []byte{9, 9, 9, 9, 9, 9, 9, 9}
	got := buildAnnounceReq(connID, conn, tor)

	if len(got) != 98 {
		t.Fatalf("expected length 98, got %d", len(got))
	}
	for i, b := range connID {
		if got[i] != b {
			t.Fatalf("connection id mismatch at byte %d: got %d, want %d", i, got[i], b)
		}
	}
	if binary.BigEndian.Uint32(got[8:12]) != 1 {
		t.Fatalf("expected announce action 1, got %d", binary.BigEndian.Uint32(got[8:12]))
	}
	for i, b := range infoHash {
		if got[16+i] != b {
			t.Fatalf("info hash mismatch at byte %d", i)
		}
	}
	if binary.BigEndian.Uint64(got[56:64]) != 0 {
		t.Fatal("expected downloaded 0")
	}
	if binary.BigEndian.Uint64(got[64:72]) != 42 {
		t.Fatalf("expected left 42, got %d", binary.BigEndian.Uint64(got[64:72]))
	}
	if binary.BigEndian.Uint64(got[72:80]) != 0 {
		t.Fatal("expected uploaded 0")
	}
	if binary.BigEndian.Uint32(got[92:96]) != 0xFFFFFFFF {
		t.Fatalf("expected num_want 0xFFFFFFFF, got %x", binary.BigEndian.Uint32(got[92:96]))
	}
	wantPort := uint16(conn.LocalAddr().(*net.UDPAddr).Port)
	if binary.BigEndian.Uint16(got[96:98]) != wantPort {
		t.Fatalf("expected port %d, got %d", wantPort, binary.BigEndian.Uint16(got[96:98]))
	}
}
