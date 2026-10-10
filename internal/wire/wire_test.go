package wire

import (
	"bytes"
	"encoding/binary"
	"reflect"
	"testing"
)

func TestBuildHandShake(t *testing.T) {
	infoHash := [20]byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}

	handshake := BuildHandShake(infoHash[:])
	if len(handshake) != 68 {
		t.Fatalf("expected handshake length 68, got %d", len(handshake))
	}

	if handshake[0] != 19 {
		t.Fatalf("expected pstrlen 19, got %d", handshake[0])
	}

	if string(handshake[1:20]) != "BitTorrent protocol" {
		t.Fatalf("expected pstr %q, got %q", "BitTorrent protocol", string(handshake[1:20]))
	}

	if !bytes.Equal(handshake[20:28], make([]byte, 8)) {
		t.Fatal("expected reserved bytes to be all zero")
	}

	if !bytes.Equal(handshake[28:48], infoHash[:]) {
		t.Fatalf("info hash mismatch: got %x", handshake[28:48])
	}

	peerID := handshake[48:68]
	if len(peerID) != 20 {
		t.Fatalf("expected peer id length 20, got %d", len(peerID))
	}
	if !bytes.HasPrefix(peerID, []byte("-qB0001-")) {
		t.Fatalf("unexpected peer id prefix: %q", string(peerID[:8]))
	}
}

func TestBuildKeepAlive(t *testing.T) {
	ka := BuildKeepAlive()
	if len(ka) != 4 {
		t.Fatalf("expected keep-alive length 4, got %d", len(ka))
	}
	if ka[0] != 0 {
		t.Fatalf("expected keep-alive byte 0, got %d", ka[0])
	}
}

func TestBuildChoke(t *testing.T) {
	tests := []struct {
		name string
		want []byte
	}{
		{name: "choke", want: []byte{0, 0, 0, 1, 0}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildChoke()
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestBuildUnchoke(t *testing.T) {
	tests := []struct {
		name string
		want []byte
	}{
		{name: "unchoke", want: []byte{0, 0, 0, 1, 1}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildUnchoke()
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestBuildInterested(t *testing.T) {
	tests := []struct {
		name string
		want []byte
	}{
		{name: "interested", want: []byte{0, 0, 0, 1, 2}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildInterested()
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestBuildUninterested(t *testing.T) {
	tests := []struct {
		name string
		want []byte
	}{
		{name: "no interested", want: []byte{0, 0, 0, 1, 3}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildUninterested()
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestBuildBitfield(t *testing.T) {
	tests := []struct {
		name    string
		payload []bool
		want    []byte
	}{
		{name: "empty bitfield", payload: []bool{}, want: []byte{0, 0, 0, 1, 5}},
		{name: "single byte bitfield", payload: []bool{true, false, false, false, false, false, false, false}, want: []byte{0, 0, 0, 2, 5, 0x80}},
		{name: "multi-byte bitfield", payload: []bool{true, false, true, false, true, false, true, false, false, true, false, true, false, true, false, true}, want: []byte{0, 0, 0, 3, 5, 0xAA, 0x55}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := BuildBitfield(tc.payload)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestBuildHave(t *testing.T) {
	tests := []struct {
		name     string
		pieceIdx uint32
		want     []byte
	}{
		{name: "have piece 0", pieceIdx: 0, want: []byte{0, 0, 0, 5, 4, 0, 0, 0, 0}},
		{name: "have piece 42", pieceIdx: 42, want: []byte{0, 0, 0, 5, 4, 0, 0, 0, 42}},
		{name: "have piece max", pieceIdx: 0xFFFFFFFF, want: []byte{0, 0, 0, 5, 4, 255, 255, 255, 255}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildHave(tc.pieceIdx)
			if !bytes.Equal(got, tc.want) {
				t.Fatalf("%s: got %v, want %v", tc.name, got, tc.want)
			}
		})
	}
}

func TestBuildRequest(t *testing.T) {
	tests := []struct {
		name        string
		pieceIdx    uint32
		begin       uint32
		pieceLength uint32
	}{
		{name: "request chunk 0", pieceIdx: 0, begin: 0, pieceLength: 16384},
		{name: "request chunk 42", pieceIdx: 42, begin: 1024, pieceLength: 32768},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildRequest(tc.pieceIdx, tc.begin, tc.pieceLength)
			if len(got) != 17 {
				t.Fatalf("%s: expected length 17, got %d", tc.name, len(got))
			}
			if got[4] != 6 {
				t.Fatalf("%s: expected message id 6, got %d", tc.name, got[4])
			}
			if binary.BigEndian.Uint32(got[5:9]) != tc.pieceIdx {
				t.Fatalf("%s: expected piece index %d, got %d", tc.name, tc.pieceIdx, binary.BigEndian.Uint32(got[5:9]))
			}
			if binary.BigEndian.Uint32(got[9:13]) != tc.begin {
				t.Fatalf("%s: expected begin %d, got %d", tc.name, tc.begin, binary.BigEndian.Uint32(got[9:13]))
			}
			if binary.BigEndian.Uint32(got[13:17]) != tc.pieceLength {
				t.Fatalf("%s: expected piece length %d, got %d", tc.name, tc.pieceLength, binary.BigEndian.Uint32(got[13:17]))
			}
		})
	}
}

func TestBuildCancel(t *testing.T) {
	tests := []struct {
		name        string
		pieceIdx    uint32
		begin       uint32
		pieceLength uint32
	}{
		{name: "cancel chunk 0", pieceIdx: 0, begin: 0, pieceLength: 16384},
		{name: "cancel chunk 42", pieceIdx: 42, begin: 1024, pieceLength: 32768},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildCancel(tc.pieceIdx, tc.begin, tc.pieceLength)
			if len(got) != 17 {
				t.Fatalf("%s: expected length 17, got %d", tc.name, len(got))
			}
			if got[4] != 8 {
				t.Fatalf("%s: expected message id 8, got %d", tc.name, got[4])
			}
			if binary.BigEndian.Uint32(got[5:9]) != tc.pieceIdx {
				t.Fatalf("%s: expected piece index %d, got %d", tc.name, tc.pieceIdx, binary.BigEndian.Uint32(got[5:9]))
			}
			if binary.BigEndian.Uint32(got[9:13]) != tc.begin {
				t.Fatalf("%s: expected begin %d, got %d", tc.name, tc.begin, binary.BigEndian.Uint32(got[9:13]))
			}
			if binary.BigEndian.Uint32(got[13:17]) != tc.pieceLength {
				t.Fatalf("%s: expected piece length %d, got %d", tc.name, tc.pieceLength, binary.BigEndian.Uint32(got[13:17]))
			}
		})
	}
}

func TestBuildPort(t *testing.T) {
	tests := []struct {
		name string
		port uint16
	}{
		{name: "port 6881", port: 6881},
		{name: "port 9999", port: 9999},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildPort(tc.port)
			if len(got) != 7 {
				t.Fatalf("%s: expected length 7, got %d", tc.name, len(got))
			}
			if got[4] != 9 {
				t.Fatalf("%s: expected message id 9, got %d", tc.name, got[4])
			}
			if binary.BigEndian.Uint16(got[5:7]) != tc.port {
				t.Fatalf("%s: expected port %d, got %d", tc.name, tc.port, binary.BigEndian.Uint16(got[5:7]))
			}
		})
	}
}

func TestBuildPiece(t *testing.T) {
	tests := []struct {
		name     string
		pieceIdx uint32
		begin    uint32
		block    []byte
	}{
		{name: "empty block", pieceIdx: 0, begin: 0, block: []byte{}},
		{name: "small block", pieceIdx: 1, begin: 4, block: []byte{0x01, 0x02, 0x03}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := BuildPiece(tc.pieceIdx, tc.begin, tc.block)
			if len(got) != 13+len(tc.block) {
				t.Fatalf("%s: expected length %d, got %d", tc.name, 13+len(tc.block), len(got))
			}
			if got[4] != 7 {
				t.Fatalf("%s: expected message id 7, got %d", tc.name, got[4])
			}
			if binary.BigEndian.Uint32(got[5:9]) != tc.pieceIdx {
				t.Fatalf("%s: expected piece index %d, got %d", tc.name, tc.pieceIdx, binary.BigEndian.Uint32(got[5:9]))
			}
			if binary.BigEndian.Uint32(got[9:13]) != tc.begin {
				t.Fatalf("%s: expected begin %d, got %d", tc.name, tc.begin, binary.BigEndian.Uint32(got[9:13]))
			}
			if !bytes.Equal(got[13:], tc.block) {
				t.Fatalf("%s: expected block %v, got %v", tc.name, tc.block, got[13:])
			}
		})
	}
}

func TestParseMessage(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    Message
		wantErr string
	}{
		{name: "keep alive", input: []byte{0, 0, 0, 0}, want: Message{Size: 0}},
		{name: "choke", input: []byte{0, 0, 0, 1, 0}, want: Message{Size: 1, ID: 0}},
		{name: "have", input: []byte{0, 0, 0, 5, 4, 0, 0, 0, 42}, want: Message{Size: 5, ID: 4, Payload: []byte{0, 0, 0, 42}}},
		{name: "truncated payload", input: []byte{0, 0, 0, 2, 0}, wantErr: "message unexpectedly short"},
		{name: "too short", input: []byte{0, 0, 0}, wantErr: "message unexpectedly short"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseMessage(tc.input)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("expected error %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Size != tc.want.Size || got.ID != tc.want.ID || !bytes.Equal(got.Payload, tc.want.Payload) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParsePayloads(t *testing.T) {
	tests := []struct {
		name    string
		fn      func([]byte) (any, error)
		input   []byte
		want    any
		wantErr string
	}{
		{
			name: "request cancel payload",
			fn: func(b []byte) (any, error) {
				return ParseRequestCancelPayload(b)
			},
			input: []byte{0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 3},
			want:  RequestCancelPayload{Index: 1, Begin: 2, Length: 3},
		},
		{
			name: "request cancel invalid length",
			fn: func(b []byte) (any, error) {
				return ParseRequestCancelPayload(b)
			},
			input:   []byte{0, 0, 0, 1},
			wantErr: errPayloadSize.Error(),
		},
		{
			name: "block payload",
			fn: func(b []byte) (any, error) {
				return ParseBlockPayload(b)
			},
			input: []byte{0, 0, 0, 1, 0, 0, 0, 2, 9, 8, 7},
			want:  BlockPayload{Index: 1, Begin: 2, Block: []byte{9, 8, 7}},
		},
		{
			name: "block payload invalid length",
			fn: func(b []byte) (any, error) {
				return ParseBlockPayload(b)
			},
			input:   []byte{0, 0, 0, 1, 0, 0, 0},
			wantErr: errPayloadSize.Error(),
		},
		{
			name: "port payload",
			fn: func(b []byte) (any, error) {
				return ParsePortPayload(b)
			},
			input: []byte{0x01, 0x02},
			want:  PortPayload{Port: 258},
		},
		{
			name: "port payload invalid length",
			fn: func(b []byte) (any, error) {
				return ParsePortPayload(b)
			},
			input:   []byte{0x01},
			wantErr: errPayloadSize.Error(),
		},
		{
			name: "have payload",
			fn: func(b []byte) (any, error) {
				return ParseHavePayload(b)
			},
			input: []byte{0, 0, 0, 42},
			want:  HavePayload{Index: 42},
		},
		{
			name: "have payload invalid length",
			fn: func(b []byte) (any, error) {
				return ParseHavePayload(b)
			},
			input:   []byte{0, 0},
			wantErr: errPayloadSize.Error(),
		},
		{
			name: "bitfield payload",
			fn: func(b []byte) (any, error) {
				return ParseBitfieldPayload(b), nil
			},
			input: []byte{0x80},
			want:  BitfieldPayload{Bitfield: []bool{true, false, false, false, false, false, false, false}},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.fn(tc.input)
			if tc.wantErr != "" {
				if err == nil || err.Error() != tc.wantErr {
					t.Fatalf("expected error %q, got %v", tc.wantErr, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("got %#v, want %#v", got, tc.want)
			}
		})
	}
}
