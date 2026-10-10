package wire

import (
	"encoding/binary"
	"errors"

	"github.com/nazifbara/ziftorrent/internal/utils"
)

type Message struct {
	Size    uint32
	ID      uint8
	Payload []byte
}

type HavePayload struct {
	Index uint32
}

type BitfieldPayload struct {
	Bitfield []bool
}

type PortPayload struct {
	Port uint16
}

type RequestCancelPayload struct {
	Index  uint32
	Begin  uint32
	Length uint32
}

type BlockPayload struct {
	Index uint32
	Begin uint32
	Block []byte
}

var errPayloadSize = errors.New("incorrect payload size")

func ParseMessage(b []byte) (Message, error) {
	if len(b) < 4 {
		return Message{}, errors.New("message unexpectedly short")
	}

	message := Message{Size: binary.BigEndian.Uint32(b[:4])}
	if message.Size == 0 {
		return message, nil
	}

	payloadLength := int(message.Size) - 1
	minSize := 4 + 1 + payloadLength
	if len(b) < minSize {
		return Message{Size: message.Size}, errors.New("message unexpectedly short")
	}

	message.ID = uint8(b[4])
	message.Payload = b[5 : 5+payloadLength]
	return message, nil
}

func ParseRequestCancelPayload(b []byte) (RequestCancelPayload, error) {
	if len(b) != 12 {
		return RequestCancelPayload{}, errPayloadSize
	}
	return RequestCancelPayload{
		Index:  binary.BigEndian.Uint32(b[:4]),
		Begin:  binary.BigEndian.Uint32(b[4:8]),
		Length: binary.BigEndian.Uint32(b[8:]),
	}, nil
}

func ParseBlockPayload(b []byte) (BlockPayload, error) {
	if len(b) < 8 {
		return BlockPayload{}, errPayloadSize
	}
	return BlockPayload{
		Index: binary.BigEndian.Uint32(b[:4]),
		Begin: binary.BigEndian.Uint32(b[4:8]),
		Block: b[8:],
	}, nil
}

func ParsePortPayload(b []byte) (PortPayload, error) {
	if len(b) != 2 {
		return PortPayload{}, errPayloadSize
	}
	return PortPayload{Port: binary.BigEndian.Uint16(b)}, nil
}

func ParseBitfieldPayload(b []byte) BitfieldPayload {
	return BitfieldPayload{Bitfield: utils.BytesToBits(b)}
}

func ParseHavePayload(b []byte) (HavePayload, error) {
	if len(b) != 4 {
		return HavePayload{}, errPayloadSize
	}
	return HavePayload{Index: binary.BigEndian.Uint32(b)}, nil
}

func BuildHandShake(infohash []byte) []byte {
	b := make([]byte, 0, 68)
	// pstrlen
	b = append(b, byte(19))
	// pstr
	b = append(b, []byte("BitTorrent protocol")...)
	// reserved
	b = binary.BigEndian.AppendUint64(b, 0)
	// info hash
	b = append(b, infohash...)
	// peer id
	b = append(b, utils.RandomPeerID()...)
	return b
}

func BuildKeepAlive() []byte {
	return make([]byte, 4)
}

func BuildChoke() []byte {
	b := make([]byte, 0, 5)
	// length
	b = binary.BigEndian.AppendUint32(b, 1)
	// message id
	b = append(b, 0)
	return b
}

func BuildUnchoke() []byte {
	b := make([]byte, 0, 5)
	// length
	b = binary.BigEndian.AppendUint32(b, 1)
	// message id
	b = append(b, 1)
	return b
}

func BuildInterested() []byte {
	b := make([]byte, 0, 5)
	// length
	b = binary.BigEndian.AppendUint32(b, 1)
	// message id
	b = append(b, 2)
	return b
}

func BuildUninterested() []byte {
	b := make([]byte, 0, 5)
	// length
	b = binary.BigEndian.AppendUint32(b, 1)
	// message id
	b = append(b, 3)
	return b
}

func BuildHave(pieceIdx uint32) []byte {
	b := make([]byte, 0, 9)
	// length
	b = binary.BigEndian.AppendUint32(b, 5)
	// message id
	b = append(b, 4)
	// piece index
	b = binary.BigEndian.AppendUint32(b, pieceIdx)
	return b
}

func BuildBitfield(payload []bool) ([]byte, error) {
	bitBytes, err := utils.BitsToBytes(payload)
	if err != nil {
		return nil, err
	}

	b := make([]byte, 0, 5+len(bitBytes))
	b = binary.BigEndian.AppendUint32(b, uint32(1+len(bitBytes)))
	b = append(b, 5)
	b = append(b, bitBytes...)
	return b, nil
}

func BuildRequest(pieceIdx, begin, pieceLength uint32) []byte {
	b := make([]byte, 0, 17)
	// length
	b = binary.BigEndian.AppendUint32(b, 13)
	// message id
	b = append(b, 6)
	// piece index
	b = binary.BigEndian.AppendUint32(b, pieceIdx)
	// begin
	b = binary.BigEndian.AppendUint32(b, begin)
	// piece length
	b = binary.BigEndian.AppendUint32(b, pieceLength)
	return b
}

func BuildPiece(pieceIdx, begin uint32, block []byte) []byte {
	blockSize := len(block)
	b := make([]byte, 0, 13+blockSize)
	// length
	b = binary.BigEndian.AppendUint32(b, uint32(9+blockSize))
	// message id
	b = append(b, 7)
	// piece index
	b = binary.BigEndian.AppendUint32(b, pieceIdx)
	// begin
	b = binary.BigEndian.AppendUint32(b, begin)
	// block
	b = append(b, block...)
	return b
}

func BuildCancel(pieceIdx, begin, pieceLength uint32) []byte {
	b := make([]byte, 0, 17)
	// length
	b = binary.BigEndian.AppendUint32(b, 13)
	// message id
	b = append(b, 8)
	// piece index
	b = binary.BigEndian.AppendUint32(b, pieceIdx)
	// begin
	b = binary.BigEndian.AppendUint32(b, begin)
	// piece length
	b = binary.BigEndian.AppendUint32(b, pieceLength)
	return b
}

func BuildPort(port uint16) []byte {
	b := make([]byte, 0, 7)
	// length
	b = binary.BigEndian.AppendUint32(b, 3)
	// message id
	b = append(b, 9)
	// port
	b = binary.BigEndian.AppendUint16(b, port)
	return b
}
