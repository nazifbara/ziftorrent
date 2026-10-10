package tracker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/url"
	"sync"

	"github.com/nazifbara/ziftorrent/internal/peer"
	"github.com/nazifbara/ziftorrent/internal/torrent"
	"github.com/nazifbara/ziftorrent/internal/utils"
)

type Tracker struct {
	mu            sync.Mutex
	activePeers   []*peer.Peer
	peerAddresses []*net.TCPAddr
}

func (t *Tracker) PeerAddresses() []*net.TCPAddr {
	t.mu.Lock()
	defer t.mu.Unlock()
	addresses := make([]*net.TCPAddr, 0, len(t.peerAddresses))
	for _, address := range t.peerAddresses {
		addresses = append(addresses, &net.TCPAddr{IP: append(net.IP(nil), address.IP...), Port: address.Port, Zone: address.Zone})
	}
	return addresses
}

func (t *Tracker) AddPeer(p *peer.Peer) {
	t.mu.Lock()
	t.activePeers = append(t.activePeers, p)
	t.mu.Unlock()
}

func (t *Tracker) FindPeer(address *net.TCPAddr) *peer.Peer {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, p := range t.activePeers {
		if p.MatchesAddress(address) {
			return p
		}
	}
	return nil
}

func (t *Tracker) Peers() []*peer.Peer {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]*peer.Peer(nil), t.activePeers...)
}

type connResp struct {
	action        []byte
	transactionID []byte
	connectionID  []byte
}

type announceResp struct {
	action        []byte
	transactionID []byte
	interval      []byte
	leechers      uint32
	seeders       uint32
	peerAddresses []*net.TCPAddr
}

func Find(announceList [][]string, torrent *torrent.Torrent) (*Tracker, error) {
	for _, announceItem := range announceList {
		if len(announceItem) == 0 {
			continue
		}
		fmt.Printf("Announcing to %s\n", announceItem[0])
		parsedURL, err := url.Parse(announceItem[0])
		if err != nil {
			continue
		}
		if parsedURL.Scheme != "udp" {
			continue
		}
		addr, err := net.ResolveUDPAddr("udp", parsedURL.Host)
		if err != nil {
			continue
		}
		fmt.Println("resolved")
		trackerConn, err := connectToTracker(addr)
		if err != nil {
			continue
		}
		fmt.Println("connected to tracker")
		conn, err := utils.Retry(10, trackerConn, func() (connResp, error) {
			return requestConn(trackerConn)
		})
		if err != nil {
			continue
		}
		t, err := utils.Retry(10, trackerConn, func() (*Tracker, error) {
			tracker, err := announce(conn, trackerConn, *torrent)
			if err != nil {
				return &Tracker{}, err
			}
			fmt.Println("announced!")
			return tracker, nil
		})
		if err != nil {
			continue
		}
		return t, nil
	}
	return nil, errors.New("tracker not found")
}

func announce(conn connResp, trackerConn *net.UDPConn, torrent torrent.Torrent) (*Tracker, error) {
	if _, err := trackerConn.Write(buildAnnounceReq(conn.connectionID, trackerConn, torrent)); err != nil {
		return &Tracker{}, err
	}
	respBuffer := make([]byte, 500)
	n, err := trackerConn.Read(respBuffer)
	if err != nil {
		fmt.Println(err)
		return &Tracker{}, err
	}
	announcResp, err := parseAnnounceResp(respBuffer[:n])
	if err != nil {
		return &Tracker{}, err
	}
	return &Tracker{peerAddresses: announcResp.peerAddresses}, nil
}

func requestConn(trackerConn *net.UDPConn) (connResp, error) {
	respBuffer := make([]byte, 16)
	if _, err := trackerConn.Write(buildConnReq()); err != nil {
		return connResp{}, err
	}
	n, err := io.ReadFull(trackerConn, respBuffer)
	if err != nil {
		return connResp{}, err
	}
	if n < 16 {
		return connResp{}, fmt.Errorf("tracker response is too short: %d", n)
	}
	return parseConnResp(respBuffer[:n]), nil
}

func buildConnReq() []byte {
	b := make([]byte, 0, 16)
	b = binary.BigEndian.AppendUint64(b, 0x041727101980)
	b = binary.BigEndian.AppendUint32(b, 0)
	b = binary.BigEndian.AppendUint32(b, rand.Uint32())
	return b
}

func buildAnnounceReq(connectID []byte, trackerConn *net.UDPConn, torrent torrent.Torrent) []byte {
	b := make([]byte, 0, 98)
	b = append(b, connectID...)
	b = binary.BigEndian.AppendUint32(b, 1)
	b = binary.BigEndian.AppendUint32(b, rand.Uint32())
	b = append(b, torrent.InfoHash[:]...)
	b = append(b, utils.RandomPeerID()...)
	b = binary.BigEndian.AppendUint64(b, 0)
	b = binary.BigEndian.AppendUint64(b, torrent.ContentSize)
	b = binary.BigEndian.AppendUint64(b, 0)
	b = binary.BigEndian.AppendUint32(b, 0)
	b = binary.BigEndian.AppendUint32(b, 0)
	b = binary.BigEndian.AppendUint32(b, rand.Uint32())
	b = binary.BigEndian.AppendUint32(b, ^uint32(0))
	b = binary.BigEndian.AppendUint16(b, uint16(trackerConn.LocalAddr().(*net.UDPAddr).Port))
	return b
}

func parseConnResp(resp []byte) connResp {
	return connResp{action: resp[:4], transactionID: resp[4:8], connectionID: resp[8:16]}
}

func parseAnnounceResp(resp []byte) (announceResp, error) {
	if len(resp) < 20 {
		return announceResp{}, fmt.Errorf("announce response is too short: %d", len(resp))
	}
	peers := make([]*net.TCPAddr, 0, (len(resp)-20)/6)
	for i := 20; i+6 <= len(resp); i += 6 {
		peers = append(peers, &net.TCPAddr{
			IP:   net.IPv4(resp[i], resp[i+1], resp[i+2], resp[i+3]),
			Port: int(binary.BigEndian.Uint16(resp[i+4 : i+6])),
		})
	}
	return announceResp{action: resp[:4], transactionID: resp[4:8], interval: resp[8:12], leechers: binary.BigEndian.Uint32(resp[12:16]), seeders: binary.BigEndian.Uint32(resp[16:20]), peerAddresses: peers}, nil
}

func connectToTracker(addr *net.UDPAddr) (*net.UDPConn, error) {
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
