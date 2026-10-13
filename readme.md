# Ziftorrent

A BitTorrent client implemented in Go from the wire protocol up: raw TCP peer
connections, a hand-rolled UDP tracker client, and a concurrent piece-download
engine. No third-party torrent library is used for any protocol logic — the only
dependency is `bencode-go`, used strictly for parsing `.torrent` file metadata.

## Motivation

Most "build your own torrent client" projects wrap an existing library and call
it a day. Ziftorrent implements the [BitTorrent protocol spec](https://www.bittorrent.org/beps/bep_0003.html)
directly — the peer wire protocol, the UDP tracker handshake, piece selection,
block-level request scheduling, adaptive timeout/retry logic, and disk I/O across
multi-file torrents are all written from scratch, as a way to work with Go's
concurrency primitives (goroutines, channels, `context.Context`) on a real,
non-trivial network protocol rather than a toy problem.

A few things worth knowing before diving in:

- **Concurrent-by-design download engine**: one reader + one worker goroutine per
  peer, a shared job queue, and an O(1) request-matching map, coordinated end-to-end
  with `context.Context` for clean shutdown (including forcing blocked reads to
  unblock on cancellation).
- **Adaptive networking**: per-peer retry timeouts derived from a live, windowed
  RTT average (`avg * 4`, clamped to `[2s, 20s]`) instead of one fixed timeout for
  every peer — slow-but-healthy peers aren't punished the same as actually-dead
  ones.
- **Correct multi-file handling**: blocks are streamed to disk as they arrive and
  transparently split across file boundaries when a block straddles two files in
  the torrent's logical byte stream.

## Quick Start

Requires Go installed locally (see [go.dev/dl](https://go.dev/dl/)).

```sh
git clone https://github.com/nazifbara/ziftorrent.git
cd ziftorrent
go run ./cmd/torrent-cli -f path/to/file.torrent
```

That's it — the client parses the torrent, announces to its tracker, connects to
peers, and starts downloading. Files are written relative to the current
directory: a multi-file torrent goes under a directory named after the torrent
(`Torrent.Name`), mirroring the file paths declared in its info dictionary; a
single-file torrent is written directly, with no parent directory.

## Usage

```sh
go run ./cmd/torrent-cli -f path/to/file.torrent
```

Or build a binary first:

```sh
go build ./cmd/torrent-cli
./torrent-cli -f path/to/file.torrent
```

Flags:

| Flag | Description                                                           |
| ---- | --------------------------------------------------------------------- |
| `-f` | Path to the `.torrent` file (required)                                |
| `-t` | Override tracker URL instead of using the torrent's own announce list |

The client exits once every byte of the torrent's content has been written and
verified against the torrent's SHA-1 piece hashes.

## Contributing

Contributions and issues are welcome. This section is a map of the codebase and
the open work, for anyone looking to dig in.

### Project layout

```
cmd/torrent-cli/     CLI entry point
internal/
  client/            Client: owns and stops the running downloads
  download/          One transfer: peers, pieces, files
  peer/              Peer connection, handshake, RTT tracking
  piece/             Per-piece buffer and completion state
  torrent/           .torrent parsing and the Torrent model
  tracker/           UDP tracker client
  wire/              Peer-wire message encode/decode
  config/            Shared constants
  utils/             Shared helpers
```

| Path                             | Responsibility                                                         |
| -------------------------------- | ---------------------------------------------------------------------- |
| `cmd/torrent-cli/main.go`        | Flag parsing, signal handling, top-level shutdown                      |
| `internal/client/client.go`      | `Client`, the registry of active `Download`s                           |
| `internal/download/download.go`  | `Download` struct, output file creation, peer connection loop          |
| `internal/download/handler.go`   | Peer message handling, request/response flow, disk writes              |
| `internal/download/job_queue.go` | Job queue, enqueue/requeue, timeout handling                           |
| `internal/peer/peers.go`         | `Peer`: connection, choke state, bitfield                              |
| `internal/peer/handshake.go`     | BitTorrent handshake exchange                                          |
| `internal/peer/rtt.go`           | Per-peer adaptive RTT tracking                                         |
| `internal/piece/piece.go`        | `State`: block assembly, reset, release                                |
| `internal/torrent/torrent.go`    | `.torrent` (bencode) parsing, info hash, piece hashes, block sizing    |
| `internal/tracker/tracker.go`    | UDP tracker connect/announce, peer address parsing, connected-peer set |
| `internal/wire/wire.go`          | Wire protocol message encode/decode                                    |
| `internal/config/config.go`      | Block size constant                                                    |
| `internal/utils/utils.go`        | Bit packing, peer ID generation, retry helper, byte-size formatting    |

### Architecture notes

A `client.Client` owns the set of running transfers. Each transfer is a
`download.Download`, which holds everything scoped to one torrent: the job queue,
the in-flight request map, the piece states, the open files, and the context that
ties their lifetimes together.

**Concurrency model.** Each peer gets its own goroutines, both methods on
`Download`: `readPeerMessages` blocks on `peer.Read`, parses the length-prefixed
wire protocol, and dispatches messages (`choke`, `unchoke`, `have`, `bitfield`,
`piece`) to handlers. `runPeerWorker` starts once the peer has sent its
`bitfield`; it pulls jobs off a single shared, buffered channel
(`Download.queue`) and sends the corresponding `request` message to that peer,
provided it isn't choking and actually has the piece. There is no polling loop in
the request path: a block request arms a `time.AfterFunc` timer sized to the
peer's current measured RTT, and if it fires before the block arrives, the job is
pushed back onto the queue for another peer. In-flight requests are tracked in a
single map (`Download.pending`, keyed by `"pieceIndex:begin"`) for O(1) matching.
Shutdown is coordinated with `context.Context`: `Client.Stop` stops every
`Download`, and cancelling a download's context stops its peer workers and, via a
small per-peer watcher goroutine, forces any blocked `peer.Read()` to return by
closing the connection. `Stop` then waits for those goroutines and closes the
output files.

**Adaptive request timeout.** `internal/peer/rtt.go` keeps a windowed average of
each peer's round-trip times (`rttTracker`, over the last 100 samples). That
peer's re-request timeout is `avg * 4`, clamped to `[2s, 20s]`, and 8s until the
first sample arrives.

**Piece and file layout.** `piece.State` tracks a piece's in-memory buffer, how
many of its blocks have arrived, and whether the piece is complete. A block that
doesn't match an entry in `Download.pending` is dropped, so late or duplicate
deliveries aren't counted twice. Blocks are written to disk as soon as they
arrive rather than batched per-piece; a completed piece's hash is verified
against the torrent's SHA-1 piece hash list, and its in-memory buffer is
released. A piece that fails verification is reset and all of its blocks are
queued again. For multi-file torrents, `fileState` entries describe each file's
offset in the logical concatenated byte stream, and `writeAtGlobal` translates a
global piece/block offset into one or more `WriteAt` calls, splitting a single
block's write across a file boundary if it straddles two files.

### Running tests

```sh
go test ./...
```

| Test file                             | Covers                                                                 |
| ------------------------------------- | ---------------------------------------------------------------------- |
| `internal/wire/wire_test.go`          | Wire protocol message building/parsing                                 |
| `internal/utils/utils_test.go`        | Bit packing, peer ID generation, the retry helper                      |
| `internal/peer/rtt_test.go`           | RTT averaging, windowing, timeout clamping                             |
| `internal/peer/handshake_test.go`     | Handshake acceptance, wrong info hash/protocol rejection               |
| `internal/torrent/torrent_test.go`    | Bencode parsing, piece hashing, single/multi-file layout, block sizing |
| `internal/tracker/tracker_test.go`    | UDP tracker request/response wire format                               |
| `internal/piece/piece_test.go`        | Block assembly, out-of-bounds rejection, reset                         |
| `internal/download/job_queue_test.go` | Job enqueue/requeue, timeout handling                                  |
| `internal/download/handler_test.go`   | Disk writes, including multi-file boundary splitting                   |
| `internal/client/client_test.go`      | Download registration                                                  |

### Good areas to contribute

The core download path — parsing, tracker communication, peer wire protocol,
concurrent piece retrieval, and multi-file disk writes — is complete and tested.
Open items if you want to send a PR:

- **HTTP tracker + DHT support**, alongside the existing UDP tracker client.
- **Resume support** — persist completed-piece state to disk so interrupted
  downloads don't restart from zero.
- **Seeding** — upload to peers instead of leech-only behavior.
- **Smarter piece/peer selection** — rarest-first piece picking, tit-for-tat
  choking, and endgame-mode redundant requests.
