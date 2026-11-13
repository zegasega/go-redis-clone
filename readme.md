# Go Redis Clone

A lightweight, in-memory key-value storage server written in Go, inspired by Redis.

The project focuses on building a concurrent TCP server and understanding low-level storage concepts in Go.

## Features

- TCP-based client connections
- Concurrent client handling using goroutines
- Sharded in-memory storage
- Per-shard `sync.RWMutex` for concurrent access
- TTL (Time To Live) support for stored items
- Background expiration cleanup
- Hash-based key distribution using `xxhash`

## Architecture

The storage layer is divided into multiple shards. Each shard maintains its own key-value map and mutex, allowing independent access to different parts of the storage.

```text
                 TCP Server
                      |
              Client Connections
                      |
                  Storage
                      |
          +-----------+-----------+
          |           |           |
        Shard 1     Shard 2     Shard N
          |           |           |
        Map         Map         Map
          |           |           |
       Cleaner     Cleaner     Cleaner
```

## Project Structure

```text
.
├── cmd/
│   └── redis-clone/
│       └── main.go
├── internal/
│   └── server/
│       ├── commands.go
│       ├── connection.go
│       └── server.go
├── test/
│   └── server_test.go
└── go.mod
```

## Requirements

- Go 1.22+

## Getting Started

Clone the repository:

```bash
git clone https://github.com/zegasega/go-redis-clone.git
cd go-redis-clone
```

Run the server:

```bash
go run ./cmd/redis-clone
```

## Running Tests

```bash
go test ./...
```

## Technologies

- Go
- TCP Networking
- Goroutines
- Sync / RWMutex
- In-memory data structures
- xxhash

## License

MIT
