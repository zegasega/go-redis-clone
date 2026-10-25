# Go Redis Clone

A simple, lightweight in-memory key-value storage written in Go (Golang), inspired by Redis. This project is built from scratch to understand concurrency control, TTL (Time-To-Live) expiration mechanisms, and clean architecture in Go.

## 🛠️ Features (Current)
- **In-Memory Storage**: Fast data storage using Go maps.
- **Thread-Safe**: Safe concurrent reads and writes using `sync.RWMutex`.
- **TTL & Expiration**: Support for setting time-to-live on keys.
- **Background Cleaner**: Automatic goroutine-based background worker that periodically sweeps and deletes expired keys.

## 🚧 Roadmap / Future Plans
- [ ] **TCP Server**: Implement a TCP server using Go's `net` package to accept raw client connections.
- [ ] **RESP Protocol**: Parse and respond using the Redis Serialization Protocol (RESP).
- [ ] **Commands Support**: Add support for core Redis commands (`SET`, `GET`, `DEL`, `EXPIRE`).
