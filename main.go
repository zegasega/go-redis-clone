package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

type Server struct {
	addr     string
	storage  *Storage
	listener net.Listener
}
type Storage struct {
	mu     sync.RWMutex
	items  map[string]item
	cancel context.CancelFunc
}

type item struct {
	value     string
	expiresAt time.Time
}

func NewInMemoryStorage(interval time.Duration) *Storage {
	ctx, cancel := context.WithCancel(context.Background())
	s := &Storage{
		items:  make(map[string]item),
		cancel: cancel,
	}

	if interval > 0 {
		go s.cleaner(ctx, interval)
	}

	return s
}

func (s *Storage) Close() {
	if s.cancel != nil {
		s.cancel()
	}
}

func (s *Storage) Get(key string) (string, bool) {
	s.mu.RLock()
	val, exists := s.items[key]

	if !exists {
		s.mu.RUnlock()
		return "", false
	}

	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		s.mu.RUnlock()

		s.mu.Lock()
		if current, ok := s.items[key]; ok && !current.expiresAt.IsZero() && time.Now().After(current.expiresAt) {
			delete(s.items, key)
		}
		s.mu.Unlock()

		return "", false
	}
	s.mu.RUnlock()
	return val.value, true
}

func (s *Storage) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	s.items[key] = item{
		value:     value,
		expiresAt: expiresAt,
	}
}

func (s *Storage) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.items[key]
	if !exists {
		return false
	}

	delete(s.items, key)

	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		return false
	}

	return true
}

func (s *Storage) pruneExpired() {
	now := time.Now()
	s.mu.RLock()

	var expiredKeys []string
	for k, v := range s.items {
		if !v.expiresAt.IsZero() && now.After(v.expiresAt) {
			expiredKeys = append(expiredKeys, k)
		}
	}
	s.mu.RUnlock()

	if len(expiredKeys) == 0 {
		return
	}

	s.mu.Lock()
	for _, key := range expiredKeys {
		if it, ok := s.items[key]; ok && !it.expiresAt.IsZero() && now.After(it.expiresAt) {
			delete(s.items, key)
		}
	}
	s.mu.Unlock()
}

func (s *Storage) cleaner(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("Storage cleaner stopped")
			return
		case <-ticker.C:
			s.pruneExpired()
		}
	}
}

func NewServer(addr string, storage *Storage) *Server {
	return &Server{
		addr:    addr,
		storage: storage,
	}
}

func (srv *Server) Start() error {
	listener, err := net.Listen("tcp", srv.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", srv.addr, err)
	}
	srv.listener = listener
	log.Printf("Server listening on %s\n", srv.addr)

	return srv.acceptConnections()
}

func (srv *Server) acceptConnections() error {
	for {
		conn, err := srv.listener.Accept()
		if err != nil {
			return fmt.Errorf("accept error: %w", err)
		}

		go srv.handleConnection(conn)
	}
}

func (srv *Server) handleConnection(conn net.Conn) {
	defer conn.Close()
	log.Printf("New client connected: %s\n", conn.RemoteAddr())
	conn.Write([]byte("hello\n"))

	reader := bufio.NewReader(conn)

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err != io.EOF {
				log.Printf("Read error [%s]: %v\n", conn.RemoteAddr(), err)
			}
			break
		}

		parts := strings.Fields(strings.TrimSpace(line))
		if len(parts) == 0 {
			continue
		}

		cmd := strings.ToUpper(parts[0])

		switch cmd {
		case "GET":
			if len(parts) < 2 {
				conn.Write([]byte("ERR: usage GET <key>\n"))
				continue
			}
			if val, ok := srv.storage.Get(parts[1]); ok {
				conn.Write([]byte(fmt.Sprintf("VALUE: %s\n", val)))
			} else {
				conn.Write([]byte("ERR: key not found or expired\n"))
			}

		case "SET":
			if len(parts) < 3 {
				conn.Write([]byte("ERR: usage SET <key> <value> [ttl_seconds]\n"))
				continue
			}
			ttl := time.Duration(0)
			if len(parts) >= 4 {
				if d, err := time.ParseDuration(parts[3] + "s"); err == nil {
					ttl = d
				}
			}
			srv.storage.Set(parts[1], parts[2], ttl)
			conn.Write([]byte("OK\n"))

		case "DEL":
			if len(parts) < 2 {
				conn.Write([]byte("ERR: usage DEL <key>\n"))
				continue
			}
			if srv.storage.Delete(parts[1]) {
				conn.Write([]byte("OK\n"))
			} else {
				conn.Write([]byte("ERR: key not found\n"))
			}

		default:
			conn.Write([]byte("ERR: unknown command\n"))
		}
	}

	log.Printf("Client disconnected: %s\n", conn.RemoteAddr())
}

func main() {
	storage := NewInMemoryStorage(1 * time.Second)
	defer storage.Close()

	server := NewServer(":8080", storage)

	if err := server.Start(); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}
