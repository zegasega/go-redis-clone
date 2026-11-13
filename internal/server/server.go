package server

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"sync"
	"time"
)

type Config struct {
	ListenAddr      string
	ShardCount      int
	CleanerInterval time.Duration
}

type TCPServer struct {
	config   Config
	storage  *Storage
	listener net.Listener
	metrics  *Metrics
}

type Storage struct {
	shards []*Shard
}

type Shard struct {
	mu    sync.RWMutex
	items map[string]*Item
}

type Item struct {
	value     []byte
	expiresAt time.Time
}

func NewTCPServer(config Config) *TCPServer {
	if config.ShardCount <= 0 {
		config.ShardCount = runtime.NumCPU()
	}

	shards := make([]*Shard, config.ShardCount)

	for i := range shards {
		shards[i] = &Shard{
			items: make(map[string]*Item),
		}
	}

	return &TCPServer{
		config:  config,
		metrics: NewMetrics(),
		storage: &Storage{
			shards: shards,
		},
	}
}

func (s *Shard) cleanSample() (checked, expired int) {
	s.mu.Lock()
	defer s.mu.Unlock()

	const sampleSize = 20
	now := time.Now()

	for k, item := range s.items {
		checked++

		if !item.expiresAt.IsZero() && now.After(item.expiresAt) {
			delete(s.items, k)
			expired++
		}

		if checked >= sampleSize {
			break
		}
	}

	return checked, expired
}

func (s *Shard) startCleaner(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			checked, expired := s.cleanSample()

			if expired > 0 {
				fmt.Printf("Cleaner: checked=%d expired=%d\n", checked, expired)
			}

		case <-ctx.Done():
			return
		}
	}
}

func (s *TCPServer) startCleaner() {
	interval := s.config.CleanerInterval

	if interval <= 0 {
		interval = 5 * time.Second
	}

	ctx := context.Background()

	for _, shard := range s.storage.shards {
		go shard.startCleaner(ctx, interval)
	}
}

func (s *TCPServer) Run() error {
	listener, err := net.Listen("tcp", s.config.ListenAddr)
	if err != nil {
		return fmt.Errorf("failed to start TCP server: %w", err)
	}

	s.listener = listener
	defer listener.Close()

	s.startCleaner()

	fmt.Printf("TCP server started on %s\n", listener.Addr())

	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("failed to accept connection: %w", err)
		}

		fmt.Println("New client connected:", conn.RemoteAddr())

		go s.handleConnection(conn)
	}
}
