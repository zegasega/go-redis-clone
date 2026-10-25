package main

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type item struct {
	value     string
	expiresAt time.Time
}

type Storage struct {
	mu     sync.RWMutex
	values map[string]item
	cancel context.CancelFunc
}

func NewInMemoryStorage(interval time.Duration) *Storage {

	ctx, cancel := context.WithCancel(context.Background())
	s := &Storage{
		values: make(map[string]item),
		cancel: cancel,
	}

	if interval > 0 {
		go s.Cleaner(ctx, interval)
	}

	return s
}

func (s *Storage) Get(key string) (bool, string) {
	s.mu.RLock()
	val, exists := s.values[key]

	if !exists {
		s.mu.RUnlock()
		return false, "Key does not exist"
	}

	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		s.mu.RUnlock()

		s.mu.Lock()
		if current, ok := s.values[key]; ok && !current.expiresAt.IsZero() && time.Now().After(current.expiresAt) {
			delete(s.values, key)
		}
		s.mu.Unlock()

		return false, "Key has expired"
	}
	s.mu.RUnlock()
	return true, val.value
}

func (s *Storage) Set(key, value string, ttl time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	s.values[key] = item{
		value:     value,
		expiresAt: expiresAt,
	}
}

func (s *Storage) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.values[key]
	if !exists {
		return false
	}

	delete(s.values, key)

	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		return false
	}

	return true
}

func (s *Storage) pruneExpried() {
	now := time.Now()
	s.mu.RLock()

	var expriedKeys []string

	for k, v := range s.values {
		if !v.expiresAt.IsZero() && now.After(v.expiresAt) {
			expriedKeys = append(expriedKeys, k)
		}
	}
	s.mu.RUnlock()

	if len(expriedKeys) == 0 {
		return

	}

	s.mu.Lock()
	for _, key := range expriedKeys {
		if !s.values[key].expiresAt.IsZero() && time.Now().After(s.values[key].expiresAt) {
			delete(s.values, key)
		}
	}
	s.mu.Unlock()

}

func (s *Storage) Cleaner(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Cleaner Stopped!")
			return
		case <-ticker.C:
			s.pruneExpried()
		}
	}
}

func main() {
	storage := NewInMemoryStorage(1 * time.Second)
	defer storage.cancel()
	storage.Set("token", "abc123", 2*time.Second)
	time.Sleep(time.Second * 3)
	storage.Get("token")
}
