package main

import (
	"fmt"
	"sync"
	"time"
)

type Val struct {
	key       string
	value     string
	ttl       time.Duration
	expiresAt time.Time
}

type Storage struct {
	mu     sync.RWMutex
	Values map[string]Val
}

func NewInMemoryStorage(interval time.Duration) *Storage {
	s := &Storage{
		Values: make(map[string]Val),
	}

	if interval > 0 {
		go s.Cleaner(interval)
	}

	return s
}

func (s *Storage) Get(key string) (bool, string) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	val, exists := s.Values[key]

	if !exists {
		return false, "Key does not exist"
	}

	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		return false, "Key has expired"
	}
	return true, val.value
}

func (s *Storage) Set(key, value string, ttl time.Duration) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var expiresAt time.Time
	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}
	s.Values[key] = Val{
		key:       key,
		value:     value,
		ttl:       ttl,
		expiresAt: expiresAt,
	}
	return true, "OK!"
}

func (s *Storage) Delete(key string) (bool, string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	val, exists := s.Values[key]
	if !exists {
		return false, "Key does not exist"
	}

	if !val.expiresAt.IsZero() && time.Now().After(val.expiresAt) {
		delete(s.Values, key)
		return false, "Key has expired"
	}

	delete(s.Values, key)
	return true, "deleted"
}

func (s *Storage) Cleaner(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		s.mu.Lock()
		now := time.Now()
		for key, val := range s.Values {
			if !val.expiresAt.IsZero() && now.After(val.expiresAt) {
				delete(s.Values, key)
			}
		}
		s.mu.Unlock()
	}
}

func main() {
	storage := NewInMemoryStorage(1 * time.Second)

	storage.Set("hello", "world", 3*time.Second)

	found, val := storage.Get("hello")
	fmt.Printf("Get -> Found: %v, Value: %s\n", found, val)

	time.Sleep(4 * time.Second)

	x, val := storage.Get("hello")
	fmt.Printf("Get (After TTL) -> Found: %v, Message: %s\n", x, val)
}
