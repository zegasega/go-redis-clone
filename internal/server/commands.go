package server

import (
	"time"
)

func (s *Shard) Get(key string) ([]byte, bool) {
	s.mu.RLock()
	item, exists := s.items[key]

	if !exists {
		s.mu.RUnlock()
		return nil, false
	}

	if !item.expiresAt.IsZero() && time.Now().After(item.expiresAt) {
		s.mu.RUnlock()

		s.mu.Lock()
		if current, exists := s.items[key]; exists && current == item {
			delete(s.items, key)
		}
		s.mu.Unlock()

		return nil, false
	}

	value := item.value
	s.mu.RUnlock()

	return value, true
}

func (s *Shard) Set(key string, value []byte, ttl time.Duration) {
	var expiresAt time.Time

	if ttl > 0 {
		expiresAt = time.Now().Add(ttl)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.items[key] = &Item{
		value:     value,
		expiresAt: expiresAt,
	}
}

func (s *Shard) Delete(key string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.items[key]; !exists {
		return false
	}

	delete(s.items, key)
	return true
}
