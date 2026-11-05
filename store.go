package kv

import (
	"fmt"
	"sync"
)

type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

func NewStore() *Store {
	return &Store{data: make(map[string]string)}
}

func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

func (s *Store) Apply(cmd Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	switch cmd.Op {
	case "put":
		s.data[cmd.Key] = cmd.Value
		return nil
	default:
		return fmt.Errorf("unsupported operation %q", cmd.Op)
	}
}
