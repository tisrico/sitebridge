// Package tagstore holds the latest value and quality of every tag.
package tagstore

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// Quality says whether a value can be trusted. It maps directly onto OPC UA
// status codes so a SCADA client sees the same thing the bridge knows.
type Quality int

const (
	// WaitingForInitialData: no successful read yet.
	WaitingForInitialData Quality = iota
	// Good: read successfully on the last poll.
	Good
	// CommFailure: the last poll failed; the value is the last known one.
	CommFailure
	// ConfigError: the device rejected the read (e.g. illegal address).
	ConfigError
	// OutOfService: this node is standby or shutting down and is not polling.
	OutOfService
)

func (q Quality) String() string {
	switch q {
	case Good:
		return "good"
	case CommFailure:
		return "comm_failure"
	case ConfigError:
		return "config_error"
	case OutOfService:
		return "out_of_service"
	}
	return "waiting"
}

// Value is a tag reading.
type Value struct {
	V  float64   `json:"v"`
	Q  Quality   `json:"-"`
	TS time.Time `json:"ts"`
}

// Store is a concurrency-safe map of tag key to latest value.
type Store struct {
	mu       sync.RWMutex
	vals     map[string]Value
	onChange func(key string)
}

// New returns a store pre-populated with the given keys.
func New(keys []string) *Store {
	s := &Store{vals: make(map[string]Value, len(keys))}
	for _, k := range keys {
		s.vals[k] = Value{Q: WaitingForInitialData}
	}
	return s
}

// OnChange registers a callback run (outside the lock) whenever a value or
// its quality changes. Set it before the store is shared.
func (s *Store) OnChange(f func(key string)) { s.onChange = f }

// Get returns the value for key.
func (s *Store) Get(key string) (Value, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	v, ok := s.vals[key]
	return v, ok
}

// Set records a new value and reports whether it changed.
func (s *Store) Set(key string, v Value) bool {
	s.mu.Lock()
	old, ok := s.vals[key]
	changed := !ok || old.V != v.V || old.Q != v.Q
	s.vals[key] = v
	s.mu.Unlock()
	if changed && s.onChange != nil {
		s.onChange(key)
	}
	return changed
}

// SetQuality changes the quality of every key with the given prefix while
// keeping the last known value.
func (s *Store) SetQuality(prefix string, q Quality) {
	var changed []string
	now := time.Now()
	s.mu.Lock()
	for k, v := range s.vals {
		if strings.HasPrefix(k, prefix) && v.Q != q {
			v.Q = q
			v.TS = now
			s.vals[k] = v
			changed = append(changed, k)
		}
	}
	s.mu.Unlock()
	if s.onChange != nil {
		for _, k := range changed {
			s.onChange(k)
		}
	}
}

// Snapshot returns a copy of all values.
func (s *Store) Snapshot() map[string]Value {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]Value, len(s.vals))
	for k, v := range s.vals {
		out[k] = v
	}
	return out
}

// Keys returns all keys in sorted order.
func (s *Store) Keys() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := make([]string, 0, len(s.vals))
	for k := range s.vals {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
