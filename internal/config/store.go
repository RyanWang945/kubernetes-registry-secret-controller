package config

import "sync"

// Store owns the last valid immutable configuration snapshot.
type Store struct {
	mu      sync.RWMutex
	current *ConfigurationSnapshot
}

type ApplyResult struct {
	Previous    ConfigurationSnapshot
	Current     ConfigurationSnapshot
	HadPrevious bool
	Changed     bool
}

// Apply atomically installs a semantically new snapshot. Equivalent updates do
// not advance Generation.
func (s *Store) Apply(candidate ConfigurationSnapshot) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	candidate = candidate.Clone()
	if s.current != nil && s.current.Equal(candidate) {
		current := s.current.Clone()
		return ApplyResult{
			Previous:    current.Clone(),
			Current:     current,
			HadPrevious: true,
		}
	}

	result := ApplyResult{Changed: true}
	if s.current != nil {
		result.Previous = s.current.Clone()
		result.HadPrevious = true
		candidate.Generation = s.current.Generation + 1
	} else {
		candidate.Generation = 1
	}

	s.current = &candidate
	result.Current = candidate.Clone()
	return result
}

func (s *Store) Load() (ConfigurationSnapshot, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.current == nil {
		return ConfigurationSnapshot{}, false
	}
	return s.current.Clone(), true
}
