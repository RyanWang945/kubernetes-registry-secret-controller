package config

import "sync"

// Store owns the last valid immutable configuration snapshot.
type Store struct {
	mu      sync.RWMutex
	current *ConfigurationSnapshot
}

type ApplyResult struct {
	Current ConfigurationSnapshot
	Changed bool
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
			Current: current,
		}
	}

	result := ApplyResult{Changed: true}
	if s.current != nil {
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

// Loaded reports whether at least one valid configuration has been applied.
func (s *Store) Loaded() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current != nil
}

// MatchesServiceAccount performs the hot event-filtering query without cloning
// the complete configuration snapshot for every ServiceAccount event.
func (s *Store) MatchesServiceAccount(namespace, serviceAccount string) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.current != nil &&
		s.current.MatchesNamespace(namespace) &&
		s.current.ServiceAccounts.Matches(serviceAccount)
}
