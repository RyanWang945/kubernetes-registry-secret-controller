package config

import "sync"

// Store owns the last valid immutable configuration snapshot.
type Store struct {
	mu      sync.RWMutex
	current *ConfigurationSnapshot
	valid   bool
}

type ApplyResult struct {
	Current         ConfigurationSnapshot
	Changed         bool
	BusinessChanged bool
}

// Apply atomically installs a semantically new snapshot. Equivalent updates do
// not advance Generation.
func (s *Store) Apply(candidate ConfigurationSnapshot) ApplyResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.valid = true

	candidate = candidate.Clone()
	if s.current != nil && s.current.Equal(candidate) {
		current := s.current.Clone()
		return ApplyResult{
			Current: current,
		}
	}

	result := ApplyResult{Changed: true, BusinessChanged: s.current == nil || !s.current.BusinessEqual(candidate)}
	if s.current != nil {
		candidate.Generation = s.current.Generation + 1
	} else {
		candidate.Generation = 1
	}

	s.current = &candidate
	result.Current = candidate.Clone()
	return result
}

// MarkInvalid preserves the last valid snapshot while exposing the state of
// the latest observed ConfigMap independently from process readiness.
func (s *Store) MarkInvalid() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.valid = false
}

func (s *Store) Valid() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.valid
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
