package credential

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sync"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// Store owns the current process-local credential for every configured
// RegistryKey. Values are copied on both read and write.
type Store struct {
	mu           sync.RWMutex
	current      map[config.RegistryKey]Entry
	nextRevision uint64
}

// Apply atomically replaces one RegistryKey's credential and assigns a new
// process-local revision.
func (s *Store) Apply(registry config.RegistryConfig, value Credential) Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.current == nil {
		s.current = make(map[config.RegistryKey]Entry)
	}
	s.nextRevision++
	entry := Entry{
		Credential:         value,
		Revision:           s.nextRevision,
		authenticationHash: registryAuthenticationHash(registry),
	}
	s.current[registry.Key()] = entry
	return entry
}

func (s *Store) Load(key config.RegistryKey) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	entry, found := s.current[key]
	return entry, found
}

func registryAuthenticationHash(registry config.RegistryConfig) string {
	input := struct {
		RegionID        string `json:"regionID"`
		InstanceID      string `json:"instanceID"`
		AccessKeyID     string `json:"accessKeyID"`
		AccessKeySecret string `json:"accessKeySecret"`
	}{
		RegionID:        registry.RegionID,
		InstanceID:      registry.InstanceID,
		AccessKeyID:     registry.AccessKeyID,
		AccessKeySecret: registry.AccessKeySecret,
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		panic("marshal fixed Registry authentication hash input: " + err.Error())
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:])
}

// Delete removes a credential when its RegistryKey leaves the valid
// configuration. It reports whether a value was present.
func (s *Store) Delete(key config.RegistryKey) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, found := s.current[key]; !found {
		return false
	}
	delete(s.current, key)
	return true
}

// Snapshot returns an independent point-in-time copy for future Secret
// rendering and state recovery code.
func (s *Store) Snapshot() map[config.RegistryKey]Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := make(map[config.RegistryKey]Entry, len(s.current))
	for key, entry := range s.current {
		snapshot[key] = entry
	}
	return snapshot
}
