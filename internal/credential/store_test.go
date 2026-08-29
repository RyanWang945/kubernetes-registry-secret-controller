package credential

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

func TestStoreAppliesLoadsSnapshotsAndDeletesCredentials(t *testing.T) {
	t.Parallel()

	store := &Store{}
	registry := config.RegistryConfig{
		RegionID: "cn-hangzhou", InstanceID: "cri-one", AccessKeyID: "key", AccessKeySecret: "secret",
	}
	key := registry.Key()
	firstCredential := Credential{
		Username:    "first-user",
		Password:    "first-password",
		RefreshedAt: time.Unix(1, 0),
		ExpiresAt:   time.Unix(2, 0),
	}
	first := store.Apply(registry, firstCredential)
	second := store.Apply(registry, Credential{Username: "second-user"})

	if first.Revision == 0 || second.Revision != first.Revision+1 {
		t.Fatalf("revisions = %d, %d; want two increasing non-zero revisions", first.Revision, second.Revision)
	}
	loaded, found := store.Load(key)
	if !found || loaded.Credential.Username != "second-user" || loaded.Revision != second.Revision {
		t.Fatalf("Load() = %+v, %v; want second entry", loaded, found)
	}
	if !loaded.MatchesRegistry(registry) {
		t.Fatal("stored credential does not match its source Registry authentication")
	}
	rotated := registry
	rotated.AccessKeySecret = "rotated-secret"
	if loaded.MatchesRegistry(rotated) {
		t.Fatal("stored credential matches a rotated AccessKeySecret")
	}

	snapshot := store.Snapshot()
	delete(snapshot, key)
	if _, found := store.Load(key); !found {
		t.Fatal("mutating Snapshot() changed Store")
	}
	if !store.Delete(key) || store.Delete(key) {
		t.Fatal("Delete() must report true once and false when the key is already absent")
	}
}

func TestStoreSupportsConcurrentReadersAndWriters(t *testing.T) {
	t.Parallel()

	store := &Store{}
	const entries = 64
	var writers sync.WaitGroup
	for index := range entries {
		writers.Add(1)
		go func() {
			defer writers.Done()
			registry := config.RegistryConfig{
				RegionID: "cn-hangzhou", InstanceID: fmt.Sprintf("cri-%d", index), AccessKeyID: "key", AccessKeySecret: "secret",
			}
			store.Apply(registry, Credential{Username: fmt.Sprintf("user-%d", index)})
			store.Load(registry.Key())
			store.Snapshot()
		}()
	}
	writers.Wait()

	if got := len(store.Snapshot()); got != entries {
		t.Fatalf("Snapshot() entries = %d, want %d", got, entries)
	}
}
