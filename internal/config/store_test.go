package config

import "testing"

func TestStoreGenerationsAndDefensiveCopies(t *testing.T) {
	t.Parallel()

	first, err := Parse(validData("production", "default"))
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	store := &Store{}
	result := store.Apply(first)
	if !result.Changed || result.HadPrevious || result.Current.Generation != 1 {
		t.Fatalf("first Apply() = %+v, want changed generation 1 without previous", result)
	}

	key := RegistryKey{RegionID: "cn-hangzhou", InstanceID: "cri-aaaaaaaa"}
	mutated := result.Current
	registry := mutated.Registries[key]
	registry.Domains[0] = "mutated.example.com"
	mutated.Registries[key] = registry
	mutated.Namespaces.Names = append(mutated.Namespaces.Names, "mutated")
	mutated.ExcludedNamespaces = append(mutated.ExcludedNamespaces, "production")

	loaded, ok := store.Load()
	if !ok {
		t.Fatal("Load() returned no snapshot")
	}
	if loaded.Registries[key].Domains[0] == "mutated.example.com" ||
		loaded.Namespaces.Matches("mutated") ||
		!loaded.MatchesNamespace("production") {
		t.Fatal("caller mutation changed the stored snapshot")
	}

	equivalent := store.Apply(first)
	if equivalent.Changed || equivalent.Current.Generation != 1 {
		t.Fatalf("equivalent Apply() = %+v, want unchanged generation 1", equivalent)
	}

	second, err := Parse(validData("production,staging", "default"))
	if err != nil {
		t.Fatalf("second Parse() error = %v", err)
	}
	changed := store.Apply(second)
	if !changed.Changed || !changed.HadPrevious || changed.Current.Generation != 2 {
		t.Fatalf("changed Apply() = %+v, want changed generation 2 with previous", changed)
	}
}
