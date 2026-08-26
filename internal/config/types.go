package config

import (
	"reflect"
	"sort"
)

const (
	NamespaceKey      = "namespace"
	ServiceAccountKey = "serviceaccount"
	RegistriesKey     = "registries"
)

// RegistryKey is the stable, cluster-wide identity of one ACR registry.
type RegistryKey struct {
	RegionID   string
	InstanceID string
}

func (k RegistryKey) String() string {
	return k.RegionID + "/" + k.InstanceID
}

// RegistryConfig is a normalized ACR registry configuration. Domains is always
// non-empty, de-duplicated, lower-case and sorted.
type RegistryConfig struct {
	RegionID        string   `json:"regionID" yaml:"regionID"`
	InstanceID      string   `json:"instanceID" yaml:"instanceID"`
	AccessKeyID     string   `json:"accessKeyID" yaml:"accessKeyID"`
	AccessKeySecret string   `json:"accessKeySecret" yaml:"accessKeySecret"`
	Domains         []string `json:"domains" yaml:"domains"`
}

func (r RegistryConfig) Key() RegistryKey {
	return RegistryKey{RegionID: r.RegionID, InstanceID: r.InstanceID}
}

// NameSelector represents either all names (minus Excluded) or an explicit,
// sorted set of names. Values are immutable after parsing.
type NameSelector struct {
	All      bool
	Names    []string
	Excluded []string
}

func (s NameSelector) Matches(name string) bool {
	if s.All {
		return !containsSorted(s.Excluded, name)
	}
	return containsSorted(s.Names, name)
}

func containsSorted(values []string, value string) bool {
	index := sort.SearchStrings(values, value)
	return index < len(values) && values[index] == value
}

// Snapshot is a complete, normalized and immutable runtime configuration.
// Generation is assigned by Store and only changes for semantic updates.
type Snapshot struct {
	Generation      uint64
	Namespaces      NameSelector
	ServiceAccounts NameSelector
	Registries      map[RegistryKey]RegistryConfig
}

// Equal reports semantic equality and intentionally ignores Generation.
func (s Snapshot) Equal(other Snapshot) bool {
	return reflect.DeepEqual(s.Namespaces, other.Namespaces) &&
		reflect.DeepEqual(s.ServiceAccounts, other.ServiceAccounts) &&
		reflect.DeepEqual(s.Registries, other.Registries)
}

func (s Snapshot) Clone() Snapshot {
	clone := Snapshot{
		Generation:      s.Generation,
		Namespaces:      cloneSelector(s.Namespaces),
		ServiceAccounts: cloneSelector(s.ServiceAccounts),
		Registries:      make(map[RegistryKey]RegistryConfig, len(s.Registries)),
	}

	for key, registry := range s.Registries {
		registry.Domains = append([]string(nil), registry.Domains...)
		clone.Registries[key] = registry
	}

	return clone
}

func cloneSelector(selector NameSelector) NameSelector {
	return NameSelector{
		All:      selector.All,
		Names:    append([]string(nil), selector.Names...),
		Excluded: append([]string(nil), selector.Excluded...),
	}
}
