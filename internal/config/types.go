package config

import (
	"reflect"
	"sort"
)

const (
	NamespaceKey        = "namespace"
	ExcludeNamespaceKey = "excludeNamespace"
	ServiceAccountKey   = "serviceaccount"
	RegistriesKey       = "registries"
	Wildcard            = "*"
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

// NameSelector represents either every name or an explicit sorted set of
// names. Values are immutable after parsing.
type NameSelector struct {
	MatchAll bool
	Names    []string
}

func (s NameSelector) Matches(name string) bool {
	if s.MatchAll {
		return true
	}
	return containsSorted(s.Names, name)
}

func containsSorted(values []string, value string) bool {
	index := sort.SearchStrings(values, value)
	return index < len(values) && values[index] == value
}

// ConfigurationSnapshot is a complete, normalized and immutable runtime
// configuration. Generation is assigned by Store and only changes for
// semantic updates.
type ConfigurationSnapshot struct {
	Generation         uint64
	Namespaces         NameSelector
	ExcludedNamespaces []string
	ServiceAccounts    NameSelector
	Registries         map[RegistryKey]RegistryConfig
}

// Equal reports semantic equality and intentionally ignores Generation.
func (s ConfigurationSnapshot) Equal(other ConfigurationSnapshot) bool {
	return reflect.DeepEqual(s.Namespaces, other.Namespaces) &&
		reflect.DeepEqual(s.ExcludedNamespaces, other.ExcludedNamespaces) &&
		reflect.DeepEqual(s.ServiceAccounts, other.ServiceAccounts) &&
		reflect.DeepEqual(s.Registries, other.Registries)
}

func (s ConfigurationSnapshot) MatchesNamespace(name string) bool {
	return s.Namespaces.Matches(name) && !containsSorted(s.ExcludedNamespaces, name)
}

func (s ConfigurationSnapshot) Clone() ConfigurationSnapshot {
	clone := ConfigurationSnapshot{
		Generation:         s.Generation,
		Namespaces:         cloneSelector(s.Namespaces),
		ExcludedNamespaces: append([]string(nil), s.ExcludedNamespaces...),
		ServiceAccounts:    cloneSelector(s.ServiceAccounts),
		Registries:         make(map[RegistryKey]RegistryConfig, len(s.Registries)),
	}

	for key, registry := range s.Registries {
		registry.Domains = append([]string(nil), registry.Domains...)
		clone.Registries[key] = registry
	}

	return clone
}

func cloneSelector(selector NameSelector) NameSelector {
	return NameSelector{
		MatchAll: selector.MatchAll,
		Names:    append([]string(nil), selector.Names...),
	}
}
