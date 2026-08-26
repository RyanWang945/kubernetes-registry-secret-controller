package config

import (
	"fmt"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/validation"
	"sigs.k8s.io/yaml"
)

// Parse validates and normalizes the supported ConfigMap data fields into an
// immutable runtime configuration.
func Parse(data map[string]string) (ConfigurationSnapshot, error) {
	for key := range data {
		switch key {
		case NamespaceKey, ExcludeNamespaceKey, ServiceAccountKey, RegistriesKey:
		default:
			return ConfigurationSnapshot{}, fmt.Errorf("unsupported ConfigMap data key %q", key)
		}
	}

	namespaces, err := parseNameSelector(
		data[NamespaceKey],
		"namespace",
		validation.IsDNS1123Label,
	)
	if err != nil {
		return ConfigurationSnapshot{}, err
	}

	excludedNamespaces, err := parseOptionalNameList(
		data[ExcludeNamespaceKey],
		"excludeNamespace",
		validation.IsDNS1123Label,
	)
	if err != nil {
		return ConfigurationSnapshot{}, err
	}

	serviceAccounts, err := parseNameSelector(
		data[ServiceAccountKey],
		"serviceaccount",
		validation.IsDNS1123Subdomain,
	)
	if err != nil {
		return ConfigurationSnapshot{}, err
	}

	registries, err := parseRegistries(data[RegistriesKey])
	if err != nil {
		return ConfigurationSnapshot{}, err
	}

	return ConfigurationSnapshot{
		Namespaces:         namespaces,
		ExcludedNamespaces: excludedNamespaces,
		ServiceAccounts:    serviceAccounts,
		Registries:         registries,
	}, nil
}

func parseNameSelector(
	raw string,
	field string,
	validate func(string) []string,
) (NameSelector, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return NameSelector{}, fmt.Errorf("%s must not be empty", field)
	}

	values, err := parseNameList(raw, field, validate, true)
	if err != nil {
		return NameSelector{}, err
	}

	if containsSorted(values, Wildcard) {
		if len(values) != 1 {
			return NameSelector{}, fmt.Errorf("%s wildcard %q cannot be combined with explicit names", field, Wildcard)
		}
		return NameSelector{MatchAll: true}, nil
	}

	return NameSelector{Names: values}, nil
}

func parseOptionalNameList(raw, field string, validate func(string) []string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	return parseNameList(raw, field, validate, false)
}

func parseNameList(
	raw string,
	field string,
	validate func(string) []string,
	allowWildcard bool,
) ([]string, error) {
	parts := strings.Split(raw, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		name := strings.TrimSpace(part)
		if name == "" {
			return nil, fmt.Errorf("%s contains an empty name", field)
		}
		if name != Wildcard || !allowWildcard {
			if problems := validate(name); len(problems) > 0 {
				return nil, fmt.Errorf("invalid %s name %q: %s", field, name, strings.Join(problems, "; "))
			}
		}
		values = append(values, name)
	}
	return sortedUnique(values), nil
}

func parseRegistries(raw string) (map[RegistryKey]RegistryConfig, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, fmt.Errorf("registries must not be empty")
	}

	var input []RegistryConfig
	if err := yaml.UnmarshalStrict([]byte(raw), &input); err != nil {
		return nil, fmt.Errorf("parse registries: %w", err)
	}
	if len(input) == 0 {
		return nil, fmt.Errorf("registries must contain at least one registry")
	}

	registries := make(map[RegistryKey]RegistryConfig, len(input))
	domainOwners := make(map[string]RegistryKey)
	for index, registry := range input {
		normalized, err := normalizeRegistry(registry)
		if err != nil {
			return nil, fmt.Errorf("registry at index %d: %w", index, err)
		}

		key := normalized.Key()
		if current, exists := registries[key]; exists {
			if current.AccessKeyID != normalized.AccessKeyID ||
				current.AccessKeySecret != normalized.AccessKeySecret {
				return nil, fmt.Errorf("registry %s has conflicting credentials", key)
			}
			normalized.Domains = sortedUnique(append(current.Domains, normalized.Domains...))
		}

		for _, domain := range normalized.Domains {
			if owner, exists := domainOwners[domain]; exists && owner != key {
				return nil, fmt.Errorf("domain %q is configured for both %s and %s", domain, owner, key)
			}
			domainOwners[domain] = key
		}
		registries[key] = normalized
	}

	return registries, nil
}

func normalizeRegistry(registry RegistryConfig) (RegistryConfig, error) {
	registry.RegionID = strings.TrimSpace(registry.RegionID)
	registry.InstanceID = strings.TrimSpace(registry.InstanceID)
	if registry.RegionID == "" {
		return RegistryConfig{}, fmt.Errorf("regionID must not be empty")
	}
	if registry.InstanceID == "" {
		return RegistryConfig{}, fmt.Errorf("instanceID must not be empty")
	}
	if strings.TrimSpace(registry.AccessKeyID) == "" {
		return RegistryConfig{}, fmt.Errorf("accessKeyID must not be empty")
	}
	if strings.TrimSpace(registry.AccessKeySecret) == "" {
		return RegistryConfig{}, fmt.Errorf("accessKeySecret must not be empty")
	}
	if registry.AccessKeyID != strings.TrimSpace(registry.AccessKeyID) {
		return RegistryConfig{}, fmt.Errorf("accessKeyID must not contain surrounding whitespace")
	}
	if registry.AccessKeySecret != strings.TrimSpace(registry.AccessKeySecret) {
		return RegistryConfig{}, fmt.Errorf("accessKeySecret must not contain surrounding whitespace")
	}
	if len(registry.Domains) == 0 {
		return RegistryConfig{}, fmt.Errorf("domains must contain at least one domain")
	}

	domains := make([]string, 0, len(registry.Domains))
	for _, rawDomain := range registry.Domains {
		domain := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(rawDomain)), ".")
		if domain == "" {
			return RegistryConfig{}, fmt.Errorf("domains contains an empty domain")
		}
		if strings.Contains(domain, "://") || strings.ContainsAny(domain, "/?#@:") {
			return RegistryConfig{}, fmt.Errorf("domain %q must not contain a scheme, port, path, query, fragment, or user information", domain)
		}
		if problems := validation.IsDNS1123Subdomain(domain); len(problems) > 0 {
			return RegistryConfig{}, fmt.Errorf("invalid domain %q: %s", domain, strings.Join(problems, "; "))
		}
		domains = append(domains, domain)
	}
	registry.Domains = sortedUnique(domains)

	return registry, nil
}

func sortedUnique(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}
