package registrysecret

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
)

const (
	StateVersion       = 1
	StateAnnotationKey = "registry-secret-controller.io/state"

	ApplicationNameLabelKey = "app.kubernetes.io/name"
	ManagedByLabelKey       = "app.kubernetes.io/managed-by"
	ControllerIdentity      = "kubernetes-registry-secret-controller"
)

type DockerConfig struct {
	Auths map[string]DockerAuth `json:"auths"`
}

type DockerAuth struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Auth     string `json:"auth"`
}

type PersistedState struct {
	Version    int                      `json:"version"`
	Registries map[string]RegistryState `json:"registries"`
}

type RegistryState struct {
	RefreshedAt time.Time `json:"refreshedAt"`
	ExpiresAt   time.Time `json:"expiresAt"`
	StateHash   string    `json:"stateHash"`
}

// Content is the complete desired Secret payload. Callers write DockerJSON and
// StateJSON in one Kubernetes Create or Update operation.
type Content struct {
	DockerJSON []byte
	StateJSON  string
}

// Build creates deterministic Docker auth and state documents from one atomic
// configuration snapshot and one credential snapshot. Existing is consulted
// only to retain credentials for configured RegistryKeys that are pending.
func Build(
	snapshot config.ConfigurationSnapshot,
	credentials map[config.RegistryKey]credential.Entry,
	existing *corev1.Secret,
) (Content, error) {
	dockerConfig := DockerConfig{Auths: make(map[string]DockerAuth)}
	state := PersistedState{
		Version:    StateVersion,
		Registries: make(map[string]RegistryState),
	}

	pending := make([]config.RegistryConfig, 0)
	for key, registry := range snapshot.Registries {
		entry, found := credentials[key]
		if !found || !entry.MatchesRegistry(registry) {
			pending = append(pending, registry)
			continue
		}
		addCredential(&dockerConfig, &state, registry, entry.Credential)
	}

	if len(pending) > 0 && existing != nil {
		existingDocker, existingState, present, err := parseExisting(existing)
		if err != nil {
			return Content{}, err
		}
		if present {
			for _, registry := range pending {
				if err := retainPending(&dockerConfig, &state, registry, existingDocker, existingState); err != nil {
					return Content{}, err
				}
			}
		}
	}

	dockerJSON, err := json.Marshal(dockerConfig)
	if err != nil {
		return Content{}, fmt.Errorf("marshal Docker config: %w", err)
	}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return Content{}, fmt.Errorf("marshal registry state: %w", err)
	}
	return Content{DockerJSON: dockerJSON, StateJSON: string(stateJSON)}, nil
}

func addCredential(
	dockerConfig *DockerConfig,
	state *PersistedState,
	registry config.RegistryConfig,
	value credential.Credential,
) {
	auth := DockerAuth{
		Username: value.Username,
		Password: value.Password,
		Auth:     encodedAuth(value.Username, value.Password),
	}
	for _, domain := range registry.Domains {
		dockerConfig.Auths[domain] = auth
	}
	state.Registries[registry.Key().String()] = RegistryState{
		RefreshedAt: value.RefreshedAt.UTC(),
		ExpiresAt:   value.ExpiresAt.UTC(),
		StateHash:   stateHash(registry, value),
	}
}

func parseExisting(secret *corev1.Secret) (DockerConfig, PersistedState, bool, error) {
	dockerJSON, hasDocker := secret.Data[corev1.DockerConfigJsonKey]
	stateJSON, hasState := secret.Annotations[StateAnnotationKey]
	if !hasDocker && !hasState {
		return DockerConfig{}, PersistedState{}, false, nil
	}
	if !hasDocker || !hasState {
		return DockerConfig{}, PersistedState{}, false, errors.New("existing managed Secret has incomplete Docker config or registry state")
	}

	var dockerConfig DockerConfig
	if err := json.Unmarshal(dockerJSON, &dockerConfig); err != nil {
		return DockerConfig{}, PersistedState{}, false, fmt.Errorf("parse existing Docker config: %w", err)
	}
	if dockerConfig.Auths == nil {
		return DockerConfig{}, PersistedState{}, false, errors.New("existing Docker config has no auths object")
	}

	var state PersistedState
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		return DockerConfig{}, PersistedState{}, false, fmt.Errorf("parse existing registry state: %w", err)
	}
	if state.Version != StateVersion {
		return DockerConfig{}, PersistedState{}, false, fmt.Errorf("existing registry state version %d is unsupported", state.Version)
	}
	if state.Registries == nil {
		return DockerConfig{}, PersistedState{}, false, errors.New("existing registry state has no registries object")
	}
	return dockerConfig, state, true, nil
}

func retainPending(
	dockerConfig *DockerConfig,
	state *PersistedState,
	registry config.RegistryConfig,
	existingDocker DockerConfig,
	existingState PersistedState,
) error {
	registryState, hasState := existingState.Registries[registry.Key().String()]
	hasAnyAuth := false
	for _, domain := range registry.Domains {
		if _, found := existingDocker.Auths[domain]; found {
			hasAnyAuth = true
			break
		}
	}

	// A newly configured Registry has neither old State nor old Auth and remains
	// absent until its first credential is available.
	if !hasState && !hasAnyAuth {
		return nil
	}
	if !hasState {
		return fmt.Errorf("pending registry %s has Docker auth without registry state", registry.Key())
	}
	if registryState.RefreshedAt.IsZero() || registryState.ExpiresAt.IsZero() || registryState.StateHash == "" {
		return fmt.Errorf("pending registry %s has incomplete registry state", registry.Key())
	}

	for _, domain := range registry.Domains {
		auth, found := existingDocker.Auths[domain]
		if !found {
			return fmt.Errorf("pending registry %s is missing existing auth for domain %q", registry.Key(), domain)
		}
		if auth.Username == "" || auth.Password == "" || auth.Auth != encodedAuth(auth.Username, auth.Password) {
			return fmt.Errorf("pending registry %s has invalid existing auth for domain %q", registry.Key(), domain)
		}
		dockerConfig.Auths[domain] = auth
	}
	state.Registries[registry.Key().String()] = registryState
	return nil
}

func stateHash(registry config.RegistryConfig, value credential.Credential) string {
	input := struct {
		RegionID        string   `json:"regionID"`
		InstanceID      string   `json:"instanceID"`
		AccessKeyID     string   `json:"accessKeyID"`
		AccessKeySecret string   `json:"accessKeySecret"`
		Domains         []string `json:"domains"`
		Username        string   `json:"username"`
		Password        string   `json:"password"`
		RefreshedAt     string   `json:"refreshedAt"`
		ExpiresAt       string   `json:"expiresAt"`
	}{
		RegionID:        registry.RegionID,
		InstanceID:      registry.InstanceID,
		AccessKeyID:     registry.AccessKeyID,
		AccessKeySecret: registry.AccessKeySecret,
		Domains:         append([]string(nil), registry.Domains...),
		Username:        value.Username,
		Password:        value.Password,
		RefreshedAt:     value.RefreshedAt.UTC().Format(time.RFC3339Nano),
		ExpiresAt:       value.ExpiresAt.UTC().Format(time.RFC3339Nano),
	}
	canonical, err := json.Marshal(input)
	if err != nil {
		panic("marshal fixed state hash input: " + err.Error())
	}
	digest := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func encodedAuth(username, password string) string {
	return base64.StdEncoding.EncodeToString([]byte(username + ":" + password))
}
