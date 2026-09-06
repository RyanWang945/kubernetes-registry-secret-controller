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
	hasAuth    bool
}

// HasAuth reports whether the rendered Docker config contains at least one
// registry credential. A target namespace must not receive an empty managed
// Secret while all configured registries are still pending.
func (c Content) HasAuth() bool {
	return c.hasAuth
}

// Build creates deterministic Docker auth and state documents from one atomic
// configuration snapshot and one credential snapshot. Existing is consulted
// only to retain credentials for configured RegistryKeys that are pending.
// A retention error may accompany usable Content: callers must still distribute
// that content, but must not delete an existing Secret when the result is empty.
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

	var retentionErrors []error
	if len(pending) > 0 && existing != nil {
		existingDocker, existingState, present, err := parseExisting(existing)
		if err != nil {
			retentionErrors = append(retentionErrors, err)
		}
		if present {
			for _, registry := range pending {
				if err := retainPending(&dockerConfig, &state, registry, existingDocker, existingState); err != nil {
					retentionErrors = append(retentionErrors, err)
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
	return Content{
		DockerJSON: dockerJSON,
		StateJSON:  string(stateJSON),
		hasAuth:    len(dockerConfig.Auths) > 0,
	}, errors.Join(retentionErrors...)
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
	// Decode records independently: a malformed entry must not hide intact
	// credentials belonging to other registries. Never return decoder errors,
	// which can embed sensitive values (for example an invalid timestamp).
	var problems []error
	dockerConfig := DockerConfig{Auths: make(map[string]DockerAuth)}
	var rawDocker struct {
		Auths map[string]json.RawMessage `json:"auths"`
	}
	if err := json.Unmarshal(dockerJSON, &rawDocker); err != nil || rawDocker.Auths == nil {
		problems = append(problems, errors.New("existing Docker config is invalid or missing"))
	} else {
		for domain, raw := range rawDocker.Auths {
			var auth DockerAuth
			if err := json.Unmarshal(raw, &auth); err != nil {
				problems = append(problems, errors.New("existing Docker config contains an invalid auth entry"))
				continue
			}
			dockerConfig.Auths[domain] = auth
		}
	}
	state := PersistedState{Version: StateVersion, Registries: make(map[string]RegistryState)}
	var rawState struct {
		Version    int                        `json:"version"`
		Registries map[string]json.RawMessage `json:"registries"`
	}
	if err := json.Unmarshal([]byte(stateJSON), &rawState); err != nil || rawState.Version != StateVersion || rawState.Registries == nil {
		problems = append(problems, errors.New("existing registry state is invalid, unsupported or missing"))
	} else {
		for key, raw := range rawState.Registries {
			var value RegistryState
			if err := json.Unmarshal(raw, &value); err != nil {
				problems = append(problems, errors.New("existing registry state contains an invalid entry"))
				continue
			}
			state.Registries[key] = value
		}
	}
	return dockerConfig, state, true, errors.Join(problems...)
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
	if registryState.RefreshedAt.IsZero() || !registryState.ExpiresAt.After(registryState.RefreshedAt) || registryState.StateHash == "" {
		return fmt.Errorf("pending registry %s has incomplete registry state", registry.Key())
	}

	var problems []error
	retained := false
	for _, domain := range registry.Domains {
		auth, found := existingDocker.Auths[domain]
		if !found {
			// A newly added domain has no old credential to retain. Wait for
			// acquisition without blocking other domains or other registries.
			continue
		}
		if auth.Username == "" || auth.Password == "" || auth.Auth != encodedAuth(auth.Username, auth.Password) {
			problems = append(problems, fmt.Errorf("pending registry %s has invalid existing auth for domain %q", registry.Key(), domain))
			continue
		}
		dockerConfig.Auths[domain] = auth
		retained = true
	}
	if retained {
		state.Registries[registry.Key().String()] = registryState
	}
	return errors.Join(problems...)
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
