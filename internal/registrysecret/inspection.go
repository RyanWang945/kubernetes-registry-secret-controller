package registrysecret

import (
	"encoding/json"
	"time"

	corev1 "k8s.io/api/core/v1"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// Inspection decodes envelopes once per cached Secret, and records independently
// per registry. It never mutates the object or verifies credentials with ACR.
type Inspection struct {
	secret *corev1.Secret
	auths  map[string]json.RawMessage
	states map[string]json.RawMessage
	valid  bool
}

func Inspect(secret *corev1.Secret) Inspection {
	i := Inspection{secret: secret}
	if secret == nil {
		return i
	}
	var docker struct {
		Auths map[string]json.RawMessage `json:"auths"`
	}
	var state struct {
		Version    int                        `json:"version"`
		Registries map[string]json.RawMessage `json:"registries"`
	}
	dockerErr := json.Unmarshal(secret.Data[corev1.DockerConfigJsonKey], &docker)
	stateErr := json.Unmarshal([]byte(secret.Annotations[StateAnnotationKey]), &state)
	i.auths, i.states = docker.Auths, state.Registries
	i.valid = secret.Type == corev1.SecretTypeDockerConfigJson && dockerErr == nil && stateErr == nil &&
		docker.Auths != nil && state.Registries != nil && state.Version == StateVersion
	return i
}

// RegistryStatus returns one bounded state and the observed expiration (zero
// when no structurally usable credential exists). expected may omit pending
// registries; a complete old credential is still pending, not invalid.
func (i Inspection) RegistryStatus(registry config.RegistryConfig, expected Inspection, now time.Time) (string, time.Time) {
	if i.secret == nil {
		return "pending", time.Time{}
	}
	if !IsManaged(i.secret) {
		return "conflict", time.Time{}
	}
	if i.secret.DeletionTimestamp != nil {
		return "pending", time.Time{}
	}
	if !i.valid {
		return "invalid", time.Time{}
	}
	rawState, hasState := i.states[registry.Key().String()]
	var state RegistryState
	if hasState && (json.Unmarshal(rawState, &state) != nil || state.RefreshedAt.IsZero() ||
		!state.ExpiresAt.After(state.RefreshedAt) || state.StateHash == "") {
		return "invalid", time.Time{}
	}
	var expectedState RegistryState
	rawExpected, hasExpected := expected.states[registry.Key().String()]
	_ = json.Unmarshal(rawExpected, &expectedState)
	matches := hasExpected && hasState && state.StateHash == expectedState.StateHash &&
		state.RefreshedAt.Equal(expectedState.RefreshedAt) && state.ExpiresAt.Equal(expectedState.ExpiresAt)
	foundAuth := false
	for _, domain := range registry.Domains {
		raw, exists := i.auths[domain]
		if !exists {
			matches = false
			continue
		}
		var auth, desired DockerAuth
		if json.Unmarshal(raw, &auth) != nil || auth.Username == "" || auth.Password == "" || auth.Auth != encodedAuth(auth.Username, auth.Password) {
			return "invalid", time.Time{}
		}
		foundAuth = true
		if !hasState {
			return "invalid", time.Time{}
		}
		if json.Unmarshal(expected.auths[domain], &desired) != nil || auth != desired {
			matches = false
		}
	}
	if !foundAuth {
		return "pending", time.Time{}
	}
	if !state.ExpiresAt.After(now) {
		return "expired", state.ExpiresAt
	}
	if matches {
		return "synced", state.ExpiresAt
	}
	return "pending", state.ExpiresAt
}
