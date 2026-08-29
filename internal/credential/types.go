package credential

import (
	"context"
	"time"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// Request contains the minimum registry configuration required to obtain one
// temporary ACR authorization token. AccessKeySecret must never be logged.
type Request struct {
	Key             config.RegistryKey
	AccessKeyID     string
	AccessKeySecret string
}

// Token is the validated provider result before it is installed in Store.
type Token struct {
	Username  string
	Password  string
	ExpiresAt time.Time
}

// TokenProvider obtains one temporary credential for one RegistryKey.
type TokenProvider interface {
	GetAuthorizationToken(ctx context.Context, request Request) (Token, error)
}

// Credential is the current in-memory credential for one RegistryKey.
// RefreshedAt is assigned by Scheduler after the provider call succeeds.
type Credential struct {
	Username    string
	Password    string
	RefreshedAt time.Time
	ExpiresAt   time.Time
}

// Entry pairs a credential with a process-local revision and a private source
// fingerprint. Revision distinguishes refreshes at the same clock instant;
// the fingerprint prevents an old AK/SK credential from being treated as a
// result of the current Registry authentication configuration.
type Entry struct {
	Credential         Credential
	Revision           uint64
	authenticationHash string
}

// MatchesRegistry reports whether the credential was obtained with the
// Registry's current identity and AK/SK. Domains are intentionally excluded so
// a Domain-only change can reuse the current token.
func (e Entry) MatchesRegistry(registry config.RegistryConfig) bool {
	return e.authenticationHash != "" && e.authenticationHash == registryAuthenticationHash(registry)
}
