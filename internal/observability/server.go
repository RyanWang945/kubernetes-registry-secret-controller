package observability

import (
	"crypto/tls"
	"errors"
	"path/filepath"

	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// ServerOptions uses native authentication/authorization without a sidecar.
// Secure mode requires a provisioned certificate, never a silent self-signed
// fallback; controller-runtime watches these files for certificate rotation.
func ServerOptions(address string, secure bool, certDir string) (metricsserver.Options, error) {
	options := metricsserver.Options{BindAddress: address}
	if !secure || address == "0" {
		return options, nil
	}
	if certDir == "" {
		return options, errors.New("secure metrics requires --metrics-cert-dir containing tls.crt and tls.key")
	}
	if _, err := tls.LoadX509KeyPair(filepath.Join(certDir, "tls.crt"), filepath.Join(certDir, "tls.key")); err != nil {
		return options, errors.New("cannot load metrics TLS certificate and key")
	}
	options.SecureServing = true
	options.CertDir = certDir
	options.FilterProvider = filters.WithAuthenticationAndAuthorization
	return options, nil
}
