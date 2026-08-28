package controller

import (
	"errors"
	"net/http"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
)

// ConfigurationReadyCheck reports ready after this process has accepted at
// least one valid configuration. Later invalid updates and deletion retain the
// last valid snapshot and therefore do not make a running replica unready.
func ConfigurationReadyCheck(store *config.Store) func(*http.Request) error {
	return func(_ *http.Request) error {
		if store == nil || !store.Loaded() {
			return errors.New("no valid controller configuration has been loaded")
		}
		return nil
	}
}
