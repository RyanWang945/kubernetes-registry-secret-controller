package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

const (
	clockControlReadHeaderTimeout = 5 * time.Second
	clockControlShutdownTimeout   = 5 * time.Second
	maximumClockAdvance           = 7 * 24 * time.Hour
)

type mockClockServer struct {
	address string
	clock   *clocktesting.FakeClock
}

func newMockClockServer(address string, fakeClock *clocktesting.FakeClock) *mockClockServer {
	return &mockClockServer{address: address, clock: fakeClock}
}

func (s *mockClockServer) Start(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.address,
		Handler:           s.handler(),
		ReadHeaderTimeout: clockControlReadHeaderTimeout,
	}
	errCh := make(chan error, 1)
	go func() {
		errCh <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), clockControlShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down mock clock server: %w", err)
		}
		return nil
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("serve mock clock control endpoint: %w", err)
	}
}

func (s *mockClockServer) NeedLeaderElection() bool {
	return false
}

func (s *mockClockServer) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /advance", func(response http.ResponseWriter, request *http.Request) {
		duration, err := time.ParseDuration(request.URL.Query().Get("duration"))
		if err != nil || duration <= 0 || duration > maximumClockAdvance {
			http.Error(response, "duration must be a positive Go duration no greater than 168h", http.StatusBadRequest)
			return
		}

		before := s.clock.Now()
		s.clock.Step(duration)
		response.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(response).Encode(map[string]any{
			"advancedBy": duration.String(),
			"before":     before,
			"after":      s.clock.Now(),
		}); err != nil {
			http.Error(response, "encode response", http.StatusInternalServerError)
		}
	})
	return mux
}
