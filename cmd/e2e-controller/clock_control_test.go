package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	clocktesting "k8s.io/utils/clock/testing"
)

func TestMockClockServerAdvancesClock(t *testing.T) {
	start := time.Date(2026, time.September, 4, 10, 0, 0, 0, time.UTC)
	fakeClock := clocktesting.NewFakeClock(start)
	server := newMockClockServer(":0", fakeClock)
	request := httptest.NewRequest(http.MethodPost, "/advance?duration=50m", nil)
	response := httptest.NewRecorder()

	server.handler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	if want := start.Add(50 * time.Minute); !fakeClock.Now().Equal(want) {
		t.Fatalf("clock = %s, want %s", fakeClock.Now(), want)
	}
}

func TestMockClockServerRejectsInvalidAdvance(t *testing.T) {
	fakeClock := clocktesting.NewFakeClock(time.Now())
	server := newMockClockServer(":0", fakeClock)
	request := httptest.NewRequest(http.MethodPost, "/advance?duration=0s", nil)
	response := httptest.NewRecorder()

	server.handler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", response.Code, http.StatusBadRequest)
	}
}
