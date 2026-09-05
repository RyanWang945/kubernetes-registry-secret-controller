package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/credential"
	"k8s.io/utils/clock"
	clocktesting "k8s.io/utils/clock/testing"
)

func TestMockTokenProviderRotatesAndExpires(t *testing.T) {
	now := time.Date(2026, time.September, 4, 10, 0, 0, 0, time.UTC)
	fakeClock := clocktesting.NewFakeClock(now)
	provider := newMockTokenProvider("pod-a", 30*time.Second, 0, fakeClock)
	request := credential.Request{Key: config.RegistryKey{RegionID: "test", InstanceID: "registry"}}

	first, err := provider.GetAuthorizationToken(context.Background(), request)
	if err != nil {
		t.Fatalf("first token: %v", err)
	}
	second, err := provider.GetAuthorizationToken(context.Background(), request)
	if err != nil {
		t.Fatalf("second token: %v", err)
	}

	if first.Username != "mock-user" || second.Username != "mock-user" {
		t.Fatalf("usernames = %q, %q", first.Username, second.Username)
	}
	if first.Password == second.Password || !strings.HasSuffix(first.Password, "-1") || !strings.HasSuffix(second.Password, "-2") {
		t.Fatalf("passwords did not rotate: %q, %q", first.Password, second.Password)
	}
	if want := now.Add(30 * time.Second); !first.ExpiresAt.Equal(want) {
		t.Fatalf("first expiry = %s, want %s", first.ExpiresAt, want)
	}
}

func TestMockTokenProviderFailsConfiguredInitialCalls(t *testing.T) {
	provider := newMockTokenProvider("pod-a", 30*time.Second, 1, clock.RealClock{})
	request := credential.Request{Key: config.RegistryKey{RegionID: "test", InstanceID: "registry"}}

	if _, err := provider.GetAuthorizationToken(context.Background(), request); err == nil {
		t.Fatal("first call succeeded, want intentional failure")
	}
	if _, err := provider.GetAuthorizationToken(context.Background(), request); err != nil {
		t.Fatalf("second call failed: %v", err)
	}
}
