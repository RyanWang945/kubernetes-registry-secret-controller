package main

import (
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestProfileFromDeploymentReadsActualArguments(t *testing.T) {
	observedAt := time.Date(2026, time.September, 4, 17, 0, 0, 0, time.FixedZone("CST", 8*60*60))
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Generation: 7},
		Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:  "controller",
				Image: "controller:test",
				Args: []string{
					"--kube-api-qps=25",
					"--kube-api-burst", "50",
					"--max-concurrent-namespace-reconciles=8",
					"--max-concurrent-service-account-reconciles=12",
					"--mock-token-ttl=1h",
					"--credential-refresh-before=10m",
				},
			}}},
		}},
	}
	profile, err := profileFromDeployment(deployment, observedAt)
	if err != nil {
		t.Fatalf("profileFromDeployment() error = %v", err)
	}
	if profile.Image != "controller:test" || profile.DeploymentGeneration != 7 {
		t.Fatalf("unexpected profile identity: %+v", profile)
	}
	if profile.NamespaceReconcileConcurrency != 8 || profile.ServiceAccountReconcileConcurrency != 12 {
		t.Fatalf("unexpected concurrency: %+v", profile)
	}
	if profile.RESTClientRateLimit != "configured QPS 25, burst 50" {
		t.Fatalf("rate limit = %q", profile.RESTClientRateLimit)
	}
	if !profile.ObservedAt.Equal(observedAt) || profile.ObservedAt.Location() != time.UTC {
		t.Fatalf("observed at = %v, want UTC %v", profile.ObservedAt, observedAt.UTC())
	}
}

func TestProfileFromDeploymentRejectsInvalidConcurrency(t *testing.T) {
	deployment := &appsv1.Deployment{Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name:  "controller",
			Image: "controller:test",
			Args:  []string{"--max-concurrent-namespace-reconciles=not-a-number"},
		}}},
	}}}
	if _, err := profileFromDeployment(deployment, time.Now()); err == nil {
		t.Fatal("invalid concurrency was accepted")
	}
}
