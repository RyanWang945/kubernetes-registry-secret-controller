package main

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func TestVerifyFinalStateIgnoresInjectedResources(t *testing.T) {
	const runID = "verify"
	clientset := fake.NewSimpleClientset(
		testSecret("ns-a", "current"),
		testSecret("ns-late", "current"),
		labeledAccount(runID, "ns-a", "default"),
		labeledAccount(runID, "ns-a", "workload"),
		labeledAccount(runID, "ns-a", serviceAccountLate),
		labeledAccount(runID, "ns-late", "default"),
		labeledAccount(runID, "ns-late", "workload"),
	)
	if err := verifyFinalState(
		context.Background(),
		clientset,
		[]string{"ns-a"},
		performanceRunLabel+"="+runID,
	); err != nil {
		t.Fatalf("verifyFinalState() error = %v", err)
	}
}

func labeledAccount(runID, namespace, name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: namespace,
			Name:      name,
			Labels:    map[string]string{performanceRunLabel: runID},
		},
		ImagePullSecrets: []corev1.LocalObjectReference{{Name: "auto-patch-secret"}},
	}
}
