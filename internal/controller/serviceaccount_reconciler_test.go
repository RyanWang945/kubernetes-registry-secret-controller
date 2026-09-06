package controller

import (
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/config"
	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/serviceaccount"
)

func TestServiceAccountDependencyBackoffIsBoundedAndIndependent(t *testing.T) {
	store := &config.Store{}
	store.Apply(config.ConfigurationSnapshot{})
	r := newServiceAccountReconciler(store, newRecordingSyncer(func(int, string) error {
		return serviceaccount.ErrManagedSecretNotReady
	}))
	// Share one reconciler across concurrent workers. Neither a different name
	// nor the same name in another namespace may inherit another SA's delay.
	for _, key := range []types.NamespacedName{
		{Namespace: "production", Name: "default"},
		{Namespace: "production", Name: "build"},
		{Namespace: "staging", Name: "default"},
	} {
		t.Run(key.String(), func(t *testing.T) {
			t.Parallel()
			for _, delay := range []time.Duration{100, 200, 400, 800, 1600, 3200, 5000, 5000} {
				result, err := r.Reconcile(testContext(), ctrl.Request{NamespacedName: key})
				if err != nil || result.RequeueAfter != delay*time.Millisecond {
					t.Fatalf("Reconcile(%s) = %+v, %v; want %s and no error", key, result, err, delay*time.Millisecond)
				}
			}
		})
	}
}

func TestServiceAccountDependencyBackoffResetsAfterSuccess(t *testing.T) {
	store := &config.Store{}
	store.Apply(config.ConfigurationSnapshot{})
	apiErr := errors.New("API unavailable")
	r := newServiceAccountReconciler(store, newRecordingSyncer(func(call int, _ string) error {
		switch call {
		case 3:
			return apiErr
		case 5:
			return nil
		default:
			return errors.Join(errors.New("Secret lookup"), serviceaccount.ErrManagedSecretNotReady)
		}
	}))
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "production", Name: "default"}}
	for call, wantMS := range []time.Duration{100, 200, 0, 400, 0, 100} {
		result, err := r.Reconcile(testContext(), request)
		if call == 2 {
			if !errors.Is(err, apiErr) || result.RequeueAfter != 0 {
				t.Fatalf("real API error must use normal error retry: result=%+v, error=%v", result, err)
			}
			continue
		}
		if err != nil || result.RequeueAfter != wantMS*time.Millisecond {
			t.Fatalf("call %d: result=%+v, error=%v; want %s", call+1, result, err, wantMS*time.Millisecond)
		}
		if call == 4 && r.dependencyBackoff.NumRequeues(request.NamespacedName) != 0 {
			t.Fatal("successful reconciliation retained dependency retry state")
		}
	}
}

func TestServiceAccountDependencyBackoffForgetsRemovedTargets(t *testing.T) {
	for _, mode := range []string{"deleted", "excluded"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			store := &config.Store{}
			snapshot, err := config.Parse(testConfigData("production", "default"))
			if err != nil {
				t.Fatal(err)
			}
			store.Apply(snapshot)
			account := &corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Namespace: "production", Name: "default"}}
			kube := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(account).Build()
			syncer, err := serviceaccount.NewSyncer(kube, store, DefaultManagedSecretName)
			if err != nil {
				t.Fatal(err)
			}
			r := newServiceAccountReconciler(store, syncer)
			request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: account.Namespace, Name: account.Name}}
			for range 2 {
				result, err := r.Reconcile(testContext(), request)
				if err != nil || result.RequeueAfter == 0 {
					t.Fatalf("expected dependency wait, got %+v, %v", result, err)
				}
			}
			if mode == "deleted" {
				if err := kube.Delete(testContext(), account); err != nil {
					t.Fatal(err)
				}
			} else {
				excluded, err := config.Parse(testConfigData("production", "other"))
				if err != nil {
					t.Fatal(err)
				}
				store.Apply(excluded)
			}
			result, err := r.Reconcile(testContext(), request)
			if err != nil || result.RequeueAfter != 0 || r.dependencyBackoff.NumRequeues(request.NamespacedName) != 0 {
				t.Fatalf("removed target retained dependency wait: result=%+v, error=%v", result, err)
			}
		})
	}
}
