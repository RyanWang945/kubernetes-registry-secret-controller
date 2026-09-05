package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	"github.com/RyanWang945/kubernetes-registry-secret-controller/internal/controller"
)

func readControllerProfile(
	ctx context.Context,
	clientset kubernetes.Interface,
) (controllerProfile, error) {
	deployment, err := clientset.AppsV1().Deployments(controllerNamespace).Get(
		ctx,
		controllerDeployment,
		metav1.GetOptions{},
	)
	if err != nil {
		return controllerProfile{}, fmt.Errorf("read performance controller profile: %w", err)
	}
	return profileFromDeployment(deployment, time.Now())
}

func profileFromDeployment(deployment *appsv1.Deployment, observedAt time.Time) (controllerProfile, error) {
	if deployment == nil {
		return controllerProfile{}, fmt.Errorf("performance Deployment must not be nil")
	}
	var args []string
	image := ""
	for index := range deployment.Spec.Template.Spec.Containers {
		container := &deployment.Spec.Template.Spec.Containers[index]
		if container.Name == "controller" {
			image = container.Image
			args = append([]string(nil), container.Args...)
			break
		}
	}
	if image == "" {
		return controllerProfile{}, fmt.Errorf("performance Deployment has no controller container")
	}

	namespaceConcurrency, err := integerArgument(
		args,
		"max-concurrent-namespace-reconciles",
		controller.DefaultMaxConcurrentNamespaceReconciles,
	)
	if err != nil {
		return controllerProfile{}, err
	}
	serviceAccountConcurrency, err := integerArgument(
		args,
		"max-concurrent-service-account-reconciles",
		controller.DefaultMaxConcurrentServiceAccountReconciles,
	)
	if err != nil {
		return controllerProfile{}, err
	}
	qps := argumentValue(args, "kube-api-qps", strconv.FormatFloat(float64(rest.DefaultQPS), 'f', -1, 32))
	burst := argumentValue(args, "kube-api-burst", strconv.Itoa(rest.DefaultBurst))

	return controllerProfile{
		Replicas:                           1,
		Image:                              image,
		Arguments:                          args,
		DeploymentGeneration:               deployment.Generation,
		ObservedAt:                         observedAt.UTC(),
		NamespaceReconcileConcurrency:      namespaceConcurrency,
		ServiceAccountReconcileConcurrency: serviceAccountConcurrency,
		RESTClientRateLimit:                fmt.Sprintf("configured QPS %s, burst %s", qps, burst),
		TokenTTL:                           argumentValue(args, "mock-token-ttl", "controller default"),
		RefreshBefore:                      argumentValue(args, "credential-refresh-before", "controller default"),
		ExpiryTrigger:                      "fake clock advanced 50m; real scheduler and Kubernetes writes",
	}, nil
}

func integerArgument(args []string, name string, fallback int) (int, error) {
	raw := argumentValue(args, name, strconv.Itoa(fallback))
	value, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("parse --%s=%q from performance Deployment: %w", name, raw, err)
	}
	return value, nil
}

func argumentValue(args []string, name, fallback string) string {
	prefix := "--" + name + "="
	flagName := "--" + name
	value := fallback
	for index := 0; index < len(args); index++ {
		if strings.HasPrefix(args[index], prefix) {
			value = strings.TrimPrefix(args[index], prefix)
			continue
		}
		if args[index] == flagName && index+1 < len(args) {
			value = args[index+1]
			index++
		}
	}
	return value
}
