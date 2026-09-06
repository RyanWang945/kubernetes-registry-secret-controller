package config

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

const (
	KubeAPIQPSKey       = "kubeAPIQPS"
	KubeAPIBurstKey     = "kubeAPIBurst"
	WorkersKey          = "workers"
	DefaultKubeAPIQPS   = 25.0
	DefaultKubeAPIBurst = 50
	DefaultWorkers      = 8
)

// RuntimeConfiguration contains optional overrides. Zero means absent; an
// explicitly configured zero is rejected by Parse. Workers applies to each
// resource controller independently and only takes effect at process startup.
type RuntimeConfiguration struct {
	KubeAPIQPS   float64
	KubeAPIBurst int
	Workers      int
}

func ValidateQPS(qps float64) error {
	if qps <= 0 || qps > math.MaxFloat32 || math.IsNaN(qps) || math.IsInf(qps, 0) || float32(qps) == 0 {
		return fmt.Errorf("kube API QPS must be a finite positive value representable as float32")
	}
	return nil
}

func parseRuntime(data map[string]string) (RuntimeConfiguration, error) {
	var result RuntimeConfiguration
	if raw, present := data[KubeAPIQPSKey]; present {
		qps, err := strconv.ParseFloat(strings.TrimSpace(raw), 64)
		if err != nil || ValidateQPS(qps) != nil {
			return result, fmt.Errorf("invalid %s: expected a finite positive float32 value", KubeAPIQPSKey)
		}
		result.KubeAPIQPS = qps
	}
	for key, target := range map[string]*int{KubeAPIBurstKey: &result.KubeAPIBurst, WorkersKey: &result.Workers} {
		if raw, present := data[key]; present {
			value, err := strconv.Atoi(strings.TrimSpace(raw))
			if err != nil || value < 1 {
				return result, fmt.Errorf("invalid %s: expected a positive integer", key)
			}
			*target = value
		}
	}
	return result, nil
}
