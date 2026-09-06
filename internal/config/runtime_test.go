package config

import (
	"strings"
	"testing"
)

func TestRuntimeOverridesValidationAndBusinessChanges(t *testing.T) {
	data := validData("production", "default")
	baseline, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if baseline.Runtime != (RuntimeConfiguration{}) {
		t.Fatal("omitted values must remain overrides, not fixed defaults")
	}
	data[KubeAPIQPSKey], data[KubeAPIBurstKey], data[WorkersKey] = " 25.5 ", "50", "8"
	next, err := Parse(data)
	if err != nil {
		t.Fatal(err)
	}
	if next.Runtime != (RuntimeConfiguration{KubeAPIQPS: 25.5, KubeAPIBurst: 50, Workers: 8}) {
		t.Fatal("incorrect parsed overrides")
	}
	store := &Store{}
	store.Apply(baseline)
	result := store.Apply(next)
	if !result.Changed || result.BusinessChanged || result.Current.Generation != 2 {
		t.Fatal("tuning change was confused with business change")
	}
	loaded, _ := store.Load()
	if loaded.Runtime != next.Runtime {
		t.Fatal("runtime values were lost while cloning")
	}
	next.Namespaces = NameSelector{Names: []string{"staging"}}
	if !store.Apply(next).BusinessChanged {
		t.Fatal("business change was missed")
	}
	for key, invalid := range map[string][]string{
		KubeAPIQPSKey:   {"", "0", "-1", "NaN", "+Inf", "1e100", "1e-100", "sensitive-test-value"},
		KubeAPIBurstKey: {"", "0", "-1", "1.5", "999999999999999999999999", "sensitive-test-value"},
		WorkersKey:      {"", "0", "-1", "1.5", "sensitive-test-value"},
	} {
		for _, value := range invalid {
			t.Run(key+"/"+value, func(t *testing.T) {
				data := validData("production", "default")
				data[key] = value
				_, err := Parse(data)
				if err == nil || strings.Contains(err.Error(), "sensitive-test-value") {
					t.Fatalf("invalid value was accepted or exposed: %v", err)
				}
			})
		}
	}
}
