package observability

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"sigs.k8s.io/yaml"
)

// Both installation modes must ship the exact rules validated by promtool.
func TestOperatorAndStandaloneRulesAgree(t *testing.T) {
	read := func(name string) map[string]any {
		t.Helper()
		data, err := os.ReadFile(filepath.Join("..", "..", "deploy", "monitoring", name))
		if err != nil {
			t.Fatal(err)
		}
		var result map[string]any
		if err := yaml.UnmarshalStrict(data, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if !reflect.DeepEqual(read("rules.yaml"), read("prometheusrule.yaml")["spec"]) {
		t.Fatal("PrometheusRule spec must match the promtool-tested rules.yaml")
	}
}
