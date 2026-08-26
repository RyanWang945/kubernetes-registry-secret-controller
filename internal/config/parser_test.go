package config

import (
	"reflect"
	"strings"
	"testing"
)

const validRegistries = `
- regionID: cn-hangzhou
  instanceID: cri-aaaaaaaa
  accessKeyID: key-a
  accessKeySecret: secret-a
  domains:
    - Registry-A.CN-HANGZHOU.CR.ALIYUNCS.COM.
`

func TestParserNormalizesSelectorsAndRegistries(t *testing.T) {
	t.Parallel()

	parser := mustParser(t)
	snapshot, err := parser.Parse(map[string]string{
		NamespaceKey:        " staging,production,staging ",
		ExcludeNamespaceKey: " excluded-b,excluded-a,excluded-b ",
		ServiceAccountKey:   "build, default,build",
		RegistriesKey: `
- regionID: cn-hangzhou
  instanceID: cri-aaaaaaaa
  accessKeyID: key-a
  accessKeySecret: secret-a
  domains:
    - registry-b.cn-hangzhou.cr.aliyuncs.com
    - Registry-A.CN-HANGZHOU.CR.ALIYUNCS.COM.
- regionID: cn-hangzhou
  instanceID: cri-aaaaaaaa
  accessKeyID: key-a
  accessKeySecret: secret-a
  domains:
    - registry-a.cn-hangzhou.cr.aliyuncs.com
`,
	})
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	if got, want := snapshot.Namespaces.Names, []string{"production", "staging"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("namespace names = %v, want %v", got, want)
	}
	if got, want := snapshot.ServiceAccounts.Names, []string{"build", "default"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("service account names = %v, want %v", got, want)
	}
	if got, want := snapshot.ExcludedNamespaces, []string{"excluded-a", "excluded-b"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("excluded namespace names = %v, want %v", got, want)
	}

	key := RegistryKey{RegionID: "cn-hangzhou", InstanceID: "cri-aaaaaaaa"}
	registry, exists := snapshot.Registries[key]
	if !exists {
		t.Fatalf("normalized registry %s not found", key)
	}
	wantDomains := []string{
		"registry-a.cn-hangzhou.cr.aliyuncs.com",
		"registry-b.cn-hangzhou.cr.aliyuncs.com",
	}
	if !reflect.DeepEqual(registry.Domains, wantDomains) {
		t.Fatalf("domains = %v, want %v", registry.Domains, wantDomains)
	}
}

func TestParserWildcardAndConfiguredNamespaceExclusions(t *testing.T) {
	t.Parallel()

	parser := mustParser(t)
	data := validData(Wildcard, Wildcard)
	data[ExcludeNamespaceKey] = "kube-system,excluded"
	snapshot, err := parser.Parse(data)
	if err != nil {
		t.Fatalf("Parse() error = %v", err)
	}

	for _, namespace := range []string{"kube-system", "excluded"} {
		if snapshot.MatchesNamespace(namespace) {
			t.Errorf("MatchesNamespace(%q) = true, want false", namespace)
		}
	}
	if !snapshot.MatchesNamespace("production") {
		t.Error("MatchesNamespace(production) = false, want true")
	}
	if !snapshot.ServiceAccounts.Matches("any-valid-name") {
		t.Error("ServiceAccounts.Matches(any-valid-name) = false, want true")
	}

	withoutExclusions, err := parser.Parse(validData(Wildcard, Wildcard))
	if err != nil {
		t.Fatalf("Parse() without exclusions error = %v", err)
	}
	if !withoutExclusions.MatchesNamespace("kube-system") {
		t.Error("MatchesNamespace(kube-system) = false without excludeNamespace, want true")
	}

	namedData := validData("production,staging", "default")
	namedData[ExcludeNamespaceKey] = "production"
	named, err := parser.Parse(namedData)
	if err != nil {
		t.Fatalf("Parse() named selector with exclusion error = %v", err)
	}
	if named.MatchesNamespace("production") || !named.MatchesNamespace("staging") {
		t.Errorf("named MatchesNamespace(): production = %v, staging = %v; exclusion must take precedence", named.MatchesNamespace("production"), named.MatchesNamespace("staging"))
	}
}

func TestParserRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		data    map[string]string
		wantErr string
	}{
		"empty namespace": {
			data:    validData("", "default"),
			wantErr: "namespace must not be empty",
		},
		"wildcard mixed with a name": {
			data:    validData("*,production", "default"),
			wantErr: "wildcard \"*\" cannot be combined",
		},
		"empty list item": {
			data:    validData("production,,staging", "default"),
			wantErr: "empty name",
		},
		"invalid service account": {
			data:    validData("production", "Not_Valid"),
			wantErr: "invalid serviceaccount name",
		},
		"wildcard excluded namespace": {
			data: func() map[string]string {
				data := validData("production", "default")
				data[ExcludeNamespaceKey] = Wildcard
				return data
			}(),
			wantErr: "invalid excludeNamespace name",
		},
		"unknown registry field": {
			data: map[string]string{
				NamespaceKey:      "production",
				ServiceAccountKey: "default",
				RegistriesKey: strings.Replace(
					validRegistries,
					"  domains:",
					"  unknown: value\n  domains:",
					1,
				),
			},
			wantErr: "unknown field",
		},
		"unknown ConfigMap data key": {
			data: map[string]string{
				NamespaceKey:      "production",
				ServiceAccountKey: "default",
				RegistriesKey:     validRegistries,
				"namespaces":      "misspelled",
			},
			wantErr: "unsupported ConfigMap data key",
		},
		"domain contains scheme": {
			data: map[string]string{
				NamespaceKey:      "production",
				ServiceAccountKey: "default",
				RegistriesKey: strings.Replace(
					validRegistries,
					"Registry-A.CN-HANGZHOU.CR.ALIYUNCS.COM.",
					"https://registry-a.cn-hangzhou.cr.aliyuncs.com",
					1,
				),
			},
			wantErr: "must not contain a scheme",
		},
	}

	parser := mustParser(t)
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parser.Parse(test.data)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Parse() error = %v, want an error containing %q", err, test.wantErr)
			}
		})
	}
}

func TestParserRejectsCredentialAndDomainConflicts(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		registries string
		wantErr    string
	}{
		"same key different credentials": {
			registries: `
- regionID: cn-hangzhou
  instanceID: cri-a
  accessKeyID: key-a
  accessKeySecret: secret-a
  domains: [a.example.com]
- regionID: cn-hangzhou
  instanceID: cri-a
  accessKeyID: key-b
  accessKeySecret: secret-b
  domains: [b.example.com]
`,
			wantErr: "conflicting credentials",
		},
		"domain owned by two keys": {
			registries: `
- regionID: cn-hangzhou
  instanceID: cri-a
  accessKeyID: key-a
  accessKeySecret: secret-a
  domains: [shared.example.com]
- regionID: cn-shanghai
  instanceID: cri-b
  accessKeyID: key-b
  accessKeySecret: secret-b
  domains: [SHARED.EXAMPLE.COM.]
`,
			wantErr: "configured for both",
		},
	}

	parser := mustParser(t)
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := parser.Parse(map[string]string{
				NamespaceKey:      "production",
				ServiceAccountKey: "default",
				RegistriesKey:     test.registries,
			})
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("Parse() error = %v, want an error containing %q", err, test.wantErr)
			}
		})
	}
}

func TestParserOrderDoesNotAffectConfigurationSnapshot(t *testing.T) {
	t.Parallel()

	parser := mustParser(t)
	first, err := parser.Parse(map[string]string{
		NamespaceKey:        "production,staging",
		ExcludeNamespaceKey: "excluded-b,excluded-a",
		ServiceAccountKey:   "default,build",
		RegistriesKey: `
- regionID: cn-hangzhou
  instanceID: cri-a
  accessKeyID: key-a
  accessKeySecret: secret-a
  domains: [b.example.com, a.example.com]
- regionID: cn-shanghai
  instanceID: cri-b
  accessKeyID: key-b
  accessKeySecret: secret-b
  domains: [c.example.com]
`,
	})
	if err != nil {
		t.Fatalf("first Parse() error = %v", err)
	}

	second, err := parser.Parse(map[string]string{
		NamespaceKey:        "staging,production",
		ExcludeNamespaceKey: "excluded-a,excluded-b",
		ServiceAccountKey:   "build,default",
		RegistriesKey: `
- regionID: cn-shanghai
  instanceID: cri-b
  accessKeyID: key-b
  accessKeySecret: secret-b
  domains: [c.example.com]
- regionID: cn-hangzhou
  instanceID: cri-a
  accessKeyID: key-a
  accessKeySecret: secret-a
  domains: [a.example.com, b.example.com]
`,
	})
	if err != nil {
		t.Fatalf("second Parse() error = %v", err)
	}

	if !first.Equal(second) {
		t.Fatal("semantically identical configurations are not equal")
	}
}

func mustParser(t *testing.T) *Parser {
	t.Helper()
	return NewParser()
}

func validData(namespace, serviceAccount string) map[string]string {
	return map[string]string{
		NamespaceKey:      namespace,
		ServiceAccountKey: serviceAccount,
		RegistriesKey:     validRegistries,
	}
}
