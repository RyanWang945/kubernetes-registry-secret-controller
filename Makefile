GOCACHE ?= $(CURDIR)/.cache/go-build

.PHONY: fmt test test-race vet test-integration test-metrics-rules verify

fmt:
	gofmt -w $$(find cmd internal -type f -name '*.go')

test:
	GOCACHE=$(GOCACHE) go test ./...

test-race:
	GOCACHE=$(GOCACHE) go test -race ./...

vet:
	GOCACHE=$(GOCACHE) go vet ./...

test-integration:
	ENVTEST_DOWNLOAD=true GOCACHE=$(GOCACHE) go test -tags=integration -count=1 ./internal/controller ./internal/observability

test-metrics-rules:
	promtool check rules deploy/monitoring/rules.yaml
	promtool test rules deploy/monitoring/rules.test.yaml

verify: fmt test test-race vet test-integration
