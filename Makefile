.PHONY: test test-coverage build custom-columns

# build stamps the binary with the most recent release tag (no tags yet →
# the binary reports "(devel)", same as a plain go build/install).
build:
	go build -ldflags "-X github.com/ozzono/daedalus/internal/version.ldflagsVersion=$$(git describe --tags --abbrev=0 2>/dev/null)" ./cmd/daedalus

test:
	go test ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@echo "coverage.out written; run 'go tool cover -html=coverage.out' for a browser view"

# custom-columns registers the visibility search attributes the pipeline
# upserts (internal/workflows visibilityChangeID: DaedalusStatus keyword,
# LastActivityAt datetime, DaedalusDependsOn keyword; dependencyWakeupChangeID:
# DaedalusStartedAt datetime) on the default namespace. The list-guard makes
# the rule idempotent — an already-registered attribute is left alone rather
# than erroring, so repeated calls are safe.
custom-columns:
	@temporal operator search-attribute list --namespace default | grep -qw DaedalusStatus || temporal operator search-attribute create --namespace default --name DaedalusStatus --type Keyword
	@temporal operator search-attribute list --namespace default | grep -qw LastActivityAt || temporal operator search-attribute create --namespace default --name LastActivityAt --type Datetime
	@temporal operator search-attribute list --namespace default | grep -qw DaedalusDependsOn || temporal operator search-attribute create --namespace default --name DaedalusDependsOn --type Keyword
	@temporal operator search-attribute list --namespace default | grep -qw DaedalusStartedAt || temporal operator search-attribute create --namespace default --name DaedalusStartedAt --type Datetime
