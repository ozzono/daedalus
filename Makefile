.PHONY: test test-coverage build

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

tasks:
	@tree -f -i /tmp/daedalus/daedalus/instructions|grep md
