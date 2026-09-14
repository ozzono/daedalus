.PHONY: test test-coverage

test:
	go test ./...

test-coverage:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out
	@echo "coverage.out written; run 'go tool cover -html=coverage.out' for a browser view"
