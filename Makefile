DIST=./_dist

.PHONY: setenv lint lint-fix test test-ci benchmark dep clean build help

setenv:
	@mkdir -p $(DIST)
	@mkdir -p $(DIST)/_output

build:
	@echo "Building..."
	@go build ./...

lint:
	@golangci-lint run -v ./...

lint-fix:
	@golangci-lint run --fix

# Unit tests only (no Docker required, skips integration tests)
test: setenv
	@echo "Running unit tests (skipping integration tests)..."
	@go test -v -count=1 -p=1 -skip "Local|Simulation|Benchmark" -cover -coverprofile=${DIST}/test-coverage.out ./... -coverpkg ./...
	@go tool cover -func=${DIST}/test-coverage.out
	@go tool cover -html=${DIST}/test-coverage.out -o ${DIST}/_output/test-coverage.html

# Integration tests - run from CI/CD
test-ci: test
# Full tests including integration tests (requires Docker)
test-full: setenv
	@echo "Running all tests including integration tests (requires Docker)..."
	@go test -v -count=1 -p=1 -timeout=60m -cover -coverprofile=${DIST}/test-coverage.out ./... -coverpkg ./...
	@go tool cover -func=${DIST}/test-coverage.out
	@go tool cover -html=${DIST}/test-coverage.out -o ${DIST}/_output/test-coverage.html

# Benchmarks with memory allocation, CPU profiling, and race detection
benchmark: setenv
	@echo "Running benchmarks with race detection..."
	@go test -v -race -run=^$$ -bench=. -benchmem -benchtime=3s \
		-cpuprofile=${DIST}/cpu.prof \
		-memprofile=${DIST}/mem.prof \
		-trace=${DIST}/trace.out \
		./... 2>&1 | tee ${DIST}/benchmark.txt
	@echo ""
	@echo "Benchmark results saved to ${DIST}/benchmark.txt"
	@echo "CPU profile: ${DIST}/cpu.prof (view with: go tool pprof ${DIST}/cpu.prof)"
	@echo "Memory profile: ${DIST}/mem.prof (view with: go tool pprof ${DIST}/mem.prof)"
	@echo "Trace: ${DIST}/trace.out (view with: go tool trace ${DIST}/trace.out)"

dep:
	@go install github.com/mariotoffia/goasciidoc@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
ifeq ($(GOOS),darwin)
	@brew install diffutils
endif

clean:
	@rm -rf $(DIST)
	@echo "Cleaned build artifacts"

help:
	@echo "Available targets:"
	@echo "  build     - Build all packages"
	@echo "  test      - Run unit tests only (no Docker required)"
	@echo "  test-ci   - Run all tests including integration (requires Docker)"
	@echo "  benchmark - Run benchmarks with memory, CPU profiling, and race detection"
	@echo "  lint      - Run golangci-lint"
	@echo "  lint-fix  - Run golangci-lint with auto-fix"
	@echo "  dep       - Install development dependencies"
	@echo "  clean     - Remove build artifacts"
