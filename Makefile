DIST=./_dist

setenv:
	@mkdir -p $(DIST)
lint:
	@golangci-lint run -v ./...
lint-fix:
	@golangci-lint run --fix
test: setenv
	@echo "Running unit tests..."
	@go test -cover -coverprofile=${DIST}/test-coverage.out ./... -coverpkg ./...
	@go tool cover -func=${DIST}/test-coverage.out
	@go tool cover -html=${DIST}/test-coverage.out -o _dist/_output/test-coverage.html
test-long-running: setenv
	@echo "Running long unit tests..."
	@LONG_UNIT_TEST=true go test -cover -coverprofile=${DIST}/test-coverage.out ./... -coverpkg ./...
	@go tool cover -func=${DIST}/test-coverage.out
	@go tool cover -html=${DIST}/test-coverage.out -o _dist/_output/test-coverage.html
dep:
	@go install github.com/mariotoffia/goasciidoc@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
# Bug in golangci-lint for mac
ifeq ($(GOOS),darwin)
	@brew install diffutils
endif