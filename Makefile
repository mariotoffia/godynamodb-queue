lint:
	@golangci-lint run -v ./...
lint-fix:
	@golangci-lint run --fix
test: setenv
	@mkdir -p ${DIST}
	@go test -cover -coverprofile=${DIST}/test-coverage.out ./... -coverpkg ./...
	@go tool cover -func=${DIST}/test-coverage.out
	@go tool cover -html=${DIST}/test-coverage.out -o _dist/_output/test-coverage.html
dep:
	@go install github.com/mariotoffia/goasciidoc@latest
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
# Bug in golangci-lint for mac
ifeq ($(GOOS),darwin)
	@brew install diffutils
endif