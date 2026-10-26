SHELL := /bin/sh

.PHONY: fmt check
fmt:
	gofmt -w .
check:
	GOWORK=off go vet ./...
	GOWORK=off go test -race ./...
