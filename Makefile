.PHONY: build test test-race vet fmt-check verify

build:
	go build -o bin/spoold ./cmd/spoold

test:
	go test ./...

test-race:
	go test -race ./...

vet:
	go vet ./...

fmt-check:
	@test -z "$$(gofmt -l .)"

verify: fmt-check vet test-race build

