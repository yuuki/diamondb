NAME=dynamond
COMMIT = $$(git describe --always)

all: build

deps:
	go get -d -t -v $(shell go list ./... | grep -v /vendor/)

build: deps
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test:
	go test -v $(shell go list ./... | grep -v /vendor/)

lint:
	go lint -v $(shell go list ./... | grep -v /vendor/)

vet:
	go vet -v $(shell go list ./... | grep -v /vendor/)

.PHONY: all deps build test lint vet
