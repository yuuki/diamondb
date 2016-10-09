NAME=dynamond
COMMIT = $$(git describe --always)

all: build

deps:
	go get -d -t -v .

build: deps
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test:
	go test -v .

lint:
	go lint ./...

vet:
	go vet ./...

.PHONY: all deps build test lint vet
