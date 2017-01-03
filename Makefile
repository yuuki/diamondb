NAME=diamondb
COMMIT = $$(git describe --always)

all: build

deps:
	go get -d -t -v $(shell go list ./... | grep -v /vendor/)

mock:
	mockgen -source vendor/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface/interface.go -destination lib/storage/dynamodb_mock.go -package storage

yacc:
	go tool yacc -o query/parse.go query/parse.go.y

build: deps
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test:
	go test -v $$(glide novendor)

fmt:
	gofmt -s -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

imports:
	goimports -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

lint:
	@for dir in $$(glide novendor); do golint $$dir; done

vet:
	go vet -v $$(glide novendor)

.PHONY: all deps build test lint vet
