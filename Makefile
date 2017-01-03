NAME=diamondb
COMMIT = $$(git describe --always)

all: build

deps:
	go get -d -t -v $(shell go list ./... | grep -v /vendor/)

mock:
	go get github.com/golang/mock/gomock
	go get github.com/golang/mock/mockgen
	mockgen -source vendor/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface/interface.go -destination lib/storage/dynamodb_mock.go -package storage

yacc:
	go tool yacc -o query/parse.go query/parse.go.y

build: deps
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test:
	go test -v $(shell go list ./... | grep -v /vendor/)

fmt:
	gofmt -s -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

imports:
	goimports -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

lint:
	golint -v $(shell go list ./... | grep -v /vendor/)

vet:
	go vet -v $(shell go list ./... | grep -v /vendor/)

.PHONY: all deps build test lint vet
