NAME=dynamond
COMMIT = $$(git describe --always)

all: build

deps:
	go get -d -t -v $(shell go list ./... | grep -v /vendor/)

mock:
	go get github.com/golang/mock/gomock
	go get github.com/golang/mock/mockgen
	mockgen -source vendor/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface/interface.go -destination tsdb/dynamodbmock.go -package tsdb

yacc:
	go tool yacc -o query/parse.go query/parse.go.y

build: deps
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test:
	go test -v $(shell go list ./... | grep -v /vendor/)

lint:
	go lint -v $(shell go list ./... | grep -v /vendor/)

vet:
	go vet -v $(shell go list ./... | grep -v /vendor/)

.PHONY: all deps build test lint vet
