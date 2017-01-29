NAME=diamondb
COMMIT = $$(git describe --always)

all: build

deps:
	glide install
	go get github.com/golang/mock/mockgen

build: yacc mock
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test: yacc mock
	go test -race -v $$(glide novendor)

cover: yacc mock
	go test -cover $$(glide novendor)

mock:
	mockgen -source vendor/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface/interface.go -destination lib/storage/dynamo/dynamodb_mock.go -package dynamo

yacc:
	go tool yacc -o lib/query/parser.go lib/query/parser.go.y

fmt:
	gofmt -s -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

imports:
	goimports -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

lint:
	@for dir in $$(glide novendor); do golint $$dir; done

vet:
	go vet -v $$(glide novendor)

.PHONY: all deps mock yecc build test fmt imports lint vet
