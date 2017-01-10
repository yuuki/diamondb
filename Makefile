NAME=diamondb
COMMIT = $$(git describe --always)

all: build

build: yacc mock
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test:
	go test -v $$(glide novendor)

mock:
	mockgen -source vendor/github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface/interface.go -destination lib/storage/dynamo/dynamodb_mock.go -package dynamo

yacc:
	go tool yacc -o lib/query/parse.go lib/query/parse.go.y

fmt:
	gofmt -s -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

imports:
	goimports -w $(shell git ls | grep -e '\.go$$' | grep -v /vendor/)

lint:
	@for dir in $$(glide novendor); do golint $$dir; done

vet:
	go vet -v $$(glide novendor)

.PHONY: all deps mock yecc build test fmt imports lint vet
