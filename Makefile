NAME=diamondb
COMMIT = $$(git describe --always)

all: build

deps:
	glide install
	go get github.com/golang/mock/mockgen

gen:
	go generate $$(glide novendor)

build: gen
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" -o $(NAME)

test: gen
	go test -race -v $$(glide novendor)

cover: gen
	go test -cover $$(glide novendor)

fmt:
	gofmt -s -w $$(git ls | grep -e '\.go$$' | grep -v /vendor/)

imports:
	goimports -w $$(git ls | grep -e '\.go$$' | grep -v /vendor/)

lint:
	golint $$(glide novendor)

vet:
	go vet -v $$(glide novendor)

docker-up:
	docker-compose up --build

.PHONY: all deps gen build test cover fmt imports lint vet docker-up
