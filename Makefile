COMMIT = $$(git describe --always)

all: build

.PHONY: deps
deps:
	glide install
	go get github.com/golang/mock/mockgen

.PHONY: gen
gen:
	go generate $$(glide novendor)

.PHONY: build
build: gen
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" cmd/...

.PHONY: test
test: gen
	go test -race -v $$(glide novendor)
	make vet

.PHONY: cover
cover: gen
	go test -cover $$(glide novendor)

.PHONY: fmt
fmt:
	gofmt -s -w $$(git ls | grep -e '\.go$$' | grep -v /vendor/)

.PHONY: imports
imports:
	goimports -w $$(git ls | grep -e '\.go$$' | grep -v /vendor/)

.PHONY: lint
lint:
	golint $$(glide novendor)

.PHONY: vet
vet:
	go vet -v $$(glide novendor)

.PHONY: up
up:
	docker-compose up --build

.PHONY: down
down:
	docker-compose down
