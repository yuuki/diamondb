COMMIT = $$(git describe --always)
PKG = github.com/yuuki/diamondb
PKGS = $$(go list ./... | grep -v vendor)

all: build

.PHONY: deps
deps:
	go get github.com/golang/mock/mockgen

.PHONY: gen
gen:
	go generate $(PKGS)

.PHONY: build
build: gen
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" $(PKG)/cmd/...

.PHONY: test
test: gen
	go test -race -v $(PKGS)
	make vet

.PHONY: cover
cover: gen
	go test -cover $(PKGS)

.PHONY: fmt
fmt:
	gofmt -s -w $$(git ls | grep -e '\.go$$' | grep -v /vendor/)

.PHONY: imports
imports:
	goimports -w $$(git ls | grep -e '\.go$$' | grep -v /vendor/)

.PHONY: lint
lint:
	golint $(PKGS)

.PHONY: vet
vet:
	go tool vet -all -printfuncs=Wrap,Wrapf,Errorf $$(find . -type d -d 1 | grep -v -e "^\.\/\." -e vendor)

.PHONY: up
up:
	docker-compose up --build

.PHONY: down
down:
	docker-compose down
