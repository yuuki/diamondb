COMMIT = $$(git describe --always)
PKG = github.com/yuuki/diamondb
PKGS = $$(go list ./... | grep -v vendor)

all: build

.PHONY: deps
deps:
	go get golang.org/x/tools/cmd/goyacc
	go get github.com/golang/mock/mockgen

.PHONY: gen
gen:
	go generate $(PKGS)

.PHONY: build
build: gen
	go build -ldflags "-X main.GitCommit=\"$(COMMIT)\"" $(PKG)/cmd/...

.PHONY: test
test:
	go test -v $(PKGS)

.PHONY: test-race
test-race:
	go test -v -race $(PKGS)

.PHONY: test-all
test-all: vet test-race

.PHONY: cover
cover: gen
	go test -cover $(PKGS)

.PHONY: fmt
fmt:
	gofmt -s -w $$(git ls | grep -e '\.go$$' | grep -v -e vendor)

.PHONY: imports
imports:
	goimports -w $$(git ls | grep -e '\.go$$' | grep -v -e vendor)

.PHONY: lint
lint:
	golint $(PKGS)

.PHONY: vet
vet:
	go tool vet -all -printfuncs=Wrap,Wrapf,Errorf $$(find . -maxdepth 1 -mindepth 1 -type d | grep -v -e "^\.\/\." -e vendor)

.PHONY: up
up:
	docker-compose up --build

.PHONY: down
down:
	docker-compose down
