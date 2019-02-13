GRAVITY_PKG := github.com/moiot/gravity

GOFILTER := grep -vE 'vendor'
GOCHECKER := $(GOFILTER) | awk '{ print } END { if (NR > 0) { exit 1 } }'

LDFLAGS += -X "$(GRAVITY_PKG)/pkg/utils.Version=0.0.1+git.$(shell git rev-parse --short HEAD)"
LDFLAGS += -X "$(GRAVITY_PKG)/pkg/utils.BuildTS=$(shell date -u '+%Y-%m-%d %I:%M:%S')"
LDFLAGS += -X "$(GRAVITY_PKG)/pkg/utils.GitHash=$(shell git rev-parse HEAD)"
LDFLAGS += -X "$(GRAVITY_PKG)/pkg/utils.GitBranch=$(shell git rev-parse --abbrev-ref HEAD)"

GO      := go
GOBUILD := $(GO) build
GOTEST  := $(GO) test

PACKAGES := $$(go list ./...| grep -vE 'vendor' | grep -vE 'nuclear')
TEST_DIRS := $(shell find . -iname "*_test.go" -exec dirname {} \; | uniq | grep -vE 'vendor' | grep -vE 'integration_test' | grep -vE 'protocol' | grep -vE 'padder' | grep -vE 'dcp')

.PHONY: update clean go-test test init dev-up dev-down run-dev test-down check tag deploy scanner e2e mock

default: build

dev-up: build dev-down run-dev

dev-down:
	docker-compose -f docker-compose-gravity-dev.yml down

go-test:
	go test -failfast -race -v ./integration_test
	go test -timeout 10m -coverprofile=cover.out $(TEST_DIRS) && go tool cover -func=cover.out | tail -n 1

test-local:
	make test; make test-down

test:
	docker-compose -f docker-compose-gravity-test.yml up --build --abort-on-container-exit

test-down:
	docker-compose -f docker-compose-gravity-test.yml down -v

run-dev:
	docker-compose -f docker-compose-gravity-dev.yml up -d --force-recreate
	-rm -f gravity/configdata/gravity.meta
	@for var in 3478 ; do \
		until mysql -h 127.0.0.1 -P $$var -u root -e 'select 1' >/dev/null 2>&1; do sleep 1; echo "Waiting for DB to come up..."; done ; \
	done
	mysql -h 127.0.0.1 -P 3478 -u root < gravity/configdata/seed_dev.sql
	bin/gravity -config=gravity/configdata/dev.toml -meta=gravity/configdata/gravity.meta -bootstrap-mode

build:
	$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/gravity cmd/gravity/main.go
	#$(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/padder cmd/padder/main.go


build-linux:
	GOARCH=amd64 GOOS=linux $(GOBUILD) -ldflags '$(LDFLAGS)' -o bin/gravity-linux-amd64 cmd/gravity/main.go

check:
	@echo "gofmt"
	@ gofmt -s -l . 2>&1 | $(GOCHECKER)

init:
	@ which glide >/dev/null || curl https://glide.sh/get | sh
	@ which glide-vc >/dev/null || go get -v -u github.com/sgotti/glide-vc
	@echo "install ginkgo"
	which ginkgo || (go get -v -u github.com/onsi/gomega && go get -v -u github.com/onsi/ginkgo/ginkgo)

lint:
	which gometalinter.v2 || (go get -u gopkg.in/alecthomas/gometalinter.v2)
	gometalinter.v2 --install
	gometalinter.v2 --vendor --deadline=120s ./...

proto:
	@ which protoc >/dev/null || brew install protobuf
	@ which protoc-gen-gofast >/dev/null || go get github.com/gogo/protobuf/protoc-gen-gofast
	protoc --gofast_out=Mgoogle/protobuf/wrappers.proto=github.com/gogo/protobuf/types:./pkg protocol/msgpb/message.proto
	protoc --gofast_out=plugins=grpc:./pkg protocol/dcp/message.proto

mock:
	mockgen -destination ./mock/binlog_checker/mock.go github.com/moiot/gravity/pkg/inputs/helper/binlog_checker BinlogChecker
	mockgen -destination ./mock/position_store/mock.go github.com/moiot/gravity/pkg/position_store PositionCacheInterface
	mockgen -destination ./mock/sliding_window/mock.go github.com/moiot/gravity/pkg/sliding_window WindowItem