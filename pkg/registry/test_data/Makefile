PWD = $(shell pwd)

docker:
	docker run --rm -v $(PWD):/usr/src/myapp -w /usr/src/myapp -e GOOS=linux -e GOARCH=amd64 golang:1.13.3 go build -buildmode=plugin -v -o dump_filter_plugin.linux.so dump_filter_plugin.go
	GOARCH=amd64 GOOS=darwin go build -buildmode=plugin -o dump_filter_plugin.darwin.so dump_filter_plugin.go

build:
	go build -buildmode=plugin -o dump_filter_plugin.linux.so dump_filter_plugin.go