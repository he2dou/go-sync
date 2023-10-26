.PHONY: build run help win

BINARY="go-sync"

VERSION = $(shell git describe --tags)

build:
	go build -ldflags "-X main.Version=${VERSION}" -o ${BINARY}

win:
	set GOOS=windows
	set GOARCH=amd64
	go build -ldflags "-X main.Version=${VERSION}" -o ${BINARY}.exe

run:
	@go run .

help:
	@echo "make - 格式化 Go 代码, 并编译生成二进制文件"
	@echo "make build - 编译 Go 代码, 生成二进制文件"
	@echo "make run - 直接运行 Go 代码"
