.PHONY: help sync test build clean

help:
	@echo "Available commands:"
	@echo "  make sync    - Sync Lua scripts from TypeScript project"
	@echo "  make test    - Run all tests"
	@echo "  make build   - Build the library"
	@echo "  make clean   - Clean build artifacts"

sync:
	@bash scripts/sync_redis_scripts.sh

test:
	@go test -v ./tests/...

build: sync
	@go build ./...

clean:
	@rm -rf lib/redis/scripts/lua/*.lua
	@go clean -cache