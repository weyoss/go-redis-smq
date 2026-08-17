# Build

This page tracks the build and test status for `go-redis-smq`, along with code coverage reported by Codecov.

## Status

| Branch  | CI                                                                                                                                                                                  | Code Coverage                                                                                                                              |
|---------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `main`  | [![CI](https://github.com/weyoss/go-redis-smq/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/weyoss/go-redis-smq/actions/workflows/ci.yml?query=branch%3Amain) | [![codecov](https://codecov.io/gh/weyoss/go-redis-smq/branch/main/graph/badge.svg)](https://codecov.io/gh/weyoss/go-redis-smq?branch=main) |
| `next`  | [![CI](https://github.com/weyoss/go-redis-smq/actions/workflows/ci.yml/badge.svg?branch=next)](https://github.com/weyoss/go-redis-smq/actions/workflows/ci.yml?query=branch%3Anext) | [![codecov](https://codecov.io/gh/weyoss/go-redis-smq/branch/next/graph/badge.svg)](https://codecov.io/gh/weyoss/go-redis-smq?branch=next) |

## Requirements

- **Go** ≥ 1.25
- **Redis** ≥ 4

## Building

```bash
make build
```

The build step also syncs Lua scripts from the TypeScript implementation before compiling:

```bash
make sync
go build ./...
```

## Testing

Run the full integration test suite:

```bash
make test
```

Or directly with `go test`:

```bash
go test ./...
```

The tests automatically start a local Redis instance. If you have `redis-server` installed, the test utilities will use it; otherwise, a pre-built Valkey binary is downloaded.

## Code Coverage

Coverage is generated in CI using:

```bash
go test -coverpkg=./... -coverprofile=coverage.out -count=1 -timeout 15m ./...
```

The report is uploaded to Codecov. The badges above reflect the latest coverage on each branch.

To generate a local coverage report:

```bash
go test -coverpkg=./... -coverprofile=coverage.out ./...
go tool cover -func=coverage.out
```

For an HTML report:

```bash
go tool cover -html=coverage.out
```
