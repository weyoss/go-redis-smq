# Running tests

## All tests

```
go test ./tests/...
```

## Single domain

```
go test ./tests/queue/...
```

## Single test

```
go test -run TestQueue_Create ./tests/queue/...
```

## Verbose

```
go test -v ./tests/...
```

## With coverage

```
go test -cover ./tests/...
```

## Short timeout (fail fast if Redis doesn't start)

```
go test -timeout 30s ./tests/...
```

## Using gotestsum util

```
gotestsum --format testname -- -count=1 ./tests/...
```


