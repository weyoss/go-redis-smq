# Running Benchmarks

`-benchtime=1x` runs each sub-benchmark exactly once. Fresh Redis per sub-benchmark prevents accumulation.
Also fix the main TestMain — it runs RedisSMQ init and starts the purge worker.

For benchmarks, the purge worker adds overhead.

Consider skipping purge worker in benchmarks or using a separate TestMain.

## Run benchmarks

```
go test -bench=. -benchmem ./benchmark/...
```

## Run specific benchmark

```
go test -bench=BenchmarkProducer_10K -benchmem ./benchmark/...
```


## Run benchmarks with CPU profiling

```
go test -bench=. -cpuprofile=cpu.prof ./benchmark/...
```
