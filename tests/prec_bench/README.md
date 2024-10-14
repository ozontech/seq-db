# Precision benches

## Purpose and design overview

The purpose of this bench is to profile and measure efficiency of changes in
some complex service under load.
Currently, the service in question is seq-db store.

The main design idea is to allow that service to run without interruption
under specified load, asynchronously measuring its performance.
This approach can be compared to checking someone's pulse without stopping
or otherwise affecting their heart's work.

## Entry points

There are two major entry points.

For understanding how everything works from inside, one should start with
[`Run` method](bench/bench.go). This is entry point of the whole process.
Reading docs completely first is a good idea though.

For starting their own benchmark, one should start with
[tests file](store_test.go), copy one of the sample tests provided here,
reuse stuff they can reuse, inline everything they want to change and, well,
write everything they need.

## Structure overview

The framework in situated in `prec_bench` package and
can be logically separated into three parts.
- First, there's `bench` package with all basic interfaces
  and abstractions as well as some utilities.
- There's `bare_store` package, containing store version that doesn't interact
  over network and all the implementations related to it's benchmarking.
- Root of the `prec_bench` contains all the `*_test.go` files (currently one)
  with benchmarks themselves and corresponding data generation.

## Workflow overview

Each benchmark starts with configuration of all the components needed.
This includes core components, service in question, data and requests etc.
These are done in the test file directly, and there are several helpers provided
for this need.

Next, the benchmark is run in three phases:
- Preload. During this phase the service is getting its starting load,
  and smoothly transits to main load.
- Main. The service is run under constant load, and several measurements can
  be done.
- Teardown. When everything's done, services and all components are stopped.

## Core

Core is situated in `bench` package and contains several important things:
- `func Run(b *testing.B, payload Payload, meter Meter, cleaner *Cleaner)`:
  This is the entry point of the whole process. It ties everything together,
  including interaction with `b *testing.B`. More details follow.
- `Cleaner` struct. Helper for the teardown and cleanup process.
- `Meter` interface. This is abstraction of the measuring method we are using.
  It's gluing `b *testting.B` and bench process together. There are several
  of them provided out-of-the-box.
- `Payload` interface. This is the abstraction of the service we are benching.
  Its main responsibility is to set service up and keep it running under load.
  Implementations should provide one method `Start(cleaner* Cleaner)`.
  They are expected (but not obliged) to interact with some provided Meter.
- Some channel-related helpers in [utils](bench/utils.go) file. These are used
  by tests to generate and organize load.

## Meter

`Meter` interface has three methods:
- `Start(n int)`. This one is called from `Run` method. It means that one
  measurement is to be performed. It blocks until measurement starts.
  `n` has the same meaning as `testing.B.N`.
- `Wait()`. This method is `Start`'s counterpart.
  It blocks till measurement end.
- `Step()`. This one is generally called by `Payload` and means that one
  operation (out of `n`) was performed. It's up to benchmark writer to decide
  what is an operation, and when to call it. Usually this is done by payload
  on per-request basis.

There are several meters included:
- `incMeter`. The simplest and most used one. It atomically reduces internal
  counter on each `Step`, and reports measurement's end
  when counter reaches zero.
- `lockingIncMeter`. The same as above, but uses mutex inside as to ensure
  correctness. Use if you believe `incMeter` is not stable enough.
- `timeMeter`. Ignores `Step` method, just sleeps for the time
  proportional to `n`.
- `warmupMeter`. Wraps another `Meter` as to ensure that enough time has passed
  for the payload to warm up before starting measurements.
  During first measurement it skips `skipSteps` steps, and then sleeps for
  additional `delay`. Then every Step is passed to `nested Meter`.
  Copy values from existing test or adjust for your need.

## Payload

`Payload` has one method `Start(cleaner* Cleaner)`. It should set everything
needed up, then block until the service is ready and under load.
When it returns, Preload phase is finished and Main starts,
i.e. all the measurements can be performed.

`Payload` should asynchronously keep service under load,
and is responsible for all the synchronization needed.
It's `Payload`'s responsibility to clean everything service-related during
teardown phase. `cleaner` is provided to set everything needed for that.

Since `Payload` is directly set up in the test, everything else is up to
specific implementation that is going to be used. For example, this includes
how teardown is handled and when `Step` method is called on some meter.

## Teardown process

Teardown is controlled by `Run` function. It has two phases.
First starts with setting `Cleaner.TeardownStart` to true as to stop
all continuous processes. It then calls `Cleaner.WgTeardown.Done()` as to
indicate it's done with first phase, and waits for everyone else to
do the same.

When the first phase is done, the final clean up is done as to free all
resources that couldn't be freed before. During the second phase `Run` calls
`Cleaner.WgCleanUp.Done()` and waits for everyone else to do the same. When
done it runs `runtime.GC()`, and after that benchmark is officially over.

Thus, every component that needs to clear something in the end has three options:
- If it has some input, it can rely on it's input (e.g. some channel) to stop
  on its own, and then teardown and call `Cleaner.WgTeardown.Done()`.
- If it continuously does some output without input, it can check
  `Cleaner.TeardownStart` time to time and then teardown when it becomes true,
  calling `Cleaner.WgTeardown.Done()` afterwards.
- If the component doesn't represent process, but still needs to be cleaned
  (e.g. some buffers), it can wait on `Cleaner.WgTeardown` and then do clean,
  calling `Cleaner.WgCleanUp.Done()` afterwards.

Depending on the option chosen, the component in question should call `Add(1)`
to the corresponding `sync.WaitGroup` in the beginning of its work.
Component is free to use several of these options together.

## Bare store payload

`Payload` implementation, handling `BareStore` store.
All the load is taken from corresponding channels by several workers.

It allows parallel execution of several bulk requests during preload.
It also allows parallel execution of several bulk and search requests during
main load. The number of parallel requests can be specified in config.

It can be provided with two `Meter`s, one for bulk requests and one for search.

It waits for all of its workers to stop, i.e. for the incoming channels
to be closed. Then stops the store and calls `Cleaner.WgTeardown.Done()`.
