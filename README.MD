<p align="center">
  <img src="https://github.com/Raezil/GoEventBus/blob/main/logoGoEventBus.png?raw=true">
</p>

# GoEventBus

A blazing-fast, in-memory, lock-free event bus for Go—ideal for low-latency pipelines, microservices, and game loops.  

[![Go Report Card](https://goreportcard.com/badge/github.com/Raezil/GoEventBus)](...)

## 📚 Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Use Cases](#-use-cases)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)
- [License](#license)


## Features

- **Lock-free ring buffer**: Efficient event dispatching using atomic operations and cache-line padding.
- **Dynamic dispatch**: Map event projections to handler functions.
- **Configurable buffer size**: Specify the ring buffer size (must be a power of two) when creating the store.
- **Async or Sync processing**: Toggle between synchronous handling or async goroutine-based processing via the `Async` flag.
- **Metrics**: Track published, processed, and errored event counts with a simple API.
- **Simple API**: Easy to subscribe, publish, and retrieve metrics.

## Why GoEventBus?

Modern Go apps demand lightning-fast, non-blocking communication—but channels can bottleneck and external brokers add latency, complexity and ops overhead. GoEventBus is your in-process, lock-free solution:

- **Micro-latency dispatch**  
  Atomic, cache-aligned ring buffers deliver sub-microsecond handoffs—no locks, no syscalls, zero garbage.  
- **Sync or Async at will**  
  Flip a switch to run handlers inline for predictability or in goroutines for massive parallelism.  
- **Built-in observability**  
  Expose counters for published, processed and errored events out of the box—no extra instrumentation.  
- **Drop-in, zero deps**  
  One import, no external services, no workers to manage—just Go 1.18+ and you’re off.  

Whether you’re building real-time analytics, high-throughput microservices, or game engines, GoEventBus keeps your events moving at Go-speed.


## Installation

```bash
go get github.com/Raezil/GoEventBus
```

## Quick Start

```go
package main

import (
    "fmt"
    "github.com/Raezil/GoEventBus"
)

func main() {
    // Create a dispatcher mapping projections to handlers
    dispatcher := GoEventBus.Dispatcher{
        "user_created": func(args map[string]any) (GoEventBus.Result, error) {
            userID := args["id"].(string)
            fmt.Println("User created with ID:", userID)
            return GoEventBus.Result{Message: "handled user_created"}, nil
        },
    }

    store := GoEventBus.NewEventStore(&dispatcher, 1<<16)

    // Optionally, run handlers asynchronously
    store.Async = true

    // Enqueue an event
    store.Subscribe(GoEventBus.Event{
        ID:         "evt1",
        Projection: "user_created",
        Args:       map[string]any{"id": "12345"},
    })

    // Process all pending events
    store.Publish()

    // Retrieve metrics
    published, processed, errors := store.Metrics()
    fmt.Printf("published=%d processed=%d errors=%d\n", published, processed, errors)
}
```

## API Reference

### `type Result`

```go
type Result struct {
    Message string // Outcome message from handler
}
```

### `type Dispatcher`

```go
type Dispatcher map[interface{}]func(map[string]any) (Result, error)
```
A map from projection keys to handler functions. Handlers receive a map of arguments and return a `Result` and an error.

### `type Event`

```go
type Event struct {
    ID         string        // Unique identifier for the event
    Projection interface{}   // Key to look up the handler in the dispatcher
    Args       map[string]any // Payload data for the event
}
```

### `type EventStore`

```go
type EventStore struct {
    dispatcher     *Dispatcher     // Pointer to the dispatcher map
    size           uint64          // Buffer size (power of two)
    buf            []unsafe.Pointer // Ring buffer of event pointers
    events         []Event         // Backing slice for Event data
    head           uint64          // Atomic write index
    tail           uint64          // Atomic read index
    Async          bool            // If true, handlers run asynchronously
    publishedCount uint64          // Number of events enqueued
    processedCount uint64          // Number of events successfully dispatched
    errorCount     uint64          // Number of handler errors
}
```

#### `NewEventStore`

```go
func NewEventStore(dispatcher *Dispatcher, bufferSize uint64) *EventStore
```
Creates a new `EventStore` with the provided dispatcher and ring buffer size.

#### `Subscribe`

```go
func (es *EventStore) Subscribe(e Event)
```
Atomically enqueues an `Event` for later publication. Thread-safe and non-blocking.

#### `Publish`

```go
func (es *EventStore) Publish()
```
Dispatches all events from the last published position to the current head. If `Async` is true, handlers run in separate goroutines; otherwise they run in the caller's goroutine. Old events are dropped on buffer overrun.

#### `Metrics`

```go
func (es *EventStore) Metrics() (published, processed, errors uint64)
```
Returns the total count of published, processed, and errored events.

### 💡 Use Cases

GoEventBus is ideal for scenarios where low-latency, high-throughput, and non-blocking event dispatching is needed:

- 🔄 Real-time event pipelines (e.g. analytics, telemetry)
- 📥 Background task execution or job queues
- 🧩 Microservice communication using in-process events
- ⚙️ Observability/event sourcing in domain-driven designs
- 🔁 In-memory pub/sub for small-scale distributed systems
- 🎮 Game loops or simulations requiring lock-free dispatching

## Benchmarks

All benchmarks were run with Go’s testing harness (`go test -bench .`) on an `-8` procs configuration.

| Benchmark                         | Iterations   | ns/op  |
|-----------------------------------|-------------:|-------:|
| `BenchmarkSubscribe-8`            | 27,080,376   | 40.37  |
| `BenchmarkSubscribeParallel-8`    | 26,418,999   | 38.42  |
| `BenchmarkPublish-8`              | 295,661,464  | 3.910  |
| `BenchmarkPublishAfterPrefill-8`  | 252,943,526  | 4.585  |
| `BenchmarkSubscribeLargePayload-8`| 1,613,017    | 771.5  |
| `BenchmarkPublishLargePayload-8`  | 296,434,225  | 3.910  |
| `BenchmarkEventStore_Async-8`     | 2,816,988    | 436.5  |
| `BenchmarkEventStore_Sync-8`      | 2,638,519    | 428.5  |
| `BenchmarkFastHTTPSync-8`         | 6,275,112    | 163.8  |
| `BenchmarkFastHTTPAsync-8`        | 1,954,884    | 662.0  |
| `BenchmarkFastHTTPParallel-8`     | 4,489,274    | 262.3  |


## Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/yourusername/GoEventBus/issues).

## License

Distributed under the MIT License. See `LICENSE` for more information.

