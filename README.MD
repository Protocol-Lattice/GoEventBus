<p align="center">
  <img src="https://github.com/Raezil/GoEventBus/blob/main/logoGoEventBus.png?raw=true">
</p>

# GoEventBus
[![Go Report Card](https://goreportcard.com/badge/github.com/Raezil/GoEventBus)](https://goreportcard.com/report/github.com/Raezil/GoEventBus)

A lightweight, lock-free, in-memory event bus for Go applications. GoEventBus allows publishers to emit events and subscribers to handle those events asynchronously in a high-performance ring buffer.

## Features

- **Lock-free ring buffer**: Efficient event dispatching using atomic operations.
- **Dynamic dispatch**: Map event projections to handler functions.
- **Simple API**: Easy to subscribe and publish events.

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

    // Initialize the event store
    store := GoEventBus.NewEventStore(&dispatcher)

    // Subscribe an event
    store.Subscribe(GoEventBus.Event{
        ID:         "evt1",
        Projection: "user_created",
        Args:       map[string]any{"id": "12345"},
    })

    // Publish all pending events
    store.Publish()
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
type Dispatcher map[any]func(args map[string]any) (Result, error)
```
A map from projection keys to handler functions. Handlers receive a map of arguments and return a `Result` and an error.

### `type Event`

```go
type Event struct {
    ID         string        // Unique identifier for the event
    Projection any           // Key to look up the handler in the dispatcher
    Args       map[string]any // Payload data for the event
}
```

### `type EventStore`

```go
type EventStore struct {
    dispatcher *Dispatcher  // Pointer to the dispatcher map
    buf        [1<<16]Event // Ring buffer for storing events
    head       uint64       // Atomic write index
    tail       uint64       // Read index
    Async      bool         // <-- Add this flag
}
```

#### `NewEventStore`

```go
func NewEventStore(dispatcher *Dispatcher) *EventStore
```
Creates a new `EventStore` with the provided dispatcher.

#### `Subscribe`

```go
func (es *EventStore) Subscribe(e Event)
```
Adds an event to the ring buffer for later publication. Thread-safe using atomic writes.

#### `Publish`

```go
func (es *EventStore) Publish()
```
Dispatches all events from the last published position to the current head. Calls the corresponding handler for each event if it exists.

## Benchmarks

All benchmarks were run with Go’s testing harness (`go test -bench .`) on an `-8` procs configuration.

## Benchmarks

All benchmarks were run with Go’s testing harness (`go test -bench .`) on an `-8` procs configuration.

| Benchmark                          | Iterations  | ns/op   | B/op  | allocs/op |
|------------------------------------|------------:|--------:|------:|-----------:|
| `BenchmarkSubscribe-8`             | 8,735,622   | 130.3   | 48    | 1         |
| `BenchmarkSubscribeParallel-8`     | 23,469,384  | 63.96   | 48    | 1         |
| `BenchmarkPublish-8`               | 329,031,526 | 3.787   | 0     | 0         |
| `BenchmarkPublishAfterPrefill-8`   | 330,586,874 | 3.622   | 0     | 0         |
| `BenchmarkSubscribeLargePayload-8` | 1,398,704   | 833.4   | 1408  | 4         |
| `BenchmarkPublishLargePayload-8`   | 318,486,877 | 3.576   | 0     | 0         |
| `BenchmarkEventStore_Async-8`      | 2,199,759   | 468.1   | 392   | 4         |
| `BenchmarkEventStore_Sync-8`       | 2,336,882   | 473.6   | 392   | 3         |
| `BenchmarkFastHTTPSync-8`          | 4,408,676   | 278.0   | 48    | 1         |
| `BenchmarkFastHTTPAsync-8`         | 1,642,339   | 726.9   | 72    | 2         |
| `BenchmarkFastHTTPParallel-8`      | 6,527,530   | 181.3   | 48    | 1         |


## Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/yourusername/GoEventBus/issues).

## License

Distributed under the MIT License. See `LICENSE` for more information.

