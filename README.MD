# GoEventBus

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

| Benchmark                          | Iterations   | ns/op    |
|------------------------------------|-------------:|---------:|
| `BenchmarkSubscribe-8`             | 76,186,141   | 14.42    |
| `BenchmarkPublish-8`               | 455,006,211  | 2.484    |
| `BenchmarkSubscribeLargePayload-8` | 1,898,906    | 635.1    |
| `BenchmarkPublishLargePayload-8`   | 437,931,962  | 2.544    |


## Contributing

Contributions, issues, and feature requests are welcome! Feel free to check the [issues page](https://github.com/yourusername/GoEventBus/issues).

## License

Distributed under the MIT License. See `LICENSE` for more information.

