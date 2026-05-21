<p align="center">
  <img src="https://github.com/Protocol-Lattice/GoEventBus/blob/main/logo2025.png?raw=true">
</p>

# GoEventBus

A high-performance, in-memory, lock-free event bus for Go — built on a cache-line-padded atomic ring buffer with fan-out dispatch, middleware, lifecycle hooks, and back-pressure policies.

[![Go Report Card](https://goreportcard.com/badge/github.com/Protocol-Lattice/GoEventBus)](https://goreportcard.com/report/github.com/Protocol-Lattice/GoEventBus)

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Fan-out](#fan-out)
- [Middleware](#middleware)
- [Lifecycle Hooks](#lifecycle-hooks)
- [Back-pressure Policies](#back-pressure-policies)
- [Async Mode](#async-mode)
- [Transactions](#transactions)
- [Scheduling](#scheduling)
- [API Reference](#api-reference)
- [Benchmarks](#benchmarks)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **Lock-free ring buffer** — atomic operations with cache-line padding keep dispatch sub-microsecond
- **Fan-out dispatch** — register multiple independent handlers per projection; each runs its own middleware chain
- **Sync or async** — flip `store.Async = true` to hand work off to a fixed worker pool
- **Middleware** — wrap handlers with logging, tracing, retries, or any cross-cutting behaviour
- **Lifecycle hooks** — `OnBefore`, `OnAfter`, `OnError` for observability without touching handler logic
- **Back-pressure** — choose `DropOldest`, `Block`, or `ReturnError` per store
- **Transactions** — batch events and commit or roll back as a unit
- **Scheduling** — fire events at a future `time.Time` or after a `time.Duration`
- **Metrics** — published, processed, and error counters via a single call

---

## Installation

```bash
go get github.com/Protocol-Lattice/GoEventBus
```

Requires Go 1.21+.

---

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/Protocol-Lattice/GoEventBus"
)

type UserCreatedPayload struct{ ID string }
type HouseWasSold struct{}
type HouseSoldPayload struct {
    Address string
    Price   int
}

func main() {
    disp := GoEventBus.Dispatcher{}

    disp.Register("user_created", func(ctx context.Context, ev GoEventBus.Event) (GoEventBus.Result, error) {
        p := ev.Data.(UserCreatedPayload)
        fmt.Println("User created:", p.ID)
        return GoEventBus.Result{Message: "ok"}, nil
    })

    disp.Register(HouseWasSold{}, func(ctx context.Context, ev GoEventBus.Event) (GoEventBus.Result, error) {
        p := ev.Data.(HouseSoldPayload)
        fmt.Printf("House sold at %s for $%d\n", p.Address, p.Price)
        return GoEventBus.Result{Message: "ok"}, nil
    })

    store := GoEventBus.NewEventStore(&disp, 1<<16, GoEventBus.DropOldest)
    store.Async = true

    _ = store.Subscribe(context.Background(), GoEventBus.Event{
        ID:         "evt1",
        Projection: "user_created",
        Data:       UserCreatedPayload{ID: "u-42"},
    })
    _ = store.Subscribe(context.Background(), GoEventBus.Event{
        ID:         "evt2",
        Projection: HouseWasSold{},
        Data:       HouseSoldPayload{Address: "1 Main St", Price: 500_000},
    })

    store.Publish()

    if err := store.Drain(context.Background()); err != nil {
        log.Fatal(err)
    }

    published, processed, errors := store.Metrics()
    fmt.Printf("published=%d processed=%d errors=%d\n", published, processed, errors)
}
```

**Key points:**

- `Projection` can be any comparable value — a string, a struct, an integer.
- `Data` carries the type-safe payload; assert it inside the handler.
- Pass the context your handler should respect to `Subscribe`; it is forwarded to every handler.

---

## Fan-out

`Register` accumulates handlers. Every handler registered for a projection is called in order for each dispatched event. Handlers are fully independent — an error in one does not prevent the others from running.

```go
disp := GoEventBus.Dispatcher{}

// Register as many handlers as you need for the same projection.
disp.Register("order.placed",
    auditLogger,
    inventoryReducer,
    notificationSender,
)

// Register can also be called incrementally across different packages.
disp.Register("order.placed", analyticsTracker)
```

In async mode each handler invocation becomes its own work item, so the four handlers above may run in parallel across the worker pool.

---

## Middleware

Middleware wraps the handler chain and is applied in the order it is registered. Use it for logging, tracing, timeout injection, or retries.

```go
// Logging middleware
store.Use(func(next GoEventBus.HandlerFunc) GoEventBus.HandlerFunc {
    return func(ctx context.Context, ev GoEventBus.Event) (GoEventBus.Result, error) {
        log.Printf("handling %v", ev.Projection)
        res, err := next(ctx, ev)
        log.Printf("done %v err=%v", ev.Projection, err)
        return res, err
    }
})

// Timeout middleware
store.Use(func(next GoEventBus.HandlerFunc) GoEventBus.HandlerFunc {
    return func(ctx context.Context, ev GoEventBus.Event) (GoEventBus.Result, error) {
        ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
        defer cancel()
        return next(ctx, ev)
    }
})
```

Each handler in a fan-out gets its own independent copy of the middleware chain.

---

## Lifecycle Hooks

Hooks fire outside the middleware chain and are useful for metrics, structured logging, and alerting without polluting handler code.

```go
store.OnBefore(func(ctx context.Context, ev GoEventBus.Event) {
    // fires before each handler invocation
})

store.OnAfter(func(ctx context.Context, ev GoEventBus.Event, res GoEventBus.Result, err error) {
    // fires after each handler invocation, whether it succeeded or failed
})

store.OnError(func(ctx context.Context, ev GoEventBus.Event, err error) {
    // fires only when a handler returns a non-nil error
    log.Printf("handler error for %v: %v", ev.Projection, err)
})
```

---

## Back-pressure Policies

Choose how `Subscribe` behaves when the ring buffer is full.

| Policy | Behaviour | Best for |
|---|---|---|
| `DropOldest` | Discards the oldest event to make room | Low-latency pipelines where fresh data matters more than completeness |
| `Block` | Blocks the caller (respecting its context) until space is available | Workloads that cannot afford to lose events |
| `ReturnError` | Returns `ErrBufferFull` immediately | Callers that manage their own retry or back-pressure logic |

```go
store := GoEventBus.NewEventStore(&disp, 1<<14, GoEventBus.Block)

ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
defer cancel()

if err := store.Subscribe(ctx, ev); errors.Is(err, context.DeadlineExceeded) {
    // buffer stayed full for 50 ms — handle accordingly
}
```

---

## Async Mode

Set `store.Async = true` before the first `Publish` call. GoEventBus starts a worker pool sized to `runtime.NumCPU()` and routes every handler invocation through it.

```go
store := GoEventBus.NewEventStore(&disp, 1<<16, GoEventBus.DropOldest)
store.Async = true

store.Publish() // dispatches handlers to the worker pool

// Wait for all in-flight handlers before shutting down.
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

if err := store.Drain(ctx); err != nil {
    log.Printf("drain timeout: %v", err)
}
```

`Drain` (or equivalently `Close`) must be called once when the store is no longer needed so worker goroutines are cleaned up.

---

## Transactions

Group multiple events into a single commit. If any handler returns an error, `Commit` stops and returns that error. Call `Rollback` to discard buffered events without publishing.

```go
type OrderPayload struct{ OrderID string }
type InvoicePayload struct{ OrderID string }

disp := GoEventBus.Dispatcher{}
disp.Register("order.created", handleOrder)
disp.Register("invoice.issued", handleInvoice)

store := GoEventBus.NewEventStore(&disp, 1<<16, GoEventBus.DropOldest)

tx := store.BeginTransaction()

tx.Publish(GoEventBus.Event{
    ID:         "ev-1",
    Projection: "order.created",
    Data:       OrderPayload{OrderID: "ord-99"},
})
tx.Publish(GoEventBus.Event{
    ID:         "ev-2",
    Projection: "invoice.issued",
    Data:       InvoicePayload{OrderID: "ord-99"},
})

if err := tx.Commit(context.Background()); err != nil {
    tx.Rollback()
    log.Fatal("transaction failed:", err)
}
```

---

## Scheduling

Fire an event at a specific point in time or after a duration. The returned `*time.Timer` can be stopped to cancel the event before it fires.

```go
// Fire at an absolute time.
timer := store.Schedule(ctx, time.Now().Add(10*time.Second), ev)
if timer != nil {
    timer.Stop() // cancel before it fires
}

// Fire after a duration.
timer = store.ScheduleAfter(ctx, 30*time.Second, ev)
```

If the target time is in the past, or the duration is zero or negative, the event is executed synchronously and immediately without entering the ring buffer, and `nil` is returned.

---

## API Reference

### `Dispatcher`

```go
type Dispatcher map[interface{}][]HandlerFunc

func (d Dispatcher) Register(projection interface{}, handlers ...HandlerFunc)
```

`Register` appends one or more handlers for the given projection. Calling it multiple times on the same key accumulates handlers in call order.

### `Event`

```go
type Event struct {
    ID         string
    Projection interface{}     // key used to look up handlers
    Data       any             // type-safe payload (preferred)
    Args       map[string]any  // legacy payload (deprecated)
}
```

### `HandlerFunc` / `Middleware`

```go
type HandlerFunc func(context.Context, Event) (Result, error)
type Middleware  func(HandlerFunc) HandlerFunc
```

### `Result`

```go
type Result struct {
    Message string
}
```

### `OverrunPolicy`

```go
const (
    DropOldest  OverrunPolicy = iota
    Block
    ReturnError
)

var ErrBufferFull = errors.New("goeventbus: buffer is full")
```

### `EventStore`

#### Constructor

```go
func NewEventStore(dispatcher *Dispatcher, bufferSize uint64, policy OverrunPolicy) *EventStore
```

Panics if `dispatcher` is nil or `bufferSize` is not a non-zero power of two.

#### Methods

| Method | Description |
|---|---|
| `Subscribe(ctx, Event) error` | Enqueue an event; applies back-pressure per `OverrunPolicy` |
| `Publish()` | Dispatch all pending events to their handlers |
| `Use(Middleware)` | Append middleware to the chain |
| `OnBefore(BeforeHook)` | Register a hook that runs before each handler |
| `OnAfter(AfterHook)` | Register a hook that runs after each handler |
| `OnError(ErrorHook)` | Register a hook that runs when a handler errors |
| `Drain(ctx) error` | Wait for all async handlers; shuts down the worker pool |
| `Close(ctx) error` | Alias for `Drain` |
| `Metrics() (published, processed, errors uint64)` | Snapshot event counters |
| `Schedule(ctx, time.Time, Event) *time.Timer` | Fire an event at a time |
| `ScheduleAfter(ctx, time.Duration, Event) *time.Timer` | Fire an event after a duration |

#### `BeginTransaction() *Transaction`

Returns a `*Transaction` scoped to this store.

### `Transaction`

| Method | Description |
|---|---|
| `Publish(Event)` | Buffer an event within the transaction |
| `Commit(ctx) error` | Enqueue and process all buffered events; stops on first error |
| `Rollback()` | Discard all buffered events |

---

## Benchmarks

Run on Apple M-series with `go test -bench . -benchtime=3s`:

| Benchmark | Iterations | ns/op |
|---|---:|---:|
| `BenchmarkSubscribe` | 27,080,376 | 40.37 |
| `BenchmarkSubscribeParallel` | 26,418,999 | 38.42 |
| `BenchmarkPublish` | 295,661,464 | 3.91 |
| `BenchmarkPublishAfterPrefill` | 252,943,526 | 4.59 |
| `BenchmarkSubscribeLargePayload` | 1,613,017 | 771.5 |
| `BenchmarkPublishLargePayload` | 296,434,225 | 3.91 |
| `BenchmarkEventStore_Async` | 2,816,988 | 436.5 |
| `BenchmarkEventStore_Sync` | 2,638,519 | 428.5 |
| `BenchmarkFastHTTPSync` | 6,275,112 | 163.8 |
| `BenchmarkFastHTTPAsync` | 1,954,884 | 662.0 |
| `BenchmarkFastHTTPParallel` | 4,489,274 | 262.3 |

---

## Contributing

Contributions, issues, and feature requests are welcome. See the [issues page](https://github.com/Protocol-Lattice/GoEventBus/issues) to get started.

## License

Distributed under the MIT License. See `LICENSE` for details.
