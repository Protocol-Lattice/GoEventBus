<p align="center">
  <img src="https://github.com/Protocol-Lattice/GoEventBus/blob/main/logo2025.png?raw=true" alt="GoEventBus">
</p>

# GoEventBus

**A high-performance event bus for Go with deterministic rules, cached decisions, and optional Jev-powered event routing.**

GoEventBus combines a bounded MPMC in-memory event bus with fan-out, ordered and batch handlers, middleware, lifecycle hooks, dead-letter handling, Redis Streams, RabbitMQ, and an optional decision layer for choosing which event type should execute.

[![Go Report Card](https://goreportcard.com/badge/github.com/Protocol-Lattice/GoEventBus)](https://goreportcard.com/report/github.com/Protocol-Lattice/GoEventBus)

```text
                           ┌──────────────┐
                           │    Rules     │
                           └──────┬───────┘
                                  │ no match
                                  ▼
Event state ───────────────► ┌──────────────┐
                             │    Cache     │
                             └──────┬───────┘
                                    │ miss
                                    ▼
                             ┌──────────────┐
                             │     Jev      │
                             └──────┬───────┘
                                    │
                                    ▼
                             selected event type
                                    │
                                    ▼
Subscribe ─► MPMC ring ─► Publish ─► handlers
                         ├─────────► ordered handlers
                         └─────────► batch handlers
```

The decision layer is optional. If you already know the event type, call `Subscribe` directly and GoEventBus behaves like a normal high-performance event bus.

## Why GoEventBus?

Most events do not need model inference. Some do.

GoEventBus keeps those paths separate:

- **Known event type** → enqueue it directly.
- **Obvious rule** → route deterministically.
- **Repeated decision** → reuse the cached choice.
- **Ambiguous input** → ask Jev to choose from a bounded candidate set.
- **Execution** → always stays inside GoEventBus.

That gives you intelligent routing without putting an LLM call in the `Publish()` hot path.

## Features

- **Bounded MPMC ring buffer** built with atomics and cache-line padding.
- **Rules → cache → Jev routing** for optional intelligent event selection.
- **Typed or string projections** for local dispatch.
- **Fan-out handlers** with independent execution.
- **Ordered handlers** with FIFO delivery per ordering key.
- **Batch handlers** for bulk writes and high-throughput pipelines.
- **Sync and async dispatch** with a fixed worker pool.
- **Middleware** for per-handler cross-cutting behavior.
- **Lifecycle hooks** with `OnBefore`, `OnAfter`, and `OnError`.
- **Back-pressure policies**: `DropOldest`, `Block`, or `ReturnError`.
- **Dead-letter queue** with replay support and panic recovery.
- **Redis Streams and RabbitMQ providers** for cross-process delivery.
- **Transactions** with local buffering and synchronous commit.
- **Scheduling** with absolute and relative timers.
- **Metrics** for published, processed, and failed events.
- **Pluggable decision cache** through the `DecisionCache` interface.

## Installation

```bash
go get github.com/Protocol-Lattice/GoEventBus
```

GoEventBus currently targets **Go 1.23+**.

---

# Quick Start

## Plain event bus

Use the direct path when the event type is already known.

```go
package main

import (
    "context"
    "fmt"
    "log"

    GoEventBus "github.com/Protocol-Lattice/GoEventBus"
)

type UserCreated struct {
    ID string
}

func main() {
    dispatcher := GoEventBus.Dispatcher{}

    dispatcher.Register("user.created", func(
        ctx context.Context,
        ev GoEventBus.Event,
    ) (GoEventBus.Result, error) {
        payload := ev.Data.(UserCreated)
        fmt.Println("user created:", payload.ID)
        return GoEventBus.Result{Message: "ok"}, nil
    })

    store := GoEventBus.NewEventStore(
        &dispatcher,
        1<<16,
        GoEventBus.DropOldest,
    )
    store.Async = true

    if err := store.Subscribe(context.Background(), GoEventBus.Event{
        ID:         "evt-1",
        Projection: "user.created",
        Data:       UserCreated{ID: "u-42"},
    }); err != nil {
        log.Fatal(err)
    }

    store.Publish()

    if err := store.Drain(context.Background()); err != nil {
        log.Fatal(err)
    }
}
```

`Projection` may be any comparable value for local dispatch, including a string or a typed struct.

---

# Intelligent Event Routing

GoEventBus can choose an event type before enqueueing it.

The recommended pipeline is:

```text
rules → cache → Jev → cache write → Subscribe → Publish
```

This keeps the fast path deterministic and only calls Jev when local logic cannot resolve the event type.

## 1. Define candidates

```go
type HouseWasSold struct{}

candidates := []GoEventBus.EventCandidate{
    {
        Key:         "user_created",
        Projection:  "user.created",
        Description: "A new user account was created",
    },
    {
        Key:         "house_sold",
        Projection:  HouseWasSold{},
        Description: "A property sale was completed",
    },
    {
        Key:         "order_cancelled",
        Projection:  "order.cancelled",
        Description: "An existing order should be cancelled",
    },
}
```

`Key` is the stable identifier exposed to the decision layer. `Projection` is the real GoEventBus dispatcher key.

That separation lets Jev choose between simple string keys while the event bus can still dispatch to typed projections.

## 2. Add deterministic rules

Rules run first. The first matching rule wins.

```go
rules := []GoEventBus.EventRule{
    {
        Name:   "explicit-cancel",
        Choice: "order_cancelled",
        Match: func(
            _ context.Context,
            state any,
            _ []GoEventBus.EventCandidate,
        ) bool {
            input, ok := state.(map[string]any)
            return ok && input["action"] == "cancel"
        },
    },
}
```

Rules are intentionally evaluated before cache. Adding a new rule can therefore override an older cached model decision immediately.

## 3. Add a decision cache

```go
cache := GoEventBus.NewMemoryDecisionCache(5 * time.Minute)
```

The built-in cache is goroutine-safe and lazily expires entries.

The default cache key is a SHA-256 hash of:

- the JSON-serializable decision state,
- candidate keys,
- candidate descriptions.

Candidate ordering does not change the key. Projection values are deliberately excluded because typed projections may not be JSON-serializable.

Need a different strategy? Supply `RuleCacheSelector.CacheKey`.

## 4. Use Jev as the fallback

```go
jev := &GoEventBus.JevSelector{
    APIKey: os.Getenv("OPENROUTER_API_KEY"),
}
```

The default model is:

```text
typesafe/jev-1.13
```

The adapter calls OpenRouter's Decisions API using the Go standard library, so the Jev integration adds no SDK dependency.

## 5. Compose the router

```go
selector := &GoEventBus.RuleCacheSelector{
    Rules:    rules,
    Cache:    cache,
    Fallback: jev,
}
```

Then route and enqueue:

```go
state := map[string]any{
    "message": "The property at 1 Main St was sold for $500000",
}

decision, err := store.DecideAndSubscribe(
    context.Background(),
    selector,
    state,
    GoEventBus.Event{
        ID: "evt-2",
    },
    candidates,
)
if err != nil {
    log.Fatal(err)
}

fmt.Printf(
    "choice=%s confidence=%.2f probabilities=%v\n",
    decision.Choice,
    decision.Confidence,
    decision.Probabilities,
)

store.Publish()
```

If a rule matches, Jev is never called. If the same decision state is already cached, Jev is never called. Only a cache miss reaches the model.

## Cache behavior

`DecisionCache` is intentionally small:

```go
type DecisionCache interface {
    Get(context.Context, string) (EventDecision, bool, error)
    Set(context.Context, string, EventDecision) error
}
```

That makes it straightforward to implement Redis, distributed, persistent, or application-specific caches.

Cache failures are **fail-open by default**. Routing continues through the fallback selector because cache is treated as an optimization.

Set:

```go
StrictCache: true
```

when cache failures should fail the routing call instead.

---

# Core Event Model

```go
type Event struct {
    ID         string
    Projection interface{}
    Data       any
    Args       map[string]any // deprecated
}
```

Prefer `Data` for payloads. `Args` remains for backwards compatibility.

Handlers use:

```go
type HandlerFunc func(
    context.Context,
    Event,
) (Result, error)
```

Register handlers with a `Dispatcher`:

```go
dispatcher := GoEventBus.Dispatcher{}

dispatcher.Register(
    "order.created",
    handleBilling,
    handleAnalytics,
)
```

Calling `Register` repeatedly for the same projection appends handlers rather than replacing them.

---

# Fan-out

Multiple handlers may subscribe to the same projection.

```go
dispatcher.Register(
    "order.placed",
    auditLogger,
    inventoryReducer,
    notificationSender,
)
```

Each handler is independent. One handler returning an error does not prevent the other fan-out handlers from running.

In async mode, each regular handler invocation becomes its own worker-pool item.

---

# Ordered Handlers

Use ordered handlers when events for the same entity must be processed sequentially while unrelated entities can still run concurrently.

```go
type OrderEvent struct {
    OrderID  string
    Sequence int
}

store.RegisterOrdered(
    "order.event",
    func(ev GoEventBus.Event) string {
        return ev.Data.(OrderEvent).OrderID
    },
    func(
        ctx context.Context,
        ev GoEventBus.Event,
    ) (GoEventBus.Result, error) {
        return applyOrderEvent(ctx, ev.Data.(OrderEvent))
    },
)
```

For a given ordering key, async delivery preserves FIFO order.

Different keys remain concurrent.

---

# Batch Handlers

Batch handlers collect pending events for a projection during a `Publish` cycle and deliver them in chunks.

```go
store.RegisterBatch(
    "metrics.recorded",
    100,
    func(
        ctx context.Context,
        events []GoEventBus.Event,
    ) ([]GoEventBus.Result, error) {
        return nil, writeMetricsBatch(ctx, events)
    },
)
```

Regular and batch handlers may coexist on the same projection.

Multiple batch handlers may also be registered for one projection; each receives the full chunk independently.

Middleware is not applied to batch handlers. Lifecycle hooks are.

---

# Middleware

Middleware wraps regular handlers.

```go
store.Use(func(next GoEventBus.HandlerFunc) GoEventBus.HandlerFunc {
    return func(
        ctx context.Context,
        ev GoEventBus.Event,
    ) (GoEventBus.Result, error) {
        started := time.Now()
        result, err := next(ctx, ev)
        log.Printf(
            "projection=%v duration=%s err=%v",
            ev.Projection,
            time.Since(started),
            err,
        )
        return result, err
    }
})
```

Middleware is applied independently to every handler in a fan-out.

---

# Lifecycle Hooks

Use hooks for observability without changing handler logic.

```go
store.OnBefore(func(ctx context.Context, ev GoEventBus.Event) {
    metrics.Inc("handler.started")
})

store.OnAfter(func(
    ctx context.Context,
    ev GoEventBus.Event,
    result GoEventBus.Result,
    err error,
) {
    metrics.Inc("handler.finished")
})

store.OnError(func(
    ctx context.Context,
    ev GoEventBus.Event,
    err error,
) {
    log.Printf("event=%s err=%v", ev.ID, err)
})
```

For batch handlers, hooks are emitted per event.

---

# Back-pressure

Choose how `Subscribe` behaves when the ring buffer is full.

| Policy | Behavior |
|---|---|
| `DropOldest` | Evict the oldest queued event and accept the new one |
| `Block` | Wait for capacity while respecting the caller context |
| `ReturnError` | Return `ErrBufferFull` immediately |

Example:

```go
store := GoEventBus.NewEventStore(
    &dispatcher,
    1<<14,
    GoEventBus.Block,
)

ctx, cancel := context.WithTimeout(
    context.Background(),
    50*time.Millisecond,
)
defer cancel()

if err := store.Subscribe(ctx, event); err != nil {
    log.Println("enqueue failed:", err)
}
```

---

# Async Mode

Enable worker-pool dispatch with:

```go
store.Async = true
```

The store uses a fixed worker pool sized to `runtime.NumCPU()`.

`Publish()` submits regular, ordered, and batch work to that pool.

Call `Drain` or `Close` when shutting down:

```go
ctx, cancel := context.WithTimeout(
    context.Background(),
    5*time.Second,
)
defer cancel()

if err := store.Drain(ctx); err != nil {
    log.Println("drain failed:", err)
}
```

Once shutdown begins, new subscriptions return `ErrEventStoreClosed`.

---

# External Providers

The local `EventStore` remains the execution engine. Providers move events between processes.

```go
type Provider interface {
    Publish(context.Context, Event) error
    Consume(context.Context, EventConsumer) error
    Close() error
}
```

Built-in providers:

- **Redis Streams**
- **RabbitMQ**

Remote providers require a **string projection** because the projection becomes a stable broker routing name.

## Redis Streams

```go
rdb := redis.NewClient(&redis.Options{
    Addr: "localhost:6379",
})

provider, err := GoEventBus.NewRedisProvider(
    GoEventBus.RedisProviderConfig{
        Client:   rdb,
        Stream:   "events",
        Group:    "billing",
        Consumer: "billing-1",
    },
)
if err != nil {
    log.Fatal(err)
}
defer provider.Close()

go func() {
    if err := store.Consume(ctx, provider); err != nil &&
        !errors.Is(err, context.Canceled) {
        log.Println(err)
    }
}()
```

## RabbitMQ

```go
provider, err := GoEventBus.NewRabbitMQProvider(
    GoEventBus.RabbitMQProviderConfig{
        URL:        "amqp://guest:guest@localhost:5672/",
        Exchange:   "events",
        Queue:      "billing",
        BindingKey: "order.*",
        Consumer:   "billing-1",
    },
)
if err != nil {
    log.Fatal(err)
}
defer provider.Close()
```

Providers use `JSONCodec` by default. Supply a custom `DecodePayload` when consumers need concrete payload types rather than generic JSON values.

---

# Dead Letter Queue

Attach a DLQ to retain handler failures and recovered panics.

```go
store.DLQ = GoEventBus.NewDeadLetterQueue()
```

Inspect failures:

```go
for _, dead := range store.DLQ.Entries() {
    log.Printf(
        "event=%s attempts=%d err=%v",
        dead.Event.ID,
        dead.Attempts,
        dead.Err,
    )
}
```

Replay:

```go
if err := store.DLQ.Replay(ctx, store); err != nil {
    log.Println("replay failed:", err)
}
```

Handler panics are recovered and converted into errors so a single handler cannot kill the worker pool.

---

# Transactions

Transactions are local event buffers, not database-style atomic transactions.

```go
tx := store.BeginTransaction()

tx.Publish(GoEventBus.Event{
    ID:         "evt-1",
    Projection: "order.created",
    Data:       order,
})

tx.Publish(GoEventBus.Event{
    ID:         "evt-2",
    Projection: "invoice.created",
    Data:       invoice,
})

if err := tx.Commit(ctx); err != nil {
    tx.Rollback()
    log.Fatal(err)
}
```

`Commit` executes buffered regular handlers synchronously and stops at the first error.

`Rollback` only discards events still buffered locally. It cannot undo side effects from handlers that already ran.

---

# Scheduling

Schedule an event at a time:

```go
timer := store.Schedule(
    ctx,
    time.Now().Add(10*time.Second),
    event,
)
```

Or after a duration:

```go
timer := store.ScheduleAfter(
    ctx,
    30*time.Second,
    event,
)
```

The returned `*time.Timer` may be stopped before it fires.

Past times and non-positive durations are handled immediately.

---

# Metrics

```go
published, processed, failures := store.Metrics()

fmt.Printf(
    "published=%d processed=%d errors=%d\n",
    published,
    processed,
    failures,
)
```

These counters cover event execution. Decision-layer metrics such as cache hit rate or Jev latency are intentionally not baked into the core API yet.

---

# API Snapshot

## Event routing

```go
type EventCandidate struct {
    Key         string
    Projection  interface{}
    Description string
}

type EventDecision struct {
    Choice        string
    Projection    interface{}
    Confidence    float64
    Probabilities map[string]float64
    Model         string
    RequestID     string
}

type EventSelector interface {
    SelectEvent(
        context.Context,
        any,
        []EventCandidate,
    ) (EventDecision, error)
}
```

Main implementations:

- `JevSelector`
- `RuleCacheSelector`
- `MemoryDecisionCache`

## EventStore

```go
func NewEventStore(
    dispatcher *Dispatcher,
    bufferSize uint64,
    policy OverrunPolicy,
) *EventStore
```

`bufferSize` must be a non-zero power of two.

Important methods:

| Method | Purpose |
|---|---|
| `Subscribe` | Enqueue a known event |
| `DecideAndSubscribe` | Select a candidate event type and enqueue it |
| `Publish` | Dispatch pending events |
| `RegisterOrdered` | Preserve FIFO per ordering key |
| `RegisterBatch` | Process projection events in chunks |
| `Consume` | Feed provider events into the local store |
| `Use` | Register middleware |
| `OnBefore` / `OnAfter` / `OnError` | Register lifecycle hooks |
| `Metrics` | Read event counters |
| `Schedule` / `ScheduleAfter` | Schedule future events |
| `BeginTransaction` | Start a local transaction buffer |
| `Drain` / `Close` | Stop accepting events and finish in-flight work |

---

# Benchmarks

Repository benchmarks on Apple M-series:

```bash
go test -bench . -benchtime=3s
```

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

The intelligent routing path is intentionally outside these core dispatch benchmarks because rules, cache, and Jev have very different latency profiles.

---

# Design Principles

GoEventBus keeps decision-making and execution separate.

```text
Decision layer                    Execution layer

rules ─┐
       ├─► choice ───────────────► EventStore
cache ─┤                           ├─ ring buffer
       │                           ├─ back-pressure
Jev ───┘                           ├─ worker pool
                                   ├─ ordering
                                   ├─ batching
                                   ├─ middleware/hooks
                                   └─ DLQ
```

This separation means:

- the event bus does not depend on Jev,
- deterministic workloads do not pay model latency,
- model failures do not change core dispatch semantics,
- alternative selectors can implement the same `EventSelector` interface,
- alternative caches can implement `DecisionCache`,
- application code can mix direct and intelligent routing in the same store.

---

# Contributing

Issues and pull requests are welcome.

Run the test suite with:

```bash
go test -race ./...
```

Integration tests:

```bash
go test -race -tags=integration -timeout=5m ./...
```

Quality checks used by CI include `go vet` and `staticcheck`.

---

# License

Distributed under the MIT License. See `LICENSE`.
