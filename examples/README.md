# GoEventBus Examples

Runnable examples live under this directory. Run them from the repository root with `go run ./examples/<name>`.

## Intelligent routing

### `routing_rules`

Deterministic routing with `EventRule`.

```bash
go run ./examples/routing_rules
```

Flow:

```text
state -> rule match -> selected projection -> Subscribe -> Publish
```

No cache and no network request are needed.

### `routing_cache`

Shows how `MemoryDecisionCache` avoids repeated fallback decisions.

```bash
go run ./examples/routing_cache
```

The example submits the same state twice. The fallback selector is called once; the second decision is served from cache.

Flow:

```text
first call:  state -> cache miss -> fallback -> cache write
second call: state -> cache hit
```

### `routing_jev`

Full intelligent routing with deterministic rules, TTL cache, and Jev fallback through OpenRouter.

```bash
OPENROUTER_API_KEY=... go run ./examples/routing_jev
```

Flow:

```text
rules -> cache -> Jev -> cache write -> Subscribe -> Publish
```

The example exits without making a request when `OPENROUTER_API_KEY` is not set.

## Core event bus

| Example | Purpose |
|---|---|
| `hello_world` | Minimal direct event dispatch |
| `publisher` | Publishing multiple event types |
| `goroutines-subscribe-publisher` | Concurrent producers and publisher loop |
| `middleware` | Middleware and lifecycle hooks |
| `drop_oldest` | `DropOldest` back-pressure |
| `return_error` | `ReturnError` back-pressure |
| `publisher_timeout` | Blocking publisher timeout |
| `handler_timeout` | Handler context timeout |
| `fasthttp` | fasthttp integration |

## Choosing a routing path

Use direct `Subscribe` when your application already knows the projection.

Use rules when the decision is deterministic.

Use cache when equivalent routing decisions repeat.

Use Jev when the input is ambiguous and the application needs to choose one event type from a bounded candidate set.
