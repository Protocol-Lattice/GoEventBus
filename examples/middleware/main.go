package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Protocol-Lattice/GoEventBus"
)

// Example of a logging middleware that logs before and after handler execution.
func loggingMiddleware(next GoEventBus.HandlerFunc) GoEventBus.HandlerFunc {
	return func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
		start := time.Now()
		fmt.Printf("[Middleware] Starting handler, args=%v\n", args)
		res, err := next(ctx, args)
		fmt.Printf("[Middleware] Finished handler in %s, result=%+v, err=%v\n", time.Since(start), res, err)
		return res, err
	}
}

// beforeHook prints a message before the handler runs.
func beforeHook(ctx context.Context, ev GoEventBus.Event) {
	fmt.Printf("[Hook Before] Event ID=%s, Projection=%v, Args=%v\n", ev.ID, ev.Projection, ev.Args)
}

// afterHook prints a message after the handler completes (regardless of error).
func afterHook(ctx context.Context, ev GoEventBus.Event, res GoEventBus.Result, err error) {
	fmt.Printf("[Hook After] Event ID=%s done: result=%+v, err=%v\n", ev.ID, res, err)
}

// errorHook handles errors from the handler.
func errorHook(ctx context.Context, ev GoEventBus.Event, err error) {
	fmt.Printf("[Hook Error] Event ID=%s, error=%v\n", ev.ID, err)
}

func main() {
	// 1. Create dispatcher and register handlers
	disp := GoEventBus.Dispatcher{}
	disp["greet"] = func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
		name := args["name"].(string)
		msg := fmt.Sprintf("Hello, %s!", name)
		return GoEventBus.Result{Message: msg}, nil
	}
	// A handler that sometimes errors
	disp["fail"] = func(ctx context.Context, args map[string]any) (GoEventBus.Result, error) {
		return GoEventBus.Result{}, fmt.Errorf("intentional error")
	}

	// 2. Initialize EventStore
	store := GoEventBus.NewEventStore(&disp, 16, GoEventBus.DropOldest)

	// 3. Register middleware and hooks
	store.Use(loggingMiddleware)
	store.OnBefore(beforeHook)
	store.OnAfter(afterHook)
	store.OnError(errorHook)

	// 4. Publish a successful event
	e1 := GoEventBus.Event{
		ID:         "evt-1",
		Projection: "greet",
		Args:       map[string]any{"name": "Alice"},
	}
	store.Subscribe(context.Background(), e1)
	// 5. Publish a failing event
	e2 := GoEventBus.Event{
		ID:         "evt-2",
		Projection: "fail",
		Args:       map[string]any{},
	}
	store.Subscribe(context.Background(), e2)

	// 6. Process all queued events
	store.Publish()

	// 7. Retrieve and display metrics
	pub, proc, errs := store.Metrics()
	fmt.Printf("Metrics: published=%d, processed=%d, errors=%d\n", pub, proc, errs)
}
