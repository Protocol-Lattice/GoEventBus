package GoEventBus

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

// BatchHandlerFunc receives a slice of events at once. It returns one Result per
// input event (the slice may be nil or shorter than events — missing entries are
// treated as zero Results) and a single error covering the whole batch.
// Middleware is not applied to batch handlers; use lifecycle hooks for
// cross-cutting concerns.
type BatchHandlerFunc func(context.Context, []Event) ([]Result, error)

type batchEntry struct {
	handler BatchHandlerFunc
	size    int
}

// RegisterBatch registers h as a batch handler for projection. On each Publish
// call, all pending events for projection are collected and delivered to h in
// chunks of up to size events. Multiple calls for the same projection accumulate
// handlers (fan-out); each handler receives the full chunk independently.
// size must be positive; values ≤ 0 are clamped to 1.
func (es *EventStore) RegisterBatch(projection interface{}, size int, h BatchHandlerFunc) {
	if size <= 0 {
		size = 1
	}
	es.batchHandlers[projection] = append(es.batchHandlers[projection], batchEntry{handler: h, size: size})
}

// executeBatch runs a batch handler, fires lifecycle hooks, and routes failures
// to the DLQ. It never panics — panics in the handler are recovered and treated
// as errors, mirroring the behaviour of execute for single-event handlers.
func (es *EventStore) executeBatch(h BatchHandlerFunc, events []Event) {
	ctx := context.Background()
	for _, ev := range events {
		if ev.Ctx != nil {
			ctx = ev.Ctx
			break
		}
	}

	// Recover panics so the worker pool is never killed by a misbehaving handler.
	var (
		results []Result
		callErr error
	)
	func() {
		defer func() {
			if r := recover(); r != nil {
				if e, ok := r.(error); ok {
					callErr = fmt.Errorf("handler panic: %w", e)
				} else {
					callErr = fmt.Errorf("handler panic: %v", r)
				}
			}
		}()
		for _, ev := range events {
			for _, hook := range es.beforeHooks {
				hook(ctx, ev)
			}
		}
		results, callErr = h(ctx, events)
	}()

	atomic.AddUint64(&es.processedCount, uint64(len(events)))

	for i, ev := range events {
		var res Result
		if i < len(results) {
			res = results[i]
		}
		for _, hook := range es.afterHooks {
			hook(ctx, ev, res, callErr)
		}
	}

	if callErr != nil {
		atomic.AddUint64(&es.errorCount, 1)
		now := time.Now()
		for _, ev := range events {
			if es.DLQ != nil {
				es.DLQ.add(DeadLetter{Event: ev, Err: callErr, FailedAt: now, Attempts: 1})
			}
			for _, hook := range es.errorHooks {
				hook(ctx, ev, callErr)
			}
		}
	}
}
