package GoEventBus

import (
	"context"
	"sync"
	"time"
)

// DeadLetter holds a failed event and the reason it could not be processed.
type DeadLetter struct {
	Event    Event
	Err      error     // handler error, or a wrapped panic value
	FailedAt time.Time
	Attempts int       // 1 on first failure; incremented on each Replay call
}

// DeadLetterQueue is a thread-safe store for events that failed during dispatch.
// Attach one to an EventStore via store.DLQ to enable dead-letter routing.
//
//	store := GoEventBus.NewEventStore(&disp, 1<<16, GoEventBus.DropOldest)
//	store.DLQ = GoEventBus.NewDeadLetterQueue()
type DeadLetterQueue struct {
	mu      sync.Mutex
	entries []DeadLetter
}

// NewDeadLetterQueue returns an empty DeadLetterQueue ready for use.
func NewDeadLetterQueue() *DeadLetterQueue {
	return &DeadLetterQueue{}
}

func (q *DeadLetterQueue) add(dl DeadLetter) {
	q.mu.Lock()
	q.entries = append(q.entries, dl)
	q.mu.Unlock()
}

// Len returns the number of dead letters currently in the queue.
func (q *DeadLetterQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// Entries returns a snapshot copy of all dead letters without removing them.
func (q *DeadLetterQueue) Entries() []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]DeadLetter, len(q.entries))
	copy(out, q.entries)
	return out
}

// Drain removes and returns all dead letters, leaving the queue empty.
func (q *DeadLetterQueue) Drain() []DeadLetter {
	q.mu.Lock()
	defer q.mu.Unlock()
	out := q.entries
	q.entries = nil
	return out
}

// Replay re-enqueues all dead letters into store for reprocessing and calls
// store.Publish() once after. Each entry has its Attempts incremented before
// re-subscribing. Entries that fail to re-enqueue (e.g. buffer full) are kept
// in the queue. Returns the first Subscribe error encountered, or nil.
func (q *DeadLetterQueue) Replay(ctx context.Context, store *EventStore) error {
	q.mu.Lock()
	entries := q.entries
	q.entries = nil
	q.mu.Unlock()

	var failed []DeadLetter
	var firstErr error
	for _, dl := range entries {
		dl.Attempts++
		if err := store.Subscribe(ctx, dl.Event); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			dl.Err = err
			failed = append(failed, dl)
		}
	}

	if len(failed) > 0 {
		q.mu.Lock()
		q.entries = append(failed, q.entries...)
		q.mu.Unlock()
	}

	if firstErr == nil {
		store.Publish()
	}
	return firstErr
}
