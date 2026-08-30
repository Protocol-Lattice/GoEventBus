package GoEventBus

import (
	"context"
	"sync"
	"sync/atomic"
)

// Transaction buffers regular handler events until Commit or Rollback. It is
// deliberately isolated from the shared ring: the old implementation rewound
// global queue positions on Rollback and could therefore erase events owned by
// concurrent producers.
//
// Commit delivers buffered events synchronously and returns the first handler
// error. It does not use batch or ordered handlers, matching the historical
// transaction contract.
type Transaction struct {
	store    *EventStore
	commitMu sync.Mutex

	mu     sync.Mutex
	events []Event
}

// BeginTransaction starts a new transaction on the EventStore.
func (es *EventStore) BeginTransaction() *Transaction {
	return &Transaction{store: es}
}

// Publish adds an event to the transaction buffer.
func (tx *Transaction) Publish(e Event) {
	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.events = append(tx.events, e)
}

// Commit delivers the events buffered when the call begins. Events appended
// concurrently stay in the transaction for a later Commit.
func (tx *Transaction) Commit(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if !tx.store.enterOperation() {
		return ErrEventStoreClosed
	}
	defer tx.store.activeOps.Done()

	tx.commitMu.Lock()
	defer tx.commitMu.Unlock()

	tx.mu.Lock()
	events := append([]Event(nil), tx.events...)
	tx.mu.Unlock()

	for _, event := range events {
		event.Ctx = ctx
		atomic.AddUint64(&tx.store.publishedCount, 1)
		for _, handler := range (*tx.store.dispatcher)[event.Projection] {
			if err := tx.store.execute(handler, event); err != nil {
				return err
			}
		}
	}

	tx.mu.Lock()
	tx.events = tx.events[len(events):]
	tx.mu.Unlock()
	return nil
}

// Rollback discards only this transaction's local buffer. It never mutates
// the shared ring, so it is safe alongside ordinary producers and publishers.
func (tx *Transaction) Rollback() {
	tx.commitMu.Lock()
	defer tx.commitMu.Unlock()

	tx.mu.Lock()
	defer tx.mu.Unlock()
	tx.events = nil
}
