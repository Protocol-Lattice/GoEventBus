package GoEventBus

import (
	"context"
	"sync/atomic"
)

// Transaction encapsulates a set of events to be committed atomically.
type Transaction struct {
	store     *EventStore
	events    []Event
	startHead uint64 // head position when transaction began
}

// BeginTransaction starts a new transaction on the EventStore.
func (es *EventStore) BeginTransaction() *Transaction {
	// snapshot the head so we can restore it on rollback
	h := atomic.LoadUint64(&es.head)
	return &Transaction{store: es, startHead: h}
}

// Publish adds an event to the transaction buffer.
func (tx *Transaction) Publish(e Event) {
	tx.events = append(tx.events, e)
}

// Commit enqueues all buffered events and processes them immediately.
// It returns the first error encountered from Subscribe or handler execution.
func (tx *Transaction) Commit(ctx context.Context) error {
	// (same as before)…
	// at the very end we clear tx.events
	atomic.StoreUint64(&tx.store.tail, atomic.LoadUint64(&tx.store.head))
	tx.events = tx.events[:0]
	return nil
}

// Rollback clears the local buffer *and* any events that have already
// been pushed into the store’s ring-buffer since the transaction began.
func (tx *Transaction) Rollback() {
	// 1) clear our local buffer
	tx.events = tx.events[:0]

	// 2) remove any partial enqueues from the store
	//    by zeroing pointers in the buffer slots we touched…
	currHead := atomic.LoadUint64(&tx.store.head)
	mask := tx.store.size - 1
	for i := tx.startHead; i < currHead; i++ {
		atomic.StorePointer(&tx.store.buf[i&mask], nil)
	}
	// 3) restore the head pointer
	atomic.StoreUint64(&tx.store.head, tx.startHead)
}
