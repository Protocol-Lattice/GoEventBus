package GoEventBus

import (
	"context"
	"sync/atomic"
)

// Transaction encapsulates a set of events to be committed atomically.
type Transaction struct {
	store  *EventStore
	events []Event
}

// BeginTransaction starts a new transaction on the EventStore.
func (es *EventStore) BeginTransaction() *Transaction {
	return &Transaction{store: es, events: nil}
}

// Publish adds an event to the transaction buffer.
func (tx *Transaction) Publish(e Event) {
	tx.events = append(tx.events, e)
}

// Commit enqueues all buffered events and processes them immediately.
// It returns the first error encountered from Subscribe or handler execution.
func (tx *Transaction) Commit(ctx context.Context) error {
	// Enqueue events
	for _, e := range tx.events {
		e.Ctx = ctx
		if err := tx.store.Subscribe(ctx, e); err != nil {
			// on subscribe failure, stop
			return err
		}
	}

	// Process events synchronously
	head := atomic.LoadUint64(&tx.store.head)
	tail := atomic.LoadUint64(&tx.store.tail)
	disp := *tx.store.dispatcher
	mask := tx.store.size - 1

	for i := tail; i < head; i++ {
		ptr := atomic.LoadPointer(&tx.store.buf[i&mask])
		if ptr == nil {
			continue
		}
		ev := *(*Event)(ptr)
		if handler, ok := disp[ev.Projection]; ok {
			// pick up recorded context
			cctx := ev.Ctx
			if c2, ok2 := ev.Args["__ctx"].(context.Context); ok2 && c2 != nil {
				cctx = c2
			}

			// before hooks
			for _, hook := range tx.store.beforeHooks {
				hook(cctx, ev)
			}

			// wrap middlewares
			wrapped := handler
			for j := len(tx.store.middlewares) - 1; j >= 0; j-- {
				wrapped = tx.store.middlewares[j](wrapped)
			}

			// invoke handler
			res, err := wrapped(cctx, ev.Args)
			atomic.AddUint64(&tx.store.processedCount, 1)

			// after hooks
			for _, hook := range tx.store.afterHooks {
				hook(cctx, ev, res, err)
			}

			if err != nil {
				atomic.AddUint64(&tx.store.errorCount, 1)
				for _, hook := range tx.store.errorHooks {
					hook(cctx, ev, err)
				}
				// advance tail and exit on first handler error
				atomic.StoreUint64(&tx.store.tail, head)
				return err
			}
		}
	}

	// advance tail past processed events
	atomic.StoreUint64(&tx.store.tail, head)
	// clear transaction buffer
	tx.events = tx.events[:0]
	return nil
}

// Rollback clears the events buffered in the transaction.
func (tx *Transaction) Rollback() {
	tx.events = tx.events[:0]
}
