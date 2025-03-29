package GoEventBus

import (
	"fmt"
	"sync"
	"testing"
)

type testProjection struct{}
type unknownProjection struct{}

// TestCommitSuccess verifies that an event is processed successfully
// when a valid handler exists in the dispatcher.
func TestCommitSuccess(t *testing.T) {
	var callCount int
	dummyHandler := func(args map[string]interface{}) (Result, error) {
		callCount++
		return Result{Message: "handled"}, nil
	}
	dispatcher := Dispatcher{
		testProjection{}: dummyHandler,
	}
	store := NewEventStore(&dispatcher)
	event := NewEvent(testProjection{}, map[string]interface{}{"key": "value"})
	store.Publish(event)
	err := store.Commit()
	if err != nil {
		t.Errorf("expected no error, got %v", err)
	}
	if callCount != 1 {
		t.Errorf("expected handler to be called once, but got %d", callCount)
	}
}

// TestCommitNoHandler verifies that Commit returns an error when no handler exists for the event.
func TestCommitNoHandler(t *testing.T) {
	dispatcher := Dispatcher{} // empty dispatcher
	store := NewEventStore(&dispatcher)
	event := NewEvent(unknownProjection{}, map[string]interface{}{"key": "value"})
	store.Publish(event)
	err := store.Commit()
	errText := fmt.Sprintf("no handler for event projection: %s", unknownProjection{})
	if err == nil {
		t.Errorf("expected error for missing handler, got nil")
	} else if err.Error() != errText {
		t.Errorf("unexpected error: %v", err)
	}
}

// TestCommitDispatcherNil verifies that Commit returns an error when the dispatcher is nil.
func TestCommitDispatcherNil(t *testing.T) {
	// Manually create an EventStore with a nil Dispatcher.
	store := &EventStore{
		Dispatcher: nil,
		Events: &sync.Pool{
			New: func() interface{} {
				return &Event{}
			},
		},
	}
	event := NewEvent(testProjection{}, map[string]interface{}{"key": "value"})
	store.Publish(event)
	err := store.Commit()
	if err == nil || err.Error() != "dispatcher is nil" {
		t.Errorf("expected 'dispatcher is nil' error, got %v", err)
	}
}

// TestBroadcast verifies that Broadcast processes all events in the pool.
func TestBroadcast(t *testing.T) {
	var callCount int
	dummyHandler := func(args map[string]interface{}) (Result, error) {
		callCount++
		return Result{Message: "handled"}, nil
	}
	dispatcher := Dispatcher{
		testProjection{}: dummyHandler,
	}
	store := NewEventStore(&dispatcher)
	event1 := NewEvent(testProjection{}, map[string]interface{}{"key": "value1"})
	event2 := NewEvent(testProjection{}, map[string]interface{}{"key": "value2"})
	store.Publish(event1)
	store.Publish(event2)
	_ = store.Broadcast()
	// Broadcast will process all published events and eventually return an error when no more events are available.

	if callCount != 2 {
		t.Errorf("expected 2 events handled, got %d", callCount)
	}
}
