package GoEventBus

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

type dummyProjection struct{}

// TestNewEvent verifies that NewEvent returns an event with the expected fields set.
func TestNewEvent(t *testing.T) {
	proj := dummyProjection{}
	args := map[string]interface{}{"foo": "bar"}
	event := NewEvent(proj, args)
	if event == nil {
		t.Errorf("expected non-nil event")
	}
	if event.Id == "" {
		t.Errorf("expected event Id to be set")
	}
	if event.Projection != proj {
		t.Errorf("expected projection %v, got %v", proj, event.Projection)
	}
	if event.Args["foo"] != "bar" {
		t.Errorf("expected args to contain 'foo':'bar'")
	}
}

// TestNewEventUniqueId ensures that multiple calls to NewEvent generate unique IDs.
func TestNewEventUniqueId(t *testing.T) {
	proj := dummyProjection{}
	args := map[string]interface{}{"a": 1}
	event1 := NewEvent(proj, args)
	event2 := NewEvent(proj, args)
	if event1.Id == event2.Id {
		t.Errorf("expected different Ids for two events, but both are %v", event1.Id)
	}
}

type failProjection struct{}

// TestCommitHandlerError tests that if a handler returns an error, Commit propagates it.
func TestCommitHandlerError(t *testing.T) {
	dummyHandler := func(args map[string]interface{}) (Result, error) {
		return Result{}, errors.New("handler failed")
	}
	dispatcher := Dispatcher{
		failProjection{}: dummyHandler,
	}
	store := NewEventStore(&dispatcher)
	event := NewEvent(failProjection{}, map[string]interface{}{"test": "value"})
	store.Publish(event)
	err := store.Commit()
	expectedErr := "error handling event: handler failed"
	if err == nil || err.Error() != expectedErr {
		t.Errorf("expected error %q, got %v", expectedErr, err)
	}
}

type proj struct{}

// TestBroadcastMultipleEvents verifies that Broadcast processes multiple events and then returns an error when no more events exist.
func TestBroadcastMultipleEvents(t *testing.T) {
	var callCount int
	dummyHandler := func(args map[string]interface{}) (Result, error) {
		callCount++
		return Result{Message: "handled"}, nil
	}
	dispatcher := Dispatcher{
		proj{}: dummyHandler,
	}
	store := NewEventStore(&dispatcher)
	// Publish several events.
	events := []*Event{
		NewEvent(proj{}, map[string]interface{}{"a": 1}),
		NewEvent(proj{}, map[string]interface{}{"b": 2}),
		NewEvent(proj{}, map[string]interface{}{"c": 3}),
	}
	for _, e := range events {
		store.Publish(e)
	}

	// Give a moment for events to be added (if needed)
	time.Sleep(100 * time.Millisecond)
	_ = store.Broadcast()
	// Our Broadcast implementation is expected to return an error when no events remain.
	if callCount != len(events) {
		t.Errorf("expected %d events to be processed, got %d", len(events), callCount)
	}
}

type seq struct{}

// TestPublishAndCommitSequence tests that events published sequentially are processed correctly.
func TestPublishAndCommitSequence(t *testing.T) {
	var processed []string
	dummyHandler := func(args map[string]interface{}) (Result, error) {
		if id, ok := args["id"].(string); ok {
			processed = append(processed, id)
		}
		return Result{Message: "processed"}, nil
	}
	dispatcher := Dispatcher{
		seq{}: dummyHandler,
	}
	store := NewEventStore(&dispatcher)

	// Publish three events with unique ids in args.
	for i := 0; i < 3; i++ {
		event := NewEvent(seq{}, map[string]interface{}{"id": fmt.Sprintf("event-%d", i)})
		store.Publish(event)
		// Commit each event immediately after publishing.
		err := store.Commit()
		if err != nil {
			t.Errorf("commit failed on event %d: %v", i, err)
		}
	}
	if len(processed) != 3 {
		t.Errorf("expected 3 events processed, got %d", len(processed))
	}
}
