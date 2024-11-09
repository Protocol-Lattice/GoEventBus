package GoEventBus

import (
	"fmt"
	"reflect"
	"testing"
)

type HouseWasSold struct{}

func TestNewEventStore(t *testing.T) {
	dispatcher := Dispatcher{
		"tests.HouseWasSold": func(m map[string]any) (Result, error) {
			fmt.Println(m)

			return Result{
				Message: "Hello",
			}, nil
		},
	}
	got := &EventStore{Dispatcher: &dispatcher}
	want := NewEventStore(&dispatcher)
	if reflect.DeepEqual(got, want) {
		t.Errorf("EventStore wants %v, got %v", got, want)
	}
}

func TestNewEvent(t *testing.T) {
	args := map[string]any{
		"price": 100,
	}
	event := NewEvent(HouseWasSold{}, args)
	if !reflect.DeepEqual(args, event.Args) {
		t.Errorf("Args are not correrct got %T, wanted %T", event.Args, args)

	}
}
