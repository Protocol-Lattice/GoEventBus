package GoEventBus

import (
	"github.com/google/uuid"
)

type Event struct {
	Id         string
	Projection any
	Args       map[string]interface{}
}

// Dispatcher maps event projections to their respective handler functions
type Dispatcher map[any]func(map[string]interface{}) (Result, error)

func NewEvent(projection any, args map[string]any) *Event {
	id := uuid.New()
	return &Event{
		Id:         id.String(),
		Projection: projection,
		Args:       args,
	}
}

type Result struct {
	Message string
}
