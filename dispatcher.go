package GoEventBus

type Event struct {
	Projection any
	Args       map[string]any
}

func NewEvent(projection any, args map[string]any) *Event {
	return &Event{
		Projection: projection,
		Args:       args,
	}
}

type Result struct {
	Message string
}

type Dispatcher map[any]func(map[string]any) (Result, error)
