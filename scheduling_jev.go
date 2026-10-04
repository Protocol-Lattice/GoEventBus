package GoEventBus

import (
	"context"
	"time"
)

// DecideAndSchedule asks selector to choose an event type immediately, assigns
// the chosen projection, and schedules the resolved event for t.
//
// Selection happens when DecideAndSchedule is called, not when the timer fires.
// This makes routing failures visible immediately and guarantees that the timer
// always represents one already-resolved event. The scheduled event follows the
// existing local Schedule semantics.
func (es *EventStore) DecideAndSchedule(
	ctx context.Context,
	t time.Time,
	selector EventSelector,
	state any,
	event Event,
	candidates []EventCandidate,
) (EventDecision, *time.Timer, error) {
	decision, selected, err := selectEvent(ctx, selector, state, event, candidates)
	if err != nil {
		return EventDecision{}, nil, err
	}

	return decision, es.Schedule(ctx, t, selected), nil
}

// DecideAndScheduleAfter asks selector to choose an event type immediately,
// assigns the chosen projection, and schedules the resolved event after d.
//
// Selection happens when DecideAndScheduleAfter is called. A non-positive
// duration preserves ScheduleAfter's immediate-execution behavior and returns
// a nil timer.
func (es *EventStore) DecideAndScheduleAfter(
	ctx context.Context,
	d time.Duration,
	selector EventSelector,
	state any,
	event Event,
	candidates []EventCandidate,
) (EventDecision, *time.Timer, error) {
	decision, selected, err := selectEvent(ctx, selector, state, event, candidates)
	if err != nil {
		return EventDecision{}, nil, err
	}

	return decision, es.ScheduleAfter(ctx, d, selected), nil
}
