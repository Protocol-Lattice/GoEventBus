package GoEventBus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

var (
	// ErrNilEventSelector is returned when DecideAndSubscribe is called without
	// an EventSelector.
	ErrNilEventSelector = errors.New("goeventbus: event selector is nil")
	// ErrNoEventCandidates is returned when there are no event types to choose from.
	ErrNoEventCandidates = errors.New("goeventbus: no event candidates")
	// ErrInvalidEventCandidate is returned when a candidate has an empty or duplicate key.
	ErrInvalidEventCandidate = errors.New("goeventbus: invalid event candidate")
	// ErrUnknownEventChoice is returned when a selector returns a choice that was
	// not present in the supplied candidate set.
	ErrUnknownEventChoice = errors.New("goeventbus: selector returned unknown event choice")
)

// EventCandidate is a named event type that an EventSelector may choose.
//
// Key is the stable string exposed to the decision model. Projection is the
// actual GoEventBus dispatcher key, so callers can keep using typed struct
// projections. Description should explain when this event type is appropriate.
type EventCandidate struct {
	Key         string
	Projection  interface{}
	Description string
}

// EventDecision is the structured result of event-type selection.
type EventDecision struct {
	Choice        string
	Projection    interface{}
	Confidence    float64
	Probabilities map[string]float64
	Model         string
	RequestID     string
}

// EventSelector chooses one candidate event type from a fixed set.
type EventSelector interface {
	SelectEvent(context.Context, any, []EventCandidate) (EventDecision, error)
}

// DecideAndSubscribe asks selector to choose the event type, assigns the chosen
// projection to event, and enqueues it. Dispatch still follows the normal
// Subscribe -> Publish lifecycle.
func (es *EventStore) DecideAndSubscribe(
	ctx context.Context,
	selector EventSelector,
	state any,
	event Event,
	candidates []EventCandidate,
) (EventDecision, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if selector == nil {
		return EventDecision{}, ErrNilEventSelector
	}
	if len(candidates) == 0 {
		return EventDecision{}, ErrNoEventCandidates
	}

	byKey := make(map[string]EventCandidate, len(candidates))
	for _, candidate := range candidates {
		key := strings.TrimSpace(candidate.Key)
		if key == "" {
			return EventDecision{}, fmt.Errorf("%w: empty key", ErrInvalidEventCandidate)
		}
		if _, exists := byKey[key]; exists {
			return EventDecision{}, fmt.Errorf("%w: duplicate key %q", ErrInvalidEventCandidate, key)
		}
		candidate.Key = key
		byKey[key] = candidate
	}

	decision, err := selector.SelectEvent(ctx, state, candidates)
	if err != nil {
		return EventDecision{}, err
	}

	candidate, ok := byKey[decision.Choice]
	if !ok {
		return EventDecision{}, fmt.Errorf("%w: %q", ErrUnknownEventChoice, decision.Choice)
	}

	event.Projection = candidate.Projection
	if err := es.Subscribe(ctx, event); err != nil {
		return EventDecision{}, err
	}

	decision.Projection = candidate.Projection
	return decision, nil
}

// JevSelector selects event types with TypeSafe Jev through OpenRouter's
// Decisions API.
type JevSelector struct {
	APIKey       string
	Model        string
	Endpoint     string
	Instructions string
	Client       *http.Client
}

const (
	defaultJevModel    = "typesafe/jev-1.13"
	defaultJevEndpoint = "https://openrouter.ai/api/alpha/decisions"
)

type jevDecisionRequest struct {
	Model     string                 `json:"model"`
	State     any                    `json:"state"`
	Questions map[string]jevQuestion `json:"questions"`
}

type jevQuestion struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria"`
}

type jevDecisionResponse struct {
	Model   string `json:"model"`
	ID      string `json:"id"`
	Answers map[string]struct {
		Type          string             `json:"type"`
		Choice        string             `json:"choice"`
		Probabilities map[string]float64 `json:"probabilities"`
		Confidence    float64            `json:"confidence"`
	} `json:"answers"`
}

// SelectEvent sends one Jev choice question whose criteria are the available
// event types. It does not enqueue or execute anything itself.
func (s *JevSelector) SelectEvent(
	ctx context.Context,
	state any,
	candidates []EventCandidate,
) (EventDecision, error) {
	if strings.TrimSpace(s.APIKey) == "" {
		return EventDecision{}, errors.New("goeventbus: Jev API key is empty")
	}
	if len(candidates) == 0 {
		return EventDecision{}, ErrNoEventCandidates
	}

	criteria := make(map[string]string, len(candidates))
	for _, candidate := range candidates {
		key := strings.TrimSpace(candidate.Key)
		if key == "" {
			return EventDecision{}, fmt.Errorf("%w: empty key", ErrInvalidEventCandidate)
		}
		if _, exists := criteria[key]; exists {
			return EventDecision{}, fmt.Errorf("%w: duplicate key %q", ErrInvalidEventCandidate, key)
		}
		description := strings.TrimSpace(candidate.Description)
		if description == "" {
			description = "Use when the input should be handled as " + key
		}
		criteria[key] = description
	}

	model := strings.TrimSpace(s.Model)
	if model == "" {
		model = defaultJevModel
	}
	endpoint := strings.TrimSpace(s.Endpoint)
	if endpoint == "" {
		endpoint = defaultJevEndpoint
	}
	instructions := strings.TrimSpace(s.Instructions)
	if instructions == "" {
		instructions = "Choose the single event type that best matches the supplied state."
	}

	payload := jevDecisionRequest{
		Model: model,
		State: state,
		Questions: map[string]jevQuestion{
			"event_type": {
				Type:         "choice",
				Instructions: instructions,
				Criteria:     criteria,
			},
		},
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return EventDecision{}, fmt.Errorf("goeventbus: encode Jev request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return EventDecision{}, fmt.Errorf("goeventbus: create Jev request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.APIKey)
	req.Header.Set("Content-Type", "application/json")

	client := s.Client
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}

	resp, err := client.Do(req)
	if err != nil {
		return EventDecision{}, fmt.Errorf("goeventbus: Jev request failed: %w", err)
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return EventDecision{}, fmt.Errorf("goeventbus: read Jev response: %w", err)
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		message := strings.TrimSpace(string(responseBody))
		if len(message) > 1024 {
			message = message[:1024]
		}
		return EventDecision{}, fmt.Errorf("goeventbus: Jev returned %s: %s", resp.Status, message)
	}

	var decoded jevDecisionResponse
	if err := json.Unmarshal(responseBody, &decoded); err != nil {
		return EventDecision{}, fmt.Errorf("goeventbus: decode Jev response: %w", err)
	}

	answer, ok := decoded.Answers["event_type"]
	if !ok {
		return EventDecision{}, errors.New("goeventbus: Jev response missing event_type answer")
	}
	if answer.Type != "choice" {
		return EventDecision{}, fmt.Errorf("goeventbus: Jev returned unexpected answer type %q", answer.Type)
	}
	if _, ok := criteria[answer.Choice]; !ok {
		return EventDecision{}, fmt.Errorf("%w: %q", ErrUnknownEventChoice, answer.Choice)
	}

	return EventDecision{
		Choice:        answer.Choice,
		Confidence:    answer.Confidence,
		Probabilities: answer.Probabilities,
		Model:         decoded.Model,
		RequestID:     decoded.ID,
	}, nil
}
