package GoEventBus

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

var (
	// ErrInvalidEventRule is returned when a rule is missing a predicate or choice.
	ErrInvalidEventRule = errors.New("goeventbus: invalid event rule")
	// ErrDecisionCache is returned when strict cache mode is enabled and a cache
	// operation or cache-key calculation fails.
	ErrDecisionCache = errors.New("goeventbus: decision cache failure")
)

// EventRuleMatch decides whether a deterministic rule applies to the current
// state and candidate set.
type EventRuleMatch func(context.Context, any, []EventCandidate) bool

// EventRule routes matching state directly to Choice without invoking cache or
// a model. Rules are evaluated in order and the first match wins.
type EventRule struct {
	Name   string
	Choice string
	Match  EventRuleMatch
}

// DecisionCache stores event-routing decisions by a stable cache key.
//
// Implementations may be in-memory, Redis-backed, or distributed. The default
// RuleCacheSelector behavior is fail-open on cache errors.
type DecisionCache interface {
	Get(context.Context, string) (EventDecision, bool, error)
	Set(context.Context, string, EventDecision) error
}

// DecisionCacheKeyFunc creates a cache key from state and the candidate set.
type DecisionCacheKeyFunc func(any, []EventCandidate) (string, error)

// RuleCacheSelector implements the routing fast path:
//
//	rules -> cache -> fallback selector (for example Jev) -> cache write
//
// Rules always run before cache so a newly-added deterministic rule can
// override a previously cached model decision immediately.
type RuleCacheSelector struct {
	Rules []EventRule
	Cache DecisionCache

	// Fallback is called when no rule matches and cache misses. JevSelector is
	// the typical fallback.
	Fallback EventSelector

	// CacheKey defaults to DefaultDecisionCacheKey.
	CacheKey DecisionCacheKeyFunc

	// StrictCache turns cache/key failures into routing errors. The default
	// false value treats cache as an optimization and falls back normally.
	StrictCache bool
}

// SelectEvent evaluates deterministic rules, then cache, then Fallback.
func (s *RuleCacheSelector) SelectEvent(
	ctx context.Context,
	state any,
	candidates []EventCandidate,
) (EventDecision, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if s == nil {
		return EventDecision{}, ErrNilEventSelector
	}
	if len(candidates) == 0 {
		return EventDecision{}, ErrNoEventCandidates
	}

	validChoices := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		key := strings.TrimSpace(candidate.Key)
		if key == "" {
			return EventDecision{}, fmt.Errorf("%w: empty candidate key", ErrInvalidEventCandidate)
		}
		if _, exists := validChoices[key]; exists {
			return EventDecision{}, fmt.Errorf("%w: duplicate key %q", ErrInvalidEventCandidate, key)
		}
		validChoices[key] = struct{}{}
	}

	for i, rule := range s.Rules {
		choice := strings.TrimSpace(rule.Choice)
		if rule.Match == nil {
			return EventDecision{}, fmt.Errorf("%w: rule %d (%q) has nil Match", ErrInvalidEventRule, i, rule.Name)
		}
		if choice == "" {
			return EventDecision{}, fmt.Errorf("%w: rule %d (%q) has empty Choice", ErrInvalidEventRule, i, rule.Name)
		}
		if !rule.Match(ctx, state, candidates) {
			continue
		}
		if _, ok := validChoices[choice]; !ok {
			return EventDecision{}, fmt.Errorf("%w: rule %q selected %q", ErrUnknownEventChoice, rule.Name, choice)
		}
		return EventDecision{
			Choice:        choice,
			Confidence:    1,
			Probabilities: map[string]float64{choice: 1},
		}, nil
	}

	cacheKey := ""
	if s.Cache != nil {
		keyFunc := s.CacheKey
		if keyFunc == nil {
			keyFunc = DefaultDecisionCacheKey
		}

		key, err := keyFunc(state, candidates)
		if err != nil {
			if s.StrictCache {
				return EventDecision{}, fmt.Errorf("%w: build key: %v", ErrDecisionCache, err)
			}
		} else {
			cacheKey = key
			cached, ok, err := s.Cache.Get(ctx, cacheKey)
			if err != nil {
				if s.StrictCache {
					return EventDecision{}, fmt.Errorf("%w: get: %v", ErrDecisionCache, err)
				}
			} else if ok {
				if _, valid := validChoices[cached.Choice]; valid {
					return cached, nil
				}
			}
		}
	}

	if s.Fallback == nil {
		return EventDecision{}, ErrNilEventSelector
	}

	decision, err := s.Fallback.SelectEvent(ctx, state, candidates)
	if err != nil {
		return EventDecision{}, err
	}
	if _, ok := validChoices[decision.Choice]; !ok {
		return EventDecision{}, fmt.Errorf("%w: %q", ErrUnknownEventChoice, decision.Choice)
	}
	if s.Cache != nil && cacheKey != "" {
		if err := s.Cache.Set(ctx, cacheKey, decision); err != nil && s.StrictCache {
			return EventDecision{}, fmt.Errorf("%w: set: %v", ErrDecisionCache, err)
		}
	}

	return decision, nil
}

// DefaultDecisionCacheKey hashes JSON-serializable state plus the stable
// candidate keys and descriptions. Projection values are intentionally omitted:
// they are execution details and can include types that are not JSON-serializable.
//
// Candidate order does not affect the key.
func DefaultDecisionCacheKey(state any, candidates []EventCandidate) (string, error) {
	type cacheCandidate struct {
		Key         string `json:"key"`
		Description string `json:"description,omitempty"`
	}

	normalized := make([]cacheCandidate, len(candidates))
	for i, candidate := range candidates {
		normalized[i] = cacheCandidate{
			Key:         strings.TrimSpace(candidate.Key),
			Description: strings.TrimSpace(candidate.Description),
		}
	}
	sort.Slice(normalized, func(i, j int) bool {
		if normalized[i].Key == normalized[j].Key {
			return normalized[i].Description < normalized[j].Description
		}
		return normalized[i].Key < normalized[j].Key
	})

	payload := struct {
		State      any              `json:"state"`
		Candidates []cacheCandidate `json:"candidates"`
	}{
		State:      state,
		Candidates: normalized,
	}

	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode cache key input: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}

type memoryDecisionCacheEntry struct {
	decision  EventDecision
	expiresAt time.Time
}

// MemoryDecisionCache is a goroutine-safe, lazy-expiring decision cache.
//
// ttl <= 0 disables expiration. Expired entries are removed on access; no
// cleanup goroutine is started.
type MemoryDecisionCache struct {
	mu    sync.Mutex
	ttl   time.Duration
	items map[string]memoryDecisionCacheEntry
}

// NewMemoryDecisionCache creates an in-memory decision cache.
func NewMemoryDecisionCache(ttl time.Duration) *MemoryDecisionCache {
	return &MemoryDecisionCache{
		ttl:   ttl,
		items: make(map[string]memoryDecisionCacheEntry),
	}
}

// Get returns a cached decision when present and not expired.
func (c *MemoryDecisionCache) Get(
	ctx context.Context,
	key string,
) (EventDecision, bool, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return EventDecision{}, false, err
		}
	}
	if c == nil {
		return EventDecision{}, false, nil
	}

	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.items[key]
	if !ok {
		return EventDecision{}, false, nil
	}
	if !entry.expiresAt.IsZero() && !now.Before(entry.expiresAt) {
		delete(c.items, key)
		return EventDecision{}, false, nil
	}
	return cloneEventDecision(entry.decision), true, nil
}

// Set stores a decision using the cache's configured TTL.
func (c *MemoryDecisionCache) Set(
	ctx context.Context,
	key string,
	decision EventDecision,
) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	if c == nil {
		return nil
	}

	entry := memoryDecisionCacheEntry{decision: cloneEventDecision(decision)}
	if c.ttl > 0 {
		entry.expiresAt = time.Now().Add(c.ttl)
	}

	c.mu.Lock()
	c.items[key] = entry
	c.mu.Unlock()
	return nil
}

// Delete removes one cache entry.
func (c *MemoryDecisionCache) Delete(key string) {
	if c == nil {
		return
	}
	c.mu.Lock()
	delete(c.items, key)
	c.mu.Unlock()
}

// Clear removes all cache entries.
func (c *MemoryDecisionCache) Clear() {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.items = make(map[string]memoryDecisionCacheEntry)
	c.mu.Unlock()
}

func cloneEventDecision(decision EventDecision) EventDecision {
	cloned := decision
	if decision.Probabilities != nil {
		cloned.Probabilities = make(map[string]float64, len(decision.Probabilities))
		for key, value := range decision.Probabilities {
			cloned.Probabilities[key] = value
		}
	}
	return cloned
}
