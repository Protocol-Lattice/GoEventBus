package GoEventBus

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

// TestWithRetry_SucceedsOnSecondAttempt verifies that a handler that fails
// once and then succeeds is called exactly twice.
func TestWithRetry_SucceedsOnSecondAttempt(t *testing.T) {
	var calls int32
	handler := func(_ context.Context, _ Event) (Result, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			return Result{}, errors.New("transient")
		}
		return Result{Message: "ok"}, nil
	}

	res, err := WithRetry(3, nil)(handler)(context.Background(), Event{})
	if err != nil {
		t.Fatalf("expected success after retry; got %v", err)
	}
	if res.Message != "ok" {
		t.Errorf("unexpected result message: %q", res.Message)
	}
	if calls != 2 {
		t.Errorf("expected 2 calls; got %d", calls)
	}
}

// TestWithRetry_ExhaustsRetries verifies that after n+1 total attempts all
// failing, the last error is returned.
func TestWithRetry_ExhaustsRetries(t *testing.T) {
	sentinel := errors.New("permanent")
	var calls int32
	handler := func(_ context.Context, _ Event) (Result, error) {
		atomic.AddInt32(&calls, 1)
		return Result{}, sentinel
	}

	_, err := WithRetry(2, nil)(handler)(context.Background(), Event{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel error; got %v", err)
	}
	if calls != 3 {
		t.Errorf("expected 3 calls (1 + 2 retries); got %d", calls)
	}
}

// TestWithRetry_NoRetryOnSuccess verifies the handler is called exactly once
// when it succeeds on the first attempt.
func TestWithRetry_NoRetryOnSuccess(t *testing.T) {
	var calls int32
	handler := func(_ context.Context, _ Event) (Result, error) {
		atomic.AddInt32(&calls, 1)
		return Result{Message: "done"}, nil
	}

	_, err := WithRetry(5, nil)(handler)(context.Background(), Event{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call; got %d", calls)
	}
}

// TestWithRetry_ZeroRetries verifies that n=0 means a single attempt with no retry.
func TestWithRetry_ZeroRetries(t *testing.T) {
	sentinel := errors.New("fail")
	var calls int32
	handler := func(_ context.Context, _ Event) (Result, error) {
		atomic.AddInt32(&calls, 1)
		return Result{}, sentinel
	}

	_, err := WithRetry(0, nil)(handler)(context.Background(), Event{})
	if !errors.Is(err, sentinel) {
		t.Fatalf("expected sentinel; got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call; got %d", calls)
	}
}

// TestWithRetry_ContextCancelledDuringBackoff verifies that a cancelled context
// during a backoff sleep stops further retries immediately.
func TestWithRetry_ContextCancelledDuringBackoff(t *testing.T) {
	var calls int32
	handler := func(_ context.Context, _ Event) (Result, error) {
		atomic.AddInt32(&calls, 1)
		return Result{}, errors.New("fail")
	}

	ctx, cancel := context.WithCancel(context.Background())
	backoff := func(_ int) time.Duration {
		cancel()          // cancel before we sleep
		return time.Hour  // long enough that time.After won't fire first
	}

	_, err := WithRetry(5, backoff)(handler)(ctx, Event{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled; got %v", err)
	}
	if calls != 1 {
		t.Errorf("expected 1 call before cancellation; got %d", calls)
	}
}

// TestWithRetry_IntegratesWithEventStore verifies WithRetry works end-to-end
// inside an EventStore: an eventually-succeeding handler records no error.
func TestWithRetry_IntegratesWithEventStore(t *testing.T) {
	var calls int32
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, _ Event) (Result, error) {
		if atomic.AddInt32(&calls, 1) < 3 {
			return Result{}, errors.New("not ready")
		}
		return Result{Message: "ok"}, nil
	})

	es := NewEventStore(&disp, 8, DropOldest)
	es.Use(WithRetry(4, nil))

	_ = es.Subscribe(context.Background(), Event{Projection: "evt"})
	es.Publish()

	if calls != 3 {
		t.Errorf("expected 3 calls (fail, fail, succeed); got %d", calls)
	}
	_, _, errs := es.Metrics()
	if errs != 0 {
		t.Errorf("expected 0 errors after eventual success; got %d", errs)
	}
}

// TestWithRetry_PairsWithDLQ verifies that after all retries are exhausted the
// event lands in the DLQ exactly once.
func TestWithRetry_PairsWithDLQ(t *testing.T) {
	sentinel := errors.New("always fails")
	var calls int32
	disp := Dispatcher{}
	disp.Register("evt", func(_ context.Context, _ Event) (Result, error) {
		atomic.AddInt32(&calls, 1)
		return Result{}, sentinel
	})

	es := NewEventStore(&disp, 8, DropOldest)
	es.DLQ = NewDeadLetterQueue()
	es.Use(WithRetry(2, nil)) // 3 total attempts

	_ = es.Subscribe(context.Background(), Event{ID: "dlq-test", Projection: "evt"})
	es.Publish()

	if calls != 3 {
		t.Errorf("expected 3 total handler calls; got %d", calls)
	}
	if es.DLQ.Len() != 1 {
		t.Fatalf("expected 1 dead letter after retry exhaustion; got %d", es.DLQ.Len())
	}
	dl := es.DLQ.Entries()[0]
	if !errors.Is(dl.Err, sentinel) {
		t.Errorf("expected sentinel in dead letter; got %v", dl.Err)
	}
}

// TestConstantBackoff verifies the returned duration is always d.
func TestConstantBackoff(t *testing.T) {
	bf := ConstantBackoff(200 * time.Millisecond)
	for i := 1; i <= 5; i++ {
		if got := bf(i); got != 200*time.Millisecond {
			t.Errorf("retry %d: want 200ms; got %v", i, got)
		}
	}
}

// TestExponentialBackoff verifies durations double with each retry.
func TestExponentialBackoff(t *testing.T) {
	bf := ExponentialBackoff(100 * time.Millisecond)
	cases := []struct {
		retry int
		want  time.Duration
	}{
		{1, 100 * time.Millisecond},
		{2, 200 * time.Millisecond},
		{3, 400 * time.Millisecond},
		{4, 800 * time.Millisecond},
		{5, 1600 * time.Millisecond},
	}
	for _, c := range cases {
		if got := bf(c.retry); got != c.want {
			t.Errorf("retry %d: want %v; got %v", c.retry, c.want, got)
		}
	}
}
