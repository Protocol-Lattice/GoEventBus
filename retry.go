package GoEventBus

import (
	"context"
	"time"
)

// WithRetry returns a Middleware that re-invokes the next handler up to n
// additional times when it returns an error (total attempts = n+1).
// backoff is called with the 1-indexed retry number to determine the wait
// before each retry. Pass nil for no delay. If the context is cancelled
// during a backoff sleep the last error is returned immediately.
func WithRetry(n int, backoff func(retry int) time.Duration) Middleware {
	return func(next HandlerFunc) HandlerFunc {
		return func(ctx context.Context, ev Event) (Result, error) {
			var (
				res Result
				err error
			)
			for attempt := 0; attempt <= n; attempt++ {
				if attempt > 0 && backoff != nil {
					if delay := backoff(attempt); delay > 0 {
						select {
						case <-ctx.Done():
							return res, ctx.Err()
						case <-time.After(delay):
						}
					}
				}
				res, err = next(ctx, ev)
				if err == nil {
					return res, nil
				}
			}
			return res, err
		}
	}
}

// ConstantBackoff returns a backoff function that waits d before every retry.
func ConstantBackoff(d time.Duration) func(int) time.Duration {
	return func(_ int) time.Duration { return d }
}

// ExponentialBackoff returns a backoff function that doubles the base duration
// on each retry: base, 2×base, 4×base, …
func ExponentialBackoff(base time.Duration) func(int) time.Duration {
	return func(retry int) time.Duration {
		shift := retry - 1
		if shift > 62 {
			shift = 62
		}
		return base << uint(shift)
	}
}
