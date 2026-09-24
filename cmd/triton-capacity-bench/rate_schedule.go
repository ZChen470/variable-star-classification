package main

import (
	"context"
	"fmt"
	"time"
)

const (
	maxRateDuration = 10 * time.Minute
	maxRateRequests = 100000
	maxDispatchLag  = 250 * time.Millisecond
)

type ratePlan struct {
	rate     int
	duration time.Duration
	requests int
}

func newRatePlan(rate int, duration time.Duration) (ratePlan, error) {
	if rate < 1 || rate > 200 {
		return ratePlan{}, fmt.Errorf("rate must be within 1..200 requests/s")
	}
	if duration < time.Second || duration > maxRateDuration {
		return ratePlan{}, fmt.Errorf("duration must be within 1s..10m")
	}

	// The supported bounds keep this integer calculation below int64 overflow.
	count := (int64(duration)*int64(rate) + int64(time.Second) - 1) / int64(time.Second)
	if count > maxRateRequests {
		return ratePlan{}, fmt.Errorf("planned requests %d exceed limit %d", count, maxRateRequests)
	}
	return ratePlan{rate: rate, duration: duration, requests: int(count)}, nil
}

// dispatchRate schedules requests against fixed times measured from a single
// start instant. The callback must respect its context when it cannot accept
// a job immediately. A persistent dispatch delay is an error, not throttling.
func dispatchRate(
	ctx context.Context,
	plan ratePlan,
	submit func(context.Context, int) error,
) (int, error) {
	if ctx == nil || submit == nil || plan.rate < 1 || plan.requests < 1 {
		return 0, fmt.Errorf("invalid rate scheduler arguments")
	}

	start := time.Now()
	submitted := 0

	for index := 0; index < plan.requests; index++ {
		if err := ctx.Err(); err != nil {
			return submitted, err
		}

		due := start.Add(time.Duration(int64(index) * int64(time.Second) / int64(plan.rate)))
		if wait := time.Until(due); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return submitted, ctx.Err()
			}
		}

		if lag := time.Since(due); lag > maxDispatchLag {
			return submitted, fmt.Errorf(
				"rate scheduler overloaded: dispatch lag %s exceeds %s at request %d",
				lag, maxDispatchLag, index,
			)
		}

		preSubmitLag := time.Since(due)
		submitStarted := time.Now()
		submitCtx, cancel := context.WithDeadline(ctx, due.Add(maxDispatchLag))
		err := submit(submitCtx, index)
		submitWait := time.Since(submitStarted)
		cancel()
		if err != nil {
			return submitted, fmt.Errorf(
				"rate scheduler could not submit request %d: pre_submit_lag=%s submit_wait=%s total_due_lag=%s budget=%s: %w",
				index, preSubmitLag, submitWait, time.Since(due), maxDispatchLag, err,
			)
		}

		submitted++
		if lag := time.Since(due); lag > maxDispatchLag {
			return submitted, fmt.Errorf(
				"rate scheduler overloaded: dispatch lag %s exceeds %s after request %d",
				lag, maxDispatchLag, index,
			)
		}
	}

	return submitted, nil
}

// resolveWorkload keeps the original finite-request mode separate from paced mode.
func resolveWorkload(requests, rate int, duration time.Duration) (ratePlan, int, bool, error) {
	if rate == 0 && duration == 0 {
		if requests < 1 || requests > 5000 {
			return ratePlan{}, 0, false, fmt.Errorf("unpaced mode requires -requests 1..5000")
		}
		return ratePlan{}, requests, false, nil
	}

	if requests != 0 {
		return ratePlan{}, 0, false, fmt.Errorf("-requests cannot be combined with -rate or -duration")
	}

	plan, err := newRatePlan(rate, duration)
	if err != nil {
		return ratePlan{}, 0, false, err
	}
	return plan, plan.requests, true, nil
}
