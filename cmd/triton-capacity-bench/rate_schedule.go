package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

const (
	maxRateDuration       = 10 * time.Minute
	maxRateRequests       = 100000
	lateDispatchThreshold = 250 * time.Millisecond
	maxDispatchLag        = time.Second
	dispatchWindow        = 30 * time.Second
	minWindowRateFraction = 0.95
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
	if submit == nil {
		return 0, fmt.Errorf("invalid rate scheduler arguments")
	}
	return dispatchRateWithDue(ctx, plan, func(ctx context.Context, index int, _ time.Time) error {
		return submit(ctx, index)
	})
}

func dispatchRateWithDue(
	ctx context.Context,
	plan ratePlan,
	submit func(context.Context, int, time.Time) error,
) (int, error) {
	if ctx == nil || submit == nil || plan.rate < 1 || plan.requests < 1 {
		return 0, fmt.Errorf("invalid rate scheduler arguments")
	}

	start := time.Now()
	submitted := 0
	windowStartSubmitted := 0
	windowsChecked := 0

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
		err := submit(submitCtx, index, due)
		submitWait := time.Since(submitStarted)
		cancel()
		if err != nil {
			return submitted, fmt.Errorf(
				"rate scheduler could not submit request %d: pre_submit_lag=%s submit_wait=%s total_due_lag=%s budget=%s: %w",
				index, preSubmitLag, submitWait, time.Since(due), maxDispatchLag, err,
			)
		}

		acceptedAt := time.Now()
		if plan.duration > 0 && !acceptedAt.Before(start.Add(plan.duration)) {
			return submitted + 1, fmt.Errorf(
				"rate scheduler request %d accepted after dispatch duration: elapsed=%s duration=%s",
				index, acceptedAt.Sub(start), plan.duration,
			)
		}

		// Count only jobs accepted before each fixed 30-second boundary.
		// The current job belongs to the new window if it crossed a boundary.
		windowNumber := int(acceptedAt.Sub(start) / dispatchWindow)
		if windowNumber > windowsChecked {
			windowCount := submitted - windowStartSubmitted
			if err := checkDispatchWindow(plan.rate, windowCount, windowsChecked+1); err != nil {
				return submitted + 1, err
			}
			log.Printf(
				"rate scheduler window: window=%d submitted=%d actual_rate=%.2f/s target_rate=%d/s",
				windowsChecked+1, windowCount,
				float64(windowCount)/dispatchWindow.Seconds(), plan.rate,
			)
			windowStartSubmitted = submitted
			windowsChecked = windowNumber
		}

		submitted++
		if lag := time.Since(due); lag > lateDispatchThreshold {
			log.Printf(
				"rate scheduler late dispatch: request=%d due_lag=%s threshold=%s limit=%s",
				index, lag, lateDispatchThreshold, maxDispatchLag,
			)
		}
		if lag := time.Since(due); lag > maxDispatchLag {
			return submitted, fmt.Errorf(
				"rate scheduler overloaded: dispatch lag %s exceeds %s after request %d",
				lag, maxDispatchLag, index,
			)
		}
	}

	// A complete planned window must reach its wall-clock end before
	// its final dispatch count can be accepted.
	if plan.duration >= dispatchWindow && plan.duration%dispatchWindow == 0 {
		if err := waitForDurationEnd(ctx, start, plan.duration); err != nil {
			return submitted, err
		}
	}

	finalCount, err := checkFinalDispatchWindow(
		plan, submitted, windowStartSubmitted, windowsChecked,
	)
	if err != nil {
		return submitted, err
	}
	if finalCount > 0 {
		log.Printf(
			"rate scheduler final window: window=%d submitted=%d actual_rate=%.2f/s target_rate=%d/s",
			int(plan.duration/dispatchWindow), finalCount,
			float64(finalCount)/dispatchWindow.Seconds(), plan.rate,
		)
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

func checkDispatchWindow(rate, submitted, windowNumber int) error {
	if rate <= 0 || submitted < 0 || windowNumber < 1 {
		return fmt.Errorf("invalid dispatch window arguments")
	}
	actualRate := float64(submitted) / dispatchWindow.Seconds()
	minRate := float64(rate) * minWindowRateFraction
	if actualRate < minRate {
		return fmt.Errorf(
			"rate scheduler sustained under-target dispatch: window=%d submitted=%d actual_rate=%.2f/s target_rate=%d/s minimum_rate=%.2f/s",
			windowNumber, submitted, actualRate, rate, minRate,
		)
	}
	return nil
}

// checkFinalDispatchWindow checks the last complete planned window, which
// otherwise has no subsequent dispatch to trigger its boundary check.
func checkFinalDispatchWindow(
	plan ratePlan,
	submitted, windowStartSubmitted, windowsChecked int,
) (int, error) {
	if plan.duration < dispatchWindow || plan.duration%dispatchWindow != 0 {
		return 0, nil
	}

	fullWindows := int(plan.duration / dispatchWindow)
	if windowsChecked == fullWindows {
		return 0, nil
	}
	if windowsChecked != fullWindows-1 {
		return 0, fmt.Errorf(
			"unexpected final dispatch window: checked=%d expected=%d",
			windowsChecked, fullWindows-1,
		)
	}

	count := submitted - windowStartSubmitted
	if err := checkDispatchWindow(plan.rate, count, fullWindows); err != nil {
		return 0, err
	}
	return count, nil
}

func waitForDurationEnd(ctx context.Context, start time.Time, duration time.Duration) error {
	remaining := time.Until(start.Add(duration))
	if remaining <= 0 {
		return ctx.Err()
	}

	timer := time.NewTimer(remaining)
	defer timer.Stop()

	select {
	case <-timer.C:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}
