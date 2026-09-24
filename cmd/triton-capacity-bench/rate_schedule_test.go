package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestNewRatePlan(t *testing.T) {
	plan, err := newRatePlan(105, 7*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if plan.requests != 44100 || plan.rate != 105 {
		t.Fatalf("unexpected rate plan: %+v", plan)
	}

	partial, err := newRatePlan(3, 1100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if partial.requests != 4 {
		t.Fatalf("partial interval requests = %d, want 4", partial.requests)
	}
}

func TestNewRatePlanRejectsInvalidBounds(t *testing.T) {
	cases := []struct {
		rate     int
		duration time.Duration
	}{
		{0, time.Minute},
		{201, time.Minute},
		{10, time.Millisecond},
		{10, 11 * time.Minute},
		{200, 10 * time.Minute}, // 120000 planned requests.
	}
	for _, tc := range cases {
		if _, err := newRatePlan(tc.rate, tc.duration); err == nil {
			t.Fatalf("newRatePlan(%d, %s) unexpectedly succeeded", tc.rate, tc.duration)
		}
	}
}

func TestDispatchRateFollowsPlannedIntervals(t *testing.T) {
	plan := ratePlan{rate: 50, requests: 4}
	var dispatchTimes []time.Time
	submitted, err := dispatchRate(context.Background(), plan, func(ctx context.Context, index int) error {
		if index != len(dispatchTimes) {
			t.Fatalf("index = %d, want %d", index, len(dispatchTimes))
		}
		dispatchTimes = append(dispatchTimes, time.Now())
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if submitted != 4 || len(dispatchTimes) != 4 {
		t.Fatalf("submitted = %d, dispatches = %d", submitted, len(dispatchTimes))
	}
	if elapsed := dispatchTimes[3].Sub(dispatchTimes[0]); elapsed < 45*time.Millisecond {
		t.Fatalf("requests were dispatched too quickly: %s", elapsed)
	}
}

func TestDispatchRateReportsOverload(t *testing.T) {
	plan := ratePlan{rate: 100, requests: 3}
	calls := 0
	submitted, err := dispatchRate(context.Background(), plan, func(ctx context.Context, index int) error {
		calls++
		if index == 0 {
			time.Sleep(310 * time.Millisecond)
		}
		return nil
	})
	if err == nil || !strings.Contains(err.Error(), "overloaded") {
		t.Fatalf("expected explicit overload error, got %v", err)
	}
	if submitted != 1 || calls != 1 {
		t.Fatalf("submitted = %d, calls = %d; want 1, 1", submitted, calls)
	}
}

func TestDispatchRateStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	submitted, err := dispatchRate(ctx, ratePlan{rate: 100, requests: 3}, func(context.Context, int) error {
		t.Fatal("submit must not run after cancellation")
		return nil
	})
	if submitted != 0 || err != context.Canceled {
		t.Fatalf("submitted = %d, err = %v; want 0, context.Canceled", submitted, err)
	}
}

func TestDispatchRateRejectsBlockedSubmission(t *testing.T) {
	submitted, err := dispatchRate(
		context.Background(),
		ratePlan{rate: 100, requests: 2},
		func(ctx context.Context, index int) error {
			<-ctx.Done()
			return ctx.Err()
		},
	)
	if submitted != 0 || err == nil || !strings.Contains(err.Error(), "could not submit") {
		t.Fatalf("submitted = %d, err = %v; want explicit submission failure", submitted, err)
	}
	for _, field := range []string{"pre_submit_lag=", "submit_wait=", "total_due_lag=", "budget="} {
		if !strings.Contains(err.Error(), field) {
			t.Fatalf("submission failure missing %s: %v", field, err)
		}
	}
}

func TestResolveWorkload(t *testing.T) {
	cases := []struct {
		name     string
		requests int
		rate     int
		duration time.Duration
		wantN    int
		wantPace bool
		wantErr  bool
	}{
		{"existing request mode", 800, 0, 0, 800, false, false},
		{"paced mode", 0, 105, 7 * time.Minute, 44100, true, false},
		{"missing mode", 0, 0, 0, 0, false, true},
		{"mixed modes", 800, 105, time.Minute, 0, false, true},
		{"rate without duration", 0, 105, 0, 0, false, true},
		{"duration without rate", 0, 0, time.Minute, 0, false, true},
		{"too many unpaced requests", 5001, 0, 0, 0, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, count, paced, err := resolveWorkload(tc.requests, tc.rate, tc.duration)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr = %v", err, tc.wantErr)
			}
			if err == nil && (count != tc.wantN || paced != tc.wantPace) {
				t.Fatalf("count = %d, paced = %v; want %d, %v", count, paced, tc.wantN, tc.wantPace)
			}
		})
	}
}

func TestDispatchRateSustained105(t *testing.T) {
	plan, err := newRatePlan(105, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	jobs := make(chan int)
	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for range jobs {
				time.Sleep(18 * time.Millisecond)
			}
		}()
	}

	started := time.Now()
	submitted, dispatchErr := dispatchRate(
		context.Background(),
		plan,
		func(ctx context.Context, index int) error {
			select {
			case jobs <- index:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		},
	)
	dispatchElapsed := time.Since(started)
	close(jobs)
	workers.Wait()

	if dispatchErr != nil {
		t.Fatal(dispatchErr)
	}
	if submitted != 210 {
		t.Fatalf("submitted = %d, want 210", submitted)
	}
	actualRate := float64(submitted) / dispatchElapsed.Seconds()
	if actualRate < 95 || actualRate > 115 {
		t.Fatalf("actual dispatch rate = %.2f/s, want 95..115/s", actualRate)
	}
	t.Logf("submitted=%d elapsed=%s actual_dispatch_rate=%.2f/s",
		submitted, dispatchElapsed, actualRate)
}

func TestDispatchRateStopsAfterWorkerFailure(t *testing.T) {
	plan, err := newRatePlan(100, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan int)
	workerDone := make(chan struct{})
	go func() {
		defer close(workerDone)
		<-jobs   // Simulate the first request being accepted.
		cancel() // Simulate the worker reporting its first request failure.
	}()

	started := time.Now()
	submitted, dispatchErr := dispatchRate(ctx, plan, func(submitCtx context.Context, index int) error {
		select {
		case jobs <- index:
			return nil
		case <-submitCtx.Done():
			return submitCtx.Err()
		}
	})
	<-workerDone

	if dispatchErr == nil {
		t.Fatal("expected dispatch to stop after worker failure")
	}
	if submitted < 1 || submitted >= plan.requests {
		t.Fatalf("submitted = %d, want at least 1 but fewer than %d", submitted, plan.requests)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("dispatch did not stop promptly: %s", elapsed)
	}
	t.Logf("planned=%d submitted=%d elapsed=%s err=%v",
		plan.requests, submitted, time.Since(started), dispatchErr)
}
