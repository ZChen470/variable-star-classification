package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestRunWorkloadPacedStopsAfterFirstClassificationFailure(t *testing.T) {
	plan, err := newRatePlan(100, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}

	wantErr := errors.New("injected classification failure")
	calls := 0

	started := time.Now()
	outcome := runWorkload(
		context.Background(),
		plan,
		plan.requests,
		true,
		1,
		time.Second,
		func(ctx context.Context, input application.ClassificationInput) error {
			calls++
			return wantErr
		},
	)

	if !errors.Is(outcome.firstErr, wantErr) {
		t.Fatalf("firstErr = %v, want injected failure", outcome.firstErr)
	}
	if outcome.dispatchErr == nil {
		t.Fatal("expected dispatch to stop after classification failure")
	}
	if outcome.submitted < 1 || outcome.submitted >= plan.requests {
		t.Fatalf("submitted = %d, want 1..%d", outcome.submitted, plan.requests-1)
	}
	if calls != outcome.submitted {
		t.Fatalf("classify calls = %d, submitted = %d", calls, outcome.submitted)
	}
	if elapsed := time.Since(started); elapsed >= time.Second {
		t.Fatalf("workload did not stop promptly: %s", elapsed)
	}

	for index := 0; index < outcome.submitted; index++ {
		if !errors.Is(outcome.results[index].err, wantErr) {
			t.Fatalf("result[%d].err = %v, want injected failure",
				index, outcome.results[index].err)
		}
	}

	t.Logf("planned=%d submitted=%d classify_calls=%d elapsed=%s first_error=%v",
		plan.requests, outcome.submitted, calls, outcome.elapsed, outcome.firstErr)
}

func TestRunWorkloadUnpacedPreservesRequestCount(t *testing.T) {
	const requests = 8
	calls := 0

	outcome := runWorkload(
		context.Background(),
		ratePlan{},
		requests,
		false,
		1,
		time.Second,
		func(ctx context.Context, input application.ClassificationInput) error {
			calls++
			return nil
		},
	)

	if outcome.dispatchErr != nil || outcome.firstErr != nil {
		t.Fatalf("unexpected errors: dispatch=%v first=%v",
			outcome.dispatchErr, outcome.firstErr)
	}
	if outcome.submitted != requests || calls != requests {
		t.Fatalf("submitted=%d calls=%d want=%d",
			outcome.submitted, calls, requests)
	}
	for index, item := range outcome.results {
		if item.err != nil {
			t.Fatalf("result[%d].err = %v", index, item.err)
		}
	}
}
