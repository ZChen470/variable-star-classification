package main

import (
	"context"
	"sync"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

type classifyFunc func(context.Context, application.ClassificationInput) error

type scheduledJob struct {
	index int
	due   time.Time
}

type workloadOutcome struct {
	results         []result
	submitted       int
	dispatchErr     error
	firstErr        error
	dispatchElapsed time.Duration
	elapsed         time.Duration
}

func runWorkload(
	parent context.Context,
	plan ratePlan,
	requestCount int,
	paced bool,
	concurrency int,
	timeout time.Duration,
	classify classifyFunc,
) workloadOutcome {
	pacedCtx, cancelPaced := context.WithCancel(parent)
	defer cancelPaced()

	requestBaseCtx := parent
	if paced {
		requestBaseCtx = pacedCtx
	}

	firstFailure := make(chan error, 1)
	jobs := make(chan scheduledJob)
	results := make([]result, requestCount)
	var workers sync.WaitGroup

	workers.Add(concurrency)
	for workerID := 0; workerID < concurrency; workerID++ {
		go func() {
			defer workers.Done()
			input := benchmarkInput()

			for job := range jobs {
				ctx, cancel := context.WithTimeout(requestBaseCtx, timeout)
				started := time.Now()
				plannedToStart := time.Duration(0)
				if paced {
					plannedToStart = started.Sub(job.due)
				}
				callErr := classify(ctx, input)
				callElapsed := time.Since(started)
				cancel()

				results[job.index] = result{
					duration:       callElapsed,
					err:            callErr,
					plannedToStart: plannedToStart,
				}

				if paced && callErr != nil {
					select {
					case firstFailure <- callErr:
					default:
					}
					cancelPaced()
					return
				}
			}
		}()
	}

	started := time.Now()
	submitted := 0
	var dispatchErr error

	if paced {
		submitted, dispatchErr = dispatchRateWithDue(
			pacedCtx,
			plan,
			func(ctx context.Context, index int, due time.Time) error {
				select {
				case jobs <- scheduledJob{index: index, due: due}:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		)
	} else {
		for index := 0; index < requestCount; index++ {
			jobs <- scheduledJob{index: index}
			submitted++
		}
	}

	dispatchElapsed := time.Since(started)
	close(jobs)
	workers.Wait()
	elapsed := time.Since(started)

	var firstErr error
	select {
	case firstErr = <-firstFailure:
	default:
	}

	return workloadOutcome{
		results:         results,
		submitted:       submitted,
		dispatchErr:     dispatchErr,
		firstErr:        firstErr,
		dispatchElapsed: dispatchElapsed,
		elapsed:         elapsed,
	}
}
