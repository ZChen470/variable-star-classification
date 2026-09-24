package main

import (
	"context"
	"sync"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

type classifyFunc func(context.Context, application.ClassificationInput) error

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
	jobs := make(chan int)
	results := make([]result, requestCount)
	var workers sync.WaitGroup

	workers.Add(concurrency)
	for workerID := 0; workerID < concurrency; workerID++ {
		go func() {
			defer workers.Done()
			input := benchmarkInput()

			for index := range jobs {
				ctx, cancel := context.WithTimeout(requestBaseCtx, timeout)
				started := time.Now()
				callErr := classify(ctx, input)
				callElapsed := time.Since(started)
				cancel()

				results[index] = result{
					duration: callElapsed,
					err:      callErr,
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
		submitted, dispatchErr = dispatchRate(
			pacedCtx,
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
	} else {
		for index := 0; index < requestCount; index++ {
			jobs <- index
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
