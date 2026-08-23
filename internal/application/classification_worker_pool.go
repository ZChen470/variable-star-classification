package application

import (
	"context"
	"errors"
	"fmt"
)

type MessageSubmitter interface {
	Submit(ctx context.Context, message InboundMessage, completion func(error)) error
}

var _ MessageSubmitter = (*ClassificationWorkerPool)(nil)

type ClassificationWorkerPool struct {
	next    MessageHandler
	workers int
	jobs    chan workerPoolJob

	// Worker Pool 可观测性接入
	observer ClassificationWorkerPoolObserver
}

var _ MessageHandler = (*ClassificationWorkerPool)(nil)

type workerPoolJob struct {
	ctx        context.Context
	msg        InboundMessage
	completion func(error)
}

type WorkerPoolCompletion struct {
	Message InboundMessage
	Err     error
}

func NewClassificationWorkerPool(
	next MessageHandler,
	workers int,
) (*ClassificationWorkerPool, error) {
	return NewClassificationWorkerPoolWithObserver(next, workers, nil)
}

func NewClassificationWorkerPoolWithObserver(
	next MessageHandler,
	workers int,
	observer ClassificationWorkerPoolObserver,
) (*ClassificationWorkerPool, error) {
	if next == nil {
		return nil, fmt.Errorf("worker pool next handler is nil")
	}

	if workers < 1 {
		return nil, fmt.Errorf("worker pool workers must be >= 1")
	}

	pool := &ClassificationWorkerPool{
		next:     next,
		workers:  workers,
		jobs:     make(chan workerPoolJob),
		observer: classificationWorkerPoolObserverOrNoop(observer),
	}

	for i := 0; i < workers; i++ {
		go pool.runWorker()
	}

	return pool, nil
}

func (pool *ClassificationWorkerPool) Handle(ctx context.Context, message InboundMessage) error {
	done := make(chan error, 1)

	err := pool.Submit(ctx, message, func(err error) {
		done <- err
	})

	if err != nil {
		return err
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// 新增异步接口
func (pool *ClassificationWorkerPool) Submit(ctx context.Context, message InboundMessage, completion func(error)) error {
	if ctx == nil {
		return errors.New("worker pool submit: nil context")
	}

	if completion == nil {
		return errors.New("worker pool submit: nil completion func")
	}

	job := workerPoolJob{
		ctx:        ctx,
		msg:        message,
		completion: completion,
	}

	select {
	case pool.jobs <- job:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (pool *ClassificationWorkerPool) SubmitWithChannel(ctx context.Context, message InboundMessage) (<-chan WorkerPoolCompletion, error) {
	result := make(chan WorkerPoolCompletion, 1)

	err := pool.Submit(ctx, message, func(err error) {
		result <- WorkerPoolCompletion{
			Message: message,
			Err:     err,
		}

		close(result)
	})

	if err != nil {
		close(result)
		return nil, err
	}

	return result, nil
}

func (pool *ClassificationWorkerPool) runWorker() {
	for job := range pool.jobs {
		pool.observer.WorkerStarted()

		err := pool.next.Handle(job.ctx, job.msg)

		pool.observer.WorkerFinished()

		job.completion(err)
	}
}

type ClassificationWorkerPoolObserver interface {
	WorkerStarted()
	WorkerFinished()
}

type noopClassificationWorkerPoolObserver struct{}

func (noopClassificationWorkerPoolObserver) WorkerStarted()  {}
func (noopClassificationWorkerPoolObserver) WorkerFinished() {}

func classificationWorkerPoolObserverOrNoop(
	observer ClassificationWorkerPoolObserver,
) ClassificationWorkerPoolObserver {
	if observer == nil {
		return noopClassificationWorkerPoolObserver{}
	}

	return observer
}
