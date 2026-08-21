package kafka

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/kgo"
)

type asyncRecordProcessor struct {
	next    recordProcessor
	workers int
	jobs    chan asyncRecordJob
}

type asyncRecordJob struct {
	ctx    context.Context
	record *kgo.Record
	done   chan error
}

func newAsyncRecordProcessor(
	next recordProcessor,
	workers int,
) (*asyncRecordProcessor, error) {
	if next == nil {
		return nil, errors.New(
			"create async record processor: nil next processor",
		)
	}

	if workers < 1 {
		return nil, errors.New(
			"create async record processor: workers must be >= 1",
		)
	}

	processor := &asyncRecordProcessor{
		next:    next,
		workers: workers,
		jobs:    make(chan asyncRecordJob),
	}

	for i := 0; i < workers; i++ {
		go processor.runWorker()
	}

	return processor, nil
}

func (processor *asyncRecordProcessor) Process(
	ctx context.Context,
	record *kgo.Record,
) error {
	done := make(chan error, 1)

	job := asyncRecordJob{
		ctx:    ctx,
		record: record,
		done:   done,
	}

	select {
	case processor.jobs <- job:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (processor *asyncRecordProcessor) runWorker() {
	for job := range processor.jobs {
		err := processor.next.Process(
			job.ctx,
			job.record,
		)

		job.done <- err
		close(job.done)
	}
}
