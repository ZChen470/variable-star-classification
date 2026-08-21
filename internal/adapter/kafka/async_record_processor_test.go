package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type asyncRecordProcessorTestProcessor struct {
	process func(context.Context, *kgo.Record) error
}

func (processor asyncRecordProcessorTestProcessor) Process(
	ctx context.Context,
	record *kgo.Record,
) error {
	return processor.process(ctx, record)
}

func TestAsyncRecordProcessorProcessesSuccessfully(t *testing.T) {
	t.Parallel()

	next := asyncRecordProcessorTestProcessor{
		process: func(
			context.Context,
			*kgo.Record,
		) error {
			return nil
		},
	}

	processor, err := newAsyncRecordProcessor(
		next,
		2,
	)

	if err != nil {
		t.Fatalf(
			"create processor: %v",
			err,
		)
	}

	err = processor.Process(
		context.Background(),
		&kgo.Record{},
	)

	if err != nil {
		t.Fatalf(
			"Process() error = %v",
			err,
		)
	}
}

func TestAsyncRecordProcessorPropagatesWorkerError(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("worker failed")

	next := asyncRecordProcessorTestProcessor{
		process: func(
			context.Context,
			*kgo.Record,
		) error {
			return expectedErr
		},
	}

	processor, err := newAsyncRecordProcessor(
		next,
		1,
	)

	if err != nil {
		t.Fatalf(
			"create processor: %v",
			err,
		)
	}

	err = processor.Process(
		context.Background(),
		&kgo.Record{},
	)

	if !errors.Is(err, expectedErr) {
		t.Fatalf(
			"Process() error = %v, want %v",
			err,
			expectedErr,
		)
	}
}

func TestAsyncRecordProcessorRunsWorkersConcurrently(t *testing.T) {
	t.Parallel()

	var active atomic.Int32
	var maxActive atomic.Int32

	started := make(chan struct{}, 2)
	release := make(chan struct{})

	next := asyncRecordProcessorTestProcessor{
		process: func(
			context.Context,
			*kgo.Record,
		) error {
			current := active.Add(1)

			for {
				old := maxActive.Load()

				if current <= old ||
					maxActive.CompareAndSwap(old, current) {
					break
				}
			}

			started <- struct{}{}

			<-release

			active.Add(-1)

			return nil
		},
	}

	processor, err := newAsyncRecordProcessor(
		next,
		2,
	)

	if err != nil {
		t.Fatalf(
			"create processor: %v", err,
		)
	}

	errCh := make(chan error, 2)

	var wg sync.WaitGroup

	for i := 0; i < 2; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			errCh <- processor.Process(
				context.Background(),
				&kgo.Record{},
			)
		}()
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first worker did not start")
	}

	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("second worker did not start")
	}

	if got := maxActive.Load(); got != 2 {
		t.Fatalf(
			"max active workers = %d, want 2",
			got,
		)
	}

	close(release)

	wg.Wait()

	close(errCh)

	for err := range errCh {
		if err != nil {
			t.Fatalf(
				"worker returned error: %v",
				err,
			)
		}
	}
}

func TestAsyncRecordProcessorHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	next := asyncRecordProcessorTestProcessor{
		process: func(
			context.Context,
			*kgo.Record,
		) error {
			return nil
		},
	}

	processor, err := newAsyncRecordProcessor(
		next,
		1,
	)

	if err != nil {
		t.Fatalf(
			"create processor: %v",
			err,
		)
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)

	cancel()

	err = processor.Process(
		ctx,
		&kgo.Record{},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"Process() error = %v, want context.Canceled",
			err,
		)
	}
}
