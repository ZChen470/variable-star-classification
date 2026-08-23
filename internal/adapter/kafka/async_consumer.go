package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
)

type AsyncConsumerRunner struct {
	consumer    consumerClient
	dispatcher  recordDispatcher
	completion  <-chan RecordCompletion
	coordinator *kafkaCommitCoordinator

	inflight chan struct{}
}

func newAsyncConsumerRunner(
	consumer consumerClient,
	dispatcher recordDispatcher,
	completion <-chan RecordCompletion,
	coordinator *kafkaCommitCoordinator,
	maxInflight int,
) (*AsyncConsumerRunner, error) {

	if consumer == nil {
		return nil, errors.New(
			"create async consumer runner: nil consumer",
		)
	}

	if dispatcher == nil {
		return nil, errors.New(
			"create async consumer runner: nil dispatcher",
		)
	}

	if completion == nil {
		return nil, errors.New(
			"create async consumer runner: nil completion",
		)
	}

	if coordinator == nil {
		return nil, errors.New(
			"create async consumer runner: nil coordinator",
		)
	}

	if maxInflight < 1 {
		return nil, errors.New(
			"create async consumer runner: invalid max inflight",
		)
	}

	return &AsyncConsumerRunner{
		consumer:    consumer,
		dispatcher:  dispatcher,
		completion:  completion,
		coordinator: coordinator,
		inflight:    make(chan struct{}, maxInflight),
	}, nil
}

func (runner *AsyncConsumerRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New(
			"run async consumer: nil context",
		)
	}

	errCh := make(chan error, 1)

	go func() {
		for {
			select {
			case completion, ok := <-runner.completion:
				if !ok {
					return
				}
				if err := runner.coordinator.HandleCompletion(ctx, completion); err != nil {
					errCh <- err
					return
				}

				<-runner.inflight
			case <-ctx.Done():
				errCh <- nil
				return
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			return nil
		}

		fetches := runner.consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			runner.consumer.AllowRebalance()
			return nil
		}

		if fetches.IsClientClosed() {
			runner.consumer.AllowRebalance()
			return nil
		}

		if err := runner.dispatchFetches(ctx, fetches); err != nil {
			return err
		}

		runner.consumer.AllowRebalance()

		select {
		case err := <-errCh:
			return err
		default:

		}
	}
}

func (runner *AsyncConsumerRunner) dispatchFetches(
	ctx context.Context,
	fetches kgo.Fetches,
) error {

	if errs := fetches.Errors(); len(errs) > 0 {

		first := errs[0]

		return fmt.Errorf(
			"poll Kafka topic %q partition %d: %w",
			first.Topic,
			first.Partition,
			first.Err,
		)
	}

	iterator := fetches.RecordIter()

	for !iterator.Done() {
		record := iterator.Next()

		if record == nil {
			return errors.New(
				"poll Kafka message: nil record",
			)
		}

		select {
		case runner.inflight <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		if err := runner.dispatcher.Dispatch(ctx, record); err != nil {
			<-runner.inflight
			return err
		}
	}

	return nil
}
