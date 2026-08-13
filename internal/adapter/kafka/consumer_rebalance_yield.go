package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type rebalanceYieldConsumerClient interface {
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches

	CommitRecords(ctx context.Context, records ...*kgo.Record) error

	AllowRebalance()
}

// RebalanceYieldConsumerRunner processes at most one Kafka record per poll and
// yields the current in-flight record when BlockRebalanceOnPoll reports that a
// rebalance callback is blocked.
type RebalanceYieldConsumerRunner struct {
	consumer rebalanceYieldConsumerClient
	handler  application.MessageHandler
	yield    *RebalanceYield
}

func NewRebalanceYieldConsumerRunner(
	client *kgo.Client,
	handler application.MessageHandler,
	yield *RebalanceYield,
) (*RebalanceYieldConsumerRunner, error) {
	if client == nil {
		return nil, errors.New("create rebalance-yield Kafka consumer runner: nil client")
	}
	if handler == nil {
		return nil, errors.New("create rebalance-yield Kafka consumer runner: nil handler")
	}
	if yield == nil {
		return nil, errors.New("create rebalance-yield Kafka consumer runner: nil rebalance yield")
	}

	autoCommitDisabled, ok := client.OptValue(kgo.DisableAutoCommit).(bool)
	if !ok || !autoCommitDisabled {
		return nil, errors.New(
			"create rebalance-yield Kafka consumer runner: DisableAutoCommit is required",
		)
	}

	rebalanceBlock, ok := client.OptValue(kgo.BlockRebalanceOnPoll).(bool)
	if !ok || !rebalanceBlock {
		return nil, errors.New(
			"create rebalance-yield Kafka consumer runner: BlockRebalanceOnPoll is required",
		)
	}

	return newRebalanceYieldConsumerRunner(client, handler, yield), nil
}

func newRebalanceYieldConsumerRunner(
	consumer rebalanceYieldConsumerClient,
	handler application.MessageHandler,
	yield *RebalanceYield,
) *RebalanceYieldConsumerRunner {
	return &RebalanceYieldConsumerRunner{
		consumer: consumer,
		handler:  handler,
		yield:    yield,
	}
}

func (runner *RebalanceYieldConsumerRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run rebalance-yield Kafka consumer: nil context")
	}
	if runner == nil || runner.consumer == nil {
		return errors.New("run rebalance-yield Kafka consumer: nil consumer")
	}
	if runner.handler == nil {
		return errors.New("run rebalance-yield Kafka consumer: nil handler")
	}
	if runner.yield == nil {
		return errors.New("run rebalance-yield Kafka consumer: nil rebalance yield")
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		generation := runner.yield.Generation()
		fetches := runner.consumer.PollRecords(ctx, 1)

		if ctx.Err() != nil {
			runner.consumer.AllowRebalance()
			return nil
		}

		if fetches.IsClientClosed() {
			runner.consumer.AllowRebalance()
			return nil
		}

		if err := firstFetchError(fetches); err != nil {
			runner.consumer.AllowRebalance()
			return err
		}

		iterator := fetches.RecordIter()
		if iterator.Done() {
			runner.consumer.AllowRebalance()
			continue
		}

		record := iterator.Next()
		if record == nil {
			runner.consumer.AllowRebalance()
			return errors.New("poll Kafka message: nil record")
		}

		recordContext, release := runner.yield.Bind(ctx, generation)

		handleErr := runner.handler.Handle(
			recordContext,
			inboundMessageFromRecord(record),
		)

		if runner.yield.RequestedSince(generation) {
			release()
			runner.consumer.AllowRebalance()
			continue
		}

		if handleErr != nil {
			release()
			runner.consumer.AllowRebalance()

			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf(
				"handle Kafka record topic %q partition %d offset %d: %w",
				record.Topic,
				record.Partition,
				record.Offset,
				handleErr,
			)
		}

		commitErr := runner.consumer.CommitRecords(recordContext, record)
		yielded := runner.yield.RequestedSince(generation)

		release()
		runner.consumer.AllowRebalance()

		if yielded {
			continue
		}

		if commitErr != nil {
			if ctx.Err() != nil {
				return nil
			}

			return fmt.Errorf(
				"commit Kafka record topic %q partition %d offset %d: %w",
				record.Topic,
				record.Partition,
				record.Offset,
				commitErr,
			)
		}
	}
}

func firstFetchError(fetches kgo.Fetches) error {
	fetchErrors := fetches.Errors()
	if len(fetchErrors) == 0 {
		return nil
	}

	first := fetchErrors[0]

	return fmt.Errorf(
		"poll Kafka topic %q partition %d: %w",
		first.Topic,
		first.Partition,
		first.Err,
	)
}
