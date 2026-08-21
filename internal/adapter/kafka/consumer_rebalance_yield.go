package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

var ErrRebalanceYielded = errors.New("kafka consumer rebalance yielded")

type rebalanceYieldConsumerClient interface {
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, records ...*kgo.Record) error
	AllowRebalance()
}

// RebalanceYieldConsumerRunner processes at most one Kafka record per poll.
// When a blocked rebalance requests a yield, the runner stops the current
// consumer session without committing the yielded record.
type RebalanceYieldConsumerRunner struct {
	consumer  rebalanceYieldConsumerClient
	processor recordProcessor
	yield     *RebalanceYield
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
		processor: newSynchronousRecordProcessor(
			handler,
			consumer,
		),
		yield: yield,
	}
}

func (runner *RebalanceYieldConsumerRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run rebalance-yield Kafka consumer: nil context")
	}
	if runner == nil || runner.consumer == nil {
		return errors.New("run rebalance-yield Kafka consumer: nil consumer")
	}
	if runner.processor == nil {
		return errors.New("run rebalance-yield Kafka consumer: nil processor")
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

		handleErr := runner.processor.Process(
			recordContext,
			record,
		)

		if runner.yield.RequestedSince(generation) {
			release()
			runner.consumer.AllowRebalance()
			return ErrRebalanceYielded
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

		yielded := runner.yield.RequestedSince(generation)

		release()
		runner.consumer.AllowRebalance()

		if yielded {
			return ErrRebalanceYielded
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
