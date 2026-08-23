package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type asyncConsumerClient interface {
	PollRecords(ctx context.Context, maxRecords int) kgo.Fetches
	CommitRecords(ctx context.Context, records ...*kgo.Record) error
	AllowRebalance()
}

// AsyncConsumerRunner polls a bounded batch, processes different Kafka keys in
// parallel, and commits only the continuously completed prefix of each
// partition. Records with the same non-empty key are processed in poll order.
//
// A batch remains inside one BlockRebalanceOnPoll ownership window. If franz-go
// reports a blocked rebalance, RebalanceYield cancels the whole batch and the
// caller must create a fresh consumer session from committed offsets.
type AsyncConsumerRunner struct {
	consumer    asyncConsumerClient
	handler     application.MessageHandler
	yield       *RebalanceYield
	concurrency int
}

func NewAsyncConsumerRunner(
	client *kgo.Client,
	handler application.MessageHandler,
	yield *RebalanceYield,
	concurrency int,
) (*AsyncConsumerRunner, error) {
	if client == nil {
		return nil, errors.New("create async Kafka consumer runner: nil client")
	}

	autoCommitDisabled, ok := client.OptValue(kgo.DisableAutoCommit).(bool)
	if !ok || !autoCommitDisabled {
		return nil, errors.New(
			"create async Kafka consumer runner: DisableAutoCommit is required",
		)
	}

	rebalanceBlocked, ok := client.OptValue(kgo.BlockRebalanceOnPoll).(bool)
	if !ok || !rebalanceBlocked {
		return nil, errors.New(
			"create async Kafka consumer runner: BlockRebalanceOnPoll is required",
		)
	}

	return newAsyncConsumerRunner(client, handler, yield, concurrency)
}

func newAsyncConsumerRunner(
	consumer asyncConsumerClient,
	handler application.MessageHandler,
	yield *RebalanceYield,
	concurrency int,
) (*AsyncConsumerRunner, error) {
	if consumer == nil {
		return nil, errors.New("create async Kafka consumer runner: nil consumer")
	}
	if handler == nil {
		return nil, errors.New("create async Kafka consumer runner: nil handler")
	}
	if yield == nil {
		return nil, errors.New("create async Kafka consumer runner: nil rebalance yield")
	}
	if concurrency < 1 {
		return nil, errors.New("create async Kafka consumer runner: concurrency must be >= 1")
	}

	return &AsyncConsumerRunner{
		consumer:    consumer,
		handler:     handler,
		yield:       yield,
		concurrency: concurrency,
	}, nil
}

func (runner *AsyncConsumerRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run async Kafka consumer: nil context")
	}
	if runner == nil || runner.consumer == nil {
		return errors.New("run async Kafka consumer: nil consumer")
	}
	if runner.handler == nil {
		return errors.New("run async Kafka consumer: nil handler")
	}
	if runner.yield == nil {
		return errors.New("run async Kafka consumer: nil rebalance yield")
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		generation := runner.yield.Generation()
		fetches := runner.consumer.PollRecords(ctx, runner.concurrency)

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

		records, err := recordsFromFetches(fetches)
		if err != nil {
			runner.consumer.AllowRebalance()
			return err
		}
		if len(records) == 0 {
			runner.consumer.AllowRebalance()
			continue
		}

		batchContext, release := runner.yield.Bind(ctx, generation)
		commitRecords, processErr := runner.processBatch(batchContext, records)
		yielded := runner.yield.RequestedSince(generation)

		if yielded {
			release()
			runner.consumer.AllowRebalance()
			return ErrRebalanceYielded
		}

		if ctx.Err() != nil {
			release()
			runner.consumer.AllowRebalance()
			return nil
		}

		if len(commitRecords) > 0 {
			if err := runner.consumer.CommitRecords(ctx, commitRecords...); err != nil {
				release()
				runner.consumer.AllowRebalance()
				return fmt.Errorf("commit async Kafka record batch: %w", err)
			}
		}

		release()
		runner.consumer.AllowRebalance()

		if processErr != nil {
			return processErr
		}
	}
}

type recordCompletion struct {
	record *kgo.Record
	err    error
}

type batchFailure struct {
	record *kgo.Record
	err    error
}

func (runner *AsyncConsumerRunner) processBatch(
	ctx context.Context,
	records []*kgo.Record,
) ([]*kgo.Record, error) {
	tracker := newPartitionCompletionTracker()
	recordsByPartitionOffset := make(map[partitionKey]map[int64]*kgo.Record)

	for _, record := range records {
		if record == nil {
			return nil, errors.New("process async Kafka batch: nil record")
		}
		if err := tracker.Track(record.Topic, record.Partition, record.Offset); err != nil {
			return nil, err
		}

		key := partitionKey{topic: record.Topic, partition: record.Partition}
		if recordsByPartitionOffset[key] == nil {
			recordsByPartitionOffset[key] = make(map[int64]*kgo.Record)
		}
		recordsByPartitionOffset[key][record.Offset] = record
	}

	groups := groupRecordsForProcessing(records)
	processingContext, cancel := context.WithCancel(ctx)
	defer cancel()

	completions := make(chan recordCompletion, len(records))
	var workers sync.WaitGroup
	var failureOnce sync.Once
	var firstFailure batchFailure

	for _, group := range groups {
		group := group
		workers.Add(1)

		go func() {
			defer workers.Done()

			for _, record := range group {
				if processingContext.Err() != nil {
					return
				}

				err := runner.handler.Handle(
					processingContext,
					inboundMessageFromRecord(record),
				)

				if err != nil {
					failureOnce.Do(func() {
						firstFailure = batchFailure{record: record, err: err}
						cancel()
					})
				}

				completions <- recordCompletion{record: record, err: err}
				if err != nil {
					return
				}
			}
		}()
	}

	go func() {
		workers.Wait()
		close(completions)
	}()

	committable := make(map[partitionKey]*kgo.Record)
	for completion := range completions {
		if completion.err != nil {
			continue
		}

		completedOffset, advanced, err := tracker.MarkCompleted(
			completion.record.Topic,
			completion.record.Partition,
			completion.record.Offset,
		)
		if err != nil {
			failureOnce.Do(func() {
				firstFailure = batchFailure{record: completion.record, err: err}
				cancel()
			})
			continue
		}
		if !advanced {
			continue
		}

		key := partitionKey{
			topic:     completion.record.Topic,
			partition: completion.record.Partition,
		}
		committable[key] = recordsByPartitionOffset[key][completedOffset]
	}

	commitRecords := make([]*kgo.Record, 0, len(committable))
	for _, record := range committable {
		commitRecords = append(commitRecords, record)
	}
	sort.Slice(commitRecords, func(i, j int) bool {
		if commitRecords[i].Topic != commitRecords[j].Topic {
			return commitRecords[i].Topic < commitRecords[j].Topic
		}
		return commitRecords[i].Partition < commitRecords[j].Partition
	})

	if firstFailure.err != nil {
		return commitRecords, fmt.Errorf(
			"handle Kafka record topic %q partition %d offset %d: %w",
			firstFailure.record.Topic,
			firstFailure.record.Partition,
			firstFailure.record.Offset,
			firstFailure.err,
		)
	}

	return commitRecords, nil
}

func recordsFromFetches(fetches kgo.Fetches) ([]*kgo.Record, error) {
	records := make([]*kgo.Record, 0, fetches.NumRecords())
	iterator := fetches.RecordIter()

	for !iterator.Done() {
		record := iterator.Next()
		if record == nil {
			return nil, errors.New("poll Kafka message: nil record")
		}
		records = append(records, record)
	}

	return records, nil
}

type processingKey struct {
	topic     string
	partition int32
	key       string
	keyed     bool
}

func groupRecordsForProcessing(records []*kgo.Record) [][]*kgo.Record {
	groupsByKey := make(map[processingKey][]*kgo.Record)
	order := make([]processingKey, 0, len(records))

	for _, record := range records {
		key := recordProcessingKey(record)
		if _, exists := groupsByKey[key]; !exists {
			order = append(order, key)
		}
		groupsByKey[key] = append(groupsByKey[key], record)
	}

	groups := make([][]*kgo.Record, 0, len(order))
	for _, key := range order {
		groups = append(groups, groupsByKey[key])
	}

	return groups
}

func recordProcessingKey(record *kgo.Record) processingKey {
	if len(record.Key) > 0 {
		return processingKey{
			topic: record.Topic,
			key:   string(record.Key),
			keyed: true,
		}
	}

	// Missing keys cannot provide object identity. Serializing them per
	// partition preserves Kafka's strongest available ordering guarantee.
	return processingKey{
		topic:     record.Topic,
		partition: record.Partition,
	}
}
