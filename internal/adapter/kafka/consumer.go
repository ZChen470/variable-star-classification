package kafka

import (
	"context"
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type consumerClient interface {
	PollFetches(ctx context.Context) kgo.Fetches

	CommitRecords(ctx context.Context, records ...*kgo.Record) error

	AllowRebalance()
}

// ConsumerRunner 轮询、处理并提交 Kafka 消息
type ConsumerRunner struct {
	consumer consumerClient
	handler  application.MessageHandler
}

// NewConsumerRunner 创建 Consumer Runner
//
// Client 必须配置 DisableAutoCommit 和 BlockRebalanceOnPoll
func NewConsumerRunner(client *kgo.Client, handler application.MessageHandler) (*ConsumerRunner, error) {
	if client == nil {
		return nil, errors.New("create Kafka consumer runner: nil client")
	}

	if handler == nil {
		return nil, errors.New("create Kafka consumer runner: nil handler")
	}

	autoCommitDisabled, ok := client.OptValue(kgo.DisableAutoCommit).(bool)
	if !ok || !autoCommitDisabled {
		return nil, errors.New("create Kafka consumer runner: DisableAutoCommit is required")
	}

	rebalanceBlock, ok := client.OptValue(kgo.BlockRebalanceOnPoll).(bool)
	if !ok || !rebalanceBlock {
		return nil, errors.New("create Kafka consumer runner: BlockRebalanceOnPoll is required")
	}

	return newConsumerRunner(client, handler), nil
}

func newConsumerRunner(consumer consumerClient, handler application.MessageHandler) *ConsumerRunner {
	return &ConsumerRunner{
		consumer: consumer,
		handler:  handler,
	}
}

// Run 持续轮询消息，直到 context 取消或处理失败
func (runner *ConsumerRunner) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("run Kafka consumer: nil context")
	}

	if runner == nil || runner.consumer == nil {
		return errors.New(
			"run Kafka consumer: nil consumer",
		)
	}

	if runner.handler == nil {
		return errors.New("run Kafka consumer: nil handler")
	}

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

		err := runner.processFetches(ctx, fetches)

		// BlockRebalanceOnPoll 要求每轮处理后显式放行
		runner.consumer.AllowRebalance()

		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}
	}
}

func (runner *ConsumerRunner) processFetches(ctx context.Context, fetches kgo.Fetches) error {
	fetchErrors := fetches.Errors()
	if len(fetchErrors) > 0 {
		first := fetchErrors[0]

		return fmt.Errorf("poll Kafka topic %q partition %d: %w", first.Topic, first.Partition, first.Err)
	}

	iterator := fetches.RecordIter()

	for !iterator.Done() {
		record := iterator.Next()
		if record == nil {
			return errors.New("poll Kafka message: nil record")
		}

		message := inboundMessageFromRecord(record)

		if err := runner.handler.Handle(ctx, message); err != nil {
			return fmt.Errorf(
				"handle Kafka record topic %q partition %d offset %d: %w",
				record.Topic,
				record.Partition,
				record.Offset,
				err,
			)
		}

		if err := runner.consumer.CommitRecords(ctx, record); err != nil {
			return fmt.Errorf(
				"commit Kafka record topic %q partition %d offset %d: %w",
				record.Topic,
				record.Partition,
				record.Offset,
				err,
			)
		}
	}

	return nil
}

func inboundMessageFromRecord(record *kgo.Record) application.InboundMessage {
	message := application.InboundMessage{
		Topic:     record.Topic,
		Partition: record.Partition,
		Offset:    record.Offset,
		Key:       cloneBytes(record.Key),
		Value:     cloneBytes(record.Value),
		Timestamp: record.Timestamp,
	}

	if len(record.Headers) > 0 {
		message.Headers = make([]application.MessageHeader, 0, len(record.Headers))

		for _, header := range record.Headers {
			message.Headers = append(
				message.Headers,
				application.MessageHeader{
					Key:   header.Key,
					Value: cloneBytes(header.Value),
				},
			)
		}
	}

	return message
}
