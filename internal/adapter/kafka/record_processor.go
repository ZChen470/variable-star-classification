package kafka

import (
	"context"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordProcessor interface {
	Process(ctx context.Context, record *kgo.Record) error
}

type synchronousRecordProcessor struct {
	handler  application.MessageHandler
	consumer interface {
		CommitRecords(context.Context, ...*kgo.Record) error
	}
}

func newSynchronousRecordProcessor(
	handler application.MessageHandler,
	consumer interface {
		CommitRecords(context.Context, ...*kgo.Record) error
	},
) *synchronousRecordProcessor {
	return &synchronousRecordProcessor{
		handler:  handler,
		consumer: consumer,
	}
}

func (processor *synchronousRecordProcessor) Process(ctx context.Context, record *kgo.Record) error {
	if err := processor.handler.Handle(ctx, inboundMessageFromRecord(record)); err != nil {
		return fmt.Errorf("handle Kafka record topic %q partition %d offset %d: %w",
			record.Topic,
			record.Partition,
			record.Offset,
			err,
		)
	}

	if err := processor.consumer.CommitRecords(
		ctx,
		record,
	); err != nil {
		return fmt.Errorf(
			"commit Kafka record topic %q partition %d offset %d: %w",
			record.Topic,
			record.Partition,
			record.Offset,
			err,
		)
	}

	return nil
}
