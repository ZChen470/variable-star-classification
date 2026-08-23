package kafka

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type commitBatcher struct {
	consumer commitRecordClient
	pending  map[commitKey]*kgo.Record
}

type commitKey struct {
	topic     string
	partition int32
}

func newCommitBatcher(consumer commitRecordClient) (*commitBatcher, error) {
	if consumer == nil {
		return nil, errors.New(
			"create commit batcher: nil consumer",
		)
	}

	return &commitBatcher{
		consumer: consumer,
		pending: make(
			map[commitKey]*kgo.Record,
		),
	}, nil
}

func (batcher *commitBatcher) Add(record *kgo.Record) error {
	if record == nil {
		return errors.New("add commit record: nil record")
	}

	key := commitKey{
		topic:     record.Topic,
		partition: record.Partition,
	}

	existing, ok := batcher.pending[key]
	if !ok || record.Offset > existing.Offset {
		batcher.pending[key] = record
	}

	return nil
}

func (batcher *commitBatcher) Flush(ctx context.Context) error {
	if len(batcher.pending) == 0 {
		return nil
	}

	records := make([]*kgo.Record, 0, len(batcher.pending))

	for _, record := range batcher.pending {
		records = append(records, record)
	}

	if err := batcher.consumer.CommitRecords(ctx, records...); err != nil {
		return err
	}

	batcher.pending = make(map[commitKey]*kgo.Record)

	return nil
}
