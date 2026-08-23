package kafka

import (
	"context"
	"errors"
	"github.com/twmb/franz-go/pkg/kgo"
)

type commitRecordClient interface {
	CommitRecords(context.Context, ...*kgo.Record) error
}

type kafkaCommitCoordinator struct {
	tracker *partitionCompletionTracker
	manager *commitManager
}

type RecordCompletion struct {
	record *kgo.Record
	err    error
}

func newKafkaCommitCoordinator(
	tracker *partitionCompletionTracker,
	manager *commitManager,
) (*kafkaCommitCoordinator, error) {

	if tracker == nil {
		return nil, errors.New(
			"create Kafka commit coordinator: nil tracker",
		)
	}

	if manager == nil {
		return nil, errors.New(
			"create Kafka commit coordinator: nil manager",
		)
	}

	return &kafkaCommitCoordinator{
		tracker: tracker,
		manager: manager,
	}, nil
}

func (coordinator *kafkaCommitCoordinator) HandleCompletion(
	ctx context.Context,
	completion RecordCompletion,
) error {

	if completion.record == nil {
		return errors.New(
			"handle completion: nil record",
		)
	}

	if completion.err != nil {
		return nil
	}

	nextOffset, err :=
		coordinator.tracker.MarkCompleted(
			completion.record.Topic,
			completion.record.Partition,
			completion.record.Offset,
		)

	if err != nil {
		return err
	}

	commitRecord := &kgo.Record{
		Topic:     completion.record.Topic,
		Partition: completion.record.Partition,
		Offset:    nextOffset,
	}

	return coordinator.manager.AddWatermark(commitRecord)
}
