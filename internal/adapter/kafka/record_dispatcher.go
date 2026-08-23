package kafka

import (
	"context"
	"errors"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type recordDispatcher interface {
	Dispatch(ctx context.Context, record *kgo.Record) error
}

type synchronousRecordDispatcher struct {
	handler application.MessageHandler
	tracker *partitionCompletionTracker
}

func newSynchronousRecordDispatcher(
	handler application.MessageHandler,
	tracker *partitionCompletionTracker,
) (*synchronousRecordDispatcher, error) {
	if handler == nil {
		return nil, errors.New(
			"create record dispatcher: nil handler",
		)
	}

	if tracker == nil {
		return nil, errors.New(
			"create record dispatcher: nil tracker",
		)
	}

	return &synchronousRecordDispatcher{
		handler: handler,
		tracker: tracker,
	}, nil
}

func (dispatcher *synchronousRecordDispatcher) Dispatch(ctx context.Context, record *kgo.Record) error {
	err := dispatcher.handler.Handle(ctx, inboundMessageFromRecord(record))

	if err != nil {
		return err
	}

	_, err = dispatcher.tracker.MarkCompleted(record.Topic, record.Partition, record.Offset)

	return err
}
