package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

type dispatcherTestHandler struct {
	err error
}

func (h dispatcherTestHandler) Handle(
	context.Context,
	application.InboundMessage,
) error {
	return h.err
}

func TestSynchronousRecordDispatcherMarksCompletion(
	t *testing.T,
) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()

	err := tracker.InitializePartition(
		"topic",
		0,
		100,
	)

	if err != nil {
		t.Fatal(err)
	}

	dispatcher, err := newSynchronousRecordDispatcher(
		dispatcherTestHandler{},
		tracker,
	)

	if err != nil {
		t.Fatal(err)
	}

	err = dispatcher.Dispatch(
		context.Background(),
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    100,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	got, err := tracker.MarkCompleted(
		"topic",
		0,
		101,
	)

	if err != nil {
		t.Fatal(err)
	}

	if got != 102 {
		t.Fatalf(
			"watermark=%d want=102",
			got,
		)
	}
}

func TestSynchronousRecordDispatcherPropagatesHandlerError(
	t *testing.T,
) {
	t.Parallel()

	expected := errors.New("failed")

	tracker := newPartitionCompletionTracker()

	_ = tracker.InitializePartition(
		"topic",
		0,
		100,
	)

	dispatcher, err := newSynchronousRecordDispatcher(
		dispatcherTestHandler{
			err: expected,
		},
		tracker,
	)

	if err != nil {
		t.Fatal(err)
	}

	err = dispatcher.Dispatch(
		context.Background(),
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    100,
		},
	)

	if !errors.Is(err, expected) {
		t.Fatalf(
			"err=%v want=%v",
			err,
			expected,
		)
	}
}
