package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	resultWriterOffsetResultTopic = "astro.classification.results.v1"

	resultWriterOffsetDLQTopic = "astro.classification.results.dlq.v1"
)

func TestClassificationResultWriterSuccessCommitsResultOffset(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	record := classificationResultWriterOffsetRecord(
		1,
		41,
	)

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},
		cancel:             cancel,
		cancelAfterCommits: 1,
	}

	writer := &classificationResultWriterOffsetHandler{}
	dlqPublisher :=
		&classificationResultWriterOffsetPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			writer,
			resultWriterOffsetDLQTopic,
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	runner := newConsumerRunner(
		consumer,
		handler,
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf(
			"ConsumerRunner.Run() error = %v",
			err,
		)
	}

	if writer.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			writer.calls,
		)
	}

	if len(dlqPublisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(dlqPublisher.messages),
		)
	}

	assertClassificationResultOffsetCommitted(
		t,
		consumer,
		record,
	)
}

func TestClassificationResultPermanentErrorDLQSuccessCommitsResultOffset(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	record := classificationResultWriterOffsetRecord(
		2,
		52,
	)

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},
		cancel:             cancel,
		cancelAfterCommits: 1,
	}

	writer := &classificationResultWriterOffsetHandler{
		err: &application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeMalformedMessage,

			Field: "value",

			Cause: errors.New(
				"invalid classification result",
			),
		},
	}

	dlqPublisher :=
		&classificationResultWriterOffsetPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			writer,
			resultWriterOffsetDLQTopic,
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	runner := newConsumerRunner(
		consumer,
		handler,
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf(
			"ConsumerRunner.Run() error = %v",
			err,
		)
	}

	if writer.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			writer.calls,
		)
	}

	if len(dlqPublisher.messages) != 1 {
		t.Fatalf(
			"DLQ publish count = %d, want 1",
			len(dlqPublisher.messages),
		)
	}

	published := dlqPublisher.messages[0]

	if published.Topic != resultWriterOffsetDLQTopic {
		t.Fatalf(
			"DLQ topic = %q, want %q",
			published.Topic,
			resultWriterOffsetDLQTopic,
		)
	}

	assertClassificationResultOffsetCommitted(
		t,
		consumer,
		record,
	)
}

func TestClassificationResultTransientWriterErrorDoesNotCommitOffset(
	t *testing.T,
) {
	repositoryCause := errors.New(
		"PostgreSQL temporarily unavailable",
	)

	record := classificationResultWriterOffsetRecord(
		3,
		63,
	)

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},
	}

	writer := &classificationResultWriterOffsetHandler{
		err: repositoryCause,
	}

	dlqPublisher :=
		&classificationResultWriterOffsetPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			writer,
			resultWriterOffsetDLQTopic,
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	runner := newConsumerRunner(
		consumer,
		handler,
	)

	got := runner.Run(
		context.Background(),
	)

	if !errors.Is(got, repositoryCause) {
		t.Fatalf(
			"ConsumerRunner.Run() error = %v, want cause %v",
			got,
			repositoryCause,
		)
	}

	if writer.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			writer.calls,
		)
	}

	if len(dlqPublisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(dlqPublisher.messages),
		)
	}

	if len(consumer.committed) != 0 {
		t.Fatalf(
			"committed record count = %d, want 0",
			len(consumer.committed),
		)
	}

	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance call count = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

func TestClassificationResultDLQPublishFailureDoesNotCommitOffset(
	t *testing.T,
) {
	publishCause := errors.New(
		"classification result DLQ unavailable",
	)

	record := classificationResultWriterOffsetRecord(
		4,
		74,
	)

	consumer := &fakeConsumerClient{
		fetches: []kgo.Fetches{
			fetchWithRecords(record),
		},
	}

	writer := &classificationResultWriterOffsetHandler{
		err: &application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeRepositoryConflict,

			Field: "classification_run",

			Cause: application.ErrClassificationRunConflict,
		},
	}

	dlqPublisher :=
		&classificationResultWriterOffsetPublisher{
			err: publishCause,
		}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			writer,
			resultWriterOffsetDLQTopic,
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	runner := newConsumerRunner(
		consumer,
		handler,
	)

	got := runner.Run(
		context.Background(),
	)

	if !errors.Is(got, publishCause) {
		t.Fatalf(
			"ConsumerRunner.Run() error = %v, want cause %v",
			got,
			publishCause,
		)
	}

	if writer.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			writer.calls,
		)
	}

	if len(dlqPublisher.messages) != 1 {
		t.Fatalf(
			"DLQ publish count = %d, want 1",
			len(dlqPublisher.messages),
		)
	}

	if len(consumer.committed) != 0 {
		t.Fatalf(
			"committed record count = %d, want 0",
			len(consumer.committed),
		)
	}

	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance call count = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

func classificationResultWriterOffsetRecord(
	partition int32,
	offset int64,
) *kgo.Record {
	return &kgo.Record{
		Topic:     resultWriterOffsetResultTopic,
		Partition: partition,
		Offset:    offset,
		Key:       []byte("OBJ-RESULT-OFFSET"),
		Value:     []byte{0x01, 0x02},

		Headers: []kgo.RecordHeader{
			{
				Key:   "traceparent",
				Value: []byte("trace-value"),
			},
		},

		Timestamp: time.Date(
			2026,
			time.August,
			6,
			21,
			0,
			0,
			0,
			time.UTC,
		),
	}
}

func assertClassificationResultOffsetCommitted(
	t *testing.T,
	consumer *fakeConsumerClient,
	wantRecord *kgo.Record,
) {
	t.Helper()

	if len(consumer.committed) != 1 {
		t.Fatalf(
			"committed record count = %d, want 1",
			len(consumer.committed),
		)
	}

	if consumer.committed[0] != wantRecord {
		t.Fatal(
			"committed record is not the consumed Result record",
		)
	}
}

type classificationResultWriterOffsetHandler struct {
	err   error
	calls int
}

func (
	handler *classificationResultWriterOffsetHandler,
) Handle(
	_ context.Context,
	_ application.InboundMessage,
) error {
	handler.calls++
	return handler.err
}

type classificationResultWriterOffsetPublisher struct {
	err      error
	messages []application.OutboundMessage
}

func (
	publisher *classificationResultWriterOffsetPublisher,
) Publish(
	_ context.Context,
	message application.OutboundMessage,
) error {
	publisher.messages = append(
		publisher.messages,
		message,
	)

	return publisher.err
}
