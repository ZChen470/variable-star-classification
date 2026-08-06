package application_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestClassificationResultDLQHandlerPassesWriterSuccess(
	t *testing.T,
) {
	next := &classificationResultDLQTestHandler{}
	publisher := &classificationResultDLQTestPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			next,
			classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	if err := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if next.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			next.calls,
		)
	}

	if len(publisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(publisher.messages),
		)
	}
}

func TestClassificationResultDLQHandlerPublishesPermanentError(
	t *testing.T,
) {
	permanentError :=
		&application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeMalformedMessage,

			Field: "value",

			Cause: errors.New(
				"invalid protobuf",
			),
		}

	next := &classificationResultDLQTestHandler{
		err: permanentError,
	}

	publisher := &classificationResultDLQTestPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			next,
			classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	message := application.InboundMessage{
		Topic: classificationResultDecodeTopic,

		Partition: 3,
		Offset:    42,
		Key:       []byte("OBJ-RESULT-001"),
		Value:     []byte{0xff},
	}

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if next.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			next.calls,
		)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf(
			"DLQ publish count = %d, want 1",
			len(publisher.messages),
		)
	}

	published := publisher.messages[0]

	if published.Topic != classificationResultDLQTopic {
		t.Fatalf(
			"DLQ topic = %q, want %q",
			published.Topic,
			classificationResultDLQTopic,
		)
	}

	assertClassificationResultDLQHeader(
		t,
		published.Headers,
		application.ClassificationResultDLQHeaderErrorCode,
		string(
			application.
				ClassificationResultErrorCodeMalformedMessage,
		),
	)
}

func TestClassificationResultDLQHandlerReturnsTransientError(
	t *testing.T,
) {
	transientError := errors.New(
		"PostgreSQL temporarily unavailable",
	)

	next := &classificationResultDLQTestHandler{
		err: transientError,
	}

	publisher := &classificationResultDLQTestPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			next,
			classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	)

	if got != transientError {
		t.Fatalf(
			"Handle() error = %v, want original %v",
			got,
			transientError,
		)
	}

	if next.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			next.calls,
		)
	}

	if len(publisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(publisher.messages),
		)
	}
}

func TestClassificationResultDLQHandlerReturnsPublishFailure(
	t *testing.T,
) {
	permanentError :=
		&application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeRepositoryConflict,

			Field: "classification_run",

			Cause: application.ErrClassificationRunConflict,
		}

	next := &classificationResultDLQTestHandler{
		err: permanentError,
	}

	publishCause := errors.New(
		"Result DLQ Kafka unavailable",
	)

	publisher := &classificationResultDLQTestPublisher{
		err: publishCause,
	}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			next,
			classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{
			Topic: classificationResultDecodeTopic,
		},
	)

	if !errors.Is(got, publishCause) {
		t.Fatalf(
			"errors.Is(error, publishCause) = false; error=%v",
			got,
		)
	}

	if next.calls != 1 {
		t.Fatalf(
			"writer call count = %d, want 1",
			next.calls,
		)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf(
			"DLQ publish count = %d, want 1",
			len(publisher.messages),
		)
	}
}

func TestClassificationResultDLQHandlerReturnsCancelledContext(
	t *testing.T,
) {
	next := &classificationResultDLQTestHandler{}
	publisher := &classificationResultDLQTestPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			next,
			classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	got := handler.Handle(
		ctx,
		application.InboundMessage{},
	)

	if !errors.Is(got, context.Canceled) {
		t.Fatalf(
			"Handle() error = %v, want context.Canceled",
			got,
		)
	}

	if next.calls != 0 {
		t.Fatalf(
			"writer call count = %d, want 0",
			next.calls,
		)
	}

	if len(publisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(publisher.messages),
		)
	}
}

func TestNewClassificationResultDLQHandlerRejectsInvalidArguments(
	t *testing.T,
) {
	validNext :=
		&classificationResultDLQTestHandler{}

	validPublisher :=
		&classificationResultDLQTestPublisher{}

	tests := []struct {
		name string

		next      application.MessageHandler
		topic     string
		publisher application.MessagePublisher
	}{
		{
			name:      "nil writer handler",
			topic:     classificationResultDLQTopic,
			publisher: validPublisher,
		},
		{
			name:      "empty DLQ topic",
			next:      validNext,
			publisher: validPublisher,
		},
		{
			name:  "nil publisher",
			next:  validNext,
			topic: classificationResultDLQTopic,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err :=
				application.
					NewClassificationResultDLQHandler(
						test.next,
						test.topic,
						test.publisher,
					)

			if err == nil {
				t.Fatalf(
					"error = nil, handler = %#v",
					handler,
				)
			}
		})
	}
}

type classificationResultDLQTestHandler struct {
	err   error
	calls int
}

func (handler *classificationResultDLQTestHandler) Handle(
	_ context.Context,
	_ application.InboundMessage,
) error {
	handler.calls++
	return handler.err
}

type classificationResultDLQTestPublisher struct {
	err      error
	messages []application.OutboundMessage
}

func (publisher *classificationResultDLQTestPublisher) Publish(
	_ context.Context,
	message application.OutboundMessage,
) error {
	publisher.messages = append(
		publisher.messages,
		message,
	)

	return publisher.err
}
