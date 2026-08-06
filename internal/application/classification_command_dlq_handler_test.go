package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestClassificationCommandDLQHandlerPassesSuccess(
	t *testing.T,
) {
	next := &commandDLQTestHandler{}
	publisher := &commandDLQTestPublisher{}

	handler, err :=
		application.NewClassificationCommandDLQHandler(
			next,
			"astro.classification.commands.dlq.v1",
			publisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationCommandDLQHandler() error = %v",
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
			"worker call count = %d, want 1",
			next.calls,
		)
	}

	if len(publisher.messages) != 0 {
		t.Fatalf(
			"publisher call count = %d, want 0",
			len(publisher.messages),
		)
	}
}

func TestClassificationCommandDLQHandlerPublishesPermanentError(
	t *testing.T,
) {
	workerError := &application.ClassificationWorkerError{
		Code: application.
			ClassificationWorkerErrorCodeLightCurveInvalid,

		Class: application.
			ClassificationWorkerErrorClassPermanent,

		Operation: application.
			ClassificationWorkerOperationPrepareInput,

		Cause: errors.New("invalid light curve"),
	}

	next := &commandDLQTestHandler{
		err: workerError,
	}
	publisher := &commandDLQTestPublisher{}

	handler, err :=
		application.NewClassificationCommandDLQHandler(
			next,
			"astro.classification.commands.dlq.v1",
			publisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationCommandDLQHandler() error = %v",
			err,
		)
	}

	timestamp := time.Date(
		2026,
		time.August,
		6,
		7,
		30,
		0,
		0,
		time.UTC,
	)

	message := application.InboundMessage{
		Topic:     "astro.classification.commands.v1",
		Partition: 3,
		Offset:    42,
		Key:       []byte("OBJ-0001"),
		Value:     []byte{0xff, 0x00},

		Headers: []application.MessageHeader{
			{
				Key:   "traceparent",
				Value: []byte("original"),
			},
		},

		Timestamp: timestamp,
	}

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf(
			"publisher call count = %d, want 1",
			len(publisher.messages),
		)
	}

	published := publisher.messages[0]

	if published.Topic !=
		"astro.classification.commands.dlq.v1" {
		t.Fatalf(
			"DLQ topic = %q",
			published.Topic,
		)
	}

	if !published.Timestamp.Equal(timestamp) {
		t.Fatalf(
			"DLQ timestamp = %v, want %v",
			published.Timestamp,
			timestamp,
		)
	}

	assertCommandDLQHeader(
		t,
		published.Headers,
		application.
			ClassificationCommandDLQHeaderErrorCode,
		string(
			application.
				ClassificationWorkerErrorCodeLightCurveInvalid,
		),
	)

	assertCommandDLQHeader(
		t,
		published.Headers,
		application.
			ClassificationCommandDLQHeaderErrorClass,
		"PERMANENT",
	)

	assertCommandDLQHeader(
		t,
		published.Headers,
		application.
			ClassificationCommandDLQHeaderErrorOperation,
		string(
			application.
				ClassificationWorkerOperationPrepareInput,
		),
	)
}

func TestClassificationCommandDLQHandlerReturnsNonPermanentErrors(
	t *testing.T,
) {
	tests := []struct {
		name  string
		class application.ClassificationWorkerErrorClass
	}{
		{
			name: "retryable",
			class: application.
				ClassificationWorkerErrorClassRetryable,
		},
		{
			name: "cancelled",
			class: application.
				ClassificationWorkerErrorClassCancelled,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			workerError :=
				&application.ClassificationWorkerError{
					Code: application.
						ClassificationWorkerErrorCodeDependencyUnavailable,

					Class: test.class,

					Operation: application.
						ClassificationWorkerOperationPrepareInput,
				}

			next := &commandDLQTestHandler{
				err: workerError,
			}
			publisher := &commandDLQTestPublisher{}

			handler, err :=
				application.NewClassificationCommandDLQHandler(
					next,
					"astro.classification.commands.dlq.v1",
					publisher,
				)
			if err != nil {
				t.Fatalf(
					"constructor error = %v",
					err,
				)
			}

			got := handler.Handle(
				context.Background(),
				application.InboundMessage{},
			)

			if got != workerError {
				t.Fatalf(
					"Handle() error = %v, want original %v",
					got,
					workerError,
				)
			}

			if len(publisher.messages) != 0 {
				t.Fatalf(
					"publisher call count = %d, want 0",
					len(publisher.messages),
				)
			}
		})
	}
}

func TestClassificationCommandDLQHandlerReturnsPublishFailure(
	t *testing.T,
) {
	publishCause := errors.New("Kafka unavailable")

	next := &commandDLQTestHandler{
		err: &application.ClassificationWorkerError{
			Code: application.
				ClassificationWorkerErrorCodeResultInvalid,

			Class: application.
				ClassificationWorkerErrorClassPermanent,

			Operation: application.
				ClassificationWorkerOperationBuildRun,
		},
	}

	publisher := &commandDLQTestPublisher{
		err: publishCause,
	}

	handler, err :=
		application.NewClassificationCommandDLQHandler(
			next,
			"astro.classification.commands.dlq.v1",
			publisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{
			Topic: "astro.classification.commands.v1",
		},
	)

	var workerError *application.ClassificationWorkerError
	if !errors.As(got, &workerError) {
		t.Fatalf(
			"Handle() error = %v, want ClassificationWorkerError",
			got,
		)
	}

	if workerError.Class !=
		application.ClassificationWorkerErrorClassRetryable {
		t.Fatalf(
			"Class = %s, want RETRYABLE",
			workerError.Class,
		)
	}

	if workerError.Code !=
		application.
			ClassificationWorkerErrorCodeCommandDLQPublishFailed {
		t.Fatalf(
			"Code = %q",
			workerError.Code,
		)
	}

	if workerError.Operation !=
		application.
			ClassificationWorkerOperationPublishCommandDLQ {
		t.Fatalf(
			"Operation = %q",
			workerError.Operation,
		)
	}

	if !errors.Is(got, publishCause) {
		t.Fatalf(
			"errors.Is(error, publishCause) = false",
		)
	}
}

func TestClassificationCommandDLQHandlerReturnsBuildFailure(
	t *testing.T,
) {
	next := &commandDLQTestHandler{
		err: &application.ClassificationWorkerError{
			Class: application.
				ClassificationWorkerErrorClassPermanent,

			Operation: application.
				ClassificationWorkerOperationBuildRun,
		},
	}

	handler, err :=
		application.NewClassificationCommandDLQHandler(
			next,
			"astro.classification.commands.dlq.v1",
			&commandDLQTestPublisher{},
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	)

	var workerError *application.ClassificationWorkerError
	if !errors.As(got, &workerError) {
		t.Fatalf(
			"Handle() error = %v, want ClassificationWorkerError",
			got,
		)
	}

	if workerError.Class !=
		application.ClassificationWorkerErrorClassPermanent {
		t.Fatalf(
			"Class = %s, want PERMANENT",
			workerError.Class,
		)
	}

	if workerError.Code !=
		application.
			ClassificationWorkerErrorCodeCommandDLQInvalid {
		t.Fatalf(
			"Code = %q",
			workerError.Code,
		)
	}

	if workerError.Operation !=
		application.
			ClassificationWorkerOperationBuildCommandDLQ {
		t.Fatalf(
			"Operation = %q",
			workerError.Operation,
		)
	}
}

type commandDLQTestHandler struct {
	err   error
	calls int
}

func (handler *commandDLQTestHandler) Handle(
	_ context.Context,
	_ application.InboundMessage,
) error {
	handler.calls++
	return handler.err
}

type commandDLQTestPublisher struct {
	err      error
	messages []application.OutboundMessage
}

func (publisher *commandDLQTestPublisher) Publish(
	_ context.Context,
	message application.OutboundMessage,
) error {
	publisher.messages = append(
		publisher.messages,
		message,
	)

	return publisher.err
}

func assertCommandDLQHeader(
	t *testing.T,
	headers []application.MessageHeader,
	key string,
	wantValue string,
) {
	t.Helper()

	for _, header := range headers {
		if header.Key == key {
			if string(header.Value) != wantValue {
				t.Fatalf(
					"Header %q = %q, want %q",
					key,
					header.Value,
					wantValue,
				)
			}
			return
		}
	}

	t.Fatalf("Header %q not found", key)
}
