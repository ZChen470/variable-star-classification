package application_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestClassificationCommandHandlerRetriesThenSucceeds(
	t *testing.T,
) {
	firstError := newCommandHandlerRetryableError(
		application.ClassificationWorkerOperationPrepareInput,
	)

	secondError := newCommandHandlerRetryableError(
		application.ClassificationWorkerOperationClassify,
	)

	worker := &classificationCommandHandlerTestWorker{
		results: []error{
			firstError,
			secondError,
			nil,
		},
	}

	dlqPublisher :=
		&classificationCommandHandlerTestPublisher{}

	handler, err :=
		application.NewClassificationCommandHandler(
			worker,
			[]time.Duration{0, 0},
			"astro.classification.commands.dlq.v1",
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationCommandHandler() error = %v",
			err,
		)
	}

	if err := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if worker.calls != 3 {
		t.Fatalf(
			"worker call count = %d, want 3",
			worker.calls,
		)
	}

	if len(dlqPublisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(dlqPublisher.messages),
		)
	}
}

func TestClassificationCommandHandlerRoutesPermanentErrorToDLQ(
	t *testing.T,
) {
	permanentError :=
		&application.ClassificationWorkerError{
			Code: application.
				ClassificationWorkerErrorCodeLightCurveInvalid,

			Class: application.
				ClassificationWorkerErrorClassPermanent,

			Operation: application.
				ClassificationWorkerOperationPrepareInput,

			Cause: errors.New(
				"invalid light curve",
			),
		}

	worker := &classificationCommandHandlerTestWorker{
		results: []error{
			permanentError,
		},
	}

	dlqPublisher :=
		&classificationCommandHandlerTestPublisher{}

	handler, err :=
		application.NewClassificationCommandHandler(
			worker,
			[]time.Duration{0, 0},
			"astro.classification.commands.dlq.v1",
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	message := application.InboundMessage{
		Topic: "astro.classification.commands.v1",

		Partition: 4,
		Offset:    51,

		Key:   []byte("OBJ-0001"),
		Value: []byte{0x01, 0x02},

		Timestamp: time.Date(
			2026,
			time.August,
			6,
			8,
			0,
			0,
			0,
			time.UTC,
		),
	}

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if worker.calls != 1 {
		t.Fatalf(
			"worker call count = %d, want 1",
			worker.calls,
		)
	}

	if len(dlqPublisher.messages) != 1 {
		t.Fatalf(
			"DLQ publish count = %d, want 1",
			len(dlqPublisher.messages),
		)
	}

	if dlqPublisher.messages[0].Topic !=
		"astro.classification.commands.dlq.v1" {
		t.Fatalf(
			"DLQ topic = %q",
			dlqPublisher.messages[0].Topic,
		)
	}
}

func TestClassificationCommandHandlerRoutesPermanentErrorAfterRetryToDLQ(
	t *testing.T,
) {
	retryableError :=
		newCommandHandlerRetryableError(
			application.
				ClassificationWorkerOperationPrepareInput,
		)

	permanentError :=
		&application.ClassificationWorkerError{
			Code: application.
				ClassificationWorkerErrorCodeLightCurveInvalid,

			Class: application.
				ClassificationWorkerErrorClassPermanent,

			Operation: application.
				ClassificationWorkerOperationPrepareInput,
		}

	worker := &classificationCommandHandlerTestWorker{
		results: []error{
			retryableError,
			permanentError,
			nil,
		},
	}

	dlqPublisher :=
		&classificationCommandHandlerTestPublisher{}

	handler, err :=
		application.NewClassificationCommandHandler(
			worker,
			[]time.Duration{0, 0},
			"astro.classification.commands.dlq.v1",
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	if err := handler.Handle(
		context.Background(),
		application.InboundMessage{
			Topic: "astro.classification.commands.v1",
		},
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if worker.calls != 2 {
		t.Fatalf(
			"worker call count = %d, want 2",
			worker.calls,
		)
	}

	if len(dlqPublisher.messages) != 1 {
		t.Fatalf(
			"DLQ publish count = %d, want 1",
			len(dlqPublisher.messages),
		)
	}
}

func TestClassificationCommandHandlerReturnsRetryableAfterExhaustion(
	t *testing.T,
) {
	firstError := newCommandHandlerRetryableError(
		application.ClassificationWorkerOperationPrepareInput,
	)

	secondError := newCommandHandlerRetryableError(
		application.ClassificationWorkerOperationClassify,
	)

	lastError := newCommandHandlerRetryableError(
		application.ClassificationWorkerOperationPublishResult,
	)

	worker := &classificationCommandHandlerTestWorker{
		results: []error{
			firstError,
			secondError,
			lastError,
		},
	}

	dlqPublisher :=
		&classificationCommandHandlerTestPublisher{}

	handler, err :=
		application.NewClassificationCommandHandler(
			worker,
			[]time.Duration{0, 0},
			"astro.classification.commands.dlq.v1",
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	)

	if got != lastError {
		t.Fatalf(
			"Handle() error = %v, want last retryable error %v",
			got,
			lastError,
		)
	}

	if worker.calls != 3 {
		t.Fatalf(
			"worker call count = %d, want 3",
			worker.calls,
		)
	}

	if len(dlqPublisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(dlqPublisher.messages),
		)
	}
}

func TestClassificationCommandHandlerDoesNotRetryDLQPublishFailure(
	t *testing.T,
) {
	permanentError :=
		&application.ClassificationWorkerError{
			Code: application.
				ClassificationWorkerErrorCodeResultInvalid,

			Class: application.
				ClassificationWorkerErrorClassPermanent,

			Operation: application.
				ClassificationWorkerOperationBuildRun,
		}

	worker := &classificationCommandHandlerTestWorker{
		results: []error{
			permanentError,
			nil,
		},
	}

	publishCause := errors.New(
		"Kafka unavailable",
	)

	dlqPublisher :=
		&classificationCommandHandlerTestPublisher{
			err: publishCause,
		}

	handler, err :=
		application.NewClassificationCommandHandler(
			worker,
			[]time.Duration{0, 0},
			"astro.classification.commands.dlq.v1",
			dlqPublisher,
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

	if !errors.Is(got, publishCause) {
		t.Fatalf(
			"errors.Is(error, publishCause) = false",
		)
	}

	// 该断言冻结 DLQ(Retry(Worker)) 的组合顺序。
	// DLQ 发布失败不得再次执行 Worker。
	if worker.calls != 1 {
		t.Fatalf(
			"worker call count = %d, want 1",
			worker.calls,
		)
	}

	if len(dlqPublisher.messages) != 1 {
		t.Fatalf(
			"DLQ publish count = %d, want 1",
			len(dlqPublisher.messages),
		)
	}
}

func TestClassificationCommandHandlerReturnsCancelledWithoutDLQ(
	t *testing.T,
) {
	cancelledError :=
		&application.ClassificationWorkerError{
			Code: application.
				ClassificationWorkerErrorCodeCancelled,

			Class: application.
				ClassificationWorkerErrorClassCancelled,

			Operation: application.
				ClassificationWorkerOperationClassify,

			Cause: context.Canceled,
		}

	worker := &classificationCommandHandlerTestWorker{
		results: []error{
			cancelledError,
		},
	}

	dlqPublisher :=
		&classificationCommandHandlerTestPublisher{}

	handler, err :=
		application.NewClassificationCommandHandler(
			worker,
			[]time.Duration{0, 0},
			"astro.classification.commands.dlq.v1",
			dlqPublisher,
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	)

	if got != cancelledError {
		t.Fatalf(
			"Handle() error = %v, want cancelled error %v",
			got,
			cancelledError,
		)
	}

	if worker.calls != 1 {
		t.Fatalf(
			"worker call count = %d, want 1",
			worker.calls,
		)
	}

	if len(dlqPublisher.messages) != 0 {
		t.Fatalf(
			"DLQ publish count = %d, want 0",
			len(dlqPublisher.messages),
		)
	}
}

func newCommandHandlerRetryableError(
	operation application.ClassificationWorkerOperation,
) *application.ClassificationWorkerError {
	return &application.ClassificationWorkerError{
		Code: application.
			ClassificationWorkerErrorCodeDependencyUnavailable,

		Class: application.
			ClassificationWorkerErrorClassRetryable,

		Operation: operation,

		Cause: errors.New(
			"temporary dependency failure",
		),
	}
}

type classificationCommandHandlerTestWorker struct {
	results []error
	calls   int
}

func (worker *classificationCommandHandlerTestWorker) Handle(
	_ context.Context,
	_ application.InboundMessage,
) error {
	worker.calls++

	index := worker.calls - 1
	if index >= len(worker.results) {
		return nil
	}

	return worker.results[index]
}

type classificationCommandHandlerTestPublisher struct {
	err      error
	messages []application.OutboundMessage
}

func (publisher *classificationCommandHandlerTestPublisher) Publish(
	_ context.Context,
	message application.OutboundMessage,
) error {
	publisher.messages = append(
		publisher.messages,
		message,
	)

	return publisher.err
}
