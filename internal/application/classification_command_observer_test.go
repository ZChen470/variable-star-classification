package application

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClassificationCommandObserverRecordsRetryAttemptsBeyondConfiguredSchedule(t *testing.T) {
	t.Parallel()

	retryError := &ClassificationWorkerError{
		Code:      ClassificationWorkerErrorCodeDependencyUnavailable,
		Class:     ClassificationWorkerErrorClassRetryable,
		Operation: ClassificationWorkerOperationClassify,
		Cause:     errors.New("retryable failure"),
	}
	next := &classificationCommandObserverTestHandler{
		results: []error{
			retryError,
			retryError,
			retryError,
			nil,
		},
	}

	observer := &classificationCommandObserverTestObserver{}
	handler, err := NewClassificationCommandRetryHandlerWithObserver(
		next,
		[]time.Duration{0},
		observer,
	)
	if err != nil {
		t.Fatalf("NewClassificationCommandRetryHandlerWithObserver() error = %v", err)
	}

	if err := handler.Handle(context.Background(), InboundMessage{}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if observer.retryAttempted != 3 {
		t.Fatalf("retry attempted = %d, want 3", observer.retryAttempted)
	}
	if next.calls != 4 {
		t.Fatalf("handler calls = %d, want 4", next.calls)
	}
}

func TestClassificationCommandObserverRecordsSuccessfulDLQ(t *testing.T) {
	t.Parallel()

	permanentError := &ClassificationWorkerError{
		Code:      ClassificationWorkerErrorCodeLightCurveInvalid,
		Class:     ClassificationWorkerErrorClassPermanent,
		Operation: ClassificationWorkerOperationPrepareInput,
		Cause:     errors.New("permanent failure"),
	}
	next := &classificationCommandObserverTestHandler{
		results: []error{permanentError},
	}

	publisher := &classificationCommandObserverTestPublisher{}
	observer := &classificationCommandObserverTestObserver{}
	handler, err := NewClassificationCommandDLQHandlerWithObserver(
		next,
		"command-dlq-test",
		publisher,
		observer,
	)
	if err != nil {
		t.Fatalf("NewClassificationCommandDLQHandlerWithObserver() error = %v", err)
	}

	if err := handler.Handle(context.Background(), InboundMessage{Topic: "command-test"}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if observer.dlqPublished != 1 {
		t.Fatalf("DLQ published = %d, want 1", observer.dlqPublished)
	}
}

type classificationCommandObserverTestObserver struct {
	retryAttempted int
	dlqPublished   int
}

func (observer *classificationCommandObserverTestObserver) RetryAttempted() {
	observer.retryAttempted++
}

func (observer *classificationCommandObserverTestObserver) DLQPublished() {
	observer.dlqPublished++
}

type classificationCommandObserverTestHandler struct {
	results []error
	calls   int
}

func (handler *classificationCommandObserverTestHandler) Handle(_ context.Context, _ InboundMessage) error {
	index := handler.calls
	handler.calls++

	if index >= len(handler.results) {
		return nil
	}

	return handler.results[index]
}

type classificationCommandObserverTestPublisher struct{}

func (*classificationCommandObserverTestPublisher) Publish(_ context.Context, _ OutboundMessage) error {
	return nil
}
