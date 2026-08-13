package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestClassificationCommandRetryHandlerReturnsFirstSuccess(
	t *testing.T,
) {
	next := &classificationCommandRetryTestHandler{}

	handler, err :=
		application.NewClassificationCommandRetryHandler(
			next,
			[]time.Duration{0},
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationCommandRetryHandler() error = %v",
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
}

func TestClassificationCommandRetryHandlerRetriesUntilSuccess(
	t *testing.T,
) {
	firstError := retryableClassificationWorkerError(
		application.ClassificationWorkerOperationPrepareInput,
	)

	secondError := retryableClassificationWorkerError(
		application.ClassificationWorkerOperationClassify,
	)

	next := &classificationCommandRetryTestHandler{
		results: []error{
			firstError,
			secondError,
			nil,
		},
	}

	handler, err :=
		application.NewClassificationCommandRetryHandler(
			next,
			[]time.Duration{0, 0},
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationCommandRetryHandler() error = %v",
			err,
		)
	}

	message := application.InboundMessage{
		Topic:     "astro.classification.commands.v1",
		Partition: 2,
		Offset:    17,
		Key:       []byte("OBJ-0001"),
		Value:     []byte{0x01, 0x02},

		Headers: []application.MessageHeader{
			{
				Key:   "traceparent",
				Value: []byte("trace-value"),
			},
		},
	}

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if next.calls != 3 {
		t.Fatalf(
			"worker call count = %d, want 3",
			next.calls,
		)
	}

	for index, received := range next.messages {
		if !reflect.DeepEqual(received, message) {
			t.Fatalf(
				"attempt %d message = %#v, want %#v",
				index+1,
				received,
				message,
			)
		}
	}
}

func TestClassificationCommandRetryHandlerContinuesBeyondConfiguredBackoffScheduleUntilSuccess(t *testing.T) {
	retryableError := retryableClassificationWorkerError(
		application.ClassificationWorkerOperationPrepareInput,
	)

	next := &classificationCommandRetryTestHandler{
		results: []error{
			retryableError,
			retryableError,
			retryableError,
			retryableError,
			nil,
		},
	}

	handler, err := application.NewClassificationCommandRetryHandler(
		next,
		[]time.Duration{0, 0},
	)
	if err != nil {
		t.Fatalf("NewClassificationCommandRetryHandler() error = %v", err)
	}

	if err := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if next.calls != 5 {
		t.Fatalf("worker call count = %d, want 5", next.calls)
	}
}

func TestClassificationCommandRetryHandlerDoesNotRetryOtherErrors(
	t *testing.T,
) {
	permanentError := &application.ClassificationWorkerError{
		Code: application.ClassificationWorkerErrorCodeLightCurveInvalid,

		Class: application.ClassificationWorkerErrorClassPermanent,

		Operation: application.ClassificationWorkerOperationPrepareInput,
	}

	cancelledError := &application.ClassificationWorkerError{
		Code: application.ClassificationWorkerErrorCodeCancelled,

		Class: application.ClassificationWorkerErrorClassCancelled,

		Operation: application.ClassificationWorkerOperationClassify,

		Cause: context.Canceled,
	}

	unclassifiedError := errors.New(
		"unclassified worker failure",
	)

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "permanent",
			err:  permanentError,
		},
		{
			name: "cancelled",
			err:  cancelledError,
		},
		{
			name: "unclassified",
			err:  unclassifiedError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next := &classificationCommandRetryTestHandler{
				results: []error{test.err},
			}

			handler, err :=
				application.NewClassificationCommandRetryHandler(
					next,
					[]time.Duration{0, 0},
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

			if got != test.err {
				t.Fatalf(
					"Handle() error = %v, want %v",
					got,
					test.err,
				)
			}

			if next.calls != 1 {
				t.Fatalf(
					"worker call count = %d, want 1",
					next.calls,
				)
			}
		})
	}
}

func TestClassificationCommandRetryHandlerStopsWhenErrorBecomesPermanent(
	t *testing.T,
) {
	retryableError := retryableClassificationWorkerError(
		application.ClassificationWorkerOperationPrepareInput,
	)

	permanentError := &application.ClassificationWorkerError{
		Code: application.ClassificationWorkerErrorCodeLightCurveInvalid,

		Class: application.ClassificationWorkerErrorClassPermanent,

		Operation: application.ClassificationWorkerOperationPrepareInput,
	}

	next := &classificationCommandRetryTestHandler{
		results: []error{
			retryableError,
			permanentError,
			nil,
		},
	}

	handler, err :=
		application.NewClassificationCommandRetryHandler(
			next,
			[]time.Duration{0, 0},
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		context.Background(),
		application.InboundMessage{},
	)

	if got != permanentError {
		t.Fatalf(
			"Handle() error = %v, want permanent error %v",
			got,
			permanentError,
		)
	}

	if next.calls != 2 {
		t.Fatalf(
			"worker call count = %d, want 2",
			next.calls,
		)
	}
}

func TestClassificationCommandRetryHandlerStopsWhenContextCancelledDuringWait(
	t *testing.T,
) {
	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	defer cancel()

	next := &classificationCommandRetryTestHandler{
		results: []error{
			retryableClassificationWorkerError(
				application.ClassificationWorkerOperationClassify,
			),
		},

		onCall: func(call int) {
			if call == 1 {
				cancel()
			}
		},
	}

	handler, err :=
		application.NewClassificationCommandRetryHandler(
			next,
			[]time.Duration{time.Hour},
		)
	if err != nil {
		t.Fatalf("constructor error = %v", err)
	}

	got := handler.Handle(
		ctx,
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
		application.ClassificationWorkerErrorClassCancelled {
		t.Fatalf(
			"Class = %s, want CANCELLED",
			workerError.Class,
		)
	}

	if workerError.Code !=
		application.ClassificationWorkerErrorCodeCancelled {
		t.Fatalf(
			"Code = %q, want %q",
			workerError.Code,
			application.ClassificationWorkerErrorCodeCancelled,
		)
	}

	if workerError.Operation !=
		application.ClassificationWorkerOperationRetryWait {
		t.Fatalf(
			"Operation = %q, want %q",
			workerError.Operation,
			application.ClassificationWorkerOperationRetryWait,
		)
	}

	if !errors.Is(got, context.Canceled) {
		t.Fatalf(
			"errors.Is(error, context.Canceled) = false",
		)
	}

	if next.calls != 1 {
		t.Fatalf(
			"worker call count = %d, want 1",
			next.calls,
		)
	}
}

func TestNewClassificationCommandRetryHandlerRejectsInvalidArguments(
	t *testing.T,
) {
	validNext := application.MessageHandlerFunc(
		func(
			context.Context,
			application.InboundMessage,
		) error {
			return nil
		},
	)

	tests := []struct {
		name string

		next   application.MessageHandler
		delays []time.Duration
	}{
		{
			name:   "nil worker handler",
			next:   nil,
			delays: []time.Duration{0},
		},
		{
			name:   "empty retry delays",
			next:   validNext,
			delays: nil,
		},
		{
			name:   "negative retry delay",
			next:   validNext,
			delays: []time.Duration{-time.Millisecond},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handler, err :=
				application.NewClassificationCommandRetryHandler(
					test.next,
					test.delays,
				)

			if err == nil {
				t.Fatalf(
					"constructor error = nil, handler = %#v",
					handler,
				)
			}
		})
	}
}

func retryableClassificationWorkerError(
	operation application.ClassificationWorkerOperation,
) *application.ClassificationWorkerError {
	return &application.ClassificationWorkerError{
		Code: application.ClassificationWorkerErrorCodeDependencyUnavailable,

		Class: application.ClassificationWorkerErrorClassRetryable,

		Operation: operation,

		Cause: errors.New(
			"temporary dependency failure",
		),
	}
}

type classificationCommandRetryTestHandler struct {
	results []error
	onCall  func(call int)

	calls    int
	messages []application.InboundMessage
}

func (handler *classificationCommandRetryTestHandler) Handle(
	_ context.Context,
	message application.InboundMessage,
) error {
	handler.calls++

	handler.messages = append(
		handler.messages,
		message,
	)

	if handler.onCall != nil {
		handler.onCall(handler.calls)
	}

	resultIndex := handler.calls - 1
	if resultIndex >= len(handler.results) {
		return nil
	}

	return handler.results[resultIndex]
}
