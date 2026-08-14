package application

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type classificationResultRetryTestHandler struct {
	handle func(context.Context, InboundMessage) error
}

func (handler *classificationResultRetryTestHandler) Handle(
	ctx context.Context,
	message InboundMessage,
) error {
	return handler.handle(ctx, message)
}

func TestClassificationResultRetryHandlerReturnsSuccessfulFirstAttempt(t *testing.T) {
	calls := 0

	next := &classificationResultRetryTestHandler{
		handle: func(context.Context, InboundMessage) error {
			calls++
			return nil
		},
	}

	handler, err := NewClassificationResultRetryHandler(
		next,
		[]time.Duration{0},
	)
	if err != nil {
		t.Fatalf("create result retry handler: %v", err)
	}

	if err := handler.Handle(context.Background(), InboundMessage{}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestClassificationResultRetryHandlerRetriesTransientFailure(t *testing.T) {
	calls := 0
	transientErr := errors.New("temporary PostgreSQL failure")

	next := &classificationResultRetryTestHandler{
		handle: func(context.Context, InboundMessage) error {
			calls++
			if calls == 1 {
				return transientErr
			}
			return nil
		},
	}

	handler, err := NewClassificationResultRetryHandler(
		next,
		[]time.Duration{0},
	)
	if err != nil {
		t.Fatalf("create result retry handler: %v", err)
	}

	if err := handler.Handle(context.Background(), InboundMessage{}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestClassificationResultRetryHandlerRetriesPersistenceDeadline(t *testing.T) {
	calls := 0

	next := &classificationResultRetryTestHandler{
		handle: func(context.Context, InboundMessage) error {
			calls++
			if calls == 1 {
				return fmt.Errorf(
					"save classification result: %w",
					context.DeadlineExceeded,
				)
			}
			return nil
		},
	}

	handler, err := NewClassificationResultRetryHandler(
		next,
		[]time.Duration{0},
	)
	if err != nil {
		t.Fatalf("create result retry handler: %v", err)
	}

	if err := handler.Handle(context.Background(), InboundMessage{}); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}
}

func TestClassificationResultRetryHandlerDoesNotRetryPermanentFailure(t *testing.T) {
	calls := 0

	permanentErr := &PermanentClassificationResultError{
		Code:  ClassificationResultErrorCodeMalformedMessage,
		Field: "value",
		Cause: errors.New("malformed"),
	}

	next := &classificationResultRetryTestHandler{
		handle: func(context.Context, InboundMessage) error {
			calls++
			return permanentErr
		},
	}

	handler, err := NewClassificationResultRetryHandler(
		next,
		[]time.Duration{0},
	)
	if err != nil {
		t.Fatalf("create result retry handler: %v", err)
	}

	err = handler.Handle(context.Background(), InboundMessage{})
	if !errors.Is(err, permanentErr) {
		t.Fatalf("Handle() error = %v, want permanent error", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestClassificationResultRetryHandlerStopsWhenContextCancelled(t *testing.T) {
	called := make(chan struct{})
	transientErr := errors.New("temporary PostgreSQL failure")

	next := &classificationResultRetryTestHandler{
		handle: func(context.Context, InboundMessage) error {
			select {
			case <-called:
			default:
				close(called)
			}
			return transientErr
		},
	}

	handler, err := NewClassificationResultRetryHandler(
		next,
		[]time.Duration{time.Hour},
	)
	if err != nil {
		t.Fatalf("create result retry handler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		result <- handler.Handle(ctx, InboundMessage{})
	}()

	<-called
	cancel()

	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Handle() error = %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("retry handler did not stop after context cancellation")
	}
}

func TestClassificationResultRetryHandlerDoesNotCallNextForCancelledContext(t *testing.T) {
	calls := 0

	next := &classificationResultRetryTestHandler{
		handle: func(context.Context, InboundMessage) error {
			calls++
			return nil
		},
	}

	handler, err := NewClassificationResultRetryHandler(
		next,
		[]time.Duration{0},
	)
	if err != nil {
		t.Fatalf("create result retry handler: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = handler.Handle(ctx, InboundMessage{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle() error = %v, want context canceled", err)
	}
	if calls != 0 {
		t.Fatalf("calls = %d, want 0", calls)
	}
}

func TestClassificationResultRetryDelayCapsAtLastDelay(t *testing.T) {
	delays := []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
	}

	tests := []struct {
		index int
		want  time.Duration
	}{
		{index: 0, want: 100 * time.Millisecond},
		{index: 1, want: 200 * time.Millisecond},
		{index: 2, want: 200 * time.Millisecond},
		{index: 10, want: 200 * time.Millisecond},
	}

	for _, test := range tests {
		if got := classificationResultRetryDelay(delays, test.index); got != test.want {
			t.Fatalf(
				"classificationResultRetryDelay(%d) = %s, want %s",
				test.index,
				got,
				test.want,
			)
		}
	}
}

func TestNewClassificationResultRetryHandlerRejectsInvalidArguments(t *testing.T) {
	validHandler := &classificationResultRetryTestHandler{
		handle: func(context.Context, InboundMessage) error {
			return nil
		},
	}

	tests := []struct {
		name   string
		next   MessageHandler
		delays []time.Duration
	}{
		{
			name:   "nil handler",
			next:   nil,
			delays: []time.Duration{time.Second},
		},
		{
			name:   "empty delays",
			next:   validHandler,
			delays: nil,
		},
		{
			name:   "negative delay",
			next:   validHandler,
			delays: []time.Duration{-time.Second},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewClassificationResultRetryHandler(
				test.next,
				test.delays,
			); err == nil {
				t.Fatal("expected constructor error")
			}
		})
	}
}
