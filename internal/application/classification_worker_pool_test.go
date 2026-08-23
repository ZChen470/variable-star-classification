package application

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestClassificationWorkerPoolSubmitCompletion(
	t *testing.T,
) {
	t.Parallel()

	expected :=
		errors.New("worker failed")

	handler :=
		MessageHandlerFunc(
			func(
				context.Context,
				InboundMessage,
			) error {
				return expected
			},
		)

	pool, err :=
		NewClassificationWorkerPool(
			handler,
			1,
		)

	if err != nil {
		t.Fatal(err)
	}

	completed :=
		make(chan error, 1)

	err =
		pool.Submit(
			context.Background(),
			InboundMessage{
				Topic: "topic",
			},
			func(err error) {
				completed <- err
			},
		)

	if err != nil {
		t.Fatal(err)
	}

	select {

	case err := <-completed:

		if !errors.Is(
			err,
			expected,
		) {
			t.Fatalf(
				"completion error=%v",
				err,
			)
		}

	case <-time.After(time.Second):

		t.Fatal(
			"timeout waiting completion",
		)
	}
}

func TestClassificationWorkerPoolHandleCompatibility(
	t *testing.T,
) {
	t.Parallel()

	pool, err :=
		NewClassificationWorkerPool(
			MessageHandlerFunc(
				func(
					context.Context,
					InboundMessage,
				) error {
					return nil
				},
			),
			1,
		)

	if err != nil {
		t.Fatal(err)
	}

	err =
		pool.Handle(
			context.Background(),
			InboundMessage{
				Topic: "topic",
			},
		)

	if err != nil {
		t.Fatal(err)
	}
}

func TestClassificationWorkerPoolSubmitWithChannel(
	t *testing.T,
) {
	t.Parallel()

	pool, err :=
		NewClassificationWorkerPool(
			MessageHandlerFunc(
				func(
					context.Context,
					InboundMessage,
				) error {
					return nil
				},
			),
			1,
		)

	if err != nil {
		t.Fatal(err)
	}

	results, err :=
		pool.SubmitWithChannel(
			context.Background(),
			InboundMessage{
				Topic: "topic",
			},
		)

	if err != nil {
		t.Fatal(err)
	}

	select {

	case result := <-results:

		if result.Err != nil {
			t.Fatal(result.Err)
		}

	case <-time.After(time.Second):

		t.Fatal(
			"timeout waiting result",
		)
	}
}

func TestClassificationWorkerPoolRejectsNilCompletion(
	t *testing.T,
) {
	t.Parallel()

	pool, err :=
		NewClassificationWorkerPool(
			MessageHandlerFunc(
				func(
					context.Context,
					InboundMessage,
				) error {
					return nil
				},
			),
			1,
		)

	if err != nil {
		t.Fatal(err)
	}

	err =
		pool.Submit(
			context.Background(),
			InboundMessage{},
			nil,
		)

	if err == nil {
		t.Fatal(
			"expected nil completion error",
		)
	}
}
