package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRebalanceYieldConsumerRunnerProcessesCommitsAndAllowsRebalance(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	record := &kgo.Record{
		Topic:     "command-test",
		Partition: 1,
		Offset:    10,
		Value:     []byte{0x01},
	}

	consumer := &fakeRebalanceYieldConsumer{
		fetches:            []kgo.Fetches{fetchWithRecords(record)},
		cancel:             cancel,
		cancelAfterCommits: 1,
	}
	handler := &rebalanceYieldTestHandler{}
	yield := NewRebalanceYield()

	runner := newRebalanceYieldConsumerRunner(
		consumer,
		handler,
		yield,
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if handler.calls != 1 {
		t.Fatalf("handler calls = %d, want 1", handler.calls)
	}
	if len(consumer.committed) != 1 {
		t.Fatalf("committed count = %d, want 1", len(consumer.committed))
	}
	if consumer.committed[0] != record {
		t.Fatal("committed record does not match polled record")
	}
	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance calls = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
	if len(consumer.maxRecords) != 1 || consumer.maxRecords[0] != 1 {
		t.Fatalf("PollRecords maxRecords = %v, want [1]", consumer.maxRecords)
	}
}

func TestRebalanceYieldConsumerRunnerDoesNotCommitYieldedRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	yield := NewRebalanceYield()

	record := &kgo.Record{
		Topic:     "command-test",
		Partition: 0,
		Offset:    215,
	}

	consumer := &fakeRebalanceYieldConsumer{
		fetches:                   []kgo.Fetches{fetchWithRecords(record)},
		cancel:                    cancel,
		cancelAfterAllowRebalance: 1,
	}

	handler := &rebalanceYieldTestHandler{
		handle: func(ctx context.Context, _ application.InboundMessage) error {
			yield.Request()
			return ctx.Err()
		},
	}

	runner := newRebalanceYieldConsumerRunner(
		consumer,
		handler,
		yield,
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(consumer.committed) != 0 {
		t.Fatalf("committed count = %d, want 0", len(consumer.committed))
	}
	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance calls = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

func TestRebalanceYieldConsumerRunnerHandlesYieldBeforeRecordBind(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	yield := NewRebalanceYield()

	record := &kgo.Record{
		Topic:     "command-test",
		Partition: 0,
		Offset:    215,
	}

	consumer := &fakeRebalanceYieldConsumer{
		fetches:                   []kgo.Fetches{fetchWithRecords(record)},
		cancel:                    cancel,
		cancelAfterAllowRebalance: 1,
		onPoll: func() {
			yield.Request()
		},
	}

	handler := &rebalanceYieldTestHandler{
		handle: func(ctx context.Context, _ application.InboundMessage) error {
			if !errors.Is(ctx.Err(), context.Canceled) {
				t.Fatalf(
					"record context error = %v, want context.Canceled",
					ctx.Err(),
				)
			}
			return ctx.Err()
		},
	}

	runner := newRebalanceYieldConsumerRunner(
		consumer,
		handler,
		yield,
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}

	if len(consumer.committed) != 0 {
		t.Fatalf("committed count = %d, want 0", len(consumer.committed))
	}
}

func TestRebalanceYieldConsumerRunnerSuppressesCommitFailureAfterYield(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	yield := NewRebalanceYield()

	record := &kgo.Record{
		Topic:     "command-test",
		Partition: 0,
		Offset:    215,
	}

	commitCause := errors.New("UNKNOWN_MEMBER_ID")

	consumer := &fakeRebalanceYieldConsumer{
		fetches:                   []kgo.Fetches{fetchWithRecords(record)},
		commitErr:                 commitCause,
		cancel:                    cancel,
		cancelAfterAllowRebalance: 1,
		onCommit: func() {
			yield.Request()
		},
	}

	handler := &rebalanceYieldTestHandler{}

	runner := newRebalanceYieldConsumerRunner(
		consumer,
		handler,
		yield,
	)

	if err := runner.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v, want nil after rebalance yield", err)
	}

	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance calls = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

func TestRebalanceYieldConsumerRunnerReturnsOrdinaryHandlerFailure(t *testing.T) {
	handlerCause := errors.New("handler failed")

	record := &kgo.Record{
		Topic:     "command-test",
		Partition: 2,
		Offset:    30,
	}

	consumer := &fakeRebalanceYieldConsumer{
		fetches: []kgo.Fetches{fetchWithRecords(record)},
	}

	handler := &rebalanceYieldTestHandler{
		handle: func(context.Context, application.InboundMessage) error {
			return handlerCause
		},
	}

	runner := newRebalanceYieldConsumerRunner(
		consumer,
		handler,
		NewRebalanceYield(),
	)

	err := runner.Run(context.Background())
	if !errors.Is(err, handlerCause) {
		t.Fatalf("Run() error = %v, want handler cause", err)
	}

	if len(consumer.committed) != 0 {
		t.Fatalf("committed count = %d, want 0", len(consumer.committed))
	}
	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance calls = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

func TestRebalanceYieldConsumerRunnerReturnsOrdinaryCommitFailure(t *testing.T) {
	commitCause := errors.New("commit failed")

	record := &kgo.Record{
		Topic:     "command-test",
		Partition: 2,
		Offset:    31,
	}

	consumer := &fakeRebalanceYieldConsumer{
		fetches:   []kgo.Fetches{fetchWithRecords(record)},
		commitErr: commitCause,
	}

	runner := newRebalanceYieldConsumerRunner(
		consumer,
		&rebalanceYieldTestHandler{},
		NewRebalanceYield(),
	)

	err := runner.Run(context.Background())
	if !errors.Is(err, commitCause) {
		t.Fatalf("Run() error = %v, want commit cause", err)
	}

	if consumer.allowRebalanceCalls != 1 {
		t.Fatalf(
			"AllowRebalance calls = %d, want 1",
			consumer.allowRebalanceCalls,
		)
	}
}

type rebalanceYieldTestHandler struct {
	handle func(context.Context, application.InboundMessage) error
	calls  int
}

func (handler *rebalanceYieldTestHandler) Handle(
	ctx context.Context,
	message application.InboundMessage,
) error {
	handler.calls++

	if handler.handle == nil {
		return nil
	}

	return handler.handle(ctx, message)
}

type fakeRebalanceYieldConsumer struct {
	fetches []kgo.Fetches

	committed []*kgo.Record
	commitErr error

	pollCalls           int
	maxRecords          []int
	allowRebalanceCalls int

	onPoll   func()
	onCommit func()

	cancel                    context.CancelFunc
	cancelAfterCommits        int
	cancelAfterAllowRebalance int
}

func (consumer *fakeRebalanceYieldConsumer) PollRecords(
	ctx context.Context,
	maxRecords int,
) kgo.Fetches {
	consumer.pollCalls++
	consumer.maxRecords = append(consumer.maxRecords, maxRecords)

	if consumer.onPoll != nil {
		consumer.onPoll()
	}

	if len(consumer.fetches) > 0 {
		fetches := consumer.fetches[0]
		consumer.fetches = consumer.fetches[1:]
		return fetches
	}

	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (consumer *fakeRebalanceYieldConsumer) CommitRecords(
	_ context.Context,
	records ...*kgo.Record,
) error {
	if consumer.onCommit != nil {
		consumer.onCommit()
	}

	if consumer.commitErr != nil {
		return consumer.commitErr
	}

	consumer.committed = append(
		consumer.committed,
		records...,
	)

	if consumer.cancel != nil &&
		consumer.cancelAfterCommits > 0 &&
		len(consumer.committed) >= consumer.cancelAfterCommits {
		consumer.cancel()
	}

	return nil
}

func (consumer *fakeRebalanceYieldConsumer) AllowRebalance() {
	consumer.allowRebalanceCalls++

	if consumer.cancel != nil &&
		consumer.cancelAfterAllowRebalance > 0 &&
		consumer.allowRebalanceCalls >= consumer.cancelAfterAllowRebalance {
		consumer.cancel()
	}
}
