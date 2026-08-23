package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeAsyncConsumer struct {
	fetches []kgo.Fetches

	allowCount int
	index      int
}

func (consumer *fakeAsyncConsumer) PollFetches(
	_ context.Context,
) kgo.Fetches {

	if consumer.index >= len(consumer.fetches) {
		return kgo.Fetches{}
	}

	fetches :=
		consumer.fetches[consumer.index]

	consumer.index++

	return fetches
}

func (consumer *fakeAsyncConsumer) CommitRecords(
	_ context.Context,
	_ ...*kgo.Record,
) error {
	return nil
}

func (consumer *fakeAsyncConsumer) AllowRebalance() {
	consumer.allowCount++
}

type fakeRecordDispatcher struct {
	record *kgo.Record

	dispatched chan struct{}

	err error
}

func (dispatcher *fakeRecordDispatcher) Dispatch(
	_ context.Context,
	record *kgo.Record,
) error {

	dispatcher.record = record

	if dispatcher.dispatched != nil {
		select {
		case <-dispatcher.dispatched:
		default:
			close(dispatcher.dispatched)
		}
	}

	return dispatcher.err
}

func TestAsyncConsumerRunnerDispatchesRecords(
	t *testing.T,
) {
	t.Parallel()

	consumer :=
		&fakeAsyncConsumer{
			fetches: []kgo.Fetches{
				{},
			},
		}

	dispatcher :=
		&fakeRecordDispatcher{
			dispatched: make(chan struct{}),
		}

	completion :=
		make(
			chan RecordCompletion,
		)

	tracker :=
		newPartitionCompletionTracker()

	if err := tracker.InitializePartition(
		"topic",
		0,
		100,
	); err != nil {
		t.Fatal(err)
	}

	client :=
		&fakeCommitClient{}

	manager :=
		newTestCommitManager(
			t,
			client,
		)

	coordinator, err :=
		newKafkaCommitCoordinator(
			tracker,
			manager,
		)

	if err != nil {
		t.Fatal(err)
	}

	runner, err :=
		newAsyncConsumerRunner(
			consumer,
			dispatcher,
			completion,
			coordinator,
			16,
		)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel :=
		context.WithCancel(
			context.Background(),
		)

	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	time.Sleep(
		20 * time.Millisecond,
	)

	if dispatcher.record != nil {
		t.Fatalf(
			"records dispatched unexpectedly",
		)
	}
}

func TestAsyncConsumerRunnerHandlesCompletion(
	t *testing.T,
) {
	t.Parallel()

	completion :=
		make(
			chan RecordCompletion,
			1,
		)

	tracker :=
		newPartitionCompletionTracker()

	if err := tracker.InitializePartition(
		"topic",
		0,
		100,
	); err != nil {
		t.Fatal(err)
	}

	client :=
		&fakeCommitClient{}

	manager :=
		newTestCommitManager(
			t,
			client,
		)

	defer func() {

		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				time.Second,
			)

		defer cancel()

		_ = manager.Stop(ctx)

	}()

	coordinator, err :=
		newKafkaCommitCoordinator(
			tracker,
			manager,
		)

	if err != nil {
		t.Fatal(err)
	}

	runner, err :=
		newAsyncConsumerRunner(
			&fakeAsyncConsumer{},
			&fakeRecordDispatcher{},
			completion,
			coordinator,
			16,
		)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel :=
		context.WithCancel(
			context.Background(),
		)

	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	record :=
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    100,
		}

	completion <- RecordCompletion{
		record: record,
		err:    nil,
	}

	manager.RequestFlush()

	deadline :=
		time.After(time.Second)

	for {

		if len(client.records) == 1 {
			break
		}

		select {

		case <-deadline:
			t.Fatalf(
				"commit count=%d want=1",
				len(client.records),
			)

		default:
			time.Sleep(time.Millisecond)
		}
	}

	if client.records[0].Offset != 101 {
		t.Fatalf(
			"offset=%d want=101",
			client.records[0].Offset,
		)
	}
}

func TestAsyncConsumerRunnerDoesNotCommitFailedCompletion(
	t *testing.T,
) {
	t.Parallel()

	completion :=
		make(
			chan RecordCompletion,
			1,
		)

	tracker :=
		newPartitionCompletionTracker()

	if err := tracker.InitializePartition(
		"topic",
		0,
		100,
	); err != nil {
		t.Fatal(err)
	}

	client :=
		&fakeCommitClient{}

	manager :=
		newTestCommitManager(
			t,
			client,
		)

	defer func() {

		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				time.Second,
			)

		defer cancel()

		_ = manager.Stop(ctx)

	}()

	coordinator, err :=
		newKafkaCommitCoordinator(
			tracker,
			manager,
		)

	if err != nil {
		t.Fatal(err)
	}

	runner, err :=
		newAsyncConsumerRunner(
			&fakeAsyncConsumer{},
			&fakeRecordDispatcher{},
			completion,
			coordinator,
			16,
		)

	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel :=
		context.WithCancel(
			context.Background(),
		)

	defer cancel()

	go func() {
		_ = runner.Run(ctx)
	}()

	completion <- RecordCompletion{
		record: &kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    100,
		},
		err: errors.New(
			"worker failed",
		),
	}

	manager.RequestFlush()

	time.Sleep(
		50 * time.Millisecond,
	)

	if len(client.records) != 0 {
		t.Fatalf(
			"commit count=%d want=0",
			len(client.records),
		)
	}
}

func TestAsyncConsumerRunnerRejectsNilDependencies(
	t *testing.T,
) {
	t.Parallel()

	_, err :=
		newAsyncConsumerRunner(
			nil,
			nil,
			nil,
			nil,
			0,
		)

	if err == nil {
		t.Fatal(
			"expected error",
		)
	}
}
