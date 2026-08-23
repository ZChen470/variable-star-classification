package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeCommitClient struct {
	records []*kgo.Record
	err     error
}

func (client *fakeCommitClient) CommitRecords(
	_ context.Context,
	records ...*kgo.Record,
) error {
	client.records = append(
		client.records,
		records...,
	)

	return client.err
}

func newTestCommitManager(
	t *testing.T,
	client *fakeCommitClient,
) *commitManager {
	t.Helper()

	batcher, err := newCommitBatcher(client)
	if err != nil {
		t.Fatal(err)
	}

	manager, err :=
		newCommitManager(
			batcher,
			10,
			time.Hour,
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.Start(
		context.Background(),
	)

	return manager
}

func TestKafkaCommitCoordinatorBatchesWatermark(
	t *testing.T,
) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()

	err := tracker.InitializePartition(
		"topic",
		0,
		100,
	)

	if err != nil {
		t.Fatal(err)
	}

	client := &fakeCommitClient{}

	manager :=
		newTestCommitManager(
			t,
			client,
		)

	defer func() {
		stopCtx, cancel :=
			context.WithTimeout(
				context.Background(),
				time.Second,
			)

		defer cancel()

		_ = manager.Stop(stopCtx)
	}()

	coordinator, err :=
		newKafkaCommitCoordinator(
			tracker,
			manager,
		)

	if err != nil {
		t.Fatal(err)
	}

	err =
		coordinator.HandleCompletion(
			context.Background(),
			RecordCompletion{
				record: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    100,
				},
			},
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.RequestFlush()

	stopCtx, cancel :=
		context.WithTimeout(
			context.Background(),
			time.Second,
		)

	defer cancel()

	if err := manager.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}

	if len(client.records) != 1 {
		t.Fatalf(
			"commit count=%d want=1",
			len(client.records),
		)
	}

	record := client.records[0]

	if record.Topic != "topic" {
		t.Fatalf(
			"topic=%q want=topic",
			record.Topic,
		)
	}

	if record.Partition != 0 {
		t.Fatalf(
			"partition=%d want=0",
			record.Partition,
		)
	}

	if record.Offset != 101 {
		t.Fatalf(
			"commit offset=%d want=101",
			record.Offset,
		)
	}
}

func TestKafkaCommitCoordinatorDoesNotBatchFailure(
	t *testing.T,
) {
	t.Parallel()

	tracker := newPartitionCompletionTracker()

	err := tracker.InitializePartition(
		"topic",
		0,
		100,
	)

	if err != nil {
		t.Fatal(err)
	}

	client := &fakeCommitClient{}

	manager :=
		newTestCommitManager(
			t,
			client,
		)

	defer func() {
		stopCtx, cancel :=
			context.WithTimeout(
				context.Background(),
				time.Second,
			)

		defer cancel()

		_ = manager.Stop(stopCtx)
	}()

	coordinator, err :=
		newKafkaCommitCoordinator(
			tracker,
			manager,
		)

	if err != nil {
		t.Fatal(err)
	}

	err =
		coordinator.HandleCompletion(
			context.Background(),
			RecordCompletion{
				record: &kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    100,
				},
				err: errors.New("failed"),
			},
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.RequestFlush()

	stopCtx, cancel :=
		context.WithTimeout(
			context.Background(),
			time.Second,
		)

	defer cancel()

	if err := manager.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}

	if len(client.records) != 0 {
		t.Fatalf(
			"commit count=%d want=0",
			len(client.records),
		)
	}
}
