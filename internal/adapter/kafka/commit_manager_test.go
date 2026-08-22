package kafka

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func waitUntil(
	t *testing.T,
	fn func() bool,
) {

	t.Helper()

	deadline :=
		time.After(
			time.Second,
		)

	ticker :=
		time.NewTicker(
			10 * time.Millisecond,
		)

	defer ticker.Stop()

	for {

		select {

		case <-deadline:
			t.Fatal(
				"timeout waiting condition",
			)

		case <-ticker.C:

			if fn() {
				return
			}
		}
	}
}

func TestCommitManagerFlushesByBatchSize(
	t *testing.T,
) {

	client :=
		&fakeCommitBatchClient{}

	batcher, err :=
		newCommitBatcher(client)

	if err != nil {
		t.Fatal(err)
	}

	manager, err :=
		newCommitManager(
			batcher,
			2,
			time.Hour,
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.Start(
		context.Background(),
	)

	defer manager.Stop(
		context.Background(),
	)

	_ = manager.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    101,
		},
	)

	_ = manager.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    102,
		},
	)

	waitUntil(
		t,
		func() bool {
			return len(client.records) > 0
		},
	)

	if client.records[0].Offset != 102 {
		t.Fatalf(
			"offset=%d want=102",
			client.records[0].Offset,
		)
	}
}

func TestCommitManagerFlushesByTimer(
	t *testing.T,
) {

	client :=
		&fakeCommitBatchClient{}

	batcher, _ :=
		newCommitBatcher(client)

	manager, err :=
		newCommitManager(
			batcher,
			100,
			20*time.Millisecond,
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.Start(
		context.Background(),
	)

	defer manager.Stop(
		context.Background(),
	)

	_ = manager.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    10,
		},
	)

	waitUntil(
		t,
		func() bool {
			return len(client.records) > 0
		},
	)
}

func TestCommitManagerStopsAndDrains(
	t *testing.T,
) {

	client :=
		&fakeCommitBatchClient{}

	batcher, _ :=
		newCommitBatcher(client)

	manager, _ :=
		newCommitManager(
			batcher,
			100,
			time.Hour,
		)

	manager.Start(
		context.Background(),
	)

	_ = manager.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    20,
		},
	)

	err :=
		manager.Stop(
			context.Background(),
		)

	if err != nil {
		t.Fatal(err)
	}

	if len(client.records) != 1 {
		t.Fatalf(
			"records=%d want=1",
			len(client.records),
		)
	}
}

func TestCommitManagerConcurrentAdd(
	t *testing.T,
) {

	client :=
		&fakeCommitBatchClient{}

	batcher, _ :=
		newCommitBatcher(client)

	manager, _ :=
		newCommitManager(
			batcher,
			1000,
			20*time.Millisecond,
		)

	manager.Start(
		context.Background(),
	)

	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {

		wg.Add(1)

		go func(offset int64) {

			defer wg.Done()

			_ = manager.Add(
				&kgo.Record{
					Topic:     "topic",
					Partition: 0,
					Offset:    offset,
				},
			)

		}(int64(i))

	}

	wg.Wait()

	err :=
		manager.Stop(
			context.Background(),
		)

	if err != nil {
		t.Fatal(err)
	}
}

func TestCommitManagerRetriesAfterFlushFailure(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitBatchClient{
		err: errors.New("commit failed"),
	}

	batcher, err :=
		newCommitBatcher(client)

	if err != nil {
		t.Fatal(err)
	}

	manager, err :=
		newCommitManager(
			batcher,
			1,
			time.Hour,
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.Start(
		context.Background(),
	)

	defer manager.Stop(
		context.Background(),
	)

	err = manager.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    100,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	waitUntil(
		t,
		func() bool {
			return len(client.records) > 0
		},
	)

	client.err = nil

	_ = manager.Stop(
		context.Background(),
	)

	if len(client.records) == 0 {
		t.Fatal(
			"expected retry commit",
		)
	}
}
