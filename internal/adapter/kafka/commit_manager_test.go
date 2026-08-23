package kafka

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestCommitManagerFlushesByBatchSize(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitClient{}

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

	manager.Start(context.Background())

	defer func() {
		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				time.Second,
			)

		defer cancel()

		_ = manager.Stop(ctx)
	}()

	if err :=
		manager.AddWatermark(
			&kgo.Record{
				Topic:     "topic",
				Partition: 0,
				Offset:    101,
			},
		); err != nil {
		t.Fatal(err)
	}

	if err :=
		manager.AddWatermark(
			&kgo.Record{
				Topic:     "topic",
				Partition: 1,
				Offset:    201,
			},
		); err != nil {
		t.Fatal(err)
	}

	deadline :=
		time.After(
			time.Second,
		)

	for {
		if len(client.records) == 2 {
			break
		}

		select {
		case <-deadline:
			t.Fatalf(
				"commit count=%d want=2",
				len(client.records),
			)

		default:
			time.Sleep(
				time.Millisecond,
			)
		}
	}
}

func TestCommitManagerFlushesOnTimer(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitClient{}

	batcher, err :=
		newCommitBatcher(client)

	if err != nil {
		t.Fatal(err)
	}

	manager, err :=
		newCommitManager(
			batcher,
			10,
			20*time.Millisecond,
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.Start(context.Background())

	defer func() {
		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				time.Second,
			)

		defer cancel()

		_ = manager.Stop(ctx)
	}()

	if err :=
		manager.AddWatermark(
			&kgo.Record{
				Topic:     "topic",
				Partition: 0,
				Offset:    101,
			},
		); err != nil {
		t.Fatal(err)
	}

	deadline :=
		time.After(
			time.Second,
		)

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
			time.Sleep(
				time.Millisecond,
			)
		}
	}
}

func TestCommitManagerKeepsPendingAfterFlushFailure(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitClient{
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
			10,
			time.Hour,
		)

	if err != nil {
		t.Fatal(err)
	}

	manager.Start(context.Background())

	defer func() {
		ctx, cancel :=
			context.WithTimeout(
				context.Background(),
				time.Second,
			)

		defer cancel()

		_ = manager.Stop(ctx)
	}()

	if err :=
		manager.AddWatermark(
			&kgo.Record{
				Topic:     "topic",
				Partition: 0,
				Offset:    101,
			},
		); err != nil {
		t.Fatal(err)
	}

	time.Sleep(
		50 * time.Millisecond,
	)

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
				"commit attempts=%d want=1",
				len(client.records),
			)

		default:
			time.Sleep(time.Millisecond)
		}
	}

	if len(batcher.pending) != 1 {
		t.Fatalf(
			"pending count=%d want=1",
			len(batcher.pending),
		)
	}
}

func TestCommitManagerStopsAndFlushesPending(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitClient{}

	batcher, err :=
		newCommitBatcher(client)

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

	manager.Start(context.Background())

	if err :=
		manager.AddWatermark(
			&kgo.Record{
				Topic:     "topic",
				Partition: 0,
				Offset:    101,
			},
		); err != nil {
		t.Fatal(err)
	}

	ctx, cancel :=
		context.WithTimeout(
			context.Background(),
			time.Second,
		)

	defer cancel()

	if err := manager.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if len(client.records) != 1 {
		t.Fatalf(
			"commit count=%d want=1",
			len(client.records),
		)
	}

	if client.records[0].Offset != 101 {
		t.Fatalf(
			"offset=%d want=101",
			client.records[0].Offset,
		)
	}
}
