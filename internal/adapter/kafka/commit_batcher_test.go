package kafka

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeCommitBatchClient struct {
	records []*kgo.Record
	err     error
}

func (client *fakeCommitBatchClient) CommitRecords(
	_ context.Context,
	records ...*kgo.Record,
) error {

	client.records = append(
		client.records,
		records...,
	)

	return client.err
}

func TestCommitBatcherKeepsHighestOffsetPerPartition(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitBatchClient{}

	batcher, err :=
		newCommitBatcher(client)

	if err != nil {
		t.Fatal(err)
	}

	err = batcher.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    101,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	err = batcher.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    103,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	err = batcher.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    102,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	err = batcher.Flush(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(client.records) != 1 {
		t.Fatalf(
			"commit records=%d want=1",
			len(client.records),
		)
	}

	if client.records[0].Offset != 103 {
		t.Fatalf(
			"offset=%d want=103",
			client.records[0].Offset,
		)
	}
}

func TestCommitBatcherSupportsMultiplePartitions(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitBatchClient{}

	batcher, err :=
		newCommitBatcher(client)

	if err != nil {
		t.Fatal(err)
	}

	_ = batcher.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    100,
		},
	)

	_ = batcher.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 1,
			Offset:    200,
		},
	)

	err = batcher.Flush(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(client.records) != 2 {
		t.Fatalf(
			"records=%d want=2",
			len(client.records),
		)
	}
}

func TestCommitBatcherKeepsPendingAfterCommitFailure(
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

	err = batcher.Add(
		&kgo.Record{
			Topic:     "topic",
			Partition: 0,
			Offset:    100,
		},
	)

	if err != nil {
		t.Fatal(err)
	}

	err = batcher.Flush(
		context.Background(),
	)

	if err == nil {
		t.Fatal(
			"expected commit failure",
		)
	}

	client.err = nil

	err = batcher.Flush(
		context.Background(),
	)

	if err != nil {
		t.Fatal(err)
	}

	if len(client.records) != 2 {
		t.Fatalf(
			"commit calls=%d want=2",
			len(client.records),
		)
	}
}

func TestCommitBatcherRejectsNilRecord(
	t *testing.T,
) {
	t.Parallel()

	client := &fakeCommitBatchClient{}

	batcher, err :=
		newCommitBatcher(client)

	if err != nil {
		t.Fatal(err)
	}

	err = batcher.Add(nil)

	if err == nil {
		t.Fatal(
			"expected nil record error",
		)
	}
}
