package kafkabacklog

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type fakeDirectRecordClient struct {
	fetches []kgo.Fetches
	index   int
	closed  bool
}

func (client *fakeDirectRecordClient) PollRecords(
	ctx context.Context,
	_ int,
) kgo.Fetches {
	if client.index < len(client.fetches) {
		fetches := client.fetches[client.index]
		client.index++
		return fetches
	}

	<-ctx.Done()
	return kgo.NewErrFetch(ctx.Err())
}

func (client *fakeDirectRecordClient) Close() {
	client.closed = true
}

func TestDirectRecordReaderFirstRecordAtOrAfter(t *testing.T) {
	const (
		topic     = "command-topic"
		partition = int32(2)
		offset    = int64(100)
	)

	timestamp := time.Unix(1_700_000_000, 0)

	client := &fakeDirectRecordClient{
		fetches: []kgo.Fetches{
			successFetch(
				topic,
				partition,
				103,
				timestamp,
			),
		},
	}

	var gotTopic string
	var gotPartition int32
	var gotOffset int64

	reader, err := newDirectRecordReader(
		func(
			topic string,
			partition int32,
			offset int64,
		) (directRecordClient, error) {
			gotTopic = topic
			gotPartition = partition
			gotOffset = offset
			return client, nil
		},
	)
	if err != nil {
		t.Fatalf(
			"newDirectRecordReader() error = %v",
			err,
		)
	}

	record, err := reader.FirstRecordAtOrAfter(
		context.Background(),
		topic,
		partition,
		offset,
	)
	if err != nil {
		t.Fatalf(
			"FirstRecordAtOrAfter() error = %v",
			err,
		)
	}

	if gotTopic != topic {
		t.Fatalf(
			"factory topic = %q, want %q",
			gotTopic,
			topic,
		)
	}
	if gotPartition != partition {
		t.Fatalf(
			"factory partition = %d, want %d",
			gotPartition,
			partition,
		)
	}
	if gotOffset != offset {
		t.Fatalf(
			"factory offset = %d, want %d",
			gotOffset,
			offset,
		)
	}

	// Numeric Kafka offsets may have gaps. Returning the first actual
	// record after the requested offset is valid.
	if record.Offset != 103 {
		t.Fatalf(
			"record offset = %d, want 103",
			record.Offset,
		)
	}
	if !record.Timestamp.Equal(timestamp) {
		t.Fatalf(
			"record timestamp = %v, want %v",
			record.Timestamp,
			timestamp,
		)
	}
	if !client.closed {
		t.Fatal("direct client was not closed")
	}
}

func TestDirectRecordReaderSkipsEmptyFetch(t *testing.T) {
	const (
		topic     = "command-topic"
		partition = int32(0)
		offset    = int64(10)
	)

	timestamp := time.Unix(1_700_000_000, 0)

	client := &fakeDirectRecordClient{
		fetches: []kgo.Fetches{
			{},
			successFetch(
				topic,
				partition,
				offset,
				timestamp,
			),
		},
	}

	reader, err := newDirectRecordReader(
		func(
			string,
			int32,
			int64,
		) (directRecordClient, error) {
			return client, nil
		},
	)
	if err != nil {
		t.Fatalf(
			"newDirectRecordReader() error = %v",
			err,
		)
	}

	record, err := reader.FirstRecordAtOrAfter(
		context.Background(),
		topic,
		partition,
		offset,
	)
	if err != nil {
		t.Fatalf(
			"FirstRecordAtOrAfter() error = %v",
			err,
		)
	}

	if record.Offset != offset {
		t.Fatalf(
			"record offset = %d, want %d",
			record.Offset,
			offset,
		)
	}
}

func TestDirectRecordReaderRejectsRecordBeforeRequestedOffset(
	t *testing.T,
) {
	const (
		topic     = "command-topic"
		partition = int32(1)
		offset    = int64(50)
	)

	client := &fakeDirectRecordClient{
		fetches: []kgo.Fetches{
			successFetch(
				topic,
				partition,
				49,
				time.Unix(1_700_000_000, 0),
			),
		},
	}

	reader, err := newDirectRecordReader(
		func(
			string,
			int32,
			int64,
		) (directRecordClient, error) {
			return client, nil
		},
	)
	if err != nil {
		t.Fatalf(
			"newDirectRecordReader() error = %v",
			err,
		)
	}

	if _, err := reader.FirstRecordAtOrAfter(
		context.Background(),
		topic,
		partition,
		offset,
	); err == nil {
		t.Fatal(
			"FirstRecordAtOrAfter() error = nil",
		)
	}
}

func TestDirectRecordReaderPropagatesFetchFailure(t *testing.T) {
	wantErr := errors.New("Kafka fetch failed")

	client := &fakeDirectRecordClient{
		fetches: []kgo.Fetches{
			kgo.NewErrFetch(wantErr),
		},
	}

	reader, err := newDirectRecordReader(
		func(
			string,
			int32,
			int64,
		) (directRecordClient, error) {
			return client, nil
		},
	)
	if err != nil {
		t.Fatalf(
			"newDirectRecordReader() error = %v",
			err,
		)
	}

	if _, err := reader.FirstRecordAtOrAfter(
		context.Background(),
		"command-topic",
		0,
		10,
	); !errors.Is(err, wantErr) {
		t.Fatalf(
			"FirstRecordAtOrAfter() error = %v, want wrapped %v",
			err,
			wantErr,
		)
	}

	if !client.closed {
		t.Fatal("direct client was not closed")
	}
}

func TestDirectRecordReaderPropagatesFactoryFailure(t *testing.T) {
	wantErr := errors.New("client creation failed")

	reader, err := newDirectRecordReader(
		func(
			string,
			int32,
			int64,
		) (directRecordClient, error) {
			return nil, wantErr
		},
	)
	if err != nil {
		t.Fatalf(
			"newDirectRecordReader() error = %v",
			err,
		)
	}

	if _, err := reader.FirstRecordAtOrAfter(
		context.Background(),
		"command-topic",
		0,
		10,
	); !errors.Is(err, wantErr) {
		t.Fatalf(
			"FirstRecordAtOrAfter() error = %v, want %v",
			err,
			wantErr,
		)
	}
}

func TestDirectRecordReaderRejectsInvalidInput(t *testing.T) {
	reader, err := newDirectRecordReader(
		func(
			string,
			int32,
			int64,
		) (directRecordClient, error) {
			t.Fatal("factory must not be called")
			return nil, nil
		},
	)
	if err != nil {
		t.Fatalf(
			"newDirectRecordReader() error = %v",
			err,
		)
	}

	tests := []struct {
		name      string
		topic     string
		partition int32
		offset    int64
	}{
		{
			name:      "blank topic",
			topic:     " ",
			partition: 0,
			offset:    0,
		},
		{
			name:      "negative partition",
			topic:     "command-topic",
			partition: -1,
			offset:    0,
		},
		{
			name:      "negative offset",
			topic:     "command-topic",
			partition: 0,
			offset:    -1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := reader.FirstRecordAtOrAfter(
				context.Background(),
				test.topic,
				test.partition,
				test.offset,
			); err == nil {
				t.Fatal(
					"FirstRecordAtOrAfter() error = nil",
				)
			}
		})
	}
}

func successFetch(
	topic string,
	partition int32,
	offset int64,
	timestamp time.Time,
) kgo.Fetches {
	return kgo.Fetches{
		{
			Topics: []kgo.FetchTopic{
				{
					Topic: topic,
					Partitions: []kgo.FetchPartition{
						{
							Partition: partition,
							Records: []*kgo.Record{
								{
									Topic:     topic,
									Partition: partition,
									Offset:    offset,
									Timestamp: timestamp,
								},
							},
						},
					},
				},
			},
		},
	}
}
