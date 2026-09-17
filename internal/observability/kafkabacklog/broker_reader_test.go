package kafkabacklog

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakePartitionPositionReader struct {
	positions []PartitionPosition
	err       error

	gotGroup string
	gotTopic string
	calls    int
}

func (reader *fakePartitionPositionReader) PartitionPositions(
	_ context.Context,
	group string,
	topic string,
) ([]PartitionPosition, error) {
	reader.calls++
	reader.gotGroup = group
	reader.gotTopic = topic

	if reader.err != nil {
		return nil, reader.err
	}

	return append([]PartitionPosition(nil), reader.positions...), nil
}

type fakeOldestRecordReader struct {
	record RecordPosition
	err    error

	gotTopic     string
	gotPartition int32
	gotOffset    int64
	calls        int
}

func (reader *fakeOldestRecordReader) FirstRecordAtOrAfter(
	_ context.Context,
	topic string,
	partition int32,
	offset int64,
) (RecordPosition, error) {
	reader.calls++
	reader.gotTopic = topic
	reader.gotPartition = partition
	reader.gotOffset = offset

	if reader.err != nil {
		return RecordPosition{}, reader.err
	}

	return reader.record, nil
}

func TestBrokerReaderDelegatesPartitionPositions(t *testing.T) {
	positions := &fakePartitionPositionReader{
		positions: []PartitionPosition{
			{
				Partition:  2,
				NextOffset: 100,
				EndOffset:  120,
			},
		},
	}
	records := &fakeOldestRecordReader{}

	reader, err := newBrokerReader(
		positions,
		records,
	)
	if err != nil {
		t.Fatalf("newBrokerReader() error = %v", err)
	}

	got, err := reader.PartitionPositions(
		context.Background(),
		"command-group",
		"command-topic",
	)
	if err != nil {
		t.Fatalf("PartitionPositions() error = %v", err)
	}

	if positions.calls != 1 {
		t.Fatalf(
			"PartitionPositions() calls = %d, want 1",
			positions.calls,
		)
	}
	if positions.gotGroup != "command-group" {
		t.Fatalf(
			"group = %q, want command-group",
			positions.gotGroup,
		)
	}
	if positions.gotTopic != "command-topic" {
		t.Fatalf(
			"topic = %q, want command-topic",
			positions.gotTopic,
		)
	}

	if len(got) != 1 || got[0] != positions.positions[0] {
		t.Fatalf(
			"positions = %+v, want %+v",
			got,
			positions.positions,
		)
	}
}

func TestBrokerReaderDelegatesFirstRecordAtOrAfter(t *testing.T) {
	timestamp := time.Unix(1_700_000_000, 0)

	positions := &fakePartitionPositionReader{}
	records := &fakeOldestRecordReader{
		record: RecordPosition{
			Offset:    103,
			Timestamp: timestamp,
		},
	}

	reader, err := newBrokerReader(
		positions,
		records,
	)
	if err != nil {
		t.Fatalf("newBrokerReader() error = %v", err)
	}

	got, err := reader.FirstRecordAtOrAfter(
		context.Background(),
		"command-topic",
		2,
		100,
	)
	if err != nil {
		t.Fatalf(
			"FirstRecordAtOrAfter() error = %v",
			err,
		)
	}

	if records.calls != 1 {
		t.Fatalf(
			"FirstRecordAtOrAfter() calls = %d, want 1",
			records.calls,
		)
	}
	if records.gotTopic != "command-topic" {
		t.Fatalf(
			"topic = %q, want command-topic",
			records.gotTopic,
		)
	}
	if records.gotPartition != 2 {
		t.Fatalf(
			"partition = %d, want 2",
			records.gotPartition,
		)
	}
	if records.gotOffset != 100 {
		t.Fatalf(
			"offset = %d, want 100",
			records.gotOffset,
		)
	}
	if got.Offset != 103 {
		t.Fatalf(
			"record offset = %d, want 103",
			got.Offset,
		)
	}
	if !got.Timestamp.Equal(timestamp) {
		t.Fatalf(
			"timestamp = %v, want %v",
			got.Timestamp,
			timestamp,
		)
	}
}

func TestBrokerReaderPropagatesPositionFailure(t *testing.T) {
	wantErr := errors.New("position lookup failed")

	reader, err := newBrokerReader(
		&fakePartitionPositionReader{
			err: wantErr,
		},
		&fakeOldestRecordReader{},
	)
	if err != nil {
		t.Fatalf("newBrokerReader() error = %v", err)
	}

	if _, err := reader.PartitionPositions(
		context.Background(),
		"command-group",
		"command-topic",
	); !errors.Is(err, wantErr) {
		t.Fatalf(
			"PartitionPositions() error = %v, want %v",
			err,
			wantErr,
		)
	}
}

func TestBrokerReaderPropagatesRecordFailure(t *testing.T) {
	wantErr := errors.New("record lookup failed")

	reader, err := newBrokerReader(
		&fakePartitionPositionReader{},
		&fakeOldestRecordReader{
			err: wantErr,
		},
	)
	if err != nil {
		t.Fatalf("newBrokerReader() error = %v", err)
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

func TestNewBrokerReaderRejectsNilDependencies(t *testing.T) {
	if _, err := newBrokerReader(
		nil,
		&fakeOldestRecordReader{},
	); err == nil {
		t.Fatal(
			"newBrokerReader(nil, records) error = nil",
		)
	}

	if _, err := newBrokerReader(
		&fakePartitionPositionReader{},
		nil,
	); err == nil {
		t.Fatal(
			"newBrokerReader(positions, nil) error = nil",
		)
	}
}
