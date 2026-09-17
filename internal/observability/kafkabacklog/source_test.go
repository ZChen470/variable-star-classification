package kafkabacklog

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeReader struct {
	positions []PartitionPosition
	records   map[int32]RecordPosition

	positionErr error
	recordErr   map[int32]error

	recordCalls map[int32]int
}

func (reader *fakeReader) PartitionPositions(
	context.Context,
	string,
	string,
) ([]PartitionPosition, error) {
	if reader.positionErr != nil {
		return nil, reader.positionErr
	}

	return append([]PartitionPosition(nil), reader.positions...), nil
}

func (reader *fakeReader) FirstRecordAtOrAfter(
	_ context.Context,
	_ string,
	partition int32,
	_ int64,
) (RecordPosition, error) {
	if reader.recordCalls == nil {
		reader.recordCalls = make(map[int32]int)
	}
	reader.recordCalls[partition]++

	if err := reader.recordErr[partition]; err != nil {
		return RecordPosition{}, err
	}

	return reader.records[partition], nil
}

func TestSourceSnapshot(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)

	reader := &fakeReader{
		positions: []PartitionPosition{
			{
				Partition:  0,
				NextOffset: 100,
				EndOffset:  107,
			},
			{
				Partition:  1,
				NextOffset: 200,
				EndOffset:  200,
			},
			{
				Partition:  2,
				NextOffset: 300,
				EndOffset:  305,
			},
		},
		records: map[int32]RecordPosition{
			0: {
				Offset:    100,
				Timestamp: now.Add(-12 * time.Second),
			},
			2: {
				// Offset gaps are valid: the first actual record does not
				// need to have exactly the requested numeric offset.
				Offset:    303,
				Timestamp: now.Add(-25 * time.Second),
			},
		},
		recordErr: make(map[int32]error),
	}

	source, err := newSource(reader, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatalf("newSource() error = %v", err)
	}

	snapshot, err := source.Snapshot(
		context.Background(),
		" command-group ",
		" command-topic ",
	)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if snapshot.Group != "command-group" {
		t.Fatalf(
			"snapshot group = %q, want command-group",
			snapshot.Group,
		)
	}
	if snapshot.Topic != "command-topic" {
		t.Fatalf(
			"snapshot topic = %q, want command-topic",
			snapshot.Topic,
		)
	}
	if !snapshot.ObservedAt.Equal(now) {
		t.Fatalf(
			"snapshot observed at = %v, want %v",
			snapshot.ObservedAt,
			now,
		)
	}

	if len(snapshot.Partitions) != 3 {
		t.Fatalf(
			"partition count = %d, want 3",
			len(snapshot.Partitions),
		)
	}

	assertPartitionSnapshot(
		t,
		snapshot.Partitions[0],
		0,
		7,
		12*time.Second,
	)
	assertPartitionSnapshot(
		t,
		snapshot.Partitions[1],
		1,
		0,
		0,
	)
	assertPartitionSnapshot(
		t,
		snapshot.Partitions[2],
		2,
		5,
		25*time.Second,
	)

	if got := reader.recordCalls[0]; got != 1 {
		t.Fatalf("partition 0 record calls = %d, want 1", got)
	}
	if got := reader.recordCalls[1]; got != 0 {
		t.Fatalf(
			"partition 1 record calls = %d, want 0 for zero lag",
			got,
		)
	}
	if got := reader.recordCalls[2]; got != 1 {
		t.Fatalf("partition 2 record calls = %d, want 1", got)
	}
}

func TestSourceSnapshotClampsFutureRecordAgeToZero(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)

	reader := &fakeReader{
		positions: []PartitionPosition{
			{
				Partition:  0,
				NextOffset: 10,
				EndOffset:  11,
			},
		},
		records: map[int32]RecordPosition{
			0: {
				Offset:    10,
				Timestamp: now.Add(time.Second),
			},
		},
		recordErr: make(map[int32]error),
	}

	source, err := newSource(reader, func() time.Time {
		return now
	})
	if err != nil {
		t.Fatalf("newSource() error = %v", err)
	}

	snapshot, err := source.Snapshot(
		context.Background(),
		"command-group",
		"command-topic",
	)
	if err != nil {
		t.Fatalf("Snapshot() error = %v", err)
	}

	if got := snapshot.Partitions[0].OldestUncommittedAge; got != 0 {
		t.Fatalf("oldest age = %v, want 0", got)
	}
}

func TestSourceSnapshotPropagatesReaderFailure(t *testing.T) {
	wantErr := errors.New("broker unavailable")

	source, err := newSource(
		&fakeReader{
			positionErr: wantErr,
		},
		func() time.Time {
			return time.Unix(1, 0)
		},
	)
	if err != nil {
		t.Fatalf("newSource() error = %v", err)
	}

	if _, err := source.Snapshot(
		context.Background(),
		"command-group",
		"command-topic",
	); !errors.Is(err, wantErr) {
		t.Fatalf(
			"Snapshot() error = %v, want wrapped %v",
			err,
			wantErr,
		)
	}
}

func TestSourceSnapshotRejectsInvalidPositions(t *testing.T) {
	tests := []struct {
		name      string
		positions []PartitionPosition
	}{
		{
			name: "empty",
		},
		{
			name: "negative partition",
			positions: []PartitionPosition{
				{
					Partition:  -1,
					NextOffset: 0,
					EndOffset:  0,
				},
			},
		},
		{
			name: "duplicate partition",
			positions: []PartitionPosition{
				{
					Partition:  0,
					NextOffset: 0,
					EndOffset:  0,
				},
				{
					Partition:  0,
					NextOffset: 0,
					EndOffset:  0,
				},
			},
		},
		{
			name: "negative next offset",
			positions: []PartitionPosition{
				{
					Partition:  0,
					NextOffset: -1,
					EndOffset:  0,
				},
			},
		},
		{
			name: "end before next",
			positions: []PartitionPosition{
				{
					Partition:  0,
					NextOffset: 10,
					EndOffset:  9,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, err := newSource(
				&fakeReader{
					positions: test.positions,
					records:   make(map[int32]RecordPosition),
					recordErr: make(map[int32]error),
				},
				func() time.Time {
					return time.Unix(1, 0)
				},
			)
			if err != nil {
				t.Fatalf("newSource() error = %v", err)
			}

			if _, err := source.Snapshot(
				context.Background(),
				"command-group",
				"command-topic",
			); err == nil {
				t.Fatal("Snapshot() error = nil")
			}
		})
	}
}

func TestSourceSnapshotRejectsInvalidOldestRecord(t *testing.T) {
	now := time.Unix(1_700_000_100, 0)

	tests := []struct {
		name   string
		record RecordPosition
	}{
		{
			name: "record before requested offset",
			record: RecordPosition{
				Offset:    9,
				Timestamp: now,
			},
		},
		{
			name: "record at log end",
			record: RecordPosition{
				Offset:    20,
				Timestamp: now,
			},
		},
		{
			name: "zero timestamp",
			record: RecordPosition{
				Offset: 10,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source, err := newSource(
				&fakeReader{
					positions: []PartitionPosition{
						{
							Partition:  0,
							NextOffset: 10,
							EndOffset:  20,
						},
					},
					records: map[int32]RecordPosition{
						0: test.record,
					},
					recordErr: make(map[int32]error),
				},
				func() time.Time {
					return now
				},
			)
			if err != nil {
				t.Fatalf("newSource() error = %v", err)
			}

			if _, err := source.Snapshot(
				context.Background(),
				"command-group",
				"command-topic",
			); err == nil {
				t.Fatal("Snapshot() error = nil")
			}
		})
	}
}

func assertPartitionSnapshot(
	t *testing.T,
	got PartitionSnapshot,
	wantPartition int32,
	wantLag int64,
	wantAge time.Duration,
) {
	t.Helper()

	if got.Partition != wantPartition {
		t.Fatalf(
			"partition = %d, want %d",
			got.Partition,
			wantPartition,
		)
	}
	if got.Lag != wantLag {
		t.Fatalf(
			"partition %d lag = %d, want %d",
			got.Partition,
			got.Lag,
			wantLag,
		)
	}
	if got.OldestUncommittedAge != wantAge {
		t.Fatalf(
			"partition %d oldest age = %v, want %v",
			got.Partition,
			got.OldestUncommittedAge,
			wantAge,
		)
	}
}
