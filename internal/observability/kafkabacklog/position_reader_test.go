package kafkabacklog

import (
	"context"
	"errors"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

type fakeOffsetAdmin struct {
	committed kadm.OffsetResponses
	starts    kadm.ListedOffsets
	ends      kadm.ListedOffsets

	committedErr error
	startErr     error
	endErr       error
}

func (admin *fakeOffsetAdmin) FetchOffsetsForTopics(
	context.Context,
	string,
	...string,
) (kadm.OffsetResponses, error) {
	return admin.committed, admin.committedErr
}

func (admin *fakeOffsetAdmin) ListStartOffsets(
	context.Context,
	...string,
) (kadm.ListedOffsets, error) {
	return admin.starts, admin.startErr
}

func (admin *fakeOffsetAdmin) ListEndOffsets(
	context.Context,
	...string,
) (kadm.ListedOffsets, error) {
	return admin.ends, admin.endErr
}

func TestPositionReaderPartitionPositions(t *testing.T) {
	const topic = "command-topic"

	reader, err := newPositionReader(&fakeOffsetAdmin{
		committed: kadm.OffsetResponses{
			topic: {
				0: {
					Offset: kadm.Offset{
						Topic:     topic,
						Partition: 0,
						At:        100,
					},
				},
				1: {
					Offset: kadm.Offset{
						Topic:     topic,
						Partition: 1,
						At:        -1,
					},
				},
				2: {
					Offset: kadm.Offset{
						Topic:     topic,
						Partition: 2,
						At:        5,
					},
				},
				3: {
					Offset: kadm.Offset{
						Topic:     topic,
						Partition: 3,
						At:        999,
					},
				},
			},
		},
		starts: kadm.ListedOffsets{
			topic: {
				0: listedOffset(topic, 0, 90),
				1: listedOffset(topic, 1, 50),
				2: listedOffset(topic, 2, 10),
				3: listedOffset(topic, 3, 10),
			},
		},
		ends: kadm.ListedOffsets{
			topic: {
				0: listedOffset(topic, 0, 110),
				1: listedOffset(topic, 1, 50),
				2: listedOffset(topic, 2, 20),
				3: listedOffset(topic, 3, 20),
			},
		},
	})
	if err != nil {
		t.Fatalf("newPositionReader() error = %v", err)
	}

	positions, err := reader.PartitionPositions(
		context.Background(),
		"command-group",
		topic,
	)
	if err != nil {
		t.Fatalf("PartitionPositions() error = %v", err)
	}

	want := []PartitionPosition{
		{
			Partition:  0,
			NextOffset: 100,
			EndOffset:  110,
		},
		{
			Partition:  1,
			NextOffset: 50,
			EndOffset:  50,
		},
		{
			Partition:  2,
			NextOffset: 10,
			EndOffset:  20,
		},
		{
			Partition:  3,
			NextOffset: 10,
			EndOffset:  20,
		},
	}

	if len(positions) != len(want) {
		t.Fatalf(
			"position count = %d, want %d",
			len(positions),
			len(want),
		)
	}

	for index := range want {
		if positions[index] != want[index] {
			t.Fatalf(
				"position[%d] = %+v, want %+v",
				index,
				positions[index],
				want[index],
			)
		}
	}
}

func TestPositionReaderRejectsPartitionSetMismatch(t *testing.T) {
	const topic = "command-topic"

	reader, err := newPositionReader(&fakeOffsetAdmin{
		committed: kadm.OffsetResponses{
			topic: {
				0: {
					Offset: kadm.Offset{
						Topic:     topic,
						Partition: 0,
						At:        0,
					},
				},
			},
		},
		starts: kadm.ListedOffsets{
			topic: {
				0: listedOffset(topic, 0, 0),
			},
		},
		ends: kadm.ListedOffsets{
			topic: {
				0: listedOffset(topic, 0, 0),
				1: listedOffset(topic, 1, 0),
			},
		},
	})
	if err != nil {
		t.Fatalf("newPositionReader() error = %v", err)
	}

	if _, err := reader.PartitionPositions(
		context.Background(),
		"command-group",
		topic,
	); err == nil {
		t.Fatal("PartitionPositions() error = nil")
	}
}

func TestPositionReaderPropagatesAdminFailure(t *testing.T) {
	wantErr := errors.New("Kafka unavailable")

	reader, err := newPositionReader(&fakeOffsetAdmin{
		committedErr: wantErr,
	})
	if err != nil {
		t.Fatalf("newPositionReader() error = %v", err)
	}

	if _, err := reader.PartitionPositions(
		context.Background(),
		"command-group",
		"command-topic",
	); !errors.Is(err, wantErr) {
		t.Fatalf(
			"PartitionPositions() error = %v, want wrapped %v",
			err,
			wantErr,
		)
	}
}

func listedOffset(
	topic string,
	partition int32,
	offset int64,
) kadm.ListedOffset {
	return kadm.ListedOffset{
		Topic:     topic,
		Partition: partition,
		Offset:    offset,
	}
}
