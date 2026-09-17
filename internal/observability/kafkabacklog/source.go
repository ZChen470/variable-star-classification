package kafkabacklog

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// PartitionPosition is the broker-derived position used for one consumer-group
// partition snapshot.
//
// NextOffset is the effective next offset the consumer group should consume.
// The Kafka adapter is responsible for resolving missing or out-of-range group
// commits according to the configured offset-reset policy before returning it.
//
// EndOffset is the broker log-end offset.
//
// EndOffset-NextOffset is therefore an offset-distance lag. Kafka logs may have
// numeric offset gaps, so this value must not be interpreted as an exact count
// of physical records.
type PartitionPosition struct {
	Partition  int32
	NextOffset int64
	EndOffset  int64
}

// RecordPosition is the first actual Kafka record found at or after a requested
// offset.
type RecordPosition struct {
	Offset    int64
	Timestamp time.Time
}

// Reader supplies authoritative broker/group positions and record timestamps.
//
// Implementations must not join the consumer group whose lag is being observed.
type Reader interface {
	PartitionPositions(
		ctx context.Context,
		group string,
		topic string,
	) ([]PartitionPosition, error)

	FirstRecordAtOrAfter(
		ctx context.Context,
		topic string,
		partition int32,
		offset int64,
	) (RecordPosition, error)
}

// Source converts broker/group state into one complete metrics Snapshot.
type Source struct {
	reader Reader
	now    func() time.Time
}

func NewSource(reader Reader) (*Source, error) {
	return newSource(reader, time.Now)
}

func newSource(
	reader Reader,
	now func() time.Time,
) (*Source, error) {
	if reader == nil {
		return nil, errors.New("Kafka backlog reader must not be nil")
	}
	if now == nil {
		return nil, errors.New("Kafka backlog clock must not be nil")
	}

	return &Source{
		reader: reader,
		now:    now,
	}, nil
}

func (source *Source) Snapshot(
	ctx context.Context,
	group string,
	topic string,
) (Snapshot, error) {
	if source == nil || source.reader == nil {
		return Snapshot{}, errors.New("Kafka backlog source must not be nil")
	}
	if ctx == nil {
		return Snapshot{}, errors.New("Kafka backlog context must not be nil")
	}

	group = strings.TrimSpace(group)
	topic = strings.TrimSpace(topic)

	if group == "" {
		return Snapshot{}, errors.New("Kafka backlog consumer group must not be blank")
	}
	if topic == "" {
		return Snapshot{}, errors.New("Kafka backlog topic must not be blank")
	}

	positions, err := source.reader.PartitionPositions(
		ctx,
		group,
		topic,
	)
	if err != nil {
		return Snapshot{}, fmt.Errorf(
			"read Kafka backlog partition positions: %w",
			err,
		)
	}
	if len(positions) == 0 {
		return Snapshot{}, errors.New(
			"Kafka backlog partition positions must not be empty",
		)
	}

	observedAt := source.now()
	if observedAt.IsZero() {
		return Snapshot{}, errors.New(
			"Kafka backlog observation time must not be zero",
		)
	}

	partitions := make(
		[]PartitionSnapshot,
		0,
		len(positions),
	)
	seen := make(map[int32]struct{}, len(positions))

	for _, position := range positions {
		if position.Partition < 0 {
			return Snapshot{}, fmt.Errorf(
				"Kafka backlog partition must be non-negative: %d",
				position.Partition,
			)
		}
		if _, exists := seen[position.Partition]; exists {
			return Snapshot{}, fmt.Errorf(
				"Kafka backlog partition %d appears more than once",
				position.Partition,
			)
		}
		seen[position.Partition] = struct{}{}

		if position.NextOffset < 0 {
			return Snapshot{}, fmt.Errorf(
				"Kafka backlog partition %d next offset must be non-negative: %d",
				position.Partition,
				position.NextOffset,
			)
		}
		if position.EndOffset < position.NextOffset {
			return Snapshot{}, fmt.Errorf(
				"Kafka backlog partition %d end offset %d is before next offset %d",
				position.Partition,
				position.EndOffset,
				position.NextOffset,
			)
		}

		lag := position.EndOffset - position.NextOffset
		if lag == 0 {
			partitions = append(
				partitions,
				PartitionSnapshot{
					Partition: position.Partition,
					Lag:       0,
				},
			)
			continue
		}

		record, err := source.reader.FirstRecordAtOrAfter(
			ctx,
			topic,
			position.Partition,
			position.NextOffset,
		)
		if err != nil {
			return Snapshot{}, fmt.Errorf(
				"read Kafka backlog oldest record for partition %d: %w",
				position.Partition,
				err,
			)
		}
		if record.Offset < position.NextOffset {
			return Snapshot{}, fmt.Errorf(
				"Kafka backlog partition %d returned record offset %d before requested offset %d",
				position.Partition,
				record.Offset,
				position.NextOffset,
			)
		}
		if record.Offset >= position.EndOffset {
			return Snapshot{}, fmt.Errorf(
				"Kafka backlog partition %d returned record offset %d at or beyond log end %d",
				position.Partition,
				record.Offset,
				position.EndOffset,
			)
		}
		if record.Timestamp.IsZero() {
			return Snapshot{}, fmt.Errorf(
				"Kafka backlog partition %d oldest record timestamp must not be zero",
				position.Partition,
			)
		}

		age := observedAt.Sub(record.Timestamp)
		if age < 0 {
			age = 0
		}

		partitions = append(
			partitions,
			PartitionSnapshot{
				Partition:            position.Partition,
				Lag:                  lag,
				OldestUncommittedAge: age,
			},
		)
	}

	snapshot := Snapshot{
		Group:      group,
		Topic:      topic,
		ObservedAt: observedAt,
		Partitions: partitions,
	}

	if err := validateSnapshot(snapshot); err != nil {
		return Snapshot{}, fmt.Errorf(
			"validate Kafka backlog snapshot: %w",
			err,
		)
	}

	return snapshot, nil
}
