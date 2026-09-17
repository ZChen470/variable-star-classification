package kafkabacklog

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type offsetAdmin interface {
	FetchOffsetsForTopics(
		ctx context.Context,
		group string,
		topics ...string,
	) (kadm.OffsetResponses, error)

	ListStartOffsets(
		ctx context.Context,
		topics ...string,
	) (kadm.ListedOffsets, error)

	ListEndOffsets(
		ctx context.Context,
		topics ...string,
	) (kadm.ListedOffsets, error)
}

// PositionReader resolves broker-authoritative consumer-group positions.
//
// The current classifier-worker uses franz-go's default earliest reset policy.
// Therefore, a missing or out-of-range committed offset resolves to the current
// log-start offset.
type PositionReader struct {
	admin offsetAdmin
}

func NewPositionReader(client *kgo.Client) (*PositionReader, error) {
	if client == nil {
		return nil, errors.New("Kafka backlog client must not be nil")
	}

	return newPositionReader(kadm.NewClient(client))
}

func newPositionReader(admin offsetAdmin) (*PositionReader, error) {
	if admin == nil {
		return nil, errors.New("Kafka backlog admin must not be nil")
	}

	return &PositionReader{
		admin: admin,
	}, nil
}

func (reader *PositionReader) PartitionPositions(
	ctx context.Context,
	group string,
	topic string,
) ([]PartitionPosition, error) {
	if reader == nil || reader.admin == nil {
		return nil, errors.New("Kafka backlog position reader must not be nil")
	}
	if ctx == nil {
		return nil, errors.New("Kafka backlog context must not be nil")
	}

	group = strings.TrimSpace(group)
	topic = strings.TrimSpace(topic)

	if group == "" {
		return nil, errors.New("Kafka backlog consumer group must not be blank")
	}
	if topic == "" {
		return nil, errors.New("Kafka backlog topic must not be blank")
	}

	committed, err := reader.admin.FetchOffsetsForTopics(
		ctx,
		group,
		topic,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"fetch Kafka consumer-group offsets: %w",
			err,
		)
	}
	if err := committed.Error(); err != nil {
		return nil, fmt.Errorf(
			"fetch Kafka consumer-group partition offset: %w",
			err,
		)
	}

	starts, err := reader.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf(
			"list Kafka partition start offsets: %w",
			err,
		)
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf(
			"list Kafka partition start offset: %w",
			err,
		)
	}

	ends, err := reader.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf(
			"list Kafka partition end offsets: %w",
			err,
		)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf(
			"list Kafka partition end offset: %w",
			err,
		)
	}

	endPartitions, exists := ends[topic]
	if !exists || len(endPartitions) == 0 {
		return nil, fmt.Errorf(
			"Kafka backlog topic %q has no end offsets",
			topic,
		)
	}

	startPartitions, exists := starts[topic]
	if !exists {
		return nil, fmt.Errorf(
			"Kafka backlog topic %q has no start offsets",
			topic,
		)
	}

	if len(startPartitions) != len(endPartitions) {
		return nil, fmt.Errorf(
			"Kafka backlog topic %q partition count mismatch: start=%d end=%d",
			topic,
			len(startPartitions),
			len(endPartitions),
		)
	}

	partitionIDs := make([]int, 0, len(endPartitions))
	for partition := range endPartitions {
		partitionIDs = append(partitionIDs, int(partition))
	}
	sort.Ints(partitionIDs)

	positions := make(
		[]PartitionPosition,
		0,
		len(partitionIDs),
	)

	for _, partitionID := range partitionIDs {
		partition := int32(partitionID)

		start, ok := starts.Lookup(topic, partition)
		if !ok {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d has no start offset",
				topic,
				partition,
			)
		}

		end, ok := ends.Lookup(topic, partition)
		if !ok {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d has no end offset",
				topic,
				partition,
			)
		}

		if start.Err != nil {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d start offset: %w",
				topic,
				partition,
				start.Err,
			)
		}
		if end.Err != nil {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d end offset: %w",
				topic,
				partition,
				end.Err,
			)
		}

		if start.Offset < 0 {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d invalid start offset %d",
				topic,
				partition,
				start.Offset,
			)
		}
		if end.Offset < start.Offset {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d end offset %d before start offset %d",
				topic,
				partition,
				end.Offset,
				start.Offset,
			)
		}

		commit, ok := committed.Lookup(topic, partition)
		if !ok {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d has no consumer-group offset response",
				topic,
				partition,
			)
		}
		if commit.Err != nil {
			return nil, fmt.Errorf(
				"Kafka backlog topic %q partition %d committed offset: %w",
				topic,
				partition,
				commit.Err,
			)
		}

		nextOffset := commit.At

		// FetchOffsetsForTopics returns -1 when the group has no commit.
		// A committed offset outside the retained log range is also resolved
		// using the worker's current earliest reset policy.
		if nextOffset < start.Offset || nextOffset > end.Offset {
			nextOffset = start.Offset
		}

		positions = append(
			positions,
			PartitionPosition{
				Partition:  partition,
				NextOffset: nextOffset,
				EndOffset:  end.Offset,
			},
		)
	}

	return positions, nil
}
