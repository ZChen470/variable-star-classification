package kafkabacklog

import (
	"context"
	"errors"
)

type partitionPositionReader interface {
	PartitionPositions(
		ctx context.Context,
		group string,
		topic string,
	) ([]PartitionPosition, error)
}

type oldestRecordReader interface {
	FirstRecordAtOrAfter(
		ctx context.Context,
		topic string,
		partition int32,
		offset int64,
	) (RecordPosition, error)
}

// BrokerReader combines broker/group offset lookup with direct record lookup.
// It implements Reader without joining or mutating the observed consumer group.
type BrokerReader struct {
	positions partitionPositionReader
	records   oldestRecordReader
}

var _ Reader = (*BrokerReader)(nil)

func NewBrokerReader(
	positionReader *PositionReader,
	recordReader *DirectRecordReader,
) (*BrokerReader, error) {
	if positionReader == nil {
		return nil, errors.New(
			"Kafka backlog position reader must not be nil",
		)
	}
	if recordReader == nil {
		return nil, errors.New(
			"Kafka backlog record reader must not be nil",
		)
	}

	return &BrokerReader{
		positions: positionReader,
		records:   recordReader,
	}, nil
}

func newBrokerReader(
	positions partitionPositionReader,
	records oldestRecordReader,
) (*BrokerReader, error) {
	if positions == nil {
		return nil, errors.New(
			"Kafka backlog position source must not be nil",
		)
	}
	if records == nil {
		return nil, errors.New(
			"Kafka backlog record source must not be nil",
		)
	}

	return &BrokerReader{
		positions: positions,
		records:   records,
	}, nil
}

func (reader *BrokerReader) PartitionPositions(
	ctx context.Context,
	group string,
	topic string,
) ([]PartitionPosition, error) {
	if reader == nil || reader.positions == nil {
		return nil, errors.New(
			"Kafka backlog broker reader must not be nil",
		)
	}

	return reader.positions.PartitionPositions(
		ctx,
		group,
		topic,
	)
}

func (reader *BrokerReader) FirstRecordAtOrAfter(
	ctx context.Context,
	topic string,
	partition int32,
	offset int64,
) (RecordPosition, error) {
	if reader == nil || reader.records == nil {
		return RecordPosition{}, errors.New(
			"Kafka backlog broker reader must not be nil",
		)
	}

	return reader.records.FirstRecordAtOrAfter(
		ctx,
		topic,
		partition,
		offset,
	)
}
