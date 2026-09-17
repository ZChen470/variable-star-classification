package kafkabacklog

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

type directRecordClient interface {
	PollRecords(ctx context.Context, maxPollRecords int) kgo.Fetches
	Close()
}

type directRecordClientFactory func(
	topic string,
	partition int32,
	offset int64,
) (directRecordClient, error)

// DirectRecordReader reads one actual Kafka record from an exact partition
// position without joining a consumer group.
//
// A short-lived direct consumer is used for each lookup. The exporter is a
// singleton and the number of lookups per refresh is bounded by the number of
// lagging partitions, so this favors simple and explicit offset semantics over
// maintaining mutable long-lived direct-consumer assignments.
type DirectRecordReader struct {
	newClient directRecordClientFactory
}

// NewDirectRecordReader derives broker, authentication, TLS, and other base
// client options from baseClient. baseClient itself is not used for consuming.
func NewDirectRecordReader(
	baseClient *kgo.Client,
) (*DirectRecordReader, error) {
	if baseClient == nil {
		return nil, errors.New(
			"Kafka backlog base client must not be nil",
		)
	}

	baseOpts := append(
		[]kgo.Opt(nil),
		baseClient.Opts()...,
	)

	return newDirectRecordReader(
		func(
			topic string,
			partition int32,
			offset int64,
		) (directRecordClient, error) {
			opts := append(
				[]kgo.Opt(nil),
				baseOpts...,
			)

			opts = append(
				opts,
				kgo.ConsumePartitions(
					map[string]map[int32]kgo.Offset{
						topic: {
							partition: kgo.
								NoResetOffset().
								At(offset),
						},
					},
				),
			)

			client, err := kgo.NewClient(opts...)
			if err != nil {
				return nil, fmt.Errorf(
					"create Kafka backlog direct consumer: %w",
					err,
				)
			}

			return client, nil
		},
	)
}

func newDirectRecordReader(
	factory directRecordClientFactory,
) (*DirectRecordReader, error) {
	if factory == nil {
		return nil, errors.New(
			"Kafka backlog direct consumer factory must not be nil",
		)
	}

	return &DirectRecordReader{
		newClient: factory,
	}, nil
}

func (reader *DirectRecordReader) FirstRecordAtOrAfter(
	ctx context.Context,
	topic string,
	partition int32,
	offset int64,
) (RecordPosition, error) {
	if reader == nil || reader.newClient == nil {
		return RecordPosition{}, errors.New(
			"Kafka backlog direct record reader must not be nil",
		)
	}
	if ctx == nil {
		return RecordPosition{}, errors.New(
			"Kafka backlog context must not be nil",
		)
	}

	topic = strings.TrimSpace(topic)

	if topic == "" {
		return RecordPosition{}, errors.New(
			"Kafka backlog topic must not be blank",
		)
	}
	if partition < 0 {
		return RecordPosition{}, fmt.Errorf(
			"Kafka backlog partition must be non-negative: %d",
			partition,
		)
	}
	if offset < 0 {
		return RecordPosition{}, fmt.Errorf(
			"Kafka backlog record offset must be non-negative: %d",
			offset,
		)
	}

	client, err := reader.newClient(
		topic,
		partition,
		offset,
	)
	if err != nil {
		return RecordPosition{}, err
	}
	defer client.Close()

	for {
		fetches := client.PollRecords(ctx, 1)

		if fetchErr := fetches.Err(); fetchErr != nil {
			return RecordPosition{}, fmt.Errorf(
				"fetch Kafka backlog record for topic %q partition %d at offset %d: %w",
				topic,
				partition,
				offset,
				fetchErr,
			)
		}

		records := fetches.Records()
		if len(records) == 0 {
			continue
		}

		record := records[0]

		if record.Topic != topic {
			return RecordPosition{}, fmt.Errorf(
				"Kafka backlog direct consumer returned topic %q, want %q",
				record.Topic,
				topic,
			)
		}
		if record.Partition != partition {
			return RecordPosition{}, fmt.Errorf(
				"Kafka backlog direct consumer returned partition %d, want %d",
				record.Partition,
				partition,
			)
		}
		if record.Offset < offset {
			return RecordPosition{}, fmt.Errorf(
				"Kafka backlog direct consumer returned offset %d before requested offset %d",
				record.Offset,
				offset,
			)
		}

		return RecordPosition{
			Offset:    record.Offset,
			Timestamp: record.Timestamp,
		}, nil
	}
}
