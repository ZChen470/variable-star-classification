package kafkabacklog

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// PartitionSnapshot is one broker-derived consumer-group backlog observation.
//
// Lag is Kafka offset lag, not an exact count of physical records. Kafka logs
// may contain offset gaps, so callers must not interpret an offset delta as an
// exact processed-record count.
//
// OldestUncommittedAge is the age of the first actual record available at or
// after the effective committed offset. It must be zero when Lag is zero.
type PartitionSnapshot struct {
	Partition            int32
	Lag                  int64
	OldestUncommittedAge time.Duration
}

// Snapshot is one complete successful observation for one consumer group and
// topic. Apply validates the whole snapshot before publishing any metric.
type Snapshot struct {
	Group      string
	Topic      string
	ObservedAt time.Time
	Partitions []PartitionSnapshot
}

type seriesKey struct {
	group string
	topic string
}

// Metrics stores the latest successfully observed Kafka consumer-group backlog.
//
// A failed refresh does not overwrite the previous lag/age values. Instead,
// snapshot_success becomes zero while snapshot_timestamp_seconds remains the
// time of the last successful refresh. This allows control-plane queries to
// reject stale data explicitly.
type Metrics struct {
	mu sync.Mutex

	lag               *prometheus.GaugeVec
	oldestAge         *prometheus.GaugeVec
	snapshotSuccess   *prometheus.GaugeVec
	snapshotTimestamp *prometheus.GaugeVec

	partitions map[seriesKey]map[string]struct{}
}

// New registers the Kafka consumer-group backlog metric contract.
func New(registerer prometheus.Registerer) (*Metrics, error) {
	if registerer == nil {
		return nil, errors.New("Kafka backlog metrics registerer must not be nil")
	}

	metrics := &Metrics{
		lag: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "astro",
				Subsystem: "kafka_consumer_group",
				Name:      "lag",
				Help:      "Current Kafka consumer-group lag in offsets for one topic partition.",
			},
			[]string{"group", "topic", "partition"},
		),
		oldestAge: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "astro",
				Subsystem: "kafka_consumer_group",
				Name:      "oldest_uncommitted_age_seconds",
				Help:      "Age in seconds of the first actual record at or after the effective committed offset, or zero when lag is zero.",
			},
			[]string{"group", "topic", "partition"},
		),
		snapshotSuccess: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "astro",
				Subsystem: "kafka_consumer_group",
				Name:      "snapshot_success",
				Help:      "Whether the most recent Kafka consumer-group backlog refresh succeeded.",
			},
			[]string{"group", "topic"},
		),
		snapshotTimestamp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "astro",
				Subsystem: "kafka_consumer_group",
				Name:      "snapshot_timestamp_seconds",
				Help:      "Unix timestamp of the most recent successful Kafka consumer-group backlog snapshot.",
			},
			[]string{"group", "topic"},
		),
		partitions: make(map[seriesKey]map[string]struct{}),
	}

	for _, collector := range []prometheus.Collector{
		metrics.lag,
		metrics.oldestAge,
		metrics.snapshotSuccess,
		metrics.snapshotTimestamp,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register Kafka backlog metric: %w", err)
		}
	}

	return metrics, nil
}

// Apply atomically validates and publishes one complete successful snapshot.
func (metrics *Metrics) Apply(snapshot Snapshot) error {
	if metrics == nil {
		return errors.New("Kafka backlog metrics must not be nil")
	}
	if err := validateSnapshot(snapshot); err != nil {
		return err
	}

	metrics.mu.Lock()
	defer metrics.mu.Unlock()

	key := seriesKey{
		group: snapshot.Group,
		topic: snapshot.Topic,
	}
	currentPartitions := make(map[string]struct{}, len(snapshot.Partitions))

	for _, partition := range snapshot.Partitions {
		partitionLabel := strconv.FormatInt(int64(partition.Partition), 10)
		currentPartitions[partitionLabel] = struct{}{}

		metrics.lag.WithLabelValues(
			snapshot.Group,
			snapshot.Topic,
			partitionLabel,
		).Set(float64(partition.Lag))

		metrics.oldestAge.WithLabelValues(
			snapshot.Group,
			snapshot.Topic,
			partitionLabel,
		).Set(partition.OldestUncommittedAge.Seconds())
	}

	for partitionLabel := range metrics.partitions[key] {
		if _, exists := currentPartitions[partitionLabel]; exists {
			continue
		}

		metrics.lag.DeleteLabelValues(
			snapshot.Group,
			snapshot.Topic,
			partitionLabel,
		)
		metrics.oldestAge.DeleteLabelValues(
			snapshot.Group,
			snapshot.Topic,
			partitionLabel,
		)
	}

	metrics.partitions[key] = currentPartitions
	metrics.snapshotSuccess.WithLabelValues(
		snapshot.Group,
		snapshot.Topic,
	).Set(1)
	metrics.snapshotTimestamp.WithLabelValues(
		snapshot.Group,
		snapshot.Topic,
	).Set(
		float64(snapshot.ObservedAt.UnixNano()) / float64(time.Second),
	)

	return nil
}

// MarkSnapshotFailure marks the latest refresh as failed while preserving the
// last successful lag, age, and timestamp values.
func (metrics *Metrics) MarkSnapshotFailure(group, topic string) error {
	if metrics == nil {
		return errors.New("Kafka backlog metrics must not be nil")
	}
	if strings.TrimSpace(group) == "" {
		return errors.New("Kafka backlog consumer group must not be blank")
	}
	if strings.TrimSpace(topic) == "" {
		return errors.New("Kafka backlog topic must not be blank")
	}

	metrics.snapshotSuccess.WithLabelValues(group, topic).Set(0)

	return nil
}

func validateSnapshot(snapshot Snapshot) error {
	if strings.TrimSpace(snapshot.Group) == "" {
		return errors.New("Kafka backlog consumer group must not be blank")
	}
	if strings.TrimSpace(snapshot.Topic) == "" {
		return errors.New("Kafka backlog topic must not be blank")
	}
	if snapshot.ObservedAt.IsZero() {
		return errors.New("Kafka backlog snapshot time must not be zero")
	}
	if len(snapshot.Partitions) == 0 {
		return errors.New("Kafka backlog snapshot must contain at least one partition")
	}

	seen := make(map[int32]struct{}, len(snapshot.Partitions))

	for _, partition := range snapshot.Partitions {
		if partition.Partition < 0 {
			return fmt.Errorf(
				"Kafka backlog partition must be non-negative: %d",
				partition.Partition,
			)
		}
		if _, exists := seen[partition.Partition]; exists {
			return fmt.Errorf(
				"Kafka backlog partition %d appears more than once",
				partition.Partition,
			)
		}
		seen[partition.Partition] = struct{}{}

		if partition.Lag < 0 {
			return fmt.Errorf(
				"Kafka backlog partition %d lag must be non-negative: %d",
				partition.Partition,
				partition.Lag,
			)
		}
		if partition.OldestUncommittedAge < 0 {
			return fmt.Errorf(
				"Kafka backlog partition %d oldest age must be non-negative",
				partition.Partition,
			)
		}
		if partition.Lag == 0 && partition.OldestUncommittedAge != 0 {
			return fmt.Errorf(
				"Kafka backlog partition %d oldest age must be zero when lag is zero",
				partition.Partition,
			)
		}
	}

	return nil
}
