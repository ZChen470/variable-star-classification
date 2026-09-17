package kafkabacklog

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

type snapshotSource interface {
	Snapshot(
		ctx context.Context,
		group string,
		topic string,
	) (Snapshot, error)
}

type snapshotMetrics interface {
	Apply(snapshot Snapshot) error
	MarkSnapshotFailure(group, topic string) error
}

// Refresher performs one atomic logical backlog refresh.
//
// A successful refresh publishes the complete snapshot.
//
// A failed source read or failed snapshot publication marks snapshot_success=0
// while preserving the last successful lag, age, and timestamp series. This
// allows downstream autoscaling logic to reject stale data explicitly instead
// of treating a refresh failure as zero backlog.
type Refresher struct {
	source  snapshotSource
	metrics snapshotMetrics
	group   string
	topic   string
}

func NewRefresher(
	source *Source,
	metrics *Metrics,
	group string,
	topic string,
) (*Refresher, error) {
	return newRefresher(
		source,
		metrics,
		group,
		topic,
	)
}

func newRefresher(
	source snapshotSource,
	metrics snapshotMetrics,
	group string,
	topic string,
) (*Refresher, error) {
	if source == nil {
		return nil, errors.New(
			"Kafka backlog snapshot source must not be nil",
		)
	}
	if metrics == nil {
		return nil, errors.New(
			"Kafka backlog snapshot metrics must not be nil",
		)
	}

	group = strings.TrimSpace(group)
	topic = strings.TrimSpace(topic)

	if group == "" {
		return nil, errors.New(
			"Kafka backlog consumer group must not be blank",
		)
	}
	if topic == "" {
		return nil, errors.New(
			"Kafka backlog topic must not be blank",
		)
	}

	return &Refresher{
		source:  source,
		metrics: metrics,
		group:   group,
		topic:   topic,
	}, nil
}

func (refresher *Refresher) Refresh(
	ctx context.Context,
) error {
	if refresher == nil ||
		refresher.source == nil ||
		refresher.metrics == nil {
		return errors.New(
			"Kafka backlog refresher must not be nil",
		)
	}
	if ctx == nil {
		return errors.New(
			"Kafka backlog refresh context must not be nil",
		)
	}

	snapshot, err := refresher.source.Snapshot(
		ctx,
		refresher.group,
		refresher.topic,
	)
	if err != nil {
		refreshErr := fmt.Errorf(
			"read Kafka backlog snapshot: %w",
			err,
		)

		if markErr := refresher.metrics.MarkSnapshotFailure(
			refresher.group,
			refresher.topic,
		); markErr != nil {
			return errors.Join(
				refreshErr,
				fmt.Errorf(
					"mark Kafka backlog snapshot failure: %w",
					markErr,
				),
			)
		}

		return refreshErr
	}

	if err := refresher.metrics.Apply(snapshot); err != nil {
		publishErr := fmt.Errorf(
			"publish Kafka backlog snapshot metrics: %w",
			err,
		)

		if markErr := refresher.metrics.MarkSnapshotFailure(
			refresher.group,
			refresher.topic,
		); markErr != nil {
			return errors.Join(
				publishErr,
				fmt.Errorf(
					"mark Kafka backlog snapshot failure: %w",
					markErr,
				),
			)
		}

		return publishErr
	}

	return nil
}
