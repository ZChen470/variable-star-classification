package kafkabacklog

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSnapshotSource struct {
	snapshot Snapshot
	err      error

	gotGroup string
	gotTopic string
	calls    int
}

func (source *fakeSnapshotSource) Snapshot(
	_ context.Context,
	group string,
	topic string,
) (Snapshot, error) {
	source.calls++
	source.gotGroup = group
	source.gotTopic = topic

	if source.err != nil {
		return Snapshot{}, source.err
	}

	return source.snapshot, nil
}

type fakeSnapshotMetrics struct {
	applyErr error
	markErr  error

	applied Snapshot

	applyCalls int
	markCalls  int

	gotFailureGroup string
	gotFailureTopic string
}

func (metrics *fakeSnapshotMetrics) Apply(
	snapshot Snapshot,
) error {
	metrics.applyCalls++
	metrics.applied = snapshot
	return metrics.applyErr
}

func (metrics *fakeSnapshotMetrics) MarkSnapshotFailure(
	group string,
	topic string,
) error {
	metrics.markCalls++
	metrics.gotFailureGroup = group
	metrics.gotFailureTopic = topic
	return metrics.markErr
}

func TestRefresherRefreshPublishesSuccessfulSnapshot(
	t *testing.T,
) {
	now := time.Unix(1_700_000_000, 0)

	want := Snapshot{
		Group:      "command-group",
		Topic:      "command-topic",
		ObservedAt: now,
		Partitions: []PartitionSnapshot{
			{
				Partition:            0,
				Lag:                  3,
				OldestUncommittedAge: 5 * time.Second,
			},
		},
	}

	source := &fakeSnapshotSource{
		snapshot: want,
	}
	metrics := &fakeSnapshotMetrics{}

	refresher, err := newRefresher(
		source,
		metrics,
		" command-group ",
		" command-topic ",
	)
	if err != nil {
		t.Fatalf("newRefresher() error = %v", err)
	}

	if err := refresher.Refresh(
		context.Background(),
	); err != nil {
		t.Fatalf("Refresh() error = %v", err)
	}

	if source.calls != 1 {
		t.Fatalf(
			"source calls = %d, want 1",
			source.calls,
		)
	}
	if source.gotGroup != "command-group" {
		t.Fatalf(
			"source group = %q, want command-group",
			source.gotGroup,
		)
	}
	if source.gotTopic != "command-topic" {
		t.Fatalf(
			"source topic = %q, want command-topic",
			source.gotTopic,
		)
	}
	if metrics.applyCalls != 1 {
		t.Fatalf(
			"apply calls = %d, want 1",
			metrics.applyCalls,
		)
	}
	if metrics.markCalls != 0 {
		t.Fatalf(
			"mark failure calls = %d, want 0",
			metrics.markCalls,
		)
	}

	if metrics.applied.Group != want.Group ||
		metrics.applied.Topic != want.Topic ||
		!metrics.applied.ObservedAt.Equal(want.ObservedAt) {
		t.Fatalf(
			"applied snapshot = %+v, want %+v",
			metrics.applied,
			want,
		)
	}
}

func TestRefresherRefreshMarksSourceFailure(
	t *testing.T,
) {
	wantErr := errors.New("Kafka unavailable")

	source := &fakeSnapshotSource{
		err: wantErr,
	}
	metrics := &fakeSnapshotMetrics{}

	refresher, err := newRefresher(
		source,
		metrics,
		"command-group",
		"command-topic",
	)
	if err != nil {
		t.Fatalf("newRefresher() error = %v", err)
	}

	err = refresher.Refresh(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf(
			"Refresh() error = %v, want wrapped %v",
			err,
			wantErr,
		)
	}

	if metrics.applyCalls != 0 {
		t.Fatalf(
			"apply calls = %d, want 0",
			metrics.applyCalls,
		)
	}
	if metrics.markCalls != 1 {
		t.Fatalf(
			"mark failure calls = %d, want 1",
			metrics.markCalls,
		)
	}
	if metrics.gotFailureGroup != "command-group" {
		t.Fatalf(
			"failure group = %q, want command-group",
			metrics.gotFailureGroup,
		)
	}
	if metrics.gotFailureTopic != "command-topic" {
		t.Fatalf(
			"failure topic = %q, want command-topic",
			metrics.gotFailureTopic,
		)
	}
}

func TestRefresherRefreshMarksApplyFailure(
	t *testing.T,
) {
	wantErr := errors.New("metric publication failed")

	source := &fakeSnapshotSource{
		snapshot: Snapshot{
			Group:      "command-group",
			Topic:      "command-topic",
			ObservedAt: time.Unix(1, 0),
			Partitions: []PartitionSnapshot{
				{
					Partition: 0,
				},
			},
		},
	}
	metrics := &fakeSnapshotMetrics{
		applyErr: wantErr,
	}

	refresher, err := newRefresher(
		source,
		metrics,
		"command-group",
		"command-topic",
	)
	if err != nil {
		t.Fatalf("newRefresher() error = %v", err)
	}

	err = refresher.Refresh(context.Background())
	if !errors.Is(err, wantErr) {
		t.Fatalf(
			"Refresh() error = %v, want wrapped %v",
			err,
			wantErr,
		)
	}

	if metrics.applyCalls != 1 {
		t.Fatalf(
			"apply calls = %d, want 1",
			metrics.applyCalls,
		)
	}
	if metrics.markCalls != 1 {
		t.Fatalf(
			"mark failure calls = %d, want 1",
			metrics.markCalls,
		)
	}
}

func TestRefresherRefreshReturnsBothRefreshAndMarkFailure(
	t *testing.T,
) {
	sourceErr := errors.New("snapshot failed")
	markErr := errors.New("mark failed")

	refresher, err := newRefresher(
		&fakeSnapshotSource{
			err: sourceErr,
		},
		&fakeSnapshotMetrics{
			markErr: markErr,
		},
		"command-group",
		"command-topic",
	)
	if err != nil {
		t.Fatalf("newRefresher() error = %v", err)
	}

	err = refresher.Refresh(context.Background())

	if !errors.Is(err, sourceErr) {
		t.Fatalf(
			"Refresh() error = %v, want source error",
			err,
		)
	}
	if !errors.Is(err, markErr) {
		t.Fatalf(
			"Refresh() error = %v, want mark error",
			err,
		)
	}
}

func TestNewRefresherRejectsInvalidConfiguration(
	t *testing.T,
) {
	source := &fakeSnapshotSource{}
	metrics := &fakeSnapshotMetrics{}

	tests := []struct {
		name    string
		source  snapshotSource
		metrics snapshotMetrics
		group   string
		topic   string
	}{
		{
			name:    "nil source",
			metrics: metrics,
			group:   "group",
			topic:   "topic",
		},
		{
			name:   "nil metrics",
			source: source,
			group:  "group",
			topic:  "topic",
		},
		{
			name:    "blank group",
			source:  source,
			metrics: metrics,
			group:   " ",
			topic:   "topic",
		},
		{
			name:    "blank topic",
			source:  source,
			metrics: metrics,
			group:   "group",
			topic:   " ",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := newRefresher(
				test.source,
				test.metrics,
				test.group,
				test.topic,
			); err == nil {
				t.Fatal("newRefresher() error = nil")
			}
		})
	}
}

func TestRefresherRejectsNilContext(t *testing.T) {
	refresher, err := newRefresher(
		&fakeSnapshotSource{},
		&fakeSnapshotMetrics{},
		"group",
		"topic",
	)
	if err != nil {
		t.Fatalf("newRefresher() error = %v", err)
	}

	if err := refresher.Refresh(nil); err == nil {
		t.Fatal("Refresh(nil) error = nil")
	}
}
