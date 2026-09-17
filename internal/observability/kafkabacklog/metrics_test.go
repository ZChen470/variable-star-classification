package kafkabacklog

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsApplySnapshot(t *testing.T) {
	registry := prometheus.NewRegistry()

	metrics, err := New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	observedAt := time.Unix(1_700_000_000, 0)

	err = metrics.Apply(Snapshot{
		Group:      "command-group",
		Topic:      "command-topic",
		ObservedAt: observedAt,
		Partitions: []PartitionSnapshot{
			{
				Partition:            0,
				Lag:                  7,
				OldestUncommittedAge: 12 * time.Second,
			},
			{
				Partition:            1,
				Lag:                  0,
				OldestUncommittedAge: 0,
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if got := testutil.ToFloat64(
		metrics.lag.WithLabelValues(
			"command-group",
			"command-topic",
			"0",
		),
	); got != 7 {
		t.Fatalf("partition 0 lag = %v, want 7", got)
	}

	if got := testutil.ToFloat64(
		metrics.oldestAge.WithLabelValues(
			"command-group",
			"command-topic",
			"0",
		),
	); got != 12 {
		t.Fatalf("partition 0 oldest age = %v, want 12", got)
	}

	if got := testutil.ToFloat64(
		metrics.oldestAge.WithLabelValues(
			"command-group",
			"command-topic",
			"1",
		),
	); got != 0 {
		t.Fatalf("partition 1 oldest age = %v, want 0", got)
	}

	if got := testutil.ToFloat64(
		metrics.snapshotSuccess.WithLabelValues(
			"command-group",
			"command-topic",
		),
	); got != 1 {
		t.Fatalf("snapshot success = %v, want 1", got)
	}

	if got := testutil.ToFloat64(
		metrics.snapshotTimestamp.WithLabelValues(
			"command-group",
			"command-topic",
		),
	); got != float64(observedAt.Unix()) {
		t.Fatalf(
			"snapshot timestamp = %v, want %v",
			got,
			float64(observedAt.Unix()),
		)
	}
}

func TestMetricsMarkSnapshotFailurePreservesLastSuccessfulSnapshot(t *testing.T) {
	registry := prometheus.NewRegistry()

	metrics, err := New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	observedAt := time.Unix(1_700_000_000, 0)

	err = metrics.Apply(Snapshot{
		Group:      "command-group",
		Topic:      "command-topic",
		ObservedAt: observedAt,
		Partitions: []PartitionSnapshot{
			{
				Partition:            0,
				Lag:                  9,
				OldestUncommittedAge: 30 * time.Second,
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	if err := metrics.MarkSnapshotFailure(
		"command-group",
		"command-topic",
	); err != nil {
		t.Fatalf("MarkSnapshotFailure() error = %v", err)
	}

	if got := testutil.ToFloat64(
		metrics.snapshotSuccess.WithLabelValues(
			"command-group",
			"command-topic",
		),
	); got != 0 {
		t.Fatalf("snapshot success = %v, want 0", got)
	}

	if got := testutil.ToFloat64(
		metrics.lag.WithLabelValues(
			"command-group",
			"command-topic",
			"0",
		),
	); got != 9 {
		t.Fatalf("partition 0 lag after failure = %v, want 9", got)
	}

	if got := testutil.ToFloat64(
		metrics.oldestAge.WithLabelValues(
			"command-group",
			"command-topic",
			"0",
		),
	); got != 30 {
		t.Fatalf(
			"partition 0 oldest age after failure = %v, want 30",
			got,
		)
	}

	if got := testutil.ToFloat64(
		metrics.snapshotTimestamp.WithLabelValues(
			"command-group",
			"command-topic",
		),
	); got != float64(observedAt.Unix()) {
		t.Fatalf(
			"snapshot timestamp after failure = %v, want %v",
			got,
			float64(observedAt.Unix()),
		)
	}
}

func TestMetricsApplyRemovesMissingPartitionSeries(t *testing.T) {
	registry := prometheus.NewRegistry()

	metrics, err := New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	firstObservedAt := time.Unix(1_700_000_000, 0)

	err = metrics.Apply(Snapshot{
		Group:      "command-group",
		Topic:      "command-topic",
		ObservedAt: firstObservedAt,
		Partitions: []PartitionSnapshot{
			{
				Partition:            0,
				Lag:                  2,
				OldestUncommittedAge: 4 * time.Second,
			},
			{
				Partition:            1,
				Lag:                  3,
				OldestUncommittedAge: 5 * time.Second,
			},
		},
	})
	if err != nil {
		t.Fatalf("first Apply() error = %v", err)
	}

	err = metrics.Apply(Snapshot{
		Group:      "command-group",
		Topic:      "command-topic",
		ObservedAt: firstObservedAt.Add(time.Second),
		Partitions: []PartitionSnapshot{
			{
				Partition:            0,
				Lag:                  1,
				OldestUncommittedAge: 6 * time.Second,
			},
		},
	})
	if err != nil {
		t.Fatalf("second Apply() error = %v", err)
	}

	body := scrapeRegistry(t, registry)

	if strings.Contains(
		body,
		`partition="1"`,
	) {
		t.Fatalf("metrics still contain removed partition series:\n%s", body)
	}
}

func TestMetricsRejectsInvalidSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		snapshot Snapshot
	}{
		{
			name: "blank group",
			snapshot: Snapshot{
				Group:      " ",
				Topic:      "command-topic",
				ObservedAt: time.Unix(1, 0),
				Partitions: []PartitionSnapshot{{Partition: 0}},
			},
		},
		{
			name: "blank topic",
			snapshot: Snapshot{
				Group:      "command-group",
				Topic:      " ",
				ObservedAt: time.Unix(1, 0),
				Partitions: []PartitionSnapshot{{Partition: 0}},
			},
		},
		{
			name: "zero observed time",
			snapshot: Snapshot{
				Group: "command-group",
				Topic: "command-topic",
				Partitions: []PartitionSnapshot{
					{Partition: 0},
				},
			},
		},
		{
			name: "no partitions",
			snapshot: Snapshot{
				Group:      "command-group",
				Topic:      "command-topic",
				ObservedAt: time.Unix(1, 0),
			},
		},
		{
			name: "negative partition",
			snapshot: Snapshot{
				Group:      "command-group",
				Topic:      "command-topic",
				ObservedAt: time.Unix(1, 0),
				Partitions: []PartitionSnapshot{
					{Partition: -1},
				},
			},
		},
		{
			name: "duplicate partition",
			snapshot: Snapshot{
				Group:      "command-group",
				Topic:      "command-topic",
				ObservedAt: time.Unix(1, 0),
				Partitions: []PartitionSnapshot{
					{Partition: 0},
					{Partition: 0},
				},
			},
		},
		{
			name: "negative lag",
			snapshot: Snapshot{
				Group:      "command-group",
				Topic:      "command-topic",
				ObservedAt: time.Unix(1, 0),
				Partitions: []PartitionSnapshot{
					{
						Partition: 0,
						Lag:       -1,
					},
				},
			},
		},
		{
			name: "negative age",
			snapshot: Snapshot{
				Group:      "command-group",
				Topic:      "command-topic",
				ObservedAt: time.Unix(1, 0),
				Partitions: []PartitionSnapshot{
					{
						Partition:            0,
						Lag:                  1,
						OldestUncommittedAge: -time.Second,
					},
				},
			},
		},
		{
			name: "age with zero lag",
			snapshot: Snapshot{
				Group:      "command-group",
				Topic:      "command-topic",
				ObservedAt: time.Unix(1, 0),
				Partitions: []PartitionSnapshot{
					{
						Partition:            0,
						Lag:                  0,
						OldestUncommittedAge: time.Second,
					},
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			registry := prometheus.NewRegistry()

			metrics, err := New(registry)
			if err != nil {
				t.Fatalf("New() error = %v", err)
			}

			if err := metrics.Apply(test.snapshot); err == nil {
				t.Fatal("Apply() error = nil")
			}
		})
	}
}

func scrapeRegistry(
	t *testing.T,
	registry *prometheus.Registry,
) string {
	t.Helper()

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)

	promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{},
	).ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf(
			"/metrics status = %d, want %d",
			recorder.Code,
			http.StatusOK,
		)
	}

	return recorder.Body.String()
}
