package workermetrics_test

import (
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/ZChen470/variable-star-classification/internal/observability/workermetrics"
)

func TestMetricsObserverRegistersWorkerMetrics(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	observer, err := workermetrics.NewObserver(
		registry,
		8,
	)
	if err != nil {
		t.Fatalf("create observer: %v", err)
	}

	if observer == nil {
		t.Fatal("observer is nil")
	}

	expected := `
		# HELP astro_classification_worker_pool_workers Configured classifier worker pool concurrency.
		# TYPE astro_classification_worker_pool_workers gauge
		astro_classification_worker_pool_workers 8

		# HELP astro_classification_worker_pool_active Currently active classifier worker pool workers.
		# TYPE astro_classification_worker_pool_active gauge
		astro_classification_worker_pool_active 0
	`

	if err := testutil.CollectAndCompare(
		registry,
		strings.NewReader(expected),
		"astro_classification_worker_pool_workers",
		"astro_classification_worker_pool_active",
	); err != nil {
		t.Fatalf("metrics mismatch: %v", err)
	}
}

func TestMetricsObserverTracksWorkerLifecycle(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	observer, err := workermetrics.NewObserver(
		registry,
		4,
	)
	if err != nil {
		t.Fatalf("create observer: %v", err)
	}

	observer.WorkerStarted()
	observer.WorkerStarted()
	observer.WorkerFinished()

	expected := `
		# HELP astro_classification_worker_pool_active Currently active classifier worker pool workers.
		# TYPE astro_classification_worker_pool_active gauge
		astro_classification_worker_pool_active 1
	`

	if err := testutil.CollectAndCompare(
		registry,
		strings.NewReader(expected),
		"astro_classification_worker_pool_active",
	); err != nil {
		t.Fatalf("active metric mismatch: %v", err)
	}
}

func TestMetricsObserverWorkerCountDoesNotChange(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	observer, err := workermetrics.NewObserver(
		registry,
		16,
	)
	if err != nil {
		t.Fatalf("create observer: %v", err)
	}

	observer.WorkerStarted()
	observer.WorkerFinished()

	expected := `
		# HELP astro_classification_worker_pool_workers Configured classifier worker pool concurrency.
		# TYPE astro_classification_worker_pool_workers gauge
		astro_classification_worker_pool_workers 16
	`

	if err := testutil.CollectAndCompare(
		registry,
		strings.NewReader(expected),
		"astro_classification_worker_pool_workers",
	); err != nil {
		t.Fatalf("worker count metric mismatch: %v", err)
	}
}
