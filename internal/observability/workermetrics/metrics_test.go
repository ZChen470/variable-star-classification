package workermetrics_test

import (
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/observability/workermetrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsObserverExportsConfiguredConcurrency(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()
	if _, err := workermetrics.NewObserver(registry, 8); err != nil {
		t.Fatalf("create observer: %v", err)
	}

	expected := `
		# HELP astro_classification_worker_concurrency Configured maximum number of records polled and processed concurrently by the classifier worker.
		# TYPE astro_classification_worker_concurrency gauge
		astro_classification_worker_concurrency 8
	`
	if err := testutil.CollectAndCompare(
		registry,
		strings.NewReader(expected),
		"astro_classification_worker_concurrency",
	); err != nil {
		t.Fatalf("metrics mismatch: %v", err)
	}
}
