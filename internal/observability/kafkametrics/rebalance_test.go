package kafkametrics

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestRebalanceObserverExportsBlockedCallbackMetric(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	observer, err := NewRebalanceObserver(registry)
	if err != nil {
		t.Fatalf("NewRebalanceObserver() error = %v", err)
	}

	observer.OnPartitionsCallbackBlocked(context.Background(), nil)
	observer.OnPartitionsCallbackBlocked(context.Background(), nil)

	body := scrapeKafkaMetrics(t, registry)

	assertKafkaMetricContains(
		t,
		body,
		"astro_kafka_rebalance_callback_blocked_total 2",
	)
}

func TestNewRebalanceObserverRejectsNilRegisterer(t *testing.T) {
	t.Parallel()

	if _, err := NewRebalanceObserver(nil); err == nil {
		t.Fatal("NewRebalanceObserver(nil) error = nil")
	}
}
