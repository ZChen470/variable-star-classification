package commandmetrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestObserverExportsRetryMetrics(t *testing.T) {
	registry := prometheus.NewRegistry()

	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	observer, err := newObserver(
		registry,
		func() time.Time {
			return now
		},
	)
	if err != nil {
		t.Fatalf("newObserver() error = %v", err)
	}

	body := scrapeCommandMetrics(t, registry)
	assertCommandMetricContains(t, body, "astro_classification_command_retrying 0")
	assertCommandMetricContains(t, body, "astro_classification_command_retry_age_seconds 0")

	observer.RetryStarted()
	observer.RetryAttempted()
	observer.RetryAttempted()

	now = now.Add(75 * time.Second)

	body = scrapeCommandMetrics(t, registry)
	assertCommandMetricContains(t, body, "astro_classification_command_retry_attempts_total 2")
	assertCommandMetricContains(t, body, "astro_classification_command_retrying 1")
	assertCommandMetricContains(t, body, "astro_classification_command_retry_age_seconds 75")

	observer.RetryFinished()

	body = scrapeCommandMetrics(t, registry)
	assertCommandMetricContains(t, body, "astro_classification_command_retrying 0")
	assertCommandMetricContains(t, body, "astro_classification_command_retry_age_seconds 0")

	if strings.Contains(body, "astro_classification_command_retry_exhausted") {
		t.Fatal("command metrics unexpectedly contain obsolete retry exhaustion metric")
	}
	if strings.Contains(body, "astro_classification_command_dlq") {
		t.Fatal("command metrics unexpectedly duplicate Kafka DLQ metrics")
	}
}

func TestNewRejectsInvalidArguments(t *testing.T) {
	registry := prometheus.NewRegistry()

	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) error = nil")
	}
	if _, err := newObserver(registry, nil); err == nil {
		t.Fatal("newObserver(registry, nil) error = nil")
	}
}

func scrapeCommandMetrics(t *testing.T, registry *prometheus.Registry) string {
	t.Helper()

	handler := promhttp.HandlerFor(registry, promhttp.HandlerOpts{})
	request := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("metrics status = %d, want %d", response.Code, http.StatusOK)
	}

	return response.Body.String()
}

func assertCommandMetricContains(t *testing.T, body string, want string) {
	t.Helper()

	if !strings.Contains(body, want) {
		t.Fatalf("metrics do not contain %q\nmetrics:\n%s", want, body)
	}
}
