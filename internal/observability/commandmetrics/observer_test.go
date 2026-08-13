package commandmetrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestObserverExportsRetryMetrics(t *testing.T) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	observer, err := New(registry)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	observer.RetryAttempted()
	observer.RetryAttempted()
	observer.DLQPublished()

	body := scrapeCommandMetrics(t, registry)

	assertCommandMetricContains(
		t,
		body,
		"astro_classification_command_retry_attempts_total 2",
	)

	if strings.Contains(body, "astro_classification_command_retry_exhausted") {
		t.Fatal("command metrics unexpectedly contain obsolete retry exhaustion metric")
	}
	if strings.Contains(body, "astro_classification_command_dlq") {
		t.Fatal("command metrics unexpectedly duplicate Kafka DLQ metrics")
	}
}

func TestNewRejectsNilRegisterer(t *testing.T) {
	t.Parallel()

	if _, err := New(nil); err == nil {
		t.Fatal("New(nil) error = nil")
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
