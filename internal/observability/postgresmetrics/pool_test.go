package postgresmetrics

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestCollectorExportsPoolSnapshot(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	collector := newCollector(
		func() poolSnapshot {
			return poolSnapshot{
				acquiredConns:     3,
				idleConns:         5,
				constructingConns: 1,
				totalConns:        9,
				maxConns:          20,

				acquireCount:         42,
				canceledAcquireCount: 2,
				emptyAcquireCount:    7,

				acquireDuration:      1500 * time.Millisecond,
				emptyAcquireWaitTime: 250 * time.Millisecond,
			}
		},
	)

	registry.MustRegister(collector)

	body := scrapePostgresMetrics(
		t,
		registry,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_connections{state="acquired"} 3`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_connections{state="idle"} 5`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_connections{state="constructing"} 1`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_connections{state="total"} 9`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_max_connections 20`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_acquires_total 42`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_acquire_duration_seconds_total 1.5`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_canceled_acquires_total 2`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_empty_acquires_total 7`,
	)

	assertPostgresMetricContains(
		t,
		body,
		`astro_postgres_pool_empty_acquire_wait_seconds_total 0.25`,
	)
}

func TestNewRejectsMissingDependencies(
	t *testing.T,
) {
	t.Parallel()

	if _, err := New(
		nil,
		nil,
	); err == nil {
		t.Fatal(
			"New(nil registerer) error = nil",
		)
	}

	registry := prometheus.NewRegistry()

	if _, err := New(
		registry,
		nil,
	); err == nil {
		t.Fatal(
			"New(nil pool) error = nil",
		)
	}
}

func scrapePostgresMetrics(
	t *testing.T,
	registry *prometheus.Registry,
) string {
	t.Helper()

	handler := promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{},
	)

	request := httptest.NewRequest(
		http.MethodGet,
		"/metrics",
		nil,
	)

	response := httptest.NewRecorder()

	handler.ServeHTTP(
		response,
		request,
	)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"metrics status = %d, want %d",
			response.Code,
			http.StatusOK,
		)
	}

	return response.Body.String()
}

func assertPostgresMetricContains(
	t *testing.T,
	body string,
	want string,
) {
	t.Helper()

	if !strings.Contains(
		body,
		want,
	) {
		t.Fatalf(
			"metrics do not contain %q\nmetrics:\n%s",
			want,
			body,
		)
	}
}
