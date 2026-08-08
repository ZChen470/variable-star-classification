package management

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestReadinessStartsNotReadyAndTransitions(
	t *testing.T,
) {
	t.Parallel()

	readiness := NewReadiness()

	if readiness.IsReady() {
		t.Fatal("readiness starts ready, want not ready")
	}

	readiness.SetReady()

	if !readiness.IsReady() {
		t.Fatal("readiness after SetReady = false")
	}

	readiness.SetNotReady()

	if readiness.IsReady() {
		t.Fatal("readiness after SetNotReady = true")
	}
}

func TestManagementHandlerLiveEndpoint(
	t *testing.T,
) {
	t.Parallel()

	handler := newTestManagementHandler(t)

	request := httptest.NewRequest(
		http.MethodGet,
		"/live",
		nil,
	)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"/live status = %d, want %d",
			response.Code,
			http.StatusOK,
		)
	}

	if response.Body.String() != "ok\n" {
		t.Fatalf(
			"/live body = %q, want %q",
			response.Body.String(),
			"ok\n",
		)
	}
}

func TestManagementHandlerReadyEndpoint(
	t *testing.T,
) {
	t.Parallel()

	readiness := NewReadiness()
	registry := NewRegistry()

	handler, err := NewHandler(
		readiness,
		registry,
	)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/ready",
		nil,
	)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code !=
		http.StatusServiceUnavailable {
		t.Fatalf(
			"/ready not-ready status = %d, want %d",
			response.Code,
			http.StatusServiceUnavailable,
		)
	}

	readiness.SetReady()

	request = httptest.NewRequest(
		http.MethodGet,
		"/ready",
		nil,
	)
	response = httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"/ready ready status = %d, want %d",
			response.Code,
			http.StatusOK,
		)
	}

	if response.Body.String() != "ready\n" {
		t.Fatalf(
			"/ready body = %q, want %q",
			response.Body.String(),
			"ready\n",
		)
	}
}

func TestManagementHandlerMetricsEndpoint(
	t *testing.T,
) {
	t.Parallel()

	readiness := NewReadiness()
	registry := NewRegistry()

	testGauge := prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "management_contract_test_metric",
			Help: "Metric used to verify management exposition.",
		},
	)

	registry.MustRegister(testGauge)
	testGauge.Set(1)

	handler, err := NewHandler(
		readiness,
		registry,
	)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	request := httptest.NewRequest(
		http.MethodGet,
		"/metrics",
		nil,
	)
	response := httptest.NewRecorder()

	handler.ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"/metrics status = %d, want %d",
			response.Code,
			http.StatusOK,
		)
	}

	body := response.Body.String()

	if !strings.Contains(
		body,
		"management_contract_test_metric 1",
	) {
		t.Fatal(
			"/metrics does not contain registered test metric",
		)
	}

	if !strings.Contains(body, "go_goroutines") {
		t.Fatal(
			"/metrics does not contain Go runtime metrics",
		)
	}
}

func TestNewHandlerRejectsMissingDependencies(
	t *testing.T,
) {
	t.Parallel()

	registry := NewRegistry()

	if _, err := NewHandler(
		nil,
		registry,
	); err == nil {
		t.Fatal(
			"NewHandler(nil readiness) error = nil",
		)
	}

	if _, err := NewHandler(
		NewReadiness(),
		nil,
	); err == nil {
		t.Fatal(
			"NewHandler(nil gatherer) error = nil",
		)
	}
}

func newTestManagementHandler(
	t *testing.T,
) http.Handler {
	t.Helper()

	handler, err := NewHandler(
		NewReadiness(),
		NewRegistry(),
	)
	if err != nil {
		t.Fatalf("NewHandler() error = %v", err)
	}

	return handler
}
