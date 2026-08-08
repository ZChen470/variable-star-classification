package httpmetrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestMetricsWrapsMultipleTargets(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	metrics, err := New(registry)
	if err != nil {
		t.Fatalf(
			"New() error = %v",
			err,
		)
	}

	lightCurveTransport, err := metrics.WrapTransport(
		TargetLightCurve,
		roundTripperFunc(
			func(
				request *http.Request,
			) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusOK,
					Body:       http.NoBody,
					Request:    request,
				}, nil
			},
		),
	)
	if err != nil {
		t.Fatalf(
			"WrapTransport(lightcurve) error = %v",
			err,
		)
	}

	tritonTransport, err := metrics.WrapTransport(
		TargetTriton,
		roundTripperFunc(
			func(
				request *http.Request,
			) (*http.Response, error) {
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Body:       http.NoBody,
					Request:    request,
				}, nil
			},
		),
	)
	if err != nil {
		t.Fatalf(
			"WrapTransport(triton) error = %v",
			err,
		)
	}

	lightCurveRequest := httptest.NewRequest(
		http.MethodGet,
		"http://lightcurve.test/internal/v1/object",
		nil,
	)

	if _, err := lightCurveTransport.RoundTrip(
		lightCurveRequest,
	); err != nil {
		t.Fatalf(
			"lightcurve RoundTrip() error = %v",
			err,
		)
	}

	tritonRequest := httptest.NewRequest(
		http.MethodPost,
		"http://triton.test/v2/models/model/infer",
		nil,
	)

	if _, err := tritonTransport.RoundTrip(
		tritonRequest,
	); err != nil {
		t.Fatalf(
			"triton RoundTrip() error = %v",
			err,
		)
	}

	body := scrapeHTTPMetrics(
		t,
		registry,
	)

	assertHTTPMetricContains(
		t,
		body,
		`astro_http_client_requests_total{method="GET",status_class="2xx",target="lightcurve"} 1`,
	)

	assertHTTPMetricContains(
		t,
		body,
		`astro_http_client_requests_total{method="POST",status_class="5xx",target="triton"} 1`,
	)

	assertHTTPMetricContains(
		t,
		body,
		`astro_http_client_request_duration_seconds_count{method="GET",status_class="2xx",target="lightcurve"} 1`,
	)

	assertHTTPMetricContains(
		t,
		body,
		`astro_http_client_request_duration_seconds_count{method="POST",status_class="5xx",target="triton"} 1`,
	)
}

func TestMetricsRecordsTransportError(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	metrics, err := New(registry)
	if err != nil {
		t.Fatalf(
			"New() error = %v",
			err,
		)
	}

	expectedErr := errors.New(
		"upstream unavailable",
	)

	transport, err := metrics.WrapTransport(
		TargetTriton,
		roundTripperFunc(
			func(
				_ *http.Request,
			) (*http.Response, error) {
				return nil, expectedErr
			},
		),
	)
	if err != nil {
		t.Fatalf(
			"WrapTransport() error = %v",
			err,
		)
	}

	request := httptest.NewRequest(
		http.MethodPost,
		"http://triton.test/infer",
		nil,
	)

	_, gotErr := transport.RoundTrip(request)

	if !errors.Is(gotErr, expectedErr) {
		t.Fatalf(
			"errors.Is(error, expectedErr) = false; error=%v",
			gotErr,
		)
	}

	body := scrapeHTTPMetrics(
		t,
		registry,
	)

	assertHTTPMetricContains(
		t,
		body,
		`astro_http_client_requests_total{method="POST",status_class="transport_error",target="triton"} 1`,
	)

	if strings.Contains(
		body,
		expectedErr.Error(),
	) {
		t.Fatalf(
			"metrics contain transport error text %q",
			expectedErr,
		)
	}
}

func TestMetricsRejectsInvalidDependencies(
	t *testing.T,
) {
	t.Parallel()

	if _, err := New(nil); err == nil {
		t.Fatal(
			"New(nil) error = nil",
		)
	}

	registry := prometheus.NewRegistry()

	metrics, err := New(registry)
	if err != nil {
		t.Fatalf(
			"New() error = %v",
			err,
		)
	}

	if _, err := metrics.WrapTransport(
		Target("arbitrary"),
		nil,
	); err == nil {
		t.Fatal(
			"WrapTransport(invalid target) error = nil",
		)
	}
}

type roundTripperFunc func(
	*http.Request,
) (*http.Response, error)

func (function roundTripperFunc) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return function(request)
}

func scrapeHTTPMetrics(
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

func assertHTTPMetricContains(
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
