package httpmetrics

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Target string

const (
	TargetLightCurve Target = "lightcurve"
	TargetTriton     Target = "triton"
)

type Metrics struct {
	requests *prometheus.CounterVec
	duration *prometheus.HistogramVec
}

func New(
	registerer prometheus.Registerer,
) (*Metrics, error) {
	if registerer == nil {
		return nil, errors.New(
			"HTTP metrics registerer must not be nil",
		)
	}

	metrics := &Metrics{
		requests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "http_client",
				Name:      "requests_total",
				Help:      "Number of outbound HTTP client requests.",
			},
			[]string{
				"target",
				"method",
				"status_class",
			},
		),
		duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "astro",
				Subsystem: "http_client",
				Name:      "request_duration_seconds",
				Help:      "Outbound HTTP client request duration.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{
				"target",
				"method",
				"status_class",
			},
		),
	}

	for _, collector := range []prometheus.Collector{
		metrics.requests,
		metrics.duration,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf(
				"register HTTP client metric: %w",
				err,
			)
		}
	}

	return metrics, nil
}

func (metrics *Metrics) WrapTransport(
	target Target,
	next http.RoundTripper,
) (http.RoundTripper, error) {
	if metrics == nil {
		return nil, errors.New(
			"HTTP metrics must not be nil",
		)
	}

	switch target {
	case TargetLightCurve, TargetTriton:
	default:
		return nil, fmt.Errorf(
			"unsupported HTTP metrics target %q",
			target,
		)
	}

	if next == nil {
		next = http.DefaultTransport
	}

	return &transport{
		target:  target,
		next:    next,
		metrics: metrics,
	}, nil
}

type transport struct {
	target  Target
	next    http.RoundTripper
	metrics *Metrics
}

func (transport *transport) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	if request == nil {
		return nil, errors.New(
			"HTTP metrics RoundTrip request must not be nil",
		)
	}

	startedAt := time.Now()

	response, err := transport.next.RoundTrip(request)

	statusClass := responseStatusClass(
		response,
		err,
	)

	method := requestMethodLabel(
		request.Method,
	)

	elapsed := time.Since(startedAt).Seconds()

	transport.metrics.requests.WithLabelValues(
		string(transport.target),
		method,
		statusClass,
	).Inc()

	transport.metrics.duration.WithLabelValues(
		string(transport.target),
		method,
		statusClass,
	).Observe(elapsed)

	return response, err
}

func requestMethodLabel(
	method string,
) string {
	switch method {
	case http.MethodGet:
		return http.MethodGet
	case http.MethodPost:
		return http.MethodPost
	default:
		return "OTHER"
	}
}

func responseStatusClass(
	response *http.Response,
	err error,
) string {
	if err != nil || response == nil {
		return "transport_error"
	}

	switch response.StatusCode / 100 {
	case 2:
		return "2xx"
	case 3:
		return "3xx"
	case 4:
		return "4xx"
	case 5:
		return "5xx"
	default:
		return "other"
	}
}
