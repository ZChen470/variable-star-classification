package management

import (
	"errors"
	"net/http"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Readiness stores the process readiness state.
//
// A process starts as not ready. Composition roots set it ready only after
// their startup gates have completed successfully, and set it not ready again
// when shutdown begins.
type Readiness struct {
	ready atomic.Bool
}

func NewReadiness() *Readiness {
	return &Readiness{}
}

func (readiness *Readiness) SetReady() {
	if readiness == nil {
		return
	}

	readiness.ready.Store(true)
}

func (readiness *Readiness) SetNotReady() {
	if readiness == nil {
		return
	}

	readiness.ready.Store(false)
}

func (readiness *Readiness) IsReady() bool {
	if readiness == nil {
		return false
	}

	return readiness.ready.Load()
}

// NewRegistry creates the process-local Prometheus registry.
//
// It deliberately does not use prometheus.DefaultRegisterer so each process
// owns an explicit registry that can later receive only its own application
// metrics.
func NewRegistry() *prometheus.Registry {
	registry := prometheus.NewRegistry()

	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(
			collectors.ProcessCollectorOpts{},
		),
		collectors.NewBuildInfoCollector(),
	)

	return registry
}

// NewHandler creates the private management HTTP handler.
//
// Endpoint contract:
//
//	GET /live    -> 200 while the process can serve management HTTP
//	GET /ready   -> 200 only when readiness is true, otherwise 503
//	GET /metrics -> Prometheus exposition for the supplied registry
func NewHandler(
	readiness *Readiness,
	gatherer prometheus.Gatherer,
) (http.Handler, error) {
	if readiness == nil {
		return nil, errors.New(
			"management readiness must not be nil",
		)
	}

	if gatherer == nil {
		return nil, errors.New(
			"management metrics gatherer must not be nil",
		)
	}

	mux := http.NewServeMux()

	mux.HandleFunc(
		"GET /live",
		func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			writer.Header().Set(
				"Content-Type",
				"text/plain; charset=utf-8",
			)

			writer.WriteHeader(http.StatusOK)

			_, _ = writer.Write([]byte("ok\n"))
		},
	)

	mux.HandleFunc(
		"GET /ready",
		func(
			writer http.ResponseWriter,
			_ *http.Request,
		) {
			if !readiness.IsReady() {
				http.Error(
					writer,
					"not ready",
					http.StatusServiceUnavailable,
				)
				return
			}

			writer.Header().Set(
				"Content-Type",
				"text/plain; charset=utf-8",
			)

			writer.WriteHeader(http.StatusOK)

			_, _ = writer.Write([]byte("ready\n"))
		},
	)

	mux.Handle(
		"GET /metrics",
		promhttp.HandlerFor(
			gatherer,
			promhttp.HandlerOpts{},
		),
	)

	return mux, nil
}
