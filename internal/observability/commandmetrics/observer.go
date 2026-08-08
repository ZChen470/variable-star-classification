package commandmetrics

import (
	"errors"
	"fmt"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/prometheus/client_golang/prometheus"
)

// Observer exposes ClassificationCommand retry behavior as low-cardinality
// Prometheus metrics.
//
// Command DLQ publication is intentionally not duplicated here because the
// existing Kafka produce metric already observes successful/error production
// for the configured DLQ topic.
type Observer struct {
	retryAttempts  prometheus.Counter
	retryExhausted prometheus.Counter
}

var _ application.ClassificationCommandObserver = (*Observer)(nil)

func New(
	registerer prometheus.Registerer,
) (*Observer, error) {
	if registerer == nil {
		return nil, errors.New(
			"classification command metrics registerer must not be nil",
		)
	}

	observer := &Observer{
		retryAttempts: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "classification_command",
				Name:      "retry_attempts_total",
				Help:      "Number of additional ClassificationCommand retry attempts actually executed.",
			},
		),

		retryExhausted: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "classification_command",
				Name:      "retry_exhausted_total",
				Help:      "Number of ClassificationCommands that exhausted the finite retry budget.",
			},
		),
	}

	for _, collector := range []prometheus.Collector{
		observer.retryAttempts,
		observer.retryExhausted,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf(
				"register classification command metric: %w",
				err,
			)
		}
	}

	return observer, nil
}

func (observer *Observer) RetryAttempted() {
	if observer == nil {
		return
	}

	observer.retryAttempts.Inc()
}

func (observer *Observer) RetryExhausted() {
	if observer == nil {
		return
	}

	observer.retryExhausted.Inc()
}

func (*Observer) DLQPublished() {
	// Intentionally no metric.
	//
	// Successful Command DLQ publication is already visible through
	// astro_kafka_produce_records_total for the configured DLQ topic.
}
