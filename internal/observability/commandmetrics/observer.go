package commandmetrics

import (
	"errors"
	"fmt"
	"sync"
	"time"

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
	mu                 sync.RWMutex
	now                func() time.Time
	retrying           int
	retryStartedAt     time.Time
	retryAttempts      prometheus.Counter
	retryingGauge      prometheus.GaugeFunc
	retryAgeGauge      prometheus.GaugeFunc
	processingDuration prometheus.Histogram
	stageDuration      *prometheus.HistogramVec
	inflight           prometheus.Gauge
}

var _ application.ClassificationCommandObserver = (*Observer)(nil)

func New(registerer prometheus.Registerer) (*Observer, error) {
	return newObserver(registerer, time.Now)
}

func newObserver(registerer prometheus.Registerer, now func() time.Time) (*Observer, error) {
	if registerer == nil {
		return nil, errors.New("classification command metrics registerer must not be nil")
	}
	if now == nil {
		return nil, errors.New("classification command metrics clock must not be nil")
	}

	observer := &Observer{
		now: now,
		retryAttempts: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "classification_command",
				Name:      "retry_attempts_total",
				Help:      "Number of additional ClassificationCommand retry attempts actually executed.",
			},
		),
	}

	observer.retryingGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "astro",
			Subsystem: "classification_command",
			Name:      "retrying",
			Help:      "Number of ClassificationCommands currently in retry state.",
		},
		observer.retryingValue,
	)

	observer.retryAgeGauge = prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: "astro",
			Subsystem: "classification_command",
			Name:      "retry_age_seconds",
			Help:      "Seconds since the current ClassificationCommand entered retry state, or zero when not retrying.",
		},
		observer.retryAgeSeconds,
	)

	observer.processingDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "astro",
			Subsystem: "classification_command",
			Name:      "processing_duration_seconds",
			Help:      "Time spent in one ClassificationWorker attempt, including failed attempts.",
		},
	)

	observer.inflight = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "astro",
			Subsystem: "classification_command",
			Name:      "inflight",
			Help:      "Number of ClassificationCommands currently being processed by this worker.",
		},
	)

	observer.stageDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "astro",
			Subsystem: "classification_command",
			Name:      "stage_duration_seconds",
			Help:      "Time spent in each low-cardinality ClassificationCommand processing stage, including failed attempts.",
		},
		[]string{"stage"},
	)

	for _, collector := range []prometheus.Collector{
		observer.retryAttempts,
		observer.retryingGauge,
		observer.retryAgeGauge,
		observer.processingDuration,
		observer.stageDuration,
		observer.inflight,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf("register classification command metric: %w", err)
		}
	}

	return observer, nil
}

func (observer *Observer) RetryStarted() {
	if observer == nil {
		return
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()

	observer.retrying++
	if observer.retrying == 1 {
		observer.retryStartedAt = observer.now()
	}
}

func (observer *Observer) RetryAttempted() {
	if observer == nil {
		return
	}

	observer.retryAttempts.Inc()
}

func (observer *Observer) RetryFinished() {
	if observer == nil {
		return
	}

	observer.mu.Lock()
	defer observer.mu.Unlock()

	if observer.retrying > 0 {
		observer.retrying--
	}
	if observer.retrying == 0 {
		observer.retryStartedAt = time.Time{}
	}
}

func (*Observer) DLQPublished() {
	// Intentionally no metric.
	//
	// Successful Command DLQ publication is already visible through
	// astro_kafka_produce_records_total for the configured DLQ topic.
}

func (observer *Observer) retryingValue() float64 {
	observer.mu.RLock()
	defer observer.mu.RUnlock()

	return float64(observer.retrying)
}

func (observer *Observer) retryAgeSeconds() float64 {
	observer.mu.RLock()
	defer observer.mu.RUnlock()

	if observer.retrying == 0 || observer.retryStartedAt.IsZero() {
		return 0
	}

	age := observer.now().Sub(observer.retryStartedAt).Seconds()
	if age < 0 {
		return 0
	}

	return age
}

func (observer *Observer) CommandStarted() {
	if observer == nil {
		return
	}

	observer.inflight.Inc()
}

func (observer *Observer) CommandFinished(duration time.Duration) {
	if observer == nil {
		return
	}

	observer.inflight.Dec()
	observer.processingDuration.Observe(duration.Seconds())
}

func (observer *Observer) StageFinished(
	stage application.ClassificationCommandStage,
	duration time.Duration,
) {
	if observer == nil {
		return
	}

	observer.stageDuration.WithLabelValues(string(stage)).Observe(duration.Seconds())
}
