package kafkametrics

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

const unknownTopic = "unknown"

// Hook exposes low-cardinality Kafka client metrics through franz-go hooks.
//
// It deliberately does not label metrics by:
//   - broker host;
//   - partition;
//   - consumer group;
//   - client ID;
//   - error text.
//
// Those dimensions either duplicate process configuration or can create
// unnecessary metric cardinality.
type Hook struct {
	recordsPolled         *prometheus.CounterVec
	produceRecords        *prometheus.CounterVec
	brokerRequestDuration *prometheus.HistogramVec
	groupManageErrors     prometheus.Counter
}

var (
	_ kgo.HookFetchRecordUnbuffered   = (*Hook)(nil)
	_ kgo.HookProduceRecordUnbuffered = (*Hook)(nil)
	_ kgo.HookBrokerE2E               = (*Hook)(nil)
	_ kgo.HookGroupManageError        = (*Hook)(nil)
)

// New creates and registers the Kafka metrics hook against a process-local
// Prometheus registry.
func New(
	registerer prometheus.Registerer,
) (*Hook, error) {
	if registerer == nil {
		return nil, errors.New(
			"Kafka metrics registerer must not be nil",
		)
	}

	hook := &Hook{
		recordsPolled: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "kafka",
				Name:      "records_polled_total",
				Help:      "Number of Kafka records returned from polling.",
			},
			[]string{"topic"},
		),
		produceRecords: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "kafka",
				Name:      "produce_records_total",
				Help:      "Number of Kafka produce record completions.",
			},
			[]string{
				"topic",
				"result",
			},
		),
		brokerRequestDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "astro",
				Subsystem: "kafka",
				Name:      "broker_request_duration_seconds",
				Help:      "Kafka broker request end-to-end duration.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{
				"api_key",
				"result",
			},
		),
		groupManageErrors: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "kafka",
				Name:      "group_manage_errors_total",
				Help:      "Number of Kafka consumer group management errors.",
			},
		),
	}

	collectors := []prometheus.Collector{
		hook.recordsPolled,
		hook.produceRecords,
		hook.brokerRequestDuration,
		hook.groupManageErrors,
	}

	for _, collector := range collectors {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf(
				"register Kafka metric: %w",
				err,
			)
		}
	}

	return hook, nil
}

// OnFetchRecordUnbuffered records only records actually returned to polling.
//
// franz-go can internally discard fetched records during assignment changes;
// those must not be counted as records observed by the application.
func (hook *Hook) OnFetchRecordUnbuffered(
	record *kgo.Record,
	polled bool,
) {
	if hook == nil || record == nil || !polled {
		return
	}

	hook.recordsPolled.WithLabelValues(
		recordTopic(record),
	).Inc()
}

// OnProduceRecordUnbuffered mirrors the completion of a Kafka produce record.
func (hook *Hook) OnProduceRecordUnbuffered(
	record *kgo.Record,
	err error,
) {
	if hook == nil || record == nil {
		return
	}

	hook.produceRecords.WithLabelValues(
		recordTopic(record),
		resultLabel(err),
	).Inc()
}

// OnBrokerE2E records franz-go's broker request/response end-to-end duration.
//
// Kafka API key is a finite protocol dimension and is therefore safe as a
// low-cardinality label. Broker hostname and node ID are deliberately omitted.
func (hook *Hook) OnBrokerE2E(
	_ kgo.BrokerMetadata,
	key int16,
	e2e kgo.BrokerE2E,
) {
	if hook == nil {
		return
	}

	hook.brokerRequestDuration.WithLabelValues(
		strconv.FormatInt(
			int64(key),
			10,
		),
		resultLabel(e2e.Err()),
	).Observe(
		e2e.DurationE2E().Seconds(),
	)
}

// OnGroupManageError records errors that cause franz-go's consumer group
// management loop to back off and retry.
//
// Error text is intentionally not used as a Prometheus label.
func (hook *Hook) OnGroupManageError(
	err error,
) {
	if hook == nil || err == nil {
		return
	}

	hook.groupManageErrors.Inc()
}

func recordTopic(
	record *kgo.Record,
) string {
	if record == nil || record.Topic == "" {
		return unknownTopic
	}

	return record.Topic
}

func resultLabel(
	err error,
) string {
	if err != nil {
		return "error"
	}

	return "success"
}
