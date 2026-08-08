package postgresmetrics

import (
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type poolSnapshot struct {
	acquiredConns     int32
	idleConns         int32
	constructingConns int32
	totalConns        int32
	maxConns          int32

	acquireCount         int64
	canceledAcquireCount int64
	emptyAcquireCount    int64

	acquireDuration      time.Duration
	emptyAcquireWaitTime time.Duration
}

// Collector exposes pgxpool statistics without attaching database,
// host, user, query, or application identity labels.
type Collector struct {
	snapshot func() poolSnapshot

	connections      *prometheus.Desc
	maxConnections   *prometheus.Desc
	acquires         *prometheus.Desc
	acquireDuration  *prometheus.Desc
	canceledAcquires *prometheus.Desc
	emptyAcquires    *prometheus.Desc
	emptyAcquireWait *prometheus.Desc
}

// New creates and registers a PostgreSQL pool collector.
func New(
	registerer prometheus.Registerer,
	pool *pgxpool.Pool,
) (*Collector, error) {
	if registerer == nil {
		return nil, errors.New(
			"PostgreSQL metrics registerer must not be nil",
		)
	}

	if pool == nil {
		return nil, errors.New(
			"PostgreSQL metrics pool must not be nil",
		)
	}

	collector := newCollector(
		func() poolSnapshot {
			stat := pool.Stat()

			return poolSnapshot{
				acquiredConns:     stat.AcquiredConns(),
				idleConns:         stat.IdleConns(),
				constructingConns: stat.ConstructingConns(),
				totalConns:        stat.TotalConns(),
				maxConns:          stat.MaxConns(),

				acquireCount:         stat.AcquireCount(),
				canceledAcquireCount: stat.CanceledAcquireCount(),
				emptyAcquireCount:    stat.EmptyAcquireCount(),

				acquireDuration:      stat.AcquireDuration(),
				emptyAcquireWaitTime: stat.EmptyAcquireWaitTime(),
			}
		},
	)

	if err := registerer.Register(collector); err != nil {
		return nil, fmt.Errorf(
			"register PostgreSQL pool metrics: %w",
			err,
		)
	}

	return collector, nil
}

func newCollector(
	snapshot func() poolSnapshot,
) *Collector {
	return &Collector{
		snapshot: snapshot,

		connections: prometheus.NewDesc(
			"astro_postgres_pool_connections",
			"Current PostgreSQL pool connections by state.",
			[]string{"state"},
			nil,
		),

		maxConnections: prometheus.NewDesc(
			"astro_postgres_pool_max_connections",
			"Configured maximum PostgreSQL pool connections.",
			nil,
			nil,
		),

		acquires: prometheus.NewDesc(
			"astro_postgres_pool_acquires_total",
			"Cumulative successful PostgreSQL pool acquires.",
			nil,
			nil,
		),

		acquireDuration: prometheus.NewDesc(
			"astro_postgres_pool_acquire_duration_seconds_total",
			"Cumulative duration of successful PostgreSQL pool acquires.",
			nil,
			nil,
		),

		canceledAcquires: prometheus.NewDesc(
			"astro_postgres_pool_canceled_acquires_total",
			"Cumulative PostgreSQL pool acquires canceled by context.",
			nil,
			nil,
		),

		emptyAcquires: prometheus.NewDesc(
			"astro_postgres_pool_empty_acquires_total",
			"Cumulative successful acquires that waited because the pool was empty.",
			nil,
			nil,
		),

		emptyAcquireWait: prometheus.NewDesc(
			"astro_postgres_pool_empty_acquire_wait_seconds_total",
			"Cumulative time spent waiting for an empty PostgreSQL pool.",
			nil,
			nil,
		),
	}
}

func (collector *Collector) Describe(
	descriptions chan<- *prometheus.Desc,
) {
	descriptions <- collector.connections
	descriptions <- collector.maxConnections
	descriptions <- collector.acquires
	descriptions <- collector.acquireDuration
	descriptions <- collector.canceledAcquires
	descriptions <- collector.emptyAcquires
	descriptions <- collector.emptyAcquireWait
}

func (collector *Collector) Collect(
	metrics chan<- prometheus.Metric,
) {
	if collector == nil || collector.snapshot == nil {
		return
	}

	snapshot := collector.snapshot()

	metrics <- prometheus.MustNewConstMetric(
		collector.connections,
		prometheus.GaugeValue,
		float64(snapshot.acquiredConns),
		"acquired",
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.connections,
		prometheus.GaugeValue,
		float64(snapshot.idleConns),
		"idle",
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.connections,
		prometheus.GaugeValue,
		float64(snapshot.constructingConns),
		"constructing",
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.connections,
		prometheus.GaugeValue,
		float64(snapshot.totalConns),
		"total",
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.maxConnections,
		prometheus.GaugeValue,
		float64(snapshot.maxConns),
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.acquires,
		prometheus.CounterValue,
		float64(snapshot.acquireCount),
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.acquireDuration,
		prometheus.CounterValue,
		snapshot.acquireDuration.Seconds(),
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.canceledAcquires,
		prometheus.CounterValue,
		float64(snapshot.canceledAcquireCount),
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.emptyAcquires,
		prometheus.CounterValue,
		float64(snapshot.emptyAcquireCount),
	)

	metrics <- prometheus.MustNewConstMetric(
		collector.emptyAcquireWait,
		prometheus.CounterValue,
		snapshot.emptyAcquireWaitTime.Seconds(),
	)
}
