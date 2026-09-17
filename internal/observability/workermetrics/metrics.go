package workermetrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsObserver struct {
	concurrency      prometheus.Gauge
	committedRecords prometheus.Counter
}

func NewObserver(
	registry prometheus.Registerer,
	workerCount int,
) (*MetricsObserver, error) {
	concurrency := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "astro_classification_worker_concurrency",
		Help: "Configured maximum number of records polled and processed concurrently by the classifier worker.",
	})

	committedRecords := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "astro_classification_command_committed_records_total",
		Help: "Number of actual ClassificationCommand Kafka records represented by successful manual commit calls.",
	})

	if err := registry.Register(concurrency); err != nil {
		return nil, err
	}
	if err := registry.Register(committedRecords); err != nil {
		registry.Unregister(concurrency)
		return nil, err
	}

	concurrency.Set(float64(workerCount))

	return &MetricsObserver{
		concurrency:      concurrency,
		committedRecords: committedRecords,
	}, nil
}

func (observer *MetricsObserver) ObserveCommittedRecords(count int) {
	if observer == nil || count <= 0 {
		return
	}

	observer.committedRecords.Add(float64(count))
}
