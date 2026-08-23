package workermetrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

var _ application.ClassificationWorkerPoolObserver = (*MetricsObserver)(nil)

type MetricsObserver struct {
	workers prometheus.Gauge
	active  prometheus.Gauge
}

func NewObserver(
	registry prometheus.Registerer,
	workerCount int,
) (*MetricsObserver, error) {
	workers := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "astro_classification_worker_pool_workers",
		Help: "Configured classifier worker pool concurrency.",
	})

	active := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "astro_classification_worker_pool_active",
		Help: "Currently active classifier worker pool workers.",
	})

	if err := registry.Register(workers); err != nil {
		return nil, err
	}

	if err := registry.Register(active); err != nil {
		return nil, err
	}

	workers.Set(float64(workerCount))

	return &MetricsObserver{
		workers: workers,
		active:  active,
	}, nil
}

func (observer *MetricsObserver) WorkerStarted() {
	observer.active.Inc()
}

func (observer *MetricsObserver) WorkerFinished() {
	observer.active.Dec()
}
