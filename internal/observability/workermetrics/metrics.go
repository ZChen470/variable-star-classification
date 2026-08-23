package workermetrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

type MetricsObserver struct {
	concurrency prometheus.Gauge
}

func NewObserver(
	registry prometheus.Registerer,
	workerCount int,
) (*MetricsObserver, error) {
	concurrency := prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "astro_classification_worker_concurrency",
		Help: "Configured maximum number of records polled and processed concurrently by the classifier worker.",
	})

	if err := registry.Register(concurrency); err != nil {
		return nil, err
	}

	concurrency.Set(float64(workerCount))

	return &MetricsObserver{
		concurrency: concurrency,
	}, nil
}
