package resultmetrics

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
)

// Saver instruments ClassificationRun persistence without changing
// repository behavior or persistence semantics.
type Saver struct {
	next application.ClassificationRunSaver

	writes          *prometheus.CounterVec
	writeDuration   *prometheus.HistogramVec
	runsPersisted   prometheus.Counter
	currentAdvanced prometheus.Counter
}

var _ application.ClassificationRunSaver = (*Saver)(nil)

func NewSaver(
	registerer prometheus.Registerer,
	next application.ClassificationRunSaver,
) (*Saver, error) {
	if registerer == nil {
		return nil, errors.New(
			"result metrics registerer must not be nil",
		)
	}

	if next == nil {
		return nil, errors.New(
			"result metrics saver must not be nil",
		)
	}

	saver := &Saver{
		next: next,

		writes: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace: "astro",
				Subsystem: "postgres",
				Name:      "writes_total",
				Help:      "Number of ClassificationRun persistence attempts.",
			},
			[]string{"result"},
		),

		writeDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Namespace: "astro",
				Subsystem: "postgres",
				Name:      "write_duration_seconds",
				Help:      "ClassificationRun persistence duration.",
				Buckets:   prometheus.DefBuckets,
			},
			[]string{"result"},
		),

		runsPersisted: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "astro",
				Name:      "classification_runs_persisted_total",
				Help:      "Number of newly inserted ClassificationRuns.",
			},
		),

		currentAdvanced: prometheus.NewCounter(
			prometheus.CounterOpts{
				Namespace: "astro",
				Name:      "current_classifications_advanced_total",
				Help:      "Number of CurrentClassification advances.",
			},
		),
	}

	for _, collector := range []prometheus.Collector{
		saver.writes,
		saver.writeDuration,
		saver.runsPersisted,
		saver.currentAdvanced,
	} {
		if err := registerer.Register(collector); err != nil {
			return nil, fmt.Errorf(
				"register result persistence metric: %w",
				err,
			)
		}
	}

	return saver, nil
}

func (saver *Saver) SaveRunAndMaybeAdvanceCurrent(
	ctx context.Context,
	run domain.ClassificationRun,
) (application.SaveRunResult, error) {
	startedAt := time.Now()

	result, err :=
		saver.next.SaveRunAndMaybeAdvanceCurrent(
			ctx,
			run,
		)

	resultLabel := "success"
	if err != nil {
		resultLabel = "error"
	}

	saver.writes.WithLabelValues(
		resultLabel,
	).Inc()

	saver.writeDuration.WithLabelValues(
		resultLabel,
	).Observe(
		time.Since(startedAt).Seconds(),
	)

	if err == nil {
		if result.RunInserted {
			saver.runsPersisted.Inc()
		}

		if result.CurrentAdvanced {
			saver.currentAdvanced.Inc()
		}
	}

	return result, err
}
