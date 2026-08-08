package resultmetrics

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func TestSaverExportsSuccessfulPersistenceMetrics(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	next := &testSaver{
		result: application.SaveRunResult{
			RunInserted:     true,
			CurrentAdvanced: true,
		},
	}

	saver, err := NewSaver(
		registry,
		next,
	)
	if err != nil {
		t.Fatalf(
			"NewSaver() error = %v",
			err,
		)
	}

	got, err := saver.SaveRunAndMaybeAdvanceCurrent(
		context.Background(),
		domain.ClassificationRun{},
	)
	if err != nil {
		t.Fatalf(
			"SaveRunAndMaybeAdvanceCurrent() error = %v",
			err,
		)
	}

	if !got.RunInserted {
		t.Fatal(
			"RunInserted = false, want true",
		)
	}

	if !got.CurrentAdvanced {
		t.Fatal(
			"CurrentAdvanced = false, want true",
		)
	}

	body := scrapeResultMetrics(
		t,
		registry,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_postgres_writes_total{result="success"} 1`,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_postgres_write_duration_seconds_count{result="success"} 1`,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_classification_runs_persisted_total 1`,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_current_classifications_advanced_total 1`,
	)
}

func TestSaverTreatsIdempotentDuplicateAsSuccessfulWrite(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	next := &testSaver{
		result: application.SaveRunResult{
			RunInserted:     false,
			CurrentAdvanced: false,
		},
	}

	saver, err := NewSaver(
		registry,
		next,
	)
	if err != nil {
		t.Fatalf(
			"NewSaver() error = %v",
			err,
		)
	}

	if _, err := saver.SaveRunAndMaybeAdvanceCurrent(
		context.Background(),
		domain.ClassificationRun{},
	); err != nil {
		t.Fatalf(
			"SaveRunAndMaybeAdvanceCurrent() error = %v",
			err,
		)
	}

	body := scrapeResultMetrics(
		t,
		registry,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_postgres_writes_total{result="success"} 1`,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_classification_runs_persisted_total 0`,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_current_classifications_advanced_total 0`,
	)
}

func TestSaverExportsErrorAndPreservesCause(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	expectedErr := errors.New(
		"database unavailable",
	)

	next := &testSaver{
		err: expectedErr,
	}

	saver, err := NewSaver(
		registry,
		next,
	)
	if err != nil {
		t.Fatalf(
			"NewSaver() error = %v",
			err,
		)
	}

	_, gotErr := saver.SaveRunAndMaybeAdvanceCurrent(
		context.Background(),
		domain.ClassificationRun{},
	)

	if !errors.Is(gotErr, expectedErr) {
		t.Fatalf(
			"errors.Is(error, expectedErr) = false; error=%v",
			gotErr,
		)
	}

	body := scrapeResultMetrics(
		t,
		registry,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_postgres_writes_total{result="error"} 1`,
	)

	assertResultMetricContains(
		t,
		body,
		`astro_postgres_write_duration_seconds_count{result="error"} 1`,
	)

	if strings.Contains(
		body,
		expectedErr.Error(),
	) {
		t.Fatalf(
			"metrics contain error text %q",
			expectedErr,
		)
	}
}

func TestNewSaverRejectsMissingDependencies(
	t *testing.T,
) {
	t.Parallel()

	if _, err := NewSaver(
		nil,
		&testSaver{},
	); err == nil {
		t.Fatal(
			"NewSaver(nil registerer) error = nil",
		)
	}

	registry := prometheus.NewRegistry()

	if _, err := NewSaver(
		registry,
		nil,
	); err == nil {
		t.Fatal(
			"NewSaver(nil saver) error = nil",
		)
	}
}

type testSaver struct {
	result application.SaveRunResult
	err    error
}

func (saver *testSaver) SaveRunAndMaybeAdvanceCurrent(
	_ context.Context,
	_ domain.ClassificationRun,
) (application.SaveRunResult, error) {
	return saver.result, saver.err
}

func scrapeResultMetrics(
	t *testing.T,
	registry *prometheus.Registry,
) string {
	t.Helper()

	handler := promhttp.HandlerFor(
		registry,
		promhttp.HandlerOpts{},
	)

	request := httptest.NewRequest(
		http.MethodGet,
		"/metrics",
		nil,
	)

	response := httptest.NewRecorder()

	handler.ServeHTTP(
		response,
		request,
	)

	if response.Code != http.StatusOK {
		t.Fatalf(
			"metrics status = %d, want %d",
			response.Code,
			http.StatusOK,
		)
	}

	return response.Body.String()
}

func assertResultMetricContains(
	t *testing.T,
	body string,
	want string,
) {
	t.Helper()

	if !strings.Contains(
		body,
		want,
	) {
		t.Fatalf(
			"metrics do not contain %q\nmetrics:\n%s",
			want,
			body,
		)
	}
}
