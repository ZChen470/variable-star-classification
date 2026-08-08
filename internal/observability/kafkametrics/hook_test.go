package kafkametrics

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestHookExportsKafkaMetrics(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	hook, err := New(registry)
	if err != nil {
		t.Fatalf(
			"New() error = %v",
			err,
		)
	}

	fetchedRecord := &kgo.Record{
		Topic: "candidate-topic",
	}

	// Internally discarded record: must not count.
	hook.OnFetchRecordUnbuffered(
		fetchedRecord,
		false,
	)

	// Record actually returned by polling.
	hook.OnFetchRecordUnbuffered(
		fetchedRecord,
		true,
	)

	producedRecord := &kgo.Record{
		Topic: "command-topic",
	}

	hook.OnProduceRecordUnbuffered(
		producedRecord,
		nil,
	)

	hook.OnProduceRecordUnbuffered(
		producedRecord,
		errors.New("produce failed"),
	)

	hook.OnGroupManageError(
		errors.New("group management failed"),
	)

	// Zero-value BrokerE2E is sufficient to verify that the hook exports the
	// histogram contract. Live duration/error values come from franz-go.
	hook.OnBrokerE2E(
		kgo.BrokerMetadata{},
		1,
		kgo.BrokerE2E{},
	)

	body := scrapeKafkaMetrics(
		t,
		registry,
	)

	assertKafkaMetricContains(
		t,
		body,
		`astro_kafka_records_polled_total{topic="candidate-topic"} 1`,
	)

	assertKafkaMetricContains(
		t,
		body,
		`astro_kafka_produce_records_total{result="success",topic="command-topic"} 1`,
	)

	assertKafkaMetricContains(
		t,
		body,
		`astro_kafka_produce_records_total{result="error",topic="command-topic"} 1`,
	)

	assertKafkaMetricContains(
		t,
		body,
		`astro_kafka_group_manage_errors_total 1`,
	)

	assertKafkaMetricContains(
		t,
		body,
		`astro_kafka_broker_request_duration_seconds_count{api_key="1",result="success"} 1`,
	)
}

func TestNewRejectsNilRegisterer(
	t *testing.T,
) {
	t.Parallel()

	if _, err := New(nil); err == nil {
		t.Fatal(
			"New(nil) error = nil",
		)
	}
}

func TestHookIgnoresNilRecords(
	t *testing.T,
) {
	t.Parallel()

	registry := prometheus.NewRegistry()

	hook, err := New(registry)
	if err != nil {
		t.Fatalf(
			"New() error = %v",
			err,
		)
	}

	hook.OnFetchRecordUnbuffered(
		nil,
		true,
	)

	hook.OnProduceRecordUnbuffered(
		nil,
		errors.New("ignored"),
	)

	body := scrapeKafkaMetrics(
		t,
		registry,
	)

	if strings.Contains(
		body,
		`topic="unknown"`,
	) {
		t.Fatal(
			"nil records unexpectedly emitted topic metric",
		)
	}
}

func scrapeKafkaMetrics(
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

func assertKafkaMetricContains(
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
