package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestClassificationWorkerWritesStructuredSuccessLogs(
	t *testing.T,
) {
	var output bytes.Buffer

	previousLogger := slog.Default()
	slog.SetDefault(
		slog.New(
			slog.NewJSONHandler(&output, nil),
		),
	)
	t.Cleanup(func() {
		slog.SetDefault(previousLogger)
	})

	fixture := newClassificationWorkerFixture(
		t,
		classificationWorkerFixtureOptions{},
	)

	const secret = "SECRET_WORKER_HEADER_DO_NOT_LOG"

	fixture.message.Headers = append(
		fixture.message.Headers,
		struct {
			Key   string
			Value []byte
		}{
			Key:   "authorization",
			Value: []byte(secret),
		},
	)

	if err := fixture.handler.Handle(
		context.Background(),
		fixture.message,
	); err != nil {
		t.Fatalf(
			"Handle() error = %v",
			err,
		)
	}

	records := decodeClassificationWorkerLogRecords(
		t,
		output.Bytes(),
	)

	received := classificationWorkerLogRecordByOperation(
		t,
		records,
		"classification_command_received",
	)

	if received["job_id"] != fixture.command.GetJobId() {
		t.Fatalf(
			"received job_id = %#v, want %q",
			received["job_id"],
			fixture.command.GetJobId(),
		)
	}

	if received["object_id"] !=
		fixture.command.GetObjectId() {
		t.Fatalf(
			"received object_id = %#v, want %q",
			received["object_id"],
			fixture.command.GetObjectId(),
		)
	}

	if received["light_curve_revision"] !=
		float64(fixture.command.GetLightCurveRevision()) {
		t.Fatalf(
			"received light_curve_revision = %#v, want %d",
			received["light_curve_revision"],
			fixture.command.GetLightCurveRevision(),
		)
	}

	if received["kafka_topic"] != fixture.message.Topic {
		t.Fatalf(
			"received kafka_topic = %#v, want %q",
			received["kafka_topic"],
			fixture.message.Topic,
		)
	}

	if received["kafka_partition"] !=
		float64(fixture.message.Partition) {
		t.Fatalf(
			"received kafka_partition = %#v, want %d",
			received["kafka_partition"],
			fixture.message.Partition,
		)
	}

	if received["kafka_offset"] !=
		float64(fixture.message.Offset) {
		t.Fatalf(
			"received kafka_offset = %#v, want %d",
			received["kafka_offset"],
			fixture.message.Offset,
		)
	}

	published := classificationWorkerLogRecordByOperation(
		t,
		records,
		"classification_result_publish",
	)

	expectedRunID, err := domain.GenerateRunID(
		domain.JobID(fixture.command.GetJobId()),
	)
	if err != nil {
		t.Fatalf(
			"GenerateRunID() error = %v",
			err,
		)
	}

	if published["job_id"] != fixture.command.GetJobId() {
		t.Fatalf(
			"published job_id = %#v, want %q",
			published["job_id"],
			fixture.command.GetJobId(),
		)
	}

	if published["run_id"] != string(expectedRunID) {
		t.Fatalf(
			"published run_id = %#v, want %q",
			published["run_id"],
			expectedRunID,
		)
	}

	if published["model_bundle_version"] !=
		fixture.command.GetModelBundleVersion() {
		t.Fatalf(
			"published model_bundle_version = %#v, want %q",
			published["model_bundle_version"],
			fixture.command.GetModelBundleVersion(),
		)
	}

	if published["kafka_topic"] !=
		testWorkerResultTopic {
		t.Fatalf(
			"published kafka_topic = %#v, want %q",
			published["kafka_topic"],
			testWorkerResultTopic,
		)
	}

	if strings.Contains(output.String(), secret) {
		t.Fatalf(
			"structured logs contain Kafka header secret %q",
			secret,
		)
	}
}

func decodeClassificationWorkerLogRecords(
	t *testing.T,
	data []byte,
) []map[string]any {
	t.Helper()

	lines := bytes.Split(
		bytes.TrimSpace(data),
		[]byte("\n"),
	)

	records := make(
		[]map[string]any,
		0,
		len(lines),
	)

	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var record map[string]any
		if err := json.Unmarshal(
			line,
			&record,
		); err != nil {
			t.Fatalf(
				"decode structured log %q: %v",
				line,
				err,
			)
		}

		records = append(records, record)
	}

	return records
}

func classificationWorkerLogRecordByOperation(
	t *testing.T,
	records []map[string]any,
	operation string,
) map[string]any {
	t.Helper()

	for _, record := range records {
		if record["operation"] == operation {
			return record
		}
	}

	t.Fatalf(
		"structured logs do not contain operation %q",
		operation,
	)

	return nil
}
