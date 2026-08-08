package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestClassificationResultWriterWritesStructuredSuccessLogs(
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

	run := validClassificationResultDecodeRun(t)

	traceContext := application.TraceContext{
		TraceID:       "trace-result-001",
		CorrelationID: "correlation-result-001",
		CausationID:   "causation-result-001",
	}

	message := buildClassificationResultDecodeMessage(
		t,
		run,
		traceContext,
	)

	const secret = "SECRET_RESULT_HEADER_DO_NOT_LOG"

	message.Headers = append(
		message.Headers,
		application.MessageHeader{
			Key:   "authorization",
			Value: []byte(secret),
		},
	)

	repository := &classificationResultWriterLoggingRepository{
		result: application.SaveRunResult{
			RunInserted:     true,
			CurrentAdvanced: true,
		},
	}

	handler, err :=
		application.NewClassificationResultWriterHandler(
			classificationResultDecodeTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	records := decodeClassificationResultWriterLogRecords(
		t,
		output.Bytes(),
	)

	received := classificationResultWriterLogRecordByOperation(
		t,
		records,
		"classification_result_received",
	)

	if received["job_id"] != string(run.JobID) {
		t.Fatalf(
			"received job_id = %#v, want %q",
			received["job_id"],
			run.JobID,
		)
	}

	if received["run_id"] != string(run.RunID) {
		t.Fatalf(
			"received run_id = %#v, want %q",
			received["run_id"],
			run.RunID,
		)
	}

	if received["object_id"] != run.ObjectID {
		t.Fatalf(
			"received object_id = %#v, want %q",
			received["object_id"],
			run.ObjectID,
		)
	}

	if received["trace_id"] != traceContext.TraceID {
		t.Fatalf(
			"received trace_id = %#v, want %q",
			received["trace_id"],
			traceContext.TraceID,
		)
	}

	if received["correlation_id"] !=
		traceContext.CorrelationID {
		t.Fatalf(
			"received correlation_id = %#v, want %q",
			received["correlation_id"],
			traceContext.CorrelationID,
		)
	}

	if received["causation_id"] !=
		traceContext.CausationID {
		t.Fatalf(
			"received causation_id = %#v, want %q",
			received["causation_id"],
			traceContext.CausationID,
		)
	}

	if received["kafka_topic"] != message.Topic {
		t.Fatalf(
			"received kafka_topic = %#v, want %q",
			received["kafka_topic"],
			message.Topic,
		)
	}

	if received["kafka_partition"] !=
		float64(message.Partition) {
		t.Fatalf(
			"received kafka_partition = %#v, want %d",
			received["kafka_partition"],
			message.Partition,
		)
	}

	if received["kafka_offset"] !=
		float64(message.Offset) {
		t.Fatalf(
			"received kafka_offset = %#v, want %d",
			received["kafka_offset"],
			message.Offset,
		)
	}

	persisted := classificationResultWriterLogRecordByOperation(
		t,
		records,
		"classification_run_persist",
	)

	if persisted["job_id"] != string(run.JobID) {
		t.Fatalf(
			"persisted job_id = %#v, want %q",
			persisted["job_id"],
			run.JobID,
		)
	}

	if persisted["run_id"] != string(run.RunID) {
		t.Fatalf(
			"persisted run_id = %#v, want %q",
			persisted["run_id"],
			run.RunID,
		)
	}

	if persisted["run_inserted"] != true {
		t.Fatalf(
			"run_inserted = %#v, want true",
			persisted["run_inserted"],
		)
	}

	if persisted["current_advanced"] != true {
		t.Fatalf(
			"current_advanced = %#v, want true",
			persisted["current_advanced"],
		)
	}

	if strings.Contains(output.String(), secret) {
		t.Fatalf(
			"structured logs contain Kafka header secret %q",
			secret,
		)
	}
}

func TestClassificationResultWriterLogsIdempotentDuplicate(
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

	run := validClassificationResultDecodeRun(t)

	message := buildClassificationResultDecodeMessage(
		t,
		run,
		application.TraceContext{},
	)

	repository := &classificationResultWriterLoggingRepository{
		result: application.SaveRunResult{
			RunInserted:     false,
			CurrentAdvanced: false,
		},
	}

	handler, err :=
		application.NewClassificationResultWriterHandler(
			classificationResultDecodeTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	records := decodeClassificationResultWriterLogRecords(
		t,
		output.Bytes(),
	)

	persisted := classificationResultWriterLogRecordByOperation(
		t,
		records,
		"classification_run_persist",
	)

	if persisted["run_inserted"] != false {
		t.Fatalf(
			"run_inserted = %#v, want false",
			persisted["run_inserted"],
		)
	}

	if persisted["current_advanced"] != false {
		t.Fatalf(
			"current_advanced = %#v, want false",
			persisted["current_advanced"],
		)
	}
}

type classificationResultWriterLoggingRepository struct {
	result application.SaveRunResult
	err    error
}

func (
	repository *classificationResultWriterLoggingRepository,
) SaveRunAndMaybeAdvanceCurrent(
	_ context.Context,
	_ domain.ClassificationRun,
) (application.SaveRunResult, error) {
	return repository.result, repository.err
}

func decodeClassificationResultWriterLogRecords(
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

func classificationResultWriterLogRecordByOperation(
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
