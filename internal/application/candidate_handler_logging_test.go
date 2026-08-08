package application

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestCandidateHandlerWritesStructuredBusinessLogs(t *testing.T) {
	var output bytes.Buffer

	publisher := &recordingMessagePublisher{}
	handler := newValidCandidateHandler(t, publisher)
	handler.logger = slog.New(
		slog.NewJSONHandler(&output, nil),
	)

	event := validCandidateEvent()

	const secret = "SECRET_DO_NOT_LOG"
	event.Producer = secret

	message := candidateInboundMessage(t, event)

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	records := decodeCandidateLogRecords(t, output.Bytes())

	received := candidateLogRecordByOperation(
		t,
		records,
		"candidate_received",
	)

	if received["event_id"] != event.GetEventId() {
		t.Fatalf(
			"event_id = %#v, want %q",
			received["event_id"],
			event.GetEventId(),
		)
	}
	if received["object_id"] != event.GetObjectId() {
		t.Fatalf(
			"object_id = %#v, want %q",
			received["object_id"],
			event.GetObjectId(),
		)
	}
	if received["trace_id"] !=
		event.GetTraceContext().GetTraceId() {
		t.Fatalf(
			"trace_id = %#v, want %q",
			received["trace_id"],
			event.GetTraceContext().GetTraceId(),
		)
	}
	if received["correlation_id"] !=
		event.GetTraceContext().GetCorrelationId() {
		t.Fatalf(
			"correlation_id = %#v, want %q",
			received["correlation_id"],
			event.GetTraceContext().GetCorrelationId(),
		)
	}
	if received["causation_id"] !=
		event.GetTraceContext().GetCausationId() {
		t.Fatalf(
			"causation_id = %#v, want %q",
			received["causation_id"],
			event.GetTraceContext().GetCausationId(),
		)
	}
	if received["kafka_topic"] != message.Topic {
		t.Fatalf(
			"kafka_topic = %#v, want %q",
			received["kafka_topic"],
			message.Topic,
		)
	}
	if received["kafka_partition"] !=
		float64(message.Partition) {
		t.Fatalf(
			"kafka_partition = %#v, want %d",
			received["kafka_partition"],
			message.Partition,
		)
	}
	if received["kafka_offset"] !=
		float64(message.Offset) {
		t.Fatalf(
			"kafka_offset = %#v, want %d",
			received["kafka_offset"],
			message.Offset,
		)
	}

	published := candidateLogRecordByOperation(
		t,
		records,
		"classification_command_publish",
	)

	if published["object_id"] != event.GetObjectId() {
		t.Fatalf(
			"published object_id = %#v, want %q",
			published["object_id"],
			event.GetObjectId(),
		)
	}
	if published["model_bundle_version"] != "bundle-v1" {
		t.Fatalf(
			"model_bundle_version = %#v, want bundle-v1",
			published["model_bundle_version"],
		)
	}
	if published["kafka_topic"] !=
		testClassificationCommandTopic {
		t.Fatalf(
			"published kafka_topic = %#v, want %q",
			published["kafka_topic"],
			testClassificationCommandTopic,
		)
	}

	if strings.Contains(output.String(), secret) {
		t.Fatalf(
			"structured logs contain secret value %q",
			secret,
		)
	}
}

func TestCandidateHandlerLogsPermanentDLQWithoutRawValue(
	t *testing.T,
) {
	var output bytes.Buffer

	publisher := &recordingMessagePublisher{}
	handler := newValidCandidateHandler(t, publisher)
	handler.logger = slog.New(
		slog.NewJSONHandler(&output, nil),
	)

	const secret = "SECRET_RAW_KAFKA_VALUE"

	message := candidateInboundMessage(
		t,
		validCandidateEvent(),
	)
	message.Value = []byte(secret)

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	records := decodeCandidateLogRecords(t, output.Bytes())

	decodeFailed := candidateLogRecordByOperation(
		t,
		records,
		"candidate_decode",
	)

	if decodeFailed["error_code"] !=
		string(CandidateMessageErrorCodeMalformedProto) {
		t.Fatalf(
			"error_code = %#v, want %q",
			decodeFailed["error_code"],
			CandidateMessageErrorCodeMalformedProto,
		)
	}
	if decodeFailed["error_class"] != "PERMANENT" {
		t.Fatalf(
			"error_class = %#v, want PERMANENT",
			decodeFailed["error_class"],
		)
	}

	dlqPublished := candidateLogRecordByOperation(
		t,
		records,
		"candidate_dlq_publish",
	)

	if dlqPublished["dlq_topic"] != testCandidateDLQTopic {
		t.Fatalf(
			"dlq_topic = %#v, want %q",
			dlqPublished["dlq_topic"],
			testCandidateDLQTopic,
		)
	}

	if strings.Contains(output.String(), secret) {
		t.Fatalf(
			"structured logs contain raw Kafka value %q",
			secret,
		)
	}
}

func decodeCandidateLogRecords(
	t *testing.T,
	data []byte,
) []map[string]any {
	t.Helper()

	lines := bytes.Split(
		bytes.TrimSpace(data),
		[]byte("\n"),
	)

	records := make([]map[string]any, 0, len(lines))

	for _, line := range lines {
		if len(bytes.TrimSpace(line)) == 0 {
			continue
		}

		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
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

func candidateLogRecordByOperation(
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
