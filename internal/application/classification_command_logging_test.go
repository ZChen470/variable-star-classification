package application

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestClassificationCommandRetryHandlerWritesStructuredLogs(t *testing.T) {
	var output bytes.Buffer

	const (
		rawValueSecret = "SECRET_RAW_COMMAND_VALUE"
		headerSecret   = "SECRET_COMMAND_HEADER"
		causeSecret    = "SECRET_RETRY_CAUSE"
	)

	retryError := &ClassificationWorkerError{
		Code:      ClassificationWorkerErrorCodeDependencyUnavailable,
		Class:     ClassificationWorkerErrorClassRetryable,
		Operation: ClassificationWorkerOperationClassify,
		Cause:     errors.New(causeSecret),
	}

	next := &classificationCommandLoggingHandler{
		results: []error{
			retryError,
			nil,
		},
	}

	handler, err := NewClassificationCommandRetryHandler(
		next,
		[]time.Duration{0},
	)
	if err != nil {
		t.Fatalf("NewClassificationCommandRetryHandler() error = %v", err)
	}

	handler.logger = slog.New(slog.NewJSONHandler(&output, nil))

	message := InboundMessage{
		Topic:     "astro.classification.commands.v1",
		Partition: 2,
		Offset:    17,
		Key:       []byte("OBJ-0001"),
		Value:     []byte(rawValueSecret),
		Headers: []MessageHeader{
			{
				Key:   "authorization",
				Value: []byte(headerSecret),
			},
		},
	}

	if err := handler.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	records := decodeClassificationCommandLogRecords(t, output.Bytes())

	scheduled := classificationCommandLogRecordByOperation(
		t,
		records,
		"classification_command_retry",
	)

	if scheduled["attempt"] != float64(1) {
		t.Fatalf("scheduled attempt = %#v, want 1", scheduled["attempt"])
	}
	if scheduled["next_attempt"] != float64(2) {
		t.Fatalf(
			"scheduled next_attempt = %#v, want 2",
			scheduled["next_attempt"],
		)
	}
	if scheduled["retry_delay_ms"] != float64(0) {
		t.Fatalf(
			"scheduled retry_delay_ms = %#v, want 0",
			scheduled["retry_delay_ms"],
		)
	}
	if scheduled["error_code"] != string(ClassificationWorkerErrorCodeDependencyUnavailable) {
		t.Fatalf("scheduled error_code = %#v", scheduled["error_code"])
	}
	if scheduled["error_class"] != "RETRYABLE" {
		t.Fatalf(
			"scheduled error_class = %#v, want RETRYABLE",
			scheduled["error_class"],
		)
	}
	if scheduled["worker_operation"] != string(ClassificationWorkerOperationClassify) {
		t.Fatalf(
			"scheduled worker_operation = %#v",
			scheduled["worker_operation"],
		)
	}
	if scheduled["kafka_topic"] != message.Topic {
		t.Fatalf(
			"scheduled kafka_topic = %#v, want %q",
			scheduled["kafka_topic"],
			message.Topic,
		)
	}
	if scheduled["kafka_partition"] != float64(message.Partition) {
		t.Fatalf(
			"scheduled kafka_partition = %#v, want %d",
			scheduled["kafka_partition"],
			message.Partition,
		)
	}
	if scheduled["kafka_offset"] != float64(message.Offset) {
		t.Fatalf(
			"scheduled kafka_offset = %#v, want %d",
			scheduled["kafka_offset"],
			message.Offset,
		)
	}

	if _, exists := scheduled["max_attempts"]; exists {
		t.Fatalf(
			"scheduled log unexpectedly contains max_attempts = %#v",
			scheduled["max_attempts"],
		)
	}

	for _, record := range records {
		if record["operation"] == "classification_command_retry_exhausted" {
			t.Fatal("structured logs unexpectedly contain retry exhaustion")
		}
	}

	assertClassificationCommandLogsDoNotContain(
		t,
		output.String(),
		rawValueSecret,
		headerSecret,
		causeSecret,
	)
}

func TestClassificationCommandDLQHandlerWritesStructuredLog(
	t *testing.T,
) {
	var output bytes.Buffer

	const (
		rawValueSecret = "SECRET_DLQ_RAW_VALUE"
		headerSecret   = "SECRET_DLQ_HEADER"
		causeSecret    = "SECRET_PERMANENT_CAUSE"
	)

	workerError := &ClassificationWorkerError{
		Code:      ClassificationWorkerErrorCodeLightCurveInvalid,
		Class:     ClassificationWorkerErrorClassPermanent,
		Operation: ClassificationWorkerOperationPrepareInput,
		Cause:     errors.New(causeSecret),
	}

	next := &classificationCommandLoggingHandler{
		results: []error{workerError},
	}
	publisher := &classificationCommandLoggingPublisher{}

	handler, err := NewClassificationCommandDLQHandler(
		next,
		"astro.classification.commands.dlq.v1",
		publisher,
	)
	if err != nil {
		t.Fatalf(
			"NewClassificationCommandDLQHandler() error = %v",
			err,
		)
	}

	handler.logger = slog.New(
		slog.NewJSONHandler(&output, nil),
	)

	message := InboundMessage{
		Topic:     "astro.classification.commands.v1",
		Partition: 3,
		Offset:    42,
		Key:       []byte("OBJ-0001"),
		Value:     []byte(rawValueSecret),
		Headers: []MessageHeader{
			{
				Key:   "authorization",
				Value: []byte(headerSecret),
			},
		},
	}

	if err := handler.Handle(
		context.Background(),
		message,
	); err != nil {
		t.Fatalf("Handle() error = %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf(
			"publisher calls = %d, want 1",
			len(publisher.messages),
		)
	}

	records := decodeClassificationCommandLogRecords(
		t,
		output.Bytes(),
	)

	published := classificationCommandLogRecordByOperation(
		t,
		records,
		"classification_command_dlq_publish",
	)

	if published["error_code"] !=
		string(ClassificationWorkerErrorCodeLightCurveInvalid) {
		t.Fatalf(
			"error_code = %#v",
			published["error_code"],
		)
	}
	if published["error_class"] != "PERMANENT" {
		t.Fatalf(
			"error_class = %#v, want PERMANENT",
			published["error_class"],
		)
	}
	if published["worker_operation"] !=
		string(ClassificationWorkerOperationPrepareInput) {
		t.Fatalf(
			"worker_operation = %#v",
			published["worker_operation"],
		)
	}
	if published["kafka_topic"] != message.Topic {
		t.Fatalf(
			"kafka_topic = %#v, want %q",
			published["kafka_topic"],
			message.Topic,
		)
	}
	if published["kafka_partition"] !=
		float64(message.Partition) {
		t.Fatalf(
			"kafka_partition = %#v, want %d",
			published["kafka_partition"],
			message.Partition,
		)
	}
	if published["kafka_offset"] !=
		float64(message.Offset) {
		t.Fatalf(
			"kafka_offset = %#v, want %d",
			published["kafka_offset"],
			message.Offset,
		)
	}
	if published["dlq_topic"] !=
		"astro.classification.commands.dlq.v1" {
		t.Fatalf(
			"dlq_topic = %#v",
			published["dlq_topic"],
		)
	}

	assertClassificationCommandLogsDoNotContain(
		t,
		output.String(),
		rawValueSecret,
		headerSecret,
		causeSecret,
	)
}

type classificationCommandLoggingHandler struct {
	results []error
	calls   int
}

func (handler *classificationCommandLoggingHandler) Handle(
	_ context.Context,
	_ InboundMessage,
) error {
	handler.calls++

	index := handler.calls - 1
	if index >= len(handler.results) {
		return nil
	}

	return handler.results[index]
}

type classificationCommandLoggingPublisher struct {
	messages []OutboundMessage
	err      error
}

func (publisher *classificationCommandLoggingPublisher) Publish(
	_ context.Context,
	message OutboundMessage,
) error {
	publisher.messages = append(
		publisher.messages,
		message,
	)

	return publisher.err
}

func decodeClassificationCommandLogRecords(
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

func classificationCommandLogRecordByOperation(
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

func assertClassificationCommandLogsDoNotContain(
	t *testing.T,
	output string,
	secrets ...string,
) {
	t.Helper()

	for _, secret := range secrets {
		if strings.Contains(output, secret) {
			t.Fatalf(
				"structured logs contain secret %q",
				secret,
			)
		}
	}
}
