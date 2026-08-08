package application_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestClassificationResultDLQWritesStructuredPermanentLog(
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

	const (
		rawKeySecret   = "SECRET_RESULT_KEY"
		rawValueSecret = "SECRET_RESULT_VALUE"
		headerSecret   = "SECRET_RESULT_HEADER"
		causeSecret    = "SECRET_RESULT_CAUSE"
	)

	permanentError :=
		&application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeMalformedMessage,
			Field: "value",
			Cause: errors.New(causeSecret),
		}

	next := &classificationResultDLQTestHandler{
		err: permanentError,
	}
	publisher := &classificationResultDLQTestPublisher{}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			next,
			classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	message := application.InboundMessage{
		Topic:     classificationResultDecodeTopic,
		Partition: 3,
		Offset:    42,
		Key:       []byte(rawKeySecret),
		Value:     []byte(rawValueSecret),
		Headers: []application.MessageHeader{
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

	records := decodeClassificationResultDLQLogRecords(
		t,
		output.Bytes(),
	)

	permanent :=
		classificationResultDLQLogRecordByOperation(
			t,
			records,
			"classification_result_permanent_failure",
		)

	if permanent["error_code"] !=
		string(
			application.
				ClassificationResultErrorCodeMalformedMessage,
		) {
		t.Fatalf(
			"error_code = %#v",
			permanent["error_code"],
		)
	}

	if permanent["error_class"] != "PERMANENT" {
		t.Fatalf(
			"error_class = %#v, want PERMANENT",
			permanent["error_class"],
		)
	}

	if permanent["error_field"] != "value" {
		t.Fatalf(
			"error_field = %#v, want value",
			permanent["error_field"],
		)
	}

	if permanent["kafka_topic"] != message.Topic {
		t.Fatalf(
			"kafka_topic = %#v, want %q",
			permanent["kafka_topic"],
			message.Topic,
		)
	}

	if permanent["kafka_partition"] !=
		float64(message.Partition) {
		t.Fatalf(
			"kafka_partition = %#v, want %d",
			permanent["kafka_partition"],
			message.Partition,
		)
	}

	if permanent["kafka_offset"] !=
		float64(message.Offset) {
		t.Fatalf(
			"kafka_offset = %#v, want %d",
			permanent["kafka_offset"],
			message.Offset,
		)
	}

	published :=
		classificationResultDLQLogRecordByOperation(
			t,
			records,
			"classification_result_dlq_publish",
		)

	if published["source_error_code"] !=
		string(
			application.
				ClassificationResultErrorCodeMalformedMessage,
		) {
		t.Fatalf(
			"source_error_code = %#v",
			published["source_error_code"],
		)
	}

	if published["source_error_class"] != "PERMANENT" {
		t.Fatalf(
			"source_error_class = %#v, want PERMANENT",
			published["source_error_class"],
		)
	}

	if published["dlq_topic"] !=
		classificationResultDLQTopic {
		t.Fatalf(
			"dlq_topic = %#v, want %q",
			published["dlq_topic"],
			classificationResultDLQTopic,
		)
	}

	assertClassificationResultDLQLogsDoNotContain(
		t,
		output.String(),
		rawKeySecret,
		rawValueSecret,
		headerSecret,
		causeSecret,
	)
}

func TestClassificationResultDLQLogsPublishFailureWithoutCause(
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

	const publishSecret = "SECRET_RESULT_DLQ_PUBLISH_CAUSE"

	permanentError :=
		&application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeRepositoryConflict,
			Field: "classification_run",
			Cause: application.ErrClassificationRunConflict,
		}

	next := &classificationResultDLQTestHandler{
		err: permanentError,
	}

	publishCause := errors.New(publishSecret)

	publisher := &classificationResultDLQTestPublisher{
		err: publishCause,
	}

	handler, err :=
		application.NewClassificationResultDLQHandler(
			next,
			classificationResultDLQTopic,
			publisher,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultDLQHandler() error = %v",
			err,
		)
	}

	message := application.InboundMessage{
		Topic:     classificationResultDecodeTopic,
		Partition: 7,
		Offset:    99,
		Key:       []byte("OBJ-RESULT-001"),
		Value:     []byte{0xff},
	}

	got := handler.Handle(
		context.Background(),
		message,
	)

	if !errors.Is(got, publishCause) {
		t.Fatalf(
			"errors.Is(error, publishCause) = false; error=%v",
			got,
		)
	}

	records := decodeClassificationResultDLQLogRecords(
		t,
		output.Bytes(),
	)

	published :=
		classificationResultDLQLogRecordByOperation(
			t,
			records,
			"classification_result_dlq_publish",
		)

	if published["source_error_code"] !=
		string(
			application.
				ClassificationResultErrorCodeRepositoryConflict,
		) {
		t.Fatalf(
			"source_error_code = %#v",
			published["source_error_code"],
		)
	}

	if published["source_error_field"] !=
		"classification_run" {
		t.Fatalf(
			"source_error_field = %#v, want classification_run",
			published["source_error_field"],
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

	if strings.Contains(
		output.String(),
		publishSecret,
	) {
		t.Fatalf(
			"structured logs contain publish cause %q",
			publishSecret,
		)
	}
}

func decodeClassificationResultDLQLogRecords(
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

		records = append(
			records,
			record,
		)
	}

	return records
}

func classificationResultDLQLogRecordByOperation(
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

func assertClassificationResultDLQLogsDoNotContain(
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
