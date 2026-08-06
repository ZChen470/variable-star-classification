package application_test

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

const classificationResultDLQTopic = "astro.classification.results.dlq.v1"

func TestBuildClassificationResultDLQMessage(
	t *testing.T,
) {
	timestamp := time.Date(
		2026,
		time.August,
		6,
		20,
		30,
		0,
		0,
		time.UTC,
	)

	original := application.InboundMessage{
		Topic:     classificationResultDecodeTopic,
		Partition: 4,
		Offset:    52,
		Key:       []byte("OBJ-RESULT-001"),
		Value:     []byte{0xff, 0x01},

		Headers: []application.MessageHeader{
			{
				Key:   "traceparent",
				Value: []byte("original-trace"),
			},
			{
				Key:   "",
				Value: []byte("empty-key-value"),
			},
		},

		Timestamp: timestamp,
	}

	permanentError :=
		&application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeMalformedMessage,

			Field: "value",

			Cause: errors.New(
				"unstable decoder error text",
			),
		}

	message, err :=
		application.BuildClassificationResultDLQMessage(
			classificationResultDLQTopic,
			original,
			permanentError,
		)
	if err != nil {
		t.Fatalf(
			"BuildClassificationResultDLQMessage() error = %v",
			err,
		)
	}

	if message.Topic != classificationResultDLQTopic {
		t.Fatalf(
			"Topic = %q, want %q",
			message.Topic,
			classificationResultDLQTopic,
		)
	}

	if !reflect.DeepEqual(message.Key, original.Key) {
		t.Fatalf(
			"Key = %#v, want %#v",
			message.Key,
			original.Key,
		)
	}

	if !reflect.DeepEqual(message.Value, original.Value) {
		t.Fatalf(
			"Value = %#v, want %#v",
			message.Value,
			original.Value,
		)
	}

	if !message.Timestamp.Equal(timestamp) {
		t.Fatalf(
			"Timestamp = %v, want %v",
			message.Timestamp,
			timestamp,
		)
	}

	assertClassificationResultDLQHeader(
		t,
		message.Headers,
		application.ClassificationResultDLQHeaderErrorCode,
		string(
			application.
				ClassificationResultErrorCodeMalformedMessage,
		),
	)

	assertClassificationResultDLQHeader(
		t,
		message.Headers,
		application.ClassificationResultDLQHeaderErrorClass,
		"PERMANENT",
	)

	assertClassificationResultDLQHeader(
		t,
		message.Headers,
		application.ClassificationResultDLQHeaderErrorField,
		"value",
	)

	assertClassificationResultDLQHeader(
		t,
		message.Headers,
		application.ClassificationResultDLQHeaderOriginalTopic,
		classificationResultDecodeTopic,
	)

	assertClassificationResultDLQHeader(
		t,
		message.Headers,
		application.
			ClassificationResultDLQHeaderOriginalPartition,
		"4",
	)

	assertClassificationResultDLQHeader(
		t,
		message.Headers,
		application.ClassificationResultDLQHeaderOriginalOffset,
		"52",
	)

	for _, header := range message.Headers {
		if strings.Contains(
			string(header.Value),
			"unstable decoder error text",
		) {
			t.Fatal(
				"DLQ headers must not contain Cause.Error() text",
			)
		}
	}

	original.Key[0] = 'X'
	original.Value[0] = 0x00
	original.Headers[0].Value[0] = 'X'

	if string(message.Key) != "OBJ-RESULT-001" {
		t.Fatal("DLQ Key was not deep copied")
	}

	if !bytes.Equal(
		message.Value,
		[]byte{0xff, 0x01},
	) {
		t.Fatal("DLQ Value was not deep copied")
	}

	if string(message.Headers[0].Value) !=
		"original-trace" {
		t.Fatal(
			"DLQ Header Value was not deep copied",
		)
	}
}

func TestBuildClassificationResultDLQMessagePreservesByteShapes(
	t *testing.T,
) {
	tests := []struct {
		name string

		key   []byte
		value []byte

		wantKeyNil   bool
		wantValueNil bool
	}{
		{
			name: "nil values",

			key:   nil,
			value: nil,

			wantKeyNil:   true,
			wantValueNil: true,
		},
		{
			name: "non-nil empty values",

			key:   make([]byte, 0),
			value: make([]byte, 0),

			wantKeyNil:   false,
			wantValueNil: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err :=
				application.
					BuildClassificationResultDLQMessage(
						classificationResultDLQTopic,
						application.InboundMessage{
							Topic: classificationResultDecodeTopic,

							Key:   test.key,
							Value: test.value,
						},
						&application.
							PermanentClassificationResultError{
							Code: application.
								ClassificationResultErrorCodeInvalidField,

							Field: "object_id",
						},
					)
			if err != nil {
				t.Fatalf(
					"BuildClassificationResultDLQMessage() error = %v",
					err,
				)
			}

			if (message.Key == nil) !=
				test.wantKeyNil {
				t.Fatalf(
					"Key nil = %v, want %v",
					message.Key == nil,
					test.wantKeyNil,
				)
			}

			if (message.Value == nil) !=
				test.wantValueNil {
				t.Fatalf(
					"Value nil = %v, want %v",
					message.Value == nil,
					test.wantValueNil,
				)
			}

			if message.Headers == nil {
				t.Fatal(
					"Headers must contain DLQ metadata",
				)
			}
		})
	}
}

func TestBuildClassificationResultDLQMessageRejectsInvalidArguments(
	t *testing.T,
) {
	validError :=
		&application.PermanentClassificationResultError{
			Code: application.
				ClassificationResultErrorCodeInvalidField,

			Field: "object_id",
		}

	tests := []struct {
		name string

		topic       string
		resultError *application.
				PermanentClassificationResultError
	}{
		{
			name:        "empty topic",
			resultError: validError,
		},
		{
			name:  "nil result error",
			topic: classificationResultDLQTopic,
		},
		{
			name:  "empty error code",
			topic: classificationResultDLQTopic,

			resultError: &application.
				PermanentClassificationResultError{
				Field: "object_id",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err :=
				application.
					BuildClassificationResultDLQMessage(
						test.topic,
						application.InboundMessage{},
						test.resultError,
					)

			if err == nil {
				t.Fatalf(
					"error = nil, message = %#v",
					message,
				)
			}
		})
	}
}

func assertClassificationResultDLQHeader(
	t *testing.T,
	headers []application.MessageHeader,
	key string,
	wantValue string,
) {
	t.Helper()

	for _, header := range headers {
		if header.Key != key {
			continue
		}

		if string(header.Value) != wantValue {
			t.Fatalf(
				"Header %q = %q, want %q",
				key,
				header.Value,
				wantValue,
			)
		}

		return
	}

	t.Fatalf("Header %q not found", key)
}
