package application

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testClassificationCommandDLQTopic = "astro.classification.commands.dlq.v1"

func TestBuildClassificationCommandDLQMessage(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(
		2026,
		time.August,
		6,
		7,
		0,
		0,
		0,
		time.UTC,
	)

	original := InboundMessage{
		Topic:     "astro.classification.commands.v1",
		Partition: 3,
		Offset:    42,
		Key:       []byte("OBJ-0001"),
		Value:     []byte{0xff, 0x00},

		Headers: []MessageHeader{
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

	workerError := &ClassificationWorkerError{
		Code: ClassificationWorkerErrorCodeLightCurveInvalid,

		Class: ClassificationWorkerErrorClassPermanent,

		Operation: ClassificationWorkerOperationPrepareInput,

		Cause: errors.New(
			"invalid light curve",
		),
	}

	message, err :=
		BuildClassificationCommandDLQMessage(
			testClassificationCommandDLQTopic,
			original,
			workerError,
		)
	if err != nil {
		t.Fatalf(
			"BuildClassificationCommandDLQMessage() error = %v",
			err,
		)
	}

	if message.Topic !=
		testClassificationCommandDLQTopic {
		t.Fatalf(
			"Topic = %q, want %q",
			message.Topic,
			testClassificationCommandDLQTopic,
		)
	}

	if !reflect.DeepEqual(
		message.Key,
		original.Key,
	) {
		t.Fatalf(
			"Key = %#v, want %#v",
			message.Key,
			original.Key,
		)
	}

	if !reflect.DeepEqual(
		message.Value,
		original.Value,
	) {
		t.Fatalf(
			"Value = %#v, want %#v",
			message.Value,
			original.Value,
		)
	}

	if !message.Timestamp.Equal(
		original.Timestamp,
	) {
		t.Fatalf(
			"Timestamp = %v, want %v",
			message.Timestamp,
			original.Timestamp,
		)
	}

	wantHeaders := []MessageHeader{
		{
			Key:   "traceparent",
			Value: []byte("original-trace"),
		},
		{
			Key:   "",
			Value: []byte("empty-key-value"),
		},
		{
			Key: ClassificationCommandDLQHeaderErrorCode,

			Value: []byte(
				ClassificationWorkerErrorCodeLightCurveInvalid,
			),
		},
		{
			Key: ClassificationCommandDLQHeaderErrorClass,

			Value: []byte("PERMANENT"),
		},
		{
			Key: ClassificationCommandDLQHeaderErrorOperation,

			Value: []byte(
				ClassificationWorkerOperationPrepareInput,
			),
		},
		{
			Key: ClassificationCommandDLQHeaderOriginalTopic,

			Value: []byte(
				"astro.classification.commands.v1",
			),
		},
		{
			Key: ClassificationCommandDLQHeaderOriginalPartition,

			Value: []byte("3"),
		},
		{
			Key: ClassificationCommandDLQHeaderOriginalOffset,

			Value: []byte("42"),
		},
	}

	if !reflect.DeepEqual(
		message.Headers,
		wantHeaders,
	) {
		t.Fatalf(
			"Headers = %#v, want %#v",
			message.Headers,
			wantHeaders,
		)
	}

	original.Key[0] = 'X'
	original.Value[0] = 0x01
	original.Headers[0].Value[0] = 'X'

	if string(message.Key) != "OBJ-0001" {
		t.Fatal("DLQ Key was not deep copied")
	}

	if !reflect.DeepEqual(
		message.Value,
		[]byte{0xff, 0x00},
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

func TestBuildClassificationCommandDLQMessagePreservesShapes(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name string

		key     []byte
		value   []byte
		headers []MessageHeader

		wantKeyNil     bool
		wantValueNil   bool
		wantHeadersNil bool
	}{
		{
			name: "nil values",

			key:     nil,
			value:   nil,
			headers: nil,

			wantKeyNil:     true,
			wantValueNil:   true,
			wantHeadersNil: false,
		},
		{
			name: "non-nil empty values",

			key:     make([]byte, 0),
			value:   make([]byte, 0),
			headers: make([]MessageHeader, 0),

			wantKeyNil:     false,
			wantValueNil:   false,
			wantHeadersNil: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			message, err :=
				BuildClassificationCommandDLQMessage(
					testClassificationCommandDLQTopic,
					InboundMessage{
						Topic: "astro.classification.commands.v1",

						Key:     test.key,
						Value:   test.value,
						Headers: test.headers,
					},
					&ClassificationWorkerError{
						Code: ClassificationWorkerErrorCodeResultInvalid,

						Class: ClassificationWorkerErrorClassPermanent,

						Operation: ClassificationWorkerOperationBuildRun,
					},
				)
			if err != nil {
				t.Fatalf(
					"BuildClassificationCommandDLQMessage() error = %v",
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

			// 即使原始 Headers 为 nil，也会追加 DLQ 元数据。
			if (message.Headers == nil) !=
				test.wantHeadersNil {
				t.Fatalf(
					"Headers nil = %v, want %v",
					message.Headers == nil,
					test.wantHeadersNil,
				)
			}
		})
	}
}

func TestBuildClassificationCommandDLQMessageRejectsInvalidArguments(
	t *testing.T,
) {
	t.Parallel()

	validError := &ClassificationWorkerError{
		Code: ClassificationWorkerErrorCodeResultInvalid,

		Class: ClassificationWorkerErrorClassPermanent,

		Operation: ClassificationWorkerOperationBuildRun,
	}

	tests := []struct {
		name string

		topic       string
		workerError *ClassificationWorkerError

		wantErrorPart string
	}{
		{
			name: "empty DLQ topic",

			workerError: validError,

			wantErrorPart: "topic must not be empty",
		},
		{
			name: "nil worker error",

			topic: testClassificationCommandDLQTopic,

			wantErrorPart: "worker error must not be nil",
		},
		{
			name: "retryable worker error",

			topic: testClassificationCommandDLQTopic,

			workerError: &ClassificationWorkerError{
				Code: ClassificationWorkerErrorCodeDependencyUnavailable,

				Class: ClassificationWorkerErrorClassRetryable,

				Operation: ClassificationWorkerOperationPrepareInput,
			},

			wantErrorPart: "requires a permanent worker error",
		},
		{
			name: "cancelled worker error",

			topic: testClassificationCommandDLQTopic,

			workerError: &ClassificationWorkerError{
				Code: ClassificationWorkerErrorCodeCancelled,

				Class: ClassificationWorkerErrorClassCancelled,

				Operation: ClassificationWorkerOperationClassify,
			},

			wantErrorPart: "requires a permanent worker error",
		},
		{
			name: "empty error code",

			topic: testClassificationCommandDLQTopic,

			workerError: &ClassificationWorkerError{
				Class: ClassificationWorkerErrorClassPermanent,

				Operation: ClassificationWorkerOperationBuildRun,
			},

			wantErrorPart: "code must not be empty",
		},
		{
			name: "empty operation",

			topic: testClassificationCommandDLQTopic,

			workerError: &ClassificationWorkerError{
				Code: ClassificationWorkerErrorCodeResultInvalid,

				Class: ClassificationWorkerErrorClassPermanent,
			},

			wantErrorPart: "operation must not be empty",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err :=
				BuildClassificationCommandDLQMessage(
					test.topic,
					InboundMessage{},
					test.workerError,
				)
			if err == nil {
				t.Fatal(
					"error = nil, want non-nil",
				)
			}

			if !strings.Contains(
				err.Error(),
				test.wantErrorPart,
			) {
				t.Fatalf(
					"error = %q, want substring %q",
					err.Error(),
					test.wantErrorPart,
				)
			}
		})
	}
}
