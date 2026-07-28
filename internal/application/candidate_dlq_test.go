package application

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

const testCandidateDLQTopic = "astro.candidate.events.dlq.v1"

func TestBuildCandidateDLQMessage(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(
		2026,
		time.July,
		28,
		11,
		0,
		0,
		0,
		time.UTC,
	)

	original := InboundMessage{
		Topic:     testCandidateTopic,
		Partition: 3,
		Offset:    42,
		Key:       []byte("object-123"),
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

	candidateErr := &PermanentCandidateMessageError{
		Code:  CandidateMessageErrorCodeMalformedProto,
		Field: "value",
		Err:   errors.New("invalid wire data"),
	}

	message, err := BuildCandidateDLQMessage(
		testCandidateDLQTopic,
		original,
		candidateErr,
	)
	if err != nil {
		t.Fatalf("BuildCandidateDLQMessage returned error: %v", err)
	}

	if message.Topic != testCandidateDLQTopic {
		t.Fatalf(
			"unexpected topic: got %q, want %q",
			message.Topic,
			testCandidateDLQTopic,
		)
	}

	if !reflect.DeepEqual(message.Key, original.Key) {
		t.Fatalf(
			"unexpected key: got %#v, want %#v",
			message.Key,
			original.Key,
		)
	}

	if !reflect.DeepEqual(message.Value, original.Value) {
		t.Fatalf(
			"unexpected value: got %#v, want %#v",
			message.Value,
			original.Value,
		)
	}

	if !message.Timestamp.Equal(original.Timestamp) {
		t.Fatalf(
			"unexpected timestamp: got %v, want %v",
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
			Key:   CandidateDLQHeaderErrorCode,
			Value: []byte(CandidateMessageErrorCodeMalformedProto),
		},
		{
			Key:   CandidateDLQHeaderOriginalTopic,
			Value: []byte(testCandidateTopic),
		},
		{
			Key:   CandidateDLQHeaderOriginalPartition,
			Value: []byte("3"),
		},
		{
			Key:   CandidateDLQHeaderOriginalOffset,
			Value: []byte("42"),
		},
	}

	if !reflect.DeepEqual(message.Headers, wantHeaders) {
		t.Fatalf(
			"unexpected headers: got %#v, want %#v",
			message.Headers,
			wantHeaders,
		)
	}

	original.Key[0] = 'X'
	original.Value[0] = 0x01
	original.Headers[0].Value[0] = 'X'

	if string(message.Key) != "object-123" {
		t.Fatal("DLQ key was not deep copied")
	}
	if !reflect.DeepEqual(message.Value, []byte{0xff, 0x00}) {
		t.Fatal("DLQ value was not deep copied")
	}
	if string(message.Headers[0].Value) != "original-trace" {
		t.Fatal("DLQ header value was not deep copied")
	}
}

func TestBuildCandidateDLQMessagePreservesNilAndEmptyValues(t *testing.T) {
	t.Parallel()

	original := InboundMessage{
		Topic: testCandidateTopic,
		Key:   nil,
		Value: []byte{},
		Headers: []MessageHeader{
			{
				Key:   "",
				Value: nil,
			},
			{
				Key:   "",
				Value: []byte{},
			},
		},
	}

	message, err := BuildCandidateDLQMessage(
		testCandidateDLQTopic,
		original,
		&PermanentCandidateMessageError{
			Code: CandidateMessageErrorCodeMissingKey,
		},
	)
	if err != nil {
		t.Fatalf("BuildCandidateDLQMessage returned error: %v", err)
	}

	if message.Key != nil {
		t.Fatalf("expected nil key, got %#v", message.Key)
	}

	if message.Value == nil {
		t.Fatal("expected non-nil empty value")
	}
	if len(message.Value) != 0 {
		t.Fatalf("expected empty value, got %#v", message.Value)
	}

	if message.Headers[0].Value != nil {
		t.Fatalf(
			"expected nil first header value, got %#v",
			message.Headers[0].Value,
		)
	}

	if message.Headers[1].Value == nil {
		t.Fatal("expected non-nil empty second header value")
	}
	if len(message.Headers[1].Value) != 0 {
		t.Fatalf(
			"expected empty second header value, got %#v",
			message.Headers[1].Value,
		)
	}
}

func TestBuildCandidateDLQMessageRejectsInvalidArguments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		topic        string
		candidateErr *PermanentCandidateMessageError
		errorPart    string
	}{
		{
			name: "empty DLQ topic",
			candidateErr: &PermanentCandidateMessageError{
				Code: CandidateMessageErrorCodeMalformedProto,
			},
			errorPart: "topic must not be empty",
		},
		{
			name:         "nil permanent error",
			topic:        testCandidateDLQTopic,
			candidateErr: nil,
			errorPart:    "permanent candidate message error must not be nil",
		},
		{
			name:  "empty permanent error code",
			topic: testCandidateDLQTopic,
			candidateErr: &PermanentCandidateMessageError{
				Err: errors.New("invalid message"),
			},
			errorPart: "code must not be empty",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := BuildCandidateDLQMessage(
				test.topic,
				InboundMessage{},
				test.candidateErr,
			)
			if err == nil {
				t.Fatal("expected Candidate DLQ construction error")
			}

			if !strings.Contains(err.Error(), test.errorPart) {
				t.Fatalf(
					"unexpected error: got %q, want substring %q",
					err.Error(),
					test.errorPart,
				)
			}
		})
	}
}
