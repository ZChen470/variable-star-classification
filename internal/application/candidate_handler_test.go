package application

import (
	"context"
	"errors"
	"strings"
	"testing"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"google.golang.org/protobuf/proto"
)

func TestCandidateHandlerPublishesClassificationCommand(t *testing.T) {
	t.Parallel()

	publisher := &recordingMessagePublisher{}
	handler := newValidCandidateHandler(t, publisher)

	event := validCandidateEvent()
	message := candidateInboundMessage(t, event)
	message.Headers = []MessageHeader{
		{
			Key:   "",
			Value: []byte{},
		},
		{
			Key:   "traceparent",
			Value: []byte("original-trace"),
		},
	}

	if err := handler.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf(
			"published message count = %d, want 1",
			len(publisher.messages),
		)
	}

	published := publisher.messages[0]
	if published.Topic != testClassificationCommandTopic {
		t.Fatalf(
			"published topic = %q, want %q",
			published.Topic,
			testClassificationCommandTopic,
		)
	}

	if string(published.Key) != event.GetObjectId() {
		t.Fatalf(
			"published key = %q, want %q",
			published.Key,
			event.GetObjectId(),
		)
	}

	if len(published.Headers) != len(message.Headers) {
		t.Fatalf(
			"published header count = %d, want %d",
			len(published.Headers),
			len(message.Headers),
		)
	}

	var command classificationv1.ClassificationCommand
	if err := proto.Unmarshal(published.Value, &command); err != nil {
		t.Fatalf("unmarshal ClassificationCommand: %v", err)
	}

	if command.GetJobId() == "" {
		t.Fatal("published ClassificationCommand has empty job_id")
	}
	if command.GetObjectId() != event.GetObjectId() {
		t.Fatalf(
			"command object_id = %q, want %q",
			command.GetObjectId(),
			event.GetObjectId(),
		)
	}
	if command.GetCandidateRevision() != event.GetCandidateRevision() {
		t.Fatalf(
			"command candidate_revision = %d, want %d",
			command.GetCandidateRevision(),
			event.GetCandidateRevision(),
		)
	}
	if command.GetLightCurveRevision() !=
		event.GetLightCurveRevision() {
		t.Fatalf(
			"command light_curve_revision = %d, want %d",
			command.GetLightCurveRevision(),
			event.GetLightCurveRevision(),
		)
	}
	if command.GetCreatedAt() == nil ||
		!command.GetCreatedAt().AsTime().Equal(
			event.GetOccurredAt().AsTime(),
		) {
		t.Fatalf(
			"command created_at = %v, want %v",
			command.GetCreatedAt(),
			event.GetOccurredAt().AsTime(),
		)
	}
}

func TestCandidateHandlerPublishesPermanentErrorToDLQ(t *testing.T) {
	t.Parallel()

	publisher := &recordingMessagePublisher{}
	handler := newValidCandidateHandler(t, publisher)

	message := candidateInboundMessage(t, validCandidateEvent())
	message.Value = []byte{0xff}
	message.Headers = []MessageHeader{
		{
			Key:   "original-header",
			Value: []byte("original-value"),
		},
	}

	if err := handler.Handle(context.Background(), message); err != nil {
		t.Fatalf("Handle returned error: %v", err)
	}

	if len(publisher.messages) != 1 {
		t.Fatalf(
			"published message count = %d, want 1",
			len(publisher.messages),
		)
	}

	published := publisher.messages[0]
	if published.Topic != testCandidateDLQTopic {
		t.Fatalf(
			"published topic = %q, want %q",
			published.Topic,
			testCandidateDLQTopic,
		)
	}

	if len(published.Headers) != len(message.Headers)+4 {
		t.Fatalf(
			"published header count = %d, want %d",
			len(published.Headers),
			len(message.Headers)+4,
		)
	}

	errorHeader := published.Headers[len(message.Headers)]
	if errorHeader.Key != CandidateDLQHeaderErrorCode {
		t.Fatalf(
			"DLQ error header key = %q, want %q",
			errorHeader.Key,
			CandidateDLQHeaderErrorCode,
		)
	}
	if string(errorHeader.Value) !=
		string(CandidateMessageErrorCodeMalformedProto) {
		t.Fatalf(
			"DLQ error code = %q, want %q",
			errorHeader.Value,
			CandidateMessageErrorCodeMalformedProto,
		)
	}

	if !published.Timestamp.Equal(message.Timestamp) {
		t.Fatalf(
			"DLQ timestamp = %v, want %v",
			published.Timestamp,
			message.Timestamp,
		)
	}
}

func TestCandidateHandlerReturnsPublishFailures(t *testing.T) {
	t.Parallel()

	publishErr := errors.New("broker unavailable")

	tests := []struct {
		name      string
		message   func(*testing.T) InboundMessage
		errorPart string
	}{
		{
			name: "classification command publish failure",
			message: func(t *testing.T) InboundMessage {
				return candidateInboundMessage(
					t,
					validCandidateEvent(),
				)
			},
			errorPart: "publish classification command message",
		},
		{
			name: "candidate DLQ publish failure",
			message: func(t *testing.T) InboundMessage {
				message := candidateInboundMessage(
					t,
					validCandidateEvent(),
				)
				message.Value = []byte{0xff}
				return message
			},
			errorPart: "publish candidate DLQ message",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			publisher := &recordingMessagePublisher{
				err: publishErr,
			}
			handler := newValidCandidateHandler(t, publisher)

			err := handler.Handle(
				context.Background(),
				test.message(t),
			)
			if err == nil {
				t.Fatal("expected publish error")
			}
			if !errors.Is(err, publishErr) {
				t.Fatalf(
					"Handle error = %v, want wrapped %v",
					err,
					publishErr,
				)
			}
			if !strings.Contains(err.Error(), test.errorPart) {
				t.Fatalf(
					"Handle error = %q, want substring %q",
					err,
					test.errorPart,
				)
			}
			if len(publisher.messages) != 1 {
				t.Fatalf(
					"publish attempt count = %d, want 1",
					len(publisher.messages),
				)
			}
		})
	}
}

func TestNewCandidateHandlerRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	validPolicy := newValidCandidateHandlerPolicy(t)
	validPublisher := &recordingMessagePublisher{}

	tests := []struct {
		name              string
		candidateTopic    string
		commandTopic      string
		candidateDLQTopic string
		policy            ClassificationPolicyV1
		publisher         MessagePublisher
		errorPart         string
	}{
		{
			name:              "empty candidate topic",
			commandTopic:      testClassificationCommandTopic,
			candidateDLQTopic: testCandidateDLQTopic,
			policy:            validPolicy,
			publisher:         validPublisher,
			errorPart:         "candidate topic",
		},
		{
			name:              "empty command topic",
			candidateTopic:    testCandidateTopic,
			candidateDLQTopic: testCandidateDLQTopic,
			policy:            validPolicy,
			publisher:         validPublisher,
			errorPart:         "command topic",
		},
		{
			name:           "empty candidate DLQ topic",
			candidateTopic: testCandidateTopic,
			commandTopic:   testClassificationCommandTopic,
			policy:         validPolicy,
			publisher:      validPublisher,
			errorPart:      "DLQ topic",
		},
		{
			name:              "zero value policy",
			candidateTopic:    testCandidateTopic,
			commandTopic:      testClassificationCommandTopic,
			candidateDLQTopic: testCandidateDLQTopic,
			publisher:         validPublisher,
			errorPart:         "policy is not configured",
		},
		{
			name:              "nil publisher",
			candidateTopic:    testCandidateTopic,
			commandTopic:      testClassificationCommandTopic,
			candidateDLQTopic: testCandidateDLQTopic,
			policy:            validPolicy,
			errorPart:         "publisher must not be nil",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := NewCandidateHandler(
				test.candidateTopic,
				test.commandTopic,
				test.candidateDLQTopic,
				test.policy,
				test.publisher,
			)
			if err == nil {
				t.Fatal("expected CandidateHandler configuration error")
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

func TestCandidateHandlerRejectsNilReceiverAndContext(t *testing.T) {
	t.Parallel()

	var nilHandler *CandidateHandler
	if err := nilHandler.Handle(
		context.Background(),
		InboundMessage{},
	); err == nil {
		t.Fatal("expected nil handler error")
	}

	handler := newValidCandidateHandler(
		t,
		&recordingMessagePublisher{},
	)
	if err := handler.Handle(
		nil,
		InboundMessage{},
	); err == nil {
		t.Fatal("expected nil context error")
	}
}

func newValidCandidateHandler(
	t *testing.T,
	publisher MessagePublisher,
) *CandidateHandler {
	t.Helper()

	handler, err := NewCandidateHandler(
		testCandidateTopic,
		testClassificationCommandTopic,
		testCandidateDLQTopic,
		newValidCandidateHandlerPolicy(t),
		publisher,
	)
	if err != nil {
		t.Fatalf("NewCandidateHandler returned error: %v", err)
	}

	return handler
}

func newValidCandidateHandlerPolicy(
	t *testing.T,
) ClassificationPolicyV1 {
	t.Helper()

	policy, err := NewClassificationPolicyV1(
		"bundle-v1",
		"classification-policy-v1",
	)
	if err != nil {
		t.Fatalf("NewClassificationPolicyV1 returned error: %v", err)
	}

	return policy
}

type recordingMessagePublisher struct {
	messages []OutboundMessage
	err      error
}

func (publisher *recordingMessagePublisher) Publish(
	_ context.Context,
	message OutboundMessage,
) error {
	publisher.messages = append(publisher.messages, message)
	return publisher.err
}
