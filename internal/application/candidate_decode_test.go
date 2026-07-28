package application

import (
	"errors"
	"testing"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	testCandidateTopic    = "astro.candidate.events.v1"
	testCandidateObjectID = "object-123"
)

func TestDecodeCandidateEventMessageSupportedEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		protoEventType classificationv1.CandidateEventType
		wantEventType  CandidateEventType
	}{
		{
			name:           "created",
			protoEventType: classificationv1.CandidateEventType_CANDIDATE_CREATED,
			wantEventType:  CandidateEventTypeCreated,
		},
		{
			name:           "updated",
			protoEventType: classificationv1.CandidateEventType_CANDIDATE_UPDATED,
			wantEventType:  CandidateEventTypeUpdated,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			event := validCandidateEvent()
			event.EventType = test.protoEventType
			message := candidateInboundMessage(t, event)

			got, err := DecodeCandidateEventMessage(testCandidateTopic, message)
			if err != nil {
				t.Fatalf("DecodeCandidateEventMessage returned error: %v", err)
			}

			if got.EventID != event.GetEventId() {
				t.Fatalf("unexpected event ID: got %q, want %q", got.EventID, event.GetEventId())
			}
			if got.EventType != test.wantEventType {
				t.Fatalf(
					"unexpected event type: got %d, want %d",
					got.EventType,
					test.wantEventType,
				)
			}
			if got.ObjectID != event.GetObjectId() {
				t.Fatalf("unexpected object ID: got %q, want %q", got.ObjectID, event.GetObjectId())
			}
			if got.CandidateRevision != event.GetCandidateRevision() {
				t.Fatalf(
					"unexpected candidate revision: got %d, want %d",
					got.CandidateRevision,
					event.GetCandidateRevision(),
				)
			}
			if got.LightCurveRevision != event.GetLightCurveRevision() {
				t.Fatalf(
					"unexpected light curve revision: got %d, want %d",
					got.LightCurveRevision,
					event.GetLightCurveRevision(),
				)
			}
			if got.EligibleEpochCount != event.GetEligibleEpochCount() {
				t.Fatalf(
					"unexpected eligible epoch count: got %d, want %d",
					got.EligibleEpochCount,
					event.GetEligibleEpochCount(),
				)
			}
			if !got.OccurredAt.Equal(event.GetOccurredAt().AsTime()) {
				t.Fatalf(
					"unexpected occurred_at: got %v, want %v",
					got.OccurredAt,
					event.GetOccurredAt().AsTime(),
				)
			}
			if got.Producer != event.GetProducer() {
				t.Fatalf("unexpected producer: got %q, want %q", got.Producer, event.GetProducer())
			}
			if got.UpstreamPipelineVersion != event.GetUpstreamPipelineVersion() {
				t.Fatalf(
					"unexpected upstream pipeline version: got %q, want %q",
					got.UpstreamPipelineVersion,
					event.GetUpstreamPipelineVersion(),
				)
			}
			if got.TraceContext.TraceID != event.GetTraceContext().GetTraceId() {
				t.Fatalf(
					"unexpected trace ID: got %q, want %q",
					got.TraceContext.TraceID,
					event.GetTraceContext().GetTraceId(),
				)
			}
			if got.TraceContext.CorrelationID != event.GetTraceContext().GetCorrelationId() {
				t.Fatalf(
					"unexpected correlation ID: got %q, want %q",
					got.TraceContext.CorrelationID,
					event.GetTraceContext().GetCorrelationId(),
				)
			}
			if got.TraceContext.CausationID != event.GetTraceContext().GetCausationId() {
				t.Fatalf(
					"unexpected causation ID: got %q, want %q",
					got.TraceContext.CausationID,
					event.GetTraceContext().GetCausationId(),
				)
			}
		})
	}
}

func TestDecodeCandidateEventMessageAllowsMissingTraceContext(t *testing.T) {
	t.Parallel()

	event := validCandidateEvent()
	event.TraceContext = nil

	got, err := DecodeCandidateEventMessage(
		testCandidateTopic,
		candidateInboundMessage(t, event),
	)
	if err != nil {
		t.Fatalf("DecodeCandidateEventMessage returned error: %v", err)
	}

	if got.TraceContext != (TraceContext{}) {
		t.Fatalf("unexpected trace context: got %+v, want zero value", got.TraceContext)
	}
}

func TestDecodeCandidateEventMessageRejectsPermanentInvalidMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		modifyEvent   func(*classificationv1.CandidateEvent)
		modifyMessage func(*InboundMessage)
		wantCode      CandidateMessageErrorCode
		wantField     string
	}{
		{
			name: "unexpected topic",
			modifyMessage: func(message *InboundMessage) {
				message.Topic = "other.topic"
			},
			wantCode:  CandidateMessageErrorCodeUnexpectedTopic,
			wantField: "topic",
		},
		{
			name: "missing key",
			modifyMessage: func(message *InboundMessage) {
				message.Key = nil
			},
			wantCode:  CandidateMessageErrorCodeMissingKey,
			wantField: "key",
		},
		{
			name: "empty value",
			modifyMessage: func(message *InboundMessage) {
				message.Value = nil
			},
			wantCode:  CandidateMessageErrorCodeMalformedProto,
			wantField: "value",
		},
		{
			name: "malformed protobuf",
			modifyMessage: func(message *InboundMessage) {
				message.Value = []byte{0xff}
			},
			wantCode:  CandidateMessageErrorCodeMalformedProto,
			wantField: "value",
		},
		{
			name: "unspecified event type",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.EventType = classificationv1.CandidateEventType_CANDIDATE_EVENT_TYPE_UNSPECIFIED
			},
			wantCode:  CandidateMessageErrorCodeUnsupportedEventType,
			wantField: "event_type",
		},
		{
			name: "retracted event type",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.EventType = classificationv1.CandidateEventType_CANDIDATE_RETRACTED
			},
			wantCode:  CandidateMessageErrorCodeUnsupportedEventType,
			wantField: "event_type",
		},
		{
			name: "unknown event type",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.EventType = classificationv1.CandidateEventType(99)
			},
			wantCode:  CandidateMessageErrorCodeUnsupportedEventType,
			wantField: "event_type",
		},
		{
			name: "empty event ID",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.EventId = ""
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "event_id",
		},
		{
			name: "empty object ID",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.ObjectId = ""
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "object_id",
		},
		{
			name: "key mismatch",
			modifyMessage: func(message *InboundMessage) {
				message.Key = []byte("other-object")
			},
			wantCode:  CandidateMessageErrorCodeKeyMismatch,
			wantField: "key",
		},
		{
			name: "invalid candidate revision",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.CandidateRevision = 0
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "candidate_revision",
		},
		{
			name: "invalid light curve revision",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.LightCurveRevision = 0
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "light_curve_revision",
		},
		{
			name: "insufficient eligible epochs",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.EligibleEpochCount = 2
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "eligible_epoch_count",
		},
		{
			name: "missing occurred at",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.OccurredAt = nil
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "occurred_at",
		},
		{
			name: "invalid occurred at",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.OccurredAt = &timestamppb.Timestamp{
					Seconds: 253402300800,
				}
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "occurred_at",
		},
		{
			name: "empty producer",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.Producer = ""
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "producer",
		},
		{
			name: "empty upstream pipeline version",
			modifyEvent: func(event *classificationv1.CandidateEvent) {
				event.UpstreamPipelineVersion = ""
			},
			wantCode:  CandidateMessageErrorCodeInvalidEvent,
			wantField: "upstream_pipeline_version",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			event := validCandidateEvent()
			if test.modifyEvent != nil {
				test.modifyEvent(event)
			}

			message := candidateInboundMessage(t, event)
			if test.modifyMessage != nil {
				test.modifyMessage(&message)
			}

			_, err := DecodeCandidateEventMessage(testCandidateTopic, message)
			requirePermanentCandidateMessageError(
				t,
				err,
				test.wantCode,
				test.wantField,
			)
		})
	}
}

func TestDecodeCandidateEventMessageRejectsEmptyExpectedTopicAsConfigurationError(t *testing.T) {
	t.Parallel()

	message := candidateInboundMessage(t, validCandidateEvent())

	_, err := DecodeCandidateEventMessage("", message)
	if err == nil {
		t.Fatal("expected error for empty expected topic")
	}

	var permanentErr *PermanentCandidateMessageError
	if errors.As(err, &permanentErr) {
		t.Fatalf("expected configuration error, got permanent message error: %v", permanentErr)
	}
}

func validCandidateEvent() *classificationv1.CandidateEvent {
	return &classificationv1.CandidateEvent{
		EventId:                 "event-123",
		EventType:               classificationv1.CandidateEventType_CANDIDATE_CREATED,
		ObjectId:                testCandidateObjectID,
		CandidateRevision:       7,
		LightCurveRevision:      11,
		EligibleEpochCount:      3,
		OccurredAt:              timestamppb.New(time.Date(2026, 7, 28, 9, 30, 0, 0, time.UTC)),
		Producer:                "candidate-pipeline",
		UpstreamPipelineVersion: "pipeline-v1",
		TraceContext: &classificationv1.TraceContext{
			TraceId:       "trace-123",
			CorrelationId: "correlation-123",
			CausationId:   "causation-123",
		},
	}
}

func candidateInboundMessage(
	t *testing.T,
	event *classificationv1.CandidateEvent,
) InboundMessage {
	t.Helper()

	value, err := proto.Marshal(event)
	if err != nil {
		t.Fatalf("marshal CandidateEvent: %v", err)
	}

	return InboundMessage{
		Topic:     testCandidateTopic,
		Partition: 2,
		Offset:    17,
		Key:       []byte(testCandidateObjectID),
		Value:     value,
		Timestamp: time.Date(2026, 7, 28, 9, 31, 0, 0, time.UTC),
	}
}

func requirePermanentCandidateMessageError(
	t *testing.T,
	err error,
	wantCode CandidateMessageErrorCode,
	wantField string,
) {
	t.Helper()

	if err == nil {
		t.Fatal("expected permanent candidate message error")
	}

	var permanentErr *PermanentCandidateMessageError
	if !errors.As(err, &permanentErr) {
		t.Fatalf("expected PermanentCandidateMessageError, got %T: %v", err, err)
	}

	if permanentErr.Code != wantCode {
		t.Fatalf(
			"unexpected error code: got %q, want %q",
			permanentErr.Code,
			wantCode,
		)
	}

	if permanentErr.Field != wantField {
		t.Fatalf(
			"unexpected error field: got %q, want %q",
			permanentErr.Field,
			wantField,
		)
	}
}
