package application

import (
	"strings"
	"testing"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
)

const testClassificationCommandTopic = "astro.classification.commands.v1"

func TestBuildClassificationCommandMessage(t *testing.T) {
	t.Parallel()

	input := validClassificationCommandInput()
	decision := validClassificationCommandDecision()
	headers := []MessageHeader{
		{
			Key:   "traceparent",
			Value: []byte("00-trace-parent"),
		},
		{
			Key:   "x-upstream-header",
			Value: []byte("upstream-value"),
		},
	}

	message, err := BuildClassificationCommandMessage(
		testClassificationCommandTopic,
		input,
		decision,
		headers,
	)
	if err != nil {
		t.Fatalf("BuildClassificationCommandMessage returned error: %v", err)
	}

	if message.Topic != testClassificationCommandTopic {
		t.Fatalf(
			"unexpected topic: got %q, want %q",
			message.Topic,
			testClassificationCommandTopic,
		)
	}
	if string(message.Key) != input.ObjectID {
		t.Fatalf(
			"unexpected key: got %q, want %q",
			message.Key,
			input.ObjectID,
		)
	}
	if !message.Timestamp.IsZero() {
		t.Fatalf(
			"expected zero Kafka record timestamp, got %v",
			message.Timestamp,
		)
	}

	if len(message.Headers) != len(headers) {
		t.Fatalf(
			"unexpected header count: got %d, want %d",
			len(message.Headers),
			len(headers),
		)
	}
	for index := range headers {
		if message.Headers[index].Key != headers[index].Key {
			t.Fatalf(
				"unexpected header %d key: got %q, want %q",
				index,
				message.Headers[index].Key,
				headers[index].Key,
			)
		}
		if string(message.Headers[index].Value) != string(headers[index].Value) {
			t.Fatalf(
				"unexpected header %d value: got %q, want %q",
				index,
				message.Headers[index].Value,
				headers[index].Value,
			)
		}
	}

	headers[0].Value[0] = 'X'
	if string(message.Headers[0].Value) != "00-trace-parent" {
		t.Fatal("command headers were not deep copied")
	}

	var command classificationv1.ClassificationCommand
	if err := proto.Unmarshal(message.Value, &command); err != nil {
		t.Fatalf("unmarshal ClassificationCommand: %v", err)
	}

	expectedJobID, err := domain.GenerateJobID(domain.JobIdentity{
		ObjectID:                    input.ObjectID,
		LightCurveRevision:          input.LightCurveRevision,
		ModelBundleVersion:          decision.ModelBundleVersion,
		ClassificationPolicyVersion: decision.ClassificationPolicyVersion,
		ExecutionMode:               decision.ExecutionMode,
	})
	if err != nil {
		t.Fatalf("GenerateJobID returned error: %v", err)
	}

	if command.GetJobId() != string(expectedJobID) {
		t.Fatalf(
			"unexpected job ID: got %q, want %q",
			command.GetJobId(),
			expectedJobID,
		)
	}
	if command.GetObjectId() != input.ObjectID {
		t.Fatalf(
			"unexpected object ID: got %q, want %q",
			command.GetObjectId(),
			input.ObjectID,
		)
	}
	if command.GetCandidateRevision() != input.CandidateRevision {
		t.Fatalf(
			"unexpected candidate revision: got %d, want %d",
			command.GetCandidateRevision(),
			input.CandidateRevision,
		)
	}
	if command.GetLightCurveRevision() != input.LightCurveRevision {
		t.Fatalf(
			"unexpected light curve revision: got %d, want %d",
			command.GetLightCurveRevision(),
			input.LightCurveRevision,
		)
	}
	if command.GetDeclaredEligibleEpochCount() != input.EligibleEpochCount {
		t.Fatalf(
			"unexpected epoch count: got %d, want %d",
			command.GetDeclaredEligibleEpochCount(),
			input.EligibleEpochCount,
		)
	}
	if command.GetModelBundleVersion() != decision.ModelBundleVersion {
		t.Fatalf(
			"unexpected model bundle version: got %q, want %q",
			command.GetModelBundleVersion(),
			decision.ModelBundleVersion,
		)
	}
	if command.GetClassificationPolicyVersion() != decision.ClassificationPolicyVersion {
		t.Fatalf(
			"unexpected policy version: got %q, want %q",
			command.GetClassificationPolicyVersion(),
			decision.ClassificationPolicyVersion,
		)
	}
	if command.GetExecutionMode() !=
		classificationv1.ExecutionMode_EXECUTION_MODE_PRODUCTION {
		t.Fatalf("unexpected execution mode: %v", command.GetExecutionMode())
	}
	if command.GetPriority() !=
		classificationv1.ClassificationPriority_CLASSIFICATION_PRIORITY_REALTIME {
		t.Fatalf("unexpected priority: %v", command.GetPriority())
	}
	if command.GetCreatedAt() == nil {
		t.Fatal("expected created_at")
	}
	if !command.GetCreatedAt().AsTime().Equal(input.OccurredAt) {
		t.Fatalf(
			"unexpected created_at: got %v, want %v",
			command.GetCreatedAt().AsTime(),
			input.OccurredAt,
		)
	}
	if command.GetDeadlineAt() != nil {
		t.Fatalf("expected nil deadline_at, got %v", command.GetDeadlineAt())
	}
	if command.GetTraceContext() == nil {
		t.Fatal("expected trace context")
	}
	if command.GetTraceContext().GetTraceId() != input.TraceContext.TraceID {
		t.Fatalf(
			"unexpected trace ID: got %q, want %q",
			command.GetTraceContext().GetTraceId(),
			input.TraceContext.TraceID,
		)
	}
	if command.GetTraceContext().GetCorrelationId() !=
		input.TraceContext.CorrelationID {
		t.Fatalf(
			"unexpected correlation ID: got %q, want %q",
			command.GetTraceContext().GetCorrelationId(),
			input.TraceContext.CorrelationID,
		)
	}
	if command.GetTraceContext().GetCausationId() !=
		input.TraceContext.CausationID {
		t.Fatalf(
			"unexpected causation ID: got %q, want %q",
			command.GetTraceContext().GetCausationId(),
			input.TraceContext.CausationID,
		)
	}
}

func TestBuildClassificationCommandMessageJobIDSemantics(t *testing.T) {
	t.Parallel()

	baseInput := validClassificationCommandInput()
	baseDecision := validClassificationCommandDecision()

	baseJobID := buildClassificationCommandJobID(
		t,
		baseInput,
		baseDecision,
	)

	nonIdentityInputs := []CandidateEventInput{
		func() CandidateEventInput {
			input := baseInput
			input.EventType = CandidateEventTypeUpdated
			return input
		}(),
		func() CandidateEventInput {
			input := baseInput
			input.CandidateRevision++
			return input
		}(),
		func() CandidateEventInput {
			input := baseInput
			input.EligibleEpochCount++
			return input
		}(),
		func() CandidateEventInput {
			input := baseInput
			input.OccurredAt = input.OccurredAt.Add(time.Minute)
			return input
		}(),
		func() CandidateEventInput {
			input := baseInput
			input.TraceContext.TraceID = "different-trace"
			return input
		}(),
	}

	for index, input := range nonIdentityInputs {
		gotJobID := buildClassificationCommandJobID(
			t,
			input,
			baseDecision,
		)
		if gotJobID != baseJobID {
			t.Fatalf(
				"non-identity input %d changed job ID: got %q, want %q",
				index,
				gotJobID,
				baseJobID,
			)
		}
	}

	identityCases := []struct {
		name     string
		input    CandidateEventInput
		decision ClassificationPolicyDecision
	}{
		{
			name: "object ID",
			input: func() CandidateEventInput {
				input := baseInput
				input.ObjectID = "object-456"
				return input
			}(),
			decision: baseDecision,
		},
		{
			name: "light curve revision",
			input: func() CandidateEventInput {
				input := baseInput
				input.LightCurveRevision++
				return input
			}(),
			decision: baseDecision,
		},
		{
			name:  "model bundle version",
			input: baseInput,
			decision: func() ClassificationPolicyDecision {
				decision := baseDecision
				decision.ModelBundleVersion = "bundle-v2"
				return decision
			}(),
		},
		{
			name:  "classification policy version",
			input: baseInput,
			decision: func() ClassificationPolicyDecision {
				decision := baseDecision
				decision.ClassificationPolicyVersion = "policy-v2"
				return decision
			}(),
		},
		{
			name:  "execution mode",
			input: baseInput,
			decision: func() ClassificationPolicyDecision {
				decision := baseDecision
				decision.ExecutionMode = domain.ExecutionModeShadow
				return decision
			}(),
		},
	}

	for _, test := range identityCases {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			gotJobID := buildClassificationCommandJobID(
				t,
				test.input,
				test.decision,
			)
			if gotJobID == baseJobID {
				t.Fatalf(
					"identity field %s did not change job ID %q",
					test.name,
					baseJobID,
				)
			}
		})
	}
}

func TestBuildClassificationCommandMessageRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	deadline := time.Date(2026, 7, 28, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name      string
		topic     string
		input     CandidateEventInput
		decision  ClassificationPolicyDecision
		headers   []MessageHeader
		errorPart string
	}{
		{
			name:      "empty topic",
			input:     validClassificationCommandInput(),
			decision:  validClassificationCommandDecision(),
			errorPart: "topic must not be empty",
		},
		{
			name:  "decision does not classify",
			topic: testClassificationCommandTopic,
			input: validClassificationCommandInput(),
			decision: func() ClassificationPolicyDecision {
				decision := validClassificationCommandDecision()
				decision.ShouldClassify = false
				return decision
			}(),
			errorPart: "does not require classification",
		},
		{
			name:  "invalid candidate revision",
			topic: testClassificationCommandTopic,
			input: func() CandidateEventInput {
				input := validClassificationCommandInput()
				input.CandidateRevision = 0
				return input
			}(),
			decision:  validClassificationCommandDecision(),
			errorPart: "candidate revision",
		},
		{
			name:  "insufficient epochs",
			topic: testClassificationCommandTopic,
			input: func() CandidateEventInput {
				input := validClassificationCommandInput()
				input.EligibleEpochCount = MinimumEligibleEpochCount - 1
				return input
			}(),
			decision:  validClassificationCommandDecision(),
			errorPart: "eligible epoch count",
		},
		{
			name:  "zero occurred at",
			topic: testClassificationCommandTopic,
			input: func() CandidateEventInput {
				input := validClassificationCommandInput()
				input.OccurredAt = time.Time{}
				return input
			}(),
			decision:  validClassificationCommandDecision(),
			errorPart: "occurred_at",
		},
		{
			name:  "deadline is not supported",
			topic: testClassificationCommandTopic,
			input: validClassificationCommandInput(),
			decision: func() ClassificationPolicyDecision {
				decision := validClassificationCommandDecision()
				decision.DeadlineAt = &deadline
				return decision
			}(),
			errorPart: "deadline is not supported",
		},
		{
			name:  "invalid execution mode",
			topic: testClassificationCommandTopic,
			input: validClassificationCommandInput(),
			decision: func() ClassificationPolicyDecision {
				decision := validClassificationCommandDecision()
				decision.ExecutionMode = domain.ExecutionModeUnspecified
				return decision
			}(),
			errorPart: "unsupported execution mode",
		},
		{
			name:  "invalid priority",
			topic: testClassificationCommandTopic,
			input: validClassificationCommandInput(),
			decision: func() ClassificationPolicyDecision {
				decision := validClassificationCommandDecision()
				decision.Priority = ClassificationPriorityUnspecified
				return decision
			}(),
			errorPart: "unsupported classification priority",
		},
		{
			name:     "empty header key",
			topic:    testClassificationCommandTopic,
			input:    validClassificationCommandInput(),
			decision: validClassificationCommandDecision(),
			headers: []MessageHeader{
				{
					Value: []byte("value"),
				},
			},
			errorPart: "header 0 key",
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := BuildClassificationCommandMessage(
				test.topic,
				test.input,
				test.decision,
				test.headers,
			)
			if err == nil {
				t.Fatal("expected command construction error")
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

func validClassificationCommandInput() CandidateEventInput {
	return CandidateEventInput{
		EventID:            "event-123",
		EventType:          CandidateEventTypeCreated,
		ObjectID:           "object-123",
		CandidateRevision:  7,
		LightCurveRevision: 11,
		EligibleEpochCount: MinimumEligibleEpochCount,
		OccurredAt: time.Date(
			2026,
			time.July,
			28,
			9,
			30,
			0,
			0,
			time.UTC,
		),
		Producer:                "candidate-pipeline",
		UpstreamPipelineVersion: "pipeline-v1",
		TraceContext: TraceContext{
			TraceID:       "trace-123",
			CorrelationID: "correlation-123",
			CausationID:   "causation-123",
		},
	}
}

func validClassificationCommandDecision() ClassificationPolicyDecision {
	return ClassificationPolicyDecision{
		ShouldClassify:              true,
		ModelBundleVersion:          "bundle-v1",
		ClassificationPolicyVersion: "classification-policy-v1",
		ExecutionMode:               domain.ExecutionModeProduction,
		Priority:                    ClassificationPriorityRealtime,
		DeadlineAt:                  nil,
	}
}

func buildClassificationCommandJobID(
	t *testing.T,
	input CandidateEventInput,
	decision ClassificationPolicyDecision,
) string {
	t.Helper()

	message, err := BuildClassificationCommandMessage(
		testClassificationCommandTopic,
		input,
		decision,
		nil,
	)
	if err != nil {
		t.Fatalf("BuildClassificationCommandMessage returned error: %v", err)
	}

	var command classificationv1.ClassificationCommand
	if err := proto.Unmarshal(message.Value, &command); err != nil {
		t.Fatalf("unmarshal ClassificationCommand: %v", err)
	}

	return command.GetJobId()
}

func TestBuildClassificationCommandMessagePreservesHeaderValueSemantics(t *testing.T) {
	t.Parallel()

	headers := []MessageHeader{
		{
			Key:   "nil-value",
			Value: nil,
		},
		{
			Key:   "empty-value",
			Value: []byte{},
		},
		{
			Key:   "non-empty-value",
			Value: []byte("original"),
		},
	}

	message, err := BuildClassificationCommandMessage(
		testClassificationCommandTopic,
		validClassificationCommandInput(),
		validClassificationCommandDecision(),
		headers,
	)
	if err != nil {
		t.Fatalf("BuildClassificationCommandMessage returned error: %v", err)
	}

	if message.Headers[0].Value != nil {
		t.Fatalf(
			"expected nil header value, got %#v",
			message.Headers[0].Value,
		)
	}

	if message.Headers[1].Value == nil {
		t.Fatal("expected empty non-nil header value")
	}
	if len(message.Headers[1].Value) != 0 {
		t.Fatalf(
			"expected empty header value, got %q",
			message.Headers[1].Value,
		)
	}

	headers[2].Value[0] = 'X'
	if string(message.Headers[2].Value) != "original" {
		t.Fatal("non-empty header value was not deep copied")
	}
}
