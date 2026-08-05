package application

import (
	"errors"
	"fmt"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// BuildClassificationCommandMessage 根据已校验的 CandidateEventInput 和 Policy 决策
// 构造尚未发布的 ClassificationCommand Kafka 消息
func BuildClassificationCommandMessage(commandTopic string, input CandidateEventInput, decision ClassificationPolicyDecision, headers []MessageHeader) (OutboundMessage, error) {
	// 验证参数
	if commandTopic == "" {
		return OutboundMessage{}, errors.New("classification command topic must not be empty")
	}

	if !decision.ShouldClassify {
		return OutboundMessage{}, errors.New("classification policy decision does not require classification")
	}

	switch input.EventType {
	case CandidateEventTypeCreated, CandidateEventTypeUpdated:
	default:
		return OutboundMessage{}, fmt.Errorf(
			"unsupported candidate event type: %d",
			input.EventType,
		)
	}

	if input.CandidateRevision <= 0 {
		return OutboundMessage{}, errors.New(
			"candidate revision must be greater than zero",
		)
	}

	if input.EligibleEpochCount < MinimumEligibleEpochCount {
		return OutboundMessage{}, fmt.Errorf(
			"eligible epoch count must be at least %d",
			MinimumEligibleEpochCount,
		)
	}

	if input.OccurredAt.IsZero() {
		return OutboundMessage{}, errors.New(
			"candidate occurred_at must not be zero",
		)
	}

	if decision.DeadlineAt != nil {
		return OutboundMessage{}, errors.New(
			"classification command deadline is not supported in policy v1",
		)
	}

	executionMode, err := classificationCommandExecutionMode(decision.ExecutionMode)
	if err != nil {
		return OutboundMessage{}, err
	}

	priority, err := classificationCommandPriority(decision.Priority)
	if err != nil {
		return OutboundMessage{}, err
	}

	jobID, err := domain.GenerateJobID(domain.JobIdentity{
		ObjectID:           input.ObjectID,
		LightCurveRevision: input.LightCurveRevision,
		ModelBundleVersion: decision.ModelBundleVersion,
		ExecutionMode:      decision.ExecutionMode,
	})
	if err != nil {
		return OutboundMessage{}, fmt.Errorf(
			"generate classification job ID: %w",
			err,
		)
	}
	createdAt := timestamppb.New(input.OccurredAt)
	if err := createdAt.CheckValid(); err != nil {
		return OutboundMessage{}, fmt.Errorf("invalid candidate occurred_at: %w", err)
	}
	command := &classificationv1.ClassificationCommand{
		JobId:                      string(jobID),
		ObjectId:                   input.ObjectID,
		CandidateRevision:          input.CandidateRevision,
		LightCurveRevision:         input.LightCurveRevision,
		DeclaredEligibleEpochCount: input.EligibleEpochCount,
		ModelBundleVersion:         decision.ModelBundleVersion,
		ExecutionMode:              executionMode,
		Priority:                   priority,
		CreatedAt:                  createdAt,
		DeadlineAt:                 nil,
		TraceContext:               classificationCommandTraceContext(input.TraceContext),
	}

	value, err := proto.Marshal(command)
	if err != nil {
		return OutboundMessage{}, fmt.Errorf(
			"marshal ClassificationCommand: %w",
			err,
		)
	}

	cloneHeader := cloneClassificationCommandHeaders(headers)

	return OutboundMessage{
		Topic:   commandTopic,
		Key:     []byte(input.ObjectID),
		Value:   value,
		Headers: cloneHeader,
	}, nil
}

func classificationCommandExecutionMode(mode domain.ExecutionMode) (classificationv1.ExecutionMode, error) {
	switch mode {
	case domain.ExecutionModeProduction:
		return classificationv1.ExecutionMode_EXECUTION_MODE_PRODUCTION, nil
	case domain.ExecutionModeShadow:
		return classificationv1.ExecutionMode_EXECUTION_MODE_SHADOW, nil
	case domain.ExecutionModeReprocess:
		return classificationv1.ExecutionMode_EXECUTION_MODE_REPROCESS, nil
	default:
		return classificationv1.ExecutionMode_EXECUTION_MODE_UNSPECIFIED, fmt.Errorf("unsupported execution mode: %d", mode)
	}
}

func classificationCommandPriority(priority ClassificationPriority) (classificationv1.ClassificationPriority, error) {
	switch priority {
	case ClassificationPriorityRealtime:
		return classificationv1.ClassificationPriority_CLASSIFICATION_PRIORITY_REALTIME,
			nil
	default:
		return classificationv1.ClassificationPriority_CLASSIFICATION_PRIORITY_UNSPECIFIED,
			fmt.Errorf("unsupported classification priority: %d", priority)
	}
}

func classificationCommandTraceContext(trace TraceContext) *classificationv1.TraceContext {
	if trace == (TraceContext{}) {
		return nil
	}

	return &classificationv1.TraceContext{
		TraceId:       trace.TraceID,
		CorrelationId: trace.CorrelationID,
		CausationId:   trace.CausationID,
	}
}

func cloneClassificationCommandHeaders(headers []MessageHeader) []MessageHeader {
	if len(headers) == 0 {
		return nil
	}

	cloned := make([]MessageHeader, len(headers))
	for index, header := range headers {

		cloned[index].Key = header.Key
		if header.Value != nil {
			cloned[index].Value = make([]byte, len(header.Value))
			copy(cloned[index].Value, header.Value)
		}
	}

	return cloned
}
