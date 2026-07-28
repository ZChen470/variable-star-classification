package application

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"google.golang.org/protobuf/proto"
)

// CandidateEventType 表示当前分类支持的候选事件类型
// RETRACTED 不进入内部 CandidateEventInput 而是在解码校验阶段作为永久非法事件处理
type CandidateEventType uint8

const (
	candidateEventTypeUnspecified CandidateEventType = iota
	CandidateEventTypeCreated
	CandidateEventTypeUpdated
)

// TraceContext 是跨应用逻辑传播的最小追踪上下文
type TraceContext struct {
	TraceID       string
	CorrelationID string
	CausationID   string
}

// CandidateEventInput 是通过消息解码和业务校验后的最小候选事件输入
// Kafka topic、partition、offset、record timestamp 不属于候选业务事实，因此不保存在此结构中
type CandidateEventInput struct {
	EventID                 string
	EventType               CandidateEventType
	ObjectID                string
	CandidateRevision       int64
	LightCurveRevision      int64
	EligibleEpochCount      uint32
	OccurredAt              time.Time
	Producer                string
	UpstreamPipelineVersion string
	TraceContext            TraceContext
}

// CandidateMessageErrorCode 是写入 Candidate DLQ Header 的稳定错误代码
type CandidateMessageErrorCode string

const (
	// 畸形
	CandidateMessageErrorCodeMalformedProto CandidateMessageErrorCode = "CANDIDATE_PROTO_MALFORMED"
	// Topic 错误
	CandidateMessageErrorCodeUnexpectedTopic CandidateMessageErrorCode = "CANDIDATE_TOPIC_UNEXPECTED"
	// Key 缺失
	CandidateMessageErrorCodeMissingKey CandidateMessageErrorCode = "CANDIDATE_KEY_MISSING"
	// Key 不匹配
	CandidateMessageErrorCodeKeyMismatch CandidateMessageErrorCode = "CANDIDATE_KEY_MISMATCH"
	// 事件类型错误
	CandidateMessageErrorCodeUnsupportedEventType CandidateMessageErrorCode = "CANDIDATE_EVENT_TYPE_UNSUPPORTED"
	// 非法的事件
	CandidateMessageErrorCodeInvalidEvent CandidateMessageErrorCode = "CANDIDATE_EVENT_INVALID"
)

// PermanentCandidateMessageError 表示由消息内容造成，重复处理也不会自行恢复的错误
// Handler 将识别该类型，并尝试把原始消息发布到 Candidate DLQ
type PermanentCandidateMessageError struct {
	Code  CandidateMessageErrorCode
	Field string
	Err   error
}

// Error 实现 error 接口。
func (e *PermanentCandidateMessageError) Error() string {
	if e == nil {
		return "permanent candidate message error"
	}

	switch {
	case e.Field != "" && e.Err != nil:
		return fmt.Sprintf("%s (%s): %v", e.Code, e.Field, e.Err)
	case e.Field != "":
		return fmt.Sprintf("%s (%s)", e.Code, e.Field)
	case e.Err != nil:
		return fmt.Sprintf("%s: %v", e.Code, e.Err)
	default:
		return string(e.Code)
	}
}

// Unwrap 返回底层错误，支持 errors.Is errors.As
func (e *PermanentCandidateMessageError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// DecodeCandidateEventMessage 解码并校验一条 CandidateEvent 入站消息
// 消息内容造成的永久错误使用 PermanentCandidateMessageError 返回
// expectedTopic 为空等本地配置错误返回普通 error
func DecodeCandidateEventMessage(expectedTopic string, message InboundMessage) (CandidateEventInput, error) {
	if expectedTopic == "" {
		return CandidateEventInput{}, errors.New("expected candidate topic must not be empty")
	}
	if message.Topic != expectedTopic {
		return CandidateEventInput{}, &PermanentCandidateMessageError{
			Code:  CandidateMessageErrorCodeUnexpectedTopic,
			Field: "topic",
			Err:   fmt.Errorf("got %q, want %q", message.Topic, expectedTopic),
		}
	}

	if len(message.Key) == 0 {
		return CandidateEventInput{}, &PermanentCandidateMessageError{
			Code:  CandidateMessageErrorCodeMissingKey,
			Field: "key",
			Err:   errors.New("must not be empty"),
		}
	}

	if len(message.Value) == 0 {
		return CandidateEventInput{}, &PermanentCandidateMessageError{
			Code:  CandidateMessageErrorCodeMalformedProto,
			Field: "value",
			Err:   errors.New("must not be empty"),
		}
	}

	var event classificationv1.CandidateEvent
	if err := proto.Unmarshal(message.Value, &event); err != nil {
		return CandidateEventInput{}, &PermanentCandidateMessageError{
			Code:  CandidateMessageErrorCodeMalformedProto,
			Field: "value",
			Err:   err,
		}
	}

	eventType, err := candidateEventTypeFromProto(event.GetEventType())
	if err != nil {
		return CandidateEventInput{}, err
	}

	if err := validateRequiredCandidateString("event_id", event.GetEventId()); err != nil {
		return CandidateEventInput{}, invalidCandidateField("event_id", err)
	}

	if err := validateRequiredCandidateString("object_id", event.GetObjectId()); err != nil {
		return CandidateEventInput{}, invalidCandidateField("object_id", err)
	}

	if event.GetCandidateRevision() <= 0 {
		return CandidateEventInput{}, invalidCandidateField("candidate_revision", errors.New("must be greater than zero"))
	}

	if event.GetLightCurveRevision() <= 0 {
		return CandidateEventInput{}, invalidCandidateField(
			"light_curve_revision",
			errors.New("must be greater than zero"),
		)
	}

	if event.GetEligibleEpochCount() < MinimumEligibleEpochCount {
		return CandidateEventInput{}, invalidCandidateField(
			"eligible_epoch_count",
			fmt.Errorf("must be at least %d, got %d", MinimumEligibleEpochCount, event.GetEligibleEpochCount()),
		)
	}

	if !bytes.Equal(message.Key, []byte(event.GetObjectId())) {
		return CandidateEventInput{}, &PermanentCandidateMessageError{
			Code:  CandidateMessageErrorCodeKeyMismatch,
			Field: "key",
			Err: fmt.Errorf(
				"Kafka key %q does not match object_id %q",
				string(message.Key),
				event.GetObjectId(),
			),
		}
	}

	if event.GetOccurredAt() == nil {
		return CandidateEventInput{}, invalidCandidateField(
			"occurred_at",
			errors.New("must be present"),
		)
	}

	if err := event.GetOccurredAt().CheckValid(); err != nil {
		return CandidateEventInput{}, invalidCandidateField("occurred_at", err)
	}

	if err := validateRequiredCandidateString("producer", event.GetProducer()); err != nil {
		return CandidateEventInput{}, invalidCandidateField("producer", err)
	}

	if err := validateRequiredCandidateString(
		"upstream_pipeline_version",
		event.GetUpstreamPipelineVersion(),
	); err != nil {
		return CandidateEventInput{}, invalidCandidateField(
			"upstream_pipeline_version",
			err,
		)
	}

	traceContext := TraceContext{}
	if trace := event.GetTraceContext(); trace != nil {
		traceContext = TraceContext{
			TraceID:       trace.GetTraceId(),
			CorrelationID: trace.GetCorrelationId(),
			CausationID:   trace.GetCausationId(),
		}
	}

	return CandidateEventInput{
		EventID:                 event.GetEventId(),
		EventType:               eventType,
		ObjectID:                event.GetObjectId(),
		CandidateRevision:       event.GetCandidateRevision(),
		LightCurveRevision:      event.GetLightCurveRevision(),
		EligibleEpochCount:      event.GetEligibleEpochCount(),
		OccurredAt:              event.GetOccurredAt().AsTime(),
		Producer:                event.GetProducer(),
		UpstreamPipelineVersion: event.GetUpstreamPipelineVersion(),
		TraceContext:            traceContext,
	}, nil
}

func candidateEventTypeFromProto(eventType classificationv1.CandidateEventType) (CandidateEventType, error) {
	switch eventType {
	case classificationv1.CandidateEventType_CANDIDATE_CREATED:
		return CandidateEventTypeCreated, nil
	case classificationv1.CandidateEventType_CANDIDATE_UPDATED:
		return CandidateEventTypeUpdated, nil
	default:
		return candidateEventTypeUnspecified, &PermanentCandidateMessageError{
			Code:  CandidateMessageErrorCodeUnsupportedEventType,
			Field: "event_type",
			Err:   fmt.Errorf("value %d is not supported", eventType),
		}
	}
}

func validateRequiredCandidateString(field, value string) error {
	if value == "" {
		return errors.New("must not be empty")
	}
	if strings.TrimSpace(value) != value {
		return errors.New("must not contain leading or trailing whitespace")
	}
	if strings.ContainsRune(value, '\x00') {
		return errors.New("must not contain NUL")
	}
	return nil
}

func invalidCandidateField(field string, err error) error {
	return &PermanentCandidateMessageError{
		Code:  CandidateMessageErrorCodeInvalidEvent,
		Field: field,
		Err:   err,
	}
}
