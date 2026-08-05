package application

import (
	"bytes"
	"errors"
	"fmt"
	"strings"
	"time"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"google.golang.org/protobuf/proto"
)

// ClassificationCommandErrorCode 是 Command DLQ 后续可使用的稳定错误代码
type ClassificationCommandErrorCode string

const (
	ClassificationCommandErrorCodeUnexpectedTopic ClassificationCommandErrorCode = "COMMAND_TOPIC_UNEXPECTED"
	ClassificationCommandErrorCodeMissingKey      ClassificationCommandErrorCode = "COMMAND_KEY_MISSING"
	ClassificationCommandErrorCodeMalformedProto  ClassificationCommandErrorCode = "COMMAND_PROTO_MALFORMED"
	ClassificationCommandErrorCodeKeyMismatch     ClassificationCommandErrorCode = "COMMAND_KEY_MISMATCH"
	ClassificationCommandErrorCodeInvalidField    ClassificationCommandErrorCode = "COMMAND_FIELD_INVALID"
	ClassificationCommandErrorCodeExecutionMode   ClassificationCommandErrorCode = "COMMAND_EXECUTION_MODE_UNSUPPORTED"
	ClassificationCommandErrorCodePriority        ClassificationCommandErrorCode = "COMMAND_PRIORITY_UNSUPPORTED"
	ClassificationCommandErrorCodeJobIDMismatch   ClassificationCommandErrorCode = "COMMAND_JOB_ID_MISMATCH"
	ClassificationCommandErrorCodeDeadline        ClassificationCommandErrorCode = "COMMAND_DEADLINE_UNSUPPORTED"
)

// PermanentClassificationCommandError 表示由 Command 消息内容造成、
// 重放也不会自行恢复的错误
type PermanentClassificationCommandError struct {
	Code  ClassificationCommandErrorCode
	Field string
	Err   error
}

func (e *PermanentClassificationCommandError) Error() string {
	if e == nil {
		return "permanent classification command error"
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

func (e *PermanentClassificationCommandError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// ClassificationCommandInput 是通过消息解码、字段校验和确定性身份校验后的应用输入。
// 不向后续 Worker 编排暴露 Protobuf 类型或 Kafka 位置信息
type ClassificationCommandInput struct {
	JobID                      domain.JobID
	ObjectID                   string
	CandidateRevision          int64
	LightCurveRevision         int64
	DeclaredEligibleEpochCount uint32
	ModelBundleVersion         string
	ExecutionMode              domain.ExecutionMode
	Priority                   ClassificationPriority
	CreatedAt                  time.Time
	TraceContext               TraceContext
}

// DecodeClassificationCommandMessage 解码并校验一条 ClassificationCommand
// Kafka Record Timestamp 是传输元数据，不参与任务身份，也不与 created_at 比较
func DecodeClassificationCommandMessage(expectedTopic string, message InboundMessage) (ClassificationCommandInput, error) {
	if expectedTopic == "" {
		return ClassificationCommandInput{}, errors.New("expected classification command topic must not be empty")
	}

	// 主题错误是永久错误，直接发送到死信队列
	if message.Topic != expectedTopic {
		return ClassificationCommandInput{}, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodeUnexpectedTopic,
			Field: "topic",
			Err:   fmt.Errorf("got %q, want %q", message.Topic, expectedTopic),
		}
	}

	if len(message.Key) == 0 {
		return ClassificationCommandInput{}, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodeMissingKey,
			Field: "key",
			Err:   errors.New("must not be empty"),
		}
	}

	if len(message.Value) == 0 {
		return ClassificationCommandInput{}, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodeMalformedProto,
			Field: "value",
			Err:   errors.New("must not be empty"),
		}
	}

	var command classificationv1.ClassificationCommand
	if err := proto.Unmarshal(message.Value, &command); err != nil {
		return ClassificationCommandInput{}, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodeMalformedProto,
			Field: "value",
			Err:   err,
		}
	}

	if err := validateRequiredClassificationCommandString("job_id", command.GetJobId()); err != nil {
		return ClassificationCommandInput{}, invalidClassificationCommandField("job_id", err)
	}

	if err := validateRequiredClassificationCommandString("object_id", command.GetObjectId()); err != nil {
		return ClassificationCommandInput{}, invalidClassificationCommandField("object_id", err)
	}

	if command.GetCandidateRevision() <= 0 {
		return ClassificationCommandInput{}, invalidClassificationCommandField("candidate_revision", errors.New("must be greater than zero"))
	}

	if command.GetLightCurveRevision() <= 0 {
		return ClassificationCommandInput{}, invalidClassificationCommandField("light_curve_revision", errors.New("must be greater than zero"))
	}

	if v := command.GetDeclaredEligibleEpochCount(); v < MinimumEligibleEpochCount {
		return ClassificationCommandInput{}, invalidClassificationCommandField("declared_eligible_epoch_count", fmt.Errorf("must be at least %d, got %d", MinimumEligibleEpochCount, v))
	}

	if err := validateRequiredClassificationCommandString("model_bundle_version", command.GetModelBundleVersion()); err != nil {
		return ClassificationCommandInput{}, invalidClassificationCommandField("model_bundle_version", err)
	}

	executionMode, err := classificationCommandInputExecutionMode(command.ExecutionMode)
	if err != nil {
		return ClassificationCommandInput{}, err
	}

	priority, err := classificationCommandInputPriority(command.GetPriority())
	if err != nil {
		return ClassificationCommandInput{}, err
	}

	if command.GetCreatedAt() == nil {
		return ClassificationCommandInput{},
			invalidClassificationCommandField("created_at", errors.New("must be present"))
	}
	if err := command.GetCreatedAt().CheckValid(); err != nil {
		return ClassificationCommandInput{}, invalidClassificationCommandField("created_at", err)
	}

	createdAt := command.GetCreatedAt().AsTime()
	if createdAt.IsZero() {
		return ClassificationCommandInput{}, invalidClassificationCommandField("created_at", errors.New("must not be zero"))
	}

	if command.GetDeadlineAt() != nil {
		return ClassificationCommandInput{},
			&PermanentClassificationCommandError{
				Code:  ClassificationCommandErrorCodeDeadline,
				Field: "deadline_at",
				Err:   errors.New("is not supported"),
			}
	}

	if !bytes.Equal(message.Key, []byte(command.GetObjectId())) {
		return ClassificationCommandInput{}, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodeKeyMismatch,
			Field: "key",
			Err:   fmt.Errorf("kafka key %q does not match object_id %q", string(message.Key), command.GetObjectId()),
		}
	}

	expectedJobID, err := domain.GenerateJobID(domain.JobIdentity{
		ObjectID:           command.GetObjectId(),
		LightCurveRevision: command.GetLightCurveRevision(),
		ModelBundleVersion: command.GetModelBundleVersion(),
		ExecutionMode:      executionMode,
	})
	if err != nil {
		return ClassificationCommandInput{}, invalidClassificationCommandField("identity", err)
	}

	if command.GetJobId() != string(expectedJobID) {
		return ClassificationCommandInput{}, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodeJobIDMismatch,
			Field: "job_id",
			Err:   fmt.Errorf("got %q, want deterministic value %q", command.GetJobId(), expectedJobID),
		}
	}

	traceContext := TraceContext{}
	if trace := command.GetTraceContext(); trace != nil {
		traceContext = TraceContext{
			TraceID:       trace.GetTraceId(),
			CorrelationID: trace.GetCorrelationId(),
			CausationID:   trace.GetCausationId(),
		}
	}

	return ClassificationCommandInput{
		JobID:                      expectedJobID,
		ObjectID:                   command.ObjectId,
		CandidateRevision:          command.GetCandidateRevision(),
		LightCurveRevision:         command.GetLightCurveRevision(),
		DeclaredEligibleEpochCount: command.GetDeclaredEligibleEpochCount(),
		ModelBundleVersion:         command.GetModelBundleVersion(),
		ExecutionMode:              executionMode,
		Priority:                   priority,
		CreatedAt:                  createdAt,
		TraceContext:               traceContext,
	}, nil
}

func invalidClassificationCommandField(field string, err error) error {
	return &PermanentClassificationCommandError{
		Code:  ClassificationCommandErrorCodeInvalidField,
		Field: field,
		Err:   err,
	}
}

func classificationCommandInputExecutionMode(mode classificationv1.ExecutionMode) (domain.ExecutionMode, error) {
	switch mode {
	case classificationv1.ExecutionMode_EXECUTION_MODE_PRODUCTION:
		return domain.ExecutionModeProduction, nil
	case classificationv1.ExecutionMode_EXECUTION_MODE_SHADOW:
		return domain.ExecutionModeShadow, nil
	case classificationv1.ExecutionMode_EXECUTION_MODE_REPROCESS:
		return domain.ExecutionModeReprocess, nil
	default:
		return domain.ExecutionModeUnspecified, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodeExecutionMode,
			Field: "execution_mode",
			Err:   fmt.Errorf("value %d is not supported", mode),
		}
	}
}

func classificationCommandInputPriority(priority classificationv1.ClassificationPriority) (ClassificationPriority, error) {
	switch priority {
	case classificationv1.ClassificationPriority_CLASSIFICATION_PRIORITY_REALTIME:
		return ClassificationPriorityRealtime, nil
	default:
		return ClassificationPriorityUnspecified, &PermanentClassificationCommandError{
			Code:  ClassificationCommandErrorCodePriority,
			Field: "priority",
			Err:   fmt.Errorf("value %d is not supported", priority),
		}
	}
}

func validateRequiredClassificationCommandString(field string, value string) error {
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
