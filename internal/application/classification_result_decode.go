package application

import (
	"bytes"
	"errors"
	"fmt"

	classificationv1 "github.com/ZChen470/variable-star-classification/gen/go/astro/classification/v1"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

// ClassificationResultErrorCode 是 Result DLQ 后续使用的稳定错误代码。
type ClassificationResultErrorCode string

const (
	ClassificationResultErrorCodeUnexpectedTopic ClassificationResultErrorCode = "RESULT_TOPIC_UNEXPECTED"

	ClassificationResultErrorCodeMalformedMessage ClassificationResultErrorCode = "RESULT_MESSAGE_MALFORMED"

	ClassificationResultErrorCodeInvalidField ClassificationResultErrorCode = "RESULT_FIELD_INVALID"

	ClassificationResultErrorCodeKeyMismatch ClassificationResultErrorCode = "RESULT_KEY_MISMATCH"

	ClassificationResultErrorCodeJobIDMismatch ClassificationResultErrorCode = "RESULT_JOB_ID_MISMATCH"

	ClassificationResultErrorCodeRunIDMismatch ClassificationResultErrorCode = "RESULT_RUN_ID_MISMATCH"
)

// PermanentClassificationResultError 表示消息内容造成的永久错误。
// 重放同一条 Result 不会自行恢复。
type PermanentClassificationResultError struct {
	Code  ClassificationResultErrorCode
	Field string
	Cause error
}

func (resultError *PermanentClassificationResultError) Error() string {
	if resultError == nil {
		return "nil permanent classification result error"
	}

	if resultError.Cause == nil {
		return fmt.Sprintf(
			"%s (%s)",
			resultError.Code,
			resultError.Field,
		)
	}

	return fmt.Sprintf(
		"%s (%s): %v",
		resultError.Code,
		resultError.Field,
		resultError.Cause,
	)
}

func (resultError *PermanentClassificationResultError) Unwrap() error {
	if resultError == nil {
		return nil
	}

	return resultError.Cause
}

// ClassificationResultInput 是经过 Result 消费边界校验后的 Writer 输入。
type ClassificationResultInput struct {
	Run          domain.ClassificationRun
	TraceContext TraceContext
}

// DecodeClassificationResultMessage 解码并验证一条成功 ClassificationResult。
//
// 本函数只验证：
//   - Kafka Topic、Key、Value；
//   - Proto 必需字段；
//   - 确定性 job_id、run_id；
//   - Run 的最小来源关系；
//   - predicted class 与现有确定性 argmax 一致。
//
// 本函数不验证概率范围、概率和、融合公式或 REUSE 概率一致性。
func DecodeClassificationResultMessage(
	expectedTopic string,
	message InboundMessage,
) (ClassificationResultInput, error) {
	if expectedTopic == "" {
		return ClassificationResultInput{}, errors.New(
			"expected classification result topic must not be empty",
		)
	}

	if message.Topic != expectedTopic {
		return ClassificationResultInput{}, newPermanentResultError(
			ClassificationResultErrorCodeUnexpectedTopic,
			"topic",
			fmt.Errorf(
				"got %q, want %q",
				message.Topic,
				expectedTopic,
			),
		)
	}

	if len(message.Key) == 0 {
		return ClassificationResultInput{}, invalidResultField(
			"key",
			errors.New("must not be empty"),
		)
	}

	if len(message.Value) == 0 {
		return ClassificationResultInput{}, newPermanentResultError(
			ClassificationResultErrorCodeMalformedMessage,
			"value",
			errors.New("must not be empty"),
		)
	}

	var result classificationv1.ClassificationResult

	if err := proto.Unmarshal(message.Value, &result); err != nil {
		return ClassificationResultInput{}, newPermanentResultError(
			ClassificationResultErrorCodeMalformedMessage,
			"value",
			err,
		)
	}

	run, traceContext, err := mapClassificationResult(&result)
	if err != nil {
		return ClassificationResultInput{}, err
	}

	if !bytes.Equal(
		message.Key,
		[]byte(run.ObjectID),
	) {
		return ClassificationResultInput{}, newPermanentResultError(
			ClassificationResultErrorCodeKeyMismatch,
			"key",
			fmt.Errorf(
				"kafka key %q does not match object_id %q",
				string(message.Key),
				run.ObjectID,
			),
		)
	}

	expectedJobID, err := domain.GenerateJobID(
		domain.JobIdentity{
			ObjectID:           run.ObjectID,
			LightCurveRevision: run.LightCurveRevision,
			ModelBundleVersion: run.Versions.ModelBundleVersion,
			ExecutionMode:      run.ExecutionMode,
		},
	)
	if err != nil {
		return ClassificationResultInput{}, invalidResultField(
			"job_identity",
			err,
		)
	}

	if run.JobID != expectedJobID {
		return ClassificationResultInput{}, newPermanentResultError(
			ClassificationResultErrorCodeJobIDMismatch,
			"job_id",
			fmt.Errorf(
				"got %q, want %q",
				run.JobID,
				expectedJobID,
			),
		)
	}

	expectedRunID, err := domain.GenerateRunID(expectedJobID)
	if err != nil {
		return ClassificationResultInput{}, invalidResultField(
			"run_id",
			err,
		)
	}

	if run.RunID != expectedRunID {
		return ClassificationResultInput{}, newPermanentResultError(
			ClassificationResultErrorCodeRunIDMismatch,
			"run_id",
			fmt.Errorf(
				"got %q, want %q",
				run.RunID,
				expectedRunID,
			),
		)
	}

	if err := validateDecodedClassificationRun(run); err != nil {
		return ClassificationResultInput{}, err
	}

	return ClassificationResultInput{
		Run:          run,
		TraceContext: traceContext,
	}, nil
}

func mapClassificationResult(
	result *classificationv1.ClassificationResult,
) (
	domain.ClassificationRun,
	TraceContext,
	error,
) {
	if result == nil {
		return domain.ClassificationRun{},
			TraceContext{},
			invalidResultField(
				"result",
				errors.New("must not be nil"),
			)
	}

	if result.GetVersions() == nil {
		return domain.ClassificationRun{},
			TraceContext{},
			invalidResultField(
				"versions",
				errors.New("must be present"),
			)
	}

	if result.GetCoarseProbabilities() == nil {
		return domain.ClassificationRun{},
			TraceContext{},
			invalidResultField(
				"coarse_probabilities",
				errors.New("must be present"),
			)
	}

	if result.GetFineConditionalProbabilities() == nil {
		return domain.ClassificationRun{},
			TraceContext{},
			invalidResultField(
				"fine_conditional_probabilities",
				errors.New("must be present"),
			)
	}

	if result.GetLeafProbabilities() == nil {
		return domain.ClassificationRun{},
			TraceContext{},
			invalidResultField(
				"leaf_probabilities",
				errors.New("must be present"),
			)
	}

	if result.GetCompletedAt() == nil {
		return domain.ClassificationRun{},
			TraceContext{},
			invalidResultField(
				"completed_at",
				errors.New("must be present"),
			)
	}

	if err := result.GetCompletedAt().CheckValid(); err != nil {
		return domain.ClassificationRun{},
			TraceContext{},
			invalidResultField(
				"completed_at",
				err,
			)
	}

	sourceRunID, err := decodeClassificationResultSourceRunID(
		result.CoarseSourceRunId,
	)
	if err != nil {
		return domain.ClassificationRun{}, TraceContext{}, err
	}

	run := domain.ClassificationRun{
		RunID:    domain.RunID(result.GetRunId()),
		JobID:    domain.JobID(result.GetJobId()),
		ObjectID: result.GetObjectId(),

		CandidateRevision: result.GetCandidateRevision(),

		LightCurveRevision: result.GetLightCurveRevision(),

		EffectiveEpochCount: result.GetEffectiveEpochCount(),

		ExecutionMode: domain.ExecutionMode(result.GetExecutionMode()),

		CoarseSourceType: domain.CoarseSourceType(
			result.GetCoarseSourceType(),
		),

		CoarseSourceRunID: sourceRunID,

		CoarseSourceLightCurveRevision: result.GetCoarseSourceLightCurveRevision(),

		CoarseSourceEpochCount: result.GetCoarseSourceEpochCount(),

		XGBoostExecuted: result.GetXgboostExecuted(),

		Versions: domain.ResolvedModelVersions{
			ModelBundleVersion: result.GetVersions().GetModelBundleVersion(),
		},

		CoarseProbabilities: decodeResultCoarseProbabilities(
			result.GetCoarseProbabilities(),
		),

		FineConditionalProbabilities: decodeResultFineProbabilities(
			result.GetFineConditionalProbabilities(),
		),

		LeafProbabilities: decodeResultLeafProbabilities(
			result.GetLeafProbabilities(),
		),

		PredictedCoarseClass: domain.CoarseClass(
			result.GetPredictedCoarseClass(),
		),

		PredictedLeafClass: domain.LeafClass(
			result.GetPredictedLeafClass(),
		),

		CompletedAt: result.GetCompletedAt().AsTime().UTC(),
	}

	traceContext := TraceContext{}

	if trace := result.GetTraceContext(); trace != nil {
		traceContext = TraceContext{
			TraceID:       trace.GetTraceId(),
			CorrelationID: trace.GetCorrelationId(),
			CausationID:   trace.GetCausationId(),
		}
	}

	return run, traceContext, nil
}

func validateDecodedClassificationRun(
	run domain.ClassificationRun,
) error {
	switch {
	case run.ObjectID == "":
		return invalidResultField(
			"object_id",
			errors.New("must not be empty"),
		)

	case run.CandidateRevision <= 0:
		return invalidResultField(
			"candidate_revision",
			errors.New("must be greater than zero"),
		)

	case run.LightCurveRevision <= 0:
		return invalidResultField(
			"light_curve_revision",
			errors.New("must be greater than zero"),
		)

	case run.EffectiveEpochCount < minimumLightCurveEpochCount ||
		run.EffectiveEpochCount > maximumLightCurveEpochCount:
		return invalidResultField(
			"effective_epoch_count",
			fmt.Errorf(
				"must be within %d..%d",
				minimumLightCurveEpochCount,
				maximumLightCurveEpochCount,
			),
		)

	case !isValidRunExecutionMode(run.ExecutionMode):
		return invalidResultField(
			"execution_mode",
			fmt.Errorf(
				"unsupported value %d",
				run.ExecutionMode,
			),
		)

	case !run.CoarseSourceType.IsValid():
		return invalidResultField(
			"coarse_source_type",
			fmt.Errorf(
				"unsupported value %d",
				run.CoarseSourceType,
			),
		)

	case run.Versions.ModelBundleVersion == "":
		return invalidResultField(
			"model_bundle_version",
			errors.New("must not be empty"),
		)

	case !run.PredictedCoarseClass.IsValid():
		return invalidResultField(
			"predicted_coarse_class",
			fmt.Errorf(
				"unsupported value %d",
				run.PredictedCoarseClass,
			),
		)

	case !run.PredictedLeafClass.IsValid():
		return invalidResultField(
			"predicted_leaf_class",
			fmt.Errorf(
				"unsupported value %d",
				run.PredictedLeafClass,
			),
		)

	case run.CompletedAt.IsZero():
		return invalidResultField(
			"completed_at",
			errors.New("must not be zero"),
		)
	}

	if run.PredictedCoarseClass !=
		predictedCoarseClass(run.CoarseProbabilities) {
		return invalidResultField(
			"predicted_coarse_class",
			errors.New(
				"does not match deterministic argmax",
			),
		)
	}

	if run.PredictedLeafClass !=
		predictedLeafClass(run.LeafProbabilities) {
		return invalidResultField(
			"predicted_leaf_class",
			errors.New(
				"does not match deterministic argmax",
			),
		)
	}

	return validateDecodedCoarseSource(run)
}

func validateDecodedCoarseSource(
	run domain.ClassificationRun,
) error {
	invalid := func(cause error) error {
		return invalidResultField(
			"coarse_source",
			cause,
		)
	}

	switch run.CoarseSourceType {
	case domain.CoarseSourceComputedCurrent:
		if run.EffectiveEpochCount >
			maximumComputeCurrentEpochCount {
			return invalid(
				errors.New(
					"COMPUTED_CURRENT requires at most 20 epochs",
				),
			)
		}

		if run.CoarseSourceRunID != nil ||
			run.CoarseSourceLightCurveRevision !=
				run.LightCurveRevision ||
			run.CoarseSourceEpochCount !=
				run.EffectiveEpochCount ||
			!run.XGBoostExecuted {
			return invalid(
				errors.New(
					"invalid COMPUTED_CURRENT source relationship",
				),
			)
		}

	case domain.CoarseSourceReusedPrevious:
		if run.EffectiveEpochCount <=
			maximumComputeCurrentEpochCount {
			return invalid(
				errors.New(
					"REUSED_PREVIOUS requires more than 20 epochs",
				),
			)
		}

		if run.CoarseSourceRunID == nil ||
			run.CoarseSourceLightCurveRevision <= 0 ||
			run.CoarseSourceLightCurveRevision >=
				run.LightCurveRevision ||
			run.CoarseSourceEpochCount <
				minimumLightCurveEpochCount ||
			run.CoarseSourceEpochCount >
				maximumLightCurveEpochCount ||
			run.XGBoostExecuted {
			return invalid(
				errors.New(
					"invalid REUSED_PREVIOUS source relationship",
				),
			)
		}

	case domain.CoarseSourceComputedBootstrap:
		if run.EffectiveEpochCount <=
			maximumComputeCurrentEpochCount {
			return invalid(
				errors.New(
					"COMPUTED_BOOTSTRAP requires more than 20 epochs",
				),
			)
		}

		if run.CoarseSourceRunID != nil ||
			run.CoarseSourceLightCurveRevision !=
				run.LightCurveRevision ||
			run.CoarseSourceEpochCount !=
				run.EffectiveEpochCount ||
			!run.XGBoostExecuted {
			return invalid(
				errors.New(
					"invalid COMPUTED_BOOTSTRAP source relationship",
				),
			)
		}
	}

	return nil
}

func decodeClassificationResultSourceRunID(
	value *string,
) (*domain.RunID, error) {
	if value == nil {
		return nil, nil
	}

	parsed, err := uuid.Parse(*value)
	if err != nil ||
		parsed == uuid.Nil ||
		parsed.String() != *value {
		return nil, invalidResultField(
			"coarse_source_run_id",
			errors.New(
				"must be a canonical non-nil UUID",
			),
		)
	}

	runID := domain.RunID(*value)
	return &runID, nil
}

func decodeResultCoarseProbabilities(
	probabilities *classificationv1.CoarseProbabilities,
) [domain.CoarseProbabilityCount]float32 {
	return [domain.CoarseProbabilityCount]float32{
		probabilities.GetRotating(),
		probabilities.GetCataclysmic(),
		probabilities.GetEclipsingBinary(),
		probabilities.GetLongPeriod(),
		probabilities.GetPulsating(),
		probabilities.GetRrLyrae(),
		probabilities.GetSupernova(),
	}
}

func decodeResultFineProbabilities(
	probabilities *classificationv1.FineConditionalProbabilities,
) [domain.ConditionalFineProbabilityCount]float32 {
	return [domain.ConditionalFineProbabilityCount]float32{
		probabilities.GetEwGivenEclipsingBinary(),
		probabilities.GetEaGivenEclipsingBinary(),
		probabilities.GetByDraGivenRotating(),
		probabilities.GetRsCvnGivenRotating(),
		probabilities.GetRrabGivenRrLyrae(),
		probabilities.GetRrcGivenRrLyrae(),
		probabilities.GetSrGivenLongPeriod(),
		probabilities.GetMiraGivenLongPeriod(),
		probabilities.GetDsctGivenPulsating(),
		probabilities.GetCepGivenPulsating(),
	}
}

func decodeResultLeafProbabilities(
	probabilities *classificationv1.LeafProbabilities,
) [domain.LeafProbabilityCount]float32 {
	return [domain.LeafProbabilityCount]float32{
		probabilities.GetEw(),
		probabilities.GetEa(),
		probabilities.GetByDra(),
		probabilities.GetRsCvn(),
		probabilities.GetRrab(),
		probabilities.GetRrc(),
		probabilities.GetSr(),
		probabilities.GetMira(),
		probabilities.GetDsct(),
		probabilities.GetCep(),
		probabilities.GetCataclysmic(),
		probabilities.GetSupernova(),
	}
}

func invalidResultField(
	field string,
	cause error,
) error {
	return newPermanentResultError(
		ClassificationResultErrorCodeInvalidField,
		field,
		cause,
	)
}

func newPermanentResultError(
	code ClassificationResultErrorCode,
	field string,
	cause error,
) error {
	return &PermanentClassificationResultError{
		Code:  code,
		Field: field,
		Cause: cause,
	}
}
