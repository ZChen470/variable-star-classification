package application

import (
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"strings"
	"time"
)

var (
	// ErrInvalidClassificationRunBuild 表示成功 Run 构造所需的身份、
	// 固定 revision、Bundle 或完成时间不完整或互相不一致。
	ErrInvalidClassificationRunBuild = errors.New(
		"invalid classification run build",
	)

	// ErrInvalidCoarseSourceMapping 表示 CoarseMode、历史粗分类来源
	// 或 XGBoostExecuted 不能映射为合法 ClassificationRun 来源。
	ErrInvalidCoarseSourceMapping = errors.New(
		"invalid coarse source mapping",
	)
)

// ClassificationRunBuildRequest 是构造一次成功 ClassificationRun
// 所需的最小应用层输入
//
// Job 身份完整性由 ClassificationCommand 解码边界负责。
// 本构造器只要求 JobID 是合法 UUID，并据此确定性生成 RunID。
type ClassificationRunBuildRequest struct {
	JobID             domain.JobID
	CandidateRevision int64
	ExecutionMode     domain.ExecutionMode

	Prepared      PreparedClassificationInput
	ServingBundle ServingBundleMetadata
	Output        ClassificationOutput

	// CompletedAt 由调用方注入
	CompletedAt time.Time
}

// BuildClassificationRun 将一次完整成功的输入准备和模型输出
// 构造成不可变 ClassificationRun。
//
// 本函数不执行概率范围、概率和、融合公式或 REUSE 概率一致性校验。
// 这些属于阶段 5 已冻结的模型与 Serving Contract 边界。
func BuildClassificationRun(request ClassificationRunBuildRequest) (domain.ClassificationRun, error) {
	runID, err := domain.GenerateRunID(request.JobID)
	if err != nil {
		return domain.ClassificationRun{}, fmt.Errorf(
			"%w: generate run ID: %v",
			ErrInvalidClassificationRunBuild,
			err,
		)
	}

	if request.CandidateRevision <= 0 {
		return domain.ClassificationRun{}, fmt.Errorf(
			"%w: candidate revision=%d must be greater than zero",
			ErrInvalidClassificationRunBuild,
			request.CandidateRevision,
		)
	}

	if !isValidRunExecutionMode(request.ExecutionMode) {
		return domain.ClassificationRun{}, fmt.Errorf(
			"%w: invalid execution mode=%d",
			ErrInvalidClassificationRunBuild,
			request.ExecutionMode,
		)
	}

	if request.CompletedAt.IsZero() {
		return domain.ClassificationRun{}, fmt.Errorf(
			"%w: completed_at must not be zero",
			ErrInvalidClassificationRunBuild,
		)
	}

	effectiveEpochCount, err := validatePreparedClassificationForRun(request.Prepared)
	if err != nil {
		return domain.ClassificationRun{}, err
	}

	if err := validateServingBundleForRun(
		request.Prepared.Selection.ModelBundle,
		request.ServingBundle,
	); err != nil {
		return domain.ClassificationRun{}, err
	}

	coarseSource, err := mapClassificationCoarseSource(
		request.Prepared,
		request.Output.XGBoostExecuted,
		effectiveEpochCount,
	)
	if err != nil {
		return domain.ClassificationRun{}, err
	}

	return domain.ClassificationRun{
		RunID:    runID,
		JobID:    request.JobID,
		ObjectID: request.Prepared.Revision.ObjectID,

		CandidateRevision:   request.CandidateRevision,
		LightCurveRevision:  request.Prepared.Revision.Revision,
		EffectiveEpochCount: effectiveEpochCount,
		ExecutionMode:       request.ExecutionMode,

		CoarseSourceType:               coarseSource.sourceType,
		CoarseSourceRunID:              coarseSource.sourceRunID,
		CoarseSourceLightCurveRevision: coarseSource.lightCurveRevision,
		CoarseSourceEpochCount:         coarseSource.epochCount,
		XGBoostExecuted:                request.Output.XGBoostExecuted,

		Versions: domain.ResolvedModelVersions{
			ModelBundleVersion: request.ServingBundle.ModelBundleVersion,
		},
		CoarseProbabilities:          request.Output.CoarseProbabilities,
		FineConditionalProbabilities: request.Output.ConditionalFineProbabilities,
		LeafProbabilities:            request.Output.LeafProbabilities,
		PredictedCoarseClass:         predictedCoarseClass(request.Output.CoarseProbabilities),
		PredictedLeafClass:           predictedLeafClass(request.Output.LeafProbabilities),
		CompletedAt:                  request.CompletedAt.UTC(),
	}, nil
}

type classificationCoarseSource struct {
	sourceType         domain.CoarseSourceType
	sourceRunID        *domain.RunID
	lightCurveRevision int64
	epochCount         uint32
}

func mapClassificationCoarseSource(prepared PreparedClassificationInput, xgboostExecuted bool, effectiveEpochCount uint32) (classificationCoarseSource, error) {
	currentRevision := prepared.Revision.Revision

	switch prepared.Selection.Mode {
	case CoarseModeComputeCurrent:
		if effectiveEpochCount > maximumComputeCurrentEpochCount {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_CURRENT epoch count=%d exceeds maximum=%d",
				ErrInvalidCoarseSourceMapping,
				effectiveEpochCount,
				maximumComputeCurrentEpochCount,
			)
		}
		if prepared.Selection.ReusedCoarse != nil {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_CURRENT must not contain reused source",
				ErrInvalidCoarseSourceMapping,
			)
		}
		if prepared.Input.ReusedCoarseProbabilities != nil {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_CURRENT input must not contain reused probabilities",
				ErrInvalidCoarseSourceMapping,
			)
		}
		if !xgboostExecuted {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_CURRENT requires xgboost_executed=true",
				ErrInvalidCoarseSourceMapping,
			)
		}

		return classificationCoarseSource{
			sourceType:         domain.CoarseSourceComputedCurrent,
			lightCurveRevision: currentRevision,
			epochCount:         effectiveEpochCount,
		}, nil

	case CoarseModeReusePrevious:
		if effectiveEpochCount <= maximumComputeCurrentEpochCount {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: REUSE_PREVIOUS epoch count=%d must exceed %d",
				ErrInvalidCoarseSourceMapping,
				effectiveEpochCount,
				maximumComputeCurrentEpochCount,
			)
		}
		if prepared.Selection.ReusedCoarse == nil {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: REUSE_PREVIOUS requires historical source",
				ErrInvalidCoarseSourceMapping,
			)
		}
		if prepared.Input.ReusedCoarseProbabilities == nil {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: REUSE_PREVIOUS input requires reused probabilities",
				ErrInvalidCoarseSourceMapping,
			)
		}
		if xgboostExecuted {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: REUSE_PREVIOUS requires xgboost_executed=false",
				ErrInvalidCoarseSourceMapping,
			)
		}

		reused := prepared.Selection.ReusedCoarse
		if reused.SourceRunID == "" {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: REUSE_PREVIOUS source run ID must not be empty",
				ErrInvalidCoarseSourceMapping,
			)
		}
		if reused.SourceLightCurveRevision <= 0 ||
			reused.SourceLightCurveRevision >= currentRevision {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: REUSE_PREVIOUS source revision=%d must be within 1..%d",
				ErrInvalidCoarseSourceMapping,
				reused.SourceLightCurveRevision,
				currentRevision-1,
			)
		}
		if reused.SourceEpochCount < minimumLightCurveEpochCount ||
			reused.SourceEpochCount > maximumLightCurveEpochCount {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: REUSE_PREVIOUS source epoch count=%d must be within %d..%d",
				ErrInvalidCoarseSourceMapping,
				reused.SourceEpochCount,
				minimumLightCurveEpochCount,
				maximumLightCurveEpochCount,
			)
		}

		sourceRunID := reused.SourceRunID
		return classificationCoarseSource{
			sourceType:         domain.CoarseSourceReusedPrevious,
			sourceRunID:        &sourceRunID,
			lightCurveRevision: reused.SourceLightCurveRevision,
			epochCount:         reused.SourceEpochCount,
		}, nil

	case CoarseModeComputeBootstrap:
		if effectiveEpochCount <= maximumComputeCurrentEpochCount {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_BOOTSTRAP epoch count=%d must exceed %d",
				ErrInvalidCoarseSourceMapping,
				effectiveEpochCount,
				maximumComputeCurrentEpochCount,
			)
		}
		if prepared.Selection.ReusedCoarse != nil {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_BOOTSTRAP must not contain reused source",
				ErrInvalidCoarseSourceMapping,
			)
		}
		if prepared.Input.ReusedCoarseProbabilities != nil {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_BOOTSTRAP input must not contain reused probabilities",
				ErrInvalidCoarseSourceMapping,
			)
		}
		if !xgboostExecuted {
			return classificationCoarseSource{}, fmt.Errorf(
				"%w: COMPUTE_BOOTSTRAP requires xgboost_executed=true",
				ErrInvalidCoarseSourceMapping,
			)
		}

		return classificationCoarseSource{
			sourceType:         domain.CoarseSourceComputedBootstrap,
			lightCurveRevision: currentRevision,
			epochCount:         effectiveEpochCount,
		}, nil

	default:
		return classificationCoarseSource{}, fmt.Errorf(
			"%w: unknown coarse mode=%d",
			ErrInvalidCoarseSourceMapping,
			prepared.Selection.Mode,
		)
	}
}

func validatePreparedClassificationForRun(prepared PreparedClassificationInput) (uint32, error) {
	if err := validateRunBuildString("object ID", prepared.Revision.ObjectID); err != nil {
		return 0, err
	}

	if prepared.Revision.Revision <= 0 {
		return 0, fmt.Errorf(
			"%w: light curve revision=%d must be greater than zero",
			ErrInvalidClassificationRunBuild,
			prepared.Revision.Revision,
		)
	}

	actualEpochCount := len(prepared.Revision.Epochs)
	if actualEpochCount < minimumLightCurveEpochCount ||
		actualEpochCount > maximumLightCurveEpochCount {
		return 0, fmt.Errorf(
			"%w: effective epoch count=%d must be within %d..%d",
			ErrInvalidClassificationRunBuild,
			actualEpochCount,
			minimumLightCurveEpochCount,
			maximumLightCurveEpochCount,
		)
	}

	effectiveEpochCount := uint32(actualEpochCount)
	if prepared.Revision.EligibleEpochCount != effectiveEpochCount {
		return 0, fmt.Errorf(
			"%w: revision epoch count metadata=%d actual=%d",
			ErrInvalidClassificationRunBuild,
			prepared.Revision.EligibleEpochCount,
			effectiveEpochCount,
		)
	}

	if len(prepared.Input.TimeMJD) != actualEpochCount ||
		len(prepared.Input.Magnitude) != actualEpochCount ||
		len(prepared.Input.MagnitudeError) != actualEpochCount {
		return 0, fmt.Errorf(
			"%w: classification input lengths time=%d magnitude=%d error=%d actual=%d",
			ErrInvalidClassificationRunBuild,
			len(prepared.Input.TimeMJD),
			len(prepared.Input.Magnitude),
			len(prepared.Input.MagnitudeError),
			actualEpochCount,
		)
	}

	if prepared.Input.CoarseMode != prepared.Selection.Mode {
		return 0, fmt.Errorf(
			"%w: input coarse mode=%d selection mode=%d",
			ErrInvalidClassificationRunBuild,
			prepared.Input.CoarseMode,
			prepared.Selection.Mode,
		)
	}

	return effectiveEpochCount, nil
}

func validateServingBundleForRun(modelBundle ModelBundleMetadata, servingBundle ServingBundleMetadata) error {
	if err := validateRunBuildString("model bundle version", modelBundle.ModelBundleVersion); err != nil {
		return err
	}

	if servingBundle.ModelBundleVersion != modelBundle.ModelBundleVersion {
		return fmt.Errorf(
			"%w: prepared model_bundle_version=%q serving model_bundle_version=%q",
			ErrInvalidClassificationRunBuild,
			modelBundle.ModelBundleVersion,
			servingBundle.ModelBundleVersion,
		)
	}

	requiredVersions := []struct {
		name  string
		value string
	}{
		{
			name:  "model bundle version",
			value: servingBundle.ModelBundleVersion,
		},
	}

	for _, version := range requiredVersions {
		if err := validateRunBuildString(version.name, version.value); err != nil {
			return err
		}
	}

	return nil
}

func validateRunBuildString(fieldName, value string) error {
	switch {
	case value == "":
		return fmt.Errorf(
			"%w: %s must not be empty",
			ErrInvalidClassificationRunBuild,
			fieldName,
		)
	case strings.TrimSpace(value) != value:
		return fmt.Errorf(
			"%w: %s must not contain leading or trailing whitespace",
			ErrInvalidClassificationRunBuild,
			fieldName,
		)
	case strings.ContainsRune(value, '\x00'):
		return fmt.Errorf(
			"%w: %s must not contain NUL",
			ErrInvalidClassificationRunBuild,
			fieldName,
		)
	default:
		return nil
	}
}

func isValidRunExecutionMode(mode domain.ExecutionMode) bool {
	switch mode {
	case domain.ExecutionModeProduction,
		domain.ExecutionModeShadow,
		domain.ExecutionModeReprocess:
		return true
	default:
		return false
	}
}

// Probability index 与稳定 Enum ID 不相同，必须显式映射。
var coarseClassByProbabilityIndex = [domain.CoarseProbabilityCount]domain.CoarseClass{
	domain.CoarseClassRotating,
	domain.CoarseClassCataclysmic,
	domain.CoarseClassEclipsingBinary,
	domain.CoarseClassLongPeriod,
	domain.CoarseClassPulsating,
	domain.CoarseClassRRLyrae,
	domain.CoarseClassSupernova,
}

// Probability index 与稳定 Enum ID 不相同，必须显式映射。
var leafClassByProbabilityIndex = [domain.LeafProbabilityCount]domain.LeafClass{
	domain.LeafClassEW,
	domain.LeafClassEA,
	domain.LeafClassByDra,
	domain.LeafClassRSCvn,
	domain.LeafClassRRAB,
	domain.LeafClassRRC,
	domain.LeafClassSR,
	domain.LeafClassMira,
	domain.LeafClassDSCT,
	domain.LeafClassCEP,
	domain.LeafClassCataclysmic,
	domain.LeafClassSupernova,
}

func predictedCoarseClass(probabilities [domain.CoarseProbabilityCount]float32) domain.CoarseClass {
	index := firstMaximumIndex(probabilities[:])
	return coarseClassByProbabilityIndex[index]
}

func predictedLeafClass(probabilities [domain.LeafProbabilityCount]float32) domain.LeafClass {
	index := firstMaximumIndex(probabilities[:])
	return leafClassByProbabilityIndex[index]
}

func firstMaximumIndex(values []float32) int {
	maximumIndex := 0

	for index := 1; index < len(values); index++ {
		if values[index] > values[maximumIndex] {
			maximumIndex = index
		}
	}

	return maximumIndex
}
