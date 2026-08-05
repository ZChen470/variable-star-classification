package application

import (
	"context"
	"errors"
	"fmt"
)

const maximumComputeCurrentEpochCount uint32 = 20

var (
	// ErrInvalidModelBundleMetadata 表示 Resolver 返回的 Bundle 身份
	// 或阶段 4 所需兼容性版本不合法
	ErrInvalidModelBundleMetadata = errors.New("invalid model bundle metadata")

	// ErrInvalidCompatibleCoarseResult 表示历史查询返回的粗分类来源
	// 不满足固定 revision 和 epoch 数约束。
	ErrInvalidCompatibleCoarseResult = errors.New("invalid compatible coarse result")
)

// CompatibleCoarseFinder 粗分类模式选择的最小历史查询边界
//
// ClassificationRepository 已实现该接口：选择器不依赖保存 Run、
// 查询 Current 等无关能力
type CompatibleCoarseFinder interface {
	FindLatestCompatibleCoarse(ctx context.Context, query CompatibleCoarseQuery) (CompatibleCoarseResult, error)
}

// CoarseModeSelection 保存粗分类模式选择结果
//
// ModelBundleMetadata 始终来自请求中明确指定的 Bundle
// 仅 CoarseModeReusePrevious 模式下 ReusedCoarse 非 nil
type CoarseModeSelection struct {
	Mode        CoarseMode
	ModelBundle ModelBundleMetadata

	ReusedCoarse *CompatibleCoarseResult
}

// CoarseModeSelector 根据实际 epoch 数、指定 Bundle 和兼容历史结果
// 选择本次分类使用的粗分类模式
type CoarseModeSelector struct {
	modelBundleResolver ModelBundleResolver
	coarseFinder        CompatibleCoarseFinder
}

func NewCoarseModeSelector(modelBundleResolver ModelBundleResolver, coarseFinder CompatibleCoarseFinder) (*CoarseModeSelector, error) {
	if modelBundleResolver == nil {
		return nil, errors.New("model bundle resolver must not be nil")
	}
	if coarseFinder == nil {
		return nil, errors.New("compatible coarse finder must not be nil")
	}
	return &CoarseModeSelector{
		modelBundleResolver: modelBundleResolver,
		coarseFinder:        coarseFinder,
	}, nil
}

// Select 选择本次分类的粗分类模式
//
// 只有 ErrCompatibleCoarseNotFound 才可以执行 bootstrap
// 其他历史查询错误必须鸳鸯传播，不能被误判为“没有历史错误”
func (selector *CoarseModeSelector) Select(ctx context.Context, objectID string, targetLightCurveRevision int64, actualEpochCount uint32, modelBundleVersion string) (CoarseModeSelection, error) {
	if ctx == nil {
		return CoarseModeSelection{}, errors.New(
			"select coarse mode: nil context",
		)
	}
	if selector == nil {
		return CoarseModeSelection{}, errors.New(
			"select coarse mode: nil selector",
		)
	}
	if selector.modelBundleResolver == nil {
		return CoarseModeSelection{}, errors.New(
			"select coarse mode: nil model bundle resolver",
		)
	}
	if selector.coarseFinder == nil {
		return CoarseModeSelection{}, errors.New(
			"select coarse mode: nil compatible coarse finder",
		)
	}
	if err := ctx.Err(); err != nil {
		return CoarseModeSelection{}, err
	}

	if objectID == "" {
		return CoarseModeSelection{}, errors.New(
			"select coarse mode: object ID must not be empty",
		)
	}
	if targetLightCurveRevision <= 0 {
		return CoarseModeSelection{}, errors.New(
			"select coarse mode: target light curve revision must be greater than zero",
		)
	}
	if modelBundleVersion == "" {
		return CoarseModeSelection{}, errors.New(
			"select coarse mode: model bundle version must not be empty",
		)
	}
	if actualEpochCount < minimumLightCurveEpochCount {
		return CoarseModeSelection{}, fmt.Errorf(
			"%w: actual=%d minimum=%d",
			ErrInsufficientLightCurveEpochs,
			actualEpochCount,
			minimumLightCurveEpochCount,
		)
	}
	if actualEpochCount > maximumLightCurveEpochCount {
		return CoarseModeSelection{}, fmt.Errorf(
			"%w: actual=%d maximum=%d",
			ErrTooManyLightCurveEpochs,
			actualEpochCount,
			maximumLightCurveEpochCount,
		)
	}

	// 解析模型Bundle最小元数据
	modelBundle, err := selector.modelBundleResolver.Resolve(ctx, modelBundleVersion)
	if err != nil {
		return CoarseModeSelection{}, fmt.Errorf("resolve model bundle %q: %w", modelBundleVersion, err)
	}
	if err := ctx.Err(); err != nil {
		return CoarseModeSelection{}, err
	}

	if err := validateModelBundleMetadata(modelBundleVersion, modelBundle); err != nil {
		return CoarseModeSelection{}, err
	}

	// 规则
	if actualEpochCount <= maximumComputeCurrentEpochCount {
		return CoarseModeSelection{
			Mode:         CoarseModeComputeCurrent,
			ModelBundle:  modelBundle,
			ReusedCoarse: nil,
		}, nil
	}

	// 超过了20 就查询兼容的历史粗概率
	query := CompatibleCoarseQuery{
		ObjectID:                 objectID,
		TargetLightCurveRevision: targetLightCurveRevision,
		ModelBundleVersion:       modelBundleVersion,
	}

	compatibleCoarse, err := selector.coarseFinder.FindLatestCompatibleCoarse(ctx, query)
	if err != nil {
		// 没有找到兼容的粗概率
		if errors.Is(err, ErrCompatibleCoarseNotFound) {
			return CoarseModeSelection{
				Mode:         CoarseModeComputeBootstrap,
				ModelBundle:  modelBundle,
				ReusedCoarse: nil,
			}, nil
		}

		return CoarseModeSelection{}, fmt.Errorf("find latest compatible coarse classification: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return CoarseModeSelection{}, err
	}

	if err := validateCompatibleCoarseResult(targetLightCurveRevision, compatibleCoarse); err != nil {
		return CoarseModeSelection{}, err
	}

	// CompatibleCoarseResult 只包含值类型和固定数组
	// 复制后返回独立结果，避免暴露 Finder 内部存储
	reusedCoarse := compatibleCoarse

	return CoarseModeSelection{
		Mode:         CoarseModeReusePrevious,
		ModelBundle:  modelBundle,
		ReusedCoarse: &reusedCoarse,
	}, nil
}

func validateModelBundleMetadata(requestedVersion string, metadata ModelBundleMetadata) error {
	if metadata.ModelBundleVersion != requestedVersion {
		return fmt.Errorf("%w: requested model_bundle_version=%q returned=%q",
			ErrInvalidModelBundleMetadata,
			requestedVersion,
			metadata.ModelBundleVersion,
		)
	}

	return nil
}

func validateCompatibleCoarseResult(targetLightCurveRevision int64, result CompatibleCoarseResult) error {
	if result.SourceRunID == "" {
		return fmt.Errorf("%w: source run ID must not be empty", ErrInvalidCompatibleCoarseResult)
	}

	if result.SourceLightCurveRevision <= 0 {
		return fmt.Errorf("%w: source revision=%d must be greater than zero", ErrInvalidCompatibleCoarseResult, result.SourceLightCurveRevision)
	}

	if result.SourceLightCurveRevision >= targetLightCurveRevision {
		return fmt.Errorf(
			"%w: source revision=%d must be less than target revision=%d",
			ErrInvalidCompatibleCoarseResult,
			result.SourceLightCurveRevision,
			targetLightCurveRevision,
		)
	}

	if result.SourceEpochCount < minimumLightCurveEpochCount ||
		result.SourceEpochCount > maximumLightCurveEpochCount {
		return fmt.Errorf(
			"%w: source epoch count=%d must be within %d..%d",
			ErrInvalidCompatibleCoarseResult,
			result.SourceEpochCount,
			minimumLightCurveEpochCount,
			maximumLightCurveEpochCount,
		)
	}

	return nil
}
