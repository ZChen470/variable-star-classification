package application

import (
	"context"
	"errors"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

var (
	// ErrCurrentClassificationNotFound 表示对象尚无 Production current。
	ErrCurrentClassificationNotFound = errors.New(
		"current classification not found",
	)

	// ErrCompatibleCoarseNotFound 表示没有满足三版本兼容条件的历史粗概率。
	ErrCompatibleCoarseNotFound = errors.New(
		"compatible coarse classification not found",
	)

	// ErrClassificationRunConflict 表示相同逻辑 Job 已对应其他 Run。
	ErrClassificationRunConflict = errors.New(
		"classification run conflicts with an existing job",
	)
)

// SaveRunResult 描述原子保存操作实际产生的状态变化。
type SaveRunResult struct {
	RunInserted     bool
	CurrentAdvanced bool
}

// CompatibleCoarseQuery 描述可复用历史粗概率的查询条件.
//
// ExecutionMode 不属于兼容条件。
type CompatibleCoarseQuery struct {
	ObjectID string

	TargetLightCurveRevision int64

	ModelBundleVersion string
}

// CompatibleCoarseResult 是建立 REUSED_PREVIOUS 来源所需的最小历史信息。
type CompatibleCoarseResult struct {
	SourceRunID              domain.RunID
	SourceLightCurveRevision int64
	SourceEpochCount         uint32
	Probabilities            [domain.CoarseProbabilityCount]float32
}

// ClassificationRepository 是应用层使用的分类结果持久化 Port。
type ClassificationRepository interface {
	// SaveRunAndMaybeAdvanceCurrent 在同一事务中保存 Run，
	// 并仅在符合 Production 且 revision 严格递增规则时推进 current。
	SaveRunAndMaybeAdvanceCurrent(ctx context.Context, run domain.ClassificationRun) (SaveRunResult, error)

	// GetCurrent 返回对象当前 Production 分类及其完整历史 Run。
	GetCurrent(ctx context.Context, objectID string) (domain.CurrentClassification, error)

	// FindLatestCompatibleCoarse 查找目标 revision 之前最近的、
	// 实际执行过 XGBoost 且三版本兼容的历史粗概率。
	FindLatestCompatibleCoarse(ctx context.Context, query CompatibleCoarseQuery) (CompatibleCoarseResult, error)
}
