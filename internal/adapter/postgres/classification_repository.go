package postgres

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ClassificationRepository 使用 PostgreSQL 保存分类结果
//
// 目前先实现原子写入路径：读查询将在后续补齐
type ClassificationRepository struct {
	pool *pgxpool.Pool
}

// 编译断言
var _ application.ClassificationRepository = (*ClassificationRepository)(nil)

// NewClassificationRepository 创建 PostgreSQL Repository
func NewClassificationRepository(pool *pgxpool.Pool) *ClassificationRepository {
	return &ClassificationRepository{
		pool: pool,
	}
}

const insertClassificationRunSQL = `
INSERT INTO classification_runs (
    run_id,
    job_id,
    object_id,
    candidate_revision,
    light_curve_revision,
    effective_epoch_count,
    execution_mode,
    coarse_source_type,
    coarse_source_run_id,
    coarse_source_light_curve_revision,
    coarse_source_epoch_count,
    xgboost_executed,
    model_bundle_version,
    taxonomy_version,
    preprocessing_version,
    feature_schema_version,
    tensor_schema_version,
    classification_policy_version,
    coarse_probabilities,
    fine_conditional_probabilities,
    leaf_probabilities,
    predicted_coarse_class,
    predicted_leaf_class,
    completed_at
) VALUES (
    $1,  $2,  $3,  $4,  $5,  $6,  $7,  $8,
    $9,  $10, $11, $12, $13, $14, $15, $16,
    $17, $18, $19, $20, $21, $22, $23, $24
)
ON CONFLICT DO NOTHING
RETURNING persisted_at
`

// inspectExistingRunIdentitySQL 是否有相同记录，是否同一个 jobID 有不同的 runID，是否同一个 runID 有不同的 jobID
const inspectExistingRunIdentitySQL = `
SELECT
    EXISTS (
        SELECT 1
        FROM classification_runs
        WHERE run_id = $1
          AND job_id = $2
    ),
    EXISTS (
        SELECT 1
        FROM classification_runs
        WHERE job_id = $2
          AND run_id <> $1
    ),
    EXISTS (
        SELECT 1
        FROM classification_runs
        WHERE run_id = $1
          AND job_id <> $2
    )
`

// advanceCurrentClassificationSQL 尝试更新 Current
const advanceCurrentClassificationSQL = `
INSERT INTO current_classifications (
    object_id,
    run_id,
    light_curve_revision
) VALUES (
    $1,
    $2,
    $3
)
ON CONFLICT (object_id) DO UPDATE
SET
    run_id = EXCLUDED.run_id,
    light_curve_revision = EXCLUDED.light_curve_revision,
    updated_at = clock_timestamp()
WHERE
    current_classifications.light_curve_revision
        < EXCLUDED.light_curve_revision
RETURNING updated_at
`

// Run 表字段 用于复用
const classificationRunSelectColumns = `
    r.run_id::text,
    r.job_id::text,
    r.object_id,
    r.candidate_revision,
    r.light_curve_revision,
    r.effective_epoch_count,
    r.execution_mode,
    r.coarse_source_type,
    COALESCE(r.coarse_source_run_id::text, ''),
    r.coarse_source_light_curve_revision,
    r.coarse_source_epoch_count,
    r.xgboost_executed,
    r.model_bundle_version,
    r.taxonomy_version,
    r.preprocessing_version,
    r.feature_schema_version,
    r.tensor_schema_version,
    r.classification_policy_version,
    r.coarse_probabilities,
    r.fine_conditional_probabilities,
    r.leaf_probabilities,
    r.predicted_coarse_class,
    r.predicted_leaf_class,
    r.completed_at,
    r.persisted_at
`

// 通过 Join 获取 run 的字段
const getCurrentClassificationSQL = `
SELECT
    c.updated_at,
` + classificationRunSelectColumns + `
FROM current_classifications AS c
JOIN classification_runs AS r
  ON r.run_id = c.run_id
 AND r.object_id = c.object_id
 AND r.light_curve_revision = c.light_curve_revision
WHERE c.object_id = $1
`

// 获取最新的兼容粗分类
const findLatestCompatibleCoarseSQL = `
SELECT
    r.run_id::text,
    r.light_curve_revision,
    r.effective_epoch_count,
    r.coarse_probabilities
FROM classification_runs AS r
WHERE r.object_id = $1
  AND r.model_bundle_version = $2
  AND r.xgboost_executed = TRUE
  AND r.light_curve_revision < $3
ORDER BY
    r.light_curve_revision DESC,
    r.persisted_at DESC,
    r.run_id DESC
LIMIT 1
`

// SaveRunAndMaybeAdvancedCurrent 在同一事务中保存 Run，并在符合条件时推进 current。
func (repository *ClassificationRepository) SaveRunAndMaybeAdvanceCurrent(ctx context.Context, run domain.ClassificationRun) (application.SaveRunResult, error) {
	if repository == nil || repository.pool == nil {
		return application.SaveRunResult{}, errors.New("save classification run: nil PostgreSQL pool")
	}

	// 提取参数
	arguments, err := classificationRunInsertArgument(run)
	if err != nil {
		return application.SaveRunResult{}, fmt.Errorf("save classification run: %w", err)
	}
	tx, err := repository.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return application.SaveRunResult{}, fmt.Errorf("save classification transaction: %w", err)
	}

	// 如何已经提交了，回滚会被忽略
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	var persistedAt time.Time

	// 插入 run，并获取插入的时间
	err = tx.QueryRow(ctx, insertClassificationRunSQL, arguments...).Scan(&persistedAt)

	// 如果未返回记录
	if errors.Is(err, pgx.ErrNoRows) {
		// 判断是否已经有记录，数据库为了幂等性导致插入失败
		idempotent, inspectErr := existingRunIsIdempotent(ctx, tx, run.RunID, run.JobID)
		if inspectErr != nil {
			return application.SaveRunResult{}, inspectErr
		}

		// 幂等性约束，未插入新记录但也没出错
		if idempotent {
			return application.SaveRunResult{}, nil
		}

		// 不满足幂等性，发生冲突
		return application.SaveRunResult{}, application.ErrClassificationRunConflict
	}

	if err != nil {
		return application.SaveRunResult{}, fmt.Errorf("insert classification run: %w", err)
	}

	// 成功插入了 run 记录，但不意味着 Current 能被更新
	result := application.SaveRunResult{
		RunInserted: true,
	}

	// 只对 Production 的 run 执行 current 更新
	if run.ExecutionMode == domain.ExecutionModeProduction {
		currentAdvanced, err := advanceCurrentClassification(ctx, tx, run)
		if err != nil {
			return application.SaveRunResult{}, err
		}

		result.CurrentAdvanced = currentAdvanced
	}

	if err := tx.Commit(ctx); err != nil {
		return application.SaveRunResult{}, fmt.Errorf("commit classification transaction: %w", err)
	}

	return result, err
}

// advanceCurrentClassification 尝试更新 currentClassification，返回是否更新了
func advanceCurrentClassification(ctx context.Context, tx pgx.Tx, run domain.ClassificationRun) (bool, error) {
	var updatedAt time.Time

	err := tx.QueryRow(ctx, advanceCurrentClassificationSQL, run.ObjectID, string(run.RunID), run.LightCurveRevision).Scan(&updatedAt)

	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, pgx.ErrNoRows):
		// 相同或更旧 revision 只保留历史，不推进 current
		return false, nil
	default:
		return false, fmt.Errorf("advance current classification: %w", err)
	}
}

// 是否存在幂等的 ClassificationRun
func existingRunIsIdempotent(ctx context.Context, tx pgx.Tx, runID domain.RunID, jobID domain.JobID) (bool, error) {
	var sameIdentity bool
	var jobConflict bool
	var runConflict bool

	err := tx.QueryRow(ctx, inspectExistingRunIdentitySQL, string(runID), string(jobID)).Scan(&sameIdentity, &jobConflict, &runConflict)
	if err != nil {
		return false, fmt.Errorf("inspect existing classification run: %w", err)
	}

	if sameIdentity {
		return true, nil
	}
	if jobConflict || runConflict {
		return false, nil
	}

	// 没有相同记录 也没有 id 冲突
	// 分类运行记录插入已跳过，没有匹配的唯一冲突
	return false, errors.New("classification run insert skipped without a matching unique conflict")
}

// 提取用于插入分类运行结果的参数，顺便进行参数的校验
func classificationRunInsertArgument(run domain.ClassificationRun) ([]any, error) {
	effectiveEpochCount, err := postgresInteger("effective epoch count", run.EffectiveEpochCount)
	if err != nil {
		return nil, err
	}

	coarseSourceEpochCount, err := postgresInteger("coarse source epoch count", run.CoarseSourceEpochCount)
	if err != nil {
		return nil, err
	}

	var coarseSourceRunID any
	if run.CoarseSourceRunID != nil {
		coarseSourceRunID = string(*run.CoarseSourceRunID)
	}

	return []any{
		string(run.RunID),
		string(run.JobID),
		run.ObjectID,
		run.CandidateRevision,
		run.LightCurveRevision,
		effectiveEpochCount,
		int16(run.ExecutionMode),
		int16(run.CoarseSourceType),
		coarseSourceRunID,
		run.CoarseSourceLightCurveRevision,
		coarseSourceEpochCount,
		run.XGBoostExecuted,
		run.Versions.ModelBundleVersion,
		run.Versions.TaxonomyVersion,
		run.Versions.PreprocessingVersion,
		run.Versions.FeatureSchemaVersion,
		run.Versions.TensorSchemaVersion,
		run.Versions.ClassificationPolicyVersion,
		run.CoarseProbabilities[:],
		run.FineConditionalProbabilities[:],
		run.LeafProbabilities[:],
		int32(run.PredictedCoarseClass),
		int32(run.PredictedLeafClass),
		run.CompletedAt,
	}, nil
}

func postgresInteger(field string, value uint32) (int32, error) {
	if value > math.MaxInt32 {
		return 0, fmt.Errorf("%s exceeds PostgreSQL INTEGER: %d", field, value)
	}
	return int32(value), nil
}

// GetCurrent 返回对象当前 Production 分类及其完整历史 Run。
func (repository *ClassificationRepository) GetCurrent(
	ctx context.Context,
	objectID string,
) (domain.CurrentClassification, error) {
	if repository == nil || repository.pool == nil {
		return domain.CurrentClassification{},
			errors.New("get current classification: nil PostgreSQL pool")
	}

	var updatedAt time.Time
	// 把接受参数的结构体的创建放在函数外部
	record := classificationRunRecord{}

	destinations := []any{&updatedAt}
	destinations = append(
		destinations,
		record.scanDestinations()...,
	)

	err := repository.pool.QueryRow(
		ctx,
		getCurrentClassificationSQL,
		objectID,
	).Scan(destinations...)

	if errors.Is(err, pgx.ErrNoRows) {
		return domain.CurrentClassification{},
			application.ErrCurrentClassificationNotFound
	}

	if err != nil {
		return domain.CurrentClassification{},
			fmt.Errorf("get current classification: %w", err)
	}

	// 把查询到的记录转换成领域数据
	run, err := record.toDomain()
	if err != nil {
		return domain.CurrentClassification{},
			fmt.Errorf("decode current classification: %w", err)
	}

	return domain.CurrentClassification{
		Run:       run,
		UpdatedAt: updatedAt,
	}, nil
}

// FindLatestCompatibleCoarse 查找目标 revision 之前最近的兼容粗概率。
func (
	repository *ClassificationRepository,
) FindLatestCompatibleCoarse(
	ctx context.Context,
	query application.CompatibleCoarseQuery,
) (application.CompatibleCoarseResult, error) {
	if repository == nil || repository.pool == nil {
		return application.CompatibleCoarseResult{},
			errors.New("find compatible coarse classification: nil PostgreSQL pool")
	}

	var sourceRunID string
	var sourceRevision int64
	var sourceEpochCount int32
	var probabilities []float32

	err := repository.pool.QueryRow(
		ctx,
		findLatestCompatibleCoarseSQL,
		query.ObjectID,
		query.ModelBundleVersion,
		query.TargetLightCurveRevision,
	).Scan(
		&sourceRunID,
		&sourceRevision,
		&sourceEpochCount,
		&probabilities,
	)

	if errors.Is(err, pgx.ErrNoRows) {
		return application.CompatibleCoarseResult{},
			application.ErrCompatibleCoarseNotFound
	}

	if err != nil {
		return application.CompatibleCoarseResult{},
			fmt.Errorf("find compatible coarse classification: %w", err)
	}

	epochCount, err := uint32FromPostgresInteger(
		"compatible coarse epoch count",
		sourceEpochCount,
	)
	if err != nil {
		return application.CompatibleCoarseResult{}, err
	}

	if len(probabilities) != domain.CoarseProbabilityCount {
		return application.CompatibleCoarseResult{}, fmt.Errorf(
			"compatible coarse probability count is %d, expected %d",
			len(probabilities),
			domain.CoarseProbabilityCount,
		)
	}

	result := application.CompatibleCoarseResult{
		SourceRunID:              domain.RunID(sourceRunID),
		SourceLightCurveRevision: sourceRevision,
		SourceEpochCount:         epochCount,
	}
	copy(result.Probabilities[:], probabilities)

	return result, nil
}

type classificationRunRecord struct {
	runID    string
	jobID    string
	objectID string

	candidateRevision   int64
	lightCurveRevision  int64
	effectiveEpochCount int32
	executionMode       int16

	coarseSourceType               int16
	coarseSourceRunID              string
	coarseSourceLightCurveRevision int64
	coarseSourceEpochCount         int32
	xgboostExecuted                bool

	modelBundleVersion          string
	taxonomyVersion             string
	preprocessingVersion        string
	featureSchemaVersion        string
	tensorSchemaVersion         string
	classificationPolicyVersion string

	coarseProbabilities          []float32
	fineConditionalProbabilities []float32
	leafProbabilities            []float32

	predictedCoarseClass int32
	predictedLeafClass   int32

	completedAt time.Time
	persistedAt time.Time
}

func (record *classificationRunRecord) scanDestinations() []any {
	return []any{
		&record.runID,
		&record.jobID,
		&record.objectID,
		&record.candidateRevision,
		&record.lightCurveRevision,
		&record.effectiveEpochCount,
		&record.executionMode,
		&record.coarseSourceType,
		&record.coarseSourceRunID,
		&record.coarseSourceLightCurveRevision,
		&record.coarseSourceEpochCount,
		&record.xgboostExecuted,
		&record.modelBundleVersion,
		&record.taxonomyVersion,
		&record.preprocessingVersion,
		&record.featureSchemaVersion,
		&record.tensorSchemaVersion,
		&record.classificationPolicyVersion,
		&record.coarseProbabilities,
		&record.fineConditionalProbabilities,
		&record.leafProbabilities,
		&record.predictedCoarseClass,
		&record.predictedLeafClass,
		&record.completedAt,
		&record.persistedAt,
	}
}

// 把查询到的 run 记录转换成领域数据
func (
	record classificationRunRecord,
) toDomain() (domain.ClassificationRun, error) {
	effectiveEpochCount, err := uint32FromPostgresInteger(
		"effective epoch count",
		record.effectiveEpochCount,
	)
	if err != nil {
		return domain.ClassificationRun{}, err
	}

	coarseSourceEpochCount, err := uint32FromPostgresInteger(
		"coarse source epoch count",
		record.coarseSourceEpochCount,
	)
	if err != nil {
		return domain.ClassificationRun{}, err
	}

	if len(record.coarseProbabilities) !=
		domain.CoarseProbabilityCount {
		return domain.ClassificationRun{}, fmt.Errorf(
			"coarse probability count is %d, expected %d",
			len(record.coarseProbabilities),
			domain.CoarseProbabilityCount,
		)
	}

	if len(record.fineConditionalProbabilities) !=
		domain.ConditionalFineProbabilityCount {
		return domain.ClassificationRun{}, fmt.Errorf(
			"conditional fine probability count is %d, expected %d",
			len(record.fineConditionalProbabilities),
			domain.ConditionalFineProbabilityCount,
		)
	}

	if len(record.leafProbabilities) !=
		domain.LeafProbabilityCount {
		return domain.ClassificationRun{}, fmt.Errorf(
			"leaf probability count is %d, expected %d",
			len(record.leafProbabilities),
			domain.LeafProbabilityCount,
		)
	}

	run := domain.ClassificationRun{
		RunID:                          domain.RunID(record.runID),
		JobID:                          domain.JobID(record.jobID),
		ObjectID:                       record.objectID,
		CandidateRevision:              record.candidateRevision,
		LightCurveRevision:             record.lightCurveRevision,
		EffectiveEpochCount:            effectiveEpochCount,
		ExecutionMode:                  domain.ExecutionMode(record.executionMode),
		CoarseSourceType:               domain.CoarseSourceType(record.coarseSourceType),
		CoarseSourceLightCurveRevision: record.coarseSourceLightCurveRevision,
		CoarseSourceEpochCount:         coarseSourceEpochCount,
		XGBoostExecuted:                record.xgboostExecuted,
		Versions: domain.ResolvedModelVersions{
			ModelBundleVersion:          record.modelBundleVersion,
			TaxonomyVersion:             record.taxonomyVersion,
			PreprocessingVersion:        record.preprocessingVersion,
			FeatureSchemaVersion:        record.featureSchemaVersion,
			TensorSchemaVersion:         record.tensorSchemaVersion,
			ClassificationPolicyVersion: record.classificationPolicyVersion,
		},
		PredictedCoarseClass: domain.CoarseClass(
			record.predictedCoarseClass,
		),
		PredictedLeafClass: domain.LeafClass(
			record.predictedLeafClass,
		),
		CompletedAt: record.completedAt,
		PersistedAt: record.persistedAt,
	}

	if record.coarseSourceRunID != "" {
		sourceRunID := domain.RunID(
			record.coarseSourceRunID,
		)
		run.CoarseSourceRunID = &sourceRunID
	}

	copy(
		run.CoarseProbabilities[:],
		record.coarseProbabilities,
	)
	copy(
		run.FineConditionalProbabilities[:],
		record.fineConditionalProbabilities,
	)
	copy(
		run.LeafProbabilities[:],
		record.leafProbabilities,
	)

	return run, nil
}

func uint32FromPostgresInteger(
	field string,
	value int32,
) (uint32, error) {
	if value < 0 {
		return 0, fmt.Errorf(
			"%s is negative: %d",
			field,
			value,
		)
	}

	return uint32(value), nil
}
