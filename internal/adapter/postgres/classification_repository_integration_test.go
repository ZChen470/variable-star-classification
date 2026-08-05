package postgres

import (
	"context"
	"errors"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestClassificationRepositoryIntegration(t *testing.T) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}
	//defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	repository := NewClassificationRepository(pool)

	t.Cleanup(func() {
		defer pool.Close()
		resetClassificationTables(t, ctx, pool)
	})

	t.Run("save idempotency and current progression", func(t *testing.T) {
		resetClassificationTables(t, ctx, pool)

		first := newTestClassificationRun(t, testRunSpec{
			objectID:        "OBJ-CURRENT",
			revision:        10,
			mode:            domain.ExecutionModeProduction,
			xgboostExecuted: true,
		})

		requireSaveResult(t, repository, first, true, true)

		current, err := repository.GetCurrent(ctx, first.ObjectID)
		if err != nil {
			t.Fatalf("GetCurrent(first): %v", err)
		}
		if current.UpdatedAt.IsZero() {
			t.Fatal("current updated_at is zero")
		}
		requireRunRoundTrip(t, current.Run, first)

		// 完全相同的 Run 重放应保持幂等。
		requireSaveResult(t, repository, first, false, false)

		// 相同 Job 对应其他 RunID 必须返回冲突。
		conflicting := first
		conflicting.RunID = domain.RunID(
			"11111111-1111-4111-8111-111111111111",
		)

		_, err = repository.SaveRunAndMaybeAdvanceCurrent(
			ctx,
			conflicting,
		)
		if !errors.Is(err, application.ErrClassificationRunConflict) {
			t.Fatalf(
				"conflicting SaveRunAndMaybeAdvanceCurrent() error = %v, want %v",
				err,
				application.ErrClassificationRunConflict,
			)
		}

		newer := newTestClassificationRun(t, testRunSpec{
			objectID:        first.ObjectID,
			revision:        12,
			mode:            domain.ExecutionModeProduction,
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, newer, true, true)

		older := newTestClassificationRun(t, testRunSpec{
			objectID:        first.ObjectID,
			revision:        11,
			mode:            domain.ExecutionModeProduction,
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, older, true, false)

		sameRevisionDifferentBundle := newTestClassificationRun(
			t,
			testRunSpec{
				objectID:        first.ObjectID,
				revision:        12,
				mode:            domain.ExecutionModeProduction,
				bundleVersion:   "bundle-v2",
				xgboostExecuted: true,
			},
		)
		requireSaveResult(
			t,
			repository,
			sameRevisionDifferentBundle,
			true,
			false,
		)

		current, err = repository.GetCurrent(ctx, first.ObjectID)
		if err != nil {
			t.Fatalf("GetCurrent(after progression): %v", err)
		}
		if current.Run.RunID != newer.RunID {
			t.Fatalf(
				"current run ID = %q, want %q",
				current.Run.RunID,
				newer.RunID,
			)
		}
		if current.Run.LightCurveRevision != 12 {
			t.Fatalf(
				"current revision = %d, want 12",
				current.Run.LightCurveRevision,
			)
		}

		var runCount int
		err = pool.QueryRow(
			ctx,
			`SELECT COUNT(*) FROM classification_runs`,
		).Scan(&runCount)
		if err != nil {
			t.Fatalf("count classification runs: %v", err)
		}
		if runCount != 4 {
			t.Fatalf("classification run count = %d, want 4", runCount)
		}
	})

	t.Run("shadow and reprocess never advance current", func(t *testing.T) {
		resetClassificationTables(t, ctx, pool)

		shadow := newTestClassificationRun(t, testRunSpec{
			objectID:        "OBJ-MODES",
			revision:        20,
			mode:            domain.ExecutionModeShadow,
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, shadow, true, false)
		requireCurrentNotFound(t, repository, shadow.ObjectID)

		reprocess := newTestClassificationRun(t, testRunSpec{
			objectID:        shadow.ObjectID,
			revision:        21,
			mode:            domain.ExecutionModeReprocess,
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, reprocess, true, false)
		requireCurrentNotFound(t, repository, shadow.ObjectID)

		production := newTestClassificationRun(t, testRunSpec{
			objectID:        shadow.ObjectID,
			revision:        22,
			mode:            domain.ExecutionModeProduction,
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, production, true, true)

		newerShadow := newTestClassificationRun(t, testRunSpec{
			objectID:        shadow.ObjectID,
			revision:        23,
			mode:            domain.ExecutionModeShadow,
			bundleVersion:   "bundle-shadow-v2",
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, newerShadow, true, false)

		newerReprocess := newTestClassificationRun(t, testRunSpec{
			objectID:        shadow.ObjectID,
			revision:        24,
			mode:            domain.ExecutionModeReprocess,
			bundleVersion:   "bundle-reprocess-v2",
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, newerReprocess, true, false)

		current, err := repository.GetCurrent(ctx, shadow.ObjectID)
		if err != nil {
			t.Fatalf("GetCurrent(): %v", err)
		}
		if current.Run.RunID != production.RunID {
			t.Fatalf(
				"current run ID = %q, want production run %q",
				current.Run.RunID,
				production.RunID,
			)
		}
	})

	t.Run("compatible coarse lookup", func(t *testing.T) {
		resetClassificationTables(t, ctx, pool)

		base := newTestClassificationRun(t, testRunSpec{
			objectID:        "OBJ-COMPATIBLE",
			revision:        30,
			mode:            domain.ExecutionModeProduction,
			bundleVersion:   "bundle-base",
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, base, true, true)

		// 使用 SHADOW 验证 execution_mode 不属于兼容条件。
		latestCompatible := newTestClassificationRun(t, testRunSpec{
			objectID:        base.ObjectID,
			revision:        31,
			mode:            domain.ExecutionModeShadow,
			bundleVersion:   base.Versions.ModelBundleVersion,
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, latestCompatible, true, false)

		// 同一 Bundle 但没有实际执行 XGBoost，不能成为来源。
		reused := newTestClassificationRun(t, testRunSpec{
			objectID:        base.ObjectID,
			revision:        32,
			mode:            domain.ExecutionModeProduction,
			bundleVersion:   base.Versions.ModelBundleVersion,
			xgboostExecuted: false,
			source:          &latestCompatible,
		})
		requireSaveResult(t, repository, reused, true, true)

		// 不同 Bundle 即使执行过 XGBoost，也不兼容。
		incompatibleBundle := newTestClassificationRun(t, testRunSpec{
			objectID:        base.ObjectID,
			revision:        33,
			mode:            domain.ExecutionModeProduction,
			bundleVersion:   "bundle-other",
			xgboostExecuted: true,
		})
		requireSaveResult(t, repository, incompatibleBundle, true, true)

		result, err := repository.FindLatestCompatibleCoarse(
			ctx,
			application.CompatibleCoarseQuery{
				ObjectID:                 base.ObjectID,
				TargetLightCurveRevision: 34,
				ModelBundleVersion:       base.Versions.ModelBundleVersion,
			},
		)
		if err != nil {
			t.Fatalf("FindLatestCompatibleCoarse(): %v", err)
		}
		if result.SourceRunID != latestCompatible.RunID {
			t.Fatalf(
				"compatible source run ID = %q, want %q",
				result.SourceRunID,
				latestCompatible.RunID,
			)
		}
		if result.SourceLightCurveRevision != 31 {
			t.Fatalf(
				"compatible source revision = %d, want 31",
				result.SourceLightCurveRevision,
			)
		}
		if result.SourceEpochCount != latestCompatible.EffectiveEpochCount {
			t.Fatalf(
				"compatible source epoch count = %d, want %d",
				result.SourceEpochCount,
				latestCompatible.EffectiveEpochCount,
			)
		}
		if result.Probabilities != latestCompatible.CoarseProbabilities {
			t.Fatalf(
				"compatible probabilities = %#v, want %#v",
				result.Probabilities,
				latestCompatible.CoarseProbabilities,
			)
		}

		// revision 使用严格小于，因此目标 31 只能找到 revision 30。
		strictlyEarlier, err := repository.FindLatestCompatibleCoarse(
			ctx,
			application.CompatibleCoarseQuery{
				ObjectID:                 base.ObjectID,
				TargetLightCurveRevision: 31,
				ModelBundleVersion:       base.Versions.ModelBundleVersion,
			},
		)
		if err != nil {
			t.Fatalf("FindLatestCompatibleCoarse(strict revision): %v", err)
		}
		if strictlyEarlier.SourceRunID != base.RunID {
			t.Fatalf(
				"strictly earlier source = %q, want %q",
				strictlyEarlier.SourceRunID,
				base.RunID,
			)
		}

		notFoundQueries := []struct {
			name  string
			query application.CompatibleCoarseQuery
		}{
			{
				name: "no earlier revision",
				query: application.CompatibleCoarseQuery{
					ObjectID:                 base.ObjectID,
					TargetLightCurveRevision: 30,
					ModelBundleVersion:       base.Versions.ModelBundleVersion,
				},
			},
			{
				name: "model bundle mismatch",
				query: application.CompatibleCoarseQuery{
					ObjectID:                 base.ObjectID,
					TargetLightCurveRevision: 34,
					ModelBundleVersion:       "bundle-missing",
				},
			},
		}

		for _, testCase := range notFoundQueries {
			t.Run(testCase.name, func(t *testing.T) {
				_, err := repository.FindLatestCompatibleCoarse(
					ctx,
					testCase.query,
				)
				if !errors.Is(
					err,
					application.ErrCompatibleCoarseNotFound,
				) {
					t.Fatalf(
						"FindLatestCompatibleCoarse() error = %v, want %v",
						err,
						application.ErrCompatibleCoarseNotFound,
					)
				}
			})
		}
	})
}

type testRunSpec struct {
	objectID string
	revision int64
	mode     domain.ExecutionMode

	bundleVersion string

	xgboostExecuted bool
	source          *domain.ClassificationRun
}

func newTestClassificationRun(
	t *testing.T,
	spec testRunSpec,
) domain.ClassificationRun {
	t.Helper()

	if spec.bundleVersion == "" {
		spec.bundleVersion = "bundle-v1"
	}

	const policyVersion = "classification-policy-v1"

	jobID, err := domain.GenerateJobID(domain.JobIdentity{
		ObjectID:                    spec.objectID,
		LightCurveRevision:          spec.revision,
		ModelBundleVersion:          spec.bundleVersion,
		ClassificationPolicyVersion: policyVersion,
		ExecutionMode:               spec.mode,
	})
	if err != nil {
		t.Fatalf("GenerateJobID(): %v", err)
	}

	runID, err := domain.GenerateRunID(jobID)
	if err != nil {
		t.Fatalf("GenerateRunID(): %v", err)
	}

	effectiveEpochCount := uint32(100 + spec.revision)

	sourceType := domain.CoarseSourceComputedCurrent
	sourceRevision := spec.revision
	sourceEpochCount := effectiveEpochCount
	var sourceRunID *domain.RunID

	if !spec.xgboostExecuted {
		if spec.source == nil {
			t.Fatal("reused coarse run requires a source")
		}

		sourceType = domain.CoarseSourceReusedPrevious
		sourceRevision = spec.source.LightCurveRevision
		sourceEpochCount = spec.source.EffectiveEpochCount

		copiedSourceRunID := spec.source.RunID
		sourceRunID = &copiedSourceRunID
	}

	return domain.ClassificationRun{
		RunID:               runID,
		JobID:               jobID,
		ObjectID:            spec.objectID,
		CandidateRevision:   spec.revision,
		LightCurveRevision:  spec.revision,
		EffectiveEpochCount: effectiveEpochCount,
		ExecutionMode:       spec.mode,

		CoarseSourceType:               sourceType,
		CoarseSourceRunID:              sourceRunID,
		CoarseSourceLightCurveRevision: sourceRevision,
		CoarseSourceEpochCount:         sourceEpochCount,
		XGBoostExecuted:                spec.xgboostExecuted,

		Versions: domain.ResolvedModelVersions{
			ModelBundleVersion:          spec.bundleVersion,
			TaxonomyVersion:             "taxonomy-v1",
			PreprocessingVersion:        "preprocessing-v1",
			FeatureSchemaVersion:        "feature-v1",
			TensorSchemaVersion:         "tensor-v1",
			ClassificationPolicyVersion: policyVersion,
		},

		CoarseProbabilities: [domain.CoarseProbabilityCount]float32{
			0.1,
			0.4,
			0.1,
			0.1,
			0.1,
			0.1,
			0.1,
		},
		FineConditionalProbabilities: [domain.ConditionalFineProbabilityCount]float32{
			0.6, 0.4,
			0.6, 0.4,
			0.6, 0.4,
			0.6, 0.4,
			0.6, 0.4,
		},
		LeafProbabilities: [domain.LeafProbabilityCount]float32{
			0.06, 0.04,
			0.06, 0.04,
			0.06, 0.04,
			0.06, 0.04,
			0.06, 0.04,
			0.4,
			0.1,
		},

		PredictedCoarseClass: domain.CoarseClassCataclysmic,
		PredictedLeafClass:   domain.LeafClassCataclysmic,

		CompletedAt: time.Date(
			2026,
			time.July,
			1,
			0,
			0,
			0,
			0,
			time.UTC,
		).Add(time.Duration(spec.revision) * time.Second),
	}
}

func requireSaveResult(
	t *testing.T,
	repository *ClassificationRepository,
	run domain.ClassificationRun,
	wantInserted bool,
	wantAdvanced bool,
) {
	t.Helper()

	result, err := repository.SaveRunAndMaybeAdvanceCurrent(
		context.Background(),
		run,
	)
	if err != nil {
		t.Fatalf("SaveRunAndMaybeAdvanceCurrent(): %v", err)
	}
	if result.RunInserted != wantInserted {
		t.Fatalf(
			"RunInserted = %v, want %v",
			result.RunInserted,
			wantInserted,
		)
	}
	if result.CurrentAdvanced != wantAdvanced {
		t.Fatalf(
			"CurrentAdvanced = %v, want %v",
			result.CurrentAdvanced,
			wantAdvanced,
		)
	}
}

func requireCurrentNotFound(
	t *testing.T,
	repository *ClassificationRepository,
	objectID string,
) {
	t.Helper()

	_, err := repository.GetCurrent(
		context.Background(),
		objectID,
	)
	if !errors.Is(err, application.ErrCurrentClassificationNotFound) {
		t.Fatalf(
			"GetCurrent() error = %v, want %v",
			err,
			application.ErrCurrentClassificationNotFound,
		)
	}
}

func requireRunRoundTrip(
	t *testing.T,
	got domain.ClassificationRun,
	want domain.ClassificationRun,
) {
	t.Helper()

	if got.PersistedAt.IsZero() {
		t.Fatal("persisted_at is zero")
	}

	gotCompletedAt := got.CompletedAt
	wantCompletedAt := want.CompletedAt

	got.PersistedAt = time.Time{}
	got.CompletedAt = time.Time{}
	want.PersistedAt = time.Time{}
	want.CompletedAt = time.Time{}

	if !gotCompletedAt.Equal(wantCompletedAt) {
		t.Fatalf(
			"completed_at = %v, want %v",
			gotCompletedAt,
			wantCompletedAt,
		)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"round-trip run mismatch\ngot:  %#v\nwant: %#v",
			got,
			want,
		)
	}
}

func resetClassificationTables(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	_, err := pool.Exec(
		ctx,
		`TRUNCATE TABLE current_classifications, classification_runs`,
	)
	if err != nil {
		t.Fatalf("reset classification tables: %v", err)
	}
}
