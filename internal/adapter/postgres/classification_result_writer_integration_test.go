package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/jackc/pgx/v5/pgxpool"
)

const classificationResultWriterIntegrationTopic = "astro.classification.results.v1"

func TestClassificationResultWriterPostgresIntegration(
	t *testing.T,
) {
	dsn := os.Getenv("TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("TEST_POSTGRES_DSN is not set")
	}

	ctx := context.Background()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create PostgreSQL pool: %v", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping PostgreSQL: %v", err)
	}

	resetClassificationResultWriterIntegrationTables(
		t,
		ctx,
		pool,
	)

	t.Cleanup(func() {
		resetClassificationResultWriterIntegrationTables(
			t,
			context.Background(),
			pool,
		)

		pool.Close()
	})

	repository := NewClassificationRepository(pool)

	writer, err :=
		application.NewClassificationResultWriterHandler(
			classificationResultWriterIntegrationTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	const objectID = "OBJ-RESULT-WRITER-PG"

	first := newClassificationResultWriterIntegrationRun(
		t,
		classificationResultWriterIntegrationRunSpec{
			objectID: objectID,
			revision: 10,
			mode:     domain.ExecutionModeProduction,
		},
	)

	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		first,
	)

	requireClassificationResultWriterCurrent(
		t,
		ctx,
		repository,
		first,
	)

	// 完全相同的 Result 重放必须幂等成功。
	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		first,
	)

	requireClassificationResultWriterRunCount(
		t,
		ctx,
		pool,
		1,
	)

	newer := newClassificationResultWriterIntegrationRun(
		t,
		classificationResultWriterIntegrationRunSpec{
			objectID: objectID,
			revision: 12,
			mode:     domain.ExecutionModeProduction,
		},
	)

	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		newer,
	)

	requireClassificationResultWriterCurrent(
		t,
		ctx,
		repository,
		newer,
	)

	// 旧 revision 应保存为历史，但不得覆盖 Current。
	older := newClassificationResultWriterIntegrationRun(
		t,
		classificationResultWriterIntegrationRunSpec{
			objectID: objectID,
			revision: 11,
			mode:     domain.ExecutionModeProduction,
		},
	)

	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		older,
	)

	requireClassificationResultWriterCurrent(
		t,
		ctx,
		repository,
		newer,
	)

	// 相同 revision 的其他 Bundle 也不得覆盖 Current。
	sameRevisionDifferentBundle :=
		newClassificationResultWriterIntegrationRun(
			t,
			classificationResultWriterIntegrationRunSpec{
				objectID:      objectID,
				revision:      12,
				mode:          domain.ExecutionModeProduction,
				bundleVersion: "bundle-v2",
			},
		)

	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		sameRevisionDifferentBundle,
	)

	requireClassificationResultWriterCurrent(
		t,
		ctx,
		repository,
		newer,
	)

	shadow := newClassificationResultWriterIntegrationRun(
		t,
		classificationResultWriterIntegrationRunSpec{
			objectID: objectID,
			revision: 13,
			mode:     domain.ExecutionModeShadow,
		},
	)

	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		shadow,
	)

	reprocess := newClassificationResultWriterIntegrationRun(
		t,
		classificationResultWriterIntegrationRunSpec{
			objectID: objectID,
			revision: 14,
			mode:     domain.ExecutionModeReprocess,
		},
	)

	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		reprocess,
	)

	// SHADOW 和 REPROCESS 即使 revision 更新，也不能推进 Current。
	requireClassificationResultWriterCurrent(
		t,
		ctx,
		repository,
		newer,
	)

	// 验证 REUSED_PREVIOUS 的来源 Run 外键和 Writer 映射。
	reused := newClassificationResultWriterIntegrationRun(
		t,
		classificationResultWriterIntegrationRunSpec{
			objectID: objectID,
			revision: 15,
			mode:     domain.ExecutionModeProduction,
			source:   &newer,
		},
	)

	writeClassificationResultIntegrationMessage(
		t,
		ctx,
		writer,
		reused,
	)

	requireClassificationResultWriterCurrent(
		t,
		ctx,
		repository,
		reused,
	)

	requireClassificationResultWriterRunCount(
		t,
		ctx,
		pool,
		7,
	)
}

type classificationResultWriterIntegrationRunSpec struct {
	objectID string
	revision int64
	mode     domain.ExecutionMode

	bundleVersion string
	source        *domain.ClassificationRun
}

func newClassificationResultWriterIntegrationRun(
	t *testing.T,
	spec classificationResultWriterIntegrationRunSpec,
) domain.ClassificationRun {
	t.Helper()

	if spec.bundleVersion == "" {
		spec.bundleVersion = "bundle-v1"
	}

	jobID, err := domain.GenerateJobID(
		domain.JobIdentity{
			ObjectID:           spec.objectID,
			LightCurveRevision: spec.revision,
			ModelBundleVersion: spec.bundleVersion,
			ExecutionMode:      spec.mode,
		},
	)
	if err != nil {
		t.Fatalf("GenerateJobID() error = %v", err)
	}

	runID, err := domain.GenerateRunID(jobID)
	if err != nil {
		t.Fatalf("GenerateRunID() error = %v", err)
	}

	effectiveEpochCount := uint32(3)

	coarseSourceType :=
		domain.CoarseSourceComputedCurrent

	var coarseSourceRunID *domain.RunID

	coarseSourceRevision := spec.revision
	coarseSourceEpochCount := effectiveEpochCount
	xgboostExecuted := true

	if spec.source != nil {
		effectiveEpochCount = 21
		coarseSourceType =
			domain.CoarseSourceReusedPrevious

		copiedSourceRunID := spec.source.RunID
		coarseSourceRunID = &copiedSourceRunID

		coarseSourceRevision =
			spec.source.LightCurveRevision

		coarseSourceEpochCount =
			spec.source.EffectiveEpochCount

		xgboostExecuted = false
	}

	return domain.ClassificationRun{
		RunID:    runID,
		JobID:    jobID,
		ObjectID: spec.objectID,

		CandidateRevision: spec.revision,

		LightCurveRevision: spec.revision,

		EffectiveEpochCount: effectiveEpochCount,

		ExecutionMode: spec.mode,

		CoarseSourceType: coarseSourceType,

		CoarseSourceRunID: coarseSourceRunID,

		CoarseSourceLightCurveRevision: coarseSourceRevision,

		CoarseSourceEpochCount: coarseSourceEpochCount,

		XGBoostExecuted: xgboostExecuted,

		Versions: domain.ResolvedModelVersions{
			ModelBundleVersion: spec.bundleVersion,
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
			0.6,
			0.4,
			0.6,
			0.4,
			0.6,
			0.4,
			0.6,
			0.4,
			0.6,
			0.4,
		},

		LeafProbabilities: [domain.LeafProbabilityCount]float32{
			0.06,
			0.04,
			0.06,
			0.04,
			0.06,
			0.04,
			0.06,
			0.04,
			0.06,
			0.04,
			0.4,
			0.1,
		},

		PredictedCoarseClass: domain.CoarseClassCataclysmic,

		PredictedLeafClass: domain.LeafClassCataclysmic,

		CompletedAt: time.Date(
			2026,
			time.August,
			6,
			12,
			0,
			0,
			0,
			time.UTC,
		).Add(
			time.Duration(spec.revision) *
				time.Second,
		),
	}
}

func writeClassificationResultIntegrationMessage(
	t *testing.T,
	ctx context.Context,
	writer *application.ClassificationResultWriterHandler,
	run domain.ClassificationRun,
) {
	t.Helper()

	outbound, err :=
		application.BuildClassificationResultMessage(
			classificationResultWriterIntegrationTopic,
			run,
			application.TraceContext{
				TraceID: "postgres-integration-trace",
			},
			nil,
		)
	if err != nil {
		t.Fatalf(
			"BuildClassificationResultMessage() error = %v",
			err,
		)
	}

	message := application.InboundMessage{
		Topic: outbound.Topic,

		Key: append(
			[]byte(nil),
			outbound.Key...,
		),

		Value: append(
			[]byte(nil),
			outbound.Value...,
		),

		Headers: outbound.Headers,

		Timestamp: outbound.Timestamp,
	}

	if err := writer.Handle(ctx, message); err != nil {
		t.Fatalf(
			"ClassificationResultWriterHandler.Handle() error = %v",
			err,
		)
	}
}

func requireClassificationResultWriterCurrent(
	t *testing.T,
	ctx context.Context,
	repository *ClassificationRepository,
	want domain.ClassificationRun,
) {
	t.Helper()

	current, err := repository.GetCurrent(
		ctx,
		want.ObjectID,
	)
	if err != nil {
		t.Fatalf("GetCurrent() error = %v", err)
	}

	if current.Run.RunID != want.RunID {
		t.Fatalf(
			"current run ID = %q, want %q",
			current.Run.RunID,
			want.RunID,
		)
	}

	if current.Run.JobID != want.JobID {
		t.Fatalf(
			"current job ID = %q, want %q",
			current.Run.JobID,
			want.JobID,
		)
	}

	if current.Run.LightCurveRevision !=
		want.LightCurveRevision {
		t.Fatalf(
			"current revision = %d, want %d",
			current.Run.LightCurveRevision,
			want.LightCurveRevision,
		)
	}

	if current.Run.Versions.ModelBundleVersion !=
		want.Versions.ModelBundleVersion {
		t.Fatalf(
			"current model bundle version = %q, want %q",
			current.Run.Versions.ModelBundleVersion,
			want.Versions.ModelBundleVersion,
		)
	}
}

func requireClassificationResultWriterRunCount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	want int,
) {
	t.Helper()

	var got int

	if err := pool.QueryRow(
		ctx,
		`SELECT COUNT(*) FROM classification_runs`,
	).Scan(&got); err != nil {
		t.Fatalf(
			"count classification runs: %v",
			err,
		)
	}

	if got != want {
		t.Fatalf(
			"classification run count = %d, want %d",
			got,
			want,
		)
	}
}

func resetClassificationResultWriterIntegrationTables(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
) {
	t.Helper()

	if _, err := pool.Exec(
		ctx,
		`TRUNCATE TABLE current_classifications, classification_runs`,
	); err != nil {
		t.Fatalf(
			"reset classification tables: %v",
			err,
		)
	}
}
