package application_test

import (
	"context"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

// TestClassificationCommandToResultWriterApplicationE2E 验证纯应用层
// ClassificationCommand → ClassificationRun 成功闭环。
//
// 本测试使用：
//   - Fake LightCurveRepository；
//   - Fake ModelBundleResolver；
//   - Fake ServingBundleResolver；
//   - Fake VariableStarClassifier；
//   - 内存 Result Publisher；
//   - Fake ClassificationRunSaver。
//
// 它不代表真实 Kafka、PostgreSQL、LightCurve HTTP 或 Triton 联合 E2E。
func TestClassificationCommandToResultWriterApplicationE2E(
	t *testing.T,
) {
	fixture := newClassificationWorkerFixture(
		t,
		classificationWorkerFixtureOptions{},
	)

	ctx := context.Background()

	// 第一段：
	//
	// Command
	// → InputPreparer
	// → Classifier
	// → ClassificationResult
	// → Publisher
	if err := fixture.handler.Handle(
		ctx,
		fixture.message,
	); err != nil {
		t.Fatalf(
			"ClassificationWorkerHandler.Handle() error = %v",
			err,
		)
	}

	published := fixture.publisher.Calls()
	if len(published) != 1 {
		t.Fatalf(
			"published Result count = %d, want 1",
			len(published),
		)
	}

	resultMessage := published[0]

	repository :=
		&classificationResultWriterTestRepository{
			result: application.SaveRunResult{
				RunInserted:     true,
				CurrentAdvanced: true,
			},
		}

	writer, err :=
		application.NewClassificationResultWriterHandler(
			testWorkerResultTopic,
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewClassificationResultWriterHandler() error = %v",
			err,
		)
	}

	// 模拟 Result Kafka 消费边界。
	inboundResult := application.InboundMessage{
		Topic: resultMessage.Topic,

		Partition: 4,
		Offset:    52,

		Key: append(
			[]byte(nil),
			resultMessage.Key...,
		),

		Value: append(
			[]byte(nil),
			resultMessage.Value...,
		),

		Headers: cloneApplicationE2EHeaders(
			resultMessage.Headers,
		),

		Timestamp: resultMessage.Timestamp,
	}

	// 第二段：
	//
	// ClassificationResult
	// → 独立 Decoder
	// → ClassificationRun
	// → Repository
	if err := writer.Handle(
		ctx,
		inboundResult,
	); err != nil {
		t.Fatalf(
			"ClassificationResultWriterHandler.Handle() error = %v",
			err,
		)
	}

	if len(repository.runs) != 1 {
		t.Fatalf(
			"saved Run count = %d, want 1",
			len(repository.runs),
		)
	}

	run := repository.runs[0]

	expectedJobID :=
		domain.JobID(fixture.command.GetJobId())

	expectedRunID, err :=
		domain.GenerateRunID(expectedJobID)
	if err != nil {
		t.Fatalf(
			"GenerateRunID() error = %v",
			err,
		)
	}

	switch {
	case run.JobID != expectedJobID:
		t.Fatalf(
			"Run JobID = %q, want %q",
			run.JobID,
			expectedJobID,
		)

	case run.RunID != expectedRunID:
		t.Fatalf(
			"Run RunID = %q, want %q",
			run.RunID,
			expectedRunID,
		)

	case run.ObjectID !=
		fixture.command.GetObjectId():
		t.Fatalf(
			"Run ObjectID = %q, want %q",
			run.ObjectID,
			fixture.command.GetObjectId(),
		)

	case run.CandidateRevision !=
		fixture.command.GetCandidateRevision():
		t.Fatalf(
			"Run CandidateRevision = %d, want %d",
			run.CandidateRevision,
			fixture.command.GetCandidateRevision(),
		)

	case run.LightCurveRevision !=
		fixture.command.GetLightCurveRevision():
		t.Fatalf(
			"Run LightCurveRevision = %d, want %d",
			run.LightCurveRevision,
			fixture.command.GetLightCurveRevision(),
		)

	case run.EffectiveEpochCount !=
		fixture.command.GetDeclaredEligibleEpochCount():
		t.Fatalf(
			"Run EffectiveEpochCount = %d, want %d",
			run.EffectiveEpochCount,
			fixture.command.
				GetDeclaredEligibleEpochCount(),
		)

	case run.ExecutionMode !=
		domain.ExecutionModeProduction:
		t.Fatalf(
			"Run ExecutionMode = %d, want PRODUCTION",
			run.ExecutionMode,
		)

	case run.Versions.ModelBundleVersion !=
		fixture.command.GetModelBundleVersion():
		t.Fatalf(
			"Run ModelBundleVersion = %q, want %q",
			run.Versions.ModelBundleVersion,
			fixture.command.GetModelBundleVersion(),
		)
	}

	if run.CoarseSourceType !=
		domain.CoarseSourceComputedCurrent {
		t.Fatalf(
			"Run CoarseSourceType = %d, want COMPUTED_CURRENT",
			run.CoarseSourceType,
		)
	}

	if run.CoarseSourceRunID != nil {
		t.Fatalf(
			"Run CoarseSourceRunID = %q, want nil",
			*run.CoarseSourceRunID,
		)
	}

	if run.CoarseSourceLightCurveRevision !=
		run.LightCurveRevision {
		t.Fatalf(
			"source revision = %d, want current revision %d",
			run.CoarseSourceLightCurveRevision,
			run.LightCurveRevision,
		)
	}

	if run.CoarseSourceEpochCount !=
		run.EffectiveEpochCount {
		t.Fatalf(
			"source epoch count = %d, want effective count %d",
			run.CoarseSourceEpochCount,
			run.EffectiveEpochCount,
		)
	}

	if !run.XGBoostExecuted {
		t.Fatal(
			"Run XGBoostExecuted = false, want true",
		)
	}

	expectedOutput :=
		validClassificationWorkerOutput()

	if run.CoarseProbabilities !=
		expectedOutput.CoarseProbabilities {
		t.Fatalf(
			"CoarseProbabilities = %#v, want %#v",
			run.CoarseProbabilities,
			expectedOutput.CoarseProbabilities,
		)
	}

	if run.FineConditionalProbabilities !=
		expectedOutput.
			ConditionalFineProbabilities {
		t.Fatalf(
			"FineConditionalProbabilities = %#v, want %#v",
			run.FineConditionalProbabilities,
			expectedOutput.
				ConditionalFineProbabilities,
		)
	}

	if run.LeafProbabilities !=
		expectedOutput.LeafProbabilities {
		t.Fatalf(
			"LeafProbabilities = %#v, want %#v",
			run.LeafProbabilities,
			expectedOutput.LeafProbabilities,
		)
	}

	if run.PredictedCoarseClass !=
		domain.CoarseClassPulsating {
		t.Fatalf(
			"PredictedCoarseClass = %d, want PULSATING",
			run.PredictedCoarseClass,
		)
	}

	if run.PredictedLeafClass !=
		domain.LeafClassCataclysmic {
		t.Fatalf(
			"PredictedLeafClass = %d, want CATACLYSMIC",
			run.PredictedLeafClass,
		)
	}

	if !run.CompletedAt.Equal(
		fixture.completedAt,
	) {
		t.Fatalf(
			"CompletedAt = %v, want %v",
			run.CompletedAt,
			fixture.completedAt,
		)
	}

	requestIDs := fixture.classifier.RequestIDs()

	if len(requestIDs) != 1 ||
		requestIDs[0] !=
			fixture.command.GetJobId() {
		t.Fatalf(
			"classifier request IDs = %#v, want [%q]",
			requestIDs,
			fixture.command.GetJobId(),
		)
	}
}

func cloneApplicationE2EHeaders(
	headers []application.MessageHeader,
) []application.MessageHeader {
	if headers == nil {
		return nil
	}

	cloned := make(
		[]application.MessageHeader,
		len(headers),
	)

	for index, header := range headers {
		cloned[index] = application.MessageHeader{
			Key: header.Key,
		}

		if header.Value != nil {
			cloned[index].Value = append(
				[]byte(nil),
				header.Value...,
			)
		}
	}

	return cloned
}
