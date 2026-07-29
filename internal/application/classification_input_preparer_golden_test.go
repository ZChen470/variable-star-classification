package application_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakelightcurve"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakemodelbundle"
)

func TestClassificationInputPreparerGoldenVectors(t *testing.T) {
	for _, scenario := range classificationInputGoldenScenarios() {
		t.Run(scenario.name, func(t *testing.T) {
			repositoryRequest := fakelightcurve.Request{
				ObjectID: scenario.request.ObjectID,
				Revision: scenario.request.LightCurveRevision,
			}

			repository := fakelightcurve.New(
				map[fakelightcurve.Request]fakelightcurve.Response{
					repositoryRequest: {
						Revision: scenario.sourceRevision,
					},
				},
			)

			resolver := fakemodelbundle.New(
				map[string]fakemodelbundle.Response{
					scenario.request.ModelBundleVersion: {
						Metadata: scenario.metadata,
					},
				},
			)

			finder := &preparationCompatibleCoarseFinder{
				result: scenario.finderResult,
				err:    scenario.finderErr,
			}

			preparer := newClassificationInputPreparerForTest(
				t,
				repository,
				resolver,
				finder,
			)

			first, err := preparer.Prepare(
				context.Background(),
				scenario.request,
			)
			if err != nil {
				t.Fatalf("first Prepare() error = %v", err)
			}
			assertPreparedClassificationInputGolden(
				t,
				first,
				scenario.want,
			)

			second, err := preparer.Prepare(
				context.Background(),
				scenario.request,
			)
			if err != nil {
				t.Fatalf("second Prepare() error = %v", err)
			}
			assertPreparedClassificationInputGolden(
				t,
				second,
				scenario.want,
			)

			if !reflect.DeepEqual(first, second) {
				t.Fatalf(
					"repeated Prepare() results differ:\nfirst = %#v\nsecond = %#v",
					first,
					second,
				)
			}

			// 修改第一次返回结果，验证它不会影响第二次结果、
			// Fake 中保存的固定 revision 或下一次准备结果。
			first.Revision.Epochs[0].ObservationTime = -1
			first.Revision.Epochs[0].Magnitude = -1
			first.Revision.Epochs[0].MagnitudeError = 99

			if first.Revision.QualityPolicyVersion != nil {
				*first.Revision.QualityPolicyVersion =
					"mutated-quality-policy"
			}

			first.Input.TimeMJD[0] = -1
			first.Input.Magnitude[0] = -1
			first.Input.MagnitudeError[0] = 99

			if first.Selection.ReusedCoarse != nil {
				first.Selection.ReusedCoarse.
					Probabilities[0] = 0.99
			}
			if first.Input.ReusedCoarseProbabilities != nil {
				first.Input.ReusedCoarseProbabilities[0] = 0.98
			}

			assertPreparedClassificationInputGolden(
				t,
				second,
				scenario.want,
			)

			third, err := preparer.Prepare(
				context.Background(),
				scenario.request,
			)
			if err != nil {
				t.Fatalf("third Prepare() error = %v", err)
			}
			assertPreparedClassificationInputGolden(
				t,
				third,
				scenario.want,
			)

			wantRepositoryCalls := []fakelightcurve.Request{
				repositoryRequest,
				repositoryRequest,
				repositoryRequest,
			}
			if calls := repository.Calls(); !reflect.DeepEqual(
				calls,
				wantRepositoryCalls,
			) {
				t.Fatalf(
					"repository calls = %#v, want %#v",
					calls,
					wantRepositoryCalls,
				)
			}

			wantResolverCalls := []string{
				scenario.request.ModelBundleVersion,
				scenario.request.ModelBundleVersion,
				scenario.request.ModelBundleVersion,
			}
			if calls := resolver.Calls(); !reflect.DeepEqual(
				calls,
				wantResolverCalls,
			) {
				t.Fatalf(
					"resolver calls = %#v, want %#v",
					calls,
					wantResolverCalls,
				)
			}

			var wantFinderCalls []application.CompatibleCoarseQuery
			if scenario.wantFinderQuery != nil {
				wantFinderCalls =
					[]application.CompatibleCoarseQuery{
						*scenario.wantFinderQuery,
						*scenario.wantFinderQuery,
						*scenario.wantFinderQuery,
					}
			}

			if !reflect.DeepEqual(
				finder.calls,
				wantFinderCalls,
			) {
				t.Fatalf(
					"finder calls = %#v, want %#v",
					finder.calls,
					wantFinderCalls,
				)
			}
		})
	}
}

func TestClassificationInputPreparerIsDeterministicAcrossEpochOrder(
	t *testing.T,
) {
	request := application.ClassificationInputPreparationRequest{
		ObjectID:                   "OBJ-GOLDEN-ORDER",
		LightCurveRevision:         501,
		DeclaredEligibleEpochCount: 5,
		ModelBundleVersion:         "bundle-golden-order-v1",
	}

	qualityPolicyVersion := "quality-golden-order-v1"
	metadata := goldenModelBundleMetadata(
		request.ModelBundleVersion,
	)

	sortedEpochs := []domain.LightCurveEpoch{
		{
			ObservationTime: 63001,
			Magnitude:       16.1,
			MagnitudeError:  0.01,
		},
		{
			ObservationTime: 63002,
			Magnitude:       16.2,
			MagnitudeError:  0.02,
		},
		{
			ObservationTime: 63003,
			Magnitude:       16.3,
			MagnitudeError:  0.03,
		},
		{
			ObservationTime: 63004,
			Magnitude:       16.4,
			MagnitudeError:  0.04,
		},
		{
			ObservationTime: 63005,
			Magnitude:       16.5,
			MagnitudeError:  0.05,
		},
	}

	orderVariants := [][]domain.LightCurveEpoch{
		{
			sortedEpochs[4],
			sortedEpochs[0],
			sortedEpochs[2],
			sortedEpochs[1],
			sortedEpochs[3],
		},
		{
			sortedEpochs[1],
			sortedEpochs[3],
			sortedEpochs[0],
			sortedEpochs[4],
			sortedEpochs[2],
		},
		{
			sortedEpochs[2],
			sortedEpochs[4],
			sortedEpochs[3],
			sortedEpochs[1],
			sortedEpochs[0],
		},
	}

	var baseline application.PreparedClassificationInput

	for index, epochs := range orderVariants {
		repositoryRequest := fakelightcurve.Request{
			ObjectID: request.ObjectID,
			Revision: request.LightCurveRevision,
		}

		repository := fakelightcurve.New(
			map[fakelightcurve.Request]fakelightcurve.Response{
				repositoryRequest: {
					Revision: domain.LightCurveRevision{
						ObjectID: request.ObjectID,
						Revision: request.LightCurveRevision,
						EligibleEpochCount: uint32(
							len(epochs),
						),
						QualityPolicyVersion: &qualityPolicyVersion,
						Epochs: append(
							[]domain.LightCurveEpoch(nil),
							epochs...,
						),
					},
				},
			},
		)

		resolver := fakemodelbundle.New(
			map[string]fakemodelbundle.Response{
				request.ModelBundleVersion: {
					Metadata: metadata,
				},
			},
		)

		finder := &preparationCompatibleCoarseFinder{}

		preparer := newClassificationInputPreparerForTest(
			t,
			repository,
			resolver,
			finder,
		)

		got, err := preparer.Prepare(
			context.Background(),
			request,
		)
		if err != nil {
			t.Fatalf(
				"variant %d Prepare() error = %v",
				index,
				err,
			)
		}

		if !reflect.DeepEqual(
			got.Revision.Epochs,
			sortedEpochs,
		) {
			t.Fatalf(
				"variant %d prepared epochs = %#v, want %#v",
				index,
				got.Revision.Epochs,
				sortedEpochs,
			)
		}

		if got.Selection.Mode !=
			application.CoarseModeComputeCurrent {
			t.Fatalf(
				"variant %d mode = %v, want %v",
				index,
				got.Selection.Mode,
				application.CoarseModeComputeCurrent,
			)
		}

		if len(finder.calls) != 0 {
			t.Fatalf(
				"variant %d finder call count = %d, want 0",
				index,
				len(finder.calls),
			)
		}

		if index == 0 {
			baseline = got
			continue
		}

		if !reflect.DeepEqual(got, baseline) {
			t.Fatalf(
				"variant %d result differs from baseline:\ngot = %#v\nbaseline = %#v",
				index,
				got,
				baseline,
			)
		}
	}
}

type classificationInputGoldenScenario struct {
	name string

	request        application.ClassificationInputPreparationRequest
	sourceRevision domain.LightCurveRevision
	metadata       application.ModelBundleMetadata

	finderResult application.CompatibleCoarseResult
	finderErr    error

	wantFinderQuery *application.CompatibleCoarseQuery
	want            application.PreparedClassificationInput
}

func classificationInputGoldenScenarios() []classificationInputGoldenScenario {
	currentRequest := application.ClassificationInputPreparationRequest{
		ObjectID:                   "OBJ-GOLDEN-CURRENT",
		LightCurveRevision:         101,
		DeclaredEligibleEpochCount: 3,
		ModelBundleVersion:         "bundle-golden-current-v1",
	}
	currentQualityPolicy := "quality-golden-current-v1"
	currentMetadata := goldenModelBundleMetadata(
		currentRequest.ModelBundleVersion,
	)
	currentExpectedEpochs := []domain.LightCurveEpoch{
		{
			ObservationTime: 59001.5,
			Magnitude:       16.1,
			MagnitudeError:  0.01,
		},
		{
			ObservationTime: 59002.5,
			Magnitude:       16.2,
			MagnitudeError:  0.02,
		},
		{
			ObservationTime: 59003.5,
			Magnitude:       16.3,
			MagnitudeError:  0.03,
		},
	}
	currentSourceRevision := domain.LightCurveRevision{
		ObjectID:             currentRequest.ObjectID,
		Revision:             currentRequest.LightCurveRevision,
		EligibleEpochCount:   3,
		QualityPolicyVersion: &currentQualityPolicy,
		Epochs: []domain.LightCurveEpoch{
			currentExpectedEpochs[2],
			currentExpectedEpochs[0],
			currentExpectedEpochs[1],
		},
	}
	currentWant := application.PreparedClassificationInput{
		Revision: domain.LightCurveRevision{
			ObjectID:             currentRequest.ObjectID,
			Revision:             currentRequest.LightCurveRevision,
			EligibleEpochCount:   3,
			QualityPolicyVersion: &currentQualityPolicy,
			Epochs: append(
				[]domain.LightCurveEpoch(nil),
				currentExpectedEpochs...,
			),
		},
		Selection: application.CoarseModeSelection{
			Mode:        application.CoarseModeComputeCurrent,
			ModelBundle: currentMetadata,
		},
		Input: goldenClassificationInput(
			currentExpectedEpochs,
			application.CoarseModeComputeCurrent,
			nil,
		),
	}

	reuseRequest := application.ClassificationInputPreparationRequest{
		ObjectID:                   "OBJ-GOLDEN-REUSE",
		LightCurveRevision:         202,
		DeclaredEligibleEpochCount: 21,
		ModelBundleVersion:         "bundle-golden-reuse-v1",
	}
	reuseQualityPolicy := "quality-golden-reuse-v1"
	reuseMetadata := goldenModelBundleMetadata(
		reuseRequest.ModelBundleVersion,
	)
	reuseExpectedEpochs := goldenEpochs(61001, 21)
	reuseSourceRevision := domain.LightCurveRevision{
		ObjectID:             reuseRequest.ObjectID,
		Revision:             reuseRequest.LightCurveRevision,
		EligibleEpochCount:   21,
		QualityPolicyVersion: &reuseQualityPolicy,
		Epochs:               reverseGoldenEpochs(reuseExpectedEpochs),
	}
	reusedCoarse := application.CompatibleCoarseResult{
		SourceRunID:              domain.RunID("golden-source-run"),
		SourceLightCurveRevision: 201,
		SourceEpochCount:         20,
		Probabilities: [domain.CoarseProbabilityCount]float32{
			0.10,
			0.20,
			0.15,
			0.05,
			0.25,
			0.15,
			0.10,
		},
	}
	reusedInputProbabilities := reusedCoarse.Probabilities
	reuseQuery := application.CompatibleCoarseQuery{
		ObjectID:                 reuseRequest.ObjectID,
		TargetLightCurveRevision: reuseRequest.LightCurveRevision,
		TaxonomyVersion:          reuseMetadata.TaxonomyVersion,
		XGBoostModelVersion:      reuseMetadata.XGBoostModelVersion,
		FeatureSchemaVersion:     reuseMetadata.FeatureSchemaVersion,
	}
	reuseWant := application.PreparedClassificationInput{
		Revision: domain.LightCurveRevision{
			ObjectID:             reuseRequest.ObjectID,
			Revision:             reuseRequest.LightCurveRevision,
			EligibleEpochCount:   21,
			QualityPolicyVersion: &reuseQualityPolicy,
			Epochs: append(
				[]domain.LightCurveEpoch(nil),
				reuseExpectedEpochs...,
			),
		},
		Selection: application.CoarseModeSelection{
			Mode:         application.CoarseModeReusePrevious,
			ModelBundle:  reuseMetadata,
			ReusedCoarse: &reusedCoarse,
		},
		Input: goldenClassificationInput(
			reuseExpectedEpochs,
			application.CoarseModeReusePrevious,
			&reusedInputProbabilities,
		),
	}

	bootstrapRequest :=
		application.ClassificationInputPreparationRequest{
			ObjectID:                   "OBJ-GOLDEN-BOOTSTRAP",
			LightCurveRevision:         303,
			DeclaredEligibleEpochCount: 21,
			ModelBundleVersion:         "bundle-golden-bootstrap-v1",
		}
	bootstrapQualityPolicy := "quality-golden-bootstrap-v1"
	bootstrapMetadata := goldenModelBundleMetadata(
		bootstrapRequest.ModelBundleVersion,
	)
	bootstrapExpectedEpochs := goldenEpochs(62001, 21)
	bootstrapSourceRevision := domain.LightCurveRevision{
		ObjectID:             bootstrapRequest.ObjectID,
		Revision:             bootstrapRequest.LightCurveRevision,
		EligibleEpochCount:   21,
		QualityPolicyVersion: &bootstrapQualityPolicy,
		Epochs: reverseGoldenEpochs(
			bootstrapExpectedEpochs,
		),
	}
	bootstrapQuery := application.CompatibleCoarseQuery{
		ObjectID:                 bootstrapRequest.ObjectID,
		TargetLightCurveRevision: bootstrapRequest.LightCurveRevision,
		TaxonomyVersion:          bootstrapMetadata.TaxonomyVersion,
		XGBoostModelVersion:      bootstrapMetadata.XGBoostModelVersion,
		FeatureSchemaVersion:     bootstrapMetadata.FeatureSchemaVersion,
	}
	bootstrapWant := application.PreparedClassificationInput{
		Revision: domain.LightCurveRevision{
			ObjectID:             bootstrapRequest.ObjectID,
			Revision:             bootstrapRequest.LightCurveRevision,
			EligibleEpochCount:   21,
			QualityPolicyVersion: &bootstrapQualityPolicy,
			Epochs: append(
				[]domain.LightCurveEpoch(nil),
				bootstrapExpectedEpochs...,
			),
		},
		Selection: application.CoarseModeSelection{
			Mode:        application.CoarseModeComputeBootstrap,
			ModelBundle: bootstrapMetadata,
		},
		Input: goldenClassificationInput(
			bootstrapExpectedEpochs,
			application.CoarseModeComputeBootstrap,
			nil,
		),
	}

	return []classificationInputGoldenScenario{
		{
			name:           "compute current",
			request:        currentRequest,
			sourceRevision: currentSourceRevision,
			metadata:       currentMetadata,
			want:           currentWant,
		},
		{
			name:            "reuse previous",
			request:         reuseRequest,
			sourceRevision:  reuseSourceRevision,
			metadata:        reuseMetadata,
			finderResult:    reusedCoarse,
			wantFinderQuery: &reuseQuery,
			want:            reuseWant,
		},
		{
			name:            "compute bootstrap",
			request:         bootstrapRequest,
			sourceRevision:  bootstrapSourceRevision,
			metadata:        bootstrapMetadata,
			finderErr:       application.ErrCompatibleCoarseNotFound,
			wantFinderQuery: &bootstrapQuery,
			want:            bootstrapWant,
		},
	}
}

func goldenModelBundleMetadata(
	modelBundleVersion string,
) application.ModelBundleMetadata {
	return application.ModelBundleMetadata{
		ModelBundleVersion:   modelBundleVersion,
		TaxonomyVersion:      "taxonomy-golden-v1",
		XGBoostModelVersion:  "xgboost-golden-v1",
		FeatureSchemaVersion: "feature-golden-v1",
	}
}

func goldenEpochs(
	firstObservationTime float64,
	count int,
) []domain.LightCurveEpoch {
	epochs := make([]domain.LightCurveEpoch, count)

	for index := range epochs {
		epochs[index] = domain.LightCurveEpoch{
			ObservationTime: firstObservationTime +
				float64(index),
			Magnitude: 15 + float32(index)/10,
			MagnitudeError: 0.01 +
				float32(index)/1000,
		}
	}

	return epochs
}

func reverseGoldenEpochs(
	epochs []domain.LightCurveEpoch,
) []domain.LightCurveEpoch {
	reversed := append(
		[]domain.LightCurveEpoch(nil),
		epochs...,
	)

	for left, right := 0, len(reversed)-1; left < right; left, right = left+1, right-1 {
		reversed[left], reversed[right] =
			reversed[right], reversed[left]
	}

	return reversed
}

func goldenClassificationInput(
	epochs []domain.LightCurveEpoch,
	mode application.CoarseMode,
	reusedProbabilities *[application.CoarseClassCount]float32,
) application.ClassificationInput {
	input := application.ClassificationInput{
		TimeMJD:        make([]float64, len(epochs)),
		Magnitude:      make([]float32, len(epochs)),
		MagnitudeError: make([]float32, len(epochs)),
		CoarseMode:     mode,
	}

	for index, epoch := range epochs {
		input.TimeMJD[index] = epoch.ObservationTime
		input.Magnitude[index] = epoch.Magnitude
		input.MagnitudeError[index] = epoch.MagnitudeError
	}

	if reusedProbabilities != nil {
		copied := *reusedProbabilities
		input.ReusedCoarseProbabilities = &copied
	}

	return input
}

func assertPreparedClassificationInputGolden(
	t *testing.T,
	got application.PreparedClassificationInput,
	want application.PreparedClassificationInput,
) {
	t.Helper()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"PreparedClassificationInput differs from Golden Vector:\ngot = %#v\nwant = %#v",
			got,
			want,
		)
	}
}
