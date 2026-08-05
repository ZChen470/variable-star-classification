package application_test

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestBuildClassificationRunMapsCoarseSources(
	t *testing.T,
) {
	tests := []struct {
		name string
		mode application.CoarseMode

		wantSourceType       domain.CoarseSourceType
		wantSourceRunID      *domain.RunID
		wantSourceRevision   int64
		wantSourceEpochCount uint32
		wantXGBoostExecuted  bool
	}{
		{
			name:                 "compute current",
			mode:                 application.CoarseModeComputeCurrent,
			wantSourceType:       domain.CoarseSourceComputedCurrent,
			wantSourceRevision:   101,
			wantSourceEpochCount: 3,
			wantXGBoostExecuted:  true,
		},
		{
			name:           "reuse previous",
			mode:           application.CoarseModeReusePrevious,
			wantSourceType: domain.CoarseSourceReusedPrevious,
			wantSourceRunID: runIDPointer(
				domain.RunID(
					"22222222-2222-2222-2222-222222222222",
				),
			),
			wantSourceRevision:   100,
			wantSourceEpochCount: 20,
			wantXGBoostExecuted:  false,
		},
		{
			name:                 "compute bootstrap",
			mode:                 application.CoarseModeComputeBootstrap,
			wantSourceType:       domain.CoarseSourceComputedBootstrap,
			wantSourceRevision:   101,
			wantSourceEpochCount: 21,
			wantXGBoostExecuted:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validClassificationRunBuildRequest(
				test.mode,
			)

			got, err := application.BuildClassificationRun(
				request,
			)
			if err != nil {
				t.Fatalf(
					"BuildClassificationRun() error = %v",
					err,
				)
			}

			wantRunID, err := domain.GenerateRunID(
				request.JobID,
			)
			if err != nil {
				t.Fatalf(
					"GenerateRunID() error = %v",
					err,
				)
			}

			if got.RunID != wantRunID {
				t.Fatalf(
					"RunID = %q, want %q",
					got.RunID,
					wantRunID,
				)
			}
			if got.CoarseSourceType != test.wantSourceType {
				t.Fatalf(
					"CoarseSourceType = %d, want %d",
					got.CoarseSourceType,
					test.wantSourceType,
				)
			}
			if !reflect.DeepEqual(
				got.CoarseSourceRunID,
				test.wantSourceRunID,
			) {
				t.Fatalf(
					"CoarseSourceRunID = %#v, want %#v",
					got.CoarseSourceRunID,
					test.wantSourceRunID,
				)
			}
			if got.CoarseSourceLightCurveRevision !=
				test.wantSourceRevision {
				t.Fatalf(
					"CoarseSourceLightCurveRevision = %d, want %d",
					got.CoarseSourceLightCurveRevision,
					test.wantSourceRevision,
				)
			}
			if got.CoarseSourceEpochCount !=
				test.wantSourceEpochCount {
				t.Fatalf(
					"CoarseSourceEpochCount = %d, want %d",
					got.CoarseSourceEpochCount,
					test.wantSourceEpochCount,
				)
			}
			if got.XGBoostExecuted !=
				test.wantXGBoostExecuted {
				t.Fatalf(
					"XGBoostExecuted = %v, want %v",
					got.XGBoostExecuted,
					test.wantXGBoostExecuted,
				)
			}

			if got.PredictedCoarseClass !=
				domain.CoarseClassPulsating {
				t.Fatalf(
					"PredictedCoarseClass = %d, want %d",
					got.PredictedCoarseClass,
					domain.CoarseClassPulsating,
				)
			}
			if got.PredictedLeafClass !=
				domain.LeafClassCataclysmic {
				t.Fatalf(
					"PredictedLeafClass = %d, want %d",
					got.PredictedLeafClass,
					domain.LeafClassCataclysmic,
				)
			}

			if got.PersistedAt != (time.Time{}) {
				t.Fatalf(
					"PersistedAt = %v, want zero",
					got.PersistedAt,
				)
			}
			if got.CompletedAt.Location() != time.UTC {
				t.Fatalf(
					"CompletedAt location = %v, want UTC",
					got.CompletedAt.Location(),
				)
			}
		})
	}
}

func TestBuildClassificationRunArgmaxUsesFirstMaximum(
	t *testing.T,
) {
	request := validClassificationRunBuildRequest(
		application.CoarseModeComputeCurrent,
	)

	request.Output.CoarseProbabilities =
		[domain.CoarseProbabilityCount]float32{
			0.4,
			0.4,
			0.1,
			0.05,
			0.02,
			0.02,
			0.01,
		}

	request.Output.LeafProbabilities =
		[domain.LeafProbabilityCount]float32{
			0.3,
			0.3,
			0.1,
			0.05,
			0.05,
			0.04,
			0.03,
			0.03,
			0.02,
			0.02,
			0.02,
			0.01,
		}

	got, err := application.BuildClassificationRun(request)
	if err != nil {
		t.Fatalf(
			"BuildClassificationRun() error = %v",
			err,
		)
	}

	if got.PredictedCoarseClass !=
		domain.CoarseClassRotating {
		t.Fatalf(
			"PredictedCoarseClass = %d, want first-index class %d",
			got.PredictedCoarseClass,
			domain.CoarseClassRotating,
		)
	}

	if got.PredictedLeafClass != domain.LeafClassEW {
		t.Fatalf(
			"PredictedLeafClass = %d, want first-index class %d",
			got.PredictedLeafClass,
			domain.LeafClassEW,
		)
	}
}

func TestBuildClassificationRunIsDeterministic(
	t *testing.T,
) {
	request := validClassificationRunBuildRequest(
		application.CoarseModeReusePrevious,
	)

	first, err := application.BuildClassificationRun(request)
	if err != nil {
		t.Fatalf(
			"first BuildClassificationRun() error = %v",
			err,
		)
	}

	second, err := application.BuildClassificationRun(request)
	if err != nil {
		t.Fatalf(
			"second BuildClassificationRun() error = %v",
			err,
		)
	}

	if !reflect.DeepEqual(first, second) {
		t.Fatalf(
			"repeated build differs:\nfirst  = %#v\nsecond = %#v",
			first,
			second,
		)
	}
}

func TestBuildClassificationRunRejectsInvalidStructure(
	t *testing.T,
) {
	tests := []struct {
		name      string
		mutate    func(*application.ClassificationRunBuildRequest)
		wantError error
	}{
		{
			name: "invalid job ID",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				request.JobID = "not-a-uuid"
			},
			wantError: application.ErrInvalidClassificationRunBuild,
		},
		{
			name: "bundle identity mismatch",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				request.ServingBundle.ModelBundleVersion =
					"other-bundle"
			},
			wantError: application.ErrInvalidClassificationRunBuild,
		},
		{
			name: "input mode mismatch",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				request.Prepared.Input.CoarseMode =
					application.CoarseModeComputeBootstrap
			},
			wantError: application.ErrInvalidClassificationRunBuild,
		},
		{
			name: "compute current did not execute xgboost",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				request.Output.XGBoostExecuted = false
			},
			wantError: application.ErrInvalidCoarseSourceMapping,
		},
		{
			name: "reuse previous executed xgboost",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				*request = validClassificationRunBuildRequest(
					application.CoarseModeReusePrevious,
				)
				request.Output.XGBoostExecuted = true
			},
			wantError: application.ErrInvalidCoarseSourceMapping,
		},
		{
			name: "bootstrap has insufficient epoch count",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				*request = validClassificationRunBuildRequest(
					application.CoarseModeComputeBootstrap,
				)
				truncatePreparedEpochs(
					&request.Prepared,
					3,
				)
			},
			wantError: application.ErrInvalidCoarseSourceMapping,
		},
		{
			name: "reuse previous missing source",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				*request = validClassificationRunBuildRequest(
					application.CoarseModeReusePrevious,
				)
				request.Prepared.Selection.ReusedCoarse = nil
			},
			wantError: application.ErrInvalidCoarseSourceMapping,
		},
		{
			name: "zero completed at",
			mutate: func(
				request *application.ClassificationRunBuildRequest,
			) {
				request.CompletedAt = time.Time{}
			},
			wantError: application.ErrInvalidClassificationRunBuild,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validClassificationRunBuildRequest(
				application.CoarseModeComputeCurrent,
			)
			test.mutate(&request)

			got, err := application.BuildClassificationRun(
				request,
			)
			if !errors.Is(err, test.wantError) {
				t.Fatalf(
					"BuildClassificationRun() error = %v, want %v",
					err,
					test.wantError,
				)
			}
			if !reflect.DeepEqual(
				got,
				domain.ClassificationRun{},
			) {
				t.Fatalf(
					"BuildClassificationRun() = %#v, want zero",
					got,
				)
			}
		})
	}
}

func validClassificationRunBuildRequest(
	mode application.CoarseMode,
) application.ClassificationRunBuildRequest {
	epochCount := 3
	xgboostExecuted := true

	if mode == application.CoarseModeReusePrevious ||
		mode == application.CoarseModeComputeBootstrap {
		epochCount = 21
	}
	if mode == application.CoarseModeReusePrevious {
		xgboostExecuted = false
	}

	epochs := make(
		[]domain.LightCurveEpoch,
		epochCount,
	)
	timeMJD := make([]float64, epochCount)
	magnitude := make([]float32, epochCount)
	magnitudeError := make([]float32, epochCount)

	for index := range epochs {
		observationTime := 60000.0 + float64(index)

		epochs[index] = domain.LightCurveEpoch{
			ObservationTime: observationTime,
			Magnitude:       18.0 + float32(index)/10,
			MagnitudeError:  0.01,
		}
		timeMJD[index] = observationTime
		magnitude[index] = epochs[index].Magnitude
		magnitudeError[index] =
			epochs[index].MagnitudeError
	}

	selection := application.CoarseModeSelection{
		Mode: mode,
		ModelBundle: application.ModelBundleMetadata{
			ModelBundleVersion: "bundle-v1",
		},
	}

	input := application.ClassificationInput{
		TimeMJD:        timeMJD,
		Magnitude:      magnitude,
		MagnitudeError: magnitudeError,
		CoarseMode:     mode,
	}

	if mode == application.CoarseModeReusePrevious {
		reused := application.CompatibleCoarseResult{
			SourceRunID: domain.RunID(
				"22222222-2222-2222-2222-222222222222",
			),
			SourceLightCurveRevision: 100,
			SourceEpochCount:         20,
			Probabilities: [domain.CoarseProbabilityCount]float32{
				0.1,
				0.1,
				0.2,
				0.1,
				0.3,
				0.1,
				0.1,
			},
		}

		selection.ReusedCoarse = &reused

		reusedProbabilities := reused.Probabilities
		input.ReusedCoarseProbabilities =
			&reusedProbabilities
	}

	return application.ClassificationRunBuildRequest{
		JobID: domain.JobID(
			"11111111-1111-1111-1111-111111111111",
		),
		CandidateRevision: 7,
		ExecutionMode:     domain.ExecutionModeProduction,

		Prepared: application.PreparedClassificationInput{
			Revision: domain.LightCurveRevision{
				ObjectID:           "OBJ-0001",
				Revision:           101,
				EligibleEpochCount: uint32(epochCount),
				Epochs:             epochs,
			},
			Selection: selection,
			Input:     input,
		},

		ServingBundle: application.ServingBundleMetadata{
			ModelBundleVersion: "bundle-v1",
		},

		Output: application.ClassificationOutput{
			CoarseProbabilities: [domain.CoarseProbabilityCount]float32{
				0.05,
				0.10,
				0.20,
				0.15,
				0.30,
				0.10,
				0.10,
			},
			ConditionalFineProbabilities: [domain.ConditionalFineProbabilityCount]float32{
				0.6,
				0.4,
				0.7,
				0.3,
				0.8,
				0.2,
				0.55,
				0.45,
				0.65,
				0.35,
			},
			LeafProbabilities: [domain.LeafProbabilityCount]float32{
				0.12,
				0.08,
				0.10,
				0.05,
				0.08,
				0.02,
				0.08,
				0.07,
				0.15,
				0.05,
				0.18,
				0.02,
			},
			XGBoostExecuted: xgboostExecuted,
		},

		CompletedAt: time.Date(
			2026,
			time.August,
			5,
			7,
			30,
			0,
			123000000,
			time.FixedZone("UTC+8", 8*60*60),
		),
	}
}

func truncatePreparedEpochs(
	prepared *application.PreparedClassificationInput,
	count int,
) {
	prepared.Revision.Epochs =
		prepared.Revision.Epochs[:count]
	prepared.Revision.EligibleEpochCount = uint32(count)

	prepared.Input.TimeMJD =
		prepared.Input.TimeMJD[:count]
	prepared.Input.Magnitude =
		prepared.Input.Magnitude[:count]
	prepared.Input.MagnitudeError =
		prepared.Input.MagnitudeError[:count]
}

func runIDPointer(value domain.RunID) *domain.RunID {
	return &value
}
