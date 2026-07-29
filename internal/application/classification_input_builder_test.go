package application_test

import (
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestBuildClassificationInputMapsPreparedEpochsForComputeModes(
	t *testing.T,
) {
	tests := []struct {
		name string
		mode application.CoarseMode
	}{
		{
			name: "compute current",
			mode: application.CoarseModeComputeCurrent,
		},
		{
			name: "compute bootstrap",
			mode: application.CoarseModeComputeBootstrap,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			revision := preparedRevisionForClassificationInput()

			got, err := application.BuildClassificationInput(
				revision,
				application.CoarseModeSelection{
					Mode: test.mode,
				},
			)
			if err != nil {
				t.Fatalf(
					"BuildClassificationInput() error = %v",
					err,
				)
			}

			want := application.ClassificationInput{
				TimeMJD: []float64{
					60421.1,
					60422.2,
					60423.3,
				},
				Magnitude: []float32{
					17.31,
					17.42,
					17.53,
				},
				MagnitudeError: []float32{
					0.03,
					0.04,
					0.05,
				},
				CoarseMode: test.mode,
			}

			if !reflect.DeepEqual(got, want) {
				t.Fatalf(
					"BuildClassificationInput() = %#v, want %#v",
					got,
					want,
				)
			}

			if got.ReusedCoarseProbabilities != nil {
				t.Fatalf(
					"reused probabilities = %#v, want nil",
					got.ReusedCoarseProbabilities,
				)
			}

			// 修改输入 revision 不得影响已经构造的分类输入。
			revision.Epochs[0].ObservationTime = 99999
			revision.Epochs[0].Magnitude = 99
			revision.Epochs[0].MagnitudeError = 99

			if got.TimeMJD[0] != want.TimeMJD[0] {
				t.Fatal("TimeMJD aliases revision Epochs")
			}
			if got.Magnitude[0] != want.Magnitude[0] {
				t.Fatal("Magnitude aliases revision Epochs")
			}
			if got.MagnitudeError[0] != want.MagnitudeError[0] {
				t.Fatal("MagnitudeError aliases revision Epochs")
			}

			// 修改返回结果也不得影响 revision。
			got.TimeMJD[1] = 88888
			got.Magnitude[1] = 88
			got.MagnitudeError[1] = 88

			if revision.Epochs[1].ObservationTime == 88888 {
				t.Fatal("revision ObservationTime aliases output")
			}
			if revision.Epochs[1].Magnitude == 88 {
				t.Fatal("revision Magnitude aliases output")
			}
			if revision.Epochs[1].MagnitudeError == 88 {
				t.Fatal("revision MagnitudeError aliases output")
			}
		})
	}
}

func TestBuildClassificationInputCopiesReusedCoarseProbabilities(
	t *testing.T,
) {
	reused := application.CompatibleCoarseResult{
		SourceRunID:              domain.RunID("source-run-001"),
		SourceLightCurveRevision: 19,
		SourceEpochCount:         20,
		Probabilities: [application.CoarseClassCount]float32{
			0.10,
			0.20,
			0.15,
			0.05,
			0.25,
			0.15,
			0.10,
		},
	}

	got, err := application.BuildClassificationInput(
		preparedRevisionForClassificationInput(),
		application.CoarseModeSelection{
			Mode:         application.CoarseModeReusePrevious,
			ReusedCoarse: &reused,
		},
	)
	if err != nil {
		t.Fatalf("BuildClassificationInput() error = %v", err)
	}

	if got.CoarseMode != application.CoarseModeReusePrevious {
		t.Fatalf(
			"CoarseMode = %v, want %v",
			got.CoarseMode,
			application.CoarseModeReusePrevious,
		)
	}
	if got.ReusedCoarseProbabilities == nil {
		t.Fatal("ReusedCoarseProbabilities = nil, want value")
	}
	if *got.ReusedCoarseProbabilities != reused.Probabilities {
		t.Fatalf(
			"ReusedCoarseProbabilities = %#v, want %#v",
			*got.ReusedCoarseProbabilities,
			reused.Probabilities,
		)
	}

	originalFirst := got.ReusedCoarseProbabilities[0]
	reused.Probabilities[0] = 0.99

	if got.ReusedCoarseProbabilities[0] != originalFirst {
		t.Fatal("classification input aliases source probabilities")
	}

	originalSecond := reused.Probabilities[1]
	got.ReusedCoarseProbabilities[1] = 0.88

	if reused.Probabilities[1] != originalSecond {
		t.Fatal("source probabilities alias classification input")
	}
}

func TestBuildClassificationInputRejectsInvalidCoarseModeSelection(
	t *testing.T,
) {
	reused := application.CompatibleCoarseResult{
		SourceRunID:              domain.RunID("source-run-002"),
		SourceLightCurveRevision: 20,
		SourceEpochCount:         20,
	}

	tests := []struct {
		name      string
		selection application.CoarseModeSelection
	}{
		{
			name: "unspecified mode",
			selection: application.CoarseModeSelection{
				Mode: application.CoarseModeUnspecified,
			},
		},
		{
			name: "compute current contains reused coarse",
			selection: application.CoarseModeSelection{
				Mode:         application.CoarseModeComputeCurrent,
				ReusedCoarse: &reused,
			},
		},
		{
			name: "compute bootstrap contains reused coarse",
			selection: application.CoarseModeSelection{
				Mode:         application.CoarseModeComputeBootstrap,
				ReusedCoarse: &reused,
			},
		},
		{
			name: "reuse previous missing reused coarse",
			selection: application.CoarseModeSelection{
				Mode: application.CoarseModeReusePrevious,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := application.BuildClassificationInput(
				preparedRevisionForClassificationInput(),
				test.selection,
			)
			if !errors.Is(
				err,
				application.ErrInvalidCoarseModeSelection,
			) {
				t.Fatalf(
					"BuildClassificationInput() error = %v, want %v",
					err,
					application.ErrInvalidCoarseModeSelection,
				)
			}

			if !reflect.DeepEqual(
				got,
				application.ClassificationInput{},
			) {
				t.Fatalf(
					"BuildClassificationInput() = %#v, want zero value",
					got,
				)
			}
		})
	}
}

func TestBuildClassificationInputPreservesPreparedEpochOrder(
	t *testing.T,
) {
	revision := domain.LightCurveRevision{
		ObjectID:           "OBJ-ORDER",
		Revision:           50,
		EligibleEpochCount: 3,
		Epochs: []domain.LightCurveEpoch{
			{
				ObservationTime: 3,
				Magnitude:       30,
				MagnitudeError:  0.3,
			},
			{
				ObservationTime: 1,
				Magnitude:       10,
				MagnitudeError:  0.1,
			},
			{
				ObservationTime: 2,
				Magnitude:       20,
				MagnitudeError:  0.2,
			},
		},
	}

	got, err := application.BuildClassificationInput(
		revision,
		application.CoarseModeSelection{
			Mode: application.CoarseModeComputeCurrent,
		},
	)
	if err != nil {
		t.Fatalf("BuildClassificationInput() error = %v", err)
	}

	// Builder 不承担排序职责，必须逐项保持调用方提供的顺序。
	wantTime := []float64{3, 1, 2}
	wantMagnitude := []float32{30, 10, 20}
	wantError := []float32{0.3, 0.1, 0.2}

	if !reflect.DeepEqual(got.TimeMJD, wantTime) {
		t.Fatalf("TimeMJD = %#v, want %#v", got.TimeMJD, wantTime)
	}
	if !reflect.DeepEqual(got.Magnitude, wantMagnitude) {
		t.Fatalf(
			"Magnitude = %#v, want %#v",
			got.Magnitude,
			wantMagnitude,
		)
	}
	if !reflect.DeepEqual(got.MagnitudeError, wantError) {
		t.Fatalf(
			"MagnitudeError = %#v, want %#v",
			got.MagnitudeError,
			wantError,
		)
	}
}

func preparedRevisionForClassificationInput() domain.LightCurveRevision {
	return domain.LightCurveRevision{
		ObjectID:           "OBJ-INPUT",
		Revision:           30,
		EligibleEpochCount: 3,
		Epochs: []domain.LightCurveEpoch{
			{
				ObservationTime: 60421.1,
				Magnitude:       17.31,
				MagnitudeError:  0.03,
			},
			{
				ObservationTime: 60422.2,
				Magnitude:       17.42,
				MagnitudeError:  0.04,
			},
			{
				ObservationTime: 60423.3,
				Magnitude:       17.53,
				MagnitudeError:  0.05,
			},
		},
	}
}
