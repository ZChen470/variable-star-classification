package application_test

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

func TestPrepareLightCurveRevisionSortsOnlyByObservationTime(
	t *testing.T,
) {
	qualityPolicyVersion := "quality-policy-v1"

	revision := domain.LightCurveRevision{
		ObjectID:             "OBJ-0001",
		Revision:             21,
		EligibleEpochCount:   3,
		QualityPolicyVersion: &qualityPolicyVersion,
		Epochs: []domain.LightCurveEpoch{
			{
				ObservationTime: 60423.3,
				Magnitude:       17.53,
				MagnitudeError:  0.05,
			},
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
		},
	}

	originalEpochs := append(
		[]domain.LightCurveEpoch(nil),
		revision.Epochs...,
	)

	got, err := application.PrepareLightCurveRevision(revision, 3)
	if err != nil {
		t.Fatalf("PrepareLightCurveRevision() error = %v", err)
	}

	want := revision
	want.Epochs = []domain.LightCurveEpoch{
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
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf(
			"PrepareLightCurveRevision() = %#v, want %#v",
			got,
			want,
		)
	}

	if !reflect.DeepEqual(revision.Epochs, originalEpochs) {
		t.Fatalf(
			"input epochs were modified: got %#v, want %#v",
			revision.Epochs,
			originalEpochs,
		)
	}

	got.Epochs[0].Magnitude = 99

	if !reflect.DeepEqual(revision.Epochs, originalEpochs) {
		t.Fatalf(
			"input epochs changed after output mutation: got %#v, want %#v",
			revision.Epochs,
			originalEpochs,
		)
	}
}

func TestPrepareLightCurveRevisionRejectsCountMismatch(t *testing.T) {
	tests := []struct {
		name     string
		declared uint32
		revision domain.LightCurveRevision
	}{
		{
			name:     "command differs from revision metadata",
			declared: 4,
			revision: domain.LightCurveRevision{
				EligibleEpochCount: 3,
				Epochs:             validEpochs(3),
			},
		},
		{
			name:     "revision metadata differs from actual length",
			declared: 4,
			revision: domain.LightCurveRevision{
				EligibleEpochCount: 4,
				Epochs:             validEpochs(3),
			},
		},
		{
			name:     "actual length differs from both declarations",
			declared: 3,
			revision: domain.LightCurveRevision{
				EligibleEpochCount: 3,
				Epochs:             validEpochs(4),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := application.PrepareLightCurveRevision(
				test.revision,
				test.declared,
			)
			if !errors.Is(
				err,
				application.ErrLightCurveEpochCountMismatch,
			) {
				t.Fatalf(
					"PrepareLightCurveRevision() error = %v, want %v",
					err,
					application.ErrLightCurveEpochCountMismatch,
				)
			}
			if !reflect.DeepEqual(
				got,
				domain.LightCurveRevision{},
			) {
				t.Fatalf(
					"PrepareLightCurveRevision() = %#v, want zero value",
					got,
				)
			}
		})
	}
}

func TestPrepareLightCurveRevisionRejectsEpochCountOutsideRange(
	t *testing.T,
) {
	tests := []struct {
		name  string
		count int
		err   error
	}{
		{
			name:  "below minimum",
			count: 2,
			err:   application.ErrInsufficientLightCurveEpochs,
		},
		{
			name:  "above maximum",
			count: 1025,
			err:   application.ErrTooManyLightCurveEpochs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			revision := domain.LightCurveRevision{
				EligibleEpochCount: uint32(test.count),
				Epochs:             validEpochs(test.count),
			}

			_, err := application.PrepareLightCurveRevision(
				revision,
				uint32(test.count),
			)
			if !errors.Is(err, test.err) {
				t.Fatalf(
					"PrepareLightCurveRevision() error = %v, want %v",
					err,
					test.err,
				)
			}
		})
	}
}

func TestPrepareLightCurveRevisionRejectsInvalidEpochValues(
	t *testing.T,
) {
	tests := []struct {
		name    string
		mutate  func([]domain.LightCurveEpoch)
		wantErr error
	}{
		{
			name: "observation time NaN",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].ObservationTime = math.NaN()
			},
			wantErr: application.ErrInvalidObservationTime,
		},
		{
			name: "observation time infinity",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].ObservationTime = math.Inf(1)
			},
			wantErr: application.ErrInvalidObservationTime,
		},
		{
			name: "magnitude NaN",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].Magnitude = float32(math.NaN())
			},
			wantErr: application.ErrInvalidMagnitude,
		},
		{
			name: "magnitude infinity",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].Magnitude = float32(math.Inf(-1))
			},
			wantErr: application.ErrInvalidMagnitude,
		},
		{
			name: "magnitude error NaN",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].MagnitudeError = float32(math.NaN())
			},
			wantErr: application.ErrInvalidMagnitudeError,
		},
		{
			name: "magnitude error infinity",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].MagnitudeError = float32(math.Inf(1))
			},
			wantErr: application.ErrInvalidMagnitudeError,
		},
		{
			name: "magnitude error zero",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].MagnitudeError = 0
			},
			wantErr: application.ErrInvalidMagnitudeError,
		},
		{
			name: "magnitude error negative",
			mutate: func(epochs []domain.LightCurveEpoch) {
				epochs[1].MagnitudeError = -0.01
			},
			wantErr: application.ErrInvalidMagnitudeError,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			epochs := validEpochs(3)
			test.mutate(epochs)

			revision := domain.LightCurveRevision{
				EligibleEpochCount: 3,
				Epochs:             epochs,
			}

			_, err := application.PrepareLightCurveRevision(
				revision,
				3,
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"PrepareLightCurveRevision() error = %v, want %v",
					err,
					test.wantErr,
				)
			}
		})
	}
}

func TestPrepareLightCurveRevisionRejectsDuplicateObservationTime(
	t *testing.T,
) {
	revision := domain.LightCurveRevision{
		ObjectID:           "OBJ-0002",
		Revision:           7,
		EligibleEpochCount: 3,
		Epochs: []domain.LightCurveEpoch{
			{
				ObservationTime: 60422.2,
				Magnitude:       17.42,
				MagnitudeError:  0.04,
			},
			{
				ObservationTime: 60421.1,
				Magnitude:       17.31,
				MagnitudeError:  0.03,
			},
			{
				ObservationTime: 60422.2,
				Magnitude:       18.99,
				MagnitudeError:  0.91,
			},
		},
	}

	originalEpochs := append(
		[]domain.LightCurveEpoch(nil),
		revision.Epochs...,
	)

	got, err := application.PrepareLightCurveRevision(revision, 3)
	if !errors.Is(
		err,
		application.ErrDuplicateObservationTime,
	) {
		t.Fatalf(
			"PrepareLightCurveRevision() error = %v, want %v",
			err,
			application.ErrDuplicateObservationTime,
		)
	}
	if !reflect.DeepEqual(got, domain.LightCurveRevision{}) {
		t.Fatalf(
			"PrepareLightCurveRevision() = %#v, want zero value",
			got,
		)
	}

	if !reflect.DeepEqual(revision.Epochs, originalEpochs) {
		t.Fatalf(
			"input epochs were modified: got %#v, want %#v",
			revision.Epochs,
			originalEpochs,
		)
	}
}

func validEpochs(count int) []domain.LightCurveEpoch {
	epochs := make([]domain.LightCurveEpoch, count)

	for index := range epochs {
		epochs[index] = domain.LightCurveEpoch{
			ObservationTime: 60421 + float64(index),
			Magnitude:       17 + float32(index)/100,
			MagnitudeError:  0.03,
		}
	}

	return epochs
}

func TestPrepareLightCurveRevisionAcceptsEpochCountBoundaries(
	t *testing.T,
) {
	tests := []struct {
		name  string
		count int
	}{
		{
			name:  "minimum",
			count: 3,
		},
		{
			name:  "maximum",
			count: 1024,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			epochs := validEpochs(test.count)

			// 反转输入，确保边界数量下仍执行确定性时间排序。
			for left, right := 0, len(epochs)-1; left < right; {
				epochs[left], epochs[right] =
					epochs[right], epochs[left]
				left++
				right--
			}

			revision := domain.LightCurveRevision{
				ObjectID:           "OBJ-BOUNDARY",
				Revision:           31,
				EligibleEpochCount: uint32(test.count),
				Epochs:             epochs,
			}

			got, err := application.PrepareLightCurveRevision(
				revision,
				uint32(test.count),
			)
			if err != nil {
				t.Fatalf(
					"PrepareLightCurveRevision() error = %v",
					err,
				)
			}

			if len(got.Epochs) != test.count {
				t.Fatalf(
					"prepared epoch count = %d, want %d",
					len(got.Epochs),
					test.count,
				)
			}

			for index := 1; index < len(got.Epochs); index++ {
				if got.Epochs[index-1].ObservationTime >=
					got.Epochs[index].ObservationTime {
					t.Fatalf(
						"epochs not strictly increasing at indices %d,%d: %v >= %v",
						index-1,
						index,
						got.Epochs[index-1].ObservationTime,
						got.Epochs[index].ObservationTime,
					)
				}
			}
		})
	}
}

func TestPrepareLightCurveRevisionRejectsNilAndEmptyEpochsAsInsufficient(
	t *testing.T,
) {
	tests := []struct {
		name   string
		epochs []domain.LightCurveEpoch
	}{
		{
			name:   "nil epochs",
			epochs: nil,
		},
		{
			name:   "non nil empty epochs",
			epochs: []domain.LightCurveEpoch{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			revision := domain.LightCurveRevision{
				EligibleEpochCount: 0,
				Epochs:             test.epochs,
			}

			got, err := application.PrepareLightCurveRevision(
				revision,
				0,
			)
			if !errors.Is(
				err,
				application.ErrInsufficientLightCurveEpochs,
			) {
				t.Fatalf(
					"PrepareLightCurveRevision() error = %v, want %v",
					err,
					application.ErrInsufficientLightCurveEpochs,
				)
			}

			if errors.Is(
				err,
				application.ErrLightCurveEpochCountMismatch,
			) {
				t.Fatalf(
					"PrepareLightCurveRevision() error unexpectedly matches %v",
					application.ErrLightCurveEpochCountMismatch,
				)
			}

			if !reflect.DeepEqual(
				got,
				domain.LightCurveRevision{},
			) {
				t.Fatalf(
					"PrepareLightCurveRevision() = %#v, want zero value",
					got,
				)
			}
		})
	}
}
