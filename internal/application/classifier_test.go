package application

import (
	"context"
	"testing"
)

type classifierFunc func(
	context.Context,
	ClassificationInput,
) (ClassificationOutput, error)

func (fn classifierFunc) Classify(
	ctx context.Context,
	input ClassificationInput,
) (ClassificationOutput, error) {
	return fn(ctx, input)
}

var _ VariableStarClassifier = classifierFunc(nil)

func TestCoarseModeIsValid(t *testing.T) {
	tests := []struct {
		name string
		mode CoarseMode
		want bool
	}{
		{
			name: "unspecified",
			mode: CoarseModeUnspecified,
			want: false,
		},
		{
			name: "compute current",
			mode: CoarseModeComputeCurrent,
			want: true,
		},
		{
			name: "reuse previous",
			mode: CoarseModeReusePrevious,
			want: true,
		},
		{
			name: "compute bootstrap",
			mode: CoarseModeComputeBootstrap,
			want: true,
		},
		{
			name: "unknown",
			mode: CoarseMode(255),
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mode.IsValid(); got != tt.want {
				t.Fatalf("CoarseMode.IsValid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestClassificationOutputDimensions(t *testing.T) {
	var output ClassificationOutput

	if got := len(output.CoarseProbabilities); got != CoarseClassCount {
		t.Fatalf(
			"coarse probability count = %d, want %d",
			got,
			CoarseClassCount,
		)
	}

	if got := len(output.ConditionalFineProbabilities); got != ConditionalFineClassCount {
		t.Fatalf(
			"conditional fine probability count = %d, want %d",
			got,
			ConditionalFineClassCount,
		)
	}

	if got := len(output.LeafProbabilities); got != LeafClassCount {
		t.Fatalf(
			"leaf probability count = %d, want %d",
			got,
			LeafClassCount,
		)
	}
}

func TestVariableStarClassifierPort(t *testing.T) {
	want := ClassificationOutput{
		XGBoostExecuted: true,
	}

	classifier := classifierFunc(func(
		ctx context.Context,
		input ClassificationInput,
	) (ClassificationOutput, error) {
		if ctx == nil {
			t.Fatal("Classify() context is nil")
		}
		if input.CoarseMode != CoarseModeComputeCurrent {
			t.Fatalf(
				"Classify() mode = %d, want %d",
				input.CoarseMode,
				CoarseModeComputeCurrent,
			)
		}
		return want, nil
	})

	got, err := classifier.Classify(
		context.Background(),
		ClassificationInput{
			CoarseMode: CoarseModeComputeCurrent,
		},
	)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}

	if got != want {
		t.Fatalf("Classify() output = %#v, want %#v", got, want)
	}
}
