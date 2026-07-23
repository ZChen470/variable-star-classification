package fakeclassifier

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestClassifierReturnsConfiguredOutputAndRecordsInput(t *testing.T) {
	reusedProbabilities := [application.CoarseClassCount]float32{
		0.1, 0.2, 0.3, 0.1, 0.1, 0.1, 0.1,
	}

	input := application.ClassificationInput{
		TimeMJD:                   []float64{60001.1, 60002.2, 60003.3},
		Magnitude:                 []float32{14.1, 14.2, 14.3},
		MagnitudeError:            []float32{0.01, 0.02, 0.03},
		CoarseMode:                application.CoarseModeReusePrevious,
		ReusedCoarseProbabilities: &reusedProbabilities,
	}

	wantOutput := application.ClassificationOutput{
		CoarseProbabilities: [application.CoarseClassCount]float32{
			0.1, 0.2, 0.3, 0.1, 0.1, 0.1, 0.1,
		},
		ConditionalFineProbabilities: [application.ConditionalFineClassCount]float32{
			0.6, 0.4,
			0.7, 0.3,
			0.8, 0.2,
			0.9, 0.1,
			0.55, 0.45,
		},
		LeafProbabilities: [application.LeafClassCount]float32{
			0.06, 0.04,
			0.14, 0.06,
			0.24, 0.06,
			0.09, 0.01,
			0.055, 0.045,
			0.1, 0.1,
		},
		XGBoostExecuted: false,
	}

	classifier := New(wantOutput, nil)

	got, err := classifier.Classify(context.Background(), input)
	if err != nil {
		t.Fatalf("Classify() error = %v", err)
	}
	if got != wantOutput {
		t.Fatalf("Classify() output = %#v, want %#v", got, wantOutput)
	}

	calls := classifier.Calls()
	if len(calls) != 1 {
		t.Fatalf("call count = %d, want 1", len(calls))
	}
	if !reflect.DeepEqual(calls[0], input) {
		t.Fatalf("recorded input = %#v, want %#v", calls[0], input)
	}

	// 修改原始输入后，Fake 内部记录不应变化。
	input.TimeMJD[0] = 99999
	input.Magnitude[0] = 99
	input.MagnitudeError[0] = 99
	reusedProbabilities[0] = 99

	recorded := classifier.Calls()[0]
	if recorded.TimeMJD[0] == 99999 {
		t.Fatal("recorded TimeMJD aliases caller input")
	}
	if recorded.Magnitude[0] == 99 {
		t.Fatal("recorded Magnitude aliases caller input")
	}
	if recorded.MagnitudeError[0] == 99 {
		t.Fatal("recorded MagnitudeError aliases caller input")
	}
	if recorded.ReusedCoarseProbabilities[0] == 99 {
		t.Fatal("recorded reused probabilities alias caller input")
	}
}

func TestClassifierReturnsConfiguredErrorAndRecordsInput(t *testing.T) {
	wantErr := errors.New("inference failed")
	classifier := New(
		application.ClassificationOutput{
			XGBoostExecuted: true,
		},
		wantErr,
	)

	got, err := classifier.Classify(
		context.Background(),
		application.ClassificationInput{
			CoarseMode: application.CoarseModeComputeCurrent,
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("Classify() error = %v, want %v", err, wantErr)
	}

	var zero application.ClassificationOutput
	if got != zero {
		t.Fatalf("Classify() output = %#v, want zero value", got)
	}

	if gotCalls := len(classifier.Calls()); gotCalls != 1 {
		t.Fatalf("call count = %d, want 1", gotCalls)
	}
}

func TestClassifierRejectsCancelledContextWithoutRecordingCall(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	classifier := New(application.ClassificationOutput{}, nil)

	_, err := classifier.Classify(
		ctx,
		application.ClassificationInput{
			CoarseMode: application.CoarseModeComputeBootstrap,
		},
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Classify() error = %v, want context.Canceled", err)
	}

	if gotCalls := len(classifier.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}

func TestClassifierRejectsNilContext(t *testing.T) {
	classifier := New(application.ClassificationOutput{}, nil)

	_, err := classifier.Classify(
		nil,
		application.ClassificationInput{},
	)
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("Classify() error = %v, want %v", err, ErrNilContext)
	}

	if gotCalls := len(classifier.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}
