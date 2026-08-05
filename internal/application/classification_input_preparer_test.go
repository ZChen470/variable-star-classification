package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakelightcurve"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakemodelbundle"
)

func TestClassificationInputPreparerBuildsComputeCurrentInput(
	t *testing.T,
) {
	request := application.ClassificationInputPreparationRequest{
		ObjectID:                   "OBJ-PREPARE-001",
		LightCurveRevision:         12,
		DeclaredEligibleEpochCount: 3,
		ModelBundleVersion:         "bundle-v1",
	}

	repositoryRequest := fakelightcurve.Request{
		ObjectID: request.ObjectID,
		Revision: request.LightCurveRevision,
	}
	repository := fakelightcurve.New(
		map[fakelightcurve.Request]fakelightcurve.Response{
			repositoryRequest: {
				Revision: domain.LightCurveRevision{
					ObjectID:           request.ObjectID,
					Revision:           request.LightCurveRevision,
					EligibleEpochCount: 3,
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
				},
			},
		},
	)

	metadata := preparationModelBundleMetadata(
		request.ModelBundleVersion,
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

	got, err := preparer.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	if got.Selection.Mode != application.CoarseModeComputeCurrent {
		t.Fatalf(
			"selection mode = %v, want %v",
			got.Selection.Mode,
			application.CoarseModeComputeCurrent,
		)
	}
	if got.Selection.ReusedCoarse != nil {
		t.Fatalf(
			"reused coarse = %#v, want nil",
			got.Selection.ReusedCoarse,
		)
	}
	if len(finder.calls) != 0 {
		t.Fatalf(
			"finder call count = %d, want 0",
			len(finder.calls),
		)
	}

	wantTime := []float64{60421.1, 60422.2, 60423.3}
	wantMagnitude := []float32{17.31, 17.42, 17.53}
	wantMagnitudeError := []float32{0.03, 0.04, 0.05}

	if !reflect.DeepEqual(got.Input.TimeMJD, wantTime) {
		t.Fatalf(
			"TimeMJD = %#v, want %#v",
			got.Input.TimeMJD,
			wantTime,
		)
	}
	if !reflect.DeepEqual(got.Input.Magnitude, wantMagnitude) {
		t.Fatalf(
			"Magnitude = %#v, want %#v",
			got.Input.Magnitude,
			wantMagnitude,
		)
	}
	if !reflect.DeepEqual(
		got.Input.MagnitudeError,
		wantMagnitudeError,
	) {
		t.Fatalf(
			"MagnitudeError = %#v, want %#v",
			got.Input.MagnitudeError,
			wantMagnitudeError,
		)
	}

	wantRepositoryCalls := []fakelightcurve.Request{
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

	wantResolverCalls := []string{request.ModelBundleVersion}
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

	// Revision 与 ClassificationInput 必须是独立数据。
	originalInputTime := got.Input.TimeMJD[0]
	got.Revision.Epochs[0].ObservationTime = 99999

	if got.Input.TimeMJD[0] != originalInputTime {
		t.Fatal("classification input aliases prepared revision")
	}
}

func TestClassificationInputPreparerReusesCompatibleHistoricalCoarse(
	t *testing.T,
) {
	request := application.ClassificationInputPreparationRequest{
		ObjectID:                   "OBJ-PREPARE-002",
		LightCurveRevision:         30,
		DeclaredEligibleEpochCount: 21,
		ModelBundleVersion:         "bundle-v2",
	}

	repositoryRequest := fakelightcurve.Request{
		ObjectID: request.ObjectID,
		Revision: request.LightCurveRevision,
	}
	repository := fakelightcurve.New(
		map[fakelightcurve.Request]fakelightcurve.Response{
			repositoryRequest: {
				Revision: domain.LightCurveRevision{
					ObjectID:           request.ObjectID,
					Revision:           request.LightCurveRevision,
					EligibleEpochCount: 21,
					Epochs:             descendingPreparationEpochs(21),
				},
			},
		},
	)

	metadata := preparationModelBundleMetadata(
		request.ModelBundleVersion,
	)
	resolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			request.ModelBundleVersion: {
				Metadata: metadata,
			},
		},
	)

	compatible := application.CompatibleCoarseResult{
		SourceRunID:              domain.RunID("source-run-002"),
		SourceLightCurveRevision: 29,
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
	finder := &preparationCompatibleCoarseFinder{
		result: compatible,
	}

	preparer := newClassificationInputPreparerForTest(
		t,
		repository,
		resolver,
		finder,
	)

	got, err := preparer.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	if got.Selection.Mode != application.CoarseModeReusePrevious {
		t.Fatalf(
			"selection mode = %v, want %v",
			got.Selection.Mode,
			application.CoarseModeReusePrevious,
		)
	}
	if got.Selection.ReusedCoarse == nil {
		t.Fatal("selection reused coarse = nil, want value")
	}
	if got.Input.ReusedCoarseProbabilities == nil {
		t.Fatal("input reused probabilities = nil, want value")
	}
	if *got.Input.ReusedCoarseProbabilities != compatible.Probabilities {
		t.Fatalf(
			"input reused probabilities = %#v, want %#v",
			*got.Input.ReusedCoarseProbabilities,
			compatible.Probabilities,
		)
	}

	wantQuery := application.CompatibleCoarseQuery{
		ObjectID:                 request.ObjectID,
		TargetLightCurveRevision: request.LightCurveRevision,
		ModelBundleVersion:       request.ModelBundleVersion,
	}
	wantFinderCalls := []application.CompatibleCoarseQuery{
		wantQuery,
	}
	if !reflect.DeepEqual(finder.calls, wantFinderCalls) {
		t.Fatalf(
			"finder calls = %#v, want %#v",
			finder.calls,
			wantFinderCalls,
		)
	}

	for index := 1; index < len(got.Input.TimeMJD); index++ {
		if got.Input.TimeMJD[index-1] >=
			got.Input.TimeMJD[index] {
			t.Fatalf(
				"input time not strictly increasing at %d,%d",
				index-1,
				index,
			)
		}
	}

	// Input 中的概率必须与 Selection 中的来源结果独立。
	originalSelectionProbability :=
		got.Selection.ReusedCoarse.Probabilities[0]
	got.Input.ReusedCoarseProbabilities[0] = 0.99

	if got.Selection.ReusedCoarse.Probabilities[0] !=
		originalSelectionProbability {
		t.Fatal(
			"classification input probabilities alias selection",
		)
	}
}

func TestClassificationInputPreparerBuildsBootstrapInput(
	t *testing.T,
) {
	request := application.ClassificationInputPreparationRequest{
		ObjectID:                   "OBJ-PREPARE-003",
		LightCurveRevision:         40,
		DeclaredEligibleEpochCount: 21,
		ModelBundleVersion:         "bundle-v3",
	}

	repositoryRequest := fakelightcurve.Request{
		ObjectID: request.ObjectID,
		Revision: request.LightCurveRevision,
	}
	repository := fakelightcurve.New(
		map[fakelightcurve.Request]fakelightcurve.Response{
			repositoryRequest: {
				Revision: domain.LightCurveRevision{
					ObjectID:           request.ObjectID,
					Revision:           request.LightCurveRevision,
					EligibleEpochCount: 21,
					Epochs:             descendingPreparationEpochs(21),
				},
			},
		},
	)

	resolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			request.ModelBundleVersion: {
				Metadata: preparationModelBundleMetadata(
					request.ModelBundleVersion,
				),
			},
		},
	)
	finder := &preparationCompatibleCoarseFinder{
		err: application.ErrCompatibleCoarseNotFound,
	}

	preparer := newClassificationInputPreparerForTest(
		t,
		repository,
		resolver,
		finder,
	)

	got, err := preparer.Prepare(context.Background(), request)
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}

	if got.Selection.Mode !=
		application.CoarseModeComputeBootstrap {
		t.Fatalf(
			"selection mode = %v, want %v",
			got.Selection.Mode,
			application.CoarseModeComputeBootstrap,
		)
	}
	if got.Input.CoarseMode !=
		application.CoarseModeComputeBootstrap {
		t.Fatalf(
			"input coarse mode = %v, want %v",
			got.Input.CoarseMode,
			application.CoarseModeComputeBootstrap,
		)
	}
	if got.Input.ReusedCoarseProbabilities != nil {
		t.Fatalf(
			"input reused probabilities = %#v, want nil",
			got.Input.ReusedCoarseProbabilities,
		)
	}
	if len(finder.calls) != 1 {
		t.Fatalf(
			"finder call count = %d, want 1",
			len(finder.calls),
		)
	}
}

func TestClassificationInputPreparerStopsBeforeModeSelectionOnFailure(
	t *testing.T,
) {
	request := application.ClassificationInputPreparationRequest{
		ObjectID:                   "OBJ-PREPARE-FAILURE",
		LightCurveRevision:         50,
		DeclaredEligibleEpochCount: 3,
		ModelBundleVersion:         "bundle-failure",
	}

	tests := []struct {
		name      string
		response  fakelightcurve.Response
		wantErr   error
		wantReads int
	}{
		{
			name: "repository failure",
			response: fakelightcurve.Response{
				Err: application.ErrLightCurveSourceUnavailable,
			},
			wantErr:   application.ErrLightCurveSourceUnavailable,
			wantReads: 1,
		},
		{
			name: "duplicate observation time",
			response: fakelightcurve.Response{
				Revision: domain.LightCurveRevision{
					ObjectID:           request.ObjectID,
					Revision:           request.LightCurveRevision,
					EligibleEpochCount: 3,
					Epochs: []domain.LightCurveEpoch{
						{
							ObservationTime: 1,
							Magnitude:       17.1,
							MagnitudeError:  0.1,
						},
						{
							ObservationTime: 2,
							Magnitude:       17.2,
							MagnitudeError:  0.1,
						},
						{
							ObservationTime: 1,
							Magnitude:       17.3,
							MagnitudeError:  0.1,
						},
					},
				},
			},
			wantErr:   application.ErrDuplicateObservationTime,
			wantReads: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repositoryRequest := fakelightcurve.Request{
				ObjectID: request.ObjectID,
				Revision: request.LightCurveRevision,
			}
			repository := fakelightcurve.New(
				map[fakelightcurve.Request]fakelightcurve.Response{
					repositoryRequest: test.response,
				},
			)
			resolver := fakemodelbundle.New(nil)
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
			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"Prepare() error = %v, want %v",
					err,
					test.wantErr,
				)
			}
			if !reflect.DeepEqual(
				got,
				application.PreparedClassificationInput{},
			) {
				t.Fatalf(
					"Prepare() = %#v, want zero value",
					got,
				)
			}
			if calls := len(repository.Calls()); calls != test.wantReads {
				t.Fatalf(
					"repository call count = %d, want %d",
					calls,
					test.wantReads,
				)
			}
			if calls := len(resolver.Calls()); calls != 0 {
				t.Fatalf(
					"resolver call count = %d, want 0",
					calls,
				)
			}
			if calls := len(finder.calls); calls != 0 {
				t.Fatalf(
					"finder call count = %d, want 0",
					calls,
				)
			}
		})
	}
}

func TestClassificationInputPreparerRejectsNilBoundaries(t *testing.T) {
	repository := fakelightcurve.New(nil)
	resolver := fakemodelbundle.New(nil)
	finder := &preparationCompatibleCoarseFinder{}

	reader, err := application.NewLightCurveRevisionReader(repository)
	if err != nil {
		t.Fatalf("NewLightCurveRevisionReader() error = %v", err)
	}
	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	preparer, err := application.NewClassificationInputPreparer(
		nil,
		selector,
	)
	if err == nil || preparer != nil {
		t.Fatal("nil reader constructor result is invalid")
	}

	preparer, err = application.NewClassificationInputPreparer(
		reader,
		nil,
	)
	if err == nil || preparer != nil {
		t.Fatal("nil selector constructor result is invalid")
	}

	preparer, err = application.NewClassificationInputPreparer(
		reader,
		selector,
	)
	if err != nil {
		t.Fatalf(
			"NewClassificationInputPreparer() error = %v",
			err,
		)
	}

	_, err = preparer.Prepare(
		nil,
		application.ClassificationInputPreparationRequest{},
	)
	if err == nil {
		t.Fatal("Prepare(nil context) error = nil, want error")
	}

	var nilPreparer *application.ClassificationInputPreparer
	_, err = nilPreparer.Prepare(
		context.Background(),
		application.ClassificationInputPreparationRequest{},
	)
	if err == nil {
		t.Fatal("nil preparer Prepare() error = nil, want error")
	}
}

func newClassificationInputPreparerForTest(
	t *testing.T,
	repository application.LightCurveRepository,
	resolver application.ModelBundleResolver,
	finder application.CompatibleCoarseFinder,
) *application.ClassificationInputPreparer {
	t.Helper()

	reader, err := application.NewLightCurveRevisionReader(repository)
	if err != nil {
		t.Fatalf("NewLightCurveRevisionReader() error = %v", err)
	}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	preparer, err := application.NewClassificationInputPreparer(
		reader,
		selector,
	)
	if err != nil {
		t.Fatalf(
			"NewClassificationInputPreparer() error = %v",
			err,
		)
	}

	return preparer
}

func preparationModelBundleMetadata(
	modelBundleVersion string,
) application.ModelBundleMetadata {
	return application.ModelBundleMetadata{
		ModelBundleVersion: modelBundleVersion,
	}
}

func descendingPreparationEpochs(
	count int,
) []domain.LightCurveEpoch {
	epochs := make([]domain.LightCurveEpoch, count)

	for index := range epochs {
		epochs[index] = domain.LightCurveEpoch{
			ObservationTime: 60000 +
				float64(count-index),
			Magnitude:      17 + float32(index)/100,
			MagnitudeError: 0.03,
		}
	}

	return epochs
}

type preparationCompatibleCoarseFinder struct {
	result application.CompatibleCoarseResult
	err    error
	calls  []application.CompatibleCoarseQuery
}

func (finder *preparationCompatibleCoarseFinder) FindLatestCompatibleCoarse(
	ctx context.Context,
	query application.CompatibleCoarseQuery,
) (application.CompatibleCoarseResult, error) {
	if ctx == nil {
		return application.CompatibleCoarseResult{},
			errors.New("nil context")
	}
	if err := ctx.Err(); err != nil {
		return application.CompatibleCoarseResult{}, err
	}

	finder.calls = append(finder.calls, query)

	if finder.err != nil {
		return application.CompatibleCoarseResult{}, finder.err
	}

	return finder.result, nil
}
