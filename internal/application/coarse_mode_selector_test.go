package application_test

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"github.com/ZChen470/variable-star-classification/internal/testsupport/fakemodelbundle"
)

func TestCoarseModeSelectorSelectsComputeCurrentWithoutHistoryLookup(
	t *testing.T,
) {
	tests := []struct {
		name       string
		epochCount uint32
	}{
		{
			name:       "minimum epoch count",
			epochCount: 3,
		},
		{
			name:       "compute current upper boundary",
			epochCount: 20,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const modelBundleVersion = "bundle-v1"

			metadata := validModelBundleMetadata(
				modelBundleVersion,
			)
			resolver := fakemodelbundle.New(
				map[string]fakemodelbundle.Response{
					modelBundleVersion: {
						Metadata: metadata,
					},
				},
			)
			finder := &recordingCompatibleCoarseFinder{}

			selector, err := application.NewCoarseModeSelector(
				resolver,
				finder,
			)
			if err != nil {
				t.Fatalf(
					"NewCoarseModeSelector() error = %v",
					err,
				)
			}

			got, err := selector.Select(
				context.Background(),
				"OBJ-0001",
				21,
				test.epochCount,
				modelBundleVersion,
			)
			if err != nil {
				t.Fatalf("Select() error = %v", err)
			}

			want := application.CoarseModeSelection{
				Mode:        application.CoarseModeComputeCurrent,
				ModelBundle: metadata,
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("Select() = %#v, want %#v", got, want)
			}

			wantResolverCalls := []string{modelBundleVersion}
			if gotCalls := resolver.Calls(); !reflect.DeepEqual(
				gotCalls,
				wantResolverCalls,
			) {
				t.Fatalf(
					"resolver calls = %#v, want %#v",
					gotCalls,
					wantResolverCalls,
				)
			}

			if gotCalls := len(finder.calls); gotCalls != 0 {
				t.Fatalf(
					"compatible coarse finder call count = %d, want 0",
					gotCalls,
				)
			}
		})
	}
}

func TestCoarseModeSelectorReusesCompatibleHistoricalCoarse(
	t *testing.T,
) {
	const modelBundleVersion = "bundle-v2"

	metadata := validModelBundleMetadata(modelBundleVersion)
	resolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			modelBundleVersion: {
				Metadata: metadata,
			},
		},
	)

	compatible := application.CompatibleCoarseResult{
		SourceRunID:              domain.RunID("source-run-001"),
		SourceLightCurveRevision: 19,
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
	finder := &recordingCompatibleCoarseFinder{
		result: compatible,
	}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	got, err := selector.Select(
		context.Background(),
		"OBJ-0002",
		22,
		21,
		modelBundleVersion,
	)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}

	want := application.CoarseModeSelection{
		Mode:         application.CoarseModeReusePrevious,
		ModelBundle:  metadata,
		ReusedCoarse: &compatible,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Select() = %#v, want %#v", got, want)
	}

	wantQuery := application.CompatibleCoarseQuery{
		ObjectID:                 "OBJ-0002",
		TargetLightCurveRevision: 22,
		TaxonomyVersion:          metadata.TaxonomyVersion,
		XGBoostModelVersion:      metadata.XGBoostModelVersion,
		FeatureSchemaVersion:     metadata.FeatureSchemaVersion,
	}
	wantCalls := []application.CompatibleCoarseQuery{wantQuery}
	if !reflect.DeepEqual(finder.calls, wantCalls) {
		t.Fatalf(
			"finder calls = %#v, want %#v",
			finder.calls,
			wantCalls,
		)
	}

	// 返回结果不得与 Finder 的预设结果共享可变指针。
	got.ReusedCoarse.Probabilities[0] = 0.99
	if finder.result.Probabilities[0] !=
		compatible.Probabilities[0] {
		t.Fatalf(
			"finder result was modified: got %v, want %v",
			finder.result.Probabilities[0],
			compatible.Probabilities[0],
		)
	}
}

func TestCoarseModeSelectorBootstrapsOnlyWhenCompatibleCoarseNotFound(
	t *testing.T,
) {
	const modelBundleVersion = "bundle-v3"

	metadata := validModelBundleMetadata(modelBundleVersion)
	resolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			modelBundleVersion: {
				Metadata: metadata,
			},
		},
	)
	finder := &recordingCompatibleCoarseFinder{
		err: application.ErrCompatibleCoarseNotFound,
	}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	got, err := selector.Select(
		context.Background(),
		"OBJ-0003",
		30,
		21,
		modelBundleVersion,
	)
	if err != nil {
		t.Fatalf("Select() error = %v", err)
	}

	want := application.CoarseModeSelection{
		Mode:        application.CoarseModeComputeBootstrap,
		ModelBundle: metadata,
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Select() = %#v, want %#v", got, want)
	}

	if gotCalls := len(finder.calls); gotCalls != 1 {
		t.Fatalf("finder call count = %d, want 1", gotCalls)
	}
}

func TestCoarseModeSelectorDoesNotBootstrapOnFinderFailure(
	t *testing.T,
) {
	const modelBundleVersion = "bundle-v4"

	finderErr := errors.New("database temporarily unavailable")
	resolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			modelBundleVersion: {
				Metadata: validModelBundleMetadata(
					modelBundleVersion,
				),
			},
		},
	)
	finder := &recordingCompatibleCoarseFinder{
		err: finderErr,
	}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	got, err := selector.Select(
		context.Background(),
		"OBJ-0004",
		31,
		100,
		modelBundleVersion,
	)
	if !errors.Is(err, finderErr) {
		t.Fatalf("Select() error = %v, want %v", err, finderErr)
	}
	if !reflect.DeepEqual(
		got,
		application.CoarseModeSelection{},
	) {
		t.Fatalf("Select() = %#v, want zero value", got)
	}

	if errors.Is(err, application.ErrCompatibleCoarseNotFound) {
		t.Fatalf(
			"Select() error unexpectedly matches %v",
			application.ErrCompatibleCoarseNotFound,
		)
	}
}

func TestCoarseModeSelectorPreservesModelBundleResolverErrors(
	t *testing.T,
) {
	const modelBundleVersion = "missing-bundle"

	resolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			modelBundleVersion: {
				Err: application.ErrModelBundleNotFound,
			},
		},
	)
	finder := &recordingCompatibleCoarseFinder{}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	got, err := selector.Select(
		context.Background(),
		"OBJ-0005",
		32,
		21,
		modelBundleVersion,
	)
	if !errors.Is(err, application.ErrModelBundleNotFound) {
		t.Fatalf(
			"Select() error = %v, want %v",
			err,
			application.ErrModelBundleNotFound,
		)
	}
	if !reflect.DeepEqual(
		got,
		application.CoarseModeSelection{},
	) {
		t.Fatalf("Select() = %#v, want zero value", got)
	}
	if gotCalls := len(finder.calls); gotCalls != 0 {
		t.Fatalf("finder call count = %d, want 0", gotCalls)
	}
}

func TestCoarseModeSelectorRejectsInvalidResolvedMetadata(
	t *testing.T,
) {
	const modelBundleVersion = "bundle-v5"

	tests := []struct {
		name     string
		metadata application.ModelBundleMetadata
	}{
		{
			name: "bundle identity mismatch",
			metadata: application.ModelBundleMetadata{
				ModelBundleVersion:   "other-bundle",
				TaxonomyVersion:      "taxonomy-v1",
				XGBoostModelVersion:  "xgboost-v1",
				FeatureSchemaVersion: "feature-v1",
			},
		},
		{
			name: "missing taxonomy version",
			metadata: application.ModelBundleMetadata{
				ModelBundleVersion:   modelBundleVersion,
				XGBoostModelVersion:  "xgboost-v1",
				FeatureSchemaVersion: "feature-v1",
			},
		},
		{
			name: "missing xgboost model version",
			metadata: application.ModelBundleMetadata{
				ModelBundleVersion:   modelBundleVersion,
				TaxonomyVersion:      "taxonomy-v1",
				FeatureSchemaVersion: "feature-v1",
			},
		},
		{
			name: "missing feature schema version",
			metadata: application.ModelBundleMetadata{
				ModelBundleVersion:  modelBundleVersion,
				TaxonomyVersion:     "taxonomy-v1",
				XGBoostModelVersion: "xgboost-v1",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := fakemodelbundle.New(
				map[string]fakemodelbundle.Response{
					modelBundleVersion: {
						Metadata: test.metadata,
					},
				},
			)
			finder := &recordingCompatibleCoarseFinder{}

			selector, err := application.NewCoarseModeSelector(
				resolver,
				finder,
			)
			if err != nil {
				t.Fatalf(
					"NewCoarseModeSelector() error = %v",
					err,
				)
			}

			got, err := selector.Select(
				context.Background(),
				"OBJ-0006",
				33,
				21,
				modelBundleVersion,
			)
			if !errors.Is(
				err,
				application.ErrInvalidModelBundleMetadata,
			) {
				t.Fatalf(
					"Select() error = %v, want %v",
					err,
					application.ErrInvalidModelBundleMetadata,
				)
			}
			if !reflect.DeepEqual(
				got,
				application.CoarseModeSelection{},
			) {
				t.Fatalf(
					"Select() = %#v, want zero value",
					got,
				)
			}
			if gotCalls := len(finder.calls); gotCalls != 0 {
				t.Fatalf(
					"finder call count = %d, want 0",
					gotCalls,
				)
			}
		})
	}
}

func TestCoarseModeSelectorRejectsInvalidEpochCountsBeforeDependencies(
	t *testing.T,
) {
	tests := []struct {
		name       string
		epochCount uint32
		wantErr    error
	}{
		{
			name:       "below minimum",
			epochCount: 2,
			wantErr:    application.ErrInsufficientLightCurveEpochs,
		},
		{
			name:       "above maximum",
			epochCount: 1025,
			wantErr:    application.ErrTooManyLightCurveEpochs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := fakemodelbundle.New(nil)
			finder := &recordingCompatibleCoarseFinder{}

			selector, err := application.NewCoarseModeSelector(
				resolver,
				finder,
			)
			if err != nil {
				t.Fatalf(
					"NewCoarseModeSelector() error = %v",
					err,
				)
			}

			_, err = selector.Select(
				context.Background(),
				"OBJ-0007",
				34,
				test.epochCount,
				"bundle-v6",
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf(
					"Select() error = %v, want %v",
					err,
					test.wantErr,
				)
			}
			if gotCalls := len(resolver.Calls()); gotCalls != 0 {
				t.Fatalf(
					"resolver call count = %d, want 0",
					gotCalls,
				)
			}
			if gotCalls := len(finder.calls); gotCalls != 0 {
				t.Fatalf(
					"finder call count = %d, want 0",
					gotCalls,
				)
			}
		})
	}
}

func TestCoarseModeSelectorPropagatesCancelledContext(
	t *testing.T,
) {
	resolver := fakemodelbundle.New(nil)
	finder := &recordingCompatibleCoarseFinder{}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = selector.Select(
		ctx,
		"OBJ-0008",
		35,
		21,
		"bundle-v7",
	)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"Select() error = %v, want context.Canceled",
			err,
		)
	}
	if gotCalls := len(resolver.Calls()); gotCalls != 0 {
		t.Fatalf("resolver call count = %d, want 0", gotCalls)
	}
	if gotCalls := len(finder.calls); gotCalls != 0 {
		t.Fatalf("finder call count = %d, want 0", gotCalls)
	}
}

func TestNewCoarseModeSelectorRejectsNilDependencies(t *testing.T) {
	finder := &recordingCompatibleCoarseFinder{}
	resolver := fakemodelbundle.New(nil)

	selector, err := application.NewCoarseModeSelector(nil, finder)
	if err == nil {
		t.Fatal("NewCoarseModeSelector(nil, finder) error = nil")
	}
	if selector != nil {
		t.Fatalf("selector = %#v, want nil", selector)
	}

	selector, err = application.NewCoarseModeSelector(resolver, nil)
	if err == nil {
		t.Fatal("NewCoarseModeSelector(resolver, nil) error = nil")
	}
	if selector != nil {
		t.Fatalf("selector = %#v, want nil", selector)
	}
}

func validModelBundleMetadata(
	modelBundleVersion string,
) application.ModelBundleMetadata {
	return application.ModelBundleMetadata{
		ModelBundleVersion:   modelBundleVersion,
		TaxonomyVersion:      "taxonomy-v1",
		XGBoostModelVersion:  "xgboost-v1",
		FeatureSchemaVersion: "xgb-feature-schema-v1",
	}
}

type recordingCompatibleCoarseFinder struct {
	result application.CompatibleCoarseResult
	err    error
	calls  []application.CompatibleCoarseQuery
}

func (finder *recordingCompatibleCoarseFinder) FindLatestCompatibleCoarse(
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

func TestCoarseModeSelectorUsesExactTwentyTwentyOneBoundary(
	t *testing.T,
) {
	const modelBundleVersion = "bundle-boundary-v1"

	metadata := validModelBundleMetadata(modelBundleVersion)
	resolver := fakemodelbundle.New(
		map[string]fakemodelbundle.Response{
			modelBundleVersion: {
				Metadata: metadata,
			},
		},
	)

	finder := &recordingCompatibleCoarseFinder{
		err: errors.Join(
			errors.New("wrapped repository result"),
			application.ErrCompatibleCoarseNotFound,
		),
	}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	gotAtTwenty, err := selector.Select(
		context.Background(),
		"OBJ-BOUNDARY",
		40,
		20,
		modelBundleVersion,
	)
	if err != nil {
		t.Fatalf("Select(n=20) error = %v", err)
	}
	if gotAtTwenty.Mode != application.CoarseModeComputeCurrent {
		t.Fatalf(
			"Select(n=20) mode = %v, want %v",
			gotAtTwenty.Mode,
			application.CoarseModeComputeCurrent,
		)
	}
	if gotCalls := len(finder.calls); gotCalls != 0 {
		t.Fatalf(
			"finder call count after n=20 = %d, want 0",
			gotCalls,
		)
	}

	gotAtTwentyOne, err := selector.Select(
		context.Background(),
		"OBJ-BOUNDARY",
		40,
		21,
		modelBundleVersion,
	)
	if err != nil {
		t.Fatalf("Select(n=21) error = %v", err)
	}
	if gotAtTwentyOne.Mode !=
		application.CoarseModeComputeBootstrap {
		t.Fatalf(
			"Select(n=21) mode = %v, want %v",
			gotAtTwentyOne.Mode,
			application.CoarseModeComputeBootstrap,
		)
	}
	if gotAtTwentyOne.ReusedCoarse != nil {
		t.Fatalf(
			"Select(n=21) reused coarse = %#v, want nil",
			gotAtTwentyOne.ReusedCoarse,
		)
	}
	if gotCalls := len(finder.calls); gotCalls != 1 {
		t.Fatalf(
			"finder call count after n=21 = %d, want 1",
			gotCalls,
		)
	}
}

func TestCoarseModeSelectorRejectsInvalidCompatibleCoarseResult(
	t *testing.T,
) {
	const (
		modelBundleVersion = "bundle-history-v1"
		targetRevision     = int64(40)
	)

	baseResult := application.CompatibleCoarseResult{
		SourceRunID:              domain.RunID("source-run-valid"),
		SourceLightCurveRevision: 39,
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

	tests := []struct {
		name   string
		mutate func(*application.CompatibleCoarseResult)
	}{
		{
			name: "empty source run id",
			mutate: func(result *application.CompatibleCoarseResult) {
				result.SourceRunID = ""
			},
		},
		{
			name: "zero source revision",
			mutate: func(result *application.CompatibleCoarseResult) {
				result.SourceLightCurveRevision = 0
			},
		},
		{
			name: "source revision equals target",
			mutate: func(result *application.CompatibleCoarseResult) {
				result.SourceLightCurveRevision = targetRevision
			},
		},
		{
			name: "source revision exceeds target",
			mutate: func(result *application.CompatibleCoarseResult) {
				result.SourceLightCurveRevision =
					targetRevision + 1
			},
		},
		{
			name: "source epoch count below minimum",
			mutate: func(result *application.CompatibleCoarseResult) {
				result.SourceEpochCount = 2
			},
		},
		{
			name: "source epoch count above maximum",
			mutate: func(result *application.CompatibleCoarseResult) {
				result.SourceEpochCount = 1025
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := baseResult
			test.mutate(&result)

			resolver := fakemodelbundle.New(
				map[string]fakemodelbundle.Response{
					modelBundleVersion: {
						Metadata: validModelBundleMetadata(
							modelBundleVersion,
						),
					},
				},
			)
			finder := &recordingCompatibleCoarseFinder{
				result: result,
			}

			selector, err := application.NewCoarseModeSelector(
				resolver,
				finder,
			)
			if err != nil {
				t.Fatalf(
					"NewCoarseModeSelector() error = %v",
					err,
				)
			}

			got, err := selector.Select(
				context.Background(),
				"OBJ-HISTORY",
				targetRevision,
				21,
				modelBundleVersion,
			)
			if !errors.Is(
				err,
				application.ErrInvalidCompatibleCoarseResult,
			) {
				t.Fatalf(
					"Select() error = %v, want %v",
					err,
					application.ErrInvalidCompatibleCoarseResult,
				)
			}
			if !reflect.DeepEqual(
				got,
				application.CoarseModeSelection{},
			) {
				t.Fatalf(
					"Select() = %#v, want zero value",
					got,
				)
			}
			if gotCalls := len(finder.calls); gotCalls != 1 {
				t.Fatalf(
					"finder call count = %d, want 1",
					gotCalls,
				)
			}
		})
	}
}

func TestCoarseModeSelectorRejectsInvalidRequestBeforeDependencies(
	t *testing.T,
) {
	tests := []struct {
		name               string
		objectID           string
		targetRevision     int64
		actualEpochCount   uint32
		modelBundleVersion string
		wantErr            error
	}{
		{
			name:               "empty object id",
			objectID:           "",
			targetRevision:     10,
			actualEpochCount:   21,
			modelBundleVersion: "bundle-v1",
		},
		{
			name:               "invalid target revision",
			objectID:           "OBJ-VALIDATION",
			targetRevision:     0,
			actualEpochCount:   21,
			modelBundleVersion: "bundle-v1",
		},
		{
			name:               "empty model bundle version",
			objectID:           "OBJ-VALIDATION",
			targetRevision:     10,
			actualEpochCount:   21,
			modelBundleVersion: "",
		},
		{
			name:               "epoch count below minimum",
			objectID:           "OBJ-VALIDATION",
			targetRevision:     10,
			actualEpochCount:   2,
			modelBundleVersion: "bundle-v1",
			wantErr:            application.ErrInsufficientLightCurveEpochs,
		},
		{
			name:               "epoch count above maximum",
			objectID:           "OBJ-VALIDATION",
			targetRevision:     10,
			actualEpochCount:   1025,
			modelBundleVersion: "bundle-v1",
			wantErr:            application.ErrTooManyLightCurveEpochs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resolver := fakemodelbundle.New(nil)
			finder := &recordingCompatibleCoarseFinder{}

			selector, err := application.NewCoarseModeSelector(
				resolver,
				finder,
			)
			if err != nil {
				t.Fatalf(
					"NewCoarseModeSelector() error = %v",
					err,
				)
			}

			got, err := selector.Select(
				context.Background(),
				test.objectID,
				test.targetRevision,
				test.actualEpochCount,
				test.modelBundleVersion,
			)
			if err == nil {
				t.Fatal("Select() error = nil, want error")
			}
			if test.wantErr != nil &&
				!errors.Is(err, test.wantErr) {
				t.Fatalf(
					"Select() error = %v, want %v",
					err,
					test.wantErr,
				)
			}
			if !reflect.DeepEqual(
				got,
				application.CoarseModeSelection{},
			) {
				t.Fatalf(
					"Select() = %#v, want zero value",
					got,
				)
			}
			if gotCalls := len(resolver.Calls()); gotCalls != 0 {
				t.Fatalf(
					"resolver call count = %d, want 0",
					gotCalls,
				)
			}
			if gotCalls := len(finder.calls); gotCalls != 0 {
				t.Fatalf(
					"finder call count = %d, want 0",
					gotCalls,
				)
			}
		})
	}
}

func TestCoarseModeSelectorRejectsNilInvocationBoundaries(
	t *testing.T,
) {
	resolver := fakemodelbundle.New(nil)
	finder := &recordingCompatibleCoarseFinder{}

	selector, err := application.NewCoarseModeSelector(
		resolver,
		finder,
	)
	if err != nil {
		t.Fatalf("NewCoarseModeSelector() error = %v", err)
	}

	got, err := selector.Select(
		nil,
		"OBJ-NIL",
		10,
		21,
		"bundle-v1",
	)
	if err == nil {
		t.Fatal("Select(nil context) error = nil, want error")
	}
	if !reflect.DeepEqual(
		got,
		application.CoarseModeSelection{},
	) {
		t.Fatalf(
			"Select(nil context) = %#v, want zero value",
			got,
		)
	}
	if gotCalls := len(resolver.Calls()); gotCalls != 0 {
		t.Fatalf("resolver call count = %d, want 0", gotCalls)
	}
	if gotCalls := len(finder.calls); gotCalls != 0 {
		t.Fatalf("finder call count = %d, want 0", gotCalls)
	}

	var nilSelector *application.CoarseModeSelector
	got, err = nilSelector.Select(
		context.Background(),
		"OBJ-NIL",
		10,
		21,
		"bundle-v1",
	)
	if err == nil {
		t.Fatal("nil selector Select() error = nil, want error")
	}
	if !reflect.DeepEqual(
		got,
		application.CoarseModeSelection{},
	) {
		t.Fatalf(
			"nil selector Select() = %#v, want zero value",
			got,
		)
	}

	var zeroSelector application.CoarseModeSelector
	got, err = zeroSelector.Select(
		context.Background(),
		"OBJ-NIL",
		10,
		21,
		"bundle-v1",
	)
	if err == nil {
		t.Fatal("zero selector Select() error = nil, want error")
	}
	if !reflect.DeepEqual(
		got,
		application.CoarseModeSelection{},
	) {
		t.Fatalf(
			"zero selector Select() = %#v, want zero value",
			got,
		)
	}
}
