package fakeservingbundle

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestResolverReturnsConfiguredMetadataAndRecordsExactVersion(
	t *testing.T,
) {
	modelBundleVersion := " bundle-2026-07-003 "
	want := testMetadata(modelBundleVersion)

	responses := map[string]Response{
		modelBundleVersion: {
			Metadata: want,
		},
	}

	resolver := New(responses)

	// New 必须复制配置 map 和 metadata 中的 slice。
	responses[modelBundleVersion] = Response{
		Metadata: testMetadata("changed-after-new"),
	}
	want.Entrypoint.Inputs[0].Dims[0] = -99

	got, err := resolver.ResolveServingBundle(
		context.Background(),
		modelBundleVersion,
	)
	if err != nil {
		t.Fatalf("ResolveServingBundle() error = %v", err)
	}

	want = testMetadata(modelBundleVersion)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ResolveServingBundle() = %#v, want %#v", got, want)
	}

	wantCalls := []string{modelBundleVersion}
	if gotCalls := resolver.Calls(); !reflect.DeepEqual(gotCalls, wantCalls) {
		t.Fatalf("Calls() = %#v, want %#v", gotCalls, wantCalls)
	}

	// 返回 metadata 和 Calls 都必须与 Fake 内部存储隔离。
	got.Entrypoint.Inputs[0].Name = "MODIFIED"
	got.Entrypoint.Inputs[0].Dims[0] = 123
	calls := resolver.Calls()
	calls[0] = "modified"

	gotAgain, err := resolver.ResolveServingBundle(
		context.Background(),
		modelBundleVersion,
	)
	if err != nil {
		t.Fatalf("second ResolveServingBundle() error = %v", err)
	}
	if !reflect.DeepEqual(gotAgain, want) {
		t.Fatalf("second ResolveServingBundle() = %#v, want %#v", gotAgain, want)
	}

	wantCalls = []string{modelBundleVersion, modelBundleVersion}
	if gotCalls := resolver.Calls(); !reflect.DeepEqual(gotCalls, wantCalls) {
		t.Fatalf("Calls() after mutation = %#v, want %#v", gotCalls, wantCalls)
	}
}

func TestResolverReturnsConfiguredErrors(t *testing.T) {
	transientErr := errors.New("serving bundle source unavailable")

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "bundle not found",
			err:  application.ErrServingBundleNotFound,
		},
		{
			name: "source unavailable",
			err:  transientErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const modelBundleVersion = "bundle-v2"

			resolver := New(map[string]Response{
				modelBundleVersion: {
					Err: test.err,
				},
			})

			got, err := resolver.ResolveServingBundle(
				context.Background(),
				modelBundleVersion,
			)
			if !errors.Is(err, test.err) {
				t.Fatalf("ResolveServingBundle() error = %v, want %v", err, test.err)
			}
			if !reflect.DeepEqual(got, application.ServingBundleMetadata{}) {
				t.Fatalf("ResolveServingBundle() = %#v, want zero value", got)
			}
		})
	}
}

func TestResolverRejectsUnconfiguredRequest(t *testing.T) {
	resolver := New(nil)

	const modelBundleVersion = "missing-bundle"
	got, err := resolver.ResolveServingBundle(
		context.Background(),
		modelBundleVersion,
	)
	if !errors.Is(err, ErrUnconfiguredRequest) {
		t.Fatalf("ResolveServingBundle() error = %v, want %v", err, ErrUnconfiguredRequest)
	}
	if !reflect.DeepEqual(got, application.ServingBundleMetadata{}) {
		t.Fatalf("ResolveServingBundle() = %#v, want zero value", got)
	}

	wantCalls := []string{modelBundleVersion}
	if gotCalls := resolver.Calls(); !reflect.DeepEqual(gotCalls, wantCalls) {
		t.Fatalf("Calls() = %#v, want %#v", gotCalls, wantCalls)
	}
}

func TestResolverPropagatesCancelledContextWithoutRecordingCall(
	t *testing.T,
) {
	resolver := New(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := resolver.ResolveServingBundle(ctx, "bundle-v2")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("ResolveServingBundle() error = %v, want context.Canceled", err)
	}
	if gotCalls := len(resolver.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}

func TestResolverRejectsNilContextWithoutRecordingCall(t *testing.T) {
	resolver := New(nil)

	_, err := resolver.ResolveServingBundle(nil, "bundle-v2")
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf("ResolveServingBundle() error = %v, want %v", err, ErrNilContext)
	}
	if gotCalls := len(resolver.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}

func testMetadata(modelBundleVersion string) application.ServingBundleMetadata {
	return application.ServingBundleMetadata{
		ModelBundleVersion:          modelBundleVersion,
		TaxonomyVersion:             "taxonomy-v1",
		ClassificationPolicyVersion: "classification-policy-v1",
		FeatureSchemaVersion:        "xgb-feature-schema-v1",
		PreprocessingVersion:        "transformer-preprocessing-v2",
		TensorSchemaVersion:         "transformer-tensor-schema-v2",
		FusionContractVersion:       "hierarchical-fusion-v1",
		ServingContractVersion:      "variable-star-serving-contract-v1",
		Entrypoint: application.ServingEntrypointMetadata{
			ModelName:        "variable_star_classifier",
			ModelVersion:     "1",
			Backend:          "python",
			Protocol:         application.ServingProtocolTritonV2HTTP,
			BinaryTensorData: true,
			MaxBatchSize:     0,
			Inputs: []application.ServingTensorContract{
				{
					Name:     "TIME_MJD",
					DataType: application.TensorDataTypeFP64,
					Dims:     []int64{-1},
					Required: true,
				},
				{
					Name:     "REUSED_COARSE_PROBS",
					DataType: application.TensorDataTypeFP32,
					Dims:     []int64{7},
					Required: true,
				},
			},
			Outputs: []application.ServingTensorContract{
				{
					Name:     "LEAF_PROBS",
					DataType: application.TensorDataTypeFP32,
					Dims:     []int64{12},
				},
			},
		},
		CoarseProbabilityOrder: [application.CoarseClassCount]string{
			"ROTATING",
			"CATACLYSMIC",
			"ECLIPSING_BINARY",
			"LONG_PERIOD",
			"PULSATING",
			"RR_LYRAE",
			"SUPERNOVA",
		},
		ConditionalFineProbabilityOrder: [application.ConditionalFineClassCount]string{
			"EW",
			"EA",
			"BY_DRA",
			"RS_CVN",
			"RRAB",
			"RRC",
			"SR",
			"MIRA",
			"DSCT",
			"CEP",
		},
		LeafProbabilityOrder: [application.LeafClassCount]string{
			"EW",
			"EA",
			"BY_DRA",
			"RS_CVN",
			"RRAB",
			"RRC",
			"SR",
			"MIRA",
			"DSCT",
			"CEP",
			"CATACLYSMIC",
			"SUPERNOVA",
		},
	}
}
