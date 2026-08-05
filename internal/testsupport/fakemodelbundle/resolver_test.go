package fakemodelbundle

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
	modelBundleVersion := " bundle-2026-07-001 "

	want := application.ModelBundleMetadata{
		ModelBundleVersion: modelBundleVersion,
	}

	responses := map[string]Response{
		modelBundleVersion: {
			Metadata: want,
		},
	}

	resolver := New(responses)

	// New 必须复制配置 map。
	responses[modelBundleVersion] = Response{
		Metadata: application.ModelBundleMetadata{
			ModelBundleVersion: "changed-after-new",
		},
	}

	got, err := resolver.Resolve(
		context.Background(),
		modelBundleVersion,
	)
	if err != nil {
		t.Fatalf("Resolve() error = %v", err)
	}
	if got != want {
		t.Fatalf("Resolve() = %#v, want %#v", got, want)
	}

	wantCalls := []string{modelBundleVersion}
	if gotCalls := resolver.Calls(); !reflect.DeepEqual(
		gotCalls,
		wantCalls,
	) {
		t.Fatalf(
			"Calls() = %#v, want %#v",
			gotCalls,
			wantCalls,
		)
	}

	// Calls 必须返回独立 slice。
	calls := resolver.Calls()
	calls[0] = "modified"

	if gotCalls := resolver.Calls(); !reflect.DeepEqual(
		gotCalls,
		wantCalls,
	) {
		t.Fatalf(
			"Calls() after mutation = %#v, want %#v",
			gotCalls,
			wantCalls,
		)
	}
}

func TestResolverReturnsConfiguredErrors(t *testing.T) {
	transientErr := errors.New("bundle source unavailable")

	tests := []struct {
		name string
		err  error
	}{
		{
			name: "bundle not found",
			err:  application.ErrModelBundleNotFound,
		},
		{
			name: "source unavailable",
			err:  transientErr,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			const modelBundleVersion = "bundle-v1"

			resolver := New(
				map[string]Response{
					modelBundleVersion: {
						Err: test.err,
					},
				},
			)

			got, err := resolver.Resolve(
				context.Background(),
				modelBundleVersion,
			)
			if !errors.Is(err, test.err) {
				t.Fatalf(
					"Resolve() error = %v, want %v",
					err,
					test.err,
				)
			}
			if got != (application.ModelBundleMetadata{}) {
				t.Fatalf(
					"Resolve() = %#v, want zero value",
					got,
				)
			}

			wantCalls := []string{modelBundleVersion}
			if gotCalls := resolver.Calls(); !reflect.DeepEqual(
				gotCalls,
				wantCalls,
			) {
				t.Fatalf(
					"Calls() = %#v, want %#v",
					gotCalls,
					wantCalls,
				)
			}
		})
	}
}

func TestResolverRejectsUnconfiguredRequest(t *testing.T) {
	resolver := New(nil)

	const modelBundleVersion = "missing-bundle"

	got, err := resolver.Resolve(
		context.Background(),
		modelBundleVersion,
	)
	if !errors.Is(err, ErrUnconfiguredRequest) {
		t.Fatalf(
			"Resolve() error = %v, want %v",
			err,
			ErrUnconfiguredRequest,
		)
	}
	if got != (application.ModelBundleMetadata{}) {
		t.Fatalf(
			"Resolve() = %#v, want zero value",
			got,
		)
	}

	wantCalls := []string{modelBundleVersion}
	if gotCalls := resolver.Calls(); !reflect.DeepEqual(
		gotCalls,
		wantCalls,
	) {
		t.Fatalf(
			"Calls() = %#v, want %#v",
			gotCalls,
			wantCalls,
		)
	}
}

func TestResolverPropagatesCancelledContextWithoutRecordingCall(
	t *testing.T,
) {
	resolver := New(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := resolver.Resolve(ctx, "bundle-v1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"Resolve() error = %v, want context.Canceled",
			err,
		)
	}
	if gotCalls := len(resolver.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}

func TestResolverRejectsNilContextWithoutRecordingCall(t *testing.T) {
	resolver := New(nil)

	_, err := resolver.Resolve(nil, "bundle-v1")
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf(
			"Resolve() error = %v, want %v",
			err,
			ErrNilContext,
		)
	}
	if gotCalls := len(resolver.Calls()); gotCalls != 0 {
		t.Fatalf("call count = %d, want 0", gotCalls)
	}
}
