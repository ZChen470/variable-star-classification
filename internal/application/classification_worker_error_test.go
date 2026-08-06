package application_test

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/adapter/triton"
	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestWrapClassificationWorkerErrorClassifiesKnownCauses(
	t *testing.T,
) {
	commandCause := &application.PermanentClassificationCommandError{
		Code: application.
			ClassificationCommandErrorCodeMalformedProto,
		Field: "value",
		Err:   errors.New("invalid protobuf"),
	}

	tests := []struct {
		name      string
		operation application.ClassificationWorkerOperation
		cause     error

		wantClass application.ClassificationWorkerErrorClass
		wantCode  application.ClassificationWorkerErrorCode
	}{
		{
			name:      "cancelled context",
			operation: application.ClassificationWorkerOperationClassify,
			cause:     context.Canceled,
			wantClass: application.ClassificationWorkerErrorClassCancelled,
			wantCode:  application.ClassificationWorkerErrorCodeCancelled,
		},
		{
			name:      "deadline exceeded",
			operation: application.ClassificationWorkerOperationPrepareInput,
			cause:     context.DeadlineExceeded,
			wantClass: application.ClassificationWorkerErrorClassCancelled,
			wantCode:  application.ClassificationWorkerErrorCodeCancelled,
		},
		{
			name:      "invalid command",
			operation: application.ClassificationWorkerOperationDecodeCommand,
			cause:     commandCause,
			wantClass: application.ClassificationWorkerErrorClassPermanent,
			wantCode: application.ClassificationWorkerErrorCode(
				application.
					ClassificationCommandErrorCodeMalformedProto,
			),
		},
		{
			name:      "light curve not found",
			operation: application.ClassificationWorkerOperationPrepareInput,
			cause:     application.ErrLightCurveRevisionNotFound,
			wantClass: application.ClassificationWorkerErrorClassPermanent,
			wantCode:  application.ClassificationWorkerErrorCodeLightCurveNotFound,
		},
		{
			name:      "light curve not ready",
			operation: application.ClassificationWorkerOperationPrepareInput,
			cause:     application.ErrLightCurveRevisionNotReady,
			wantClass: application.ClassificationWorkerErrorClassRetryable,
			wantCode:  application.ClassificationWorkerErrorCodeLightCurveNotReady,
		},
		{
			name:      "light curve unavailable",
			operation: application.ClassificationWorkerOperationPrepareInput,
			cause:     application.ErrLightCurveSourceUnavailable,
			wantClass: application.ClassificationWorkerErrorClassRetryable,
			wantCode:  application.ClassificationWorkerErrorCodeLightCurveUnavailable,
		},
		{
			name:      "invalid light curve",
			operation: application.ClassificationWorkerOperationPrepareInput,
			cause:     application.ErrDuplicateObservationTime,
			wantClass: application.ClassificationWorkerErrorClassPermanent,
			wantCode:  application.ClassificationWorkerErrorCodeLightCurveInvalid,
		},
		{
			name:      "bundle not found",
			operation: application.ClassificationWorkerOperationResolveBundle,
			cause:     application.ErrServingBundleNotFound,
			wantClass: application.ClassificationWorkerErrorClassPermanent,
			wantCode:  application.ClassificationWorkerErrorCodeModelBundleNotFound,
		},
		{
			name:      "HTTP 400",
			operation: application.ClassificationWorkerOperationClassify,
			cause: &triton.HTTPStatusError{
				StatusCode: http.StatusBadRequest,
			},
			wantClass: application.ClassificationWorkerErrorClassPermanent,
			wantCode:  application.ClassificationWorkerErrorCodeTritonRequestRejected,
		},
		{
			name:      "HTTP 429",
			operation: application.ClassificationWorkerOperationClassify,
			cause: &triton.HTTPStatusError{
				StatusCode: http.StatusTooManyRequests,
			},
			wantClass: application.ClassificationWorkerErrorClassRetryable,
			wantCode:  application.ClassificationWorkerErrorCodeTritonUnavailable,
		},
		{
			name:      "HTTP 503",
			operation: application.ClassificationWorkerOperationClassify,
			cause: &triton.HTTPStatusError{
				StatusCode: http.StatusServiceUnavailable,
			},
			wantClass: application.ClassificationWorkerErrorClassRetryable,
			wantCode:  application.ClassificationWorkerErrorCodeTritonUnavailable,
		},
		{
			name:      "unknown publish failure",
			operation: application.ClassificationWorkerOperationPublishResult,
			cause:     errors.New("Kafka temporarily unavailable"),
			wantClass: application.ClassificationWorkerErrorClassRetryable,
			wantCode:  application.ClassificationWorkerErrorCodePublishFailed,
		},
		{
			name:      "unknown internal builder failure",
			operation: application.ClassificationWorkerOperationBuildRun,
			cause:     errors.New("unexpected builder failure"),
			wantClass: application.ClassificationWorkerErrorClassPermanent,
			wantCode:  application.ClassificationWorkerErrorCodeInternalInvalid,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			wrapped := application.WrapClassificationWorkerError(
				test.operation,
				test.cause,
			)

			var workerError *application.ClassificationWorkerError
			if !errors.As(wrapped, &workerError) {
				t.Fatalf(
					"error = %v, want ClassificationWorkerError",
					wrapped,
				)
			}

			if workerError.Class != test.wantClass {
				t.Fatalf(
					"Class = %s, want %s",
					workerError.Class,
					test.wantClass,
				)
			}

			if workerError.Code != test.wantCode {
				t.Fatalf(
					"Code = %q, want %q",
					workerError.Code,
					test.wantCode,
				)
			}

			if workerError.Operation != test.operation {
				t.Fatalf(
					"Operation = %q, want %q",
					workerError.Operation,
					test.operation,
				)
			}

			if !errors.Is(wrapped, test.cause) {
				t.Fatalf(
					"errors.Is(error, cause) = false; error=%v cause=%v",
					wrapped,
					test.cause,
				)
			}
		})
	}
}

func TestWrapClassificationWorkerErrorPreservesWrappedCause(
	t *testing.T,
) {
	cause := application.ErrLightCurveRevisionNotReady
	wrappedCause := fmt.Errorf(
		"read fixed revision: %w",
		cause,
	)

	got := application.WrapClassificationWorkerError(
		application.ClassificationWorkerOperationPrepareInput,
		wrappedCause,
	)

	if !errors.Is(got, cause) {
		t.Fatalf(
			"errors.Is(error, cause) = false; error=%v",
			got,
		)
	}
}

func TestWrapClassificationWorkerErrorNilCause(t *testing.T) {
	got := application.WrapClassificationWorkerError(
		application.ClassificationWorkerOperationClassify,
		nil,
	)

	if got != nil {
		t.Fatalf(
			"WrapClassificationWorkerError(nil) = %v, want nil",
			got,
		)
	}
}
