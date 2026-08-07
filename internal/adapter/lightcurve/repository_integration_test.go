package lightcurve_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	lightcurveadapter "github.com/ZChen470/variable-star-classification/internal/adapter/lightcurve"
	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestHTTPRepositoryFeedsLightCurvePreparation(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				if request.URL.EscapedPath() !=
					"/internal/v1/objects/OBJ-001/light-curves/7" {
					t.Fatalf(
						"path = %q",
						request.URL.EscapedPath(),
					)
				}

				writer.Header().Set(
					"Content-Type",
					"application/json",
				)

				_, _ = io.WriteString(
					writer,
					`{
						"object_id": "OBJ-001",
						"light_curve_revision": 7,
						"eligible_epoch_count": 3,
						"epochs": [
							{
								"observation_time": 60003,
								"magnitude": 14.3,
								"magnitude_error": 0.03
							},
							{
								"observation_time": 60001,
								"magnitude": 14.1,
								"magnitude_error": 0.01
							},
							{
								"observation_time": 60002,
								"magnitude": 14.2,
								"magnitude_error": 0.02
							}
						]
					}`,
				)
			},
		),
	)
	defer server.Close()

	repository, err := lightcurveadapter.NewRepository(
		server.URL,
		server.Client(),
	)
	if err != nil {
		t.Fatalf(
			"NewRepository() error = %v",
			err,
		)
	}

	reader, err :=
		application.NewLightCurveRevisionReader(
			repository,
		)
	if err != nil {
		t.Fatalf(
			"NewLightCurveRevisionReader() error = %v",
			err,
		)
	}

	revision, err := reader.ReadRevision(
		context.Background(),
		"OBJ-001",
		7,
	)
	if err != nil {
		t.Fatalf(
			"ReadRevision() error = %v",
			err,
		)
	}

	// HTTP Adapter 必须保留上游顺序。
	if revision.Epochs[0].ObservationTime != 60003 {
		t.Fatalf(
			"raw first observation time = %v, want 60003",
			revision.Epochs[0].ObservationTime,
		)
	}

	prepared, err :=
		application.PrepareLightCurveRevision(
			revision,
			3,
		)
	if err != nil {
		t.Fatalf(
			"PrepareLightCurveRevision() error = %v",
			err,
		)
	}

	if len(prepared.Epochs) != 3 {
		t.Fatalf(
			"prepared epoch count = %d, want 3",
			len(prepared.Epochs),
		)
	}

	wantTimes := []float64{
		60001,
		60002,
		60003,
	}

	for index, want := range wantTimes {
		if prepared.Epochs[index].ObservationTime != want {
			t.Fatalf(
				"prepared epochs[%d].ObservationTime = %v, want %v",
				index,
				prepared.Epochs[index].ObservationTime,
				want,
			)
		}
	}

	// Prepare 必须只在副本上排序。
	if revision.Epochs[0].ObservationTime != 60003 {
		t.Fatalf(
			"raw revision was mutated; first observation time = %v",
			revision.Epochs[0].ObservationTime,
		)
	}
}

func TestHTTPRepositoryErrorsMapToWorkerClassification(
	t *testing.T,
) {
	tests := []struct {
		name string

		statusCode int

		wantCause error

		wantClass application.
				ClassificationWorkerErrorClass

		wantCode application.
				ClassificationWorkerErrorCode
	}{
		{
			name:       "404 permanent not found",
			statusCode: http.StatusNotFound,

			wantCause: application.
				ErrLightCurveRevisionNotFound,

			wantClass: application.
				ClassificationWorkerErrorClassPermanent,

			wantCode: application.
				ClassificationWorkerErrorCodeLightCurveNotFound,
		},
		{
			name:       "409 retryable not ready",
			statusCode: http.StatusConflict,

			wantCause: application.
				ErrLightCurveRevisionNotReady,

			wantClass: application.
				ClassificationWorkerErrorClassRetryable,

			wantCode: application.
				ClassificationWorkerErrorCodeLightCurveNotReady,
		},
		{
			name: "422 permanent inconsistent",

			statusCode: http.StatusUnprocessableEntity,

			wantCause: application.
				ErrLightCurveRevisionInconsistent,

			wantClass: application.
				ClassificationWorkerErrorClassPermanent,

			wantCode: application.
				ClassificationWorkerErrorCodeLightCurveInvalid,
		},
		{
			name: "503 retryable unavailable",

			statusCode: http.StatusServiceUnavailable,

			wantCause: application.
				ErrLightCurveSourceUnavailable,

			wantClass: application.
				ClassificationWorkerErrorClassRetryable,

			wantCode: application.
				ClassificationWorkerErrorCodeLightCurveUnavailable,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(
				http.HandlerFunc(
					func(
						writer http.ResponseWriter,
						_ *http.Request,
					) {
						writer.WriteHeader(
							test.statusCode,
						)
					},
				),
			)
			defer server.Close()

			repository, err :=
				lightcurveadapter.NewRepository(
					server.URL,
					server.Client(),
				)
			if err != nil {
				t.Fatalf(
					"NewRepository() error = %v",
					err,
				)
			}

			_, cause := repository.GetRevision(
				context.Background(),
				"OBJ-001",
				7,
			)

			if !errors.Is(
				cause,
				test.wantCause,
			) {
				t.Fatalf(
					"GetRevision() error = %v, want errors.Is(..., %v)",
					cause,
					test.wantCause,
				)
			}

			got :=
				application.
					WrapClassificationWorkerError(
						application.
							ClassificationWorkerOperationPrepareInput,
						cause,
					)

			var workerError *application.
				ClassificationWorkerError

			if !errors.As(
				got,
				&workerError,
			) {
				t.Fatalf(
					"error = %v, want ClassificationWorkerError",
					got,
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

			if workerError.Operation !=
				application.
					ClassificationWorkerOperationPrepareInput {
				t.Fatalf(
					"Operation = %q, want %q",
					workerError.Operation,
					application.
						ClassificationWorkerOperationPrepareInput,
				)
			}

			if !errors.Is(
				got,
				test.wantCause,
			) {
				t.Fatalf(
					"wrapped error lost original cause: %v",
					got,
				)
			}
		})
	}
}
