package lightcurve

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestRepositoryGetRevision(
	t *testing.T,
) {
	var gotMethod string
	var gotPath string
	var gotAccept string

	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				gotMethod = request.Method
				gotPath = request.URL.EscapedPath()
				gotAccept =
					request.Header.Get("Accept")

				writer.Header().Set(
					"Content-Type",
					"application/json",
				)

				_, _ = io.WriteString(
					writer,
					`{
						"object_id": "OBJ 001/alpha",
						"light_curve_revision": 7,
						"eligible_epoch_count": 3,
						"quality_policy_version": "quality-v1",
						"epochs": [
							{
								"observation_time": 60003.0,
								"magnitude": 14.3,
								"magnitude_error": 0.03
							},
							{
								"observation_time": 60001.0,
								"magnitude": 14.1,
								"magnitude_error": 0.01
							},
							{
								"observation_time": 60002.0,
								"magnitude": 14.2,
								"magnitude_error": 0.02
							}
						],

						"future_optional_field": "allowed"
					}`,
				)
			},
		),
	)
	defer server.Close()

	repository, err := NewRepository(
		server.URL,
		server.Client(),
	)
	if err != nil {
		t.Fatalf(
			"NewRepository() error = %v",
			err,
		)
	}

	got, err := repository.GetRevision(
		context.Background(),
		"OBJ 001/alpha",
		7,
	)
	if err != nil {
		t.Fatalf(
			"GetRevision() error = %v",
			err,
		)
	}

	if gotMethod != http.MethodGet {
		t.Fatalf(
			"HTTP method = %q, want GET",
			gotMethod,
		)
	}

	if gotPath !=
		"/internal/v1/objects/OBJ%20001%2Falpha/light-curves/7" {
		t.Fatalf(
			"HTTP escaped path = %q",
			gotPath,
		)
	}

	if gotAccept != "application/json" {
		t.Fatalf(
			"Accept = %q, want application/json",
			gotAccept,
		)
	}

	if got.ObjectID != "OBJ 001/alpha" {
		t.Fatalf(
			"ObjectID = %q",
			got.ObjectID,
		)
	}

	if got.Revision != 7 {
		t.Fatalf(
			"Revision = %d, want 7",
			got.Revision,
		)
	}

	if got.EligibleEpochCount != 3 {
		t.Fatalf(
			"EligibleEpochCount = %d, want 3",
			got.EligibleEpochCount,
		)
	}

	if got.QualityPolicyVersion == nil ||
		*got.QualityPolicyVersion != "quality-v1" {
		t.Fatalf(
			"QualityPolicyVersion = %#v",
			got.QualityPolicyVersion,
		)
	}

	if len(got.Epochs) != 3 {
		t.Fatalf(
			"epoch count = %d, want 3",
			len(got.Epochs),
		)
	}

	// Adapter 不排序。
	if got.Epochs[0].ObservationTime !=
		60003.0 {
		t.Fatalf(
			"first observation time = %v, want original order 60003",
			got.Epochs[0].ObservationTime,
		)
	}
}

func TestRepositoryGetRevisionAllowsMissingOptionalQualityPolicyVersion(
	t *testing.T,
) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				_ *http.Request,
			) {
				_, _ = io.WriteString(
					writer,
					`{
						"object_id": "OBJ-001",
						"light_curve_revision": 1,
						"eligible_epoch_count": 0,
						"epochs": []
					}`,
				)
			},
		),
	)
	defer server.Close()

	repository, err := NewRepository(
		server.URL,
		server.Client(),
	)
	if err != nil {
		t.Fatalf(
			"NewRepository() error = %v",
			err,
		)
	}

	got, err := repository.GetRevision(
		context.Background(),
		"OBJ-001",
		1,
	)
	if err != nil {
		t.Fatalf(
			"GetRevision() error = %v",
			err,
		)
	}

	if got.QualityPolicyVersion != nil {
		t.Fatalf(
			"QualityPolicyVersion = %#v, want nil",
			got.QualityPolicyVersion,
		)
	}

	if got.Epochs == nil {
		t.Fatal(
			"Epochs = nil, want non-nil empty slice",
		)
	}

	if len(got.Epochs) != 0 {
		t.Fatalf(
			"epoch count = %d, want 0",
			len(got.Epochs),
		)
	}
}

func TestRepositoryGetRevisionMapsHTTPStatus(
	t *testing.T,
) {
	tests := []struct {
		name       string
		statusCode int
		wantError  error
	}{
		{
			name:       "404 not found",
			statusCode: http.StatusNotFound,
			wantError: application.
				ErrLightCurveRevisionNotFound,
		},
		{
			name:       "409 not ready",
			statusCode: http.StatusConflict,
			wantError: application.
				ErrLightCurveRevisionNotReady,
		},
		{
			name:       "422 inconsistent",
			statusCode: http.StatusUnprocessableEntity,
			wantError: application.
				ErrLightCurveRevisionInconsistent,
		},
		{
			name:       "429 unavailable",
			statusCode: http.StatusTooManyRequests,
			wantError: application.
				ErrLightCurveSourceUnavailable,
		},
		{
			name:       "500 unavailable",
			statusCode: http.StatusInternalServerError,
			wantError: application.
				ErrLightCurveSourceUnavailable,
		},
		{
			name:       "503 unavailable",
			statusCode: http.StatusServiceUnavailable,
			wantError: application.
				ErrLightCurveSourceUnavailable,
		},
		{
			name:       "unexpected 502 unavailable",
			statusCode: http.StatusBadGateway,
			wantError: application.
				ErrLightCurveSourceUnavailable,
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

						_, _ = io.WriteString(
							writer,
							"upstream error body",
						)
					},
				),
			)
			defer server.Close()

			repository, err := NewRepository(
				server.URL,
				server.Client(),
			)
			if err != nil {
				t.Fatalf(
					"NewRepository() error = %v",
					err,
				)
			}

			_, got := repository.GetRevision(
				context.Background(),
				"OBJ-001",
				7,
			)

			if !errors.Is(
				got,
				test.wantError,
			) {
				t.Fatalf(
					"GetRevision() error = %v, want errors.Is(..., %v)",
					got,
					test.wantError,
				)
			}
		})
	}
}

func TestRepositoryGetRevisionRejectsMalformedResponse(
	t *testing.T,
) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "malformed JSON",
			body: `{`,
		},
		{
			name: "missing object id",
			body: `{
				"light_curve_revision": 7,
				"eligible_epoch_count": 3,
				"epochs": []
			}`,
		},
		{
			name: "missing revision",
			body: `{
				"object_id": "OBJ-001",
				"eligible_epoch_count": 3,
				"epochs": []
			}`,
		},
		{
			name: "missing eligible count",
			body: `{
				"object_id": "OBJ-001",
				"light_curve_revision": 7,
				"epochs": []
			}`,
		},
		{
			name: "missing epochs",
			body: `{
				"object_id": "OBJ-001",
				"light_curve_revision": 7,
				"eligible_epoch_count": 3
			}`,
		},
		{
			name: "missing epoch magnitude",
			body: `{
				"object_id": "OBJ-001",
				"light_curve_revision": 7,
				"eligible_epoch_count": 1,
				"epochs": [
					{
						"observation_time": 60001,
						"magnitude_error": 0.01
					}
				]
			}`,
		},
		{
			name: "multiple JSON values",
			body: `{
				"object_id": "OBJ-001",
				"light_curve_revision": 7,
				"eligible_epoch_count": 0,
				"epochs": []
			} {}`,
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
						_, _ = io.WriteString(
							writer,
							test.body,
						)
					},
				),
			)
			defer server.Close()

			repository, err := NewRepository(
				server.URL,
				server.Client(),
			)
			if err != nil {
				t.Fatalf(
					"NewRepository() error = %v",
					err,
				)
			}

			_, got := repository.GetRevision(
				context.Background(),
				"OBJ-001",
				7,
			)

			if !errors.Is(
				got,
				application.
					ErrLightCurveRevisionInconsistent,
			) {
				t.Fatalf(
					"GetRevision() error = %v, want ErrLightCurveRevisionInconsistent",
					got,
				)
			}
		})
	}
}

func TestRepositoryGetRevisionRejectsIdentityMismatch(
	t *testing.T,
) {
	tests := []struct {
		name string
		body string
	}{
		{
			name: "object mismatch",
			body: `{
				"object_id": "OTHER",
				"light_curve_revision": 7,
				"eligible_epoch_count": 0,
				"epochs": []
			}`,
		},
		{
			name: "revision mismatch",
			body: `{
				"object_id": "OBJ-001",
				"light_curve_revision": 8,
				"eligible_epoch_count": 0,
				"epochs": []
			}`,
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
						_, _ = io.WriteString(
							writer,
							test.body,
						)
					},
				),
			)
			defer server.Close()

			repository, err := NewRepository(
				server.URL,
				server.Client(),
			)
			if err != nil {
				t.Fatalf(
					"NewRepository() error = %v",
					err,
				)
			}

			_, got := repository.GetRevision(
				context.Background(),
				"OBJ-001",
				7,
			)

			if !errors.Is(
				got,
				application.
					ErrLightCurveRevisionInconsistent,
			) {
				t.Fatalf(
					"GetRevision() error = %v, want ErrLightCurveRevisionInconsistent",
					got,
				)
			}
		})
	}
}

func TestRepositoryGetRevisionMapsNetworkFailure(
	t *testing.T,
) {
	client := &http.Client{
		Transport: roundTripFunc(
			func(
				*http.Request,
			) (*http.Response, error) {
				return nil, errors.New(
					"network unavailable",
				)
			},
		),
	}

	repository, err := NewRepository(
		"http://light-curve.test",
		client,
	)
	if err != nil {
		t.Fatalf(
			"NewRepository() error = %v",
			err,
		)
	}

	_, got := repository.GetRevision(
		context.Background(),
		"OBJ-001",
		7,
	)

	if !errors.Is(
		got,
		application.ErrLightCurveSourceUnavailable,
	) {
		t.Fatalf(
			"GetRevision() error = %v, want ErrLightCurveSourceUnavailable",
			got,
		)
	}
}

func TestRepositoryGetRevisionPreservesContextCancellation(
	t *testing.T,
) {
	repository, err := NewRepository(
		"http://light-curve.test",
		&http.Client{},
	)
	if err != nil {
		t.Fatalf(
			"NewRepository() error = %v",
			err,
		)
	}

	ctx, cancel := context.WithCancel(
		context.Background(),
	)
	cancel()

	_, got := repository.GetRevision(
		ctx,
		"OBJ-001",
		7,
	)

	if !errors.Is(
		got,
		context.Canceled,
	) {
		t.Fatalf(
			"GetRevision() error = %v, want context.Canceled",
			got,
		)
	}
}

func TestNewRepositoryRejectsInvalidConfiguration(
	t *testing.T,
) {
	tests := []struct {
		name       string
		baseURL    string
		httpClient *http.Client
	}{
		{
			name: "empty base URL",

			httpClient: &http.Client{},
		},
		{
			name: "surrounding whitespace",

			baseURL: " http://light-curve.test",

			httpClient: &http.Client{},
		},
		{
			name: "unsupported scheme",

			baseURL: "ftp://light-curve.test",

			httpClient: &http.Client{},
		},
		{
			name: "missing host",

			baseURL: "http:///path",

			httpClient: &http.Client{},
		},
		{
			name: "query not allowed",

			baseURL: "http://light-curve.test?x=1",

			httpClient: &http.Client{},
		},
		{
			name: "nil HTTP client",

			baseURL: "http://light-curve.test",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			repository, err := NewRepository(
				test.baseURL,
				test.httpClient,
			)

			if err == nil {
				t.Fatalf(
					"error = nil, repository = %#v",
					repository,
				)
			}

			if !errors.Is(
				err,
				ErrInvalidRepositoryConfiguration,
			) {
				t.Fatalf(
					"error = %v, want ErrInvalidRepositoryConfiguration",
					err,
				)
			}
		})
	}
}

func TestRepositoryGetRevisionRejectsInvalidRequest(
	t *testing.T,
) {
	repository, err := NewRepository(
		"http://light-curve.test",
		&http.Client{},
	)
	if err != nil {
		t.Fatalf(
			"NewRepository() error = %v",
			err,
		)
	}

	tests := []struct {
		name     string
		objectID string
		revision int64
	}{
		{
			name:     "empty object id",
			revision: 1,
		},
		{
			name:     "zero revision",
			objectID: "OBJ-001",
			revision: 0,
		},
		{
			name:     "negative revision",
			objectID: "OBJ-001",
			revision: -1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, got := repository.GetRevision(
				context.Background(),
				test.objectID,
				test.revision,
			)

			if !errors.Is(
				got,
				ErrInvalidRevisionRequest,
			) {
				t.Fatalf(
					"GetRevision() error = %v, want ErrInvalidRevisionRequest",
					got,
				)
			}
		})
	}
}

type roundTripFunc func(
	*http.Request,
) (*http.Response, error)

func (roundTrip roundTripFunc) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return roundTrip(request)
}

func TestRepositoryDoesNotTrimObjectID(
	t *testing.T,
) {
	var escapedPath string

	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				escapedPath =
					request.URL.EscapedPath()

				_, _ = io.WriteString(
					writer,
					`{
						"object_id": " OBJ-001 ",
						"light_curve_revision": 1,
						"eligible_epoch_count": 0,
						"epochs": []
					}`,
				)
			},
		),
	)
	defer server.Close()

	repository, err := NewRepository(
		server.URL,
		server.Client(),
	)
	if err != nil {
		t.Fatalf(
			"NewRepository() error = %v",
			err,
		)
	}

	_, err = repository.GetRevision(
		context.Background(),
		" OBJ-001 ",
		1,
	)
	if err != nil {
		t.Fatalf(
			"GetRevision() error = %v",
			err,
		)
	}

	if !strings.Contains(
		escapedPath,
		"%20OBJ-001%20",
	) {
		t.Fatalf(
			"escaped path = %q, object ID appears to have been rewritten",
			escapedPath,
		)
	}
}
