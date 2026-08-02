package triton

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestClientExecutesVersionedModelRequests(t *testing.T) {
	tests := []struct {
		name       string
		operation  ModelOperation
		wantMethod string
		wantPath   string
		body       []byte
	}{
		{
			name:       "metadata",
			operation:  ModelOperationMetadata,
			wantMethod: http.MethodGet,
			wantPath:   "/v2/models/variable_star_classifier/versions/1",
		},
		{
			name:       "config",
			operation:  ModelOperationConfig,
			wantMethod: http.MethodGet,
			wantPath:   "/v2/models/variable_star_classifier/versions/1/config",
		},
		{
			name:       "ready",
			operation:  ModelOperationReady,
			wantMethod: http.MethodGet,
			wantPath:   "/v2/models/variable_star_classifier/versions/1/ready",
		},
		{
			name:       "infer",
			operation:  ModelOperationInfer,
			wantMethod: http.MethodPost,
			wantPath:   "/v2/models/variable_star_classifier/versions/1/infer",
			body:       []byte("binary-request"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(
				http.HandlerFunc(
					func(
						writer http.ResponseWriter,
						request *http.Request,
					) {
						if request.Method != test.wantMethod {
							t.Fatalf(
								"method = %q, want %q",
								request.Method,
								test.wantMethod,
							)
						}

						if request.URL.Path != test.wantPath {
							t.Fatalf(
								"path = %q, want %q",
								request.URL.Path,
								test.wantPath,
							)
						}

						if request.Header.Get("X-Test-Header") != "test-value" {
							t.Fatalf(
								"X-Test-Header = %q",
								request.Header.Get("X-Test-Header"),
							)
						}

						body, err := io.ReadAll(request.Body)
						if err != nil {
							t.Fatalf(
								"io.ReadAll(request.Body) error = %v",
								err,
							)
						}

						if string(body) != string(test.body) {
							t.Fatalf(
								"body = %q, want %q",
								body,
								test.body,
							)
						}

						writer.Header().Set(
							"X-Triton-Test",
							"response-value",
						)
						writer.WriteHeader(http.StatusOK)

						if _, err := writer.Write(
							[]byte("response-body"),
						); err != nil {
							t.Fatalf(
								"writer.Write() error = %v",
								err,
							)
						}
					},
				),
			)
			defer server.Close()

			client, err := NewClient(
				server.URL+"/",
				server.Client(),
				1024,
			)
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			response, err := client.Do(
				context.Background(),
				ModelRequest{
					ModelName:    "variable_star_classifier",
					ModelVersion: "1",
					Operation:    test.operation,
					Header: http.Header{
						"X-Test-Header": []string{
							"test-value",
						},
					},
					Body: test.body,
				},
			)
			if err != nil {
				t.Fatalf("Do() error = %v", err)
			}

			if response.StatusCode != http.StatusOK {
				t.Fatalf(
					"StatusCode = %d, want %d",
					response.StatusCode,
					http.StatusOK,
				)
			}

			if response.Header.Get("X-Triton-Test") != "response-value" {
				t.Fatalf(
					"X-Triton-Test = %q",
					response.Header.Get("X-Triton-Test"),
				)
			}

			if string(response.Body) != "response-body" {
				t.Fatalf(
					"Body = %q, want response-body",
					response.Body,
				)
			}
		})
	}
}

func TestClientReturnsHTTPStatusError(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				_ *http.Request,
			) {
				writer.Header().Set(
					"Content-Type",
					"application/json",
				)
				writer.WriteHeader(http.StatusServiceUnavailable)

				_, _ = writer.Write(
					[]byte(`{"error":"model unavailable"}`),
				)
			},
		),
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	response, err := client.Do(
		context.Background(),
		ModelRequest{
			ModelName:    "variable_star_classifier",
			ModelVersion: "1",
			Operation:    ModelOperationReady,
		},
	)

	var statusErr *HTTPStatusError
	if !errors.As(err, &statusErr) {
		t.Fatalf(
			"Do() error = %v, want HTTPStatusError",
			err,
		)
	}

	if statusErr.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf(
			"error status = %d, want %d",
			statusErr.StatusCode,
			http.StatusServiceUnavailable,
		)
	}

	if string(statusErr.Body) != `{"error":"model unavailable"}` {
		t.Fatalf(
			"error body = %q",
			statusErr.Body,
		)
	}

	if response.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf(
			"response status = %d, want %d",
			response.StatusCode,
			http.StatusServiceUnavailable,
		)
	}
}

func TestClientRejectsOversizedResponse(t *testing.T) {
	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				_ *http.Request,
			) {
				_, _ = writer.Write([]byte("12345"))
			},
		),
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		4,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	_, err = client.Do(
		context.Background(),
		ModelRequest{
			ModelName:    "variable_star_classifier",
			ModelVersion: "1",
			Operation:    ModelOperationMetadata,
		},
	)

	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf(
			"Do() error = %v, want ErrResponseTooLarge",
			err,
		)
	}
}

func TestClientRejectsInvalidConfiguration(t *testing.T) {
	httpClient := &http.Client{}

	tests := []struct {
		name            string
		baseURL         string
		client          *http.Client
		maxResponseSize int64
	}{
		{
			name:            "empty base URL",
			baseURL:         "",
			client:          httpClient,
			maxResponseSize: 1024,
		},
		{
			name:            "unsupported scheme",
			baseURL:         "ftp://localhost:8000",
			client:          httpClient,
			maxResponseSize: 1024,
		},
		{
			name:            "missing host",
			baseURL:         "http:///v2",
			client:          httpClient,
			maxResponseSize: 1024,
		},
		{
			name:            "nil HTTP client",
			baseURL:         "http://localhost:8000",
			client:          nil,
			maxResponseSize: 1024,
		},
		{
			name:            "invalid response limit",
			baseURL:         "http://localhost:8000",
			client:          httpClient,
			maxResponseSize: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewClient(
				test.baseURL,
				test.client,
				test.maxResponseSize,
			)

			if !errors.Is(
				err,
				ErrInvalidClientConfiguration,
			) {
				t.Fatalf(
					"NewClient() error = %v, want ErrInvalidClientConfiguration",
					err,
				)
			}
		})
	}
}

func TestClientRejectsInvalidModelRequest(t *testing.T) {
	client, err := NewClient(
		"http://localhost:8000",
		&http.Client{},
		1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	tests := []ModelRequest{
		{
			ModelName:    "",
			ModelVersion: "1",
			Operation:    ModelOperationMetadata,
		},
		{
			ModelName:    "variable/star",
			ModelVersion: "1",
			Operation:    ModelOperationMetadata,
		},
		{
			ModelName:    "variable_star_classifier",
			ModelVersion: "",
			Operation:    ModelOperationMetadata,
		},
		{
			ModelName:    "variable_star_classifier",
			ModelVersion: "latest",
			Operation:    ModelOperationMetadata,
		},
		{
			ModelName:    "variable_star_classifier",
			ModelVersion: "1",
			Operation:    ModelOperationUnspecified,
		},
		{
			ModelName:    "variable_star_classifier",
			ModelVersion: "1",
			Operation:    ModelOperationConfig,
			Body:         []byte("unexpected"),
		},
	}

	for index, request := range tests {
		_, requestErr := client.Do(
			context.Background(),
			request,
		)

		if !errors.Is(
			requestErr,
			ErrInvalidModelRequest,
		) {
			t.Fatalf(
				"request %d error = %v, want ErrInvalidModelRequest",
				index,
				requestErr,
			)
		}
	}
}

func TestClientHonorsCanceledContext(t *testing.T) {
	called := false

	server := httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				_ *http.Request,
			) {
				called = true
				writer.WriteHeader(http.StatusOK)
			},
		),
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = client.Do(
		ctx,
		ModelRequest{
			ModelName:    "variable_star_classifier",
			ModelVersion: "1",
			Operation:    ModelOperationMetadata,
		},
	)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"Do() error = %v, want context.Canceled",
			err,
		)
	}

	if called {
		t.Fatal("HTTP server was called after context cancellation")
	}
}

func TestClientRejectsNilContext(t *testing.T) {
	client, err := NewClient(
		"http://localhost:8000",
		&http.Client{},
		1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	_, err = client.Do(
		nil,
		ModelRequest{
			ModelName:    "variable_star_classifier",
			ModelVersion: "1",
			Operation:    ModelOperationMetadata,
		},
	)

	if !errors.Is(err, ErrNilContext) {
		t.Fatalf(
			"Do() error = %v, want ErrNilContext",
			err,
		)
	}
}

func TestHTTPStatusErrorMessage(t *testing.T) {
	err := &HTTPStatusError{
		StatusCode: http.StatusBadRequest,
	}

	if !strings.Contains(err.Error(), "400") {
		t.Fatalf(
			"Error() = %q, want status code",
			err.Error(),
		)
	}
}
