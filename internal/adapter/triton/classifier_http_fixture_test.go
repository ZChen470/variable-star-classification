package triton

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestVariableStarClassifierHTTPServerSuccessFixtures(t *testing.T) {
	reused := [application.CoarseClassCount]float32{
		0.10,
		0.20,
		0.15,
		0.10,
		0.15,
		0.10,
		0.20,
	}

	tests := []struct {
		name           string
		mode           application.CoarseMode
		reused         *[application.CoarseClassCount]float32
		wantSentReused [application.CoarseClassCount]float32
		wantExecuted   bool
	}{
		{
			name:         "success compute current",
			mode:         application.CoarseModeComputeCurrent,
			wantExecuted: true,
		},
		{
			name:           "success reuse previous",
			mode:           application.CoarseModeReusePrevious,
			reused:         &reused,
			wantSentReused: reused,
			wantExecuted:   false,
		},
		{
			name:         "success compute bootstrap",
			mode:         application.CoarseModeComputeBootstrap,
			wantExecuted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlerErrors := make(chan error, 1)

			server := httptest.NewServer(
				http.HandlerFunc(
					func(
						writer http.ResponseWriter,
						request *http.Request,
					) {
						err := verifyHTTPFixtureRequest(
							request,
							test.mode,
							test.wantSentReused,
						)
						if err != nil {
							recordHTTPFixtureError(
								handlerErrors,
								err,
							)
							http.Error(
								writer,
								err.Error(),
								http.StatusBadRequest,
							)
							return
						}

						if err := writeHTTPFixtureResponse(
							writer,
							test.wantExecuted,
						); err != nil {
							recordHTTPFixtureError(
								handlerErrors,
								err,
							)
						}
					},
				),
			)
			t.Cleanup(server.Close)

			classifier := newHTTPFixtureClassifier(
				t,
				server,
			)

			output, err := classifier.Classify(
				context.Background(),
				httpFixtureClassificationInput(
					test.mode,
					test.reused,
				),
			)
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}

			select {
			case handlerErr := <-handlerErrors:
				t.Fatalf(
					"HTTP fixture handler error = %v",
					handlerErr,
				)
			default:
			}

			want := httpFixtureExpectedOutput(
				test.wantExecuted,
			)
			if output != want {
				t.Fatalf(
					"Classify() output = %#v, want %#v",
					output,
					want,
				)
			}
		})
	}
}

func TestVariableStarClassifierHTTPStatusFixtures(t *testing.T) {
	tests := []struct {
		name       string
		statusCode int
		body       string
	}{
		{
			name:       "bad request",
			statusCode: http.StatusBadRequest,
			body:       `{"error":"invalid inference request"}`,
		},
		{
			name:       "service unavailable",
			statusCode: http.StatusServiceUnavailable,
			body:       `{"error":"model is unavailable"}`,
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
						if request.URL.Path !=
							"/v2/models/variable_star_classifier/versions/1/infer" {
							http.NotFound(writer, request)
							return
						}

						writer.Header().Set(
							"Content-Type",
							"application/json",
						)
						writer.WriteHeader(test.statusCode)

						_, _ = writer.Write(
							[]byte(test.body),
						)
					},
				),
			)
			t.Cleanup(server.Close)

			classifier := newHTTPFixtureClassifier(
				t,
				server,
			)

			_, err := classifier.Classify(
				context.Background(),
				httpFixtureClassificationInput(
					application.CoarseModeComputeCurrent,
					nil,
				),
			)
			if err == nil {
				t.Fatal("Classify() error = nil")
			}

			var statusErr *HTTPStatusError
			if !errors.As(err, &statusErr) {
				t.Fatalf(
					"Classify() error = %v, want HTTPStatusError",
					err,
				)
			}

			if statusErr.StatusCode != test.statusCode {
				t.Fatalf(
					"StatusCode = %d, want %d",
					statusErr.StatusCode,
					test.statusCode,
				)
			}

			if string(statusErr.Body) != test.body {
				t.Fatalf(
					"Body = %q, want %q",
					statusErr.Body,
					test.body,
				)
			}

			if statusErr.Header.Get("Content-Type") !=
				"application/json" {
				t.Fatalf(
					"Content-Type = %q",
					statusErr.Header.Get("Content-Type"),
				)
			}
		})
	}
}

func TestVariableStarClassifierMalformedHTTPResponseFixtures(
	t *testing.T,
) {
	tests := []struct {
		name  string
		write func(http.ResponseWriter) error
	}{
		{
			name: "missing inference header length",
			write: func(writer http.ResponseWriter) error {
				writer.Header().Set(
					"Content-Type",
					BinaryContentType,
				)
				writer.WriteHeader(http.StatusOK)

				_, err := writer.Write(
					[]byte(`{"model_name":"variable_star_classifier"}`),
				)
				return err
			},
		},
		{
			name: "truncated binary payload",
			write: func(writer http.ResponseWriter) error {
				size := int64(
					application.CoarseClassCount * 4,
				)

				header := binaryInferResponseHeader{
					ModelName:    "variable_star_classifier",
					ModelVersion: "1",
					Outputs: []binaryInferResponseOutputHeader{
						{
							Name: coarseProbsOutputName,
							Shape: []int64{
								application.CoarseClassCount,
							},
							DataType: TensorDataTypeFP32,
							Parameters: binaryResponseParameters{
								BinaryDataSize: &size,
							},
						},
					},
				}

				jsonHeader, err := json.Marshal(header)
				if err != nil {
					return err
				}

				writer.Header().Set(
					"Content-Type",
					BinaryContentType,
				)
				writer.Header().Set(
					InferenceHeaderContentLength,
					strconv.Itoa(len(jsonHeader)),
				)
				writer.WriteHeader(http.StatusOK)

				_, err = writer.Write(jsonHeader)
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			handlerErrors := make(chan error, 1)

			server := httptest.NewServer(
				http.HandlerFunc(
					func(
						writer http.ResponseWriter,
						request *http.Request,
					) {
						if request.URL.Path !=
							"/v2/models/variable_star_classifier/versions/1/infer" {
							http.NotFound(writer, request)
							return
						}

						if err := test.write(writer); err != nil {
							recordHTTPFixtureError(
								handlerErrors,
								err,
							)
						}
					},
				),
			)
			t.Cleanup(server.Close)

			classifier := newHTTPFixtureClassifier(
				t,
				server,
			)

			_, err := classifier.Classify(
				context.Background(),
				httpFixtureClassificationInput(
					application.CoarseModeComputeCurrent,
					nil,
				),
			)
			if !errors.Is(
				err,
				ErrInvalidBinaryInferResponse,
			) {
				t.Fatalf(
					"Classify() error = %v, want ErrInvalidBinaryInferResponse",
					err,
				)
			}

			select {
			case handlerErr := <-handlerErrors:
				t.Fatalf(
					"HTTP fixture handler error = %v",
					handlerErr,
				)
			default:
			}
		})
	}
}

func newHTTPFixtureClassifier(
	t *testing.T,
	server *httptest.Server,
) *VariableStarClassifier {
	t.Helper()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024*1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	classifier, err := NewVariableStarClassifier(
		client,
		application.ServingEntrypointMetadata{
			ModelName:        "variable_star_classifier",
			ModelVersion:     "1",
			Protocol:         application.ServingProtocolTritonV2HTTP,
			BinaryTensorData: true,
			MaxBatchSize:     0,
		},
	)
	if err != nil {
		t.Fatalf(
			"NewVariableStarClassifier() error = %v",
			err,
		)
	}

	return classifier
}

func httpFixtureClassificationInput(
	mode application.CoarseMode,
	reused *[application.CoarseClassCount]float32,
) application.ClassificationInput {
	return application.ClassificationInput{
		TimeMJD: []float64{
			60001,
			60002,
			60003,
		},
		Magnitude: []float32{
			14.1,
			14.2,
			14.3,
		},
		MagnitudeError: []float32{
			0.01,
			0.02,
			0.03,
		},
		CoarseMode:                mode,
		ReusedCoarseProbabilities: reused,
	}
}

func verifyHTTPFixtureRequest(
	request *http.Request,
	wantMode application.CoarseMode,
	wantReused [application.CoarseClassCount]float32,
) error {
	if request.Method != http.MethodPost {
		return fmt.Errorf(
			"method=%q, want POST",
			request.Method,
		)
	}

	const wantPath = "/v2/models/variable_star_classifier/versions/1/infer"

	if request.URL.Path != wantPath {
		return fmt.Errorf(
			"path=%q, want %q",
			request.URL.Path,
			wantPath,
		)
	}

	if request.Header.Get("Content-Type") !=
		BinaryContentType {
		return fmt.Errorf(
			"Content-Type=%q, want %q",
			request.Header.Get("Content-Type"),
			BinaryContentType,
		)
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		return fmt.Errorf("read request body: %w", err)
	}

	if request.ContentLength != int64(len(body)) {
		return fmt.Errorf(
			"Content-Length=%d, body length=%d",
			request.ContentLength,
			len(body),
		)
	}

	headerLength, err := strconv.Atoi(
		request.Header.Get(
			InferenceHeaderContentLength,
		),
	)
	if err != nil ||
		headerLength <= 0 ||
		headerLength > len(body) {
		return fmt.Errorf(
			"invalid %s=%q",
			InferenceHeaderContentLength,
			request.Header.Get(
				InferenceHeaderContentLength,
			),
		)
	}

	var header binaryInferRequestHeader
	if err := json.Unmarshal(
		body[:headerLength],
		&header,
	); err != nil {
		return fmt.Errorf(
			"decode request header: %w",
			err,
		)
	}

	wantInputs := []struct {
		name     string
		dataType TensorDataType
		shape    []int64
	}{
		{
			name:     timeMJDInputName,
			dataType: TensorDataTypeFP64,
			shape:    []int64{3},
		},
		{
			name:     magnitudeInputName,
			dataType: TensorDataTypeFP32,
			shape:    []int64{3},
		},
		{
			name:     magnitudeErrorInputName,
			dataType: TensorDataTypeFP32,
			shape:    []int64{3},
		},
		{
			name:     coarseModeInputName,
			dataType: TensorDataTypeINT32,
			shape:    []int64{1},
		},
		{
			name:     reusedCoarseProbsInputName,
			dataType: TensorDataTypeFP32,
			shape: []int64{
				application.CoarseClassCount,
			},
		},
	}

	if len(header.Inputs) != len(wantInputs) {
		return fmt.Errorf(
			"input count=%d, want %d",
			len(header.Inputs),
			len(wantInputs),
		)
	}

	tensors := make(map[string]BinaryTensor, len(header.Inputs))
	offset := headerLength

	for index, want := range wantInputs {
		actual := header.Inputs[index]

		if actual.Name != want.name {
			return fmt.Errorf(
				"input[%d].name=%q, want %q",
				index,
				actual.Name,
				want.name,
			)
		}

		if actual.DataType != want.dataType {
			return fmt.Errorf(
				"input %q datatype=%q, want %q",
				actual.Name,
				actual.DataType,
				want.dataType,
			)
		}

		if !reflect.DeepEqual(actual.Shape, want.shape) {
			return fmt.Errorf(
				"input %q shape=%v, want %v",
				actual.Name,
				actual.Shape,
				want.shape,
			)
		}

		size := actual.Parameters.BinaryDataSize
		if size <= 0 || offset > len(body)-size {
			return fmt.Errorf(
				"input %q has invalid binary_data_size=%d",
				actual.Name,
				size,
			)
		}

		end := offset + size

		tensor := BinaryTensor{
			Name:     actual.Name,
			DataType: actual.DataType,
			Shape:    append([]int64(nil), actual.Shape...),
			Data:     append([]byte(nil), body[offset:end]...),
		}

		if err := validateBinaryTensor(tensor); err != nil {
			return err
		}

		tensors[tensor.Name] = tensor
		offset = end
	}

	if offset != len(body) {
		return fmt.Errorf(
			"request has %d trailing bytes",
			len(body)-offset,
		)
	}

	wantOutputs := []string{
		coarseProbsOutputName,
		fineConditionalOutputName,
		leafProbsOutputName,
		xgboostExecutedOutputName,
	}

	if len(header.Outputs) != len(wantOutputs) {
		return fmt.Errorf(
			"output count=%d, want %d",
			len(header.Outputs),
			len(wantOutputs),
		)
	}

	for index, wantName := range wantOutputs {
		actual := header.Outputs[index]

		if actual.Name != wantName {
			return fmt.Errorf(
				"output[%d].name=%q, want %q",
				index,
				actual.Name,
				wantName,
			)
		}

		if !actual.Parameters.BinaryData {
			return fmt.Errorf(
				"output %q did not request binary data",
				actual.Name,
			)
		}
	}

	timeValues, err := DecodeFP64Values(
		tensors[timeMJDInputName],
	)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(
		timeValues,
		[]float64{60001, 60002, 60003},
	) {
		return fmt.Errorf(
			"TIME_MJD=%v",
			timeValues,
		)
	}

	magnitudeValues, err := DecodeFP32Values(
		tensors[magnitudeInputName],
	)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(
		magnitudeValues,
		[]float32{14.1, 14.2, 14.3},
	) {
		return fmt.Errorf(
			"MAGNITUDE=%v",
			magnitudeValues,
		)
	}

	errorValues, err := DecodeFP32Values(
		tensors[magnitudeErrorInputName],
	)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(
		errorValues,
		[]float32{0.01, 0.02, 0.03},
	) {
		return fmt.Errorf(
			"MAGNITUDE_ERROR=%v",
			errorValues,
		)
	}

	modeValues, err := DecodeINT32Values(
		tensors[coarseModeInputName],
	)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(
		modeValues,
		[]int32{int32(wantMode)},
	) {
		return fmt.Errorf(
			"COARSE_MODE=%v, want [%d]",
			modeValues,
			wantMode,
		)
	}

	reusedValues, err := DecodeFP32Values(
		tensors[reusedCoarseProbsInputName],
	)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(
		reusedValues,
		wantReused[:],
	) {
		return fmt.Errorf(
			"REUSED_COARSE_PROBS=%v, want %v",
			reusedValues,
			wantReused,
		)
	}

	return nil
}

func writeHTTPFixtureResponse(
	writer http.ResponseWriter,
	executed bool,
) error {
	tensors, err := httpFixtureResponseTensors(executed)
	if err != nil {
		return err
	}

	header := binaryInferResponseHeader{
		ID:           "fixture-response",
		ModelName:    "variable_star_classifier",
		ModelVersion: "1",
		Outputs: make(
			[]binaryInferResponseOutputHeader,
			len(tensors),
		),
	}

	for index, tensor := range tensors {
		size := int64(len(tensor.Data))

		header.Outputs[index] =
			binaryInferResponseOutputHeader{
				Name:     tensor.Name,
				Shape:    append([]int64(nil), tensor.Shape...),
				DataType: tensor.DataType,
				Parameters: binaryResponseParameters{
					BinaryDataSize: &size,
				},
			}
	}

	jsonHeader, err := json.Marshal(header)
	if err != nil {
		return err
	}

	writer.Header().Set(
		"Content-Type",
		BinaryContentType,
	)
	writer.Header().Set(
		InferenceHeaderContentLength,
		strconv.Itoa(len(jsonHeader)),
	)
	writer.WriteHeader(http.StatusOK)

	if _, err := writer.Write(jsonHeader); err != nil {
		return err
	}

	for _, tensor := range tensors {
		if _, err := writer.Write(tensor.Data); err != nil {
			return err
		}
	}

	return nil
}

func httpFixtureResponseTensors(
	executed bool,
) ([]BinaryTensor, error) {
	output := httpFixtureExpectedOutput(executed)

	coarse, err := NewFP32Tensor(
		coarseProbsOutputName,
		[]int64{application.CoarseClassCount},
		output.CoarseProbabilities[:],
	)
	if err != nil {
		return nil, err
	}

	fine, err := NewFP32Tensor(
		fineConditionalOutputName,
		[]int64{application.ConditionalFineClassCount},
		output.ConditionalFineProbabilities[:],
	)
	if err != nil {
		return nil, err
	}

	leaf, err := NewFP32Tensor(
		leafProbsOutputName,
		[]int64{application.LeafClassCount},
		output.LeafProbabilities[:],
	)
	if err != nil {
		return nil, err
	}

	flag, err := NewBOOLTensor(
		xgboostExecutedOutputName,
		[]int64{1},
		[]bool{executed},
	)
	if err != nil {
		return nil, err
	}

	// 故意使用与客户端请求不同的顺序。
	return []BinaryTensor{
		flag,
		leaf,
		coarse,
		fine,
	}, nil
}

func httpFixtureExpectedOutput(
	executed bool,
) application.ClassificationOutput {
	return application.ClassificationOutput{
		CoarseProbabilities: [application.CoarseClassCount]float32{
			0.10,
			0.20,
			0.15,
			0.10,
			0.15,
			0.10,
			0.20,
		},
		ConditionalFineProbabilities: [application.ConditionalFineClassCount]float32{
			0.60,
			0.40,
			0.70,
			0.30,
			0.80,
			0.20,
			0.55,
			0.45,
			0.65,
			0.35,
		},
		LeafProbabilities: [application.LeafClassCount]float32{
			0.06,
			0.04,
			0.14,
			0.06,
			0.12,
			0.03,
			0.08,
			0.02,
			0.09,
			0.06,
			0.10,
			0.20,
		},
		XGBoostExecuted: executed,
	}
}

func recordHTTPFixtureError(
	target chan<- error,
	err error,
) {
	select {
	case target <- err:
	default:
	}
}
