package triton

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return function(request)
}

func TestVariableStarClassifierClassifyModes(t *testing.T) {
	tests := []struct {
		name           string
		mode           application.CoarseMode
		reused         *[application.CoarseClassCount]float32
		wantExecuted   bool
		wantSentReused [application.CoarseClassCount]float32
	}{
		{
			name:         "compute current",
			mode:         application.CoarseModeComputeCurrent,
			wantExecuted: true,
		},
		{
			name: "reuse previous",
			mode: application.CoarseModeReusePrevious,
			reused: &[application.CoarseClassCount]float32{
				0.10, 0.20, 0.15, 0.10, 0.15, 0.10, 0.20,
			},
			wantExecuted: false,
			wantSentReused: [application.CoarseClassCount]float32{
				0.10, 0.20, 0.15, 0.10, 0.15, 0.10, 0.20,
			},
		},
		{
			name:         "compute bootstrap",
			mode:         application.CoarseModeComputeBootstrap,
			wantExecuted: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			httpClient := &http.Client{
				Transport: roundTripFunc(
					func(
						request *http.Request,
					) (*http.Response, error) {
						if request.Method != http.MethodPost {
							t.Fatalf(
								"method = %q, want POST",
								request.Method,
							)
						}

						if request.URL.Path !=
							"/v2/models/variable_star_classifier/versions/1/infer" {
							t.Fatalf(
								"path = %q",
								request.URL.Path,
							)
						}

						tensors := readClassificationRequest(
							t,
							request,
						)

						assertRequestTensorNames(t, tensors)

						timeValues, err := DecodeFP64Values(
							tensors[timeMJDInputName],
						)
						if err != nil {
							t.Fatalf(
								"DecodeFP64Values() error = %v",
								err,
							)
						}
						if !reflect.DeepEqual(
							timeValues,
							[]float64{60001, 60002, 60003},
						) {
							t.Fatalf(
								"TIME_MJD = %v",
								timeValues,
							)
						}

						magnitudeValues, err := DecodeFP32Values(
							tensors[magnitudeInputName],
						)
						if err != nil {
							t.Fatalf(
								"DecodeFP32Values() error = %v",
								err,
							)
						}
						if !reflect.DeepEqual(
							magnitudeValues,
							[]float32{14.1, 14.2, 14.3},
						) {
							t.Fatalf(
								"MAGNITUDE = %v",
								magnitudeValues,
							)
						}

						modeValues, err := DecodeINT32Values(
							tensors[coarseModeInputName],
						)
						if err != nil {
							t.Fatalf(
								"DecodeINT32Values() error = %v",
								err,
							)
						}
						if len(modeValues) != 1 ||
							modeValues[0] != int32(test.mode) {
							t.Fatalf(
								"COARSE_MODE = %v",
								modeValues,
							)
						}

						reusedValues, err := DecodeFP32Values(
							tensors[reusedCoarseProbsInputName],
						)
						if err != nil {
							t.Fatalf(
								"DecodeFP32Values() error = %v",
								err,
							)
						}
						if !reflect.DeepEqual(
							reusedValues,
							test.wantSentReused[:],
						) {
							t.Fatalf(
								"REUSED_COARSE_PROBS = %v, want %v",
								reusedValues,
								test.wantSentReused,
							)
						}

						return successfulClassificationResponse(
							t,
							test.wantExecuted,
						), nil
					},
				),
			}

			client, err := NewClient(
				"http://triton.test",
				httpClient,
				1024*1024,
			)
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			classifier, err := NewVariableStarClassifier(
				client,
				classifierTestEntrypoint(),
			)
			if err != nil {
				t.Fatalf(
					"NewVariableStarClassifier() error = %v",
					err,
				)
			}

			output, err := classifier.Classify(
				context.Background(),
				application.ClassificationInput{
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
					CoarseMode:                test.mode,
					ReusedCoarseProbabilities: test.reused,
				},
			)
			if err != nil {
				t.Fatalf("Classify() error = %v", err)
			}

			want := expectedClassificationOutput(
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

func TestVariableStarClassifierRejectsInvalidInput(t *testing.T) {
	httpCalls := 0

	client, err := NewClient(
		"http://triton.test",
		&http.Client{
			Transport: roundTripFunc(
				func(
					_ *http.Request,
				) (*http.Response, error) {
					httpCalls++
					return nil, errors.New(
						"HTTP must not be called",
					)
				},
			),
		},
		1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	classifier, err := NewVariableStarClassifier(
		client,
		classifierTestEntrypoint(),
	)
	if err != nil {
		t.Fatalf(
			"NewVariableStarClassifier() error = %v",
			err,
		)
	}

	reused := [application.CoarseClassCount]float32{
		0.10, 0.20, 0.15, 0.10, 0.15, 0.10, 0.20,
	}

	tests := []struct {
		name  string
		input application.ClassificationInput
	}{
		{
			name: "unknown mode",
			input: application.ClassificationInput{
				TimeMJD:        []float64{1, 2, 3},
				Magnitude:      []float32{1, 2, 3},
				MagnitudeError: []float32{1, 1, 1},
				CoarseMode:     application.CoarseModeUnspecified,
			},
		},
		{
			name: "reuse without probabilities",
			input: application.ClassificationInput{
				TimeMJD:        []float64{1, 2, 3},
				Magnitude:      []float32{1, 2, 3},
				MagnitudeError: []float32{1, 1, 1},
				CoarseMode:     application.CoarseModeReusePrevious,
			},
		},
		{
			name: "compute with reused probabilities",
			input: application.ClassificationInput{
				TimeMJD:                   []float64{1, 2, 3},
				Magnitude:                 []float32{1, 2, 3},
				MagnitudeError:            []float32{1, 1, 1},
				CoarseMode:                application.CoarseModeComputeBootstrap,
				ReusedCoarseProbabilities: &reused,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, classifyErr := classifier.Classify(
				context.Background(),
				test.input,
			)

			if !errors.Is(
				classifyErr,
				ErrInvalidClassificationInput,
			) {
				t.Fatalf(
					"Classify() error = %v, want ErrInvalidClassificationInput",
					classifyErr,
				)
			}
		})
	}

	if httpCalls != 0 {
		t.Fatalf(
			"HTTP call count = %d, want 0",
			httpCalls,
		)
	}
}

func TestVariableStarClassifierRejectsInvalidOutput(t *testing.T) {
	tests := []struct {
		name     string
		response func(*testing.T) *http.Response
	}{
		{
			name: "wrong model version",
			response: func(t *testing.T) *http.Response {
				tensors := classificationResponseTensors(
					t,
					true,
				)
				return binaryClassificationResponse(
					t,
					"variable_star_classifier",
					"2",
					tensors,
				)
			},
		},
		{
			name: "missing output",
			response: func(t *testing.T) *http.Response {
				tensors := classificationResponseTensors(
					t,
					true,
				)
				return binaryClassificationResponse(
					t,
					"variable_star_classifier",
					"1",
					tensors[:3],
				)
			},
		},
		{
			name: "execution flag mismatch",
			response: func(t *testing.T) *http.Response {
				return successfulClassificationResponse(
					t,
					false,
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(
				"http://triton.test",
				&http.Client{
					Transport: roundTripFunc(
						func(
							_ *http.Request,
						) (*http.Response, error) {
							return test.response(t), nil
						},
					),
				},
				1024*1024,
			)
			if err != nil {
				t.Fatalf("NewClient() error = %v", err)
			}

			classifier, err := NewVariableStarClassifier(
				client,
				classifierTestEntrypoint(),
			)
			if err != nil {
				t.Fatalf(
					"NewVariableStarClassifier() error = %v",
					err,
				)
			}

			_, classifyErr := classifier.Classify(
				context.Background(),
				application.ClassificationInput{
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
					CoarseMode: application.CoarseModeComputeCurrent,
				},
			)

			if !errors.Is(
				classifyErr,
				ErrInvalidClassificationOutput,
			) {
				t.Fatalf(
					"Classify() error = %v, want ErrInvalidClassificationOutput",
					classifyErr,
				)
			}
		})
	}
}

func TestNewVariableStarClassifierRejectsInvalidConfiguration(
	t *testing.T,
) {
	_, err := NewVariableStarClassifier(
		nil,
		classifierTestEntrypoint(),
	)
	if !errors.Is(
		err,
		ErrInvalidClassifierConfiguration,
	) {
		t.Fatalf(
			"nil client error = %v",
			err,
		)
	}

	client, err := NewClient(
		"http://triton.test",
		&http.Client{},
		1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	entrypoint := classifierTestEntrypoint()
	entrypoint.ModelVersion = "latest"

	_, err = NewVariableStarClassifier(
		client,
		entrypoint,
	)
	if !errors.Is(
		err,
		ErrInvalidClassifierConfiguration,
	) {
		t.Fatalf(
			"invalid version error = %v",
			err,
		)
	}
}

func classifierTestEntrypoint() application.ServingEntrypointMetadata {
	return application.ServingEntrypointMetadata{
		ModelName:        "variable_star_classifier",
		ModelVersion:     "1",
		Protocol:         application.ServingProtocolTritonV2HTTP,
		BinaryTensorData: true,
		MaxBatchSize:     0,
	}
}

func readClassificationRequest(
	t *testing.T,
	request *http.Request,
) map[string]BinaryTensor {
	t.Helper()

	if request.Header.Get("Content-Type") != BinaryContentType {
		t.Fatalf(
			"Content-Type = %q",
			request.Header.Get("Content-Type"),
		)
	}

	body, err := io.ReadAll(request.Body)
	if err != nil {
		t.Fatalf("io.ReadAll() error = %v", err)
	}

	headerLength, err := strconv.Atoi(
		request.Header.Get(
			InferenceHeaderContentLength,
		),
	)
	if err != nil {
		t.Fatalf("strconv.Atoi() error = %v", err)
	}

	var header binaryInferRequestHeader
	if err := json.Unmarshal(
		body[:headerLength],
		&header,
	); err != nil {
		t.Fatalf("json.Unmarshal() error = %v", err)
	}

	tensors := make(map[string]BinaryTensor, len(header.Inputs))
	offset := headerLength

	for _, input := range header.Inputs {
		end := offset + input.Parameters.BinaryDataSize

		tensors[input.Name] = BinaryTensor{
			Name:     input.Name,
			DataType: input.DataType,
			Shape:    append([]int64(nil), input.Shape...),
			Data:     append([]byte(nil), body[offset:end]...),
		}

		offset = end
	}

	if offset != len(body) {
		t.Fatalf(
			"request has %d trailing bytes",
			len(body)-offset,
		)
	}

	return tensors
}

func assertRequestTensorNames(
	t *testing.T,
	tensors map[string]BinaryTensor,
) {
	t.Helper()

	for _, name := range []string{
		timeMJDInputName,
		magnitudeInputName,
		magnitudeErrorInputName,
		coarseModeInputName,
		reusedCoarseProbsInputName,
	} {
		if _, exists := tensors[name]; !exists {
			t.Fatalf("missing input tensor %q", name)
		}
	}

	if len(tensors) != 5 {
		t.Fatalf(
			"input tensor count = %d, want 5",
			len(tensors),
		)
	}
}

func expectedClassificationOutput(
	executed bool,
) application.ClassificationOutput {
	return application.ClassificationOutput{
		CoarseProbabilities: [application.CoarseClassCount]float32{
			0.10, 0.20, 0.15, 0.10, 0.15, 0.10, 0.20,
		},
		ConditionalFineProbabilities: [application.ConditionalFineClassCount]float32{
			0.60, 0.40,
			0.70, 0.30,
			0.80, 0.20,
			0.55, 0.45,
			0.65, 0.35,
		},
		LeafProbabilities: [application.LeafClassCount]float32{
			0.06, 0.04,
			0.14, 0.06,
			0.12, 0.03,
			0.08, 0.02,
			0.09, 0.06,
			0.10, 0.20,
		},
		XGBoostExecuted: executed,
	}
}

func classificationResponseTensors(
	t *testing.T,
	executed bool,
) []BinaryTensor {
	t.Helper()

	output := expectedClassificationOutput(executed)

	coarse, err := NewFP32Tensor(
		coarseProbsOutputName,
		[]int64{application.CoarseClassCount},
		output.CoarseProbabilities[:],
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor(coarse) error = %v", err)
	}

	fine, err := NewFP32Tensor(
		fineConditionalOutputName,
		[]int64{application.ConditionalFineClassCount},
		output.ConditionalFineProbabilities[:],
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor(fine) error = %v", err)
	}

	leaf, err := NewFP32Tensor(
		leafProbsOutputName,
		[]int64{application.LeafClassCount},
		output.LeafProbabilities[:],
	)
	if err != nil {
		t.Fatalf("NewFP32Tensor(leaf) error = %v", err)
	}

	flag, err := NewBOOLTensor(
		xgboostExecutedOutputName,
		[]int64{1},
		[]bool{executed},
	)
	if err != nil {
		t.Fatalf("NewBOOLTensor() error = %v", err)
	}

	// 故意不按请求顺序返回，验证 Adapter 按名称映射。
	return []BinaryTensor{
		flag,
		leaf,
		coarse,
		fine,
	}
}

func successfulClassificationResponse(
	t *testing.T,
	executed bool,
) *http.Response {
	t.Helper()

	return binaryClassificationResponse(
		t,
		"variable_star_classifier",
		"1",
		classificationResponseTensors(t, executed),
	)
}

func binaryClassificationResponse(
	t *testing.T,
	modelName string,
	modelVersion string,
	tensors []BinaryTensor,
) *http.Response {
	t.Helper()

	header := binaryInferResponseHeader{
		ModelName:    modelName,
		ModelVersion: modelVersion,
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
		t.Fatalf("json.Marshal() error = %v", err)
	}

	body := append([]byte(nil), jsonHeader...)
	for _, tensor := range tensors {
		body = append(body, tensor.Data...)
	}

	return &http.Response{
		StatusCode: http.StatusOK,
		Header: http.Header{
			"Content-Type": []string{
				BinaryContentType,
			},
			InferenceHeaderContentLength: []string{
				strconv.Itoa(len(jsonHeader)),
			},
		},
		Body: io.NopCloser(bytes.NewReader(body)),
	}
}
