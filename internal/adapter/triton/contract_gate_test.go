package triton

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

func TestModelContractGateVerify(t *testing.T) {
	server := newContractTestServer(
		t,
		http.StatusOK,
		validModelMetadata(),
		validModelConfig(),
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024*1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	gate, err := NewModelContractGate(client)
	if err != nil {
		t.Fatalf("NewModelContractGate() error = %v", err)
	}

	err = gate.Verify(
		context.Background(),
		expectedEntrypoint(),
	)
	if err != nil {
		t.Fatalf("Verify() error = %v", err)
	}
}

func TestModelContractGateRejectsNotReadyModel(t *testing.T) {
	server := newContractTestServer(
		t,
		http.StatusServiceUnavailable,
		validModelMetadata(),
		validModelConfig(),
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024*1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	gate, err := NewModelContractGate(client)
	if err != nil {
		t.Fatalf("NewModelContractGate() error = %v", err)
	}

	err = gate.Verify(
		context.Background(),
		expectedEntrypoint(),
	)
	if !errors.Is(err, ErrModelNotReady) {
		t.Fatalf(
			"Verify() error = %v, want ErrModelNotReady",
			err,
		)
	}
}

func TestModelContractGateRejectsMetadataMismatch(t *testing.T) {
	metadata := validModelMetadata()
	metadata.Inputs[0].DataType = "FP32"

	server := newContractTestServer(
		t,
		http.StatusOK,
		metadata,
		validModelConfig(),
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024*1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	gate, err := NewModelContractGate(client)
	if err != nil {
		t.Fatalf("NewModelContractGate() error = %v", err)
	}

	err = gate.Verify(
		context.Background(),
		expectedEntrypoint(),
	)
	if !errors.Is(err, ErrModelContractMismatch) {
		t.Fatalf(
			"Verify() error = %v, want ErrModelContractMismatch",
			err,
		)
	}
}

func TestModelContractGateRejectsConfigMismatch(t *testing.T) {
	config := validModelConfig()
	config.MaxBatchSize = 8

	server := newContractTestServer(
		t,
		http.StatusOK,
		validModelMetadata(),
		config,
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024*1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	gate, err := NewModelContractGate(client)
	if err != nil {
		t.Fatalf("NewModelContractGate() error = %v", err)
	}

	err = gate.Verify(
		context.Background(),
		expectedEntrypoint(),
	)
	if !errors.Is(err, ErrModelContractMismatch) {
		t.Fatalf(
			"Verify() error = %v, want ErrModelContractMismatch",
			err,
		)
	}
}

func TestModelContractGateRejectsTensorOrderMismatch(t *testing.T) {
	config := validModelConfig()
	config.Inputs[0], config.Inputs[1] =
		config.Inputs[1], config.Inputs[0]

	server := newContractTestServer(
		t,
		http.StatusOK,
		validModelMetadata(),
		config,
	)
	defer server.Close()

	client, err := NewClient(
		server.URL,
		server.Client(),
		1024*1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	gate, err := NewModelContractGate(client)
	if err != nil {
		t.Fatalf("NewModelContractGate() error = %v", err)
	}

	err = gate.Verify(
		context.Background(),
		expectedEntrypoint(),
	)
	if !errors.Is(err, ErrModelContractMismatch) {
		t.Fatalf(
			"Verify() error = %v, want ErrModelContractMismatch",
			err,
		)
	}
}

func TestModelContractGateContextHandling(t *testing.T) {
	client, err := NewClient(
		"http://localhost:8000",
		&http.Client{},
		1024,
	)
	if err != nil {
		t.Fatalf("NewClient() error = %v", err)
	}

	gate, err := NewModelContractGate(client)
	if err != nil {
		t.Fatalf("NewModelContractGate() error = %v", err)
	}

	err = gate.Verify(nil, expectedEntrypoint())
	if !errors.Is(err, ErrNilContext) {
		t.Fatalf(
			"nil context error = %v, want ErrNilContext",
			err,
		)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err = gate.Verify(ctx, expectedEntrypoint())
	if !errors.Is(err, context.Canceled) {
		t.Fatalf(
			"cancelled context error = %v",
			err,
		)
	}
}

func TestNewModelContractGateRejectsNilClient(t *testing.T) {
	_, err := NewModelContractGate(nil)
	if !errors.Is(err, ErrInvalidModelContractGate) {
		t.Fatalf(
			"error = %v, want ErrInvalidModelContractGate",
			err,
		)
	}
}

func newContractTestServer(
	t *testing.T,
	readyStatus int,
	metadata modelMetadataResponse,
	config modelConfigResponse,
) *httptest.Server {
	t.Helper()

	metadataBody, err := json.Marshal(metadata)
	if err != nil {
		t.Fatalf("json.Marshal(metadata) error = %v", err)
	}

	configBody, err := json.Marshal(config)
	if err != nil {
		t.Fatalf("json.Marshal(config) error = %v", err)
	}

	return httptest.NewServer(
		http.HandlerFunc(
			func(
				writer http.ResponseWriter,
				request *http.Request,
			) {
				const basePath = "/v2/models/" +
					"variable_star_classifier/versions/1"

				switch request.URL.Path {
				case basePath + "/ready":
					writer.WriteHeader(readyStatus)

				case basePath:
					writer.Header().Set(
						"Content-Type",
						"application/json",
					)
					_, _ = writer.Write(metadataBody)

				case basePath + "/config":
					writer.Header().Set(
						"Content-Type",
						"application/json",
					)
					_, _ = writer.Write(configBody)

				default:
					http.NotFound(writer, request)
				}
			},
		),
	)
}

func expectedEntrypoint() application.ServingEntrypointMetadata {
	return application.ServingEntrypointMetadata{
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
				Name:     "MAGNITUDE",
				DataType: application.TensorDataTypeFP32,
				Dims:     []int64{-1},
				Required: true,
			},
			{
				Name:     "MAGNITUDE_ERROR",
				DataType: application.TensorDataTypeFP32,
				Dims:     []int64{-1},
				Required: true,
			},
			{
				Name:     "COARSE_MODE",
				DataType: application.TensorDataTypeINT32,
				Dims:     []int64{1},
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
				Name:     "COARSE_PROBS",
				DataType: application.TensorDataTypeFP32,
				Dims:     []int64{7},
			},
			{
				Name:     "FINE_CONDITIONAL_PROBS",
				DataType: application.TensorDataTypeFP32,
				Dims:     []int64{10},
			},
			{
				Name:     "LEAF_PROBS",
				DataType: application.TensorDataTypeFP32,
				Dims:     []int64{12},
			},
			{
				Name:     "XGBOOST_EXECUTED",
				DataType: application.TensorDataTypeBOOL,
				Dims:     []int64{1},
			},
		},
	}
}

func validModelMetadata() modelMetadataResponse {
	expected := expectedEntrypoint()

	return modelMetadataResponse{
		Name:     expected.ModelName,
		Versions: []string{expected.ModelVersion},
		Platform: expected.Backend,
		Inputs:   metadataTensors(expected.Inputs),
		Outputs:  metadataTensors(expected.Outputs),
	}
}

func metadataTensors(
	contracts []application.ServingTensorContract,
) []modelMetadataTensor {
	tensors := make([]modelMetadataTensor, len(contracts))

	for index, contract := range contracts {
		tensors[index] = modelMetadataTensor{
			Name:     contract.Name,
			DataType: string(contract.DataType),
			Shape:    append([]int64(nil), contract.Dims...),
		}
	}

	return tensors
}

func validModelConfig() modelConfigResponse {
	expected := expectedEntrypoint()

	return modelConfigResponse{
		Name:         expected.ModelName,
		Backend:      expected.Backend,
		MaxBatchSize: expected.MaxBatchSize,
		Inputs: configTensors(
			expected.Inputs,
			true,
		),
		Outputs: configTensors(
			expected.Outputs,
			false,
		),
	}
}

func configTensors(
	contracts []application.ServingTensorContract,
	useRequired bool,
) []modelConfigTensor {
	tensors := make([]modelConfigTensor, len(contracts))

	for index, contract := range contracts {
		optional := false
		if useRequired {
			optional = !contract.Required
		}

		tensors[index] = modelConfigTensor{
			Name:     contract.Name,
			DataType: "TYPE_" + string(contract.DataType),
			Dims:     append([]int64(nil), contract.Dims...),
			Optional: optional,
		}
	}

	return tensors
}
