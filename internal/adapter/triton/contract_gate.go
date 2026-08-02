package triton

import (
	"context"
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/application"
)

var (
	ErrInvalidModelContractGate = errors.New("invalid Triton model contract gate")
	ErrModelNotReady            = errors.New("Triton model is not ready")
	ErrModelContractMismatch    = errors.New("Triton model contract mismatch")
)

type modelMetadataResponse struct {
	Name     string                `json:"name"`
	Versions []string              `json:"versions"`
	Platform string                `json:"platform"`
	Inputs   []modelMetadataTensor `json:"inputs"`
	Outputs  []modelMetadataTensor `json:"outputs"`
}

type modelMetadataTensor struct {
	Name     string  `json:"name"`
	DataType string  `json:"datatype"`
	Shape    []int64 `json:"shape"`
}

type modelConfigResponse struct {
	Name         string              `json:"name"`
	Platform     string              `json:"platform"`
	Backend      string              `json:"backend"`
	MaxBatchSize int                 `json:"max_batch_size"`
	Inputs       []modelConfigTensor `json:"input"`
	Outputs      []modelConfigTensor `json:"output"`
}

type modelConfigTensor struct {
	Name     string  `json:"name"`
	DataType string  `json:"data_type"`
	Dims     []int64 `json:"dims"`
	Optional bool    `json:"optional"`
}

// ModelContractGate 在程序启动时检查精确版本的 Triton 模型
type ModelContractGate struct {
	client *Client
}

func NewModelContractGate(client *Client) (*ModelContractGate, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidModelContractGate)
	}

	return &ModelContractGate{client: client}, nil
}

// Verify 依次检查 ready、metadata 和 config
func (gate *ModelContractGate) Verify(ctx context.Context, expected application.ServingEntrypointMetadata) error {
	if ctx == nil {
		return ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if gate == nil || gate.client == nil {
		return fmt.Errorf("%w: gate is not initialized", ErrInvalidModelContractGate)
	}

	if err := validateExpectedEntrypoint(expected); err != nil {
		return fmt.Errorf("%w: expected contract: %v", ErrModelContractMismatch, err)
	}

	if err := gate.verifyReady(ctx, expected); err != nil {
		return err
	}

	metadataResponse, err := gate.client.Do(ctx, ModelRequest{
		ModelName:    expected.ModelName,
		ModelVersion: expected.ModelVersion,
		Operation:    ModelOperationMetadata,
	})
	if err != nil {
		return fmt.Errorf("get Triton model metadata: %w", err)
	}
	var metadata modelMetadataResponse
	if err := decodeSingleJSON(metadataResponse.Body, &metadata); err != nil {
		return fmt.Errorf("%w: decode metadata: %v", ErrModelContractMismatch, err)
	}
	if err := validateModelMetadata(metadata, expected); err != nil {
		return fmt.Errorf("%w: metadata: %v", ErrModelContractMismatch, err)
	}

	configResponse, err := gate.client.Do(ctx, ModelRequest{
		ModelName:    expected.ModelName,
		ModelVersion: expected.ModelVersion,
		Operation:    ModelOperationConfig,
	})
	if err != nil {
		return fmt.Errorf("get Triton model config: %w", err)
	}

	var config modelConfigResponse
	if err := decodeSingleJSON(configResponse.Body, &config); err != nil {
		return fmt.Errorf("%w: decode config: %v", ErrModelContractMismatch, err)
	}

	if err := validateModelConfig(config, expected); err != nil {
		return fmt.Errorf("%w: config: %v", ErrModelContractMismatch, err)
	}

	return nil
}

func (gate *ModelContractGate) verifyReady(ctx context.Context, expected application.ServingEntrypointMetadata) error {
	_, err := gate.client.Do(ctx, ModelRequest{
		ModelName:    expected.ModelName,
		ModelVersion: expected.ModelVersion,
		Operation:    ModelOperationReady,
	})
	if err == nil {
		return nil
	}

	var statusErr *HTTPStatusError
	if errors.As(err, &statusErr) {
		return fmt.Errorf("%w: HTTP status %d", ErrModelNotReady, statusErr.StatusCode)
	}

	return fmt.Errorf("check Triton model readiness: %w", err)
}

func validateExpectedEntrypoint(expected application.ServingEntrypointMetadata) error {
	if !modelNamePattern.MatchString(expected.ModelName) {
		return fmt.Errorf(
			"invalid model name %q",
			expected.ModelName,
		)
	}
	if !modelVersionPattern.MatchString(expected.ModelVersion) {
		return fmt.Errorf("invalid model version %q", expected.ModelVersion)
	}
	if expected.Backend == "" {
		return errors.New("backend is empty")
	}
	if expected.Protocol != application.ServingProtocolTritonV2HTTP {
		return fmt.Errorf("max_batch_size=%d, want 0", expected.MaxBatchSize)
	}
	if !expected.BinaryTensorData {
		return errors.New("binary tensor data must be enabled")
	}
	if expected.MaxBatchSize != 0 {
		return fmt.Errorf("max_batch_size=%d, want 0", expected.MaxBatchSize)
	}
	if err := validateExpectedTensors("input", expected.Inputs); err != nil {
		return err
	}
	if err := validateExpectedTensors("output", expected.Outputs); err != nil {
		return err
	}
	return nil
}

func validateExpectedTensors(kind string, tensors []application.ServingTensorContract) error {
	if len(tensors) == 0 {
		return fmt.Errorf("%s tensors are empty", kind)
	}
	names := make(map[string]struct{}, len(tensors))

	for i, tensor := range tensors {
		if !modelNamePattern.MatchString(tensor.Name) {
			return fmt.Errorf("%s[%d] has invalid name %q", kind, i, tensor.Name)
		}

		if _, exists := names[tensor.Name]; exists {
			return fmt.Errorf("%s contains duplicate tensor %q", kind, tensor.Name)
		}
		names[tensor.Name] = struct{}{}

		switch tensor.DataType {
		case application.TensorDataTypeFP64,
			application.TensorDataTypeFP32,
			application.TensorDataTypeINT32,
			application.TensorDataTypeBOOL:
		default:
			return fmt.Errorf("%s %q has unsupported datatype %q", kind, tensor.Name, tensor.DataType)
		}

		if len(tensor.Dims) == 0 {
			return fmt.Errorf("%s %q has empty dims", kind, tensor.Name)
		}

		for dimensionIndex, dimension := range tensor.Dims {
			if dimension == 0 || dimension < -1 {
				return fmt.Errorf("%s %q dims[%d]=%d is invalid", kind, tensor.Name, dimensionIndex, dimension)
			}
		}
	}

	return nil
}

func validateModelMetadata(actual modelMetadataResponse, expected application.ServingEntrypointMetadata) error {
	if actual.Name != expected.ModelName {
		return fmt.Errorf("name=%q, want %q", actual.Name, expected.ModelName)
	}

	if !containsExactString(actual.Versions, expected.ModelVersion) {
		return fmt.Errorf("versions=%v does not contain %q", actual.Versions, expected.ModelVersion)
	}

	if actual.Platform != expected.Backend {
		return fmt.Errorf("platform=%q, want %q", actual.Platform, expected.Backend)
	}

	if err := compareMetadataTensors("input", actual.Inputs, expected.Inputs); err != nil {
		return err
	}

	if err := compareMetadataTensors("output", actual.Outputs, expected.Outputs); err != nil {
		return err
	}

	return nil
}

func validateModelConfig(actual modelConfigResponse, expected application.ServingEntrypointMetadata) error {
	if actual.Name != expected.ModelName {
		return fmt.Errorf("name=%q, want %q", actual.Name, expected.ModelName)
	}

	if actual.Backend != expected.Backend {
		return fmt.Errorf("backend=%q, want %q", actual.Backend, expected.Backend)
	}

	if actual.MaxBatchSize != expected.MaxBatchSize {
		return fmt.Errorf("max_batch_size=%d, want %d", actual.MaxBatchSize, expected.MaxBatchSize)
	}

	if err := compareConfigTensors("input", actual.Inputs, expected.Inputs, true); err != nil {
		return err
	}

	if err := compareConfigTensors("output", actual.Outputs, expected.Outputs, false); err != nil {
		return err
	}

	return nil
}

func compareMetadataTensors(kind string, actual []modelMetadataTensor, expected []application.ServingTensorContract) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%s count=%d, want %d", kind, len(actual), len(expected))
	}

	for index := range expected {
		got := actual[index]
		want := expected[index]

		if got.Name != want.Name {
			return fmt.Errorf("%s[%d].name=%q, want %q", kind, index, got.Name, want.Name)
		}

		if got.DataType != string(want.DataType) {
			return fmt.Errorf("%s %q datatype=%q, want %q", kind, want.Name, got.DataType, want.DataType)
		}

		if !equalDimensions(got.Shape, want.Dims) {
			return fmt.Errorf("%s %q shape=%v, want %v", kind, want.Name, got.Shape, want.Dims)
		}
	}

	return nil
}

func compareConfigTensors(kind string, actual []modelConfigTensor, expected []application.ServingTensorContract, checkOptional bool) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("%s count=%d, want %d", kind, len(actual), len(expected))
	}

	for index := range expected {
		got := actual[index]
		want := expected[index]

		if got.Name != want.Name {
			return fmt.Errorf("%s[%d].name=%q, want %q", kind, index, got.Name, want.Name)
		}

		wantDataType := "TYPE_" + string(want.DataType)
		if got.DataType != wantDataType {
			return fmt.Errorf("%s %q data_type=%q, want %q", kind, want.Name, got.DataType, wantDataType)
		}

		if !equalDimensions(got.Dims, want.Dims) {
			return fmt.Errorf("%s %q dims=%v, want %v", kind, want.Name, got.Dims, want.Dims)
		}

		if checkOptional && got.Optional == want.Required {
			return fmt.Errorf("%s %q optional=%t, required=%t", kind, want.Name, got.Optional, want.Required)
		}
	}

	return nil
}

func containsExactString(values []string, expected string) bool {
	for _, value := range values {
		if value == expected {
			return true
		}
	}
	return false
}

func equalDimensions(left, right []int64) bool {
	if len(left) != len(right) {
		return false
	}

	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}

	return true
}
