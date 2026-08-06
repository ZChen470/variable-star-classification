package triton

import (
	"context"
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/application"
	"math"
)

const (
	timeMJDInputName            = "TIME_MJD"
	magnitudeInputName          = "MAGNITUDE"
	magnitudeErrorInputName     = "MAGNITUDE_ERROR"
	coarseModeInputName         = "COARSE_MODE"
	reusedCoarseProbsInputName  = "REUSED_COARSE_PROBS"
	coarseProbsOutputName       = "COARSE_PROBS"
	fineConditionalOutputName   = "FINE_CONDITIONAL_PROBS"
	leafProbsOutputName         = "LEAF_PROBS"
	xgboostExecutedOutputName   = "XGBOOST_EXECUTED"
	minClassificationEpochCount = 3
	maxClassificationEpochCount = 1024
)

var (
	ErrInvalidClassifierConfiguration = errors.New("invalid Triton classifier configuration")
	ErrInvalidClassificationInput     = errors.New("invalid Triton classification input")
	ErrInvalidClassificationOutput    = errors.New("invalid Triton classification output")
)

// VariableStarClassifier 通过统一 Triton 模型入口执行变星分类
type VariableStarClassifier struct {
	client       *Client
	modelName    string
	modelVersion string
}

var _ application.VariableStarClassifier = (*VariableStarClassifier)(nil)

func NewVariableStarClassifier(client *Client, entrypoint application.ServingEntrypointMetadata) (*VariableStarClassifier, error) {
	if client == nil {
		return nil, fmt.Errorf("%w: client is nil", ErrInvalidClassifierConfiguration)
	}

	if !modelNamePattern.MatchString(entrypoint.ModelName) {
		return nil, fmt.Errorf("%w: invalid model name %q", ErrInvalidClassifierConfiguration, entrypoint.ModelName)
	}

	if !modelVersionPattern.MatchString(entrypoint.ModelVersion) {
		return nil, fmt.Errorf("%w: invalid model version %q", ErrInvalidClassifierConfiguration, entrypoint.ModelVersion)
	}

	if entrypoint.Protocol != application.ServingProtocolTritonV2HTTP {
		return nil, fmt.Errorf("%w: protocol=%q", ErrInvalidClassifierConfiguration, entrypoint.Protocol)
	}

	if !entrypoint.BinaryTensorData {
		return nil, fmt.Errorf("%w: binary tensor data is disabled", ErrInvalidClassifierConfiguration)
	}

	if entrypoint.MaxBatchSize != 0 {
		return nil, fmt.Errorf("%w: max_batch_size=%d, want 0", ErrInvalidClassifierConfiguration, entrypoint.MaxBatchSize)
	}

	return &VariableStarClassifier{
		client:       client,
		modelName:    entrypoint.ModelName,
		modelVersion: entrypoint.ModelVersion,
	}, nil
}

func (classifier *VariableStarClassifier) Classify(ctx context.Context, input application.ClassificationInput) (application.ClassificationOutput, error) {
	if ctx == nil {
		return application.ClassificationOutput{}, ErrNilContext
	}

	if err := ctx.Err(); err != nil {
		return application.ClassificationOutput{}, err
	}

	if classifier == nil || classifier.client == nil {
		return application.ClassificationOutput{}, fmt.Errorf("%w: classifier is not initialized", ErrInvalidClassifierConfiguration)
	}

	if err := validateClassificationInput(input); err != nil {
		return application.ClassificationOutput{}, err
	}
	tensors, err := buildClassificationInputTensors(input)
	if err != nil {
		return application.ClassificationOutput{}, fmt.Errorf("build Triton classification input: %w", err)
	}

	// TODO get request ID from context
	requestID, _ := application.ClassificationRequestIDFromContext(ctx)
	request, err := EncodeBinaryInferRequest(
		classifier.modelName,
		classifier.modelVersion,
		requestID,
		tensors,
		[]string{
			coarseProbsOutputName,
			fineConditionalOutputName,
			leafProbsOutputName,
			xgboostExecutedOutputName,
		},
	)
	if err != nil {
		return application.ClassificationOutput{}, fmt.Errorf("encode Triton classification request: %w", err)
	}
	response, err := classifier.client.Do(ctx, request)
	if err != nil {
		return application.ClassificationOutput{}, fmt.Errorf("execute Triton classification request: %w", err)
	}

	decoded, err := DecodeBinaryInferResponse(response)
	if err != nil {
		return application.ClassificationOutput{}, fmt.Errorf("decode Triton classification response: %w", err)
	}

	output, err := mapClassificationOutput(decoded, classifier.modelName, classifier.modelVersion, input.CoarseMode)
	if err != nil {
		return application.ClassificationOutput{}, err
	}
	return output, nil
}

func validateClassificationInput(input application.ClassificationInput) error {
	if !input.CoarseMode.IsValid() {
		return fmt.Errorf("%w: unknown coarse mode=%d", ErrInvalidClassificationInput, input.CoarseMode)
	}

	// 准备光变曲线的时候已经校验过值了，不需要再一个一个校验
	// 还没有校验过可能的 历史兼容粗概率
	switch input.CoarseMode {
	case application.CoarseModeReusePrevious:
		if input.ReusedCoarseProbabilities == nil {
			return fmt.Errorf("%w: reuse mode requires coarse probabilities", ErrInvalidClassificationInput)
		}

		for i, prob := range input.ReusedCoarseProbabilities {
			if math.IsNaN(float64(prob)) || math.IsInf(float64(prob), 0) || prob < 0 || prob > 1 {
				return fmt.Errorf("%w: reused coarse probability[%d]=%v", ErrInvalidClassificationInput, i, prob)
			}
		}
	case application.CoarseModeComputeCurrent, application.CoarseModeComputeBootstrap:
		if input.ReusedCoarseProbabilities != nil {
			return fmt.Errorf("%w: mode=%d must not contain reused probabilities", ErrInvalidClassificationInput, input.CoarseMode)
		}
	}

	return nil
}

func buildClassificationInputTensors(input application.ClassificationInput) ([]BinaryTensor, error) {
	shape := []int64{int64(len(input.TimeMJD))}

	timeTensor, err := NewFP64Tensor(timeMJDInputName, shape, input.TimeMJD)
	if err != nil {
		return nil, err
	}

	magnitudeTensor, err := NewFP32Tensor(magnitudeInputName, shape, input.Magnitude)
	if err != nil {
		return nil, err
	}

	magnitudeErrorTensor, err := NewFP32Tensor(magnitudeErrorInputName, shape, input.MagnitudeError)
	if err != nil {
		return nil, err
	}

	modeTensor, err := NewINT32Tensor(coarseModeInputName, []int64{1}, []int32{int32(input.CoarseMode)})
	if err != nil {
		return nil, err
	}

	reused := [application.CoarseClassCount]float32{}
	if input.ReusedCoarseProbabilities != nil {
		reused = *(input.ReusedCoarseProbabilities)
	}

	reusedTensor, err := NewFP32Tensor(reusedCoarseProbsInputName, []int64{application.CoarseClassCount}, reused[:])
	if err != nil {
		return nil, err
	}

	return []BinaryTensor{
		timeTensor,
		magnitudeTensor,
		magnitudeErrorTensor,
		modeTensor,
		reusedTensor,
	}, nil
}

func mapClassificationOutput(response BinaryInferResponse, expectedModelName string, expectedModelVersion string, mode application.CoarseMode) (application.ClassificationOutput, error) {
	if response.ModelName != expectedModelName {
		return application.ClassificationOutput{}, fmt.Errorf("%w: model_name=%q, want %q", ErrInvalidClassificationOutput, response.ModelName, expectedModelName)
	}

	if response.ModelVersion != expectedModelVersion {
		return application.ClassificationOutput{}, fmt.Errorf(
			"%w: model_version=%q, want %q", ErrInvalidClassificationOutput, response.ModelVersion, expectedModelVersion)
	}

	if len(response.Outputs) != 4 {
		return application.ClassificationOutput{}, fmt.Errorf("%w: output count=%d, want 4", ErrInvalidClassificationOutput, len(response.Outputs))
	}

	var output application.ClassificationOutput
	seen := make(map[string]struct{}, 4)
	for _, tensor := range response.Outputs {
		if _, ok := seen[tensor.Name]; ok {
			return application.ClassificationOutput{}, fmt.Errorf("%w: duplicate output %q", ErrInvalidClassificationOutput, tensor.Name)
		}
		seen[tensor.Name] = struct{}{}

		switch tensor.Name {
		case coarseProbsOutputName:
			values, err := decodeProbabilityTensor(tensor, application.CoarseClassCount)
			if err != nil {
				return application.ClassificationOutput{}, err
			}
			output.CoarseProbabilities = [application.CoarseClassCount]float32(values)
		case fineConditionalOutputName:
			values, err := decodeProbabilityTensor(tensor, application.ConditionalFineClassCount)
			if err != nil {
				return application.ClassificationOutput{}, err
			}
			output.ConditionalFineProbabilities = [application.ConditionalFineClassCount]float32(values)
		case leafProbsOutputName:
			values, err := decodeProbabilityTensor(tensor, application.LeafClassCount)
			if err != nil {
				return application.ClassificationOutput{}, err
			}
			output.LeafProbabilities = [application.LeafClassCount]float32(values)
		case xgboostExecutedOutputName:
			if err := requireOutputTensor(tensor, TensorDataTypeBOOL, 1); err != nil {
				return application.ClassificationOutput{}, err
			}
			values, err := DecodeBOOLValues(tensor)
			if err != nil {
				return application.ClassificationOutput{}, fmt.Errorf("%w: %s: %v", ErrInvalidClassificationOutput, tensor.Name, err)
			}
			output.XGBoostExecuted = values[0]

		default:
			return application.ClassificationOutput{}, fmt.Errorf("%w: unexpected output %q", ErrInvalidClassificationOutput, tensor.Name)
		}
	}

	for _, name := range []string{
		coarseProbsOutputName,
		fineConditionalOutputName,
		leafProbsOutputName,
		xgboostExecutedOutputName,
	} {
		if _, ok := seen[name]; !ok {
			return application.ClassificationOutput{}, fmt.Errorf("%w: missing output %q", ErrInvalidClassificationOutput, name)
		}
	}
	expectedExecuted := mode == application.CoarseModeComputeCurrent || mode == application.CoarseModeComputeBootstrap

	if output.XGBoostExecuted != expectedExecuted {
		return application.ClassificationOutput{}, fmt.Errorf("%w: mode=%d returned XGBOOST_EXECUTED=%t", ErrInvalidClassificationOutput, mode, output.XGBoostExecuted)
	}

	return output, nil
}

func decodeProbabilityTensor(tensor BinaryTensor, expectedCount int) ([]float32, error) {
	if err := requireOutputTensor(tensor, TensorDataTypeFP32, int64(expectedCount)); err != nil {
		return nil, err
	}

	values, err := DecodeFP32Values(tensor)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrInvalidClassificationOutput, tensor.Name, err)
	}

	for index, value := range values {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) || value < 0 || value > 1 {
			return nil, fmt.Errorf("%w: %s[%d]=%v is not a probability", ErrInvalidClassificationOutput, tensor.Name, index, value)
		}
	}
	return values, nil
}

func requireOutputTensor(tensor BinaryTensor, expectedDataType TensorDataType, expectedCount int64) error {
	if tensor.DataType != expectedDataType {
		return fmt.Errorf("%w: %s datatype=%q, want %q", ErrInvalidClassificationOutput, tensor.Name, tensor.DataType, expectedDataType)
	}

	if len(tensor.Shape) != 1 || tensor.Shape[0] != expectedCount {
		return fmt.Errorf("%w: %s shape=%v, want [%d]", ErrInvalidClassificationOutput, tensor.Name, tensor.Shape, expectedCount)
	}

	return nil
}
