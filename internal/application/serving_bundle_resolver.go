package application

import (
	"context"
	"errors"
)

var (
	// ErrServingBundleNotFound 表示指定的不可变 Serving Bundle 不存在。
	ErrServingBundleNotFound = errors.New("serving bundle not found")
)

// ServingProtocol 表示统一模型入口使用的服务协议。
type ServingProtocol string

const (
	ServingProtocolTritonV2HTTP ServingProtocol = "triton-v2-http"
)

// TensorDataType 表示 Serving Contract 中的 Triton tensor datatype。
type TensorDataType string

const (
	TensorDataTypeFP64  TensorDataType = "FP64"
	TensorDataTypeFP32  TensorDataType = "FP32"
	TensorDataTypeINT32 TensorDataType = "INT32"
	TensorDataTypeBOOL  TensorDataType = "BOOL"
)

// ServingTensorContract 描述统一入口的一个输入或输出 tensor。
//
// Dims 使用 Triton config.pbtxt 的维度语义；-1 表示动态维度。
type ServingTensorContract struct {
	Name     string
	DataType TensorDataType
	Dims     []int64
	Required bool
}

// ServingEntrypointMetadata 描述一个精确版本的统一 Triton 模型入口。
type ServingEntrypointMetadata struct {
	ModelName        string
	ModelVersion     string
	Backend          string
	Protocol         ServingProtocol
	BinaryTensorData bool
	MaxBatchSize     int

	Inputs  []ServingTensorContract
	Outputs []ServingTensorContract
}

// ServingBundleMetadata 是阶段 5 Go Adapter 所需的完整只读 Serving 元数据。
//
// 它只描述 Command 已绑定 Bundle 的精确入口和契约，不包含部署地址，
// 也不允许 Resolver 自动选择 latest。
type ServingBundleMetadata struct {
	ModelBundleVersion          string
	TaxonomyVersion             string
	ClassificationPolicyVersion string
	FeatureSchemaVersion        string
	PreprocessingVersion        string
	TensorSchemaVersion         string
	FusionContractVersion       string
	ServingContractVersion      string

	Entrypoint ServingEntrypointMetadata

	CoarseProbabilityOrder          [CoarseClassCount]string
	ConditionalFineProbabilityOrder [ConditionalFineClassCount]string
	LeafProbabilityOrder            [LeafClassCount]string
}

// ServingBundleResolver 根据 command 已绑定的模型 Bundle 版本，
// 解析阶段 5 推理和启动契约门禁所需的完整 Serving 元数据。
//
// 实现不得自动选择 latest，也不得 trim、转换大小写或改写请求版本。
type ServingBundleResolver interface {
	ResolveServingBundle(
		ctx context.Context,
		modelBundleVersion string,
	) (ServingBundleMetadata, error)
}
