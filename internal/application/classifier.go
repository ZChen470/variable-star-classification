package application

import (
	"context"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

const (
	CoarseClassCount          = domain.CoarseProbabilityCount
	ConditionalFineClassCount = domain.ConditionalFineProbabilityCount
	LeafClassCount            = domain.LeafProbabilityCount
)

// CoarseMode 决定本次推理怎样取得七维粗分类概率。
type CoarseMode uint8

const (
	CoarseModeUnspecified CoarseMode = iota
	CoarseModeComputeCurrent
	CoarseModeReusePrevious
	CoarseModeComputeBootstrap
)

// IsValid 判断模式是否为可以执行分类的已知值。
func (mode CoarseMode) IsValid() bool {
	switch mode {
	case CoarseModeComputeCurrent,
		CoarseModeReusePrevious,
		CoarseModeComputeBootstrap:
		return true
	default:
		return false
	}
}

// ClassificationInput 是应用层提交给分类器的科学输入。
//
// 它不包含 Triton、Protobuf、Kafka 或数据库类型。
type ClassificationInput struct {
	TimeMJD        []float64
	Magnitude      []float32
	MagnitudeError []float32

	CoarseMode CoarseMode

	// 仅 CoarseModeReusePrevious 使用。
	// 指针用于区分“未提供”和一组实际存在的七维概率。
	ReusedCoarseProbabilities *[CoarseClassCount]float32
}

// ClassificationOutput 是一次成功推理返回的模型输出。
type ClassificationOutput struct {
	CoarseProbabilities          [CoarseClassCount]float32
	ConditionalFineProbabilities [ConditionalFineClassCount]float32
	LeafProbabilities            [LeafClassCount]float32
	XGBoostExecuted              bool
}

// VariableStarClassifier 是应用层使用的分类器 Port。
//
// 具体实现可以是 Fake、Triton Adapter 或其他兼容实现。
type VariableStarClassifier interface {
	Classify(
		ctx context.Context,
		input ClassificationInput,
	) (ClassificationOutput, error)
}
