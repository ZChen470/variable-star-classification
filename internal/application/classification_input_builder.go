package application

import (
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

var (
	// ErrInvalidCoarseModeSelection 表示 CoarseMode 与历史粗概率的存在性不符合约定。
	ErrInvalidCoarseModeSelection = errors.New("invalid coarse mode selection")
)

// BuildClassificationInput 将已经完成机械校验和时间排序的固定 LightCurveRevision
// 与已经完成历史查询的 CoarseModeSelection 组装为现有 VariableStarClassifier 使用的 ClassificationInput
//
// 本函数不会重新排序或修改 prepareRevision.Epochs
// 返回的三个序列和可选历史粗概率均为独立副本
//
// 调用方仍应保留 CoarseModeSelection，以便后续构造
// ClassificationResult 的粗分类来源追溯字段
func BuildClassificationInput(preparedRevision domain.LightCurveRevision, selection CoarseModeSelection) (ClassificationInput, error) {
	if !selection.Mode.IsValid() {
		return ClassificationInput{}, fmt.Errorf(
			"%w: unknown coarse mode=%d",
			ErrInvalidCoarseModeSelection,
			selection.Mode,
		)
	}

	switch selection.Mode {
	case CoarseModeComputeCurrent, CoarseModeComputeBootstrap:
		if selection.ReusedCoarse != nil {
			return ClassificationInput{}, fmt.Errorf(
				"%w: mode=%d must not contain reused coarse probabilities",
				ErrInvalidCoarseModeSelection,
				selection.Mode,
			)
		}
	case CoarseModeReusePrevious:
		if selection.ReusedCoarse == nil {
			return ClassificationInput{}, fmt.Errorf(
				"%w: reuse previous mode requires compatible coarse probabilities",
				ErrInvalidCoarseModeSelection,
			)
		}
	}

	epochCount := len(preparedRevision.Epochs)

	input := ClassificationInput{
		TimeMJD:        make([]float64, epochCount),
		Magnitude:      make([]float32, epochCount),
		MagnitudeError: make([]float32, epochCount),
		CoarseMode:     selection.Mode,
	}

	for index, epoch := range preparedRevision.Epochs {
		input.TimeMJD[index] = epoch.ObservationTime
		input.Magnitude[index] = epoch.Magnitude
		input.MagnitudeError[index] = epoch.MagnitudeError
	}

	if selection.Mode == CoarseModeReusePrevious {
		probabilities := selection.ReusedCoarse.Probabilities
		input.ReusedCoarseProbabilities = &probabilities
	}

	return input, nil
}
