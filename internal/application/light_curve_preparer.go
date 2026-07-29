package application

import (
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/domain"
	"math"
	"sort"
)

const (
	minimumLightCurveEpochCount = 3
	maximumLightCurveEpochCount = 1024
)

var (
	// ErrLightCurveEpochCountMismatch 表示 Command 声明数量、
	// revision 元数据数量和实际 Epochs 长度不一致。
	ErrLightCurveEpochCountMismatch = errors.New(
		"light curve epoch count mismatch",
	)

	// ErrInsufficientLightCurveEpochs 表示实际 epoch 数少于分类下限。
	ErrInsufficientLightCurveEpochs = errors.New(
		"insufficient light curve epochs",
	)

	// ErrTooManyLightCurveEpochs 表示实际 epoch 数超过分类上限。
	ErrTooManyLightCurveEpochs = errors.New(
		"too many light curve epochs",
	)

	// ErrInvalidObservationTime 表示 observation time 是 NaN 或无穷值。
	ErrInvalidObservationTime = errors.New(
		"invalid observation time",
	)

	// ErrInvalidMagnitude 表示 magnitude 是 NaN 或无穷值。
	ErrInvalidMagnitude = errors.New(
		"invalid magnitude",
	)

	// ErrInvalidMagnitudeError 表示 magnitude error 非有限值或不大于零。
	ErrInvalidMagnitudeError = errors.New(
		"invalid magnitude error",
	)

	// ErrDuplicateObservationTime 表示同一 revision 中存在完全相同的
	// ObservationTime。
	ErrDuplicateObservationTime = errors.New(
		"duplicate observation time",
	)
)

// PrepareLightCurveRevision 对固定 revision 执行分类侧机械校验与规范化。
//
// 处理顺序
// 1. 校验 Command 声明数量、revision 元数据数量和实际长度一致
// 2. 校验实际数量在 3..1024 范围内
// 3. 校验每个 Epoch 值的合法性
// 4. 复制 Epochs，并仅按 ObservationTime 升序排序
// 5. 拒绝重复的 ObservationTime
//
// 本函数不修改传入 revision 的 Epochs 底层数组
func PrepareLightCurveRevision(revision domain.LightCurveRevision, declaredEligibleEpochCount uint32) (domain.LightCurveRevision, error) {
	actualEpochCount := len(revision.Epochs)

	// 1. epoch 数量校验
	if declaredEligibleEpochCount != revision.EligibleEpochCount || uint64(revision.EligibleEpochCount) != uint64(actualEpochCount) {
		return domain.LightCurveRevision{}, fmt.Errorf(
			"%w: declared=%d revision_metadata=%d actual=%d",
			ErrLightCurveEpochCountMismatch,
			declaredEligibleEpochCount,
			revision.EligibleEpochCount,
			actualEpochCount,
		)
	}

	// 2. epoch 数量是否处于范围内
	if actualEpochCount < minimumLightCurveEpochCount {
		return domain.LightCurveRevision{}, fmt.Errorf(
			"%w: actual=%d minimum=%d",
			ErrInsufficientLightCurveEpochs,
			actualEpochCount,
			minimumLightCurveEpochCount,
		)
	}

	if actualEpochCount > maximumLightCurveEpochCount {
		return domain.LightCurveRevision{}, fmt.Errorf(
			"%w: actual=%d maximum=%d",
			ErrTooManyLightCurveEpochs,
			actualEpochCount,
			maximumLightCurveEpochCount,
		)
	}

	// 3. epoch 数值校验
	for index, epoch := range revision.Epochs {
		if !(!math.IsNaN(epoch.ObservationTime) && !math.IsInf(epoch.ObservationTime, 0)) {
			return domain.LightCurveRevision{}, fmt.Errorf(
				"%w: epoch_index=%d value=%v",
				ErrInvalidObservationTime,
				index,
				epoch.ObservationTime,
			)
		}

		if !(!math.IsNaN(float64(epoch.Magnitude)) && !math.IsInf(float64(epoch.Magnitude), 0)) {
			return domain.LightCurveRevision{}, fmt.Errorf(
				"%w: epoch_index=%d value=%v",
				ErrInvalidMagnitude,
				index,
				epoch.Magnitude,
			)
		}

		if !(!math.IsNaN(float64(epoch.MagnitudeError)) && !math.IsInf(float64(epoch.MagnitudeError), 0)) ||
			epoch.MagnitudeError <= 0 {
			return domain.LightCurveRevision{}, fmt.Errorf(
				"%w: epoch_index=%d value=%v",
				ErrInvalidMagnitudeError,
				index,
				epoch.MagnitudeError,
			)
		}
	}

	sortedEpochs := make([]domain.LightCurveEpoch, len(revision.Epochs))
	copy(sortedEpochs, revision.Epochs)

	sort.Slice(sortedEpochs, func(left, right int) bool {
		return sortedEpochs[left].ObservationTime <
			sortedEpochs[right].ObservationTime
	})

	// 校验重复项
	for index := 1; index < len(sortedEpochs); index++ {
		previousTime := sortedEpochs[index-1].ObservationTime
		currentTime := sortedEpochs[index].ObservationTime

		if previousTime == currentTime {
			return domain.LightCurveRevision{}, fmt.Errorf(
				"%w: observation_time=%v sorted_indices=%d,%d",
				ErrDuplicateObservationTime,
				currentTime,
				index-1,
				index,
			)
		}
	}

	prepared := revision
	prepared.Epochs = sortedEpochs

	return prepared, nil

}
