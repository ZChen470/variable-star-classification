package application

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/ZChen470/variable-star-classification/internal/domain"
)

// MinimumEligibleEpochCount 是 CandidateEvent 可以触发分类的最低合格 epoch 数。
const MinimumEligibleEpochCount uint32 = 3

// ClassificationPriority 表示分类命令的业务优先级
type ClassificationPriority uint8

const (
	ClassificationPriorityUnspecified ClassificationPriority = iota
	ClassificationPriorityRealtime
)

// ClassificationPolicyDecision 是 Policy 对已校验 CandidateEvent 的判断结果。
type ClassificationPolicyDecision struct {
	ShouldClassify              bool
	ModelBundleVersion          string
	ClassificationPolicyVersion string
	ExecutionMode               domain.ExecutionMode
	Priority                    ClassificationPriority
	DeadlineAt                  *time.Time
}

// ClassificationPolicyV1 是最小的候选分类策略
//
// 当前版本固定使用 PRODUCTION REALTIME 并不设置 deadline
type ClassificationPolicyV1 struct {
	modelBundleVersion          string
	classificationPolicyVersion string
}

// NewClassificationPolicyV1 创建并校验最小分类策略
func NewClassificationPolicyV1(modelBundleVersion string, classificationPolicyVersion string) (ClassificationPolicyV1, error) {
	// 1 参数校验
	if err := validateClassificationPolicyString("model bundle version", modelBundleVersion); err != nil {
		return ClassificationPolicyV1{}, err
	}

	if err := validateClassificationPolicyString("classification policy version", classificationPolicyVersion); err != nil {
		return ClassificationPolicyV1{}, err
	}

	// 构建分类策略
	return ClassificationPolicyV1{
		modelBundleVersion:          modelBundleVersion,
		classificationPolicyVersion: classificationPolicyVersion,
	}, nil
}

// Evaluate 判断已校验的候选事件是否需要生成 classificationCommand
func (policy ClassificationPolicyV1) Evaluate(input CandidateEventInput) (ClassificationPolicyDecision, error) {
	if policy.modelBundleVersion == "" || policy.classificationPolicyVersion == "" {
		return ClassificationPolicyDecision{}, errors.New("classification policy is not configured")
	}

	switch input.EventType {
	case CandidateEventTypeCreated, CandidateEventTypeUpdated:
	default:
		return ClassificationPolicyDecision{}, fmt.Errorf("unsupported candidate event type: %d", input.EventType)
	}

	if input.EligibleEpochCount < MinimumEligibleEpochCount {
		return ClassificationPolicyDecision{}, nil
	}

	return ClassificationPolicyDecision{
		ShouldClassify:              true,
		ModelBundleVersion:          policy.modelBundleVersion,
		ClassificationPolicyVersion: policy.classificationPolicyVersion,
		ExecutionMode:               domain.ExecutionModeProduction,
		Priority:                    ClassificationPriorityRealtime,
		DeadlineAt:                  nil,
	}, nil

}

func validateClassificationPolicyString(field, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be empty", field)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf(
			"%s must not contain leading or trailing whitespace",
			field,
		)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%s must not contain NUL", field)
	}
	return nil
}
