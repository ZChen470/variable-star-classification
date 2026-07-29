package application

import (
	"context"
	"errors"
	"fmt"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

// ClassificationInputPreparationRequest 是阶段 4 输入准备闭环
// 最小请求。它只引用 ClassificationCommand 已经携带的稳定字段
type ClassificationInputPreparationRequest struct {
	ObjectID                   string
	LightCurveRevision         int64
	DeclaredEligibleEpochCount uint32
	ModelBundleVersion         string
}

// PreparedClassificationInput 保存阶段 4 完成后的全部结果
//
// Revision 用于保留固定 revision 及其规范化 epoch
// Selection 用于后续构造粗分类来源追溯字段
// Input 是 VariableStarClassifier 的原始输入
type PreparedClassificationInput struct {
	Revision  domain.LightCurveRevision
	Selection CoarseModeSelection
	Input     ClassificationInput
}

// ClassificationInputPreparer 编排固定 revision 读取、机械准备、粗分类模式选择和 ClassificationInput 构造
type ClassificationInputPreparer struct {
	reader   *LightCurveRevisionReader
	selector *CoarseModeSelector
}

func NewClassificationInputPreparer(reader *LightCurveRevisionReader, selector *CoarseModeSelector) (*ClassificationInputPreparer, error) {
	if reader == nil {
		return nil, errors.New(
			"light curve revision reader must not be nil",
		)
	}
	if selector == nil {
		return nil, errors.New(
			"coarse mode selector must not be nil",
		)
	}

	return &ClassificationInputPreparer{
		reader:   reader,
		selector: selector,
	}, nil
}

// Prepare 执行阶段 4 的最小输入准备闭环。
//
// 错误发生后不会继续执行后续步骤。例如机械合法性失败时，
// 不解析 Model Bundle，也不查询历史粗分类结果。
func (prepare *ClassificationInputPreparer) Prepare(ctx context.Context, request ClassificationInputPreparationRequest) (PreparedClassificationInput, error) {
	if ctx == nil {
		return PreparedClassificationInput{}, errors.New(
			"prepare classification input: nil context",
		)
	}
	if prepare == nil {
		return PreparedClassificationInput{}, errors.New(
			"prepare classification input: nil preparer",
		)
	}
	if err := ctx.Err(); err != nil {
		return PreparedClassificationInput{}, err
	}

	if request.ObjectID == "" {
		return PreparedClassificationInput{}, errors.New(
			"prepare classification input: object ID must not be empty",
		)
	}
	if request.LightCurveRevision <= 0 {
		return PreparedClassificationInput{}, errors.New(
			"prepare classification input: light curve revision must be greater than zero",
		)
	}
	if request.ModelBundleVersion == "" {
		return PreparedClassificationInput{}, errors.New(
			"prepare classification input: model bundle version must not be empty",
		)
	}
	// 1. 获取 LightCurverRevision
	revision, err := prepare.reader.ReadRevision(ctx, request.ObjectID, request.LightCurveRevision)
	if err != nil {
		return PreparedClassificationInput{}, fmt.Errorf("read fixed light curve revision: %w", err)
	}

	// 2. 对原始 Revision 进行校验和排序
	preparedRevision, err := PrepareLightCurveRevision(revision, request.DeclaredEligibleEpochCount)
	if err != nil {
		return PreparedClassificationInput{}, fmt.Errorf("prepare fixed light curve revision: %w", err)
	}

	// PrepareLightCurveRevision 已限制最大 epoch 数量为 1024，
	// 这里转 uint32 是安全的
	actualEpochCount := uint32(len(preparedRevision.Epochs))

	selection, err := prepare.selector.Select(ctx, request.ObjectID, request.LightCurveRevision, actualEpochCount, request.ModelBundleVersion)
	if err != nil {
		return PreparedClassificationInput{}, fmt.Errorf("select coarse classification mode: %w", err)
	}

	input, err := BuildClassificationInput(preparedRevision, selection)
	if err != nil {
		return PreparedClassificationInput{}, fmt.Errorf("build classification input: %w", err)
	}

	return PreparedClassificationInput{
		Revision:  preparedRevision,
		Selection: selection,
		Input:     input,
	}, nil
}
