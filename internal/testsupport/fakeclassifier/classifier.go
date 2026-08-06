package fakeclassifier

import (
	"context"
	"errors"
	"sync"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

var ErrNilContext = errors.New("context must not be nil")

// Classifier 是 VariableStarClassifier 的线程安全测试替身。
//
// 配置错误时，Classify 返回零值输出和该错误。
// Calls 返回深拷贝，避免调用方后续修改输入影响测试记录。
type Classifier struct {
	mu sync.Mutex

	output     application.ClassificationOutput
	err        error
	calls      []application.ClassificationInput
	requestIDs []string
}

var _ application.VariableStarClassifier = (*Classifier)(nil)

// New 创建返回固定结果的 Fake Classifier。
func New(
	output application.ClassificationOutput,
	err error,
) *Classifier {
	return &Classifier{
		output: output,
		err:    err,
	}
}

// Classify 记录调用并返回预设结果。
func (classifier *Classifier) Classify(
	ctx context.Context,
	input application.ClassificationInput,
) (application.ClassificationOutput, error) {
	if ctx == nil {
		return application.ClassificationOutput{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return application.ClassificationOutput{}, err
	}
	requestID, _ := application.ClassificationRequestIDFromContext(ctx)

	classifier.mu.Lock()

	classifier.calls = append(
		classifier.calls,
		cloneInput(input),
	)

	classifier.requestIDs = append(
		classifier.requestIDs,
		requestID,
	)

	output := classifier.output
	configuredErr := classifier.err

	classifier.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return application.ClassificationOutput{}, err
	}
	if configuredErr != nil {
		return application.ClassificationOutput{}, configuredErr
	}

	return output, nil
}

// Calls 返回截至当前记录的全部调用输入。
func (classifier *Classifier) Calls() []application.ClassificationInput {
	classifier.mu.Lock()
	defer classifier.mu.Unlock()

	calls := make([]application.ClassificationInput, len(classifier.calls))
	for index, input := range classifier.calls {
		calls[index] = cloneInput(input)
	}
	return calls
}

// RequestIDs 返回每次 Classify 调用接收到的 request ID。
func (classifier *Classifier) RequestIDs() []string {
	classifier.mu.Lock()
	defer classifier.mu.Unlock()

	return append(
		[]string(nil),
		classifier.requestIDs...,
	)
}

func cloneInput(
	input application.ClassificationInput,
) application.ClassificationInput {
	cloned := input

	cloned.TimeMJD = append([]float64(nil), input.TimeMJD...)
	cloned.Magnitude = append([]float32(nil), input.Magnitude...)
	cloned.MagnitudeError = append([]float32(nil), input.MagnitudeError...)

	if input.ReusedCoarseProbabilities != nil {
		probabilities := *input.ReusedCoarseProbabilities
		cloned.ReusedCoarseProbabilities = &probabilities
	}

	return cloned
}
