package fakelightcurve

import (
	"context"
	"errors"
	"sync"

	"github.com/ZChen470/variable-star-classification/internal/application"
	"github.com/ZChen470/variable-star-classification/internal/domain"
)

var (
	// ErrNilContext 表示调用方传入了 nil Context。
	ErrNilContext = errors.New("context must not be nil")

	// ErrUnconfiguredRequest 表示测试没有为指定读取键配置响应。
	//
	// 它不是业务上的 revision not found。测试权威不存在场景时，
	// 应显式配置 application.ErrLightCurveRevisionNotFound。
	ErrUnconfiguredRequest = errors.New(
		"light curve request is not configured",
	)
)

// Request 是 Fake Repository 记录和匹配的固定 revision 读取键。
type Request struct {
	ObjectID string
	Revision int64
}

// Response 是测试为一次固定 revision 请求配置的结果。
type Response struct {
	Revision domain.LightCurveRevision
	Err      error
}

// Repository 是 LightCurveRepository 的线程安全测试替身。
//
// 构造时的响应数据和 GetRevision 返回的数据都会被深拷贝，
// 避免调用方修改 Epochs 或 QualityPolicyVersion 后影响 Fake 内部状态。
type Repository struct {
	mu sync.Mutex

	responses map[Request]Response
	calls     []Request
}

var _ application.LightCurveRepository = (*Repository)(nil)

// New 使用固定请求响应表创建 Fake Repository。
func New(responses map[Request]Response) *Repository {
	clonedResponses := make(map[Request]Response, len(responses))

	for request, response := range responses {
		clonedResponse := response
		clonedResponse.Revision = cloneRevision(response.Revision)
		clonedResponses[request] = clonedResponse
	}

	return &Repository{
		responses: clonedResponses,
	}
}

// GetRevision 记录精确请求并返回预先配置的结果。
func (repository *Repository) GetRevision(
	ctx context.Context,
	objectID string,
	revision int64,
) (domain.LightCurveRevision, error) {
	if ctx == nil {
		return domain.LightCurveRevision{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return domain.LightCurveRevision{}, err
	}

	request := Request{
		ObjectID: objectID,
		Revision: revision,
	}

	repository.mu.Lock()
	repository.calls = append(repository.calls, request)
	response, configured := repository.responses[request]
	repository.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return domain.LightCurveRevision{}, err
	}
	if !configured {
		return domain.LightCurveRevision{}, ErrUnconfiguredRequest
	}
	if response.Err != nil {
		return domain.LightCurveRevision{}, response.Err
	}

	return cloneRevision(response.Revision), nil
}

// Calls 返回截至当前记录的全部读取请求。
func (repository *Repository) Calls() []Request {
	repository.mu.Lock()
	defer repository.mu.Unlock()

	calls := make([]Request, len(repository.calls))
	copy(calls, repository.calls)

	return calls
}

func cloneRevision(
	revision domain.LightCurveRevision,
) domain.LightCurveRevision {
	cloned := revision

	if revision.QualityPolicyVersion != nil {
		qualityPolicyVersion := *revision.QualityPolicyVersion
		cloned.QualityPolicyVersion = &qualityPolicyVersion
	}

	if revision.Epochs != nil {
		cloned.Epochs = make(
			[]domain.LightCurveEpoch,
			len(revision.Epochs),
		)
		copy(cloned.Epochs, revision.Epochs)
	}

	return cloned
}
