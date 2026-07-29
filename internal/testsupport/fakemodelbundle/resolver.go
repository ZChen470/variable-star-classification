package fakemodelbundle

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/ZChen470/variable-star-classification/internal/application"
)

var (
	ErrNilContext = errors.New("context must not be nil")

	ErrUnconfiguredRequest = errors.New(
		"model bundle resolver request is not configured",
	)
)

// Response 是一次 Resolve 请求的预设响应。
type Response struct {
	Metadata application.ModelBundleMetadata
	Err      error
}

// Resolver 是 ModelBundleResolver 的线程安全测试替身。
//
// 请求使用 model_bundle_version 精确匹配，不 trim、不转换大小写。
// Fake 返回配置内容本身，不负责校验元数据身份和完整性，
// 以便后续选择器测试错误响应。
type Resolver struct {
	mu sync.Mutex

	responses map[string]Response
	calls     []string
}

var _ application.ModelBundleResolver = (*Resolver)(nil)

// New 创建使用固定响应集合的 Fake Resolver。
//
// 输入 map 会被复制，调用方后续修改 map 不会改变 Fake 配置。
func New(responses map[string]Response) *Resolver {
	copiedResponses := make(
		map[string]Response,
		len(responses),
	)

	for modelBundleVersion, response := range responses {
		copiedResponses[modelBundleVersion] = response
	}

	return &Resolver{
		responses: copiedResponses,
	}
}

// Resolve 记录精确请求并返回对应预设响应。
func (resolver *Resolver) Resolve(
	ctx context.Context,
	modelBundleVersion string,
) (application.ModelBundleMetadata, error) {
	if ctx == nil {
		return application.ModelBundleMetadata{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return application.ModelBundleMetadata{}, err
	}

	resolver.mu.Lock()
	resolver.calls = append(
		resolver.calls,
		modelBundleVersion,
	)
	response, configured := resolver.responses[modelBundleVersion]
	resolver.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return application.ModelBundleMetadata{}, err
	}

	if !configured {
		return application.ModelBundleMetadata{}, fmt.Errorf(
			"%w: model_bundle_version=%q",
			ErrUnconfiguredRequest,
			modelBundleVersion,
		)
	}

	if response.Err != nil {
		return application.ModelBundleMetadata{}, response.Err
	}

	return response.Metadata, nil
}

// Calls 返回截至当前记录的全部精确请求版本。
func (resolver *Resolver) Calls() []string {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()

	return append([]string(nil), resolver.calls...)
}
