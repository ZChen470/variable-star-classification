package fakeservingbundle

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
		"serving bundle resolver request is not configured",
	)
)

// Response 是一次 ResolveServingBundle 请求的预设响应。
type Response struct {
	Metadata application.ServingBundleMetadata
	Err      error
}

// Resolver 是 ServingBundleResolver 的线程安全测试替身。
//
// 请求使用 model_bundle_version 精确匹配，不 trim、不转换大小写。
// Fake 返回配置内容本身，不负责校验 Bundle 身份和 Serving Contract，
// 以便后续 loader、门禁和 Adapter 测试错误响应。
type Resolver struct {
	mu sync.Mutex

	responses map[string]Response
	calls     []string
}

var _ application.ServingBundleResolver = (*Resolver)(nil)

// New 创建使用固定响应集合的 Fake Resolver。
//
// 输入 map 和其中的可变 metadata 字段都会被复制，调用方后续修改
// 配置不会改变 Fake 行为。
func New(responses map[string]Response) *Resolver {
	copiedResponses := make(map[string]Response, len(responses))

	for modelBundleVersion, response := range responses {
		response.Metadata = cloneMetadata(response.Metadata)
		copiedResponses[modelBundleVersion] = response
	}

	return &Resolver{
		responses: copiedResponses,
	}
}

// ResolveServingBundle 记录精确请求并返回对应预设响应。
func (resolver *Resolver) ResolveServingBundle(
	ctx context.Context,
	modelBundleVersion string,
) (application.ServingBundleMetadata, error) {
	if ctx == nil {
		return application.ServingBundleMetadata{}, ErrNilContext
	}
	if err := ctx.Err(); err != nil {
		return application.ServingBundleMetadata{}, err
	}

	resolver.mu.Lock()
	resolver.calls = append(resolver.calls, modelBundleVersion)
	response, configured := resolver.responses[modelBundleVersion]
	resolver.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return application.ServingBundleMetadata{}, err
	}

	if !configured {
		return application.ServingBundleMetadata{}, fmt.Errorf(
			"%w: model_bundle_version=%q",
			ErrUnconfiguredRequest,
			modelBundleVersion,
		)
	}

	if response.Err != nil {
		return application.ServingBundleMetadata{}, response.Err
	}

	return cloneMetadata(response.Metadata), nil
}

// Calls 返回截至当前记录的全部精确请求版本。
func (resolver *Resolver) Calls() []string {
	resolver.mu.Lock()
	defer resolver.mu.Unlock()

	return append([]string(nil), resolver.calls...)
}

func cloneMetadata(
	metadata application.ServingBundleMetadata,
) application.ServingBundleMetadata {
	cloned := metadata
	cloned.Entrypoint.Inputs = cloneTensorContracts(metadata.Entrypoint.Inputs)
	cloned.Entrypoint.Outputs = cloneTensorContracts(metadata.Entrypoint.Outputs)
	return cloned
}

func cloneTensorContracts(
	contracts []application.ServingTensorContract,
) []application.ServingTensorContract {
	if contracts == nil {
		return nil
	}

	cloned := make([]application.ServingTensorContract, len(contracts))
	for index, contract := range contracts {
		cloned[index] = contract
		cloned[index].Dims = append([]int64(nil), contract.Dims...)
	}
	return cloned
}
