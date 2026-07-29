package application

import (
	"context"
	"errors"
)

var (
	// ErrModelBundleNotFound 表示指定的不可变模型 Bundle 不存在。
	ErrModelBundleNotFound = errors.New("model bundle not found")
)

// ModelBundleMetadata 阶段 4 为查询兼容历史粗概率所需
// 最小模型 Bundle 元数据
//
// 它不表示完整 manifest，也不包含 Triton 入口、模型文件、
// checksum、Transformer 或部署配置
type ModelBundleMetadata struct {
	ModelBundleVersion string

	TaxonomyVersion      string
	XGBoostModelVersion  string
	FeatureSchemaVersion string
}

// ModelBundleResolver 根据 command 已绑定的模型 Bundle 版本
// 解析阶段 4 所需的最小兼容性元数据
//
// 实现不得自动选择 latest，也不得修改请求的版本
type ModelBundleResolver interface {
	Resolve(ctx context.Context, modelBundleVersion string) (ModelBundleMetadata, error)
}
