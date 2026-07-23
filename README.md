# Variable Star Classification

变源候选体实时分类系统的 Go 工程仓库。

当前处于阶段 1：最小代码仓库、开发环境与基础 CI。

阶段 0 已冻结的 Protobuf、taxonomy、分类策略及其他跨系统契约，是后续代码实现的基线。

## 当前范围

当前仓库只提供最小、可编译、可测试的 Go 工程骨架。

暂不包含：

- 完整业务服务
- Kafka 和 PostgreSQL 业务流程
- 真实 XGBoost 或 Transformer 推理
- Kubernetes 和生产部署
- 生产级公共运行时框架

## 环境要求

- Go 1.23
- Git
- Make（可选，主要供 Linux 和 CI 使用）

## 本地验证

Windows 本地可以直接执行：

    gofmt -w .
    go vet ./...
    go test ./...
    go build ./...

安装 Make 的环境可以执行：

    make check

## Go Module

    github.com/ZChen470/variable-star-classification

## Protobuf

Proto 源文件位于：

    api/proto/astro/classification/v1/

检查 Proto：

    buf format --exit-code
    buf lint
    buf build

重新生成 Go 代码：

    buf generate

生成代码位于：

    gen/go/astro/classification/v1/

生成的 `.pb.go` 文件不应直接手工修改。需要调整消息契约时，应修改对应的 `.proto` 文件，再重新生成。buf generate 会读取仓库根目录的 buf.gen.yaml，调用已配置的本地 protoc-gen-go，并按照配置重新生成 gen/go 内容。



## 确定性任务标识

系统不持久化 `ClassificationJob`。一个逻辑分类任务由以下字段唯一确定：

- `object_id`
- `light_curve_revision`
- `model_bundle_version`
- `classification_policy_version`
- `execution_mode`

领域代码位于：

    internal/domain/identity.go

相同任务输入始终生成相同的 `job_id`。一个 `job_id` 始终生成相同的成功结果 `run_id`，用于支持 Kafka 至少一次投递下的幂等处理。

当前 ID 算法使用 UUIDv5。字符串按 UTF-8 原样参与计算，不自动去除空白或转换大小写。

固定测试向量：

    object_id: OBJ-0001
    light_curve_revision: 21
    model_bundle_version: bundle-2026-07-001
    classification_policy_version: classification-policy-v1
    execution_mode: PRODUCTION

预期结果：

    job_id: 46709af9-e19b-5dfc-beb5-b9213127fd18
    run_id: d42c8015-e1f6-59df-b3ad-3e0f3cff2702