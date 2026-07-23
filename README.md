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