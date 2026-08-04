# Variable Star Classification

变源候选体实时分类系统的 Go 工程仓库。

系统面向地基光学望远镜产生的变源候选体，通过固定光变曲线 revision、版本化模型契约、确定性任务身份和事件驱动流程，形成可追溯、可重放、可扩展的实时分类基础。

## 当前阶段

阶段 1 至阶段 5 的最小工程闭环已经完成，阶段 5 已通过真实 Triton 服务器契约与三种模式推理验收：

```text
阶段 1：工程仓库、Protobuf、确定性身份、分类器 Port 与基础 CI
阶段 2：PostgreSQL Classification Repository 与 Kafka 基础设施
阶段 3：CandidateEvent → ClassificationCommand 最小业务闭环
阶段 4：固定 revision → 规范化 ClassificationInput 最小输入准备闭环
阶段 5：Serving Bundle → Triton V2 HTTP → ClassificationOutput（S5-GO-01..08）
```

当前已形成：

```text
ClassificationCommand 中的固定 object_id + light_curve_revision
        ↓
LightCurveRepository
        ↓
LightCurveRevisionReader
        ↓
机械合法性校验、复制和时间排序
        ↓
ModelBundleResolver + CoarseModeSelector
        ↓
ClassificationInput Builder
        ↓
PreparedClassificationInput
        ↓
ServingBundleResolver + manifest-v2 Loader
        ↓
Triton Metadata / Config / Ready 契约门禁
        ↓
VariableStarClassifier Adapter
        ↓
Triton V2 HTTP + Binary Tensor
        ↓
ClassificationOutput
```

当前覆盖三条粗分类路径：

```text
3 <= n <= 20
→ COMPUTE_CURRENT
→ Triton 内执行 XGBoost

n > 20 且存在兼容历史粗概率
→ REUSE_PREVIOUS
→ Triton 使用 REUSED_COARSE_PROBS

n > 20 且明确不存在兼容历史粗概率
→ COMPUTE_BOOTSTRAP
→ Triton 内执行 XGBoost
```

阶段 5 已完成：

```text
S5-GO-01 ServingBundleResolver Port + Fake
S5-GO-02 model-bundle-manifest-v2 Loader
S5-GO-03 Triton V2 HTTP Client
S5-GO-04 Binary Tensor Codec
S5-GO-05 Metadata / Config / Ready 契约门禁
S5-GO-06 VariableStarClassifier Adapter
S5-GO-07 HTTP Fake Server 推理夹具
S5-GO-08 真实 Triton 契约、三种模式推理与服务器验收
```

真实服务器已验证精确 Metadata、Config、Ready、Binary Tensor 推理和 `COMPUTE_CURRENT`、`REUSE_PREVIOUS`、`COMPUTE_BOOTSTRAP` 三种模式。下一开发阶段为阶段 6。

真实上游 LightCurve HTTP Adapter、真实 Kafka Broker 验证和独立服务器 PostgreSQL 验证仍保持 `DEFERRED`。

## 当前范围

当前仓库包含：

```text
Protobuf v1 契约
确定性 job_id / run_id
分类器应用 Port 与 Fake
PostgreSQL Migration 与 Classification Repository
Kafka Publisher 与 Consumer Runner
CandidateEvent 解码、校验与 ClassificationPolicy v1
ClassificationCommand 确定性构造
Candidate 最小 DLQ
CandidateHandler
candidate-orchestrator
LightCurveRevision 领域类型与 Repository Port
Fake LightCurveRepository
固定 revision Reader
LightCurveRevision 机械准备
ModelBundleResolver Port 与 Fake
CoarseModeSelector
ClassificationInput Builder
ClassificationInputPreparer
输入准备 Golden Vector 与确定性测试
ServingBundleResolver Port 与 Fake
model-bundle-manifest-v2 类型、加载与精确版本解析
Triton V2 HTTP Client
Triton Binary Tensor 编解码
Triton Metadata / Config / Ready 契约门禁
VariableStarClassifier Triton Adapter
HTTP Fake Server 三种模式推理夹具与错误响应测试
真实 Triton 服务器集成测试
serving-contract-v1 与规范化 HTTP fixtures
契约制品 SHA-256 完整性门禁
```

当前阶段不包含：

- 真实上游 LightCurve HTTP Adapter；
- 运行时自动选择 latest revision 或 latest 模型版本；
- Go 侧 XGBoost 特征计算；
- Go 侧 Transformer 标准化、分桶、padding 或 mask；
- 在 Go 仓库中保存 XGBoost、Transformer checkpoint、ONNX 或其他模型制品；
- 自动重试、熔断、通用推理错误分类与可观测性；
- 完整 ClassificationResult Worker；
- ClassificationResult Writer；
- Transactional Outbox；
- Retry Topic 和通用 Command DLQ；
- 查询 API、GUI 和人工复核；
- Kubernetes 与生产部署。

## 环境要求

基础开发环境：

- Go 1.25.0
- Git

可选工具和基础设施：

- Make：主要供 Linux 和 CI 使用；
- Buf：检查和生成 Protobuf；
- `protoc-gen-go`：生成 Go Protobuf 代码；
- Goose：验证 PostgreSQL migration；
- PostgreSQL：执行 Repository 集成测试；
- Kafka：执行真实 Broker 集成测试；
- Triton Inference Server：执行阶段 5 真实模型契约与推理验收。

## Go Module

```text
github.com/ZChen470/variable-star-classification
```

## 项目结构

```text
api/proto/astro/classification/v1/      Protobuf v1 源文件
gen/go/astro/classification/v1/         生成的 Go Protobuf 代码
cmd/candidate-orchestrator/              Candidate 编排服务入口
internal/domain/                         领域类型与确定性身份规则
internal/application/                    应用 Port、业务用例、输入准备与 Serving Contract
internal/adapter/postgres/               PostgreSQL Repository
internal/adapter/kafka/                  Kafka Publisher 与 Consumer Runner
internal/adapter/modelbundle/            Serving Bundle manifest-v2 Loader
internal/adapter/triton/                 Triton HTTP、Binary Tensor、契约门禁与分类器 Adapter
internal/testsupport/fakeclassifier/     Fake Classifier
internal/testsupport/fakelightcurve/     Fake LightCurveRepository
internal/testsupport/fakemodelbundle/    Fake ModelBundleResolver
internal/testsupport/fakeservingbundle/  Fake ServingBundleResolver
models/bundles/                          Manifest、Serving Contract 与 HTTP fixtures
migrations/                              Goose SQL migration
tests/                                   跨包测试
docs/contracts/                          上下游契约文档
```

## 本地验证

PowerShell 一次性执行基础 Go 门禁：

```powershell
& { gofmt -w .;if ($LASTEXITCODE -ne 0) { return };go test ./... -count=1;if ($LASTEXITCODE -ne 0) { return };go vet ./...;if ($LASTEXITCODE -ne 0) { return };go build ./...;if ($LASTEXITCODE -ne 0) { return };git diff --check;git status --short }
```

安装 Make 的 Linux 或 CI 环境可以执行：

```bash
make ci
```

CI 当前使用 Go 1.25.0，并检查：

- Go 格式；
- Protobuf 格式、lint 和 build；
- Protobuf 生成代码漂移；
- Go Module 依赖漂移；
- Goose migration；
- `go vet`；
- 全部测试；
- 全部包构建；
- 最终 Git 工作区漂移。

## Protobuf

Proto 源文件位于：

```text
api/proto/astro/classification/v1/
```

当前核心消息包括：

```text
CandidateEvent
ClassificationCommand
ClassificationResult
```

检查和重新生成：

```bash
buf format --exit-code
buf lint
buf build
buf generate
```

生成代码位于：

```text
gen/go/astro/classification/v1/
```

生成的 `.pb.go` 文件不应直接手工修改。契约变化应先修改 `.proto`，再重新生成。

## 确定性任务身份

系统当前不持久化 `ClassificationJob`。

一个逻辑分类任务由以下字段唯一确定：

- `object_id`
- `light_curve_revision`
- `model_bundle_version`
- `classification_policy_version`
- `execution_mode`

领域代码位于：

```text
internal/domain/identity.go
```

相同任务输入始终生成相同 `job_id`；一个 `job_id` 始终生成相同成功结果 `run_id`。这为 Kafka 至少一次投递下的幂等处理提供稳定身份。

当前算法使用 UUIDv5。字符串按 UTF-8 原样参与计算，不自动去除空白或转换大小写。

## 分类器应用边界

应用层通过以下接口调用分类器：

```text
internal/application.VariableStarClassifier
```

稳定输入输出类型位于：

```text
internal/application/classifier.go
```

`ClassificationInput` 包含：

```text
TimeMJD
Magnitude
MagnitudeError
CoarseMode
ReusedCoarseProbabilities（可选）
```

模型输出维度固定为：

- 7 个粗类别概率；
- 10 个条件细类别概率；
- 12 个最终叶子类别概率。

应用接口不依赖 Protobuf、Kafka、PostgreSQL 或 Triton 类型。

测试替身位于：

```text
internal/testsupport/fakeclassifier/
```

## Triton Serving Bundle 与推理 Adapter

### Serving Bundle 边界

阶段 5 使用独立于阶段 4 `ModelBundleResolver` 的完整 Serving Resolver：

```text
internal/application/serving_bundle_resolver.go
internal/testsupport/fakeservingbundle/
```

`ServingBundleResolver` 根据 Command 已绑定的 `model_bundle_version` 解析固定 Bundle 身份及入口元数据：

```text
模型 Bundle 与科学契约版本
精确 model_name / model_version
backend / protocol / max_batch_size
输入输出 Tensor 契约
7 / 10 / 12 维概率类别顺序
```

解析规则保持精确版本语义：

- 不选择 latest；
- 不 trim；
- 不转换大小写；
- 不改写请求版本；
- 部署地址不进入 Bundle，由运行环境提供。

文件加载器位于：

```text
internal/adapter/modelbundle/serving_manifest.go
models/bundles/model-bundle-manifest-v2.yaml
```

当前加载器使用严格 YAML 字段解析，拒绝多文档，并校验 v2 schema、manifest 状态、Bundle 身份、统一入口模型名和 Python backend。真实运行时契约由 Triton 启动门禁继续验证。

### 统一 Triton 入口

第一版统一入口固定为：

```text
model_name: variable_star_classifier
model_version: 1
protocol: triton-v2-http
max_batch_size: 0
binary_tensor_data: true
```

Go → Triton 输入：

```text
TIME_MJD                 FP64  [N]
MAGNITUDE                FP32  [N]
MAGNITUDE_ERROR          FP32  [N]
COARSE_MODE              INT32 [1]
REUSED_COARSE_PROBS      FP32  [7]
```

Triton → Go 输出：

```text
COARSE_PROBS             FP32 [7]
FINE_CONDITIONAL_PROBS   FP32 [10]
LEAF_PROBS               FP32 [12]
XGBOOST_EXECUTED         BOOL [1]
```

`REUSED_COARSE_PROBS` 始终存在：

- `REUSE_PREVIOUS` 发送七维历史粗概率；
- `COMPUTE_CURRENT` 和 `COMPUTE_BOOTSTRAP` 发送七维全零占位；
- 不使用 optional Tensor。

概率数组顺序由 Serving Bundle 契约固定。Go Adapter 不根据模型内部实现推断类别，也不对输出做隐藏重排。

### Triton V2 HTTP 与 Binary Tensor

HTTP Client 和 Binary Tensor Codec 位于：

```text
internal/adapter/triton/client.go
internal/adapter/triton/binary_tensor.go
```

精确版本端点：

```text
GET  /v2/models/{model_name}/versions/{model_version}
GET  /v2/models/{model_name}/versions/{model_version}/config
GET  /v2/models/{model_name}/versions/{model_version}/ready
POST /v2/models/{model_name}/versions/{model_version}/infer
```

推理请求使用：

```text
Content-Type: application/octet-stream
Inference-Header-Content-Length: <JSON header bytes>
Body: JSON header + little-endian tensor bytes
```

Codec 支持当前契约使用的 `FP64`、`FP32`、`INT32` 和 `BOOL`，校验 shape、元素字节数、重复 Tensor、响应边界和尾随字节。响应按 JSON `outputs` 中的实际顺序切分二进制区；业务 Adapter 再按输出名称映射，不依赖服务端返回顺序。

HTTP Client：

- 使用调用方传入的 Context；
- 使用注入的 `http.Client`；
- 限制最大响应体；
- 保留非 2xx 状态码、响应 Header 和 Body；
- 当前不内置重试策略。

### 启动契约门禁

契约门禁位于：

```text
internal/adapter/triton/contract_gate.go
```

启动时检查精确版本的：

```text
Ready
Model Metadata
Model Config
```

当前校验包括：

- 精确模型名称和版本；
- Python backend；
- `max_batch_size = 0`；
- 五个输入和四个输出；
- Tensor 名称、顺序、datatype 和 dims；
- 输入 optional/required 语义。

这些检查已经通过单元测试、CI 和真实 Triton `2.67.0` 服务器验收；运行时必须使用精确模型版本，不回退到 latest。

### VariableStarClassifier Adapter

生产 Adapter 位于：

```text
internal/adapter/triton/classifier.go
```

职责：

```text
ClassificationInput
→ 五个 Binary Tensor
→ 精确版本 Triton infer
→ 解码四个输出
→ ClassificationOutput
```

应用层已经负责 epoch 数量、序列一致性、有限值、误差正值、时间排序和重复时间拒绝。Triton Adapter 不重复这些机械校验，只检查影响 Wire Mapping 的模式组合：

- `REUSE_PREVIOUS` 必须携带历史粗概率；
- `COMPUTE_CURRENT` 和 `COMPUTE_BOOTSTRAP` 不得携带历史粗概率；
- 未知模式拒绝；
- `XGBOOST_EXECUTED` 必须与模式一致。

### HTTP 推理夹具

HTTP Fake Server 测试位于：

```text
internal/adapter/triton/classifier_http_fixture_test.go
```

已经覆盖：

- `COMPUTE_CURRENT`；
- `REUSE_PREVIOUS`；
- `COMPUTE_BOOTSTRAP`；
- 真实 Go HTTP 请求链路；
- Binary Tensor JSON Header 与二进制 Body；
- 服务端输出乱序；
- Triton 400 与 503 响应传播；
- 缺失 Header 长度和截断二进制响应拒绝。

这些夹具验证 Go 侧协议与映射，不单独代表真实模型科学结果或服务器环境验收。

### 真实 Triton 服务器验证

真实服务器集成测试位于：

```text
internal/adapter/triton/server_integration_test.go
```

默认测试会跳过真实服务器，通过以下环境变量显式启用：

```powershell
$env:VSC_TRITON_INTEGRATION='1'; $env:VSC_TRITON_BASE_URL='<triton-base-url>'; go test ./internal/adapter/triton -run '^TestTritonServerIntegration$' -count=1 -v
```

服务器验收已经覆盖：

- 精确 `variable_star_classifier:1` Metadata、Config 和深度 Ready；
- `COMPUTE_CURRENT`、`REUSE_PREVIOUS` 和 `COMPUTE_BOOTSTRAP`；
- 7/10/12 概率输出及 `XGBOOST_EXECUTED`；
- 输入反序等价性与重复时间拒绝；
- GPU FP32 数值容差和重复推理确定性。

生产地址属于部署配置，不写入 Bundle 或 README。Go 仓库只保存 Serving Contract、规范化 HTTP fixtures 及其 SHA-256。

## 固定 LightCurveRevision 输入准备

### 上游读取边界

应用 Port 位于：

```text
internal/application/light_curve_repository.go
```

领域类型位于：

```text
internal/domain/light_curve.go
```

逻辑读取接口为：

```go
type LightCurveRepository interface {
    GetRevision(
        ctx context.Context,
        objectID string,
        revision int64,
    ) (domain.LightCurveRevision, error)
}
```

冻结的目标上游接口为：

```text
GET /internal/v1/objects/{object_id}/light-curves/{light_curve_revision}
```

生产分类必须读取 Command 指定的固定 revision，禁止默认读取 latest。

阶段 4 只实现 Port、Fake 和读取用例。真实 HTTP Adapter 保持 `DEFERRED`。

### LightCurveRevision 最小字段

```text
object_id
revision
eligible_epoch_count
quality_policy_version（可选）
epochs:
  observation_time
  magnitude
  magnitude_error
```

上游负责科学质量过滤。分类侧不重新判断饱和、坏像元、背景异常或仪器级质量标记，只执行机械合法性校验。

### 固定 revision Reader

读取用例位于：

```text
internal/application/light_curve_reader.go
```

Reader 负责：

- 原样传递 `object_id` 和 `revision`；
- 传播 Context 与 Repository 错误；
- 拒绝返回身份不匹配；
- 返回 Repository 已隔离的数据。

Reader 不进行排序、机械校验、模式选择或模型输入构造。

### 机械准备

机械准备位于：

```text
internal/application/light_curve_preparer.go
```

处理顺序：

```text
三类 epoch 计数一致性
        ↓
实际 epoch 数范围 3..1024
        ↓
数值有限性
        ↓
magnitude_error > 0
        ↓
复制 Epochs
        ↓
仅按 ObservationTime 升序排序
        ↓
拒绝任何重复 ObservationTime
```

三类计数必须一致：

```text
ClassificationCommand.declared_eligible_epoch_count
LightCurveRevision.EligibleEpochCount
len(LightCurveRevision.Epochs)
```

规则：

- `ObservationTime` 必须为有限值；
- `Magnitude` 必须为有限值；
- `MagnitudeError` 必须为有限值且大于零；
- epoch 数必须位于 `3..1024`；
- 不截断、不补齐、不去重；
- 只按 `ObservationTime` 升序排序；
- 任意两个 epoch 的 `ObservationTime` 完全相同，则永久拒绝整个 revision；
- 排序前复制 `Epochs`，不修改 Repository 返回的底层数组。

## Model Bundle 最小解析边界

应用 Port 位于：

```text
internal/application/model_bundle_resolver.go
```

阶段 4 只解析选择历史粗概率所需的最小元数据：

```text
model_bundle_version
taxonomy_version
xgboost_model_version
feature_schema_version
```

测试替身位于：

```text
internal/testsupport/fakemodelbundle/
```

Resolver 必须精确解析 Command 已绑定的版本：

- 不自动选择 latest；
- 不 trim；
- 不转换大小写；
- 返回的 `model_bundle_version` 必须与请求一致。

阶段 5 已新增独立 `ServingBundleResolver`、manifest-v2 Loader 和 Triton 入口契约门禁；阶段 4 的最小 `ModelBundleResolver` 保持不变。Serving Contract、HTTP fixtures 与运行时制品摘要已经冻结并通过服务器验证，模型权重仍由外部 Triton 模型仓库管理。

## 粗分类模式选择

模式选择位于：

```text
internal/application/coarse_mode_selector.go
```

选择规则：

```text
3 <= actual_epoch_count <= 20
→ COMPUTE_CURRENT
→ 不查询历史粗分类结果

actual_epoch_count > 20
→ FindLatestCompatibleCoarse
```

历史查询结果：

```text
找到兼容结果
→ REUSE_PREVIOUS

仅 errors.Is(err, ErrCompatibleCoarseNotFound)
→ COMPUTE_BOOTSTRAP

其他查询错误
→ 原样传播
→ 禁止误判为未找到
→ 禁止 bootstrap
```

历史粗结果必须满足：

- `SourceRunID` 非空；
- 来源 revision 大于零；
- 来源 revision 严格小于目标 revision；
- 来源 epoch 数位于 `3..1024`。

兼容查询要求：

- object 相同；
- `xgboost_executed = true`；
- `taxonomy_version` 相同；
- `xgboost_model_version` 相同；
- `feature_schema_version` 相同；
- 来源 revision 严格小于目标 revision。

`execution_mode` 不参与粗分类兼容判断。

## ClassificationInput 构造与闭环

输入 Builder 位于：

```text
internal/application/classification_input_builder.go
```

它只负责：

- 保持 prepared revision 当前顺序；
- 复制 `TimeMJD`；
- 复制 `Magnitude`；
- 复制 `MagnitudeError`；
- 设置 `CoarseMode`；
- 仅在 `REUSE_PREVIOUS` 时复制七维历史粗概率。

Builder 不重新排序、不重复机械校验、不访问 Repository、不调用分类器。

闭环用例位于：

```text
internal/application/classification_input_preparer.go
```

编排顺序：

```text
ReadRevision
→ PrepareLightCurveRevision
→ CoarseModeSelector.Select
→ BuildClassificationInput
```

任一步失败后不继续调用后续依赖。例如：

- Repository 读取失败时不解析 Model Bundle；
- 机械合法性失败时不解析 Model Bundle；
- 机械合法性失败时不查询历史粗概率；
- 历史查询故障时不构造分类输入。

返回类型：

```text
PreparedClassificationInput
  Revision
  Selection
  Input
```

其中：

- `Revision` 保存规范化后的固定 revision；
- `Selection` 保存模式和粗概率来源追溯；
- `Input` 供后续 `VariableStarClassifier` 使用。

## Golden Vector 与确定性

Golden 测试位于：

```text
internal/application/classification_input_preparer_golden_test.go
```

覆盖：

```text
COMPUTE_CURRENT
REUSE_PREVIOUS
COMPUTE_BOOTSTRAP
```

验收内容：

- 相同固定输入重复执行得到完全相同结果；
- 修改前一次返回结果不会影响后续执行；
- 同一组 epoch 的不同输入排列得到相同规范化输入；
- Fake Repository 的固定响应不被调用方修改；
- `Revision`、`Selection` 与 `ClassificationInput` 的可变数据互不共享。

这些测试冻结的是阶段 4 的结构和确定性。阶段 5 已补充 Tensor Wire Mapping、HTTP fixtures 和真实 Triton 参考输出验证；正式 science-benchmark-v1 与科学审批仍保持 `PENDING_FORMAL_BENCHMARK`。

## PostgreSQL 存储

第一版 migration 位于：

```text
migrations/00001_create_classification_storage.sql
```

当前创建：

```text
classification_runs
current_classifications
```

当前阶段有意不创建：

```text
classification_jobs
model_bundles
processed_messages
outbox_events
上游测光和候选实体表
人工复核相关表
```

应用 Port：

```text
internal/application/classification_repository.go
```

PostgreSQL Adapter：

```text
internal/adapter/postgres/classification_repository.go
```

当前提供：

```text
SaveRunAndMaybeAdvanceCurrent
GetCurrent
FindLatestCompatibleCoarse
```

只有满足以下条件时推进 Current：

```text
execution_mode == PRODUCTION
且
new.light_curve_revision > current.light_curve_revision
```

因此旧 revision、相同 revision 的其他 bundle、SHADOW 和 REPROCESS 均不会覆盖当前生产分类。

### PostgreSQL 集成测试

通过环境变量启用：

```text
TEST_POSTGRES_DSN
```

PowerShell 示例：

```powershell
& { $env:TEST_POSTGRES_DSN = "postgres://<user>:<password>@127.0.0.1:5432/<test-database>?sslmode=disable";go test ./internal/adapter/postgres -run TestClassificationRepositoryIntegration -count=1 -v;Remove-Item Env:TEST_POSTGRES_DSN -ErrorAction SilentlyContinue }
```

未设置变量时，普通测试和 CI 跳过真实 PostgreSQL 集成测试。

## Kafka Adapter 与 Candidate Orchestrator

应用消息 Port：

```text
internal/application/messaging.go
```

Kafka Adapter：

```text
internal/adapter/kafka/
```

当前提供：

```text
Publisher
ConsumerRunner
```

Publisher 只要求 Topic 非空，并保持原始 Key、Value 和 Headers 语义。

Consumer Runner 使用：

```text
DisableAutoCommit
BlockRebalanceOnPoll
```

只有 Handler 成功后才提交 Offset。

Candidate Orchestrator 位于：

```text
cmd/candidate-orchestrator/
```

它消费 CandidateEvent，执行解码、Policy、Command 构造与发布；永久非法消息进入最小 Candidate DLQ。

### Kafka Broker 集成测试

通过以下变量启用：

```text
TEST_KAFKA_BROKERS
TEST_KAFKA_TOPIC
```

PowerShell 示例：

```powershell
& { $env:TEST_KAFKA_BROKERS = "<broker-host>:9092";$env:TEST_KAFKA_TOPIC = "<dedicated-test-topic>";go test ./internal/adapter/kafka -run TestKafkaPublisherConsumerIntegration -count=1 -v;Remove-Item Env:TEST_KAFKA_BROKERS -ErrorAction SilentlyContinue;Remove-Item Env:TEST_KAFKA_TOPIC -ErrorAction SilentlyContinue }
```

真实 Broker 实际执行仍为 `DEFERRED`。

## 验证状态

| 验证项 | 状态 |
| --- | --- |
| PostgreSQL Migration | `VERIFIED_CI` |
| Classification Repository | `VERIFIED_CI` |
| PostgreSQL Repository 本地真实集成测试 | `VERIFIED_LOCAL` |
| 独立服务器 PostgreSQL 验证 | `DEFERRED` |
| Kafka Publisher | `VERIFIED_CI` |
| Kafka Consumer Runner | `VERIFIED_CI` |
| CandidateEvent → ClassificationCommand | `VERIFIED_CI` |
| Candidate 最小 DLQ | `VERIFIED_CI` |
| Candidate Orchestrator | `VERIFIED_CI` |
| LightCurveRevision 领域类型与 Port | `VERIFIED_CI` |
| Fake LightCurveRepository | `VERIFIED_CI` |
| 固定 revision Reader | `VERIFIED_CI` |
| LightCurveRevision 机械准备 | `VERIFIED_CI` |
| ModelBundleResolver Port 与 Fake | `VERIFIED_CI` |
| CoarseModeSelector | `VERIFIED_CI` |
| ClassificationInput Builder | `VERIFIED_CI` |
| ClassificationInputPreparer | `VERIFIED_CI` |
| 输入准备 Golden 与确定性测试 | `VERIFIED_CI` |
| ServingBundleResolver Port 与 Fake | `VERIFIED_CI` |
| model-bundle-manifest-v2 Loader | `VERIFIED_CI` |
| Triton V2 HTTP Client | `VERIFIED_CI` |
| Triton Binary Tensor Codec | `VERIFIED_CI` |
| Triton Metadata / Config / Ready 契约门禁 | `VERIFIED_CI` |
| VariableStarClassifier Triton Adapter | `VERIFIED_CI` |
| HTTP Fake Server 推理夹具 | `VERIFIED_CI` |
| Serving Contract 与 fixture SHA-256 完整性门禁 | `VERIFIED_CI` |
| 真实 Triton 精确版本契约验收 | `VERIFIED_SERVER` |
| 真实 Triton 三模式单任务推理 | `VERIFIED_SERVER` |
| 真实 LightCurve HTTP Adapter | `DEFERRED` |
| 真实 Kafka Broker 集成测试 | `DEFERRED` |
| 完整 ClassificationResult Worker / Writer | `NOT_STARTED` |

状态含义：

```text
VERIFIED_LOCAL   已在本地真实环境验证
VERIFIED_CI      已由 GitHub Actions 验证
VERIFIED_SERVER  已在独立服务器环境验证
FAILED           已验证但失败
DEFERRED         已明确延后
NOT_STARTED      尚未开始
```

阶段 5 工程状态为 `VERIFIED_SERVER`，Serving Contract 和 Go Adapter 已接受。Manifest 继续保持 `DRAFT`，唯一未完成项是正式科学基准与 `scientific_approval`。

## 阶段边界

阶段 1 已完成工程基础、Protobuf、确定性身份和分类器 Port。

阶段 2 已完成 PostgreSQL Repository 与 Kafka Publisher、Consumer Runner。

阶段 3 已完成 CandidateEvent → ClassificationCommand 最小业务闭环。

阶段 4 已完成固定 revision 到规范化 `ClassificationInput` 的最小输入准备闭环。

阶段 5 已完成 Serving Bundle 解析、Triton V2 HTTP、Binary Tensor、启动契约门禁、VariableStarClassifier Adapter、HTTP fixtures 和真实 Triton 服务器验收。阶段 5 工程闭环已经关闭，正式科学基准作为独立待办保留。

后续阶段：

```text
阶段 6：ClassificationResult → ClassificationRun → CurrentClassification
阶段 7：查询 API、GUI 与人工复核
阶段 8：可靠性、可观测性与安全
阶段 9：Kubernetes 生产化
```

当前保持以下边界：

- 不读取 latest revision；
- 不自动选择 latest 模型版本；
- 不虚构上游数据库表或账号；
- 不实现真实 LightCurve HTTP Adapter；
- Go 不执行 XGBoost 特征计算；
- Go 不执行 Transformer 标准化、分桶、padding 或 mask；
- Triton 统一入口内部负责科学预处理、XGBoost、Transformer 与概率融合；
- 模型权重和 ONNX 制品不提交到 Go 仓库；
- 当前不实现推理自动重试、熔断和通用错误分类；
- 当前不发布 ClassificationResult；
- 当前不写入 ClassificationRun；
- 当前不更新 CurrentClassification。
