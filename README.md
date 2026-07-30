# Variable Star Classification

变源候选体实时分类系统的 Go 工程仓库。

系统面向地基光学望远镜产生的变源候选体，通过固定光变曲线 revision、版本化模型契约、确定性任务身份和事件驱动流程，形成可追溯、可重放、可扩展的实时分类基础。

## 当前阶段

阶段 1 至阶段 4 的最小工程闭环已经完成：

```text
阶段 1：工程仓库、Protobuf、确定性身份、分类器 Port 与基础 CI
阶段 2：PostgreSQL Classification Repository 与 Kafka 基础设施
阶段 3：CandidateEvent → ClassificationCommand 最小业务闭环
阶段 4：固定 revision → 规范化 ClassificationInput 最小输入准备闭环
```

阶段 4 当前形成：

```text
ClassificationCommand 中的固定 object_id + light_curve_revision
        ↓
LightCurveRepository
        ↓
LightCurveRevisionReader
        ↓
机械合法性校验、复制和时间排序
        ↓
ModelBundleResolver
        ↓
CoarseModeSelector
        ↓
ClassificationInput Builder
        ↓
PreparedClassificationInput
```

阶段 4 已覆盖三条粗分类输入路径：

```text
3 <= n <= 20
→ COMPUTE_CURRENT

n > 20 且存在兼容历史粗概率
→ REUSE_PREVIOUS

n > 20 且明确不存在兼容历史粗概率
→ COMPUTE_BOOTSTRAP
```

真实上游 HTTP Adapter、真实 Kafka Broker 验证和独立服务器 PostgreSQL 验证仍保持 `DEFERRED`。

下一开发阶段为：

```text
阶段 5：Triton 推理闭环
```

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
```

当前阶段不包含：

- 真实上游 LightCurve HTTP Adapter；
- 运行时自动选择 latest revision；
- Go 侧 XGBoost 特征计算；
- Go 侧 Transformer 标准化、分桶、padding 或 mask；
- 真实 XGBoost、Transformer 或 Triton 推理；
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
- Kafka：执行真实 Broker 集成测试。

## Go Module

```text
github.com/ZChen470/variable-star-classification
```

## 项目结构

```text
api/proto/astro/classification/v1/     Protobuf v1 源文件
gen/go/astro/classification/v1/        生成的 Go Protobuf 代码
cmd/candidate-orchestrator/             Candidate 编排服务入口
internal/domain/                       领域类型与确定性身份规则
internal/application/                  应用 Port、业务用例与输入准备
internal/adapter/postgres/             PostgreSQL Repository
internal/adapter/kafka/                Kafka Publisher 与 Consumer Runner
internal/testsupport/fakeclassifier/   Fake Classifier
internal/testsupport/fakelightcurve/   Fake LightCurveRepository
internal/testsupport/fakemodelbundle/  Fake ModelBundleResolver
migrations/                            Goose SQL migration
tests/                                 跨包测试
docs/contracts/                        上下游契约文档
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

完整 manifest loader、模型文件 checksum 和 Triton 入口校验属于阶段 5。

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

这些测试冻结的是阶段 4 的结构和确定性，不代表训练端特征、Tensor 或模型概率 Golden。模型侧科学 Golden 属于阶段 5。

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
| 真实 LightCurve HTTP Adapter | `DEFERRED` |
| 真实 Kafka Broker 集成测试 | `DEFERRED` |
| Triton 推理 | `NOT_STARTED` |
| 完整 ClassificationResult Writer | `NOT_STARTED` |

状态含义：

```text
VERIFIED_LOCAL   已在本地真实环境验证
VERIFIED_CI      已由 GitHub Actions 验证
VERIFIED_SERVER  已在独立服务器环境验证
FAILED           已验证但失败
DEFERRED         已明确延后
NOT_STARTED      尚未开始
```

## 阶段边界

阶段 1 已完成工程基础、Protobuf、确定性身份和分类器 Port。

阶段 2 已完成 PostgreSQL Repository 与 Kafka Publisher、Consumer Runner。

阶段 3 已完成 CandidateEvent → ClassificationCommand 最小业务闭环。

阶段 4 已完成固定 revision 到规范化 `ClassificationInput` 的最小输入准备闭环。

后续阶段：

```text
阶段 5：Triton 模型入口、输入契约校验、推理与概率结果
阶段 6：ClassificationResult → ClassificationRun → CurrentClassification
阶段 7：查询 API、GUI 与人工复核
阶段 8：可靠性、可观测性与安全
阶段 9：Kubernetes 生产化
```

阶段 4 保持以下边界：

- 不读取 latest；
- 不虚构上游数据库表或账号；
- 不实现真实 HTTP Adapter；
- 不执行 XGBoost 特征计算；
- 不执行 Transformer Tensor 构造；
- 不调用 Triton；
- 不发布 ClassificationResult；
- 不写入 ClassificationRun；
- 不更新 CurrentClassification。
