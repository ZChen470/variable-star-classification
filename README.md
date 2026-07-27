# Variable Star Classification

变源候选体实时分类系统的 Go 工程仓库。

系统面向地基光学望远镜产生的变源候选体，通过固定的光变曲线 revision、版本化模型契约和事件驱动流程，形成可追溯、可重放、可扩展的实时分类基础。

## 当前阶段

阶段 1 的工程仓库、Protobuf、确定性任务标识、分类器 Port、Fake 和基础 CI 已完成。

阶段 2 当前已经完成：

- PostgreSQL 分类结果存储 migration；
- `ClassificationRun` 与 `CurrentClassification` 领域模型；
- PostgreSQL Classification Repository；
- Classification Run 幂等保存和 Current 条件推进；
- 历史兼容粗分类概率查询；
- Kafka 消息发布 Port；
- franz-go 同步 Publisher；
- 手动提交 Offset 的 Consumer Runner；
- Publisher 和 Consumer Runner 单元测试；
- PostgreSQL Repository 本地集成测试；
- Kafka 真实 Broker 集成测试代码。

真实 Kafka Broker 验证和独立服务器 PostgreSQL 验证延迟到服务器环境执行。

下一开发阶段为：

```text
S3-01 CandidateEvent → ClassificationCommand 范围校准
```

## 当前范围

当前仓库包含以下工程基础：

```text
Protobuf v1 契约
        ↓
确定性 job_id / run_id
        ↓
分类器应用 Port 与 Fake
        ↓
PostgreSQL Migration
        ↓
Classification Repository
        ↓
Kafka Publisher / Consumer Runner
```

当前阶段不包含：

- CandidateEvent 到 ClassificationCommand 的业务编排；
- 完整 ClassificationResult Writer；
- 持久化 ClassificationJob；
- Transactional Outbox；
- Retry Topic 和 DLQ 业务流程；
- 真实 XGBoost 或 Transformer 推理；
- 查询 API、GUI 和人工复核；
- Kubernetes 和生产部署。

## 环境要求

基础开发环境：

- Go 1.25.0
- Git

可选工具和基础设施：

- Make：主要供 Linux 和 CI 使用；
- Buf：检查和生成 Protobuf；
- `protoc-gen-go`：生成 Go Protobuf 代码；
- PostgreSQL：执行 Repository 集成测试；
- Kafka：执行真实 Broker 集成测试。

## Go Module

```text
github.com/ZChen470/variable-star-classification
```

## 项目结构

```text
api/proto/astro/classification/v1/   Protobuf v1 源文件
gen/go/astro/classification/v1/      生成的 Go Protobuf 代码
internal/domain/                     领域类型和确定性身份规则
internal/application/                应用 Port
internal/adapter/postgres/           PostgreSQL Repository
internal/adapter/kafka/              Kafka Publisher 和 Consumer Runner
internal/testsupport/                测试替身
migrations/                          Goose SQL migration
tests/                               跨包测试
```

## 本地验证

执行基础 Go 门禁：

```powershell
gofmt -w .
go test ./...
go vet ./...
go build ./...
git diff --check
```

安装 Make 的环境可以执行：

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
- 工作区漂移。

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

检查 Proto：

```bash
buf format --exit-code
buf lint
buf build
```

重新生成 Go 代码：

```bash
buf generate
```

生成代码位于：

```text
gen/go/astro/classification/v1/
```

生成的 `.pb.go` 文件不应直接手工修改。消息契约发生变化时，应修改对应 `.proto` 文件，再重新生成。

## 确定性任务标识

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

相同任务输入始终生成相同的 `job_id`。一个 `job_id` 始终生成相同的成功结果 `run_id`，用于支持 Kafka 至少一次投递下的幂等处理。

当前 ID 算法使用 UUIDv5。字符串按 UTF-8 原样参与计算，不自动去除空白或转换大小写。

固定测试向量：

```text
object_id: OBJ-0001
light_curve_revision: 21
model_bundle_version: bundle-2026-07-001
classification_policy_version: classification-policy-v1
execution_mode: PRODUCTION
```

预期结果：

```text
job_id: 46709af9-e19b-5dfc-beb5-b9213127fd18
run_id: d42c8015-e1f6-59df-b3ad-3e0f3cff2702
```

## 分类器应用边界

应用层通过以下接口调用变源分类器：

```text
internal/application.VariableStarClassifier
```

稳定输入输出类型位于：

```text
internal/application/classifier.go
```

当前模型输出维度固定为：

- 7 个粗类别概率；
- 10 个条件细类别概率；
- 12 个最终叶子类别概率。

应用接口不依赖 Protobuf、Kafka、数据库或 Triton 类型。

本地测试使用以下 Fake 实现：

```text
internal/testsupport/fakeclassifier/
```

Fake Classifier 可以返回预设结果、注入错误并记录调用输入。它只用于测试，不代表真实科学模型或生产推理实现。

## PostgreSQL 存储

第一版 migration 位于：

```text
migrations/00001_create_classification_storage.sql
```

当前只创建两张分类存储表：

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

### Classification Run

`classification_runs` 保存一次成功分类产生的不可变历史事实，包括：

- 确定性 `run_id` 和 `job_id`；
- object 和 revision；
- 执行模式；
- 粗概率来源；
- 完整模型和 Schema 版本；
- 7/10/12 维概率；
- 预测类别；
- 分阶段耗时；
- 完成和持久化时间。

### Current Classification

`current_classifications` 是每个 object 的当前分类指针。

只有满足以下条件时才会推进：

```text
execution_mode == PRODUCTION
且
new.light_curve_revision > current.light_curve_revision
```

因此：

- 旧 revision 不能覆盖新 revision；
- 相同 revision 的其他 bundle 只保留历史；
- SHADOW 和 REPROCESS 只保存历史，不更新 current。

### Repository

应用 Port 位于：

```text
internal/application/classification_repository.go
```

PostgreSQL Adapter 位于：

```text
internal/adapter/postgres/classification_repository.go
```

当前提供：

```text
SaveRunAndMaybeAdvanceCurrent
GetCurrent
FindLatestCompatibleCoarse
```

兼容粗分类结果要求：

- object 相同；
- `xgboost_executed = true`；
- `taxonomy_version` 相同；
- `xgboost_model_version` 相同；
- `feature_schema_version` 相同；
- 来源 revision 严格小于目标 revision。

`execution_mode` 不参与粗分类兼容判断。

### PostgreSQL 集成测试

集成测试文件：

```text
internal/adapter/postgres/classification_repository_integration_test.go
```

通过环境变量启用：

```text
TEST_POSTGRES_DSN
```

PowerShell 示例：

```powershell
$env:TEST_POSTGRES_DSN = "postgres://<user>:<password>@127.0.0.1:5432/<test-database>?sslmode=disable"
go test ./internal/adapter/postgres -run TestClassificationRepositoryIntegration -count=1 -v
Remove-Item Env:TEST_POSTGRES_DSN
```

测试数据库应使用独立的非生产数据库。

未设置 `TEST_POSTGRES_DSN` 时，普通本地测试和 CI 会跳过真实 PostgreSQL 集成测试。

## Kafka Adapter

应用消息 Port 位于：

```text
internal/application/messaging.go
```

Kafka Adapter 位于：

```text
internal/adapter/kafka/
```

当前使用 franz-go，并提供：

```text
Publisher
ConsumerRunner
```

### Publisher

Publisher 使用同步生产：

```text
ProduceSync
```

只有 Kafka 返回生产结果后，`Publish` 才返回成功。

业务消息必须包含：

- Topic；
- 非空 Key；
- 非空 Value；
- 合法的 Header Key。

本项目业务消息约定使用 `object_id` 作为 Kafka Key。

### Consumer Runner

Consumer Runner 使用：

```text
DisableAutoCommit
BlockRebalanceOnPoll
```

处理流程为：

```text
PollFetches
    ↓
MessageHandler.Handle
    ↓ 成功
CommitRecords
    ↓
AllowRebalance
```

Handler 返回错误时不会提交对应 Offset，因此消息可以在消费者重启后重新投递。

当前实现按单条消息同步提交 Offset，优先保证语义清晰。批量提交和性能优化将在真实负载测试后决定。

### Kafka Broker 集成测试

集成测试文件：

```text
internal/adapter/kafka/broker_integration_test.go
```

通过以下环境变量启用：

```text
TEST_KAFKA_BROKERS
TEST_KAFKA_TOPIC
```

测试 Topic 应是专用的单分区 Topic。

PowerShell 示例：

```powershell
$env:TEST_KAFKA_BROKERS = "<broker-host>:9092"
$env:TEST_KAFKA_TOPIC = "<dedicated-test-topic>"
go test ./internal/adapter/kafka -run TestKafkaPublisherConsumerIntegration -count=1 -v
Remove-Item Env:TEST_KAFKA_BROKERS
Remove-Item Env:TEST_KAFKA_TOPIC
```

该测试验证：

- 真实 Publisher 发布；
- Consumer Runner 消费；
- Handler 成功后提交 Offset；
- Handler 失败时不提交；
- 同一 Consumer Group 重启后重放未提交消息；
- 成功提交后不再重放。

当前本地 Kafka 环境验证状态为 `DEFERRED`，计划在服务器环境执行。

## 验证状态

| 验证项 | 状态 |
| --- | --- |
| Go 1.25.0 本地工具链 | `VERIFIED_LOCAL` |
| Go 1.25.0 GitHub Actions | `VERIFIED_CI` |
| Protobuf 检查和生成代码漂移 | `VERIFIED_CI` |
| Goose migration 检查 | `VERIFIED_CI` |
| PostgreSQL migration 本地 up/down/up | `VERIFIED_LOCAL` |
| PostgreSQL Repository 集成测试 | `VERIFIED_LOCAL` |
| 独立服务器 PostgreSQL 验证 | `DEFERRED` |
| Kafka Publisher 单元测试 | `VERIFIED_CI` |
| Kafka Consumer Runner 单元测试 | `VERIFIED_CI` |
| 真实 Kafka Broker 集成测试 | `DEFERRED` |
| CandidateEvent → ClassificationCommand | `NOT_STARTED` |
| 完整 ClassificationResult Writer | `NOT_STARTED` |

状态含义：

```text
VERIFIED_LOCAL   已在本地真实环境验证
VERIFIED_CI      已由 GitHub Actions 验证
VERIFIED_SERVER  已在独立服务器环境验证
FAILED           已验证但失败
DEFERRED         已明确延后验证
NOT_STARTED      尚未开始
```

## 阶段边界

阶段 2 只建立数据库和 Kafka 的可靠基础，不提前实现完整业务闭环。

后续阶段划分：

```text
阶段 3：CandidateEvent → ClassificationCommand
阶段 4：固定 revision 数据读取、预处理和特征
阶段 5：Triton 推理
阶段 6：ClassificationResult → Run → Current 完整闭环
```

这种划分避免在数据库和消息基础尚未稳定时，提前耦合编排、推理和结果写入逻辑。
