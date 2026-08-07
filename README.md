# Variable Star Classification

变源候选体实时分类系统的 Go 工程仓库。

系统面向地基光学望远镜产生的变源候选体，通过固定光变曲线 revision、版本化模型契约、确定性任务身份、Kafka 异步消息和 PostgreSQL 幂等持久化，形成可追溯、可重放、可扩展的实时分类基础。

## 当前阶段

阶段 1 至阶段 6 的工程闭环已经完成：

```text
阶段 1：工程仓库、Protobuf、确定性身份、分类器 Port 与基础 CI
阶段 2：PostgreSQL Classification Repository 与 Kafka 基础设施
阶段 3：CandidateEvent → ClassificationCommand
阶段 4：固定 revision → 规范化 ClassificationInput
阶段 5：Serving Bundle → Triton → ClassificationOutput
阶段 6：ClassificationCommand → ClassificationResult → ClassificationRun → CurrentClassification
```

当前运行链路：

```text
CandidateEvent
        ↓
candidate-orchestrator
        ↓
ClassificationCommand
        ↓
classifier-worker
        ↓
固定 LightCurveRevision HTTP 读取
        ↓
ClassificationInputPreparer
        ↓
Triton variable_star_classifier
        ↓
ClassificationResult
        ↓
classification-result-writer
        ↓
ClassificationRun
        ↓
CurrentClassification
```

阶段 6 已完成自动分类的应用层、Adapter 和运行时装配闭环。

必须区分不同验证层级：

```text
应用层与 Adapter：VERIFIED_CI
真实 Triton：VERIFIED_SERVER
本地真实 PostgreSQL：VERIFIED_LOCAL
真实 Kafka Broker：DEFERRED
真实上游 LightCurve 服务联调：DEFERRED
Kafka + LightCurve + Triton + PostgreSQL 联合 E2E：DEFERRED
正式科学批准：PENDING_FORMAL_BENCHMARK
```

下一阶段：

```text
阶段 7：查询 API、GUI 与人工复核
```

---

## 当前范围

当前仓库包含：

```text
Protobuf v1 契约

确定性 job_id / run_id

PostgreSQL:
  ClassificationRun
  CurrentClassification
  幂等保存
  Current 条件推进
  兼容历史粗概率查询

Kafka:
  Publisher
  ConsumerRunner
  手动 Offset
  Candidate DLQ
  Command DLQ
  Result DLQ

Candidate:
  CandidateEvent 解码与校验
  ClassificationPolicy v1
  ClassificationCommand 确定性构造
  CandidateHandler
  candidate-orchestrator

LightCurve:
  固定 revision Repository Port
  Fake Repository
  真实 HTTP Adapter
  LightCurveRevisionReader
  机械合法性校验
  防御性复制与确定性时间排序

Classification Input:
  ModelBundleResolver
  CoarseModeSelector
  ClassificationInputBuilder
  ClassificationInputPreparer
  Golden / deterministic tests

Serving:
  ServingBundleResolver
  model-bundle-manifest-v2 Loader
  Triton V2 HTTP Client
  Binary Tensor Codec
  Metadata / Config / Ready 契约门禁
  VariableStarClassifier Triton Adapter

Stage 6:
  ClassificationCommand Decoder
  Classification Worker
  ClassificationWorkerError
  有限快速 Retry
  Command DLQ
  ClassificationRun 构造
  ClassificationResult Proto / Kafka 构造
  job_id → Triton request id
  ClassificationResult Decoder
  Classification Result Writer
  Result DLQ
  Result → PostgreSQL 分层 E2E
  classifier-worker composition root
  classification-result-writer composition root
```

当前不包含：

- 自动选择 latest LightCurve revision；
- 自动选择 latest Model Bundle；
- 持久化 `ClassificationJob`；
- Go 侧 XGBoost 特征计算；
- Go 侧 Transformer 标准化、分桶、padding 或 mask；
- Transactional Outbox；
- `classification.updated` 领域事件；
- Retry Topic 或独立延迟调度器；
- 查询 API；
- GUI；
- 人工复核；
- UNKNOWN / OOD 业务判定；
- 完整生产可观测性与安全体系；
- Kubernetes 生产部署。

---

## 环境要求

基础开发环境：

- Go 1.25.0
- Git

可选工具和基础设施：

- Make：主要供 Linux 和 CI 使用；
- Buf：检查和生成 Protobuf；
- `protoc-gen-go`：生成 Go Protobuf；
- Goose：验证 PostgreSQL migration；
- PostgreSQL：Repository 和 Result Writer 集成测试；
- Kafka：真实 Broker 集成测试；
- Triton Inference Server：真实模型契约与推理验收；
- LightCurve HTTP Service：生产固定 revision 数据源。

Go Module：

```text
github.com/ZChen470/variable-star-classification
```

---

## 项目结构

```text
api/proto/astro/classification/v1/      Protobuf v1 源文件
gen/go/astro/classification/v1/         生成的 Go Protobuf

cmd/candidate-orchestrator/              Candidate → Command 入口
cmd/classifier-worker/                   Command → Result 入口
cmd/classification-result-writer/        Result → PostgreSQL 入口

internal/domain/                         领域类型与确定性身份
internal/application/                    应用 Port、用例与 Handler

internal/adapter/kafka/                  Kafka Publisher / ConsumerRunner
internal/adapter/postgres/               PostgreSQL Classification Repository
internal/adapter/lightcurve/             LightCurveRevision HTTP Adapter
internal/adapter/modelbundle/            Serving Bundle Manifest Loader
internal/adapter/triton/                 Triton V2 HTTP Adapter

internal/testsupport/fakeclassifier/
internal/testsupport/fakelightcurve/
internal/testsupport/fakemodelbundle/
internal/testsupport/fakeservingbundle/

models/bundles/                          Manifest、Serving Contract 与 fixtures
migrations/                              Goose migrations
docs/contracts/                          上下游契约
tests/                                   跨包测试
```

---

## 本地验证

PowerShell 基础门禁：

```powershell
& { gofmt -w .; if ($LASTEXITCODE -ne 0) { return }; go test ./... -count=1; if ($LASTEXITCODE -ne 0) { return }; go vet ./...; if ($LASTEXITCODE -ne 0) { return }; go build ./...; if ($LASTEXITCODE -ne 0) { return }; git diff --check; git status --short }
```

Linux / CI：

```bash
make ci
```

GitHub Actions 当前检查：

- Go 格式；
- Protobuf format / lint / build；
- Protobuf 生成代码漂移；
- Go Module 漂移；
- Goose migration；
- `go vet`；
- 全部普通测试；
- 全部包构建；
- Git 工作区漂移。

真实外部服务集成测试根据环境显式启用，不强塞入普通 CI。

---

## Protobuf

核心 Proto：

```text
CandidateEvent
ClassificationCommand
ClassificationResult
```

位置：

```text
api/proto/astro/classification/v1/
```

生成代码：

```text
gen/go/astro/classification/v1/
```

检查与生成：

```bash
buf format --exit-code
buf lint
buf build
buf generate
```

生成的 `.pb.go` 不应手工修改。

阶段 6 已退役不再使用的旧字段通过 Proto `reserved` 保留字段号/名称保护，避免后续误复用。

---

## 确定性任务身份

系统不持久化 `ClassificationJob`。

当前逻辑任务身份由：

```text
object_id
light_curve_revision
model_bundle_version
execution_mode
```

唯一确定。

不参与 JobID 的字段包括：

```text
candidate_revision
priority
trace
时间字段
```

领域实现：

```text
internal/domain/identity.go
```

规则：

```text
相同 JobIdentity
→ 相同 job_id

相同 job_id
→ 相同 run_id
```

当前使用 UUIDv5。

字符串身份字段原样参与计算，不自动 trim、不转换大小写。

`classifier-worker` 消费 Command 时会重新计算并验证 `job_id`。

`classification-result-writer` 消费 Result 时会重新计算并验证：

```text
job_id
run_id
```

`classification_policy_version` 已从活动契约和任务身份中退役，不再参与 Command、Result、Run 或 JobID。

---

## Candidate → ClassificationCommand

生产入口：

```text
cmd/candidate-orchestrator/
```

正常流程：

```text
CandidateEvent
→ Decode / Validate
→ ClassificationPolicy
→ deterministic job_id
→ ClassificationCommand
→ Kafka
```

当前 `RETRACTED` 不支持分类，作为永久非法 Candidate 消息进入 Candidate DLQ。

Command Kafka Key：

```text
object_id
```

Command 中固定：

```text
object_id
candidate_revision
light_curve_revision
declared_eligible_epoch_count
model_bundle_version
execution_mode
job_id
trace_context
```

Worker 不读取 latest revision，也不重新决定 Command 的任务身份。

---

## 固定 LightCurveRevision

生产 Worker 通过真实 HTTP Adapter 精确读取：

```text
GET /internal/v1/objects/{object_id}/light-curves/{light_curve_revision}
```

实现：

```text
internal/adapter/lightcurve/repository.go
```

禁止：

```text
latest
closest revision
revision fallback
```

HTTP 错误语义：

```text
404
→ ErrLightCurveRevisionNotFound
→ PERMANENT

409
→ ErrLightCurveRevisionNotReady
→ RETRYABLE

422
→ ErrLightCurveRevisionInconsistent
→ PERMANENT

429 / 5xx / network
→ ErrLightCurveSourceUnavailable
→ RETRYABLE
```

Adapter：

- 保留上游 epoch 原始顺序；
- 允许上游增加未知 JSON 字段；
- 检查请求与响应中的 `object_id` / revision 身份一致性；
- 不执行科学质量判断；
- 不执行 epoch 排序；
- 不执行 `3..1024` 数量范围校验；
- 不执行有限值或重复时间校验。

上游认证方式当前未被冻结，因此 Adapter 不虚构 Bearer Token、API Key 或 mTLS 契约；需要时由部署配置与 `http.Client.Transport` 扩展。

机械准备由应用层负责：

```text
LightCurveRevisionReader
→ 三类 epoch 数一致
→ 3..1024
→ finite values
→ magnitude_error > 0
→ 复制 Epochs
→ ObservationTime 升序排序
→ 重复 ObservationTime 永久拒绝
```

排序只发生在副本上，Repository 返回对象保持原始顺序。

---

## CoarseMode

当前支持：

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

只有：

```text
ErrCompatibleCoarseNotFound
```

允许进入 bootstrap。

其他历史查询错误保持失败，不得误判成“不存在历史结果”。

`classifier-worker` 使用 PostgreSQL `ClassificationRepository` 查询兼容历史粗概率，但 Worker 本身不写数据库。

---

## Serving Bundle 与 Triton

Serving Bundle 解析：

```text
internal/application/serving_bundle_resolver.go
internal/adapter/modelbundle/
```

运行时只使用精确：

```text
model_bundle_version
model_name
model_version
```

禁止自动选择 latest 或版本 fallback。

`classifier-worker` 每个进程只加载一次不可变 `FileServingBundleResolver`。同一个 Resolver：

```text
├── 作为 Worker 的 ServingBundleResolver
└── 投影为阶段 4 所需的最小 ModelBundleResolver
```

因此运行时 Bundle 身份由同一份 Manifest 统一提供，不增加每条消息的重复绑定校验。

统一 Triton 入口：

```text
model_name: variable_star_classifier
model_version: 1
protocol: triton-v2-http
max_batch_size: 0
binary_tensor_data: true
```

Go → Triton：

```text
TIME_MJD                 FP64  [N]
MAGNITUDE                FP32  [N]
MAGNITUDE_ERROR          FP32  [N]
COARSE_MODE              INT32 [1]
REUSED_COARSE_PROBS      FP32  [7]
```

Triton → Go：

```text
COARSE_PROBS             FP32 [7]
FINE_CONDITIONAL_PROBS   FP32 [10]
LEAF_PROBS               FP32 [12]
XGBOOST_EXECUTED         BOOL [1]
```

启动时执行精确版本：

```text
Ready
Metadata
Config
```

契约门禁。

`job_id` 会通过 Context 传给 Triton inference request：

```text
Triton request.id = ClassificationCommand.job_id
```

同一 Command 快速重试仍使用相同 request id。

---

## ClassificationRun 与 ClassificationResult

成功推理结果先构造：

```text
domain.ClassificationRun
```

然后映射为：

```text
ClassificationResult Proto
→ Kafka OutboundMessage
```

`run_id` 由 `job_id` 确定性生成。

Result Kafka Key：

```text
object_id
```

Result Kafka Timestamp：

```text
completed_at
```

Result Proto 和 Kafka Headers 传播 trace / 消息上下文。

阶段 6 不采集推理耗时，因此旧 timing 字段已退役，不填充虚构耗时。

当前活动 Result/Run 版本身份只保留：

```text
model_bundle_version
```

不再单独携带：

```text
classification_policy_version
xgboost_model_version
transformer_model_version
feature_schema_version
```

这些内部模型组成由不可变 Bundle Manifest 管理，不作为 Result/Run 的独立活动身份。

---

## Predicted Class

当前：

```text
predicted_coarse_class = argmax(coarse_probabilities)
predicted_leaf_class   = argmax(leaf_probabilities)
```

并列最大值时：

```text
选择最小数组索引
```

类别映射使用显式稳定 Enum ID，不依赖 Enum 数值恰好等于数组索引。

阶段 6 不增加第二套 Probability Validator。

概率范围、概率和、融合公式以及 REUSE 概率一致性继续由阶段 5 Serving Contract、服务端和 Triton Adapter 边界负责。

Result Writer 只重新检查 predicted class 与确定性 argmax 一致，不重新实现模型科学验证。

---

## Classification Worker

生产入口：

```text
cmd/classifier-worker/
```

处理链：

```text
ClassificationCommand
→ Decode + deterministic JobID check
→ fixed LightCurveRevision HTTP read
→ ClassificationInputPreparer
→ ServingBundleResolver
→ Triton Classify
→ ClassificationRun
→ ClassificationResult
→ Kafka Result publish
```

Worker：

- 不写 ClassificationRun 数据库；
- 不提交 Kafka Offset；
- 不读取 latest；
- 不填充虚构 timing；
- 只有完整成功结果才发布 ClassificationResult。

Command 处理装饰链：

```text
CommandDLQHandler
    ↓
CommandRetryHandler
    ↓
ClassificationWorkerHandler
```

即：

```text
DLQ(Retry(Worker))
```

这个顺序保证 Command DLQ 发布失败不会重新执行整个 Worker。

---

## Worker 错误与有限快速重试

Worker 使用结构化：

```text
ClassificationWorkerError
  Code
  Class
  Operation
  Cause
```

Class：

```text
RETRYABLE
PERMANENT
CANCELLED
```

保留 `errors.Is / errors.As` Cause 链，不依赖错误全文进行业务分类。

当前默认快速重试：

```text
首次执行
→ RETRYABLE
→ 等待 100 ms
→ 第 2 次执行
→ RETRYABLE
→ 等待 300 ms
→ 第 3 次执行
```

只重试 `RETRYABLE`。

不重试：

```text
PERMANENT
CANCELLED
非结构化错误
```

等待期间 Context 取消返回结构化 `CANCELLED`。

重试耗尽返回最后一个 `RETRYABLE`。

当前不实现 Retry Topic 或独立延迟调度器。

---

## Command DLQ

永久 Command 错误：

```text
PERMANENT
→ 原始 Command 发布到 Command DLQ
→ DLQ 成功
→ Handler 返回 nil
→ ConsumerRunner 提交原 Command offset
```

保留：

```text
原始 Key
原始 Value
原始 Headers
原始 Kafka Timestamp
```

追加稳定元数据：

```text
x-astro-error-code
x-astro-error-class
x-astro-error-operation
x-astro-original-topic
x-astro-original-partition
x-astro-original-offset
```

不把 `Cause.Error()` 文本作为稳定消息契约。

DLQ 发布失败：

```text
→ RETRYABLE
→ 不提交原 Command offset
```

---

## Command Offset 语义

Worker 只有在：

```text
ClassificationResult Kafka 发布成功
```

后才返回 `nil`。

因此：

```text
Result publish success
→ Handler nil
→ ConsumerRunner commit Command offset

Result publish failure
→ RETRYABLE
→ finite retry
→ failure remains
→ Handler error
→ no offset commit
```

Result 发布失败不进入 Command DLQ。

---

## Classification Result Writer

生产入口：

```text
cmd/classification-result-writer/
```

处理链：

```text
ClassificationResult
→ Decode
→ deterministic job_id / run_id check
→ ClassificationRun
→ SaveRunAndMaybeAdvanceCurrent
```

Result Writer 不信任 Kafka Result，但不会重新实现模型科学验证。

消费边界验证包括：

```text
Topic
Key
Proto
required fields
Kafka Key == object_id
job_id
run_id
CoarseSource relationship
predicted class argmax
```

不重新检查：

```text
概率范围
概率和
融合公式
REUSE 概率一致性
```

Repository 保存结果的正常返回值无论是：

```text
RunInserted=true,  CurrentAdvanced=true
RunInserted=true,  CurrentAdvanced=false
RunInserted=false, CurrentAdvanced=false
```

都属于成功处理。

重复 Result 因此是幂等成功。

---

## PostgreSQL

Migration：

```text
migrations/00002_create_classification_storage.sql
```

当前表：

```text
classification_runs
current_classifications
```

不创建：

```text
classification_jobs
outbox_events
```

Repository：

```text
internal/adapter/postgres/classification_repository.go
```

提供：

```text
SaveRunAndMaybeAdvanceCurrent
GetCurrent
FindLatestCompatibleCoarse
```

一个事务内：

```text
插入不可变 ClassificationRun
+
必要时推进 CurrentClassification
```

Current 推进条件：

```text
execution_mode == PRODUCTION
且
new.light_curve_revision > current.light_curve_revision
```

因此：

- 第一个 Production Run 可以建立 Current；
- 更高 revision Production Run 可以推进；
- 旧 revision 不推进；
- 相同 revision 的其他 Bundle 不推进；
- SHADOW 不推进；
- REPROCESS 不推进。

相同 Result 重放属于幂等成功。

Repository 身份冲突属于永久 Result 错误，由 Result DLQ 处置。

---

## Result DLQ 与 Offset

永久无效 Result 或 Repository 身份冲突：

```text
→ Result DLQ
→ DLQ 成功
→ Handler 返回 nil
→ 提交原 Result offset
```

Result DLQ 保留：

```text
原始 Key
原始 Value
原始 Headers
原始 Kafka Timestamp
```

追加：

```text
x-astro-error-code
x-astro-error-class
x-astro-error-field
x-astro-original-topic
x-astro-original-partition
x-astro-original-offset
```

数据库临时错误：

```text
→ 不进 Result DLQ
→ Handler error
→ 不提交 Result offset
```

Result DLQ 发布失败：

```text
→ Handler error
→ 不提交 Result offset
```

Context 取消同样不提交 offset。

阶段 6 不为 Result Writer 增加独立快速重试层。

---

## Runtime Composition

当前三个可执行服务：

```text
cmd/candidate-orchestrator/
cmd/classifier-worker/
cmd/classification-result-writer/
```

运行结构：

```text
candidate-orchestrator
        ↓
classifier-worker
        ↓
classification-result-writer
        ↓
PostgreSQL
```

### classifier-worker

主要运行依赖：

```text
Kafka
LightCurve HTTP Service
PostgreSQL
Serving Bundle Manifest
Triton
```

主要环境变量：

```text
KAFKA_BROKERS
KAFKA_CONSUMER_GROUP
KAFKA_CLIENT_ID

CLASSIFICATION_COMMAND_TOPIC
CLASSIFICATION_RESULT_TOPIC
CLASSIFICATION_COMMAND_DLQ_TOPIC

MODEL_BUNDLE_VERSION
MODEL_BUNDLE_MANIFEST_PATH

TRITON_BASE_URL
LIGHT_CURVE_BASE_URL
POSTGRES_DSN
```

`KAFKA_CLIENT_ID` 可选；其他变量为运行所需配置。

启动过程：

```text
配置校验
→ Serving Manifest 加载
→ 精确 Bundle 解析
→ LightCurve HTTP Repository
→ PostgreSQL
→ ClassificationInputPreparer
→ Triton Client
→ Ready / Metadata / Config Gate
→ VariableStarClassifier
→ Kafka
→ DLQ(Retry(Worker))
→ ConsumerRunner
```

当前 HTTP timeout：

```text
LightCurve: 10 s
Triton:     10 s
```

Triton 单响应最大读取：

```text
1 MiB
```

### classification-result-writer

主要依赖：

```text
Kafka
PostgreSQL
```

主要环境变量：

```text
KAFKA_BROKERS
KAFKA_CONSUMER_GROUP
KAFKA_CLIENT_ID

CLASSIFICATION_RESULT_TOPIC
CLASSIFICATION_RESULT_DLQ_TOPIC

POSTGRES_DSN
```

运行链：

```text
Kafka Result Consumer
→ ClassificationResultWriterHandler
→ PostgreSQL
→ Result DLQ Handler
→ ConsumerRunner
```

两个进程都使用：

```text
DisableAutoCommit
BlockRebalanceOnPoll
CloseAllowingRebalance
```

只有 Handler 返回 `nil` 时 ConsumerRunner 才提交对应消息 offset。

---

## 运行入口

### classifier-worker

示例 PowerShell 环境变量：

```powershell
$env:KAFKA_BROKERS="localhost:9092"
$env:KAFKA_CONSUMER_GROUP="variable-star-classifier-worker"
$env:CLASSIFICATION_COMMAND_TOPIC="astro.classification.commands.v1"
$env:CLASSIFICATION_RESULT_TOPIC="astro.classification.results.v1"
$env:CLASSIFICATION_COMMAND_DLQ_TOPIC="astro.classification.commands.dlq.v1"
$env:MODEL_BUNDLE_VERSION="bundle-v1"
$env:MODEL_BUNDLE_MANIFEST_PATH=".\models\bundles\model-bundle-manifest-v2.yaml"
$env:TRITON_BASE_URL="http://localhost:8000"
$env:LIGHT_CURVE_BASE_URL="http://localhost:8080"
$env:POSTGRES_DSN="postgres://..."
go run ./cmd/classifier-worker
```

### classification-result-writer

```powershell
$env:KAFKA_BROKERS="localhost:9092"
$env:KAFKA_CONSUMER_GROUP="variable-star-classification-result-writer"
$env:CLASSIFICATION_RESULT_TOPIC="astro.classification.results.v1"
$env:CLASSIFICATION_RESULT_DLQ_TOPIC="astro.classification.results.dlq.v1"
$env:POSTGRES_DSN="postgres://..."
go run ./cmd/classification-result-writer
```

以上仅展示配置形态，不代表仓库已经完成真实外部环境联合 E2E。

---

## 分层验收

### 应用层 Command → Run E2E

```text
ClassificationCommand
→ InputPreparer
→ Classifier
→ ClassificationResult
→ Result Writer
→ ClassificationRun
```

状态：

```text
VERIFIED_CI
```

覆盖：

- 确定性 JobID；
- 确定性 RunID；
- 固定 revision；
- Model Bundle；
- CoarseSource；
- 7 / 10 / 12 概率映射；
- predicted class；
- completed_at；
- `job_id → Triton request id`。

该测试使用 Fake 依赖，不替代真实外部环境 E2E。

### LightCurve HTTP 分层测试

```text
HTTP JSON
→ LightCurveRepository
→ LightCurveRevisionReader
→ PrepareLightCurveRevision
```

状态：

```text
VERIFIED_CI
```

覆盖：

- 固定 URL path；
- 响应身份；
- 上游顺序保留；
-应用层副本排序；
- `404 → PERMANENT / NotFound`；
- `409 → RETRYABLE / NotReady`；
- `422 → PERMANENT / Inconsistent`；
- `503 → RETRYABLE / SourceUnavailable`。

使用 HTTP Fake Server，不等于真实上游 LightCurve 服务联调。

### PostgreSQL Writer E2E

```text
ClassificationResult
→ Decoder
→ Writer
→ PostgreSQL
→ ClassificationRun / CurrentClassification
```

真实本地 PostgreSQL：

```text
VERIFIED_LOCAL
```

测试代码与普通工程门禁：

```text
VERIFIED_CI
```

覆盖：

- 幂等 Result 重放；
- Current 严格按更高 revision 推进；
- 旧 revision 不覆盖；
- 同 revision 不覆盖；
- SHADOW / REPROCESS 不推进；
- REUSED_PREVIOUS 来源 Run 关系。

### Triton

真实 Triton：

```text
VERIFIED_SERVER
```

覆盖：

```text
Ready
Metadata
Config
COMPUTE_CURRENT
REUSE_PREVIOUS
COMPUTE_BOOTSTRAP
Binary Tensor
```

### 尚未执行的联合外部环境 E2E

```text
真实 Kafka Broker：DEFERRED
真实上游 LightCurve HTTP 服务：DEFERRED
独立服务器 PostgreSQL：DEFERRED

Kafka + LightCurve + Triton + PostgreSQL 全链路：
DEFERRED
```

不得把应用层 Fake、HTTP Fake 或本地单依赖测试描述成真实外部环境全链路验收。

---

## Transactional Outbox

阶段 6 不实现：

```text
classification.updated
outbox_events
Transactional Outbox Publisher
```

并且禁止：

```text
PostgreSQL commit
→ 直接 Kafka publish classification.updated
```

因为这会产生数据库提交成功、Kafka 事件丢失的双写窗口。

当前状态：

```text
classification.updated：DEFERRED
Transactional Outbox：DEFERRED
```

后续确实出现下游领域事件需求时，再以独立事务 Outbox 设计实现。

---

## 验证状态

| 验证项 | 状态 |
| --- | --- |
| PostgreSQL Migration | `VERIFIED_CI` |
| Classification Repository | `VERIFIED_CI` |
| PostgreSQL Repository 本地真实测试 | `VERIFIED_LOCAL` |
| Result Writer → PostgreSQL 本地 E2E | `VERIFIED_LOCAL` |
| 独立服务器 PostgreSQL | `DEFERRED` |
| Kafka Publisher | `VERIFIED_CI` |
| Kafka ConsumerRunner | `VERIFIED_CI` |
| 真实 Kafka Broker | `DEFERRED` |
| Candidate Orchestrator | `VERIFIED_CI` |
| LightCurveRevision Reader / Prepare | `VERIFIED_CI` |
| LightCurve HTTP Adapter | `VERIFIED_CI` |
| 真实上游 LightCurve 服务联调 | `DEFERRED` |
| ClassificationInputPreparer | `VERIFIED_CI` |
| Serving Bundle Loader | `VERIFIED_CI` |
| Triton Client / Codec / Contract Gate | `VERIFIED_CI` |
| VariableStarClassifier Adapter | `VERIFIED_CI` |
| 真实 Triton | `VERIFIED_SERVER` |
| ClassificationCommand Decoder | `VERIFIED_CI` |
| Classification Worker | `VERIFIED_CI` |
| Worker Error Classification | `VERIFIED_CI` |
| Command Retry / DLQ | `VERIFIED_CI` |
| Result Publish / Command Offset | `VERIFIED_CI` |
| ClassificationResult Decoder | `VERIFIED_CI` |
| Classification Result Writer | `VERIFIED_CI` |
| Result DLQ / Result Offset | `VERIFIED_CI` |
| Runtime Composition | `VERIFIED_CI` |
| 应用层 Command → Run E2E | `VERIFIED_CI` |
| Transactional Outbox | `DEFERRED` |
| Kafka + LightCurve + Triton + PostgreSQL 联合 E2E | `DEFERRED` |
| 正式科学批准 | `PENDING_FORMAL_BENCHMARK` |

状态含义：

```text
VERIFIED_LOCAL
已在本地真实依赖环境执行通过

VERIFIED_CI
代码和普通自动化门禁已由 GitHub Actions 验证

VERIFIED_SERVER
已在独立服务器环境验证

DEFERRED
明确延后，不能描述为已验证

PENDING_FORMAL_BENCHMARK
工程链路允许继续，但正式科学批准尚未完成
```

---

## 阶段边界

阶段 1：

```text
工程基础、Protobuf、确定性身份、分类器 Port
```

阶段 2：

```text
PostgreSQL Repository + Kafka 基础设施
```

阶段 3：

```text
CandidateEvent → ClassificationCommand
```

阶段 4：

```text
fixed LightCurveRevision → ClassificationInput
```

阶段 5：

```text
Serving Bundle → Triton → ClassificationOutput
```

阶段 6：

```text
ClassificationCommand
→ Worker
→ ClassificationResult
→ Result Writer
→ ClassificationRun
→ CurrentClassification
```

阶段 6 工程闭环状态：

```text
CLOSED
```

继续保留的外部验证边界：

```text
真实 Kafka Broker：DEFERRED
真实上游 LightCurve 服务：DEFERRED
独立服务器 PostgreSQL：DEFERRED
完整联合服务器 E2E：DEFERRED
Transactional Outbox：DEFERRED
正式科学批准：PENDING_FORMAL_BENCHMARK
```

下一阶段：

```text
阶段 7：查询 API、GUI 与人工复核
```

后续：

```text
阶段 8：可靠性、可观测性与安全
阶段 9：Kubernetes 生产化
```
