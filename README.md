# Variable Star Classification

变源候选体实时分类系统的 Go 工程仓库。

系统面向地基光学望远镜产生的变源候选体，通过固定光变曲线 revision、版本化模型契约、确定性任务身份、Kafka 异步消息、Triton 推理和 PostgreSQL 幂等持久化，形成可追溯、可重放、可恢复、可观测的实时分类基础。

## 当前状态

核心实时分类链路已经完成应用层、Adapter、运行时装配、服务器联合 E2E、故障恢复、安全基线、备份恢复和短时容量验收。

当前主链路：

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

当前主要验证状态：

```text
应用层与 Adapter：VERIFIED_CI
真实 Triton：VERIFIED_SERVER
真实 Kafka：VERIFIED_SERVER
独立服务器 PostgreSQL：VERIFIED_SERVER
LightCurve Mock HTTP 联合链路：VERIFIED_SERVER
Kafka + LightCurve + Triton + PostgreSQL 联合 E2E：VERIFIED_SERVER

33 events/s production peak：PASS / SERVER_VERIFIED
53 events/s short-duration safety headroom：PASS / SERVER_VERIFIED
67 events/s upper boundary：
  correctness / recovery PASS
  sustained capacity NOT PASS

安全基线：VERIFIED_CI
PostgreSQL PITR / backup / second-disk recovery：VERIFIED_SERVER

长时间 Soak：DEFERRED
Model Artifact Recovery：DEFERRED / DOCUMENTED RISK
正式科学批准：PENDING_FORMAL_BENCHMARK
```

查询 API、完整 GUI 与人工复核平台当前不作为本仓库生产化主线的阻塞项。仓库保留轻量科学测试入口 `cmd/science-classifier-web`，用于上传光变曲线并直接调用真实 Triton 做科学推理检查。

---

## 当前范围

当前仓库包含：

```text
Protobuf v1 契约

确定性：
  job_id
  run_id

PostgreSQL：
  ClassificationRun
  CurrentClassification
  幂等保存
  Current 条件推进
  兼容历史粗概率查询

Kafka：
  Publisher
  ConsumerRunner
  RebalanceYieldConsumerRunner
  手动 Offset
  SCRAM-SHA-256 可选认证
  Candidate DLQ
  Command DLQ
  Result DLQ

Candidate：
  CandidateEvent 解码与校验
  ClassificationPolicy
  ClassificationCommand 确定性构造
  CandidateHandler
  candidate-orchestrator

LightCurve：
  固定 revision Repository Port
  Fake Repository
  真实 HTTP Adapter
  LightCurveRevisionReader
  机械合法性校验
  防御性复制
  ObservationTime 确定性排序

Classification Input：
  ModelBundleResolver
  CoarseModeSelector
  ClassificationInputBuilder
  ClassificationInputPreparer
  Golden / deterministic tests

Serving：
  ServingBundleResolver
  model-bundle-manifest-v2 Loader
  Triton V2 HTTP Client
  Binary Tensor Codec
  Metadata / Config / Ready 契约门禁
  VariableStarClassifier Triton Adapter
  job_id → Triton request id

Result：
  ClassificationRun 构造
  ClassificationResult Proto / Kafka 构造
  ClassificationResult Decoder
  classification-result-writer
  Result → PostgreSQL 分层 E2E

Observability：
  JSON structured logging
  /live
  /ready
  /metrics
  Kafka metrics
  HTTP metrics
  PostgreSQL pgxpool metrics
  retry / retry-age metrics
  rebalance-blocked metrics
  Result persistence metrics

Operations：
  Runbooks
  Kafka 单 Broker 故障恢复
  PostgreSQL PITR
  WAL archive
  verified physical base backup
  second-disk WAL mirror
  bounded backup retention

Security：
  govulncheck CI gate
  Gitleaks full-history CI gate

Load：
  17 / 33 / 53 / 67 events/s 服务器负载证据
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
- 完整 Query API；
- 完整科学 GUI / 人工复核平台；
- UNKNOWN / OOD 业务判定；
- Kubernetes 生产部署；
- 自动化 Model Artifact Recovery；
- 正式 science-benchmark 批准。

---

## 环境要求

基础开发环境：

- Go；
- Git。

当前 CI 使用：

```text
Go 1.25.13
```

可选工具和基础设施：

- Make；
- Buf；
- `protoc-gen-go`；
- Goose；
- PostgreSQL；
- Kafka；
- Triton Inference Server；
- Docker / Docker Compose；
- NVIDIA GPU Runtime；
- Prometheus-compatible metrics tooling。

Go Module：

```text
github.com/ZChen470/variable-star-classification
```

---

## 项目结构

```text
api/proto/astro/classification/v1/       Protobuf v1 源文件
gen/go/astro/classification/v1/          生成的 Go Protobuf

cmd/candidate-orchestrator/               Candidate → Command
cmd/classifier-worker/                    Command → Result
cmd/async-classifier-worker/              有界并发 Command → Result（压测候选版本）
cmd/classification-result-writer/         Result → PostgreSQL
cmd/lightcurve-mock-server/               联调 LightCurve + Candidate mock
cmd/science-classifier-web/               轻量科学推理入口

internal/domain/                          领域类型与确定性身份
internal/application/                     应用 Port、用例与 Handler

internal/adapter/kafka/                   Kafka Publisher / Consumer
internal/adapter/postgres/                PostgreSQL Classification Repository
internal/adapter/lightcurve/              LightCurveRevision HTTP Adapter
internal/adapter/modelbundle/             Serving Bundle Manifest Loader
internal/adapter/triton/                  Triton V2 HTTP Adapter

internal/observability/logging/           slog JSON logging
internal/observability/management/        /live /ready /metrics
internal/observability/kafkametrics/      Kafka Prometheus metrics
internal/observability/httpmetrics/       HTTP client metrics
internal/observability/postgresmetrics/   pgxpool metrics

internal/testsupport/                     Fake / fixture 支持

models/bundles/                           Manifest、Serving Contract、fixtures
migrations/                               Goose migrations
docs/runbooks/                            运维与恢复 Runbook
tests/                                    跨包测试
```

---

## 本地与 CI 验证

基础 Go 门禁：

```bash
gofmt -w .
go test ./... -count=1
go vet ./...
go build ./...
git diff --check
```

Linux / CI：

```bash
make ci
```

GitHub Actions 当前覆盖：

- Go 格式检查；
- Protobuf format / lint / build；
- Protobuf 生成代码漂移；
- Go Module 漂移；
- Goose migration；
- `go vet`；
- 全部普通测试；
- 全部包构建；
- `govulncheck`；
- Gitleaks 自检；
- Gitleaks 完整 Git 历史扫描；
- Git 工作区漂移检查。

真实 Kafka、PostgreSQL、Triton 和联合服务器 E2E 不强塞入普通 CI，而使用显式服务器验收。

手动触发 `.github/workflows/publish-go-images.yml` 会分别构建并发布：

```text
candidate-orchestrator
classifier-worker
async-classifier-worker（并发压测候选）
classification-result-writer
```

镜像使用 Git commit SHA 作为 tag，并在 workflow summary 中记录 digest。并发候选镜像发布为：

```text
ghcr.io/zchen470/variable-star-async-classifier-worker:${GIT_SHA}
```

当前 K8s `classifier-worker` Deployment 仍固定到已验证的串行镜像 digest。发布并发候选镜像后，应先用 workflow 输出的真实 digest 做实验部署；完成压测和故障验收前，不把未知 digest 或可变 tag 写入生产 base manifest。

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

已退役字段通过 Proto `reserved` 保留字段号和名称，避免未来误复用。

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

`classifier-worker` 消费 Command 时重新计算并验证 `job_id`。

`classification-result-writer` 消费 Result 时重新计算并验证：

```text
job_id
run_id
```

`classification_policy_version` 已从活动任务身份中退役，不再参与 Command / Result / Run / JobID 身份计算。

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

Command 固定携带：

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

Worker 不读取 latest revision，也不重新决定 Command 的逻辑身份。

---

## 固定 LightCurveRevision

生产 Worker 精确读取：

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

当前上游语义要求：只有固定 `LightCurveRevision` 完整生成并可读取后，才发布对应 Kafka 消息。

当前主要 HTTP 错误语义：

```text
200
→ 固定 revision 存在且可读取

404
→ ErrLightCurveRevisionNotFound
→ PERMANENT

422
→ ErrLightCurveRevisionInconsistent
→ PERMANENT

429 / 5xx / network
→ ErrLightCurveSourceUnavailable
→ RETRYABLE
```

当前不依赖 record-level `409 NotReady` 作为生产重试机制。

Adapter：

- 保留上游 epoch 原始顺序；
- 允许上游增加未知 JSON 字段；
- 验证响应 `object_id` / revision 身份；
- 不执行科学质量判断；
- 不执行 epoch 排序；
- 不执行 `3..1024` 数量范围校验；
- 不执行有限值或重复时间校验。

机械准备由应用层负责：

```text
LightCurveRevisionReader
→ Command / Revision / Epoch 实际数量一致
→ 3..1024
→ finite values
→ magnitude_error > 0
→ 复制 Epochs
→ ObservationTime 升序排序
→ 重复 ObservationTime 永久拒绝
```

排序只发生在副本上。

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

`classifier-worker` 使用 PostgreSQL `ClassificationRepository` 查询兼容历史粗概率，但 Worker 本身不写 ClassificationRun 数据库。

---

## Serving Bundle 与 Triton

Serving Bundle：

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

禁止 latest 或版本 fallback。

`classifier-worker` 每个进程只加载一次不可变 `FileServingBundleResolver`。

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

`job_id` 通过 Context 传给 Triton inference request：

```text
Triton request.id = ClassificationCommand.job_id
```

同一 Command 重试继续使用同一个 request id。

---

## ClassificationRun 与 ClassificationResult

成功推理先形成：

```text
domain.ClassificationRun
```

再映射为：

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

TraceContext 和 Kafka Headers 按既有契约传播。

当前活动 Result / Run 模型身份保留：

```text
model_bundle_version
```

内部 XGBoost / Transformer / Schema 组成由不可变 Bundle Manifest 管理。

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

类别映射使用显式稳定 Enum ID。

Result Writer 只重新检查 predicted class 与确定性 argmax 一致，不重新实现概率范围、概率和、融合公式或 REUSE 概率一致性验证。

---

## Classification Worker

当前生产串行入口：

```text
cmd/classifier-worker/
```

并发压测候选入口：

```text
cmd/async-classifier-worker/
```

候选入口复用相同的业务 Handler、Retry、DLQ、LightCurve、PostgreSQL、Triton 和 Result publish 语义，只替换 Kafka record 的执行与提交编排。验证完成前两个入口并存；完成真实正确性、rebalance 和 `1/2/4/8/16` 吞吐测试后，再将并发 runner 合并回 `cmd/classifier-worker`，继续沿用原生产镜像名和 Deployment，避免长期维护两份 composition root。

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

- 不直接写 ClassificationRun；
- 不读取 latest；
- 只有完整成功结果才发布 ClassificationResult；
- Result 发布成功后才允许原 Command offset 被提交。

并发候选版本额外保证：

- `PollRecords(ctx, concurrency)` 形成有界批次；
- 相同 Kafka key（`object_id`）按 poll 顺序串行，不同 key 并行；
- record 可乱序完成，但每个 partition 只提交连续成功前缀；
- completion tracker 按实际 fetched offset 顺序推进，不假设 offset 数字连续；
- rebalance yield 取消整个批次，旧 session 结束后从 committed offset 恢复。

并发度配置：

```text
CLASSIFIER_WORKER_CONCURRENCY=8
```

允许范围为 `1..64`，默认 `1`。完整实现说明、Mermaid 图、压测路线和面试问答见 [`docs/S10_classifier_worker_concurrency_design.md`](docs/S10_classifier_worker_concurrency_design.md)。

Command 装饰链：

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

这个顺序保证 Command DLQ 发布失败不会重新执行完整 Worker 流程。

---

## Worker 错误与长期 RETRYABLE

Worker 使用结构化错误：

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

保留 `errors.Is / errors.As` Cause 链，不依赖错误全文做业务分类。

生产运行时对 `RETRYABLE` 使用：

```text
capped backoff
+
无最大尝试次数
```

当前 backoff 上限：

```text
10 s
```

语义：

```text
RETRYABLE
→ 持续等待并重新执行
→ 直到成功或 Context 取消

PERMANENT
→ 不重试
→ 交给 Command DLQ

CANCELLED
→ 不提交当前 record
```

该设计替代了早期“有限快速重试耗尽后让进程退出”的方案。

不实现 Retry Topic 或独立延迟调度器。

Prometheus 暴露：

```text
astro_classification_command_retry_attempts_total
astro_classification_command_retrying
astro_classification_command_retry_age_seconds
```

---

## Rebalance Yield 与 Consumer Session Recovery

串行 `classifier-worker` 使用：

```text
DisableAutoCommit
BlockRebalanceOnPoll
PollRecords(ctx, 1)
```

长期 RETRYABLE 与 `BlockRebalanceOnPoll` 组合时，consumer group rebalance 可能等待当前 record 处理完成。

为避免无限阻塞 rebalance，Worker 使用专用：

```text
RebalanceYieldConsumerRunner
```

当 `OnPartitionsCallbackBlocked` 触发时：

```text
记录 rebalance blocked metric
→ 请求 RebalanceYield
→ 取消当前 record Context
→ 不提交当前 record
→ AllowRebalance
→ 返回 ErrRebalanceYielded
→ 结束旧 consumer session
→ CloseAllowingRebalance
→ 创建 fresh consumer client
→ 重新加入 group
→ 从 committed offset 恢复
```

关键约束：

> yield 后禁止继续使用旧 consumer session Poll 同 partition 后续 offset。

真实 Kafka 双 Worker 测试已验证：未提交的旧 offset 能由新的 consumer session / 其他 group member 从 committed offset 重新读取，不会因为 fetch position 前进而被后续 commit 越过。

Prometheus 指标：

```text
astro_kafka_rebalance_callback_blocked_total
```

并发候选版本继续使用同一 generation/fresh-session 原则，但取消单位从单条 record 扩展为一个有界 poll batch：

```text
blocked rebalance
→ cancel batch Context
→ 等待所有 in-flight Handler 退出
→ 整批不 commit
→ AllowRebalance
→ 关闭旧 consumer session
→ fresh session 从 committed offset 恢复
```

---

## Command DLQ 与 Offset

永久 Command 错误：

```text
PERMANENT
→ 发布原始 Command 到 Command DLQ
→ DLQ 成功
→ Handler 返回 nil
→ 提交原 Command offset
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

DLQ 发布失败：

```text
→ RETRYABLE
→ 不提交原 Command offset
```

Result 发布失败同样属于 RETRYABLE，不进入 Command DLQ。

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

Repository 正常返回：

```text
RunInserted=true,  CurrentAdvanced=true
RunInserted=true,  CurrentAdvanced=false
RunInserted=false, CurrentAdvanced=false
```

都视为成功。

重复 Result 因此属于幂等成功。

---

## Result Writer RETRYABLE 与 Rebalance

PostgreSQL 临时错误不进入 Result DLQ。

当前运行链：

```text
ResultDLQ(
    ResultRetry(
        ResultWriter
    )
)
```

数据库单次持久化仍受独立 timeout 限制，但暂时性数据库错误会在当前 record 上持续 RETRYABLE，而不是直接让 Result Writer 进程退出。

`classification-result-writer` 与 Worker 一样使用：

```text
RebalanceYieldConsumerRunner
+
fresh consumer session recovery
```

blocked rebalance 时：

```text
取消当前 record
→ 不提交
→ AllowRebalance
→ 关闭旧 session
→ 创建 fresh consumer
→ 从 committed offset 恢复
```

---

## PostgreSQL

Migration 位于：

```text
migrations/
```

核心表：

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

同一事务中：

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

- 第一个 Production Run 建立 Current；
- 更高 revision Production Run 推进；
- 旧 revision 不推进；
- 同 revision 不覆盖；
- SHADOW 不推进；
- REPROCESS 不推进。

相同 Result 重放是幂等成功。

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
→ RETRYABLE
→ 不提交 Result offset
```

Context 取消同样不提交当前 offset。

---

## Runtime Composition

生产主链路当前三个核心 daemon：

```text
candidate-orchestrator
classifier-worker
classification-result-writer
```

联调辅助：

```text
lightcurve-mock-server
```

科学测试辅助：

```text
science-classifier-web
```

### candidate-orchestrator

主要依赖：

```text
Kafka
```

负责：

```text
CandidateEvent
→ ClassificationPolicy
→ ClassificationCommand
```

### classifier-worker

主要依赖：

```text
Kafka
LightCurve HTTP
PostgreSQL
Serving Bundle Manifest
Triton
```

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
→ Kafka producer
→ 可重建 Kafka consumer session
→ DLQ(Retry(Worker))
```

### classification-result-writer

主要依赖：

```text
Kafka
PostgreSQL
```

运行链：

```text
Kafka Result Consumer
→ ResultRetry
→ ClassificationResultWriter
→ PostgreSQL
→ Result DLQ
```

Worker 和 Result Writer 都使用：

```text
稳定 producer client
+
可重建 consumer client/session
```

以支持 rebalance-yield 后安全结束旧 session 并从 committed offset 恢复。

---

## Kafka 配置

Kafka client 使用 franz-go。

生产 Consumer 基础：

```text
DisableAutoCommit
BlockRebalanceOnPoll
manual commit
CloseAllowingRebalance
```

服务器环境支持可选：

```text
KAFKA_SASL_USERNAME
KAFKA_SASL_PASSWORD
```

必须成对提供。

启用后使用：

```text
SASL_PLAINTEXT
SCRAM-SHA-256
```

当前服务器业务 / DLQ topic 已验证：

```text
partitions = 3
replication factor = 3
min.insync.replicas = 2
```

单 Broker 短时故障期间链路能够继续运行，Broker 恢复后 ISR 恢复为 3。

---

## Management 与可观测性

三个核心 daemon 提供独立 management listener：

```text
/live
/ready
/metrics
```

配置：

```text
MANAGEMENT_LISTEN_ADDR
```

management listener 应部署在私有管理网络，不作为业务 API 暴露。

当前日志使用：

```text
log/slog
JSON Handler
```

主要结构化字段包括：

```text
service
operation
object_id
light_curve_revision
job_id
run_id
model_bundle_version
trace_id
correlation_id
causation_id
kafka_topic
kafka_partition
kafka_offset
error_code
error_class
```

Prometheus instrumentation 包括：

```text
Kafka broker / producer / consumer
Kafka group management errors
rebalance callback blocked
LightCurve HTTP
Triton HTTP
pgxpool
ClassificationCommand retry
retry age
Result persistence
ClassificationRun persisted
CurrentClassification advanced
```

避免使用以下高基数标签：

```text
object_id
job_id
run_id
trace_id
partition
offset
error text
broker host
SQL text
```

---

## Readiness

`candidate-orchestrator` readiness 在运行依赖装配和 management listener 成功 bind 后置为 ready。

`classifier-worker` readiness 需要启动阶段完成：

```text
Serving Manifest / Bundle
PostgreSQL startup Ping
Triton serving contract gate
Kafka / Worker / Retry / DLQ 装配
management listener bind
```

`/ready` 不主动访问 LightCurve 上游。

`classification-result-writer` readiness 需要：

```text
PostgreSQL startup Ping
Kafka / Writer / Retry / DLQ 装配
management listener bind
```

长期运行期间，暂时性外部依赖错误通过 retry 处理；readiness 不作为每条消息依赖健康状态的实时代理。

---

## LightCurve Mock Server

联调入口：

```text
cmd/lightcurve-mock-server/
```

从本地真实 CSV / TXT 光变曲线加载固定 revision 数据集。

同一进程：

```text
HTTP:
GET /internal/v1/objects/{object_id}/light-curves/{revision}

Kafka:
按配置速率持续发布 CandidateEvent
```

主要配置：

```text
LIGHTCURVE_MOCK_DATA_DIR
LIGHTCURVE_MOCK_LISTEN_ADDR
CANDIDATE_TOPIC
CANDIDATE_RATE_PER_SECOND
KAFKA_BROKERS
KAFKA_SASL_USERNAME
KAFKA_SASL_PASSWORD
```

该工具用于服务器 E2E / 故障 / 负载测试，不代表真实上游生产服务。

---

## Science Classifier Web

轻量科学入口：

```text
cmd/science-classifier-web/
```

用途：

- 上传包含 `time / magnitude / magnitude_error` 的 CSV / TXT；
- 直接调用真实 Triton；
- 展示 7 / 10 / 12 概率；
- 展示 predicted coarse / leaf class；
- 展示 CoarseMode 和 XGBoost 是否执行。

该入口：

- 不使用 Kafka；
- 不使用 PostgreSQL；
- 不使用 LightCurve HTTP；
- 不创建 ClassificationCommand / Result / Run；
- 不保存上传数据和分类结果；
- `3..20` 使用 `COMPUTE_CURRENT`；
- `21..1024` 使用 `COMPUTE_BOOTSTRAP`；
- 不支持 `REUSE_PREVIOUS`。

它仅是科学测试工具，不属于生产实时分类链路。

---

## 安全基线

GitHub Actions 当前固定：

```text
Go 1.25.13
govulncheck v1.7.0
Gitleaks v8.18.4
```

`govulncheck`：

```bash
govulncheck ./...
```

当前 CI 已通过。

Gitleaks：

- 使用官方 release binary；
- CI checkout 使用完整 Git 历史；
- 包含 synthetic self-test；
- 对整个 Git 历史执行扫描；
- 已知 synthetic self-test finding 只通过精确 fingerprint ignore；
- 不使用宽泛路径忽略或 baseline 隐藏真实泄漏。

当前状态：

```text
govulncheck：VERIFIED_CI
Gitleaks：VERIFIED_CI
```

---

## PostgreSQL 备份与恢复

当前服务器已完成实际恢复演练。

覆盖：

```text
WAL archive
物理 pg_basebackup
pg_verifybackup
PITR
独立恢复实例验证
第二磁盘备份副本
持续 WAL mirror
有限保留
恢复 Runbook
```

当前 provisional 目标：

```text
RPO <= 5 min
RTO <= 1 h
```

同机异盘 WAL mirror 已进行真实时间测量，满足当前：

```text
SECOND_DISK_RPO_WITHIN_5MIN = PASS
```

需要明确：

> 第二磁盘仍位于同一台物理服务器，不构成异机、异地或机房级灾备。

Model Artifact Recovery 当前仍为：

```text
DEFERRED / DOCUMENTED RISK
```

---

## Runbooks

目录：

```text
docs/runbooks/
```

当前包含：

```text
README.md
classifier-worker-dependency-unavailable.md
kafka-consumer-lag-and-stuck.md
dlq-growth.md
postgresql-unavailable-and-slow.md
postgresql-recovery.md
contract-and-manifest-mismatch.md
disk-space.md
service-restart-loop.md
```

覆盖：

- Triton / LightCurve 不可用；
- Worker 长期 RETRYABLE；
- Kafka lag 与 stuck consumer；
- DLQ 增长；
- PostgreSQL unavailable / slow；
- PostgreSQL backup / PITR recovery；
- Bundle / Contract mismatch；
- 磁盘空间；
- 服务重启循环；
- Triton immutable model release / rollback。

---

## 服务器故障验收

已验证的主要故障场景：

```text
classification-result-writer 重启恢复
classifier-worker 停机积压与恢复
candidate-orchestrator 停机积压与恢复
classifier-worker docker kill
Triton 短时不可用与自动恢复
PostgreSQL 短时不可用与自动恢复
双 Worker rebalance-yield
fresh consumer session recovery
Kafka 单 Broker 故障与 ISR 恢复
```

关键正确性语义：

```text
副作用完成前不提交 Kafka offset
PERMANENT → DLQ → 成功后提交
RETRYABLE → 不 DLQ → 持续重试
rebalance yield → 当前 record 不提交
fresh consumer session → 从 committed offset 恢复
```

---

## Load / Capacity 验收

服务器负载链路：

```text
lightcurve-mock-server
→ Candidate Kafka
→ candidate-orchestrator
→ ClassificationCommand Kafka
→ classifier-worker
→ LightCurve HTTP
→ Triton
→ ClassificationResult Kafka
→ classification-result-writer
→ PostgreSQL
```

### 17 events/s

```text
PASS / SERVER_VERIFIED
```

单 Worker 下端到端链路稳定，最终三个 consumer group lag 为 0。

### 33 events/s production peak

```text
PASS / SERVER_VERIFIED
classifier workers = 2
```

约 5 分钟窗口：

```text
Candidate consumed       10,640
Command produced         10,640
Workers consumed total   10,640
Results produced total   10,640
Writer consumed          10,640
PostgreSQL success       10,640
```

最终三个 consumer group 所有 partition：

```text
LAG = 0
```

测试窗口：

```text
new retry = 0
new Command DLQ = 0
restart = 0
core errors = 0
```

### 53 events/s short-duration safety headroom

```text
PASS / SERVER_VERIFIED
classifier workers = 2
```

约 5 分钟窗口：

```text
Candidate consumed       17,231
Command produced         17,231

Worker 1 consumed         7,387
Worker 2 consumed         9,845
Workers consumed total   17,232

Worker 1 results          7,386
Worker 2 results          9,844
Results produced total   17,230

Writer consumed          17,232
PostgreSQL success       17,232
```

Worker / Result 的少量 ±1 边界来自顺序抓取 metrics 时的 in-flight record。

窗口结束瞬时 lag：

```text
Candidate total lag = 3
Command total lag   = 8
Result total lag    = 4
```

恢复 1/s 后最终全部归零。

测试期间：

```text
new retry = 0
new Command DLQ = 0
restart = 0
core errors = 0
all readiness = 200
```

因此：

> 在当前服务器、当前测试数据分布、3 Kafka partitions 和 2 classifier-workers 条件下，53 events/s 是已经验证的短时持续安全余量。

### 67 events/s upper boundary

```text
Correctness:        PASS
Recovery:           PASS
Sustained capacity: NOT PASS
```

窗口计数：

```text
Candidate consumed       28,125
Command produced         28,125
Workers consumed total   25,289
Results produced total   25,287
Writer consumed          25,289
PostgreSQL success       25,288
```

窗口内 total lag：

```text
sample 01 =   93
sample 02 =  390
sample 03 =  684
sample 04 =  956
sample 05 = 1238
sample 06 = 1515
sample 07 = 1796
sample 08 = 2074
sample 09 = 2352
sample 10 = 2627

window end = 2896
```

主要积压：

```text
ClassificationCommand partition 2 lag = 2885
```

Command partition 0 / 1 仅约 1 条。

当前 mock 只有 7 个固定 `object_id`，Kafka key 使用 `object_id`，测试 partition 分布近似：

```text
p0 : p1 : p2 ≈ 1 : 2 : 4
```

67/s 时 hot partition 2 offered load 约：

```text
67 × 4 / 7 ≈ 38.3 events/s
```

高于当前该单 partition / 单 Worker 的持续消费能力，因此产生稳定 backlog。

恢复到 1/s 后：

```text
2803
→ 2223
→ 1638
→ 1053
→ 462
→ 0
```

约 97 秒全部 drain。

因此当前容量结论是：

```text
33 events/s production peak:
PASS

53 events/s short-duration safety headroom:
PASS

67 events/s:
RECOVERABLE OVERLOAD
NOT SUSTAINABLE under current key distribution
```

67/s 的主要限制是当前 Kafka key / partition skew 与单 partition 串行处理能力，不能简单解释为 GPU 总体饱和，也不能据此宣称系统硬件最大吞吐为 53/s。

### Load 测试边界

当前 mock 重复发送有限的一组确定性 `object_id / revision`。

因此上述测试验证：

```text
实时消息传输
Kafka producer / consumer
LightCurve HTTP
Triton inference
双 Worker 并行
Result Writer
PostgreSQL 幂等写路径
```

不等价于验证：

```text
同等速率的全新 ClassificationRun INSERT/s
```

长时间 Soak：

```text
DEFERRED / NOT EXECUTED
```

短时负载结果不应描述为长时间稳定性验证。

---

## Transactional Outbox

当前不实现：

```text
classification.updated
outbox_events
Transactional Outbox Publisher
```

并禁止：

```text
PostgreSQL commit
→ 直接 Kafka publish classification.updated
```

避免引入数据库提交成功但 Kafka 事件丢失的双写窗口。

状态：

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
| 独立服务器 PostgreSQL | `VERIFIED_SERVER` |
| Kafka Publisher / Consumer 基础 | `VERIFIED_CI` |
| Kafka SCRAM-SHA-256 | `VERIFIED_SERVER` |
| Kafka 真实 Broker | `VERIFIED_SERVER` |
| Kafka 单 Broker 故障恢复 | `VERIFIED_SERVER` |
| Candidate Orchestrator | `VERIFIED_CI / VERIFIED_SERVER` |
| LightCurveRevision Reader / Prepare | `VERIFIED_CI` |
| LightCurve HTTP Adapter | `VERIFIED_CI / VERIFIED_SERVER` |
| ClassificationInputPreparer | `VERIFIED_CI` |
| Serving Bundle Loader | `VERIFIED_CI` |
| Triton Client / Codec / Contract Gate | `VERIFIED_CI` |
| VariableStarClassifier Adapter | `VERIFIED_CI` |
| 真实 Triton | `VERIFIED_SERVER` |
| Classification Worker | `VERIFIED_CI / VERIFIED_SERVER` |
| 长期 RETRYABLE | `VERIFIED_CI / VERIFIED_SERVER` |
| Rebalance Yield / Fresh Consumer Session | `VERIFIED_CI / VERIFIED_SERVER` |
| Command DLQ / Offset | `VERIFIED_CI / VERIFIED_SERVER` |
| Classification Result Writer | `VERIFIED_CI / VERIFIED_SERVER` |
| Result Retry / Rebalance Yield | `VERIFIED_CI / VERIFIED_SERVER` |
| Result DLQ / Offset | `VERIFIED_CI / VERIFIED_SERVER` |
| Kafka + LightCurve + Triton + PostgreSQL 联合 E2E | `VERIFIED_SERVER` |
| Structured Logging | `VERIFIED_CI / VERIFIED_SERVER` |
| Management `/live` `/ready` `/metrics` | `VERIFIED_CI / VERIFIED_SERVER` |
| Kafka / HTTP / PostgreSQL Metrics | `VERIFIED_CI / VERIFIED_SERVER` |
| govulncheck | `VERIFIED_CI` |
| Gitleaks full-history scan | `VERIFIED_CI` |
| PostgreSQL PITR | `VERIFIED_SERVER` |
| PostgreSQL second-disk WAL mirror | `VERIFIED_SERVER` |
| PostgreSQL backup automation | `VERIFIED_SERVER` |
| Runbooks | `VERIFIED_CI` |
| 33 events/s production peak | `PASS / SERVER_VERIFIED` |
| 53 events/s safety headroom | `PASS / SERVER_VERIFIED` |
| 67 events/s upper boundary | `RECOVERABLE_OVERLOAD` |
| Long-duration Soak | `DEFERRED` |
| Transactional Outbox | `DEFERRED` |
| Model Artifact Recovery | `DEFERRED / DOCUMENTED RISK` |
| 正式科学批准 | `PENDING_FORMAL_BENCHMARK` |

状态含义：

```text
VERIFIED_CI
代码和自动化门禁已经由 GitHub Actions 验证

VERIFIED_SERVER
已经在独立服务器真实依赖环境验证

PASS / SERVER_VERIFIED
指定服务器验收场景已经明确通过

RECOVERABLE_OVERLOAD
正确性和恢复通过，但该负载不能持续零积压运行

DEFERRED
明确延后，不应描述为已验证

PENDING_FORMAL_BENCHMARK
工程链路允许继续，但正式科学批准尚未完成
```

---

## 当前工程结论

当前实时分类主链已经具备：

```text
固定 revision 输入
确定性 job_id / run_id
精确 Serving Bundle
真实 Triton
Kafka 至少一次语义
幂等 PostgreSQL 持久化
PERMANENT DLQ
长期 RETRYABLE
rebalance-safe consumer session recovery
结构化日志
Prometheus metrics
健康检查
安全 CI
数据库 PITR
备份自动化
Runbooks
服务器 E2E
服务器负载证据
```

当前工程状态：

```text
PRODUCTION_PREACCEPTANCE_CORE_COMPLETE
```

明确保留的后续工作和风险：

```text
Kubernetes 生产化
长时间 Soak
Model Artifact Recovery
更均匀的生产 Kafka key / partition 分布验证
全新 ClassificationRun 高速持续 INSERT 压力验证
Transactional Outbox（如后续确实需要 classification.updated）
正式科学 benchmark / approval
```
