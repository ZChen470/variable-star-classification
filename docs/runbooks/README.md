# Operations Runbooks

本目录用于记录“变源候选体实时分类系统”的生产预验收故障判断、恢复步骤与模型发布操作。

Runbook 只描述当前系统真实存在或已经明确冻结的运行机制，不引入新的业务语义、Retry Topic、状态机、Kubernetes 或其他未实施基础设施。

## 1. 使用原则

处理故障时优先回答以下问题：

1. 故障发生在哪一层：Candidate、Command、Worker、LightCurve、Triton、Result、Writer、PostgreSQL 或 Kafka。
2. 当前消息是否已经提交 Kafka offset。
3. 错误属于 RETRYABLE、PERMANENT 还是 CANCELLED。
4. 是否存在持续积压、重复处理或 DLQ 增长。
5. 依赖恢复后系统是否能够自动继续处理。
6. 是否需要人工介入，还是应保持现有 retry / replay 语义。
7. 恢复后如何确认没有丢失任务或破坏 ClassificationRun 幂等性。

禁止为了快速恢复而：

- 手工跳过未确认的 Kafka offset；
- 删除仍可能需要的 PostgreSQL WAL；
- 修改 ClassificationResult 或 ClassificationRun 身份；
- 绕过 Triton Serving Contract；
- 把 RETRYABLE 错误直接改成 PERMANENT；
- 清空 DLQ 而不保留原因和证据；
- 在正在运行的 Triton model repository 中直接覆盖模型文件。

## 2. Runbook 标准结构

每份详细 Runbook 应尽量包含：

1. 现象
2. 关键指标
3. 关键日志字段
4. 判断步骤
5. 处理动作
6. 升级条件
7. 恢复确认

命令不得输出 Secret、数据库密码、Kafka SASL 密码或原始敏感 Header。

## 3. 故障覆盖矩阵

| 故障场景                                 | 主要组件                                  | 当前工程验证状态            | 详细 Runbook   |
| ---------------------------------------- | ----------------------------------------- | --------------------------- | -------------- |
| Kafka consumer lag 持续增长              | Orchestrator / Worker / Writer            | PARTIALLY_VERIFIED_SERVER   | TODO           |
| Candidate DLQ 增长                       | candidate-orchestrator                    | VERIFIED_CI                 | TODO           |
| Command DLQ 增长                         | classifier-worker                         | VERIFIED_CI                 | TODO           |
| Result DLQ 增长                          | classification-result-writer              | VERIFIED_CI                 | TODO           |
| Triton unavailable                       | classifier-worker / Triton                | SERVER_VERIFIED             | TODO           |
| LightCurve API unavailable               | classifier-worker / LightCurve            | SERVER_VERIFIED             | TODO           |
| PostgreSQL unavailable                   | classification-result-writer / PostgreSQL | SERVER_VERIFIED             | TODO           |
| PostgreSQL lock / slow query             | Writer / PostgreSQL                       | NOT_YET_FAULT_INJECTED      | TODO           |
| 服务频繁重启                             | 三个核心 daemon                           | PARTIALLY_VERIFIED_SERVER   | TODO           |
| Triton contract mismatch                 | classifier-worker / Triton                | VERIFIED_CI_AND_SERVER_GATE | TODO           |
| Manifest mismatch                        | classifier-worker / Model Bundle          | VERIFIED_CI                 | TODO           |
| 模型不可用                               | Triton / Model Repository                 | DEFERRED                    | 本文第 7、8 节 |
| 模型版本升级 / 回滚                      | Triton / Model Repository                 | PROCEDURE_DEFINED           | 本文第 8 节    |
| 磁盘空间不足                             | PostgreSQL / backup / model artifacts     | NOT_YET_FAULT_INJECTED      | TODO           |
| PostgreSQL 数据库恢复                    | PostgreSQL                                | SERVER_VERIFIED             | TODO           |
| Kafka consumer stuck / rebalance blocked | classifier-worker / result-writer         | SERVER_VERIFIED             | TODO           |

## 4. 已验证的恢复能力

### 4.1 Kafka

当前服务器环境已验证：

- 3 broker、ReplicationFactor=3、min.insync.replicas=2；
- 单 broker 短时停止后 leader election 正常；
- broker 缺失期间业务链路仍可继续；
- broker 恢复后 ISR 可恢复到 3；
- classifier-worker 长期 RETRYABLE + rebalance 时不会错误提交未完成消息；
- fresh consumer session 可以从 committed offset 恢复处理。

### 4.2 Triton / LightCurve

当前服务器环境已验证：

- Triton 短时不可用时 classifier-worker 保持运行；
- RETRYABLE 使用 capped backoff 持续重试；
- Triton 恢复后无需重启 Worker 即可继续处理同一 Command；
- LightCurve 暂时不可用时使用相同的 retry / rebalance 安全语义；
- 固定 revision 404 按永久不存在处理，不使用旧的 record-level 409 NotReady 设计。

### 4.3 PostgreSQL

当前服务器环境已验证：

- classification-result-writer 在 PostgreSQL 短时不可用时不会错误提交未完成 Result；
- PostgreSQL 恢复后可以继续持久化；
- physical base backup 已通过 `pg_verifybackup`；
- WAL continuous archive 已启用；
- PITR 已实际恢复到指定时间点；
- 第二盘 WAL mirror 的受控 RPO 实测低于 5 分钟；
- 每日 verified physical base backup 已自动调度；
- retention 保留最近 7 份成功 base backup，并按最老保留 backup 的 START WAL 计算 WAL 安全边界。

## 5. PostgreSQL Recovery 当前边界

当前恢复方案覆盖：

- 数据库逻辑错误；
- 指定时间点 PITR；
- `/dev/sdb` 单盘损坏情况下，从 `/dev/sda` 的 PostgreSQL 恢复副本恢复数据。

当前不覆盖：

- 整台服务器损坏；
- RAID 控制器整体故障；
- 机箱级故障；
- 机房级故障；
- 异地灾难恢复。

因此当前 PostgreSQL 第二盘副本只能定义为**同机异盘恢复能力**，不能定义为异地灾备。

## 6. Triton 当前部署基线

当前 Triton 运行形态：

```
container:
  variable-star-triton-final-timeorder-7bd80afc87bf

image:
  variable-star-triton:26.03-py312-v1

model_repository_inside_container:
  /models

host_model_repository:
  /home/zhouyuyang/variable-star-deploy/releases/
  b4e811dc99cb-final-d4f0cd7f98bd-timeorder-7bd80afc87bf/
  model_repository

mount:
  bind
  read_only: true

storage_device:
  /dev/sdb2
```

当前 model repository 包含：

```
transformer_multihead
variable_star_classifier
xgb_coarse
xgb_feature_extractor
```

关键结论：

- 模型制品**不在 Triton 镜像内部**；
- Triton 镜像和模型 repository 是分离的；
- 当前 `/models` 来自宿主机只读 bind mount；
- Triton 容器销毁后，只要相同 image 和 model repository 仍存在，就可以重新创建服务；
- 当前 model repository 位于 `/dev/sdb2`，模型灾备仍存在单盘风险。

当前生产 Triton 未显式配置 `--model-control-mode`，因此使用 Triton 默认的 `none` 模式：

- 模型在 Triton 启动时加载；
- 运行期间不通过 Model Repository API 动态 load / unload；
- 对本项目当前发布频率而言，优先采用 immutable model release + Triton recreate，而不是生产热更新。

## 7. Model Artifact Recovery

状态：

```
status: DEFERRED
```

当前已确认：

- Triton model repository 位于宿主机 `/dev/sdb2`；
- Triton 通过只读 bind mount 使用该 repository；
- model repository 不包含在 Go Repository 中；
- 当前 model repository 约 582 MiB；
- 当前未建立 `/dev/sda` 第二副本或独立外部模型制品恢复源。

当前风险：

如果 `/dev/sdb2` 整盘损坏，同时又无法从其他外部来源重新取得完全相同的 model repository，则当前 Triton 模型服务无法仅依赖现有容器镜像完成恢复。

未来恢复能力验证成功标准：

1. 存在独立于 `/dev/sdb2` 的完整 model repository；
2. 文件摘要或 manifest 校验通过；
3. 能从该恢复源以只读方式启动隔离 Triton；
4. `/v2/health/ready` 成功；
5. 必需模型全部加载；
6. Serving Contract gate 通过。

## 8. Triton 模型版本升级与回滚

### 8.1 适用场景

例如：

- 对 `transformer_multihead` 完成新的后训练；
- 新 Transformer 在离线科学评估中性能优于当前版本；
- 导出新的 ONNX；
- 希望将新模型上线到现有 Triton 分类服务。

### 8.2 核心原则

**不要直接覆盖正在运行 release 中的** `**model.onnx**`**。**

生产模型发布应采用：

```
训练新模型
    ↓
导出新 ONNX
    ↓
创建新的 immutable model_repository release
    ↓
隔离 Triton 验证
    ↓
Serving Contract / Golden / 科学指标验收
    ↓
切换正式 Triton 到新 release
    ↓
验证 ready 和业务恢复
```

旧 release 在新版本稳定前继续保留，用于快速回滚。

### 8.3 推荐 repository 版本方式

Triton model repository 原生支持模型版本目录，例如：

```
model_repository/
└── transformer_multihead/
    ├── config.pbtxt
    ├── 1/
    │   └── model.onnx
    └── 2/
        └── model.onnx
```

但对本项目更推荐以**完整 release**作为运维发布单元，例如：

```
releases/
├── release-A/
│   └── model_repository/
│       ├── transformer_multihead/
│       ├── variable_star_classifier/
│       ├── xgb_coarse/
│       └── xgb_feature_extractor/
│
└── release-B/
    └── model_repository/
        ├── transformer_multihead/
        ├── variable_star_classifier/
        ├── xgb_coarse/
        └── xgb_feature_extractor/
```

原因：

- Transformer 并不是独立于整个推理契约存在；
- `variable_star_classifier`、XGBoost、Python backend 和 `config.pbtxt` 之间存在版本关系；
- 完整 release 更容易审计、验证和回滚；
- 不需要在故障时临时拼装多个模型版本。

### 8.4 推荐发布流程

#### Step 1：科学侧完成新模型

完成：

- 新 Transformer 训练；
- 独立验证集评估；
- 导出 ONNX；
- 明确新模型版本；
- 保存训练和评估证据。

#### Step 2：建立新 immutable release

复制当前有效 model repository 为一个新的 release，然后只在新 release 中替换目标模型。

例如：

```
release-old
    ↓ copy
release-new
    ↓
replace transformer_multihead/<new-version>/model.onnx
```

不得修改当前正在运行的旧 release。

#### Step 3：隔离 Triton 验证

使用新 release 启动一个不接生产流量的临时 Triton。

至少验证：

```
/v2/health/live
/v2/health/ready
model metadata
model config
Serving Contract
```

如果是 Transformer 更新，还应执行固定科学样本 / Golden inference，比对：

- 输出 shape；
- 输出类别顺序；
- 概率输出是否合法；
- 分类结果是否符合预期；
- 新模型科学指标是否达到发布要求。

#### Step 4：正式切换

当前系统推荐采用：

```
old Triton
    ↓ stop / recreate
new Triton + new immutable release
    ↓
ready PASS
    ↓
classifier-worker 自动恢复
```

Triton 的短时间不可用已经被 classifier-worker 的 RETRYABLE 机制覆盖：

- Worker 不应因为 Triton 短时不可用而丢弃 Command；
- 未完成 Command 不提交 offset；
- Triton 恢复后继续处理；
- 不需要为了模型切换专门清空 Kafka。

#### Step 5：上线确认

正式 Triton 启动后确认：

```
Triton /ready = PASS
Serving Contract gate = PASS
classifier-worker 无持续 RETRYABLE
Command backlog 开始下降 / 保持正常
ClassificationResult 正常发布
classification-result-writer 正常持久化
DLQ 无异常增长
```

#### Step 6：观察期

新版本上线后保留旧 release。

不要立即删除旧模型制品。

观察重点：

- Triton HTTP error；
- Worker retry；
- Kafka Command lag；
- 推理输出异常；
- 科学抽样结果；
- Result / PostgreSQL 写入是否正常。

### 8.5 回滚流程

如果新模型出现以下问题：

- Triton 无法加载；
- `/ready` 长时间失败；
- Serving Contract 不通过；
- Golden inference 不通过；
- 科学结果明显异常；
- 推理错误率异常；
- 性能下降到不可接受水平；

应回滚到上一 immutable release。

推荐：

```
停止当前 Triton
    ↓
重新指向 previous model_repository release
    ↓
重新创建 / 启动 Triton
    ↓
/ready PASS
    ↓
Serving Contract PASS
    ↓
Worker 自动继续处理
```

回滚不需要：

- 修改 Kafka offset；
- 删除 Command；
- 清空 DLQ；
- 修改 ClassificationRun；
- 修改 PostgreSQL CurrentClassification。

### 8.6 当前为什么不采用 POLL

Triton 支持 `poll` 模式定期检测 model repository 变化并加载模型。

当前项目不推荐生产使用 POLL，主要因为：

- 运维复制大 ONNX / XGBoost 文件期间，repository 可能处于中间状态；
- Triton poll 与文件发布操作之间需要额外同步机制；
- 当前模型发布频率不高，没有必要引入这类复杂性；
- immutable release + recreate 更容易验证和回滚。

### 8.7 当前为什么不采用 EXPLICIT

Triton 也支持 `explicit` model control mode，通过 Model Repository API 显式 load / unload 模型，例如：

```
POST /v2/repository/index
POST /v2/repository/models/<model-name>/load
POST /v2/repository/models/<model-name>/unload
```

这适合未来模型发布频率明显提高、需要无服务器重启模型更新时使用。

当前阶段不启用，原因：

- 当前发布频率预计较低；
- 需要额外控制 Model Repository API 的访问权限；
- 需要增加 load / unload、健康检查、Golden 验证、失败回滚的自动编排；
- 当前 restart / recreate 的短暂 Triton unavailable 已被 Kafka + Worker retry 机制安全覆盖；
- 对现阶段而言，收益不足以抵消运维复杂度。

未来如果需要升级，可演进为：

```
Model Registry / Release
        ↓
发布新 immutable repository
        ↓
EXPLICIT load / reload
        ↓
ready + contract + golden validation
        ↓
切换
        ↓
失败自动 rollback
```

### 8.8 模型发布故障判断

#### 现象

- Triton `/ready` 失败；
- Worker 持续 `DEPENDENCY_UNAVAILABLE / RETRYABLE / classify`；
- 新模型 metadata/config 与预期不一致；
- Golden inference 失败；
- Command lag 持续增长。

#### 判断步骤

1. 确认当前 Triton 实际挂载的 model repository。
2. 确认 release ID 是否为预期版本。
3. 检查 Triton startup log。
4. 检查四个模型是否全部加载。
5. 检查 model metadata/config。
6. 执行 Serving Contract gate。
7. 执行固定科学样本 inference。
8. 判断是模型文件、config、backend、模型依赖还是科学性能问题。

#### 处理动作

如果无法快速确认新 release 正常：

**优先回滚旧 release，不在生产目录内现场修改模型。**

#### 恢复确认

至少确认：

```
Triton ready = PASS
Serving Contract = PASS
Worker retrying = 0 或持续下降至 0
Kafka backlog 恢复
Result 正常发布
PostgreSQL 正常持久化
无异常 DLQ 增长
```

## 9. 状态定义

- `SERVER_VERIFIED`：真实服务器环境已完成对应验证。
- `VERIFIED_CI`：代码和自动测试已在 CI 验证，但不代表真实服务器故障已演练。
- `VERIFIED_CI_AND_SERVER_GATE`：CI 契约测试已验证，并在真实服务器启动路径执行契约门禁。
- `PARTIALLY_VERIFIED_SERVER`：相关路径已经真实验证，但该场景尚未作为独立 Runbook 全量验收。
- `PROCEDURE_DEFINED`：操作流程已经冻结，但尚未对新模型真实执行一次完整发布/回滚演练。
- `NOT_YET_FAULT_INJECTED`：实现可能已有观测能力，但尚未执行该故障注入。
- `DEFERRED`：明确不在当前阶段继续实施，并记录当前风险和未来成功标准。

## 10. 后续详细 Runbook

后续按实际运行风险优先级补充：

```
classifier-worker-dependency-unavailable.md
kafka-consumer-lag-and-stuck.md
dlq-growth.md
postgresql-unavailable-and-slow.md
postgresql-recovery.md
contract-and-manifest-mismatch.md
disk-space.md
service-restart-loop.md
```

Runbook 应保持短、可执行，并优先使用已经经过服务器故障演练验证的处理步骤。

## 11. Triton 模型管理技术说明

当前 Triton 的模型管理策略为默认 `none` 模式。

Triton 还提供：

- `none`：启动时加载模型，运行期间不通过管理 API 动态 load/unload；
- `poll`：定期检测 model repository 的变化；
- `explicit`：通过 Model Repository API 显式 load/unload。

当前项目推荐继续使用：

```
none
+
immutable model_repository release
+
Triton recreate
+
readiness / contract / golden validation
+
previous release rollback
```

当未来模型发布频率或零停机要求明显提高时，再评估 `explicit` model control mode。

技术依据：NVIDIA Triton Inference Server 官方 Model Management 与 Model Repository Extension 文档。