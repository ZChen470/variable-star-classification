# DLQ Growth Runbook

本 Runbook 用于处理“变源候选体实时分类系统”中的 Candidate DLQ、Command DLQ、Result DLQ 持续增长或突然增长问题。

适用组件：

- `candidate-orchestrator`
- `classifier-worker`
- `classification-result-writer`

当前系统没有 Retry Topic。DLQ 只用于已经被判定为永久错误、且继续重试没有意义的消息或结果处理场景。

---

## 1. 三类 DLQ 的职责

### Candidate DLQ

处理 CandidateEvent 入站阶段的永久非法消息，例如：

- Proto 解码失败；
- topic / key 不满足契约；
- 必填字段非法；
- revision / epoch / timestamp 不合法；
- `RETRACTED`；
- `UNSPECIFIED`；
- 未知事件类型。

当前系统不支持 RETRACTED，因此它属于永久非法事件，不生成 ClassificationCommand。

Candidate DLQ 成功后，原 Candidate offset 才允许提交。

如果 Candidate DLQ 发布失败：

```text
原 Candidate offset 不提交
```

---

### Command DLQ

处理 `classifier-worker` 已确认的 PERMANENT Command / Worker 错误。

典型永久错误包括：

- ClassificationCommand 本身非法；
- JobID 与确定性身份不一致；
- 固定 LightCurve revision 明确不存在；
- 固定 revision 数据永久不一致；
- 其他已经被 WorkerError 分类为 PERMANENT 的错误。

不应该进入 Command DLQ：

```text
Triton 暂时 unavailable
LightCurve network / timeout / 5xx
其他 RETRYABLE dependency failure
CANCELLED
```

这些情况应保持 retry / no-commit 语义。

Command DLQ 发布成功后，原 Command 才允许被视为已处理。

如果 Command DLQ 发布失败：

```text
原 Command offset 不提交
```

并应继续按可恢复错误处理，而不是继续执行后续 offset。

---

### Result DLQ

处理 `classification-result-writer` 侧的永久 Result 问题。

当前已实现的典型场景包括：

- ClassificationResult 解码永久失败；
- ClassificationRun / Repository 身份冲突等永久持久化语义错误。

不应该进入 Result DLQ：

```text
PostgreSQL 临时 unavailable
PostgreSQL timeout
暂时网络失败
Context cancellation
```

这些 transient failure 应保持 retry / no-commit。

Result DLQ 发布成功后，原 Result offset 才允许提交。

如果 Result DLQ 发布失败：

```text
原 Result offset 不提交
```

---

## 2. 看到 DLQ 增长时不要先做什么

不要先：

```text
清空 DLQ
```

不要先：

```text
把 DLQ 消息重新全部灌回原 topic
```

不要先：

```text
手工修改 consumer group offset
```

不要因为 DLQ 数量增长就立即认定 Kafka 有问题。

DLQ 的存在通常意味着：

```text
业务消息已经被明确判定为永久不可处理
```

因此第一目标是回答：

```text
为什么被判定为 PERMANENT？
```

而不是：

```text
怎么最快把 DLQ 清零？
```

---

## 3. 首先判断是哪一种 DLQ

先确认：

```text
Candidate DLQ
Command DLQ
Result DLQ
```

三者对应的责任边界完全不同。

### Candidate DLQ 增长

优先检查：

- 上游 Candidate producer 是否发布了违反契约的数据；
- event type 是否异常；
- object_id / key 是否一致；
- revision / eligible epoch / timestamp 是否非法；
- 是否出现 RETRACTED。

### Command DLQ 增长

优先检查：

- ClassificationCommand 是否被错误构造或篡改；
- deterministic JobID 是否匹配；
- fixed LightCurve revision 是否真实存在；
- LightCurve 是否返回永久不一致数据；
- WorkerError 是否确实为 PERMANENT；
- 是否把 transient dependency failure 错误分类成 PERMANENT。

### Result DLQ 增长

优先检查：

- Result Proto 是否合法；
- Result identity 是否与持久化约束冲突；
- 是否存在不应该出现的 ClassificationRun 身份冲突；
- 是否错误地把 PostgreSQL transient failure 送入 DLQ。

---

## 4. 关键日志字段

排查时优先保留稳定字段。

### Candidate DLQ

关注：

```text
topic
partition
offset
error_code
original_topic
original_partition
original_offset
```

### Command DLQ

关注：

```text
job_id
object_id
light_curve_revision
topic
partition
offset
error_code
error_class
worker_operation
```

### Result DLQ

关注：

```text
run_id
job_id
object_id
light_curve_revision
topic
partition
offset
error_code
```

不要以完整错误文本作为自动业务分类依据。

不要在事故记录中复制：

- Kafka SASL password；
- PostgreSQL password；
- Secret；
- 原始敏感 Header；
- 不必要的完整 Kafka Value。

---

## 5. Candidate DLQ 排查

### 现象

常见表现：

```text
Candidate DLQ produce 持续增加
```

或 candidate-orchestrator 日志反复出现永久 Candidate validation error。

### 判断步骤

1. 找到最近一条进入 Candidate DLQ 的原始 topic / partition / offset。
2. 确认稳定 `error_code`。
3. 判断是否为单个坏消息，还是某一类消息持续出现。
4. 检查上游 producer 是否发生版本或契约变化。
5. 对照 CandidateEvent Proto / 当前阶段冻结规则。
6. 如果是 RETRACTED，确认它仍属于当前系统明确不支持的事件，而不是临时错误。
7. 确认 DLQ publish 是否成功。
8. 只有 DLQ 成功后，原 offset 才应继续推进。

### 高风险信号

如果大量合法 CREATED / UPDATED 都进入 Candidate DLQ：

```text
可能是上游契约变化
或 orchestrator decoder / validation 回归
```

这时不应把它当成“正常脏数据”。

---

## 6. Command DLQ 排查

### 现象

常见表现：

```text
Command DLQ 增长
classifier-worker 仍运行
某些 Command 被永久拒绝
```

### 第一判断：是否真的应该 PERMANENT

先确认日志中的：

```text
error_class=PERMANENT
```

如果看到的是：

```text
error_class=RETRYABLE
```

却同时产生 Command DLQ，应视为严重语义异常。

当前系统已经冻结：

```text
RETRYABLE
→ retry
→ no commit

PERMANENT
→ Command DLQ
→ DLQ success 后允许 commit

CANCELLED
→ no DLQ
→ no commit
```

### 常见永久错误

可能包括：

```text
invalid command
deterministic job_id mismatch
fixed revision not found
fixed revision permanently inconsistent
```

固定 revision 404 当前按永久不存在处理。

不要再使用旧的 record-level 409 NotReady 语义。

### Triton / LightCurve outage

如果 Command DLQ 增长同时伴随：

```text
Triton unavailable
LightCurve timeout
LightCurve 5xx
```

应重点检查是否发生错误分类。

这些情况正常应是：

```text
DEPENDENCY_UNAVAILABLE
RETRYABLE
```

而不是 Command DLQ。

---

## 7. Result DLQ 排查

### 现象

常见表现：

```text
Result DLQ 增长
classification-result-writer 仍运行
部分 Result 无法持久化
```

### 判断步骤

1. 确认 Result 是否能正常解码。
2. 检查 `run_id / job_id / object_id / revision` 等身份字段。
3. 检查 Repository 是否报告身份冲突。
4. 区分永久 identity conflict 和 PostgreSQL transient failure。
5. 检查 Result DLQ publish 是否成功。
6. 确认只有 DLQ 成功后原 Result offset 才推进。

### PostgreSQL unavailable

如果 PostgreSQL 暂时不可用：

```text
不应该进入 Result DLQ
```

正确行为是：

```text
Result persistence RETRYABLE
        ↓
当前 Result offset 不提交
        ↓
持续 retry
        ↓
PostgreSQL 恢复
        ↓
原 Result 持久化
        ↓
offset commit
```

如果 PostgreSQL outage 期间 Result DLQ 大量增长，应升级排查。

---

## 8. 判断是“单条坏消息”还是“系统性增长”

### 单条或少量坏消息

特点：

```text
DLQ 少量增长
error_code 稳定
来源集中在少数 offset/object
随后主链继续正常
```

这种情况通常可以保留 DLQ 证据并继续观察。

### 系统性增长

特点：

```text
DLQ 连续增长
多个 partition 同时出现
同一 error_code 大量出现
主链成功率显著下降
```

这通常意味着：

- producer 契约发生变化；
- deployment / config 版本不匹配；
- decoder / validator 回归；
- Model Bundle / Serving Contract 配置错误；
- 错误分类逻辑异常；
- 上游固定 revision 数据批量异常。

系统性 DLQ 增长应优先止住新的坏数据来源，而不是先重放 DLQ。

---

## 9. Offset 安全边界

三类 DLQ 都遵守同一个核心原则：

```text
永久错误
        ↓
构造 DLQ
        ↓
DLQ publish success
        ↓
原消息可以完成
        ↓
原 offset 可以提交
```

如果：

```text
DLQ publish failed
```

则：

```text
原 offset 不提交
```

不能因为“原消息本身已经确定永久错误”就绕过 DLQ publish failure。

否则会出现：

```text
原消息既没有成功处理
也没有可靠进入 DLQ
但 offset 已经推进
```

这相当于消息丢失。

---

## 10. DLQ Publish Failure

### 现象

可能看到：

```text
DLQ build failed
DLQ publish failed
```

### 判断

检查：

- Kafka broker 是否正常；
- DLQ topic 是否存在；
- producer 是否能够写入；
- SASL / network 是否正常；
- 是否只有某一个 DLQ topic 失败。

### 处理

恢复 Kafka / DLQ publish 能力。

不要：

```text
手工 commit 原 offset
```

恢复后让原 record 再次按照正常 handler 语义处理。

---

## 11. 是否可以重放 DLQ

默认答案：

```text
不能直接批量重放
```

重放前至少回答：

1. 原始永久错误原因是否已经被修复？
2. 修复发生在 producer、数据、配置、代码还是契约？
3. 原消息重新进入主 topic 后，是否真的会变成合法消息？
4. deterministic identity 是否仍然成立？
5. 是否可能产生重复 ClassificationCommand / ClassificationResult？
6. 下游幂等边界是否能够承受此次 replay？
7. 是否已经保留原 DLQ 证据？

### 不应重放的例子

例如当前系统明确不支持的：

```text
CandidateEvent RETRACTED
```

如果业务语义没有改变，把它重新灌回 Candidate topic 只会再次进入 DLQ。

### 可能考虑重放的例子

如果确认：

```text
producer 曾经错误编码
现在已经修复
并且 DLQ 中消息经过明确转换后符合新契约
```

则可以单独设计受控 replay。

但这不属于日常 DLQ 清理动作。

当前系统没有通用自动 DLQ replay 工具，因此不得假设存在“一键重放”。

---

## 12. DLQ 与重复处理

DLQ 不等价于“从未处理过”。

例如：

```text
PERMANENT
→ DLQ success
→ original offset commit
```

此时该原消息已经按照系统定义完成处理。

如果再手工把 DLQ 原样写回原 topic，会形成新的消费事件。

所以重放前必须明确：

```text
这是重新处理
而不是恢复一个未提交 record
```

不要把 Kafka 对未提交 offset 的正常重读和 DLQ replay 混为一谈。

---

## 13. 恢复确认

DLQ 增长问题解决后，至少确认：

### DLQ

```text
新增速率恢复正常
不再持续增长
```

### 主链

Candidate 路径：

```text
Candidate
→ ClassificationCommand
```

恢复正常。

Worker 路径：

```text
ClassificationCommand
→ ClassificationResult
```

恢复正常。

Writer 路径：

```text
ClassificationResult
→ PostgreSQL
```

恢复正常。

### Offset

确认：

```text
DLQ success 后原 offset 正常推进
DLQ failure 时没有错误推进
```

### Error Classification

确认 transient failure 没有被错误送入 DLQ：

```text
Triton unavailable
LightCurve transient unavailable
PostgreSQL transient unavailable
```

这些都不应该形成持续 DLQ。

---

## 14. 升级条件

满足以下任一条件时，应升级工程排查：

- DLQ 突然持续高速增长；
- 多个 partition 同时产生相同 PERMANENT error；
- 正常 CREATED / UPDATED Candidate 大量进入 Candidate DLQ；
- RETRYABLE Worker error 出现在 Command DLQ；
- PostgreSQL transient failure 导致 Result DLQ 增长；
- DLQ publish failure 持续出现；
- DLQ topic 本身不可写；
- DLQ 成功但原 offset 不推进；
- DLQ 失败但原 offset 却推进；
- 新 deployment 后 DLQ error_code 分布明显改变；
- contract / manifest 变更后出现集中永久错误。

---

## 15. 故障记录建议

事故记录至少保存：

```text
incident start time
DLQ type
DLQ topic
affected source topic
affected partitions
first observed offset
error_code distribution
error_class distribution
DLQ publish success/failure
main chain impact
consumer lag impact
deployment/config change around incident
root cause
replay required: yes/no
recovery time
```

不要保存：

- Kafka SASL password；
- PostgreSQL password；
- Secret；
- 不必要的完整业务 payload；
- 敏感 Kafka Header。

---

## 16. 当前工程边界

当前系统：

```text
有 Candidate DLQ
有 Command DLQ
有 Result DLQ
没有 Retry Topic
没有自动 DLQ replay service
没有分布式 retry store
```

因此当前 DLQ 运维原则是：

```text
先保留证据
        ↓
确认永久错误原因
        ↓
修复根因
        ↓
必要时单独设计受控 replay
```

而不是：

```text
DLQ 增长
        ↓
直接清空
或全部回灌
```

DLQ 是故障隔离和审计边界，不是普通消息缓存。
