# Kafka Consumer Lag and Stuck Runbook

本 Runbook 用于处理“变源候选体实时分类系统”中的 Kafka consumer lag 持续增长、partition 长时间不推进、consumer stuck、rebalance blocked，以及异常 consumer session 信号。

适用组件：

- `candidate-orchestrator`
- `classifier-worker`
- `classification-result-writer`

本 Runbook 基于当前已经实现并经过 CI / 服务器故障演练验证的消费、手动提交、长期 RETRYABLE、rebalance-yield 和 fresh consumer session 语义。

---

## 1. 适用范围

适用于以下现象：

- Kafka consumer group lag 持续增长；
- 某个 partition 长时间停留在同一个 offset；
- consumer 仍存活，但消息处理没有继续；
- classifier-worker 或 classification-result-writer 长时间 RETRYABLE；
- 新 consumer 加入后出现 rebalance blocked；
- consumer session 被 yield；
- 日志中反复出现 group / commit / rebalance 异常；
- `UNKNOWN_MEMBER_ID` 或 `service failed` 持续出现；
- broker 恢复后 consumer 仍无法继续。

不适用于：

- 单条 PERMANENT 消息按设计进入 DLQ；
- 已知上游完全停止产生消息，因此 lag 为 0 且没有新流量；
- 计划内服务停止；
- 仅仅因为某个 partition 当前没有消息而判断为 stuck。

---

## 2. 首先区分：Lag 不等于 Stuck

Kafka lag 增长可能是正常背压，也可能是异常停滞。

### 正常背压示例

```text
下游依赖暂时不可用
        ↓
当前 record RETRYABLE
        ↓
该 partition offset 暂时不提交
        ↓
lag 增长
        ↓
依赖恢复
        ↓
原 record 成功
        ↓
offset commit
        ↓
继续消费
```

这种情况下，lag 增长是系统为了“不丢消息”主动承担的结果。

### 真正的 Stuck

更需要关注的是：

```text
依赖已经恢复
+
consumer process 仍存活
+
当前 record 没有明确 RETRYABLE 原因
+
offset 长时间不推进
```

或者：

```text
反复 rebalance / member error
+
consumer session 无法稳定建立
```

处理时必须先判断属于哪一种。

---

## 3. 三类 Consumer 的安全边界

### candidate-orchestrator

负责：

```text
CandidateEvent
    ↓
ClassificationCommand
```

主要行为：

- Candidate 永久非法时可进入 Candidate DLQ；
- Command 发布成功后才允许当前 Candidate 消费继续完成；
- offset 由 ConsumerRunner 负责提交；
- 不应通过手工 offset reset 绕过当前未确认消息。

当前 candidate-orchestrator 不依赖 classifier-worker 的长期 RETRYABLE + rebalance-yield 专用 session 机制。

因此如果 orchestrator 长时间不消费，需要重点检查：

- Candidate handler error；
- Command publish error；
- Kafka broker / producer error；
- consumer group 状态；
- 进程是否已经退出或重启。

---

### classifier-worker

负责：

```text
ClassificationCommand
    ↓
LightCurve
    ↓
Triton
    ↓
ClassificationResult
```

关键安全语义：

- PERMANENT → Command DLQ；
- RETRYABLE → capped backoff 持续重试；
- RETRYABLE 期间当前 Command offset 不提交；
- rebalance 到来时可以 yield 当前处理；
- yield 后旧 consumer session 结束；
- fresh consumer session 重新加入 group；
- 未提交 record 必须能够被新 owner 重新读取。

classifier-worker 的 partition 暂停推进并不自动意味着 stuck。

如果它正在对同一 Command 做明确的 RETRYABLE，那么“offset 不前进”可能正是正确行为。

---

### classification-result-writer

负责：

```text
ClassificationResult
    ↓
PostgreSQL
```

关键安全语义：

- PostgreSQL transient failure 不应导致未完成 Result 被错误提交；
- Writer 对 transient persistence failure 持续 retry；
- rebalance 时使用安全的 yield / fresh consumer session 边界；
- PostgreSQL 恢复后应先完成原 Result，再继续后续 offset；
- 持久化成功后才允许消费进度安全向前。

因此 PostgreSQL 故障期间 Result lag 增长通常属于预期背压。

---

## 4. 关键观察对象

排查 lag / stuck 时至少同时观察四类信息。

### Consumer Group

需要知道：

```text
group
topic
partition
current committed offset
log end offset
lag
current member / assignment
```

如果使用 Kafka CLI，应使用已经配置好 SCRAM 凭据的安全 client properties 文件。

不要把 Kafka SASL 密码直接写在命令行或 Runbook 记录中。

示意：

```bash
kafka-consumer-groups.sh \
  --bootstrap-server <broker> \
  --command-config <client.properties> \
  --describe \
  --group <group>
```

### Process

确认服务是否仍然存活：

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

同时检查 management endpoint：

```text
/live
/ready
/metrics
```

### Structured Logs

重点保留：

```text
topic
partition
offset
job_id
object_id
error_class
error_code
worker_operation
```

以及：

```text
retry scheduled
retry wait cancelled
consumer session yielded
publish failed
commit failed
service failed
UNKNOWN_MEMBER_ID
```

### Metrics

优先观察：

- Kafka consume / produce 请求是否仍在发生；
- consumer-group management error；
-业务 retry 状态；
- HTTP dependency error；
- PostgreSQL write error；
- `astro_kafka_rebalance_callback_blocked_total`（当前暴露该指标的运行路径）。

不要只看 lag 一个指标下结论。

---

## 5. 判断流程

### Step 1：确认是否真的有 Lag

先确认：

```text
当前 committed offset
<
log end offset
```

并观察 lag 是否持续增长，而不是瞬时波动。

记录：

```text
group
topic
partition
committed offset
log end offset
lag
timestamp
```

至少连续观察两次。

---

### Step 2：定位到具体 Daemon

根据 topic / group 确定：

```text
Candidate
    → candidate-orchestrator

ClassificationCommand
    → classifier-worker

ClassificationResult
    → classification-result-writer
```

不要把三个 consumer 的恢复操作混在一起。

---

### Step 3：确认当前 Partition 是否正在处理一个未完成 Record

检查结构化日志中的：

```text
topic
partition
offset
```

如果持续看到同一个 offset，并伴随明确的 RETRYABLE：

```text
RETRYABLE
retry scheduled
retry age increasing
```

则优先判断为“依赖故障导致的安全背压”。

不要立即执行 offset reset。

---

### Step 4：确认依赖状态

对于 classifier-worker：

检查：

- LightCurve API；
- Triton `/v2/health/ready`；
- Triton HTTP request error；
- LightCurve HTTP request error。

对于 classification-result-writer：

检查：

- PostgreSQL container / service；
- PostgreSQL connectivity；
- PostgreSQL timeout / write error；
- pgxpool 状态。

对于 candidate-orchestrator：

检查：

- Kafka broker；
- ClassificationCommand publish；
- Candidate handler / DLQ publish。

---

### Step 5：检查 Rebalance

如果近期发生：

- 新实例加入；
- 实例退出；
- broker / network 波动；
- consumer session 重建；

检查是否出现：

```text
rebalance callback blocked
retry wait cancelled
consumer session yielded
```

对于支持 rebalance-yield 的 consumer，正确行为应为：

```text
正在处理 offset N
        ↓
rebalance requested
        ↓
cancel 当前 record context
        ↓
offset N 不提交
        ↓
AllowRebalance
        ↓
旧 session 结束
        ↓
创建 fresh consumer session
        ↓
新的 owner 从 N 重读
```

---

### Step 6：判断是否为异常 Stuck

以下组合更接近真正异常：

```text
依赖健康
+
process healthy
+
没有明确 RETRYABLE
+
没有正常 rebalance recovery
+
同一个 committed offset 长时间不变化
```

或者：

```text
持续 UNKNOWN_MEMBER_ID
```

或者：

```text
consumer session 反复失败
+
service failed
```

或者：

```text
record 已宣称成功
但 committed offset 不前进
```

这时需要升级为工程排查，而不是继续等待。

---

## 6. Rebalance Blocked 的正确理解

`BlockRebalanceOnPoll` 的作用不是“永远阻止 rebalance”。

它用于避免：

```text
正在处理 record
        ↓
partition 已经被 group 回收
        ↓
旧 handler 仍然继续处理
        ↓
错误 commit / 跨 generation 操作
```

classifier-worker 已实现 `RebalanceYield`。

当 callback 被阻塞时：

```text
OnPartitionsCallbackBlocked
        ↓
记录 rebalance blocked 指标
        ↓
请求 yield
        ↓
取消当前 record Context
```

随后 ConsumerRunner：

```text
不 commit 当前 record
        ↓
AllowRebalance
        ↓
返回 ErrRebalanceYielded
```

runtime 再：

```text
CloseAllowingRebalance
        ↓
销毁旧 consumer client
        ↓
创建 fresh consumer client
        ↓
重新加入 group
```

关键判断不是“有没有 rebalance blocked”。

关键是：

```text
blocked 后是否安全 yield
+
未提交 offset 是否被重新消费
```

---

## 7. 已验证的 Rebalance 边界

classifier-worker 双 Worker 服务器验收中已经验证：

```text
Worker A
partition 0 / offset 309
长期 RETRYABLE
```

随后 Worker B 加入 group：

```text
astro_kafka_rebalance_callback_blocked_total
0 → 1
```

Worker A：

```text
retry wait cancelled
consumer session yielded
```

旧 session 不再继续：

```text
309
→
310
```

而是结束。

之后 Worker B 获得 partition 0，并从：

```text
offset 309
```

重新读取。

依赖恢复后：

```text
309 成功
310 成功
311 成功
```

证明：

- 309 在 yield 前没有被错误提交；
- 新 owner 能够重新取得 309；
- 只有完成 309 后才继续更高 offset。

该行为属于正确的 rebalance recovery，不应被误判为消息重复故障。

---

## 8. PostgreSQL Failure 下的 Result Lag

classification-result-writer 的服务器故障实验已经确认：

```text
PostgreSQL network unavailable
        ↓
Result offset 保持未完成
        ↓
Writer capped retry
        ↓
process 保持运行
        ↓
PostgreSQL network restored
        ↓
同一 Result 成功持久化
        ↓
继续后续 offsets
```

因此 PostgreSQL outage 期间：

```text
ClassificationResult lag ↑
```

本身不是数据丢失证据。

正确的判断重点是：

```text
当前未完成 offset 是否保持未提交
```

以及恢复后：

```text
是否先完成原 offset
```

---

## 9. Kafka Broker Failure 下的 Lag

当前 Kafka 为：

```text
3 brokers
ReplicationFactor = 3
min.insync.replicas = 2
```

单 broker 短时故障服务器测试已经验证：

- leader 能迁移到剩余 broker；
- ISR 可降为 2；
- Candidate → Command → Result → PostgreSQL 链路可以继续；
- broker 恢复后 ISR 可恢复为 3。

所以发现 lag 时不能看到“一个 broker Down”就直接判断整个 Kafka 不可用。

先检查：

```text
topic partition 是否仍有 leader
ISR 是否仍满足写入要求
业务 produce / consume 是否仍成功
```

如果出现：

```text
Leader = -1
```

或大量 partition 无可用 leader，则属于更严重 Kafka 故障，需要升级处理。

---

## 10. 处理动作

### 场景 A：明确依赖 RETRYABLE

例如：

```text
Triton down
LightCurve down
PostgreSQL down
```

处理：

1. 恢复真正的依赖；
2. 保持 consumer offset 不动；
3. 观察原 record 自动恢复；
4. 确认 lag 开始下降。

不要通过 offset reset “解决 lag”。

---

### 场景 B：Rebalance 正在进行

如果看到：

```text
rebalance blocked
consumer session yielded
```

先等待 fresh consumer session 建立。

确认：

```text
new assignment
+
uncommitted offset re-read
```

不要同时频繁重启多个 Worker。

---

### 场景 C：Consumer Process 已退出

先检查退出原因。

如果是：

```text
SYSTEM / config / startup gate
```

修复配置或依赖后重新启动。

如果是运行期异常：

保留日志并确认 committed offset，再重启服务。

Kafka 应从 committed offset 继续，而不是从人为指定的更高 offset 开始。

---

### 场景 D：Lag 持续增长但消费者仍工作

如果：

```text
offset 持续前进
但生产速度 > 消费速度
```

这是 throughput / capacity 问题，不是 stuck。

需要进入容量分析：

- 当前输入速率；
- Worker 数量；
- Triton latency；
- LightCurve latency；
- PostgreSQL write latency；
- partition 分布；
- backlog oldest age。

不要通过跳消息降低 lag。

---

## 11. 禁止操作

禁止：

```text
kafka-consumer-groups --reset-offsets
```

除非已经完成独立事故评估，并明确知道要放弃或重放哪些业务记录。

正常 lag / stuck 处理过程中不要使用。

禁止：

```text
手工 commit 未完成 record
```

禁止：

```text
为了降低 lag 删除 Kafka 消息
```

禁止：

```text
清空 ClassificationCommand / ClassificationResult topic
```

禁止：

```text
把 transient failure 对应 record 强制放进 DLQ
```

禁止：

```text
在 rebalance 期间同时频繁 restart 多个 consumer
```

禁止将：

```text
重复读取未提交 offset
```

误判成数据重复故障。

Kafka at-least-once 风格处理下，未提交记录在故障/rebalance 后重新读取是预期行为。

---

## 12. 恢复确认

Lag / stuck 故障只有在以下条件同时满足时才能关闭。

### Kafka

确认：

```text
consumer group assignment 稳定
committed offset 持续推进
lag 停止增长
lag 开始下降或回到正常范围
```

### Consumer

确认：

```text
process healthy
/live 正常
没有持续 service failed
没有持续 UNKNOWN_MEMBER_ID
```

### 当前 Record

如果故障发生时有明确未完成 offset N：

必须确认：

```text
N 最终成功
```

然后才看到：

```text
N+1
N+2
```

### 下游结果

对于 Worker：

```text
ClassificationResult 恢复发布
```

对于 Writer：

```text
ClassificationRun / CurrentClassification 恢复持久化
```

对于 Orchestrator：

```text
ClassificationCommand 恢复发布
```

### DLQ

确认：

```text
没有因为 transient outage 出现异常 DLQ 增长
```

---

## 13. 升级条件

出现以下任一情况时，应升级为工程故障排查：

- 依赖已恢复但 lag 仍持续增加；
- consumer process healthy，但 committed offset 长时间完全不变化；
- `UNKNOWN_MEMBER_ID` 持续或反复出现；
- `service failed` 持续出现；
- rebalance-yield 后没有从未提交 offset 重新消费；
- 旧 consumer session 在 yield 后仍继续处理更高 offset；
- record 宣称处理成功但 commit 始终失败；
- 多个 partition 同时失去 leader；
- ISR 低于当前写入要求；
- broker 恢复后 consumer group 长时间无法稳定；
- transient dependency failure 导致 DLQ 异常增长；
- lag oldest age 已超过项目当前允许的实时处理边界。

---

## 14. 故障关闭记录建议

事故记录至少保存：

```text
incident start time
affected consumer group
affected topic / partitions
maximum observed lag
oldest affected offset
dependency status
rebalance occurred: yes/no
rebalance-yield occurred: yes/no
UNKNOWN_MEMBER_ID observed: yes/no
service failed observed: yes/no
dependency recovery time
original offset recovery evidence
lag recovery time
DLQ impact
```

禁止在事故记录中保存：

- Kafka SASL password；
- PostgreSQL password；
- Secret；
- 原始敏感 Kafka Value / Header。

---

## 15. 当前工程边界

当前系统没有：

- Retry Topic；
- Redis retry state；
- 分布式 retry database；
- 自动 Kafka offset reset；
- Kubernetes consumer autoscaling。

这些不是本 Runbook 的恢复手段。

当前策略优先保证：

```text
消息不丢
offset 不越过未完成任务
依赖恢复后自动继续
rebalance 后从 committed boundary 恢复
```

容量不足导致的持续 lag，应在 S8-08 Load / Soak / SLO 验收中处理，而不是通过破坏 offset 语义解决。
