# Classifier Worker Dependency Unavailable Runbook

本 Runbook 用于处理 `classifier-worker` 的外部推理依赖暂时不可用场景，当前主要包括：

- Triton Inference Server unavailable；
- LightCurve fixed-revision HTTP API unavailable。

这些场景已经在真实服务器环境完成故障恢复验证。

## 1. 适用范围

适用于以下症状：

- `classifier-worker` 持续报告 RETRYABLE；
- Worker 长时间停留在同一 ClassificationCommand；
- Triton HTTP 请求失败或 Triton `/ready` 不可用；
- LightCurve HTTP 请求失败；
- ClassificationResult 暂时停止产生；
- ClassificationCommand consumer lag 开始增长；
- 新 Worker 加入 group 时出现 rebalance，并触发 rebalance-yield。

不适用于：

- ClassificationCommand 永久非法；
- JobID 不匹配；
- Proto / enum / field validation 失败；
- 明确的 Model Bundle / Serving Contract 配置错误；
- 应进入 Command DLQ 的 PERMANENT 错误。

## 2. 预期系统行为

依赖暂时不可用时，正确行为应为：

```
ClassificationCommand
        ↓
classifier-worker
        ↓
dependency unavailable
        ↓
RETRYABLE
        ↓
capped retry
        ↓
dependency recovered
        ↓
same Command succeeds
        ↓
ClassificationResult published
        ↓
Command offset committed
```

当前 RETRYABLE 机制采用 capped backoff，等待时间逐步增加，最大约 10 秒，并持续重试直到：

- 成功；
- Context 被取消；
- consumer session 因 rebalance-yield 结束。

依赖不可用期间：

- 不应进入 Command DLQ；
- 不应提交当前未完成 Command 的 offset；
- 不应手工跳过当前 offset。

## 3. 现象

### Triton unavailable

结构化日志中可能出现：

```
error_class=RETRYABLE
worker_operation=classify
error_code=DEPENDENCY_UNAVAILABLE
```

同时可能看到：

```
retry scheduled
retry exhausted
```

对于当前无限 capped retry 运行方式，`retry exhausted` 表示一轮快速 retry 周期已经结束并继续进入下一轮处理语义，不代表 Command 可以丢弃。

### LightCurve unavailable

结构化日志中通常表现为 LightCurve 读取阶段的 RETRYABLE failure。

分类不会继续进入 Triton。

### Kafka 侧

如果依赖持续不可用：

```
ClassificationCommand lag ↑
当前 partition 的消费进度停止
```

这是预期的背压行为。

系统优先保证：

```
不丢 Command
>
暂时降低吞吐
```

## 4. 关键指标

优先检查 classifier-worker 的 management `/metrics`。

重点关注：

```
astro_classification_command_retry_attempts_total
astro_classification_command_retry_exhausted_total
astro_classification_command_retrying
astro_classification_command_retry_age_seconds
astro_kafka_rebalance_callback_blocked_total
```

同时观察：

- ClassificationCommand consumer lag；
- Kafka consume / produce errors；
- Triton HTTP 请求状态；
- LightCurve HTTP 请求状态；
- ClassificationResult produce 是否停止；
- Worker process `/live`；
- Worker `/ready`。

注意：

`/ready` 主要表示进程启动和关键启动依赖门禁状态。

运行过程中某个上游短暂不可用，不应简单等价为“Worker 必须退出”。

## 5. 关键日志字段

排查时优先保留以下关联字段：

```
job_id
object_id
light_curve_revision
model_bundle_version
topic
partition
offset
error_code
error_class
worker_operation
```

还应关注：

```
retry scheduled
retry wait cancelled
consumer session yielded
ClassificationResult published
```

不要依赖完整错误文本进行业务分类。

不要在排障记录中复制：

- Kafka SASL password；
- 数据库密码；
- 原始敏感 Kafka Header；
- 完整原始 Kafka Value。

## 6. 判断步骤

### Step 1：确认 Worker 是否仍存活

确认 classifier-worker 容器/进程仍在运行。

例如：

```
docker ps --format 'table {{.Names}}\t{{.Status}}' | grep -i classifier
```

如果进程持续运行且 `/live` 正常，优先继续判断依赖状态，不要立即重启 Worker。

### Step 2：确认失败 Operation

检查结构化日志。

例如：

```
docker logs --since 10m <classifier-worker-container> 2>&1 \
  | grep -E 'RETRYABLE|DEPENDENCY_UNAVAILABLE|retry|worker_operation'
```

重点判断：

```
worker_operation=classify
```

还是 LightCurve/input preparation 相关 operation。

如果是 Triton 分类阶段，则继续检查 Triton。

如果是在 LightCurve 读取阶段，则继续检查 LightCurve API。

### Step 3：检查 Triton

当前生产 Triton 容器：

```
variable-star-triton-final-timeorder-7bd80afc87bf
```

检查：

```
docker ps \
  --filter name='variable-star-triton-final-timeorder-7bd80afc87bf' \
  --format 'table {{.Names}}\t{{.Status}}'
```

检查 readiness：

```
curl -fsS http://127.0.0.1:18000/v2/health/ready \
  >/dev/null \
  && echo 'triton_ready=PASS' \
  || echo 'triton_ready=FAIL'
```

如果 Triton 不可用，检查其日志：

```
docker logs \
  --since 10m \
  variable-star-triton-final-timeorder-7bd80afc87bf
```

不要因为 Worker 正在 retry 就跳过 Kafka offset。

### Step 4：检查 LightCurve API

确认 classifier-worker 当前配置使用的 LightCurve endpoint。

不要假设 URL。

从当前运行配置或容器环境中确认目标地址后，对 fixed revision endpoint 做健康/请求验证。

需要区分：

```
网络失败 / timeout / 5xx
```

和：

```
固定 revision 明确不存在
```

前者通常属于 transient dependency failure。

后者可能属于永久语义，不应盲目无限 retry。

### Step 5：确认是否正在同一 offset 重试

日志中记录：

```
topic
partition
offset
job_id
```

依赖持续不可用时，同一个 partition 的当前 offset 停留是预期行为。

不要仅因为 offset 长时间不前进就手工提交。

## 7. Rebalance 场景

长期 RETRYABLE 期间如果有新的 Worker 加入 consumer group，可能触发 rebalance。

当前系统已经实现：

```
BlockRebalanceOnPoll
+
rebalance callback blocked observation
+
RebalanceYield
+
fresh consumer session
```

正确行为为：

```
Worker A 正在处理 offset N
        ↓
dependency unavailable
        ↓
长期 RETRYABLE
        ↓
Worker B 加入 group
        ↓
rebalance callback blocked
        ↓
RebalanceYield cancel 当前 Command context
        ↓
offset N 不提交
        ↓
AllowRebalance
        ↓
旧 consumer session 结束
        ↓
创建 fresh consumer client/session
        ↓
新的 owner 从未提交 offset N 重新读取
```

关键日志：

```
retry wait cancelled
Kafka consumer session yielded
```

关键指标：

```
astro_kafka_rebalance_callback_blocked_total
```

### 必须确认

发生 yield 后：

- 旧 session 不继续处理 offset N+1；
- offset N 没有被提交；
- 新 owner 从 N 重读。

如果看到：

```
UNKNOWN_MEMBER_ID
service failed
```

应认为 consumer session recovery 可能异常，需要升级排查。

当前服务器验收中，fresh consumer session 方案已验证不会继续使用失效旧 session。

## 8. 处理动作

### Triton unavailable

首选处理：

1. 确认 Triton 容器状态；
2. 恢复 Triton 服务本身；
3. 等待 `/v2/health/ready` 返回成功；
4. 不重置 Worker offset；
5. 不清空 ClassificationCommand topic；
6. 观察 Worker 自动恢复。

如果是模型发布导致 Triton 无法 ready：

按模型发布 Runbook 回滚到上一 immutable model release。

不要直接在正在运行的 model repository 中现场修改 ONNX。

### LightCurve unavailable

首选处理：

1. 确认 LightCurve 服务进程和网络；
2. 确认 fixed revision endpoint 是否恢复；
3. 不手工修改 ClassificationCommand；
4. 不跳过当前 Kafka offset；
5. 等待 Worker retry 自动恢复。

如果服务端明确返回永久不存在，应按当前错误分类规则判断，不把永久错误伪装成 transient。

## 9. 禁止操作

故障期间禁止：

```
手工 commit 当前未完成 Command offset
```

禁止：

```
直接删除 ClassificationCommand
```

禁止为了“恢复消费”而：

```
把 RETRYABLE 改成 PERMANENT
```

禁止：

```
把依赖不可用消息手工塞入 Command DLQ
```

禁止在没有确认 committed offset 的情况下执行 group offset 跳转。

禁止为了清 lag 而并发启动大量 Worker，导致未经评估的 rebalance storm。

## 10. 恢复确认

依赖恢复后，必须同时确认下面几类信号。

### Worker

应再次看到：

```
ClassificationResult published
```

当前 RETRYABLE 状态应恢复：

```
astro_classification_command_retrying = 0
```

retry age 应回落到正常状态。

### Kafka

当前未完成 offset 应先成功处理。

随后才能看到：

```
offset N
→
offset N+1
→
offset N+2
```

不能出现：

```
N 尚未完成
但直接继续 N+1
```

### Triton / LightCurve

Triton：

```
/v2/health/ready = PASS
```

LightCurve：

目标 fixed revision HTTP 请求恢复正常。

### Result

ClassificationResult 应恢复产生。

### PostgreSQL

classification-result-writer 应继续正常持久化 Result。

### DLQ

不得因为一次 transient dependency outage 出现异常 Command DLQ 增长。

## 11. 升级条件

满足以下任一情况时，应从常规依赖恢复升级为工程排查：

- Triton 已 ready，但 Worker 仍长期 `DEPENDENCY_UNAVAILABLE`；
- LightCurve endpoint 已恢复，但 Worker 仍持续 retry；
- retry age 持续增长且没有恢复趋势；
- Worker 出现 `UNKNOWN_MEMBER_ID`；
- Worker 出现 `service failed`；
- rebalance-yield 后没有从未提交 offset 重读；
- Command DLQ 因 transient dependency failure 增长；
- 同一故障恢复后出现 ClassificationResult 缺失；
- Triton Serving Contract gate 失败；
- 新模型 release 无法通过 readiness / contract / golden validation。

## 12. 已验证的服务器场景

### Triton 短时不可用

真实服务器验收已确认：

```
Triton stop
    ↓
classifier-worker RETRYABLE
    ↓
当前 Command 不提交
    ↓
Worker 保持运行
    ↓
Triton restart
    ↓
ready 恢复
    ↓
同一 Command 成功
    ↓
ClassificationResult published
    ↓
继续后续 Command
```

期间未观察到：

```
Command DLQ
UNKNOWN_MEMBER_ID
service failed
```

### 长期 RETRYABLE + 双 Worker + rebalance

真实服务器验收已确认：

```
Worker A: partition 0 / offset 309 RETRYABLE
Worker B joins group
        ↓
A rebalance callback blocked
        ↓
A yields old session
        ↓
offset 309 remains uncommitted
        ↓
B receives partition 0
        ↓
B reads offset 309 again
```

依赖恢复后：

```
B completes 309
then consumes 310 / 311
```

证明未提交 offset 的恢复边界正确。

## 13. 恢复完成条件

只有同时满足以下条件，才可以关闭本次依赖故障：

```
dependency health restored
Worker process healthy
current RETRYABLE cleared
retry age returned to normal
original uncommitted Command completed
ClassificationResult published
subsequent offsets continue normally
no abnormal Command DLQ growth
no UNKNOWN_MEMBER_ID
no service failed
```

如果故障期间发生 rebalance，还必须确认：

```
fresh consumer session recovered from uncommitted offset
```