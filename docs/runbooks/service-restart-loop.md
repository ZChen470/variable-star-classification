# Service Restart Loop Runbook

本 Runbook 用于处理“变源候选体实时分类系统”三个核心 daemon 的反复退出、反复启动、启动后立即失败或容器 restart loop 问题。

适用组件：

- `candidate-orchestrator`
- `classifier-worker`
- `classification-result-writer`

本 Runbook 只处理**进程 / 容器级 restart loop**。

以下情况不应误判为 restart loop：

- classifier-worker 对 Triton / LightCurve 的长期 RETRYABLE；
- classification-result-writer 对 PostgreSQL transient failure 的长期 RETRYABLE；
- classifier-worker / result-writer 因 rebalance-yield 主动结束旧 Kafka consumer session 并创建 fresh consumer session；
- 单次计划内 restart；
- 单次 SIGTERM graceful shutdown。

---

## 1. 当前验证状态

当前状态：

```text
PARTIALLY_VERIFIED_SERVER
```

已经在服务器真实验证过：

- candidate-orchestrator 停止期间 Candidate 可以继续积压；
- candidate-orchestrator 重启后可以恢复消费并继续发布 ClassificationCommand；
- classifier-worker 非优雅 `docker kill` 后可以重新启动；
- Worker 重启后重新通过 Triton startup contract gate；
- Worker 能继续消费未完成 / 积压 Command；
- classifier-worker 长期 RETRYABLE 不再因为 retry exhaustion 直接退出进程；
- rebalance-yield 只重建 Kafka consumer session，不应导致整个 Worker 进程退出；
- classification-result-writer 在 PostgreSQL transient failure 下保持进程运行；
- Writer rebalance-yield 后可重建 fresh consumer session；
- PostgreSQL 恢复后 Writer 可继续处理。

尚未作为独立故障实验完成：

```text
人为制造三个 daemon 的持续启动失败并观察 Docker restart loop
```

因此本 Runbook 的“反复启动失败”部分属于诊断与处置流程，不应标记为完整 SERVER_VERIFIED。

---

## 2. 先区分四种现象

### A. 真正的 Restart Loop

表现为：

```text
container starts
    ↓
process exits
    ↓
container restarts
    ↓
process exits again
```

通常伴随：

```text
RestartCount 持续增加
```

这才是本 Runbook 的主要处理对象。

---

### B. 长期 RETRYABLE

例如：

```text
classifier-worker
→ Triton unavailable
→ process 仍然 Up
→ 同一 Command 持续 retry
```

或者：

```text
classification-result-writer
→ PostgreSQL unavailable
→ process 仍然 Up
→ 同一 Result 持续 retry
```

这不是 restart loop。

---

### C. Kafka Consumer Session Rebuild

classifier-worker / result-writer 在 rebalance-yield 后可能出现：

```text
old consumer session ends
fresh consumer client created
group rejoins
```

只要主进程仍然存活：

```text
这不是 service restart
```

不要因为日志里出现：

```text
consumer session yielded
consumer session started
```

就判断容器在 restart loop。

---

### D. 单次正常 Restart

计划发布、人工 restart、SIGTERM、Docker recreate 都可能造成一次进程重启。

如果：

```text
restart count 不持续增加
+
服务恢复 ready
+
消费继续
```

则不是 restart loop。

---

## 3. 第一判断：容器到底有没有反复重启

先检查：

```bash
docker ps -a \
  --format 'table {{.Names}}\t{{.Status}}'
```

再针对具体容器：

```bash
docker inspect <container> \
  --format 'name={{.Name}} status={{.State.Status}} running={{.State.Running}} exit_code={{.State.ExitCode}} restart_count={{.RestartCount}} started={{.State.StartedAt}} finished={{.State.FinishedAt}}'
```

连续观察两次。

如果：

```text
RestartCount 持续增加
```

才确认存在 restart loop。

不要只看：

```text
Up 20 seconds
```

就下结论。

---

## 4. 检查 Restart Policy

当前不要假设所有容器都使用相同 restart policy。

检查：

```bash
docker inspect <container> \
  --format 'restart_policy={{.HostConfig.RestartPolicy.Name}} max_retry={{.HostConfig.RestartPolicy.MaximumRetryCount}}'
```

可能出现：

```text
no
always
unless-stopped
on-failure
```

Restart policy 决定：

```text
进程退出后 Docker 是否会自动重新启动
```

但 restart policy 不是根因。

看到反复 restart 时仍然必须先查进程为什么退出。

---

## 5. 三个 Daemon 的正常启动门禁

### candidate-orchestrator

正常需要完成：

```text
config load
Kafka client / consumer group assembly
management listener bind
runner startup
```

如果配置、Kafka 初始化或 listener bind 失败：

```text
进程可能启动失败
```

---

### classifier-worker

启动路径更严格，至少涉及：

```text
config
Serving Manifest / Serving Bundle
PostgreSQL startup connectivity
Triton Serving Contract gate
Kafka producer
Kafka consumer session
LightCurve HTTP client
retry / DLQ handler
management listener
```

注意：

运行期 LightCurve unavailable 不应该导致 startup restart loop。

但 Triton 在 startup contract gate 阶段不可访问或契约不正确时：

```text
Worker 可能无法完成启动
```

需要根据日志判断是：

```text
dependency unavailable
```

还是：

```text
contract / manifest mismatch
```

---

### classification-result-writer

启动至少涉及：

```text
config
PostgreSQL startup connectivity
Kafka producer
Kafka consumer session
Result Writer / DLQ / Retry assembly
management listener
```

如果 PostgreSQL startup Ping 无法通过：

```text
启动可能失败
```

这与“已经启动后 PostgreSQL 临时不可用”的行为不同。

---

## 6. 关键日志

首先查看最近一次退出前后的日志。

```bash
docker logs \
  --since 15m \
  <container> \
  2>&1
```

重点寻找：

```text
startup
service failed
config
bind
listen
kafka
postgres
triton
manifest
contract
panic
fatal
context
signal
shutdown
```

当前结构化日志应优先使用稳定字段判断。

不要依赖单一错误全文做自动业务分类。

---

## 7. Exit Code 判断

检查：

```bash
docker inspect <container> \
  --format 'exit_code={{.State.ExitCode}} oom_killed={{.State.OOMKilled}} error={{.State.Error}}'
```

需要区分：

```text
程序正常退出
程序主动返回错误
被 SIGKILL
OOM killed
Docker runtime error
```

如果：

```text
OOMKilled=true
```

应转向：

```text
资源 / memory 问题
```

不要继续按普通配置错误排查。

---

## 8. Management Listener 判断

如果进程短暂启动：

检查是否曾经成功暴露：

```text
/live
/ready
/metrics
```

### `/live`

表示：

```text
进程活着
```

### `/ready`

表示：

```text
该 daemon 已完成当前定义的启动门禁
```

如果容器：

```text
反复 Up 几秒
然后退出
```

而 `/ready` 从未成功：

优先怀疑：

```text
startup gate
configuration
port bind
dependency initialization
```

如果 `/ready` 曾经成功，然后进程才退出：

重点查看：

```text
runtime fatal error
Kafka runner error
unexpected top-level return
signal / OOM
```

---

## 9. candidate-orchestrator Restart

服务器已经验证：

```text
orchestrator stop
    ↓
Candidate continues accumulating
    ↓
orchestrator restart
    ↓
consumer rejoins
    ↓
backlog consumed
    ↓
ClassificationCommand published
```

因此单次 orchestrator restart 不等于消息丢失。

发生 restart loop 时重点检查：

- Kafka configuration；
- SASL configuration；
- Candidate topic / group；
- management listen address；
- Kafka runner top-level error；
- DLQ / Command publisher error；
- container restart policy。

不要：

```text
reset Candidate group offset
```

来解决 orchestrator 启动失败。

---

## 10. classifier-worker Restart

### 已验证的非优雅退出恢复

服务器曾对 classifier-worker 执行：

```text
docker kill
```

随后：

```text
Worker restart
    ↓
/ready 恢复
    ↓
重新执行 Triton startup contract gate
    ↓
消费未完成 / 积压 Command
    ↓
ClassificationResult 发布
```

证明单次 crash / kill 后恢复路径成立。

### 当前不应该导致 Worker 退出的场景

运行期：

```text
Triton unavailable
LightCurve unavailable
```

应进入：

```text
RETRYABLE
```

而不是：

```text
top-level service exit
```

如果 transient dependency failure 又开始导致 Worker restart loop：

```text
这是严重回归
```

---

## 11. Worker Retry 与 Restart Loop 的历史边界

旧实现曾存在风险：

```text
RETRYABLE 快速重试耗尽
    ↓
错误上抛
    ↓
classifier-worker 进程退出
```

当前实现已经修正为：

```text
RETRYABLE
    ↓
capped backoff
    ↓
无固定最大尝试次数
    ↓
持续到成功或 Context cancellation
```

因此现在如果看到：

```text
DEPENDENCY_UNAVAILABLE
紧接 service failed
紧接 container restart
```

不能认为这是正常设计。

应该升级排查。

---

## 12. Worker Rebalance-Yield 不是 Restart

当前 Worker runtime 使用：

```text
稳定 producer client
+
可重建 consumer client/session
```

rebalance-yield 后：

```text
ErrRebalanceYielded
    ↓
CloseAllowingRebalance
    ↓
old consumer client closes
    ↓
fresh consumer client
```

Worker 主进程：

```text
应该继续存活
```

如果每次 rebalance-yield 都导致：

```text
整个 classifier-worker container restart
```

则是异常。

---

## 13. Result Writer Restart

classification-result-writer 当前已经具备：

```text
PostgreSQL transient retry
+
RebalanceYieldConsumerRunner
+
fresh consumer session
```

PostgreSQL 临时不可用时：

```text
Writer 应保持运行
```

而不是：

```text
数据库短时失败
→ service exits
→ container restart
```

如果出现这种行为：

检查是否为：

- startup Ping 阶段；
- runtime persistence 阶段；
- 未分类 top-level error；
- 配置 / network；
- regression。

---

## 14. Startup Failure 与 Runtime Failure 要分开

### Startup Failure

例如：

```text
invalid config
manifest invalid
Triton contract mismatch
PostgreSQL startup Ping failed
management listen bind failed
Kafka client assembly failed
```

通常需要：

```text
修复配置 / deployment
然后重新启动
```

自动无限 restart 不会自动修好静态配置错误。

### Runtime Failure

例如：

```text
unexpected Kafka runner error
unexpected top-level handler return
panic
OOM
runtime fatal
```

需要先保留日志与 exit code。

不要只靠 Docker 自动 restart 掩盖根因。

---

## 15. Contract / Manifest 引发的 Restart Loop

如果 classifier-worker 启动反复失败，并看到：

```text
manifest
bundle
metadata
config
contract
model version
```

转入：

```text
contract-and-manifest-mismatch.md
```

处理原则：

```text
修复 deployment / model release
```

不要：

```text
关闭 Serving Contract gate
```

---

## 16. PostgreSQL 引发的 Restart Loop

如果 classification-result-writer：

```text
每次启动都因为 PostgreSQL startup connectivity 失败而退出
```

先确认：

```text
PostgreSQL 是否真的可连接
Docker network
DSN
credentials
startup Ping
```

如果服务已经成功启动后 PostgreSQL 才短时故障：

则正常应该：

```text
runtime retry
```

而不是 restart。

运行期场景参见：

```text
postgresql-unavailable-and-slow.md
```

---

## 17. Kafka 引发的 Restart Loop

检查：

```text
broker connectivity
SASL
consumer group
topic
produce / consume
UNKNOWN_MEMBER_ID
```

当前 Worker / Writer 已通过 fresh consumer session 解决长期 RETRYABLE + rebalance 后旧 session 继续运行的问题。

因此如果看到：

```text
UNKNOWN_MEMBER_ID
→ service failed
→ restart
```

持续发生：

应升级 Kafka consumer session 排查。

参见：

```text
kafka-consumer-lag-and-stuck.md
```

---

## 18. 配置错误

检查当前容器实际 environment。

不要直接打印所有环境变量，因为其中可能包含：

```text
Kafka SASL password
PostgreSQL password
```

应只检查非敏感配置名称，或在本机安全环境中单独确认 Secret 是否存在。

重点包括：

- brokers；
- topics；
- consumer group；
- management listen address；
- Triton endpoint；
- LightCurve endpoint；
- manifest path；
- model bundle version；
- database host / db name 等非敏感部分。

---

## 19. Port Bind Failure

如果日志出现：

```text
address already in use
bind failed
listen failed
```

检查：

```bash
ss -lntp
```

以及 Docker port mapping。

不要通过：

```text
关闭 management listener
```

来绕过端口冲突。

应该修正：

```text
重复容器
错误端口映射
冲突进程
```

---

## 20. OOM / Resource Failure

如果：

```text
OOMKilled=true
```

检查：

```bash
docker stats --no-stream
```

以及：

```bash
free -h
```

必要时检查 kernel OOM 记录：

```bash
sudo dmesg -T | grep -i -E 'oom|killed process'
```

不要仅通过：

```text
无限增加 restart
```

解决 OOM。

需要确认：

- 是否有 memory leak；
- 是否容器 memory limit 太小；
- 是否多个进程竞争；
- 是否新模型显著增加内存。

---

## 21. Docker Image / Binary 不一致

如果新版本部署后开始 restart loop：

确认当前容器实际 image：

```bash
docker inspect <container> \
  --format 'image={{.Config.Image}} image_id={{.Image}}'
```

并与预期 deployment 版本比较。

不要因为：

```text
镜像 tag 名一样
```

就假设 image 内容一定一样。

如果需要回滚：

优先使用明确的已知 good image / release。

---

## 22. 处理动作优先级

### 静态配置错误

处理：

```text
修复 config
→ recreate / restart
→ 验证 ready
```

### Manifest / Contract 错误

处理：

```text
回滚或修复 model release / manifest
→ restart
→ Serving Contract PASS
```

### Transient Dependency

如果已经进入 runtime：

```text
不要重启
```

优先恢复 dependency，让 retry 自动继续。

### Crash / Panic / OOM

处理：

```text
保留日志
确认 exit code
定位根因
修复
再 restart
```

---

## 23. 是否应该手工停止 Restart Loop

如果 container 每几秒反复启动，且原因是明确的静态错误：

例如：

```text
invalid config
missing manifest
contract mismatch
port conflict
```

可以考虑先：

```text
停止自动 restart
```

或停止该容器，避免：

- 日志刷屏；
- 不断重复无意义启动；
- 不断触发依赖连接；
- 干扰排障。

但是否修改 restart policy 应根据当前部署方式决定。

不要在不知道 Compose / Docker restart 配置的情况下永久改动策略。

---

## 24. Restart Loop 期间的 Kafka Offset

如果服务未完成当前 record：

```text
不要手工推进 offset
```

Kafka 应依赖：

```text
committed offset
```

在服务恢复后重新读取未完成 record。

特别是：

- classifier-worker；
- classification-result-writer。

不要为了“防止反复处理”而跳过未完成消息。

---

## 25. Restart Loop 期间的 DLQ

不要把：

```text
服务启动失败
```

对应的所有 backlog 消息批量送入 DLQ。

如果是：

```text
deployment config / dependency / contract
```

问题：

```text
Kafka 消息通常并没有永久错误
```

DLQ 不是 service restart loop 的缓冲队列。

---

## 26. 重启后的恢复确认

服务重新稳定启动后，至少确认：

### Container

```text
RestartCount 不再增长
container 持续 Up
```

### Health

```text
/live 正常
/ready 正常
```

### Logs

不再持续：

```text
service failed
panic
fatal
UNKNOWN_MEMBER_ID
startup gate failed
```

### Kafka

```text
consumer group 稳定
committed offset 恢复推进
lag 停止增长
lag 开始下降
```

### 业务链

candidate-orchestrator：

```text
ClassificationCommand 恢复发布
```

classifier-worker：

```text
ClassificationResult 恢复发布
```

classification-result-writer：

```text
PostgreSQL persistence 恢复
```

---

## 27. classifier-worker 特殊恢复确认

如果 Worker restart 前有未完成 Command N：

恢复后必须确认：

```text
Command N 被重新处理或最终成功
```

然后才继续：

```text
N+1
N+2
```

如果 startup 时 Triton contract gate 会执行：

必须确认：

```text
Serving Contract PASS
```

之后 Worker 才应 ready。

---

## 28. Result Writer 特殊恢复确认

如果 Writer restart 前有未完成 Result N：

恢复后必须确认：

```text
Result N 最终持久化
```

并依赖 deterministic / idempotent storage 语义避免重复污染。

确认：

```text
classification_runs
current_classifications
```

继续满足当前 revision 推进规则。

---

## 29. Orchestrator 特殊恢复确认

candidate-orchestrator 单次 restart 后已经服务器验证：

```text
Candidate backlog
→ restart
→ consume resumes
→ ClassificationCommand publish resumes
```

恢复时重点确认：

```text
Candidate group offset 推进
Command produce 成功
Candidate DLQ 无异常增长
```

---

## 30. 禁止操作

禁止：

```text
为了消除 restart loop reset Kafka offset
```

禁止：

```text
把 backlog 批量送 DLQ
```

禁止：

```text
关闭 contract gate
```

禁止：

```text
因为 transient dependency failure 反复手工 restart Worker / Writer
```

禁止：

```text
在未确认原因前删除容器日志
```

禁止：

```text
不检查 OOMKilled 就直接扩大 restart 次数
```

禁止：

```text
把 consumer session rebuild 当成 container restart
```

---

## 31. 升级条件

满足任一条件时升级工程排查：

- RestartCount 持续增加；
- 同一版本在多个实例同时 restart；
- 出现 panic；
- `OOMKilled=true`；
- `UNKNOWN_MEMBER_ID` 持续导致 service failed；
- transient dependency failure 导致进程退出；
- rebalance-yield 导致整个 Worker / Writer 退出；
- Serving Contract 对 previous known-good release 也失败；
- PostgreSQL healthy 但 Writer 每次启动都失败；
- Kafka healthy 但 consumer 初始化持续失败；
- container ready 后短时间内反复退出；
- restart 后未完成 offset 无法恢复；
- restart 后出现异常 DLQ 增长。

---

## 32. 事故记录建议

至少记录：

```text
incident start time
service
container
image tag
image id
restart policy
restart count
exit code
OOMKilled
first failure log
startup ready ever reached: yes/no
dependency status
Kafka group
affected topic / partition / offset
contract gate result
recovery action
known-good version used
recovery time
lag impact
DLQ impact
```

不要记录：

- Kafka SASL password；
- PostgreSQL password；
- Secret；
- 原始敏感 payload。

---

## 33. 当前工程边界

当前已经具备：

```text
structured process logs
/live
/ready
/metrics
manual Kafka commit
dependency retry
rebalance-yield
fresh consumer session
DLQ
startup Serving Contract gate
Docker container deployment
```

当前没有：

```text
Kubernetes CrashLoopBackOff controller
automatic canary rollback
automatic image rollback
centralized restart remediation controller
```

因此当前 restart loop 处理原则是：

```text
先区分 process restart
和 retry / consumer session rebuild
        ↓
确认 exit reason
        ↓
修复真正根因
        ↓
稳定启动
        ↓
验证 committed offset 与业务链恢复
```

而不是：

```text
服务反复退出
        ↓
不停 restart
        ↓
希望它自己变好
```
