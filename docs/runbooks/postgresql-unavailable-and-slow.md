# PostgreSQL Unavailable and Slow Runbook

本 Runbook 用于处理“变源候选体实时分类系统”中 PostgreSQL 暂时不可用、网络中断、连接失败、写入超时、连接池异常，以及疑似慢查询 / 锁等待导致的 `classification-result-writer` 处理停滞。

适用组件：

- `classification-result-writer`
- PostgreSQL 16
- `classification_runs`
- `current_classifications`

本 Runbook 主要覆盖**运行期数据库依赖故障**。

PITR、物理 Base Backup、WAL Archive 和指定时间点恢复操作，单独参见：

```text
postgresql-recovery.md
```

---

## 1. 当前验证状态

### 已完成真实服务器验证

以下场景已经完成服务器故障演练：

- PostgreSQL 短时不可用；
- Result Writer 与 PostgreSQL 网络断开；
- Result Writer 对 transient persistence failure 持续 retry；
- 当前未完成 Result offset 不提交；
- PostgreSQL 恢复后无需重启 Writer 即可继续；
- 双 Result Writer + transient PostgreSQL failure + rebalance-yield；
- fresh consumer session 从未提交 offset 恢复；
- PostgreSQL 恢复后先完成原 Result，再继续后续 offset；
- 未观察到持续 `UNKNOWN_MEMBER_ID` / `service failed`。

### 尚未单独故障注入

以下场景当前主要具备观测和诊断能力，但尚未作为独立故障实验完成验收：

```text
长期锁等待
慢 SQL
连接池耗尽
磁盘 I/O 持续饱和
数据库 CPU 持续饱和
```

因此本 Runbook 对这些场景的处理步骤属于**诊断流程**，不能标记为 SERVER_VERIFIED。

---

## 2. 预期系统行为

PostgreSQL 暂时不可用时，正确链路应为：

```text
ClassificationResult
        ↓
classification-result-writer
        ↓
PostgreSQL persistence failure
        ↓
RETRYABLE
        ↓
capped retry
        ↓
当前 Result offset 不提交
        ↓
PostgreSQL recovered
        ↓
same Result persisted
        ↓
offset committed
        ↓
继续后续 Result
```

数据库 transient failure 不应该直接进入 Result DLQ。

---

## 3. 典型现象

### PostgreSQL unavailable

可能表现为：

```text
connection refused
network unreachable
connection reset
timeout
temporary database unavailable
```

对应业务表现：

```text
ClassificationResult Kafka lag ↑
classification-result-writer 持续 retry
PostgreSQL write success 暂停
```

### PostgreSQL slow

可能表现为：

```text
单次 persistence 接近或达到 timeout
连接获取时间增加
pgxpool acquire wait 增加
Result lag 缓慢上涨
```

### Lock / contention

可能表现为：

```text
Writer 进程存活
PostgreSQL 也存活
但部分写入长时间不返回
```

这时要区分：

- SQL 本身慢；
- 行锁 / 表锁等待；
- connection pool 耗尽；
- storage / CPU 饱和；
- 网络延迟。

---

## 4. Result Writer 当前安全语义

当前 `classification-result-writer` 处理链已经冻结为：

```text
ClassificationResult
        ↓
Result Retry
        ↓
Result Writer
        ↓
PostgreSQL
```

永久 Result 错误由 Result DLQ 处理。

Transient PostgreSQL failure：

```text
不进入 Result DLQ
```

而是：

```text
RETRYABLE
        ↓
不 commit 当前 offset
```

当前单次 persistence 调用受 timeout 限制。

运行时还具备：

```text
RebalanceYield
+
fresh consumer session
```

用于长时间 retry 期间的安全 rebalance。

---

## 5. 关键指标

优先检查 classification-result-writer 的 management `/metrics`。

重点观察 PostgreSQL pool：

```text
astro_postgres_pool_*
```

具体应关注：

- acquired / idle connection；
- max connections；
- acquire count；
- acquire duration；
- canceled acquire；
- empty-pool wait count；
- empty-pool wait duration。

同时观察业务 persistence 指标：

```text
astro_postgres_writes_total
astro_postgres_write_duration_seconds
astro_classification_runs_persisted_total
astro_current_classifications_advanced_total
```

以及 Kafka / consumer 指标：

- ClassificationResult consumer lag；
- Kafka consume errors；
- Kafka commit / group management error；
- rebalance callback blocked；
- Result retry 状态。

不要只看 PostgreSQL 容器是否 `Up`。

容器存活不代表业务写入一定健康。

---

## 6. 关键日志字段

排查时优先保留：

```text
run_id
job_id
object_id
light_curve_revision
topic
partition
offset
error_code
error_class
operation
```

同时关注：

```text
retry
retry wait cancelled
consumer session yielded
persisted
Result DLQ
service failed
UNKNOWN_MEMBER_ID
```

不要复制：

- PostgreSQL password；
- DSN 中的密码；
- Kafka SASL password；
- Secret；
- 原始敏感 Kafka payload。

---

## 7. 第一判断：数据库真的不可用吗

先确认 PostgreSQL 容器 / 服务状态。

当前服务器容器：

```text
astro-postgres
```

检查：

```bash
docker ps \
  --filter name='^/astro-postgres$' \
  --format 'table {{.Names}}\t{{.Status}}'
```

如果容器不是 healthy，继续检查 PostgreSQL 自身。

如果容器 healthy，也不能直接排除：

- network path failure；
- connection pool failure；
- SQL timeout；
- lock wait。

---

## 8. 检查 PostgreSQL Readiness

可在服务器上使用：

```bash
docker exec astro-postgres \
  pg_isready
```

如果需要针对业务数据库验证，应使用当前运行配置中已有的连接参数。

不要把密码直接打印到终端历史或事故记录。

判断：

```text
pg_isready success
```

只说明 PostgreSQL 可以接受连接。

它不证明：

```text
业务 SQL 一定快速
Writer 网络路径一定正常
连接池一定健康
```

---

## 9. 检查 Writer 是否仍存活

确认 Result Writer 容器 / 进程仍然运行。

```bash
docker ps --format 'table {{.Names}}\t{{.Status}}' \
  | grep -i 'classification-result-writer'
```

同时检查：

```text
/live
/ready
/metrics
```

如果 Writer 仍存活并持续 RETRYABLE，优先修复 PostgreSQL / network，不要立即 reset Kafka offset。

---

## 10. 检查当前 Result Offset

从 Writer 日志中记录：

```text
topic
partition
offset
run_id
```

如果 PostgreSQL unavailable：

```text
同一 offset 长时间不推进
```

可能是正确行为。

核心要确认：

```text
当前 Result 尚未持久化
→ 当前 offset 没有 commit
```

不要把“lag 增长”直接判断成 consumer stuck。

---

## 11. PostgreSQL 网络故障

### 已验证场景

服务器实验已经验证：

```text
Writer A
    ↓
PostgreSQL network disconnect
    ↓
当前 Result 持续 transient retry
    ↓
offset 保持未提交
```

随后 Worker B / Writer B 加入 group 时：

```text
rebalance blocked
        ↓
retry wait cancelled
        ↓
consumer session yielded
        ↓
fresh consumer session
```

恢复 PostgreSQL network 后：

```text
原未提交 Result 成功持久化
        ↓
然后才继续后续 offsets
```

### 正确处理

1. 恢复 PostgreSQL network；
2. 不 reset Result consumer offset；
3. 不把 Result 手工放入 DLQ；
4. 不清空 Result topic；
5. 观察原 Result 自动恢复。

---

## 12. PostgreSQL 短时停止

如果 PostgreSQL 服务被停止：

```text
Result Writer RETRYABLE
Result lag ↑
当前 offset 不 commit
```

恢复 PostgreSQL 后应看到：

```text
PostgreSQL connection recovered
same Result persisted
offset advances
subsequent Result continues
```

如果 PostgreSQL 已 healthy，但 Writer 仍不恢复，继续检查：

- pgxpool；
- DNS / Docker network；
- 当前 DSN；
- Writer error log；
- Kafka consumer session；
- timeout。

---

## 13. 慢查询 / 写入超时诊断

当前系统尚未对“慢 SQL”做独立故障注入。

因此发现 write duration 明显升高时，先做诊断，不直接修改数据库参数。

### 检查当前活动

可以从 PostgreSQL 侧查看：

```sql
SELECT
    pid,
    state,
    wait_event_type,
    wait_event,
    query_start,
    xact_start,
    backend_type
FROM pg_stat_activity
WHERE datname = current_database()
ORDER BY query_start NULLS LAST;
```

事故记录中不要直接复制包含敏感值的完整 SQL。

### 重点判断

是否出现：

```text
wait_event_type = Lock
```

或者：

```text
长时间 active
```

或者：

```text
大量 session 等待 connection / transaction
```

---

## 14. Lock Wait 诊断

如果怀疑锁等待，可先查：

```sql
SELECT
    pid,
    locktype,
    mode,
    granted,
    relation::regclass
FROM pg_locks
WHERE NOT granted
ORDER BY pid;
```

如果存在未授予锁：

继续结合 `pg_stat_activity` 判断 blocker / waiter。

不要为了快速恢复直接：

```text
kill 所有 session
```

或：

```text
restart PostgreSQL
```

先确认：

- 哪个 transaction 持锁；
- 是否为业务 Writer；
- 是否有人为维护操作；
- 是否存在 migration / DDL；
- 终止 blocker 是否会造成更大影响。

当前 lock / slow query 处置尚未经过独立服务器 fault injection，因此高风险 kill 操作需要人工升级判断。

---

## 15. Connection Pool 异常

如果观察到：

```text
acquired connections 接近 max
idle connections = 0
acquire duration 持续增长
empty-pool wait 增长
```

可能是：

- PostgreSQL 本身慢；
- transaction 没及时释放；
- network 卡住；
- pool max 太小；
- 突发并发超过当前设计。

不要第一时间简单扩大 pool。

先判断：

```text
数据库是否真的有处理能力
```

否则增加连接数可能使 PostgreSQL 更差。

---

## 16. PostgreSQL CPU / Storage 饱和

如果：

```text
数据库 healthy
连接也成功
但所有写入持续变慢
```

应检查宿主机：

- CPU；
- memory；
- disk utilization；
- filesystem free space；
- I/O wait。

尤其注意：

```text
PGDATA
WAL archive
```

所在文件系统是否接近满。

磁盘空间不足的完整处置单独由：

```text
disk-space.md
```

覆盖。

---

## 17. 不应该进入 Result DLQ 的场景

以下 transient failure 不应形成 Result DLQ：

```text
PostgreSQL unavailable
network timeout
temporary connection failure
query timeout
Context cancellation
```

如果这些故障期间 Result DLQ 持续增长：

```text
需要怀疑错误分类回归
```

并升级工程排查。

---

## 18. Rebalance 场景

长期 PostgreSQL RETRYABLE 期间，新 Writer 加入可能触发 rebalance。

正确流程：

```text
Writer A 正在处理 offset N
        ↓
PostgreSQL unavailable
        ↓
持续 retry
        ↓
Writer B joins group
        ↓
rebalance callback blocked
        ↓
cancel 当前 record context
        ↓
offset N 不 commit
        ↓
old session yields
        ↓
fresh consumer session
        ↓
新的 owner 从 N 重读
```

服务器真实实验已验证这一语义。

如果出现：

```text
N 未成功
但直接处理 N+1
```

属于严重 offset 安全问题。

---

## 19. PostgreSQL 恢复后的确认

PostgreSQL 恢复后，不要只检查：

```text
astro-postgres healthy
```

还必须确认 Writer 恢复。

### Persistence

应再次看到成功 persistence。

业务指标应看到：

```text
astro_postgres_writes_total{result="success"}
```

继续增长。

### Kafka

原未完成 offset 必须先成功。

然后才应看到：

```text
N
→
N+1
→
N+2
```

### Retry

持续 retry 状态应消失或下降到正常状态。

### Pool

连接池应恢复，例如：

```text
acquired 回落
idle connection 恢复
acquire wait 恢复正常
```

### Database

业务表继续可访问：

```text
classification_runs
current_classifications
```

---

## 20. 幂等性确认

Result replay / retry 可能导致同一个确定性 ClassificationRun 再次到达 persistence 层。

当前 Repository 语义已经支持：

- 新 Run：成功插入；
- 幂等重复：成功；
- 旧 revision：不推进 Current；
- 同 revision：不覆盖 Current；
- 更高 revision：按规则推进 Current；
- SHADOW / REPROCESS：不推进 Current。

因此依赖故障后的重新消费并不等价于重复数据损坏。

不要因为看到同一 Result 被重新处理，就手工删除数据库记录。

---

## 21. 禁止操作

PostgreSQL transient outage 期间禁止：

```text
手工 commit 当前未完成 Result offset
```

禁止：

```text
把 transient Result 手工塞入 Result DLQ
```

禁止：

```text
清空 ClassificationResult topic
```

禁止：

```text
reset consumer offset 跳过未完成 Result
```

禁止在没有确认恢复点的情况下：

```text
删除 WAL
```

禁止未经评估：

```text
删除 classification_runs
修改 current_classifications
```

禁止为了消除慢查询：

```text
直接 kill 所有 PostgreSQL session
```

---

## 22. 恢复完成条件

只有同时满足以下条件，才能关闭 PostgreSQL unavailable / slow 故障：

```text
PostgreSQL connection healthy
Writer process healthy
current persistence retry cleared
original uncommitted Result persisted
original offset committed
subsequent offsets continue
Result lag stops growing
Result lag starts recovering
PostgreSQL writes return to success
pgxpool returns to normal range
no abnormal Result DLQ growth
no UNKNOWN_MEMBER_ID
no service failed
```

如果发生 rebalance，还必须确认：

```text
fresh consumer session recovered from uncommitted offset
```

---

## 23. 升级条件

满足任一条件时应升级工程 / DBA 排查：

- PostgreSQL 已 healthy，但 Writer 仍持续 retry；
- Result lag 持续增加且 committed offset 不推进；
- pgxpool acquire wait 持续升高；
- connection pool 长时间耗尽；
- 大量未授予 PostgreSQL lock；
- 单条 persistence 持续超过当前 timeout；
- 数据库 CPU / I/O 长时间饱和；
- `UNKNOWN_MEMBER_ID` 持续出现；
- `service failed` 持续出现；
- transient PostgreSQL failure 导致 Result DLQ 增长；
- 恢复后原未提交 offset 没有重新处理；
- persistence 成功日志存在，但 Kafka offset 不推进；
- 数据库出现 corruption / data-loss 怀疑。

如果怀疑数据损坏或需要时间点恢复：

```text
停止使用本 Runbook 的常规运行期处置
```

转入：

```text
postgresql-recovery.md
```

---

## 24. 当前工程边界

当前已经具备：

```text
Result persistence retry
manual Kafka offset commit
Result DLQ
rebalance-yield
fresh consumer session
pgxpool metrics
PostgreSQL write metrics
WAL archive
physical base backup
PITR
```

当前没有：

```text
自动 failover PostgreSQL cluster
Patroni
云数据库 HA
读写分离
自动 SQL kill
自动 lock resolver
```

这些不属于本 Runbook 的恢复手段。

当前原则是：

```text
transient failure
→ 保留未完成 offset
→ 恢复 PostgreSQL
→ 自动继续

permanent Result error
→ Result DLQ

database corruption / point-in-time recovery
→ PostgreSQL Recovery Runbook
```
