# PostgreSQL Recovery Runbook

本 Runbook 用于处理“变源候选体实时分类系统”中的 PostgreSQL 数据恢复、物理 Base Backup 恢复和 Point-in-Time Recovery（PITR）。

适用组件：

- PostgreSQL 16
- `astro_classification_test`
- `classification_runs`
- `current_classifications`
- PostgreSQL WAL archive
- physical base backup
- 同机异盘第二副本 `/data`

本 Runbook 不用于普通 transient database outage。

如果 PostgreSQL 只是暂时不可用、网络中断、连接超时或慢查询，请先使用：

```text
postgresql-unavailable-and-slow.md
```

---

## 1. 什么时候进入本 Runbook

满足以下情况之一时，才进入数据库恢复流程：

- 数据被误删除或误更新，需要恢复到事故前时间点；
- PostgreSQL 数据目录损坏；
- 怀疑数据 corruption；
- `/dev/sdb` 主数据盘损坏；
- 生产数据库无法通过常规启动 / transient recovery 恢复；
- 明确需要 PITR；
- 需要从 physical base backup 重建 PostgreSQL 实例。

不要因为：

```text
一次连接失败
一次 timeout
Result Writer lag
PostgreSQL 短时停止
```

就直接做 PITR。

---

## 2. 当前已经验证的恢复能力

当前服务器环境已经真实验证：

- `archive_mode=on`；
- WAL continuous archive 正常；
- `archive_timeout=240s`；
- `pg_stat_archiver.failed_count=0`；
- physical base backup 可成功生成；
- backup manifest 使用 SHA256；
- `pg_verifybackup` 验证成功；
- 使用 `recovery.signal + restore_command + recovery_target_time` 完成真实 PITR；
- 恢复结果满足：
  - BEFORE marker 存在；
  - AFTER marker 不存在；
  - `classification_runs` / `current_classifications` 仍可正常读取；
- 实验恢复实例与生产实例隔离；
- 实验 restore time 在当前小数据规模下约 11 秒；
- 低流量 WAL 自然 archive 实测约 141 秒；
- 同机异盘 WAL mirror 受控 RPO 实测约 251 秒，小于 5 分钟；
- 每日 physical base backup 已自动调度；
- retention 保留最近 7 份成功 base backup，并从最老保留 backup 的 START WAL 计算 WAL 安全边界。

---

## 3. 当前恢复资产位置

### PostgreSQL 主数据

```text
PGDATA:
  /home/zhouyuyang/astro-platform/postgres/data

device:
  /dev/sdb2
```

### 源 WAL Archive

```text
/home/zhouyuyang/astro-platform/postgres/archive
```

该目录当前与 PGDATA 同处 `/dev/sdb2`。

### 第二盘 WAL Mirror

```text
/data/astro-platform/backups/postgres/wal-archive-live
```

位于：

```text
/dev/sda
```

### 每日 Physical Base Backup

```text
/data/astro-platform/backups/postgres/basebackups-live
```

位于：

```text
/dev/sda
```

### 已验证的 PITR 证据

```text
/home/zhouyuyang/astro-platform/postgres/recovery-evidence/
```

历史验证中使用过：

```text
pitr_20260814T131042Z.txt
```

---

## 4. 当前恢复边界

当前方案覆盖：

```text
数据库逻辑误操作
指定时间点恢复
PGDATA 损坏
/dev/sdb 单盘故障（依赖 /dev/sda 第二副本）
```

当前不覆盖：

```text
整机损坏
RAID 控制器整体损坏
机箱级故障
机房级故障
异地灾难恢复
```

`/dev/sda` 与 `/dev/sdb` 虽然是不同块设备，但仍在同一台服务器。

因此当前能力只能描述为：

```text
同机异盘恢复
```

不能描述为：

```text
异机灾备
异地灾备
```

---

## 5. 恢复前禁止操作

发生数据事故后，不要立即：

```text
删除 PGDATA
```

不要立即：

```text
覆盖现有 WAL archive
```

不要执行：

```text
pg_archivecleanup
```

直到已经冻结恢复点和需要的 WAL 范围。

不要为了“恢复服务”先清理：

```text
/data/.../basebackups-live
/data/.../wal-archive-live
```

不要修改：

```text
classification_runs
current_classifications
```

来伪造恢复结果。

如果怀疑主数据损坏，应先停止继续写入或隔离故障实例，再决定恢复路径。

---

## 6. 第一阶段：确定事故恢复目标

PITR 前必须回答：

```text
希望恢复到哪个时间点？
```

例如：

```text
误操作发生时间:
2026-08-15 10:32:15 UTC

期望恢复目标:
2026-08-15 10:32:14 UTC
```

尽量使用 UTC。

不要使用模糊描述：

```text
大概十分钟前
```

恢复目标应来自：

- 操作日志；
- 应用日志；
- Kafka / Result 时间；
- 数据库审计证据；
- 明确的业务事件时间。

---

## 7. 第二阶段：选择 Base Backup

列出可用 base backup：

```bash
find /data/astro-platform/backups/postgres/basebackups-live \
  -mindepth 1 \
  -maxdepth 1 \
  -type d \
  -name 'base_*' \
  -printf '%f\n' \
  | sort
```

查看候选 backup 的：

```text
backup_label
backup_manifest
```

例如：

```bash
grep -E \
  'START WAL LOCATION|CHECKPOINT LOCATION|START TIME|LABEL' \
  /data/astro-platform/backups/postgres/basebackups-live/<backup>/backup_label
```

选择规则：

```text
backup 完成时间
<
目标恢复时间
```

不能选择一个晚于目标时间的 base backup 来恢复到更早时间。

---

## 8. 第三阶段：验证 Base Backup

恢复前必须验证 backup。

使用 PostgreSQL 16：

```bash
docker run --rm \
  --entrypoint /usr/lib/postgresql/16/bin/pg_verifybackup \
  -v /data/astro-platform/backups/postgres/basebackups-live/<backup>:/backup:ro \
  postgres:16 \
  -P /backup
```

期望：

```text
backup successfully verified
```

如果 `pg_verifybackup` 失败：

```text
不要继续把该 backup 当作恢复基础
```

应选择另一份已验证 backup 或升级恢复判断。

---

## 9. 第四阶段：确认 WAL 连续性

从选定 backup 的：

```text
START WAL LOCATION
```

得到最早需要的 WAL。

例如历史已验证 backup 中曾出现：

```text
START WAL LOCATION:
0/4000028

file:
000000010000000000000004
```

恢复所需 WAL 必须从该位置开始连续存在，直到 recovery target。

优先检查第二盘 mirror：

```text
/data/astro-platform/backups/postgres/wal-archive-live
```

如主盘仍可用，也可以交叉检查：

```text
/home/zhouyuyang/astro-platform/postgres/archive
```

不要只因为文件数量很多就认为 WAL 连续。

应检查 WAL 文件名序列和目标时间对应的 archive 范围。

---

## 10. 第五阶段：先做隔离恢复

除非已经明确进入正式灾难切换，否则优先：

```text
先恢复到隔离 PostgreSQL
```

不要直接覆盖生产 `astro-postgres`。

隔离恢复的目标是先回答：

```text
这个 Base Backup + WAL
能否恢复到目标点？
```

以及：

```text
恢复后的业务数据是否正确？
```

隔离实例必须：

- 不暴露生产端口；
- 不连接生产业务 Writer；
- 不加入生产应用 network；
- 不接受真实业务写入。

历史服务器 PITR 测试使用了：

```text
--network none
```

作为隔离手段。

---

## 11. 创建恢复副本

不要直接修改保留的 base backup。

先复制成恢复工作目录。

示意：

```bash
RESTORE_ROOT=/path/to/isolated/restore
BASE=/data/astro-platform/backups/postgres/basebackups-live/<backup>

mkdir -p "$RESTORE_ROOT"

rsync -a \
  --numeric-ids \
  "$BASE/" \
  "$RESTORE_ROOT/"
```

恢复工作目录应具有 PostgreSQL 可以读取 / 写入的正确 ownership 和权限。

保留原 backup 不变。

---

## 12. 配置 restore_command

恢复实例需要从 WAL archive 读取 WAL。

示意：

```text
restore_command =
  'cp /wal-archive/%f %p'
```

推荐把 WAL archive 以只读方式挂载到隔离恢复容器。

例如：

```text
/data/astro-platform/backups/postgres/wal-archive-live
→
/wal-archive:ro
```

这样恢复实验不能修改保留的 WAL。

---

## 13. 配置 PITR 目标

在恢复副本中配置：

```text
recovery_target_time = '<UTC target>'
recovery_target_inclusive = 'on'
recovery_target_action = 'pause'
```

并创建：

```text
recovery.signal
```

`pause` 适合恢复验收，因为数据库达到目标点后不会立即继续开放为普通生产实例。

不要在未检查恢复内容前直接使用：

```text
promote
```

---

## 14. 隔离恢复容器原则

恢复容器应：

```text
使用与生产兼容的 PostgreSQL major version
不覆盖生产 PGDATA
不映射生产 5432
不接受生产应用连接
```

当前生产为 PostgreSQL 16。

因此恢复测试应继续使用：

```text
postgres:16
```

除非已经经过独立升级流程。

---

## 15. 判断 Recovery 是否到达目标

进入恢复状态后检查：

```sql
SELECT pg_is_in_recovery();
```

如果使用：

```text
recovery_target_action = pause
```

还可以检查：

```sql
SELECT pg_is_wal_replay_paused();
```

历史验证中成功结果为：

```text
pg_is_in_recovery = true
pg_is_wal_replay_paused = true
```

同时记录：

```sql
SELECT pg_last_wal_replay_lsn();
```

---

## 16. 内容边界验证

PITR 成功与否不能只看：

```text
PostgreSQL 启动成功
```

必须检查业务内容。

理想方式是在事故前后存在明确 marker。

例如：

```text
BEFORE_TARGET
AFTER_TARGET
```

正确恢复应该满足：

```text
BEFORE_TARGET exists
AFTER_TARGET does not exist
```

历史服务器实验已经按这个方式完成真实验证。

---

## 17. 核心业务表验证

至少检查：

```text
classification_runs
current_classifications
```

示意：

```sql
SELECT count(*) FROM classification_runs;
SELECT count(*) FROM current_classifications;
```

如果有已知业务对象，还应抽查：

```text
object_id
run_id
revision
model_bundle_version
predicted class
```

不要仅凭总行数判断完全正确。

---

## 18. 恢复日志检查

恢复日志中重点寻找：

```text
starting point-in-time recovery
restored log file
consistent recovery state reached
recovery stopping before/after transaction
```

历史已验证 PITR 曾看到类似语义：

```text
recovery stopping before commit of transaction ...
```

这类日志是 PITR 确实命中目标边界的重要证据。

---

## 19. 恢复验收通过后再决定是否切换生产

隔离恢复成功后，才决定：

```text
是否需要正式替换生产数据库
```

如果只是演练：

```text
保留证据
删除隔离实例
生产继续运行
```

如果是真实事故：

```text
停止生产写入
冻结旧故障实例
确认最终恢复目标
使用已验证恢复副本建立正式 PostgreSQL
重新配置业务连接
逐层恢复业务服务
```

正式切换属于高风险操作，应保留明确时间线和人工批准。

---

## 20. 正式恢复后的服务启动顺序

建议：

```text
PostgreSQL
    ↓
确认 DB healthy
    ↓
classification-result-writer
    ↓
classifier-worker
    ↓
candidate-orchestrator / upstream traffic
```

实际恢复时，应根据当前部署依赖关系调整。

核心原则：

```text
先确认数据库可写
再恢复 Result persistence
再恢复持续业务流量
```

不要在数据库尚未验证时直接恢复所有 producer 流量。

---

## 21. 恢复后的 Kafka 语义

数据库恢复到过去时间点后，Kafka 可能仍然包含恢复点之后已经产生的 Result。

因此必须明确：

```text
数据库时间点
和
Kafka committed offsets
```

并不自动同步回滚。

这是正式 PITR 切换时最重要的跨系统边界之一。

当前项目已经具备确定性 `run_id` 和幂等 persistence，因此部分 Result replay 可以安全归并，但不能未经评估就重置整个 consumer group。

正式事故恢复时必须单独确定：

- Result Writer committed offset；
- 恢复后的数据库已经包含哪些 Run；
- 哪些 Kafka Result 需要自然重放；
- 是否存在恢复点之后但 offset 已提交的数据。

不要简单执行：

```text
reset all offsets to earliest
```

---

## 22. CurrentClassification 验证

恢复后不仅要确认 `classification_runs`。

还必须确认：

```text
current_classifications
```

符合当前业务规则。

当前持久化语义：

- 更高 revision 才能推进 Current；
- 旧 revision 不覆盖；
- 同 revision 不覆盖；
- SHADOW / REPROCESS 不推进 Current。

因此 PITR 后如果发生 Result replay，应再次确认 Current 没有被错误推进。

---

## 23. WAL Archive / Mirror 自动化

当前已经启用：

```text
astro-postgres-wal-mirror.timer
```

约每 30 秒镜像 WAL 到：

```text
/data/astro-platform/backups/postgres/wal-archive-live
```

当前受控实测：

```text
SECOND_DISK_RPO_WITHIN_5MIN=PASS
```

实际结果曾为：

```text
source archive ≈239s
secondary mirror ≈251s
```

这只是当前服务器受控测试证据。

不能扩展解释为：

```text
整机灾难也保证 RPO≤5min
```

---

## 24. Daily Base Backup 自动化

当前已经启用：

```text
astro-postgres-basebackup.timer
```

每天自动运行 verified physical base backup。

backup 流程包括：

```text
pg_basebackup
    ↓
pg_verifybackup
    ↓
copy to .partial
    ↓
second verify
    ↓
rename to final base_*
```

只有验证成功的正式：

```text
base_*
```

目录才应进入恢复候选集合。

不要使用：

```text
.*.partial
```

作为恢复基础。

---

## 25. Retention 自动化

当前保留：

```text
最近 7 份成功 base backup
```

WAL 边界基于：

```text
最老仍保留 base backup
的 START WAL
```

而不是：

```text
文件 mtime
```

当前已经启用：

```text
astro-postgres-retention.timer
```

每天在 basebackup 之后运行。

真实生产第一次“有实际删除动作”的 retention 尚需自然积累到超过 7 份成功 backup 后发生。

在事故恢复期间，如果需要冻结恢复资产：

```text
应暂停 retention
```

避免事故分析过程中恢复边界继续变化。

---

## 26. 发生真实恢复事故时暂停自动清理

如果已经确认需要 PITR / disaster restore，建议先暂停：

```bash
sudo systemctl stop astro-postgres-retention.timer
```

必要时也可临时停止：

```bash
sudo systemctl stop astro-postgres-basebackup.timer
```

但 WAL archive / mirror 是否暂停需要根据事故类型判断。

如果生产 PostgreSQL 仍在继续产生有效 WAL，通常不应随意停止 WAL archive / mirror。

恢复完成后必须重新确认 timer 是否应恢复：

```bash
systemctl is-enabled astro-postgres-retention.timer
systemctl is-active astro-postgres-retention.timer
systemctl is-enabled astro-postgres-basebackup.timer
systemctl is-active astro-postgres-basebackup.timer
systemctl is-enabled astro-postgres-wal-mirror.timer
systemctl is-active astro-postgres-wal-mirror.timer
```

---

## 27. 恢复证据

每次真实或演练恢复至少记录：

```text
incident / drill id
selected base backup
backup START WAL
backup START TIME
backup manifest hash
recovery target time
WAL source
recovery start time
recovery reached target time
restore duration
pg_is_in_recovery
replay LSN
BEFORE marker result
AFTER marker result
classification_runs verification
current_classifications verification
final decision
```

当前历史服务器 PITR 证据已保留在：

```text
/home/zhouyuyang/astro-platform/postgres/recovery-evidence
```

---

## 28. RPO / RTO 的正确解释

当前服务器证据支持：

### RPO

对于：

```text
/dev/sdb 单盘损坏
+
/dev/sda 仍可用
```

受控 WAL mirror 测试已验证：

```text
RPO < 5 minutes
```

### RTO

当前小数据规模下：

```text
isolated PITR restore ≈ 11 seconds
```

但该数据量很小。

因此当前只能把：

```text
RTO ≤ 1h
```

作为当前规模下的 provisional / lab-supported recovery objective。

不能根据 11 秒结果推断未来大规模数据库仍然只需 11 秒。

---

## 29. 禁止操作

禁止在未冻结恢复目标前：

```text
删除 WAL
```

禁止直接修改原 base backup。

禁止恢复实验覆盖生产 PGDATA。

禁止只因为数据库能启动就宣布恢复成功。

禁止忽略：

```text
Kafka committed offset
```

与 PITR 数据时间点之间的关系。

禁止：

```text
直接 reset Result consumer group to earliest
```

作为通用 PITR 后处理。

禁止在没有验证：

```text
classification_runs
current_classifications
```

的情况下恢复全部业务流量。

---

## 30. 恢复完成条件

真实 PostgreSQL 恢复只有在以下条件满足后才能关闭：

```text
selected base backup verified
required WAL available
PITR reached intended target
recovered PostgreSQL healthy
business schema available
classification_runs verified
current_classifications verified
Kafka / DB recovery boundary reviewed
classification-result-writer restored
new Results persist successfully
consumer offsets advance safely
no abnormal Result DLQ growth
backup / WAL timers returned to intended state
recovery evidence saved
```

如果恢复目标涉及历史时间点，还必须确认：

```text
事故后不应存在的数据确实不存在
```

---

## 31. 升级条件

满足以下任一条件时，停止常规恢复并升级：

- 没有可验证的 base backup；
- required WAL 缺失；
- `pg_verifybackup` 失败；
- WAL archive 不连续；
- recovery 无法达到目标点；
- 恢复后业务表不一致；
- 恢复后 CurrentClassification 语义异常；
- 主盘和 `/data` 第二盘同时不可用；
- 怀疑控制器 / 整机故障；
- 需要跨主机 / 异地灾备；
- Kafka 与数据库恢复边界无法确定；
- 需要丢弃或大规模重放 Kafka 数据。

当前系统没有完整异机 PostgreSQL HA / DR 自动切换能力。

---

## 32. 当前工程边界

当前 PostgreSQL 恢复能力是：

```text
continuous WAL archive
+
physical base backup
+
pg_verifybackup
+
PITR
+
/dev/sda second-disk WAL mirror
+
daily verified base backup
+
bounded retention
```

当前没有：

```text
Patroni
streaming standby
automatic failover
cross-host backup target
object-storage backup
off-site disaster recovery
```

因此恢复设计的核心是：

```text
先保留和验证恢复资产
        ↓
隔离 PITR
        ↓
验证业务边界
        ↓
再决定生产切换
```

而不是：

```text
数据库出问题
        ↓
直接覆盖生产目录
```
