# Disk Space Runbook

本 Runbook 用于处理“变源候选体实时分类系统”中的磁盘空间不足、文件系统接近满、PostgreSQL WAL / Base Backup 增长、Docker 存储增长，以及模型制品占用导致的容量风险。

适用范围：

- `/dev/sdb2` 根文件系统；
- `/data`（`/dev/sda`）；
- PostgreSQL PGDATA；
- PostgreSQL WAL archive；
- PostgreSQL second-disk WAL mirror；
- PostgreSQL daily base backup；
- Docker image / container / log storage；
- Triton model repository；
- 运行期临时文件和恢复证据。

本 Runbook 主要用于**容量判断与安全处置**。

它不是文件清理脚本，也不能代替 PostgreSQL recovery / retention 语义。

---

## 1. 当前服务器存储基线

当前已审计到两个主要文件系统：

```text
/dev/sdb2
mount: /
role:
  PostgreSQL 主数据
  PostgreSQL 源 WAL archive
  当前 Triton model repository
  Docker / 系统运行文件
```

以及：

```text
/dev/sda
mount: /data
role:
  PostgreSQL second-disk WAL mirror
  PostgreSQL daily physical base backup
  PostgreSQL verified recovery copy
  未来其他同机异盘恢复资产
```

历史审计时：

```text
/dev/sdb2:
  total ≈ 438 GiB
  used ≈ 135 GiB
  available ≈ 281 GiB
  usage ≈ 33%
```

`/data` 曾达到约：

```text
85% used
```

因此 `/data` 的 bounded retention 是必要能力，而不是可选优化。

这些值是历史审计基线，不应作为永久实时值。

故障判断必须重新执行当前磁盘检查。

---

## 2. 关键目录

### PostgreSQL PGDATA

```text
/home/zhouyuyang/astro-platform/postgres/data
```

位于：

```text
/dev/sdb2
```

### PostgreSQL 源 WAL Archive

```text
/home/zhouyuyang/astro-platform/postgres/archive
```

位于：

```text
/dev/sdb2
```

### PostgreSQL Second-Disk WAL Mirror

```text
/data/astro-platform/backups/postgres/wal-archive-live
```

位于：

```text
/dev/sda
```

### PostgreSQL Daily Base Backup

```text
/data/astro-platform/backups/postgres/basebackups-live
```

位于：

```text
/dev/sda
```

### PostgreSQL Frozen Recovery Evidence / Snapshot

位于：

```text
/home/zhouyuyang/astro-platform/postgres/recovery-evidence
```

以及：

```text
/data/astro-platform/backups/postgres/
```

下的已验证恢复资产。

### Triton Model Repository

当前生产 repository：

```text
/home/zhouyuyang/variable-star-deploy/releases/
b4e811dc99cb-final-d4f0cd7f98bd-timeorder-7bd80afc87bf/
model_repository
```

位于：

```text
/dev/sdb2
```

当前审计大小约：

```text
582 MiB
```

当前模型制品 second-disk recovery：

```text
DEFERRED
```

---

## 3. 建议的运维容量等级

以下阈值作为本 Runbook 的 S8 运维默认值，不是业务 SLO：

```text
< 80%
  normal

>= 80%
  WARNING

>= 90%
  CRITICAL

>= 95%
  EMERGENCY
```

同时必须观察：

```text
available bytes
```

而不能只看百分比。

例如大盘即使只用了 85%，仍可能有大量可用空间；但如果增长速度很快，也需要立即处理。

真正需要关注的是：

```text
当前使用率
+
剩余空间
+
增长速率
+
增长来源
```

---

## 4. 为什么磁盘满是严重故障

### PostgreSQL 主盘满

可能导致：

- WAL 无法写入；
- checkpoint / temp file 失败；
- database write 失败；
- PostgreSQL 异常；
- classification-result-writer 持续 RETRYABLE；
- Result Kafka lag 增长。

### WAL Archive 目录满

可能导致：

```text
archive_command failure
```

随后：

```text
pg_stat_archiver.failed_count ↑
```

同时 PostgreSQL 可能因为 WAL 不能安全归档而持续保留 `pg_wal` 中的 WAL。

如果长期无法归档：

```text
PGDATA 本身也可能继续增长
```

形成二次磁盘压力。

### `/data` 满

可能导致：

- WAL mirror 失败；
- daily basebackup 失败；
- `.partial` backup 残留；
- retention 无法正常运行；
- second-disk RPO 能力下降。

### Docker 存储满

可能导致：

- 新 container 无法创建；
- image pull / build 失败；
- container writable layer 写失败；
- 日志异常；
- 服务重启失败。

---

## 5. 第一判断：哪个文件系统在增长

先执行：

```bash
df -hT /
df -hT /data
```

以及：

```bash
df -i /
df -i /data
```

需要同时排除：

```text
block exhaustion
inode exhaustion
```

记录：

```text
filesystem
total
used
available
use%
inode use%
timestamp
```

不要看到“磁盘不足”就直接开始删文件。

---

## 6. 快速目录占用定位

### `/dev/sdb2`

先看已知业务目录：

```bash
du -sh \
  /home/zhouyuyang/astro-platform/postgres/data \
  /home/zhouyuyang/astro-platform/postgres/archive \
  /home/zhouyuyang/astro-platform/postgres/basebackups \
  /home/zhouyuyang/variable-star-deploy/releases \
  2>/dev/null
```

Docker：

```bash
docker system df
```

如需进一步确认：

```bash
sudo du -x -h -d 1 /var/lib/docker 2>/dev/null | sort -h
```

### `/data`

检查：

```bash
sudo du -h -d 3 \
  /data/astro-platform/backups \
  2>/dev/null \
  | sort -h
```

优先识别：

```text
wal-archive-live
basebackups-live
verified PITR snapshot
其他历史恢复资产
```

---

## 7. PostgreSQL WAL Archive 增长判断

检查：

```bash
sudo du -sh \
  /home/zhouyuyang/astro-platform/postgres/archive

sudo find \
  /home/zhouyuyang/astro-platform/postgres/archive \
  -maxdepth 1 \
  -type f \
  | wc -l
```

查看 archiver：

```sql
SELECT
    archived_count,
    last_archived_wal,
    last_archived_time,
    failed_count,
    last_failed_wal,
    last_failed_time
FROM pg_stat_archiver;
```

重点判断：

```text
failed_count 是否增长
last_archived_time 是否停止推进
archive 目录是否仍持续增长
```

---

## 8. archive_timeout 的正确理解

当前：

```text
archive_timeout = 240s
```

这不意味着：

```text
完全空闲时每 4 分钟固定生成一个 16 MiB WAL
```

历史服务器审计已经观察到：

- 数据库完全空闲时不会无条件每 240 秒创建新 segment；
- 有 WAL 活动并达到 archive timeout 条件时，可能触发 WAL switch；
- WAL 文件仍按完整 segment 大小存储。

因此不能用：

```text
360 segments/day
```

作为“实际每天必然增长量”。

它只能作为持续有低水平 WAL 活动时的容量上界参考之一。

---

## 9. WAL Mirror 增长判断

检查：

```bash
sudo du -sh \
  /data/astro-platform/backups/postgres/wal-archive-live
```

以及：

```bash
sudo find \
  /data/astro-platform/backups/postgres/wal-archive-live \
  -maxdepth 1 \
  -type f \
  | wc -l
```

正常情况下：

```text
source archive
和
second-disk mirror
```

应接近同步。

如果 source 持续增加而 mirror 不增加：

检查：

```text
astro-postgres-wal-mirror.timer
astro-postgres-wal-mirror.service
```

以及日志：

```bash
systemctl is-enabled astro-postgres-wal-mirror.timer
systemctl is-active astro-postgres-wal-mirror.timer

sudo journalctl \
  -u astro-postgres-wal-mirror.service \
  --since '-30 minutes' \
  --no-pager
```

---

## 10. WAL Mirror 当前恢复作用

当前 WAL mirror：

```text
source:
/home/zhouyuyang/astro-platform/postgres/archive

destination:
/data/astro-platform/backups/postgres/wal-archive-live
```

约每 30 秒运行。

历史服务器受控测试：

```text
source archive ≈ 239s
second-disk mirror ≈ 251s
```

因此在：

```text
/dev/sdb 单盘故障
且 /dev/sda 正常
```

这一受控场景下已经得到：

```text
RPO < 5min
```

不能为了释放 `/data` 空间随意删除：

```text
wal-archive-live
```

中的最近 WAL。

---

## 11. Base Backup 增长判断

当前 daily physical base backup：

```text
/data/astro-platform/backups/postgres/basebackups-live
```

查看：

```bash
find \
  /data/astro-platform/backups/postgres/basebackups-live \
  -mindepth 1 \
  -maxdepth 1 \
  -type d \
  -name 'base_*' \
  -printf '%f\n' \
  | sort
```

查看大小：

```bash
du -sh \
  /data/astro-platform/backups/postgres/basebackups-live/base_*
```

当前策略：

```text
保留最近 7 份成功 base backup
```

不要手工按：

```text
mtime
```

删除旧 backup。

---

## 12. Base Backup `.partial` 目录

正常成功 backup 会：

```text
生成临时 backup
    ↓
verify
    ↓
copy 到 .partial
    ↓
再次 verify
    ↓
rename 为 base_*
```

所以如果出现：

```text
.*.partial
```

说明：

```text
backup 未正常完成
```

不要把 `.partial` 当恢复资产。

先检查：

```bash
sudo journalctl \
  -u astro-postgres-basebackup.service \
  --since '-24 hours' \
  --no-pager
```

确认失败原因。

如果确认是失败后的孤儿 `.partial`：

应在人工确认：

```text
没有正在运行的 basebackup service
且该目录不是有效正式 backup
```

之后再清理。

不能按 cron / find 自动删除未知 `.partial`。

---

## 13. Retention 当前安全语义

当前：

```text
astro-postgres-retention.timer
```

每天运行。

策略：

```text
KEEP_BACKUPS = 7
```

只有：

```text
backup_count > 7
```

才会开始真实删除旧 base backup。

WAL retention 边界来自：

```text
最老仍保留 base backup 的 START WAL
```

不是：

```text
文件年龄
```

生产已验证：

```text
backup_count=1
→ PASS_APPLY_NOOP
```

隔离 fixture 已验证：

```text
9 backups
→ delete oldest 2
→ keep latest 7
→ WAL boundary = oldest retained backup
```

---

## 14. 磁盘压力下不要绕过 Retention 逻辑

即使 `/data` 接近满，也禁止：

```bash
find /data/.../wal-archive-live -mtime +7 -delete
```

禁止：

```bash
rm -rf old WAL
```

禁止根据：

```text
“看起来很老”
```

来删 WAL。

原因：

某个最老仍保留 base backup 可能仍需要这些 WAL 才能 PITR。

如果必须紧急释放空间：

```text
先确认 retention 边界
```

再使用已有 retention 机制。

---

## 15. Retention 运行检查

检查：

```bash
systemctl is-enabled astro-postgres-retention.timer
systemctl is-active astro-postgres-retention.timer
```

查看：

```bash
sudo journalctl \
  -u astro-postgres-retention.service \
  --since '-24 hours' \
  --no-pager
```

正常可能看到：

```text
retention_mode=APPLY
backup_count=...
basebackup_delete_count=...
oldest_required_wal=...
retention_status=PASS_APPLY
```

或者：

```text
PASS_APPLY_NOOP
```

如果 retention 持续失败：

优先解决失败原因。

不要额外写临时删除脚本绕过它。

---

## 16. Shared WAL / Retention Lock

WAL mirror 与 retention 当前共享：

```text
/run/lock/astro-postgres-wal-retention.lock
```

目的是避免：

```text
retention 正在清理旧 archive
同时
rsync 正在读取相同目录
```

因此如果 retention 因 lock timeout 跳过：

```text
不代表需要立刻手工删除
```

下一次日程可以再次执行。

WAL mirror 优先级更高，因为它直接服务 second-disk RPO。

---

## 17. `/data` 接近满时的安全处理顺序

推荐顺序：

```text
1. 确认当前 df / inode
2. 确认增长来源
3. 检查 retention 是否正常
4. 检查失败的 .partial backup
5. 检查是否存在额外人工测试 copy
6. 检查过期的隔离恢复实验目录
7. 再决定是否有明确可删除对象
```

不要第一步就清 WAL。

---

## 18. 可优先评估清理的对象

以下对象**可以被评估是否清理**，但仍需人工确认：

### 已完成的临时测试恢复目录

例如：

```text
isolated PITR restore working copy
```

如果已经：

- 验证完成；
- 证据已保存；
- 不再承担恢复用途；

可以清理 working copy。

### 明确失败的 `.partial` Base Backup

条件：

```text
basebackup service 已结束
该目录未通过 verify
不属于正式 base_*
```

### 明确不用的 Docker build cache

先检查：

```bash
docker system df
```

再决定是否：

```text
prune build cache
```

不要使用：

```bash
docker system prune -a --volumes
```

作为默认磁盘清理命令。

---

## 19. 不应随意清理的对象

禁止未经恢复评估删除：

```text
PostgreSQL PGDATA
source WAL archive
second-disk WAL mirror
最近 7 份成功 base backup
当前 Triton model repository
当前 Docker production image
PostgreSQL recovery evidence
```

如果这些对象确实需要回收：

必须先证明：

```text
有其他可恢复来源
```

---

## 20. Docker 磁盘增长

先检查：

```bash
docker system df
```

确认占用来源：

```text
images
containers
local volumes
build cache
```

### Build Cache

如果只是服务器反复构建 Go image 产生 build cache：

可以评估：

```bash
docker builder prune
```

但执行前应确认：

```text
不会影响当前 running container
```

### Images

不要只因为：

```text
old image 看起来没在运行
```

就立即删除。

旧 image 可能承担：

```text
快速 rollback
```

特别是：

```text
Triton image
业务 daemon image
```

应先确认 rollback 需求。

---

## 21. Container Logs

Docker `json-file` 日志如果没有 rotation，也可能增长。

检查：

```bash
docker inspect <container> \
  --format '{{.LogPath}}'
```

再检查大小：

```bash
sudo ls -lh <log-path>
```

不要直接：

```text
rm 当前 container log file
```

需要结合 Docker logging driver / rotation 方式处理。

如果日志增长成为主要容量问题，应在后续部署配置中增加受控 rotation，而不是靠人工定期删除。

---

## 22. Triton Model Repository 空间

当前生产 model repository：

```text
≈ 582 MiB
```

当前只读挂载。

如果未来模型 release 持续增加：

```text
releases/
release-A
release-B
release-C
...
```

需要建立 model release retention。

但当前：

```text
Model Artifact Recovery = DEFERRED
```

因此不要为了节省空间过早删除：

```text
previous known-good release
```

至少当前发布 / rollback Runbook 需要上一稳定 release。

---

## 23. PostgreSQL PGDATA 增长

如果主要增长来自：

```text
postgres/data
```

不要手工删除 PGDATA 内文件。

需要从 PostgreSQL 内部分析：

- database size；
- table size；
- index size；
- temporary usage；
- WAL accumulation；
- dead tuples；
- long transaction。

例如：

```sql
SELECT pg_size_pretty(pg_database_size(current_database()));
```

进一步表级容量分析可以单独执行。

不要：

```text
在 PGDATA 目录直接 rm 文件
```

---

## 24. pg_wal 异常增长

如果：

```text
PGDATA 增长很快
```

并发现：

```text
pg_wal
```

很大：

优先检查：

```text
archive_command 是否失败
replication slot 是否阻止 WAL 回收
长时间 backup / replication 是否存在
```

当前项目没有生产 replication slot 依赖作为主要架构。

但仍应实际查询，不要假设没有。

禁止：

```text
手工删除 PGDATA/pg_wal 下的 WAL
```

这可能直接破坏数据库。

---

## 25. 磁盘空间和 PostgreSQL Archiver

如果 `/dev/sdb2` 接近满，同时：

```text
pg_stat_archiver.failed_count 增长
```

这是高风险组合。

优先处理：

```text
为什么 archive_command 失败
```

而不是：

```text
删除 pg_wal
```

如果 archive destination 本身空间不足：

需要先从已验证 retention / second-disk recovery 边界判断可回收对象。

---

## 26. WARNING 处置

当某文件系统达到：

```text
>= 80%
```

建议：

1. 记录当前 `df`；
2. 记录 inode；
3. 找到增长最快目录；
4. 检查相关 timer / retention；
5. 估算增长速度；
6. 在达到 90% 前完成容量处置。

WARNING 不代表必须立即删除数据。

它意味着：

```text
容量风险需要进入运维处理
```

---

## 27. CRITICAL 处置

当达到：

```text
>= 90%
```

或者剩余空间已经不足以承受近期增长：

应：

1. 停止非必要的大文件操作；
2. 暂停新的人工 backup / restore 实验；
3. 查明增长来源；
4. 确认 PostgreSQL archiver 是否正常；
5. 确认 `/data` backup / mirror 是否仍工作；
6. 清理明确安全的临时资产；
7. 必要时扩容。

如果是 `/data`：

```text
不要停 WAL mirror 来“节省空间”
```

这只会失去恢复副本，而不会解决源 WAL 增长。

---

## 28. EMERGENCY 处置

当达到：

```text
>= 95%
```

或者预计很快耗尽：

这是高优先级事故。

优先目标：

```text
避免 PostgreSQL / Kafka / Docker 因 ENOSPC 崩溃
```

应立即：

- 停止非必要测试；
- 停止新的大型 build；
- 停止新的人工模型复制；
- 暂停非必要恢复实验；
- 保留业务主链和 WAL mirror；
- 清理已经明确验证可删除的临时工作目录；
- 评估扩容 / 增加存储。

如果不能确认某个 WAL / backup 是否安全删除：

```text
不要删
```

升级人工判断。

---

## 29. 磁盘故障与“空间不足”不是同一个问题

如果看到：

```text
I/O error
filesystem error
device error
read-only filesystem
```

不能只按“空间不足”处理。

这可能是：

```text
磁盘 / 控制器 / 文件系统故障
```

需要立即升级。

特别是：

```text
/dev/sdb
```

当前同时承载：

- PostgreSQL 主数据；
- 源 WAL；
- Triton model repository。

如果 `/dev/sdb` 故障：

PostgreSQL 可以依赖 `/dev/sda` 的已验证恢复资产。

但 Triton Model Artifact Recovery 当前仍：

```text
DEFERRED
```

因此模型制品恢复风险更高。

---

## 30. 恢复确认

完成容量处置后至少确认：

### Filesystem

```text
usage 回落
available bytes 恢复到安全范围
inode 正常
```

### PostgreSQL

```text
astro-postgres healthy
pg_stat_archiver.failed_count 不再增长
last_archived_time 正常推进
```

### WAL Mirror

```text
astro-postgres-wal-mirror.timer enabled
astro-postgres-wal-mirror.timer active
mirror service success
```

### Base Backup

```text
astro-postgres-basebackup.timer enabled
astro-postgres-basebackup.timer active
```

### Retention

```text
astro-postgres-retention.timer enabled
astro-postgres-retention.timer active
```

### Application

```text
classification-result-writer 正常持久化
classifier-worker 正常处理
Kafka lag 没有因为存储问题持续增长
```

### Triton

如果处理涉及 model release / Docker：

```text
Triton /ready = PASS
```

---

## 31. 升级条件

出现以下任一条件时应升级：

- `/` 或 `/data` >= 95%；
- 空间持续增长但无法找到来源；
- PostgreSQL archive failure 持续；
- `pg_wal` 持续异常增长；
- `/data` WAL mirror 停止推进；
- daily basebackup 持续失败；
- retention 持续失败；
- inode 接近耗尽；
- filesystem 变成 read-only；
- 出现 I/O error；
- Docker 无法创建 / 启动 container；
- PostgreSQL 因 ENOSPC 报错；
- `/dev/sdb` 存储故障；
- `/dev/sda` 与 `/dev/sdb` 同时出现异常；
- 需要删除当前 recovery boundary 内的 WAL 才能腾空间。

最后一种情况不能由普通 Runbook 操作员自行处理。

---

## 32. 事故记录建议

至少记录：

```text
incident start time
filesystem
device
usage %
available bytes
inode usage
largest directories
growth source
postgres archiver status
WAL archive size
WAL mirror size
basebackup count / size
docker storage size
model repository size
actions taken
files deleted
retention boundary used
recovery confirmation
```

如果删除了任何恢复资产：

必须明确记录：

```text
为什么安全
依据哪个恢复边界
删除了什么
```

---

## 33. 当前工程边界

当前已具备：

```text
PostgreSQL bounded retention
daily physical basebackup
continuous WAL archive
30s second-disk WAL mirror
shared WAL/retention flock
Triton immutable model release 运行方式
Docker image 化部署
```

当前没有：

```text
自动磁盘扩容
对象存储 backup
跨主机 backup
自动 Docker log retention 全局策略
自动 model artifact retention
Kubernetes ephemeral-storage 管理
```

当前磁盘空间原则：

```text
先确认增长来源
        ↓
保护业务和恢复边界
        ↓
优先清理明确的临时 / 可再生成资产
        ↓
使用既有 retention
        ↓
必要时扩容
```

而不是：

```text
磁盘快满
        ↓
随便删除 WAL / backup / model
```
