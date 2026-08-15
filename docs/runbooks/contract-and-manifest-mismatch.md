# Contract and Manifest Mismatch Runbook

本 Runbook 用于处理“变源候选体实时分类系统”中的 Model Bundle / Serving Manifest / Triton Metadata / Triton Config / Serving Contract 不一致问题。

适用组件：

- `classifier-worker`
- Serving Bundle Resolver
- Model Bundle Manifest
- Triton Inference Server
- `variable_star_classifier`
- `transformer_multihead`
- `xgb_coarse`
- `xgb_feature_extractor`

本 Runbook主要覆盖**启动门禁失败和模型发布后的契约不一致**。

---

## 1. 当前设计原则

classifier-worker 在进入正式 ClassificationCommand 消费之前，会先完成模型 Serving 相关启动门禁。

核心原则：

```text
manifest / bundle identity
        ↓
Triton metadata
        ↓
Triton config
        ↓
Serving Contract
        ↓
全部通过
        ↓
Worker ready
        ↓
开始正式消费
```

如果契约不一致：

```text
Worker 不应绕过检查继续运行
```

因为这类问题属于：

```text
部署 / 模型制品 / 配置 / 契约错误
```

而不是单条 Kafka 消息的 transient failure。

---

## 2. 适用场景

适用于：

- classifier-worker 启动失败；
- Worker `/ready` 长时间不能进入 ready；
- Serving Bundle 无法解析；
- Model Bundle version 与实际部署不一致；
- Triton model metadata 与预期 tensor contract 不一致；
- Triton model config 与预期不一致；
- 输入 / 输出 tensor 名称、datatype、shape 不一致；
- Triton model name / version 不一致；
- `XGBOOST_EXECUTED` 等输出语义与契约不一致；
- 新模型发布后 Worker 无法通过 startup gate；
- Model Repository release 与运行配置没有同步切换。

不适用于：

- Triton 单纯暂时 unavailable；
- LightCurve transient failure；
- PostgreSQL unavailable；
- 单条 ClassificationCommand 永久非法。

Triton 单纯不可用时应使用：

```text
classifier-worker-dependency-unavailable.md
```

---

## 3. 当前验证状态

### Triton Serving Contract

当前已完成：

```text
VERIFIED_CI_AND_SERVER_GATE
```

意味着：

- CI 已验证 metadata / config / inference contract 逻辑；
- 真实 classifier-worker 启动路径已经执行 Triton serving contract gate；
- 真实服务器启动时能够通过该 gate。

### Manifest / Bundle 校验

当前已完成：

```text
VERIFIED_CI
```

包括：

- Serving Bundle manifest 加载；
- 基础严格校验；
- 模型 bundle identity；
- taxonomy / XGBoost / feature schema 等已实现约束。

但当前没有单独记录一套真实服务器“故意破坏 manifest 后再恢复”的独立 fault injection。

因此：

```text
Manifest mismatch = VERIFIED_CI
```

不能写成独立：

```text
SERVER_FAULT_INJECTION_VERIFIED
```

---

## 4. 常见根因

### Model Repository Release 切换不完整

例如：

```text
Worker 配置已经指向新 model bundle
但 Triton 仍运行旧 model_repository
```

或者反过来：

```text
Triton 已切到新 release
Worker 仍使用旧 manifest
```

### ONNX 更新但 Config 未同步

例如 Transformer 后训练后：

```text
model.onnx 已更新
config.pbtxt 仍是旧契约
```

### Python Backend 与模型版本不一致

例如：

```text
variable_star_classifier/model.py
pipeline.py
inference_contract.py
```

与 Transformer / XGBoost 制品不属于同一 immutable release。

### Tensor Contract 变化

例如：

- tensor 名称改变；
- datatype 改变；
- shape 改变；
- 输出数量改变；
- 输出语义改变；
- REUSED_COARSE_PROBS 等输入契约改变。

### Model Name / Version 不一致

Worker 期待的模型名称或版本与 Triton 实际加载值不一致。

### Manifest 文件损坏或手工修改

例如：

```text
字段缺失
版本不匹配
非法值
未知部署组合
```

---

## 5. 典型现象

可能看到：

```text
classifier-worker 启动失败
```

或者：

```text
/live 可用
/ready 不可用
```

如果失败发生在 runtime composition 完成前，甚至可能：

```text
management listener 尚未 ready
```

结构化日志中应重点寻找：

```text
serving bundle
manifest
metadata
config
contract
triton
model
version
startup
```

不要把这类 startup gate failure 当成 Kafka consumer stuck。

如果 Worker 根本没有完成启动：

```text
它可能还没有进入 consumer group
```

此时 Command lag 增长只是下游容量暂时为 0 的结果。

---

## 6. 首先确认当前部署身份

不要凭目录名猜测。

需要确认：

```text
当前 classifier-worker 使用的 Model Bundle version
当前 Serving Bundle manifest
当前 Triton model repository release
当前 Triton image
当前 Triton model name / version
```

### Triton 当前 repository

当前服务器基线中，Triton 使用：

```text
--model-repository=/models
```

`/models` 来自宿主机只读 bind mount。

当前已审计 release：

```text
b4e811dc99cb-final-d4f0cd7f98bd-timeorder-7bd80afc87bf
```

但事故排查时不要永远假设仍是这个 release。

应使用：

```bash
docker inspect <triton-container> \
  --format '{{range .Mounts}}type={{.Type}} source={{.Source}} destination={{.Destination}} rw={{.RW}}{{println}}{{end}}'
```

确认当前实际 source。

---

## 7. 检查 Triton 当前运行命令

查看：

```bash
docker exec <triton-container> \
  sh -lc 'tr "\0" " " </proc/1/cmdline; echo'
```

确认：

```text
--model-repository
--model-control-mode（如存在）
```

当前项目基线使用默认：

```text
model-control-mode = none
```

即：

```text
启动时加载模型
运行期间不依赖 Model Repository API 动态切换
```

因此更新 model repository 后，如果没有 recreate / restart Triton：

```text
不要假设新模型已经被加载
```

---

## 8. 检查 Triton Readiness

先检查：

```bash
curl -fsS \
  http://127.0.0.1:18000/v2/health/ready \
  >/dev/null \
  && echo 'triton_ready=PASS' \
  || echo 'triton_ready=FAIL'
```

如果 Triton 本身不 ready：

先处理 Triton startup / model loading 问题。

如果 Triton ready，但 Worker contract gate 失败：

重点检查：

```text
Triton 实际契约
vs
Worker / manifest 预期契约
```

---

## 9. 检查模型是否实际加载

可使用 Triton HTTP API 查询 model metadata / readiness。

至少确认：

```text
variable_star_classifier
transformer_multihead
xgb_coarse
xgb_feature_extractor
```

是否处于预期状态。

不要只检查 server-level `/ready`。

如果某个必需模型没有加载：

```text
Serving Contract 不成立
```

---

## 10. 检查 Model Metadata

重点比较：

```text
model name
model version
inputs
outputs
datatype
shape
```

不要只判断：

```text
HTTP 200
```

因为 HTTP 200 只说明 API 请求成功，不代表返回契约和 Worker 预期一致。

---

## 11. 检查 Model Config

重点比较：

```text
input name
output name
data_type
dims
max_batch_size
backend / platform
version policy（如配置）
```

当前统一入口第一版是：

```text
max_batch_size = 0
```

即单对象请求，不使用 Triton server-side batching。

如果新模型发布时擅自改成其他 batching 语义：

```text
需要重新进行 Serving Contract 评审
```

不能只改配置然后上线。

---

## 12. 当前关键 Tensor Contract

当前 classifier adapter 会把 ClassificationInput 映射到固定 Triton Binary Tensor 请求。

尤其注意：

```text
REUSED_COARSE_PROBS
```

当前语义：

- 固定 `[7]`；
- FP32；
- REUSE_PREVIOUS 时发送历史七维粗概率；
- 非 REUSE 模式发送七个 0。

如果新模型版本改变这一约定：

```text
不仅是“换一个 ONNX”
```

而是：

```text
Serving Contract 变化
```

需要同步修改：

- manifest；
- Triton config；
- adapter；
- tests；
- deployment；
- Runbook / contract evidence。

---

## 13. 输出契约

当前系统按名称解析 Triton 输出。

输出不能只满足：

```text
数量一样
```

还必须满足：

```text
名称
datatype
shape
语义
```

例如：

```text
XGBOOST_EXECUTED
```

属于业务 wire contract 的一部分。

如果新模型让这个输出语义发生变化：

```text
必须视为 contract change
```

不能作为普通模型权重升级处理。

---

## 14. Manifest 排查

发生 manifest mismatch 时，先定位当前实际 manifest 文件。

不要先修改生产文件。

检查：

- 文件路径；
- 文件内容；
- model bundle version；
- Triton endpoint / model identity；
- taxonomy version；
- XGBoost version；
- feature schema version；
- 与当前 immutable model repository release 的对应关系。

如果 manifest 是旧版本：

```text
优先恢复部署一致性
```

不要为了让 gate 通过而随意删除校验字段。

---

## 15. 模型发布导致的 Mismatch

模型升级推荐采用：

```text
old immutable release
        ↓ copy
new immutable release
        ↓
替换目标模型
        ↓
隔离 Triton
        ↓
ready
        ↓
metadata / config
        ↓
Serving Contract
        ↓
Golden inference
        ↓
正式切换
```

如果正式切换后 Worker startup gate 失败：

优先判断：

```text
新 release 未通过完整发布验证
```

而不是：

```text
Worker gate 太严格
```

不要第一时间关闭 gate。

---

## 16. 正确处理动作：回滚优先

如果新的 Model Repository release 无法快速确认正确：

推荐：

```text
停止 / recreate 当前异常 Triton
        ↓
重新挂载 previous immutable release
        ↓
启动 Triton
        ↓
/ready PASS
        ↓
Serving Contract PASS
        ↓
classifier-worker 恢复启动
```

当前旧 release 应在新版本稳定前保留。

不要在生产 repository 中：

```text
现场覆盖 model.onnx
现场编辑 config.pbtxt
现场修改 Python backend
```

来“试着修好”。

---

## 17. 为什么不绕过 Startup Gate

如果忽略 contract mismatch 直接运行 Worker，可能造成：

```text
输入 tensor 映射错误
输出 tensor 被错误解释
概率索引错位
模型版本身份与 Run 记录不一致
错误的分类结果写入 Kafka / PostgreSQL
```

这类问题比：

```text
Worker 暂时不消费
```

严重得多。

当前系统选择：

```text
fail closed
```

而不是：

```text
contract mismatch 时继续推理
```

---

## 18. 与 Kafka Lag 的关系

如果 Worker 因 startup gate 无法启动：

```text
ClassificationCommand lag
```

可能持续增长。

正确处理：

```text
修复 model / manifest / contract deployment
```

错误处理：

```text
reset Command offsets
删除 Command
把消息塞入 DLQ
```

因为：

```text
Kafka 消息本身通常没有错
```

问题发生在部署契约。

---

## 19. 与 Command DLQ 的关系

Startup contract mismatch 不应该导致所有 Command 被批量送入 Command DLQ。

如果 Worker 根本未通过 startup gate：

```text
最好不要开始消费
```

而不是：

```text
开始消费
→ 每条都 PERMANENT
→ 全部 DLQ
```

如果发现 deployment mismatch 引发大量 Command DLQ：

```text
应升级为严重错误分类 / startup gate 边界问题
```

---

## 20. Triton 暂时不可用与 Contract Mismatch 的区别

### Triton unavailable

例如：

```text
connection refused
timeout
5xx
server temporarily down
```

通常属于：

```text
DEPENDENCY_UNAVAILABLE
RETRYABLE
```

处理：

```text
恢复 Triton
→ Worker 自动继续
```

### Contract mismatch

例如：

```text
metadata shape 不匹配
tensor name 不匹配
model version 不匹配
manifest identity 不匹配
```

属于：

```text
部署契约问题
```

不能无限 retry 期待它“自己恢复”。

必须修正 deployment / model release / manifest。

---

## 21. 推荐诊断顺序

发生 mismatch 时按以下顺序：

```text
1. 当前 Worker 配置
2. 当前 manifest
3. 当前 Triton container
4. 当前 Triton image
5. 当前 model repository bind mount source
6. Triton server readiness
7. 必需模型 readiness
8. model metadata
9. model config
10. Serving Contract gate 输出
11. Golden inference
12. release / manifest 是否属于同一发布版本
```

不要从：

```text
修改代码
```

开始。

大多数这类事故首先是：

```text
部署版本组合错误
```

---

## 22. Golden Inference

如果 contract 检查通过但仍怀疑模型 release 有问题：

运行固定科学样本 / Golden inference。

至少确认：

```text
请求成功
输出 tensor 完整
概率 shape 正确
粗分类 / 叶子分类索引正确
XGBOOST_EXECUTED 语义正确
没有 NaN / Inf 等明显异常
```

Serving Contract PASS 只能证明：

```text
接口兼容
```

不能证明：

```text
科学性能更好
```

科学性能需要独立评估证据。

---

## 23. Transformer 后训练发布

如果只重新训练了 Transformer：

不要认为只需要：

```text
覆盖 transformer_multihead/1/model.onnx
```

推荐：

```text
创建新完整 release
        ↓
放入新 Transformer ONNX
        ↓
保留与其兼容的 classifier / XGBoost / config
        ↓
验证完整 repository
```

如果 ONNX 的 tensor contract 没有变化：

```text
它可以是模型权重升级
```

如果 tensor contract 变化：

```text
它是 Serving Contract 变更
```

两者运维风险不同。

---

## 24. 当前为什么不使用 POLL

Triton 支持 POLL model management。

当前项目不使用，原因：

- 模型发布频率较低；
- model repository 是完整 immutable release；
- 大文件复制中间状态需要额外同步；
- 自动 poll 可能增加发布时序复杂度；
- recreate 的短暂 unavailable 已被 Worker retry 安全覆盖；
- rollback 旧 release 更简单。

因此当前：

```text
POLL = 不启用
```

---

## 25. 当前为什么不使用 EXPLICIT

Triton 支持 explicit model control API。

当前阶段不启用，原因：

- 需要额外控制 load / unload API 权限；
- 需要自动化 model publish orchestration；
- 需要 load 后 readiness / contract / golden gate；
- 需要失败 rollback 编排；
- 当前模型发布频率不足以证明增加这些复杂性有收益。

未来模型发布频率提高后可以重新评估。

---

## 26. 恢复确认

Mismatch 解决后必须确认：

### Triton

```text
/v2/health/ready = PASS
```

### 必需模型

```text
variable_star_classifier ready
transformer_multihead ready
xgb_coarse ready
xgb_feature_extractor ready
```

### Contract

```text
Serving Contract gate = PASS
```

### Worker

```text
classifier-worker 启动成功
/ready = PASS
```

### Kafka

```text
ClassificationCommand 消费恢复
lag 停止增长
lag 开始下降或恢复正常
```

### Result

```text
ClassificationResult 正常发布
```

### Writer / PostgreSQL

```text
Result 正常持久化
```

### DLQ

```text
没有因为 deployment mismatch 出现异常 Command DLQ 增长
```

---

## 27. 升级条件

满足任一条件时应升级工程排查：

- previous known-good release 也无法通过 Serving Contract；
- Triton ready，但 metadata/config 与 repository 文件明显不一致；
- Worker 使用的 manifest identity 无法确认；
- release 中多个模型来自不同不可追溯来源；
- tensor contract 发生变化但没有对应代码 / manifest 变更；
- Golden inference 失败；
- contract pass 但科学结果异常；
- 回滚旧 release 后仍无法恢复；
- Model Repository 本身损坏；
- `/dev/sdb2` model repository 丢失且没有可恢复来源。

最后一种情况对应当前：

```text
Model Artifact Recovery = DEFERRED
```

需要进入更高等级的模型制品恢复处理。

---

## 28. 禁止操作

禁止：

```text
关闭 Serving Contract gate 以便 Worker 启动
```

禁止：

```text
直接覆盖正在运行 release 中的 ONNX
```

禁止：

```text
在生产 model_repository 中现场拼装不同版本模型
```

禁止：

```text
为了清 Kafka lag reset Command offset
```

禁止：

```text
把部署 mismatch 对应 Command 批量送入 DLQ
```

禁止：

```text
只验证 Triton /ready 就宣布模型发布成功
```

---

## 29. 事故记录建议

至少记录：

```text
incident start time
worker version / commit
model bundle version
manifest path
model repository release
triton image
triton container
triton model names / versions
failed gate
metadata/config mismatch detail
golden result
rollback release
recovery time
Kafka lag impact
DLQ impact
```

不要记录：

- Secret；
- Kafka SASL password；
- PostgreSQL password；
- 不必要的完整业务 payload。

---

## 30. 当前工程边界

当前模型发布与契约策略：

```text
immutable model_repository release
+
Triton default none model control
+
Triton recreate
+
startup Serving Contract gate
+
Golden inference
+
previous release rollback
```

当前没有：

```text
Model Registry 自动发布流水线
Triton POLL 自动 reload
Triton EXPLICIT 自动 load/unload
自动 canary model rollout
自动 model rollback controller
```

这些不是当前故障恢复的前提。

当前最重要的原则是：

```text
模型发布失败
→ 回滚 deployment

而不是

模型发布失败
→ 绕过 contract
```
