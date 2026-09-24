# S11-08E3 Triton 容量感知自动伸缩验收记录

- 验收日期：2026-09-24（北京时间，UTC+08:00）
- 验收状态：VERIFIED_SERVER
- 验收对象：`variable-star` namespace 中的 Triton Deployment、Gateway、KEDA ScaledObject 与 HPA
- 验收范围：固定 21 epoch、`COMPUTE_BOOTSTRAP` 模式、经 Gateway 发起的受控推理负载；不包含 Kafka、Worker、Result Writer 或 PostgreSQL 链路。

## 1. 伸缩契约

生产 HPA 最小副本数为 1、最大副本数为 2。两个 Prometheus 触发器分别使用：

- `s0-prometheus`：顶层模型总成功推理速率，`AverageValue` 目标为每副本 90 次/秒。
- `s1-prometheus`：顶层模型各 Pod 平均排队时间的最大值，`Value` 目标为 5 ms。

HPA 缩容稳定窗口为 300 秒，每 60 秒最多缩减 1 个 Pod。双副本时，HPA 展示的 `s0` 当前值是每副本平均值；判断总速率时不得把该值误认为全部 Pod 的总和。

## 2. 持续负载与扩容

使用 `/tmp/s11-triton-capacity-bench-rate-v3`，向生产 Gateway 施加 105 requests/s、并发度 4、持续 9 分钟的受控推理负载。

- 计划投递：56,700；实际投递：56,700；成功：56,700；失败：0。
- 全程实际投递速率及成功吞吐：105.00/s。
- 18 个完整的 30 秒投递窗口均为 3,150 次、105.00/s。
- 客户端推理延迟 P50/P95/P99：21.257/22.932/27.175 ms。
- 从计划投递到开始推理的等待时间 P99：1.190 ms；最大值：286.328 ms。
- 测试中出现过短暂的超过 250 ms 投递延迟告警，但未导致窗口投递不足或推理失败。

HPA 的 `SuccessfulRescale` 事件明确记录：由于 `s0-prometheus` 高于目标，期望副本数由 1 增至 2。第二个 Triton Pod 于 15:16:06 就绪；扩容后 Triton 为 2/2 Ready，Gateway 为 1/1 Ready。

## 3. 高总负载、低排队时的双副本保护

查询区间：15:16:30～15:22:30（北京时间）；Prometheus 查询步长：15 秒，共 25 个采样点。

| 指标 | 历史查询结果 |
| --- | --- |
| 顶层模型总成功推理速率 | 最低 100.439/s，最高 106.010/s；25/25 个采样点均高于 90/s |
| 顶层模型各 Pod 平均排队时间的最大值 | 最低 0.136 ms，最高 0.756 ms；25/25 个采样点均低于 5 ms |

观察期间的 HPA 检查结果为 `currentReplicas=2`、`desiredReplicas=2`，Triton 保持 2/2 Ready。该结果验证了：在本次持续高总负载、低排队条件下，总速率触发器能够维持双副本需求，未观察到提前缩容。

历史指标按 15 秒采样；HPA 状态来自观察期间的检查点及伸缩事件。以上证据不等于对两个检查点之间每一瞬间的副本需求做了连续记录。

## 4. 负载结束后的自动缩容

压测正常结束后，未手动修改 HPA、ScaledObject 或 Triton 副本数。2026-09-24 15:29:03 的检查结果：

- 压测进程已停止。
- HPA：`currentReplicas=1`、`desiredReplicas=1`。
- HPA 事件：`New size: 1; reason: All metrics below target`。
- Triton：1/1 Ready；Gateway：1/1 Ready。
- HPA 的 `AbleToScale`、`ScalingActive` 均为 True，`ScalingLimited` 为 False。

## 5. 验收结论与适用边界

在本次固定输入和服务器环境下，已验证“总速率触发 1→2 扩容 → 持续高总负载、低排队时保持双副本 → 负载下降后自动 2→1 缩容”的运行闭环，S11-08E3 标记为 `VERIFIED_SERVER`。

本记录不将 105/s 视为不同输入长度、模型版本或部署规模下的通用容量上限，也不将 15 秒采样结果表述为逐瞬间保证。

原始服务器日志：`/tmp/s11-e3i-rate105-v3.log`；启动时间记录：`/tmp/s11-e3i-rate105-v3-started.txt`。这些 `/tmp` 文件是本轮服务器临时证据路径，不属于本次 Git 文档提交内容。
