# Variable Star Classification Observability Coverage

## 1. Purpose

This document records the production observability coverage established during S9-10.

The current observability stack provides:

- Prometheus metric collection through kube-prometheus-stack;
- Grafana dashboards provisioned from Git;
- Prometheus alert rule evaluation;
- application, Kafka, PostgreSQL, Gateway, Triton, GPU, Kubernetes, Go runtime, and node-level visibility.

The goal is not to duplicate every kube-prometheus-stack platform alert. Custom rules are limited to application-specific failure modes that materially affect the real-time classification pipeline.

Alertmanager is currently disabled. Therefore the current acceptance scope is:

```text
metric produced
→ Prometheus scrape
→ PromQL evaluation
→ inactive / pending / firing
→ Grafana / Prometheus visibility
```

External alert delivery is outside the current S9-10 scope.

---

## 2. Pipeline under observation

```text
Candidate Event
    ↓ Kafka
candidate-orchestrator
    ↓ ClassificationCommand
classifier-worker
    ↓ HTTP
LightCurve
    ↓
classifier-worker
    ↓ HTTP
Traefik Triton Gateway
    ↓
Triton-A / Triton-B
    ↓
GPU inference
    ↓
ClassificationResult
    ↓ Kafka
classification-result-writer
    ↓
PostgreSQL
```

The three Go daemons expose private management endpoints:

```text
/live
/ready
/metrics
```

Prometheus scrapes application metrics through PodMonitor resources.

---

## 3. Grafana dashboards

### 3.1 Variable Star - Application Operations

UID:

```text
variable-star-app-ops
```

Primary coverage:

- Candidate consumption rate;
- ClassificationCommand publish/consume rate;
- ClassificationResult publish/consume rate;
- PostgreSQL successful write rate;
- Kafka produce results;
- workers currently retrying;
- oldest retry age;
- retry attempt rate;
- outbound LightCurve/Triton HTTP request rate;
- outbound HTTP P95 latency;
- PostgreSQL write rate.

Primary use:

```text
Is the end-to-end application pipeline still moving?
```

### 3.2 Variable Star - Kafka Retry DLQ

UID:

```text
variable-star-kafka-reliability
```

Primary coverage:

- Kafka consume rate by topic;
- Kafka produce rate by topic/result;
- consumer-group management errors;
- rebalance callback blocking;
- workers currently retrying;
- retry age by worker;
- retry attempt rate;
- DLQ publish activity.

Important limitation:

```text
astro_kafka_records_polled_total
```

measures records returned by Kafka polling. It is not committed consumer lag.

Committed Kafka consumer lag is still a known observability gap.

### 3.3 Variable Star - Kubernetes Go PostgreSQL

UID:

```text
variable-star-platform
```

Primary coverage:

- node CPU utilization;
- node memory utilization;
- variable-star Pod phases;
- container CPU;
- container memory;
- Go process CPU;
- Go goroutines;
- Go heap;
- PostgreSQL pool connections;
- PostgreSQL write P95 latency.

Generic Kubernetes/node failure alerts are primarily provided by kube-prometheus-stack and are not duplicated as Variable Star custom alerts.

### 3.4 Variable Star - Gateway Triton

UID:

```text
variable-star-gateway-triton
```

Primary coverage:

- Gateway inference request rate;
- Gateway 5xx rate;
- Gateway P95 latency;
- Triton success rate;
- Triton failure rate;
- Triton mean request latency;
- Triton mean queue latency;
- Triton mean compute latency;
- Triton pending requests;
- GPU utilization;
- GPU memory utilization.

Traefik exposes backend health separately for:

```text
http://10.3.10.166:18000
http://10.3.10.166:18001
```

through:

```text
traefik_service_server_up
```

Triton inference metrics are scraped from:

```text
Triton-A metrics: 10.3.10.166:18002
Triton-B metrics: 10.3.10.166:18003
```

Triton user-level inference rate/failure interpretation should prefer:

```text
model="variable_star_classifier"
```

rather than summing all ensemble/internal models.

Triton request, queue, and compute duration metrics are cumulative microsecond counters. The current dashboard therefore reports mean latency using rate(duration) / rate(requests). It does not claim Triton P95 latency.

---

## 4. Custom application alert rules

PrometheusRule:

```text
variable-star-application-alerts
```

Rule group:

```text
variable-star.application
```

Current rules:

| Alert | Signal | Purpose |
| --- | --- | --- |
| `TritonBackendDown` | `traefik_service_server_up == 0` | Detect a Triton backend that the Gateway can no longer use |
| `GatewayHigh5xxRatio` | Gateway 5xx ratio | Detect sustained inference Gateway server errors |
| `TritonInferenceFailureDetected` | top-level Triton failure counter | Detect failed `variable_star_classifier` inference |
| `ClassifierWorkerRetryStuck` | retrying + retry age | Detect a worker stuck in long-running dependency retry |
| `KafkaGroupManageErrorDetected` | Kafka group management error counter | Detect consumer-group/rebalance management failures |
| `PostgreSQLWriteFailureDetected` | PostgreSQL write error counter | Detect failed ClassificationResult persistence |
| `DLQPublishDetected` | successful DLQ Kafka produce | Detect poison/permanent messages entering a DLQ |

---

## 5. Operational thresholds

Current alert thresholds are provisional operational thresholds, not scientific SLOs.

### TritonBackendDown

```text
backend unavailable for 1 minute
```

### GatewayHigh5xxRatio

```text
5xx ratio > 5%
and request rate > 0.1 requests/s
for 2 minutes
```

### ClassifierWorkerRetryStuck

```text
retrying > 0
and retry age > 60 seconds
for 2 minutes
```

The classifier worker uses capped-unlimited RETRYABLE behavior with a maximum backoff of approximately 10 seconds.

### Event-based alerts

The following rules use a five-minute lookback window:

```text
TritonInferenceFailureDetected
KafkaGroupManageErrorDetected
PostgreSQLWriteFailureDetected
DLQPublishDetected
```

A single event may therefore keep the corresponding alert condition true until it exits that lookback window.

---

## 6. DLQ first-series handling

Prometheus CounterVec series are created lazily.

During S9-10G validation, the first real Command DLQ event created:

```text
astro_kafka_produce_records_total{
  topic="light-curve-classification-command-dlq",
  result="success"
}
```

for the first time.

The first Prometheus sample was already:

```text
1
```

There was no previously scraped zero-valued sample. Therefore:

```promql
increase(counter[5m])
```

alone did not detect the first DLQ event.

The final `DLQPublishDetected` rule handles both cases:

```text
normal existing series:
counter N → N+1
→ increase(...[5m])

first appearance of a new series:
series absent → first sample > 0
→ current series unless same series offset 5m
```

This prevents the first DLQ generated by a newly created CounterVec label set or a new worker Pod from being silently missed.

---

## 7. Fault and signal coverage matrix

| Failure / risk | Primary dashboard | Primary signal | Custom alert | Verification |
| --- | --- | --- | --- | --- |
| Candidate/Command/Result pipeline stops moving | Application Operations | Kafka poll/produce pipeline rates | none | runtime PromQL verified |
| Kafka consumer-group management error | Kafka Retry DLQ | `astro_kafka_group_manage_errors_total` | `KafkaGroupManageErrorDetected` | rule loaded/healthy |
| Kafka rebalance callback blocked | Kafka Retry DLQ | `astro_kafka_rebalance_callback_blocked_total` | none | server fault testing completed previously |
| Classifier dependency retry | Kafka Retry DLQ / Application Operations | retrying, retry age, retry attempts | `ClassifierWorkerRetryStuck` | metrics/runtime behavior verified |
| Poison/permanent message | Kafka Retry DLQ | DLQ Kafka produce counter | `DLQPublishDetected` | real fault injection verified |
| PostgreSQL write failure | Application Operations / Kubernetes Go PostgreSQL | `astro_postgres_writes_total` | `PostgreSQLWriteFailureDetected` | rule loaded/healthy; previous PG outage recovery verified |
| PostgreSQL latency/pool pressure | Kubernetes Go PostgreSQL | write latency + pgxpool metrics | platform observation | runtime metrics verified |
| Gateway 5xx | Gateway Triton | Traefik request metrics | `GatewayHigh5xxRatio` | metrics/PromQL verified |
| One Triton backend unavailable | Gateway Triton | `traefik_service_server_up` | `TritonBackendDown` | real fault injection verified |
| Triton inference failure | Gateway Triton | `nv_inference_request_failure` | `TritonInferenceFailureDetected` | rule loaded/healthy |
| Triton queue/compute slowdown | Gateway Triton | request/queue/compute mean latency | none | runtime PromQL verified |
| GPU high utilization/memory | Gateway Triton | `nv_gpu_utilization`, memory metrics | none | observation only |
| Pod/node/runtime failure | Kubernetes Go PostgreSQL + built-in dashboards | kube-state-metrics/node-exporter/kubelet | kube-prometheus-stack built-in rules | platform targets verified |

---

## 8. Real alert fault-injection evidence

### 8.1 TritonBackendDown

A single Triton backend was intentionally stopped while the other backend remained available.

Observed state transition:

```text
inactive
→ pending
→ firing
→ inactive
```

Historical Prometheus evidence:

```text
2026-08-17 19:26:30 +0800  pending
2026-08-17 19:27:30 +0800  firing
```

After Triton-B recovery:

- Triton readiness recovered;
- Triton metrics recovered;
- both Gateway backend health series returned to `1`;
- `ALERTS{alertname="TritonBackendDown"}` disappeared.

Result:

```text
PASS / VERIFIED_SERVER
```

### 8.2 DLQPublishDetected

A deliberately invalid ClassificationCommand was sent to the real command topic.

Observed behavior:

```text
invalid ClassificationCommand
→ permanent decode failure
→ Command DLQ publish
→ Kafka DLQ produce counter increment
→ DLQPublishDetected pending
→ DLQPublishDetected firing
→ event exits five-minute window
→ DLQPublishDetected inactive
```

Observed second-event evidence:

```text
DLQ counter: 1 → 2
increase over 5m: > 0

20:04:00  pending
20:05:00  firing
later      inactive
```

Result:

```text
PASS / VERIFIED_SERVER
```

---

## 9. Known gaps and intentional exclusions

### 9.1 Kafka committed consumer lag

Current application metrics expose Kafka records polled, produce activity, broker request latency, group errors, and rebalance behavior.

They do not yet expose authoritative committed consumer lag.

```text
records_polled_total != committed consumer lag
```

Consumer lag remains an explicit future improvement.

### 9.2 Alert delivery

Alertmanager is currently disabled.

Current guarantees stop at:

```text
Prometheus alert evaluation
+
Grafana / Prometheus visibility
```

Email, Slack, PagerDuty, webhook, or other external notification delivery has not been accepted.

### 9.3 GPU alerts

GPU utilization and GPU memory are intentionally dashboard-only.

The current two-GPU performance environment is shared with another workload. High utilization does not by itself imply a Variable Star production incident.

Capacity conclusions from the current environment must therefore be described as:

```text
shared-GPU experimental evidence
```

not dedicated dual-GPU production capacity.

### 9.4 Long-duration soak

Short normal/peak/safety load and failure-recovery evidence exist.

A long-duration soak remains deferred and must not be represented as completed.

### 9.5 Triton latency percentile

Traefik exposes histogram metrics and therefore supports Gateway P95 latency.

The currently enabled Triton duration metrics are cumulative counters, so the dashboard computes mean Triton latency rather than P95.

---

## 10. Operational diagnosis flow

When classification throughput drops:

```text
1. Application Operations
   → determine which pipeline stage stopped moving

2. Kafka Retry DLQ
   → inspect retry, rebalance, group errors, and DLQ activity

3. Gateway Triton
   → inspect Gateway 5xx, backend health, Triton failures,
      queue/compute latency, pending requests, and GPU state

4. Kubernetes Go PostgreSQL
   → inspect Pod/node health, Go runtime, PostgreSQL pool,
      and PostgreSQL write latency

5. Structured logs
   → use job_id, run_id, object_id, revision, trace context,
      Kafka topic/partition/offset for individual-event diagnosis
```

High-cardinality business identities are intentionally kept in structured logs rather than Prometheus labels.

---

## 11. S9-10 acceptance status

```text
S9-10A Metrics Inventory
  VERIFIED_SERVER

S9-10B Prometheus / Grafana platform
  VERIFIED_SERVER

S9-10C Application Operations Dashboard
  CLOSED / VERIFIED_CI_WITH_SERVER_EVIDENCE

S9-10D Kafka Retry DLQ Dashboard
  CLOSED / VERIFIED_CI_WITH_SERVER_EVIDENCE

S9-10E Kubernetes Go PostgreSQL Dashboard
  CLOSED / VERIFIED_CI_WITH_SERVER_EVIDENCE

S9-10F Gateway Triton observability
  CLOSED / VERIFIED_CI_WITH_SERVER_EVIDENCE

S9-10G Alert Rules
  CLOSED / VERIFIED_CI_WITH_SERVER_EVIDENCE

S9-10H Observability coverage closure
  pending documentation commit and CI
```
