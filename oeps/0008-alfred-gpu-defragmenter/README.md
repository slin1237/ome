# OEP-0008: Alfred GPU Defragmenter

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Context: The GPU Fragmentation Problem](#context-the-gpu-fragmentation-problem)
  - [Why Pure Eviction Is Insufficient](#why-pure-eviction-is-insufficient)
  - [Why Today's ome-operator Alfred Is Insufficient](#why-todays-ome-operator-alfred-is-insufficient)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [What Alfred Is](#what-alfred-is)
  - [Architectural Posture: Observer + Recommender + Narrow Actuator](#architectural-posture-observer--recommender--narrow-actuator)
  - [Core Concepts](#core-concepts)
  - [User Stories](#user-stories)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Architecture Overview](#architecture-overview)
  - [Observation Layer](#observation-layer)
  - [Fragmentation Scoring](#fragmentation-scoring)
  - [Candidate Selection and Recommendation Production](#candidate-selection-and-recommendation-production)
  - [Placement Hint Computation](#placement-hint-computation)
  - [Execution Layer](#execution-layer)
  - [Policy Model](#policy-model)
  - [Opt-In / Opt-Out Semantics](#opt-in--opt-out-semantics)
  - [Rate Limiting and Safety Bounds](#rate-limiting-and-safety-bounds)
  - [Concurrent Operation Awareness](#concurrent-operation-awareness)
  - [Spot and Preemptible Nodes](#spot-and-preemptible-nodes)
  - [Multi-Tenancy](#multi-tenancy)
  - [Model-Download Coordination](#model-download-coordination)
  - [Degraded Mode](#degraded-mode)
  - [Deployment Model](#deployment-model)
  - [Leader Election](#leader-election)
  - [RBAC](#rbac)
  - [Observability](#observability)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

Alfred is a new cluster-level controller that detects GPU
fragmentation in OME-managed clusters and produces recommendations to
consolidate workload placement. For OMENative-managed workloads (see
[OEP-0007](../0007-omenative-workload-strategy/README.md)), Alfred
can optionally execute migrations via OMENative's first-class
migration verb — delivered as a narrow annotation contract. For
`RawDeployment` single-pod workloads, Alfred can optionally use the
K8s Eviction API directly. For legacy LWS-backed workloads, Alfred
produces recommendations only — auto-migration is unsafe for LWS and
the recommendations are surfaced to operators for manual action until
those workloads are migrated to OMENative.

Alfred's architecture is deliberately narrow: it observes, computes,
recommends, and (optionally) triggers. It **does not** write to
OME-owned state beyond a single narrow annotation contract with
OMENative. It **does not** pin pods to specific nodes or bypass the
K8s scheduler. It **does not** replicate logic from cluster
autoscaler or the scheduler — it composes with them.

Alfred introduces **no new CRDs**. Configuration lives in a
`ConfigMap`; per-workload opt-in/out via annotations on
`InferenceService`; output as Prometheus metrics, K8s Events,
optional recommendations ConfigMap, and (only for
OMENative-managed workloads that opt into execution) migration
request annotations on `InferenceService` — precisely the published
contract defined by OEP-0007.

Alfred explicitly replaces the defragmentation and bin-packing
capabilities that the archived `ome-operator` Alfred never shipped
(a stubbed `rescheduling/` module and hardcoded `capacityCheck.go`).
The auto-patching and auto-repair capabilities of the original
Alfred are **out of scope** for this OEP; re-introduction to the
new OME ecosystem is deferred to later work.

## Motivation

### Context: The GPU Fragmentation Problem

In GPU-dense clusters running LLM inference, the default K8s
scheduler's spread behavior produces a predictable failure mode.
Consider 10 nodes × 8 H100s = 80 GPUs. Deploy 10 × 1-GPU workloads
spread one-per-node. Each node has 7 free GPUs (70 total). Now
deploy a workload needing 8 GPUs on one node: the scheduler cannot
place it despite 70 GPUs being "free," because no single node has 8
contiguous free GPUs.

With bin-packing, the 10 small workloads occupy 1–2 nodes (e.g.,
Node1 full at 8 workloads, Node2 holds 2), leaving 8 empty nodes —
each of which can accept the 8-GPU workload.

Two sub-problems:

1. **Initial placement (bin-packing).** At deployment time. Solved
   at the scheduler level: deploy Volcano, Yunikorn, or
   scheduler-plugins with `NodeResourcesFit: MostAllocated`. OME
   supports this today via `PodSpec.SchedulerName`. **Not Alfred's
   concern.**

2. **Runtime defragmentation.** As workloads come and go, placement
   drifts from optimal. Scale-downs, deletions, node additions,
   failed-then-rescheduled pods all leave fragmentation. Even with a
   bin-packing scheduler at placement time, cluster state degrades.
   **This is Alfred's problem.**

Analog: cluster-autoscaler (CA). CA doesn't handle initial resource
requests; CA handles dynamic state after workloads evolve. Alfred is
to GPU fragmentation what CA is to node utilization.

### Why Pure Eviction Is Insufficient

A naive Alfred would use the K8s Eviction API universally, trusting
the scheduler to re-place evicted pods more densely. For single-pod
workloads this works — brief disruption, pod lands wherever.

For **multi-pod Instances** (OMENative's multi-node engine/decoder)
and legacy **LWS groups**, eviction has sharp edges:

- **LWS with `RecreateGroupOnPodRestart`** (OME's current default)
  tears down the entire group on any pod deletion. No surge. No
  rollback. If the scheduler can't place all `N` replacement pods
  simultaneously, the service stays down.
- **OMENative Instances** have atomic group semantics too — evicting
  one pod triggers `RecreateInstanceOnPodRestart` (per-Instance
  restart), which is cleaner than LWS's whole-group teardown but
  still not surge-protected. Pure eviction is therefore also unsafe
  for OMENative multi-pod Instances.

The answer is not "smarter eviction." The answer is to narrow
Alfred's action surface: for multi-pod Instances, delegate execution
to OMENative's first-class migration verb (surge-then-swap or
drain-then-rebuild). Eviction stays available only for single-pod
`RawDeployment` workloads where it's safe.

### Why Today's ome-operator Alfred Is Insufficient

The archived `bitbucket.oci.oraclecorp.com/genaicore/ome-operator`
repository contains a component also named "Alfred." An audit
showed:

- **Auto-patching** (`pkg/alfred/autopatching/autopatching.go`):
  cordons nodes, triggers GPU node image rotation via OCI Compute
  API, drains single-replica InferenceServices. Only handles
  single-replica workloads.
- **Auto-repair** (`pkg/alfred/autorepair/autorepair.go`): watches
  for `GpuUnhealthy` node condition, cordons, drains, triggers soft
  reset via OCI Compute API.
- **Rescheduling** (`pkg/alfred/rescheduling/rescheduling.go`):
  stub, 6 lines, one function `gpuEfficiencyScore` that is never
  called.
- **Capacity check** (`pkg/alfred/application/capacityCheck.go`):
  stub, returns hardcoded `true` with a `// will add real logic
  later` comment.

The rescheduling and bin-packing logic was never implemented. This
OEP is the realization of that original vision, narrowed to the
defragmentation problem.

Auto-patching and auto-repair are valuable but orthogonal. They are
explicitly **not** covered by this OEP. Their re-introduction to the
new OME ecosystem is future work, tracked as separate OEPs when
demand surfaces.

### Goals

1. **Continuously observe** GPU utilization, workload placement, and
   pending-pod pressure in the cluster; compute a fragmentation
   metric.
2. **Produce recommendations** identifying which workloads, if
   relocated, would reduce fragmentation most effectively.
3. **Optionally execute migrations** via:
   - OMENative's migration annotation contract for OMENative
     workloads.
   - K8s Eviction API for `RawDeployment` single-pod workloads.
4. **Never modify OME-owned reconciliation state** except via the
   published migration annotation contract (a narrow, explicit
   exception documented in OEP-0007).
5. **Introduce no new CRDs.** Configuration via ConfigMap;
   per-workload gating via annotations; output via metrics, Events,
   and ConfigMap.
6. **Operate safely by default.** Conservative rate limits,
   per-workload cooldowns, explicit rejection of actions with high
   uncertainty. First principle: *primum non nocere.*
7. **Be useful without OMENative** for RawDeployment single-pod
   workloads (non-trivial fraction of real clusters).
8. **Scale to large clusters** (tested design: 1000 nodes, 10k
   pods, 100+ InferenceServices).

### Non-Goals

1. **Not a scheduler.** Alfred does not make pod placement
   decisions. The K8s scheduler (default or custom) places pods.
2. **Not a K8s scheduler extender or plugin.** Alfred is a separate
   controller, not in the scheduling hot path.
3. **Not managing node lifecycle.** Alfred does not cordon, drain,
   or terminate nodes. That is the operator's or cluster
   autoscaler's responsibility.
4. **Not handling auto-patching or auto-repair.** Explicitly
   deferred.
5. **Not integrated with DRA in v1.** Forward-looking — revisit
   when DRA matures.
6. **Not solving GPU health detection.** Consumes existing signals
   (AcceleratorClass status, node conditions). Adds no new
   telemetry.
7. **Not an optimization engine in the mathematical sense.** Uses
   heuristic candidate selection with safety bounds. "Good enough"
   is the standard; global optimality is not promised.
8. **Not managing workloads beyond InferenceService.** Non-OME GPU
   workloads are observed (for fragmentation scoring) but never
   migrated.
9. **Not introducing new CRDs.** Hard constraint.
10. **Not causing preemption.** Users set `priorityClass` on
    InferenceService; the scheduler enforces. Alfred does not
    preempt.

## Proposal

### What Alfred Is

Alfred is a Kubernetes controller, deployed as a leader-elected
`Deployment` (typically 3 replicas, one active) in OME's namespace
or a dedicated namespace. It runs two loops:

- **Observation loop** (default 30s): refresh cluster-state snapshot
  from informers; compute fragmentation metrics; update Prometheus
  gauges.
- **Decision loop** (default 5m): evaluate whether fragmentation
  exceeds policy threshold; if so, select candidate migrations;
  produce recommendations; optionally execute.

Alfred reads:
- Nodes (with GPU capacity and allocation).
- Pods (labeled by OME conventions).
- InferenceServices.
- BaseModels / ClusterBaseModels.
- AcceleratorClasses.
- Alfred's own ConfigMap for policy configuration.

Alfred writes:
- Prometheus metrics (continuous).
- K8s Events on InferenceServices (recommendations, migrations).
- Optional `alfred-recommendations` ConfigMap in its own namespace.
- Migration-request annotations on InferenceServices (only when
  execute mode is enabled and workload is OMENative-managed).

### Architectural Posture: Observer + Recommender + Narrow Actuator

The central design choice: Alfred is primarily an **observer +
recommender**. Execution is delegated to OMENative (for multi-pod
workloads) or the K8s Eviction API (for single-pod workloads). Alfred
does not itself orchestrate pod lifecycle beyond that narrow surface.

Rationale:
- Eviction is destructive and races with other cluster activity.
- For atomic multi-pod groups (OMENative Instances and legacy LWS
  groups), eviction is unsafe.
- Safe migration requires the workload controller (OMENative), which
  owns the lifecycle.
- Alfred's failure modes are bounded: a bad recommendation can be
  rejected by OMENative; a bad eviction results in normal K8s pod
  restart.

**The narrow write contracts** (everything Alfred writes to the
wider cluster):

| Target | Resource | Write | Why |
|--------|----------|-------|-----|
| Alfred namespace | ConfigMap (`alfred-recommendations`) | Full read/write | Alfred's own output |
| Alfred namespace | Lease | Full read/write | Leader election |
| Any namespace | Events | `create`, `patch` | Surface information |
| Any namespace | Pod eviction subresource | `create` | Only for RawDeployment single-pod migrations |
| Any namespace | InferenceService annotations | `patch` (narrow to `ome.io/migration-request-v1-*`) | Migration verb per OEP-0007 |

Every other interaction is read-only.

### Core Concepts

- **Fragmentation Score.** A cluster-level number in [0, 1]
  summarizing GPU fragmentation. 0 = perfectly packed; 1 = maximally
  fragmented. Composed of four signals (see [Fragmentation
  Scoring](#fragmentation-scoring)).
- **Workload.** A unit from Alfred's perspective, typically one
  InferenceService broken down into Components and Instances. Alfred
  reasons at the Component/Instance level.
- **Candidate Migration.** A proposed move — "Component X's Instance
  Y off Node Z." Each candidate has a *benefit score* (expected
  fragmentation improvement) and *cost score* (disruption risk).
- **Recommendation.** A candidate that has passed all policy filters
  (opt-in, cooldown, rate limit, capacity check) and is ready for
  emission.
- **Policy.** Cluster-level configuration governing when Alfred
  acts, what workloads are eligible, aggressiveness.

### User Stories

#### Story 1: Operator observes fragmentation via Grafana

Operator deploys OME with Alfred enabled, policy set to
`mode: recommend-only`. Over a week, workloads come and go; the
`alfred_cluster_fragmentation_score` gauge rises from 0.2 to 0.6.
Operator reviews recommendations in `kubectl get events` and the
`alfred-recommendations` ConfigMap. Decides to switch policy to
`mode: execute`. Alfred begins writing migration-request annotations
to OMENative-managed workloads. Fragmentation drops over the next
hour.

#### Story 2: Alfred unblocks a pending large workload

A new InferenceService for Llama4 requires 8 GPUs on a single node.
No node has 8 contiguous free GPUs despite 70 GPUs being free.
Alfred detects the Pending pod, prioritizes candidates whose
migration would free 8-GPU capacity on one node. Within minutes,
small OMENative workloads are migrated (via OMENative's migration
verb), consolidating Node1. Llama4 schedules on Node3.

#### Story 3: Maintenance-window honor

Operator sets `maintenanceWindows: [{start: "08:00", end: "18:00",
timezone: "UTC", daysOfWeek: [Mon..Fri]}]`. At 10:00 UTC,
fragmentation is high; Alfred continues producing recommendations
(metrics + events) but does not execute. At 18:00 UTC, Alfred
resumes execution per the standard rate limit. An
`emergencyPendingAgeMinutes` override lets Alfred act during the
window if a Pending pod has been waiting past the threshold.

#### Story 4: Opt-out for a critical workload

A business-critical InferenceService is annotated
`alfred.ome.io/movable: "false"`. Alfred excludes it from candidate
selection entirely. An event on the ISVC explains why (useful when
operator wonders why no recommendation was produced despite
obvious fragmentation).

#### Story 5: Coexistence with cluster-autoscaler

CA is enabled. CA scales down underutilized nodes. Alfred's
consolidation activity feeds this: as Alfred packs workloads onto
fewer nodes, CA notices the emptied nodes and scales them down.
Alfred watches for CA's `cluster-autoscaler.kubernetes.io/scale-down-disabled`
annotation and excludes such nodes from placement hints to avoid
conflict.

#### Story 6: LWS legacy workload — recommendation only

An InferenceService using `deploymentMode: MultiNode` (LWS-backed)
is highly fragmented. Alfred produces a recommendation but does
**not** execute. The event explains:

> LWS-backed workload cannot be migrated safely by alfred. Migrate
> the workload to OMENative strategy for automatic defragmentation,
> or handle manually.

Operator either migrates the workload to OMENative (via the
procedure in OEP-0007 §Strategy Migration Procedure) or handles
the defragmentation manually.

### Risks and Mitigations

**Risk: Thrashing.** Alfred migrates workload A, creating new
fragmentation elsewhere, triggering migration of workload B that
undoes A's placement.

*Mitigation:* per-workload cooldown (no migration within 30 minutes
of the last); per-node cooldown (10 minutes after a migration
touches the node); trend-based recommendations (only act when
fragmentation is consistently above threshold, not on spikes);
simulation before recommendation (predict post-migration cluster
state and reject regressive moves).

**Risk: Broken GPU discovered after migration.** Alfred migrates to
Node5 which has a broken NVLink; NCCL fails.

*Mitigation:* Layer-1 consumes GPU-health signals (AcceleratorClass
status, node conditions). Unhealthy nodes excluded from hints.
Post-migration health monitoring — if the migrated workload fails
within a window, Alfred marks the node suspect and backs off from
it.

**Risk: Migration storm.** Alfred recommends many migrations at
once; OMENative is overwhelmed.

*Mitigation:* configurable cluster-wide cap (default 3 in-flight, 10
per hour). Rejections from OMENative (`RateLimited`) are observed
and trigger backoff. Circuit breaker if failure rate > 50% of recent
10 migrations — pause for 1 hour, emit critical event.

**Risk: Scoring bug causes counterproductive recommendations.**

*Mitigation:* extensive unit and simulation testing (adversarial
cluster states); `recommend-only` default mode during early
deployments; structured reasoning in each recommendation (`from_node`,
`hint_target_nodes`, benefit score) reviewable by operators; metrics
track accept/reject ratios.

**Risk: Conflict with HPA / scheduler / other controllers.**

*Mitigation:* Alfred checks HPA scaling activity on target ISVC
before emitting a recommendation; defers during active HPA. Alfred
respects `scale-down-disabled` annotations on nodes. OMENative
enforces per-Instance migration-in-progress lock (OEP-0007 Q-004),
so concurrent requests are serialized.

**Risk: Migration-request annotation write lost or delayed.**

*Mitigation:* UUID-keyed annotations are idempotent (OEP-0007 Q-021).
If OMENative does not clear within 5 minutes, Alfred retries with
the same UUID. If still no ack at 1 hour, OMENative clears as stale
and emits `MigrationRequestStale`; Alfred restarts from observation.

**Risk: Alfred / OMENative version skew on the migration wire
contract.**

*Mitigation:* Alfred writes only the OEP-0007 v1 request shape
(`ome.io/migration-request-v1-*` with `schemaVersion: "v1"`).
OMENative rejects unsupported `schemaVersion` values explicitly. Within
the same `schemaVersion`, additive unknown fields are ignored;
behavior-changing fields require a new schema version. If Alfred sees
`UnsupportedSchemaVersion` or equivalent rejection, it degrades that
workload to recommend-only and emits an event.

**Risk: Alfred is a cluster-wide SPOF.**

*Mitigation:* Leader-elected with 3 replicas. Crashed leader
triggers failover within ~15s. Alfred is NOT on the critical path:
workloads run fine if Alfred is down for hours.

**Risk: AcceleratorClass status lag produces stale GPU inventory.**

*Mitigation:* Alfred builds its own cache from pod/node informer
watches, cross-checked with AcceleratorClass at observation loop
period. Staleness bounded by the loop frequency. Every action
includes a pre-flight re-check.

## Design Details

### Architecture Overview

Alfred is a new binary (`cmd/alfred/`) and package (`pkg/alfred/`)
in the OME repository. Deployed as a separate Deployment, not
embedded in the main OME manager. Rationale:
- Independent release cadence.
- Independent failure domain.
- Independent resource footprint.

```
┌──────────────────────────────────────────────────────────────────┐
│  Alfred Controller                                               │
│                                                                  │
│  ┌────────────┐   ┌──────────┐   ┌──────────────────────────┐   │
│  │ Observer   │──▶│ Scorer   │──▶│ Recommender              │   │
│  │ (informers)│   │ (fragmen-│   │ (candidate selection +   │   │
│  │            │   │  tation) │   │  ranking)                │   │
│  └────────────┘   └──────────┘   └───────────┬──────────────┘   │
│                                               │                  │
│                                               ▼                  │
│                                       ┌───────────────┐          │
│                                       │ Executor      │          │
│                                       │ (dispatcher)  │          │
│                                       └───────┬───────┘          │
└───────────────────────────────────────────────┼─────────────────┘
                                                │
          ┌─────────────────────────────────────┼────────────────────┐
          │                                     │                    │
          ▼                                     ▼                    ▼
  OMENative migration               Eviction API              Prometheus metrics
  (annotation on ISVC               (RawDeployment            + K8s Events
   per OEP-0007 Q-004)               single-pod)              + ConfigMap
```

Package layout:

```
cmd/alfred/
├── main.go
├── server.go                      (/healthz, /metrics)
└── config.go                      (CLI flags, config loading)

pkg/alfred/
├── observer/
│   ├── observer.go                (informer-driven snapshot)
│   ├── cache.go                   (in-memory cluster model)
│   └── types.go                   (observation snapshot types)
├── scorer/
│   ├── scorer.go                  (fragmentation score formulas)
│   └── schedulability.go          (reference-workload feasibility)
├── recommender/
│   ├── recommender.go             (candidate selection loop)
│   ├── selector.go                (workload eligibility filters)
│   ├── ranker.go                  (benefit/cost ranking)
│   └── simulator.go               (predict post-migration state)
├── executor/
│   ├── executor.go                (dispatch by workload type)
│   ├── migration.go               (OMENative annotation writer)
│   ├── eviction.go                (K8s Eviction API)
│   ├── rate_limiter.go            (rate limits + cooldowns)
│   └── circuit_breaker.go         (failure-rate circuit breaker)
├── policy/
│   ├── policy.go                  (ConfigMap + annotations)
│   ├── schema.go                  (config schema validation)
│   └── types.go
├── controller/
│   ├── controller.go              (reconcile tickers)
│   └── leader_election.go
├── metrics/
│   └── metrics.go                 (Prometheus instrumentation)
└── utils/
    └── ...
```

### Observation Layer

In-memory cluster model refreshed from K8s informers and periodic
re-scans:

```go
type ClusterSnapshot struct {
    Timestamp       time.Time
    Nodes           map[string]*NodeState
    Workloads       map[types.NamespacedName]*WorkloadState
    Models          map[string]*ModelAvailability
    PendingPods     []*PendingPodInfo
    OMENativeAvailable bool                      // API discovery result
}

type NodeState struct {
    Name                   string
    AcceleratorClass       string
    TotalGPUs              int
    AllocatedGPUs          int
    FreeGPUs               int
    LargestContiguousFree  int                   // topology-aware
    Unhealthy              bool
    Cordoned               bool
    ScaleDownDisabled      bool                  // CA annotation observed
    Preemptible            bool                  // spot / preemptible label observed
    OMEManagedPods         []OMEPodInfo
    OtherOccupants         []OtherPodInfo        // non-OME GPU workloads
}

type WorkloadState struct {
    ISVC            *omev1beta1.InferenceService
    Components      map[string]*ComponentState   // router, engine, decoder
    Movable         bool                         // from annotation
    Priority        float64                      // from annotation or default
    LastMigration   time.Time                    // per-workload cooldown
    ActiveMigration *MigrationInFlight
}

type ComponentState struct {
    DeploymentMode string                        // OMENative, RawDeployment, MultiNode (LWS), etc.
    Instances      []*InstanceState              // per OEP-0007 hierarchy
}

type InstanceState struct {
    InstanceIndex int32
    NodesSet      map[string]int                 // node -> pod count
    TotalGPUs     int32
    PodCount      int32
    ReadyPods     int32
    DesiredPods   int32
}

type PendingPodInfo struct {
    Namespace     string
    Name          string
    ISVC          types.NamespacedName
    GPUsNeeded    int32
    PendingSince  time.Time
}

type ModelAvailability struct {
    Name        string
    NodesReady  []string                         // from BaseModel.Status.NodesReady
    NodesFailed []string
}
```

**Refresh model**:
- Informer-driven incremental updates on Pod, Node, InferenceService,
  BaseModel, AcceleratorClass events.
- Full snapshot refresh every `observationLoopInterval` (default
  30s) to correct any drift.
- **Non-OME GPU workloads** (Kubeflow Notebook Pods, generic Jobs,
  etc.) are observed as `OtherOccupants` on nodes and contribute to
  GPU allocation for fragmentation scoring, but are **never**
  candidates for migration.

### Fragmentation Scoring

Four signals, weighted, combined. Weights tunable via policy.

**Signal 1: Node contiguity.** For each node with GPUs, the largest
contiguous free GPU block as a fraction of total:

```
NodeContiguityScore(n) = LargestContiguousFree(n) / TotalGPUs(n)
ClusterContiguityScore = mean(NodeContiguityScore(n) for n with GPUs)
```

Higher is better (more room for large workloads). Captures the
Llama4-Maverick trap.

**Signal 2: Bin-packing efficiency.** For each non-empty node:

```
NodeBinPackScore(n) = AllocatedGPUs(n) / TotalGPUs(n)
ClusterBinPackScore = mean(NodeBinPackScore(n) for n with AllocatedGPUs(n) > 0)
```

Higher is better (nodes trend toward "either empty or full").

**Signal 3: Schedulability.** For reference workload sizes
`[1, 2, 4, 8, 16, 32]` GPU, count how many of each could schedule now:

```
SchedulabilityScore(size) = NumberOfNodesWithAtLeast(size, free) / TotalNodesWithGPUs
ClusterSchedulabilityScore = weighted_sum(SchedulabilityScore(s) * w(s))
```

Default weights favor larger sizes (small-workload schedulability
is usually fine).

**Signal 4: Pending-workload penalty.**

```
PendingPenalty = sum(GPUsNeeded(p) * age(p)) / TotalClusterGPUs
                 for p in PendingPods
```

Older pending pods contribute more. This signal forces urgency into
the decision loop.

**Combined score:**

```
FragmentationScore =
     1 - (w1 * ClusterContiguityScore
        + w2 * ClusterBinPackScore
        + w3 * ClusterSchedulabilityScore)
        + w4 * PendingPenalty

clamped to [0, 1]
```

Default weights: `w1=0.3, w2=0.2, w3=0.3, w4=0.2`.

Published as `alfred_cluster_fragmentation_score` (gauge) with
per-node scores as `alfred_node_fragmentation_score{node}`.

**Threshold**: if `FragmentationScore > policy.fragmentationThreshold`
(default 0.5), the decision loop proceeds; below, Alfred is
quiescent.

### Candidate Selection and Recommendation Production

On each decision loop with fragmentation above threshold:

1. **Build candidate set.** For every movable workload (`Movable=true`,
   not in cooldown, not in active migration), enumerate each Instance
   as a candidate.
2. **Filter by strategy**:
   - OMENative workloads: full candidate eligible.
   - RawDeployment single-pod: candidate eligible (Alfred can use
     eviction).
   - LWS-backed multi-pod: candidate NOT eligible for execution, but
     STILL included for recommendation emission (with
     `executable: false` flag).
   - Ray and Knative workloads: not managed by Alfred.
3. **Simulate each candidate.** Predict post-migration cluster state
   (virtual: free source node's GPUs; virtual: best-available target
   node receives the Instance). Compute predicted fragmentation
   score.
4. **Score each candidate:**

   ```
   BenefitScore  = FragmentationScore_before - FragmentationScore_after
                   (positive = improvement)
   CostScore     = f(disruption_risk, pods_impacted, migration_mode)
   FinalScore    = BenefitScore - CostWeight * CostScore
   ```

   Cost scoring:
   - RawDeployment single-pod: very low (brief disruption).
   - OMENative rolling (multi-replica, Service preserved): low.
   - OMENative surge (single-replica, needs 2× capacity briefly):
     medium.
   - LWS-backed: effectively infinite (not executable).

5. **Prioritization boost** for candidates whose migration would
   unblock a Pending pod older than
   `emergencyPendingAgeMinutes` — multiply their `FinalScore` by a
   factor.
6. **Rank candidates** by `FinalScore` descending.
7. **Policy filters**: exclude cooldown violations, excluded
   maintenance-window candidates, rate-limited excess, tenant
   boundaries (see [Multi-Tenancy](#multi-tenancy)).
8. **Emit recommendations**:
   - K8s Event `FragmentationRecommendationProduced` on each target
     InferenceService.
   - Metric `alfred_recommendations_produced_total` incremented with
     labels.
   - Entry written to `alfred-recommendations` ConfigMap (if
     enabled).
   - If `policy.mode == execute` and candidate is executable: dispatch
     to Executor.

### Placement Hint Computation

Recommendations include `hint_target_nodes` — a ranked list of
candidate targets. Computed by:

1. Enumerate nodes capable of accommodating the Instance's resource
   footprint (GPU count and type).
2. Filter by:
   - Model availability: `BaseModel.Status.NodesReady` or node label
     `models.ome.io/{ns}.basemodel.{name}=Ready` (OEP-0007 Q-017).
   - GPU health: node not `Unhealthy` / cordoned.
   - CA compatibility: no `scale-down-disabled` being processed.
   - Spot/preemptible: excluded by default from targets (see [Spot
     and Preemptible Nodes](#spot-and-preemptible-nodes)).
3. Rank by bin-packing heuristic:
   - Prefer partially-filled nodes (goal: consolidate) OR
   - Prefer empty nodes (goal: free up contiguous capacity
     elsewhere).
4. Return top N (default 3).

Hints are **advisory**. OMENative's migrator reads them as
`nodeAffinity.preferred`; K8s scheduler makes the final placement.
If no hint node is feasible at migration time, the scheduler picks
something else; Alfred observes the outcome and re-evaluates.

### Execution Layer

Dispatches by workload type:

#### RawDeployment single-pod

Direct eviction via K8s Eviction API:

```go
eviction := &policyv1.Eviction{
    ObjectMeta: metav1.ObjectMeta{
        Name:      pod.Name,
        Namespace: pod.Namespace,
    },
    DeleteOptions: &metav1.DeleteOptions{
        GracePeriodSeconds: ptr.Int64(30),
    },
}
client.CoreV1().Pods(ns).EvictV1(ctx, eviction)
```

Deployment controller recreates the pod; scheduler places wherever
it fits. Brief disruption.

#### OMENative-managed workloads

Migration-request annotation per OEP-0007 Q-004:

```go
uuid := generateUUID()
payload := MigrationRequest{
    SchemaVersion:  "v1",
    Component:       "engine",
    Instance:        0,
    Reason:          "fragmentation",
    FromNode:        "node1",
    HintTargetNodes: []string{"node3", "node7"},
    RequestedAt:     time.Now(),
    RequestedBy:     "alfred-controller",
}
patch := fmt.Sprintf(`{
    "metadata": {
        "annotations": {
            "ome.io/migration-request-v1-%s": %q
        }
    }
}`, uuid, marshalJSON(payload))
client.Patch(ctx, isvc, types.MergePatchType, []byte(patch))
```

OMENative observes the annotation, validates, accepts or rejects,
updates `Status.MigrationHistory`. Alfred watches the status to
observe outcome.

**Retry semantics** (per OEP-0007 Q-021):
- If OMENative does not clear the annotation within 5 minutes,
  Alfred re-PATCHes the same UUID. Idempotent.
- If after 1 hour no clear has happened, OMENative auto-clears
  with `MigrationRequestStale`; Alfred abandons the request and
  emits event.

#### Legacy LWS-backed workloads

**No execution.** A recommendation event is emitted with
`executable: false` and a message directing operators to migrate
to OMENative. Reasoning: LWS's `RecreateGroupOnPodRestart` tears
down the whole group on eviction without surge protection —
unsafe.

Metric `alfred_lws_recommendations_total{isvc, action: manual}`
increments. Dashboard can alert operators that manual
defragmentation is needed.

#### Failure handling

- Eviction failure (PDB blocks, API error): emit event, retry on
  next decision loop.
- Migration request rejected by OMENative
  (`InsufficientCapacity`, `RateLimited`, `MigrationInProgress`):
  log reason, mark candidate in cooldown, try another candidate.
- Migration request accepted but `Status.MigrationHistory`
  reports `Failed`: metric increment, event, cooldown for that
  workload.

### Policy Model

Configured via `alfred-config` ConfigMap:

```yaml
config.yaml: |
  schemaVersion: 1

  # Overall mode
  mode: recommend-only    # recommend-only | execute

  # Triggering
  fragmentationThreshold: 0.5
  decisionLoopInterval: 5m
  observationLoopInterval: 30s

  # Scoring weights
  scoring:
    contiguityWeight: 0.3
    binPackWeight: 0.2
    schedulabilityWeight: 0.3
    pendingPenaltyWeight: 0.2
    referenceWorkloadSizes: [1, 2, 4, 8, 16, 32]

  # Rate limiting
  maxInFlightMigrations: 3
  maxMigrationsPerHour: 10
  perWorkloadCooldownMinutes: 30
  perNodeCooldownMinutes: 10

  # Circuit breaker
  circuitBreaker:
    failureRateThreshold: 0.5        # 50% of recent 10 migrations
    pauseDurationMinutes: 60
    recentMigrationsWindow: 10

  # Maintenance windows
  maintenanceWindows:
    - start: "08:00"
      end: "18:00"
      timezone: "UTC"
      daysOfWeek: [Mon, Tue, Wed, Thu, Fri]
  emergencyPendingAgeMinutes: 10     # override window if pending pod age exceeds this

  # Per-workload defaults
  defaultMovable: true

  # Execution surfaces
  rawDeploymentEvictionEnabled: true
  omenativeMigrationEnabled: true
  lwsRecommendationsEnabled: true    # produce recommendations for LWS (never execute)

  # Spot-node policy
  spotPolicy:
    avoidAsTarget: true              # exclude spot nodes from placement hints
    preferAsSource: true             # prioritize evacuating spot nodes
    preemptibleLabels: [node.kubernetes.io/preemptible, cloud.google.com/gke-preemptible]

  # Multi-tenancy
  tenantBoundary: namespace          # tenant = namespace
  allowCrossTenantOptimization: false

  # Output
  recommendationsConfigMapEnabled: true
  recommendationsConfigMapName: alfred-recommendations

  # Logging
  logLevel: info
  structuredLogging: true
```

Alfred watches the ConfigMap and reloads on change. Invalid
`schemaVersion` triggers fallback to last-known-good with an event
on the ConfigMap and metric `alfred_policy_reload_total{outcome:
failure}`.

**Per-workload overrides via InferenceService annotations:**

```yaml
metadata:
  annotations:
    alfred.ome.io/movable: "false"                       # opt out entirely
    alfred.ome.io/priority: "0.3"                        # lower = more protected
    alfred.ome.io/cooldown-minutes: "60"                 # per-workload override
    alfred.ome.io/opt-out-reason: "critical production"  # operator note
    alfred.ome.io/tenant-group: "team-alpha"             # for cross-tenant opt-in
```

Annotations win over ConfigMap defaults.

### Opt-In / Opt-Out Semantics

Default: opt-in per `policy.defaultMovable: true`. A workload is
eligible unless:
- Annotation `alfred.ome.io/movable: "false"`.
- In cooldown.
- In active migration.
- Strategy is unsupported (LWS → recommendation-only;
  MultiNodeRayVLLM / Serverless → not managed).

Operators preferring opt-in-only set
`policy.defaultMovable: false`; then workloads need
`alfred.ome.io/movable: "true"` explicitly.

### Rate Limiting and Safety Bounds

- **In-flight cap (cluster-wide)**: default 3. Alfred tracks
  in-flight migrations and refuses to dispatch more.
- **Per-hour cap (cluster-wide)**: default 10.
- **Per-workload cooldown**: default 30 minutes after last
  migration, inclusive of failures.
- **Per-node cooldown**: default 10 minutes after a migration
  touches the node (source or target).
- **Circuit breaker**: if the recent-10-migrations failure rate
  exceeds 50%, pause all execution for 60 minutes; emit critical
  event.
- **Dry-run mode**: `policy.mode: recommend-only` disables
  execution entirely. Useful during rollout.

### Concurrent Operation Awareness

- **HPA**: Alfred checks HPA status conditions / scale history for
  the target InferenceService. If HPA is actively scaling within
  the last 2 minutes, the ISVC is deferred.
- **Cluster Autoscaler**: nodes annotated
  `cluster-autoscaler.kubernetes.io/scale-down-disabled: "true"`
  are excluded from placement hints. Nodes in active CA scale-down
  (detected via CA's deletion-candidate label) are also excluded.
- **OMENative in-flight migrations**: if
  `InferenceService.Status.MigrationHistory` has an entry with
  `phase: InProgress`, Alfred defers new requests for that ISVC.
- **Node maintenance**: nodes with common maintenance taints
  (`node.kubernetes.io/unschedulable`, custom `ome.io/maintenance`)
  excluded from targets.

### Spot and Preemptible Nodes

Policy-driven:

```yaml
spotPolicy:
  avoidAsTarget: true
  preferAsSource: true
  preemptibleLabels: [...]
```

**Detection**: Alfred checks each node's labels against
`spotPolicy.preemptibleLabels` (or optionally, against standard K8s
label `node.kubernetes.io/preemptible`).

**Behavior**:
- `avoidAsTarget: true` excludes preemptible nodes from
  `hint_target_nodes`. Stable workloads preferred on
  non-preemptible nodes.
- `preferAsSource: true` boosts the priority of workloads on
  preemptible nodes (evacuate before preemption event).
- Per-workload override via
  `alfred.ome.io/spot-policy: avoid|migrate|ignore` annotation.

### Multi-Tenancy

Tenant boundary = namespace by default. Alfred-produced
recommendations and migrations are scoped to the ISVC's own
namespace — Alfred does not migrate workload A in namespace `ns1`
to benefit workload B in namespace `ns2`.

Operators with cross-tenant intent can set
`alfred.ome.io/tenant-group: <group-name>` on ISVCs across
namespaces; Alfred treats ISVCs in the same group as cross-migratable
(still respecting other filters).

Policy flag `allowCrossTenantOptimization: false` makes the
boundary hard even if tenant-group annotations are present.

### Model-Download Coordination

Alfred's placement hints filter by
`BaseModel.Status.NodesReady` / `ClusterBaseModel.Status.NodesReady`
(OEP-0007 Q-017). If no ready node is feasible as a target:
- Skip the migration for this candidate.
- Emit event `NoFeasibleTarget` on the ISVC with the reason.
- Do not trigger a pre-download in v1. Operator pre-provisions
  the model (via BaseModel / ClusterBaseModel distribution) before
  expecting migration.

A future iteration may add auto-triggered pre-download: Alfred
requests model-agent to distribute the model to a specific target
node before retrying the migration. Out of scope for v1.

### Degraded Mode

At startup, Alfred queries API discovery for OMENative's
registration (presence of `WorkloadStrategy` feature-gate status
and ability to read `InferenceService.Status.MigrationHistory`
field schema).

**If OMENative is not installed** (feature gate off or strategy
not registered):
- Alfred operates in degraded recommend-only mode for all
  workloads, even those annotated with `movable: true`.
- RawDeployment eviction remains enabled.
- Metric `alfred_omenative_unavailable` gauge set to 1.
- Event `OMENativeUnavailable` emitted on transition into degraded
  mode, not on every decision loop.
- ConfigMap policy `omenativeMigrationEnabled: true` is a no-op
  while in degraded mode.

### Deployment Model

Ships as:
- Binary: `cmd/alfred/main.go` → `alfred` container image.
- Helm chart: `charts/ome-alfred/` (or sub-chart of
  `ome-resources`).
- Deployment: 3 replicas, leader election enabled.
- ServiceAccount + ClusterRole + ClusterRoleBinding per
  [RBAC](#rbac) below.
- ConfigMap `alfred-config` with default values.

Alfred is installable alongside OME (same Helm release) or
independently. Hard dependencies:
- OME `InferenceService` CRD installed.
- OMENative strategy registered (otherwise degraded mode).

### Leader Election

`client-go/tools/leaderelection` with a Lease resource:
- Lease duration: 15s
- Renew deadline: 10s
- Retry period: 2s

Only the leader runs the decision loop. All replicas run the
observation loop (so Prometheus scrapes any replica). Mirrors
cluster-autoscaler's pattern.

On leader loss, in-flight migration tracking is transient state;
the new leader rebuilds it from `InferenceService.Status.MigrationHistory`
(OMENative is the authoritative source) plus Alfred's
`alfred-recommendations` ConfigMap.

### RBAC

```yaml
# Nodes (read-only)
- apiGroups: [""]
  resources: [nodes]
  verbs: [get, list, watch]

# Pods (read) + eviction (create on subresource)
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch]
- apiGroups: [""]
  resources: [pods/eviction]
  verbs: [create]

# ConfigMaps (read for policy loading, write only to pre-created Alfred-owned ConfigMaps)
- apiGroups: [""]
  resources: [configmaps]
  verbs: [get, list, watch]
- apiGroups: [""]
  resources: [configmaps]
  verbs: [update, patch, delete]
  resourceNames:
    - alfred-config
    - alfred-recommendations

# Events
- apiGroups: [""]
  resources: [events]
  verbs: [create, patch]

# Leader election
- apiGroups: [coordination.k8s.io]
  resources: [leases]
  verbs: [create, get, update]

# OME CRDs (read)
- apiGroups: [ome.io]
  resources:
    - inferenceservices
    - inferenceservices/status
    - servingruntimes
    - clusterservingruntimes
    - basemodels
    - clusterbasemodels
    - acceleratorclasses
  verbs: [get, list, watch]

# OME InferenceService annotation patching (narrow contract)
- apiGroups: [ome.io]
  resources: [inferenceservices]
  verbs: [patch]
```

**Authorization boundary**: the dedicated Alfred service account is the
only principal allowed to trigger OMENative migration in v1. In
production, no other principal should be granted generic `patch` on
`InferenceService`, and cluster-side admission must still reject
migration-annotation mutation by non-Alfred callers even if such RBAC
exists.

**RBAC invariant**: Alfred's only allowed `patch` effect on
`InferenceService` is to write or delete
`ome.io/migration-request-v1-*` annotations. An internal patch-gateway
helper still guards this at runtime, but it is **not** the primary
security boundary.

**Mandatory cluster-side enforcement**: Alfred execute mode requires a
`ValidatingAdmissionPolicy` (K8s 1.30+) installed by the Helm chart.
If the policy or binding is absent, Alfred must refuse to start in
`mode: execute` and fall back to recommend-only.

The reference policy object belongs in the Alfred Helm chart template
(`charts/ome-alfred/templates/admission-policy.yaml`, or the equivalent
Alfred subtree under `charts/ome-resources/` if packaged there). The
negative test matrix for that policy is covered by integration tests 21
and 22 below.

The policy must enforce two invariants:

1. Only Alfred's service account may add, update, or delete
   `ome.io/migration-request-v1-*` annotations.
2. Even Alfred's service account may not change
   `spec`, `status`, labels, finalizers, owner references, or
   non-migration annotations as part of that patch.

The exact CEL expression is implementation detail and must be covered by
negative integration tests. The design contract is the enforcement
behavior above, not a loosely reviewed sample snippet.

PATCH-type caveat: the implementation must be validated against JSON
merge patch and JSON Patch semantics, not only one patch encoding. If
the chosen admission checks cannot soundly prove the invariants above
for a patch type Alfred might send, Alfred execute mode must reject that
patch type rather than rely on ambiguous CEL behavior.

**ConfigMap write boundary**: Alfred does not create ConfigMaps at
runtime. Helm pre-creates `alfred-config` and, if recommendation
snapshots are enabled, `alfred-recommendations`; Alfred only
updates/patches/deletes those named objects.

### Observability

**Prometheus metrics**:

- `alfred_cluster_fragmentation_score` (gauge, 0-1)
- `alfred_node_fragmentation_score{node}` (gauge)
- `alfred_gpu_capacity{node,status}` (gauge; status: total/allocated/free/contiguous_max)
- `alfred_schedulability_score{size}` (gauge)
- `alfred_pending_pod_count` (gauge)
- `alfred_pending_pod_gpu_requirements{size}` (gauge)
- `alfred_recommendations_produced_total{workload,component,reason,executable}` (counter)
- `alfred_recommendations_accepted_total{workload,component}` (counter)
- `alfred_recommendations_rejected_total{workload,component,reason}` (counter)
- `alfred_migration_calls_total{workload,mode,surface}` (counter; surface: eviction/omenative)
- `alfred_migration_outcome_total{workload,mode,outcome}` (counter; outcome: completed/failed/timeout)
- `alfred_lws_recommendations_total{isvc,action}` (counter; action: manual)
- `alfred_observation_loop_duration_seconds` (histogram)
- `alfred_decision_loop_duration_seconds` (histogram)
- `alfred_leader_status{pod}` (gauge; 0/1)
- `alfred_policy_reload_total{outcome}` (counter; outcome: success/failure)
- `alfred_circuit_breaker_state` (gauge; 0: closed/1: open)
- `alfred_omenative_unavailable` (gauge; 0/1)

**Events** on InferenceService:
- `FragmentationRecommendationProduced`
- `MigrationRequested`, `MigrationCompleted`, `MigrationFailed`, `MigrationRejected`
- `MigrationSkippedOptOut`
- `MigrationSkippedMaintenanceWindow`
- `MigrationSkippedCooldown`
- `MigrationSkippedRateLimit`
- `LWSMigrationUnsupported`
- `NoFeasibleTarget`

**Events** on Alfred's own ConfigMap:
- `PolicyReloadFailed`
- `CircuitBreakerOpened` / `CircuitBreakerClosed`

**Events** cluster-wide:
- `OMENativeUnavailable`

**Logs**: structured JSON. Every multi-step operation carries a
correlation ID (recommendation UUID).

**Audit ConfigMap (optional)**: if policy's
`recommendationsConfigMapEnabled: true`, Alfred maintains
`alfred-recommendations` ConfigMap in its own namespace:

```yaml
data:
  recommendations.json: |
    {
      "generated_at": "2026-04-16T12:00:00Z",
      "cluster_fragmentation_score": 0.62,
      "threshold": 0.5,
      "recommendations": [
        {
          "id": "rec-001",
          "workload": "prod/llama-70b",
          "component": "engine",
          "instance": 0,
          "from_node": "node1",
          "hint_target_nodes": ["node3", "node7"],
          "benefit_score": 0.18,
          "cost_score": 0.1,
          "final_score": 0.08,
          "reason": "fragmentation",
          "executable": true,
          "strategy": "OMENative",
          "status": "accepted"
        }
      ]
    }
```

Provides a single structured snapshot operators can read without
scraping events.

## Test Plan

### Unit Tests

Target coverage per package:

- `observer`: ≥ 90%
- `scorer`: ≥ 95%
- `recommender`: ≥ 90%
- `executor`: ≥ 85%
- `policy`: ≥ 90%
- `metrics`: ≥ 80%
- `controller`: ≥ 80%

Synthetic cluster snapshot harness for unit testing without real
K8s.

### Integration Tests

1. **End-to-end observation**: deploy Alfred in a kind cluster,
   deploy mock InferenceServices (via test CRDs), verify
   `alfred_cluster_fragmentation_score` changes with workload
   deployments.
2. **Recommendation generation**: construct a 3-single-GPU-on-3-nodes
   scenario. Verify Alfred produces 2 consolidation
   recommendations.
3. **Pending-pod prioritization**: create a Pending 8-GPU pod;
   verify Alfred boosts candidates that would unblock it.
4. **RawDeployment eviction**: `mode: execute`; verify Alfred
   calls Eviction API for a RawDeployment workload.
5. **OMENative migration invocation**: OMENative workload; verify
   Alfred writes `ome.io/migration-request-v1-<uuid>` on the
   InferenceService. Mock OMENative acks (clears annotation,
   emits status).
6. **LWS recommendation-only**: LWS-backed workload; verify
   recommendation emitted but no migration request; event reflects
   `LWSMigrationUnsupported`.
7. **Opt-out**: `alfred.ome.io/movable: "false"` excludes the
   workload from candidates; event explains.
8. **Per-workload cooldown**: execute a migration, verify no new
   attempt for the same workload within cooldown.
9. **Maintenance window**: active window; Alfred continues
   recommending, does not execute.
10. **Rate limiting**: 10 candidates; only 3 migrations in-flight.
11. **Policy hot reload**: update ConfigMap; Alfred reloads
    without restart.
12. **Leader election**: 3 replicas; kill leader; new leader
    elected within 20s.
13. **Post-migration health monitoring**: migration succeeds but
    target GPU fails within window; Alfred marks node suspect,
    backs off.
14. **HPA coexistence**: HPA scaling the target ISVC; Alfred
    defers.
15. **CA coexistence**: node marked `scale-down-disabled`;
    excluded from hints.
16. **OMENative-unavailable degraded mode**: feature gate off;
    Alfred enters degraded mode, only RawDeployment evictions
    occur.
17. **Circuit breaker**: simulate 6+ failed migrations; verify
    circuit opens, pauses execution.
18. **Non-OME workload**: Kubeflow Notebook Pod on a node; Alfred
    observes but does not migrate; fragmentation score accounts
    for its GPU consumption.
19. **Multi-tenancy default**: workload in namespace A cannot be
    migrated to benefit workload in namespace B without explicit
    tenant-group annotation.
20. **Spot node avoidance**: preemptible-labeled node excluded
    from targets; preemptible source prioritized.
21. **Admission hardening**: non-Alfred caller attempts to modify
    `ome.io/migration-request-v1-*`; request rejected by mandatory
    admission policy.
22. **Alfred narrow patch enforcement**: Alfred caller attempts to
    modify `spec` or non-migration annotations in the same patch;
    request rejected by admission policy.
23. **Unsupported schema version**: Alfred writes a request with
    unsupported `schemaVersion`; OMENative rejects it and Alfred marks
    the workload recommend-only for OMENative execution until versions
    are compatible.

### Chaos / Robustness

- Kill Alfred during in-flight migration; verify recovery.
- API server slow response; verify backoff.
- OMENative rejects all requests; verify circuit breaker triggers.
- InferenceService deleted mid-migration; verify Alfred cleans up
  gracefully.

### Simulation

- 10 random cluster states; verify post-Alfred state has lower or
  equal fragmentation.
- Adversarial states designed to trick scorer; verify safe
  rejection or correct handling.
- Long-running simulation (100 decision cycles) — verify no
  thrashing (bounded migration count per workload).

## Graduation Criteria

### Alpha

- Unit tests ≥ 80% coverage.
- Integration tests 1–10 passing.
- Deployment via Helm documented.
- Feature gate `AlfredGPUDefragmenter` disabled by default in OME
  Helm chart.
- Policy schema documented.
- `recommend-only` is default mode; `execute` is opt-in.

### Beta

- Integration tests 1–23 + chaos + simulation passing.
- 2+ production users running Alfred for ≥ 60 days without
  Sev1/Sev2 incidents attributed to Alfred.
- Dashboard + runbook published.
- RBAC policy review by security reviewer.
- OMENative at Beta maturity (execute path validated end-to-end).

### GA

- 6 months of Beta.
- Test coverage ≥ 85%.
- Policy schema stabilized.
- OMENative at GA.
- Deprecation story documented (if Alfred is ever replaced).

## Implementation History

- 2026-04-16: OEP-0008 initial draft.
- 2026-04-16: Design questions consolidated in
  `.claude/designs/design-questions.md`; 37 decisions locked.
- 2026-04-16: OEP-0008 updated to reflect locked decisions (this
  document).
- TBD: Alpha implementation.
- TBD: First Beta user.
- TBD: GA.

## Drawbacks

1. **Cluster-wide logical controller.** Alfred is one actor making
   cluster-wide decisions. Bugs have broad blast radius. Similar
   risk profile to cluster-autoscaler.
2. **Requires OMENative for full value on multi-pod workloads.**
   Until users adopt OMENative, Alfred is limited to RawDeployment
   + recommendations-only for others.
3. **Heuristic, not optimal.** Greedy candidate selection may miss
   non-obvious consolidations. Trade: simplicity and speed over
   global optimality.
4. **Operational overhead.** Operators learn Alfred's metrics,
   policy schema, failure modes.
5. **Policy complexity.** ConfigMap schema has many knobs. Good
   defaults mitigate.
6. **Cross-controller interaction space.** Alfred, OMENative, HPA,
   CA, scheduler form a complex interaction graph. Subtle bugs
   possible. Mitigated by conservative defaults and extensive
   observability.
7. **Narrow write contract is brittle.** Annotation contract with
   OMENative is the primary coupling point. Race conditions
   possible during annotation write/clear; mitigated by idempotent
   UUID semantics.

## Alternatives

### Alternative 1: Alfred as a scheduler plugin

Run Alfred inside a custom K8s scheduler, intercepting scheduling
events to apply bin-packing at placement time.

- **Pros:** Scheduling is the right place for placement logic.
- **Cons:** Only affects new pods; does not handle runtime
  defragmentation. Would need combination with a separate actor
  for migration anyway.

Rejected — core problem is runtime defragmentation, not initial
placement.

### Alternative 2: Embed Alfred in OME manager

Run Alfred's logic inside the main OME controller manager.

- **Pros:** Single deployment, shared informer cache.
- **Cons:** Alfred's failure takes down OME core. Operational
  profiles differ. Less flexible release cadence.

Rejected — keeping them separate is cleaner.

### Alternative 3: Alfred as a CRD-driven system

Alfred's policy = CRD (`DefragmentationPolicy`); recommendations =
CRD (`RecommendationHistory`).

- **Pros:** K8s-idiomatic configuration.
- **Cons:** Violates the "no new CRDs" hard constraint; adds API
  surface; high-frequency data (recommendations) in etcd is
  expensive.

Rejected — no new CRDs.

### Alternative 4: Don't build Alfred; manual operator intervention

Operators manually trigger migrations during scheduled maintenance.

- **Pros:** Zero new components.
- **Cons:** Does not scale; reactive not preventive; defrag
  continuously drifts.

Rejected — problem scales with cluster size and workload churn.

### Alternative 5: Extend cluster-autoscaler

Fork / contribute to CA to add GPU-aware defragmentation.

- **Pros:** Leverages CA's existing architecture.
- **Cons:** CA is node-focused (scale nodes), not pod-focused
  (move pods). Different problem. Fork is high-cost.

Rejected — architectural mismatch.

## Open Questions

Remaining genuinely-open questions (P3 in `design-questions.md`);
research-level, not blocking design completeness.

1. **Fragmentation score formula validation** (Q-049). Defaults ship
   with the proposed weights; alpha-user feedback will refine them.
   What's the evaluation methodology? TBD during alpha.

2. **GPU topology awareness within a node (NVLink, PCIe)** (Q-040).
   v1 treats intra-node GPUs as fungible. v2+ considers NVLink
   neighborhoods. Source of topology data still open (gpu-feature-discovery,
   DCGM, custom?).

3. **RDMA fabric topology across nodes** (Q-039). Multi-node
   migration targets need full RDMA mesh. Data source unclear.
   Research.

4. **DRA (Dynamic Resource Allocation) integration** (Q-041).
   K8s 1.31+ `ResourceClaims` reshape GPU allocation. Alfred's view
   of "GPU allocation" must adapt. Revisit as DRA adoption
   progresses.

5. **Historical learning from past migration outcomes** (Q-042).
   Adjust scoring weights or cost model based on real-world
   outcomes. v2+ feature; data collection starts in v1.

6. **Cost accounting — runtime vs static** (Q-043). v1 uses static
   per-workload-shape cost constants. v2 measures actual
   disruption cost from past migrations.

7. **Dedicated UI beyond metrics + events** (Q-044). v1 = metrics +
   events + ConfigMap. Dashboards operator-owned. No dedicated UI.

8. **Unified "OME Ops" controller** (Q-045). Alfred-defragmenter +
   auto-patch + auto-repair + maintenance tooling unified into one
   controller later. Future architecture question. Current design
   is compatible — Alfred coexists with but does not absorb others.

9. **Auto-patch / auto-repair re-introduction** (Q-046, Q-022 on
   plugin architecture). Separate OEPs when needed. Alfred
   designed not to preclude this.

10. **Model pre-download coordination** (Q-032 extension). v2 may
    add auto-triggered pre-download to target nodes before
    retrying migrations blocked by model unavailability.

11. **Final name** (Q-048). "Alfred" is working name, inherited
    from ome-operator. Candidates: `GPUDefragmenter`,
    `WorkloadConsolidator`, `OMEScheduler`. Finalize before GA.

12. **Kueue interaction detail** (Q-027). Alfred respects
    `SuspendedLabel`. Full integration with queueing and quotas
    depends on OEP-0003 AcceleratorClass maturity. Revisit then.

13. **Priority-class boost semantics** (new, not yet tracked in
    `design-questions.md`; add as Q-053 if this matures). Operator-set
    `priorityClass` on InferenceService influences scheduler
    placement; should Alfred also weight candidate-selection by
    `priorityClass`? Current design: no. Priority is for
    placement/preemption, not defragmentation. May revisit.
