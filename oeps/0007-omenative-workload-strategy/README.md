# OEP-0007: OMENative Workload Strategy

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Context](#context)
  - [Problem 1: LWS Restart Semantics Are Unsuitable for Defragmentation](#problem-1-lws-restart-semantics-are-unsuitable-for-defragmentation)
  - [Problem 2: Downstream Alternatives Multiply User-Visible CRDs](#problem-2-downstream-alternatives-multiply-user-visible-crds)
  - [Problem 3: No Native Migration Primitive Exists](#problem-3-no-native-migration-primitive-exists)
  - [Problem 4: External Dependency Drift and LLM-Specific Primitives](#problem-4-external-dependency-drift-and-llm-specific-primitives)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
  - [What OMENative Is, Conceptually](#what-omenative-is-conceptually)
  - [Terminology and Core Concepts](#terminology-and-core-concepts)
  - [User Stories](#user-stories)
  - [Risks and Mitigations](#risks-and-mitigations)
- [Design Details](#design-details)
  - [Architecture Overview](#architecture-overview)
  - [Internal Data Model](#internal-data-model)
  - [Pod Topology Model](#pod-topology-model)
  - [Restart Policy](#restart-policy)
  - [Update Strategy (In-Place and Recreate)](#update-strategy-in-place-and-recreate)
  - [Coordinated Cross-Component Rollout](#coordinated-cross-component-rollout)
  - [Service Discovery](#service-discovery)
  - [Readiness and Warmup](#readiness-and-warmup)
  - [Port Allocation](#port-allocation)
  - [Migration API and Mechanics](#migration-api-and-mechanics)
  - [Strategy Migration Procedure (LWS → OMENative)](#strategy-migration-procedure-lws--omenative)
  - [Model Availability Preconditions](#model-availability-preconditions)
  - [Gang Scheduling](#gang-scheduling)
  - [Webhook Interaction](#webhook-interaction)
  - [Status Propagation](#status-propagation)
  - [RBAC](#rbac)
  - [Observability](#observability)
- [Coexistence with LWS-Backed Modes](#coexistence-with-lws-backed-modes)
  - [Component Strategy Selection Rules](#component-strategy-selection-rules)
  - [Deprecation Plan](#deprecation-plan)
- [Feature Gating and Rollout](#feature-gating-and-rollout)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Implementation History](#implementation-history)
- [Drawbacks](#drawbacks)
- [Alternatives](#alternatives)
- [Open Questions](#open-questions)
<!-- /toc -->

## Summary

OMENative is a new workload strategy for OME's `InferenceService` that
manages inference workload pods directly using native Kubernetes
primitives (`Pod`, `Service`, `PodDisruptionBudget`,
`ControllerRevision`, and optional scheduler-plugins `PodGroup`)
without depending on `sigs.k8s.io/lws` (LeaderWorkerSet) or
`sigs.k8s.io/rbgs` (RoleBasedGroup). The strategy introduces a
four-level internal hierarchy — `InferenceService` → `Component` →
`Instance` → `Runner` — and provides first-class primitives for:

1. **Atomic group semantics** — Instance-level gang scheduling,
   restart policies, and readiness aggregation.
2. **In-place pod updates** — image-only changes use a predeclared
   controller-owned readiness gate (`ome.io/serving`) plus endpoint
   drain observation so EmptyDir-backed model weights survive a
   container restart.
3. **Native migration verb** — a first-class migration API on
   `InferenceService` that Alfred consumes to surge a replacement
   Instance, wait for readiness, drain the old one, and delete it.

Cross-Component coordinated rollout and OMENative-specific autoscaling
are explicitly deferred from v1 mechanics.

OMENative coexists with the existing LWS-backed `MultiNode` and
`PDDisaggregated` deployment modes through a six-release deprecation
window. Users opt into OMENative per-Component at their own pace.
Eventually the LWS-backed paths are removed and `sigs.k8s.io/lws`
leaves OME's module graph.

`RawDeployment` remains the default for simple single-pod components.
OMENative is primarily the successor to the LWS-backed multi-pod paths,
and an opt-in for single-pod workloads only when Alfred needs
OMENative's migration and drain lifecycle.

The strategy introduces **no new user-visible CRDs**. `InferenceService`
remains the sole user-facing workload API. Every internal concept
(`Instance`, `Runner`, migration request, coordination policy) is
either surfaced on the existing InferenceService spec/status or
materializes as standard K8s resources that users already understand.

A related design, [OEP-0008 (Alfred GPU
Defragmenter)](../0008-alfred-gpu-defragmenter/README.md), depends on
OMENative's migration verb to safely move Instance-scoped workloads
during cluster defragmentation. The two designs land and ship
independently.

## Motivation

### Context

OME defines eight CRDs in the `ome.io/v1beta1` API group and runs
four controllers. The workload-facing CRD is `InferenceService`, which
today supports six deployment modes defined in
`pkg/constants/constants.go:440-446`:

| Mode | Primary resource | Multi-pod Instance | Multi-Component | Uses LWS? |
|------|------------------|--------------------|-----------------|-----------|
| `RawDeployment` | `Deployment` | No | Router+Engine via separate Deployments | No |
| `MultiNode` | `LeaderWorkerSet` | Yes (leader + workers) | No | **Yes** |
| `MultiNodeRayVLLM` | `RayCluster` | Yes (head + workers) | No | No |
| `Serverless` | `Knative Service` | No | Per-component | No |
| `PDDisaggregated` | `LeaderWorkerSet` × 2 + Deployment | Yes per component | Yes (router + engine + decoder) | **Yes** |
| `VirtualDeployment` | (none) | N/A | N/A | No |

LWS is therefore the backbone for OME's multi-node tensor-parallel
engines and for prefill-decode disaggregated serving. An audit of OME
against its LWS usage (see
`pkg/controller/v1beta1/inferenceservice/reconcilers/lws/lws_reconciler.go`,
~130 lines of construction + reconciliation, plus peripheral reads in
status propagation) confirmed:

- LWS usage is localized. Only 5 Go files import
  `sigs.k8s.io/lws/api/leaderworkerset/v1`. All LWS-specific logic is
  in `lws_reconciler.go`.
- OME's public API (`InferenceService` spec) does not reference LWS
  types. The `Leader` / `Worker` / `Size` fields are semantic names.
- OME does not consume LWS-specific pod labels. Status propagation
  treats LWS as opaque (reads `Conditions`, `Generation`,
  `Annotations["resourceVersion"]`).
- Replacement scope estimate: refactor the `lws/` reconciler to emit
  direct `Pod` sets + `Service` + `PodDisruptionBudget` +
  `ControllerRevision`; update status readers. The mechanics are
  materially larger than the current LWS adapter because OMENative
  owns instance lifecycle directly rather than delegating it to
  StatefulSet.

LWS is pinned at `sigs.k8s.io/lws v0.5.1` in `go.mod` (upstream at
v0.8.0 as of 2026-04). Upstream has added useful features (KEP#407
Gang Scheduling in v0.7.0, KEP#552 size updates,
`RecreateGroupAfterStart` annotation in v0.8.0), but none address the
core problems documented below.

OEP-0006 introduced the `WorkloadStrategy` abstraction. PR #506 ships
the framework with a `SingleComponentStrategy` preserving the existing
reconciler paths. OMENative is a new strategy that registers alongside
it.

### Problem 1: LWS Restart Semantics Are Unsuitable for Defragmentation

OME configures every LWS it creates with
`Spec.LeaderWorkerTemplate.RestartPolicy = RecreateGroupOnPodRestart`
(`lws_reconciler.go:98`). Verified in LWS source
(`sigs.k8s.io/lws@v0.6.2/pkg/controllers/pod_controller.go:192-229`),
this policy causes the LWS controller to **delete the leader pod with
foreground propagation** whenever any pod in the group is deleted.
Leader deletion cascades through the worker StatefulSet; the group
goes to zero replicas before beginning to come back.

The LWS `RolloutStrategy.RollingUpdate` with `MaxSurge=1,
MaxUnavailable=1` (`lws_reconciler.go:78-93`) is triggered only by a
spec change, not by pod deletion. An eviction flows through
`handleRestartPolicy`, not through the rollout controller, so
`MaxSurge` never creates a surge replica during eviction.

Consequence for a defragmenter (Alfred) wanting to migrate a workload
by evicting a pod: the entire group is torn down (no surge); the
scheduler attempts to place `N` replacement pods simultaneously; if
any fails, the LWS replica is not Ready and the service is down; no
rollback is possible because the evicted pods and their GPU
allocations are gone.

LWS 0.7/0.8 do not change this. KEP#407 gang scheduling addresses
**initial** placement, not post-loss restart. `RecreateGroupAfterStart`
controls start-phase behavior, orthogonal to the defragmentation
case.

Any multi-Instance workload deployed through LWS cannot be safely
migrated via eviction. For OMENative-managed workloads, migration
becomes a first-class operation with explicit surge semantics (see
[Migration API and Mechanics](#migration-api-and-mechanics)).

### Problem 2: Downstream Alternatives Multiply User-Visible CRDs

RBG (RoleBasedGroup, `sigs.k8s.io/rbgs`, sibling to OME in the
sgl-project org) is a natural candidate to replace LWS — it has
native multi-role coordination and richer restart policies. RBG
KEP-30 (InstanceSet) explicitly proposes replacing LWS inside RBG for
the same dependency-drift reasons this OEP cites.

Adopting RBG directly in OME would, however, install the following
user-visible CRDs:

- `RoleBasedGroup` (`workloads.sgl.ai/v1alpha2`)
- `RoleInstanceSet` (`workloads.sgl.ai/v1alpha2`)
- `RoleInstance` / `Instance` (`workloads.sgl.ai/v1alpha1|v1alpha2`)
- `CoordinatedPolicy`, `RoleBasedGroupScalingAdapter`

Four layers of CRDs between `InferenceService` and a `Pod`, each
observable to operators via `kubectl api-resources` and `kubectl
describe`. For operators debugging a failing workload this is a
significant mental-load increase.

OMENative borrows RBG's **design patterns** (Instance-as-atomic-unit,
predeclared readiness gates, revision history, gang adapters) while
emitting **only native K8s resources**. Users see `InferenceService`
plus vanilla `Pod`, `Service`, `PodDisruptionBudget`,
`ControllerRevision`, and optional scheduler-plugins `PodGroup`.

### Problem 3: No Native Migration Primitive Exists

Neither LWS nor RBG exposes a first-class "migrate this Instance from
here to there" verb. Both require spec modification to trigger a
rolling update with affinity changes. A defragmenter that follows
"alfred does not write to the workload's reconciliation state" cannot
invoke this path; it must delegate to the workload controller, and
the workload controller must have a migration verb to delegate to.

OMENative makes migration a first-class operation:

- A migration request is an annotation on InferenceService
  (`ome.io/migration-request-v1-<uuid>`) with a versioned JSON payload.
- OMENative's controller consumes the request, executes surge
  migration (the only supported v1 mechanism), and tracks progress
  in `Status.MigrationHistory`.
- Alfred writes only this annotation; OMENative handles everything
  else.

### Problem 4: External Dependency Drift and LLM-Specific Primitives

LWS and RBG evolve on schedules independent of OME's release cadence.
OME is three minor versions behind LWS upstream. Upgrading requires
validating multiple new features (gang scheduling, partition updates,
size changes), integrating with controller changes, coordinating
CRD upgrades in user clusters. This drift is a continuous tax.

Separately, LLM serving needs primitives that general workload
controllers don't prioritize:

- **In-place image update preserving model weights.** LWS has no
  native in-place update. RBG added it (KEP-30 InstanceSet) as a new
  feature. OMENative gets this for free in its design.
- **Migration-time model availability preconditions.** OMENative
  inherits OME's existing per-node model labels
  (`models.ome.io/{ns}.basemodel.{name}=Ready`) and uses them
  directly in pod affinity — no new primitive needed.
- **Coordinated rollout for PD workloads.** Prefill/decode roles
  update in proportion. RBG's `CoordinatedPolicy` design is
  proven; OMENative may adopt a similar semantic later, but v1 keeps
  rollout correctness scoped to one Component at a time.
- **KV cache handoff hooks, RDMA fabric awareness.** Future
  directions; OMENative's internal hierarchy accommodates them.

### Goals

1. **Replace LWS** as the underlying primitive for OME's multi-node
   and PD paths. Users can opt into OMENative per-Component;
   eventually LWS-backed paths are removed and `sigs.k8s.io/lws` is
   dropped from `go.mod`.
2. **Zero new user-visible CRDs.** `InferenceService` remains the
   sole user-facing workload resource. Every internal concept is
   surfaced through the existing spec/status or materializes as
   standard K8s resources.
3. **Unified handling** of single-pod, single-Component multi-node,
   and multi-Component PD workloads under one strategy implementation.
4. **First-class migration verb** on InferenceService, with
   surge-only semantics in v1.
5. **In-place updates** as default. Container image and metadata
   changes preserve EmptyDir-backed model state and avoid full
   model-weight reload when the runtime tolerates a container restart.
6. **Keep the controller kernel narrow and implementable.**
   Single-Component correctness ships first; richer cross-Component
   coordination can layer on later.
7. **Coexist with LWS-backed modes** during a six-release deprecation
   window. No breaking changes; users migrate at their own pace.

### Non-Goals

1. **Not replacing Knative or RayCluster paths.** `Serverless` and
   `MultiNodeRayVLLM` modes remain unchanged.
2. **Not becoming a general-purpose workload controller.** OMENative
   is specialized for LLM inference.
3. **Not implementing a scheduler.** OMENative relies on the K8s
   scheduler. It supplies scheduling hints and gates; it does not
   replace scheduler logic.
4. **Not shipping OMENative-managed HPA in v1.** Existing modes keep
   their current scaling paths; OMENative v1 reconciles replica count
   from `InferenceService` spec and does not emit an HPA.
5. **Not owning cluster-wide defragmentation decisions.** That is
   Alfred's job (OEP-0008). OMENative provides the migration verb;
   Alfred decides when and why to call it.
6. **Not solving GPU health observation.** Standard K8s node
   conditions and AcceleratorClass status are consumed; OMENative
   adds no new telemetry collection.
7. **Not exposing any new CRDs.** Hard constraint.
8. **Not supporting cross-cluster InferenceService.** Explicit
   non-goal; if needed later, a separate OEP.
9. **Not shipping a user-facing rollback verb in v1.** Internal
   `ControllerRevision` is emitted for in-place update decisioning,
   but the user-facing rollback mechanism (replay revision N) is
   deferred to a follow-up.

## Proposal

### What OMENative Is, Conceptually

OMENative is a `WorkloadStrategy` registered in OME's strategy
manager (from OEP-0006) under the strategy name `OMENative`. The
strategy is selected per-Component on an InferenceService via the
existing `deploymentMode` field:

```yaml
spec:
  router:
    deploymentMode: Serverless      # router is free to pick any mode
  engine:
    deploymentMode: OMENative        # NEW mode introduced by this OEP
  decoder:
    deploymentMode: OMENative        # must match engine in PD mode
```

Engine and decoder deployment modes **must match** when both are
present — they are coupled through coordinated rollout and shared
migration semantics. Router's mode is independent (it is stateless,
CPU-only, decoupled via Service discovery). See [Component Strategy
Selection Rules](#component-strategy-selection-rules).

When selected, OMENative takes over reconciliation downstream of
"rendered Component specs":

```
InferenceService spec + ServingRuntime template + BaseModel metadata
        │
        ▼  (existing OME spec-merging code)
        │
        ▼
Rendered Component specs (engine, decoder, router)
        │
        ▼
OMENative strategy (THIS OEP)
        │
        ▼
Pod(s), Services, PDB, ControllerRevision, ConfigMap, RBAC,
optional PodGroup
```

The strategy emits:

- Directly managed pods with stable names per
  `(Component, Instance, Runner, ordinal)` (see [Pod Topology
  Model](#pod-topology-model)).
- Three `Service`s per Component: external (`<isvc>`),
  component-routing (`<isvc>-<component>`), and peer-DNS headless
  (`<isvc>-<component>-headless`). See [Service
  Discovery](#service-discovery).
- A `PodDisruptionBudget` per Component.
- A `ConfigMap` with runtime configuration.
- `ControllerRevision` per-Component for update-decisioning history.
- A scheduler-plugins `PodGroup` per multi-pod Instance when gang
  scheduling is enabled.
- `Role` / `RoleBinding` / `ServiceAccount` as needed.

### Terminology and Core Concepts

The following concepts live **inside OMENative's controller** or on
existing InferenceService fields. None are expressed as CRDs.

- **Component** (existing OME term). A top-level role within
  an inference service: `router`, `engine`, or `decoder`. Already
  defined in `InferenceService.Spec`.
- **Instance** (new). One replica of a Component — the atomic unit
  for gang scheduling, restart policy, migration, and readiness
  aggregation. A Component with `MinReplicas=3` has three Instances
  (indexed 0, 1, 2).
- **Runner** (extends existing OME concept). A kind of pod within
  an Instance. Named `leader`, `worker`, or `default` (for
  single-pod Instances). A Runner has a `size` (pod count) and a
  template (`RunnerSpec`, the existing container-level config type).
- **Pod.** Standard K8s Pod running a Runner's container spec.

Example — PD InferenceService `llama-70b` with router (2 replicas),
engine (2 Instances × 4 pods each), decoder (1 Instance × 2 pods):

```
InferenceService llama-70b
├── Component router (2 Instances)
│   ├── Instance 0: 1 Default Runner (size 1)
│   └── Instance 1: 1 Default Runner (size 1)
├── Component engine (2 Instances)
│   ├── Instance 0: 1 Leader Runner + 1 Worker Runner (size 3)
│   └── Instance 1: same shape
└── Component decoder (1 Instance)
    └── Instance 0: 1 Leader Runner + 1 Worker Runner (size 1)

Total: 5 Instances, 12 Pods
```

**Deployment Plan.** An internal reconciler type computed each
reconcile from `InferenceService` + `ServingRuntime` + `BaseModel`.
Drives creation of pods, Services, PDBs, revisions, and optional
PodGroups. Not persisted, not a CRD.

**Migration Request.** An external ask — "migrate Instance X off
Node Y, with optional target hints." Delivered via a temporary
annotation on InferenceService; tracked through `Status.MigrationHistory`.

### User Stories

#### Story 1: Operator upgrades an InferenceService to multi-node tensor parallelism

Operator has a 7B model on `RawDeployment`. Upgrade to a 70B model
requiring tensor parallelism across 2 nodes (1 leader + 1 worker).

Operator changes `InferenceService.spec.engine` to add `leader` and
`worker` sections with `worker.size: 1`, and sets
`engine.deploymentMode: OMENative`. OMENative's controller creates a
headless Service, two pods (`-engine-0-leader-0`,
`-engine-0-worker-0`), and one PodGroup. Pods come up; runtime
readiness and `ome.io/serving` gate them; Instance 0 is Ready only
when both are Ready.

Operator sees `kubectl get pods,services,podgroups` and recognizes
everything — no LWS CRD vocabulary to learn.

#### Story 2: Alfred migrates an engine Instance for defragmentation

Alfred observes that Instance 0 of an OMENative engine sits alone on
Node1 contributing to GPU fragmentation. Alfred writes annotation
`ome.io/migration-request-v1-<uuid>` on the InferenceService with
`{schemaVersion: "v1", component: engine, instance: 0, from_node:
node1, hint_target_nodes: [node3, node7], reason: fragmentation}`.

OMENative reads the annotation, clears it within 30s, creates a new
surge Instance with a fresh instance index and affinity excluding
Node1 while preferring Node3/Node7. The scheduler places the surge pod
on Node3. When the surge Instance is runtime-ready, OMENative sets
`ome.io/serving=True` on the surge pods, waits for ready endpoints,
then drains the old Instance by flipping `ome.io/serving=False`,
waiting for EndpointSlice convergence, and deleting the old pods.
Migration is marked Complete in `Status.MigrationHistory`.

Service never drops below the Component's minimum availability
(throughout migration, the other Instance handles traffic).

#### Story 3: Model version upgrade via in-place update

Operator bumps `ServingRuntime` image tag for a PD workload.
`InferenceService` generation increments. OMENative compares old and
new `ControllerRevision`s per-Component:

- Router: only image changed → in-place eligible. Pods get
  `ome.io/serving=False` first (traffic drains after endpoint
  convergence), image is patched, kubelet restarts the container, Pod
  returns to Ready. Model reload is zero (router has no model).
- Engine: same — in-place. 70B model weights sit in EmptyDir; they
  survive the container restart. Warmup time ≈ seconds (just the
  inference runtime re-attaching), not 10+ minutes of re-download.
- Decoder: same.

Each Component rolls independently in v1. Cross-Component coordinated
rollout is explicitly deferred until the single-Component controller
kernel is proven.

#### Story 4: Evacuate a node for maintenance

Operator drains Node5 for GPU driver update. For each InferenceService
with an Instance on Node5, the operator calls Alfred's "evacuate node"
endpoint (or directly writes migration-request annotations). OMENative
executes surge migration for OMENative-managed Instances. For simple
single-pod `RawDeployment` workloads, Alfred can continue using
eviction instead of OMENative. Operator then cordons and drains Node5
once no protected pods remain.

#### Story 5: Single-pod engine stays on RawDeployment unless migration semantics are needed

Operator runs an engine-only `InferenceService` where one pod is the
entire serving unit. The default remains `deploymentMode:
RawDeployment`, because Alfred can safely evict and reschedule that pod
without group semantics. If the operator later needs OMENative's
controller-owned drain or surge migration semantics, they opt the
Component into `deploymentMode: OMENative` explicitly.

### Risks and Mitigations

**Risk: Implementing LWS-equivalent semantics introduces bugs that LWS
has already fixed.**

*Mitigation:* Implementation is greenfield but informed by LWS's and
RBG's public source — patterns studied, not code-copied. Test plan
specifically covers known group-lifecycle edge cases (pod loss,
drain races, controller restart mid-operation). Beta graduation gates
on a fixed list of chaos-test scenarios passing.

**Risk: Divergence from sgl-project ecosystem (RBG).**

*Mitigation:* Accepted trade-off. RBG's CRD surface is a non-starter
for OME's UX. OMENative scopes narrowly to OME needs, not a general
LLM workload abstraction. Patterns (not code) are shared. If a shared
library of Go helpers emerges organically, it can be extracted later.

**Risk: Feature gap vs. LWS for niche features.**

*Mitigation:* Per-feature classification in Q-012 / [Coexistence
with LWS-Backed Modes](#coexistence-with-lws-backed-modes): subgroup
policies out of scope; exclusive placement via standard K8s
`topologySpreadConstraints`; partition rollouts adopted; mutable
`size` out of scope (document immutability). Coexistence with LWS is
long enough for users needing unsupported features.

**Risk: Migration-induced service disruption.**

*Mitigation:* Migration API enforces cluster-wide rate limits (default
3 in-flight, 10/hour). Every migration emits events and status
updates. `InferenceService.Spec.MigrationPolicy.mode: never` lets
operators opt specific workloads out. Alfred has its own rate
limiting and per-workload gates.

**Risk: Annotation-based migration-request surface is fragile.**

*Mitigation:* Annotation is idempotent via UUID. Lifecycle semantics
(30s clear, 5m retry, 1h stale) are precise. OMENative's clear is
best-effort; alfred's retry handles contention. If operator UX
demands a cleaner interface later, annotation-as-shim behind a
subresource is straightforward.

**Risk: Operators confuse OMENative errors with native K8s errors.**

*Mitigation:* Owner references from InferenceService flow through to
Pods and other emitted resources. OMENative emits its own event stream
on InferenceService
summarizing lower-level events with explicit OMENative references.
Errors clearly distinguish OMENative-issued vs. K8s-native.

**Risk: Engine/decoder coupling rule surprises users.**

*Mitigation:* Admission webhook rejects mismatched
`engine.deploymentMode` vs. `decoder.deploymentMode` with
`InvalidDeploymentModeCombination` and a clear error message. Users
see the rejection at `kubectl apply` time, not in production.

## Design Details

### Architecture Overview

OMENative is **not** a parallel controller. It plugs into the existing
`InferenceService` controller as a new deployment backend.

At a high level, implementation adds:

- a new `DeploymentModeType` value: `OMENative`
- a new backend package:
  `pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/`
- one new `case constants.OMENative` branch in
  `components/engine.go`, `decoder.go`, and `router.go`
- additional watches in `SetupWithManager()` for:
  - Pods
  - `ControllerRevision`
  - `EndpointSlice`
  - optional scheduler-plugins `PodGroup`

OMENative reuses OME's existing shared reconcilers for:

- ClusterIP and external Services
- `PodDisruptionBudget`
- RBAC
- model ConfigMap
- ingress / gateway integration
- top-level `InferenceService.Status` aggregation

OMENative does **not** use the current HPA reconciler in v1. The
current HPA code targets one `Deployment`, while OMENative manages
Instance-scoped direct pod sets.

Detailed reconcile-loop mechanics, file layout, and watch mapping live
in [controller-mechanics.md](./controller-mechanics.md), especially
sections 12 and 13. This README stays at the design-contract level and
intentionally avoids duplicating line-by-line controller flow.

### Internal Data Model

OMENative recomputes an in-memory plan every reconcile. The plan tracks:

- Components (`router`, `engine`, `decoder`)
- desired Instance count per Component
- Runner layout per Instance (`leader`, `worker`, `default`)
- rendered pod templates
- restart / update / drain policy
- active revision metadata
- migration intent

Persistent state lives on:

- `InferenceService.Status`:
  - top-level conditions
  - per-Component OMENative status
  - per-Instance status
  - migration history
- emitted Pods, Services, `PodDisruptionBudget`, and optional
  scheduler-plugins `PodGroup`
- `ControllerRevision` objects per Component
- optional controller-owned audit ConfigMap for durable migration UUID
  dedup

There is no StatefulSet state in OMENative v1.

### Pod Topology Model

OMENative maps each `(Component, Instance, Runner, ordinal)` tuple to
exactly one directly managed `Pod`. Names are derived deterministically:

```
Pod name:            <isvc>-<component>-<instance-idx>-<runner-name>-<ordinal>
Hostname:            same as pod name
Subdomain:           <isvc>-<component>-headless
```

Example for our PD workload:

```
llama-70b-router-0-default-0
llama-70b-router-1-default-0
llama-70b-engine-0-leader-0
llama-70b-engine-0-worker-0
llama-70b-engine-0-worker-1
llama-70b-engine-0-worker-2
llama-70b-engine-1-leader-0
llama-70b-engine-1-worker-0
llama-70b-engine-1-worker-1
llama-70b-engine-1-worker-2
llama-70b-decoder-0-leader-0
llama-70b-decoder-0-worker-0
```

Twelve Pods total. User sees `InferenceService` plus vanilla K8s
objects.

**Why direct Pods** (not one StatefulSet per `(Instance, Runner)`):

- v1 surge migration needs one full replacement Instance without
  relying on StatefulSet naming tricks or unsupported `MaxSurge`.
- OMENative owns Instance atomicity directly, so the full pod set is
  created, drained, and deleted together.
- Stable pod names plus explicit owner references keep debugging and GC
  straightforward.
- Headless-Service DNS still works because OMENative sets pod
  `hostname`/`subdomain` directly.

**Common labels on every Pod**:

```
ome.io/inferenceservice:   <isvc>
ome.io/component:          <router|engine|decoder>
ome.io/instance-index:     <idx>
ome.io/runner:             <leader|worker|default>
ome.io/managed-by:         OMENative
```

These labels enable OMENative's pod watcher, status aggregation, and
alfred's observation logic.

**Single-pod Instances** still use one Runner named `default`. For
simple single-pod workloads, `RawDeployment` remains the default mode;
single-pod OMENative is an explicit opt-in when Alfred needs
controller-owned drain or surge migration semantics.

### Restart Policy

A typed enum field on each Component spec:

```yaml
spec:
  engine:
    restartPolicy: RecreateInstanceOnPodRestart
```

Three values:

- `None` — a pod failure restarts only that pod. OMENative does not
  trigger cross-pod coordination.
- `RecreateInstanceOnPodRestart` — if any pod in an Instance fails,
  all pods in that Instance are drained, deleted, and recreated.
  Default for multi-pod Instances. Analog of LWS's
  `RecreateGroupOnPodRestart` but scoped to the Instance, not a whole
  LWS.
- `RecreateISVCOnPodRestart` — a pod failure cascades through the
  whole InferenceService. Rare; reserved for workloads with
  cross-Component state coupling.

**Defaults**:
- `RecreateInstanceOnPodRestart` when the Component has any Runner
  with `size > 1` (multi-pod Instance).
- `None` when all Runners have `size == 1`.

**Implementation**: the OMENative pod watch watches pods with
`ome.io/managed-by: OMENative`. On a pod failure event:

- Read the Component's `restartPolicy` from InferenceService spec.
- If `RecreateInstanceOnPodRestart`: enumerate all live pods with the
  same `ome.io/instance-index`, patch `ome.io/serving=False` on the
  still-live siblings, wait for drain, then delete and recreate the
  whole set.
- If `RecreateISVCOnPodRestart`: apply the same pattern to every
  OMENative-managed pod in the InferenceService.
- If `None`: no controller action.

**Suppression during migration**: during an in-flight migration, the
pod watcher suppresses `RecreateInstance` triggers for the target
Instance. The migrator explicitly controls pod lifecycle during its
window.

### Update Strategy (In-Place and Recreate)

Per-Component field. Default `InPlaceIfPossible`.

```yaml
spec:
  engine:
    updateStrategy:
      type: InPlaceIfPossible       # RecreatePod / InPlaceIfPossible / InPlaceOnly
      rollingUpdate:
        partition: 0                 # 0 = update all; N = canary hold-back
        maxUnavailable: 1            # concurrent Instances not-Ready during rollout
      inPlaceUpdateStrategy:
        gracePeriodSeconds: 30
        markNotReadyDuringLifecycle: true
```

**Three modes:**

| Mode | Behavior |
|------|----------|
| `RecreatePod` | Always delete pods and recreate. Preserves today's behavior. Use when in-place is untrusted. |
| `InPlaceIfPossible` | Try in-place first; fall back to recreate if the template change is not in-place-capable. **Default.** |
| `InPlaceOnly` | Try in-place; if the change is not in-place-capable, **fail the update**. Safety net. |

**In-place-capable changes** (template fields that OMENative treats
as in-place eligible):

- `spec.containers[].image`
- `spec.containers[].imagePullPolicy`
- `metadata.labels`, `metadata.annotations`

**Not in-place-capable** (forces recreate under `InPlaceIfPossible`,
rejects under `InPlaceOnly`):

- `spec.containers[].command`, `args`, `env`, `envFrom`,
  `volumeMounts`, `volumeDevices`, `ports`, `securityContext`,
  `lifecycle`.
- `spec.initContainers[]` (any change).
- `spec.volumes[]`.
- `spec.nodeSelector`, `spec.affinity`, `spec.tolerations`.
- `spec.serviceAccountName`, `spec.imagePullSecrets`.
- `spec.runtimeClassName`, `spec.schedulerName`.
- any future field that would require OMENative to change pod identity
  or regenerate storage wiring.

**Decision logic** (in `updater.go`):

```
func decideUpdate(oldRev, newRev *ControllerRevision, policy UpdateStrategy) Decision {
    if policy.Type == RecreatePod {
        return Decision{Action: Recreate}
    }
    diffs := compareTemplate(oldRev, newRev)
    inPlaceEligible := allDiffsIn(diffs, inPlaceCapableFields)
    switch policy.Type {
    case InPlaceIfPossible:
        if inPlaceEligible { return Decision{Action: InPlace} }
        return Decision{Action: Recreate}
    case InPlaceOnly:
        if inPlaceEligible { return Decision{Action: InPlace} }
        return Decision{Action: Reject, Reason: "InPlaceUpdateNotPossible"}
    }
}
```

**Execution of an in-place update on an Instance**:

1. OMENative emits K8s event `InPlaceUpdateStarted`.
2. `ome.io/serving` is already present on every OMENative pod at pod
   creation time; OMENative never mutates the readiness-gate list on a
   running Pod.
3. If `markNotReadyDuringLifecycle: true`, OMENative patches
   `ome.io/serving=False` on every pod in the Instance and waits until
   EndpointSlices no longer publish those endpoints as ready.
4. Patch the pod container image (and metadata changes if eligible).
5. Kubelet observes the image change, restarts the container in place,
   and the pod re-runs its normal readiness probe. EmptyDir-backed
   model weights survive the container restart.
6. OMENative patches `ome.io/serving=True`, waits for ready endpoints,
   then emits `InPlaceUpdateCompleted`.

If the diff is not in-place-capable, `InPlaceIfPossible` falls back to
recreate and `InPlaceOnly` rejects the update.

**Rollout pacing across Instances**: OMENative updates complete
Instances while respecting `rollingUpdate.partition` and
`rollingUpdate.maxUnavailable`.

**Migration vs update**: migration (movement between nodes) always
triggers recreate because node-affinity change is not in-place
capable. This is independent of `updateStrategy.type`.

**Internal ControllerRevision usage**: every InferenceService
generation produces per-Component `ControllerRevision`s capturing
the rendered `PodTemplateSpec`. These are compared across generations
to compute `diffs` for the in-place decision. A retention limit
(default 10 revisions per Component) caps object count.

### Coordinated Cross-Component Rollout

Coordinated engine/decoder rollout is **deferred from OMENative v1**.

This OEP keeps the concept because it remains a plausible future
extension, but the v1 controller contract is intentionally narrower:
single-Component correctness first, cross-Component skew coordination
later.

In v1:

- each OMENative-managed Component rolls independently
- Alfred migration targets one Component Instance at a time
- no `CoordinationPaused` condition or coordination-specific metrics are
  required

### Service Discovery

**Three Services per Component**, each with a distinct name:

| Service | Name | Type | Purpose |
|---------|------|------|---------|
| External | `<isvc>` | ClusterIP / LoadBalancer / via Ingress | Top-level external entry (existing `PredictorServiceName`) |
| Component routing | `<isvc>-<component>` | ClusterIP | Component-level entry (existing `RouterServiceName` / `EngineServiceName` / `DecoderServiceName`) |
| Component peer DNS | `<isvc>-<component>-headless` | Headless (`ClusterIP: None`) | Stable pod subdomain for peer DNS |

The external and component-routing Services are the **existing**
Services OME's controllers create today. Behavior unchanged; ingress
and VirtualService consumers keep working.

The **peer-DNS headless Service is new**. OMENative sets each pod's
`hostname` and `subdomain` directly, which gives each pod a stable DNS
name:

```
<pod-name>.<isvc>-<component>-headless.<ns>.svc.<cluster-domain>
```

Concrete example:
```
llama-70b-engine-0-worker-1.llama-70b-engine-headless.prod.svc.cluster.local
```

**Pod-level env vars** injected into every OMENative-managed pod:

| Variable | Example | Meaning |
|----------|---------|---------|
| `OME_INFERENCESERVICE_NAME` | `llama-70b` | parent InferenceService |
| `OME_COMPONENT` | `engine` | router / engine / decoder |
| `OME_COMPONENT_REPLICAS` | `2` | total Instances of this Component |
| `OME_INSTANCE_INDEX` | `0` | this Instance's ordinal |
| `OME_RUNNER` | `worker` | leader / worker / default |
| `OME_RUNNER_SIZE` | `3` | pod count in this Runner |
| `OME_RUNNER_INDEX` | `1` | ordinal within this Runner |
| `OME_LEADER_ADDRESS` | `llama-70b-engine-0-leader-0.llama-70b-engine-headless.prod.svc.cluster.local` | FQDN of this Instance's Leader pod |
| `OME_INSTANCE_SUBDOMAIN` | `llama-70b-engine-0` | prefix for building peer DNS |

Injected via `spec.containers[].env` at pod template rendering. Runtimes
declaring additional env via `RunnerSpec.Env` are merged.

### Readiness and Warmup

Per-Component fields:

```yaml
spec:
  engine:
    readyPolicy: AllPodReady         # AllPodReady | None
    instanceReadyTimeout: 30m        # OMENative's wait ceiling
```

**Runtime-native readiness probe is authoritative**. The inference
runtime declares a readiness probe in its PodTemplate (via
ServingRuntime). When the probe passes, the pod's `Ready` condition
is true. OMENative observes via `Pod.Status.Conditions`.

**Instance readiness aggregation**:

- `readyPolicy: AllPodReady` (default for multi-pod Instances): Instance
  is Ready ⇔ every pod in the Instance has `Ready=True`.
- `readyPolicy: None`: Instance-level readiness is not aggregated;
  pods are reported individually.

**`instanceReadyTimeout`**: OMENative's upper bound for waiting on a
newly-created Instance to become Ready (during migration, rollout, or
initial creation). Default 30 minutes (accommodates large-model
loading). On timeout, the operation (e.g., migration) is marked failed
with reason `InstanceReadyTimeout`; surge rollback where applicable.

**Controller-owned readiness gate**: OMENative appends one readiness
gate to every managed pod at pod creation time:

```text
ome.io/serving
```

OMENative flips that condition during update, restart, migration, and
scale-down to drain Service traffic before mutating the pod set.

**Custom readiness gates**: PodSpec-level `readinessGates` from the
runtime or user spec are still respected. OMENative appends its gate;
it does not replace or remove existing readiness gates.

**Relation to migration**: migration surge pods must reach `Ready`
before OMENative proceeds to terminate the old pods. Bounded by
`instanceReadyTimeout`.

### Port Allocation

For workloads using `hostNetwork: true` (common with RDMA), OMENative
provides annotation-driven port allocation:

```yaml
metadata:
  annotations:
    ome.io/port-allocations: |
      {
        "allocations": [
          {"name": "grpc",        "env": "GRPC_PORT",        "scope": "PodScoped"},
          {"name": "nccl",        "env": "NCCL_PORT",        "scope": "InstanceScoped"},
          {"name": "prom",        "env": "PROM_PORT",        "scope": "ComponentScoped"}
        ],
        "references": [
          {"env": "LEADER_PORT_REF", "from": "engine.leader.grpc"}
        ]
      }
```

**Scopes**:

- `PodScoped` — unique port per pod. OMENative allocates a distinct
  port for each pod.
- `InstanceScoped` — shared port across pods within the same Instance
  (useful for NCCL within a tensor-parallel group).
- `ComponentScoped` — shared port across all pods of a Component
  (useful for well-known service ports).

**Cross-Component references**: the `references` block pulls another
Component/Runner/port-name's allocated value into the current pod's
env. Used by routers needing the leader's gRPC port.

**Implementation**: an allocator interface with a default
random-allocator chooses unallocated ports in a configurable range
(default 30000–65000). Allocation state must persist in controller-owned
status or revision metadata; it must not depend on StatefulSet
annotations because OMENative v1 manages Pods directly.

### Migration API and Mechanics

**Request surface**: annotation on InferenceService. Alfred (or any
authorized caller) writes:

```yaml
metadata:
  annotations:
    ome.io/migration-request-v1-<uuid>: |
      {
        "schemaVersion": "v1",
        "component": "engine",
        "instance": 0,
        "reason": "fragmentation",
        "from_node": "node1",
        "hint_target_nodes": ["node3", "node7"],
        "requested_at": "2026-04-16T12:00:00Z",
        "requested_by": "alfred-controller"
      }
```

**Lifecycle**:

1. OMENative observes the annotation. Within 30s, it validates and
   accepts (clearing the annotation) or rejects.
2. On accept: entry added to `Status.MigrationHistory` with
   `phase: Pending`. The annotation is removed. The request UUID is
   recorded in the history entry for idempotency.
3. Caller (alfred) observes the status update, knows the request is
   accepted.
4. If OMENative has not cleared the annotation after 5 minutes,
   alfred may retry by writing the same UUID. OMENative dedupes via
   the UUID.
5. Annotations older than 1 hour without acknowledgment are stale;
   OMENative clears them with event `MigrationRequestStale`.

These `30s / 5m / 1h` values are the **Alfred-facing request lifecycle
contract only**. Controller-internal step deadlines and retry policy
are defined canonically in
[controller-mechanics.md](./controller-mechanics.md#54-operation-timeouts-and-backoff).

**Status.MigrationHistory**:

```yaml
status:
  migrationHistory:
    - id: <uuid>
      component: engine
      instance: 0
      replacementInstance: 2
      mode: surge
      phase: InProgress
      requestedAt: ...
      startedAt: ...
      completedAt: null
      reason: fragmentation
      requestedBy: alfred-controller
      events:
        - at: ...
          message: "surge Instance created on node3"
      outcomeReason: ""
```

Rolling window of 20 entries per InferenceService. When the window
is full and a new entry is added, the oldest entry is dropped by
default. If durable archiving is enabled, dropped entries are appended
to an OMENative-owned audit ConfigMap (one ConfigMap per
InferenceService). Metric
`omenative_migration_history_truncated_total` counts every
truncation event regardless of whether it was archived.

Version-skew rule:

- unsupported `schemaVersion` -> reject with
  `UnsupportedSchemaVersion`
- supported `schemaVersion` with unknown additive fields -> ignore
  those fields
- behavioral changes require a new schema version and annotation key
  prefix

**Mode selection (auto-picker)**:

```
if cluster_has_capacity_for_one_extra_instance:
    use Surge
else:
    reject             (Reason: InsufficientCapacity)
```

Override via `InferenceService.Spec.<component>.migrationPolicy.mode`:
`auto` (default), `surge`, `never`.

In v1, `auto` resolves to `surge` or rejection. There is no rolling
per-pod migration path.

**Surge migration mechanics**:

1. Validate cluster has capacity for one extra Instance of this
   Component.
2. Allocate a fresh surge Instance index. Create a full replacement pod
   set for that new Instance using stable names:
   `<isvc>-<component>-<new-instance>-<runner>-<ordinal>`.
   Pod template has hard anti-affinity excluding `from_node`, plus
   optional preferred affinity toward `hint_target_nodes`.
3. Wait for all surge pods to reach runtime `Ready` (bounded by
   `instanceReadyTimeout`). Respects model warmup via runtime
   readiness probe.
4. Patch `ome.io/serving=True` on the surge Instance and wait until
   EndpointSlices show it ready and `minReadySeconds` is satisfied.
5. Drain the old Instance by patching `ome.io/serving=False` on its
   pods and waiting for EndpointSlice convergence.
6. Delete the old Instance pods and old PodGroup.
7. Remove the old `InstanceStatus`, keep the surge `InstanceStatus`,
   and mark migration Completed.

V1 accepts sparse live instance indices after migration. Example:
migrating Instance `0` may leave the Component with live indices
`{1,2}`. OMENative does not attempt in-place renumbering.

**Concurrent migration prevention**:

- One migration in progress per Instance at a time. Second request
  for the same Instance while pending is rejected
  `Reason: MigrationInProgress`.
- Cluster-wide cap: `omeNativeConfig.inFlightMigrationCap` (default
  3). Excess rejected `Reason: RateLimited`.
- Per-hour cap: `omeNativeConfig.maxMigrationsPerHour` (default 10).

These controller defaults are defined in
[controller-mechanics.md](./controller-mechanics.md#54-operation-timeouts-and-backoff)
and must stay aligned there.

**Timeout and rollback**:

- Per-migration timeout = `instanceReadyTimeout + 5m` (overhead).
- On timeout:
  - Surge mode: delete the surge Instance pod set (best-effort
    rollback). Migration marked Failed.

### Strategy Migration Procedure (LWS → OMENative)

Users with running InferenceServices on the LWS-backed `MultiNode` /
`PDDisaggregated` modes may migrate to OMENative, but **zero-downtime
cutover is not universal**. Procedure:

1. User annotates InferenceService with
   `ome.io/migrate-strategy-to: OMENative` (or changes
   `deploymentMode` values — annotation is a declarative hint to the
   controller for intermediate phased migration).
2. OMENative's controller detects the annotation and classifies the
   target Component's cutover as either overlap-safe or not.
3. **Safety boundary**:
   - For stateless or explicitly overlap-safe runtimes, the controller
     may create OMENative-managed resources in parallel with the
     existing LWS-backed ones, consuming ~2× capacity briefly, and may
     widen the Component Service selector briefly so both populations
     can serve during cutover.
   - For stateful engine/decoder runtimes, OMENative must **not**
     assume that old and new populations may safely co-serve behind one
     selector. Split-brain, duplicate membership, and cache incoherence
     are real risks.
4. Therefore, zero-downtime strategy migration is supported only for
   Components whose runtime contract explicitly declares overlap-safe
   cutover. For engine/decoder Components without that proof, the safe
   v1 strategy migration path is controlled drain-to-zero of the LWS
   population, then recreate under OMENative, which may require a
   maintenance window.
5. After the old LWS path is drained and removed, or after an
   overlap-safe cutover completes, the controller removes the old
   LWS-managed resources and Services/PDB no longer needed.
6. Annotation is cleared. InferenceService's
   `spec.*.deploymentMode` is updated to `OMENative`.
7. Migration history entry is recorded.

**Requirements**:

- Cluster has ~2× the Component's capacity available during the
  migration window.
- ServingRuntime is compatible with OMENative (same pod template
  semantics work for both strategies).
- If the migration plan expects overlap-safe cutover, the runtime must
  explicitly tolerate simultaneous old+new populations behind one
  Service selector.

**Limitations**:

- If the ServingRuntime requires LWS-specific labels or features, the
  automatic procedure fails; user must manually adjust the runtime
  first.
- Single-replica InferenceServices without 2× capacity available
  must either accept a restart (managed recreate) or wait for
  capacity.

### Operational Runbooks

**Sparse instance index inspection**

When migration leaves live indices non-dense, operators should inspect
both the live set and the replacement history:

```bash
kubectl get isvc <name> -o jsonpath='{range .status.components.engine.omenative.instanceStatuses[*]}{.index}{"\t"}{.phase}{"\t"}{.runningRevision}{"\n"}{end}'
kubectl get isvc <name> -o jsonpath='{range .status.migrationHistory[*]}{.instance}{"->"}{.replacementInstance}{"\t"}{.phase}{"\n"}{end}'
```

`instanceStatuses[*].index` shows the current live Instances.
`migrationHistory[*].replacementInstance` records which live index
replaced which original index during surge migration.

**LWS → OMENative cutover abort**

There is no dedicated rollback verb for strategy migration in v1.
Operator guidance:

- Before the old LWS population has begun draining:
  - clear `ome.io/migrate-strategy-to`
  - remove any OMENative candidate resources
    (`ome.io/managed-by=OMENative`)
  - keep the existing LWS population serving
- After overlap-safe selector widening but before old LWS drain:
  - restore the selector to the old LWS-serving population only
  - clear `ome.io/migrate-strategy-to`
  - remove OMENative candidate resources
- After old LWS drain has begun:
  - rollback is not supported
  - do **not** re-widen the selector to include both populations again
  - force the cutover forward to one surviving serving population, then
    reattempt migration later if needed

### Model Availability Preconditions

OME's existing mechanism is reused unchanged:

- `pkg/constants/constants.go:801-817` defines label constructors
  `GetBaseModelLabel()` and `GetClusterBaseModelLabel()` producing
  labels like `models.ome.io/{ns}.basemodel.{name}` or
  `models.ome.io/clusterbasemodel.{name}` with value `Ready`.
- Model-agent sets these labels on nodes when the model is ready.
- OME's ISVC-controller util layer already injects
  `NodeSelector`/`NodeAffinity` on pod specs based on referenced
  models. OMENative-emitted pods inherit this injection unchanged.

**Consequence for migration**: when OMENative creates a surge pod,
the pod already has `NodeAffinity` requiring the model-ready label on
the target node. If no feasible node matches (perhaps because model
hasn't been distributed there yet), the pod stays Pending; migration
times out per `instanceReadyTimeout`; marked failed.

Alfred consumes the same labels when filtering `hint_target_nodes`
(see OEP-0008 §Execution).

No new API surface. No auto-download in v1 — operator pre-provisions
the model via BaseModel/ClusterBaseModel before the migration is
expected.

### Gang Scheduling

Multi-pod Instances need gang scheduling: all pods of the Instance
schedule together or none.

**Supported scheduler in v1**: Kubernetes `scheduler-plugins`
Coscheduling only.

For each multi-pod Instance, OMENative creates one
`scheduling.x-k8s.io/v1alpha1` `PodGroup` and stamps the corresponding
pod-group label on every pod in the Instance.

**What happens without a gang scheduler**: multi-pod Instance
creation may succeed partially — leader scheduled, some workers
Pending. LLM runtime will hang waiting for worker connections. This
is observable via `instanceReadyTimeout`; OMENative marks the
Instance failed and emits an event.

Volcano and Yunikorn are explicit deferrals.

### Webhook Interaction

OME's existing pod admission webhook at
`pkg/webhook/admission/pod/` (RDMA config injection, sidecar
containers, model-init container, annotation translation) works
**unchanged** for OMENative-emitted pods:

- Webhook is additive — only adds env/volumes/containers/labels;
  never removes pod-identity fields (hostname, subdomain) set by
  OMENative at pod creation.
- OMENative's `OME_*` env-var prefix (see [Service
  Discovery](#service-discovery)) avoids collision with
  webhook-injected env.

Integration tests cover: for a `deploymentMode: OMENative`
InferenceService, emitted pods have correct RDMA injection, sidecar
containers, model-init container, OMENative's env vars, and
controller-assigned hostname/subdomain.

### Status Propagation

OMENative updates `InferenceService.Status`:

```yaml
status:
  conditions:
    - type: Ready
      status: "True"
    - type: Progressing
      status: "False"
    - type: Degraded
      status: "False"
  observedGeneration: 42
  components:
    engine:
      omenative:
        observedGeneration: 42
        currentRevision: llama-70b-engine-rev-abc123
        updateRevision: llama-70b-engine-rev-abc123
        labelSelector: "ome.io/inferenceservice=llama-70b,ome.io/component=engine"
        replicas: 4
        readyReplicas: 4
        availableReplicas: 4
        instanceStatuses:
          - index: 0
            incarnation: 3
            phase: Ready
            runningRevision: llama-70b-engine-rev-abc123
            targetRevision: llama-70b-engine-rev-abc123
            podCount: 4
            readyPodCount: 4
            availablePodCount: 4
            scheduledPodCount: 4
            nodesOccupied: [node3, node7, node9, node12]
    decoder: { ... similar ... }
    router: { ... similar ... }
  migrationHistory:
    - id: ...
      component: engine
      instance: 0
      phase: Completed
      ...
```

The nested `components[*].omenative` field is additive and optional in
CRD terms. Older clients ignore it, and Components not using
`deploymentMode: OMENative` leave it `nil`.

After upgrade, OMENative reconstructs this status from live Pods,
Services, `ControllerRevision`, PodGroups, and EndpointSlices, so no
storage migration or feature gate is required just to add the field.

Per-runner counters are intentionally **not** persisted in
`InstanceStatus` in v1. Runner-level debugging comes from live pod
labels (`ome.io/runner`) rather than a second aggregated status layer.

### RBAC

OMENative controller permissions (additive to existing OME
ClusterRole):

```yaml
# Core K8s resources
- apiGroups: [apps]
  resources: [controllerrevisions]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [""]
  resources: [services, configmaps]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [""]
  resources: [pods]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [""]
  resources: [pods/status]
  verbs: [get, patch, update]
- apiGroups: [""]
  resources: [events]
  verbs: [create, patch]
- apiGroups: [discovery.k8s.io]
  resources: [endpointslices]
  verbs: [get, list, watch]
- apiGroups: [policy]
  resources: [poddisruptionbudgets]
  verbs: [get, list, watch, create, update, patch, delete]
- apiGroups: [scheduling.x-k8s.io]
  resources: [podgroups]
  verbs: [get, list, watch, create, update, patch, delete]

# OME API
- apiGroups: [ome.io]
  resources: [inferenceservices, inferenceservices/status]
  verbs: [get, list, watch, update, patch]
- apiGroups: [ome.io]
  resources: [servingruntimes, clusterservingruntimes, basemodels,
              clusterbasemodels, acceleratorclasses]
  verbs: [get, list, watch]
```

LWS-related RBAC remains until the deprecation window closes.

### Observability

**Prometheus metrics**:

- `omenative_reconcile_duration_seconds{isvc,component,outcome}` (histogram)
- `omenative_instance_count{isvc,component,state}` (gauge; state: Running, Pending, Failed, Migrating)
- `omenative_inplace_update_total{isvc,component,outcome}` (counter)
- `omenative_migration_requests_total{isvc,component,mode,outcome}` (counter)
- `omenative_migration_duration_seconds{isvc,component,mode,outcome}` (histogram)
- `omenative_migration_failures_total{isvc,component,reason}` (counter)
- `omenative_migration_history_truncated_total` (counter)
- `omenative_gang_scheduling_wait_seconds{isvc,component}` (histogram)
- `omenative_group_restart_total{isvc,component,reason}` (counter)
- `ome_deprecated_deploymentmode_total{namespace,isvc,mode}` (counter)

**Events** on InferenceService:

- `OMENativeReconcileSucceeded`, `OMENativeReconcileFailed`
- `InstanceCreated`, `InstanceReady`, `InstanceFailed`
- `InPlaceUpdateStarted`, `InPlaceUpdateCompleted`, `InPlaceUpdateFailed`, `InPlaceUpdateRejected`
- `MigrationRequestAccepted`, `MigrationRequestRejected`, `MigrationCompleted`, `MigrationFailed`, `MigrationRequestStale`
- `GroupRestartTriggered`
- `DeprecatedDeploymentMode` (for LWS-mode ISVCs)

**Logs**: structured JSON; every multi-step operation carries a
correlation ID (request UUID for migrations; revision hash for
updates).

## Coexistence with LWS-Backed Modes

### Component Strategy Selection Rules

OMENative is selected per-Component via `deploymentMode: OMENative`.
Other Components can use any other strategy.

**Router**: free. Typically `Serverless` (Knative), `RawDeployment`,
or `OMENative`. Decoupled from engine/decoder via Service discovery.

**Engine and Decoder**: when both are present (PD mode), they **must
share the same deploymentMode**. Enforced at admission:

```
Admission webhook:
    if spec.engine.deploymentMode != spec.decoder.deploymentMode:
        reject: "InvalidDeploymentModeCombination: engine and decoder must use the same deploymentMode"
```

Rationale: coordinated rollout, shared migration semantics, and
restart-policy coupling all require a single controller watching
both.

If only `engine` is present (single-Component serving, no PD),
`engine` picks any mode independently.

For a simple single-pod engine, the default should remain
`RawDeployment`. OMENative is the better choice only when the workload
needs Instance-scoped migration or drain semantics for Alfred.

### Deprecation Plan

Six-release deprecation window for LWS-backed paths.

| Phase | OME version | LWS-backed modes | OMENative | Communication |
|-------|-------------|------------------|-----------|---------------|
| 0 | current | Supported (default) | Does not exist | none |
| 1 | +1 minor | Supported | Alpha, feature-gated opt-in | none |
| 2 | +2 minor | Supported | Beta | Controller logs + K8s Events on ISVC |
| 3 | +3 minor | Supported | Beta (default for new multi-node ISVCs) | + Admission warnings (`kubectl apply` stderr) |
| 4 | +4 minor | Frozen (bug fixes only) | GA, recommended | + Admission warnings escalated (multi-line, removal date) |
| 5 | +5 minor | Marked for removal | GA | + Announcement |
| 6 | +6 minor | Removed; `sigs.k8s.io/lws` removed from go.mod | GA (sole multi-node path) | Admission **rejects** LWS-mode |

Warning text at phase 3+ (exact `<URL>` and `vX.Y` values are
populated when OMENative reaches GA and the migration guide is
published):
```
InferenceService field .spec.<component>.deploymentMode=MultiNode uses the
deprecated LWS-backed strategy. Migrate to deploymentMode=OMENative.
See migration guide: <URL>. This path will be removed in OME vX.Y.
```

Supplementary signals:
- Metric `ome_deprecated_deploymentmode_total` — alertable.
- K8s Event `DeprecatedDeploymentMode` on transition into the
  deprecated-mode state, not on every reconcile.
- Controller logs WARN.

## Feature Gating and Rollout

A feature gate `OMENativeWorkloadStrategy` controls strategy
registration:

- Alpha (disabled by default; opt-in via
  `--feature-gates=OMENativeWorkloadStrategy=true`).
- Beta (enabled by default; can be disabled).
- GA (always enabled; feature gate removed).

When disabled, OMENative is not registered. An InferenceService with
`deploymentMode: OMENative` should be rejected at admission with
reason `StrategyNotAvailable`.

## Test Plan

### Unit Tests

Target coverage per package:

- `planner.go`: ≥ 85%
- `reconciler.go`: ≥ 85%
- `updater.go`: ≥ 90%
- `migrator.go`: ≥ 90%
- `pod_watcher.go`: ≥ 90%
- `drain.go`: ≥ 90%
- `expectations.go`: ≥ 85%
- `podgroup.go`: ≥ 85%
- `status.go`: ≥ 85%

### Integration Tests

Deployed in kind clusters with scheduler-plugins Coscheduling for
gang scheduling. Fake GPU device plugin for resource accounting.

**Core scenarios:**

1. Single-pod RawDeployment-equivalent: InferenceService with
   `engine.minReplicas=1` and `engine.deploymentMode=OMENative`, no
   leader/worker. Verify one direct Pod with stable name, headless
   Service, and env vars.
2. Single-Component multi-node: leader + 3 workers. Verify 2
   Runner pod sets (leader=1, worker=3), DNS resolution correct, one
   PodGroup created, pods gang-scheduled.
3. Multi-Component single-Instance PD: router + engine + decoder,
   each with 1 Instance. Verify Service topology, cross-Component
   discovery.
4. Multi-Component multi-Instance PD: engine (2 Instances) + decoder
   (3 Instances) + router (2). Verify correct Pod and PodGroup count.
5. Spec rollout with image change, `InPlaceIfPossible`. Verify pods
   stay, container restarts in place, EmptyDir preserved (drop a
   sentinel file in EmptyDir before update; verify it survives).
6. Spec rollout with volume change, `InPlaceIfPossible`. Verify fall
   back to recreate.
7. Spec rollout with volume change, `InPlaceOnly`. Verify rejected
   with status condition.
8. Scale up/down: change MinReplicas. Verify new Instance pod sets
   created/deleted.
9. Group restart on pod failure: kill a worker pod with
   `restartPolicy: RecreateInstanceOnPodRestart`. Verify all pods of
   the Instance are restarted.
10. Migration (surge mode). Verify surge Instance with fresh index is
    created, Ready, old Instance drained and terminated, sparse
    indices are tolerated, and `migrationHistory` records
    `instance -> replacementInstance`.
11. EndpointSlice drain. Verify `ome.io/serving=False` is observed
    before image patch or pod deletion.
12. Migration rejection (insufficient capacity). Verify rejection
    with correct reason.
13. Migration timeout. Verify surge rollback and Failed status.
14. Concurrent migration protection. Two requests for same Instance;
    second rejected.
15. Rate limiting. 10 requests rapidly; only 3 in-flight.
16. User readiness gates preserved. Verify OMENative appends
    `ome.io/serving` without clobbering existing readiness gates.
17. Deprecation warnings at phase 3. Verify webhook emits warning
    on LWS-mode ISVC apply.
18. Strategy rejection with feature gate off. Verify OMENative ISVC is
    rejected at admission.
19. Coexistence: OMENative ISVC and LWS-mode ISVC in same namespace.
    No interference.
20. LWS → OMENative strategy migration procedure. Verify dual-write,
    drain, completion.
21. Engine/decoder deploymentMode mismatch. Verify admission
    rejection.
22. Port allocation: PodScoped, InstanceScoped, ComponentScoped.
    Verify correct env var values.
23. Unsupported schema version. Write a migration request with
    unknown `schemaVersion`; verify rejection with
    `UnsupportedSchemaVersion`.

### Chaos / Robustness Tests

- Controller restart mid-reconcile during migration.
- Node failure during migration.
- API server slow response.
- etcd slow.

### Simulation / CI Environment

- Unit and planner tests use synthetic cluster snapshots (no real
  K8s).
- Integration tests use kind with fake GPUs via a stub device plugin.
- End-to-end tests with real GPUs run on dedicated CI runners.

## Graduation Criteria

### Alpha

- All unit tests passing.
- Integration tests 1–9 passing.
- Feature gate disabled by default.
- Documentation: opt-in mechanism, known limitations, feature-gate
  flag.
- No backward-compatibility promises.

### Beta

- Integration tests 1–22 passing, including chaos.
- 2+ production users running OMENative-managed workloads ≥ 60 days
  without Sev1/Sev2 incidents attributed to OMENative.
- Dashboard published.
- Migration guide published.
- Feature gate enabled by default.
- API surface frozen except for bug fixes.

### GA

- 3 months of Beta with no regressions.
- Deprecation plan reaches phase 3 (admission warnings).
- Feature gate removed.
- Test coverage ≥ 85% across all OMENative packages.
- Post-mortem process for any OMENative-attributed production
  incidents.

## Implementation History

- 2026-01-15: OEP-0006 authored (@bcfre, approved @slin1237).
- 2026-03-27: PR #506 (Workload Strategy framework) opened.
- 2026-04-16: OEP-0007 initial draft.
- 2026-04-16: Design questions consolidated in
  `.claude/designs/design-questions.md`; 37 decisions locked.
- 2026-04-16: OEP-0007 updated to reflect locked decisions (this
  document).
- TBD: PR #506 merged.
- TBD: OMENative Alpha implementation.
- TBD: Alpha release.
- TBD: Beta release.
- TBD: GA.
- TBD: LWS removal from go.mod.

## Drawbacks

1. **Implementation cost.** ~2000+ lines of new code plus extensive
   tests and documentation. 1–2 engineer-quarters to alpha quality;
   sustained ownership forever.
2. **Reinventing LWS patterns.** LWS has years of bug-fix history
   that OMENative restarts without. Mitigated by test plan covering
   known LWS edge cases.
3. **Divergence from sgl-project ecosystem (RBG).** RBG is
   sgl-project's official multi-role workload CRD. This OEP
   deliberately diverges. Accepted trade-off for UX win.
4. **No benefit for users satisfied with LWS.** Deprecation will
   eventually force them to migrate; some may resist.
5. **Migration verb opens new failure modes.** Migration is
   privileged and disruptive. Bugs could cause outages. Blast radius
   bounded to one Instance; severity per incident can still be high.
6. **Documentation burden.** New concepts (Instance, Runner,
   migration modes, coordination) must be documented. Operators who
   know LWS must re-learn.
7. **LWS coexistence increases cognitive load during deprecation.**
   Two code paths active for ~6 releases.

## Alternatives

Four alternatives were considered. Each is documented with
trade-offs in the comparison document at
`.claude/docs/05-omenative-vs-rbg-comparison.md`. Brief summary:

- **Continue with LWS, contribute upstream.** LWS charter is
  general-purpose; LLM-specific primitives unlikely to be accepted.
  Upstream release cadence unaligned with OME's.
- **Adopt RBG via RBGStrategy.** Multiplies user-visible CRDs (4+
  sgl-project CRDs visible). Velocity coupled to RBG's roadmap.
- **Fork RBG InstanceSet code into OME.** Forked code diverges
  quickly. `InstanceSet` API designed for RBG's context, imperfect
  for OME. Effectively greenfield with legacy baggage.
- **OMENative** (chosen): greenfield design inside OME, emitting
  only K8s-native resources, with LLM-specific primitives as
  first-class features. One CRD for users (InferenceService).

## Open Questions

Remaining genuinely-open questions (P3 in `design-questions.md`).
These are research-level, not blocking design completeness:

1. **KV cache handoff protocol during migration** (Q-038). Future
   primitive for transferring in-flight KV cache between old and new
   Instance during migration. Requires runtime cooperation. Research.

2. **RDMA fabric topology data source** (Q-039). How does OMENative
   / alfred know which nodes have full RDMA mesh connectivity?
   Sources: gpu-feature-discovery labels, operator config, runtime
   discovery. No clear standard yet.

3. **GPU topology within a node (NVLink, PCIe)** (Q-040). Selecting
   the right GPUs for an 8-GPU workload inside a node. Deferred; v1
   treats intra-node GPUs as fungible.

4. **DRA (Dynamic Resource Allocation) integration** (Q-041).
   Migrate from `Resources.Limits["nvidia.com/gpu"]` to
   `ResourceClaims`. Revisit when DRA matures.

5. **User-facing rollback mechanism** (Q-009). Internal
   `ControllerRevision` is emitted; a rollback verb
   (`ome.io/rollback-to-revision: <N>` or subresource) is future
   work.

6. **Component dependencies** (Q-007). Explicit `dependencies`
   field on Component spec for cases requiring orderly restart
   cascades (e.g., Mooncake-style KV master). Not required in v1
   because router uses Service discovery; add when a concrete need
   surfaces.

7. **Gang scheduling fallback** (Q-005). OMENative-managed pod
   scheduling gates for clusters without a gang-aware scheduler.
   Substantial work; deferred; separate OEP if demand emerges.

8. **Final names** (Q-047 OMENative; Q-048 Alfred). Candidates for
   consideration before GA: `Native`, `NativeGroup`, `OMECore`,
   `Bundle`.

9. **Resource resize in place (K8s 1.33+)** (part of Q-002). When
   `resizePolicy: RestartNotRequired` is set and the cluster is ≥
   1.33, resource requests/limits could be classified as in-place
   capable. Follow-up addendum.

10. **Historical migration learning** (Q-042 — alfred-side, but
    OMENative could consume feedback). Adjust migration timeouts and
    retry policy based on past outcomes. v2+ feature.
