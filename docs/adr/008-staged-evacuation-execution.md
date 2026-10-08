# ADR-008: Keep verifier-v1 evacuation actions disabled

Status: Blocked

## Context

`EvacuationStep` in the current plan schema names one logical replica, its exact
source node, and its exact destination node. The independent C++ verifier models
one surge replica on that destination, checks the intermediate state, removes
the named source replica, and then requires the final workload/replica/node
placements to match exactly.

The durable executor serializes only those source/destination identifiers and
currently supports `SET_REPLICA_COUNT`. A Kubernetes Deployment cannot bind a
new replica to an exact node or select an exact replica for scale-down. Its
scheduler chooses replacement placement. Evicting the source while the
Deployment remains at the surged replica count can cause its ReplicaSet to
create another Pod; scaling back down can then remove an arbitrary Pod through
the controller, outside the Eviction API and PDB admission. The existing
Deployment topology-spread constraint is not an exact-node placement rule.

The existing eviction helper does use the Eviction API and source UID/resource
version preconditions, but is not a journaled reconciler action. A Pod fence
patch can invalidate an eviction request constructed with an older resource
version on the normal healthy-Pod path. It is not a universal fence: Kubernetes
v1.37.0's Eviction handler can replace the supplied resource-version
precondition with the freshly read Pod version for an unready Pod admitted by
its unhealthy-Pod PDB policy ([upstream handler](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/storage/eviction.go#L216-L274)).
UID still protects against deleting a different Pod recreated under the same
name, but it does not prevent a stale controller from evicting the same source
Pod. The current reconciler fences Deployments, not source Pods; the lease
check and eviction request are separate operations, so its takeover path does
not prove this handoff safe.

## Decision

Keep `MOVE_REPLICA` and `DEACTIVATE_NODE` rejected at approval/execution. Do not
wire the existing eviction helper into the current action representation, and
do not claim exact movement, durable PDB-aware eviction, or node-drain support.
This preserves the current fail-closed behavior; no milestone tag is warranted.

## Minimum contract change before enabling

Add a new, explicitly versioned canonical evacuation protocol and verifier
proof. Preserve the current exact-move meaning for existing plans. The new
protocol must choose one enforceable promise:

* For exact placement, use a workload lifecycle/placement mechanism that can
  bind the replacement and retirement to the verified replica identity and
  node, while honoring disruption policy; or
* For source-node evacuation, encode that weaker promise explicitly and have
  the independent verifier prove every scheduler/controller outcome allowed
  by the approved destination set, including surge headroom, topology,
  availability, resource and recovery reserves, and controller-selected Pod
  retirement. Do not treat one successful simulated placement as proof of all
  possible runtime placements.

The durable action contract must bind the proof digest, Deployment and owning
ReplicaSet identities, source Pod name and UID, baseline Pod inventory,
approved placement set, surge limit, and expected final conditions. It must
journal each stage before external mutation, bind the source Pod's UID and
resource version to the current durable fence receipt, reconcile ambiguous
Eviction responses by source UID and observed owner state, wait for replacement
readiness, and fail closed unless the mutation admission path rejects stale
fencing tokens for every supported Eviction branch. A database lease, Pod
pre-read, or resource-version receipt alone is insufficient for that guarantee.
Any replacement/retirement path that bypasses PDB admission is unsupported.

## Consequences

Replica scaling remains the only supported Kubernetes mutation. The current
plan schema, verifier proof, and Deployment adapter are unchanged. Staged
replacement, durable PDB eviction, and node-drain orchestration remain blocked
until the canonical contract and target-side fencing semantics can be proven
and tested together.

## 2026-10-08 execution-contract audit

The repository still implements only schema-v1 exact placements. No
constraint-based evacuation schema or independent verifier was added in this
audit: accepting such a plan before its Kubernetes effects can be fenced and
reconciled would create another verified-but-unexecutable plan type. The
planning admission now rejects candidate plans containing `MOVE_REPLICA`,
`DEACTIVATE_NODE`, `RESIZE_WORKLOAD`, or a scale action outside the configured
target's workload set. The mathematical verifier result remains separately
recorded; an exact plan can have `verification_accepted=true` and
`admission_accepted=false`, with lifecycle state `REJECTED`. Approval also
rejects unsupported/tampered action intents in every API mode, before any
execution journal is created. The existing exact-plan verifier behavior is
preserved.

Sol High's contract review found that a future constraint-based operation
needs an explicit promise, independently enumerated permitted scheduler
outcomes, baseline Pod and controller identity, termination-overlap capacity,
availability and topology checks, and recovery reserve. Sol Max confirmed one
possible narrower contract: permanently increase a workload from N to N+1 and
retire one named source Pod UID, without claiming node drain or exact
destination placement. It still needs enforceable destination bounds,
controller and owner identity checks, N+2 overlap accounting, and safe
cordon/uncordon semantics if cordoning is used.

That narrower protocol is not executable safely with the current target API.
In Kubernetes v1.37.0, the policy/v1 Eviction handler sets
`updateDeletionOptions` for an unready Pod admitted under `AlwaysAllow`, or
under `IfHealthyBudget` when `CurrentHealthy >= DesiredHealthy > 0`; it then
unconditionally assigns the freshly read Pod resourceVersion into the delete
preconditions, replacing a caller-supplied resourceVersion
([handler branch and assignment](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/storage/eviction.go#L220-L274),
[assignment helper](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/storage/eviction.go#L351-L356)). The special 429 handling for a conflict
on that branch applies only when the request originally omitted a
resourceVersion ([lines 278-284](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/storage/eviction.go#L278-L284)); it does not preserve a caller-supplied fence. A lease takeover between
the controller check and API-server mutation can therefore leave an old
request able to evict the same Pod UID.
The UID precondition prevents acting on a different Pod with the same name;
it does not fence a stale request against the same Pod. A DB token, pre-read,
Pod annotation, or request resourceVersion is insufficient across that race.
No target-side mechanism in this repository enforces the fencing token at the
final eviction mutation for every supported branch. Therefore no replacement,
eviction, restart-recovery, or live Kubernetes evacuation test was attempted.

The current observation helpers also do not bind a newly created replacement
to the approved Deployment and ReplicaSet UIDs or baseline inventory: a Ready
Pod at a destination is insufficient proof of replacement identity. The
journal has no evacuation-stage/proof record, and current fencing receipts do
not make a Pod Eviction atomic with PostgreSQL takeover. These gaps preclude
claiming durable restart recovery for evacuation.

The dedicated safety-lab cluster and isolated `fleetright_test` database were
healthy during preflight. This does not qualify as live evacuation evidence.
The action remains disabled pending a separately versioned contract, an
independent proof for its complete allowed-outcome set, and a target-side
fencing mechanism whose final mutation semantics are demonstrated against the
pinned Kubernetes release.
