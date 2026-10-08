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

Only monotonic replica growth on a newly created, explicitly marked
FleetRightWorkload is exposed through the Kubernetes API target. Direct
Deployment execution is rejected by application configuration and the executor
Role has no Deployment, Pod, Eviction, PDB, or Node mutation permission. The
executor atomically publishes its durable plan/action identity, SQL fencing
generation, expected replica count, target count, and new replica high-water
mark using a resourceVersion-guarded write to the FleetRightWorkload object.
The existing FleetRightWorkload controller remains the only Pod creator.

This is a narrow monotonic scale-up guarantee, not a general stale-request
fence: an old Pod CREATE can arrive after takeover. It remains compatible only
because every admitted target count is nondecreasing, the object UID/template/
placement are fixed, and each ordinal has one deterministic Pod name and owner
UID. A newer action waits until the previous action is `APPLIED`. Scale-down,
terminal Pod replacement, `MOVE_REPLICA`, `DEACTIVATE_NODE`, Eviction, and
node-drain orchestration remain disabled. No Eviction/Deployment claim can be
derived from this protocol or its resourceVersion CAS.

`_ensure_pdb` stores the workload generation in a managed-PDB annotation and
refuses to patch from a lower-generation snapshot when that marker is present.
It fails closed with `PDBGenerationUnknown` for an existing PDB without the
marker: a possibly stale workload snapshot cannot safely establish the
high-water generation. A trusted migration procedure is not implemented.
Older controller binaries ignore the marker and can still patch an existing PDB
from a fresh PDB resourceVersion using stale workload data. The Deployment is
configured with `Recreate`, but a mixed-version rollout with an already running
older process has not been qualified. For a marked monotonic workload, stale
Pod CREATEs remain possible but are bounded by the prior replica high-water
mark and deterministic immutable slot identity; this is not a general Pod
request fence. Legacy objects cannot be promoted and direct API execution is
disabled for the Deployment path. Mixed-version PDB behavior remains a blocker
for any future staged retirement or drain.

The FleetRightWorkload controller fails closed when live Pod count exceeds the
desired count and reports `ScaleDownRequiresStagedExecution`. It no longer has
a Pod DELETE path, including for terminal Pods; terminal logical identities
require a future staged protocol. The direct Deployment `SET_REPLICA_COUNT`
adapter remains in the code for legacy non-Kubernetes adapters/tests, but
`create_app` rejects it as a Kubernetes execution target and its executor Role
does not grant Deployment patch permission.

## Checkpoint A monotonic scale-up protocol (2026-10-08)

The supported object must be created with
`spec.executionProtocol: monotonic-scale-up-v1`. CRD CEL makes the protocol,
workload ID, Pod template, placement, availability floor, disruption settings,
and surge budget immutable; it rejects scale-down, unbound replica growth, and
in-place promotion of an object without the marker. The API Role can get and
patch only the named `cpu-api` object in `fleetright-v2`. The controller Role
can create Pods and manage the matching PDB, but cannot patch workload spec,
delete Pods, submit Evictions, or mutate Nodes.

The execution generation is stored with plan ID, operation ID, expected replica
count, and target replica count in `.spec.executionRequest`. One Kubernetes
Merge Patch guarded by the object's `metadata.resourceVersion` updates that
record and `.spec.replicas`; concurrent writers using the same prior object
version cannot both succeed. The controller validates current workload UID,
deterministic logical Pod identity, owner UID, immutable template fingerprint,
allowed Node name/UID, observed readiness, and availability floor before it
creates or adopts any Pod. It records `RECONCILING` before Pod creation and
`APPLIED` after all desired ordinals are present and verified. New operation
identity is blocked until prior status is `APPLIED`, the object generation is
observed with `Reconciled=True`, and the current availability floor is met.

Safety property: under this protocol and while the same workload object UID is
retained, any delayed Pod CREATE can only create an ordinal within a previously
authorized replica high-water mark. Creates for one ordinal use the same name;
Kubernetes admits at most one object at that name, and an existing object is
adopted only after owner, logical ID, template, and placement checks. This is
order-safe monotonic execution, not atomic fencing of Pod POST requests and not
a guarantee that only one controller process is live. It does not cover
workload deletion/recreation by a privileged administrator. The executor's
resourceVersion CAS serializes authorization writes only. No lease, Pod UID,
PDB observation, or admission-webhook check is claimed to atomically fence a
separate Eviction request.

The retained live v1.37.0 evidence below demonstrates why Eviction stays
disabled: an unready same-UID Pod under `IfHealthyBudget` was accepted after its
resourceVersion changed even when the PDB reported zero disruptions allowed.
PDB enforcement for any future retirement must use the Eviction API, and this
repository grants no Eviction permission to either executor or workload
controller.

## Live Kubernetes v1.37.0 confirmation (2026-10-08)

On the dedicated `kind-fleetright-safety-lab` server, an allowed healthy-Pod
Eviction with a stale Pod resourceVersion was rejected while its separate PDB
reported `currentHealthy=2`, `desiredHealthy=1`, and `disruptionsAllowed=1`.
Under a separate PDB using `IfHealthyBudget`, a stale Eviction request for an
unready Pod was accepted with response code 201 after that same Pod UID's
resourceVersion had changed; Ready was False, and the PDB reported
`currentHealthy=1`, `desiredHealthy=1`, `disruptionsAllowed=0` immediately before
the request. The exact source UID became absent. The temporary test namespaces
were removed and confirmed absent. See [the live execution-authority evidence](../evidence/execution-authority-race-2026-10-08.txt).

This confirms that Pod UID plus caller resourceVersion is not a universal stale
controller fence for Eviction. It does not enable the helper or V2 execution.

## 2026-10-08 preimplementation audit (historical)

At the time of this audit, the repository implemented only schema-v1 exact
placements. No constraint-based evacuation schema or independent verifier was
added in that audit. The
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
For the permitted unready-Pod branches, the [v1.37.0 handler](https://github.com/kubernetes/kubernetes/blob/v1.37.0/pkg/registry/core/pod/storage/eviction.go)
returns before `checkAndDecrement`, so the accepted request is not recorded in
the PDB's `disruptedPods` status map. A takeover cannot use that map to prove
that no older same-UID eviction remains in flight.
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
healthy during preflight. This did not qualify as live evacuation evidence.
The action remained disabled pending a separately versioned contract, an
independent proof for its complete allowed-outcome set, and a target-side
fencing mechanism whose final mutation semantics are demonstrated against the
pinned Kubernetes release.

## 2026-10-08 V2 contract and verifier foundation

Status remains **Blocked for execution**. Schema `fleetright.v2` now defines a
separate `EvacuationSnapshot`, `EvacuationPlan`, and `VerificationResult`; the
v1 exact-placement messages and behavior are unchanged.

V2 currently supports one source Pod UID per plan. It records the source
workload, Deployment and active ReplicaSet identities, Pod name/UID, node UID,
request shape (mCPU, MiB, Mbps), profile binding, PDB budget, availability
floor, readiness durations (milliseconds), topology, failure tolerance,
the explicit N-1 failure-domain label key, intermediate replica counts, action
dependencies, postconditions, and
fail-closed behavior. The placement promise is a constraint set of two to
eight eligible nodes. It does not name an exact destination. The verifier
enumerates every ordered first/second destination pair allowed by the node set
and hard pod anti-affinity, including N+2 termination overlap. It checks
capacity, topology, availability, profile-derived SLO replicas, and N-1
recovery reserve at each modeled stage. Profile Wilson bounds are recomputed
from the recorded trial counts, and recovery destinations are checked in every
permitted insertion order. Plans needing more destinations,
more than one N-1 workload, an unsupported field, or any unsafe outcome return
`NO FEASIBLE PLAN` or fail verification.

The six ordered stages apply destination constraints, request one permanent
extra replica, wait for stable readiness, evict the named source UID through
the observed PDB, wait for source absence and the final N+1 state, then
deactivate the source node after the modeled workload placements are gone.
Runtime postcondition observation and owner/fence rechecks are required before
each dependent destructive action. Any stage failure keeps the surge and
blocks later actions. This is a proof contract, not an implementation of those
mutations.

The independent C++ verifier recomputes resource, topology, profile, stage,
and recovery checks from the sealed snapshot and serialized plan. It reports
mathematical acceptance separately and always sets
`execution_admitted=false`. Python rejects V2 for execution with
`UNSUPPORTED_EXECUTION_PROTOCOL_V2` and creates no `MOVE_REPLICA` or
`DEACTIVATE_NODE` intents. The existing target adapter still cannot enforce a
bounded destination set and anti-affinity, bind eviction atomically to the
durable fencing token, recheck Deployment/ReplicaSet ownership, or resume the
staged protocol after restart. No live drain or eviction was attempted.
