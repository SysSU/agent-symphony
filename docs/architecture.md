# MVP Architecture

**Status:** Current implementation

**Last updated:** 2026-09-15

**Scope:** one repository per orchestrator instance, multiple independent instances per host, macOS, Linux, and WSL2

## Decision summary

Agent Symphony is one long-running Go process with a CLI mode. GitHub Issues, pull requests, reviews, checks, and repository rules are the durable workflow record. The process keeps only scheduling state in memory and bounded, reconstructible execution metadata on disk; it does not have a task database or a second workflow engine.

PR governance and durable handoff state are integrated with bounded daemon scheduling, authoritative restart reconstruction, exact runtime monitoring, scoped handoff delivery, and evidenced outcome completion.

The design follows the useful boundaries in the [OpenAI Symphony specification](https://github.com/openai/symphony/blob/main/SPEC.md): a single scheduling authority, a tracker adapter, deterministic workspaces, an agent runner, and an operator status surface. It deliberately differs in three places required by this product: GitHub owns the whole delivery lifecycle, portfolio policy is coordinator code rather than agent prompt logic, and workers have no direct GitHub or runtime-state authority. The upstream Elixir implementation is a prototype; its in-memory blocked state and runtime dependency make it a reference, not the release base.

### Stack and release

- **Go 1.26, pinned in `go.mod` and built with the latest 1.26 security patch.** Goroutines, `net/http`, `os/exec`, and `encoding/json` cover the daemon, process supervision, GitHub CLI transport, local CLI, and dashboard server. The dashboard terminal uses the small `coder/websocket` and `creack/pty` packages for the protocols the standard library does not provide.
- **One `agent-symphony` executable.** The same binary provides the operator CLI, daemon, dashboard, and internal `agent-host` boundary. `install-host` remains only for diagnosing legacy installations; `serve` never selects a root or sudo execution path. `serve` owns the periodic loop, its private local control socket, and its loopback-by-default dashboard. `control` invokes guarded owner actions without competing for the lifetime lock. `status`, `list`, and `inspect` read the revision-tagged owner projection, while `--attempts` remains an offline diagnostic. Released binaries embed the dashboard and require no Node.js runtime, database, separate browser service, or container.
- **Web-only graphical interface.** `serve` hosts the embedded dashboard for a browser. There is no desktop application, desktop-specific runtime, or desktop-client configuration.
- **GitHub Releases** publish signed-tag build artifacts and SHA-256 checksums for `darwin/{arm64,amd64}` and `linux/{arm64,amd64}`. WSL2 uses the Linux artifact. Release CI runs the repository lint gate and unit tests, builds with `CGO_ENABLED=0`, smoke-tests each supported OS, and verifies a downloaded artifact against its checksum.
- **Reproducible packaging.** `scripts/release.sh` invokes the repository's Go-stdlib packer with fixed timestamps, sorted checksum output, stripped host paths/build IDs, and `CGO_ENABLED=0`. A second build must be byte-identical before release. Local cross-compilation proves buildability and runtime independence, not execution on another OS; CI records native macOS/Linux and WSL2 runtime evidence.
- **External runtime prerequisites:** Git, tmux, authenticated GitHub CLI, a supported Codex CLI, and repository access. Authentication normally comes from `gh auth login`; `GH_TOKEN` or `GITHUB_TOKEN` is an optional daemon-only non-interactive alternative. No administrator provisioning is part of normal setup or runtime. Before admitting work, `doctor` verifies the tools, GitHub identity and repository access, and the rootless Codex confinement capability.

## System boundaries and ownership

There is one runtime-state owner goroutine per repository daemon. It is the only component that commits the bounded local ledger. Dashboard controls, CLI controls, reconciliation, process supervision, and recovery submit concrete commands to that owner; none writes runtime state directly. Independent repository daemons may share a host but not state paths, and the daemon lifetime lock prevents a second owner for one path.

Slow work never runs in the owner goroutine. Reconciliation captures an immutable owner snapshot, performs GitHub, Git, tmux, filesystem, and subprocess work outside the owner, then returns a result carrying the source epoch, revision, cycle, issue generation, and attempt generation. The owner applies it only while those identities and its concrete preconditions still match. A newer cycle wins over an older result, and an identical observation is a no-op. Dismiss, Abandon, Remove, Stop, Retry, and other operator commands therefore remain responsive while external collection is blocked.

Destructive commands first commit a generation change and tombstone. That commit invalidates pending work before cleanup starts. A pending tombstone retains the exact manifest and cleanup effect needed after restart; a completed tombstone masks later external observations. Reconciliation cannot recreate an attempt whose generation changed or whose tombstone is current.

An attempt number is also a durable local reservation once the owner has recorded or tombstoned it. If GitHub still proposes an already reserved number after a pre-bind destructive action, the planner selects the first greater number that has never been owned locally. Restart does not erase this reservation.

| Boundary | Owns | Must not own |
| --- | --- | --- |
| GitHub integration | `gh api` reads/writes, authenticated user discovery, normalized issue/PR/check/review models, authorization, and rate-limit handling | Scheduling decisions or agent processes |
| Runtime-state owner | Ordered local ledger mutations, revisions, generations, tombstones, durable effect intents, and operator receipts | Network, Git, tmux, filesystem cleanup, subprocess work, or policy discovery |
| Coordinator/scheduler | Immutable snapshot collection, eligibility, priority, dependencies, capacity, effect planning, and slow effect execution | Direct runtime-ledger writes or credentials stored in repository files |
| Runtime | Worktree and branch lifecycle, tmux session, agent subprocess, timeouts/signals, captured logs, resume feasibility | Issue policy, PR creation, push credentials, or merge decisions |
| Local metadata | One bounded runtime ledger, immutable completion markers, daemon lock, and revision-tagged status projection; enough to resume or verify exact work | A second mutation authority, credentials, or raw issue/review bodies |
| CLI | Configuration, diagnostics, local control, human and JSON projections | An alternate mutation path around issue, review, or merge policy |
| Dashboard | Loopback-by-default repository-bound status projection, read-only presentation of explicitly configured peer deployments, exact-session terminal attachment, confirmed cleanup, and narrowly constrained attempt recovery | Peer mutation or terminal proxying, arbitrary GitHub mutation, arbitrary commands/resources, TLS termination, or shared multi-repository state |
| Implementation/review worker | Edit only its unique disposable workspace, run validation, and return a bounded result or generation-bound status request | GitHub/network access, runtime state, control sockets, coordinator or sibling files, publication, or merge |

An optional repository orchestrator agent is a long-lived advisory operator console, not another scheduler or implementation worker. Its deterministic tmux session is `as-o-<repository-id>` and runs as the coordinator user without sudo. A projection change starts or coalesces `commands.orchestrator_audit` as a separate one-shot process; unchanged nonterminal work also starts one no more than once every five minutes. The audit gets the current projection, previous audit time and bounded completed report, any bounded reconciliation diagnostic, and instructions for tracing the expected workflow transition. A workflow or GitHub transition, commit change, meaningful tmux output change, or direct owner reply is progress. A stall is actionable only after two consecutive observations at least one heartbeat interval apart show none of those signals. Its prompt budgets eight live checks, 20 seconds per command, and three minutes for investigation; the host enforces a four-minute child-process deadline and bounded output through the same fixed orchestrator boundary. The audit never writes into or wakes the primary conversation. Its narrowly scoped status request remains separate from worker authority and must still pass coordinator validation.

Go persists the last audit start time in `orchestrator-agent.json` but does not trust the model to classify or authorize recovery. The one-shot agent labels evidence as verified, inferred, or unknown and writes one bounded result; the default Codex command writes only its final message to a transient validated artifact so progress logs cannot displace or truncate the diagnosis. The coordinator removes that artifact after atomically replacing `orchestrator-heartbeat-report.json`. Audit output is diagnostic context only. After the audit completes, fails, or times out, a changed coordinator-owned `blocked`, `failed`, `conflicting`, `orphaned`, or explicitly needs-attention projection may create one exact `orchestrator-attention-handoff.json` and one fixed prompt in the primary pane. The same applies to an authorized active or review-ready attempt with no pull request whose implementation is complete but remains in validation or publication after reconciliation. The handoff binds the repository, issue, attempt, state, full projection digest, and exact target digest. Unchanged target digests do not wake the primary again.

The attention handoff has a twelve-minute deadline and a durable state in `orchestrator-agent.json`; the mode-`0440` workspace file exposes only the exact current handoff, while the state file retains the latest 64 completed outcomes. A restart never repeats an acknowledged wake. If a crash leaves wake delivery ambiguous, the coordinator records human attention instead of risking duplicate input. Confirmed human instructions retain precedence over the automated prompt. A fresh coordinator projection, not proposal dispatch or command success, decides whether the target recovered. A fixed action that returns successfully while the target still needs attention becomes a durable human-attention result. A materially changed target may create one later handoff; an unchanged target cannot loop.

A daemon restart adopts an exact live primary pane and marks an unfinished local audit report failed. `orchestrator-context.md` remains reconstructible mode-`0600` context. A missing or dead primary pane starts a fresh generation after bounded backoff. `clear` uses role rules only, while `rebuild` adds the latest sanitized projection. Neither operation changes GitHub facts.

The trusted full-access Codex orchestrator can query the authenticated GitHub CLI exposed through its coordinator-user environment and inspect that user's tmux server. Implementation/review workers cannot. Both orchestrator prompts permit only read-only inspection and require verified, inferred, or unknown labels for material operational claims. The primary orchestrator instructions also require the complete diagnostic and retry loop on the first request to investigate a stuck attempt. Proposals pass through the fixed `agent-host orchestrator-proposal` adapter, which validates one strict bounded object and writes it to a pre-created, size-bounded proposal artifact under an exclusive file lock. Its `submitted` response proves that durable write, not coordinator acceptance. The separate fixed `orchestrator-proposal-status` adapter compares the same exact object with a coordinator-authored binding observation and cannot mutate coordinator state.

A `check_in_attempt` proposal carries the exact attention handoff ID. The coordinator accepts it only for the exact active or review-ready attempt marked needs-attention with one verified live implementation owner, then sends a fixed status request through the existing worker tmux boundary. The request tells the owner to report progress and continue only its existing work; no arbitrary instructions pass through this automatic path. A `retry_transition` proposal carries a bounded unique request ID. The coordinator starts one ten-minute guarded reconciliation only when a fresh read-only projection identifies the exact unblocked active attempt, its implementation session is completed, and its current phase is validation or publication.

An automatic `recover_attempt` proposal additionally carries the exact 64-hex-character handoff ID. The coordinator re-reads the projection, then calls the same recovery guard used by the dashboard: only the latest retryable failed attempt or an exact runtime-liveness mismatch with matching local manifest identity and no pull request can record the fixed retry control. `human_attention` carries the same identity plus a bounded diagnostic and makes no workflow mutation. Conflicting, orphaned, unsafe, stale, ambiguous, non-retryable, review-blocked, or check-blocked targets cannot pass these controls. Proposal status moves from `pending` to `running`, then `succeeded`, `failed`, or `refused`; a separate fresh projection must still prove recovery.

Repository configuration names exactly one managed implementation role and one managed review role. The deterministic implementation worker owns implementation and follow-up turns for the attempt. When policy requires independent review, the coordinator launches a separate worker generation against an owner-created snapshot and validation evidence; its permission profile cannot access the implementation workspace, and its findings return to the implementation lifecycle for resolution. The two fixed responsibilities satisfy role selection without a general role or plugin framework.

## GitHub is authoritative

### Repository contract

Git-ignored, machine-local `.agent-symphony.yaml` contains its schema version, repository identity, labels, explicit dependency syntax, completion policy, concurrency, reconciliation interval, local/offline worktree path, documentation paths, implementation and review commands, optional primary and one-shot orchestrator commands, environment allowlist, and status preferences. Secrets and mutable status are forbidden. Schema version `1` rejects unknown keys and unsafe paths.

An eligible issue is open, satisfies the [implementation issue contract](github-controls.md#implementation-issue-contract), has the configured ready label, has exactly one P1-P3 label, has the optional issue-filter label when configured, has no conflicting completion label, has no unresolved explicit dependency, and is not represented by an active or completed attempt. The configured dependency section is required; its issue references are enforced, while `None` declares no dependencies. Controls require current `maintain` or `admin` permission, including when the actor is the authenticated coordinator user.

GitHub stores durable execution facts using machine-readable HTML markers in coordinator-authored issue comments and PR bodies:

```text
<!-- agent-symphony:active-attempt:v1
{"version":1,"issue":8,"attempt":2,"branch":"agent-symphony/owner-repo-<hash>/8-2","base_sha":"<approved-base-sha>"}
-->

<!-- agent-symphony:attempt:v1
{"version":1,"issue":8,"attempt":2,"branch":"agent-symphony/owner-repo-<hash>/8-2","head":"<commit-sha>","pr":31,"outcome":"review"}
-->
```

The marker schemas are strict and size-bounded and are parsed only from the authenticated coordinator user returned by `gh api /user`. Dispatch first commits a typed Prepare intent and preparing manifest to the owner ledger. Prepare creates the isolated repository, checks out the deterministic branch at the approved base, and verifies the resulting worktree. Bind then persists the active GitHub marker for that base and branch. Only after the owner records that verified Bind result may Start launch the process in the prepared worktree. A matching final PR marker or terminal marker later supersedes the active marker. Human-readable text accompanies each marker. GitHub proposes the integer after its highest valid marker; the owner advances past any attempt numbers already reserved locally. Branch `agent-symphony/<repo-id>/<issue>-<attempt>`, worktree `<root>/<repo-id>-<issue>-<attempt>`, and tmux session `as-<repo-id>-<issue>-<attempt>` are deterministic. A PR contains `Closes #N` and the final attempt marker. These identifiers make discovery possible without local files.

Issue/PR state, labels, comments, reviews, check runs, branch heads, and repository rules always beat local metadata. A contradiction blocks mutation, emits diagnostics, and requests reconciliation. Local files may never make completed work eligible again.

Worker-requested status is an overlay on that authoritative lifecycle. A private generation/profile-bound mailbox asks the owner to apply `needs-attention` or `clear`; it does not mutate GitHub itself. The newest valid, unedited coordinator comment must agree with the issue's fixed `needs-attention` label. A missing reason or partial comment/label update remains attention rather than reporting success. Needs-attention blocks dispatch and moves the current card to the dashboard attention lane while retaining the underlying state.

### Bounded local metadata and reconstruction

Production commands require an explicit `--runtime-state` path. Give each repository daemon a distinct absolute state root. The examples in this repository use paths below `~/.local/state/agent-symphony`, but the program does not select that path implicitly. A root contains:

```text
deployment.json              # mode 0600 immutable versioned repository binding
daemon.lock                  # single-instance advisory lock
control.sock                 # mode 0600 private daemon control socket
runtime-state.json           # mode 0600 authoritative bounded local ledger
status.json                  # mode 0600 revision-tagged derived projection
runtime-effects/*.done       # immutable exact runtime-effect completion proofs
reconciliation-effects/*.done # immutable exact reconciliation-effect completion proofs
seals/                       # owner-private immutable worker exports
worker-codex-home/           # isolated auth-only Codex client home
orchestrator-agent.json      # mode 0600 when the advisory agent is configured
orchestrator-context.md      # mode 0600 bounded advisory context
snapshots/.../orchestrator-attention-handoff.json # mode 0440 current exact attention handoff
worktrees/                   # unique disposable generation workspaces
snapshots/                   # review snapshots and managed orchestrator workspaces
attempts/<repo-id>/<issue>-<n>/agent.log
```

Legacy per-attempt `manifest.json` files are migration input only. The v2 ledger contains the authoritative manifest.

`deployment.json` version 2 is a one-way deployment fence. `serve` authenticates first, acquires `daemon.lock`, installs and syncs the fence, then installs or loads `runtime-state.json` before admitting dashboard, control, proposal, or reconciliation work. A pre-v2 writer rejects the fence. If the process stops after the fence but before the first ledger commit, the next v2 process completes the migration while holding the same lock. An existing valid v2 ledger is never re-imported from legacy files.

The owner makes a candidate state visible only after its private persistence worker has written and synced the exact ledger transition. A persistence failure leaves the previously committed revision visible. The ledger contains bounded normalized observations and proposals, manifests, per-issue and per-attempt generations, destructive tombstones, PR recovery state, operator receipts, typed effect intents, and a count of stale reconciliation results discarded by the owner. It does not contain raw issue bodies, credentials, callbacks, or process handles. Legacy `dashboard-state.json`, `removal-state.json`, and `control-receipts.json` are read only during the one-time migration and are not production write targets.

Each attempt manifest remains part of the ledger and contains bounded attempt identity, deterministic resource paths and sessions, implementation/review state, diagnostics, timestamps, and log path. `status.json` is rebuilt from an owner snapshot and carries the source ledger revision. It has no independent mutation authority. A failed reconciliation records a cycle-bound diagnostic through the owner; an older overlapping cycle cannot overwrite a newer result, and the next successful fresh cycle clears the error.

`serve` exposes the embedded dashboard and bounded status projection on a configurable loopback address by default. A non-loopback address requires the explicit unsafe-network flag and a password that protects every HTTP and WebSocket route with standard HTTP Basic authentication. Direct HTTP provides no transport encryption or rate limiting. Fixed same-origin implementation and reviewer WebSocket routes require the deployment repository plus an issue and attempt, re-derive the exact role-specific deterministic name, verify the live tmux session, and attach it through a PTY. Both local routes accept direct operator input; disconnect ends only that tmux client. Same-origin POST actions require the same repository, issue, and attempt identity. The server rejects a repository that does not match `deployment.json` before resolving projected status or local resources. Plan review additionally requires an authorized active attempt and one verified deterministic implementation owner before launching the shared reviewer. Archive, Abandon, Dismiss, Remove, Stop, Retry, and Recover submit concrete owner commands. Slow authorization and cleanup run from the committed effect outside the owner and revalidate immediately before mutation. Hidden-card state, removal progress, and control receipts live in the ledger, so no dashboard file is a competing authority. Peer deployments are presented read-only and their controls and terminals are not proxied.

Startup reconstruction is always:

1. Validate configuration, the managed Codex profile, isolated worker home, and host path preconditions, then authenticate the daemon to GitHub and verify repository access.
2. Acquire the per-repository daemon lock before binding or modifying the runtime-state root. Install the version 2 deployment fence before the ledger so both the normal path and the interrupted fence-before-ledger path fail closed to old writers.
3. Strictly load the v2 ledger or import legacy manifests, hidden-card state, removal intents, completed control receipts, and PR recovery once. Quarantine issue-scoped legacy implementation/reviewer identities unless durable evidence proves no worker launched or a later verified boot excludes survival.
4. Start the owner and persist its new epoch. Build the initial revision-tagged status projection before admitting reads, so a legacy status file is never served as current.
5. Sweep pending typed effects. Consume an exact immutable completion marker first. Retry unmarked work only after reconstructing the same sanitized request and digest from a fresh observation; otherwise retain it with a diagnostic. Remove a completion marker only after the ledger durably records the completed effect, then sync its parent directory.
6. Admit dashboard, control, proposal, and periodic reconciliation lifecycles. Trigger the first slow collection asynchronously so operator mutations do not wait for GitHub or process inspection.
7. Collect current GitHub and local facts from an immutable snapshot. The owner validates cycle, epoch, revision, and generation identities when results and typed effect transitions return. Never adopt an unmarked branch, directory, process, or PR.
8. On shutdown, stop admission and cancel the daemon lifecycle before joining the external proposal, reconciliation, operator, effect, status, and supervisor workers concurrently. Shut down the owner last so it remains available long enough to retain any incomplete durable intent.

## Polling, authentication, and authorization

### Polling and idempotency

The coordinator computes desired state from current GitHub facts at startup, at every interval up to 60 seconds, and when `reconcile` is invoked. Repeated reads, restarts, or changes between polls cannot create a second attempt, branch, worktree, session, PR, feedback turn, or merge because side effects use stable identities and fresh preconditions:

- dispatch first searches markers, branches, PRs, manifests, worktrees, and sessions for the issue/attempt;
- a review comment turn is keyed by immutable comment/review ID and recorded before any feedback side effect;
- policy publication updates the commit status for the current head and fixed context;
- PR creation searches by exact head branch before creating;
- merge supplies the freshly observed head SHA.

### GitHub CLI and actor authorization

The daemon's GitHub integration invokes `gh api` for reconciliation and `gh auth git-credential` for publication. It discovers the coordinator's stable user ID with `/user` and verifies the configured repository through the same authenticated session. Workers have no `gh`, GitHub credential, or network capability; their status mailbox and final result are owner-validated inputs. Setup fails if the daemon CLI is unavailable, unauthenticated, or lacks repository access.

A control-changing event is accepted when a fresh GitHub permission query returns repository `maintain` or `admin`; this includes the authenticated coordinator user. Review feedback may also use `write`. The coordinator rechecks authorization at action time. Edits that change readiness or the configured issue-filter match, close work, change review policy, cancel, retry, or authorize autonomous merge follow this rule. Exact coordinator artifact schemas are filtered from human input; ordinary comments from the same user remain eligible feedback.

Authorization must also survive restart without trusting daemon memory. Control metadata is a canonical, sorted representation of readiness, the optional issue-filter match, priority, dependencies, completion/review policy, cancellation/retry intent, and the complete issue body revision. A body revision is the SHA-256 of the exact body plus its latest immutable body-edit timeline event ID. If the body has never been edited, its anchor is the immutable tuple `(issue node ID, created_at, author ID)`. Current authorized `agent-ready` provenance at or after that boundary authorizes dispatch. A configured issue filter also requires current authorized label provenance but does not replace readiness authorization. Autonomous controls additionally require current authorized `autonomous-merge` provenance at or after the boundary.

After accepting controls, the coordinator writes a snapshot comment binding the normalized-control SHA-256 hash, body hash and edit-event/creation anchor, and immutable timeline event IDs/actor IDs for non-body controls. New snapshots leave the legacy approval fields empty. The snapshot contains no independent policy value, and every provenance actor is freshly authorized. Existing approval-bound snapshots remain readable across upgrades, but new intake uses the ready-label boundary; autonomous authorization must also retain the exact qualifying autonomous label.

On startup and before dispatch, rework, cancellation, or merge, the coordinator rebuilds the current controls/body hash, finds the latest body-edit event or creation anchor, reconstructs non-body provenance from the GitHub timeline, freshly authorizes every provenance actor, and compares all fields with the latest valid coordinator-authored snapshot. Only an exact match restores authorization. A later body edit changes the hash/anchor and blocks dispatch until `agent-ready` is reapplied and a new snapshot is written; autonomous merge also requires a current `autonomous-merge` event after that edit. Missing timeline events, conflicting edits, an anchor/hash mismatch, an unauthorized actor, or inability to attribute every current value blocks mutation; local files cannot fill the gap.

### Credential isolation

Implementation and review workers always use the managed rootless Codex profile. `serve` launches the internal boundary as its current unprivileged user and never selects `sudo`, a root helper, or provisioned worker accounts. Startup fails closed when the installed Codex CLI cannot enforce the exact profile. Arbitrary custom implementation and reviewer commands are not a supported escape hatch.

The profile denies the host filesystem by default, permits minimal runtime reads, and grants intended writes only to one unique per-generation disposable workspace. Project-local Codex and agent configuration is denied. Agent Symphony places each worker's status-request mailbox, result, temporary, and cache paths inside that workspace; authoritative state, credentials, owner commands, manifests, seals, and the daemon control socket remain under the private state root and are never placed in shared temporary directories. Model-generated commands and every descendant have no network access. Plugins, apps, connectors, browser/computer tools, hooks, web search, multi-agent features, skill discovery, and user/project rules are disabled. Approval prompting is disabled without bypassing the sandbox.

Codex 0.153.x on macOS still permits sandboxed commands to read and write unrelated same-user files under shared `/tmp` and `/private/tmp`; explicit deny entries do not close that platform behavior. Agent Symphony diagnoses and reports this residual risk rather than claiming shared-temp isolation. The design keeps all Agent Symphony authority out of shared temp, so shared-temp access cannot produce an accepted state, status, result, seal, or GitHub effect. Operators must not place unrelated same-user secrets in shared temporary directories while workers run.

Workers receive an isolated `CODEX_HOME` containing only the authentication material the trusted Codex client needs for model traffic. They do not receive GitHub tokens, GitHub CLI configuration, SSH-agent sockets, credential helpers, dashboard credentials, owner state paths, or ambient cloud/proxy credentials. The trusted Codex client may authenticate to the model service; commands produced by the model remain inside the denied-network worker profile. See [Security](security.md) for the exact trust boundary.

Each implementation attempt is an independent clone with no remote and no credential helper. The worker may write that clone's local `.git` data, but no coordinator repository, sibling attempt, review snapshot, or owner staging path. Workspace identities are never reused. A detached child can therefore modify only a quarantined old workspace after revocation; the owner never consumes it again.

Worker results and status updates are requests, not mutations. Status travels through a private mailbox bound to the exact worker generation, launch ID, sequence, and managed profile digest. The owner validates the request before applying any GitHub status effect. Completion is copied into an owner-private immutable seal bound to repository, issue, attempt, generation, base, head, bundle digest, and profile digest. Publication and review consume only that seal, never mutable worker files.

Only the coordinator has GitHub and runtime-state authority. It performs `gh api`, authenticated fetch/push, status publication, PR creation, comments, labels, and merge through owner-issued effects. Each effect carries its source revision and generation and is revalidated before its result commits. Local tombstones and generations linearize runtime invalidation immediately; they do not retract an HTTP request GitHub already accepted. Reversible or discoverable remote effects that finish after invalidation receive a durable generation-bound convergence effect that repairs or supersedes them. An exact-head merge accepted before cancellation cannot be rolled back: GitHub remains authoritative for that remote fact, the owner retains and reconciles the invalidated governance intent, and the local tombstone still prevents runtime resurrection.

Old unconfined attempts cannot gain trust from a missing pane or process group. Startup quarantines legacy identities unless durable evidence proves they never launched or a verified later boot proves the old process cannot survive. Quarantine is issue-scoped, visible as physical cleanup pending, prevents resource reuse and publication, and survives restart. It never recreates the historical attempt as an active card. `install-host` remains available only to diagnose or identify legacy installations; it is not a production prerequisite or execution mode.

## Scheduling, execution, and state

Priority is deterministic: P1 before P2 before P3, then oldest `created_at`, then issue number. Dependencies filter candidates before priority. Capacity and optional `## Paths` declarations (one repository-relative file or directory per list line) then select work. Missing, invalid, or overlapping scope serializes same-repository work because disjointness cannot be proven. Rate limits and outages pause dispatch but do not reorder the queue. The coordinator records a concise human-readable reason for every non-runnable item.

The scheduler is a pure recomputation over normalized current GitHub facts. It validates unknown, self, and cyclic dependencies; treats missing, invalid, and overlapping declared paths as conflicting; accounts for global and per-repository capacity; and returns an explanation with every projection. Exact duplicate inputs collapse, while contradictory snapshots for one issue block. It stores no event or cancellation history, so repeated polls and unrelated progress only affect the next projection when authoritative facts change.

One attempt has these projected states:

```text
eligible -> claimed -> preparing -> running -> validating -> publishing
   |           |          |           |             |           |
   +--------> blocked <----+-----------+-------------+-----------+
               |                                      |
               +-> eligible (authorized retry)        +-> review
                                                       |    |
                                                       |    +-> rework -> running
                                                       |    +-> ready-for-human
                                                       +-> merge-check -> merged

any nonterminal -> cancelled (issue closed/readiness removed by authorized actor)
any nonterminal -> failed (bounded retries exhausted)
```

These are projections, not durable workflow rows. GitHub facts define them: ready issue/no marker is `eligible`; valid marker plus local preparation is `claimed/preparing`; live matching session is `running`; agent outcome/validation comment is `validating/publishing`; open PR and review/check facts define `review`, `rework`, and `merge-check`; merged/closed PR defines terminal state. `blocked`, `failed`, and `cancelled` require a coordinator comment/check outcome. Only the coordinator transitions the projection.

The runtime writes a credential-free source bundle whose name includes the repository identity inside the private attempt root. The rootless boundary creates a unique disposable clone and launches the exact managed Codex command in the named tmux session. The prompt contains the issue's exact title/body, repository guidance, attempt identity, prior authorized feedback, allowed actions, and required structured result. Independent review uses one reviewer role for `plan-review` and `implementation-review`; plan review binds the issue-plan digest, while implementation review binds the exact owner-sealed base/head range. Both persist their mode and target and return a target-bound structured result. Status changes use the generation/profile-bound mailbox. Authorized feedback remains ordered; later human instructions supersede conflicting earlier text, and automated findings cannot supersede human instructions. Only a clean review of the exact current seal permits publication.

For both roles, tmux starts a managed wrapper behind a launch gate. The owner durably binds the exact generation, launch identity, profile digest, and workspace before release. Dismiss, Abandon, Remove, Stop, cancellation, supersession, and changed review targets first revoke that generation. A late status, result, seal, or effect cannot commit after revocation. Process-group and pane state remain useful liveness observations, but never prove that an arbitrary descendant is dead. Safety comes from capability revocation: an escaped descendant retains access only to its never-reused disposable workspace.

Each reviewer run also records a confinement-policy version. That version attests the no-network, no-GitHub-credentials, no-owner-state, and snapshot-isolation guarantees that permit exact dead reviewer resources to be cleaned after a Codex binary or profile-digest upgrade. The version must change whenever those guarantees change; unknown or missing versions stay quarantined and fail closed rather than inheriting the current policy.

Implementation review can finish after a label or attention update only when the owner generations, issue body, current attempt, local manifest, and admitted base/head target still match. New reviewer intents hash only the issue fields used by the prompt, leaving status-only changes out of their execution identity; older pending intents keep their original digest rule. A changed issue or attested worker head cancels the old run, stops and proves its process group dead, then records an invalidated review before a fresh review can be planned. The new head is a cancellation signal, not evidence that it was reviewed. Certified old review resources must be cleaned before reusing the target; an ordinary failed review still does not retry the same head automatically.

The worker boundary exports a bounded Git bundle plus branch/head/base, clean-tree, result, profile, and bundle-digest attestation; it never publishes. Outside owner access, the coordinator copies and verifies the exact content into an owner-private immutable seal. It rejects oversized objects, symlinks, result markers, invalid ancestry, a changed head, or any generation/profile mismatch. Publication uses the seal's content-addressed commit. It is reconstructed before each remote mutation from the current owner effect and GitHub markers, so ambiguous responses and crashes resume or compensate the exact phase instead of creating another PR.

Feedback and validation identity includes immutable feedback source/ID values and a validation generation. Authorized issue and pull-request feedback crosses the existing durable implementation handoff; later human instructions supersede conflicting earlier text, while automated review findings cannot supersede human instructions.

The deterministic Go coordinator serializes authoritative mutations, binds attempts to current GitHub facts, publishes sealed results, governs pull-request transitions, delivers review-findings rework, cleans exact attempt resources, and monitors heartbeat/attention state. It is not a routine conversation relay: issue dispatch and review routing are issue-bound, operator chat attaches directly to exact sessions, worker status uses the bounded mailbox, and no coordinator-authored operator-message queue exists.

## Review, rate limits, and merge safety

Open coordinator-authored PRs are reconciled for their entire lifetime, including after local cleanup. Authorized actionable feedback includes issue comments after the first publication marker for the exact issue, attempt, and pull request, pull-request conversation comments, inline review comments, and review bodies. Pre-PR issue history is excluded. Feedback is ordered by immutable GitHub timestamp/ID, deduplicated by source and ID, attached to the existing attempt, and run in the existing safe worktree when available. Control commands and coordinator workflow artifacts are excluded. If the worktree cannot be proven to match the PR head, it is recreated from that branch. Before a feedback turn, the coordinator refetches the exact comment, current `write`-or-higher authorization, PR state, and head SHA. Addressed/blocked disposition and new validation evidence are written to GitHub.

API calls honor `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`, and GitHub secondary-limit responses. Reads use conditional requests where supported. Transient reads retry with exponential backoff, jitter, and a cap; mutations are not blindly retried after ambiguous responses and instead reconcile their stable identity. Near exhaustion pauses dispatch and nonessential status refresh, reserving calls for active cancellation/security and merge checks. Outage/backoff state is visible in CLI status.

Human review is the default. The coordinator publishes the `agent-symphony/policy` commit status for the current head SHA, but repository rules do not need to require it. It fails or remains pending while human review, authorized feedback, validation evidence, or documentation assessment is unresolved.

Immediately before any autonomous merge, the coordinator refetches and requires all of the following for the same head SHA: issue is open/eligible and permits autonomous merge; actor/policy changes remain authorized; PR is open, non-draft, unmodified except through the attempt branch, mergeable, and not behind when rules require current base; no conflicting active path scope exists; all repository-required reviews are present with no current change request; all repository-required checks and the coordinator policy status succeed; the authenticated account can merge; and no unresolved authorized feedback remains. It then calls GitHub's merge endpoint with the expected head SHA and configured allowed method, where GitHub enforces any branch protection or repository rules that exist. A mismatch or ambiguous result returns to reconciliation. The system never force-pushes, overrides rules, admin-merges, dismisses reviews, or writes directly to the integration branch.

## Platform behavior

The executable uses Go filesystem/process APIs and invokes the same Git, tmux, and managed Codex boundary on every platform. `agent-host` implements a bounded command/result seam and is launched directly by `serve` as the current unprivileged user. Paths are canonicalized below the private runtime root and passed as argument arrays, never shell-concatenated. Case-collision and path-length failures are detected before dispatch.

- **Linux:** native binary, advisory file lock, POSIX signals, and the managed rootless Codex permission profile.
- **macOS:** native binary and the same managed rootless Codex profile; no unsupported `sandbox-exec` policy or privileged helper is used.
- **Windows:** WSL2 only. The daemon, Git repository, workspaces, tmux, Codex, and state must live inside one distribution on its Linux filesystem. `/mnt/*` workspaces are rejected.

`doctor` proves executable discovery, minimum versions, tmux session creation/removal, isolated Git creation/export, state-root permissions/atomic rename, signal handling, repository identity, GitHub authentication/access, isolated auth-only worker `CODEX_HOME`, and the effective managed Codex profile. Its canaries verify that worker commands and detached descendants cannot read sibling workspaces, owner state, credentials, or control sockets; cannot use TCP or Unix sockets; cannot load user/project capability configuration; and can write only their exact disposable workspace and private result/status paths. Unsupported Codex versions, ineffective profiles, unsafe legacy records, native Windows, and WSL paths under `/mnt/*` fail with corrective guidance. CLI output honors `NO_COLOR`, never uses color alone, and JSON output has a versioned envelope.

## Repository structure

Create files only as implementation needs them:

```text
cmd/agent-symphony/              command, dashboard, and host boundaries
internal/config/                 repository contract and validation
internal/github/                 GitHub CLI transport, API, normalized models
internal/orchestrator/           projection, scheduling, reconciliation
internal/orchestratoragent/      advisory agent supervision
internal/runtime/                Git worktrees, tmux, agent process
docs/                           product, operator, security, and design records
```

Packages are boundaries for credentials and process effects, not extension points. There are no plugin interfaces, event bus, repository layer, ORM, migration system, or general-purpose web application framework beyond the statically exported dashboard page.

## Validation strategy and capability trace

Pure policy functions use table-driven unit tests. Boundary adapters use temporary Git repositories, fake `gh` and agent executables, and a real local tmux when available. Release smoke tests cover real OS/process differences. Live GitHub CLI tests run only in a dedicated test repository and are required before pilot release, not for ordinary unit tests.

| MVP capability area | Owner | Minimum proof |
| --- | --- | --- |
| Intake/governance (FR1-9) | GitHub + integration/config | Contract parsing, label/policy and fresh actor-authorization cases |
| Backlog (FR10-18) | Coordinator | 100-issue deterministic priority/dependency/scope/capacity simulation |
| Agent/workspace (FR19-27) | Runtime + coordinator | FR19 dispatches the configured primary and, when required, reviewer; FR20 selects those fixed capabilities solely from repository/issue policy; FR25 returns proposed checklist results for coordinator verification/update; isolated export, cancellation, and dual-identity proofs cover the remaining workspace requirements |
| PR/review/merge (FR28-37) | Integration + coordinator | Delayed feedback resume and protected merge cases |
| Validation/docs (FR38-43) | Coordinator + agent result contract | Missing evidence/docs blocks policy check; validated export/evidence publishes; FR43 records material implementation decisions in the originating issue |
| Recovery/status (FR44-50) | Reconciler + state/status | Duplicate delivery and restart/orphan matrix; human/JSON parity |
| Config/CLI (FR51-58) | CLI/config | FR51 `init`; FR52 repository policy config; FR53 boundary/prerequisite validation; FR54 serve/stop/inspect; FR55 human diagnostics; FR56 versioned JSON; FR57 live GitHub CLI identity/connectivity check; FR58 platform/dependency/isolation guidance, proven by managed-profile canaries, `doctor`, and release smoke tests |
| Dashboard (FR59-66) | Loopback-by-default Go server + embedded Next.js page | Embedded export/build, bounded file serving, network opt-in/password authentication, exact tmux WebSocket input/resize/disconnect, archive/abandon/permanent cleanup, constrained recovery, cross-origin/wrong-state/identity-drift rejection, desktop/mobile accessibility and console checks |

Required checks include GitHub CLI transport/authentication failures; repeated polling and restart recovery; initial-body creation anchor, latest body-edit event, unrelated issue activity, ready-label and autonomous-label authorization boundaries, later body edits, and authorized/unauthorized label actors; restart with matching, missing, conflicting, and mismatched anchor/hash/control snapshots; legacy approval-bound snapshot recovery and quarantine; rootless profile capability and denial canaries; uncredentialed local repos; malicious bundle/tree exports; checklist-result verification and issue decision recording; malformed markers/config/paths/API responses; dependency cycles; overlapping/unknown scopes; rate exhaustion and ambiguous or compensated mutations; process crash/cancel; detached descendants; stale/force-pushed head; revoked permission; required-status outcomes; and redaction. Logs are inspected for known canary secrets.

### Required scenario walkthroughs

1. **Dispatch:** after `doctor` proves the GitHub CLI identity, repository access, and active host boundary, a complete ready P1 issue body hash/latest-edit-or-creation anchor has current authorized `agent-ready` provenance plus a matching coordinator snapshot. Reconciliation finds no attempt resources; the coordinator writes attempt 1, seeds the uncredentialed local repository, and launches the implementation agent. It validates the exported patch/tree before publishing from its own checkout. A concurrent independent issue may start within capacity; unknown overlap serializes.
2. **Delayed review feedback:** an open marked PR remains discoverable with no daemon memory. A later poll finds a new comment; fresh actor authorization and immutable comment ID pass. The coordinator records that ID before feedback side effects, reconstructs the matching worktree at current head, runs rework and review, validates, pushes, and updates the same PR and policy status. Later polls find the recorded ID and do nothing.
3. **Failed PR policy:** verified worker evidence missing from GitHub is republished idempotently. If checks, repository rules, or permissions still prevent the unchanged head from proceeding, a canonical coordinator-authored PR comment records the blockers; its exact head/reason body prevents duplicate comments across polls and restarts.
4. **Restart:** after lock acquisition, GitHub markers, control snapshots/timeline provenance, and open PRs are fetched before local resources. Current normalized controls, body hash, and latest edit-event/creation anchor must exactly match a coordinator snapshot bound to the current authorized ready-label boundary and, when enabled, the autonomous-label boundary. Unrelated issue activity does not invalidate it, but any body or required-label mismatch blocks until reauthorization. A matching live session is monitored; a missing session is marked orphaned and safely recreated or blocked. Active/completed markers prevent redispatch. No queue database or remembered authorization is restored.
5. **Repeated polling:** repeated reads of unchanged GitHub facts use stable attempt/comment/status/branch/PR identities and fresh preconditions, so they permit at most one side effect.
6. **Protected merge:** after successful validation, the coordinator refreshes policy, authorization, head, reviews, checks, mergeability, and repository rules. Human-review still present leaves the policy check pending. When policy permits and every gate passes, merge uses the expected SHA; protection failure or head movement blocks and reconciles—never overrides.
7. **Rootless worker authority:** `doctor` checks the exact managed Codex profile, isolated auth-only `CODEX_HOME`, private generation paths, and denial canaries. A detached child cannot read another attempt, owner state, GitHub credentials, or sockets and cannot use the network. It may alter only its never-reused workspace. Status is owner-mediated, and publication succeeds only from the current immutable owner seal.
8. **Self-contained executable:** on clean macOS, Linux, and WSL2 hosts with only declared external tools, verify the checksum, run `agent-symphony doctor`, start/status/stop against the harness as an ordinary user, and confirm that no root setup, Go runtime, helper package, or shared library is required.

## Adversarial review resolutions

- **GitHub has no atomic issue claim:** MVP permits exactly one locked coordinator instance. Deterministic markers/resources plus reconcile-before-effect prevent duplicates after crash; HA is explicitly unsupported.
- **A local manifest can become a shadow database:** its schema excludes policy and queue state, is disposable, and always loses conflicts to GitHub.
- **A commit status does not enforce itself:** the coordinator reevaluates its policy immediately before merging with the expected head; configuring the status as a repository-required check is optional.
- **Issue body edits could silently change execution controls:** the coordinator snapshot binds body/control hashes and the latest body-edit event or initial creation anchor. Dispatch requires a current authorized ready-label event at or after that boundary; autonomous merge additionally requires a current authorized autonomous-label event, so unrelated activity is harmless but a later body edit blocks.
- **A restart could forget who authorized current controls:** current normalized metadata/revision must match a coordinator-authored snapshot bound to current label provenance; ambiguity blocks.
- **Implementation and review workers could retain owner authority after cancellation:** the managed rootless Codex profile removes GitHub, network, state, socket, sibling, and ambient configuration capabilities; unique never-reused workspaces, generation-bound mailboxes, immutable owner seals, and detached-child canaries enforce revocation.
- **Resuming the wrong process/worktree corrupts work:** resume requires agreement among GitHub marker/head, manifest, deterministic path/session, and Git worktree state; uncertainty blocks or recreates.
- **Concurrent issues can overlap despite optimistic descriptions:** MVP requires declared disjoint path scopes and serializes uncertainty; semantic inference is deferred.
- **Rate-limit retry can repeat a mutation:** ambiguous mutations reconcile stable identities instead of blind retry.
- **WSL mixed filesystems/processes violate Unix assumptions:** the MVP requires one WSL2 distribution and Linux filesystem paths.

## Deferred decisions

Centralized multi-repository operation, HA/multiple coordinators, remote workers, inferred conflicts/dependencies, native Windows, first-class TLS termination or identity-aware dashboard authentication, package managers, containers, specialist role frameworks, a general workflow/plugin system, and arbitrary non-Codex worker commands are post-MVP. Add them only when a separate issue supplies evidence and acceptance criteria.
