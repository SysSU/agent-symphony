# MVP Architecture

**Status:** Proposed for MVP implementation  
**Scope:** one repository, one orchestrator instance, macOS, Linux, and WSL2

## Decision summary

Agent Symphony is one long-running Go process with a CLI mode. GitHub Issues, pull requests, reviews, checks, and repository rules are the durable workflow record. The process keeps only scheduling state in memory and bounded, reconstructible execution metadata on disk; it does not have a task database or a second workflow engine.

Issue #10 implements only the one-shot PR-governance reconciliation and durable handoff boundary described here. Issue #4 owns daemon scheduling, state production and consumption, runtime resumption, and end-to-end wiring; those capabilities are not yet available.

The design follows the useful boundaries in the [OpenAI Symphony specification](https://github.com/openai/symphony/blob/main/SPEC.md): a single scheduling authority, a tracker adapter, deterministic workspaces, an agent runner, and an operator status surface. It deliberately differs in three places required by this product: GitHub owns the whole delivery lifecycle, portfolio policy is coordinator code rather than agent prompt logic, and agents never receive tracker credentials. The upstream Elixir implementation is a prototype; its in-memory blocked state and runtime dependency make it a reference, not the release base.

### Stack and release

- **Go 1.26, pinned in `go.mod` and built with the latest 1.26 security patch.** Goroutines, `net/http`, `os/exec`, `crypto/hmac`, and `encoding/json` cover the daemon, webhook endpoint, process supervision, signature checks, and CLI. Add a dependency only where the standard library cannot safely implement a protocol (initially, none is required).
- **One `agent-symphony` executable.** The same binary provides `init`, `validate`, `serve`, `stop`, `status`, `inspect`, `reconcile`, and `doctor`. `serve` owns orchestration; other commands use a local Unix socket and read-only GitHub queries. No language runtime, database, browser service, or container is required.
- **GitHub Releases** publish signed-tag build artifacts and SHA-256 checksums for `darwin/{arm64,amd64}` and `linux/{arm64,amd64}`. WSL2 uses the Linux artifact. Release CI runs unit tests, `go vet`, builds with `CGO_ENABLED=0`, smoke-tests each supported OS, and verifies a downloaded artifact against its checksum.
- **External runtime prerequisites:** Git, tmux, configured coding-agent executables, repository access, GitHub connectivity, and one completed `agent-symphony install-host` run by a host administrator. `doctor` checks versions, provisioned identities, privilege rules, and behavior before `serve` accepts work. Package-manager and container distribution are post-MVP.

## System boundaries and ownership

There is one coordinator loop and one command queue. Webhooks, the reconciliation timer, worker exits, and CLI reconciliation requests only enqueue hints; they never mutate orchestration state directly. The coordinator refreshes authoritative state and makes every transition. This preserves upstream Symphony's single-writer property without adopting OTP or a workflow framework.

| Boundary | Owns | Must not own |
| --- | --- | --- |
| GitHub integration | App authentication, API reads/writes, normalized issue/PR/check/review models, authorization, rate-limit handling, webhook intake | Scheduling decisions or agent processes |
| Coordinator/scheduler | Eligibility, priority, explicit dependencies, conservative conflict locks, capacity, claims, retries, cancellation, reconciliation | Durable task truth or GitHub credentials passed to workers |
| Runtime | Worktree and branch lifecycle, tmux session, agent subprocess, timeouts/signals, captured logs, resume feasibility | Issue policy, PR creation, push credentials, or merge decisions |
| Local metadata | Atomic manifest per attempt and daemon lock/socket; enough to find local resources | Queue, issue body/checklist, policy, review/check state, or webhook history |
| CLI | Configuration, diagnostics, local control, human and JSON projections | An alternate mutation path around issue, review, or merge policy |
| Agent | Edit files, run validation, assess/update documentation, return structured outcome | GitHub API/tooling, credentials, policy decisions, push, PR creation, or merge |

Repository configuration names exactly one primary implementation agent command and prompt profile. That deterministic agent owns implementation and follow-up turns for the attempt. When issue or repository policy requires independent agent review, the coordinator then runs one separately configured review agent under a different OS identity against the resulting snapshot and validation evidence; it cannot access the implementation worktree, and its findings return to the primary agent for resolution. The two fixed responsibilities satisfy role selection without a general role or plugin framework.

## GitHub is authoritative

### Repository contract

Version-controlled `.agent-symphony.yaml` contains labels, priority mapping, explicit dependency syntax, completion policy, concurrency (default `1`, minimum supported `2` when configured), repository subdirectory below the provisioned attempt root, documentation paths, primary implementation and independent review agent commands/profiles, and status preferences. Secrets and mutable status are forbidden. Version `1` is added only when an incompatible schema is introduced; unknown keys and unsafe paths fail validation.

An eligible issue is open, has the configured ready label, has exactly one P1-P3 label, contains the required contract sections, has no unresolved explicit dependency, and is not represented by an active or completed attempt. Dependencies use explicit issue references in the configured issue section; MVP never infers them. Parallel execution requires disjoint declared path scopes. Missing, overlapping, or invalid scopes serialize, which is the safe minimum.

GitHub stores durable execution facts using machine-readable HTML markers in coordinator-authored issue comments and PR bodies:

```text
<!-- agent-symphony:attempt:v1
{"attempt":2,"branch":"agent-symphony/8-2","head":"...","pr":31,"outcome":"review"}
-->
```

The marker schema is strict, size-bounded, and parsed only from the configured GitHub App identity. Human-readable text accompanies it. Attempt number is the next integer after the highest valid marker for the issue. Branch `agent-symphony/<issue>-<attempt>`, worktree `<root>/<owner>-<repo>-<issue>-<attempt>`, and tmux session `as-<repo-id>-<issue>-<attempt>` are deterministic. A PR contains `Closes #N` and the attempt marker. These identifiers make discovery possible without local files.

Issue/PR state, labels, comments, reviews, check runs, branch heads, and repository rules always beat local metadata. A contradiction blocks mutation, emits diagnostics, and requests reconciliation. Local files may never make completed work eligible again.

### Bounded local metadata and reconstruction

The state root is the OS user-state directory (`$XDG_STATE_HOME/agent-symphony` or `~/.local/state/agent-symphony` on Linux/WSL, `~/Library/Application Support/agent-symphony` on macOS), overrideable outside the repository. It contains:

```text
repo-<repository-id>/
  daemon.lock            # single-instance advisory lock
  control.sock           # mode 0600
  attempts/<issue>-<n>/manifest.json
  attempts/<issue>-<n>/agent.log
```

Each manifest contains only schema version, repository/issue/attempt IDs, deterministic resource names, agent process/session identity, last observed GitHub object IDs and head SHA, timestamps, and log path. It is written by temp-file plus atomic rename, mode `0600`. Logs are size-rotated and retention-bounded; terminal manifests/logs expire after a configurable period (default 30 days). Deleting the entire state root loses diagnostics and resumable terminal context, but not workflow truth.

Startup reconstruction is always:

1. Acquire the per-repository daemon lock; refuse a second instance. Multi-host/high-availability operation is out of MVP.
2. Validate config, dependencies, worker OS identity/isolation, App installation, effective permissions, webhook/check configuration, repository identity, paths, and the clean primary coordination checkout.
3. Fetch open eligible issues, all open App-marked PRs, recent valid attempt/control snapshots, issue timeline provenance, reviews, checks, labels, branch heads, and merge/rules state.
4. Scan only deterministic worktree/tmux names and bounded manifests for those attempts. Never adopt an unmarked branch, directory, process, or PR.
5. Build the in-memory projection. If a GitHub-active attempt has a live matching tmux/worktree and manifest/head agreement, resume monitoring. Otherwise mark it orphaned/blocked in GitHub and either repair the same attempt when provably safe or create a new numbered attempt. Never attach to an unknown process.
6. Reconcile terminal/closed work before dispatch, then evaluate the backlog. Target: complete within two minutes under normal API availability.

## Events, authentication, and authorization

### Webhooks and idempotency

The webhook handler reads a bounded raw body, requires JSON content type and known event headers, verifies `X-Hub-Signature-256` as HMAC-SHA256 over the exact body using constant-time comparison, then validates installation and repository IDs before parsing event-specific fields. Invalid requests receive no state mutation. A valid request receives a success response only after its repository/object hint is accepted by the bounded coalescing queue; a full or unavailable queue returns a retryable failure. Processing happens asynchronously. If the process crashes after acknowledgement but before processing, periodic reconciliation derives the missed transition from current GitHub state.

`X-GitHub-Delivery` is useful for correlation, not durable correctness. The process keeps a bounded in-memory LRU of recent delivery IDs to reduce work. Every event is only a hint to reconcile the affected GitHub object; reconciliation computes the desired state from current GitHub facts. Therefore redelivery, reordering, a crash after acknowledgement, or an evicted delivery ID cannot create a second attempt, branch, worktree, session, PR, feedback turn, or merge. Side effects use stable identities and preconditions:

- dispatch first searches markers, branches, PRs, manifests, worktrees, and sessions for the issue/attempt;
- a review comment turn is keyed by immutable comment/review ID and recorded in the attempt marker before any feedback side effect;
- check runs use a stable external ID and are updated rather than multiplied;
- PR creation searches by exact head branch before creating;
- merge supplies the freshly observed head SHA.

Periodic full reconciliation (default 60 seconds, with jitter) and a manual `reconcile` recover missed webhooks. Events covered are issue edits/labels/closure, PR and review comments, reviews, checks/statuses, pushes, pull-request changes, installation/permission changes, and repository-rule changes.

### GitHub App and actor authorization

The App uses a locally readable private key or OS credential store and exchanges its JWT for short-lived installation tokens in coordinator memory. Requested repository permissions are the least privilege needed by enabled MVP features: metadata read; administration read; issues read/write; pull requests read/write; contents read/write; checks read/write; commit statuses read; and members read only when organization/team authorization is configured. Setup fails if effective permissions are insufficient.

Webhook signature proves GitHub delivery, not human authority. A control-changing event is accepted only when its actor is not the App/bot and a fresh GitHub permission/team query satisfies configured roles (default repository `maintain` or `admin`; review approval may separately allow `write`). The coordinator rechecks authorization at action time. Edits that remove readiness, close work, change review policy, cancel, retry, or authorize autonomous merge follow this rule. Unauthorized content is visible feedback at most; it cannot drive execution or policy.

Authorization must also survive restart without trusting daemon memory. Control metadata is a canonical, sorted representation of readiness, priority, dependencies, path scope, completion/review policy, cancellation/retry intent, and the issue body sections that carry those values. A body revision is the SHA-256 of the exact body plus its latest immutable body-edit timeline event ID. If the body has never been edited, its anchor is the immutable tuple `(issue node ID, created_at, author ID)`. Issue-wide `updated_at` is never used because labels, comments, and other unrelated activity can change it. Body-derived execution controls are inert until an authorized actor posts the configured explicit approval command after the anchored body revision; labels and other non-body controls retain their own GitHub timeline provenance.

After accepting controls, the App writes a new App-authored snapshot comment binding the normalized-control SHA-256 hash, body hash and edit-event/creation anchor, immutable approval-command comment ID and actor ID, and immutable timeline event IDs/actor IDs for non-body controls. The snapshot contains no independent policy value. The approval actor must be freshly authorized, the approval comment must still contain the exact command, and its creation time must be later than the bound edit event or initial issue creation.

On startup and before dispatch, rework, cancellation, or merge, the coordinator rebuilds current controls/body hash, finds the latest body-edit event or creation anchor, reconstructs non-body provenance from the GitHub timeline, refetches the bound approval comment, freshly authorizes every provenance/command actor, and compares all fields with the latest valid App-authored snapshot. Only an exact match restores authorization. Any later body edit changes the hash/anchor and blocks body-derived controls until a new authorized approval comment and App snapshot exist. Missing or edited commands, missing timeline events, conflicting edits, an anchor/hash mismatch, an unauthorized actor, or inability to attribute every current value blocks mutation and reports the required correction; local files and webhook payloads cannot fill the gap.

### Credential isolation

The coordinator, implementation agent, and reviewer run as three distinct OS identities with exactly two sharing groups. `agent-symphony-attempt` is the worker's primary group and contains the coordinator only as a supplementary member. `agent-symphony-snapshot` is the reviewer's primary group and contains the coordinator only as a supplementary member. Worker and reviewer are excluded from each other's group. Both agent accounts have no login password, other supplementary groups, sudo rights, ACL, socket access, or access to coordinator secrets/state.

The attempt root is owned `agent-symphony-worker:agent-symphony-attempt` mode setgid `2770`; each isolated attempt repository/worktree directory is also setgid `2770`, and files are created with umask `0007` so group inheritance and coordinator access are deterministic. The reviewer is excluded. Because every attempt uses the same fixed worker UID, worker processes can access all attempt roots: OS permissions are not a security boundary between attempts.

The threat model treats all fixed-UID worker processes as mutually trusted for repository integrity, but untrusted for coordinator secrets and GitHub authority. Separate repositories/worktrees, disjoint-scope scheduling, and coordinator export validation contain accidental mistakes; they do not contain a hostile sibling worker. Attempts contain no credential, credential helper, or credentialed remote, and the coordinator seeds them through a local bundle/archive without exposing its authenticated checkout. Hostile worker-to-worker containment requires per-attempt identities or a proven sandbox and is deferred post-MVP.

The snapshot root is owned `coordinator:agent-symphony-snapshot` mode `0750`; the reviewer is in the snapshot group and the worker is excluded. Each completed snapshot has directories mode `0550` and files mode `0440`, so the reviewer can read but not mutate it. Secrets, manifests, logs, lock, and control socket remain in a separate coordinator-owned mode-`0700` tree (socket `0600`) shared with neither group. Unsupported hosts that cannot enforce these boundaries fail `doctor` and cannot serve.

A host administrator installs the release at root-owned mode-`0755` `/usr/local/libexec/agent-symphony/<version>/agent-symphony`, then runs that exact binary once as `install-host --coordinator <user>`. It creates fixed accounts `agent-symphony-worker` and `agent-symphony-reviewer`, groups `agent-symphony-attempt` and `agent-symphony-snapshot`, and the attempt/snapshot roots with the ownership and modes above at `/var/lib/agent-symphony/{attempts,snapshots}` on Linux/WSL or `/var/db/agent-symphony/{attempts,snapshots}` on macOS. Linux/WSL uses `groupadd --system`, `useradd --system --gid` to assign each agent its stated primary group plus matching private home and `/usr/sbin/nologin`, and `usermod --append --groups agent-symphony-attempt,agent-symphony-snapshot <coordinator>` for the coordinator's two supplementary memberships. macOS uses `dscl` to create hidden local users with allocated unique IDs, the matching `PrimaryGroupID`, private homes, and `/usr/bin/false`, and `dseditgroup` to add only the coordinator to both groups.

The root-owned `/etc/sudoers.d/agent-symphony` names the versioned binary path, coordinator user, exact target user and group, and exactly two passwordless invocations: `sudo -u agent-symphony-worker -g agent-symphony-attempt <versioned-binary> agent-host implementation` and `sudo -u agent-symphony-reviewer -g agent-symphony-snapshot <versioned-binary> agent-host review`, with no additional arguments; launch requests arrive on standard input. No shell, arbitrary user/group, command, wildcard path/argument, account-management, or coordinator-as-root rule is granted. The administrator reruns the new version's `install-host` after every binary upgrade so it atomically validates accounts/groups/roots, rewrites the exact path/rules, removes the superseded managed rules, and refuses conflicting identities or broader unmanaged grants.

Environment filtering is defense in depth, not the security boundary. The two `agent-host` modes create agent/tmux processes with explicit allowlists rather than inherited environments. They remove GitHub/SSH/cloud tokens, credential-helper variables, proxy credentials, and App key paths; disable Git credential helpers; supply no GitHub MCP/tool; and redact known secret values and credential-shaped fields from output. Each tmux server runs under its assigned agent identity, never the coordinator identity.

Only the coordinator performs authenticated fetch, push, GitHub mutation, and merge. For an authenticated Git command it launches a short-lived, non-agent process with an ephemeral installation token through a mode-`0700` askpass helper and a process-only environment, then deletes the helper and environment. Tokens never appear in command arguments, Git config, worktrees, tmux history, or agent logs. The agents may use their separately configured model credentials only; those credentials grant no repository authority. OS permissions are the MVP isolation boundary—containers are not claimed as a security boundary.

## Scheduling, execution, and state

Priority is deterministic: P1 before P2 before P3, then oldest `created_at`, then issue number. Dependencies filter candidates before priority. Capacity and declared path-scope locks then select work. Rate limits and outages pause dispatch but do not reorder the queue. The coordinator records a concise human-readable reason for every non-runnable item.

The scheduler is a pure recomputation over normalized current GitHub facts. It validates unknown, self, and cyclic dependencies; treats missing, invalid, and overlapping declared paths as conflicting; accounts for global and per-repository capacity; and returns an explanation with every projection. Exact duplicate inputs collapse, while contradictory snapshots for one issue block. It stores no webhook or cancellation history, so duplicate events and unrelated progress only affect the next projection when authoritative facts change.

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

The runtime seeds a worker-owned isolated local repository/worktree from the freshly fetched approved integration head, then invokes the exact provisioned implementation launch as `agent-symphony-worker`; that mode starts the named tmux session and deterministic primary command in the worktree. The prompt contains the issue contract, repository guidance, attempt identity, prior authorized feedback, allowed actions, and a required structured result. When policy requires independent review, the coordinator exports an immutable tree/diff into the snapshot root, then invokes the exact review launch as `agent-symphony-reviewer` with a distinct prompt; findings either return the attempt to the primary agent or permit validation. Process control uses process groups: graceful interrupt, bounded wait, then targeted kill of only that attempt. Attempts are retained for open PRs/rework and removed only after merge/abandonment is durable in GitHub.

The worker exports a patch plus tree manifest; it never publishes. The coordinator validates paths, file types/modes, size bounds, base SHA, patch applicability, resulting tree hash, policy scope, and absence of secrets before applying the export in a coordinator-controlled publishing checkout. It then runs configured validation, checks documentation impact, commits/pushes the branch, creates/updates the one PR, and records evidence and material implementation decisions in the issue. The primary agent's structured result includes proposed issue-checklist completions; the credentialed coordinator verifies and applies those updates, satisfying FR25 without giving the agent GitHub access.

## Review, rate limits, and merge safety

Open App-authored PRs are reconciled for their entire lifetime, including after local cleanup. Authorized actionable review comments are ordered by immutable GitHub timestamp/ID, deduplicated by ID, attached to the existing attempt, and run in the existing safe worktree when available. If the worktree cannot be proven to match the PR head, it is recreated from that branch. Before a feedback turn, the coordinator refetches the comment, authorization, PR state, and head SHA. Addressed/blocked disposition and new validation evidence are written to GitHub.

API calls honor `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`, and GitHub secondary-limit responses. Reads use conditional requests where supported. Transient reads retry with exponential backoff, jitter, and a cap; mutations are not blindly retried after ambiguous responses and instead reconcile their stable identity. Near exhaustion pauses dispatch and nonessential status refresh, reserving calls for active cancellation/security and merge checks. Outage/backoff state is visible in CLI status.

Human review is the default. The App publishes one `agent-symphony/policy` Check Run for the current head SHA. Setup validates that repository rules require this check; otherwise autonomous merge is disabled. The check fails/pends while `needs-human-review`, unresolved authorized feedback, missing validation evidence, or missing documentation assessment exists.

Immediately before any autonomous merge, the coordinator refetches and requires all of the following for the same head SHA: issue is open/eligible and permits autonomous merge; actor/policy changes remain authorized; PR is open, non-draft, unmodified except through the attempt branch, mergeable, and not behind when rules require current base; no conflicting active path scope exists; all required reviews are present with no current change request; all required checks including policy succeed; repository rules/branch protection and App permission permit merge; and no unresolved authorized feedback remains. It then calls GitHub's merge endpoint with the expected head SHA and configured allowed method. A mismatch or ambiguous result returns to reconciliation. The system never force-pushes, overrides rules, admin-merges, dismisses reviews, or writes directly to the integration branch.

## Platform behavior

The executable uses Go filesystem/process APIs and invokes the same Git and tmux commands on every platform. Paths are canonicalized, required to remain below their provisioned attempt/snapshot roots, and passed as argument arrays—never shell-concatenated. Case-collision and path-length failures are detected before dispatch; the local bundle/export boundary does not require coordinator and worker repositories to share a filesystem.

- **Linux:** native binary, Unix socket, advisory file lock, POSIX signals; `install-host` uses `groupadd`/`useradd`/`usermod`, `/var/lib` runtime roots, and the exact versioned-binary sudoers rules above.
- **macOS:** native signed/notarized binary when release infrastructure is available; application-support state path; `install-host` uses `dscl`/`dseditgroup`, `/var/db` runtime roots, and the exact versioned-binary sudoers rules above; BSD userland assumptions are avoided.
- **Windows:** WSL2 only. `install-host`, the three identities, sudo, daemon, Git repository, worktrees, tmux, agents, and state must all live inside the same WSL distribution on its Linux filesystem. `/mnt/c` worktrees and cross-boundary Windows accounts/processes are rejected because permissions, signals, sockets, and filesystem performance are not reliable enough for the MVP.

`doctor` proves executable discovery, minimum versions, tmux session creation/removal, isolated local Git creation/export, state-root permissions/atomic rename, signal handling, repository remote/identity, GitHub App permissions, required webhook events, and required policy check. It verifies the installed binary version/path/owner/mode, platform-specific account creation semantics, worker/reviewer primary groups, coordinator supplementary memberships, setgid `2770` attempt root/directories and umask `0007`, `0750` snapshot root and completed `0550`/`0440` snapshots, managed sudoers content, and both exact user/group/command tuples. Denial checks cover swapped users or groups, shells, extra arguments, arbitrary binaries, and coordinator root. Canaries confirm that the fixed worker can traverse multiple attempt directories, while the reviewer cannot traverse the attempt root, the worker cannot traverse the snapshot root, neither agent can reach coordinator `0700` state/secret paths or the `0600` socket, snapshots are read-only to the reviewer, and attempt repositories have no credentialed remote/helper. Unsupported native Windows, non-WSL environments, stale-version installation, shared identities, broader sudo rules, and ineffective permissions fail with corrective guidance. CLI output honors `NO_COLOR`, never uses color alone, and JSON output has a versioned envelope.

## Repository structure

Create files only as implementation needs them:

```text
cmd/agent-symphony/main.go       command parsing and process entry
internal/config/                 repository contract and validation
internal/github/                 App auth, API, webhooks, normalized models
internal/orchestrator/           projection, scheduling, reconciliation
internal/runtime/                Git worktrees, tmux, agent process
internal/state/                  bounded manifests, lock, control socket
internal/status/                 human and JSON projections
docs/architecture.md
```

Packages are boundaries for credentials and process effects, not extension points. There are no plugin interfaces, event bus, repository layer, ORM, migration system, or web dashboard in the MVP.

## Validation strategy and capability trace

Pure policy functions use table-driven unit tests. Boundary adapters use `httptest.Server`, temporary Git repositories, fake executables, and a real local tmux when available. One end-to-end harness runs the compiled binary against a fake GitHub HTTP server and fake agent; release smoke tests cover real OS/process differences. Live GitHub App tests run only in a dedicated test repository and are required before pilot release, not for ordinary unit tests.

| MVP capability area | Owner | Minimum proof |
| --- | --- | --- |
| Intake/governance (FR1-9) | GitHub + integration/config | Contract parsing, label/policy and fresh actor-authorization cases |
| Backlog (FR10-18) | Coordinator | 100-issue deterministic priority/dependency/scope/capacity simulation |
| Agent/workspace (FR19-27) | Runtime + coordinator | FR19 dispatches the configured primary and, when required, reviewer; FR20 selects those fixed capabilities solely from repository/issue policy; FR25 returns proposed checklist results for coordinator verification/update; isolated export, cancellation, and dual-identity proofs cover the remaining workspace requirements |
| PR/review/merge (FR28-37) | Integration + coordinator | Delayed feedback resume and protected merge cases |
| Validation/docs (FR38-43) | Coordinator + agent result contract | Missing evidence/docs blocks policy check; validated export/evidence publishes; FR43 records material implementation decisions in the originating issue |
| Recovery/status (FR44-50) | Reconciler + state/status | Duplicate delivery and restart/orphan matrix; human/JSON parity |
| Config/CLI (FR51-58) | CLI/config | FR51 `init`; FR52 repository policy config; FR53 boundary/prerequisite validation; FR54 serve/stop/inspect; FR55 human diagnostics; FR56 versioned JSON; FR57 live App permission/connectivity check; FR58 platform/dependency/isolation guidance, proven by `install-host`, `doctor`, sudo allow/deny, and release smoke tests |

Required checks include webhook signature/body/header limits; enqueue failure and post-ack crash recovery; initial-body creation anchor, latest body-edit event, unrelated issue activity, approval before/current anchor, later body edit, edited command, and authorized/unauthorized command actors; restart with matching, missing, conflicting, and mismatched anchor/hash/control snapshots; platform-specific install/upgrade, primary/supplementary memberships, setgid `2770` inheritance and umask `0007`, fixed-worker access across attempts, worker/reviewer cross-group denial, exact sudo user/group/command allow-and-deny matrices, coordinator secret/socket canaries, uncredentialed local repos, malicious patch/tree exports, checklist-result verification, and issue decision recording; malformed markers/config/paths/API responses; dependency cycles; overlapping/unknown scopes; rate exhaustion and ambiguous mutations; process crash/cancel; stale/force-pushed head; revoked permission; missing required check; and redaction. Tests assert that scope/export controls limit accidental cross-attempt changes and make no hostile-sibling containment claim. Race-enabled tests exercise coordinator queues. Logs are inspected for known canary secrets.

### Required scenario walkthroughs

1. **Dispatch:** after the current-version `install-host` and `doctor` prove primary/supplementary groups, setgid roots, umask, and exact sudo user/group/commands, a ready P1 body hash/latest-edit-or-creation anchor has a later authorized approval command and matching App snapshot. Reconciliation finds no attempt resources; the coordinator writes attempt 1, seeds the worker-owned uncredentialed local repository, and launches the primary with user `agent-symphony-worker` and group `agent-symphony-attempt`. It validates the exported patch/tree before publishing from its own checkout. A concurrent independent issue may start within capacity; unknown overlap serializes. Both workers are mutually trusted for repository integrity; required review uses `agent-symphony-reviewer:agent-symphony-snapshot` with only a `0550`/`0440` snapshot.
2. **Delayed review feedback:** an open marked PR remains discoverable with no daemon memory. Weeks later a signed webhook is acknowledged only after its hint is queued. Fresh actor authorization and immutable comment ID pass; the coordinator records that ID before feedback side effects, reconstructs/recreates the matching worktree at current head, runs rework, performs configured review under the separate reviewer identity when required, validates, pushes, and updates the same PR/check. Redelivery finds the recorded ID and does nothing.
3. **Restart:** after lock acquisition, GitHub markers, control snapshots/timeline provenance, approval commands, and open PRs are fetched before local resources. Current normalized controls, body hash, and latest edit-event/creation anchor must exactly match an App snapshot bound to a later, still-valid, freshly authorized command; unrelated issue activity does not invalidate it, but any body edit or mismatch blocks until reapproval. A matching live session is monitored; a missing session is marked orphaned and safely recreated or blocked. Active/completed markers prevent redispatch. No queue database or remembered authorization is restored.
4. **Duplicate webhook:** two identical deliveries are each acknowledged only after enqueue and produce one or two harmless hints. Both reconcile the same current facts; stable attempt/comment/check/branch/PR identities and preconditions permit at most one side effect. A crash after acknowledgement loses only the hint because periodic reconciliation discovers the current GitHub state.
5. **Protected merge:** after successful validation, the coordinator refreshes policy, authorization, head, reviews, checks, mergeability, and repository rules. Human-review still present leaves the policy check pending. When policy permits and every gate passes, merge uses the expected SHA; protection failure or head movement blocks and reconciles—never overrides.
6. **No credentials to agents:** after current-version admin `install-host`, `doctor` checks setgid `2770` attempt ownership/inheritance, umask `0007`, exact sudo user/group/command permissions/denials, and both real tmux paths. Coordinator secrets/state/socket are `0700`/`0600` and unreachable. The fixed worker can access all attempt repositories but no snapshot; the reviewer can access no attempt and can only read coordinator-owned completed `0550`/`0440` snapshots. An attempted `git push` from either agent fails authentication; separate repositories, disjoint-scope scheduling, and coordinator export validation contain accidental worker mistakes, not a hostile sibling.
7. **Self-contained executable:** on clean macOS, Linux, and WSL2 hosts with only declared external tools, verify checksum, use the same binary for the one-time admin `install-host`, run `agent-symphony doctor`, start/status/stop against the harness, and confirm no Go runtime, helper package, or shared library is required.

## Adversarial review resolutions

- **Webhook delivery IDs alone lose work after crash:** events are hints; periodic authoritative reconciliation is the recovery mechanism.
- **GitHub has no atomic issue claim:** MVP permits exactly one locked coordinator instance. Deterministic markers/resources plus reconcile-before-effect prevent duplicates after crash; HA is explicitly unsupported.
- **A local manifest can become a shadow database:** its schema excludes policy/queue state, is disposable and retention-bounded, and always loses conflicts to GitHub.
- **A Check Run does not enforce itself:** autonomous merge remains disabled unless repository rules require `agent-symphony/policy`; merge gates are still reevaluated.
- **A valid webhook can contain an unauthorized command:** actor permission is fetched separately at action time.
- **Issue body edits could silently change execution controls:** body controls require a later explicit authorized approval; the App snapshot binds command identity, body/control hashes, and latest body-edit event or initial creation anchor, so unrelated activity is harmless but any body edit blocks until reapproval.
- **A restart could forget who authorized current controls:** current normalized metadata/revision must match an App-authored snapshot bound to the still-valid approval command and timeline provenance; ambiguity blocks.
- **Implementation and review agents could share authority or read daemon secrets:** versioned `install-host` creates separate identities and disjoint sharing groups with two exact sudo launch rules; ownership, export validation, denial, and dual-path canary tests enforce separation while environment filtering remains defense in depth.
- **Resuming the wrong process/worktree corrupts work:** resume requires agreement among GitHub marker/head, manifest, deterministic path/session, and Git worktree state; uncertainty blocks or recreates.
- **Concurrent issues can overlap despite optimistic descriptions:** MVP requires declared disjoint path scopes and serializes uncertainty; semantic inference is deferred.
- **Rate-limit retry can repeat a mutation:** ambiguous mutations reconcile stable identities instead of blind retry.
- **WSL mixed filesystems/processes violate Unix assumptions:** the MVP requires one WSL2 distribution and Linux filesystem paths.

## Deferred decisions

Multi-repository operation, HA/multiple coordinators, remote workers, inferred conflicts/dependencies, native Windows, dashboard, package managers, containers, specialist role frameworks, a general workflow/plugin system, and hostile worker-to-worker containment through per-attempt identities or sandboxing are post-MVP. Add them only when a separate issue supplies evidence and acceptance criteria.
