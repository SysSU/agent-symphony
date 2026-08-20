# MVP Architecture

**Status:** Current implementation

**Last verified:** 2026-08-17

**Scope:** one repository per orchestrator instance, multiple independent instances per host, macOS, Linux, and WSL2

## Decision summary

Agent Symphony is one long-running Go process with a CLI mode. GitHub Issues, pull requests, reviews, checks, and repository rules are the durable workflow record. The process keeps only scheduling state in memory and bounded, reconstructible execution metadata on disk; it does not have a task database or a second workflow engine.

PR governance and durable handoff state are integrated with bounded daemon scheduling, authoritative restart reconstruction, exact runtime monitoring, scoped handoff delivery, and evidenced outcome completion.

The design follows the useful boundaries in the [OpenAI Symphony specification](https://github.com/openai/symphony/blob/main/SPEC.md): a single scheduling authority, a tracker adapter, deterministic workspaces, an agent runner, and an operator status surface. It deliberately differs in three places required by this product: GitHub owns the whole delivery lifecycle, portfolio policy is coordinator code rather than agent prompt logic, and agents never receive tracker credentials. The upstream Elixir implementation is a prototype; its in-memory blocked state and runtime dependency make it a reference, not the release base.

### Stack and release

- **Go 1.26, pinned in `go.mod` and built with the latest 1.26 security patch.** Goroutines, `net/http`, `os/exec`, and `encoding/json` cover the daemon, process supervision, GitHub CLI transport, local CLI, and dashboard server. The dashboard terminal uses the small `coder/websocket` and `creack/pty` packages for the protocols the standard library does not provide.
- **One `agent-symphony` executable.** The same binary provides `install-host`, `agent-host`, `init`, `validate`, `config view`, `serve`, `status`, `list`, `inspect`, `reconcile`, `doctor`, `diagnostics`, and `pr-governance`. `serve` owns the periodic loop and its loopback-by-default dashboard; production status and one-shot commands refresh from GitHub, while `--attempts` remains an offline diagnostic. Build and release scripts generate the ignored Next.js export and embed it with the xterm.js assets, so released binaries need no Node.js runtime, database, separate browser service, or container.
- **GitHub Releases** publish signed-tag build artifacts and SHA-256 checksums for `darwin/{arm64,amd64}` and `linux/{arm64,amd64}`. WSL2 uses the Linux artifact. Release CI runs the repository lint gate and unit tests, builds with `CGO_ENABLED=0`, smoke-tests each supported OS, and verifies a downloaded artifact against its checksum.
- **Reproducible packaging.** `scripts/release.sh` invokes the repository's Go-stdlib packer with fixed timestamps, sorted checksum output, stripped host paths/build IDs, and `CGO_ENABLED=0`. A second build must be byte-identical before release. Local cross-compilation proves buildability and runtime independence, not execution on another OS; CI records native macOS/Linux and WSL2 runtime evidence.
- **External runtime prerequisites:** Git, tmux, authenticated GitHub CLI, configured coding-agent executables, and repository access. Authentication normally comes from `gh auth login`; `GH_TOKEN` or `GITHUB_TOKEN` is an optional non-interactive alternative. By default no separate provisioning step is required; `agent-symphony install-host` is an optional, one-time, host-administrator-run upgrade to the stronger isolated boundary (see "Credential isolation" below). `doctor` checks versions, the GitHub CLI identity/repository access, and the active host boundary before `serve` accepts work. Package-manager and container distribution are post-MVP.

## System boundaries and ownership

There is one coordinator loop per repository. It refreshes authoritative GitHub state at startup, on each polling interval, and for explicit CLI reconciliation, then makes every transition. Independent repository loops may share a host but not state paths; source bundle, worktree, snapshot, and tmux session names include the repository identity. This preserves upstream Symphony's single-writer property without adopting OTP or a workflow framework.

| Boundary | Owns | Must not own |
| --- | --- | --- |
| GitHub integration | `gh api` reads/writes, authenticated user discovery, normalized issue/PR/check/review models, authorization, and rate-limit handling | Scheduling decisions or agent processes |
| Coordinator/scheduler | Eligibility, priority, explicit dependencies, conservative conflict locks, capacity, claims, retries, cancellation, reconciliation | Durable task truth or GitHub credentials passed to workers |
| Runtime | Worktree and branch lifecycle, tmux session, agent subprocess, timeouts/signals, captured logs, resume feasibility | Issue policy, PR creation, push credentials, or merge decisions |
| Local metadata | Atomic manifest per attempt, daemon lock, and bounded status files; enough to find local resources | Queue, issue body/checklist, policy, or review/check state |
| CLI | Configuration, diagnostics, local control, human and JSON projections | An alternate mutation path around issue, review, or merge policy |
| Dashboard | Loopback-by-default status projection, optional password-protected network binding, exact-session terminal attachment, confirmed cleanup, narrowly constrained attempt recovery, and authenticated exact-message confirmation | Arbitrary GitHub mutation, arbitrary commands/resources, TLS termination, or shared multi-repository state |
| Agent | Edit files, run validation, assess/update documentation, return structured outcome | GitHub API/tooling, credentials, policy decisions, push, PR creation, or merge |

An optional repository orchestrator agent is a long-lived advisory operator console, not another scheduler. Its deterministic tmux session is `as-o-<repository-id>`. The coordinator owns the tmux server while the pane launches through the reviewer identity boundary with only bounded sanitized status. `orchestrator-agent.json` and `orchestrator-context.md` are mode-`0600`, reconstructible local state. A daemon restart adopts an exact live pane; a missing or dead pane starts a fresh generation after bounded backoff. `clear` uses role rules only, while `rebuild` adds the latest sanitized projection. Neither operation restores hidden provider state or changes GitHub facts. The orchestrator's only implementation proposal passes on standard input through the fixed `agent-host orchestrator-proposal` adapter, which validates the strict schema and emits one framed proposal on standard output. The coordinator reads that frame from the exact tmux pane and durably records its consumed digest, so the documented read-only agent sandbox needs no writable path. The proposal names the configured repository, one positive issue/attempt pair, and at most 8192 bytes of UTF-8 text; the adapter grants no tmux, worker command, scheduling, filesystem-write, or GitHub capability.

Repository configuration names exactly one primary implementation agent command and prompt profile. That deterministic agent owns implementation and follow-up turns for the attempt. When issue or repository policy requires independent agent review, the coordinator then runs one separately configured review agent under a different OS identity against the resulting snapshot and validation evidence; it cannot access the implementation worktree, and its findings return to the primary agent for resolution. The two fixed responsibilities satisfy role selection without a general role or plugin framework.

## GitHub is authoritative

### Repository contract

Version-controlled `.agent-symphony.yaml` contains its schema version, repository identity, labels, explicit dependency syntax, completion policy, concurrency, local/offline worktree path, documentation paths, implementation and review commands, optional orchestrator command, environment allowlist, and status preferences. Secrets and mutable status are forbidden. Schema version `1` rejects unknown keys and unsafe paths.

An eligible issue is open, has the configured ready label, has exactly one P1-P3 label, has no conflicting completion label, has no unresolved explicit dependency, and is not represented by an active or completed attempt. The body is unrestricted. If the configured Dependencies section is present, its issue references are enforced; otherwise the issue has no declared dependencies. Controls require current `maintain` or `admin` permission, including when the actor is the authenticated coordinator user.

GitHub stores durable execution facts using machine-readable HTML markers in coordinator-authored issue comments and PR bodies:

```text
<!-- agent-symphony:active-attempt:v1
{"version":1,"issue":8,"attempt":2,"branch":"agent-symphony/owner-repo-<hash>/8-2","base_sha":"<approved-base-sha>"}
-->

<!-- agent-symphony:attempt:v1
{"version":1,"issue":8,"attempt":2,"branch":"agent-symphony/owner-repo-<hash>/8-2","head":"<commit-sha>","pr":31,"outcome":"review"}
-->
```

The marker schemas are strict and size-bounded and are parsed only from the authenticated coordinator user returned by `gh api /user`. Dispatch persists the active marker before local mutation; it binds the approved base and deterministic branch until a matching final PR marker or terminal marker supersedes it. Human-readable text accompanies each marker. Attempt number is the next integer after the highest valid marker for the issue. Branch `agent-symphony/<repo-id>/<issue>-<attempt>`, worktree `<root>/<repo-id>-<issue>-<attempt>`, and tmux session `as-<repo-id>-<issue>-<attempt>` are deterministic. A PR contains `Closes #N` and the final attempt marker. These identifiers make discovery possible without local files.

Issue/PR state, labels, comments, reviews, check runs, branch heads, and repository rules always beat local metadata. A contradiction blocks mutation, emits diagnostics, and requests reconciliation. Local files may never make completed work eligible again.

### Bounded local metadata and reconstruction

Production commands require an explicit `--runtime-state` path. Give each repository daemon a distinct absolute state root. The examples in this repository use paths below `~/.local/state/agent-symphony`, but the program does not select that path implicitly. A root contains:

```text
daemon.lock                  # single-instance advisory lock
status.json                  # mode 0600 latest issue/blocker projection
dashboard-state.json         # mode 0600 hidden archived/abandoned cards only
orchestrator-agent.json      # mode 0600 when the advisory agent is configured
orchestrator-context.md      # mode 0600 bounded advisory context
worktrees/                   # default same-user attempt roots
snapshots/                   # default same-user review snapshots
attempts/<repo-id>/<issue>-<n>/manifest.json
attempts/<repo-id>/<issue>-<n>/agent.log
```

Each manifest contains bounded attempt identity, deterministic resource paths and sessions, implementation/review state, diagnostics, timestamps, and log path. It is written by temp-file plus atomic rename, mode `0600`. Each refresh similarly replaces `status.json` with the timestamped current projection, including blockers, diagnostics, and next actions. There is no configurable retention or automatic log rotation: exact reconciliation and dashboard actions remove only the resources documented for their lifecycle. Deleting the entire state root loses diagnostics and resumable terminal context, but not workflow truth.

`serve` exposes the embedded dashboard and those bounded JSON projections on a configurable loopback address by default. A non-loopback address requires the explicit unsafe-network flag and a password that protects every HTTP and WebSocket route with standard HTTP Basic authentication. Direct HTTP provides no transport encryption or rate limiting. A same-origin WebSocket re-resolves issue/attempt identity, verifies the exact deterministic live tmux session, and attaches it through a PTY; disconnect ends only that tmux client. Same-origin POST actions accept issue/attempt numbers only. Archive requires a completed projection and strict branch/head cleanup. Abandon requires an orphaned projection and removes exact local resources after boundary verification. Recover is the only attempt-dashboard GitHub mutation: it freshly revalidates the latest retryable attempt, may mark a stuck runtime failed, and posts only the fixed retry control. `dashboard-state.json` is presentation state, not a task database, and can hide only archived or abandoned cards.

Startup reconstruction is always:

1. Acquire the per-repository daemon lock; refuse a second instance. Multi-host/high-availability operation is out of MVP.
2. Validate config, dependencies, worker OS identity/isolation, GitHub CLI authentication, effective permissions, repository identity, paths, and the clean primary coordination checkout.
3. Fetch open eligible issues, all open coordinator-marked PRs, recent valid attempt/control snapshots, issue timeline provenance, reviews, checks, statuses, labels, branch heads, and merge/rules state.
4. Scan only deterministic worktree/tmux names and bounded manifests for those attempts. Never adopt an unmarked branch, directory, process, or PR.
5. Build the in-memory projection. If a GitHub-active attempt has a live matching tmux/worktree and manifest/head agreement, resume monitoring. Otherwise mark it orphaned/blocked in GitHub and either repair the same attempt when provably safe or create a new numbered attempt. Never attach to an unknown process.
6. Run the same authoritative reconciliation immediately at startup and at an interval no greater than 60 seconds, with a two-minute deadline around the entire cycle. Only strict v1 attempt markers present on both the coordinator-created PR and a coordinator-authored issue snapshot, with matching actor, branch, issue, and attempt identities, are authoritative. Only a merged PR is completed; a separately closed issue/PR is blocked.
7. Monitor exact live active and review-ready runtimes. Claim feedback and validation atomically only for verified owners, pass an immutable key and coordinator-owned outcome path to the runtime, and persist explicit evidenced outcomes before acknowledging/removing the outcome record. In-flight work is never pasted again by a later cycle.
8. Reconcile terminal/closed work before dispatch, then evaluate the backlog. Target: complete within two minutes under normal API availability; timeout diagnostics identify the next bounded backoff cycle.

## Polling, authentication, and authorization

### Polling and idempotency

The coordinator computes desired state from current GitHub facts at startup, at every interval up to 60 seconds, and when `reconcile` is invoked. Repeated reads, restarts, or changes between polls cannot create a second attempt, branch, worktree, session, PR, feedback turn, or merge because side effects use stable identities and fresh preconditions:

- dispatch first searches markers, branches, PRs, manifests, worktrees, and sessions for the issue/attempt;
- a review comment turn is keyed by immutable comment/review ID and recorded before any feedback side effect;
- policy publication updates the commit status for the current head and fixed context;
- PR creation searches by exact head branch before creating;
- merge supplies the freshly observed head SHA.

### GitHub CLI and actor authorization

The coordinator invokes `gh api` for every GitHub API request and `gh auth git-credential` for publication. It discovers the coordinator's stable user ID with `/user` and verifies the configured repository through the same authenticated session. Setup fails if the CLI is unavailable, unauthenticated, or lacks repository access.

A control-changing event is accepted when a fresh GitHub permission query returns repository `maintain` or `admin`; this includes the authenticated coordinator user. Review feedback may also use `write`. The coordinator rechecks authorization at action time. Edits that remove readiness, close work, change review policy, cancel, retry, or authorize autonomous merge follow this rule. Exact coordinator artifact schemas are filtered from human input; ordinary comments from the same user remain eligible feedback.

Authorization must also survive restart without trusting daemon memory. Control metadata is a canonical, sorted representation of readiness, priority, optional dependencies, completion/review policy, cancellation/retry intent, and the arbitrary issue body revision. A body revision is the SHA-256 of the exact body plus its latest immutable body-edit timeline event ID. If the body has never been edited, its anchor is the immutable tuple `(issue node ID, created_at, author ID)`. Current authorized `agent-ready` provenance at or after that boundary authorizes dispatch. Autonomous controls additionally require current authorized `autonomous-merge` provenance at or after the boundary.

After accepting controls, the coordinator writes a snapshot comment binding the normalized-control SHA-256 hash, body hash and edit-event/creation anchor, and immutable timeline event IDs/actor IDs for non-body controls. New snapshots leave the legacy approval fields empty. The snapshot contains no independent policy value, and every provenance actor is freshly authorized. Existing approval-bound snapshots remain readable across upgrades, but new intake uses the ready-label boundary; autonomous authorization must also retain the exact qualifying autonomous label.

On startup and before dispatch, rework, cancellation, or merge, the coordinator rebuilds the current controls/body hash, finds the latest body-edit event or creation anchor, reconstructs non-body provenance from the GitHub timeline, freshly authorizes every provenance actor, and compares all fields with the latest valid coordinator-authored snapshot. Only an exact match restores authorization. A later body edit changes the hash/anchor and blocks dispatch until `agent-ready` is reapplied and a new snapshot is written; autonomous merge also requires a current `autonomous-merge` event after that edit. Missing timeline events, conflicting edits, an anchor/hash mismatch, an unauthorized actor, or inability to attribute every current value blocks mutation; local files cannot fill the gap.

### Credential isolation

Two boundary implementations select automatically, based solely on whether `install-host` has ever provisioned the advanced identities described below — there is no separate configuration flag.

By default, the implementation/review boundary runs as the same OS user as the coordinator: no separate account, no `install-host` step. This preserves credential-environment isolation but not OS-identity isolation. The boundary still assembles an explicit environment allowlist, strips GitHub/SSH/cloud/proxy credentials, disables the Git credential helper, and removes remotes from attempt worktrees. It provisions private (mode `0700`) worktree/snapshot roots under the coordinator's own runtime state directory. What it does not provide is OS-enforced separation between the coordinator and the agent process: a compromised or malicious agent has the same filesystem/process access as the coordinator, including GitHub CLI credential storage. See `docs/security.md` for the full statement of that tradeoff.

The remainder of this section describes the advanced host-isolated mode, which closes that gap. The coordinator, implementation agent, and reviewer run as three distinct OS identities with exactly two sharing groups. `agent-symphony-attempt` is the worker's primary group and contains the coordinator only as a supplementary member. `agent-symphony-snapshot` is the reviewer's primary group and contains the coordinator only as a supplementary member. Worker and reviewer are excluded from each other's group. Both agent accounts have no login password, other supplementary groups, sudo rights, ACL, socket access, or access to coordinator secrets/state.

The attempt root is owned `agent-symphony-worker:agent-symphony-attempt` mode setgid `2770`; each isolated attempt repository/worktree directory is also setgid `2770`, and files are created with umask `0007` so group inheritance and coordinator access are deterministic. The reviewer is excluded. Because every attempt uses the same fixed worker UID, worker processes can access all attempt roots: OS permissions are not a security boundary between attempts.

The threat model treats all fixed-UID worker processes as mutually trusted for repository integrity, but untrusted for coordinator secrets and GitHub authority. Separate repositories/worktrees, disjoint-scope scheduling, and coordinator export validation contain accidental mistakes; they do not contain a hostile sibling worker. Attempts contain no credential, credential helper, or credentialed remote, and the coordinator seeds them through a local bundle/archive without exposing its authenticated checkout. Hostile worker-to-worker containment requires per-attempt identities or a proven sandbox and is deferred post-MVP.

The snapshot root is owned `coordinator:agent-symphony-snapshot` mode `0750`; the reviewer is in the snapshot group and the worker is excluded. Each completed snapshot has directories mode `0550` and files mode `0440`, so the reviewer can read but not mutate it. A head-bound mode-`0770` result directory beside the snapshot is the reviewer's only output channel; the shared capture helper exclusively creates its private result file. The review boundary opens that exact regular, non-symlink artifact without following links, limits it to 64 KiB, and the coordinator strictly decodes the complete JSON object; tmux transcript text is never treated as a result. Secrets, manifests, logs, and the daemon lock remain in a separate coordinator-owned mode-`0700` tree shared with neither group. Unsupported hosts that cannot enforce these boundaries fail `doctor` and cannot serve.

This advanced mode is optional. A host administrator who wants OS-enforced isolation installs the release at root-owned mode-`0755` `/usr/local/libexec/agent-symphony/<version>/agent-symphony`, then runs that exact binary once as `install-host --coordinator <user>`. It creates fixed accounts `agent-symphony-worker` and `agent-symphony-reviewer`, groups `agent-symphony-attempt` and `agent-symphony-snapshot`, and the attempt/snapshot roots with the ownership and modes above at `/var/lib/agent-symphony/{attempts,snapshots}` on Linux/WSL or `/var/db/agent-symphony/{attempts,snapshots}` on macOS. Linux/WSL uses `groupadd --system`, `useradd --system --gid` to assign each agent its stated primary group plus matching private home and `/usr/sbin/nologin`, and `usermod --append --groups agent-symphony-attempt,agent-symphony-snapshot <coordinator>` for the coordinator's two supplementary memberships. macOS uses `dscl` to create hidden local users with allocated unique IDs, the matching `PrimaryGroupID`, private homes, and `/usr/bin/false`, and `dseditgroup` to add only the coordinator to both groups.

The root-owned `/etc/sudoers.d/agent-symphony` names the versioned binary path, coordinator user, exact target user and group, and exactly three passwordless invocations: implementation as the worker identity, and review or orchestrator as the reviewer identity. Each invokes the same versioned binary and one exact `agent-host` mode with no additional arguments; launch requests arrive on standard input. No shell, arbitrary user/group, command, wildcard path/argument, account-management, or coordinator-as-root rule is granted. The administrator reruns the new version's `install-host` after every binary upgrade so it atomically validates accounts/groups/roots, rewrites the exact path/rules, removes the superseded managed rules, and refuses conflicting identities or broader unmanaged grants.

Environment filtering is defense in depth, not the security boundary. The three `agent-host` modes create agent/tmux processes with explicit allowlists rather than inherited environments. They remove GitHub/SSH/cloud tokens, credential-helper variables, proxy credentials, and key paths; disable Git credential helpers; supply no GitHub MCP/tool; and redact known secret values and credential-shaped fields from output. Each tmux server runs under its assigned agent identity, never the coordinator identity.

Only the coordinator performs authenticated fetch, push, GitHub mutation, and merge. Authenticated Git commands use `gh auth git-credential` in the coordinator process, backed by either the stored CLI login or an optional coordinator environment token. Credentials never enter command arguments, committed Git config, worktrees, tmux history, or agent logs. The agents may use their separately configured model credentials only; those credentials grant no repository authority. OS permissions are the MVP isolation boundary—containers are not claimed as a security boundary.

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

The runtime writes a credential-free source bundle whose name includes the repository identity inside the provisioned attempt root, then the worker boundary creates its isolated repository/worktree there and invokes the exact implementation launch as `agent-symphony-worker`; that mode starts the named tmux session and deterministic primary command in the worktree. The prompt contains the issue's arbitrary title/body, repository guidance, attempt identity, prior authorized feedback, allowed actions, and a required structured result. Independent review checks out the exact attested worker object in an immutable snapshot and tmux session whose names include the repository identity, persists its head/session state across reconciliation cycles, and requires a bounded structured clean/findings result. Findings enter the existing durable implementation handoff/rework lifecycle; only clean review permits publication. Process control uses process groups: graceful interrupt, bounded wait, then targeted kill of only that attempt. Attempts are retained for open PRs/rework. After fresh GitHub facts report an exact published attempt as merged, the implementation boundary verifies its deterministic runtime identity, kills only its named tmux session even when a queued or claimed follow-up has made it live again, and removes its clone and worker result; the recovery manifest and diagnostic log remain retained.

The trusted worker boundary exports a bounded Git bundle plus immutable branch/head/base, clean-tree, result, and bundle-digest attestation; it never publishes. The coordinator imports into a temporary bare repository, rejects oversized objects, symlinks, result markers, invalid ancestry, or a changed head, and only then imports the verified head for push. Publication is reconstructed before each mutation from the deterministic coordinator-owned branch/head, PR body marker, and issue comment marker, so ambiguous responses and crashes resume the missing phase instead of creating another PR.

Feedback and validation identity includes immutable feedback source/ID values and a validation generation. Confirmed operator messages use the same boundary: an authenticated dashboard POST binds the exact proposal digest, and the coordinator requires either a `RecoverChecked`-verified live runtime or a `MatchesPublishedAttempt`-verified retained runtime before writing a strict coordinator-authored GitHub message marker. Base and published head SHAs, deterministic branch/worktree/session identity, and any competing active binding participate in that proof. Reconciliation waits while the pane is live, rejects stale/cancelled/completed/merged targets, and starts at most one safe follow-up turn per message. Before execution it writes a durable GitHub delivery claim, then rereads authoritative issue, attempt, and local-runtime state at the worker acceptance boundary; terminal reconciliation wins by recording a rejection without starting the worker. A restart resumes a claimed message with its stable key. The worker boundary atomically accepts each identity, persists a binding that includes the original implementation command, starts the replacement worker, records the exact launch identity in tmux, and then acknowledges it. Recovery preserves that binding and refuses command drift instead of launching the durable message again. Worker-writable inbox, launch, and receipt files are not publication authority; only the coordinator-authored delivered-message state permits an operator follow-up to publish. Stable content-derived keys make duplicate confirmation and restart reconciliation idempotent. Dashboard status projects internal claims as `queued` and exposes only message IDs, operator-facing outcomes, timestamps, and bounded content-free diagnostics. Terminal failures use a strict coordinator-authored issue marker, which dispatch reconciliation reads as authoritative suppression until an authorized retry creates a later attempt. Every boundary adapter launch receives only the minimal platform environment (`PATH`, temporary-directory location, and Windows system root when present), never inherited coordinator credentials.

## Review, rate limits, and merge safety

Open coordinator-authored PRs are reconciled for their entire lifetime, including after local cleanup. Authorized actionable review comments are ordered by immutable GitHub timestamp/ID, deduplicated by ID, attached to the existing attempt, and run in the existing safe worktree when available. If the worktree cannot be proven to match the PR head, it is recreated from that branch. Before a feedback turn, the coordinator refetches the comment, authorization, PR state, and head SHA. Addressed/blocked disposition and new validation evidence are written to GitHub.

API calls honor `X-RateLimit-Remaining`, `X-RateLimit-Reset`, `Retry-After`, and GitHub secondary-limit responses. Reads use conditional requests where supported. Transient reads retry with exponential backoff, jitter, and a cap; mutations are not blindly retried after ambiguous responses and instead reconcile their stable identity. Near exhaustion pauses dispatch and nonessential status refresh, reserving calls for active cancellation/security and merge checks. Outage/backoff state is visible in CLI status.

Human review is the default. The coordinator publishes the `agent-symphony/policy` commit status for the current head SHA, but repository rules do not need to require it. It fails or remains pending while human review, authorized feedback, validation evidence, or documentation assessment is unresolved.

Immediately before any autonomous merge, the coordinator refetches and requires all of the following for the same head SHA: issue is open/eligible and permits autonomous merge; actor/policy changes remain authorized; PR is open, non-draft, unmodified except through the attempt branch, mergeable, and not behind when rules require current base; no conflicting active path scope exists; all repository-required reviews are present with no current change request; all repository-required checks and the coordinator policy status succeed; the authenticated account can merge; and no unresolved authorized feedback remains. It then calls GitHub's merge endpoint with the expected head SHA and configured allowed method, where GitHub enforces any branch protection or repository rules that exist. A mismatch or ambiguous result returns to reconciliation. The system never force-pushes, overrides rules, admin-merges, dismisses reviews, or writes directly to the integration branch.

## Platform behavior

The executable uses Go filesystem/process APIs and invokes the same Git and tmux commands on every platform. The shipped `agent-host` adapter implements the existing bounded command/result JSON seam; the coordinator selects its exact installed sudo tuple when no test seam is injected. Paths are canonicalized, required to remain below their provisioned attempt/snapshot roots, and passed as argument arrays—never shell-concatenated. Case-collision and path-length failures are detected before dispatch; the local bundle/export boundary does not require coordinator and worker repositories to share a filesystem.

- **Linux:** native binary, advisory file lock, and POSIX signals; `install-host` uses `groupadd`/`useradd`/`usermod`, `/var/lib` runtime roots, and the exact versioned-binary sudoers rules above.
- **macOS:** native signed/notarized binary when release infrastructure is available; application-support state path; `install-host` uses `dscl`/`dseditgroup`, `/var/db` runtime roots, and the exact versioned-binary sudoers rules above; BSD userland assumptions are avoided.
- **Windows:** WSL2 only. `install-host`, the three identities, sudo, daemon, Git repository, worktrees, tmux, agents, and state must all live inside the same WSL distribution on its Linux filesystem. `/mnt/c` worktrees and cross-boundary Windows accounts/processes are rejected because permissions, signals, locks, and filesystem performance are not reliable enough for the MVP.

`doctor` proves executable discovery, minimum versions, tmux session creation/removal, isolated local Git creation/export, state-root permissions/atomic rename, signal handling, repository remote/identity, GitHub CLI authentication, and effective repository access. It verifies the installed binary version/path/owner/mode, platform-specific account creation semantics, worker/reviewer primary groups, coordinator supplementary memberships, setgid `2770` attempt root/directories and umask `0007`, `0750` snapshot root and completed `0550`/`0440` snapshots, managed sudoers content, and all three exact user/group/command tuples. Denial checks cover swapped users or groups, shells, extra arguments, arbitrary binaries, and coordinator root. Canaries confirm that the fixed worker can traverse multiple attempt directories, while the reviewer cannot traverse the attempt root, the worker cannot traverse the snapshot root, neither agent can reach coordinator `0700` state or secret paths, snapshots are read-only to the reviewer, and attempt repositories have no credentialed remote/helper. Unsupported native Windows, non-WSL environments, stale-version installation, shared identities, broader sudo rules, and ineffective permissions fail with corrective guidance. CLI output honors `NO_COLOR`, never uses color alone, and JSON output has a versioned envelope.

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
| Config/CLI (FR51-58) | CLI/config | FR51 `init`; FR52 repository policy config; FR53 boundary/prerequisite validation; FR54 serve/stop/inspect; FR55 human diagnostics; FR56 versioned JSON; FR57 live GitHub CLI identity/connectivity check; FR58 platform/dependency/isolation guidance, proven by `install-host`, `doctor`, sudo allow/deny, and release smoke tests |
| Dashboard (FR59-64) | Loopback-by-default Go server + embedded Next.js page | Embedded export/build, bounded file serving, network opt-in/password authentication, exact tmux WebSocket input/resize/disconnect, archive/abandon cleanup, constrained recovery, cross-origin/wrong-state/identity-drift rejection, desktop/mobile accessibility and console checks |

Required checks include GitHub CLI transport/authentication failures; repeated polling and restart recovery; initial-body creation anchor, latest body-edit event, unrelated issue activity, ready-label and autonomous-label authorization boundaries, later body edits, and authorized/unauthorized label actors; restart with matching, missing, conflicting, and mismatched anchor/hash/control snapshots; legacy approval-bound snapshot recovery; platform-specific install/upgrade and identity permissions; uncredentialed local repos; malicious patch/tree exports; checklist-result verification and issue decision recording; malformed markers/config/paths/API responses; dependency cycles; overlapping/unknown scopes; rate exhaustion and ambiguous mutations; process crash/cancel; stale/force-pushed head; revoked permission; required-status outcomes; and redaction. Logs are inspected for known canary secrets.

### Required scenario walkthroughs

1. **Dispatch:** after `doctor` proves the GitHub CLI identity, repository access, and active host boundary, a ready P1 body hash/latest-edit-or-creation anchor has current authorized `agent-ready` provenance plus a matching coordinator snapshot. Reconciliation finds no attempt resources; the coordinator writes attempt 1, seeds the uncredentialed local repository, and launches the implementation agent. It validates the exported patch/tree before publishing from its own checkout. A concurrent independent issue may start within capacity; unknown overlap serializes.
2. **Delayed review feedback:** an open marked PR remains discoverable with no daemon memory. A later poll finds a new comment; fresh actor authorization and immutable comment ID pass. The coordinator records that ID before feedback side effects, reconstructs the matching worktree at current head, runs rework and review, validates, pushes, and updates the same PR and policy status. Later polls find the recorded ID and do nothing.
3. **Failed PR policy:** verified worker evidence missing from GitHub is republished idempotently. If checks, repository rules, or permissions still prevent the unchanged head from proceeding, a canonical coordinator-authored PR comment records the blockers; its exact head/reason body prevents duplicate comments across polls and restarts.
4. **Restart:** after lock acquisition, GitHub markers, control snapshots/timeline provenance, and open PRs are fetched before local resources. Current normalized controls, body hash, and latest edit-event/creation anchor must exactly match a coordinator snapshot bound to the current authorized ready-label boundary and, when enabled, the autonomous-label boundary. Unrelated issue activity does not invalidate it, but any body or required-label mismatch blocks until reauthorization. A matching live session is monitored; a missing session is marked orphaned and safely recreated or blocked. Active/completed markers prevent redispatch. No queue database or remembered authorization is restored.
5. **Repeated polling:** repeated reads of unchanged GitHub facts use stable attempt/comment/status/branch/PR identities and fresh preconditions, so they permit at most one side effect.
6. **Protected merge:** after successful validation, the coordinator refreshes policy, authorization, head, reviews, checks, mergeability, and repository rules. Human-review still present leaves the policy check pending. When policy permits and every gate passes, merge uses the expected SHA; protection failure or head movement blocks and reconciles—never overrides.
7. **No credentials to agents:** after current-version admin `install-host`, `doctor` checks setgid `2770` attempt ownership/inheritance, umask `0007`, exact sudo user/group/command permissions/denials, and the implementation, review, and orchestrator boundary paths. Coordinator secrets and state are unreachable. The fixed worker can access all attempt repositories but no snapshot; the reviewer can access no attempt and can only read coordinator-owned completed `0550`/`0440` snapshots. An attempted `git push` from either agent fails authentication; separate repositories, disjoint-scope scheduling, and coordinator export validation contain accidental worker mistakes, not a hostile sibling.
8. **Self-contained executable:** on clean macOS, Linux, and WSL2 hosts with only declared external tools, verify checksum, use the same binary for the one-time admin `install-host`, run `agent-symphony doctor`, start/status/stop against the harness, and confirm no Go runtime, helper package, or shared library is required.

## Adversarial review resolutions

- **GitHub has no atomic issue claim:** MVP permits exactly one locked coordinator instance. Deterministic markers/resources plus reconcile-before-effect prevent duplicates after crash; HA is explicitly unsupported.
- **A local manifest can become a shadow database:** its schema excludes policy and queue state, is disposable, and always loses conflicts to GitHub.
- **A commit status does not enforce itself:** the coordinator reevaluates its policy immediately before merging with the expected head; configuring the status as a repository-required check is optional.
- **Issue body edits could silently change execution controls:** the coordinator snapshot binds body/control hashes and the latest body-edit event or initial creation anchor. Dispatch requires a current authorized ready-label event at or after that boundary; autonomous merge additionally requires a current authorized autonomous-label event, so unrelated activity is harmless but a later body edit blocks.
- **A restart could forget who authorized current controls:** current normalized metadata/revision must match a coordinator-authored snapshot bound to current label provenance; ambiguity blocks.
- **Implementation and review agents could share authority or read daemon secrets:** versioned `install-host` creates separate identities and disjoint sharing groups with three exact sudo launch rules; ownership, export validation, denial, and boundary canary tests enforce separation while environment filtering remains defense in depth.
- **Resuming the wrong process/worktree corrupts work:** resume requires agreement among GitHub marker/head, manifest, deterministic path/session, and Git worktree state; uncertainty blocks or recreates.
- **Concurrent issues can overlap despite optimistic descriptions:** MVP requires declared disjoint path scopes and serializes uncertainty; semantic inference is deferred.
- **Rate-limit retry can repeat a mutation:** ambiguous mutations reconcile stable identities instead of blind retry.
- **WSL mixed filesystems/processes violate Unix assumptions:** the MVP requires one WSL2 distribution and Linux filesystem paths.

## Deferred decisions

Centralized multi-repository operation, HA/multiple coordinators, remote workers, inferred conflicts/dependencies, native Windows, first-class TLS termination or identity-aware dashboard authentication, package managers, containers, specialist role frameworks, a general workflow/plugin system, and hostile worker-to-worker containment through per-attempt identities or sandboxing are post-MVP. Add them only when a separate issue supplies evidence and acceptance criteria.
