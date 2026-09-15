# Security

The [architecture](architecture.md#system-boundaries-and-ownership) is the source of truth for ownership and worker authority. The central rule is simple: the daemon owns runtime state and GitHub effects; implementation and review workers do not.

## Rootless worker boundary

Agent Symphony runs as an ordinary user on macOS, Linux, and WSL2. `serve` never selects sudo, root, provisioned accounts, or an installed host helper. `install-host` remains only as a legacy diagnostic command and is not part of production setup.

Implementation and review use one generated, strict Codex permission profile. The profile denies the host filesystem by default, permits minimal runtime reads, and grants intended writes only within one unique disposable workspace. Worker commands and descendants have no network access. User/project Codex configuration and side-effect surfaces such as plugins, apps, connectors, browser/computer tools, hooks, web search, and multi-agent features are disabled. Arbitrary custom worker commands are rejected because Agent Symphony cannot prove that they enforce the same boundary.

Workers use a daemon-managed `CODEX_HOME` that exposes only the authentication material needed by the trusted Codex client for model traffic. They do not receive GitHub tokens or configuration, SSH-agent sockets, Git credential helpers, dashboard credentials, coordinator state paths, or ambient cloud/proxy credentials. Their clones have no remote. The worker may write its attempt-local `.git` data, but cannot read or modify the coordinator repository, another attempt, a review snapshot, or owner staging.

Agent Symphony places each worker's status-request mailbox, result, temporary, and cache paths inside the disposable workspace. It keeps authoritative state, credentials, owner commands, manifests, seals, and the daemon control socket under the private state root, never in shared temporary directories. Startup and `doctor` verify the installed Codex version and effective profile. Canaries exercise filesystem, TCP, Unix-socket, configuration, credential, and detached-child denial. An unsupported or ineffective authority boundary fails closed; there is no full-access fallback.

Residual macOS risk: Codex 0.153.x sandboxed commands can read and write unrelated same-user files in shared `/tmp` and `/private/tmp` despite explicit deny entries. Agent Symphony reports this limitation and does not claim arbitrary shared-temp isolation. Because no Agent Symphony authority path is stored there and all accepted inputs are owner- and generation-bound, that access cannot mutate accepted state. Do not store unrelated same-user secrets in shared temporary directories while workers run.

## Revocation, status, and publication

Every worker workspace and launch has an attempt generation, launch ID, and permission-profile digest. Workspaces are never reused. A destructive command commits its tombstone and generation change before slow cleanup, so old results become stale immediately. A detached child may continue changing its quarantined workspace, but it has no owner, GitHub, network, sibling, or control-socket authority and its files are never consumed again.

Reviewer proofs persist a confinement-policy version separately from the executable profile digest. The policy version must be bumped whenever the sandbox guarantees used for cleanup authorization change, including network, credential, owner-state, or snapshot isolation. A known policy version permits cleanup across ordinary Codex binary upgrades; a missing or unknown version remains quarantined.

Worker status travels through a private mailbox containing the exact generation, launch ID, increasing sequence, profile digest, status, and reason. The owner validates it and performs any resulting GitHub operation. A worker cannot call GitHub directly or mutate `runtime-state.json`.

Completion is copied into an owner-private immutable seal bound to repository, issue, attempt, generation, base/head, bundle digest, and profile digest. Review and publication use only that sealed object. A stale seal, mutable worker path, changed generation, or invalidated effect cannot publish.

Owner-issued GitHub effects are serialized per attempt and generation-bound. Cancellation cannot retract a request GitHub already accepted. Reversible or discoverable late effects are durably converged; an already accepted exact-head merge remains authoritative on GitHub while the local tombstone prevents runtime resurrection. See [Architecture](architecture.md#credential-isolation) for the complete effect policy.

## Legacy records

Pane disappearance and process-group absence do not prove that an old unconfined descendant is dead. Legacy records without positive evidence are quarantined per issue, survive restart, block publication and resource reuse, and appear as physical cleanup pending rather than as an active attempt. A verified later boot may release process quarantine because an old process cannot survive reboot; missing boot identity remains fail closed.

## Daemon and dashboard boundary

The daemon's authenticated GitHub CLI session is the workflow publication boundary. Implementation/review workers never receive it. Runtime state is private to the coordinator. `deployment.json` binds one runtime root to one repository; `runtime-state.json` is the authoritative bounded ledger and `status.json` is only a revision-tagged projection. Legacy dashboard, removal, and receipt files are migration inputs, not production write targets.

The running daemon exposes a mode-`0600` Unix control socket inside the private runtime-state root. The CLI rejects symlinked, foreign-owned, non-socket, or broadly accessible paths. Requests contain only a bounded request identity, repository, fixed action, numeric attempt identity, and confirmation bit. Dashboard and CLI mutations use the same concrete owner command and durable receipt; neither holds owner access during slow I/O.

The dashboard binds to loopback by default. Non-loopback use requires `--allow-unsafe-dashboard-network` and a private password file. HTTP Basic authentication does not encrypt credentials or terminal traffic, so use a trusted network plus host firewall or an encrypted tunnel. Terminal and lifecycle routes re-resolve repository, issue, attempt, generation, role, and deterministic session identity. The browser cannot choose arbitrary paths, sessions, commands, or GitHub policy.

## Advisory orchestrator

The optional orchestrator is advisory, not an implementation/review worker or state owner. It receives bounded projections rather than raw issue, review, or chat bodies. It runs as the coordinator user without sudo; its configured Codex access therefore requires a trusted model. Its fixed proposals carry no arbitrary command or operator message and must pass fresh owner validation. Worker isolation must never rely on the orchestrator's trust model.

## Operational hygiene

Before release, verify worker denials, retained logs, configuration, Git history, and artifacts contain no credential. Pattern scans are supporting evidence, not proof that an unknown secret format was absent. Rotate any credential suspected of exposure and preserve only redacted incident evidence. Root can bypass local permissions, so the threat model does not claim protection from a privileged local process.

Fresh GitHub permission checks authorize actors. `agent-ready` and `autonomous-merge` must follow the latest body edit and be applied by a current maintainer or administrator. The coordinator re-reads permission, head, reviews, checks, mergeability, and repository rules before merge and never uses an administrative bypass.
