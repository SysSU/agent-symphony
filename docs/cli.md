# CLI reference

The CLI provides configuration, diagnostics, production reconciliation, and restart recovery.

## Commands

```text
agent-symphony help
agent-symphony --help
agent-symphony -h
agent-symphony --version
agent-symphony install-host [--json]
agent-symphony agent-host implementation|review|orchestrator|orchestrator-proposal|orchestrator-proposal-status
agent-symphony init [--config path] [--json]
agent-symphony validate [--config path] [--json]
agent-symphony config view [--config path] [--json]
agent-symphony doctor [--config path] [--runtime-state path] [--offline] [--json]
agent-symphony diagnostics [--config path] [--runtime-state path] [--offline] [--json]
agent-symphony pr-governance --state path [--config path] [--json]
agent-symphony serve --state path --runtime-state path [--interval duration | --disable-periodic-reconciliation] [--dashboard-address address] [--dashboard-project URL ...] [--allow-unsafe-dashboard-network --dashboard-password-file path] [--config path]
agent-symphony chat --repository owner/repository --role implementation|reviewer --issue number --attempt number --runtime-state path
agent-symphony chat --repository owner/repository --role orchestrator --runtime-state path
agent-symphony control --repository owner/repository --runtime-state path --action reconcile [--request-id id] [--timeout duration] [--json]
agent-symphony control --repository owner/repository --runtime-state path --action recover|review-plan|dismiss|orchestrator-investigate --issue number --attempt number [--request-id id] [--timeout duration] [--json]
agent-symphony control --repository owner/repository --runtime-state path --action archive|abandon|remove --issue number --attempt number --confirm [--request-id id] [--timeout duration] [--json]
agent-symphony control --repository owner/repository --runtime-state path --action orchestrator-recover|orchestrator-clear|orchestrator-rebuild [--request-id id] [--timeout duration] [--json]
agent-symphony status (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
agent-symphony list (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
agent-symphony inspect --issue number (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
agent-symphony reconcile (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
```

- `help`, `--help`, and `-h` print the command summary. `--version` prints the release version.
- `install-host` is a non-mutating migration diagnostic. It reports whether obsolete Agent Symphony host identities exist, but never creates, modifies, or uses accounts, groups, files, or sudo rules. Run Agent Symphony as the ordinary user who owns its private runtime-state root.
- `agent-host` is the internal boundary adapter for implementation, review, and orchestrator processes. `orchestrator-proposal` accepts only the bounded proposal JSON on standard input, durably writes the canonical object to its fixed protected artifact, and reports `submitted` with the exact binding. `orchestrator-proposal-status` accepts that same exact JSON and reports `pending`, `running`, `succeeded`, `failed`, `refused`, `consumed`, `replaced`, or `unknown` for its coordinator-observed binding. These are not interactive operator commands.
- `init` creates a new config with conservative defaults and refuses to overwrite a file. It requires a GitHub `origin` in the current repository.
- `validate` requires the config file to be inside the resolved Git root. It rejects malformed input, duplicate JSON keys at any nesting depth, unknown keys, secret-shaped keys or command arguments, invalid policy values, duplicate label names, empty required labels, whitespace-only issue filters, unsafe command arguments, and paths that are absolute, traverse outside the repository, target Git metadata, or escape through symlinks. Worktree and documentation paths are always anchored at the Git root, not the config file's directory.
- `config view` prints the validated configuration. Invalid or secret-bearing files are never echoed.
- `doctor` and its `diagnostics` alias check the supported platform, WSL filesystem placement, Git, tmux, configured agent commands, Git repository/remote identity, GitHub CLI authentication, and effective repository access. They bind the exact shared implementation/reviewer Codex executable and run the real managed sandbox canary without model or GitHub traffic. `--runtime-state` selects the private persistent state root to check; shared temporary roots and symlink escapes fail. `--offline` skips only the GitHub probe and emits an explicit warning.
- `pr-governance` is a one-shot pull-request governance command. It creates an empty private recovery-state JSON file when the named file is absent, then durably writes feedback and validation handoffs. Recovery claims those handoffs before they cross into the isolated runtime. All GitHub reads and writes use the authenticated `gh` session.
- `chat` attaches directly to the exact current tmux session selected by `--role`. Implementation and reviewer roles require the deployment repository, issue, and attempt; the orchestrator role requires only the deployment repository. The command re-reads the repository-bound projection or asks the running daemon for its server-derived orchestrator target, verifies that tmux still owns the session, and leaves it running when the operator detaches with `Ctrl-b d`. Interactive terminal input is immediate and is not a durable issue instruction; add an authorized issue or pull-request comment when the instruction must enter a later handoff.
- `control` asks the already-running daemon to run one fixed action. Attempt/operator mutations use owner-backed receipts and typed effects: the owner durably binds the request ID and exact input before external work, exact replay returns the completed result, conflicting reuse is refused, and a pending result resumes only from exact proof. `reconcile` is a direct v2 advisory request that triggers one coalesced cycle and waits for its completion; orchestrator lifecycle actions use their concrete supervisor boundary. None competes for `daemon.lock` or writes a legacy receipt file. Archive, abandon, and removal require `--confirm`. A successful command proves only that the requested action completed; re-read status or GitHub before claiming a downstream workflow change.
- `serve --state path --runtime-state path` authenticates GitHub, acquires the non-following single-instance lock, installs the repository-bound v2 deployment fence, loads or migrates the owner ledger, sweeps pending effects, and publishes an initial owner-revision status before admitting requests. It then starts the first GitHub collection asynchronously and polls on the configured cadence; blocked GitHub or process inspection does not block owner-backed controls. Each cycle uses one read snapshot and logs bounded metrics and failure phase. `--interval` overrides the configured cadence. `--disable-periodic-reconciliation` disables only the periodic ticker: startup collection, dashboard Check Now, control requests, and event-triggered cycles still run. This mode is for controlled diagnostics and end-to-end tests, not normal deployments; it cannot be combined with an explicit `--interval`. The daemon serves its private control socket and dashboard at `--dashboard-address` (default `127.0.0.1:8080`). Repeat `--dashboard-project URL` for read-only peer status. Non-loopback serving requires `--allow-unsafe-dashboard-network` and a private `--dashboard-password-file`; HTTP Basic authentication then protects every route but does not encrypt traffic. Use `control --action reconcile` or the `reconcile` command to request a waitable cycle from the running daemon; both fail when no daemon owns the state root. `status`, `list`, and `inspect` read the owner ledger, while `--attempts path` remains a nonmutating offline diagnostic. Independent repository daemons must use distinct state paths and dashboard addresses.

Unsafe network mode serves plain HTTP: the password and terminal traffic are not encrypted, and anyone with the password can use the dashboard's terminal, recovery, and cleanup controls. Use it only on a trusted network with host-level firewall rules, or carry it over an encrypted VPN or tunnel.

## Issue eligibility and recorded blockers

Implementation issue bodies must satisfy the [implementation issue contract](github-controls.md#implementation-issue-contract). The configured dependency section is required; referenced open issues block dispatch. An optional `## Paths` section declares one repository-relative file or directory per list line for concurrent scheduling. Missing or invalid path scope does not make an issue ineligible, but it serializes that issue against other active work in the same repository because disjointness cannot be proven.

Dispatch requires an open, non-cancelled issue with `agent-ready` applied after the latest body edit, exactly one configured P1-P3 label, the optional `labels.issue_filter` label when configured, and no conflicting completion labels. Every actor changing those controls must currently have repository `maintain` or `admin` permission; the authenticated coordinator account is allowed. `autonomous-merge` is the explicit opt-in for coordinator-managed merge and must also follow the latest body edit. Without it, Agent Symphony creates the pull request but never merges it. `needs-human-review` remains available as an optional explicit PR label and pending policy Check; it is not required for the default non-autonomous path. An existing active/completed attempt, contradictory markers, unresolved dependencies, terminal failure without an authorized retry, or exhausted concurrency also prevents dispatch. Coordinator marker syntax is reserved and exact coordinator artifacts are not treated as human feedback.

Every successful refresh calculates a **status projection** from an immutable owner snapshot. It rejects a status belonging to any repository other than the bound deployment project, then atomically writes `<runtime-state>/status.json` with mode `0600` and the source ledger revision. The file includes a timestamp, issue state, current phase, blockers, diagnostics, next action, and a bounded list of attempt-owned sessions. Each session records its role, deterministic name, lifecycle state, current marker, and available timestamps. Reviewer entries also record `plan-review` or `implementation-review` mode and the exact target. A failed daemon refresh commits a cycle-bound redacted error through the owner; the projection retains the last successful statuses and displays that error. A newer successful cycle clears it. The same status fields appear in `status` and `inspect`. The projection is not a mutation authority or an append-only history.

## Status and next actions

Read `blockers` first, then `diagnostic`, then `action`. The human output uses those names; JSON uses `blockers`, `diagnostic`, and `next_action`.

| State | Meaning | Operator response |
| --- | --- | --- |
| `queued` or `runnable` | Work is waiting or eligible to start. | Follow `action`; resolve any listed blocker or wait for capacity. |
| `active` or `review-ready` | The exact attempt is in implementation, validation, independent review, findings handoff, or publication. | Follow `current_phase` and `action`; inspect the session marked current when one is available. |
| `blocked` or `conflicting` | Identity, policy, dependency, or runtime facts prevent safe mutation. | Follow `blockers` and `diagnostic`; repair the authoritative fact, then reconcile. |
| `failed` | The attempt ended with retained diagnostics. | Inspect the log. Use **Recover attempt** only when the latest attempt is marked retryable. |
| `orphaned` | Local resources have no matching authoritative GitHub attempt. | Compare exact identities. Use **Abandon attempt** only when the resources are confirmed stale. |
| `completed` or `cancelled` | The attempt is terminal. | Archive a completed card if desired. If its issue is closed, dismiss the card while retaining diagnostics. |

Do not infer safety from the state name alone. The exact `action` and identity checks govern recovery and cleanup; see [Recovery](recovery.md).

The dashboard reads that projection every five seconds. When peer URLs are configured, its project selector reads each peer's repository-bound status but exposes peer cards read-only. A peer that returns another repository's status is rejected; open the peer dashboard link to use that deployment's controls. Its five-lane board—Queue, In progress, In review, Needs attention, and Done—shows current and completed attempts at the same time. When a newer attempt exists for the same issue, older failed, orphaned, or cancelled attempts move to a collapsed Previous attempts section. Their diagnostics remain visible, but they do not affect current state counts or health. Dismiss, Archive, Abandon, Recover, and Permanent removal create owner-backed receipts. Destructive actions first commit an attempt generation change and tombstone, then run exact cleanup outside the owner. Their results survive refresh and restart in `runtime-state.json`; legacy dashboard and removal files are no longer written. The GitHub issue, pull request, and remote refs remain unchanged by local cleanup. Destructive controls require browser confirmation; all controls require a same-origin POST. The browser supplies only the deployment repository plus issue/attempt numbers, never paths, branches, sessions, commands, or arbitrary GitHub policy. See [Recovery](recovery.md).

Attempt cards also list the bounded implementation and reviewer sessions retained by the manifest. A reviewer entry shows its mode and exact target: plan review binds a digest of the issue plan, while implementation review binds the owner-sealed `base..head` range. Both modes use the same `reviewer` role, managed command, deterministic session identity, and owner-mediated status mailbox; there is no UI-specific reviewer type. The primary implementation selector uses `/terminal`; reviewer selectors use `/reviewer/terminal`. Both routes require the exact deployment repository, issue, and attempt, then derive and compare the deterministic tmux name before attaching. Both local terminals accept direct operator input. Peer cards stay read-only and do not proxy terminals. Unknown roles remain visible in lifecycle data but are not attachable.

When the optional supervised orchestrator agent is configured, its dashboard card shows lifecycle and context health and opens the exact server-selected tmux session. Recover keeps an adoptable live conversation, while Clear context and Rebuild context explicitly start a new generation. Attention cards start one deduplicated, issue-specific one-shot audit. Projection changes start or coalesce the same separate audit path; unchanged nonterminal work starts one at most every five minutes. The audit compares the current projection with the bounded prior report. A workflow or GitHub transition, commit change, meaningful tmux output change, or direct owner reply is progress; two unchanged observations at least one heartbeat apart are required for an actionable stall. The audit result replaces `orchestrator-heartbeat-report.json`. It may use the direct GitHub status contract to set one specific needs-attention reason, skip an identical update, or clear its prior reason after fresh evidence of recovery. After it finishes, fails, or times out, a changed coordinator-owned attention projection may persist `orchestrator-attention-handoff.json` and send one fixed follow-through prompt to the primary conversation. Unchanged attention does not send another prompt. The orchestrator remains advisory and cannot directly schedule, mark, publish, merge, or relay operator conversations. Workflow proposals use the same owner-backed effects and generations as dashboard controls. Its interactive terminal is available only from a loopback browser request, including when unsafe network dashboard access is enabled.

### Retry a completed transition

The orchestrator may submit `{"version":1,"repository":"owner/repository","issue":123,"attempt":1,"action":"retry_transition","request_id":"unique-1"}` through `orchestrator-proposal`. The service observes the durable artifact without dashboard polling. It checks an immutable owner projection and accepts only the exact unblocked active attempt whose implementation session is completed and whose current phase is validation or publication. It then waits for one coalesced reconciliation cycle; a stale, blocked, terminal, mismatched, or unrelated phase is refused without transition mutation.

An automatic attention follow-through for an active attempt may submit `{"version":1,"repository":"owner/repository","issue":123,"attempt":1,"action":"check_in_attempt","request_id":"unique-2","handoff_id":"<64-hex-character-id>"}`. The coordinator re-reads the exact needs-attention target, verifies its one live implementation owner, and sends a fixed progress request through that worker's existing tmux boundary. This path accepts no message text or implementation direction and remains deduplicated until fresh status changes. A recovery follow-through may instead use `recover_attempt` with a new request ID and the same handoff ID. It calls the dashboard recovery guard; only the latest retryable failed attempt or exact retryable runtime-liveness mismatch with matching local identity and no pull request can record the fixed retry control. If no safe action exists, the primary submits `human_attention` with the same identity and a concise verified `detail`. This records the reason without workflow mutation.

Pass the same JSON to `orchestrator-proposal-status`. `running` proves that exact validation passed and the fixed coordinator action started. `succeeded` proves only that action returned successfully; `failed` identifies its bounded failing stage, and `refused` identifies a validation refusal. The coordinator records recovery only after a fresh projection shows that the exact target no longer needs attention. Use a new bounded `request_id` for a later material state change. These controls cannot send an arbitrary worker message or implementation instruction, restart a worker, cancel or abandon an attempt, rerun checks, merge a pull request, send arbitrary tmux input, or execute commands.

Autonomous merge has additional restrictions: the PR must remain open, non-draft, mergeable, on the expected head, current with its required base, free of unresolved authorized feedback, and compliant with repository-required reviews and checks plus repository merge permissions. Branch protection is optional, and `agent-symphony/policy` does not need to be configured as a required status. GitHub still enforces any rules that do exist when the coordinator submits the expected-head merge.

For a published attempt, the coordinator repairs missing validation/documentation evidence from the verified worker result. If the unchanged head remains blocked by checks, repository settings, or permissions the coordinator cannot safely change, it posts one deduplicated explanation on the pull request.

Commands produce plain human-readable text by default and never depend on color. `NO_COLOR` is therefore honored without special handling. `--json` emits one JSON object with envelope `version: 1`, `command`, `ok`, and `data`, `diagnostics`, or `error` as applicable. Status output contains identities, resource paths, diagnostics, and actions only—never feedback bodies, credentials, or policy controls. A failing validation or diagnostic exits with status 1; command-line misuse exits with status 2.

## Configuration

`.agent-symphony.yaml` uses the JSON subset of YAML so the single Go binary can parse it strictly without a dependency. The abbreviated shape below omits the generated implementation/reviewer/auditor arrays because those arrays contain the complete managed confinement profile; run `agent-symphony init` to write the valid current defaults rather than copying this excerpt.

```json
{
  "version": 1,
  "repository": "owner/repository",
  "labels": {
    "ready": "agent-ready",
    "priority_p1": "priority:P1",
    "priority_p2": "priority:P2",
    "priority_p3": "priority:P3"
  },
  "dependencies": {
    "section": "Dependencies"
  },
  "completion_policies": {
    "default": "human-review",
    "human_review_label": "needs-human-review",
    "autonomous_merge_label": "autonomous-merge"
  },
  "concurrency": 1,
  "reconciliation_interval_seconds": 60,
  "worktree_root": ".worktrees",
  "docs_paths": ["README.md", "docs"],
  "commands": {
    "orchestrator": ["codex", "-c", "projects={\"{orchestrator_workspace}\"={trust_level=\"trusted\"}}", "--sandbox", "danger-full-access", "--ask-for-approval", "never", "--no-alt-screen"],
    "environment_allowlist": ["LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"]
  },
  "status": {
    "format": "human",
    "color": "auto"
  }
}
```

This excerpt omits `commands`; `agent-symphony init` writes the exact managed values. Do not hand-build or replace the implementation/reviewer arrays: validation accepts only the generated rootless Codex profile. The reviewer runs in its interactive tmux session with the bound prompt as its final argument and atomically writes its structured result to `AGENT_SYMPHONY_REVIEW_RESULT`.

`reconciliation_interval_seconds` controls continuous `serve` reconciliation in whole seconds. It must be between 1 and 60 and defaults to 60 when omitted from an older configuration. An explicit `serve --interval duration` overrides it for that run. `serve --disable-periodic-reconciliation` instead disables the periodic ticker for that run while retaining startup and explicit/event reconciliation.

The generated default omits `labels.issue_filter`, so no extra queue label is required. To limit intake to a repository-specific queue, add one label name under `labels`:

```json
"issue_filter": "agent-symphony"
```

An empty value also disables the filter. When configured, an issue must currently have this label in addition to `agent-ready` and one priority label; removing it makes the issue ineligible on the next reconciliation.

New configuration created by `agent-symphony init` enables both generated orchestrator commands. Remove `commands.orchestrator` to disable the advisory console, heartbeat audits, and attention handoffs. Remove only `commands.orchestrator_audit` to keep the console and structured attention handoffs without periodic audit model usage. Agent Symphony replaces `{managed_workspace}` for implementation and review and `{orchestrator_workspace}` for orchestrator roles with that process's exact absolute managed workspace; it does not change global Codex trust. The primary command receives bounded generated context as an argument. The exact managed auditor command uses the read-only profile with `exec --json --ephemeral` and receives its prompt over stdin. The previous generated file-result command migrates automatically on configuration load; custom auditor commands are rejected. The dedicated `agent-host orchestrator-audit` boundary accepts one strict stdin launch contract (128 KiB maximum), checks the stable audit parent and command, and forwards only the prompt. The complete stdout JSONL stream is limited to 1 MiB; the final message is limited to 64 KiB and must precede successful turn completion. Stderr cannot supply a result. The process deadline is four minutes. No per-audit directory or result file is created.

The optional orchestrator runs as the coordinator user without sudo. Its `danger-full-access` Codex command can use the authenticated `gh` CLI and inspect same-user tmux sessions, so use it only with a trusted model. It is advisory and separate from the rootless implementation/review boundary.

The agent cannot replace deterministic workflow decisions. Its proposal adapter accepts only the fixed recovery actions documented above; it cannot carry operator instructions or arbitrary GitHub mutations.

Commands are argument arrays, not shell strings. Implementation and reviewer commands use Agent Symphony's fixed strict, no-approval, network-denied profile with user/project rules and hosted side-effect tools disabled; custom and sandbox-bypass arrays are rejected. Both commands must resolve to the same canonical `codex-cli 0.153.x` executable. Startup and every launch bind its canonical path, version, byte hash, and profile digest; identity drift fails closed.

The boundary helper captures implementation or review output in exclusively created private artifacts. An implementation returns one `agent-symphony-result-v1` object no larger than 64 KiB; a reviewer returns one bounded `agent-symphony-review-v1` object. Results are bound to the launch generation and permission-profile digest. Process-group state is a liveness observation, not proof that detached descendants are dead.

The implementation boundary requires the exact deterministic branch, unique disposable workspace, session, generation, profile digest, contained Git directory, valid base ancestry, and no remote or credential helper. The reviewer receives an owner-created snapshot and exact target. **Start plan review** binds the issue-body digest; `implementation-review` binds the owner-sealed approved-base-through-head range. Terminal transcript text is never parsed as a result.

The boundary supplies control, result, temporary, and cache paths inside the disposable workspace plus an isolated `CODEX_HOME` containing only Codex authentication assets. `environment_allowlist` may add non-secret process variables, but cannot add GitHub credentials, `TMPDIR`, cache roots, SSH-agent, cloud, proxy, or other authority-bearing values. Worker commands and descendants have no network. On macOS, Codex may still read/write unrelated same-user shared `/tmp`; no Agent Symphony authority path is stored there, and `doctor` reports the residual risk. Secret-shaped command arguments and assignments are rejected so `config view` cannot disclose them. The required dependency section contains issue references or `None`; completion defaults to human review.

## Attempt runtime troubleshooting

Production source bundles, branches, directories, and tmux names include repository and attempt identity. Each generation gets a unique private workspace below `--runtime-state`; it is never reused. `worktree_root` remains an offline/local field and does not move production work outside that boundary. Recovery manifests and retained output remain below the separate coordinator state root; the manifest is diagnostic metadata, not workflow truth.

The runtime verifies the rootless profile before creating resources and fails closed if any denial canary fails. It supplies managed private paths after filtering the environment; `HOME`, `CODEX_HOME`, temporary paths, and cache paths cannot be overridden through the allowlist.

- If launch fails, inspect the manifest `diagnostic` and `agent.log`. Failed resources are retained intentionally.
- If an attempt appears active after restart, compare its manifest, worktree HEAD, and `tmux has-session -t <session>` before resuming. Never attach to a session or directory whose deterministic identity does not match.
- Cancellation sends `C-c`, waits briefly, then kills only the named attempt session. It does not remove the worktree, so partial work and diagnostics remain available.
- A durably merged PR removes the exact verified attempt clone (including its local branch), worker result, and named tmux session during reconciliation. Its recovery manifest and diagnostic log remain available.
- An attempt worktree has no remote and a disabled local credential helper. A successful `git push` from it indicates a broken host boundary; stop serving work and rerun diagnostics.
- “resources already exist” is a safety stop. Reconcile the recorded attempt instead of deleting or adopting resources by hand.

Secrets—including GitHub tokens and passwords—are forbidden in configuration. Authenticate the daemon with `gh auth login` or let its process read `GH_TOKEN` or `GITHUB_TOKEN`. Agent Symphony does not forward those values or GitHub CLI configuration to implementation/review workers. Worker status and publication are owner-mediated; see [GitHub CLI integration](github-cli.md).

## Diagnostic boundaries

`doctor` requires `gh`, reads the daemon's authenticated identity, verifies the configured repository, reports effective access, and probes the managed Codex profile. Workers are expected to lack `gh`/GitHub access; that denial is a security result, not a setup failure.

On WSL, diagnostics resolve the Git root, choose the longest containing entry from `/proc/mounts`, and reject `drvfs` or `9p` mounts. Linux and WSL also require an already-working unprivileged-user-namespace/bubblewrap capability. Agent Symphony does not enable that host capability itself and never asks for root; `doctor` and `serve` fail closed when the managed Codex confinement proof cannot run.

## Release commands

`scripts/release.sh VERSION [OUTPUT_DIR]` creates reproducible no-CGO archives and `SHA256SUMS` without overwriting existing output. `scripts/validate-release.sh [VERSION]` runs the complete local release gate. These repository scripts are maintainer commands, not installed CLI subcommands.
## Internal worker adapter

`agent-symphony agent-host` runs only as an internal same-user adapter beneath the private runtime-state root; production does not invoke it through sudo or a separate OS identity. `orchestrator-proposal` validates the exact orchestrator workspace and bounded schema and writes only its pre-created protected proposal artifact under an exclusive file lock. `orchestrator-proposal-status` only reads the coordinator-authored binding status for the exact submitted schema. None of these modes is an interactive operator command. Unsupported native Windows and WSL repositories under `/mnt/*` fail closed.

## Legacy host installation command

`install-host` only reports legacy host artifacts. It never provisions, modifies, or selects accounts, groups, roots, or sudo rules. Do not use obsolete sudo policy for current runtime.
