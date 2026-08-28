# CLI reference

The CLI provides configuration, diagnostics, production reconciliation, and restart recovery.

## Commands

```text
agent-symphony help
agent-symphony --help
agent-symphony -h
agent-symphony --version
agent-symphony install-host --coordinator user [--json]
agent-symphony agent-host implementation|review|orchestrator|orchestrator-proposal|orchestrator-proposal-status
agent-symphony init [--config path] [--json]
agent-symphony validate [--config path] [--json]
agent-symphony config view [--config path] [--json]
agent-symphony doctor [--config path] [--runtime-state path] [--offline] [--json]
agent-symphony diagnostics [--config path] [--runtime-state path] [--offline] [--json]
agent-symphony pr-governance --state path [--config path] [--json]
agent-symphony serve --state path --runtime-state path [--interval duration] [--dashboard-address address] [--allow-unsafe-dashboard-network --dashboard-password-file path] [--config path]
agent-symphony status (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
agent-symphony list (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
agent-symphony inspect --issue number (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
agent-symphony reconcile (--state path --runtime-state path | --attempts path [--runtime-state path]) [--config path] [--json]
```

- `help`, `--help`, and `-h` print the command summary. `--version` prints the release version.
- `install-host` provisions the optional advanced host-isolation boundary. Run it as root from the exact installed binary and repeat it after each binary upgrade.
- `agent-host` is the internal boundary adapter for implementation, review, and orchestrator processes. `orchestrator-proposal` accepts only the bounded proposal JSON on standard input, durably writes the canonical object to its fixed protected artifact, and reports `submitted` with the exact binding. `orchestrator-proposal-status` accepts that same exact JSON and reports `pending`, `running`, `succeeded`, `failed`, `refused`, `consumed`, `replaced`, or `unknown` for its coordinator-observed binding. These are not interactive operator commands.
- `init` creates a new config with conservative defaults and refuses to overwrite a file. It requires a GitHub `origin` in the current repository.
- `validate` requires the config file to be inside the resolved Git root. It rejects malformed input, duplicate JSON keys at any nesting depth, unknown keys, secret-shaped keys or command arguments, invalid policy values, duplicate/empty labels, unsafe command arguments, and paths that are absolute, traverse outside the repository, target Git metadata, or escape through symlinks. Worktree and documentation paths are always anchored at the Git root, not the config file's directory.
- `config view` prints the validated configuration. Invalid or secret-bearing files are never echoed.
- `doctor` and its `diagnostics` alias check the supported platform, WSL filesystem placement, Git, tmux, configured agent commands, Git repository/remote identity, GitHub CLI authentication, and effective repository access. `--runtime-state` selects the state root to check. `--offline` skips only the GitHub probe and emits an explicit warning.
- `pr-governance` is a one-shot pull-request governance command. It creates an empty private recovery-state JSON file when the named file is absent, then durably writes feedback and validation handoffs. Recovery claims those handoffs before they cross into the isolated runtime. All GitHub reads and writes use the authenticated `gh` session.
- `serve --state path --runtime-state path` verifies the authenticated GitHub CLI account and repository, acquires a non-following single-instance lock for that runtime state, reconciles immediately, and polls at most every 60 seconds across bounded GitHub failures. It also serves that repository's dashboard at `--dashboard-address` (default `127.0.0.1:8080`). Localhost or a loopback IP is required unless `--allow-unsafe-dashboard-network` is set; that opt-in requires `--dashboard-password-file`. The file must be a coordinator-owned regular file with no group or other permissions and one nonempty password line. The coordinator reads it without placing the password in process arguments. HTTP Basic authentication protects every dashboard route using username `agent-symphony`; a password file may also protect loopback without the unsafe flag. Every cycle has a whole-cycle two-minute deadline. `reconcile` performs one production cycle; `status`, `list`, and `inspect` refresh and expose the same queued, active, blocked, review-ready, and completed projection. Supplying `--attempts path` selects the nonmutating offline diagnostic. No GitHub credential or identity environment variables are required or read. Independent repository daemons on one host must use distinct `--state`, `--runtime-state`, and dashboard addresses.

Unsafe network mode serves plain HTTP: the password and terminal traffic are not encrypted, and anyone with the password can use the dashboard's terminal, recovery, and cleanup controls. Use it only on a trusted network with host-level firewall rules, or carry it over an encrypted VPN or tunnel.

## Issue eligibility and recorded blockers

Issue bodies may contain arbitrary text, including no Markdown sections. A configured `## Dependencies` section is optional; when present, referenced open issues block dispatch. An optional `## Paths` section declares one repository-relative file or directory per list line for concurrent scheduling. Missing or invalid path scope does not make an issue ineligible, but it serializes that issue against other active work in the same repository because disjointness cannot be proven.

Dispatch requires an open, non-cancelled issue with `agent-ready` applied after the latest body edit, exactly one configured P1-P3 label, and no conflicting completion labels. Every actor changing those controls must currently have repository `maintain` or `admin` permission; the authenticated coordinator account is allowed. `autonomous-merge` is the explicit opt-in for coordinator-managed merge and must also follow the latest body edit. Without it, Agent Symphony creates the pull request but never merges it. `needs-human-review` remains available as an optional explicit PR label and pending policy Check; it is not required for the default non-autonomous path. An existing active/completed attempt, contradictory markers, unresolved dependencies, terminal failure without an authorized retry, or exhausted concurrency also prevents dispatch. Coordinator marker syntax is reserved and exact coordinator artifacts are not treated as human feedback.

Every refresh calculates a **status projection**: the current view built from GitHub and local runtime facts. It atomically writes that view to `<runtime-state>/status.json` with mode `0600`. The file includes a timestamp, issue state, current phase, blockers, diagnostics, next action, and a bounded list of attempt-owned sessions. Each session records its role, deterministic name, lifecycle state, current marker, and available timestamps. The same fields appear in `status` and `inspect`. This is the latest snapshot, not an append-only history.

## Status and next actions

Read `blockers` first, then `diagnostic`, then `action`. The human output uses those names; JSON uses `blockers`, `diagnostic`, and `next_action`.

| State | Meaning | Operator response |
| --- | --- | --- |
| `queued` or `runnable` | Work is waiting or eligible to start. | Follow `action`; resolve any listed blocker or wait for capacity. |
| `active` or `review-ready` | The exact attempt is in implementation, validation, independent review, findings handoff, or publication. | Follow `current_phase` and `action`; inspect the session marked current when one is available. |
| `blocked` or `conflicting` | Identity, policy, dependency, or runtime facts prevent safe mutation. | Follow `blockers` and `diagnostic`; repair the authoritative fact, then reconcile. |
| `failed` | The attempt ended with retained diagnostics. | Inspect the log. Use **Recover attempt** only when the latest attempt is marked retryable. |
| `orphaned` | Local resources have no matching authoritative GitHub attempt. | Compare exact identities. Use **Abandon attempt** only when the resources are confirmed stale. |
| `completed` or `cancelled` | The attempt is terminal. | Archive a completed card if desired. Cancelled work remains ineligible until its controls change. |

Do not infer safety from the state name alone. The exact `action` and identity checks govern recovery and cleanup; see [Recovery](recovery.md).

The dashboard reads that snapshot every five seconds. Its five-lane board—Queue, In progress, In review, Needs attention, and Done—shows current and completed attempts at the same time. Clicking a tmux name attaches an xterm.js terminal to only that exact projected, live session; closing the browser detaches that client without ending the worker. Archive is available only for a completed projection and reuses completed-attempt cleanup before adding a local hidden-card marker to `<runtime-state>/dashboard-state.json`. Abandon is available only for an orphaned projection; it stops the exact session, removes the deterministic worktree/result and retained manifest/log, then hides the stale card. Recover revalidates one exact stuck attempt, may mark it failed, and posts the fixed retry control. An authenticated active or review-ready card can also queue a bounded implementation follow-up. Destructive controls require browser confirmation; all controls require a same-origin POST and the checks described below. The browser supplies only issue/attempt numbers; paths, branches, sessions, commands, and arbitrary GitHub policy are never accepted from it. See [Recovery](recovery.md).

Attempt cards also list the bounded implementation and reviewer sessions retained by the manifest. The primary implementation selector uses `/terminal`; reviewer selectors use `/reviewer/terminal`. Both routes accept only issue and attempt numbers, then derive and compare the deterministic tmux name before attaching. Reviewer terminals are read-only, while implementation terminals keep their existing interactive behavior. Unknown roles remain visible in lifecycle data but are not attachable.

When the optional supervised orchestrator agent is configured, its dashboard card shows lifecycle and context health and opens the exact server-selected tmux session. Recover keeps an adoptable live conversation, while Clear context and Rebuild context explicitly start a new generation. Attention cards start one deduplicated, issue-specific one-shot audit. Projection changes start or coalesce the same separate audit path; unchanged nonterminal work starts one at most every five minutes. The primary conversation receives no programmatic input after launch. The latest bounded result replaces `orchestrator-heartbeat-report.json` in its managed workspace, where the primary orchestrator may read and reverify it when relevant. The orchestrator remains advisory and cannot directly schedule, mark, publish, or merge GitHub work. Lifecycle and investigation actions use the reconciliation lock and origin checks shared by other dashboard controls. Message confirmation uses a separate narrow admission lock, verifies the exact local runtime owner, records the durable GitHub marker, and never launches a worker itself. Delivery performs the fresh GitHub target checks before the worker can start. Its interactive terminal is available only from a loopback browser request, including when unsafe network dashboard access is enabled.

### Send a bounded worker message

Worker messages are asynchronous follow-up turns, not raw input pasted into a dead pane. Configure `--dashboard-password-file` even on loopback. Agent Symphony refuses them without dashboard authentication and a signed browser session. An orchestrator proposal also requires a nonce bound to the exact reviewed proposal.

Do not put secrets in a worker message. Confirmation records the complete text durably on the GitHub issue, where normal repository access controls apply; dashboard status intentionally omits the text.

1. Either enter the message in **Tell this agent what to do next** on an active or review-ready attempt card, or ask the orchestrator to propose a message for one exact repository issue and attempt. The fixed `agent-host orchestrator-proposal` adapter enforces the strict schema and 8192-byte UTF-8 limit. `submitted` proves the durable artifact write; only a matching `pending` status proves coordinator observation. The status adapter reads a coordinator-authored binding record and cannot mutate coordinator state.
2. For an orchestrator-proposed message, review the dashboard confirmation panel. It shows the exact repository, issue, attempt, and complete message. Choose **Confirm and queue** or **Cancel proposal**. The direct card form submits the text you entered without this second panel.
3. After direct submission or proposal confirmation, Agent Symphony requires the authenticated exact projected target and one verified deterministic local runtime owner, then records the message on the issue. Repeating the same exact message reuses its stable identity. Admission does not prove that the remote attempt is still active; that fresh check belongs to delivery, so a terminal race may produce a durable `rejected` outcome.
4. An accepted message interrupts the current slow coordinator pass, including a guarded transition retry, and starts one bounded target-only delivery under the coordinator lock. It does not wait for the next polling interval or unrelated pull-request governance. Delivery reads only the deterministic pull request and issue, freshly proves the exact active owner including base and published head, and checks them again at worker acceptance. If the worker is busy, the message remains queued. Otherwise Agent Symphony writes a durable delivery claim and starts one bounded implementation turn in the same worktree. The default Codex command starts a fresh session so an oversized or stuck prior transcript cannot delay the instruction; explicit custom implementation commands run unchanged. Delivery is not acknowledged until the replacement worker produces startup output. A retired handoff that remains live past the recovery deadline is interrupted and its deterministic tmux session is recreated before the queued message starts. The card's tmux link shows the follow-up after it starts; send further instructions through the bounded form so the result remains captured and publishable. Ambiguous recovery verifies the exact coordinator-known launch identity through the worker boundary; files in the worker-writable handoff directory are not delivery authority.
5. Check the attempt card for `queued`, `delivered`, `rejected`, or `failed`. The accepted response is projected immediately instead of waiting for a repository poll. Status shows only the stable message ID and outcome; the message text remains in the authenticated confirmation response and the associated GitHub issue record.

Cancellation, completion, a mismatched attempt, or a merged pull request rejects pending delivery. The authoritative check after the durable claim prevents a terminal change during earlier cleanup or discovery from reaching worker acceptance. A daemon restart reconstructs accepted messages, claims, and outcomes from coordinator-authored GitHub markers. For a claimed delivery in the crash window, the worker boundary checks the exact tmux launch identity before reconciliation can start another follow-up turn. Publication requires the coordinator-authored delivered outcome.

### Retry a completed transition

The orchestrator may submit `{"version":1,"repository":"owner/repository","issue":123,"attempt":1,"action":"retry_transition","request_id":"unique-1"}` through `orchestrator-proposal`. The service observes the durable artifact without dashboard polling. Under the reconciliation lock, it builds a fresh read-only projection and accepts only the exact unblocked active attempt whose implementation session is completed and whose current phase is validation or publication. It then runs one guarded reconciliation with a ten-minute deadline and stage-specific errors; a stale, blocked, terminal, mismatched, or unrelated phase is refused without transition mutation.

Pass the same JSON to `orchestrator-proposal-status`. `running` proves that exact validation passed and reconciliation started. `succeeded` proves that the pass returned successfully; `failed` identifies its bounded failing stage, and `refused` identifies a validation refusal. Verify the resulting issue, pull request head, handoff, and status through their authoritative live sources. Use a new bounded `request_id` for a later retry. This control cannot message or restart a worker, cancel or abandon an attempt, rerun checks, merge a pull request, send tmux input, or execute arbitrary commands.

Autonomous merge has additional restrictions: the PR must remain open, non-draft, mergeable, on the expected head, current with its required base, free of unresolved authorized feedback, and compliant with repository-required reviews and checks plus repository merge permissions. Branch protection is optional, and `agent-symphony/policy` does not need to be configured as a required status. GitHub still enforces any rules that do exist when the coordinator submits the expected-head merge.

For a published attempt, the coordinator repairs missing validation/documentation evidence from the verified worker result. If the unchanged head remains blocked by checks, repository settings, or permissions the coordinator cannot safely change, it posts one deduplicated explanation on the pull request.

Commands produce plain human-readable text by default and never depend on color. `NO_COLOR` is therefore honored without special handling. `--json` emits one JSON object with envelope `version: 1`, `command`, `ok`, and `data`, `diagnostics`, or `error` as applicable. Status output contains identities, resource paths, diagnostics, and actions only—never feedback bodies, credentials, or policy controls. A failing validation or diagnostic exits with status 1; command-line misuse exits with status 2.

## Configuration

`.agent-symphony.yaml` uses the JSON subset of YAML so the single Go binary can parse it strictly without a dependency:

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
  "worktree_root": ".worktrees",
  "docs_paths": ["README.md", "docs"],
  "commands": {
    "implementation": ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"],
    "reviewer": ["codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"],
    "orchestrator": ["codex", "-c", "projects={\"{orchestrator_workspace}\"={trust_level=\"trusted\"}}", "--sandbox", "danger-full-access", "--ask-for-approval", "never", "--no-alt-screen"],
    "orchestrator_audit": ["codex", "exec", "-c", "projects={\"{orchestrator_workspace}\"={trust_level=\"trusted\"}}", "-c", "model_reasoning_effort=\"medium\"", "--sandbox", "danger-full-access", "--skip-git-repo-check", "--ephemeral", "--output-last-message", "{orchestrator_result}", "-"],
    "environment_allowlist": ["LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"]
  },
  "status": {
    "format": "human",
    "color": "auto"
  }
}
```

New configuration created by `agent-symphony init` enables both orchestrator commands above. Remove `commands.orchestrator` to disable the advisory console and all heartbeat audits. Remove only `commands.orchestrator_audit` to keep the console without periodic model usage. Agent Symphony replaces `{orchestrator_workspace}` with the process's absolute managed workspace and `{orchestrator_result}` with a transient result path. It appends bounded generated context to the primary command and sends the one-shot audit prompt on standard input. The default audit uses medium reasoning effort and Codex's final-message output so progress logs cannot displace the diagnosis. A custom audit may omit `{orchestrator_result}` and return one plain-text result on standard output instead. Agent Symphony bounds the result and stops the process after four minutes.

In zero-admin mode, the orchestrator runs as the coordinator user. The `danger-full-access` Codex example lets it use the authenticated `gh` CLI and inspect the same-user tmux server when the bounded projection lacks progress detail. This setting also lets the model access other resources available to that user; Agent Symphony filters the launch environment and instructs the agent to keep GitHub and tmux inspection read-only, but the Codex sandbox no longer enforces that limit. Use this mode only when the orchestrator model is trusted. In advanced host-isolated mode, the orchestrator runs as the reviewer identity and cannot use the coordinator's GitHub login or tmux server even with the same Codex setting.

The agent cannot replace coordinator workflow decisions. Its worker-message output remains the fixed proposal adapter's `submitted` receipt; the read-only status adapter distinguishes durable submission from coordinator acknowledgement, and the authenticated dashboard requires explicit confirmation before the coordinator records or delivers it.

Commands are argument arrays, not shell strings, so runtime code does not use shell interpolation. The default noninteractive Codex implementation and reviewer commands use `--dangerously-bypass-approvals-and-sandbox` so both roles can use the host without Codex sandbox restrictions or approval prompts. Use advanced host isolation to confine each role to its unprivileged account.

The boundary helper captures implementation or review stdout in an exclusively created private result file outside the worktree or snapshot; stderr remains in tmux for diagnostics. An implementation must return one `agent-symphony-result-v1` JSON object no larger than 64 KiB. A reviewer must return one bounded `agent-symphony-review-v1` object. The helper owns the process group, stops only that group on completion, overflow, or cancellation, and fails boundedly if an escaped process keeps stdout open. Results remain outside the source tree for safe export and retry.

The implementation boundary requires the exact deterministic branch, worktree, and session, a contained Git directory, valid base ancestry, and no remotes or credential helpers. A confirmed follow-up runs the configured implementation command as a fresh turn in that exact worktree. This keeps the default `codex exec --dangerously-bypass-approvals-and-sandbox -` path independent of an old transcript's size or health; the explicit `-` makes Codex consume Agent Symphony's bounded standard-input prompt. Existing configs with the former default command are normalized in memory, while explicit custom implementation arrays run unchanged. The default reviewer uses the same Codex stdin form; explicit reviewer arrays replace those arguments unchanged. Review covers the complete approved-base-through-attested-`HEAD` range, and terminal transcript text is never parsed as the result.

Both boundaries set child `TMPDIR` to `/tmp`. `environment_allowlist` is the complete set of inherited variable names for implementation and review; add model-provider credentials explicitly. GitHub, Git askpass, SSH-agent, and cloud credential variables remain forbidden. Secret-shaped arguments and assignments are rejected so `config view` cannot disclose them. Dependencies are explicit issue references under the optional configured body section. Completion defaults to human review.

## Attempt runtime troubleshooting

Production attempt source bundle, branch, directory, and tmux names include the repository identity and are deterministic. Reviewer snapshot and session names do too. By default the attempt root is a private (mode `0700`) `worktrees` directory under the runtime state root (`--runtime-state`); under the optional advanced host-isolated mode (see [Setup](setup.md)) it is instead the shared provisioned `/var/lib/agent-symphony/attempts` (Linux/WSL2) or `/var/db/agent-symphony/attempts` (macOS) root. `worktree_root` remains an offline/local configuration field and does not move production work outside that boundary. Recovery manifests and retained agent output remain under each coordinator state root's separate `attempts` directory; the manifest is diagnostic metadata, not workflow truth.

The runtime requires the boundary's verification hook and fails closed before creating resources when it is absent or fails, in either mode. Environment inheritance happens only after verification through the shared agent environment filter. `HOME` is forbidden in configured allowlists; the boundary supplies the resolved account's home afterward — the coordinator's own home by default, or the provisioned worker/reviewer's home under advanced host isolation.

- If launch fails, inspect the manifest `diagnostic` and `agent.log`. Failed resources are retained intentionally.
- If an attempt appears active after restart, compare its manifest, worktree HEAD, and `tmux has-session -t <session>` before resuming. Never attach to a session or directory whose deterministic identity does not match.
- Cancellation sends `C-c`, waits briefly, then kills only the named attempt session. It does not remove the worktree, so partial work and diagnostics remain available.
- A durably merged PR removes the exact verified attempt clone (including its local branch), worker result, and named tmux session during reconciliation. Its recovery manifest and diagnostic log remain available.
- An attempt worktree has no remote and a disabled local credential helper. A successful `git push` from it indicates a broken host boundary; stop serving work and rerun diagnostics.
- “resources already exist” is a safety stop. Reconcile the recorded attempt instead of deleting or adopting resources by hand.

Secrets—including GitHub tokens, passwords, and credentials—are forbidden in configuration. Authenticate GitHub CLI with `gh auth login` or, optionally, let `gh` read `GH_TOKEN` or `GITHUB_TOKEN` from the coordinator environment. Agent Symphony does not parse or store those values; workers and reviewers do not inherit them or the stored CLI session.

## Diagnostic boundaries

`doctor` requires `gh`, reads the authenticated identity, verifies the configured repository, and reports the effective repository access GitHub returns. It fails with guidance to run `gh auth login` or grant the account repository access.

On WSL, diagnostics resolve the Git root, choose the longest containing entry from `/proc/mounts`, and reject `drvfs` or `9p` mounts. `serve` fails closed when its runtime boundary cannot be verified, whether that boundary is the zero-admin default or an administrator-provisioned advanced host-isolated install.

## Release commands

`scripts/release.sh VERSION [OUTPUT_DIR]` creates reproducible no-CGO archives and `SHA256SUMS` without overwriting existing output. `scripts/validate-release.sh [VERSION]` runs the complete local release gate. These repository scripts are maintainer commands, not installed CLI subcommands.
## Host isolation (advanced, optional)

By default no host isolation is installed: `agent-symphony agent-host implementation|review` runs as a plain, same-user subprocess of the coordinator, with no `sudo` and no separate OS identity — see [Setup](setup.md) and [Security](security.md). `agent-symphony install-host --coordinator USER` opts into the stronger advanced boundary; it provisions the documented macOS or Linux/WSL2 host boundary and must run as root from the installed versioned binary. The optional orchestrator is rejected until that boundary is installed. Once installed, the three process modes use only their exact sudo adapters. `orchestrator-proposal` is callable only from the already-running reviewer identity, validates the exact orchestrator workspace and bounded schema, and writes only its pre-created mode-`0620` proposal artifact under an exclusive file lock. `orchestrator-proposal-status` runs under that same identity and only reads the coordinator-authored, mode-`0440` binding status for the exact submitted schema. None of these modes is an interactive operator command. Unsupported native Windows and WSL repositories under `/mnt/*` fail closed regardless of mode.
