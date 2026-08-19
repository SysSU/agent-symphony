# CLI reference

The CLI provides configuration, diagnostics, production reconciliation, and restart recovery.

## Commands

```text
agent-symphony help
agent-symphony --help
agent-symphony -h
agent-symphony --version
agent-symphony install-host --coordinator user [--json]
agent-symphony agent-host implementation|review|orchestrator|orchestrator-proposal
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
- `agent-host` is the internal boundary adapter for implementation, review, and orchestrator processes. `orchestrator-proposal` accepts only the bounded proposal JSON on standard input and emits one strict frame on standard output for coordinator capture. These are not interactive operator commands.
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

Every refresh calculates a **status projection**: the current view built from GitHub and local runtime facts. It atomically writes that view to `<runtime-state>/status.json` with mode `0600`. The file includes a timestamp, issue state, blockers, diagnostics, and next action, and the same fields appear in `status` and `inspect`. This is the latest snapshot, not an append-only history.

## Status and next actions

Read `blockers` first, then `diagnostic`, then `action`. The human output uses those names; JSON uses `blockers`, `diagnostic`, and `next_action`.

| State | Meaning | Operator response |
| --- | --- | --- |
| `queued` or `runnable` | Work is waiting or eligible to start. | Follow `action`; resolve any listed blocker or wait for capacity. |
| `active` or `review-ready` | The exact attempt is running or its pull request is in review. | Wait, inspect the named tmux session, or review the linked GitHub work. |
| `blocked` or `conflicting` | Identity, policy, dependency, or runtime facts prevent safe mutation. | Follow `blockers` and `diagnostic`; repair the authoritative fact, then reconcile. |
| `failed` | The attempt ended with retained diagnostics. | Inspect the log. Use **Recover attempt** only when the latest attempt is marked retryable. |
| `orphaned` | Local resources have no matching authoritative GitHub attempt. | Compare exact identities. Use **Abandon attempt** only when the resources are confirmed stale. |
| `completed` or `cancelled` | The attempt is terminal. | Archive a completed card if desired. Cancelled work remains ineligible until its controls change. |

Do not infer safety from the state name alone. The exact `action` and identity checks govern recovery and cleanup; see [Recovery](recovery.md).

The dashboard reads that snapshot every five seconds. Current and completed attempts use separate tabs. Clicking a tmux name attaches an xterm.js terminal to only that exact projected, live session; closing the browser detaches that client without ending the worker. Archive is available only for a completed projection and reuses completed-attempt cleanup before adding a local hidden-card marker to `<runtime-state>/dashboard-state.json`. Abandon is available only for an orphaned projection; it stops the exact session, removes the deterministic worktree/result and retained manifest/log, then hides the stale card. Recover revalidates one exact stuck attempt, may mark it failed, and posts the fixed retry control. These controls require browser confirmation and a same-origin POST. The browser supplies only issue/attempt numbers; paths, branches, sessions, commands, and arbitrary GitHub policy are never accepted from it. See [Recovery](recovery.md).

When the optional supervised orchestrator agent is configured, its dashboard card shows lifecycle and context health and opens the exact server-selected tmux session. Recover keeps an adoptable live conversation, while Clear context and Rebuild context explicitly start a new generation. Attention cards can enqueue one structured, deduplicated investigation notice for their current issue and attempt; the orchestrator remains advisory and cannot directly schedule, mark, publish, or merge GitHub work. Orchestrator actions use the same reconciliation lock and origin checks as other dashboard controls. Its interactive terminal is available only from a loopback browser request, including when unsafe network dashboard access is enabled.

### Send a bounded worker message

Worker messages are asynchronous follow-up turns, not live terminal input. Configure `--dashboard-password-file` even on loopback. Agent Symphony refuses message confirmation without both dashboard authentication and a signed nonce bound to the authenticated browser session and exact proposal.

Do not put secrets in a worker message. Confirmation records the complete text durably on the GitHub issue, where normal repository access controls apply; dashboard status intentionally omits the text.

1. In the orchestrator console, ask for a message to one exact repository issue and attempt. The orchestrator submits the displayed strict schema to the fixed `agent-host orchestrator-proposal` adapter, which emits a framed response on standard output and enforces an 8192-byte UTF-8 message limit. The coordinator captures that frame from the exact pane, so the read-only agent receives no writable path.
2. Review the dashboard confirmation panel. It shows the exact repository, issue, attempt, and complete message. Choose **Confirm and queue** or **Cancel proposal**.
3. After confirmation, Agent Symphony freshly proves the exact active owner, including base SHA, published head when present, and verified deterministic runtime resources. It records the message on the issue before any worker delivery. Duplicate confirmation of the same exact message reuses its stable identity.
4. If the worker is busy, the message remains queued. After the current turn exits safely, reconciliation writes a durable delivery claim, rereads authoritative terminal and ownership state at worker acceptance, and then starts one bounded implementation turn in the same worktree. Ambiguous recovery verifies the exact coordinator-known launch identity through the worker boundary; files in the worker-writable handoff directory are not delivery authority.
5. Check the attempt card for `queued`, `delivered`, `rejected`, or `failed`. Status shows only the stable message ID and outcome; the message text remains in the authenticated confirmation response and the associated GitHub issue record.

Cancellation, completion, a mismatched attempt, or a merged pull request rejects pending delivery. The authoritative check after the durable claim prevents a terminal change during earlier cleanup or discovery from reaching worker acceptance. A daemon restart reconstructs accepted messages, claims, and outcomes from coordinator-authored GitHub markers. For a claimed delivery in the crash window, the worker boundary checks the exact tmux launch identity before reconciliation can start another follow-up turn. Publication requires the coordinator-authored delivered outcome.

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
    "implementation": ["codex", "exec", "--sandbox", "workspace-write"],
    "reviewer": ["codex", "exec", "--sandbox", "read-only", "-"],
    "orchestrator": ["codex", "--sandbox", "read-only", "--ask-for-approval", "never", "--no-alt-screen"],
    "environment_allowlist": ["LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"]
  },
  "status": {
    "format": "human",
    "color": "auto"
  }
}
```

`commands.orchestrator` is optional and requires an advanced host-isolated installation. Omitting it disables the long-lived advisory agent, so upgrades do not unexpectedly start a model or add cost. When configured, Agent Symphony replaces `{orchestrator_workspace}` in each argument with the absolute managed workspace path, then appends bounded generated context as the command's final argument. The command must accept an initial prompt there. The agent cannot replace coordinator workflow decisions or receive GitHub, SSH-agent, cloud, token, password, private-key, or authorization credentials. Its only worker-message output is the fixed proposal adapter's framed standard-output response; it receives no writable proposal path, tmux target, worker command, GitHub mutation, or scheduling interface.

Commands are argument arrays, not shell strings, so runtime code does not use shell interpolation. The default noninteractive Codex implementation command uses `workspace-write` for source edits.

The boundary helper captures implementation or review stdout in an exclusively created private result file outside the worktree or snapshot; stderr remains in tmux for diagnostics. An implementation must return one `agent-symphony-result-v1` JSON object no larger than 64 KiB. A reviewer must return one bounded `agent-symphony-review-v1` object. The helper owns the process group, stops only that group on completion, overflow, or cancellation, and fails boundedly if an escaped process keeps stdout open. Results remain outside the source tree for safe export and retry.

The implementation boundary requires the exact deterministic branch, worktree, and session, a contained Git directory, valid base ancestry, and no remotes or credential helpers. The default reviewer uses `codex exec --sandbox read-only -`; explicit reviewer arrays replace those arguments unchanged. Review covers the complete approved-base-through-attested-`HEAD` range, and terminal transcript text is never parsed as the result.

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

By default no host isolation is installed: `agent-symphony agent-host implementation|review` runs as a plain, same-user subprocess of the coordinator, with no `sudo` and no separate OS identity — see [Setup](setup.md) and [Security](security.md). `agent-symphony install-host --coordinator USER` opts into the stronger advanced boundary; it provisions the documented macOS or Linux/WSL2 host boundary and must run as root from the installed versioned binary. The optional orchestrator is rejected until that boundary is installed. Once installed, the three process modes use only their exact sudo adapters. `orchestrator-proposal` is callable only from the already-running reviewer identity, validates the exact orchestrator workspace and bounded schema, and writes only its standard-output frame. None of these modes is an interactive operator command. Unsupported native Windows and WSL repositories under `/mnt/*` fail closed regardless of mode.
