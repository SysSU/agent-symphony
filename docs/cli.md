# CLI reference

The CLI provides configuration, diagnostics, production reconciliation, and restart recovery.

## Commands

```text
agent-symphony init [--config path] [--json]
agent-symphony validate [--config path] [--json]
agent-symphony config view [--config path] [--json]
agent-symphony doctor [--config path] [--offline] [--json]
agent-symphony diagnostics [--config path] [--offline] [--json]
agent-symphony pr-governance --state path [--config path] [--json]
agent-symphony serve --state path --runtime-state path [--interval duration] [--config path]
agent-symphony status --state path --runtime-state path [--json]
agent-symphony list --state path --runtime-state path [--json]
agent-symphony inspect --issue number --state path --runtime-state path [--json]
agent-symphony reconcile --state path --runtime-state path [--json]
```

- `init` creates a new config with conservative defaults and refuses to overwrite a file. It requires a GitHub `origin` in the current repository.
- `validate` requires the config file to be inside the resolved Git root. It rejects malformed input, duplicate JSON keys at any nesting depth, unknown keys, secret-shaped keys or command arguments, invalid policy values, duplicate/empty labels, unsafe command arguments, and paths that are absolute, traverse outside the repository, target Git metadata, or escape through symlinks. Worktree and documentation paths are always anchored at the Git root, not the config file's directory.
- `config view` prints the validated configuration. Invalid or secret-bearing files are never echoed.
- `doctor` and its `diagnostics` alias check the supported platform, WSL filesystem placement, Git, tmux, both configured commands, Git repository/remote identity, GitHub CLI authentication, and effective repository access. `--offline` skips only the GitHub probe and emits an explicit warning.
- `pr-governance` is a one-shot pull-request governance command. It creates an empty private recovery-state JSON file when the named file is absent, then durably writes feedback and validation handoffs. Recovery claims those handoffs before they cross into the isolated runtime. All GitHub reads and writes use the authenticated `gh` session.
- `serve --state path --runtime-state path` verifies the authenticated GitHub CLI account and repository, acquires a non-following single-instance lock, reconciles immediately, and polls at most every 60 seconds across bounded GitHub failures. Every cycle has a whole-cycle two-minute deadline. `reconcile` performs one production cycle; `status`, `list`, and `inspect` refresh and expose the same queued, active, blocked, review-ready, and completed projection. Supplying `--attempts path` selects the nonmutating offline diagnostic. No GitHub credential or identity environment variables are required or read.

## Issue eligibility and recorded blockers

Issue bodies may contain arbitrary text, including no Markdown sections. A configured `## Dependencies` section is optional; when present, referenced open issues block dispatch. An optional `## Paths` section declares one repository-relative file or directory per list line for concurrent scheduling. Missing or invalid path scope does not make an issue ineligible, but it serializes that issue against other active work in the same repository because disjointness cannot be proven.

Dispatch still requires an open, non-cancelled issue with `agent-ready`, exactly one configured P1-P3 label, and no conflicting completion labels. Every actor changing those controls must currently have repository `maintain` or `admin` permission; the authenticated coordinator account is allowed. Human-review mode requires the exact, unedited `/agent-symphony approve` comment after the latest body or control change. Label-only autonomous mode requires `autonomous-merge` after the latest body edit; label application order otherwise does not matter. An existing active/completed attempt, contradictory markers, unresolved dependencies, terminal failure without an authorized retry, or exhausted concurrency also prevents dispatch. Coordinator marker syntax is reserved and exact coordinator artifacts are not treated as human feedback.

Every refresh atomically writes the current projection to `<runtime-state>/status.json` with mode `0600`. It includes a timestamp, issue state, blockers, diagnostics, and next action, and is also exposed by `status` and `inspect`. This is the latest state snapshot, not an append-only history.

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
    "environment_allowlist": ["LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"]
  },
  "status": {
    "format": "human",
    "color": "auto"
  }
}
```

Commands are argument arrays, not shell strings. Runtime code therefore executes the configured program without shell interpolation. The default noninteractive Codex implementation command uses `workspace-write` for source edits. A descriptor-owning boundary helper removes the delivered prompt's temporary name before launch and captures implementation or review stdout concurrently through an exclusively created private result file outside the assigned worktree or immutable snapshot; stderr remains in tmux for diagnostics. The entire stdout of default or custom implementation commands must be one `agent-symphony-result-v1` JSON object no larger than 64 KiB; reviewers must emit one bounded `agent-symphony-review-v1` object. A boundary-owned process-group leader remains alive after the configured command exits. On completion, overflow, or cancellation, the helper terminates only that still-owned group, drains the bounded capture, and then reaps its leader. If an out-of-group process keeps stdout open past the bounded drain, the helper closes its read side and fails the attempt; it does not claim to contain escaped processes. Results are retained for retry-safe export or review outcome persistence and never enter the source tree. The bounded implementation worker also requires the exact shared deterministic branch, worktree, and session plus a contained Git directory, commit-format base ancestry, and absent remotes and credential helpers. The default reviewer uses plain `codex exec --sandbox read-only -`, because Codex's built-in review subcommand owns its output format and cannot satisfy the strict result contract. Its prompt scopes review to the complete validated approved-base-through-attested-`HEAD` range, including preserved multi-commit custom-agent histories. Explicit reviewer arrays replace the default arguments unchanged; merged terminal output is never parsed as a result. Both paths normalize the child `TMPDIR` to `/tmp`. `environment_allowlist` is the complete set of inherited variable names available to implementation/review processes; add model-provider credentials explicitly. GitHub, Git askpass, SSH-agent, and cloud credential variables are forbidden even when listed. Arguments or assignments shaped like tokens, passwords, private keys, API keys, credentials, or authorization values are rejected so `config view` cannot disclose them. Dependencies are explicit issue references under the optional configured issue-body section. Completion defaults to human review.

## Attempt runtime troubleshooting

Production attempts use deterministic branch, directory, and tmux names beneath a provisioned execution root. By default that is a private (mode `0700`) `worktrees` directory under the runtime state root (`--runtime-state`); under the optional advanced host-isolated mode (see [Setup](setup.md)) it is instead the provisioned `/var/lib/agent-symphony/attempts` (Linux/WSL2) or `/var/db/agent-symphony/attempts` (macOS) root. `worktree_root` remains an offline/local configuration field and does not move production work outside that boundary. Recovery manifests and retained agent output remain under the coordinator state root's separate `attempts` directory; the manifest is diagnostic metadata, not workflow truth.

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
# Host isolation (advanced, optional)

By default no host isolation is installed: `agent-symphony agent-host implementation|review` runs as a plain, same-user subprocess of the coordinator, with no `sudo` and no separate OS identity — see [Setup](setup.md) and [Security](security.md). `agent-symphony install-host --coordinator USER` opts into the stronger advanced boundary; it provisions the documented macOS or Linux/WSL2 host boundary and must run as root from the installed versioned binary. Once installed, `agent-host` becomes the sudo-only bounded JSON adapter and is no longer an interactive operator command in either mode. Unsupported native Windows and WSL repositories under `/mnt/*` fail closed regardless of mode.
