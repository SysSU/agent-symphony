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
- `doctor` and its `diagnostics` alias check the supported platform, WSL filesystem placement, Git, tmux, both configured commands, Git repository/remote identity, and GitHub connectivity/effective repository access. `--offline` skips only the network probe and emits an explicit warning.
- `pr-governance` is a one-shot pull-request governance command. It creates an empty private recovery-state JSON file when the named file is absent, then durably writes feedback and validation handoffs. Recovery claims those handoffs before they cross into the isolated runtime. The command also requires `GITHUB_TOKEN`, `AGENT_SYMPHONY_GITHUB_APP_ID`, and `AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID` in the environment. The token must be a short-lived installation token for that App and is never printed.
- `serve --state path --runtime-state path` verifies the installation token, acquires a non-following single-instance lock, reconciles immediately, and repeats at most every 60 seconds across bounded GitHub failures. Every cycle has a whole-cycle two-minute deadline. `reconcile` performs one production cycle; `status`, `list`, and `inspect` refresh and expose the same queued, active, blocked, review-ready, and completed projection. All require `AGENT_SYMPHONY_GITHUB_APP_ID` and `AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID`, plus one of two credential sources: `AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH` and `AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID` together mint and auto-refresh installation tokens from the App's own key — the only way `serve` stays authenticated past the 1-hour lifetime of a single token — or a static `GITHUB_TOKEN`, sufficient for a one-shot `reconcile`/`status`/`list`/`inspect` but not for a long-running `serve`. Supplying `--attempts path` selects the nonmutating offline diagnostic instead and needs neither. `serve` optionally also binds an HTTP webhook listener when `AGENT_SYMPHONY_WEBHOOK_ADDR`, `AGENT_SYMPHONY_WEBHOOK_SECRET`, and `AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID` are all set — a valid signed delivery wakes the next reconcile early, but periodic polling remains the sole authoritative recovery path either way; leaving these three unset (the default) means `serve` relies on polling alone, exactly as before this existed.

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
    "reviewer": ["codex", "review"],
    "environment_allowlist": ["LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"]
  },
  "status": {
    "format": "human",
    "color": "auto"
  }
}
```

Commands are argument arrays, not shell strings. Runtime code therefore executes the configured program without shell interpolation. The default noninteractive Codex implementation command uses `workspace-write` for source edits. A descriptor-owning boundary helper removes the delivered prompt's temporary name before launch and captures implementation stdout concurrently through an exclusively created private result file beside—but outside—the assigned worktree; stderr remains in tmux for diagnostics. The entire stdout of default or custom implementation commands must be one `agent-symphony-result-v1` JSON object no larger than 64 KiB. A boundary-owned process-group leader remains alive after the configured command exits. On completion, overflow, or cancellation, the helper terminates only that still-owned group, drains the bounded capture, and then reaps its leader. The retained result is reread for safe export retries and never enters the worktree. The bounded implementation worker also requires the exact shared deterministic branch, worktree, and session plus a contained Git directory, commit-format base ancestry, and absent remotes and credential helpers. The default `codex review` remains read-only and its output stays in tmux. Both paths normalize the child `TMPDIR` to `/tmp`. `environment_allowlist` is the complete set of inherited variable names available to implementation/review processes; add model-provider credentials explicitly. GitHub, Git askpass, SSH-agent, and cloud credential variables are forbidden even when listed. Arguments or assignments shaped like tokens, passwords, private keys, API keys, credentials, or authorization values are rejected so `config view` cannot disclose them. Dependencies are explicit issue references under the configured issue-body section; issue parsing and enforcement belong to downstream intake/scheduler work. Completion defaults to human review.

## Attempt runtime troubleshooting

Production attempts use deterministic branch, directory, and tmux names beneath a provisioned execution root. By default that is a private (mode `0700`) `worktrees` directory under the runtime state root (`--runtime-state`); under the optional advanced host-isolated mode (see [Setup](setup.md)) it is instead the provisioned `/var/lib/agent-symphony/attempts` (Linux/WSL2) or `/var/db/agent-symphony/attempts` (macOS) root. `worktree_root` remains an offline/local configuration field and does not move production work outside that boundary. Recovery manifests and retained agent output remain under the coordinator state root's separate `attempts` directory; the manifest is diagnostic metadata, not workflow truth.

The runtime requires the boundary's verification hook and fails closed before creating resources when it is absent or fails, in either mode. Environment inheritance happens only after verification through the shared agent environment filter. `HOME` is forbidden in configured allowlists; the boundary supplies the resolved account's home afterward — the coordinator's own home by default, or the provisioned worker/reviewer's home under advanced host isolation.

- If launch fails, inspect the manifest `diagnostic` and `agent.log`. Failed resources are retained intentionally.
- If an attempt appears active after restart, compare its manifest, worktree HEAD, and `tmux has-session -t <session>` before resuming. Never attach to a session or directory whose deterministic identity does not match.
- Cancellation sends `C-c`, waits briefly, then kills only the named attempt session. It does not remove the worktree, so partial work and diagnostics remain available.
- An attempt worktree has no remote and a disabled local credential helper. A successful `git push` from it indicates a broken host boundary; stop serving work and rerun diagnostics.
- “resources already exist” is a safety stop. Reconcile the recorded attempt instead of deleting or adopting resources by hand.

Secrets—including GitHub tokens, App keys, webhook secrets, passwords, and credentials—are forbidden in configuration. Supply temporary diagnostic authentication through `GITHUB_TOKEN` or `GH_TOKEN`; full App credential handling belongs to GitHub integration.

## Diagnostic boundaries

An unauthenticated probe can prove public GitHub connectivity but not write authority. An authenticated probe reports the repository access returned by GitHub, but cannot prove GitHub App-specific issue, pull-request, checks, webhook, repository-rules, or installation permissions. `doctor` reports those as actionable warnings.

On WSL, diagnostics resolve the Git root, choose the longest containing entry from `/proc/mounts`, and reject `drvfs` or `9p` mounts. `serve` fails closed when its runtime boundary cannot be verified, whether that boundary is the zero-admin default or an administrator-provisioned advanced host-isolated install.

## Release commands

`scripts/release.sh VERSION [OUTPUT_DIR]` creates reproducible no-CGO archives and `SHA256SUMS` without overwriting existing output. `scripts/validate-release.sh [VERSION]` runs the complete local release gate. These repository scripts are maintainer commands, not installed CLI subcommands.
# Host isolation (advanced, optional)

By default no host isolation is installed: `agent-symphony agent-host implementation|review` runs as a plain, same-user subprocess of the coordinator, with no `sudo` and no separate OS identity — see [Setup](setup.md) and [Security](security.md). `agent-symphony install-host --coordinator USER` opts into the stronger advanced boundary; it provisions the documented macOS or Linux/WSL2 host boundary and must run as root from the installed versioned binary. Once installed, `agent-host` becomes the sudo-only bounded JSON adapter and is no longer an interactive operator command in either mode. Unsupported native Windows and WSL repositories under `/mnt/*` fail closed regardless of mode.
