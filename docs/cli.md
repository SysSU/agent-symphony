# CLI reference

Issue #6 provides a configuration and prerequisite shell. It does not start agents or mutate GitHub.

## Commands

```text
agent-symphony init [--config path] [--json]
agent-symphony validate [--config path] [--json]
agent-symphony config view [--config path] [--json]
agent-symphony doctor [--config path] [--json]
agent-symphony diagnostics [--config path] [--json]
agent-symphony pr-governance --state path [--config path] [--json]
```

- `init` creates a new config with conservative defaults and refuses to overwrite a file. It requires a GitHub `origin` in the current repository.
- `validate` requires the config file to be inside the resolved Git root. It rejects malformed input, duplicate JSON keys at any nesting depth, unknown keys, secret-shaped keys or command arguments, invalid policy values, duplicate/empty labels, unsafe command arguments, and paths that are absolute, traverse outside the repository, target Git metadata, or escape through symlinks. Worktree and documentation paths are always anchored at the Git root, not the config file's directory.
- `config view` prints the validated configuration. Invalid or secret-bearing files are never echoed.
- `doctor` and its `diagnostics` alias check the supported platform, WSL filesystem placement, Git, tmux, both configured commands, Git repository/remote identity, and GitHub connectivity/effective repository access.
- `pr-governance` is issue #10's one-shot pull-request governance command. It reads an existing recovery-state JSON file and durably writes feedback and validation handoffs, then exits without starting an agent. Issue #4 still owns daemon scheduling, production and consumption of that state, runtime resumption, and end-to-end wiring; those capabilities are not yet available. The command also requires `GITHUB_TOKEN`, `AGENT_SYMPHONY_GITHUB_APP_ID`, and `AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID` in the environment. The token must be a short-lived installation token for that App and is never printed.

Commands produce plain human-readable text by default and never depend on color. `NO_COLOR` is therefore honored without special handling. `--json` emits one JSON object with envelope `version: 1`, `command`, `ok`, and `data`, `diagnostics`, or `error` as applicable. A failing validation or diagnostic exits with status 1; command-line misuse exits with status 2.

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
    "implementation": ["codex", "exec"],
    "reviewer": ["codex", "review"],
    "environment_allowlist": ["HOME", "LANG", "LC_ALL", "PATH", "TERM", "TMPDIR"]
  },
  "status": {
    "format": "human",
    "color": "auto"
  }
}
```

Commands are argument arrays, not shell strings. Runtime code therefore executes the configured program without shell interpolation. `environment_allowlist` is the complete set of inherited variable names available to implementation/review processes; add model-provider credentials explicitly. GitHub, Git askpass, SSH-agent, and cloud credential variables are forbidden even when listed. Arguments or assignments shaped like tokens, passwords, private keys, API keys, credentials, or authorization values are rejected so `config view` cannot disclose them. Dependencies are explicit issue references under the configured issue-body section; issue parsing and enforcement belong to downstream intake/scheduler work. Completion defaults to human review.

## Attempt runtime troubleshooting

Local attempts use deterministic branch, directory, and tmux names. A manifest and retained agent output live below the coordinator state root; the manifest is diagnostic metadata, not workflow truth.

The runtime must be given a non-nil worker-identity verification hook and fails closed before creating resources when it is absent or fails. This hook is the integration point for the already-provisioned `sudo` agent-host mode; installing or provisioning that host is outside the runtime. Environment inheritance happens only after verification through the shared agent environment filter. In particular, inherited `HOME` must be the worker account's home after the identity switch, never the coordinator's home.

- If launch fails, inspect the manifest `diagnostic` and `agent.log`. Failed resources are retained intentionally.
- If an attempt appears active after restart, compare its manifest, worktree HEAD, and `tmux has-session -t <session>` before resuming. Never attach to a session or directory whose deterministic identity does not match.
- Cancellation sends `C-c`, waits briefly, then kills only the named attempt session. It does not remove the worktree, so partial work and diagnostics remain available.
- An attempt worktree has no remote and a disabled local credential helper. A successful `git push` from it indicates a broken host boundary; stop serving work and rerun diagnostics.
- “resources already exist” is a safety stop. Reconcile the recorded attempt instead of deleting or adopting resources by hand.

Secrets—including GitHub tokens, App keys, webhook secrets, passwords, and credentials—are forbidden in configuration. Supply temporary diagnostic authentication through `GITHUB_TOKEN` or `GH_TOKEN`; full App credential handling belongs to GitHub integration.

## Diagnostic boundaries

An unauthenticated probe can prove public GitHub connectivity but not write authority. An authenticated probe reports the repository access returned by GitHub, but cannot prove GitHub App-specific issue, pull-request, checks, webhook, repository-rules, or installation permissions. `doctor` reports those as actionable warnings.

On WSL, diagnostics resolve the Git root, choose the longest containing entry from `/proc/mounts`, and reject `drvfs` or `9p` mounts. Issue #6 also does not install or prove worker/reviewer OS identities, setgid roots, sudo rules, tmux isolation canaries, or the policy check. The future `install-host` and runtime implementation must turn those warnings into enforced preconditions before `serve` accepts work.
