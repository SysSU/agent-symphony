# CLI reference

Issue #6 provides a configuration and prerequisite shell. It does not start agents or mutate GitHub.

## Commands

```text
agent-symphony init [--config path] [--json]
agent-symphony validate [--config path] [--json]
agent-symphony config view [--config path] [--json]
agent-symphony doctor [--config path] [--json]
agent-symphony diagnostics [--config path] [--json]
```

- `init` creates a new config with conservative defaults and refuses to overwrite a file. It requires a GitHub `origin` in the current repository.
- `validate` requires the config file to be inside the resolved Git root. It rejects malformed input, duplicate JSON keys at any nesting depth, unknown keys, secret-shaped keys or command arguments, invalid policy values, duplicate/empty labels, unsafe command arguments, and paths that are absolute, traverse outside the repository, target Git metadata, or escape through symlinks. Worktree and documentation paths are always anchored at the Git root, not the config file's directory.
- `config view` prints the validated configuration. Invalid or secret-bearing files are never echoed.
- `doctor` and its `diagnostics` alias check the supported platform, WSL filesystem placement, Git, tmux, both configured commands, Git repository/remote identity, and GitHub connectivity/effective repository access.

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
    "reviewer": ["codex", "review"]
  },
  "status": {
    "format": "human",
    "color": "auto"
  }
}
```

Commands are argument arrays, not shell strings. Downstream runtime code can therefore execute the configured program without shell interpolation. Arguments or assignments shaped like tokens, passwords, private keys, API keys, credentials, or authorization values are rejected so `config view` cannot disclose them. Dependencies are explicit issue references under the configured issue-body section; issue parsing and enforcement belong to downstream intake/scheduler work. Completion defaults to human review. Raising concurrency only records policy; issue #6 does not dispatch work.

Secrets—including GitHub tokens, App keys, webhook secrets, passwords, and credentials—are forbidden in configuration. Supply temporary diagnostic authentication through `GITHUB_TOKEN` or `GH_TOKEN`; full App credential handling belongs to GitHub integration.

## Diagnostic boundaries

An unauthenticated probe can prove public GitHub connectivity but not write authority. An authenticated probe reports the repository access returned by GitHub, but cannot prove GitHub App-specific issue, pull-request, checks, webhook, repository-rules, or installation permissions. `doctor` reports those as actionable warnings.

On WSL, diagnostics resolve the Git root, choose the longest containing entry from `/proc/mounts`, and reject `drvfs` or `9p` mounts. Issue #6 also does not install or prove worker/reviewer OS identities, setgid roots, sudo rules, tmux isolation canaries, or the policy check. The future `install-host` and runtime implementation must turn those warnings into enforced preconditions before `serve` accepts work.
