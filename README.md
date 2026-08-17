# Agent Symphony

Agent Symphony is a GitHub-native, multi-agent software delivery orchestrator inspired by [OpenAI Symphony](https://github.com/openai/symphony). It coordinates coding agents from a ready GitHub Issue through implementation, validation, review, and policy-controlled merge.

## Features

- Uses GitHub Issues as the work queue and implementation record
- Prioritizes work and respects readiness, dependency, and completion policies
- Runs agents safely in isolated Git worktrees and tmux sessions
- Handles pull-request feedback and validation across multiple agent runs
- Provides a per-repository browser dashboard for monitoring attempts, opening live tmux sessions, and cleaning up local resources
- Leaves non-autonomous pull requests open for review and supports explicit autonomous merge
- Keeps project documentation aligned with implementation changes
- Recovers safely after restarts and reports operational status in the terminal
- Runs on macOS, Linux, and Windows through WSL2
- Supports independent single-repository daemons on the same host

## Getting Started

Download the archive for your platform from [GitHub Releases](https://github.com/SysSU/agent-symphony/releases). You also need Git, tmux, [GitHub CLI](https://cli.github.com/), and Codex or another coding agent.

After installing the binary, authenticate GitHub CLI and initialize Agent Symphony inside the repository you want it to manage:

```sh
gh auth login
cd /path/to/repository
agent-symphony init
agent-symphony validate
```

Start continuous reconciliation and inspect its status:

```sh
agent-symphony serve \
  --state ~/.local/state/agent-symphony/pr.json \
  --runtime-state ~/.local/state/agent-symphony

agent-symphony status \
  --state ~/.local/state/agent-symphony/pr.json \
  --runtime-state ~/.local/state/agent-symphony
```

`serve` prints the dashboard URL. It is reachable only from the local computer by default; read the [security guide](docs/security.md) before enabling network access.

See the [setup guide](docs/setup.md) for installation, configuration, and your first issue.

## Development

Run the repository lint gate before review:

```sh
scripts/lint.sh
```

It checks Go formatting, `go vet`, the pinned Staticcheck tool, and dashboard ESLint rules after installing the exact locked development dependencies.

## Documentation

- [Setup](docs/setup.md)
- [GitHub controls](docs/github-controls.md)
- [CLI reference](docs/cli.md)
- [Troubleshooting](docs/troubleshooting.md)
- [Security](docs/security.md)
- [Recovery](docs/recovery.md)
- [Architecture](docs/architecture.md)
- [Product requirements](docs/PRD.md)
- [Releases](https://github.com/SysSU/agent-symphony/releases)

Documentation on `main` describes `main`. For an installed release, read the documentation at its matching [Git tag](https://github.com/SysSU/agent-symphony/tags).

See [AGENTS.md](AGENTS.md) for contributor guidance.
