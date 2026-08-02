# Agent Symphony

Agent Symphony is a planned GitHub-native, multi-agent software delivery orchestrator inspired by [OpenAI Symphony](https://github.com/openai/symphony). Stakeholders define and prioritize work in GitHub Issues; an orchestrator coordinates agents through implementation, validation, documentation, pull-request review, and policy-controlled merge.

> [!NOTE]
> GitHub intake, configuration, scheduling policy, and the local attempt runtime are implemented. Pull-request automation and merge behavior are not implemented yet.

## Planned MVP

- GitHub Issues as the exclusive work-intake and implementation record
- P1-P3 priority, readiness, dependency, and completion policies
- Safe parallel execution in isolated Git worktrees and tmux sessions
- Long-lived pull-request feedback handling
- Human-review gates and repository-policy-controlled merge
- Living documentation updated with implementation changes
- Restart-safe reconciliation and terminal-based operational status
- macOS, Linux, and Windows through WSL

The browser dashboard, multi-repository orchestration, GitHub Projects synchronization, and inferred dependencies are post-MVP work.

## Documentation

- [Product Requirements Document](docs/PRD.md)
- [MVP Architecture](docs/architecture.md)
- [GitHub App setup and security](docs/github-app.md)
- [Agent development guidance](AGENTS.md)

Durable project documentation belongs in `docs/`. BMAD working output belongs in `_bmad-output/` and is ignored by Git.

## Development Workflow

GitHub Issues are the source of truth for implementation state. Before implementation begins, an issue must contain:

- Request context and supporting evidence
- Acceptance criteria
- Task checklist
- Validation expectations

Keep the issue current during development, link its pull request, and record validation evidence before review. See [AGENTS.md](AGENTS.md) for the complete repository rules.

## Getting Started

Go 1.26 is required to build from source. The resulting executable is self-contained; operators also need Git, tmux, configured implementation/reviewer executables, repository access, and GitHub connectivity.

```sh
go build -o agent-symphony ./cmd/agent-symphony
./agent-symphony init
./agent-symphony validate
./agent-symphony config view
./agent-symphony doctor
```

`init` derives `owner/repository` from the GitHub `origin` and creates `.agent-symphony.yaml` without overwriting an existing file. Configuration is JSON, which is a valid YAML 1.2 subset and allows strict stdlib-only parsing. Keep credentials outside this committed file. `doctor` may use `GITHUB_TOKEN` or `GH_TOKEN` for a read-only effective-access probe; it never prints the value.

See the [CLI reference](docs/cli.md) for the schema, structured output, diagnostics, and current scope.
