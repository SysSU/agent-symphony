# Agent Symphony

Agent Symphony is a planned GitHub-native, multi-agent software delivery orchestrator inspired by [OpenAI Symphony](https://github.com/openai/symphony). Stakeholders define and prioritize work in GitHub Issues; an orchestrator coordinates agents through implementation, validation, documentation, pull-request review, and policy-controlled merge.

> [!NOTE]
> Restart recovery joins authoritative attempt facts to bounded runtime manifests, prevents active/completed redispatch, and exposes corrective status. Feedback and validation handoffs are claimed durably through the same recovery boundary.

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
- [GitHub CLI integration](docs/github-cli.md)
- [Setup](docs/setup.md), [security](docs/security.md), [recovery](docs/recovery.md), and [troubleshooting](docs/troubleshooting.md)
- [Release validation and pilot evidence](docs/release-validation.md)
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

Go 1.26 is required to build from source. To run Agent Symphony, install Git, tmux, GitHub CLI, and Codex (or another coding agent), and make sure you can access the repository.

```sh
go build -o agent-symphony ./cmd/agent-symphony
./agent-symphony init
./agent-symphony validate
./agent-symphony config view
./agent-symphony doctor
./agent-symphony pr-governance --state /path/to/pr-state.json
./agent-symphony serve --state /path/to/pr-state.json --runtime-state /path/to/state
./agent-symphony status --state /path/to/pr-state.json --runtime-state /path/to/state
```

Before `serve`, install the release binary root-owned at `/usr/local/libexec/agent-symphony/<version>/agent-symphony`, then run `sudo .../agent-symphony install-host --coordinator "$USER"`. The coordinator uses the installed implementation and review `agent-host` sudo boundaries automatically; `AGENT_SYMPHONY_WORKER_BOUNDARY` and `AGENT_SYMPHONY_REVIEW_BOUNDARY` remain only test seams.

Authenticate GitHub CLI with `gh auth login`; for non-interactive use, `GH_TOKEN` or `GITHUB_TOKEN` may authenticate `gh` instead. `init` derives `owner/repository` from the GitHub `origin` and creates `.agent-symphony.yaml` without overwriting an existing file. Configuration is JSON, which is a valid YAML 1.2 subset and allows strict stdlib-only parsing. Agent Symphony does not parse or store GitHub credentials.

See the [CLI reference](docs/cli.md) for the schema, structured output, diagnostics, and current scope.

## Release validation

`scripts/validate-release.sh VERSION` runs race tests, vet, production-seam integration tests, cross-builds four runtime-independent binaries, proves archive reproducibility, strictly verifies each streamed archive and executable target, scans all regular candidate files (including ignored environment files) without printing matches, and checks the durable documentation set. It safely excludes only `.git`, `.worktrees`, and generated `dist`. It reports only the current host as locally passed. The GitHub Actions matrix supplies separate macOS, Linux, and WSL2 evidence; a release is not validated until all three jobs have links and successful results recorded in the pilot evidence template.
