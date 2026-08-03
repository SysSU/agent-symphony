# Agent Symphony

Agent Symphony is a planned GitHub-native, multi-agent software delivery orchestrator inspired by [OpenAI Symphony](https://github.com/openai/symphony). Stakeholders define and prioritize work in GitHub Issues; an orchestrator coordinates agents through implementation, validation, documentation, pull-request review, and policy-controlled merge.

> [!NOTE]
> Restart recovery joins authoritative attempt facts to bounded runtime manifests, prevents active/completed redispatch, and exposes corrective status. Issue #10's feedback and validation handoffs are claimed durably through the same recovery boundary.

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

Go 1.26 is required to build from source. The resulting executable is self-contained; operators also need Git, tmux, configured implementation/reviewer executables, repository access, and GitHub connectivity.

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

`init` derives `owner/repository` from the GitHub `origin` and creates `.agent-symphony.yaml` without overwriting an existing file. Configuration is JSON, which is a valid YAML 1.2 subset and allows strict stdlib-only parsing. Keep credentials outside this committed file. `doctor` may use `GITHUB_TOKEN` or `GH_TOKEN` for a read-only effective-access probe; it never prints the value.

See the [CLI reference](docs/cli.md) for the schema, structured output, diagnostics, and current scope.

## Release validation

`scripts/validate-release.sh VERSION` runs race tests, vet, production-seam integration tests, cross-builds four runtime-independent binaries, proves archive reproducibility, strictly verifies each streamed archive and executable target, scans all regular candidate files (including ignored environment files) without printing matches, and checks the durable documentation set. It safely excludes only `.git`, `.worktrees`, and generated `dist`. It reports only the current host as locally passed. The GitHub Actions matrix supplies separate macOS, Linux, and WSL2 evidence; a release is not validated until all three jobs have links and successful results recorded in the pilot evidence template.
