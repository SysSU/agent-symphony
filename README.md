# Agent Symphony

Agent Symphony is a planned GitHub-native, multi-agent software delivery orchestrator inspired by [OpenAI Symphony](https://github.com/openai/symphony). Stakeholders define and prioritize work in GitHub Issues; an orchestrator coordinates agents through implementation, validation, documentation, pull-request review, and policy-controlled merge.

> [!NOTE]
> This repository is in the planning stage. The product described below is not implemented yet.

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

Installation and usage instructions will be added when the first executable MVP exists. Until then, review the [PRD](docs/PRD.md) and use GitHub Issues for planning and implementation work.
