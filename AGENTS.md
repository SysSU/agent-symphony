# AGENTS.md

This file is the shared source of truth for coding agents working in this repository. Keep setup and project usage in `README.md`; keep agent-specific development guidance here.

## GitHub-Only Implementation Workflow

GitHub Issues are the sole source of truth for implementation state.

- Do not implement bugs, features, spikes, or tasks directly from chat. Every unit of implementation work must start from, or be attached to, a GitHub issue.
- Before implementation, record the request, relevant evidence and context, acceptance criteria, task checklist, and validation expectations in the issue.
- Keep the issue current while working: note when work starts, check off completed tasks, record blockers and scope changes, and link the pull request.
- Before requesting review, add the actual validation evidence to the issue or pull request.
- Do not create local BMAD story files such as `docs/stories/*.story.md`; use the GitHub issue instead.
- Use chat and messaging tools for discussion and notifications only. Record decisions in the GitHub issue before continuing implementation.

## Isolated Worktrees

All issue implementation must occur in a dedicated Git worktree. Keep the primary checkout for coordination, inspection, and integration only.

- Create one branch and worktree per GitHub issue before editing files. Use a descriptive branch such as `issue-<number>-<slug>` and record the branch and worktree path in the issue when work starts.
- Never implement two issues in the same worktree or let multiple agents edit the same worktree concurrently.
- Base a worktree on the current approved integration branch. If required commits are unavailable locally, fetch them without overwriting local or unrelated work.
- Create every issue worktree under `<project-root>/.worktrees/<issue-branch>`. Keep `.worktrees/` ignored by Git, and never create a worktree inside another issue worktree.
- Run relevant validation inside the issue worktree. When authorized to publish changes, commit and push only the issue branch, then open or update its pull request. Never push issue changes directly to the integration branch.
- Deliver every implementation change through a pull request before integration. Do not merge locally or bypass the pull request, required reviews, checks, or branch protections.
- Before integration, reconcile dependencies and overlapping files with active issue branches. Do not merge around unresolved conflicts or required GitHub checks.
- Remove a worktree only after its branch is safely integrated or explicitly abandoned and any needed evidence is preserved. Remove only the exact worktree created for the issue; never use broad cleanup commands.
- The orchestrator may inspect every worktree, but implementation sub-agents must be scoped to their assigned issue worktree and tmux session.

## Working Rules

- Understand the existing flow and search for established helpers and patterns before changing code.
- Fix root causes in the shared path used by all affected callers, not symptoms in individual call sites.
- Keep changes scoped. Do not add speculative abstractions, dependencies, configuration, or tests.
- Validate untrusted data at system boundaries, including requests, external APIs, storage, environment variables, and configuration.
- Do not hide type, lint, or build failures with suppression comments. If a verified tool limitation requires one, make it narrow and explain why.
- Preserve unrelated user changes and untracked files. Never use destructive Git commands or broad cleanup operations to resolve conflicts.
- Before committing code changes, run `scripts/lint.sh` and resolve every failure.
- Run the smallest relevant build, lint, and test checks before finishing, and report the actual results.
- Write documentation in plain English. Be direct and concise, cut filler, prefer common words to jargon, and briefly define any technical term readers need.

## Test Practices

Tests must protect behavior that users and operators depend on. Do not add a test only to satisfy a checklist, increase a count, or execute code without a meaningful assertion.

- Start with the regression or risk. The issue or pull request must say what behavior the test protects and what failure it would catch. For a bug fix, add a test that fails before the fix and passes after it.
- Prioritize core user journeys and externally observable results. Assert the outcome a user, operator, API client, or dependent component sees—not private call order, internal fields, or incidental markup.
- Use the narrowest test level that provides real confidence:
  - Unit tests cover branching, validation, state transitions, transformations, and failure handling within one component.
  - Integration tests cover boundaries between components or processes, including GitHub, tmux, the filesystem, runtime services, and dashboard state.
  - End-to-end tests cover critical operator workflows in the running system. Add them when the behavior crosses the full stack and the environment can support a reliable test. If an end-to-end test is not practical, record the constraint and the closest integration coverage.
- Cover the successful path plus important failures and edge cases. Use table-driven Go tests when several meaningful cases share the same setup. Use Go fuzzing for parsers and untrusted input boundaries when the risk justifies it, and the race detector when concurrency behavior changes.
- Keep tests isolated and deterministic. Control test data, time, filesystem state, and network boundaries. Mock an external dependency only at its boundary; do not write a test whose only evidence is that its mock was called.
- For dashboard flows, use the existing Playwright setup. Interact through visible text, roles, labels, and other accessible contracts; use web-first assertions instead of fixed sleeps or CSS selectors tied to implementation details.
- Reject low-value tests that check constants, type-only exports, trivial re-exports, copied production logic, implementation snapshots, or unconditional success. A useful test fails for a plausible product regression and tells the reader which behavior broke.
- Run the smallest relevant checks while developing, then the required broader checks before review. Typical commands are `go test ./...`, `npm --prefix cmd/agent-symphony/dashboard test`, and `npm --prefix cmd/agent-symphony/dashboard run test:browser`. Record the exact commands and results in the issue or pull request.
- Do not invent runtime tests for documentation-only or other non-runtime changes. Record why unit, integration, or end-to-end coverage does not apply and run the appropriate structural or content validation instead.

Research references: [Go table-driven tests](https://go.dev/wiki/TableDrivenTests), [Go fuzzing](https://go.dev/doc/security/fuzz/), [Playwright best practices](https://playwright.dev/docs/best-practices), [Testing Library guiding principles](https://testing-library.com/docs/), and [Google guidance on change-detector tests](https://testing.googleblog.com/2015/01/testing-on-toilet-change-detector-tests.html).

## BMAD Artifacts

- Use `docs/` for durable product, architecture, UX, QA, operations, and decision records.
- Use `_bmad-output/planning-artifacts/` and `_bmad-output/implementation-artifacts/` for BMAD-generated working artifacts.
- Do not commit one-off query output, investigation scratch notes, or transient status dumps. Move only reusable knowledge into `docs/`.

## Git and Shared Resources

- Do not commit, merge, push, or rewrite history unless the user explicitly requests it.
- Follow the [release runbook](docs/releases.md) and [release validation policy](docs/release-validation.md) when preparing a release.
- Use clear conventional commit messages when asked to commit. Do not add AI attribution or AI co-author lines.
- Never use blanket `pkill`, `killall`, or similar broad process termination. Target only processes started for this task.
- Avoid starting development servers unless verification needs one. Use an available non-default port when other agents may be active, and stop the server when finished.

## UI Changes

When changing user-facing UI, verify the affected view in a browser when the project can run locally:

- Check the accessibility structure and browser console.
- Exercise changed interactions.
- Inspect representative desktop and mobile layouts.
- Capture screenshots when visual correctness is part of the task.
