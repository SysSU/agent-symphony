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

## Working Rules

- Understand the existing flow and search for established helpers and patterns before changing code.
- Fix root causes in the shared path used by all affected callers, not symptoms in individual call sites.
- Keep changes scoped. Do not add speculative abstractions, dependencies, configuration, or tests.
- Validate untrusted data at system boundaries, including requests, external APIs, storage, environment variables, and configuration.
- Do not hide type, lint, or build failures with suppression comments. If a verified tool limitation requires one, make it narrow and explain why.
- Preserve unrelated user changes and untracked files. Never use destructive Git commands or broad cleanup operations to resolve conflicts.
- Run the smallest relevant build, lint, and test checks before finishing, and report the actual results.
- Tests should verify behavior, branching, validation, side effects, or failure handling—not constants, type-only exports, or trivial re-exports.

## BMAD Artifacts

- Use `docs/` for durable product, architecture, UX, QA, operations, and decision records.
- Use `_bmad-output/planning-artifacts/` and `_bmad-output/implementation-artifacts/` for BMAD-generated working artifacts.
- Do not commit one-off query output, investigation scratch notes, or transient status dumps. Move only reusable knowledge into `docs/`.

## Git and Shared Resources

- Do not commit, merge, push, or rewrite history unless the user explicitly requests it.
- Use clear conventional commit messages when asked to commit. Do not add AI attribution or AI co-author lines.
- Never use blanket `pkill`, `killall`, or similar broad process termination. Target only processes started for this task.
- Avoid starting development servers unless verification needs one. Use an available non-default port when other agents may be active, and stop the server when finished.

## UI Changes

When changing user-facing UI, verify the affected view in a browser when the project can run locally:

- Check the accessibility structure and browser console.
- Exercise changed interactions.
- Inspect representative desktop and mobile layouts.
- Capture screenshots when visual correctness is part of the task.
