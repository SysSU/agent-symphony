---
stepsCompleted:
  - step-01-init
  - step-02-discovery
  - step-02b-vision
  - step-02c-executive-summary
  - step-03-success
  - step-04-journeys
  - step-05-domain
  - step-06-innovation
  - step-07-project-type
  - step-08-scoping
  - step-09-functional
  - step-10-nonfunctional
  - step-11-polish
  - step-12-complete
inputDocuments:
  - https://github.com/openai/symphony
documentCounts:
  productBriefs: 0
  research: 1
  brainstorming: 0
  projectDocs: 0
workflowType: prd
classification:
  projectType: developer_tool
  domain: software-development workflow automation
  complexity: low regulatory complexity with high technical risk
  projectContext: greenfield
---

# Product Requirements Document - agent-symphony

**Author:** Seldon Stone
**Date:** 2026-08-01

## Executive Summary

Agent Symphony is a GitHub-native, multi-agent software delivery orchestrator for project stakeholders, including developers, product managers, maintainers, and owners. A stakeholder creates one or more GitHub issues, assigns priority and readiness metadata, and delegates execution without supervising individual agent turns.

The orchestrator continuously evaluates the backlog, prioritizes work from P1 through P3, identifies dependencies and conflicts, and decides which issues can run safely in parallel. It assigns work to sub-agents operating in isolated Git worktrees and observable tmux sessions. Each work item proceeds through implementation, validation, documentation, pull-request creation, and either human review or policy-authorized merge.

GitHub Issues are the exclusive work-intake mechanism and single source of truth for requirements, decisions, implementation state, progress, validation evidence, and completion. Pull requests, reviews, checks, and branch protections govern delivery. Merge is permitted only when repository-required checks, reviews, permissions, and protections are satisfied.

MVP CLI status exposes the backlog, active and queued work, dependencies, agents, tmux sessions, worktrees, issues, pull requests, checks, blockers, and expected next actions. A per-repository browser dashboard presents the same projection, attaches to exact live tmux sessions, and provides confirmed cleanup of exact completed or orphaned local attempts without mutating GitHub work policy. The orchestrator recovers from restarts without duplicating active or completed work.

Documentation is part of the definition of done. Every pull request assesses documentation impact and updates affected PRDs, `README.md`, and durable project documentation in the same branch. Documentation remains versioned in the repository, defaults to `/docs`, and supports configurable repository-local paths.

### What Makes This Special

Agent Symphony extends the Symphony model from autonomous issue execution to autonomous portfolio orchestration. Its central capability is reasoning across multiple work items: urgency, dependency order, resource conflicts, safe concurrency, review policy, and documentation impact.

Users manage intent, priority, and governance rather than agent sessions. Execution remains inspectable through GitHub, tmux, worktrees, and CLI status. Human approval remains available where required, while eligible work can proceed through merge without continuous supervision.

## Project Classification

- **Project type:** Developer tool
- **Domain:** Software-development workflow automation
- **Complexity:** Low regulatory complexity with high technical risk around autonomous changes, permissions, concurrency, and recovery
- **Context:** Greenfield
- **Primary interface:** GitHub Issues and pull requests
- **Secondary interface:** CLI operational status and a loopback-by-default per-repository browser dashboard

## Success Criteria

### User Success

- A stakeholder can create a valid GitHub issue, mark it ready, and delegate it without monitoring individual agent turns.
- At least 80% of eligible pilot issues reach a reviewable PR or policy-authorized merge without stakeholder intervention.
- Ready P1 issues begin within five minutes when execution capacity is available.
- CLI status explains what is running, queued, blocked, or complete without requiring tmux or filesystem access.
- Every completed issue links its PR, validation evidence, documentation assessment, and final outcome.

### Business Success

- Within three months, one production repository uses Agent Symphony for at least 20 issues.
- At least 70% of participating stakeholders continue delegating work after their first completed issue.
- Stakeholders report at least a 50% reduction in time spent coordinating agent execution.
- No pilot repository abandons the product because work state or agent actions cannot be understood.

### Technical Success

- GitHub remains the sole implementation source of truth; no orchestration state contradicts issue, PR, review, or check state.
- Restart recovery produces no duplicate active execution or duplicate PR for the same attempt.
- P1 work takes precedence over P2 and P3 while respecting dependencies and already-running safe work.
- Conflicting issues do not execute concurrently; independent issues can.
- Every active task has an isolated worktree and identifiable tmux session.
- No GitHub credential is exposed to a sub-agent environment or persisted in logs.
- Agents cannot bypass repository permissions, required checks, reviews, or branch protections.
- Every PR records documentation impact and updates affected repository documentation before completion.
- Orchestration events and failures are traceable to an issue and execution attempt.

### Measurable Outcomes

- 100% of execution attempts originate from an eligible GitHub issue.
- 100% of active attempts appear in CLI status with issue, agent, tmux, worktree, and PR data when available.
- 100% of merged agent PRs satisfy configured completion policy.
- 100% of repeated polling and restart recovery scenarios avoid duplicate dispatch.
- At least two independent issues can execute concurrently in the MVP.
- Blocked or failed work is reflected in GitHub and CLI status within one minute.
- Documentation-impact validation runs for every agent-created PR.

## Product Scope

**MVP approach:** Problem-solving MVP. Prove that one orchestrator can safely take a prioritized GitHub backlog through multi-agent implementation, review, documentation, and merge with minimal stakeholder supervision.

**Resource requirements:** One experienced systems developer familiar with GitHub CLI, Git, process supervision, and CLI tooling; one pilot repository; stakeholder access for workflow validation and review-policy testing.

### MVP - Minimum Viable Product

- One configured GitHub repository accessible through an authenticated GitHub CLI session.
- GitHub Issue intake using configurable readiness, priority P1-P3, and completion-policy metadata.
- Backlog reconciliation, dependency-aware prioritization, and safe parallel dispatch.
- Orchestrator-created sub-agents in isolated Git worktrees and tmux sessions.
- Implementation, validation, documentation updates, branch creation, and PR lifecycle handling.
- Human-review and policy-authorized merge completion modes.
- Restart-safe claim and recovery behavior.
- Human-readable and structured CLI status for backlog, queue, attempts, agents, tmux sessions, worktrees, issues, PRs, checks, blockers, and next actions.
- Repository-local configuration for worktree and documentation paths.
- Audit trail linked to GitHub issues and PRs.

### Growth Features (Post-MVP)

- Multiple repositories per coordinator instance.
- GitHub Projects priority and status synchronization.
- Improved dependency inference and workload forecasting.
- Configurable agent roles, models, concurrency limits, and execution policies.
- Notifications and operational history.
- Richer review-feedback and rework orchestration.

### Vision (Future)

- Organization-wide portfolio orchestration across repositories.
- Dynamic specialist teams selected per issue.
- Cross-repository dependency planning and coordinated releases.
- Predictive scheduling based on risk, capacity, and historical performance.
- Governance policies for regulated or high-assurance environments.

### Risk Mitigation

- Require explicit dependencies in the MVP and serialize work when safe parallelism is uncertain.
- Use idempotent event handling and deterministic attempt identity to prevent duplicate work.
- Reconcile GitHub state before dispatch, feedback handling, and merge.
- Default to human review until repository policy explicitly permits autonomous merge.
- Reconstruct lost execution context from GitHub and bounded local metadata.
- Validate correct prioritization, avoided conflicts, and reduced stakeholder supervision in a pilot repository.
- Ship sequential execution first if resources constrain safe parallel execution.
- Validate Linux first, then macOS and WSL before declaring cross-platform MVP completion.

## User Journeys

### Journey 1: Delegating a Backlog

Alex maintains a repository with several approved issues but lacks time to coordinate implementation. Alex applies readiness and P1-P3 priority metadata to multiple issues. The orchestrator evaluates dependencies, identifies independent work, and dispatches safe tasks in parallel.

Alex checks CLI status later and sees each issue's queue position, assigned agents, tmux session, worktree, branch, checks, and PR. Independent work progressed concurrently; dependent work remained queued with an explanation. Eligible PRs merged automatically, while review-required PRs wait for Alex.

This journey requires bulk backlog evaluation, priority scheduling, dependency analysis, safe concurrency, policy-based completion, and observable execution.

### Journey 2: Reviewing Sensitive Work

Alex marks an issue `needs-human-review`. When the orchestrator opens the PR, it applies the same label and publishes a required review-policy Check. The PR cannot merge while that label remains, regardless of otherwise successful validation.

The orchestrator continuously reconciles open agent PRs. When an authorized stakeholder posts an actionable review comment, the orchestrator promptly attaches it to the existing issue and execution context, then delegates the change to the appropriate sub-agent. This remains active for the full lifetime of the PR, even if new feedback arrives weeks later.

The sub-agent updates code, tests, and affected documentation in the same worktree and PR. The orchestrator responds to or resolves handled feedback and reruns relevant validation. After stakeholders finish reviewing, they remove `needs-human-review`; the required Check passes only if all other completion conditions are satisfied, allowing normal repository merge policy to proceed.

This journey requires synchronized issue and PR review labels, an enforceable required Check, long-lived PR monitoring, authorized feedback handling, preserved execution context, documentation gating, validation evidence, and branch-protection compliance.

### Journey 3: Recovering Blocked Work

An agent discovers that an issue lacks required acceptance criteria or conflicts with another active change. It stops before making unsafe assumptions, records the blocker on the GitHub issue, and updates CLI status.

Alex sees why the task is blocked, what information is needed, and whether other work can continue. After Alex updates the issue, the orchestrator reevaluates it and resumes or safely restarts execution without creating a duplicate branch or PR.

This journey requires boundary validation, blocker reporting, resumable execution, idempotent dispatch, conflict detection, and clear recovery actions.

### Journey 4: Operating and Configuring the System

Alex configures repository-local policies for readiness labels, priorities, completion modes, concurrency, worktree placement, documentation paths, and agent behavior. Configuration validation catches an invalid path before work begins.

After an orchestrator restart, Alex runs CLI status and sees active work reconciled from GitHub and preserved execution state. No completed or active issue is dispatched twice. When investigating a failure, Alex can trace the issue through its attempt, agent, tmux session, worktree, logs, branch, checks, and PR.

This journey requires versioned configuration, trust-boundary validation, restart reconciliation, auditability, and operational diagnostics.

### Journey Requirements Summary

- GitHub-native issue intake, priority, readiness, and completion policies
- Backlog scheduling with dependency and conflict awareness
- Parallel isolated worktrees and tmux sessions
- Read-only operational visibility
- Human-review and autonomous-merge paths
- Issue-to-PR review-policy synchronization enforced by a required Check
- Long-lived monitoring and handling of authorized, actionable PR feedback
- Blocker, retry, cancellation, and restart recovery
- Same-PR code, validation, and documentation changes
- Repository-local configuration with safe defaults
- End-to-end traceability through GitHub and execution metadata

## Innovation & Novel Patterns

### Detected Innovation Areas

- **Portfolio-level orchestration:** The system reasons across a prioritized backlog, dependencies, conflicts, capacity, and safe concurrency rather than dispatching issues independently.
- **Persistent multi-agent collaboration:** An orchestrator creates and coordinates specialized sub-agents while preserving issue, worktree, tmux, branch, and PR context across long-running work.
- **Long-lived review responsiveness:** Open agent PRs remain active orchestration objects; authorized feedback can restart work days or weeks later.
- **GitHub-enforced governance:** Issue metadata drives PR labels and required Checks, making human-review policy enforceable through native merge controls.
- **Documentation-aware completion:** Documentation impact is assessed and resolved inside the same delivery lifecycle as code.

The innovation claim is the integration and operating model: stakeholder-controlled autonomy across a GitHub backlog with explainable scheduling and native governance. It is not a claim that individual scheduling, tmux, worktree, or agent techniques are unprecedented.

### Market Context & Competitive Landscape

OpenAI Symphony already provides isolated issue execution, orchestration, retries, observability, and a GitHub Issues adapter. Agent Symphony differentiates through deeper GitHub lifecycle integration and reasoning across multiple issues and agents.

The MVP should reuse Symphony's proven boundaries where practical and validate the narrower claim that one orchestrator can schedule and govern a backlog more effectively than independent issue workers.

### Validation Approach

- Run mixed backlogs containing P1-P3, dependent, conflicting, and independent issues.
- Verify deterministic priority ordering, correct dependency handling, and explainable parallelization decisions.
- Measure avoided conflicts and correct prioritization alongside throughput.
- Test cyclic dependencies, contradictory priorities, repeated reconciliation, stale review comments, force-pushed branches, revoked permissions, and issues modifying overlapping files.
- Leave a review-gated PR open, then submit authorized feedback after an extended delay and confirm contextual resumption.
- Restart the orchestrator during active work and verify no duplicate attempts or PRs.
- Compare stakeholder supervision time against manually coordinated agent runs.
- Confirm every merged PR satisfies review, validation, and documentation policy.

### Risk Mitigation

- When dependency, authorization, or merge safety is uncertain, reduce autonomy, serialize or block the work, and explain the decision in GitHub and CLI status.
- If dependency inference is uncertain, serialize work or require explicit issue relationships.
- If autonomous merge confidence is insufficient, default to human review.
- If multi-agent coordination creates excessive conflicts, cap concurrency per repository.
- If long-lived execution state cannot be safely resumed, reconstruct from GitHub and start a new traceable attempt.
- Keep GitHub authoritative; dashboard presentation state may hide locally archived/abandoned cards but must not become a parallel task database or GitHub workflow mutation surface.

## Developer Tool Specific Requirements

### Project-Type Overview

Agent Symphony is a terminal-first orchestration service operated through a cross-platform CLI and a per-repository loopback-by-default dashboard. The MVP supports macOS, Linux, and Windows through WSL. It integrates with GitHub, Git, tmux, and local worktrees.

### Platform and Installation

- Distribute self-contained platform executables through GitHub Releases.
- Publish checksums for release verification.
- Require supported Git, tmux, and GitHub connectivity.
- Detect missing or incompatible dependencies before accepting work.
- Do not require users to install the implementation language runtime.
- Defer Homebrew, other package managers, and container distribution until post-MVP demand.

### CLI Surface

The CLI must support:

- Initializing and validating repository configuration
- Verifying GitHub CLI authentication and repository connectivity
- Starting, stopping, and inspecting the orchestrator
- Listing queued, active, blocked, review-ready, and completed work
- Inspecting an issue's agents, tmux sessions, worktrees, branch, PR, checks, and blockers
- Triggering safe reconciliation with GitHub
- Viewing configuration and diagnostics
- Producing human-readable output by default and structured output for automation

The CLI must not bypass GitHub issue readiness, review, or merge policies.

### Configuration

- Store versioned configuration inside the repository.
- Support configurable labels, priority mapping, completion policies, concurrency, worktree root, documentation paths, and agent behavior.
- Validate configuration and filesystem paths before orchestration begins.
- Keep secrets outside committed configuration.
- Provide safe defaults for one repository and conservative concurrency.

### Documentation and Examples

Repository documentation must include:

- Installation and prerequisites
- GitHub CLI authentication and least-privilege account permissions
- Minimal configuration example
- Issue metadata and lifecycle examples
- Human-review and autonomous-merge examples
- Worktree and tmux troubleshooting
- Recovery, cancellation, and security guidance
- CLI command reference
- CLI status interpretation guide

No migration guide is required for the greenfield MVP.

### Technical Architecture Considerations

- Keep GitHub authoritative and reconstruct orchestration state from GitHub plus bounded local execution metadata.
- Hold GitHub credentials in the orchestrator boundary; do not expose them to sub-agents.
- Poll current GitHub state at startup and at intervals no greater than 60 seconds.
- Preserve deterministic issue-to-attempt, worktree, branch, tmux-session, and PR relationships.
- Limit dashboard mutations to confirmed cleanup of exact projected local resources; never mutate GitHub issue, PR, review, check, or merge policy from the dashboard.
- Treat macOS, Linux, and WSL path and process behavior as explicit compatibility requirements.

### Implementation Considerations

- Validate the CLI on every supported platform before release.
- Prefer one execution path across platforms.
- Add alternate session backends only if tmux through WSL proves insufficient.
- Provide actionable errors for dependency, permission, configuration, and repository-state failures.
- Version configuration only when compatibility demands it.

## Functional Requirements

### GitHub Work Intake and Governance

- **FR1:** A stakeholder can submit implementation work by creating a GitHub issue.
- **FR2:** A stakeholder can mark an issue eligible or ineligible for agent execution.
- **FR3:** A stakeholder can assign an issue priority from P1 through P3.
- **FR4:** A stakeholder can declare dependencies between issues.
- **FR5:** A stakeholder can require human review for an issue.
- **FR6:** A stakeholder can allow policy-controlled merge for an issue.
- **FR7:** The system accepts arbitrary issue-body text while validating label, authorization, dependency, and lifecycle eligibility separately.
- **FR8:** The system can record execution status, blockers, scope changes, decisions, validation evidence, and completion in GitHub.
- **FR9:** The system can restrict control actions to authorized GitHub actors.

### Backlog Orchestration

- **FR10:** The orchestrator can discover all eligible issues in a configured repository.
- **FR11:** The orchestrator can order eligible work by priority.
- **FR12:** The orchestrator can prevent dependent work from starting before prerequisites are satisfied.
- **FR13:** The orchestrator can identify issues that are explicitly safe to execute concurrently.
- **FR14:** The orchestrator can serialize or block work when safe concurrency cannot be established.
- **FR15:** The orchestrator can explain why an issue is queued, running, blocked, or serialized and persist the latest blocker projection in coordinator state.
- **FR16:** The orchestrator can enforce configurable global and repository execution limits.
- **FR17:** The orchestrator can reevaluate the backlog when relevant GitHub state changes.
- **FR18:** The orchestrator can continue eligible work while unrelated work is blocked.

### Agent and Workspace Execution

- **FR19:** The orchestrator can assign an issue to one or more sub-agents.
- **FR20:** The orchestrator can select agent responsibilities according to repository configuration and work needs.
- **FR21:** Each active attempt can execute in an isolated Git worktree.
- **FR22:** Each active attempt can execute within an identifiable tmux session.
- **FR23:** The system can maintain traceable relationships among an issue, attempt, agents, worktree, session, branch, and PR.
- **FR24:** A sub-agent can receive the approved issue context, repository guidance, and relevant prior execution context.
- **FR25:** A sub-agent can implement changes, run relevant validation, and update the issue checklist.
- **FR26:** The orchestrator can stop or cancel work that becomes ineligible or unsafe.
- **FR27:** The orchestrator can prevent sub-agents from directly accessing GitHub credentials.

### Pull Request Review and Completion

- **FR28:** The system can create or update a PR linked to its originating issue and execution attempt.
- **FR29:** The system can synchronize the `needs-human-review` policy from an issue to its PR.
- **FR30:** The system can prevent merge while human review remains required.
- **FR31:** The system can monitor every open agent-created PR throughout its lifetime.
- **FR32:** The system can detect actionable feedback from authorized stakeholders.
- **FR33:** The orchestrator can delegate actionable PR feedback to an appropriate sub-agent using the existing work context.
- **FR34:** The system can report whether each actionable review comment is pending, addressed, or blocked.
- **FR35:** The system can rerun relevant validation after review-driven changes.
- **FR36:** The system can merge a PR only when its issue policy, repository permissions, reviews, required checks, and branch protections permit it.
- **FR37:** The system can leave a completed PR ready for human action when autonomous merge is not permitted.

### Validation and Living Documentation

- **FR38:** The system can require validation evidence before a PR becomes review-ready or merge-eligible.
- **FR39:** The system can require every agent-created PR to record a documentation-impact assessment.
- **FR40:** A sub-agent can update affected PRDs, `README.md`, and durable repository documentation in the implementation PR.
- **FR41:** The system can prevent completion when required documentation updates are missing.
- **FR42:** A stakeholder can configure repository-local documentation locations.
- **FR43:** The system can preserve implementation decisions in the originating issue.

### Recovery and Operational Visibility

- **FR44:** The system can process repeated GitHub events without duplicating execution.
- **FR45:** The system can reconcile active and completed work after restart.
- **FR46:** The system can resume an existing attempt or create a new traceable attempt when safe resumption is impossible.
- **FR47:** The system can detect and report stale, conflicting, failed, or orphaned execution state.
- **FR48:** A stakeholder can inspect queued, active, blocked, review-ready, and completed work.
- **FR49:** A stakeholder can inspect an issue's priority, dependencies, agents, tmux sessions, worktrees, branch, PR, checks, blockers, and next action.
- **FR50:** A stakeholder can request reconciliation and diagnostics without mutating GitHub work policy.

### Configuration and CLI Operation

- **FR51:** A stakeholder can initialize repository-local orchestration configuration.
- **FR52:** A stakeholder can configure readiness metadata, priorities, dependencies, completion policy, concurrency, worktree location, documentation paths, agents, and status output.
- **FR53:** The system can validate configuration, prerequisites, permissions, and repository state before accepting work.
- **FR54:** A stakeholder can start, stop, and inspect the orchestrator through a CLI.
- **FR55:** A stakeholder can receive human-readable status and diagnostics.
- **FR56:** Automation can consume structured status and diagnostics.
- **FR57:** A stakeholder can verify GitHub CLI authentication, identity, connectivity, and effective repository permissions.
- **FR58:** The system can identify unsupported platform or dependency conditions with corrective guidance.

### Per-Repository Dashboard

- **FR59:** A stakeholder can inspect the current status projection in a browser dashboard owned by one repository daemon.
- **FR60:** A stakeholder can attach an in-browser terminal to an exact projected live tmux session without creating a session or arbitrary command.
- **FR61:** A stakeholder can archive a completed attempt by confirming cleanup of its exact local worktree, local branch, worker result, and tmux session while retaining diagnostic metadata.
- **FR62:** A stakeholder can abandon a selected orphaned attempt by confirming cleanup of its exact local resources and retained manifest/log.
- **FR63:** Dashboard terminal and cleanup controls default to loopback and always require same-origin requests and server-resolved deterministic attempt identity; non-loopback binding requires an explicit unsafe-network opt-in and password authentication on every route.

## Non-Functional Requirements

### Performance

- The continuous coordinator must poll GitHub at intervals no greater than 60 seconds.
- Relevant issue or PR changes must appear in CLI status within 60 seconds under normal GitHub API availability.
- Authorized actionable PR feedback must enter the execution queue within 60 seconds.
- CLI status and inspection commands must return within two seconds for a repository containing up to 100 active or eligible issues, excluding GitHub network latency.
- An eligible P1 issue must begin within five minutes when capacity and prerequisites permit.

### Security

- Every GitHub API response must be treated as untrusted input and validated at the integration boundary.
- The authenticated GitHub CLI account must have only the repository permissions required by enabled features.
- GitHub CLI credentials must never enter sub-agent environments, committed files, tmux history, or logs.
- Secrets must be redacted from diagnostics and error output.
- Only authorized GitHub actors may change execution, review, or completion policy.
- Repository permissions, required Checks, reviews, and branch protections must be reevaluated immediately before merge.
- Configuration, GitHub payloads, API responses, paths, and repository content must be treated as untrusted input at system boundaries.
- Release executables must publish integrity checksums.

### Reliability and Recovery

- Repeating reconciliation over unchanged GitHub state must not create another attempt, branch, worktree, tmux session, or PR.
- After restart, the orchestrator must reconcile active state within two minutes under normal GitHub API availability.
- Restart recovery must not redispatch active or completed work.
- A process failure must not damage the primary repository working tree.
- Failed or blocked attempts must preserve enough context for diagnosis and safe retry.
- Lost or contradictory state must reduce autonomy and require reconciliation rather than silently continuing.
- Periodic reconciliation must discover GitHub changes made between polls.

### Capacity and Scalability

- The MVP must correctly prioritize and reconcile at least 100 eligible issues in one repository.
- The orchestrator must support at least two concurrent issue executions.
- Concurrency must be configurable and bounded by host capacity.
- Exceeding GitHub rate or host capacity must delay work without losing, duplicating, or incorrectly reprioritizing it.
- Multi-repository scaling is not an MVP requirement.

### Platform Compatibility

- All MVP CLI capabilities must pass release smoke tests on supported macOS, Linux, and WSL2 environments.
- Cross-compilation establishes artifact availability and runtime independence; only execution on each target establishes platform smoke evidence.
- Release candidates must be reproducible from a recorded source commit, self-contained, and accompanied by verified SHA-256 checksums.
- Pilot completion requires durable issue, PR, CI, artifact, review, and timing evidence sufficient to calculate the stakeholder-intervention success rate; missing external evidence remains explicitly pending.
- Paths, process handling, executable discovery, and signals must behave consistently across supported environments.
- Unsupported operating systems or dependency versions must fail before work begins with corrective guidance.
- The product must not require installation of its implementation-language runtime.

### GitHub Integration

- GitHub API throttling, transient failures, and secondary rate limits must use bounded retry and backoff.
- Reconciliation must detect issue edits, label changes, PR comments, reviews, check results, branch updates, permission changes, and closure events.
- Stale state must be refreshed before dispatch, rework, or merge decisions.
- Every GitHub mutation must be attributable to an issue and execution attempt.
- A GitHub outage must pause affected actions safely and resume through reconciliation.

### Operability and Accessibility

- Logs and structured status output must identify issue, attempt, agent, worktree, session, branch, and PR where applicable.
- User-facing errors must state the failed operation, affected work item, and corrective action.
- CLI output must not rely on color alone and must honor standard no-color behavior.
- Human-readable output must remain usable with terminal screen readers and text capture.
- Diagnostic output must be available without changing work state.
