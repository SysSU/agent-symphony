# GitHub controls

Agent Symphony reads issue labels and exact issue comments as workflow controls. Control changes are accepted only from actors whose current repository permission is `maintain` or `admin`.

Label names are configurable. Run `agent-symphony config view` to see the names for the current repository; the tables below show the defaults.

## Implementation issue contract

GitHub Issues are the sole source of truth for implementation work. Chat messages and local story files do not authorize implementation.

Before applying `agent-ready`, the issue body must contain nonempty `## Context`, `## Acceptance criteria`, `## Checklist`, `## Validation`, and dependency sections. The dependency heading is configured by `dependencies.section` and defaults to `## Dependencies`. Put the reason and relevant evidence in Context, at least one Markdown task in Checklist, expected checks in Validation, and either issue references or `None` in Dependencies. Missing, empty, or malformed required sections block dispatch.

Dispatch records the deterministic implementation branch, worktree, and tmux session in the active-attempt comment before launch. The implementation session receives that same project and issue identity in its launch context. Keep later status changes, checklist progress, blockers, scope changes, and validation evidence current. Link the pull request and make it close the issue.

## Issue comments

Post each command as the entire comment body and do not edit it. Of `cancel` and `retry`, the latest valid comment wins.

| Comment | Effect |
| --- | --- |
| `/agent-symphony cancel` | Cancels the issue for Agent Symphony, prevents dispatch, and makes current work ineligible. |
| `/agent-symphony retry` | Clears a prior cancellation. When posted after the latest coordinator-authored terminal failure, it authorizes the next numbered attempt. It does not replace an existing active or completed attempt or resolve conflicting markers. |
| `/agent-symphony approve` | Legacy compatibility only. Current dispatch authorization uses `agent-ready`; new approval comments do not authorize work. Existing pre-upgrade control snapshots that already reference an approval remain readable. |

After posting a control comment, wait for the next `serve` cycle or run `agent-symphony reconcile`.

After an attempt has a pull request, other nonempty issue comments posted after its first coordinator-authored publication marker are implementation feedback. Pre-PR issue history is not replayed. Pull-request conversation comments, inline review comments, and review bodies use the same path. The coordinator freshly requires `write`, `maintain`, or `admin` permission before each handoff, and excludes control commands and its own workflow comments. These comments amend the issue contract in chronological order: a later human instruction wins over conflicting earlier text, and automated review findings never override a human instruction.

## Issue labels

| Default label | Configuration key | Effect |
| --- | --- | --- |
| `agent-ready` | `labels.ready` | Required for dispatch. Apply it after the latest issue-body edit. Removing it makes current work ineligible. |
| `priority:P1` | `labels.priority_p1` | Selects the highest scheduling priority. Exactly one configured priority label is required. |
| `priority:P2` | `labels.priority_p2` | Selects the middle scheduling priority. Exactly one configured priority label is required. |
| `priority:P3` | `labels.priority_p3` | Selects the lowest scheduling priority. Exactly one configured priority label is required. |
| None | `labels.issue_filter` | Optional repository-specific queue filter. When configured, an issue must currently have this label as well as `agent-ready`; removing it makes the issue ineligible. |
| `needs-human-review` | `completion_policies.human_review_label` | Explicitly requests the human-review PR label and pending policy status. Human review is already the default when no completion label is present. |
| `autonomous-merge` | `completion_policies.autonomous_merge_label` | Opts into coordinator-managed merge after all review, validation, repository-rule, and permission checks pass. Apply it after the latest issue-body edit. |
| `needs-attention` | Fixed | Create this reserved label during setup. It pairs with a reasoned direct-status comment to hold dispatch and move the current dashboard card to **Needs attention**. |

`needs-human-review` and `autonomous-merge` conflict; do not apply both. Required control-label names must be nonempty. `labels.issue_filter` may be omitted or empty; when nonempty, it must be unique like every other configured label name. Label names are compared case-insensitively, matching GitHub label identity.

Edit the issue body before applying `agent-ready`. If an already-ready issue body changes, reapply `agent-ready`; reapply `autonomous-merge` too when autonomous completion is still intended.

## Direct agent status

Authorized daemon, implementation, review, orchestrator, heartbeat, and other agent sessions use the installed `gh` CLI directly. The shared status vocabulary has two commands, each posted as one unedited issue or pull-request comment:

```text
/agent-symphony status needs-attention: REASON
/agent-symphony status clear: REASON
```

`REASON` must be nonempty and at most 1,024 bytes. Pair the comment with the fixed `needs-attention` label on the bound issue: add it when setting status and remove it when clearing. The newest valid comment from the currently authenticated Agent Symphony GitHub account wins across the issue and its current pull request when it agrees with that label. Foreign or edited comments do not change status. An authenticated unedited malformed or reasonless status comment, or any comment/label mismatch, remains visible as incomplete needs-attention instead of reporting success. Needs-attention prevents a queued issue from dispatching and moves the current dashboard card to **Needs attention** without replacing its underlying attempt lifecycle; clearing it restores the authoritative issue/PR lifecycle projection.

Use native `gh issue comment`, `gh pr comment`, `gh issue edit`, and `gh pr edit` commands for authorized comments, labels, and Markdown links. A zero exit status proves only that the CLI request completed: re-read the exact issue or pull request before reporting the mutation as successful. Missing authentication, insufficient permission, a failed post, or a failed re-read is an explicit failure and must never be reported as success.

## Dashboard actions are separate

Dashboard **Archive** and **Abandon** are local cleanup actions, not GitHub issue controls. Archive applies only to completed attempts. Abandon applies only to orphaned attempts and removes the exact local attempt resources and retained record; it does not post a cancel or retry comment or change issue labels.

**Recover attempt** is the narrow exception. After confirmation, the server revalidates the exact latest retryable attempt. It may stop and mark a stuck attempt failed, then post the fixed `/agent-symphony retry` control. The dashboard never accepts arbitrary comments or policy changes.

See the [CLI reference](cli.md#issue-eligibility-and-recorded-blockers) for dispatch and merge restrictions and [Recovery](recovery.md) for orphan handling.
