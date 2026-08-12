# Recovery

GitHub markers and current issue/PR state are authoritative; local manifests are bounded diagnostic state. On startup the coordinator acquires its runtime-state lock, reads authoritative attempts, checks exact deterministic worktree/session/head identities, reconciles terminal work, and only then schedules eligible issues. Separate repository daemons use separate state roots; shared resource names include the repository identity so one daemon cannot discover or clean another's resources.

Before local launch, dispatch writes a strict coordinator-authored active-attempt comment binding the issue, attempt, deterministic branch, and approved base. Restarts and repeated polls resume that binding through implementation, review, and publication while current issue controls remain authorized; revoked controls cancel a live worker and suppress publication. Final PR or terminal markers supersede the binding, so a restart cannot create a duplicate attempt or PR. Foreign, conflicting, or unverifiable bindings and contradictory authoritative PR attempts block mutation.

For an orphaned attempt, preserve its manifest and log, compare GitHub markers with the branch, worktree HEAD, and named tmux session, then use `status`, `inspect`, and `reconcile`. Never delete or adopt an unidentified resource to make recovery proceed.
