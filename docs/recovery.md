# Recovery

GitHub markers and current issue/PR state are authoritative; local manifests are bounded diagnostic state. On startup the coordinator acquires one repository lock, reads authoritative attempts, checks exact deterministic worktree/session/head identities, reconciles terminal work, and only then schedules eligible issues.

Duplicate webhooks only wake reconciliation. A restart or redelivery cannot create a duplicate attempt or PR because dispatch recovers stable issue/attempt identities and PR creation searches the exact head branch. Conflicting or unverifiable state blocks mutation.

For an orphaned attempt, preserve its manifest and log, compare GitHub markers with the branch, worktree HEAD, and named tmux session, then use `status`, `inspect`, and `reconcile`. Never delete or adopt an unidentified resource to make recovery proceed.
