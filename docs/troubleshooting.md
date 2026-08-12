# Troubleshooting

- `doctor` rejects WSL paths: move the checkout and state beneath the Linux home directory; `drvfs` and `9p` are unsupported for worktrees and locks.
- `resources already exist`: run reconciliation and inspect the exact attempt. Do not remove the directory/session manually.
- an attempt is blocked after restart: compare its GitHub marker, manifest base/head, branch, worktree HEAD, and tmux session; repair only when identity is exact.
- feedback remains pending: verify immutable feedback ID, actor authorization, PR head, claim/outcome record, and the next reconciliation result.
- merge remains gated: check current-head validation/docs evidence, actionable feedback, independent review, human-review-label removal by an authorized actor, required checks, approvals, branch currency, and protection.
- checksum verification fails: discard the artifact and regenerate from a clean source commit with the same version and `SOURCE_DATE_EPOCH`.
- `host isolation` fails in the zero-admin default: repair the `attempts`/`snapshots` directories under the runtime state root so they are non-symlink, mode `0700`, and owned by the account running `agent-symphony`, or pass a `--runtime-state` that account can write to. This never requires `install-host`.
- the dashboard does not start: choose an unused localhost or loopback address with `--dashboard-address`; non-loopback addresses are rejected.
- Archive or Abandon is refused: refresh the projection and verify the card is still completed or orphaned and its branch, worktree, session, and retained manifest match exactly. Do not delete around an identity mismatch.
# Host isolation is missing or stale (advanced mode only)

This only applies once `install-host` has been run at least once; the zero-admin default never requires it. Install the current release at the documented root-owned mode-`0755` path and rerun `sudo <exact-binary> install-host --coordinator USER`. Do not broaden the managed sudo rule or add shell access. On WSL2, move the repository and all state out of `/mnt/*` before retrying. Conflicting pre-existing users, groups, ownership, or primary groups must be repaired explicitly; the installer will not weaken them.
