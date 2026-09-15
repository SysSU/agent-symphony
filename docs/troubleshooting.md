# Troubleshooting

- `doctor` rejects WSL paths: move the checkout and state beneath the Linux home directory; `drvfs` and `9p` are unsupported for worktrees and locks.
- `resources already exist`: run reconciliation and inspect the exact attempt. Do not remove the directory/session manually.
- an attempt is blocked after restart: compare its GitHub marker, manifest base/head, branch, worktree HEAD, and tmux session; repair only when identity is exact.
- feedback remains pending: verify immutable feedback ID, actor authorization, PR head, claim/outcome record, and the next reconciliation result.
- merge remains gated: check current-head validation/docs evidence, actionable feedback, independent review, human-review-label removal by an authorized actor, required checks, approvals, branch currency, and protection.
- checksum verification fails: discard the artifact and regenerate from a clean source commit with the same version and `SOURCE_DATE_EPOCH`.
- `worker confinement` fails: use a private persistent runtime-state root owned by the Agent Symphony account, install the supported Codex version, and ensure implementation/reviewer resolve to the same executable. On Linux/WSL, verify the host permits unprivileged user namespaces for bubblewrap. Do not add sandbox-bypass flags, custom workers, or sudo; an unproved profile fails closed.
- a worker cannot use `gh`, GitHub, the network, or a sibling path: this is expected. Workers request status through their generation-bound mailbox and return a result for owner sealing; only the daemon publishes.
- `doctor` warns that shared temporary files are visible: Codex 0.153.x on macOS permits sandboxed commands to read/write unrelated same-user `/tmp` data. Keep the runtime root, daemon socket, credentials, and authority data out of `/tmp`, `/private/tmp`, `/var/tmp`, and `/dev/shm`; move unrelated secrets out of shared temp.
- `physical cleanup pending` or `legacy reviewer absence unknown`: an older unconfined process cannot be proved dead from a missing pane or process group. The issue remains quarantined and its resources are not reused. Reboot, then restart Agent Symphony so a verified changed boot identity can release process quarantine; if boot identity is unavailable, preserve the quarantine.
- the dashboard does not start: choose an unused localhost or loopback address with `--dashboard-address`. A non-loopback address additionally requires both `--allow-unsafe-dashboard-network` and `--dashboard-password-file` pointing to a private, coordinator-owned, one-line password file.
- the remote dashboard cannot be reached: verify the daemon warning says unsafe network access is enabled, connect to the host's real IP rather than `0.0.0.0`, and narrowly allow the selected TCP port through the host firewall. HTTP Basic username is `agent-symphony`; direct HTTP is unencrypted.
- Archive, Abandon, or Recover is refused: refresh status and verify the card still has the required state and its branch, worktree, session, and retained manifest match exactly. Recover also requires the latest retryable attempt. Do not delete around an identity mismatch; see [Recovery](recovery.md).

## Obsolete host identities are detected

`install-host` is now a non-mutating diagnostic. If it reports legacy identities, Agent Symphony ignores them and continues to use the ordinary current-user boundary. Remove old accounts, groups, or sudo policy only through the host's normal reviewed administration process; the product does not request privilege or perform that cleanup.
