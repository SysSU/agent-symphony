# Troubleshooting

- `doctor` rejects WSL paths: move the checkout and state beneath the Linux home directory; `drvfs` and `9p` are unsupported for worktrees and locks.
- `resources already exist`: run reconciliation and inspect the exact attempt. Do not remove the directory/session manually.
- legacy audit directories remain after restart: new audits use bounded stdin/stdout and create no private children. Old children remain because a missing PID or lease cannot rule out detached writers. Do not delete by prefix; independently verify quiescence before exact-child cleanup. See [Recovery](recovery.md).
- an attempt is blocked after restart: compare its GitHub marker, manifest base/head, branch, worktree HEAD, and tmux session; repair only when identity is exact.
- feedback remains pending: verify immutable feedback ID, actor authorization, PR head, claim/outcome record, and the next reconciliation result.
- merge remains gated: check current-head validation/docs evidence, actionable feedback, independent review, human-review-label removal by an authorized actor, required checks, approvals, branch currency, and protection.
- checksum verification fails: discard the artifact and regenerate from a clean source commit with the same version and `SOURCE_DATE_EPOCH`.
- `worker confinement` fails: use a private persistent runtime-state root owned by the Agent Symphony account, install the supported Codex version, and ensure implementation/reviewer resolve to the same executable. On Linux/WSL, verify the host permits unprivileged user namespaces for bubblewrap. Do not add sandbox-bypass flags, custom workers, or sudo; an unproved profile fails closed.
- a worker cannot use `gh`, GitHub, the network, or a sibling path: this is expected. Workers request status through their generation-bound mailbox and return a result for owner sealing; only the daemon publishes.
- `doctor` warns that shared temporary files are visible: Codex 0.153.x on macOS permits sandboxed commands to read/write unrelated same-user `/tmp` data. Keep the runtime root, daemon socket, credentials, and authority data out of `/tmp`, `/private/tmp`, `/var/tmp`, and `/dev/shm`; move unrelated secrets out of shared temp.
- `physical cleanup pending` or `legacy reviewer absence unknown`: an older unconfined process cannot be proved dead from a missing pane or process group. Follow the verified reboot recovery procedure below. A daemon restart alone cannot release legacy reviewer quarantine.
- the dashboard does not start: choose an unused localhost or loopback address with `--dashboard-address`. A non-loopback address additionally requires both `--allow-unsafe-dashboard-network` and `--dashboard-password-file` pointing to a private, coordinator-owned, one-line password file.
- the remote dashboard cannot be reached: verify the daemon warning says unsafe network access is enabled, connect to the host's real IP rather than `0.0.0.0`, and narrowly allow the selected TCP port through the host firewall. HTTP Basic username is `agent-symphony`; direct HTTP is unencrypted.
- Archive, Abandon, or Recover is refused: refresh status and verify the card still has the required state and its branch, worktree, session, and retained manifest match exactly. Recover also requires the latest retryable attempt. Do not delete around an identity mismatch; see [Recovery](recovery.md).

## Recover legacy reviewer quarantine after a verified reboot

1. Install a binary containing the boot recovery fix. Record its revision and clean build metadata.
2. Start Agent Symphony once. Verify that the service starts successfully and that `runtime-state.json` contains a `legacy_reviewer_baselines` entry for each `legacy_reviewer_quarantines` entry, with a nonempty boot source and UUID. Read the ledger only; never edit it. Existing warnings must remain on this first baseline startup.
3. Reboot the host. On WSL2, reboot the WSL kernel instance; restarting only the daemon or a distribution without changing the kernel boot identity is insufficient.
4. Start Agent Symphony again. It compares macOS `kern.bootsessionuuid` or Linux/WSL2 `/proc/sys/kernel/random/boot_id` with the persisted baseline. Only a different valid identity from the same source releases unchanged legacy evidence.
5. Verify the current owner projection through the dashboard or `/status.json`. `physical-unverified` entries caused solely by the released legacy quarantine disappear. Read the ledger to confirm those quarantine/baseline entries were removed and historical release certificates were persisted. Account for every remaining warning separately.

If status says `verified reboot recovery unavailable`, restore access to the host's boot identity source and repeat the baseline/start, reboot, and verification sequence. Missing or malformed identity does not prove a reboot. A changed identity source or newly admitted legacy evidence establishes a new baseline and requires another reboot.

Release preserves historical records and does not certify filesystem cleanup. Current reviewer leases, Stop/cleanup effects, implementation-start candidates, handoff compensation, and unresolved GitHub outcomes still require their ordinary proofs. Do not delete historical worktrees or sessions based on reboot recovery.

If startup fails while persisting the transition, it admits no work and publishes no success. Before ledger replacement, the old ledger remains authoritative. After replacement, the complete new ledger may already be installed even if directory sync failed. Fix the persistence problem and restart to read whichever complete ledger survived; do not roll it back or clear entries manually.

## Obsolete host identities are detected

`install-host` is now a non-mutating diagnostic. If it reports legacy identities, Agent Symphony ignores them and continues to use the ordinary current-user boundary. Remove old accounts, groups, or sudo policy only through the host's normal reviewed administration process; the product does not request privilege or perform that cleanup.
