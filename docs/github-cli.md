# GitHub CLI integration

Agent Symphony uses the installed `gh` executable for every GitHub API request and for Git push authentication. The daemon, orchestrator, heartbeat, implementation, and review roles all receive the same GitHub CLI capability. GitHub credentials remain runtime environment or `gh` credential-store state, never Agent Symphony configuration or repository files.

## Authentication

Install GitHub CLI and authenticate the operating-system account that runs Agent Symphony:

```sh
gh auth login
gh auth status
gh repo view OWNER/REPOSITORY
```

For non-interactive use, set `GH_TOKEN` or `GITHUB_TOKEN` in the Agent Symphony process instead. The shared agent environment boundary forwards those variables, their GitHub Enterprise equivalents, and `GH_HOST`, `GH_REPO`, or `GH_CONFIG_DIR`; unrelated credential variables remain blocked. A GitHub App user access token is supported because it identifies a user; App IDs, private keys, installation tokens, and token minting remain outside Agent Symphony. The token path is optional in zero-admin mode when `gh auth login` is already configured, because every role uses the same operating-system account and credential store.

Agent session creation imports only the filtered variable names from the tmux client environment; credential values do not appear in tmux command arguments. Advanced host isolation uses command-scoped sudo `env_keep` entries for the same fixed GitHub CLI allowlist and does not grant `SETENV`. Implementation and review requests carry their filtered environment inside the bounded adapter input rather than the sudo process environment. Returned launch errors, retained pane logs, diagnostics, and manifests redact credential values.

Advanced host isolation uses separate worker and reviewer accounts, so the coordinator user's stored login is not available there. Supply one of the supported token variables to the Agent Symphony service or authenticate each fixed account separately. Agent Symphony passes runtime values through the bounded process environment and does not write them to worktrees, snapshots, manifests, logs, launch contracts, or repository configuration. Attempt worktrees still have no remote or Git credential helper; use `gh` for authorized issue and pull-request operations, not `git push`.

The authenticated account must be able to read issues, pull requests, reviews, checks, commit statuses, and any available branch protection or rules, and to perform the mutations enabled by the repository's Agent Symphony policy. Branch protection is optional. Use `gh auth refresh` if the account is missing a required scope.

`agent-symphony doctor` verifies the daemon's executable, authenticated identity, configured repository, and effective repository permissions. In an agent session, `gh auth status` and `gh repo view "$GH_REPO"` verify that role's same runtime path. Missing or invalid authentication returns the GitHub CLI authentication error and a nonzero status; no role reports success from configuration alone.

## Runtime behavior

The daemon invokes `gh api` for reconciliation reads and writes. It discovers its stable coordinator identity from `gh api /user`, uses that user ID to recognize its own markers and comments, and publishes `agent-symphony/policy` as a commit status. Authorized agents invoke the same installed CLI directly. Git publication remains daemon-owned through `gh auth git-credential`; agent worktrees have no authenticated Git remote.

Direct workflow status uses the two immutable comment commands documented in [GitHub controls](github-controls.md#direct-agent-status). Reconciliation reads the newest valid issue/PR command into the same projection served by the CLI and dashboard; no coordinator relay or dashboard mutation endpoint is involved.

`serve` reads current GitHub state immediately at startup and then polls at the configured interval, capped at 60 seconds. `reconcile` performs the same authoritative read once. No inbound HTTP endpoint or event subscription is part of the integration.
