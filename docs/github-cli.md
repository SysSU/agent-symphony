# GitHub CLI integration

Agent Symphony uses the installed `gh` executable for GitHub API requests and Git publication. The daemon is the only workflow publication/mutation authority; implementation and review workers have no GitHub credential, CLI, or network capability. The optional trusted advisory orchestrator is a separate coordinator-user process described in [Security](security.md#advisory-orchestrator). See the [ownership architecture](architecture.md#system-boundaries-and-ownership).

## Authentication

Install GitHub CLI and authenticate the ordinary operating-system account that runs Agent Symphony:

```sh
gh auth login
gh auth status
gh repo view OWNER/REPOSITORY
```

For non-interactive service use, set `GH_TOKEN` or `GITHUB_TOKEN` in the daemon environment. GitHub Enterprise equivalents and target variables are also service inputs. Agent Symphony never copies them into implementation/review worker environments, workspaces, snapshots, manifests, logs, launch contracts, status mailboxes, or repository configuration. A GitHub App user access token is supported because it identifies a user; App IDs, private keys, installation tokens, and token minting remain outside Agent Symphony.

The authenticated account must be able to read issues, pull requests, reviews, checks, commit statuses, and available repository rules, and to perform the mutations enabled by repository policy. Use `gh auth refresh` if the account lacks a required scope. `agent-symphony doctor` verifies the daemon's authenticated identity, configured repository, and effective permission.

## Worker boundary

Implementation and review workers use an isolated `CODEX_HOME` containing only Codex model-authentication assets. They do not receive the daemon's GitHub environment or `gh` configuration. The managed Codex profile disables command network access and ambient capability tools. Attempt clones have no remote or credential helper, so local Git work cannot publish directly.

While running, a worker requests `needs-attention` or `clear` through its private status mailbox. The request is bound to its generation, launch ID, increasing sequence, and permission-profile digest. The owner validates current state and performs the GitHub comment/label effect. A mailbox write proves only that the request was submitted; current GitHub state proves whether the status changed.

Completion follows the same rule. The worker returns a bounded result and credential-free bundle. The coordinator verifies and seals the exact generation and content in owner-private storage, then uses its own GitHub session to publish from that immutable seal. Workers never create pull requests, push branches, post comments, change labels, or merge.

## Runtime behavior

The daemon invokes `gh api` for reconciliation reads and owner-issued writes and `gh auth git-credential` for authenticated publication. It discovers its stable identity from `gh api /user` and uses that identity to recognize coordinator markers and comments. Every asynchronous GitHub result is checked against its source revision, issue/attempt generation, and effect identity before it commits local state.

An HTTP request already accepted by GitHub cannot be cancelled retroactively. If a reversible or discoverable effect lands after local invalidation, the owner records a generation-bound convergence effect. An exact-head merge accepted before cancellation remains a GitHub fact; the owner reconciles the invalidated intent and keeps the local tombstone so the runtime attempt cannot return.

`serve` reads GitHub at startup and on the configured cadence. `reconcile` requests the same authoritative collection through the running daemon. No inbound GitHub webhook or event subscription is part of this integration.
