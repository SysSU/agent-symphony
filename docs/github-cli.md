# GitHub CLI integration

Agent Symphony uses the installed `gh` executable for every GitHub API request and for Git push authentication. It does not accept GitHub tokens, private keys, application IDs, or installation IDs as Agent Symphony configuration.

## Authentication

Install GitHub CLI and authenticate the operating-system account that runs Agent Symphony:

```sh
gh auth login
gh auth status
gh repo view OWNER/REPOSITORY
```

The authenticated account must be able to read issues, pull requests, reviews, checks, commit statuses, and any available branch protection or rules, and to perform the mutations enabled by the repository's Agent Symphony policy. Branch protection is optional. Use `gh auth refresh` if the account is missing a required scope.

`agent-symphony doctor` verifies the executable, authenticated identity, configured repository, and effective repository permissions. Workers and reviewers never inherit GitHub CLI credential environment variables, credential helpers, or repository remotes.

## Runtime behavior

The coordinator invokes `gh api` for GitHub reads and writes. It discovers its stable coordinator identity from `gh api /user`, uses that user ID to recognize its own markers and comments, and publishes `agent-symphony/policy` as a commit status. Git publication uses `gh auth git-credential` only in the coordinator process.

`serve` reads current GitHub state immediately at startup and then polls at the configured interval, capped at 60 seconds. `reconcile` performs the same authoritative read once. No inbound HTTP endpoint or event subscription is part of the integration.
