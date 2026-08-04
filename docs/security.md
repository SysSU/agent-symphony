# Security

The coordinator owns GitHub credentials and policy mutations. Workers receive a small environment allowlist, no GitHub/SSH/cloud credentials, a disabled Git credential helper, and no remote in attempt worktrees. Logs and diagnostics pass through credential-shape redaction; release validation scans every regular candidate file, including ignored `.env` files and the validator itself, for common GitHub token and private-key forms without printing matching contents or paths. It prunes only Git metadata/worktrees and generated `dist` output; Git history, logs, tmux, and published artifacts remain explicit external scans.

Before a pilot, verify the worker identity boundary, filesystem permissions, tmux history, retained logs, config, Git history, and release artifacts contain no credential. A clean pattern scan is supporting evidence, not proof that an unknown secret format was absent. Rotate any credential suspected of exposure and preserve only redacted incident evidence.

`install-host` must run as root from the root-owned versioned release path. It writes only the two exact no-argument `agent-host` sudo tuples. `agent-host` rechecks its effective user and primary group, accepts one bounded JSON value, permits only Git/tmux execution below its provisioned root, and re-applies the shared credential environment filter. It never grants an agent GitHub authority.

Webhook signatures authenticate delivery, while fresh GitHub permission checks authorize actors. Required checks, review state, current head, and branch protection are re-read before merge; the coordinator never uses an administrative bypass.
