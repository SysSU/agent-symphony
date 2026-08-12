# Setup

This walkthrough goes from a fresh machine to a working `reconcile`.

## What you need

- macOS, Linux, or WSL2.
- Agent Symphony, Git, tmux, and GitHub CLI.
- Access to the GitHub repository and a completed `gh auth login`.
- Codex set up, or another coding agent if you prefer.
- Go 1.26 only if you build Agent Symphony yourself.

## 1. Install Agent Symphony and prerequisites

Install Git, tmux, [GitHub CLI](https://cli.github.com/), and Codex (or the coding agent you plan to use). Download the Agent Symphony release archive matching the host (`darwin` or `linux`, `amd64` or `arm64`; WSL2 uses `linux_amd64`) and verify it:

```sh
grep "  agent-symphony_VERSION_OS_ARCH.tar.gz$" SHA256SUMS | shasum -a 256 -c -
tar -xzf agent-symphony_VERSION_OS_ARCH.tar.gz
install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony ~/.local/bin/agent-symphony
agent-symphony --help
```

On Linux, `sha256sum -c SHA256SUMS` can verify the downloads. On WSL2, keep the repository and state on the Linux filesystem, never under `/mnt/c`. Contributors can instead build with the Go version pinned in `go.mod`:

```sh
go build -o agent-symphony ./cmd/agent-symphony
```

## 2. Authenticate GitHub CLI

Sign in with the GitHub account Agent Symphony should use:

```sh
gh auth login
gh auth status
gh repo view OWNER/REPOSITORY
```

Agent Symphony uses that account to work with issues and pull requests. For unattended setups, `gh` can read `GH_TOKEN` or `GITHUB_TOKEN` instead; Agent Symphony does not store it. See [GitHub CLI integration](github-cli.md).

## 3. Initialize the repository

Run inside the Git repository Agent Symphony should operate on. It must have a GitHub `origin`:

```sh
cd /path/to/repository
agent-symphony init
agent-symphony validate
```

The defaults use Codex. If you use another coding agent, update the `commands` section in `.agent-symphony.yaml` and list any API key variable it needs. See the [CLI reference](cli.md#configuration) for the full schema.

## 4. Run diagnostics

```sh
agent-symphony doctor --runtime-state ~/.local/state/agent-symphony
```

The GitHub CLI, connectivity, and permissions diagnostics should pass and identify the authenticated account and configured repository. If authentication fails, run `gh auth login`; if access is incomplete, update the account or repository permissions and use `gh auth refresh` when needed.

## 5. Label a test issue and reconcile

Open an issue with any body text and add `agent-ready` plus exactly one configured priority label. The same GitHub account authenticated by `gh` may create the issue, apply labels, approve it, and leave feedback. Then choose one completion path:

- For unattended completion, also add `autonomous-merge`.
- For human review, omit that label and post the exact `/agent-symphony approve` comment from an authorized account.

See [Issue eligibility and recorded blockers](cli.md#issue-eligibility-and-recorded-blockers) for every remaining dispatch and merge restriction. The latest blockers are persisted at `<runtime-state>/status.json`.

Run one reconciliation:

```sh
agent-symphony reconcile \
  --state ~/.local/state/agent-symphony/pr.json \
  --runtime-state ~/.local/state/agent-symphony

agent-symphony status \
  --state ~/.local/state/agent-symphony/pr.json \
  --runtime-state ~/.local/state/agent-symphony
```

For continuous operation, use `serve`; it reconciles immediately and then polls GitHub at most every 60 seconds:

```sh
agent-symphony serve \
  --state ~/.local/state/agent-symphony/pr.json \
  --runtime-state ~/.local/state/agent-symphony
```

## Multiple repositories on one host

Run one separately supervised process from each repository checkout and give each repository its own state directory. These are two independent service commands, not a single shell sequence:

```sh
cd /path/to/first-repository
agent-symphony serve \
  --state ~/.local/state/agent-symphony/first/pr.json \
  --runtime-state ~/.local/state/agent-symphony/first
```

```sh
cd /path/to/second-repository
agent-symphony serve \
  --state ~/.local/state/agent-symphony/second/pr.json \
  --runtime-state ~/.local/state/agent-symphony/second
```

Each process still manages only its configured repository. The daemons share the authenticated GitHub CLI account and, in advanced mode, the provisioned worker/reviewer identities; source bundle, worktree, review snapshot, and tmux session names include the repository identity. Concurrency remains per daemon, so size the sum of their configured capacities for the host.

Before upgrading an existing installation to the first release with repository-namespaced reviewer sessions, let active independent reviews finish and reconcile their cleanup. Implementation attempt identities are unchanged.

## Advanced: host-isolated mode

The default boundary runs implementation and review processes as the coordinator's operating-system user while filtering their environment, remotes, and credential helpers. For OS-enforced separation, install a versioned release as root and provision the fixed worker and reviewer identities:

```sh
sudo mkdir -p /usr/local/libexec/agent-symphony/VERSION
sudo install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony \
  /usr/local/libexec/agent-symphony/VERSION/agent-symphony
sudo /usr/local/libexec/agent-symphony/VERSION/agent-symphony \
  install-host --coordinator "$(whoami)"
```

Rerun `install-host` after every binary upgrade. `doctor` and `serve` automatically require the stricter boundary once it is installed. The boundary environment variables are test seams, not production setup.
