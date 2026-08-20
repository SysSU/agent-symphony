# Setup

This walkthrough goes from a fresh machine to a working `reconcile`.

## What you need

- macOS, Linux, or WSL2.
- Agent Symphony, Git, tmux, and GitHub CLI.
- A GitHub account with `maintain` or `admin` permission on the repository, and a completed `gh auth login`.
- Codex set up, or another coding agent if you prefer.
- Go 1.26 only if you build Agent Symphony yourself.
- Node.js 20.9 or newer only if you build Agent Symphony from source.

## 1. Install Agent Symphony and prerequisites

Install Git, tmux, [GitHub CLI](https://cli.github.com/), and Codex (or the coding agent you plan to use). Download `SHA256SUMS` and the Agent Symphony archive matching the host from [GitHub Releases](https://github.com/SysSU/agent-symphony/releases) (`darwin` or `linux`, `amd64` or `arm64`; WSL2 uses `linux_amd64`), then verify it:

```sh
grep "  agent-symphony_VERSION_OS_ARCH.tar.gz$" SHA256SUMS | shasum -a 256 -c -
tar -xzf agent-symphony_VERSION_OS_ARCH.tar.gz
mkdir -p ~/.local/bin
install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony ~/.local/bin/agent-symphony
export PATH="$HOME/.local/bin:$PATH"
command -v agent-symphony
agent-symphony --help
```

Add `$HOME/.local/bin` to your shell startup file if it is not already on `PATH`. On Linux, replace the first command with `grep "  agent-symphony_VERSION_OS_ARCH.tar.gz$" SHA256SUMS | sha256sum -c -`. On WSL2, keep the repository and state on the Linux filesystem, never under `/mnt/c`. Contributors can instead build with the Go version pinned in `go.mod`:

```sh
scripts/build-dashboard.sh
go build -o agent-symphony ./cmd/agent-symphony
```

Released binaries already contain the dashboard and do not need Node.js at runtime.

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

### Create the default labels

Create any missing control labels. If the repository already uses one of these names, keep it and skip that command.

```sh
gh label create agent-ready --color 0E8A16 --description "Ready for Agent Symphony"
gh label create "priority:P1" --color B60205 --description "Highest Agent Symphony priority"
gh label create "priority:P2" --color D97706 --description "Middle Agent Symphony priority"
gh label create "priority:P3" --color FBCA04 --description "Lowest Agent Symphony priority"
gh label create needs-human-review --color D93F0B --description "Require explicit human review"
gh label create autonomous-merge --color 5319E7 --description "Allow policy-controlled autonomous merge"
```

If you change the names in `.agent-symphony.yaml`, create those names instead. See [GitHub controls](github-controls.md) for their meaning.

## 4. Run diagnostics

```sh
agent-symphony doctor --runtime-state ~/.local/state/agent-symphony
```

The GitHub CLI, connectivity, and permissions diagnostics should pass and identify the authenticated account and configured repository. If authentication fails, run `gh auth login`; if access is incomplete, update the account or repository permissions and use `gh auth refresh` when needed.

## 5. Label a test issue and reconcile

Open an issue with any body text and add `agent-ready` plus exactly one configured priority label. Apply `agent-ready` after the final body edit; that label authorizes dispatch and pull-request creation. Then choose one completion path:

- For unattended completion, also add `autonomous-merge`.
- For normal GitHub review and manual merge, omit `autonomous-merge`; Agent Symphony leaves the pull request open.

The optional `needs-human-review` label adds an explicit review-policy label and pending Check, but it is not required to keep a non-autonomous pull request open.

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

Success means the issue appears in the status output and its `action` explains the next step. If it does not start, use its `blockers`, `diagnostic`, and `action` fields with the [status interpretation guide](cli.md#status-and-next-actions).

For continuous operation, use `serve`; it reconciles immediately and then polls GitHub at most every 60 seconds:

```sh
agent-symphony serve \
  --state ~/.local/state/agent-symphony/pr.json \
  --runtime-state ~/.local/state/agent-symphony \
  --dashboard-address 127.0.0.1:8080
```

Open the dashboard URL printed by `serve`. It stays on loopback by default because it provides terminal access plus confirmed recovery and cleanup controls for exact attempts shown in current status.

See [Recovery](recovery.md) before using Recover, Archive, or Abandon.

To opt into direct access from another machine on a trusted network, bind a non-loopback address and supply the required password:

```sh
agent-symphony serve \
  --state ~/.local/state/agent-symphony/pr.json \
  --runtime-state ~/.local/state/agent-symphony \
  --dashboard-address 0.0.0.0:8080 \
  --allow-unsafe-dashboard-network \
  --dashboard-password-file ~/.config/agent-symphony/dashboard-password
```

Create the password file as the coordinator user with mode `0600`; it must contain one nonempty line. The HTTP Basic username is `agent-symphony`. This mode is deliberately named unsafe: HTTP does not encrypt the password or terminal traffic, and the dashboard has powerful local controls. Restrict the port with the host firewall and use a long, unique password. Use an encrypted VPN or tunnel instead on an untrusted network.

## Stop the daemon

Press `Ctrl-C` in the terminal running `serve`. A service manager should send `SIGTERM`. Agent Symphony stops the reconciliation loop and dashboard, and the next start reconstructs state from GitHub and the local runtime records.

## Multiple repositories on one host

Run one separately supervised process from each repository checkout and give each repository its own state directory. These are two independent service commands, not a single shell sequence:

```sh
cd /path/to/first-repository
agent-symphony serve \
  --state ~/.local/state/agent-symphony/first/pr.json \
  --runtime-state ~/.local/state/agent-symphony/first \
  --dashboard-address 127.0.0.1:8081
```

```sh
cd /path/to/second-repository
agent-symphony serve \
  --state ~/.local/state/agent-symphony/second/pr.json \
  --runtime-state ~/.local/state/agent-symphony/second \
  --dashboard-address 127.0.0.1:8082
```

Each process still manages only its configured repository. The daemons share the authenticated GitHub CLI account and, in advanced mode, the provisioned worker/reviewer identities; source bundle, worktree, review snapshot, and tmux session names include the repository identity. Concurrency remains per daemon, so size the sum of their configured capacities for the host.

Before upgrading an existing installation to the first release with repository-namespaced reviewer sessions, let active independent reviews finish and reconcile their cleanup. Implementation attempt identities are unchanged.

## Advisory orchestrator

New configuration created by `agent-symphony init` enables the advisory orchestrator. In zero-admin mode, its default Codex command lets the orchestrator use the coordinator user's authenticated `gh` CLI and inspect same-user tmux sessions:

```json
["codex", "-c", "projects={\"{orchestrator_workspace}\"={trust_level=\"trusted\"}}", "--sandbox", "danger-full-access", "--ask-for-approval", "never", "--no-alt-screen"]
```

Agent Symphony replaces `{orchestrator_workspace}` with the managed absolute path. Full Codex access is necessary for direct GitHub and tmux inspection, but it also exposes other resources readable by the coordinator user. Use it only with a trusted model. Advanced host isolation runs the orchestrator as the reviewer identity, which cannot inspect the coordinator user's GitHub login or tmux server. After changing the command, use the dashboard Rebuild action, then rerun `validate` and `doctor`.

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

When the orchestrator is configured in this mode, Agent Symphony launches it through the reviewer identity and snapshot group. Rerun `install-host` after upgrading so the exact reviewer-identity orchestrator rule remains current.
