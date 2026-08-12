# Setup

This walkthrough goes from a fresh machine to a working `reconcile`.

## 1. Install Agent Symphony and prerequisites

Install Git, tmux, [GitHub CLI](https://cli.github.com/), and the configured implementation/reviewer executables. Download the Agent Symphony release archive matching the host (`darwin` or `linux`, `amd64` or `arm64`; WSL2 uses `linux_amd64`) and verify it:

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

Authenticate the same operating-system account that will run Agent Symphony:

```sh
gh auth login
gh auth status
gh repo view OWNER/REPOSITORY
```

That account is the coordinator identity. It needs repository access for the issues, pull requests, reviews, statuses, rules, and mutations enabled by your policy. No Agent Symphony identity variables are required. For non-interactive use, `gh` can instead read `GH_TOKEN` or `GITHUB_TOKEN`; Agent Symphony does not parse or store it. See [GitHub CLI integration](github-cli.md).

## 3. Initialize the repository

Run inside the Git repository Agent Symphony should operate on. It must have a GitHub `origin`:

```sh
cd /path/to/repository
agent-symphony init
agent-symphony validate
```

Edit `.agent-symphony.yaml` to select the implementation and reviewer commands and add only the model-provider environment variable names those commands require. GitHub, Git askpass, SSH-agent, and cloud credential variables are rejected. See the [CLI reference](cli.md#configuration) for the full schema.

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
