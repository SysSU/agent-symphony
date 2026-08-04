# Setup

## Default: zero-admin install

Download `SHA256SUMS` and the archive matching `darwin` or `linux` and `amd64` or `arm64` from the GitHub Release. WSL2 uses `linux_amd64`. Verify and install it without Go or root:

```sh
grep "  agent-symphony_VERSION_OS_ARCH.tar.gz$" SHA256SUMS | shasum -a 256 -c -
tar -xzf agent-symphony_VERSION_OS_ARCH.tar.gz
install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony ~/.local/bin/agent-symphony  # any directory on your PATH works
agent-symphony --help
```

On Linux, `sha256sum -c SHA256SUMS` may instead verify every downloaded archive. Run `agent-symphony init`, review `.agent-symphony.yaml`, then run `validate` and `doctor` before `serve`. Use a native Linux or macOS filesystem; on WSL2, do not install or operate under `/mnt/c`.

No separate coordinator OS account, `sudo`, or `install-host` step is required. You keep using the account you're already logged in as, admin or not: the coordinator and the implementation/review boundary run as that same OS user. The first time `doctor` or `serve` runs, it provisions private (mode `0700`) attempt and snapshot directories under the runtime state root (`--runtime-state`, or `$XDG_STATE_HOME/agent-symphony`/`~/.local/state/agent-symphony` on Linux/WSL2, `~/Library/Application Support/agent-symphony` on macOS by default). See [Security](security.md) for exactly what this default mode protects against and what it does not, and consider the advanced path below if your threat model needs OS-enforced isolation between the coordinator and the agent process.

See [CLI](cli.md) for configuration and [GitHub App setup](github-app.md) for permissions.

## Advanced: host-isolated mode (optional)

Host isolation trades zero-admin simplicity for OS-enforced separation between the coordinator and the implementation/review agents: a host administrator provisions separate, unprivileged `agent-symphony-worker`/`agent-symphony-reviewer` OS identities so a compromised or misbehaving agent cannot read the coordinator's credentials even if it deliberately tries.

Install the release as root-owned mode `0755` at `/usr/local/libexec/agent-symphony/<version>/agent-symphony`, then run that exact binary as root with `install-host --coordinator USER`. The command idempotently installs or validates the worker/reviewer identities, native roots, and exact versioned sudo rules. Rerun it after every binary upgrade. Once installed, `doctor` and `serve` automatically detect and require this stricter boundary instead of the zero-admin default — there is no separate flag to opt back out short of removing the provisioned identities. The coordinator invokes the installed `agent-host` boundaries automatically; `AGENT_SYMPHONY_WORKER_BOUNDARY` and `AGENT_SYMPHONY_REVIEW_BOUNDARY` are test seams, not production setup. Keep GitHub App credentials in coordinator-owned secret storage; only explicitly allowed model-provider variables cross the worker boundary.

## Contributor build

Contributors need the Go version declared by `go.mod`, Git, and tmux. Build from source with `go build ./cmd/agent-symphony`; release users do not need Go.
