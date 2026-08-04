# Binary installation (no Go required)

Download `SHA256SUMS` and the archive matching `darwin` or `linux` and `amd64` or `arm64` from the GitHub Release. WSL2 uses `linux_amd64`. Verify and install it without Go:

```sh
grep "  agent-symphony_VERSION_OS_ARCH.tar.gz$" SHA256SUMS | shasum -a 256 -c -
tar -xzf agent-symphony_VERSION_OS_ARCH.tar.gz
sudo install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony /usr/local/bin/agent-symphony
agent-symphony --help
```

On Linux, `sha256sum -c SHA256SUMS` may instead verify every downloaded archive. Run `agent-symphony init`, review `.agent-symphony.yaml`, then run `validate` and `doctor` before `serve`. Use a native Linux or macOS filesystem; on WSL2, do not install or operate under `/mnt/c`.

Install the release as root-owned mode `0755` at `/usr/local/libexec/agent-symphony/<version>/agent-symphony`, then run that exact binary as root with `install-host --coordinator USER`. The command idempotently installs or validates the worker/reviewer identities, native roots, and exact versioned sudo rules. Rerun it after every binary upgrade. The coordinator invokes the installed `agent-host` boundaries automatically; `AGENT_SYMPHONY_WORKER_BOUNDARY` and `AGENT_SYMPHONY_REVIEW_BOUNDARY` are test seams, not production setup. Keep GitHub App credentials in coordinator-owned secret storage; only explicitly allowed model-provider variables cross the worker boundary.

See [CLI](cli.md) for configuration and [GitHub App setup](github-app.md) for permissions.

## Contributor build

Contributors need the Go version declared by `go.mod`, Git, and tmux. Build from source with `go build ./cmd/agent-symphony`; release users do not need Go.
