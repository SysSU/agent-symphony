# Setup

A step-by-step walkthrough from a fresh machine to a working `reconcile`. Every command below is copy-pasteable; you shouldn't need to open another doc to know what to type next. Linked docs are reference material for people who want the design rationale, not required reading to get running.

## 1. Install the binary

No admin rights required. Download `SHA256SUMS` and the release archive matching your OS (`darwin`/`linux`) and architecture (`amd64`/`arm64`) from the GitHub Release. WSL2 uses `linux_amd64`.

```sh
grep "  agent-symphony_VERSION_OS_ARCH.tar.gz$" SHA256SUMS | shasum -a 256 -c -
tar -xzf agent-symphony_VERSION_OS_ARCH.tar.gz
install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony ~/.local/bin/agent-symphony
agent-symphony --help
```

- On Linux, `sha256sum -c SHA256SUMS` can instead verify every downloaded archive in one pass.
- On WSL2, keep the repository and all state on the Linux filesystem — never operate under `/mnt/c`.
- No separate coordinator OS account, `sudo`, or extra install step is needed for this default path: you keep using the account you're already logged in as, admin or not.
- Contributors building from source: `go build ./cmd/agent-symphony` instead, using the Go version pinned in `go.mod`. Release users don't need Go.

## 2. Initialize and validate your repository's config

Run these inside the Git repository you want `agent-symphony` to operate on — it needs a GitHub `origin` remote:

```sh
cd /path/to/your/repo
agent-symphony init        # writes .agent-symphony.yaml, refuses to overwrite an existing one
agent-symphony validate    # checks it
```

Open `.agent-symphony.yaml` and adjust two things for your setup:

- `commands.implementation` / `commands.reviewer` — the coding-agent CLI to run (defaults to `codex exec` / `codex review`).
- `commands.environment_allowlist` — add any model-provider credential variable name your agent needs (e.g. `OPENAI_API_KEY`). GitHub/SSH/cloud credential variables are rejected here even if listed; see [CLI reference](cli.md#configuration) for the full schema and why.

## 3. Run doctor offline

```sh
agent-symphony doctor --offline --runtime-state ~/.local/state/agent-symphony
```

Expect `PASS` on platform, git, tmux, your two configured commands, repository, and repository identity, plus:

```
PASS  host isolation zero-admin default boundary is active: no separate OS identity, reduced isolation from the agent process
```

That confirms the zero-admin default is working — no `install-host` step needed. A `WARN` on GitHub permissions is expected right now; that's fixed in step 6. See [Security](security.md) for exactly what this default boundary protects against (accidental credential leakage) and what it doesn't (a malicious or compromised agent process) — read that before pointing this at anything you don't fully trust the coding agent with.

## 4. Create a GitHub App

github.com → Settings → Developer settings → GitHub Apps → **New GitHub App**. Set the exact permissions and webhook events listed in [GitHub App setup](github-app.md#required-permissions-and-events). Generate a private key (downloads a `.pem`) and note the **App ID** shown on the app's settings page.

A webhook is only exercised if you run `serve`. If you're only testing with `reconcile`/`doctor`, uncheck **Active** under the Webhook section instead of entering a placeholder URL — GitHub won't require a URL or attempt any deliveries. Check it and fill in a real URL/secret only once you're ready to run `serve`.

## 5. Install the App

On the App's settings page → **Install App** → select the **one repository** you're testing against, per [GitHub App setup](github-app.md#required-permissions-and-events). The resulting URL looks like `github.com/settings/installations/<id>` (or `github.com/organizations/<org>/settings/installations/<id>`) — that trailing number is your installation ID.

## 6. Get credentials into the environment

```sh
export AGENT_SYMPHONY_GITHUB_APP_ID=<app id from step 4>
export AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID=<the App's bot account user id, see below>
export GITHUB_TOKEN=<a short-lived installation token, see below>
```

`agent-symphony` does not mint installation tokens from the PEM itself — you supply one via `GITHUB_TOKEN`. For local testing, mint one with the App's JWT (standard GitHub App auth flow; needs `openssl`, `curl`, `jq`):

```sh
APP_ID=<app id>
PEM_PATH=/path/to/private-key.pem
INSTALLATION_ID=<installation id from step 5>

now=$(date +%s)
b64() { openssl base64 -e -A | tr -d '=' | tr '/+' '_-'; }
header=$(printf '{"alg":"RS256","typ":"JWT"}' | b64)
payload=$(printf '{"iat":%d,"exp":%d,"iss":%s}' "$((now-60))" "$((now+300))" "$APP_ID" | b64)
signature=$(printf '%s.%s' "$header" "$payload" | openssl dgst -sha256 -sign "$PEM_PATH" | b64)
jwt="$header.$payload.$signature"

curl -s -X POST -H "Authorization: Bearer $jwt" -H "Accept: application/vnd.github+json" \
  "https://api.github.com/app/installations/$INSTALLATION_ID/access_tokens" | jq -r .token
```

The token expires in an hour; rerun this to get a fresh one. (Production deployments should mint tokens through your own secret-issuing process, not a hand-run script — never commit the PEM.)

Get `AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID` — the numeric user ID of the App's bot account, distinct from the App ID above:

```sh
curl -s "https://api.github.com/users/<app-slug>%5Bbot%5D" | jq .id
```

(`<app-slug>` is the URL-safe name you gave the App when creating it; `%5B`/`%5D` are the URL-encoded `[`/`]` around `bot`.)

Confirm everything works:

```sh
agent-symphony doctor --runtime-state ~/.local/state/agent-symphony
```

`GitHub permissions` should now `PASS`.

## 7. Label a test issue and dispatch it

On the repository, open an issue and add two labels: `agent-ready` and one of `priority:P1` / `priority:P2` / `priority:P3` (exact text comes from `.agent-symphony.yaml`'s `labels` section — the defaults from a fresh `init` match those shown here).

```sh
agent-symphony reconcile --state ~/.local/state/agent-symphony/pr.json --runtime-state ~/.local/state/agent-symphony
```

**You should see:** the command exits `0`, and a new directory appears under `~/.local/state/agent-symphony/attempts/` for that issue — your configured coding-agent command running under your own OS user against a private, isolated worktree, with no GitHub/SSH/cloud credentials in its environment.

```sh
agent-symphony status --state ~/.local/state/agent-symphony/pr.json --runtime-state ~/.local/state/agent-symphony
```

projects that same attempt as queued/active/blocked/review-ready/completed.

For a persistent loop instead of a one-shot run, use `serve` in place of `reconcile` — see [CLI reference](cli.md) — which additionally needs the webhook endpoint from step 4 to actually be reachable.

## Advanced: host-isolated mode (optional)

Host isolation trades zero-admin simplicity for OS-enforced separation between the coordinator and the implementation/review agents: a host administrator provisions separate, unprivileged `agent-symphony-worker`/`agent-symphony-reviewer` OS identities so a compromised or misbehaving agent cannot read the coordinator's credentials even if it deliberately tries. Everything in steps 1–7 above still applies except step 1's install location — do this instead:

```sh
sudo mkdir -p /usr/local/libexec/agent-symphony/VERSION
sudo install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony \
  /usr/local/libexec/agent-symphony/VERSION/agent-symphony
sudo /usr/local/libexec/agent-symphony/VERSION/agent-symphony install-host --coordinator "$(whoami)"
```

That command idempotently installs or validates the worker/reviewer identities, native roots, and exact versioned sudo rules. Rerun it after every binary upgrade. Once installed, `doctor` and `serve` automatically detect and require this stricter boundary instead of the zero-admin default — there is no separate flag to opt back out short of removing the provisioned identities. `AGENT_SYMPHONY_WORKER_BOUNDARY` and `AGENT_SYMPHONY_REVIEW_BOUNDARY` are test seams, not production setup.
