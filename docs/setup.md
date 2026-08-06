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

- `commands.implementation` / `commands.reviewer` — the coding-agent CLI to run (defaults to `codex exec --sandbox workspace-write` for noninteractive source edits in the assigned worktree and read-only `codex review`). Agent Symphony validates and commits completed source edits through its bounded implementation worker, so Codex does not need access to protected Git metadata.
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

github.com → Settings → Developer settings → GitHub Apps → **New GitHub App**. Set the exact permissions listed in [GitHub App setup](github-app.md#required-permissions-and-events). Generate a private key (downloads a `.pem`) and note the **App ID** shown on the app's settings page.

The webhook is optional — periodic polling (at most every 60 seconds) is always the authoritative recovery path for `serve`, with or without one; a webhook only wakes it up sooner between polls. `reconcile`/`doctor` never use it at all.

- **Testing with `reconcile`/`doctor`, or running `serve` without the optional webhook:** leave **Active** unchecked and every event unsubscribed — there's nothing to deliver to.
- **Running `serve` with the webhook enabled:** check **Active**, and see step 8 for the URL/secret/events to configure — you'll need the address `serve` listens on first, which comes after step 6.

## 5. Install the App

On the App's settings page → **Install App** → select the **one repository** you're testing against, per [GitHub App setup](github-app.md#required-permissions-and-events). The resulting URL looks like `github.com/settings/installations/<id>` (or `github.com/organizations/<org>/settings/installations/<id>`) — that trailing number is your installation ID.

## 6. Get credentials into the environment

Both paths below need two env vars first — get these regardless of which one you pick:

```sh
export AGENT_SYMPHONY_GITHUB_APP_ID=<app id from step 4>
export AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID=<app's bot account user id, see below>
```

`AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID` is the numeric user ID of the App's bot account — distinct from the App ID above:

```sh
curl -s "https://api.github.com/users/<app-slug>%5Bbot%5D" | jq .id
```

(`<app-slug>` is the URL-safe name you gave the App when creating it; `%5B`/`%5D` are the URL-encoded `[`/`]` around `bot`.)

### Production: point `serve` at the PEM directly (recommended)

A real installation token expires after 1 hour, hard limit, and nothing can refresh it inside an already-running process — so a static token cannot keep `serve` authenticated past that first hour. Give it the App's own credentials instead and it mints and refreshes its own tokens for as long as it runs:

```sh
export AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH=/path/to/private-key.pem   # from step 4
export AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID=<installation id from step 5>
```

That's it — no token to mint or rotate by hand. Keep the PEM file readable only by the account running `agent-symphony` and never commit it.

### Quick/testing: a hand-minted static token

For a short one-off `reconcile`/`doctor` run — not `serve` — skip the PEM and supply an installation token directly:

```sh
export GITHUB_TOKEN=<a short-lived installation token, see below>
```

Mint one with the App's JWT (standard GitHub App auth flow; needs `openssl`, `curl`, `jq`):

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

It expires in an hour; rerun this to get a fresh one. Fine for a quick check, not for anything long-running.

### Confirm it works

```sh
agent-symphony doctor --runtime-state ~/.local/state/agent-symphony
```

`GitHub permissions` should now `PASS`, regardless of which of the two paths above you used.

## 7. Label a test issue and dispatch it

On the repository, open an issue and add two labels: `agent-ready` and one of `priority:P1` / `priority:P2` / `priority:P3` (exact text comes from `.agent-symphony.yaml`'s `labels` section — the defaults from a fresh `init` match those shown here).

```sh
agent-symphony reconcile --state ~/.local/state/agent-symphony/pr.json --runtime-state ~/.local/state/agent-symphony
```

**You should see:** the command exits `0`, and a new directory appears under `~/.local/state/agent-symphony/worktrees/` for that issue — your configured coding-agent command running under your own OS user against a private, isolated worktree, with no GitHub/SSH/cloud credentials in its environment. Recovery manifests and retained logs remain under `~/.local/state/agent-symphony/attempts/`.

```sh
agent-symphony status --state ~/.local/state/agent-symphony/pr.json --runtime-state ~/.local/state/agent-symphony
```

projects that same attempt as queued/active/blocked/review-ready/completed.

For a persistent loop instead of a one-shot run, use `serve` in place of `reconcile` — see [CLI reference](cli.md). It runs the same reconciliation on a timer (at most every 60 seconds) and needs nothing beyond what you already set up above.

## 8. (optional) Enable the webhook for `serve`

Periodic polling is always the authoritative recovery path — this step only makes `serve` react sooner between polls; skip it if the 60-second polling floor is fine for you.

```sh
export AGENT_SYMPHONY_WEBHOOK_ADDR=127.0.0.1:8443     # address agent-symphony listens on
export AGENT_SYMPHONY_WEBHOOK_SECRET=<a strong random secret>
export AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID=<installation id from step 5>   # required for the webhook regardless of which credential path from step 6 you used
```

`agent-symphony` speaks plain HTTP — put a reverse proxy in front of `AGENT_SYMPHONY_WEBHOOK_ADDR` to terminate TLS and expose it at a public URL GitHub can reach. Then, on the App's settings page:

- Check **Active** under the Webhook section.
- Webhook URL: the public URL your reverse proxy exposes.
- Webhook secret: the exact same value as `AGENT_SYMPHONY_WEBHOOK_SECRET`.
- Subscribe to: Issues, Issue comment, Pull request, Pull request review, Pull request review comment, Check run, Check suite, Status, Push, Installation, Repository rule.

Run `serve` as normal after that — it binds `AGENT_SYMPHONY_WEBHOOK_ADDR` alongside its usual polling loop, and a valid signed delivery wakes the next reconcile immediately instead of waiting for the timer.

## Advanced: host-isolated mode (optional)

Host isolation trades zero-admin simplicity for OS-enforced separation between the coordinator and the implementation/review agents: a host administrator provisions separate, unprivileged `agent-symphony-worker`/`agent-symphony-reviewer` OS identities so a compromised or misbehaving agent cannot read the coordinator's credentials even if it deliberately tries. Everything in steps 1–7 above still applies except step 1's install location — do this instead:

```sh
sudo mkdir -p /usr/local/libexec/agent-symphony/VERSION
sudo install -m 0755 agent-symphony_VERSION_OS_ARCH/agent-symphony \
  /usr/local/libexec/agent-symphony/VERSION/agent-symphony
sudo /usr/local/libexec/agent-symphony/VERSION/agent-symphony install-host --coordinator "$(whoami)"
```

That command idempotently installs or validates the worker/reviewer identities, native roots, and exact versioned sudo rules. Rerun it after every binary upgrade. Once installed, `doctor` and `serve` automatically detect and require this stricter boundary instead of the zero-admin default — there is no separate flag to opt back out short of removing the provisioned identities. `AGENT_SYMPHONY_WORKER_BOUNDARY` and `AGENT_SYMPHONY_REVIEW_BOUNDARY` are test seams, not production setup.
