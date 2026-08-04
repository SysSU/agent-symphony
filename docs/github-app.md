# GitHub App setup and security

This document covers the host-side GitHub boundary. It does not start workers, create pull requests, schedule work, or merge.

For the step-by-step "create the App, install it, get credentials into the environment" walkthrough, see [Setup](setup.md#4-create-a-github-app). This document covers the exact permission/event list that walkthrough points to, plus the governance and credential-handling rules behind it.

## Required permissions and events

Create a single-repository installation (not "all repositories") with exactly:

| Permission | Access |
| --- | --- |
| Metadata | Read |
| Administration | Read |
| Issues | Read & write |
| Pull requests | Read & write |
| Contents | Read & write |
| Checks | Read & write |
| Commit statuses | Read |
| Members | Read — only if team-based authorization is configured |

These permissions govern API access and are required today: `reconcile`/`serve` read and write issues, PRs, contents, and checks directly through the API regardless of webhooks.

Leave **Active** unchecked under the Webhook section, and leave every event unsubscribed. The shipped `agent-symphony` binary does not currently bind an HTTP listener for webhook deliveries at all — `serve` runs the same reconciliation as `reconcile`, just on a repeating timer — so there is nothing to deliver to and nothing that reads the event subscription list. Event subscriptions only control what GitHub pushes to a webhook URL; they don't affect API access, so skipping them costs nothing right now.

## Pull-request governance

Agent-created pull requests link their issue and attempt and contain validation evidence, a documentation-impact assessment, and material implementation decisions. After validating a head, the coordinator posts the canonical `EvidenceBody` issue comment for validation or documentation. Reconciliation accepts a durable evidence fact only when the entire comment exactly equals that canonical body for the issue, attempt, kind, and current head, and every comment page confirms both the configured App actor and `performed_via_github_app`; local state is only a cache. The coordinator mirrors the authorized issue control snapshot's human-review policy onto the pull request and publishes the required `agent-symphony/policy` check for each head SHA. A force-push makes prior validation and documentation evidence stale.

Open agent pull requests must be reconciled for their entire lifetime. Only freshly authorized, non-empty feedback is delegated; each item is recorded as pending, addressed, or blocked. Unauthorized feedback remains visible but cannot cause execution.

Autonomous merge uses the freshly observed head SHA and is attempted only after issue eligibility, feedback, reviews, required checks, branch currency and protection, App permission, path scope, and the policy check all permit it. A recovered dispatched merge is resolved only by `GET /repos/{owner}/{repo}/pulls/{pull_number}/merge`: 204 suppresses retry as merged, 404 records a definitive unmerged result that permits a later freshly gated attempt, and every other result remains suppressive. The coordinator never retries an ambiguous merge, bypasses protection, force-pushes, dismisses reviews, or uses admin merge.

Supply the App ID, installation ID, PEM private key, and webhook secret from coordinator-owned environment/secret storage. Never put them in `.agent-symphony.yaml`, Git, command arguments, worker environments, tmux, or logs. `internal/github` provides the App-JWT-to-installation-token exchange (`AppJWT`, `InstallationTokens`) as a library capability, but the shipped CLI commands (`serve`, `reconcile`, `status`, `pr-governance`, ...) consume an already-minted installation token directly through `GITHUB_TOKEN` — they do not perform that exchange themselves. See [Setup](setup.md#6-get-credentials-into-the-environment) for how to mint one. Agent environments start from a small safe allowlist; separately configured model credential variable names must be added explicitly, while unrelated inherited variables are excluded.

Webhook requests are body-bounded, HMAC-SHA256 verified over exact bytes, and repository/installation bound. Accepted events are only reconciliation hints; a bounded delivery cache reduces duplicate reads, while authoritative periodic reads recover redelivery, reordering, eviction, and crash-after-acknowledgement. A full queue returns `503` so GitHub can retry.

Operational credential-exclusion checks and incident handling are in [security.md](security.md); restart procedures are in [recovery.md](recovery.md).

Issue controls require an open issue, readiness, exactly one P1-P3 label, the configured dependency section, resolved explicit dependencies, and a non-conflicting completion policy. Body controls are inert until a freshly authorized non-App actor posts the exact approval command. Each App snapshot binds the approved body/control hashes and exactly one authorized immutable provenance event for every current non-body control: readiness, priority, completion policy, open/closed state, cancellation, and retry. Governance accepts only the enriched `controls:v2` snapshot schema. Legacy `controls:v1` snapshots fail closed and must be regenerated through fresh authorized intake and approval; unverifiable v1 provenance is never migrated. Snapshot construction and validation also require an authoritative timeline lookup to match each exact control name, value, event ID, and actor ID. Missing, invented, duplicate, extra, conflicting, edited, stale, closed, unauthorized, rate-limited, or permission-revoked state blocks mutation and requires reconciliation.

Reads use conditional requests and bounded retry for transient/rate-limit responses. Mutations require issue/attempt attribution persisted in the GitHub body and are never blindly retried after an ambiguous result. Errors and diagnostics redact known and credential-shaped values.
