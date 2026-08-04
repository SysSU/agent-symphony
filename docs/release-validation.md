# Release validation and pilot evidence

## Reproduce a candidate

Run `scripts/validate-release.sh VERSION`. It executes all tests with the race detector, `go vet`, a no-CGO build, focused orchestration/security checks, two byte-identical four-target builds, SHA-256 verification, a scan of every regular candidate file (including ignored and untracked files), and documentation-presence checks. `scripts/release.sh VERSION OUTPUT` refuses to overwrite an output directory and emits four `agent-symphony_VERSION_OS_ARCH.tar.gz` archives plus `SHA256SUMS`.

Each archive contains one self-contained executable. WSL2 uses `linux_amd64`. Cross-compilation is not an OS smoke pass: the workflow's macOS, Ubuntu, and Windows/WSL2 jobs must each succeed and their immutable run URLs must be recorded.

The packaged smoke runs the real binary for configuration/status/inspection and `doctor --offline`, and proves `install-host` and `agent-host` dispatch without privileged mutation. A missing host installation is an expected diagnostic failure in the disposable smoke repository. `validate-release.sh` separately exercises fake host commands/filesystems plus the production seams for `serve`, recovery, governance, handoffs, and runtime lifecycle. An online authenticated `doctor`, real privileged installation, host canaries, GitHub mutations, and end-to-end pilot remain explicit external gates.

## Signed release tags

Release tags must be annotated tags SSH-signed by an identity and public key pinned in `.github/release-signing-allowed-signers` on the protected `main` integration branch. The release job explicitly fetches `main` into `refs/remotes/origin/main`, requires the tagged commit to be an ancestor of that ref, exports the allowed-signers file from that ref into runner-temporary storage, and only then runs `git verify-tag`. It never trusts signer policy from the candidate tag or an ambiguous `FETCH_HEAD`. `gh --verify-tag` is only a remote-existence check.

Bootstrap uses the maintainer public key already pinned on protected `main`; the corresponding private key never enters the repository or CI. The maintainer configures `gpg.format ssh`, selects that private signing key locally, and creates an annotated signed tag with `git tag -s` only after the tagged commit and signer entry have landed on `main`. To rotate or add a maintainer or tag bot, obtain its public key through an independently authenticated channel, verify its SHA-256 fingerprint with the owner, add a reviewed `principal key-type key` line to `main`, and wait for that protected-branch change to land before the new key signs a tag. Remove an old line only in a later reviewed `main` change after its in-flight tags are retired. Never copy trust from a tag, API response, mutable key URL, or other candidate-controlled ref.

## Scenario evidence map

| Gate | Automated proof | External proof still required |
| --- | --- | --- |
| two independent concurrent issues; P1-P3/dependencies | scheduler race/unit tests use disjoint scopes, capacity, priority, and explicit dependency facts | pilot issue/attempt/PR links and timestamps |
| duplicate webhook/restart | delivery-cache, recovery, attempt-marker, feedback-claim, and PR-governance tests | pilot delivery/restart timestamps and resulting single attempt/PR |
| long-lived feedback and human review | immutable feedback/outcome and current-head governance tests | authorized comment, independent review, label-removal, checks, and merge links |
| credential exclusion | environment/redaction/runtime tests plus all-regular-candidate-file scan | redacted worker/log/tmux/artifact scan records and rotation confirmation if needed |
| macOS/Linux/WSL2 | native CI matrix and WSL2 execution job | successful run URLs; local cross-build alone is insufficient |
| artifacts/docs | reproducibility, checksum, archive, and documentation checks | published release URL and checksum verification record |

## Pilot evidence template

Copy this to the epic or release record. Do not replace blanks with invented results.

```text
Version/source commit:
Release URL and SHA256SUMS URL:
macOS CI run/job/result:
Linux CI run/job/result:
WSL2 CI run/job/result:
Independent issues (issue -> attempt -> PR, start/end timestamps):
Observed maximum concurrency and configured capacity:
P1/P2/P3 and dependency ordering (issue links + dispatch timestamps):
Duplicate delivery IDs/restart time -> single attempt/PR evidence:
Long-lived PR dates; authorized feedback IDs and dispositions:
Independent review result; human-review label removal event; required checks; merge:
Credential scan commands, scopes, timestamps, redacted results:
Documentation review commit/result:
Eligible pilot issues total:
Reached reviewable PR or authorized merge without stakeholder intervention:
Rate and target (>=80%):
Failures/blockers and time-to-diagnosis:
Evidence owner/date:
```

Pilot success is measurable only when the numerator, denominator, intervention definition, timestamps, and durable GitHub/CI/release links are present. Local tests establish readiness to pilot, not pilot success.
