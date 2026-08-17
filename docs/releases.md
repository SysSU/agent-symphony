# Releasing Agent Symphony

This runbook creates an official GitHub Release. Pull-request artifacts are test builds only; the release workflow rebuilds the binaries after a signed version tag is pushed.

## Before you start

You need:

- repository administrator access;
- Git, GitHub CLI, Go 1.26, Ruby, and tmux;
- Node.js 20.9 or newer and npm when dashboard sources or lockfiles changed;
- the SSH private key that matches an entry in `.github/release-signing-allowed-signers`;
- a clean checkout of protected `main` containing every change intended for the release.

Never commit or upload the private signing key. If the approved key is unavailable, stop and ask its owner to sign the tag.

## 1. Choose the version

List existing releases and tags:

```sh
gh release list --limit 20
git fetch origin main --tags
git tag --sort=-version:refname | head -20
```

Agent Symphony uses semantic versions. While the project is below `1.0.0`, use a patch version for a compatible fix and a minor version for a new feature or incompatible behavior. Do not reuse a version that already has a local or remote tag.

Use the version without `v` in build commands and with `v` in the Git tag. For example:

```sh
version=X.Y.Z
```

## 2. Merge the release changes

Every release change must reach `main` through a pull request. Wait for the required `release-validation` checks and resolve every review conversation before merging.

```sh
gh pr checks PR_NUMBER --required
gh pr view PR_NUMBER --json mergeable,mergeStateStatus,statusCheckRollup
```

After the pull request is merged, update the local checkout without rewriting history:

```sh
git switch main
git fetch origin main --tags
git merge --ff-only origin/main
test -z "$(git status --porcelain)"
test "$(git rev-parse HEAD)" = "$(git rev-parse origin/main)"
```

## 3. Validate the exact release commit

If dashboard sources or lockfiles changed, rebuild the committed export and confirm the rebuild is clean:

```sh
scripts/build-dashboard.sh
git diff --exit-code -- cmd/agent-symphony/dashboard/out
```

Run the complete local release gate from the clean `main` checkout:

```sh
scripts/validate-release.sh "$version"
```

This runs race tests, vet, security and workflow checks, reproducible builds, archive verification, packaged smoke tests, and the credential scan. A local pass does not replace the native macOS, Linux, and WSL2 jobs that run again for the tag.

## 4. Configure SSH tag signing

Configure Git to use the approved SSH key. The path may name the private key, or its public key when the matching private key is available through `ssh-agent`.

```sh
git config --local gpg.format ssh
git config --local user.signingkey /path/to/approved_signing_key
git config --local gpg.ssh.allowedSignersFile .github/release-signing-allowed-signers
```

These settings stay in the local repository configuration. Do not add the private key or its path to a tracked file.

## 5. Create and verify the tag

Create an annotated, signed tag on the exact validated commit:

```sh
git tag -s "v$version" -m "Agent Symphony v$version"
test "$(git cat-file -t "v$version")" = tag
git verify-tag "v$version"
tag_commit=$(git rev-parse --verify "v$version^{commit}")
test "$tag_commit" = "$(git rev-parse HEAD)"
git merge-base --is-ancestor "$tag_commit" origin/main
```

Review the tag before publishing it:

```sh
git show --show-signature "v$version"
```

If local verification fails and the tag has never been pushed, delete only that local tag, fix the signing setup, and create it again. Never move, replace, or reuse a published tag.

## 6. Publish the tag

Push only the verified tag:

```sh
git push origin "refs/tags/v$version"
```

The tag starts `.github/workflows/release-validation.yml`. The workflow:

1. runs the test and release-validation gates on macOS and Linux;
2. runs the release-validation gate inside WSL2;
3. builds and verifies four release archives plus `SHA256SUMS`;
4. verifies the tag against the allowed signers file from protected `main`;
5. publishes the GitHub Release;
6. downloads and smoke-tests the published Linux archive.

Do not manually promote a pull-request artifact or run `gh release create`. The tag workflow owns official artifacts and publication.

## 7. Watch the workflow

Find the tag run, then wait for it to finish:

```sh
gh run list --workflow release-validation --limit 10
gh run watch RUN_ID --exit-status
gh run view RUN_ID
```

The `test (macos-latest)`, `test (ubuntu-latest)`, `wsl2`, `artifacts`, and `release` jobs must succeed.

## 8. Verify the published release

Confirm the release and download its assets into a new temporary directory:

```sh
gh release view "v$version" --json tagName,url,isDraft,isPrerelease
release_dir=$(mktemp -d)
gh release download "v$version" --dir "$release_dir"
go run ./tools/release -verify "$release_dir"
```

The release must contain these files:

- `agent-symphony_VERSION_darwin_amd64.tar.gz`
- `agent-symphony_VERSION_darwin_arm64.tar.gz`
- `agent-symphony_VERSION_linux_amd64.tar.gz`
- `agent-symphony_VERSION_linux_arm64.tar.gz`
- `SHA256SUMS`

Record the release URL, checksum URL, and native CI run links in the release or pilot record described in [Release validation and pilot evidence](release-validation.md).

## If publication fails

- Do not move, delete, or reuse a tag that was pushed.
- Preserve the failed workflow run and open a GitHub issue with its URL and diagnosis.
- Fix the problem through a pull request to `main`.
- Publish the fix under the next version.
- If a bad release was already published, keep its evidence and publish a corrected version instead of replacing its files.
