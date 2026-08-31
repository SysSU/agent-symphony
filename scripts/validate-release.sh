#!/bin/sh
set -eu

version=${1:-0.0.0-local}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-symphony-release.XXXXXX")
trap 'chmod -R u+w "$tmp" 2>/dev/null || true; rm -rf "$tmp"' EXIT HUP INT TERM
export GOCACHE="$tmp/go-cache"
export GOMODCACHE="$tmp/go-mod-cache"

scripts/build-dashboard.sh
rm -rf cmd/agent-symphony/dashboard/node_modules
go test -race -p 1 ./...
scripts/lint.sh
rm -rf cmd/agent-symphony/dashboard/node_modules
sh -n scripts/*.sh
test -s cmd/agent-symphony/dashboard/out/index.html
test "$(git ls-files cmd/agent-symphony/dashboard/out | grep -vc '/.gitkeep$' || true)" -eq 0
git check-ignore -q cmd/agent-symphony/dashboard/out/index.html
test -s cmd/agent-symphony/dashboard/package-lock.json
scripts/credential-scan-test.sh
for script in scripts/*.sh; do
  ! grep -q "$(printf '\r')" "$script"
  test "$(git check-attr eol -- "$script")" = "$script: eol: lf"
done
ruby <<'RUBY'
require "yaml"
path = ".github/workflows/release-validation.yml"
workflow = YAML.parse_file(path).to_ruby
release = workflow.fetch("jobs").fetch("release").fetch("steps").find { |step| step["name"] == "Require signed tag" }.fetch("run").lines.map(&:strip)
fetch_tag = 'git fetch --no-tags origin "+refs/tags/${GITHUB_REF_NAME}:refs/tags/${GITHUB_REF_NAME}"'
check_tag = 'test "$(git cat-file -t "refs/tags/${GITHUB_REF_NAME}")" = tag'
resolve_tag = 'tag_commit=$(git rev-parse --verify "${GITHUB_REF_NAME}^{commit}")'
bind_tag = 'test "$tag_commit" = "$GITHUB_SHA"'
abort "release must fetch, check, and bind the exact tag object" unless release.index(fetch_tag) == 0 && release.index(check_tag) == 1 && release.index(bind_tag) == release.index(resolve_tag) + 1
runs = workflow.fetch("jobs").fetch("wsl2").fetch("steps").map { |step| step["run"] }.compact
snapshots = runs.flat_map(&:lines).grep(/commit -qm snapshot/)
abort "expected one WSL snapshot command" unless snapshots.length == 1
commands = snapshots.first.match(/bash -lc "(.*)"\s*$/)&.captures&.first&.split(/;\s*/)
chmod = "chmod 0755 scripts/credential-scan.sh scripts/credential-scan-test.sh scripts/lint.sh scripts/release.sh scripts/smoke-release.sh scripts/validate-release.sh"
abort "invalid WSL snapshot chmod" unless File.read(path).scan(/\bchmod\b/).length == 1 && commands&.count(chmod) == 1
index = commands.index(chmod)
abort "WSL snapshot chmod must immediately precede git init" unless index && commands[index + 1] == "git init -q"
RUBY
git -C "$tmp" init -q tag-binding
git -C "$tmp/tag-binding" config user.name test
git -C "$tmp/tag-binding" config user.email test@invalid
git -C "$tmp/tag-binding" commit --allow-empty -qm first
event_sha=$(git -C "$tmp/tag-binding" rev-parse HEAD)
git -C "$tmp/tag-binding" tag -am first v0.0.0
git -C "$tmp/tag-binding" commit --allow-empty -qm second
git -C "$tmp/tag-binding" tag -fam moved v0.0.0
! test "$(git -C "$tmp/tag-binding" rev-parse --verify 'v0.0.0^{commit}')" = "$event_sha"
grep -qF 'wsl --install --distribution $distribution --web-download --no-launch' .github/workflows/release-validation.yml
grep -qF 'sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends build-essential ca-certificates curl git ruby tmux xz-utils' .github/workflows/release-validation.yml
grep -qF "throw 'Failed to install WSL validation prerequisites'" .github/workflows/release-validation.yml
grep -qF "\$goArchiveVersion = '1.26.0'" .github/workflows/release-validation.yml
grep -qF 'https://go.dev/dl/go${goArchiveVersion}.linux-${goArch}.tar.gz' .github/workflows/release-validation.yml
grep -qF "sha256sum -c -" .github/workflows/release-validation.yml
grep -qF "throw 'Failed to install pinned Go toolchain in WSL'" .github/workflows/release-validation.yml
grep -qF "\$nodeVersion = '22.15.1'" .github/workflows/release-validation.yml
grep -qF 'https://nodejs.org/dist/v${nodeVersion}/node-v${nodeVersion}-linux-${nodeArch}.tar.xz' .github/workflows/release-validation.yml
grep -qF "'x86_64' { \$goArch = 'amd64'; \$goSHA256 = 'aac1b08a0fb0c4e0a7c1555beb7b59180b05dfc5a3d62e40e9de90cd42f88235'; \$nodeArch = 'x64'; \$nodeSHA256 = '7dca2ab34ec817aa4781e2e99dfd34d349eff9be86e5d5fbaa7e96cae8ee3179' }" .github/workflows/release-validation.yml
grep -qF "'aarch64' { \$goArch = 'arm64'; \$goSHA256 = 'bd03b743eb6eb4193ea3c3fd3956546bf0e3ca5b7076c8226334afe6b75704cd'; \$nodeArch = 'arm64'; \$nodeSHA256 = 'f4ae8ddf7487dfaf7da92fef463ee55cc29d8772d62891361dc3fc8b8e469205' }" .github/workflows/release-validation.yml
grep -qF "throw 'Failed to install pinned Node.js toolchain in WSL'" .github/workflows/release-validation.yml
grep -qF 'sudo tar -C /usr/local/node --strip-components=1 -xJf /tmp/node.tar.xz' .github/workflows/release-validation.yml
grep -qF "bash -lc 'cd ~/agent-symphony-ci && PATH=/usr/local/node/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin scripts/validate-release.sh 0.0.0-wsl'" .github/workflows/release-validation.yml
grep -qF "throw 'WSL release validation failed'" .github/workflows/release-validation.yml
! grep -F '$PATH' .github/workflows/release-validation.yml | grep -qF 'scripts/validate-release.sh 0.0.0-wsl'
git diff --check
CGO_ENABLED=0 go build -o "$tmp/agent-symphony" ./cmd/agent-symphony
go test ./cmd/agent-symphony -run 'Test(PRGovernanceCommandWiresFakeGitHubAndRecoveryState|ProductionHandoffOutcomeIsCompletedWithoutRedelivery|DaemonLockIsSingleInstanceAndNoFollow)' -count=1
go test ./internal/orchestrator -run 'Test(ReconcileLoopRunsAtStartupAndRecoversAfterTransientOutage|RecoverRestartDuplicateStaleAndOrphans)' -count=1
go test ./internal/github -run 'Test(CLITransportUsesGitHubCLIAuthenticatedSession|IssueControlsApprovalAndCredentialExclusion|SameUserFeedbackAllowedAndCoordinatorArtifactsFiltered|FetchIssueFactsAutonomousLabelsAuthorizeWithoutApproval|ProductionReconcilerRunsRecoveredIssuesThenPullRequests|EvaluatePRGovernance)' -count=1
go test ./internal/runtime -run 'Test(LifecycleCreatesUncredentialedRepositoryAndPreservesPrimary|AgentFailureCancelAndIneligibility)' -count=1

SOURCE_DATE_EPOCH=0 scripts/release.sh "$version" "$tmp/one"
SOURCE_DATE_EPOCH=0 scripts/release.sh "$version" "$tmp/two"
cmp "$tmp/one/SHA256SUMS" "$tmp/two/SHA256SUMS"
for archive in "$tmp/one"/*.tar.gz; do
  name=${archive##*/}
  cmp "$archive" "$tmp/two/$name"
done
go run ./tools/release -verify "$tmp/one"
host_os=$(go env GOOS)
host_arch=$(go env GOARCH)
tar -xzf "$tmp/one/agent-symphony_${version}_${host_os}_${host_arch}.tar.gz" -C "$tmp"
"$tmp/agent-symphony_${version}_${host_os}_${host_arch}/agent-symphony" --help >/dev/null
test "$("$tmp/agent-symphony_${version}_${host_os}_${host_arch}/agent-symphony" --version)" = "$version"
go version -m "$tmp/agent-symphony_${version}_${host_os}_${host_arch}/agent-symphony" >/dev/null
scripts/smoke-release.sh "$tmp/agent-symphony_${version}_${host_os}_${host_arch}/agent-symphony"

set +e
scripts/credential-scan.sh .
scan_status=$?
set -e
if test "$scan_status" -eq 1; then
  echo 'credential-shaped material found' >&2
  exit 1
fi
test "$scan_status" -eq 0 || { echo 'credential scan failed' >&2; exit 1; }

for doc in README.md docs/PRD.md docs/architecture.md docs/cli.md docs/setup.md docs/security.md docs/recovery.md docs/troubleshooting.md docs/releases.md docs/release-validation.md; do
  test -s "$doc" || { echo "missing documentation: $doc" >&2; exit 1; }
done

echo "local validation passed on $(go env GOOS)/$(go env GOARCH); Linux and WSL2 remain CI-required"
