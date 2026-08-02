#!/bin/sh
set -eu

version=${1:-0.0.0-local}
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-symphony-release.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
export GOCACHE="$tmp/go-cache"
export GOMODCACHE="$tmp/go-mod-cache"

go test -race ./...
go vet ./...
sh -n scripts/release.sh scripts/smoke-release.sh scripts/validate-release.sh
ruby -e 'require "yaml"; YAML.parse_file(".github/workflows/release-validation.yml")'
git diff --check
CGO_ENABLED=0 go build -o "$tmp/agent-symphony" ./cmd/agent-symphony
go test ./cmd/agent-symphony -run 'Test(PRGovernanceCommandWiresFakeGitHubAndRecoveryState|ProductionHandoffOutcomeIsCompletedWithoutRedelivery|DaemonLockIsSingleInstanceAndNoFollow)' -count=1
go test ./internal/orchestrator -run 'Test(ReconcileLoopRunsAtStartupAndRecoversAfterTransientOutage|RecoverRestartDuplicateStaleAndOrphans)' -count=1
go test ./internal/github -run 'Test(WebhookSecurityQueueAndDuplicate|ProductionReconcilerRunsRecoveredIssuesThenPullRequests|EvaluatePRGovernance)' -count=1
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

credential_pattern='(github_pat_[A-Za-z0-9_]+|gh[pousr]_[A-Za-z0-9]{20,}|-----BEGIN (RSA |EC |OPENSSH )?PRIVATE'" KEY-----)"
set +e
find . \( -path './.git' -o -path './.worktrees' -o -path './dist' \) -prune -o -type f -print0 | xargs -0 grep -qEI "$credential_pattern" >/dev/null 2>&1
scan_status=$?
set -e
if test "$scan_status" -eq 0; then
  echo 'credential-shaped material found' >&2
  exit 1
fi
test "$scan_status" -eq 1 || { echo 'credential scan failed' >&2; exit 1; }

for doc in README.md docs/PRD.md docs/architecture.md docs/cli.md docs/setup.md docs/security.md docs/recovery.md docs/troubleshooting.md docs/release-validation.md; do
  test -s "$doc" || { echo "missing documentation: $doc" >&2; exit 1; }
done

echo "local validation passed on $(go env GOOS)/$(go env GOARCH); Linux and WSL2 remain CI-required"
