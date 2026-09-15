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
scripts/live-pilot-test.sh
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
chmod = "chmod 0755 scripts/credential-scan.sh scripts/credential-scan-test.sh scripts/lint.sh scripts/live-pilot.sh scripts/live-pilot-test.sh scripts/release.sh scripts/smoke-release.sh scripts/validate-release.sh"
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
grep -qF 'sudo env DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends bubblewrap build-essential ca-certificates curl git ruby tmux xz-utils' .github/workflows/release-validation.yml
grep -qF "throw 'Failed to install WSL validation prerequisites'" .github/workflows/release-validation.yml
grep -qF 'cache="$RUNNER_TEMP/agent-symphony-codex-npm-cache"' .github/workflows/release-validation.yml
grep -qF 'sudo -H env "PATH=$PATH" npm install --global --prefix "$prefix" --cache "$cache" "@openai/codex@$CODEX_VERSION"' .github/workflows/release-validation.yml
grep -qF 'Darwin/x86_64) package=codex-darwin-x64; target=x86_64-apple-darwin' .github/workflows/release-validation.yml
grep -qF 'Darwin/arm64) package=codex-darwin-arm64; target=aarch64-apple-darwin' .github/workflows/release-validation.yml
grep -qF 'Linux/x86_64) package=codex-linux-x64; target=x86_64-unknown-linux-musl' .github/workflows/release-validation.yml
grep -qF 'Linux/aarch64) package=codex-linux-arm64; target=aarch64-unknown-linux-musl' .github/workflows/release-validation.yml
grep -qF 'native_root="$prefix/lib/node_modules/@openai/codex/node_modules/@openai/$package"' .github/workflows/release-validation.yml
grep -qF 'native="$native_root/vendor/$target/bin/codex"' .github/workflows/release-validation.yml
grep -qF 'sudo chown -R 0:0 "$prefix"' .github/workflows/release-validation.yml
grep -qF 'sudo chmod -R go-w "$prefix"' .github/workflows/release-validation.yml
grep -qF 'test -f "$native" && test ! -L "$native" && test -x "$native"' .github/workflows/release-validation.yml
grep -qF 'find "$native_root" ! -type d ! -type f -print -quit' .github/workflows/release-validation.yml
grep -qF 'find "$native_root" \( -type d -o -type f \) \( ! -user root -o -perm -020 -o -perm -002 \)' .github/workflows/release-validation.yml
grep -qF 'printf '\''CODEX_NATIVE=%s\n'\'' "$native" >> "$GITHUB_ENV"' .github/workflows/release-validation.yml
grep -qF 'dirname "$native" >> "$GITHUB_PATH"' .github/workflows/release-validation.yml
grep -qF 'sed "s#\"codex\"#\"$CODEX_NATIVE\"#g" "$config" > "$config.native"' .github/workflows/release-validation.yml
grep -qF "/usr/local/node/bin/npm install --global --prefix /usr/local/codex '@openai/codex@" .github/workflows/release-validation.yml
grep -qF 'test "$(command -v node)" = /usr/local/node/bin/node' .github/workflows/release-validation.yml
grep -qF 'test "$(command -v codex)" = "$CODEX_NATIVE"' .github/workflows/release-validation.yml
grep -qF 'test "$(/usr/local/node/bin/node --version)" = v22.15.1' .github/workflows/release-validation.yml
grep -qF 'test "$("$CODEX_NATIVE" --version)" = "codex-cli 0.153.0"' .github/workflows/release-validation.yml
grep -qF 'sed -i "s#\"codex\"#\"$CODEX_NATIVE\"#g" .agent-symphony-ci.yaml' .github/workflows/release-validation.yml
grep -qF 'kernel.unprivileged_userns_clone=1' .github/workflows/release-validation.yml
grep -qF 'kernel.apparmor_restrict_unprivileged_userns=0' .github/workflows/release-validation.yml
grep -qF "throw 'Failed to enable WSL unprivileged user namespaces'" .github/workflows/release-validation.yml
grep -qF "\$goArchiveVersion = '1.26.0'" .github/workflows/release-validation.yml
grep -qF 'https://go.dev/dl/go${goArchiveVersion}.linux-${goArch}.tar.gz' .github/workflows/release-validation.yml
grep -qF "sha256sum -c -" .github/workflows/release-validation.yml
grep -qF "throw 'Failed to install pinned Go toolchain in WSL'" .github/workflows/release-validation.yml
grep -qF "\$nodeVersion = '22.15.1'" .github/workflows/release-validation.yml
grep -qF 'https://nodejs.org/dist/v${nodeVersion}/node-v${nodeVersion}-linux-${nodeArch}.tar.xz' .github/workflows/release-validation.yml
grep -qF "\$codexPlatform = 'linux-x64'; \$codexTarget = 'x86_64-unknown-linux-musl'" .github/workflows/release-validation.yml
grep -qF "\$codexPlatform = 'linux-arm64'; \$codexTarget = 'aarch64-unknown-linux-musl'" .github/workflows/release-validation.yml
grep -qF "throw 'Failed to install pinned Node.js toolchain in WSL'" .github/workflows/release-validation.yml
! grep -qF -- '--prefix /usr/local/node' .github/workflows/release-validation.yml
grep -qF "CODEX_VERSION: '0.153.0'" .github/workflows/release-validation.yml
grep -qF "\$codexVersion = '0.153.0'" .github/workflows/release-validation.yml
grep -qF "throw 'Failed to install pinned Codex CLI in WSL'" .github/workflows/release-validation.yml
grep -qF '$codexRoot = "/usr/local/codex/lib/node_modules/@openai/codex/node_modules/@openai/codex-${codexPlatform}"' .github/workflows/release-validation.yml
grep -qF '$codexNative = "/usr/local/codex/lib/node_modules/@openai/codex/node_modules/@openai/codex-${codexPlatform}/vendor/${codexTarget}/bin/codex"' .github/workflows/release-validation.yml
grep -qF "test -f '\${codexNative}' && test ! -L '\${codexNative}' && test -x '\${codexNative}'" .github/workflows/release-validation.yml
grep -qF "find '\${codexRoot}' ! -type d ! -type f -print -quit" .github/workflows/release-validation.yml
grep -qF 'find '\''${codexRoot}'\'' \( -type d -o -type f \) \( ! -user root -o -perm -020 -o -perm -002 \)' .github/workflows/release-validation.yml
grep -qF "throw 'Pinned WSL Codex installation is unsafe or invalid'" .github/workflows/release-validation.yml
grep -qF "throw 'WSL rootless Codex confinement proof failed'" .github/workflows/release-validation.yml
test "$(grep -cF 'CODEX_NATIVE=$1' .github/workflows/release-validation.yml)" -eq 2
test "$(grep -cF 'agent-symphony "$codexNative"' .github/workflows/release-validation.yml)" -eq 2
grep -qF 'PATH="$(dirname "$CODEX_NATIVE"):/usr/local/node/bin:/usr/local/go/bin:' .github/workflows/release-validation.yml
grep -qF 'AGENT_SYMPHONY_REQUIRE_CODEX_SANDBOX=1 scripts/validate-release.sh 0.0.0-wsl' .github/workflows/release-validation.yml
grep -qF "throw 'WSL release validation failed'" .github/workflows/release-validation.yml
! grep -F '$PATH' .github/workflows/release-validation.yml | grep -qF 'AGENT_SYMPHONY_REQUIRE_CODEX_SANDBOX=1 scripts/validate-release.sh 0.0.0-wsl'
git diff --check
CGO_ENABLED=0 go build -o "$tmp/agent-symphony" ./cmd/agent-symphony
go test ./cmd/agent-symphony -run 'Test(PRGovernanceCommandWiresFakeGitHubAndRecoveryState|ProductionHandoffOutcomeIsCompletedWithoutRedelivery|DaemonLockIsSingleInstanceAndNoFollow|DaemonGitHubAuthenticationBoundary|ReviewAuthenticationCrossesIndependentReviewBoundary|SudoPolicyPreservesOnlyBoundedGitHubEnvironment|AdvancedAgentHostRejectsLocalRootSeamBeforeExecution)' -count=1
go test ./internal/orchestrator -run 'Test(ReconcileLoopRunsAtStartupAndRecoversAfterTransientOutage|RecoverRestartDuplicateStaleAndOrphans)' -count=1
go test ./internal/orchestratoragent -run 'Test(OrchestratorAuthenticationCrossesItsSessionBoundary|HeartbeatAuthenticationCrossesItsOneShotBoundary)' -count=1
go test ./internal/github -run 'Test(CLITransportUsesGitHubCLIAuthenticatedSession|RedactEnvironmentRemovesRawCredentialValues|IssueControlsApprovalAndCredentialExclusion|SameUserFeedbackAllowedAndCoordinatorArtifactsFiltered|FetchIssueFactsAutonomousLabelsAuthorizeWithoutApproval|ProductionReconcilerRunsRecoveredIssuesThenPullRequests|EvaluatePRGovernance)' -count=1
go test ./internal/runtime -run 'Test(LifecycleCreatesCredentialedSessionWithoutCredentialedRepository|CredentialedSessionLaunchFailureIsRedacted|CredentialedPaneOutputIsRedactedBeforeLogPersistence|ImplementationAuthenticationCrossesRuntimeBoundary|TmuxSessionImportsAuthenticationWithoutPuttingValuesInArgv|AgentFailureCancelAndIneligibility)' -count=1

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
