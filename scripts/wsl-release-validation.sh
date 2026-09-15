#!/bin/sh
set -eu

mode=${1:-}
codex_native=${2:-}
case "$codex_native" in
/usr/local/codex/lib/node_modules/@openai/codex/node_modules/@openai/codex-linux-*/vendor/*/bin/codex) ;;
*) exit 2 ;;
esac
test -f "$codex_native" && test ! -L "$codex_native" && test -x "$codex_native"
export PATH="$(dirname "$codex_native"):/usr/local/node/bin:/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
cd "$HOME/agent-symphony-ci"

case "$mode" in
proof)
  test "$(id -u)" -ne 0
  test "$(command -v node)" = /usr/local/node/bin/node
  test "$(command -v codex)" = "$codex_native"
  test "$(/usr/local/node/bin/node --version)" = v22.15.1
  test "$("$codex_native" --version)" = "codex-cli 0.153.0"
  config=.agent-symphony-ci.yaml
  go run ./cmd/agent-symphony init --config "$config"
  sed "s#\"codex\"#\"$codex_native\"#g" "$config" > "$config.native"
  mv "$config.native" "$config"
  go run ./cmd/agent-symphony validate --config "$config"
  go run ./cmd/agent-symphony doctor --offline --config "$config" --runtime-state "$HOME/.local/state/agent-symphony-doctor"
  ;;
release)
  AGENT_SYMPHONY_REQUIRE_CODEX_SANDBOX=1 scripts/validate-release.sh 0.0.0-wsl
  ;;
*)
  echo "usage: $0 proof|release codex-native" >&2
  exit 2
  ;;
esac
