#!/bin/sh
set -eu

binary=$(cd "$(dirname "$1")" && pwd)/$(basename "$1")
remote=$(git config --get remote.origin.url)
tmp=$(mktemp -d "${TMPDIR:-/tmp}/agent-symphony-smoke.XXXXXX")
trap 'rm -rf "$tmp"' EXIT HUP INT TERM
mkdir "$tmp/bin" "$tmp/repo"
printf '#!/bin/sh\nexit 0\n' >"$tmp/bin/codex"
printf '#!/bin/sh\nexit 0\n' >"$tmp/bin/tmux"
chmod +x "$tmp/bin/codex" "$tmp/bin/tmux"
cd "$tmp/repo"
git init -q
git remote add origin "$remote"
PATH="$tmp/bin:$PATH" "$binary" init --json >init.json
PATH="$tmp/bin:$PATH" "$binary" validate --json >validate.json
PATH="$tmp/bin:$PATH" "$binary" config view --json >config.json
printf '[{"repository":"example/repository","issue":9,"attempt":1,"base_sha":"abcdef1","state":"queued"}]\n' >attempts.json
PATH="$tmp/bin:$PATH" "$binary" status --attempts attempts.json --json >status.json
PATH="$tmp/bin:$PATH" "$binary" inspect --issue 9 --attempts attempts.json --json >inspect.json
if PATH="$tmp/bin:$PATH" "$binary" install-host --coordinator smoke --json >/dev/null 2>&1; then
  echo "unprivileged install-host unexpectedly succeeded" >&2
  exit 1
fi
if printf '{}\n' | PATH="$tmp/bin:$PATH" "$binary" agent-host implementation >/dev/null 2>&1; then
  echo "unprivileged agent-host unexpectedly succeeded" >&2
  exit 1
fi
PATH="$tmp/bin:$PATH" "$binary" doctor --offline --json >doctor.json || test "$?" -eq 1
for result in init validate config status inspect doctor; do
  grep -q '"version":1' "$result.json"
  grep -q '"command"' "$result.json"
done
grep -q '"issue":9' inspect.json
grep -q '"diagnostics"' doctor.json
grep -q '"name":"platform","status":"pass"' doctor.json
grep -q '"name":"host isolation","status":"fail"' doctor.json
warnings=$(grep -o '"status":"warn"' doctor.json | wc -l | tr -d ' ')
test "$warnings" -eq 2
grep -q '"name":"GitHub permissions","status":"warn"' doctor.json
grep -q '"name":"GitHub policy","status":"warn"' doctor.json
echo "packaged binary smoke passed"
