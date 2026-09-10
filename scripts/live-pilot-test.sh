#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/agent-symphony-live-pilot-test.XXXXXX")
trap 'rm -rf "$test_root"' EXIT HUP INT TERM
fake_bin="$test_root/bin"
mkdir -m 700 "$fake_bin"
log="$test_root/gh.log"

cat >"$fake_bin/gh" <<'EOF'
#!/bin/sh
set -eu
printf '%s\n' "$*" >>"$LIVE_PILOT_TEST_LOG"
case "$1 $2" in
  'repo view') printf 'SysSU/agent-symphony-sample\ttrue\n' ;;
  'repo clone') mkdir -p "$4" ;;
  'issue create') printf 'https://github.com/SysSU/agent-symphony-sample/issues/99\n' ;;
  'issue view') printf 'OPEN\n' ;;
  'pr list') printf '[]\n' ;;
  'api --paginate')
    case " $* " in
      *' --slurp '*) ;;
      *) exit 91 ;;
    esac
    case "$*" in
      *'/issues?state=all&per_page=100'*)
        case "$LIVE_PILOT_TEST_SCENARIO" in
          pagination) printf '%s\n' '[[],[{"number":101,"title":"Protected issue","state":"open","html_url":"https://example.invalid/issues/101","labels":[{"name":"agent-ready"}]}]]' ;;
          duplicate) printf '%s\n' '[[{"number":4,"title":"Live pilot duplicate-run","state":"closed","html_url":"https://example.invalid/issues/4","labels":[]}]]' ;;
          *) printf '%s\n' '[[]]' ;;
        esac
        ;;
      *'/pulls?state=open&per_page=100'*)
        case "$LIVE_PILOT_TEST_SCENARIO" in
          pagination) printf '%s\n' '[[],[{"number":202,"title":"Protected PR","html_url":"https://example.invalid/pulls/202","head":{"ref":"agent-symphony/protected"},"mergeable_state":"clean"}]]' ;;
          *) printf '%s\n' '[[]]' ;;
        esac
        ;;
      *) exit 92 ;;
    esac
    ;;
  *) exit 93 ;;
esac
EOF

cat >"$fake_bin/go" <<'EOF'
#!/bin/sh
set -eu
output=
while [ "$#" -gt 0 ]; do
  if [ "$1" = -o ]; then shift; output=$1; fi
  shift
done
test -n "$output"
cat >"$output" <<'SCRIPT'
#!/bin/sh
set -eu
case "$1" in
  init)
    printf '%s\n' '{"commands":{"orchestrator":[],"orchestrator_audit":[]}}' >.agent-symphony.yaml
    ;;
  serve)
    exec ruby -e 'Signal.trap("INT") { exit }; sleep'
    ;;
  *) exit 94 ;;
esac
SCRIPT
chmod 0700 "$output"
EOF

cat >"$fake_bin/git" <<'EOF'
#!/bin/sh
exit 0
EOF

cat >"$fake_bin/tmux" <<'EOF'
#!/bin/sh
exit 1
EOF

cat >"$fake_bin/date" <<'EOF'
#!/bin/sh
if [ "$LIVE_PILOT_TEST_SCENARIO" = failure ]; then exit 17; fi
exec /bin/date "$@"
EOF
chmod 0700 "$fake_bin/gh" "$fake_bin/go" "$fake_bin/git" "$fake_bin/tmux" "$fake_bin/date"

report="$test_root/pagination.json"
set +e
PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=pagination LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=pagination-run "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 3
ruby -rjson -e 'r=JSON.parse(File.read(ARGV.fetch(0))); abort unless r["status"]=="blocked" && r.dig("preflight","eligible_issues").length==1 && r.dig("preflight","managed_pull_requests").length==1' "$report"
! grep -q '^issue create' "$log"

: >"$log"
report="$test_root/duplicate.json"
set +e
PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=duplicate LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=duplicate-run "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 3
ruby -rjson -e 'r=JSON.parse(File.read(ARGV.fetch(0))); abort unless r["status"]=="blocked" && r["blocker"].include?("already used") && r.dig("preflight","used_run_id").length==1' "$report"
! grep -q '^issue create' "$log"

: >"$log"
report="$test_root/failure.json"
set +e
PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=failure LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=failure-run "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 17
ruby -rjson -e '
  r=JSON.parse(File.read(ARGV.fetch(0))); commands=r.dig("cleanup","commands")
  abort unless r["status"]=="failed" && r["exit_status"]==17 && r.dig("created","issues")==[{"number"=>99,"url"=>"https://github.com/SysSU/agent-symphony-sample/issues/99"}]
  abort unless r.dig("cleanup","diagnostics_preserved") && commands.any? { |command| command.include?("gh issue close 99") } && commands.any? { |command| command.include?("only after preserving diagnostics") }
' "$report"
grep -q '^issue create' "$log"
runtime=$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).dig("created","runtime_roots",0)' "$report")
pilot_root=${runtime%/runtime}
case "$pilot_root" in /tmp/agent-symphony-failure-run.*) ;; *) exit 95;; esac
rm -rf "$pilot_root"

echo "live pilot safety tests passed"
