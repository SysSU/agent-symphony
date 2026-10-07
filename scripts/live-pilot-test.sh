#!/bin/sh
set -eu

project_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/agent-symphony-live-pilot-test.XXXXXX")
test_home=$(mktemp -d "$HOME/.aslp-home.XXXXXX")
unsafe_home=$(mktemp -d "$HOME/.aslp-unsafe-home.XXXXXX")
linked_home=$(mktemp -d "$HOME/.aslp-linked-home.XXXXXX")
trap 'rm -rf "$test_root" "$test_home" "$unsafe_home" "$linked_home"' EXIT HUP INT TERM
private_root="$test_home/.as-live-pilot"
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
  'issue view') case "$LIVE_PILOT_TEST_SCENARIO" in success|branch-query-error) printf 'CLOSED\n' ;; *) printf 'OPEN\n' ;; esac ;;
  'pr list')
    if [ "$LIVE_PILOT_TEST_SCENARIO" = success ] || [ "$LIVE_PILOT_TEST_SCENARIO" = branch-query-error ]; then
      printf '%s\n' '[{"number":100,"url":"https://example.invalid/pulls/100","state":"MERGED","isDraft":false,"headRefName":"agent-symphony/success","mergedAt":"2026-10-07T00:00:00Z"}]'
    else
      printf '[]\n'
    fi
    ;;
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
printf 'go %s\n' "$*" >>"$LIVE_PILOT_TEST_LOG"
if [ "$LIVE_PILOT_TEST_SCENARIO" = setup-failure ]; then exit 18; fi
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
	runtime=
	while [ "$#" -gt 0 ]; do
	  if [ "$1" = --runtime-state ]; then shift; runtime=$1; fi
	  shift
	done
	case "$runtime" in /tmp/*|/private/tmp/*|/var/tmp/*|/dev/shm/*) exit 95;; esac
    if [ "$LIVE_PILOT_TEST_SCENARIO" = success ]; then
      mkdir -p "$runtime/worker-executable/pinned"
      : >"$runtime/worker-executable/pinned/codex"
      chmod 0500 "$runtime/worker-executable/pinned" "$runtime/worker-executable/pinned/codex"
    fi
    if [ "$LIVE_PILOT_TEST_SCENARIO" = stuck ]; then exec ruby -e 'Signal.trap("INT") {}; sleep 30'; fi
    exec ruby -e 'Signal.trap("INT") { exit }; sleep'
    ;;
  *) exit 94 ;;
esac
SCRIPT
chmod 0700 "$output"
EOF

cat >"$fake_bin/git" <<'EOF'
#!/bin/sh
if [ "$1" = -C ] && [ "$3" = ls-remote ]; then
  case "$LIVE_PILOT_TEST_SCENARIO" in success) exit 2 ;; branch-query-error) exit 128 ;; esac
fi
exit 0
EOF

cat >"$fake_bin/codex" <<'EOF'
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
if [ "$LIVE_PILOT_TEST_SCENARIO" = stuck ]; then
  count_file=${LIVE_PILOT_TEST_LOG}.date
  count=0
  if [ -f "$count_file" ]; then count=$(cat "$count_file"); fi
  count=$((count + 1))
  printf '%s\n' "$count" >"$count_file"
  if [ "$count" -eq 1 ]; then printf '100\n'; else printf '1000\n'; fi
  exit 0
fi
exec /bin/date "$@"
EOF
chmod 0700 "$fake_bin/gh" "$fake_bin/go" "$fake_bin/git" "$fake_bin/codex" "$fake_bin/tmux" "$fake_bin/date"

mkdir -m 755 "$unsafe_home/.as-live-pilot"
: >"$log"
set +e
HOME="$unsafe_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=failure LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=unsafe-parent "$project_root/scripts/live-pilot.sh" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 2
! grep -q '^issue create' "$log"

ln -s "$test_root" "$linked_home/.as-live-pilot"
: >"$log"
set +e
HOME="$linked_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=failure LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=linked-parent "$project_root/scripts/live-pilot.sh" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 2
! grep -q '^issue create' "$log"

report="$test_root/pagination.json"
set +e
HOME="$test_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=pagination LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=pagination-run "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 3
ruby -rjson -e 'r=JSON.parse(File.read(ARGV.fetch(0))); abort unless r["status"]=="blocked" && r.dig("preflight","eligible_issues").length==1 && r.dig("preflight","managed_pull_requests").length==1' "$report"
! grep -q '^issue create' "$log"

: >"$log"
report="$test_root/duplicate.json"
set +e
HOME="$test_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=duplicate LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=duplicate-run "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 3
ruby -rjson -e 'r=JSON.parse(File.read(ARGV.fetch(0))); abort unless r["status"]=="blocked" && r["blocker"].include?("already used") && r.dig("preflight","used_run_id").length==1' "$report"
! grep -q '^issue create' "$log"

: >"$log"
report="$test_root/setup-failure.json"
set +e
HOME="$test_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=setup-failure LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=sf "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 18
ruby -rjson -e '
  r=JSON.parse(File.read(ARGV.fetch(0)))
  abort unless r["status"]=="failed" && r["exit_status"]==18 && r.dig("cleanup","performed")==true
  abort unless r.dig("cleanup","verified")=={"runtime_root_absent"=>true} && r.dig("created","issues")==[]
' "$report"
runtime=$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).dig("created","runtime_roots",0)' "$report")
test ! -e "${runtime%/runtime}"
! grep -q '^issue create' "$log"

: >"$log"
report="$test_root/success.json"
set +e
HOME="$test_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=success LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=live-20261007T200000Z-1234 "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 0
ruby -rjson -e 'r=JSON.parse(File.read(ARGV.fetch(0))); abort unless r["status"]=="passed" && r.dig("cleanup","verified")=={"remote_branch_absent"=>true,"tmux_server_absent"=>true,"runtime_root_absent"=>true}' "$report"
runtime=$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).dig("created","runtime_roots",0)' "$report")
test ! -e "${runtime%/runtime}"

: >"$log"
report="$test_root/branch-query-error.json"
set +e
HOME="$test_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=branch-query-error LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=bqerr "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 6
ruby -rjson -e 'r=JSON.parse(File.read(ARGV.fetch(0))); abort unless r["status"]=="failed" && r.dig("cleanup","diagnostics_preserved")==true' "$report"
runtime=$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).dig("created","runtime_roots",0)' "$report")
rm -rf "${runtime%/runtime}"

: >"$log"
report="$test_root/failure.json"
set +e
HOME="$test_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=failure LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=failure-run "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 17
ruby -rjson -e '
  r=JSON.parse(File.read(ARGV.fetch(0))); commands=r.dig("cleanup","commands")
  abort unless r["status"]=="failed" && r["exit_status"]==17 && r.dig("created","issues")==[{"number"=>99,"url"=>"https://github.com/SysSU/agent-symphony-sample/issues/99"}]
  abort unless r.dig("cleanup","diagnostics_preserved") && commands.any? { |command| command.include?("gh issue close 99") } && commands.any? { |command| command.include?("only after preserving diagnostics") }
' "$report"
grep -q '^issue create' "$log"
grep -q "^go build -ldflags -X=main.livePilotRunID=failure-run -o .*\/worker-package\/bin\/codex .*\/scripts\/live_pilot_codex.go$" "$log"
runtime=$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).dig("created","runtime_roots",0)' "$report")
pilot_root=${runtime%/runtime}
case "$pilot_root" in "$private_root"/run.*) ;; *) exit 95;; esac
tmux_socket="$runtime/tmux/tmux-$(id -u)/default"
test "${#tmux_socket}" -le 100
rm -rf "$pilot_root"

: >"$log"
rm -f "$log.date"
report="$test_root/stuck.json"
set +e
HOME="$test_home" PATH="$fake_bin:/usr/bin:/bin:/usr/sbin:/sbin" LIVE_PILOT_TEST_SCENARIO=stuck LIVE_PILOT_TEST_LOG="$log" AGENT_SYMPHONY_LIVE_PILOT=1 AGENT_SYMPHONY_LIVE_RUN_ID=stuck-run "$project_root/scripts/live-pilot.sh" "$report" >/dev/null 2>&1
status=$?
set -e
test "$status" -eq 6
test "$(cat "$log.date")" -ge 2
ruby -rjson -e '
  r=JSON.parse(File.read(ARGV.fetch(0))); abort unless r["status"]=="failed"
  abort unless r.dig("cleanup","processes_stopped")==false && r.dig("cleanup","tmux_stopped")==true && r.dig("cleanup","diagnostics_preserved")==true
  abort unless r.dig("cleanup","commands").any? { |item| item.include?("verified still running after bounded SIGINT") }
' "$report"
server_pid=$(ruby -rjson -e 'command=JSON.parse(File.read(ARGV.fetch(0))).dig("cleanup","commands").find { |item| item.start_with?("kill -TERM ") }; puts command.split[1]' "$report")
kill -KILL "$server_pid" 2>/dev/null || true
attempts=0
while kill -0 "$server_pid" 2>/dev/null && [ "$attempts" -lt 20 ]; do sleep 0.1; attempts=$((attempts + 1)); done
! kill -0 "$server_pid" 2>/dev/null
runtime=$(ruby -rjson -e 'puts JSON.parse(File.read(ARGV.fetch(0))).dig("created","runtime_roots",0)' "$report")
pilot_root=${runtime%/runtime}
case "$pilot_root" in "$private_root"/run.*) ;; *) exit 96;; esac
tmux_socket="$runtime/tmux/tmux-$(id -u)/default"
test "${#tmux_socket}" -le 100
rm -rf "$pilot_root"

echo "live pilot safety tests passed"
