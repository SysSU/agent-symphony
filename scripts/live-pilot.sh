#!/bin/sh
set -eu

repository=SysSU/agent-symphony-sample
report=${1:-}
run_id=${AGENT_SYMPHONY_LIVE_RUN_ID:-live-$(date -u +%Y%m%dT%H%M%SZ)-$$}

if [ "${AGENT_SYMPHONY_LIVE_PILOT:-}" != 1 ]; then
  echo "set AGENT_SYMPHONY_LIVE_PILOT=1 to authorize the isolated sample-repository pilot" >&2
  exit 2
fi
case "$run_id" in *[!A-Za-z0-9._-]*|'') echo "invalid live pilot run ID" >&2; exit 2;; esac
for command in gh git go ps ruby tmux; do command -v "$command" >/dev/null; done

identity=$(gh repo view "$repository" --json nameWithOwner,isPrivate --jq '[.nameWithOwner,.isPrivate] | @tsv')
if [ "$identity" != "$(printf '%s\ttrue' "$repository")" ]; then
  echo "sample repository identity or privacy does not match" >&2
  exit 2
fi
issue_pages=$(gh api --paginate --slurp "/repos/$repository/issues?state=all&per_page=100")
pr_pages=$(gh api --paginate --slurp "/repos/$repository/pulls?state=open&per_page=100")
open_issues=$(PAGES="$issue_pages" ruby -rjson -e 'puts JSON.parse(ENV.fetch("PAGES")).flatten.reject { |item| item.key?("pull_request") || item["state"]!="open" }.map { |item| {number:item["number"],title:item["title"],url:item["html_url"],labels:item.fetch("labels",[])} }.to_json')
open_prs=$(PAGES="$pr_pages" ruby -rjson -e 'puts JSON.parse(ENV.fetch("PAGES")).flatten.map { |item| {number:item["number"],title:item["title"],url:item["html_url"],headRefName:item.dig("head","ref"),mergeStateStatus:item["mergeable_state"]} }.to_json')
used=$(PAGES="$issue_pages" TITLE="Live pilot $run_id" ruby -rjson -e 'puts JSON.parse(ENV.fetch("PAGES")).flatten.reject { |item| item.key?("pull_request") }.select { |item| item["title"]==ENV.fetch("TITLE") }.map { |item| {number:item["number"],state:item["state"],url:item["html_url"]} }.to_json')
eligible=$(printf '%s' "$open_issues" | ruby -rjson -e 'puts JSON.parse(STDIN.read).select { |issue| issue.fetch("labels").any? { |label| label.fetch("name")=="agent-ready" } }.to_json')
managed=$(printf '%s' "$open_prs" | ruby -rjson -e 'puts JSON.parse(STDIN.read).select { |pr| pr.fetch("headRefName").start_with?("agent-symphony/") }.to_json')
status=ready
blocker=
if [ "$used" != '[]' ]; then
  status=blocked
  blocker='live pilot run ID was already used'
elif [ "$eligible" != '[]' ] || [ "$managed" != '[]' ]; then
  status=blocked
  blocker='pre-existing eligible issues or managed pull requests make repository-wide governance isolation unsafe'
fi

result=$(RUN_ID="$run_id" REPOSITORY="$repository" STATUS="$status" BLOCKER="$blocker" OPEN_ISSUES="$open_issues" OPEN_PRS="$open_prs" ELIGIBLE="$eligible" MANAGED="$managed" USED="$used" ruby -rjson -e 'puts JSON.generate({schema:"agent-symphony-live-pilot-v1",run_id:ENV.fetch("RUN_ID"),repository:ENV.fetch("REPOSITORY"),status:ENV.fetch("STATUS"),blocker:ENV.fetch("BLOCKER"),preflight:{open_issues:JSON.parse(ENV.fetch("OPEN_ISSUES")),open_pull_requests:JSON.parse(ENV.fetch("OPEN_PRS")),eligible_issues:JSON.parse(ENV.fetch("ELIGIBLE")),managed_pull_requests:JSON.parse(ENV.fetch("MANAGED")),used_run_id:JSON.parse(ENV.fetch("USED"))},created:{issues:[],pull_requests:[],branches:[],sessions:[],worktrees:[],review_snapshots:[],runtime_roots:[]},cleanup:{performed:false,commands:[]}})')
printf '%s\n' "$result"
if [ -n "$report" ]; then umask 077; printf '%s\n' "$result" >"$report"; fi
if [ "$status" = blocked ]; then exit 3; fi

project_root=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
pilot_root=$(mktemp -d "/tmp/agent-symphony-${run_id}.XXXXXX")
checkout="$pilot_root/repository"
runtime="$pilot_root/runtime"
binary="$pilot_root/agent-symphony"
fake_bin="$pilot_root/bin"
issue=
issue_url=
server_pid=
pr='[]'
resources=
mutation_started=false
completed=false
processes_stopped=true
tmux_stopped=true

collect_resources() {
  RUNTIME="$runtime" ruby -rjson -e '
    manifests=Dir[File.join(ENV.fetch("RUNTIME"),"attempts","*","*","manifest.json")].sort.map { |path| JSON.parse(File.read(path)).merge("manifest"=>path) }
    puts JSON.generate({
      attempts:manifests.map { |item| {repository:item["repository"],issue:item["issue"],attempt:item["attempt"],manifest:item["manifest"]} },
      branches:manifests.map { |item| item["branch"] }.compact.reject(&:empty?).uniq,
      sessions:manifests.flat_map { |item| [item["session"],item["review_session"]] }.compact.reject(&:empty?).uniq,
      worktrees:manifests.map { |item| item["worktree"] }.compact.reject(&:empty?).uniq,
      review_snapshots:manifests.map { |item| item["review_snapshot"] }.compact.reject(&:empty?).uniq,
      runtime_roots:[ENV.fetch("RUNTIME")]
    })'
}

stop_server() {
  processes_stopped=true
  if [ -z "$server_pid" ] || ! kill -0 "$server_pid" 2>/dev/null; then return 0; fi
  kill -INT "$server_pid" 2>/dev/null || true
  attempts=0
  while kill -0 "$server_pid" 2>/dev/null && [ "$attempts" -lt 20 ]; do
    process_state=$(ps -o stat= -p "$server_pid" 2>/dev/null || true)
    case "$process_state" in *Z*) wait "$server_pid" 2>/dev/null || true; return 0;; esac
    sleep 0.1
    attempts=$((attempts + 1))
  done
  if kill -0 "$server_pid" 2>/dev/null; then processes_stopped=false; return 1; fi
  wait "$server_pid" 2>/dev/null || true
}

stop_tmux() {
  tmux_stopped=true
  TMUX_TMPDIR="$runtime/tmux" tmux kill-server 2>/dev/null || true
  if TMUX_TMPDIR="$runtime/tmux" tmux list-sessions >/dev/null 2>&1; then tmux_stopped=false; return 1; fi
}

report_failure() {
  exit_status=$1
  trap - EXIT HUP INT TERM
  set +e
  if [ "$completed" = true ] || [ "$mutation_started" != true ]; then exit "$exit_status"; fi
  latest=$(collect_resources 2>/dev/null)
  if [ -n "$latest" ]; then resources=$latest; fi
  stop_server || true
  stop_tmux || true
  diagnostics_preserved=false
  if [ -d "$pilot_root" ]; then diagnostics_preserved=true; fi
  latest_pr=$(gh pr list --repo "$repository" --state all --search "$run_id in:title" --limit 100 --json number,url,state,isDraft,headRefName,mergedAt 2>/dev/null)
  if printf '%s' "$latest_pr" | ruby -rjson -e 'JSON.parse(STDIN.read)' >/dev/null 2>&1; then pr=$latest_pr; fi
  result=$(RUN_ID="$run_id" REPOSITORY="$repository" ISSUE="$issue" ISSUE_URL="$issue_url" ROOT="$pilot_root" PID="$server_pid" RUNTIME="$runtime" RESOURCES="$resources" PR="$pr" EXIT_STATUS="$exit_status" PROCESSES_STOPPED="$processes_stopped" TMUX_STOPPED="$tmux_stopped" DIAGNOSTICS_PRESERVED="$diagnostics_preserved" ruby -rjson -rshellwords -e '
    resources=ENV.fetch("RESOURCES","").empty? ? {attempts:[],branches:[],sessions:[],worktrees:[],review_snapshots:[],runtime_roots:[ENV.fetch("RUNTIME")]} : JSON.parse(ENV.fetch("RESOURCES"))
    prs=JSON.parse(ENV.fetch("PR")); commands=[]
    commands << "kill -TERM #{ENV.fetch("PID")} # verified still running after bounded SIGINT" unless ENV.fetch("PID","").empty? || ENV.fetch("PROCESSES_STOPPED")=="true"
    commands << "TMUX_TMPDIR=#{Shellwords.escape(ENV.fetch("RUNTIME")+"/tmux")} tmux kill-server" unless ENV.fetch("TMUX_STOPPED")=="true"
    commands << "gh issue close #{ENV.fetch("ISSUE")} --repo #{ENV.fetch("REPOSITORY")}" unless ENV.fetch("ISSUE","").empty?
    prs.each { |item| commands << "gh pr close #{item.fetch("number")} --repo #{ENV.fetch("REPOSITORY")}" unless item["state"]=="MERGED" }
    (resources.fetch("branches",[])+prs.map { |item| item["headRefName"] }).compact.uniq.each { |branch| commands << "git -C #{Shellwords.escape(ENV.fetch("ROOT")+"/repository")} push origin --delete #{Shellwords.escape(branch)}" }
    commands << "rm -rf -- #{Shellwords.escape(ENV.fetch("ROOT"))} # only after preserving diagnostics"
    issues=ENV.fetch("ISSUE","").empty? ? [] : [{number:ENV.fetch("ISSUE").to_i,url:ENV.fetch("ISSUE_URL")}]
    puts JSON.generate({schema:"agent-symphony-live-pilot-v1",run_id:ENV.fetch("RUN_ID"),repository:ENV.fetch("REPOSITORY"),status:"failed",exit_status:ENV.fetch("EXIT_STATUS").to_i,created:resources.merge(issues:issues,pull_requests:prs),cleanup:{performed:false,processes_stopped:ENV.fetch("PROCESSES_STOPPED")=="true",tmux_stopped:ENV.fetch("TMUX_STOPPED")=="true",diagnostics_preserved:ENV.fetch("DIAGNOSTICS_PRESERVED")=="true",commands:commands}})')
  printf '%s\n' "$result" >&2
  if [ -n "$report" ]; then umask 077; printf '%s\n' "$result" >"$report"; fi
  exit "$exit_status"
}

trap 'report_failure $?' EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

mkdir -m 700 "$fake_bin"
(cd "$project_root" && go build -o "$binary" ./cmd/agent-symphony)
gh repo clone "$repository" "$checkout" -- --quiet
git -C "$checkout" config user.name "Agent Symphony live pilot"
git -C "$checkout" config user.email "live-pilot@example.invalid"
(
  cd "$checkout"
  "$binary" init
)
ruby -rjson -e 'path=ARGV.fetch(0); config=JSON.parse(File.read(path)); config["reconciliation_interval_seconds"]=1; config["commands"]["orchestrator"]=nil; config["commands"]["orchestrator_audit"]=nil; File.write(path,JSON.pretty_generate(config)+"\n")' "$checkout/.agent-symphony.yaml"
cat >"$fake_bin/codex" <<'EOF'
#!/bin/sh
set -eu
if [ -n "${AGENT_SYMPHONY_REVIEW_RESULT:-}" ]; then
  printf '%s' '{"type":"agent-symphony-review-v1","status":"clean","findings":[]}' >"$AGENT_SYMPHONY_REVIEW_RESULT"
  exit 0
fi
printf 'isolated live pilot %s\n' '__AGENT_SYMPHONY_LIVE_RUN_ID__' >LIVE_PILOT.md
git add LIVE_PILOT.md
git commit -qm "test: isolated live pilot"
printf '%s\n' '{"type":"agent-symphony-result-v1","validation":"live pilot commit and review lifecycle","documentation":"temporary live pilot marker"}' >"$AGENT_SYMPHONY_IMPLEMENTATION_RESULT"
EOF
ruby -e 'path,run_id=ARGV; marker="__AGENT_SYMPHONY_LIVE_RUN_ID__"; body=File.read(path); abort "missing or duplicate live run marker" unless body.scan(marker).length==1; File.write(path,body.sub(marker,run_id))' "$fake_bin/codex" "$run_id"
chmod 0700 "$fake_bin/codex"

body=$(printf '## Context\n\nAuthenticated isolated pilot `%s`.\n\n## Acceptance criteria\n\n- Complete one implementation, review, pull request, checks, merge, and closure lifecycle.\n\n## Checklist\n\n- [ ] Run the isolated lifecycle.\n\n## Validation\n\nValidate GitHub state, dashboard projection, and exact cleanup.\n\n## Dependencies\n\nNone\n' "$run_id")
mutation_started=true
issue_url=$(gh issue create --repo "$repository" --title "Live pilot $run_id" --body "$body" --label agent-ready --label priority:P1 --label autonomous-merge)
issue=${issue_url##*/}
port=$(ruby -rsocket -e 'socket=TCPServer.new("127.0.0.1",0); puts socket.addr[1]; socket.close')
state="$pilot_root/pr-state.json"
printf '[]\n' >"$state"
started=$(date +%s)
(
  cd "$checkout"
  exec env PATH="$fake_bin:$PATH" CODEX_HOME="$pilot_root/codex-home" TMUX_TMPDIR="$runtime/tmux" "$binary" serve --config "$checkout/.agent-symphony.yaml" --state "$state" --runtime-state "$runtime" --dashboard-address "127.0.0.1:$port" --interval 200ms
) >"$pilot_root/serve.log" 2>&1 &
server_pid=$!

closed=false
deadline=$((started + 300))
while [ "$(date +%s)" -lt "$deadline" ]; do
  if [ "$(gh issue view "$issue" --repo "$repository" --json state --jq .state)" = CLOSED ]; then closed=true; break; fi
  if ! kill -0 "$server_pid" 2>/dev/null; then break; fi
  if RUNTIME="$runtime" ruby -rjson -e 'exit Dir[File.join(ENV.fetch("RUNTIME"),"attempts","*","*","manifest.json")].any? { |path| JSON.parse(File.read(path))["state"]=="failed" } ? 0 : 1'; then break; fi
  sleep 2
done
resources=$(collect_resources)
stop_server || exit 6
stop_tmux || exit 6

pr=$(gh pr list --repo "$repository" --state all --search "$run_id in:title" --limit 100 --json number,url,state,isDraft,headRefName,mergedAt)
if [ "$closed" != true ] || [ "$(printf '%s' "$pr" | ruby -rjson -e 'rows=JSON.parse(STDIN.read); puts rows.length==1 && rows[0]["state"]=="MERGED" ? "true" : "false"')" != true ]; then exit 5; fi

branch=$(printf '%s' "$pr" | ruby -rjson -e 'puts JSON.parse(STDIN.read).fetch(0).fetch("headRefName")')
if git -C "$checkout" ls-remote --exit-code --heads origin "refs/heads/$branch" >/dev/null 2>&1; then
  git -C "$checkout" push --quiet origin --delete "$branch"
fi
if git -C "$checkout" ls-remote --exit-code --heads origin "refs/heads/$branch" >/dev/null 2>&1; then
  echo "live pilot branch remains after exact cleanup: $branch" >&2
  exit 6
fi
if TMUX_TMPDIR="$runtime/tmux" tmux list-sessions >/dev/null 2>&1; then
  echo "live pilot tmux server remains after exact cleanup" >&2
  exit 6
fi
elapsed=$(($(date +%s) - started))
rm -rf "$pilot_root"
test ! -e "$pilot_root"
result=$(RUN_ID="$run_id" REPOSITORY="$repository" ISSUE="$issue" ISSUE_URL="$issue_url" PR="$pr" BRANCH="$branch" ELAPSED="$elapsed" RESOURCES="$resources" ruby -rjson -e '
  resources=JSON.parse(ENV.fetch("RESOURCES"))
  puts JSON.generate({schema:"agent-symphony-live-pilot-v1",run_id:ENV.fetch("RUN_ID"),repository:ENV.fetch("REPOSITORY"),status:"passed",created:resources.merge(issues:[{number:ENV.fetch("ISSUE").to_i,url:ENV.fetch("ISSUE_URL")}],pull_requests:JSON.parse(ENV.fetch("PR")),branches:[ENV.fetch("BRANCH")]),timings:{total_seconds:ENV.fetch("ELAPSED").to_i},cleanup:{performed:true,verified:{remote_branch_absent:true,tmux_server_absent:true,runtime_root_absent:true},commands:[]}})')
printf '%s\n' "$result"
if [ -n "$report" ]; then umask 077; printf '%s\n' "$result" >"$report"; fi
completed=true
