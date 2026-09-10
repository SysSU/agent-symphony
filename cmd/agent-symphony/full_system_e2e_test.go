package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type fullSystemGitHub struct {
	mu              sync.Mutex
	base            string
	origin          string
	comments        []map[string]any
	labels          map[string]bool
	requests        []string
	failNext        bool
	pr              map[string]any
	merged          bool
	closed          bool
	denyMutations   bool
	deniedMutations []string
}

type synchronizedBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (b *synchronizedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.Write(p)
}

func (b *synchronizedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func (f *fullSystemGitHub) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests = append(f.requests, r.Method+" "+r.URL.RequestURI())
	w.Header().Set("Content-Type", "application/json")
	if f.denyMutations && githubMutationRequest(r) {
		f.deniedMutations = append(f.deniedMutations, r.Method+" "+r.URL.RequestURI())
		http.Error(w, `{"message":"GitHub mutation forbidden during removal"}`, http.StatusInternalServerError)
		return
	}
	if f.failNext {
		f.failNext = false
		http.Error(w, `{"message":"transient fixture failure"}`, http.StatusServiceUnavailable)
		return
	}
	now := "2026-09-09T12:00:00Z"
	body := "## Context\nProtect the full operator journey.\n\n## Acceptance criteria\n- The change is visible.\n\n## Checklist\n- [ ] Implement and review.\n\n## Validation\nRun the deterministic full-system test.\n\n## Dependencies\n#72"
	labels := make([]map[string]string, 0, len(f.labels))
	for label, present := range f.labels {
		if present {
			labels = append(labels, map[string]string{"name": label})
		}
	}
	issueState := "open"
	if f.closed {
		issueState = "closed"
	}
	issue := map[string]any{"number": 73, "node_id": "I_73", "title": "Deterministic full-system journey", "body": body, "state": issueState, "created_at": now, "updated_at": now, "user": map[string]any{"id": 42}, "labels": labels}
	path := r.URL.Path
	switch {
	case r.Method == http.MethodGet && path == "/user":
		writeFixtureJSON(w, map[string]any{"id": 42, "login": "coordinator"})
	case r.Method == http.MethodGet && path == "/user/42":
		writeFixtureJSON(w, map[string]any{"id": 42, "login": "coordinator"})
	case r.Method == http.MethodGet && path == "/repos/o/r/collaborators/coordinator/permission":
		writeFixtureJSON(w, map[string]string{"permission": "admin"})
	case r.Method == http.MethodGet && path == "/repos/o/r":
		writeFixtureJSON(w, map[string]any{"full_name": "o/r", "default_branch": "main", "permissions": map[string]bool{"pull": true, "push": true}})
	case r.Method == http.MethodGet && path == "/repos/o/r/branches/main":
		writeFixtureJSON(w, map[string]any{"name": "main", "commit": map[string]string{"sha": f.base}, "protected": false})
	case r.Method == http.MethodGet && path == "/repos/o/r/issues":
		if f.closed {
			writeFixtureJSON(w, []any{})
		} else {
			writeFixtureJSON(w, []any{issue})
		}
	case r.Method == http.MethodGet && path == "/repos/o/r/issues/73":
		writeFixtureJSON(w, issue)
	case r.Method == http.MethodGet && path == "/repos/o/r/issues/72":
		writeFixtureJSON(w, map[string]any{"number": 72, "node_id": "I_72", "title": "Completed prerequisite", "body": "", "state": "closed", "created_at": now, "updated_at": now, "user": map[string]any{"id": 42}, "labels": []any{}})
	case r.Method == http.MethodGet && path == "/repos/o/r/issues/73/comments":
		writeFixtureJSON(w, f.comments)
	case r.Method == http.MethodGet && path == "/repos/o/r/issues/73/timeline":
		writeFixtureJSON(w, []any{
			map[string]any{"id": 731, "event": "labeled", "label": map[string]string{"name": "agent-ready"}, "created_at": now, "actor": map[string]any{"id": 42}},
			map[string]any{"id": 732, "event": "labeled", "label": map[string]string{"name": "priority:P1"}, "created_at": now, "actor": map[string]any{"id": 42}},
			map[string]any{"id": 733, "event": "labeled", "label": map[string]string{"name": "autonomous-merge"}, "created_at": now, "actor": map[string]any{"id": 42}},
		})
	case r.Method == http.MethodGet && path == "/repos/o/r/pulls":
		pulls := []any{}
		if f.pr != nil {
			pulls = append(pulls, f.pr)
		}
		writeFixtureJSON(w, pulls)
	case r.Method == http.MethodGet && strings.HasPrefix(path, "/repos/o/r/issues/73/events"):
		writeFixtureJSON(w, []any{})
	case r.Method == http.MethodPost && path == "/repos/o/r/pulls":
		var input map[string]string
		_ = json.NewDecoder(r.Body).Decode(&input)
		headOutput, _ := exec.Command("git", "--git-dir", f.origin, "rev-parse", "refs/heads/"+input["head"]).Output()
		head := strings.TrimSpace(string(headOutput))
		f.pr = map[string]any{"number": 91, "body": input["body"], "state": "open", "merged": false, "mergeable": true, "mergeable_state": "clean", "user": map[string]any{"id": 42}, "head": map[string]any{"sha": head, "ref": input["head"]}, "base": map[string]any{"sha": f.base, "ref": "main"}, "labels": []any{}}
		writeFixtureStatusJSON(w, http.StatusCreated, f.pr)
	case r.Method == http.MethodGet && path == "/repos/o/r/pulls/91":
		writeFixtureJSON(w, f.pr)
	case r.Method == http.MethodPatch && path == "/repos/o/r/pulls/91":
		var input map[string]string
		_ = json.NewDecoder(r.Body).Decode(&input)
		f.pr["body"] = input["body"]
		writeFixtureJSON(w, f.pr)
	case r.Method == http.MethodGet && (path == "/repos/o/r/issues/91/comments" || path == "/repos/o/r/pulls/91/comments" || path == "/repos/o/r/pulls/91/reviews"):
		writeFixtureJSON(w, []any{})
	case r.Method == http.MethodGet && strings.Contains(path, "/commits/") && strings.HasSuffix(path, "/check-runs"):
		writeFixtureJSON(w, map[string]any{"check_runs": []any{map[string]any{"name": "full-system-ci", "status": "completed", "conclusion": "success"}}})
	case r.Method == http.MethodGet && strings.Contains(path, "/commits/") && strings.HasSuffix(path, "/status"):
		writeFixtureJSON(w, map[string]any{"statuses": []any{}})
	case r.Method == http.MethodGet && strings.Contains(path, "/commits/") && strings.HasSuffix(path, "/statuses"):
		writeFixtureJSON(w, []any{})
	case r.Method == http.MethodPost && strings.Contains(path, "/statuses/"):
		writeFixtureStatusJSON(w, http.StatusCreated, map[string]any{"state": "success", "context": "agent-symphony/policy"})
	case r.Method == http.MethodGet && path == "/repos/o/r/branches/main/protection":
		http.Error(w, `{"message":"not protected"}`, http.StatusNotFound)
	case r.Method == http.MethodGet && path == "/repos/o/r/rules/branches/main":
		writeFixtureJSON(w, []any{})
	case r.Method == http.MethodPut && path == "/repos/o/r/pulls/91/merge":
		f.merged, f.closed = true, true
		f.pr["state"], f.pr["merged"], f.pr["merged_at"] = "closed", true, time.Now().UTC().Format(time.RFC3339Nano)
		head := f.pr["head"].(map[string]any)["sha"].(string)
		_ = exec.Command("git", "--git-dir", f.origin, "update-ref", "refs/heads/main", head).Run()
		writeFixtureJSON(w, map[string]any{"merged": true, "sha": head, "message": "merged"})
	case r.Method == http.MethodPost && path == "/repos/o/r/issues/73/comments":
		var input map[string]string
		_ = json.NewDecoder(r.Body).Decode(&input)
		created := time.Now().UTC().Format(time.RFC3339Nano)
		comment := map[string]any{"id": len(f.comments) + 1, "body": input["body"], "created_at": created, "updated_at": created, "user": map[string]any{"id": 42}}
		f.comments = append(f.comments, comment)
		writeFixtureStatusJSON(w, http.StatusCreated, comment)
	case r.Method == http.MethodPost && path == "/repos/o/r/issues/73/labels":
		var input struct {
			Labels []string `json:"labels"`
		}
		_ = json.NewDecoder(r.Body).Decode(&input)
		for _, label := range input.Labels {
			f.labels[label] = true
		}
		writeFixtureJSON(w, labels)
	case r.Method == http.MethodDelete && strings.HasPrefix(path, "/repos/o/r/issues/73/labels/"):
		delete(f.labels, strings.TrimPrefix(path, "/repos/o/r/issues/73/labels/"))
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodPost && path == "/graphql":
		writeFixtureJSON(w, map[string]any{"data": map[string]any{"repository": map[string]any{"issue": nil}}})
	default:
		http.Error(w, fmt.Sprintf(`{"message":"unhandled fixture endpoint %s %s"}`, r.Method, r.URL.RequestURI()), http.StatusNotFound)
	}
}

func githubMutationRequest(r *http.Request) bool {
	return r.Method != http.MethodGet && r.Method != http.MethodHead && !(r.Method == http.MethodPost && r.URL.Path == "/graphql")
}

func writeFixtureJSON(w http.ResponseWriter, value any) {
	writeFixtureStatusJSON(w, http.StatusOK, value)
}

func writeFixtureStatusJSON(w http.ResponseWriter, status int, value any) {
	body, _ := json.Marshal(value)
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func TestFullSystemE2E(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_E2E") != "1" {
		t.Skip("set AGENT_SYMPHONY_FULL_SYSTEM_E2E=1 to run the compiled full-system gate")
	}
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is unavailable")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is unavailable")
	}
	raceMode := os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_RACE") == "1"
	deadline := func(normal time.Duration) time.Duration {
		if raceMode {
			return normal * 4
		}
		return normal
	}
	latencyBudget := 10 * time.Second
	serveInterval, controlTimeout := "200ms", "30s"
	if raceMode {
		latencyBudget = 40 * time.Second
		serveInterval, controlTimeout = "5s", "2m"
	}

	source, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/tmp", "agent-symphony-full-system-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	repository := filepath.Join(root, "repository")
	origin := filepath.Join(root, "origin.git")
	runExternal(t, "", "git", "init", "-q", "--bare", origin)
	runExternal(t, "", "git", "init", "-q", "-b", "main", repository)
	runExternal(t, repository, "git", "config", "user.name", "Full system fixture")
	runExternal(t, repository, "git", "config", "user.email", "fixture@example.invalid")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runExternal(t, repository, "git", "add", "README.md")
	runExternal(t, repository, "git", "commit", "-qm", "base")
	runExternal(t, repository, "git", "remote", "add", "origin", origin)
	runExternal(t, repository, "git", "push", "-q", "-u", "origin", "main")
	base := strings.TrimSpace(runExternal(t, repository, "git", "rev-parse", "HEAD"))

	fixture := &fullSystemGitHub{base: base, origin: origin, labels: map[string]bool{"agent-ready": true, "priority:P1": true, "autonomous-merge": true}}
	github := httptest.NewServer(fixture)
	defer github.Close()
	binDir := filepath.Join(root, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(binDir, "agent-symphony")
	buildArgs := []string{"build", "-o", binary, "."}
	if raceMode {
		buildArgs = []string{"build", "-race", "-o", binary, "."}
	}
	runExternal(t, source, "go", buildArgs...)
	writeExecutable(t, filepath.Join(binDir, "gh"), `#!/bin/sh
method=GET
endpoint=
input=0
while [ "$#" -gt 0 ]; do
  case "$1" in
    --method) shift; method=$1 ;;
    --input) shift; input=1 ;;
    /*|graphql) endpoint=$1 ;;
  esac
  shift
done
test "$endpoint" = graphql && endpoint=/graphql
if [ "$input" -eq 1 ]; then exec curl -sS -i -X "$method" --data-binary @- "$FAKE_GITHUB_URL$endpoint"; fi
exec curl -sS -i -X "$method" "$FAKE_GITHUB_URL$endpoint"
`)
	writeExecutable(t, filepath.Join(binDir, "codex"), `#!/bin/sh
HOME=$FULL_SYSTEM_FIXTURE/empty-git-home
export HOME
mkdir -p "$HOME"
trusted=0
bypass=0
want_trust=$(printf 'projects={"%s"={trust_level="trusted"}}' "$PWD")
for argument do
  test "$argument" != "$want_trust" || trusted=1
  test "$argument" != --dangerously-bypass-approvals-and-sandbox || bypass=1
done
test "$trusted" -eq 1 || { printf 'Do you trust the contents of this directory?\n' >&2; exit 18; }
test "$bypass" -eq 1 || { printf 'Approval required\n' >&2; exit 19; }
if [ -n "${AGENT_SYMPHONY_REVIEW_RESULT:-}" ]; then
	  sleep 1
	  if ! grep -qx reworked change.txt; then
	    if [ ! -e "$FULL_SYSTEM_FIXTURE/reviewed-once" ]; then
	    : >"$FULL_SYSTEM_FIXTURE/review-started"
	    while [ ! -e "$FULL_SYSTEM_FIXTURE/allow-review" ]; do sleep 0.05; done
	    fi
	    : >"$FULL_SYSTEM_FIXTURE/reviewed-once"
	    result='{"type":"agent-symphony-review-v1","status":"findings","findings":["append the reviewed marker"]}'
	  else
	    : >"$FULL_SYSTEM_FIXTURE/reviewed-twice"
	    result='{"type":"agent-symphony-review-v1","status":"clean","findings":[]}'
	  fi
	  printf '%s' "$result" >"$AGENT_SYMPHONY_REVIEW_RESULT.tmp"
	  mv "$AGENT_SYMPHONY_REVIEW_RESULT.tmp" "$AGENT_SYMPHONY_REVIEW_RESULT"
	  exit 0
fi
	if [ "$1" = exec ]; then
	  IFS= read -r prompt || exit 19
	  case "$prompt" in
	    *"Apply this authorized Agent Symphony handoff"*)
	      printf 'reworked\n' >>change.txt
	      git add change.txt && git -c user.name='Full system fixture' -c user.email=fixture@example.invalid commit -qm 'address independent review' || exit 27
	      printf '%s\n' '{"type":"agent-symphony-result-v1","validation":"rework fixture passed","documentation":"none"}'
	      exit 0
	      ;;
	  esac
	fi
if [ ! -e "$FULL_SYSTEM_FIXTURE/launched-once" ]; then
  : >"$FULL_SYSTEM_FIXTURE/launched-once"
  printf 'scripted launch failure\n' >&2
  exit 42
fi
printf 'implementation-ready\n'
IFS= read -r message || exit 20
printf '%s' '{"body":"/agent-symphony status needs-attention: waiting for operator decision"}' | gh api --method POST /repos/o/r/issues/73/comments --input - >/dev/null || exit 21
printf '%s' '{"labels":["needs-attention"]}' | gh api --method POST /repos/o/r/issues/73/labels --input - >/dev/null || exit 22
printf 'attention-status-set\n'
IFS= read -r message || exit 23
printf '%s' '{"body":"/agent-symphony status clear: operator supplied the decision"}' | gh api --method POST /repos/o/r/issues/73/comments --input - >/dev/null || exit 24
gh api --method DELETE /repos/o/r/issues/73/labels/needs-attention >/dev/null || exit 25
printf 'operator-message-received:%s\n' "$message"
printf 'reviewed\n' >>change.txt
git add change.txt && git -c user.name='Full system fixture' -c user.email=fixture@example.invalid commit -qm 'implement fixture journey' || exit 26
printf '%s\n' '{"type":"agent-symphony-result-v1","validation":"full-system fixture passed","documentation":"none"}' >"$AGENT_SYMPHONY_IMPLEMENTATION_RESULT"
`)

	cfg := config.Default("o/r")
	cfg.Commands.Orchestrator, cfg.Commands.OrchestratorAudit = nil, nil
	cfg.Commands.Environment = append(cfg.Commands.Environment, "FAKE_GITHUB_URL", "FULL_SYSTEM_FIXTURE")
	cfg.ReconciliationIntervalSeconds = 1
	configPath := filepath.Join(repository, config.DefaultPath)
	if err := config.Write(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	statePath, stateRoot := filepath.Join(repository, "pr-state.json"), filepath.Join(root, "runtime")
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	address := freeAddress(t)
	server := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--interval", serveInterval)
	server.Dir = repository
	server.Env = append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GITHUB_URL="+github.URL, "FULL_SYSTEM_FIXTURE="+root, "CODEX_HOME="+filepath.Join(root, "codex-home"), "TMUX_TMPDIR="+filepath.Join(root, "tmux"))
	output := &synchronizedBuffer{}
	server.Stdout, server.Stderr = output, output
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	currentSession := "as-o-r-964584196b5c-73-1"
	t.Cleanup(func() {
		_ = stopFullSystemTmux(server.Env, currentSession)
		_ = stopFullSystemProcesses(root)
		if !stopped {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
	})
	waitHTTP(t, "http://"+address+"/status.json", deadline(15*time.Second), output)
	response, err := http.Get("http://" + address + "/status.json")
	if err != nil {
		t.Fatal(err)
	}
	statusBody, _ := io.ReadAll(response.Body)
	_ = response.Body.Close()
	fixture.mu.Lock()
	t.Logf("initial status=%s requests=%q", statusBody, fixture.requests)
	fixture.mu.Unlock()
	failureStarted := time.Now()
	if !waitFor(deadline(15*time.Second), func() bool {
		response, err := http.Get("http://" + address + "/status.json")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var snapshot dashboardStatusSnapshot
		return json.NewDecoder(response.Body).Decode(&snapshot) == nil && len(snapshot.Statuses) == 1 && snapshot.Statuses[0].State == "failed" && snapshot.Statuses[0].Retryable && snapshot.Statuses[0].Attempt == 1
	}) {
		t.Fatalf("scripted launch failure did not become recoverable: %s", output.String())
	}
	if elapsed := time.Since(failureStarted); elapsed >= latencyBudget {
		t.Fatalf("launch failure projection latency=%s", elapsed)
	}
	recoverCLI := exec.Command(binary, "control", "--repository", "o/r", "--runtime-state", stateRoot, "--action", "recover", "--issue", "73", "--attempt", "1", "--request-id", "full-system-launch-recovery", "--timeout", controlTimeout, "--json")
	recoverOutput, err := recoverCLI.CombinedOutput()
	if err != nil || !strings.Contains(string(recoverOutput), `"ok":true`) {
		t.Fatalf("launch recovery CLI: %v output=%s serve=%s", err, recoverOutput, output.String())
	}
	if !waitFor(deadline(15*time.Second), func() bool {
		response, err := http.Get("http://" + address + "/status.json")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var snapshot dashboardStatusSnapshot
		if json.NewDecoder(response.Body).Decode(&snapshot) != nil {
			return false
		}
		for _, status := range snapshot.Statuses {
			if status.State == "active" && status.Attempt == 2 {
				currentSession = status.Session
			}
		}
		return currentSession == "as-o-r-964584196b5c-73-2"
	}) {
		response, _ := http.Get("http://" + address + "/status.json")
		latest, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		t.Fatalf("implementation session did not start: status=%s serve=%s", latest, output.String())
	}
	if err := server.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("implementation-checkpoint shutdown: %v output=%s", err, output.String())
	}
	stopped = true
	implementationManifests, _ := filepath.Glob(filepath.Join(stateRoot, "attempts", "*", "73-*", "manifest.json"))
	if len(implementationManifests) == 0 {
		t.Fatal("implementation restart checkpoint has no durable attempt")
	}
	if manifests, _ := filepath.Glob(filepath.Join(stateRoot, "attempts", "*", "73-*", "manifest.json")); len(manifests) != len(implementationManifests) {
		t.Fatalf("implementation restart checkpoint manifests=%q", manifests)
	}
	address = freeAddress(t)
	server = exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--interval", serveInterval)
	server.Dir = repository
	server.Env = append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GITHUB_URL="+github.URL, "FULL_SYSTEM_FIXTURE="+root, "CODEX_HOME="+filepath.Join(root, "codex-home"), "TMUX_TMPDIR="+filepath.Join(root, "tmux"))
	output = &synchronizedBuffer{}
	server.Stdout, server.Stderr = output, output
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	stopped = false
	waitHTTP(t, "http://"+address+"/status.json", deadline(15*time.Second), output)

	playwright := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "playwright"))
	playwright.Dir = source
	playwright.Env = append(os.Environ(), "AGENT_SYMPHONY_FULL_SYSTEM_URL=http://"+address, "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(raceMode))
	playwrightOutput, err := playwright.CombinedOutput()
	if err != nil {
		response, _ := http.Get("http://" + address + "/status.json")
		latest, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		fixture.mu.Lock()
		requests := append([]string(nil), fixture.requests...)
		fixture.mu.Unlock()
		var diagnostics strings.Builder
		_ = filepath.WalkDir(stateRoot, func(path string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil || entry.IsDir() || !map[string]bool{"manifest.json": true, "agent.log": true}[entry.Name()] {
				return nil
			}
			body, _ := os.ReadFile(path)
			fmt.Fprintf(&diagnostics, "%s: %s\n", strings.TrimPrefix(path, stateRoot), body)
			return nil
		})
		t.Fatalf("real-server Playwright: %v\n%s\nstatus=%s\nrequests=%q\ndiagnostics=%s\nserve:\n%s", err, playwrightOutput, latest, requests, diagnostics.String(), output.String())
	}
	if !waitFor(deadline(15*time.Second), func() bool {
		_, err := os.Lstat(filepath.Join(root, "review-started"))
		return err == nil
	}) {
		t.Fatalf("review session did not reach the durable restart checkpoint: %s", output.String())
	}
	if err := server.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("review-checkpoint shutdown: %v output=%s", err, output.String())
	}
	stopped = true
	fixture.mu.Lock()
	createdAtReviewRestart := countRequest(fixture.requests, "POST /repos/o/r/pulls")
	fixture.mu.Unlock()
	if manifests, _ := filepath.Glob(filepath.Join(stateRoot, "attempts", "*", "73-*", "manifest.json")); len(manifests) != len(implementationManifests) || createdAtReviewRestart != 0 {
		t.Fatalf("review restart checkpoint manifests=%q PR creates=%d", manifests, createdAtReviewRestart)
	}
	address = freeAddress(t)
	server = exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--interval", serveInterval)
	server.Dir = repository
	server.Env = append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GITHUB_URL="+github.URL, "FULL_SYSTEM_FIXTURE="+root, "CODEX_HOME="+filepath.Join(root, "codex-home"), "TMUX_TMPDIR="+filepath.Join(root, "tmux"))
	output = &synchronizedBuffer{}
	server.Stdout, server.Stderr = output, output
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	stopped = false
	waitHTTP(t, "http://"+address+"/status.json", deadline(15*time.Second), output)
	if err := os.WriteFile(filepath.Join(root, "allow-review"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if !waitFor(deadline(30*time.Second), func() bool {
		_, err := os.Lstat(filepath.Join(root, "reviewed-once"))
		return err == nil
	}) {
		t.Fatalf("timed out waiting for first review finding: %s", output.String())
	}
	if !waitFor(deadline(30*time.Second), func() bool {
		_, err := os.Lstat(filepath.Join(root, "reviewed-twice"))
		return err == nil
	}) {
		t.Fatalf("timed out waiting for clean second review: %s", output.String())
	}
	if !waitFor(deadline(45*time.Second), func() bool {
		fixture.mu.Lock()
		defer fixture.mu.Unlock()
		return fixture.merged && fixture.closed
	}) {
		fixture.mu.Lock()
		requests := append([]string(nil), fixture.requests...)
		fixture.mu.Unlock()
		manifests, _ := filepath.Glob(filepath.Join(stateRoot, "attempts", "*", "73-*", "manifest.json"))
		var manifest []byte
		if len(manifests) == 1 {
			manifest, _ = os.ReadFile(manifests[0])
		}
		t.Fatalf("timed out waiting for pull request merge and issue closure: requests=%q manifest=%s serve=%s", requests, manifest, output.String())
	}
	mergedChange := runExternal(t, "", "git", "--git-dir", origin, "show", "refs/heads/main:change.txt")
	if !strings.Contains(mergedChange, "reviewed") || !strings.Contains(mergedChange, "reworked") {
		t.Fatalf("merged change does not contain implementation and review rework: %q", mergedChange)
	}
	cyclesAfterMerge := strings.Count(output.String(), `"ok":true`)
	if !waitFor(deadline(15*time.Second), func() bool {
		return strings.Count(output.String(), `"ok":true`) > cyclesAfterMerge
	}) {
		t.Fatalf("merged state did not reconcile: %s", output.String())
	}
	var completed dashboardStatusSnapshot
	response, err = http.Get("http://" + address + "/status.json")
	if err != nil {
		t.Fatal(err)
	}
	if decodeErr := json.NewDecoder(response.Body).Decode(&completed); decodeErr != nil {
		_ = response.Body.Close()
		t.Fatal(decodeErr)
	}
	_ = response.Body.Close()
	completedAttempt := slices.IndexFunc(completed.Statuses, func(status orchestrator.RecoveryStatus) bool {
		return status.Attempt == 2 && status.Issue == 73 && status.State == "completed" && status.IssueClosed && status.PR == 91
	})
	if completedAttempt < 0 {
		t.Fatalf("post-merge dashboard projection=%#v", completed.Statuses)
	}
	if err := server.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("serve shutdown: %v\n%s", err, output.String())
	}
	stopped = true
	if strings.Count(output.String(), `"ok":true`) < 2 {
		t.Fatalf("expected repeated real reconciliation, output:\n%s", output.String())
	}
	fixture.mu.Lock()
	createdBeforeRestart := countRequest(fixture.requests, "POST /repos/o/r/pulls")
	mergedBeforeRestart := countRequest(fixture.requests, "PUT /repos/o/r/pulls/91/merge")
	fixture.mu.Unlock()

	restartAddress := freeAddress(t)
	restarted := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", restartAddress, "--interval", "30s")
	restarted.Dir, restarted.Env = repository, server.Env
	var restartOutput synchronizedBuffer
	restarted.Stdout, restarted.Stderr = &restartOutput, &restartOutput
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	restartedStopped := false
	t.Cleanup(func() {
		if !restartedStopped {
			_ = restarted.Process.Kill()
			_ = restarted.Wait()
		}
	})
	waitHTTP(t, "http://"+restartAddress+"/status.json", deadline(15*time.Second), &restartOutput)
	fixture.mu.Lock()
	fixture.failNext = true
	fixture.mu.Unlock()
	control := exec.Command(binary, "control", "--repository", "o/r", "--runtime-state", stateRoot, "--action", "reconcile", "--request-id", "full-system-post-merge", "--timeout", controlTimeout, "--json")
	controlOutput, err := control.CombinedOutput()
	if err != nil || !strings.Contains(string(controlOutput), `"ok":true`) {
		t.Fatalf("post-restart CLI recovery: %v output=%s serve=%s", err, controlOutput, restartOutput.String())
	}
	if !strings.Contains(restartOutput.String(), `"retries":1`) {
		t.Fatalf("transient GitHub failure was not observably retried: %s", restartOutput.String())
	}
	var afterRestart dashboardStatusSnapshot
	response, err = http.Get("http://" + restartAddress + "/status.json")
	if err != nil {
		t.Fatal(err)
	}
	decodeErr := json.NewDecoder(response.Body).Decode(&afterRestart)
	_ = response.Body.Close()
	if decodeErr != nil || slices.IndexFunc(afterRestart.Statuses, func(status orchestrator.RecoveryStatus) bool {
		return status.Attempt == 2 && status.State == "completed" && status.IssueClosed
	}) < 0 {
		t.Fatalf("post-restart projection=%#v decode=%v", afterRestart.Statuses, decodeErr)
	}
	fixture.mu.Lock()
	requests := append([]string(nil), fixture.requests...)
	createdAfterRestart := countRequest(requests, "POST /repos/o/r/pulls")
	mergedAfterRestart := countRequest(requests, "PUT /repos/o/r/pulls/91/merge")
	fixture.mu.Unlock()
	if createdBeforeRestart != 1 || mergedBeforeRestart != 1 || createdAfterRestart != 1 || mergedAfterRestart != 1 {
		t.Fatalf("restart duplicated publication: PR creates=%d→%d merges=%d→%d", createdBeforeRestart, createdAfterRestart, mergedBeforeRestart, mergedAfterRestart)
	}

	runtimeState := &agentruntime.Runtime{Root: productionAttemptRoot(stateRoot), StateRoot: stateRoot}
	manifests, err := runtimeState.Discover()
	if err != nil {
		t.Fatal(err)
	}
	findAttempt := func(number int) agentruntime.Manifest {
		index := slices.IndexFunc(manifests, func(manifest agentruntime.Manifest) bool {
			return manifest.Repository == "o/r" && manifest.Issue == 73 && manifest.Attempt == number
		})
		if index < 0 {
			t.Fatalf("attempt %d manifest missing before permanent-removal fixture: %#v", number, manifests)
		}
		return manifests[index]
	}
	removedManifest, retainedManifest := findAttempt(1), findAttempt(2)
	if info, statErr := os.Stat(removedManifest.Worktree); statErr != nil || !info.IsDir() {
		t.Fatalf("historical worktree is unavailable before removal: %v", statErr)
	}
	removedResult := agentruntime.ResultPath(removedManifest.Worktree)
	if err := os.WriteFile(removedResult, []byte("historical result"), 0o600); err != nil {
		t.Fatal(err)
	}
	removedHandoff := filepath.Join(filepath.Dir(removedManifest.LogPath), "handoff.json")
	retainedHandoff := filepath.Join(filepath.Dir(retainedManifest.LogPath), "unrelated-handoff.json")
	for path, body := range map[string]string{removedHandoff: "selected", retainedHandoff: "unrelated"} {
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	removedAttempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: removedManifest.BaseSHA}
	retainedAttempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 2, BaseSHA: retainedManifest.BaseSHA}
	snapshotRoot := productionSnapshotRoot(stateRoot)
	if err := os.MkdirAll(snapshotRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	removedSnapshot, removedReviewSession := reviewIdentity(removedAttempt, snapshotRoot)
	retainedSnapshot, _ := reviewIdentity(retainedAttempt, snapshotRoot)
	removedReviewResult := removedSnapshot + ".result-0123456789abcdef"
	for _, path := range []string{removedSnapshot, removedReviewResult, retainedSnapshot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	tmuxEnvironment := append(os.Environ(), "TMUX_TMPDIR="+projectTmuxRoot(stateRoot))
	unrelatedSession := "as-unrelated-permanent-removal"
	for _, session := range []string{removedManifest.Session, removedReviewSession, unrelatedSession} {
		ensureFullSystemTmuxSession(t, tmuxEnvironment, session, repository)
	}
	fixture.mu.Lock()
	fixture.denyMutations = true
	fixture.mu.Unlock()
	journalPath := filepath.Join(stateRoot, "removal-state.json")
	if _, err := os.Lstat(journalPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("unexpected removal journal before partial-failure fixture: %v", err)
	}
	permissionChanged := make(chan error, 1)
	go func() {
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			state, err := (&dashboardServer{stateRoot: stateRoot, repository: "o/r"}).readRemovalState()
			if err == nil && len(state.Intents) == 1 && state.Intents[0].CleanupStarted {
				permissionChanged <- os.Chmod(stateRoot, 0o500)
				return
			}
			time.Sleep(time.Millisecond)
		}
		permissionChanged <- errors.New("removal cleanup start was not observed")
	}()
	lateRemovalCLI := exec.Command(binary, "control", "--repository", "o/r", "--runtime-state", stateRoot, "--action", "remove", "--issue", "73", "--attempt", "1", "--confirm", "--request-id", "full-system-late-removal", "--timeout", controlTimeout, "--json")
	lateRemovalOutput, lateRemovalErr := lateRemovalCLI.CombinedOutput()
	permissionErr := <-permissionChanged
	restoreErr := os.Chmod(stateRoot, 0o700)
	if permissionErr != nil || restoreErr != nil {
		t.Fatalf("inject late dashboard-state failure: chmod=%v restore=%v", permissionErr, restoreErr)
	}
	if lateRemovalErr == nil {
		t.Fatalf("late dashboard-state failure unexpectedly succeeded: %s", lateRemovalOutput)
	}
	if _, err := os.Lstat(filepath.Dir(removedManifest.LogPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("late failure did not occur after retained record cleanup: %v output=%s", err, lateRemovalOutput)
	}
	partialState, err := (&dashboardServer{stateRoot: stateRoot, repository: "o/r"}).readState()
	if err != nil || slices.Contains(partialState.Hidden, dashboardHiddenAttempt{Repository: "o/r", Issue: 73, Attempt: 1, Reason: "removed"}) {
		t.Fatalf("late failure incorrectly completed dashboard state: state=%#v err=%v output=%s", partialState, err, lateRemovalOutput)
	}
	removalPlaywright := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/permanent-removal-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "removal-playwright"))
	removalPlaywright.Dir = source
	removalPlaywright.Env = append(os.Environ(), "AGENT_SYMPHONY_REMOVAL_E2E_URL=http://"+restartAddress)
	if removalOutput, removalErr := removalPlaywright.CombinedOutput(); removalErr != nil {
		t.Fatalf("real permanent-removal Playwright: %v\n%s\nserve:\n%s", removalErr, removalOutput, restartOutput.String())
	}
	removeCLI := exec.Command(binary, "control", "--repository", "o/r", "--runtime-state", stateRoot, "--action", "remove", "--issue", "73", "--attempt", "1", "--confirm", "--request-id", "full-system-removal-retry", "--timeout", controlTimeout, "--json")
	removeOutput, err := removeCLI.CombinedOutput()
	if err != nil || !strings.Contains(string(removeOutput), `"ok":true`) {
		t.Fatalf("idempotent permanent-removal CLI: %v output=%s serve=%s", err, removeOutput, restartOutput.String())
	}
	for _, path := range []string{removedManifest.Worktree, removedResult, filepath.Dir(removedManifest.LogPath), removedSnapshot, removedReviewResult} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("selected resource remains after permanent removal: %s: %v", path, err)
		}
	}
	for _, session := range []string{removedManifest.Session, removedReviewSession} {
		if fullSystemTmuxSessionExists(tmuxEnvironment, session) {
			t.Fatalf("selected tmux session remains after permanent removal: %s", session)
		}
	}
	for _, path := range []string{filepath.Join(filepath.Dir(retainedManifest.LogPath), "manifest.json"), retainedHandoff, retainedSnapshot} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("unrelated resource changed during permanent removal: %s: %v", path, err)
		}
	}
	for _, session := range []string{unrelatedSession} {
		if !fullSystemTmuxSessionExists(tmuxEnvironment, session) {
			t.Fatalf("unrelated tmux session changed during permanent removal: %s", session)
		}
	}
	fixture.mu.Lock()
	deniedMutations := append([]string(nil), fixture.deniedMutations...)
	fixture.mu.Unlock()
	if len(deniedMutations) != 0 {
		t.Fatalf("permanent removal attempted GitHub mutations: %q", deniedMutations)
	}
	if err := restarted.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := restarted.Wait(); err != nil {
		t.Fatalf("restarted serve shutdown: %v output=%s", err, restartOutput.String())
	}
	restartedStopped = true

	removalRestartAddress := freeAddress(t)
	removalRestarted := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", removalRestartAddress, "--interval", "30s")
	removalRestarted.Dir, removalRestarted.Env = repository, server.Env
	var removalRestartOutput synchronizedBuffer
	removalRestarted.Stdout, removalRestarted.Stderr = &removalRestartOutput, &removalRestartOutput
	if err := removalRestarted.Start(); err != nil {
		t.Fatal(err)
	}
	removalRestartStopped := false
	t.Cleanup(func() {
		if !removalRestartStopped {
			_ = removalRestarted.Process.Kill()
			_ = removalRestarted.Wait()
		}
	})
	waitHTTP(t, "http://"+removalRestartAddress+"/status.json", deadline(15*time.Second), &removalRestartOutput)
	reconcileAfterRemoval := exec.Command(binary, "control", "--repository", "o/r", "--runtime-state", stateRoot, "--action", "reconcile", "--request-id", "full-system-after-removal", "--timeout", controlTimeout, "--json")
	reconcileOutput, err := reconcileAfterRemoval.CombinedOutput()
	if err != nil || !strings.Contains(string(reconcileOutput), `"ok":true`) {
		t.Fatalf("post-removal restart reconcile: %v output=%s serve=%s", err, reconcileOutput, removalRestartOutput.String())
	}
	response, err = http.Get("http://" + removalRestartAddress + "/dashboard-state.json")
	if err != nil {
		t.Fatal(err)
	}
	var removalState dashboardState
	decodeErr = json.NewDecoder(response.Body).Decode(&removalState)
	_ = response.Body.Close()
	if decodeErr != nil || !slices.Contains(removalState.Hidden, dashboardHiddenAttempt{Repository: "o/r", Issue: 73, Attempt: 1, Reason: "removed"}) {
		t.Fatalf("removed attempt did not persist after restart/reconcile: state=%#v err=%v", removalState, decodeErr)
	}
	fixture.mu.Lock()
	deniedMutations = append([]string(nil), fixture.deniedMutations...)
	fixture.mu.Unlock()
	if len(deniedMutations) != 0 {
		t.Fatalf("restart/reconcile attempted GitHub mutations during removal proof: %q", deniedMutations)
	}
	if err := removalRestarted.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := removalRestarted.Wait(); err != nil {
		t.Fatalf("post-removal serve shutdown: %v output=%s", err, removalRestartOutput.String())
	}
	removalRestartStopped = true
	if err := stopFullSystemTmux(server.Env, currentSession); err != nil {
		t.Fatal(err)
	}
	if err := stopFullSystemProcesses(root); err != nil {
		t.Fatal(err)
	}
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(root); !os.IsNotExist(err) {
		t.Fatalf("isolated full-system root remained after cleanup: %v", err)
	}
	t.Logf("compiled full-system Playwright passed; GitHub requests=%d; serve output:\n%s", len(requests), output.String())
}

func countRequest(requests []string, want string) int {
	count := 0
	for _, request := range requests {
		if request == want {
			count++
		}
	}
	return count
}

func runExternal(t *testing.T, dir, name string, args ...string) string {
	t.Helper()
	command := exec.Command(name, args...)
	command.Dir = dir
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("%s %v: %v: %s", name, args, err, output)
	}
	return string(output)
}

func writeExecutable(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
}

func freeAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitHTTP(t *testing.T, target string, timeout time.Duration, output fmt.Stringer) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		response, err := http.Get(target)
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("server did not become ready: %s", output.String())
}

func waitFor(timeout time.Duration, ready func() bool) bool {
	deadline := time.Now().Add(timeout)
	for !ready() && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	return ready()
}

func ensureFullSystemTmuxSession(t *testing.T, environment []string, session, dir string) {
	t.Helper()
	if fullSystemTmuxSessionExists(environment, session) {
		return
	}
	command := exec.Command("tmux", "new-session", "-d", "-s", session, "-c", dir, "sleep", "300")
	command.Env = environment
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("create exact fixture tmux session %s: %v: %s", session, err, output)
	}
}

func fullSystemTmuxSessionExists(environment []string, session string) bool {
	command := exec.Command("tmux", "has-session", "-t", "="+session)
	command.Env = environment
	return command.Run() == nil
}

func stopFullSystemTmux(environment []string, session string) error {
	query := exec.Command("tmux", "display-message", "-p", "-t", "="+session, "#{pid}")
	query.Env = environment
	body, _ := query.Output()
	pid, _ := strconv.Atoi(strings.TrimSpace(string(body)))
	stop := exec.Command("tmux", "kill-server")
	stop.Env = environment
	_ = stop.Run()
	if pid > 1 {
		process, err := os.FindProcess(pid)
		if err != nil {
			return err
		}
		_ = process.Kill()
	}
	return nil
}

func stopFullSystemProcesses(root string) error {
	listed, err := exec.Command("ps", "-axo", "pid=,command=").Output()
	if err != nil {
		return err
	}
	for _, line := range strings.Split(string(listed), "\n") {
		if !strings.Contains(line, root) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil || pid <= 1 {
			return fmt.Errorf("invalid isolated process identity %q", line)
		}
		process, err := os.FindProcess(pid)
		if err != nil {
			return err
		}
		_ = process.Kill()
	}
	return nil
}
