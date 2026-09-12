package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// This fixture keeps the actual daemon, dashboard, tmux supervisor, and
// persistence path; only the external GitHub and agent executables are faked.
func TestDashboardOrchestratorFullSystemE2E(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_E2E") != "1" {
		t.Skip("set AGENT_SYMPHONY_FULL_SYSTEM_E2E=1 to run the compiled full-system gate")
	}
	for _, name := range []string{"tmux", "curl"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skip(name + " is unavailable")
		}
	}
	source, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/tmp", "agent-symphony-orchestrator-e2e-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stopFullSystemProcesses(root); _ = os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	// macOS /tmp can be setgid wheel; the local agent-host contract requires
	// its launch file to belong to the invoking process group.
	if err := os.Chown(root, -1, os.Getegid()); err != nil {
		t.Fatal(err)
	}
	repository, origin := filepath.Join(root, "repository"), filepath.Join(root, "origin.git")
	runExternal(t, "", "git", "init", "-q", "--bare", origin)
	runExternal(t, "", "git", "init", "-q", "-b", "main", repository)
	runExternal(t, repository, "git", "config", "user.name", "Orchestrator fixture")
	runExternal(t, repository, "git", "config", "user.email", "fixture@example.invalid")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runExternal(t, repository, "git", "add", "README.md")
	runExternal(t, repository, "git", "commit", "-qm", "base")
	runExternal(t, repository, "git", "remote", "add", "origin", origin)
	runExternal(t, repository, "git", "push", "-q", "-u", "origin", "main")
	base := strings.TrimSpace(runExternal(t, repository, "git", "rev-parse", "HEAD"))
	stateRoot := filepath.Join(root, "runtime")
	for _, path := range []string{stateRoot, projectTmuxRoot(stateRoot), productionAttemptRoot(stateRoot), productionSnapshotRoot(stateRoot)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	sourceGit := filepath.Join(productionAttemptRoot(stateRoot), "source.git")
	runExternal(t, "", "git", "init", "-q", "--bare", sourceGit)
	runExternal(t, repository, "git", "remote", "add", "runtime-fixture", sourceGit)
	runExternal(t, repository, "git", "push", "-q", "runtime-fixture", "main")
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	if err := writeRuntimeOwnerState(stateRoot, productionAttemptRoot(stateRoot), state); err != nil {
		t.Fatal(err)
	}
	githubFixture := &fullSystemGitHub{base: base, origin: origin, historicalIssues: map[int]map[string]any{191: {
		"number": 191, "node_id": "I_191", "title": "Orchestrator control fixture", "body": "## Context\nClosed fixture.\n", "state": "closed", "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}, "labels": []any{},
	}}, historicalComments: map[int][]map[string]any{}}
	github := httptest.NewServer(githubFixture)
	defer github.Close()
	peer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/project.json" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version": 1, "repository": "peer/project",
			"snapshot": map[string]any{"updated_at": time.Now().UTC(), "statuses": []map[string]any{{"repository": "peer/project", "issue": 27, "attempt": 1, "title": "Read-only peer fixture", "state": "failed"}}},
			"state":    map[string]any{"version": 1, "hidden": []any{}},
		})
	}))
	defer peer.Close()
	binDir := filepath.Join(root, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(binDir, "agent-symphony")
	build := []string{"build", "-o", binary, "."}
	raceMode := os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_RACE") == "1"
	if raceMode {
		build = []string{"build", "-race", "-o", binary, "."}
	}
	runExternal(t, source, "go", build...)
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
if [ "$1" = audit ]; then printf 'fixture audit complete\n'; exit 0; fi
printf 'orchestrator-ready\n'
while IFS= read -r line; do printf 'orchestrator-received:%s\n' "$line"; done
`)
	cfg := config.Default("o/r")
	cfg.Commands.Orchestrator = []string{"codex", "orchestrate"}
	cfg.Commands.OrchestratorAudit = []string{"codex", "audit"}
	cfg.ReconciliationIntervalSeconds = 60
	configPath := filepath.Join(repository, config.DefaultPath)
	if err := config.Write(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(repository, "pr-state.json")
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	environment := append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GITHUB_URL="+github.URL, "CODEX_HOME="+filepath.Join(root, "codex-home"), "TMUX_TMPDIR="+projectTmuxRoot(stateRoot))
	address := freeAddress(t)
	start := func() (*exec.Cmd, *synchronizedBuffer) {
		command := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--dashboard-project", peer.URL, "--interval", "60s")
		command.Dir, command.Env = repository, environment
		output := &synchronizedBuffer{}
		command.Stdout, command.Stderr = output, output
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		return command, output
	}
	server, output := start()
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
	})
	deadline := 30 * time.Second
	if raceMode {
		deadline *= 4
	}
	baseURL := "http://" + address
	waitHTTP(t, baseURL+"/status.json", deadline, output)
	readOrchestrator := func() (orchestratoragent.Status, error) {
		response, err := http.Get(baseURL + "/orchestrator.json")
		if err != nil {
			return orchestratoragent.Status{}, err
		}
		defer response.Body.Close()
		var status orchestratoragent.Status
		err = json.NewDecoder(response.Body).Decode(&status)
		if response.StatusCode != http.StatusOK {
			return status, fmt.Errorf("orchestrator HTTP %d: %w", response.StatusCode, err)
		}
		return status, err
	}
	ready := waitFor(deadline, func() bool {
		status, err := readOrchestrator()
		if err != nil || status.State != "running" || status.Generation < 1 {
			return false
		}
		response, err := http.Get(baseURL + "/status.json")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var snapshot dashboardStatusSnapshot
		if json.NewDecoder(response.Body).Decode(&snapshot) != nil {
			return false
		}
		return len(snapshot.Statuses) == 0
	})
	if !ready {
		t.Fatalf("orchestrator dashboard was not ready: %s", output.String())
	}
	initial, err := readOrchestrator()
	if err != nil {
		t.Fatal(err)
	}
	livePane := exec.Command("tmux", "display-message", "-p", "-t", agentruntime.PaneTarget(initial.Session), "#{pane_dead}")
	livePane.Env = environment
	liveOutput, liveErr := livePane.Output()
	if liveErr != nil || strings.TrimSpace(string(liveOutput)) != "0" {
		capture := exec.Command("tmux", "capture-pane", "-p", "-S", "-1000", "-t", agentruntime.PaneTarget(initial.Session))
		capture.Env = environment
		captured, _ := capture.CombinedOutput()
		t.Fatalf("orchestrator pane is not live: %v %s\n%s\nserve=%s", liveErr, liveOutput, captured, output.String())
	}
	ledgerBefore, err := readRuntimeOwnerState(stateRoot, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	runBrowser := func(phase string) {
		browser := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/orchestrator-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "playwright"))
		browser.Dir = source
		browser.Env = append(os.Environ(), "AGENT_SYMPHONY_ORCHESTRATOR_E2E_URL="+baseURL, "AGENT_SYMPHONY_ORCHESTRATOR_E2E_PEER="+peer.URL, "AGENT_SYMPHONY_ORCHESTRATOR_E2E_PHASE="+phase, "AGENT_SYMPHONY_ORCHESTRATOR_E2E_GENERATION="+strconv.Itoa(initial.Generation), "AGENT_SYMPHONY_ORCHESTRATOR_E2E_SESSION="+initial.Session, "AGENT_SYMPHONY_ORCHESTRATOR_E2E_MODE="+initial.ContextMode, "AGENT_SYMPHONY_ORCHESTRATOR_E2E_LAST_HEALTHY="+initial.LastHealthyAt.Format(time.RFC3339Nano), "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(raceMode))
		if browserOutput, err := browser.CombinedOutput(); err != nil {
			t.Fatalf("orchestrator browser phase %s: %v\n%s\nserve:\n%s", phase, err, browserOutput, output.String())
		}
	}
	runBrowser("initial")
	finalBody, err := os.ReadFile(filepath.Join(stateRoot, "orchestrator-agent.json"))
	var final orchestratoragent.Status
	if err == nil {
		err = json.Unmarshal(finalBody, &final)
	}
	if err != nil || final.Generation != initial.Generation+2 || final.ContextMode != "rebuild" || final.Session != initial.Session {
		t.Fatalf("orchestrator transitions: initial=%+v final=%+v err=%v serve=%s", initial, final, err, output.String())
	}
	if !fullSystemTmuxSessionExists(environment, final.Session) {
		t.Fatal("orchestrator exact tmux session is not live after controls")
	}
	if !waitFor(deadline, func() bool {
		ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
		return err == nil && ledger.CycleOutcomeID > ledgerBefore.CycleOutcomeID
	}) {
		t.Fatal("Check now did not commit a reconciliation cycle")
	}
	if err := server.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("orchestrator shutdown: %v\n%s", err, output.String())
	}
	stopped = true
	manifest := historicalFullSystemManifest(t, sourceGit, stateRoot, base, 191, 9)
	ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey("o/r", 191, 9)
	ledger.Revision++
	ledger.IssueGenerations[ownerIssueKey("o/r", 191)] = 1
	ledger.AttemptGenerations[key] = 1
	ledger.Attempts[key] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	if err := writeRuntimeOwnerState(stateRoot, productionAttemptRoot(stateRoot), ledger); err != nil {
		t.Fatal(err)
	}
	address = freeAddress(t)
	baseURL = "http://" + address
	server, output = start()
	stopped = false
	waitHTTP(t, baseURL+"/status.json", deadline, output)
	if !waitFor(deadline, func() bool {
		status, err := readOrchestrator()
		if err != nil || status.State != "running" || status.Generation != final.Generation || status.ContextMode != "rebuild" || status.Session != final.Session {
			return false
		}
		heartbeat, err := os.ReadFile(filepath.Join(productionSnapshotRoot(stateRoot), "orchestrator-"+strings.TrimPrefix(final.Session, "as-o-"), orchestratoragent.HeartbeatReportFile))
		if err != nil || !strings.Contains(string(heartbeat), `"state": "completed"`) {
			return false
		}
		response, err := http.Get(baseURL + "/status.json")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var snapshot dashboardStatusSnapshot
		if json.NewDecoder(response.Body).Decode(&snapshot) != nil {
			return false
		}
		for _, attempt := range snapshot.Statuses {
			if attempt.Issue == 191 && attempt.Attempt == 9 && attempt.State == "orphaned" && !attempt.OperatorBlocked {
				return true
			}
		}
		return false
	}) {
		t.Fatalf("orchestrator context did not survive restart: %s", output.String())
	}
	runBrowser("post-restart")
	stateBody, err := os.ReadFile(filepath.Join(stateRoot, "orchestrator-agent.json"))
	var persisted struct {
		LastInvestigation string `json:"last_investigation_digest"`
	}
	if err != nil || json.Unmarshal(stateBody, &persisted) != nil || len(persisted.LastInvestigation) != 64 {
		t.Fatalf("Investigate did not persist its exact projection digest: %v %s", err, stateBody)
	}
}
