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
	"syscall"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
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
	githubFixture := &fullSystemGitHub{base: base, origin: origin, historicalIssues: map[int]map[string]any{
		191: {"number": 191, "node_id": "I_191", "title": "Orchestrator control fixture", "body": "## Context\nClosed fixture.\n", "state": "closed", "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}, "labels": []any{}},
	}, historicalComments: map[int][]map[string]any{}}
	github := httptest.NewServer(githubFixture)
	defer github.Close()
	peerRoot := filepath.Join(root, "peer")
	if err := writeDashboardStatusSnapshot(peerRoot, dashboardStatusSnapshot{UpdatedAt: time.Now().UTC(), Statuses: []orchestrator.RecoveryStatus{{Repository: "peer/project", Issue: 27, Attempt: 1, Title: "Action-eligible peer fixture", State: "completed", IssueClosed: true}}}); err != nil {
		t.Fatal(err)
	}
	peer := httptest.NewServer(newProjectDashboardServer(t.Context(), peerRoot, "peer/project", nil, "tmux", nil, nil, false, "").webHandler())
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
	eventsFile, err := os.CreateTemp("/tmp", "agent-symphony-orchestrator-events-")
	if err != nil {
		t.Fatal(err)
	}
	fixtureEvents := eventsFile.Name()
	t.Cleanup(func() { _ = os.Remove(fixtureEvents) })
	if err := eventsFile.Chmod(0o666); err != nil {
		t.Fatal(err)
	}
	if err := eventsFile.Close(); err != nil {
		t.Fatal(err)
	}
	holdAudit, releaseAudit, failAudit := fixtureEvents+"-hold", fixtureEvents+"-release", fixtureEvents+"-fail"
	if err := syscall.Mkfifo(releaseAudit, 0o666); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(releaseAudit, 0o666); err != nil {
		t.Fatal(err)
	}
	releaseHeldAudit := func() error {
		pipe, err := os.OpenFile(releaseAudit, os.O_RDWR|syscall.O_NONBLOCK, 0)
		if err != nil {
			return err
		}
		defer pipe.Close()
		_, err = pipe.Write([]byte("release\n"))
		return err
	}
	t.Cleanup(func() {
		_ = releaseHeldAudit()
		_ = os.Remove(holdAudit)
		_ = os.Remove(releaseAudit)
		_ = os.Remove(failAudit)
	})
	writeExecutable(t, filepath.Join(binDir, "codex"), strings.NewReplacer("@EVENTS@", fixtureEvents, "@HOLD@", holdAudit, "@RELEASE@", releaseAudit, "@FAIL@", failAudit).Replace(`#!/bin/sh
if [ "$1" = audit ]; then
  audit_context=$(cat)
  case "$audit_context" in
    *'"issue":191,"attempt":9'*) printf 'audit:191:9\n' >> "@EVENTS@" ;;
    *) printf 'audit:other\n' >> "@EVENTS@" ;;
  esac
  if [ -f "@HOLD@" ]; then
    printf 'audit:blocked\n' >> "@EVENTS@"
    IFS= read -r release < "@RELEASE@"
  fi
  if [ -f "@FAIL@" ]; then
    printf 'audit:failed\n' >> "@EVENTS@"
    printf 'fixture audit failed\n' >&2
    exit 17
  fi
  printf 'fixture audit complete\n'
  exit 0
fi
printf 'orchestrator-ready\n'
case "$2" in *"## Sanitized current projection"*) printf 'orchestrator-projection-present\n'; printf 'start:projection-present\n' >> "@EVENTS@" ;; *) printf 'orchestrator-projection-absent\n'; printf 'start:projection-absent\n' >> "@EVENTS@" ;; esac
case "$2" in *conversation-before-clear*) printf 'start:stale-conversation\n' >> "@EVENTS@" ;; esac
while IFS= read -r line; do printf 'orchestrator-received:%s\n' "$line"; printf 'received:%s\n' "$line" >> "@EVENTS@"; done
`))
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
	reportPath := filepath.Join(productionSnapshotRoot(stateRoot), "orchestrator-"+internalgithub.RepositoryIdentifier("o/r"), orchestratoragent.HeartbeatReportFile)
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
		response, _ := http.Get(baseURL + "/status.json")
		var snapshot dashboardStatusSnapshot
		if response != nil {
			_ = json.NewDecoder(response.Body).Decode(&snapshot)
			response.Body.Close()
		}
		t.Fatalf("orchestrator dashboard was not ready: statuses=%+v serve=%s", snapshot.Statuses, output.String())
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
			pane := exec.Command("tmux", "display-message", "-p", "-t", agentruntime.PaneTarget(initial.Session), "#{pane_dead} #{pane_pid} #{pane_current_command}")
			pane.Env = environment
			paneOutput, paneErr := pane.CombinedOutput()
			capture := exec.Command("tmux", "capture-pane", "-p", "-S", "-100", "-t", agentruntime.PaneTarget(initial.Session))
			capture.Env = environment
			captured, _ := capture.CombinedOutput()
			status, statusErr := readOrchestrator()
			t.Fatalf("orchestrator browser phase %s: %v\n%s\nstatus=%+v statusErr=%v pane=%s paneErr=%v capture=%s\nserve:\n%s", phase, err, browserOutput, status, statusErr, paneOutput, paneErr, captured, output.String())
		}
	}
	runBrowser("initial")
	if !waitFor(deadline, func() bool {
		body, err := os.ReadFile(fixtureEvents)
		if err != nil {
			return false
		}
		events := string(body)
		received := strings.Index(events, "received:conversation-before-clear\n")
		cleared := strings.Index(events, "start:projection-absent\n")
		rebuilt := strings.LastIndex(events, "start:projection-present\n")
		return received >= 0 && cleared > received && rebuilt > cleared && !strings.Contains(events, "start:stale-conversation\n")
	}) {
		body, _ := os.ReadFile(fixtureEvents)
		t.Fatalf("Clear/Rebuild did not restart with empty then projected context after the previous conversation: %s", body)
	}
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
		heartbeat, err := os.ReadFile(reportPath)
		if err != nil || !strings.Contains(string(heartbeat), `"state": "completed"`) {
			return false
		}
		var persisted struct {
			Handoff *struct {
				State string `json:"state"`
			} `json:"attention_handoff"`
		}
		persistedBody, err := os.ReadFile(filepath.Join(stateRoot, "orchestrator-agent.json"))
		if err != nil || json.Unmarshal(persistedBody, &persisted) != nil || persisted.Handoff == nil || (persisted.Handoff.State != "waiting" && persisted.Handoff.State != "human-attention") {
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
	var priorReport struct {
		StartedAt time.Time `json:"started_at"`
		State     string    `json:"state"`
	}
	priorBody, err := os.ReadFile(reportPath)
	if err != nil || json.Unmarshal(priorBody, &priorReport) != nil || priorReport.State != "completed" {
		t.Fatalf("startup audit did not complete before Investigate: %v %s", err, priorBody)
	}
	priorEvents, err := os.ReadFile(fixtureEvents)
	if err != nil {
		t.Fatal(err)
	}
	runBrowser("post-restart")
	stateBody, err := os.ReadFile(filepath.Join(stateRoot, "orchestrator-agent.json"))
	var persisted struct {
		LastInvestigation string `json:"last_investigation_digest"`
	}
	if err != nil || json.Unmarshal(stateBody, &persisted) != nil || len(persisted.LastInvestigation) != 64 {
		t.Fatalf("Investigate did not persist its exact projection digest: %v %s", err, stateBody)
	}
	var investigation struct {
		StartedAt        time.Time `json:"started_at"`
		CompletedAt      time.Time `json:"completed_at"`
		ProjectionDigest string    `json:"projection_digest"`
		State            string    `json:"state"`
		Report           string    `json:"report"`
	}
	if !waitFor(deadline, func() bool {
		body, readErr := os.ReadFile(reportPath)
		return readErr == nil && json.Unmarshal(body, &investigation) == nil && investigation.StartedAt.After(priorReport.StartedAt) && !investigation.CompletedAt.IsZero() && investigation.State == "completed" && investigation.ProjectionDigest == persisted.LastInvestigation && strings.Contains(investigation.Report, "fixture audit complete")
	}) {
		body, _ := os.ReadFile(reportPath)
		t.Fatalf("exact Investigate audit did not complete: prior=%s current=%s serve=%s", priorBody, body, output.String())
	}
	events, err := os.ReadFile(fixtureEvents)
	if err != nil || len(events) < len(priorEvents) || !strings.Contains(string(events[len(priorEvents):]), "audit:191:9\n") {
		t.Fatalf("new Investigate subprocess did not receive issue #191 attempt 9: err=%v prior=%q current=%q", err, priorEvents, events)
	}
	if err := os.WriteFile(holdAudit, []byte("hold next audit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	priorHeldEvents := len(events)
	runBrowser("start-held")
	if !waitFor(deadline, func() bool {
		body, readErr := os.ReadFile(fixtureEvents)
		return readErr == nil && len(body) >= priorHeldEvents && strings.Contains(string(body[priorHeldEvents:]), "audit:blocked\n")
	}) {
		body, _ := os.ReadFile(fixtureEvents)
		t.Fatalf("new Investigate subprocess did not enter controlled blocker: events=%q serve=%s", body, output.String())
	}
	if err := os.Remove(holdAudit); err != nil {
		t.Fatal(err)
	}
	var held struct {
		StartedAt time.Time `json:"started_at"`
		State     string    `json:"state"`
	}
	body, err := os.ReadFile(reportPath)
	if err != nil || json.Unmarshal(body, &held) != nil || held.State != "running" || !held.StartedAt.After(investigation.StartedAt) {
		t.Fatalf("controlled audit was not active before Recover: %v %s", err, body)
	}
	runBrowser("recover-held")
	body, err = os.ReadFile(reportPath)
	if err != nil || json.Unmarshal(body, &held) != nil || held.State != "running" {
		t.Fatalf("Recover did not return while controlled audit remained active: %v %s", err, body)
	}
	if err := releaseHeldAudit(); err != nil {
		t.Fatal(err)
	}
	if !waitFor(deadline, func() bool {
		body, readErr := os.ReadFile(reportPath)
		var report struct {
			StartedAt time.Time `json:"started_at"`
			State     string    `json:"state"`
			Report    string    `json:"report"`
		}
		return readErr == nil && json.Unmarshal(body, &report) == nil && report.StartedAt.Equal(held.StartedAt) && report.State == "completed" && strings.Contains(report.Report, "fixture audit complete")
	}) {
		body, _ := os.ReadFile(reportPath)
		t.Fatalf("controlled audit did not complete after Recover and explicit release: %s serve=%s", body, output.String())
	}
	if err := os.WriteFile(failAudit, []byte("fail next audit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeFailureEvents, err := os.ReadFile(fixtureEvents)
	if err != nil {
		t.Fatal(err)
	}
	runBrowser("fail-manual")
	var failed struct {
		StartedAt        time.Time `json:"started_at"`
		ProjectionDigest string    `json:"projection_digest"`
		State            string    `json:"state"`
	}
	if !waitFor(deadline, func() bool {
		body, readErr := os.ReadFile(reportPath)
		return readErr == nil && json.Unmarshal(body, &failed) == nil && failed.StartedAt.After(held.StartedAt) && failed.State == "failed" && failed.ProjectionDigest == persisted.LastInvestigation
	}) {
		body, _ := os.ReadFile(reportPath)
		t.Fatalf("nonzero audit did not produce a failed target-bound report: %s serve=%s", body, output.String())
	}
	events, err = os.ReadFile(fixtureEvents)
	if err != nil || len(events) < len(beforeFailureEvents) || !strings.Contains(string(events[len(beforeFailureEvents):]), "audit:191:9\naudit:failed\n") {
		t.Fatalf("failed audit subprocess was not observed: err=%v prior=%q current=%q", err, beforeFailureEvents, events)
	}
	stateBody, err = os.ReadFile(filepath.Join(stateRoot, "orchestrator-agent.json"))
	var failedState struct {
		LastInvestigation string `json:"last_investigation_digest"`
	}
	if err != nil || json.Unmarshal(stateBody, &failedState) != nil || failedState.LastInvestigation != persisted.LastInvestigation {
		t.Fatalf("failed audit did not retain the historical launch marker under test: %v %s", err, stateBody)
	}
	if err := os.Remove(failAudit); err != nil {
		t.Fatal(err)
	}
	beforeRetryEvents := len(events)
	runBrowser("retry-manual")
	if !waitFor(deadline, func() bool {
		body, readErr := os.ReadFile(reportPath)
		var report struct {
			StartedAt        time.Time `json:"started_at"`
			ProjectionDigest string    `json:"projection_digest"`
			State            string    `json:"state"`
			Report           string    `json:"report"`
		}
		return readErr == nil && json.Unmarshal(body, &report) == nil && report.StartedAt.After(failed.StartedAt) && report.ProjectionDigest == failed.ProjectionDigest && report.State == "completed" && strings.Contains(report.Report, "fixture audit complete")
	}) {
		body, _ := os.ReadFile(reportPath)
		t.Fatalf("same visible Investigate click did not retry failed audit: %s serve=%s", body, output.String())
	}
	events, err = os.ReadFile(fixtureEvents)
	if err != nil || len(events) < beforeRetryEvents || !strings.Contains(string(events[beforeRetryEvents:]), "audit:191:9\n") || strings.Contains(string(events[beforeRetryEvents:]), "audit:failed\n") {
		t.Fatalf("same-target retry did not launch a fresh successful subprocess: err=%v events=%q", err, events)
	}
}
