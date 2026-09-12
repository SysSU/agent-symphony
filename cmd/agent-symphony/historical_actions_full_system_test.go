package main

import (
	"encoding/json"
	"errors"
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
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// This exercises the compiled daemon and embedded dashboard against the
// historical combinations that escaped the original deployment gate.
func TestHistoricalAttemptActionsFullSystemE2E(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_E2E") != "1" {
		t.Skip("set AGENT_SYMPHONY_FULL_SYSTEM_E2E=1 to run the compiled full-system gate")
	}
	for _, name := range []string{"tmux", "curl"} {
		if _, err := exec.LookPath(name); err != nil {
			t.Skip(name + " is unavailable")
		}
	}
	raceMode := os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_RACE") == "1"
	deadline := func(normal time.Duration) time.Duration {
		if raceMode {
			return normal * 4
		}
		return normal
	}
	source, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root, err := os.MkdirTemp("/tmp", "agent-symphony-historical-actions-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	repository, origin := filepath.Join(root, "repository"), filepath.Join(root, "origin.git")
	runExternal(t, "", "git", "init", "-q", "--bare", origin)
	runExternal(t, "", "git", "init", "-q", "-b", "main", repository)
	runExternal(t, repository, "git", "config", "user.name", "Historical fixture")
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
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(projectTmuxRoot(stateRoot), 0o700); err != nil {
		t.Fatal(err)
	}
	attemptRoot := productionAttemptRoot(stateRoot)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	sourceGit := filepath.Join(attemptRoot, "source.git")
	runExternal(t, "", "git", "init", "-q", "--bare", sourceGit)
	runExternal(t, repository, "git", "remote", "add", "runtime-fixture", sourceGit)
	runExternal(t, repository, "git", "push", "-q", "runtime-fixture", "main")
	if err := os.MkdirAll(productionSnapshotRoot(stateRoot), 0o700); err != nil {
		t.Fatal(err)
	}
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	manifests := map[int]agentruntime.Manifest{}
	for issue, attempt := range map[int]int{160: 1, 162: 1, 191: 9, 192: 9} {
		manifest := historicalFullSystemManifest(t, sourceGit, stateRoot, base, issue, attempt)
		if issue == 160 || issue == 162 {
			manifest.State, manifest.ReviewHead = "completed", base
		} else {
			manifest.State = "failed"
		}
		body, _ := json.Marshal(manifest)
		if err := os.WriteFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"), body, 0o600); err != nil {
			t.Fatal(err)
		}
		manifests[issue] = manifest
		key := ownerAttemptKey("o/r", issue, attempt)
		state.IssueGenerations[ownerIssueKey("o/r", issue)] = 1
		state.AttemptGenerations[key] = 1
		state.Attempts[key] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	}
	archived := ownerAttemptKey("o/r", 161, 1)
	state.IssueGenerations[ownerIssueKey("o/r", 161)] = 1
	state.AttemptGenerations[archived] = 2
	// The old-valid Dismiss tombstone has the same invalidated prior-attempt
	// shape as the live Archive tombstone, so this binary fixture can run red
	// against unmodified main rather than failing during fixture startup.
	state.Tombstones[archived] = runtimeTombstone{Repository: "o/r", Issue: 161, Attempt: 1, Action: "dismissed", InvalidatedGeneration: 1, Generation: 2, CleanupPhase: "completed", Revision: 1}
	if err := writeRuntimeOwnerState(stateRoot, attemptRoot, state); err != nil {
		t.Fatal(err)
	}
	fixture := &fullSystemGitHub{base: base, origin: origin, historicalIssues: map[int]map[string]any{}, historicalComments: map[int][]map[string]any{}}
	for _, issue := range []int{160, 161, 162, 163, 191, 192} {
		fixture.historicalIssues[issue] = map[string]any{"number": issue, "node_id": fmt.Sprintf("I_%d", issue), "title": fmt.Sprintf("Historical issue %d", issue), "body": "## Context\nHistorical operator state.\n", "state": "closed", "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}, "labels": []any{}}
	}
	for _, issue := range []int{160, 161, 162, 163} {
		branch, err := internalgithub.AttemptBranch("o/r", issue, 1)
		if err != nil {
			t.Fatal(err)
		}
		pr := issue + 800
		marker, err := internalgithub.AttemptMarker(issue, 1, branch, base, pr, "review")
		if err != nil {
			t.Fatal(err)
		}
		fixture.historicalPulls = append(fixture.historicalPulls, map[string]any{"number": pr, "body": marker, "state": "closed", "merged_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}, "head": map[string]any{"sha": base, "ref": branch}, "base": map[string]any{"sha": base}})
		fixture.historicalComments[issue] = []map[string]any{{"id": issue + 1000, "body": marker, "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}}}
	}
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
	writeExecutable(t, filepath.Join(binDir, "codex"), "#!/bin/sh\nexit 0\n")
	cfg := config.Default("o/r")
	cfg.Commands.Orchestrator, cfg.Commands.OrchestratorAudit = nil, nil
	cfg.ReconciliationIntervalSeconds = 1
	configPath := filepath.Join(repository, config.DefaultPath)
	if err := config.Write(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(repository, "pr-state.json")
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	address := freeAddress(t)
	environment := append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GITHUB_URL="+github.URL, "CODEX_HOME="+filepath.Join(root, "codex-home"), "TMUX_TMPDIR="+projectTmuxRoot(stateRoot))
	start := func(address string) (*exec.Cmd, *synchronizedBuffer) {
		command := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--interval", "1s")
		command.Dir, command.Env = repository, environment
		output := &synchronizedBuffer{}
		command.Stdout, command.Stderr = output, output
		if err := command.Start(); err != nil {
			t.Fatal(err)
		}
		return command, output
	}
	server, output := start(address)
	stopped := false
	t.Cleanup(func() {
		_ = stopFullSystemProcesses(root)
		if !stopped {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
	})
	waitHTTP(t, "http://"+address+"/status.json", deadline(20*time.Second), output)
	ready := waitFor(deadline(20*time.Second), func() bool {
		response, err := http.Get("http://" + address + "/status.json")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var status dashboardStatusSnapshot
		if json.NewDecoder(response.Body).Decode(&status) != nil {
			return false
		}
		ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
		if err != nil || status.OwnerEpoch != ledger.Epoch {
			return false
		}
		for _, issue := range []int{191, 192} {
			key := ownerIssueKey("o/r", issue)
			observation := ledger.Observations[key]
			attemptKey := ownerAttemptKey("o/r", issue, 9)
			if observation.Present || observation.ObservationEpoch != ledger.Epoch || observation.OwnerGeneration != ledger.IssueGenerations[key] || ledger.Attempts[attemptKey].Generation != ledger.AttemptGenerations[attemptKey] {
				return false
			}
		}
		found := map[int]string{}
		for _, attempt := range status.Statuses {
			found[attempt.Issue] = attempt.State
			if attempt.Issue == 161 {
				return false
			}
			if (attempt.Issue == 160 || attempt.Issue == 162 || attempt.Issue == 163 || attempt.Issue == 191 || attempt.Issue == 192) && attempt.OperatorBlocked {
				return false
			}
		}
		return found[160] == "completed" && found[162] == "completed" && found[163] == "completed" && found[191] == "orphaned" && found[192] == "orphaned"
	})
	if !ready {
		response, _ := http.Get("http://" + address + "/status.json")
		var status any
		if response != nil {
			_ = json.NewDecoder(response.Body).Decode(&status)
			_ = response.Body.Close()
		}
		fixture.mu.Lock()
		requests := append([]string(nil), fixture.requests...)
		fixture.mu.Unlock()
		if len(requests) > 30 {
			requests = requests[len(requests)-30:]
		}
		t.Fatalf("historical projection not ready: status=%#v requests=%q serve=%s", status, requests, output.String())
	}
	ensureFullSystemTmuxSession(t, environment, manifests[160].Session, repository)
	const unrelatedSession = "historical-fixture-unrelated"
	ensureFullSystemTmuxSession(t, environment, unrelatedSession, repository)
	t.Cleanup(func() { _ = stopFullSystemTmux(environment, unrelatedSession) })
	playwright := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/historical-actions-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "playwright"))
	playwright.Dir = source
	playwright.Env = append(os.Environ(), "AGENT_SYMPHONY_HISTORICAL_E2E_URL=http://"+address, "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(raceMode))
	if browserOutput, err := playwright.CombinedOutput(); err != nil {
		t.Fatalf("real historical browser actions: %v\n%s\nserve:\n%s", err, browserOutput, output.String())
	}
	// The dashboard intentionally does not consume a completed Dismiss response
	// body. Verify full HTTP JSON framing separately from the browser's visible
	// success and the durable owner assertions below.
	apiRequest, err := http.NewRequest(http.MethodPost, "http://"+address+"/actions/dismiss?repository=o%2Fr&issue=162&attempt=1", nil)
	if err != nil {
		t.Fatal(err)
	}
	apiRequest.Header.Set("Origin", "http://"+address)
	apiResponse, err := http.DefaultClient.Do(apiRequest)
	if err != nil {
		t.Fatal(err)
	}
	var apiResult controlResult
	decodeErr := json.NewDecoder(apiResponse.Body).Decode(&apiResult)
	_ = apiResponse.Body.Close()
	if decodeErr != nil || apiResponse.StatusCode != http.StatusOK || !apiResult.OK || apiResult.OwnerRevision == 0 {
		t.Fatalf("completed Dismiss API response: status=%d result=%#v decode=%v", apiResponse.StatusCode, apiResult, decodeErr)
	}
	if !waitFor(deadline(20*time.Second), func() bool {
		ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
		return err == nil && ledger.Tombstones[ownerAttemptKey("o/r", 160, 1)].CleanupPhase == "completed" && ledger.Tombstones[ownerAttemptKey("o/r", 162, 1)].Action == "dismissed" && ledger.Tombstones[ownerAttemptKey("o/r", 163, 1)].Action == "archived" && ledger.Tombstones[ownerAttemptKey("o/r", 163, 1)].Manifest == nil && ledger.Tombstones[ownerAttemptKey("o/r", 191, 9)].CleanupPhase == "completed" && ledger.Tombstones[ownerAttemptKey("o/r", 192, 9)].Action == "dismissed"
	}) {
		ledger, _ := os.ReadFile(filepath.Join(stateRoot, runtimeOwnerStateFile))
		t.Fatalf("historical mutations did not complete: %s\nserve:\n%s", ledger, output.String())
	}
	for _, issue := range []int{160, 191} {
		if _, err := os.Lstat(manifests[issue].Worktree); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("%d worktree was not cleaned: %v", issue, err)
		}
	}
	if fullSystemTmuxSessionExists(environment, manifests[160].Session) || !fullSystemTmuxSessionExists(environment, unrelatedSession) {
		t.Fatal("archive removed an unrelated tmux session or kept its exact owned session")
	}
	for _, issue := range []int{162, 192} {
		if _, err := os.Stat(manifests[issue].Worktree); err != nil {
			t.Fatalf("%d dismissed worktree was not retained: %v", issue, err)
		}
	}
	for _, issue := range []int{160, 162, 192} {
		if _, err := os.Stat(manifests[issue].LogPath); err != nil {
			t.Fatalf("%d log was not retained: %v", issue, err)
		}
	}
	if _, err := os.Lstat(filepath.Dir(manifests[191].LogPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandoned diagnostics were not removed: %v", err)
	}
	if err := server.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("shutdown: %v\n%s", err, output.String())
	}
	stopped = true
	address = freeAddress(t)
	server, output = start(address)
	stopped = false
	waitHTTP(t, "http://"+address+"/status.json", deadline(20*time.Second), output)
	var freshRevision uint64
	if !waitFor(deadline(20*time.Second), func() bool {
		ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
		if err != nil || ledger.Epoch < 3 || ledger.Observations[ownerIssueKey("o/r", 163)].ObservationEpoch != ledger.Epoch {
			return false
		}
		for _, expected := range []struct {
			issue, attempt int
			action         string
		}{{160, 1, "archived"}, {162, 1, "dismissed"}, {163, 1, "archived"}, {191, 9, "abandoned"}, {192, 9, "dismissed"}} {
			key := ownerAttemptKey("o/r", expected.issue, expected.attempt)
			if ledger.Tombstones[key].Action != expected.action || ledger.Attempts[key].Generation != 0 {
				return false
			}
		}
		if freshRevision == 0 {
			freshRevision = ledger.Revision
		}
		response, err := http.Get("http://" + address + "/status.json")
		if err != nil {
			return false
		}
		defer response.Body.Close()
		var status dashboardStatusSnapshot
		if json.NewDecoder(response.Body).Decode(&status) != nil || status.OwnerEpoch != ledger.Epoch || status.OwnerRevision < freshRevision {
			return false
		}
		for _, attempt := range status.Statuses {
			if attempt.Issue == 160 || attempt.Issue == 161 || attempt.Issue == 162 || attempt.Issue == 163 || attempt.Issue == 191 || attempt.Issue == 192 {
				return false
			}
		}
		return true
	}) {
		t.Fatalf("restart or stale GitHub facts restored hidden cards: %s", output.String())
	}
}

func historicalFullSystemManifest(t *testing.T, sourceGit, stateRoot, base string, issue, number int) agentruntime.Manifest {
	t.Helper()
	attempt, err := agentruntime.AttemptIdentity(productionAttemptRoot(stateRoot), agentruntime.Attempt{Repository: "o/r", Issue: issue, Number: number, BaseSHA: base})
	if err != nil {
		t.Fatal(err)
	}
	attempt.State = "failed"
	attempt.CreatedAt, attempt.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	attempt.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier("o/r"), fmt.Sprintf("%d-%d", issue, number), "agent.log")
	if err := os.MkdirAll(filepath.Dir(attempt.LogPath), 0o700); err != nil {
		t.Fatal(err)
	}
	runExternal(t, "", "git", "--git-dir", sourceGit, "worktree", "add", "-q", "-b", attempt.Branch, attempt.Worktree, base)
	if err := os.WriteFile(attempt.LogPath, []byte("retained diagnostic\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(attempt)
	if err := os.WriteFile(filepath.Join(filepath.Dir(attempt.LogPath), "manifest.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	return attempt
}
