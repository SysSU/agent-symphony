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
	"syscall"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// Exercise the remaining attempt-action buttons through the compiled daemon,
// embedded dashboard, real tmux, and a GitHub API fake at the network boundary.
func TestDashboardLifecycleFullSystemE2E(t *testing.T) {
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
	tracing := os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_RACE") == "1"
	for _, action := range []string{"cancel", "recover", "review-plan", "review-plan-cancel"} {
		t.Run(action, func(t *testing.T) {
			root, err := os.MkdirTemp("/tmp", "as-lifecycle-")
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
			runExternal(t, repository, "git", "config", "user.name", "Lifecycle fixture")
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
			for _, directory := range []string{stateRoot, projectTmuxRoot(stateRoot), productionAttemptRoot(stateRoot), productionSnapshotRoot(stateRoot)} {
				if err := os.MkdirAll(directory, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			sourceGit := filepath.Join(productionAttemptRoot(stateRoot), "source.git")
			runExternal(t, "", "git", "init", "-q", "--bare", sourceGit)
			runExternal(t, repository, "git", "remote", "add", "runtime-fixture", sourceGit)
			runExternal(t, repository, "git", "push", "-q", "runtime-fixture", "main")
			manifest := historicalFullSystemManifest(t, sourceGit, stateRoot, base, 73, 1)
			manifest.State = "running"
			if action == "recover" {
				manifest.State = "failed"
				manifest.Diagnostic = "fixture worker failed"
			}
			body, _ := json.Marshal(manifest)
			if err := os.WriteFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"), body, 0o600); err != nil {
				t.Fatal(err)
			}
			state := newRuntimeOwnerState("o/r")
			state.Epoch, state.Revision = 1, 1
			key := ownerAttemptKey("o/r", 73, 1)
			state.IssueGenerations[ownerIssueKey("o/r", 73)] = 1
			state.AttemptGenerations[key] = 1
			state.Attempts[key] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
			if err := writeRuntimeOwnerState(stateRoot, productionAttemptRoot(stateRoot), state); err != nil {
				t.Fatal(err)
			}
			active, err := internalgithub.ActiveAttemptMarker("o/r", 73, 1, base)
			if err != nil {
				t.Fatal(err)
			}
			comments := []map[string]any{{"id": 1, "body": active, "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}}}
			if action == "recover" {
				terminal, err := internalgithub.TerminalFailureMarker(73, 1, manifest.UpdatedAt)
				if err != nil {
					t.Fatal(err)
				}
				comments = append(comments, map[string]any{"id": 2, "body": "Attempt failed closed: fixture worker failed\n\n" + terminal, "created_at": manifest.UpdatedAt.Format(time.RFC3339Nano), "updated_at": manifest.UpdatedAt.Format(time.RFC3339Nano), "user": map[string]any{"id": 42}})
			}
			fixture := &fullSystemGitHub{base: base, origin: origin, labels: map[string]bool{"agent-ready": true, "priority:P1": true, "autonomous-merge": true}, comments: comments}
			github := httptest.NewServer(fixture)
			defer github.Close()
			binDir := filepath.Join(root, "bin")
			if err := os.Mkdir(binDir, 0o700); err != nil {
				t.Fatal(err)
			}
			binary := filepath.Join(binDir, "agent-symphony")
			args := []string{"build", "-o", binary, "."}
			if tracing {
				args = []string{"build", "-race", "-o", binary, "."}
			}
			runExternal(t, source, "go", args...)
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
			codex := "#!/bin/sh\nexec tail -f /dev/null\n"
			reviewGate := ""
			reviewerPID := filepath.Join(root, "reviewer.pid")
			if strings.HasPrefix(action, "review-plan") {
				reviewGate = filepath.Join(root, "release-review.fifo")
				if err := syscall.Mkfifo(reviewGate, 0o600); err != nil {
					t.Fatal(err)
				}
				codex = fmt.Sprintf(`#!/bin/sh
IFS= read -r release < %q || exit 1
test "$release" = release || exit 1
result='{"type":"agent-symphony-review-v1","status":"clean","findings":[]}'
tmp="$AGENT_SYMPHONY_REVIEW_RESULT.tmp.$$"
printf '%%s\n' "$result" > "$tmp" || exit 1
mv "$tmp" "$AGENT_SYMPHONY_REVIEW_RESULT" || exit 1
printf '%%s\n' "$result"
`, reviewGate)
				if action == "review-plan-cancel" {
					codex = fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$$" > %q || exit 1
IFS= read -r release < %q || exit 1
test "$release" = release || exit 1
printf '%%s\n' '{"body":"/agent-symphony status needs-attention: reviewer wrote after cancellation"}' | curl -fsS -X POST -H 'Content-Type: application/json' --data-binary @- %q
result='{"type":"agent-symphony-review-v1","status":"clean","findings":[]}'
printf '%%s\n' "$result" > "$AGENT_SYMPHONY_REVIEW_RESULT"
`, reviewerPID, reviewGate, github.URL+"/repos/o/r/issues/73/comments")
				}
			}
			writeExecutable(t, filepath.Join(binDir, "codex"), codex)
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
			if action != "recover" {
				ensureFullSystemTmuxSession(t, environment, manifest.Session, repository)
			}
			server := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--interval", "200ms")
			server.Dir, server.Env = repository, environment
			output := &synchronizedBuffer{}
			server.Stdout, server.Stderr = output, output
			if err := server.Start(); err != nil {
				t.Fatal(err)
			}
			stopped := false
			t.Cleanup(func() {
				if !stopped {
					_ = server.Process.Kill()
					_ = server.Wait()
				}
				_ = stopFullSystemTmux(environment, manifest.Session)
				_ = stopFullSystemProcesses(root)
			})
			limit := 30 * time.Second
			if tracing {
				limit = 90 * time.Second
			}
			waitHTTP(t, "http://"+address+"/status.json", limit, output)
			if !waitFor(limit, func() bool {
				ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil || len(ledger.Observations[ownerIssueKey("o/r", 73)].IssueUpdates) != 0 {
					return false
				}
				controlsSettled := false
				for _, effect := range ledger.Effects {
					if effect.Reconciliation != nil && effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueControlSnapshot && effect.State == "completed" {
						controlsSettled = true
					}
				}
				if !controlsSettled {
					return false
				}
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
					if status.Issue == 73 && status.Attempt == 1 {
						return action == "recover" && status.Retryable || action != "recover" && status.State == "active"
					}
				}
				return false
			}) {
				ledger, _ := os.ReadFile(filepath.Join(stateRoot, runtimeOwnerStateFile))
				t.Fatalf("%s button never became available: ledger=%s serve=%s", action, ledger, output.String())
			}
			playwright := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/dashboard-lifecycle-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "playwright"))
			playwright.Dir = source
			playwright.Env = append(os.Environ(), "AGENT_SYMPHONY_LIFECYCLE_E2E_URL=http://"+address, "AGENT_SYMPHONY_LIFECYCLE_E2E_ACTION="+action, "AGENT_SYMPHONY_LIFECYCLE_E2E_REVIEW_GATE="+reviewGate, "AGENT_SYMPHONY_LIFECYCLE_E2E_REVIEWER_PID="+reviewerPID, "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(tracing))
			if browserOutput, err := playwright.CombinedOutput(); err != nil {
				ledger, _ := os.ReadFile(filepath.Join(stateRoot, runtimeOwnerStateFile))
				reviewerArtifact := ""
				if current, readErr := readRuntimeOwnerState(stateRoot, "o/r"); readErr == nil {
					for _, effect := range current.Effects {
						if effect.Reconciliation != nil && effect.Reconciliation.Reviewer != nil {
							reviewer := effect.Reconciliation.Reviewer
							artifact, artifactErr := os.ReadFile(reviewResultPath(reviewer.Snapshot, reviewer.Target))
							inspect := exec.Command("tmux", "display-message", "-p", "-t", "="+reviewer.Session+":0.0", "#{pane_dead}|#{pane_pid}|#{pane_current_command}")
							inspect.Env = environment
							pane, paneErr := inspect.CombinedOutput()
							reviewerArtifact = fmt.Sprintf("path=%s body=%q err=%v session=%s has_session=%t pane=%q pane_err=%v", reviewResultPath(reviewer.Snapshot, reviewer.Target), artifact, artifactErr, reviewer.Session, fullSystemTmuxSessionExists(environment, reviewer.Session), pane, paneErr)
						}
					}
				}
				t.Fatalf("%s browser: %v\n%s\nreviewer artifact=%s\nledger=%s\nserve=%s", action, err, browserOutput, reviewerArtifact, ledger, output.String())
			}
			terminalAction := action
			if action == "review-plan-cancel" {
				terminalAction = "cancel"
			}
			var completed controlReceipt
			if !waitFor(limit, func() bool {
				ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil {
					return false
				}
				for _, receipt := range ledger.ControlReceipts {
					if receipt.Request.Action == terminalAction && receipt.State == "completed" && receipt.Phase == operatorPhaseCompleted && receipt.EffectID != "" && ledger.Effects[receipt.EffectID].State == "completed" {
						completed = receipt
						return true
					}
				}
				return false
			}) {
				ledger, _ := os.ReadFile(filepath.Join(stateRoot, runtimeOwnerStateFile))
				t.Fatalf("%s did not commit durable completion: %s\nserve=%s", action, ledger, output.String())
			}
			if action == "review-plan-cancel" {
				reviewerSession, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
				if err != nil {
					t.Fatal(err)
				}
				if !waitFor(limit, func() bool { return !fullSystemTmuxSessionExists(environment, reviewerSession) }) {
					t.Fatalf("cancel left reviewer tmux session %s active", reviewerSession)
				}
				pidBody, err := os.ReadFile(reviewerPID)
				if err != nil {
					t.Fatalf("reviewer process never started: %v", err)
				}
				pid, err := strconv.Atoi(strings.TrimSpace(string(pidBody)))
				if err != nil || pid < 2 {
					t.Fatalf("invalid reviewer PID %q: %v", pidBody, err)
				}
				if !waitFor(limit, func() bool { return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH) }) {
					t.Fatalf("cancel left reviewer process %d alive", pid)
				}
				gate, err := os.OpenFile(reviewGate, os.O_RDWR|syscall.O_NONBLOCK, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := gate.WriteString("release\n"); err != nil {
					t.Fatal(err)
				}
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
				assertNoCancelledReviewerGitHubStatus(t, fixture)
				if !waitFor(limit, func() bool {
					current, err := readRuntimeOwnerState(stateRoot, "o/r")
					return err == nil && completed.Result != nil && current.CycleOutcomeSource >= completed.Result.OwnerRevision
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Errorf("no reconciliation cycle completed after cancel: outcome source=%d cancel revision=%d diagnostic=%q", current.CycleOutcomeSource, completed.Result.OwnerRevision, current.CycleDiagnostic)
				}
			}
			if err := server.Process.Signal(os.Interrupt); err != nil {
				t.Fatal(err)
			}
			if err := server.Wait(); err != nil {
				t.Fatalf("%s shutdown: %v\n%s", action, err, output.String())
			}
			stopped = true
			ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
			if err != nil {
				t.Fatal(err)
			}
			if action == "review-plan-cancel" && ledger.CycleDiagnostic != "" {
				t.Errorf("cancel left reconciliation unhealthy: %s; effects: %s", ledger.CycleDiagnostic, fullSystemEffectSummary(ledger))
			}
			switch action {
			case "cancel", "review-plan-cancel":
				if ledger.Attempts[key].Manifest.State != "cancelled" || fullSystemTmuxSessionExists(environment, manifest.Session) {
					t.Fatalf("cancel left active runtime: %#v", ledger.Attempts[key])
				}
				for _, path := range []string{manifest.Worktree, manifest.LogPath} {
					if _, err := os.Stat(path); err != nil {
						t.Fatalf("cancel lost retained diagnostics %s: %v", path, err)
					}
				}
				if action == "review-plan-cancel" {
					if ledger.Effects[completed.EffectID].SupersededReviewerID == "" {
						t.Fatal("cancel stop effect lost exact superseded reviewer identity")
					}
					var superseded *controlReceipt
					for i := range ledger.ControlReceipts {
						if ledger.ControlReceipts[i].Request.Action == "review-plan" {
							superseded = &ledger.ControlReceipts[i]
						}
					}
					if superseded == nil || superseded.State != "completed" || superseded.Phase != operatorPhaseCompleted || superseded.EffectID != "" || superseded.Result == nil || superseded.Result.Status != http.StatusConflict {
						t.Fatalf("cancel did not terminalize superseded plan review: %#v", superseded)
					}
					for _, effect := range ledger.Effects {
						if effect.Action == string(reconciliationReviewer) && effect.Issue == 73 && effect.Attempt == 1 {
							t.Fatalf("superseded reviewer effect survived cancel: %#v", effect)
						}
					}
					assertNoCancelledReviewerGitHubStatus(t, fixture)
				}
			case "recover":
				if effect := ledger.Effects[completed.EffectID]; effect.Reconciliation == nil || effect.Reconciliation.GitHubIssueUpdate == nil || effect.Reconciliation.GitHubIssueUpdate.Kind != githubIssueRetry {
					t.Fatalf("recover completed the wrong effect: %#v", effect)
				}
				fixture.mu.Lock()
				comments := append([]map[string]any(nil), fixture.comments...)
				fixture.mu.Unlock()
				if len(comments) < 3 || !strings.Contains(fmt.Sprint(comments[len(comments)-1]["body"]), "retry") {
					t.Fatalf("recover did not durably authorize a retry on GitHub: %#v", comments)
				}
			case "review-plan":
				effect := ledger.Effects[completed.EffectID]
				if effect.Reconciliation == nil || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Mode != "plan-review" || ledger.Attempts[key].Manifest.ReviewState != "clean" {
					t.Fatalf("plan review has no durable clean outcome: effect=%#v attempt=%#v", effect, ledger.Attempts[key])
				}
				if !fullSystemTmuxSessionExists(environment, manifest.Session) {
					t.Fatal("plan review terminated the implementation session")
				}
			}
			restartAddress := freeAddress(t)
			restart := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", restartAddress, "--interval", "200ms")
			restart.Dir, restart.Env = repository, environment
			restartOutput := &synchronizedBuffer{}
			restart.Stdout, restart.Stderr = restartOutput, restartOutput
			if err := restart.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restart.Process.Kill(); _ = restart.Wait() })
			waitHTTP(t, "http://"+restartAddress+"/status.json", limit, restartOutput)
			persisted, err := readRuntimeOwnerState(stateRoot, "o/r")
			if err != nil || persisted.Effects[completed.EffectID].State != "completed" {
				t.Fatalf("%s terminal effect did not survive restart: effect=%#v err=%v serve=%s", action, persisted.Effects[completed.EffectID], err, restartOutput.String())
			}
			found := false
			for _, receipt := range persisted.ControlReceipts {
				found = found || receipt.Request.RequestID == completed.Request.RequestID && receipt.State == "completed"
			}
			if !found {
				t.Fatalf("%s terminal receipt did not survive restart: %#v", action, persisted.ControlReceipts)
			}
			if action == "review-plan" && persisted.Attempts[key].Manifest.ReviewState != "clean" {
				t.Fatalf("clean reviewer outcome did not survive restart: %#v", persisted.Attempts[key])
			}
			if (action == "cancel" || action == "review-plan-cancel") && persisted.Attempts[key].Manifest.State != "cancelled" {
				t.Fatalf("cancelled attempt did not survive restart: %#v", persisted.Attempts[key])
			}
			if action == "review-plan-cancel" {
				assertNoCancelledReviewerGitHubStatus(t, fixture)
				if !waitFor(limit, func() bool {
					response, err := http.Get("http://" + restartAddress + "/status.json")
					if err != nil {
						return false
					}
					defer response.Body.Close()
					var snapshot dashboardStatusSnapshot
					if json.NewDecoder(response.Body).Decode(&snapshot) != nil {
						return false
					}
					for _, status := range snapshot.Statuses {
						if status.Issue == 73 && status.Attempt == 1 {
							for _, session := range status.Sessions {
								if session.Role == agentruntime.SessionRoleReviewer {
									return false
								}
							}
							return status.State == "failed" && status.Retryable
						}
					}
					return false
				}) {
					t.Fatal("restart did not preserve failed/retryable GitHub projection with cancelled local runtime and no reviewer session")
				}
			}
		})
	}
}

func assertNoCancelledReviewerGitHubStatus(t *testing.T, fixture *fullSystemGitHub) {
	t.Helper()
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	for _, comment := range fixture.comments {
		if strings.Contains(fmt.Sprint(comment["body"]), "reviewer wrote after cancellation") {
			t.Fatalf("canceled reviewer posted late GitHub status: %#v", comment)
		}
	}
}

func fullSystemEffectSummary(state runtimeOwnerState) string {
	var rows []string
	for id, effect := range state.Effects {
		requestAction, resultAction, valid := "", "", true
		if effect.Reconciliation != nil {
			requestAction = string(effect.Reconciliation.Action)
			valid = validPersistedReconciliationEffect(state, effect)
		}
		if effect.ReconciliationResult != nil {
			resultAction = string(effect.ReconciliationResult.Action)
		}
		rows = append(rows, fmt.Sprintf("id=%s id_valid=%t action=%s state=%s issue=%d attempt=%d issue_gen=%d/%d attempt_gen=%d/%d epoch=%d revision=%d request=%s result=%s digest_valid=%t payload_valid=%t launched=%t superseded=%s", id, id == runtimeEffectID(effect), effect.Action, effect.State, effect.Issue, effect.Attempt, effect.IssueGeneration, state.IssueGenerations[ownerIssueKey(effect.Repository, effect.Issue)], effect.AttemptGeneration, state.AttemptGenerations[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)], effect.IntentEpoch, effect.IntentRevision, requestAction, resultAction, agentruntime.ValidEffectRequestDigest(effect.RequestDigest), valid, effect.ReviewerLaunched, effect.SupersededReviewerID))
	}
	return strings.Join(rows, "; ")
}
