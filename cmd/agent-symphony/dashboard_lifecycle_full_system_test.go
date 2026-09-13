package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
	for _, action := range []string{"cancel", "recover", "review-plan", "review-plan-cancel", "review-plan-archive", "dismiss-overlap", "abandon-overlap", "archive-overlap"} {
		t.Run(action, func(t *testing.T) {
			overlap := strings.HasSuffix(action, "-overlap")
			completedOverlap := action == "dismiss-overlap" || action == "archive-overlap"
			controlledCycle := overlap
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
			if completedOverlap {
				if err := os.WriteFile(filepath.Join(manifest.Worktree, "completed.txt"), []byte("published attempt\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				runExternal(t, manifest.Worktree, "git", "-c", "user.name=Lifecycle fixture", "-c", "user.email=fixture@example.invalid", "add", "completed.txt")
				runExternal(t, manifest.Worktree, "git", "-c", "user.name=Lifecycle fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "completed attempt")
				manifest.State, manifest.ReviewHead = "completed", strings.TrimSpace(runExternal(t, manifest.Worktree, "git", "rev-parse", "HEAD"))
				if err := os.WriteFile(agentruntime.ResultPath(manifest.Worktree), []byte(`{"type":"agent-symphony-result-v1","validation":"completed fixture passed","documentation":"none"}`), 0o600); err != nil {
					t.Fatal(err)
				}
			} else if action == "recover" || overlap {
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
			if completedOverlap {
				marker, err := internalgithub.AttemptMarker(73, 1, manifest.Branch, manifest.ReviewHead, 91, "review")
				if err != nil {
					t.Fatal(err)
				}
				comments = []map[string]any{{"id": 73, "body": marker, "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}}}
			}
			if action == "abandon-overlap" {
				comments = nil
			}
			labels := map[string]bool{"agent-ready": true, "priority:P1": true, "autonomous-merge": true}
			if action == "abandon-overlap" {
				labels = map[string]bool{}
			}
			fixture := &fullSystemGitHub{base: base, origin: origin, labels: labels, comments: comments, closed: completedOverlap}
			if completedOverlap {
				fixture.pr = map[string]any{"number": 91, "body": comments[0]["body"], "state": "closed", "merged": true, "merged_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}, "head": map[string]any{"sha": manifest.ReviewHead, "ref": manifest.Branch}, "base": map[string]any{"sha": base}}
			}
			retryEntered, retryRelease := make(chan struct{}, 1), make(chan struct{})
			var releaseRetry sync.Once
			var holdReconcile atomic.Bool
			var markerExposed, markerObserved atomic.Bool
			var heldIssueListObserved atomic.Bool
			reconcileEntered, reconcileRelease := make(chan struct{}), make(chan struct{})
			var releaseReconcile sync.Once
			var holdRestartReconcile atomic.Bool
			restartReconcileEntered, restartReconcileRelease := make(chan struct{}), make(chan struct{})
			var releaseRestartReconcile sync.Once
			github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if overlap && r.Method == http.MethodGet && r.URL.Path == "/repos/o/r/issues/73/comments" && markerExposed.Load() {
					markerObserved.Store(true)
				}
				if overlap && r.Method == http.MethodGet && r.URL.Path == "/repos/o/r/issues" && holdReconcile.CompareAndSwap(true, false) {
					close(reconcileEntered)
					select {
					case <-reconcileRelease:
					case <-r.Context().Done():
						return
					}
					heldIssueListObserved.Store(true)
				}
				if r.Method == http.MethodGet && r.URL.Path == "/repos/o/r/issues" && holdRestartReconcile.CompareAndSwap(true, false) {
					close(restartReconcileEntered)
					select {
					case <-restartReconcileRelease:
					case <-r.Context().Done():
						return
					}
				}
				if action == "recover" && r.Method == http.MethodPost && r.URL.Path == "/fixture/release-retry" {
					releaseRetry.Do(func() { close(retryRelease) })
					w.WriteHeader(http.StatusNoContent)
					return
				}
				if action == "recover" && r.Method == http.MethodPost && r.URL.Path == "/repos/o/r/issues/73/comments" {
					body, err := io.ReadAll(r.Body)
					if err != nil {
						http.Error(w, err.Error(), http.StatusBadRequest)
						return
					}
					r.Body = io.NopCloser(bytes.NewReader(body))
					if bytes.Contains(body, []byte("/agent-symphony retry")) {
						select {
						case retryEntered <- struct{}{}:
						default:
						}
						select {
						case <-retryRelease:
						case <-r.Context().Done():
							return
						}
					}
				}
				fixture.ServeHTTP(w, r)
			}))
			defer github.Close()
			defer releaseRetry.Do(func() { close(retryRelease) })
			defer releaseReconcile.Do(func() { close(reconcileRelease) })
			defer releaseRestartReconcile.Do(func() { close(restartReconcileRelease) })
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
trap '' TERM HUP
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
			serveInterval := "200ms"
			if controlledCycle {
				cfg.ReconciliationIntervalSeconds = 60
				serveInterval = "60s"
			}
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
			if action != "recover" && !overlap {
				ensureFullSystemTmuxSession(t, environment, manifest.Session, repository)
			}
			server := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--interval", serveInterval)
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
			if controlledCycle {
				request, err := http.NewRequest(http.MethodPost, "http://"+address+"/actions/reconcile", nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Origin", "http://"+address)
				response, err := http.DefaultClient.Do(request)
				if err != nil {
					t.Fatal(err)
				}
				body, readErr := io.ReadAll(response.Body)
				_ = response.Body.Close()
				if readErr != nil || response.StatusCode != http.StatusNoContent {
					t.Fatalf("initial closed-issue reconciliation: HTTP %d, read=%v, body=%s", response.StatusCode, readErr, body)
				}
			}
			if !waitFor(limit, func() bool {
				ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil || len(ledger.Observations[ownerIssueKey("o/r", 73)].IssueUpdates) != 0 {
					return false
				}
				if overlap {
					observation := ledger.Observations[ownerIssueKey("o/r", 73)]
					if observation.ObservationEpoch != ledger.Epoch || observation.OwnerGeneration != ledger.IssueGenerations[ownerIssueKey("o/r", 73)] || !observation.Present || completedOverlap && (!observation.Attempts[key].Present || observation.Attempts[key].Fact.State != "completed" || observation.Attempts[key].Fact.PR != 91) {
						return false
					}
				}
				controlsSettled := false
				for _, effect := range ledger.Effects {
					if effect.Reconciliation != nil && effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueControlSnapshot && effect.State == "completed" {
						controlsSettled = true
					}
				}
				if !overlap && !controlsSettled {
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
						return action == "recover" && status.Retryable || completedOverlap && status.State == "completed" && status.IssueClosed && !status.OperatorBlocked || action == "abandon-overlap" && status.State == "orphaned" && !status.OperatorBlocked || action != "recover" && !overlap && status.State == "active"
					}
				}
				return false
			}) {
				ledger, _ := os.ReadFile(filepath.Join(stateRoot, runtimeOwnerStateFile))
				t.Fatalf("%s button never became available: ledger=%s serve=%s", action, ledger, output.String())
			}
			if action == "recover" {
				select {
				case <-retryEntered:
				case <-time.After(limit):
					t.Fatal("automatic retry never reached the fake-GitHub barrier")
				}
			}
			var blockedCycle <-chan error
			var sourceCycle, sourceStale uint64
			var heldMarkerObserved bool
			if overlap {
				before, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil {
					t.Fatal(err)
				}
				sourceCycle = before.CycleOutcomeID
				sourceStale = before.StaleReconciliations
				if completedOverlap {
					fixture.mu.Lock()
					fixture.includeClosedIssue = true
					fixture.mu.Unlock()
					markerExposed.Store(true)
				}
				holdReconcile.Store(true)
				request, err := http.NewRequest(http.MethodPost, "http://"+address+"/actions/reconcile", nil)
				if err != nil {
					t.Fatal(err)
				}
				request.Header.Set("Origin", "http://"+address)
				done := make(chan error, 1)
				blockedCycle = done
				go func() {
					response, err := http.DefaultClient.Do(request)
					if err != nil {
						done <- err
						return
					}
					body, readErr := io.ReadAll(response.Body)
					_ = response.Body.Close()
					if readErr != nil {
						done <- readErr
					} else if response.StatusCode != http.StatusNoContent {
						done <- fmt.Errorf("reconciliation returned HTTP %d: %s", response.StatusCode, body)
					} else {
						done <- nil
					}
				}()
				select {
				case <-reconcileEntered:
				case err := <-blockedCycle:
					t.Fatalf("reconciliation finished before GitHub read was blocked: %v", err)
				case <-time.After(limit):
					t.Fatal("reconciliation did not enter blocked fake-GitHub read")
				}
				if completedOverlap {
					heldMarkerObserved = markerObserved.Load()
					if !heldMarkerObserved {
						t.Fatal("held reconciliation did not read the completed-attempt marker before Dismiss")
					}
				}
			}
			playwright := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/dashboard-lifecycle-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "playwright"))
			playwright.Dir = source
			playwright.Env = append(os.Environ(), "AGENT_SYMPHONY_LIFECYCLE_E2E_URL=http://"+address, "AGENT_SYMPHONY_LIFECYCLE_E2E_ACTION="+action, "AGENT_SYMPHONY_LIFECYCLE_E2E_FAKE_GITHUB_URL="+github.URL, "AGENT_SYMPHONY_LIFECYCLE_E2E_REVIEW_GATE="+reviewGate, "AGENT_SYMPHONY_LIFECYCLE_E2E_REVIEWER_PID="+reviewerPID, "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(tracing))
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
			if overlap {
				mutation := strings.TrimSuffix(action, "-overlap")
				terminalAction := map[string]string{"dismiss": "dismissed", "abandon": "abandoned", "archive": "archived"}[mutation]
				verifyAbsent := func(address, stage string) {
					verify := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/dashboard-lifecycle-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, stage+"-playwright"))
					verify.Dir = source
					verify.Env = append(os.Environ(), "AGENT_SYMPHONY_LIFECYCLE_E2E_URL=http://"+address, "AGENT_SYMPHONY_LIFECYCLE_E2E_ACTION="+action+"-verify", "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(tracing))
					if browserOutput, err := verify.CombinedOutput(); err != nil {
						t.Fatalf("%s after %s: %v\n%s\n%s", mutation, stage, err, browserOutput, fullSystemLifecycleDiagnostic(address, stateRoot, fixture))
					}
				}
				if !waitFor(limit, func() bool {
					ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
					tombstone := ledger.Tombstones[key]
					if err != nil || tombstone.Action != terminalAction || tombstone.InvalidatedGeneration != 1 || tombstone.Generation <= tombstone.InvalidatedGeneration || ledger.Attempts[key].Generation != 0 || mutation == "dismiss" && tombstone.CleanupPhase != "completed" {
						return false
					}
					for _, receipt := range ledger.ControlReceipts {
						if receipt.Request.Action == mutation && receipt.Request.Issue == 73 && receipt.Request.Attempt == 1 && ((mutation == "abandon" || mutation == "archive") && receipt.State == "pending" && receipt.EffectID == tombstone.EffectID || receipt.State == "completed" && receipt.Result != nil && receipt.Result.OK) {
							return true
						}
					}
					return false
				}) {
					ledger, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("%s did not commit receipt/tombstone while GitHub was blocked: tombstone=%#v receipts=%#v", mutation, ledger.Tombstones[key], ledger.ControlReceipts)
				}
				select {
				case err := <-blockedCycle:
					t.Fatalf("%s only finished after blocked reconciliation escaped early: %v", mutation, err)
				default:
				}
				fixture.mu.Lock()
				if mutation == "abandon" {
					fixture.closed = false
				}
				fixture.comments = []map[string]any{{"id": 73, "body": active, "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}}}
				markerExposed.Store(true)
				fixture.mu.Unlock()
				parsed, err := internalgithub.CollectIssueFactsV2(t.Context(), internalgithub.API{BaseURL: github.URL, HTTP: github.Client(), Retries: -1}, githubPRConfig(cfg, 42), nil)
				if err != nil || len(parsed.Facts) != 1 || parsed.Facts[0].Issue != 73 || parsed.Facts[0].ActiveAttempt == nil || parsed.Facts[0].ActiveAttempt.Attempt != 1 || parsed.Facts[0].ActiveAttempt.BaseSHA != base || parsed.Facts[0].ActiveAttempt.State != "active" {
					t.Fatalf("fake GitHub did not parse an active attempt-1 fact after %s: facts=%#v err=%v", mutation, parsed.Facts, err)
				}
				// For Abandon, the marker is introduced after mutation; ignore the fixture probe.
				if mutation == "abandon" {
					markerObserved.Store(false)
				}
				releaseReconcile.Do(func() { close(reconcileRelease) })
				select {
				case err := <-blockedCycle:
					if err != nil {
						t.Fatalf("stale reconciliation after %s: %v", mutation, err)
					}
				case <-time.After(limit):
					t.Fatalf("stale reconciliation did not finish after %s", mutation)
				}
				if !heldIssueListObserved.Load() || (mutation == "dismiss" || mutation == "archive") && !heldMarkerObserved || mutation == "abandon" && !markerObserved.Load() {
					fixture.mu.Lock()
					requests := append([]string(nil), fixture.requests...)
					fixture.mu.Unlock()
					t.Fatalf("held reconciliation did not consume the expected fake-GitHub response: issue_list=%t attempt_marker=%t requests=%q", heldIssueListObserved.Load(), markerObserved.Load(), requests)
				}
				if !waitFor(limit, func() bool {
					ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
					return err == nil && ledger.CycleOutcomeID > sourceCycle && (mutation != "abandon" || ledger.StaleReconciliations > sourceStale) && ledger.Tombstones[key].Action == terminalAction && ledger.Attempts[key].Generation == 0 && !ledger.Observations[ownerIssueKey("o/r", 73)].Attempts[key].Present
				}) {
					ledger, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("stale reconciliation restored %s attempt or was not rejected: stale=%d source=%d observation=%#v", mutation, ledger.StaleReconciliations, sourceStale, ledger.Observations[ownerIssueKey("o/r", 73)].Attempts[key])
				}
				verifyAbsent(address, "post-cycle")
				if !waitFor(limit, func() bool {
					ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
					if err != nil || ledger.Tombstones[key].CleanupPhase != "completed" {
						return false
					}
					for _, receipt := range ledger.ControlReceipts {
						if receipt.Request.Action == mutation && receipt.Request.Issue == 73 && receipt.Request.Attempt == 1 && receipt.State == "completed" && receipt.Result != nil && receipt.Result.OK {
							return true
						}
					}
					return false
				}) {
					ledger, _ := readRuntimeOwnerState(stateRoot, "o/r")
					tombstone := ledger.Tombstones[key]
					_, worktreeErr := os.Lstat(manifest.Worktree)
					_, logErr := os.Lstat(manifest.LogPath)
					_, manifestErr := os.Lstat(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"))
					_, markerErr := os.Lstat(filepath.Join(stateRoot, "runtime-effects", tombstone.EffectID+".done"))
					t.Fatalf("%s cleanup did not complete after GitHub release: tombstone=%#v receipts=%#v effect=%#v worktree=%v log=%v manifest=%v marker=%v tmux=%t serve=%s", mutation, tombstone, ledger.ControlReceipts, ledger.Effects[tombstone.EffectID], worktreeErr, logErr, manifestErr, markerErr, fullSystemTmuxSessionExists(environment, manifest.Session), output.String())
				}
				if mutation == "archive" {
					if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("Archive completed but retained worktree %s: %v", manifest.Worktree, err)
					}
					for _, path := range []string{manifest.LogPath, filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")} {
						if _, err := os.Lstat(path); err != nil {
							t.Fatalf("Archive did not retain diagnostic %s: %v", path, err)
						}
					}
				}
				if err := server.Process.Signal(os.Interrupt); err != nil {
					t.Fatal(err)
				}
				if err := server.Wait(); err != nil {
					t.Fatalf("%s shutdown: %v", mutation, err)
				}
				stopped = true
				restarted := freeAddress(t)
				restart := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", restarted, "--interval", serveInterval)
				restart.Dir, restart.Env = repository, environment
				restartOutput := &synchronizedBuffer{}
				restart.Stdout, restart.Stderr = restartOutput, restartOutput
				if err := restart.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = restart.Process.Kill(); _ = restart.Wait() })
				waitHTTP(t, "http://"+restarted+"/status.json", limit, restartOutput)
				beforeRestartCycle := fullSystemHeldRestartCycle(t, restarted, stateRoot, &holdRestartReconcile, restartReconcileEntered, func() { releaseRestartReconcile.Do(func() { close(restartReconcileRelease) }) }, limit)
				if !waitFor(limit, func() bool {
					ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
					if err != nil || !fullSystemNewCycleAfter(ledger, beforeRestartCycle) || ledger.Tombstones[key].Action != terminalAction || ledger.Attempts[key].Generation != 0 {
						return false
					}
					response, err := http.Get("http://" + restarted + "/status.json")
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
							return false
						}
					}
					return true
				}) {
					t.Fatalf("restart did not finish a fresh GitHub-backed cycle without restoring %s attempt: %s\n%s", mutation, restartOutput.String(), fullSystemLifecycleDiagnostic(restarted, stateRoot, fixture))
				}
				verifyAbsent(restarted, "restart")
				return
			}
			terminalAction := action
			if action == "review-plan-cancel" {
				terminalAction = "cancel"
			} else if action == "review-plan-archive" {
				terminalAction = "review-plan"
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
			if action == "recover" {
				second, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 73, 2)
				if err != nil {
					t.Fatal(err)
				}
				if !waitFor(limit, func() bool {
					current, err := readRuntimeOwnerState(stateRoot, "o/r")
					recovered, ok := current.Attempts[ownerAttemptKey("o/r", 73, 2)]
					return err == nil && ok && recovered.Manifest.State == "running" && recovered.Manifest.Session == second && fullSystemTmuxSessionExists(environment, second)
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("recover did not start usable attempt 2: %#v\nserve=%s", current.Attempts[ownerAttemptKey("o/r", 73, 2)], output.String())
				}
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
				if err := syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
					t.Fatalf("cancel completed while TERM/HUP-ignoring reviewer process %d was still alive: %v", pid, err)
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
			if action == "review-plan-archive" {
				if ledger, err := readRuntimeOwnerState(stateRoot, "o/r"); err != nil || ledger.Attempts[key].Manifest.ReviewState != "clean" {
					t.Fatalf("archive fixture never completed owner-bound plan review: attempt=%#v err=%v", ledger.Attempts[key], err)
				}
				if err := os.WriteFile(filepath.Join(manifest.Worktree, "reviewed.txt"), []byte("reviewed implementation\n"), 0o600); err != nil {
					t.Fatal(err)
				}
				runExternal(t, manifest.Worktree, "git", "-c", "user.name=Lifecycle fixture", "-c", "user.email=fixture@example.invalid", "add", "reviewed.txt")
				runExternal(t, manifest.Worktree, "git", "-c", "user.name=Lifecycle fixture", "-c", "user.email=fixture@example.invalid", "commit", "-qm", "reviewed implementation")
				reviewedHead := strings.TrimSpace(runExternal(t, manifest.Worktree, "git", "rev-parse", "HEAD"))
				if err := os.WriteFile(agentruntime.ResultPath(manifest.Worktree), []byte(`{"type":"agent-symphony-result-v1","validation":"reviewed fixture passed","documentation":"none"}`), 0o600); err != nil {
					t.Fatal(err)
				}
				target := agentruntime.PaneTarget(manifest.Session)
				for _, args := range [][]string{{"set-option", "-w", "-t", target, "remain-on-exit", "on"}, {"respawn-pane", "-k", "-t", target, "--", binary, "pane-exit-status", "tmux", "--", "true"}} {
					command := exec.Command("tmux", args...)
					command.Env = environment
					if result, err := command.CombinedOutput(); err != nil {
						t.Fatalf("complete reviewed implementation pane: %v: %s", err, result)
					}
				}
				if !waitFor(limit, func() bool {
					current, err := readRuntimeOwnerState(stateRoot, "o/r")
					return err == nil && current.Attempts[key].Manifest.State == "completed"
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("reviewed implementation did not complete: attempt=%#v serve=%s", current.Attempts[key], output.String())
				}
				if !waitFor(limit, func() bool {
					current, err := readRuntimeOwnerState(stateRoot, "o/r")
					if err != nil {
						return false
					}
					for _, effect := range current.Effects {
						if effect.Action == string(reconciliationReviewer) && effect.State == "pending" && effect.ReviewerLaunched && effect.ReviewerGroupPID > 1 && effect.IssueGeneration == current.IssueGenerations[ownerIssueKey("o/r", 73)] && effect.AttemptGeneration == current.AttemptGenerations[key] && effect.Reconciliation != nil && effect.Reconciliation.Reviewer != nil {
							reviewer := effect.Reconciliation.Reviewer
							if reviewer.Mode != agentruntime.ReviewModeImplementation || reviewer.Phase != "run-observe" || reviewer.BaseSHA != manifest.BaseSHA || reviewer.HeadSHA != reviewedHead || reviewer.Target != manifest.BaseSHA+".."+reviewedHead || reviewer.Snapshot == "" || reviewer.Session == "" || !fullSystemTmuxSessionExists(environment, reviewer.Session) {
								continue
							}
							response, err := http.Get("http://" + address + "/status.json")
							if err != nil {
								return false
							}
							var snapshot dashboardStatusSnapshot
							err = json.NewDecoder(response.Body).Decode(&snapshot)
							_ = response.Body.Close()
							if err != nil || response.StatusCode != http.StatusOK {
								return false
							}
							for _, status := range snapshot.Statuses {
								if status.Repository == "o/r" && status.Issue == 73 && status.Attempt == 1 {
									for _, session := range status.Sessions {
										if session.Role == agentruntime.SessionRoleReviewer && session.Name == reviewer.Session && session.State == "running" && session.Mode == reviewer.Mode && session.Target == reviewer.Target && session.Current {
											return true
										}
									}
								}
							}
						}
					}
					return false
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					var pending []runtimeEffectIntent
					for _, effect := range current.Effects {
						if effect.Action == string(reconciliationReviewer) && effect.State == "pending" {
							pending = append(pending, effect)
						}
					}
					t.Fatalf("Plan cleanup did not yield an implementation reviewer: attempt=%#v diagnostic=%q pending_reviewer=%#v effects=%s serve=%s", current.Attempts[key], current.CycleDiagnostic, pending, fullSystemEffectSummary(current), output.String())
				}
				gate, err := os.OpenFile(reviewGate, os.O_RDWR|syscall.O_NONBLOCK, 0)
				if err != nil {
					t.Fatal(err)
				}
				if _, err := gate.WriteString("release\n"); err != nil {
					t.Fatal(err)
				}
				if !waitFor(limit, func() bool {
					current, err := readRuntimeOwnerState(stateRoot, "o/r")
					attempt := current.Attempts[key].Manifest
					return err == nil && attempt.ReviewState == "clean" && attempt.ReviewMode == agentruntime.ReviewModeImplementation
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("implementation reviewer did not finish clean: attempt=%#v effects=%s serve=%s", current.Attempts[key], fullSystemEffectSummary(current), output.String())
				}
				if err := gate.Close(); err != nil {
					t.Fatal(err)
				}
				if !waitFor(limit, func() bool {
					current, err := readRuntimeOwnerState(stateRoot, "o/r")
					if err != nil {
						return false
					}
					for _, effect := range current.Effects {
						if effect.Action == string(reconciliationGitHubPublish) && effect.Issue == 73 && effect.Attempt == 1 && effect.State == "completed" {
							return true
						}
					}
					return false
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("reviewed implementation was not published by an owner effect: effects=%s serve=%s", fullSystemEffectSummary(current), output.String())
				}
				if !waitFor(limit, func() bool {
					fixture.mu.Lock()
					defer fixture.mu.Unlock()
					return fixture.merged && fixture.closed
				}) {
					fixture.mu.Lock()
					pr := fixture.pr
					fixture.mu.Unlock()
					t.Fatalf("published reviewed PR was not merged and closed: pr=%#v serve=%s", pr, output.String())
				}
				if !waitFor(limit, func() bool {
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
							return status.State == "completed" && !status.OperatorBlocked
						}
					}
					return false
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("reviewed attempt never became Archive-eligible: observation=%#v cycle=%q serve=%s", current.Observations[ownerIssueKey("o/r", 73)], current.CycleDiagnostic, output.String())
				}
				archive := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/dashboard-lifecycle-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "archive-playwright"))
				archive.Dir = source
				archive.Env = append(os.Environ(), "AGENT_SYMPHONY_LIFECYCLE_E2E_URL=http://"+address, "AGENT_SYMPHONY_LIFECYCLE_E2E_ACTION=review-plan-archive-click", "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(tracing))
				if browserOutput, err := archive.CombinedOutput(); err != nil {
					t.Fatalf("reviewed Archive browser: %v\n%s\nserve=%s", err, browserOutput, output.String())
				}
				if !waitFor(limit, func() bool {
					current, err := readRuntimeOwnerState(stateRoot, "o/r")
					if err != nil || current.Tombstones[key].Action != "archived" || current.Tombstones[key].CleanupPhase != "completed" || current.Attempts[key].Generation != 0 {
						return false
					}
					for _, receipt := range current.ControlReceipts {
						if receipt.Request.Action == "archive" && receipt.Request.Issue == 73 && receipt.Request.Attempt == 1 && receipt.State == "completed" && receipt.Result != nil && receipt.Result.OK && receipt.EffectID == current.Tombstones[key].EffectID && (receipt.EffectID == "" || current.Effects[receipt.EffectID].State == "completed") {
							return true
						}
					}
					return false
				}) {
					current, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("reviewed Archive did not commit: tombstone=%#v receipts=%#v effects=%#v serve=%s", current.Tombstones[key], current.ControlReceipts, current.Effects, output.String())
				}
				snapshotPath, _ := reviewIdentity(agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1}, productionSnapshotRoot(stateRoot))
				current, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil || current.Effects[completed.EffectID].Reconciliation == nil || current.Effects[completed.EffectID].Reconciliation.Reviewer == nil {
					t.Fatalf("reviewer effect identity lost after Archive: effect=%#v err=%v", current.Effects[completed.EffectID], err)
				}
				reviewer := current.Effects[completed.EffectID].Reconciliation.Reviewer
				for _, path := range []string{manifest.Worktree, snapshotPath, reviewResultPath(snapshotPath, reviewer.Target)} {
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("Archive retained runtime resource %s: %v", path, err)
					}
				}
				if fullSystemTmuxSessionExists(environment, manifest.Session) {
					t.Fatalf("Archive retained implementation session %s", manifest.Session)
				}
				if err := server.Process.Signal(os.Interrupt); err != nil {
					t.Fatal(err)
				}
				if err := server.Wait(); err != nil {
					t.Fatalf("reviewed Archive shutdown: %v\n%s", err, output.String())
				}
				stopped = true
				restarted := freeAddress(t)
				restart := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", restarted, "--interval", serveInterval)
				restart.Dir, restart.Env = repository, environment
				restartOutput := &synchronizedBuffer{}
				restart.Stdout, restart.Stderr = restartOutput, restartOutput
				if err := restart.Start(); err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = restart.Process.Kill(); _ = restart.Wait() })
				waitHTTP(t, "http://"+restarted+"/status.json", limit, restartOutput)
				beforeRestartCycle := fullSystemHeldRestartCycle(t, restarted, stateRoot, &holdRestartReconcile, restartReconcileEntered, func() { releaseRestartReconcile.Do(func() { close(restartReconcileRelease) }) }, limit)
				if !waitFor(limit, func() bool {
					persisted, err := readRuntimeOwnerState(stateRoot, "o/r")
					return err == nil && fullSystemNewCycleAfter(persisted, beforeRestartCycle) && persisted.Tombstones[key].Action == "archived" && persisted.Tombstones[key].CleanupPhase == "completed" && persisted.Attempts[key].Generation == 0
				}) {
					persisted, _ := readRuntimeOwnerState(stateRoot, "o/r")
					t.Fatalf("reviewed Archive restart did not finish a fresh GitHub-backed cycle without restoring the attempt: tombstone=%#v attempt=%#v serve=%s\n%s", persisted.Tombstones[key], persisted.Attempts[key], restartOutput.String(), fullSystemLifecycleDiagnostic(restarted, stateRoot, fixture))
				}
				verify := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/dashboard-lifecycle-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "archive-restart-playwright"))
				verify.Dir = source
				verify.Env = append(os.Environ(), "AGENT_SYMPHONY_LIFECYCLE_E2E_URL=http://"+restarted, "AGENT_SYMPHONY_LIFECYCLE_E2E_ACTION=review-plan-archive-verify", "AGENT_SYMPHONY_FULL_SYSTEM_RACE="+strconv.FormatBool(tracing))
				if browserOutput, err := verify.CombinedOutput(); err != nil {
					t.Fatalf("reviewed Archive restart browser: %v\n%s", err, browserOutput)
				}
				return
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
				retries := 0
				for _, comment := range comments {
					if fmt.Sprint(comment["body"]) == "/agent-symphony retry" {
						retries++
					}
				}
				if retries != 1 {
					t.Fatalf("recover expected one durable GitHub retry authorization, got %d: %#v", retries, comments)
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
			if action == "recover" {
				second, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 73, 2)
				recovered, ok := persisted.Attempts[ownerAttemptKey("o/r", 73, 2)]
				if !ok || recovered.Manifest.State != "running" || recovered.Manifest.Session != second || !fullSystemTmuxSessionExists(environment, second) {
					t.Fatalf("recovered attempt 2 was not live after restart: %#v", recovered)
				}
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
						if status.Issue == 73 && status.Attempt == 2 {
							return status.State == "active" && status.Session == second
						}
					}
					return false
				}) {
					t.Fatal("restart did not project recovered attempt 2 as active with the owned live session")
				}
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
					failed, retryable, reviewerGone, replacementActive := false, false, false, false
					for _, status := range snapshot.Statuses {
						if status.Issue != 73 {
							continue
						}
						if status.Attempt == 2 && status.State == "active" {
							replacementActive = status.Session != "" && fullSystemTmuxSessionExists(environment, status.Session)
						}
						if status.Attempt == 1 {
							failed, retryable, reviewerGone = status.State == "failed", status.Retryable, true
							for _, session := range status.Sessions {
								reviewerGone = reviewerGone && session.Role != agentruntime.SessionRoleReviewer
							}
						}
					}
					if replacementActive {
						return failed && reviewerGone && !retryable
					}
					return failed && reviewerGone && retryable
				}) {
					t.Fatal("restart did not preserve cancelled attempt as failed without its reviewer, or show a valid recovery/active next attempt")
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

func fullSystemNewCycleAfter(current, before runtimeOwnerState) bool {
	return current.CycleOutcomeEpoch == current.Epoch && (current.CycleOutcomeEpoch > before.CycleOutcomeEpoch || current.CycleOutcomeEpoch == before.CycleOutcomeEpoch && current.CycleOutcomeID > before.CycleOutcomeID)
}

func fullSystemHeldRestartCycle(t *testing.T, address, stateRoot string, hold *atomic.Bool, entered <-chan struct{}, release func(), limit time.Duration) runtimeOwnerState {
	t.Helper()
	if !waitFor(limit, func() bool {
		state, err := readRuntimeOwnerState(stateRoot, "o/r")
		return err == nil && state.CycleOutcomeEpoch == state.Epoch && state.CycleOutcomeID > 0
	}) {
		t.Fatal("startup reconciliation did not commit before the held restart cycle")
	}
	hold.Store(true)
	defer release()
	completed := make(chan error, 1)
	go func() {
		request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+address+"/actions/reconcile", nil)
		if err != nil {
			completed <- err
			return
		}
		request.Header.Set("Origin", "http://"+address)
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			completed <- err
			return
		}
		defer response.Body.Close()
		body, err := io.ReadAll(response.Body)
		if err == nil && response.StatusCode != http.StatusNoContent {
			err = fmt.Errorf("HTTP %d: %s", response.StatusCode, body)
		}
		completed <- err
	}()
	postFinished := false
	select {
	case <-entered:
	case err := <-completed:
		postFinished = true
		if err != nil {
			t.Fatalf("post-restart reconciliation: %v", err)
		}
		select {
		case <-entered:
		case <-time.After(limit):
			t.Fatal("accepted post-restart reconciliation did not reach the held GitHub issue-list read")
		}
	case <-time.After(limit):
		t.Fatal("post-restart reconciliation did not reach the held GitHub issue-list read")
	}
	before, err := readRuntimeOwnerState(stateRoot, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	release()
	if !postFinished {
		select {
		case err := <-completed:
			if err != nil {
				t.Fatalf("post-restart reconciliation: %v", err)
			}
		case <-time.After(limit):
			t.Fatal("post-restart reconciliation did not finish after GitHub read was released")
		}
	}
	return before
}

func fullSystemLifecycleDiagnostic(address, stateRoot string, fixture *fullSystemGitHub) string {
	state, stateErr := readRuntimeOwnerState(stateRoot, "o/r")
	response, responseErr := http.Get("http://" + address + "/status.json")
	status := ""
	if responseErr == nil {
		body, err := io.ReadAll(response.Body)
		_ = response.Body.Close()
		status = fmt.Sprintf("HTTP %d body=%s read=%v", response.StatusCode, body, err)
	}
	fixture.mu.Lock()
	requests := append([]string(nil), fixture.requests...)
	fixture.mu.Unlock()
	if len(requests) > 20 {
		requests = requests[len(requests)-20:]
	}
	key := ownerAttemptKey("o/r", 73, 1)
	return fmt.Sprintf("owner_read=%v epoch=%d cycle=%d/%d tombstone=%#v attempt=%#v observation=%#v diagnostic=%q status=%s status_read=%v recent_github_requests=%q", stateErr, state.Epoch, state.CycleOutcomeEpoch, state.CycleOutcomeID, state.Tombstones[key], state.Attempts[key], state.Observations[ownerIssueKey("o/r", 73)], state.CycleDiagnostic, status, responseErr, requests)
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
