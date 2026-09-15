package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
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

func ensureFullSystemParkedImplementation(t *testing.T, environment []string, helper string, manifest agentruntime.Manifest) {
	t.Helper()
	if manifest.Version != agentruntime.ManifestVersion2 || manifest.LaunchID == "" || manifest.LaunchToken == "" {
		t.Fatal("parked implementation fixture requires a complete V2 identity")
	}
	args := agentruntime.TmuxNewSessionArgs(manifest.Session, manifest.Worktree, environment)
	args = slices.Insert(args, 7, "-P", "-F", agentruntime.ImplementationPaneFormat)
	args = append(args, helper, "implementation-gate", "tmux", manifest.LogPath, manifest.Worktree, manifest.Session, manifest.LaunchToken, manifest.LaunchID, "--", "/bin/false")
	args = append([]string{"wait-for", "-L", agentruntime.ImplementationGateChannel(manifest.LaunchID), ";"}, args...)
	target := agentruntime.PaneTarget(manifest.Session)
	args = append(args,
		";", "set-option", "-p", "-t", target, "@agent-symphony-launch-token", manifest.LaunchToken,
		";", "set-option", "-w", "-t", target, "remain-on-exit", "on",
		";", "set-option", "-w", "-t", target, "history-limit", "5000",
		";", "set-option", "-p", "-t", target, agentruntime.PaneExitStatusOption, "",
		";", "set-option", "-p", "-t", target, agentruntime.PaneExitSignalOption, "",
	)
	command := exec.Command("tmux", args...)
	command.Env = environment
	created, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("create parked V2 implementation: %v: %s", err, created)
	}
	initial, err := agentruntime.ParseImplementationPane(string(created))
	if err != nil || initial.SessionName != manifest.Session || initial.StartPath != manifest.Worktree || initial.Token != "" {
		t.Fatalf("created parked pane identity=%#v err=%v", initial, err)
	}
	inspect := exec.Command("tmux", "display-message", "-p", "-t", target, agentruntime.ImplementationPaneFormat)
	inspect.Env = environment
	observed, err := inspect.CombinedOutput()
	if err != nil {
		t.Fatalf("inspect parked V2 implementation: %v: %s", err, observed)
	}
	pane, err := agentruntime.ParseImplementationPane(string(observed))
	if err != nil || pane.ServerPID != initial.ServerPID || pane.ServerStart != initial.ServerStart || pane.SessionID != initial.SessionID || pane.PaneID != initial.PaneID {
		t.Fatalf("parked pane changed before binding: initial=%#v observed=%#v err=%v", initial, pane, err)
	}
	binding, err := agentruntime.BindImplementationPane(manifest, manifest.LaunchID, "capture", pane)
	if err != nil {
		t.Fatal(err)
	}
	if err := agentruntime.WriteImplementationBinding(manifest, binding); err != nil {
		t.Fatal(err)
	}
}

func fullSystemControlSnapshot(t *testing.T, labels map[string]bool, closed bool) []map[string]any {
	t.Helper()
	created := time.Date(2026, 9, 9, 12, 0, 0, 0, time.UTC)
	controls := internalgithub.Controls{Dependencies: []int{72}, Completion: "human-review", Closed: closed}
	provenance := []internalgithub.Provenance{
		{Name: "cancelled", Value: "false", Source: "creation", ActorID: 42, CreatedAt: created},
		{Name: "retry", Value: "false", Source: "creation", ActorID: 42, CreatedAt: created},
	}
	if labels["agent-ready"] {
		controls.Ready = true
		provenance = append(provenance, internalgithub.Provenance{Name: "ready", Value: "true", Source: "timeline", EventID: 731, ActorID: 42, CreatedAt: created})
	} else {
		provenance = append(provenance, internalgithub.Provenance{Name: "ready", Value: "false", Source: "creation", ActorID: 42, CreatedAt: created})
	}
	if labels["priority:P1"] {
		controls.Priority = 1
		provenance = append(provenance, internalgithub.Provenance{Name: "priority", Value: "1", Source: "timeline", EventID: 732, ActorID: 42, CreatedAt: created})
	} else {
		provenance = append(provenance, internalgithub.Provenance{Name: "priority", Value: "0", Source: "creation", ActorID: 42, CreatedAt: created})
	}
	if labels["autonomous-merge"] {
		controls.Completion = "autonomous-merge"
		provenance = append(provenance, internalgithub.Provenance{Name: "completion", Value: "autonomous-merge", Source: "timeline", EventID: 733, ActorID: 42, CreatedAt: created})
	} else {
		provenance = append(provenance, internalgithub.Provenance{Name: "completion", Value: "human-review", Source: "creation", ActorID: 42, CreatedAt: created})
	}
	if closed {
		provenance = append(provenance, internalgithub.Provenance{Name: "closed", Value: "true", Source: "timeline", EventID: 734, ActorID: 42, CreatedAt: created})
	} else {
		provenance = append(provenance, internalgithub.Provenance{Name: "closed", Value: "false", Source: "creation", ActorID: 42, CreatedAt: created})
	}
	approval := internalgithub.Approval{}
	comments := []map[string]any{}
	if !controls.Ready {
		approval = internalgithub.Approval{CommentID: 99, ActorID: 42, Body: "/agent-symphony approve", CreatedAt: created.Add(time.Second)}
		comments = append(comments, map[string]any{"id": int64(99), "body": approval.Body, "created_at": approval.CreatedAt.Format(time.RFC3339Nano), "updated_at": approval.CreatedAt.Format(time.RFC3339Nano), "user": map[string]any{"id": 42}})
	}
	snapshot, err := internalgithub.NewSnapshot(controls, fullSystemIssueBody, internalgithub.Anchor{IssueNodeID: "I_73", CreatedAt: created, ChangedAt: created, AuthorID: 42}, approval, provenance, "/agent-symphony approve", func(actor int) bool { return actor == 42 }, func(event internalgithub.Provenance) bool { return slices.Contains(provenance, event) })
	if err != nil {
		t.Fatal(err)
	}
	comments = append(comments, map[string]any{"id": int64(100), "body": internalgithub.SnapshotComment(snapshot), "created_at": created.Format(time.RFC3339Nano), "updated_at": created.Format(time.RFC3339Nano), "user": map[string]any{"id": 42}})
	return comments
}

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
	home, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}
	for _, action := range []string{"cancel", "recover", "review-plan", "review-plan-cancel", "review-plan-archive", "dismiss-overlap", "abandon-overlap", "archive-overlap"} {
		t.Run(action, func(t *testing.T) {
			overlap := strings.HasSuffix(action, "-overlap")
			completedOverlap := action == "dismiss-overlap" || action == "archive-overlap"
			parkedImplementation := action == "cancel" || action == "review-plan-cancel" || overlap
			controlledCycle := overlap
			root, err := os.MkdirTemp(home, ".as-lifecycle-")
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
			manifest.WorkerGeneration, manifest.WorkerProfileDigest = 1, config.WorkerProfileDigest()
			if parkedImplementation {
				manifest.Version = agentruntime.ManifestVersion2
				manifest.LaunchToken, err = agentruntime.NewLaunchToken()
				if err != nil {
					t.Fatal(err)
				}
				manifest.LaunchID, err = agentruntime.NewLaunchToken()
				if err != nil {
					t.Fatal(err)
				}
			}
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
			var completedComment map[string]any
			if completedOverlap {
				marker, err := internalgithub.AttemptMarker(73, 1, manifest.Branch, manifest.ReviewHead, 91, "review")
				if err != nil {
					t.Fatal(err)
				}
				completedComment = map[string]any{"id": 73, "body": marker, "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}}
				comments = []map[string]any{completedComment}
				if action == "archive-overlap" {
					// The older remote head keeps startup reconciliation from retiring the local worktree before Archive.
					older, err := internalgithub.AttemptMarker(73, 1, manifest.Branch, base, 91, "review")
					if err != nil {
						t.Fatal(err)
					}
					comments = []map[string]any{{"id": 73, "body": older, "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}}}
				}
			}
			if action == "abandon-overlap" {
				comments = nil
			}
			labels := map[string]bool{"agent-ready": true, "priority:P1": true, "autonomous-merge": true}
			if action == "abandon-overlap" {
				labels = map[string]bool{}
			}
			if parkedImplementation {
				comments = append(comments, fullSystemControlSnapshot(t, labels, completedOverlap)...)
			}
			fixture := &fullSystemGitHub{base: base, origin: origin, labels: labels, comments: comments, closed: completedOverlap}
			fixture.includeClosedIssue = action == "archive-overlap"
			if completedOverlap {
				head := manifest.ReviewHead
				if action == "archive-overlap" {
					head = base
				}
				fixture.pr = map[string]any{"number": 91, "body": comments[0]["body"], "state": "closed", "merged": true, "merged_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}, "head": map[string]any{"sha": head, "ref": manifest.Branch}, "base": map[string]any{"sha": base}}
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
			if parkedImplementation {
				ensureFullSystemParkedImplementation(t, environment, binary, manifest)
			} else if action != "recover" && !overlap {
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
			if !waitFor(limit, func() bool {
				ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil || len(ledger.Observations[ownerIssueKey("o/r", 73)].IssueUpdates) != 0 {
					return false
				}
				if overlap {
					observation := ledger.Observations[ownerIssueKey("o/r", 73)]
					if observation.ObservationEpoch != ledger.Epoch || observation.OwnerGeneration != ledger.IssueGenerations[ownerIssueKey("o/r", 73)] || !observation.Present || completedOverlap && (!observation.Attempts[key].Present || observation.Attempts[key].Fact.PR != 91 || observation.Attempts[key].Fact.State != "completed" || action == "archive-overlap" && observation.Attempts[key].Fact.HeadSHA != base) {
						return false
					}
				}
				controlsSettled := parkedImplementation
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
					if action == "archive-overlap" {
						fixture.comments = []map[string]any{completedComment}
						fixture.pr["body"] = completedComment["body"]
						fixture.pr["head"] = map[string]any{"sha": manifest.ReviewHead, "ref": manifest.Branch}
					}
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
						t.Fatal("held reconciliation did not read the completed-attempt marker before the browser action")
					}
				}
				if action == "archive-overlap" {
					ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
					if err != nil || ledger.Attempts[key].Generation != 1 || ledger.Tombstones[key].Generation != 0 {
						t.Fatalf("Archive lost the owned attempt before browser action: attempt=%#v tombstone=%#v err=%v", ledger.Attempts[key], ledger.Tombstones[key], err)
					}
					if _, err := os.Lstat(manifest.Worktree); err != nil {
						t.Fatalf("Archive worktree was absent before the browser action: %v", err)
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
					ledger, err := readRuntimeOwnerState(stateRoot, "o/r")
					tombstone := ledger.Tombstones[key]
					if err != nil || tombstone.EffectID == "" || tombstone.Manifest == nil || ledger.Effects[tombstone.EffectID].Action != string(agentruntime.EffectCleanup) || ledger.Effects[tombstone.EffectID].State != "completed" {
						t.Fatalf("Archive did not complete local cleanup effect: tombstone=%#v effect=%#v err=%v", tombstone, ledger.Effects[tombstone.EffectID], err)
					}
					if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("Archive completed but retained worktree %s: %v", manifest.Worktree, err)
					}
					for _, path := range []string{manifest.LogPath, filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")} {
						if _, err := os.Lstat(path); err != nil {
							t.Fatalf("Archive did not retain diagnostic %s: %v", path, err)
						}
					}
				}
				// Model the issue disappearing from the next GitHub list before restart.
				// Replay must use the tombstone, not that now-missing observation.
				fixture.mu.Lock()
				fixture.closed, fixture.includeClosedIssue = true, false
				fixture.mu.Unlock()
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
				beforeReplay, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil {
					t.Fatal(err)
				}
				if beforeReplay.Observations[ownerIssueKey("o/r", 73)].Present {
					t.Fatal("replay fixture did not lose its GitHub issue observation")
				}
				cleanupCount := func(state runtimeOwnerState) int {
					count := 0
					for _, effect := range state.Effects {
						if effect.Action == string(agentruntime.EffectCleanup) && effect.Repository == "o/r" && effect.Issue == 73 && effect.Attempt == 1 {
							count++
						}
					}
					return count
				}
				replayRequest, err := http.NewRequest(http.MethodPost, fmt.Sprintf("http://%s/actions/%s?repository=o%%2Fr&issue=73&attempt=1", restarted, mutation), nil)
				if err != nil {
					t.Fatal(err)
				}
				replayRequest.Header.Set("Origin", "http://"+restarted)
				replayResponse, err := http.DefaultClient.Do(replayRequest)
				if err != nil {
					t.Fatal(err)
				}
				var replay controlResult
				decodeErr := json.NewDecoder(replayResponse.Body).Decode(&replay)
				_ = replayResponse.Body.Close()
				if decodeErr != nil || replayResponse.StatusCode != http.StatusOK || !replay.OK || replay.RequestID == "" {
					t.Fatalf("%s fresh-ID replay after restart: HTTP %d result=%#v decode=%v", mutation, replayResponse.StatusCode, replay, decodeErr)
				}
				afterReplay, err := readRuntimeOwnerState(stateRoot, "o/r")
				if err != nil || !reflect.DeepEqual(afterReplay.Tombstones[key], beforeReplay.Tombstones[key]) || afterReplay.AttemptGenerations[key] != beforeReplay.AttemptGenerations[key] || afterReplay.Attempts[key].Generation != 0 || cleanupCount(afterReplay) != cleanupCount(beforeReplay) {
					t.Fatalf("%s replay changed durable invalidation: err=%v before=%#v after=%#v", mutation, err, beforeReplay, afterReplay)
				}
				if effectID := beforeReplay.Tombstones[key].EffectID; effectID != "" && !reflect.DeepEqual(afterReplay.Effects[effectID], beforeReplay.Effects[effectID]) {
					t.Fatalf("%s replay changed the original cleanup effect: before=%#v after=%#v", mutation, beforeReplay.Effects[effectID], afterReplay.Effects[effectID])
				}
				if receipt, ok := operatorReceiptByID(afterReplay, replay.RequestID); !ok || receipt.State != "completed" || receipt.Result == nil || !receipt.Result.OK {
					t.Fatalf("%s replay did not commit completed receipt: %#v exists=%t", mutation, receipt, ok)
				}
				verifyAbsent(restarted, "replay")
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
					process, psErr := exec.Command("ps", "-o", "pid,ppid,pgid,stat", "-p", strconv.Itoa(pid)).CombinedOutput()
					groupPID, groupErr := syscall.Getpgid(pid)
					ledger, ledgerErr := readRuntimeOwnerState(stateRoot, "o/r")
					stop := ledger.Effects[completed.EffectID]
					reviewer, reviewerFound := ledger.Effects[stop.SupersededReviewerID]
					var reviewerEffects []string
					for id, effect := range ledger.Effects {
						if effect.Repository == "o/r" && effect.Issue == 73 && effect.Attempt == 1 && effect.Action == string(reconciliationReviewer) {
							phase := ""
							if effect.Reconciliation != nil && effect.Reconciliation.Reviewer != nil {
								phase = effect.Reconciliation.Reviewer.Phase
							}
							reviewerEffects = append(reviewerEffects, fmt.Sprintf("id=%s state=%s group=%d phase=%s launched=%t session_requested=%t gate=%t revoked=%t issue_gen=%d attempt_gen=%d", id, effect.State, effect.ReviewerGroupPID, phase, effect.ReviewerLaunched, effect.ReviewerSessionRequested, effect.ReviewerGateProtocol, effect.ReviewerRevoked, effect.IssueGeneration, effect.AttemptGeneration))
						}
					}
					var proofs []string
					for _, proof := range ledger.ReviewerProofs {
						if proof.Repository == "o/r" && proof.Issue == 73 && proof.Attempt == 1 {
							proofs = append(proofs, fmt.Sprintf("effect=%s group=%d issue_gen=%d attempt_gen=%d dead=%t never_ran=%t", proof.EffectID, proof.GroupPID, proof.IssueGeneration, proof.AttemptGeneration, proof.DeadProved, proof.NeverRan))
						}
					}
					reviewSnapshot, _ := reviewIdentity(operatorEffectAttempt(manifest), productionSnapshotRoot(stateRoot))
					launchPath, _ := reviewerLifecyclePaths(reviewSnapshot, stop.SupersededReviewerTarget)
					var launch reviewerLaunchIdentity
					launchFound, launchErr := readReviewerRecord(launchPath, &launch)
					t.Fatalf("cancel completed while TERM/HUP-ignoring reviewer process %d was still alive: %v; ps=%q ps_err=%v getpgid=%d getpgid_err=%v owner_err=%v stop={id:%s state:%s superseded:%s group:%d stopped:%t issue_gen:%d attempt_gen:%d} reviewer={found:%t state:%s group:%d launched:%t session_requested:%t gate:%t revoked:%t issue_gen:%d attempt_gen:%d} reviewer_effects=%q owner_gens=%d/%d proofs=%q launch={found:%t err:%v effect:%s child:%d issue_gen:%d attempt_gen:%d gate:%t session_requested:%t}", pid, err, process, psErr, groupPID, groupErr, ledgerErr, completed.EffectID, stop.State, stop.SupersededReviewerID, stop.SupersededReviewerGroupPID, stop.ReviewerStopped, stop.IssueGeneration, stop.AttemptGeneration, reviewerFound, reviewer.State, reviewer.ReviewerGroupPID, reviewer.ReviewerLaunched, reviewer.ReviewerSessionRequested, reviewer.ReviewerGateProtocol, reviewer.ReviewerRevoked, reviewer.IssueGeneration, reviewer.AttemptGeneration, reviewerEffects, ledger.IssueGenerations[ownerIssueKey("o/r", 73)], ledger.AttemptGenerations[key], proofs, launchFound, launchErr, launch.EffectID, launch.ChildPID, launch.IssueGeneration, launch.AttemptGeneration, launch.GateProtocol, launch.SessionRequested)
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
					latest, latestErr := readRuntimeOwnerState(stateRoot, "o/r")
					serve := restartOutput.String()
					if len(serve) > 8192 {
						serve = serve[len(serve)-8192:]
					}
					t.Fatalf("restart did not preserve cancelled attempt as failed without its reviewer, or show a valid recovery/active next attempt: owner_read=%v effects=%s pending_start=%s receipts=%s\n%s\nserve=%s", latestErr, internalgithub.Redact(fullSystemEffectSummary(latest)), fullSystemPendingStartDiagnostic(latest, stateRoot, environment), internalgithub.Redact(fmt.Sprintf("%#v", latest.ControlReceipts)), internalgithub.Redact(fullSystemLifecycleDiagnostic(restartAddress, stateRoot, fixture)), internalgithub.Redact(serve))
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

func fullSystemPendingStartDiagnostic(state runtimeOwnerState, stateRoot string, environment []string) string {
	redact := func(value string) string { return internalgithub.RedactEnvironment(value, environment) }
	var rows []string
	for id, effect := range state.Effects {
		if effect.Repository != "o/r" || effect.Issue != 73 || effect.Attempt != 2 || effect.Action != string(agentruntime.EffectStart) || effect.State != "pending" {
			continue
		}
		marker := "missing"
		if _, err := os.Lstat(filepath.Join(stateRoot, "runtime-effects", id+".done")); err == nil {
			marker = "present"
		} else if !errors.Is(err, os.ErrNotExist) {
			marker = "error: " + fmt.Sprintf("%.256s", redact(err.Error()))
		}
		session := state.Attempts[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)].Manifest.Session
		pane := "no session"
		if session != "" {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			command := exec.CommandContext(ctx, "tmux", "display-message", "-p", "-t", agentruntime.PaneTarget(session), "#{session_name}|#{session_id}|#{window_id}|#{pane_id}|#{pane_pid}|#{pane_dead}|#{pane_current_command}")
			command.Env = environment
			command.WaitDelay = time.Second
			output, err := command.CombinedOutput()
			probeErr := ctx.Err()
			cancel()
			pane = fmt.Sprintf("output=%.512q error=%.256s context=%.128s", redact(string(output)), redact(fmt.Sprint(err)), redact(fmt.Sprint(probeErr)))
		}
		rows = append(rows, fmt.Sprintf("id=%.64s diagnostic=%.256q marker=%s session=%.128q pane=%s", redact(id), redact(effect.Diagnostic), marker, redact(session), pane))
		if len(rows) == 3 {
			break
		}
	}
	result := redact(strings.Join(rows, "; "))
	if len(result) > 2048 {
		result = result[:2048]
	}
	return result
}

func TestFullSystemPendingStartDiagnostic(t *testing.T) {
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.Mkdir(bin, 0o700); err != nil {
		t.Fatal(err)
	}
	tmux := filepath.Join(bin, "tmux")
	writeExecutable(t, tmux, fmt.Sprintf("#!/bin/sh\nprintf 'pane-probe canary-private-token %%s %s\\n' \"$*\"\n", strings.Repeat("p", 800)))
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	id := strings.Repeat("a", 32)
	state := newRuntimeOwnerState("o/r")
	state.Effects[id] = runtimeEffectIntent{Repository: "o/r", Issue: 73, Attempt: 2, Action: string(agentruntime.EffectStart), State: "pending", Diagnostic: "external completion remains ambiguous canary-private-token " + strings.Repeat("x", 5000)}
	state.Attempts[ownerAttemptKey("o/r", 73, 2)] = runtimeAttemptRecord{Manifest: agentruntime.Manifest{Session: "test-session"}}
	environment := []string{"PATH=" + bin, "GH_TOKEN=canary-private-token"}
	marker := filepath.Join(root, "runtime-effects", id+".done")
	if err := os.MkdirAll(filepath.Dir(marker), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(marker, []byte("proof"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := fullSystemPendingStartDiagnostic(state, root, environment); !strings.Contains(got, "marker=present") || !strings.Contains(got, `diagnostic="external completion remains ambiguous [REDACTED]`) || !strings.Contains(got, "pane-probe [REDACTED] display-message -p -t =test-session:0.0") || strings.Contains(got, "canary-private-token") || len(got) > 2048 {
		t.Fatalf("pending Start failure omitted bounded redacted marker, diagnostic, or pane identity: %s", got)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if got := fullSystemPendingStartDiagnostic(state, root, environment); !strings.Contains(got, "marker=missing") {
		t.Fatalf("pending Start failure reported a missing marker as present: %s", got)
	}
	for _, next := range []string{strings.Repeat("b", 32), strings.Repeat("c", 32)} {
		state.Effects[next] = runtimeEffectIntent{Repository: "o/r", Issue: 73, Attempt: 2, Action: string(agentruntime.EffectStart), State: "pending", Diagnostic: strings.Repeat("x", 5000)}
	}
	if got := fullSystemPendingStartDiagnostic(state, root, environment); len(got) != 2048 || strings.Contains(got, "canary-private-token") {
		t.Fatalf("three pending Starts did not exercise bounded redacted aggregate output: length=%d diagnostic=%s", len(got), got)
	}
	gate := filepath.Join(root, "tmux-gate.fifo")
	if err := syscall.Mkfifo(gate, 0o600); err != nil {
		t.Fatal(err)
	}
	writeExecutable(t, tmux, "#!/bin/sh\nIFS= read -r release < \"$FAKE_TMUX_GATE\"\n")
	state.Effects = map[string]runtimeEffectIntent{id: state.Effects[id]}
	started := time.Now()
	got := fullSystemPendingStartDiagnostic(state, root, append(environment, "FAKE_TMUX_GATE="+gate))
	if !strings.Contains(got, "context=context deadline exceeded") || time.Since(started) > 5*time.Second {
		t.Fatalf("blocked tmux diagnostic did not return on context deadline: elapsed=%s diagnostic=%s", time.Since(started), got)
	}
}
