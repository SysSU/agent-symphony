package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
	"github.com/coder/websocket"
)

// A projected session name is not proof that a newly-created pane belongs to
// the attempt. Admission must fail before upgrade without touching S2.
func TestDashboardImplementationTerminalRejectsSameNameReplacement(t *testing.T) {
	tmuxBinary, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	root, err := os.MkdirTemp("/tmp", "as311-terminal-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	tmux := filepath.Join(root, "tmux")
	socket := filepath.Join(root, "tmux.sock")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nexec "+strconv.Quote(tmuxBinary)+" -S "+strconv.Quote(socket)+" -f /dev/null \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		output, err := exec.Command(tmux, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("tmux %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	const repository, issue, attempt = "o/r", 23, 2
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, repository, issue, attempt)
	status := orchestrator.RecoveryStatus{Repository: repository, Issue: issue, Attempt: attempt, State: "active", Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "running", Current: true}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	run("new-session", "-d", "-s", "keeper", "cat")
	run("new-session", "-d", "-s", session, "cat")
	first := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), "#{session_id}")
	run("kill-session", "-t", "="+session)
	foreignInput := filepath.Join(root, "foreign-input")
	foreign := filepath.Join(root, "foreign")
	const marker = "FOREIGN_IMPLEMENTATION_S2"
	if err := os.WriteFile(foreign, []byte("#!/bin/sh\nprintf '"+marker+"\\r\\n'\nIFS= read -r line\nprintf '%s\\n' \"$line\" > "+strconv.Quote(foreignInput)+"\nexec cat\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run("new-session", "-d", "-s", session, foreign)
	second := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), "#{session_id}")
	if first == second {
		t.Fatal("replacement did not get a new session ID")
	}
	server := httptest.NewServer(newProjectDashboardHandlerWithOptions(t.Context(), root, repository, nil, tmux, nil, false, ""))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/terminal?repository=o%2Fr&issue=23&attempt=2"
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	connection, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	if connection != nil {
		connection.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusConflict {
		t.Fatalf("foreign implementation pane was not rejected before upgrade: response=%v err=%v", response, err)
	}
	if got := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), "#{session_id}"); got != second {
		t.Fatalf("foreign session changed: got %s, want %s", got, second)
	}
	if _, err := os.Stat(foreignInput); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("operator input reached foreign pane: %v", err)
	}
}

func TestDashboardTerminalOwnerCommitRevocation(t *testing.T) {
	for _, test := range []struct {
		name string
		mode string
	}{
		{name: "invalidating commit closes stream", mode: "invalidate"},
		{name: "unrelated commit keeps stream", mode: "unrelated"},
		{name: "failed persistence keeps stream", mode: "persist-failure"},
	} {
		t.Run(test.name, func(t *testing.T) {
			stateRoot := resolvedTempDir(t)
			manifest := ownerTestManifest(t, stateRoot, 701, 1, "running")
			manifest.Version = agentruntime.ManifestVersion2
			manifest.LaunchToken = strings.Repeat("a", 32)
			manifest.LaunchID = strings.Repeat("b", 32)
			manifest.WorkerGeneration = 1
			manifest.WorkerProfileDigest = config.WorkerProfileDigest()
			manifest.Interactive = true
			brokerPath := agentruntime.TerminalBrokerPath(manifest)
			broker := startTestTerminalBroker(t, stateRoot, brokerPath, "/bin/sh", "-c", `printf 'READY\n'; while IFS= read -r line; do printf 'ECHO:%s\n' "$line"; done`)
			implementation := agentruntime.ImplementationLaunchBinding{Version: 1, Role: "interactive", Token: manifest.LaunchToken, EffectID: manifest.LaunchID, ServerPID: os.Getpid(), ServerStart: 1, SessionName: manifest.Session, SessionID: "$1", PaneID: "%1", PanePID: broker.OuterPID, StartPath: manifest.Worktree, Command: "test terminal broker"}
			state := runtimeEffectInitialState(manifest)
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			record := state.Attempts[key]
			record.ImplementationBinding, record.TerminalBroker = &implementation, &broker
			state.Attempts[key] = record
			var fail atomic.Bool
			owner, err := startTestStateOwner(t, stateRoot, state, func(runtimeOwnerState) error {
				if fail.Load() {
					return errors.New("injected persistence failure")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			dashboard := newProjectDashboardServer(t.Context(), stateRoot, manifest.Repository, nil, "tmux", nil, nil, false, "")
			dashboard.operator = &operatorMutationService{owner: owner}
			permit, err := dashboard.implementationTerminalPermit(mustOwnerSnapshot(t, owner), manifest.Issue, manifest.Attempt)
			if err != nil {
				t.Fatal(err)
			}
			statusOnly := mustOwnerSnapshot(t, owner)
			statusRecord := statusOnly.State.Attempts[key]
			statusRecord.Manifest.WorkerStatus = "needs-attention"
			statusRecord.Manifest.WorkerStatusReason = "operator input requested"
			statusRecord.Manifest.WorkerStatusSeq = 1
			statusRecord.Manifest.WorkerStatusApplied = 1
			statusRecord.Manifest.UpdatedAt = statusRecord.Manifest.UpdatedAt.Add(time.Second)
			statusOnly.State.Attempts[key] = statusRecord
			if !permit.current(statusOnly) {
				t.Fatal("worker status projection revoked an unchanged implementation terminal certificate")
			}
			server := httptest.NewServer(dashboard.webHandler())
			defer server.Close()
			endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/terminal?repository=o%2Fr&issue=701&attempt=1"
			connection, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
			if err != nil {
				t.Fatalf("terminal dial response=%v err=%v", response, err)
			}
			defer connection.CloseNow()
			readUntil := func(marker string) {
				t.Helper()
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				var output strings.Builder
				for !strings.Contains(output.String(), marker) {
					kind, message, readErr := connection.Read(ctx)
					if readErr != nil || kind != websocket.MessageBinary {
						t.Fatalf("read %q: output=%q kind=%v err=%v", marker, output.String(), kind, readErr)
					}
					output.Write(message)
				}
			}
			readUntil("READY")
			snapshot := mustOwnerSnapshot(t, owner)
			switch test.mode {
			case "invalidate":
				_, _, err = owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "dismissed", CleanupPhase: "completed"})
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
				defer cancel()
				if _, _, err := connection.Read(ctx); err == nil {
					t.Fatal("invalidated owner certificate left terminal stream open")
				}
				return
			case "persist-failure":
				fail.Store(true)
			}
			_, err = owner.admitMachineStatus(t.Context(), admitMachineStatusCommand{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, ExpectedStatusSequence: snapshot.State.MachineStatuses[ownerIssueKey(manifest.Repository, manifest.Issue)].Sequence, Source: "worker", SourceID: "terminal-test", SourceSequence: 1, Status: "clear", Reason: "terminal remains current"})
			if test.mode == "persist-failure" && err == nil {
				t.Fatal("injected persistence failure unexpectedly committed")
			}
			if test.mode == "unrelated" && err != nil {
				t.Fatal(err)
			}
			if err := connection.Write(t.Context(), websocket.MessageBinary, []byte("after-commit\n")); err != nil {
				t.Fatalf("current terminal rejected input: %v", err)
			}
			readUntil("ECHO:after-commit")
		})
	}
}

func TestDashboardReviewerTerminalUsesOwnerBoundBroker(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 702, "active", false)
	service := operatorTestMutationService(t, owner)
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	_ = bindLiveReviewerForService(t, owner, reviewer)
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	foreignSocket := filepath.Join(owner.stateRoot, "foreign-tmux.sock")
	foreignInput := filepath.Join(owner.stateRoot, "foreign-reviewer-input")
	foreignScript := filepath.Join(owner.stateRoot, "foreign-reviewer")
	if err := os.WriteFile(foreignScript, []byte("#!/bin/sh\nprintf 'FOREIGN_REVIEWER_S2\\r\\n'\nIFS= read -r line\nprintf '%s\\n' \"$line\" > "+strconv.Quote(foreignInput)+"\nexec cat\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	runForeign := func(args ...string) error {
		return exec.Command(tmux, append([]string{"-S", foreignSocket, "-f", "/dev/null"}, args...)...).Run()
	}
	if err := runForeign("new-session", "-d", "-s", reviewer.Reconciliation.Reviewer.Session, foreignScript); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runForeign("kill-server") })
	snapshot := mustOwnerSnapshot(t, owner)
	proof := snapshot.State.ReviewerProofs[reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, reviewer.Reconciliation.Reviewer.Mode, reviewer.Reconciliation.Reviewer.Target)]
	if proof.TerminalBroker == nil {
		t.Fatal("reviewer proof has no terminal broker certificate")
	}
	if err := agentruntime.ReleaseTerminalBroker(t.Context(), *proof.TerminalBroker); err != nil {
		t.Fatal(err)
	}
	dashboard := newProjectDashboardServer(t.Context(), owner.stateRoot, manifest.Repository, nil, "tmux", nil, nil, false, "")
	dashboard.operator = &operatorMutationService{owner: owner}
	permit, err := dashboard.reviewerTerminalPermit(mustOwnerSnapshot(t, owner), manifest.Issue, manifest.Attempt)
	if err != nil {
		t.Fatalf("reviewer terminal permit: %v proof=%#v effect=%#v", err, proof, snapshot.State.Effects[reviewer.ID])
	}
	diagnosticOnly := stateOwnerSnapshot{State: cloneRuntimeOwnerState(snapshot.State)}
	effect := diagnosticOnly.State.Effects[reviewer.ID]
	effect.Diagnostic = "unrelated reconciliation diagnostic"
	diagnosticOnly.State.Effects[reviewer.ID] = effect
	if !permit.current(diagnosticOnly) {
		t.Fatal("diagnostic-only owner commit revoked the current reviewer terminal")
	}
	profileChanged := stateOwnerSnapshot{State: cloneRuntimeOwnerState(snapshot.State)}
	profileChanged.State.WorkerProfileDigest = strings.Repeat("f", 64)
	if profileChanged.State.WorkerProfileDigest == proof.ProfileDigest {
		profileChanged.State.WorkerProfileDigest = strings.Repeat("e", 64)
	}
	if permit.current(profileChanged) {
		t.Fatal("worker profile change left the reviewer terminal current")
	}
	revoked := stateOwnerSnapshot{State: cloneRuntimeOwnerState(snapshot.State)}
	effect = revoked.State.Effects[reviewer.ID]
	effect.ReviewerRevoked = true
	revoked.State.Effects[reviewer.ID] = effect
	if permit.current(revoked) {
		t.Fatal("reviewer revocation left the reviewer terminal current")
	}
	server := httptest.NewServer(dashboard.webHandler())
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/reviewer/terminal?repository=o%2Fr&issue=702&attempt=1"
	connection, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	if err != nil {
		t.Fatalf("reviewer terminal dial response=%v err=%v", response, err)
	}
	defer connection.CloseNow()
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte("reviewer-input\n")); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	var output strings.Builder
	for !strings.Contains(output.String(), "reviewer-input") {
		kind, message, err := connection.Read(ctx)
		if err != nil || kind != websocket.MessageBinary {
			t.Fatalf("reviewer terminal output=%q kind=%v err=%v", output.String(), kind, err)
		}
		output.Write(message)
	}
	if _, err := os.Stat(foreignInput); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer input reached same-name replacement: %v", err)
	}
	if err := runForeign("has-session", "-t", "="+reviewer.Reconciliation.Reviewer.Session); err != nil {
		t.Fatalf("same-name replacement was not left alive: %v", err)
	}
	if _, err := owner.diagnoseReconciliationEffect(t.Context(), diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*reviewer), Action: reconciliationReviewer, Diagnostic: "informational reviewer diagnostic"}); err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte("after-diagnostic\n")); err != nil {
		t.Fatalf("diagnostic-only commit revoked reviewer terminal: %v", err)
	}
	diagnosticCtx, diagnosticCancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer diagnosticCancel()
	for !strings.Contains(output.String(), "after-diagnostic") {
		kind, message, err := connection.Read(diagnosticCtx)
		if err != nil || kind != websocket.MessageBinary {
			t.Fatalf("reviewer terminal after diagnostic output=%q kind=%v err=%v", output.String(), kind, err)
		}
		output.Write(message)
	}
	current := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(current.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "changed after reviewer terminal admission"
	attempt := expandAttemptFact(current.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	if !mustOwnerSnapshot(t, owner).State.Effects[reviewer.ID].ReviewerRevoked {
		t.Fatal("changed plan input did not revoke the reviewer")
	}
	closed, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	for {
		if _, _, err := connection.Read(closed); err != nil {
			if errors.Is(closed.Err(), context.DeadlineExceeded) {
				t.Fatal("invalidating owner commit left reviewer terminal open")
			}
			break
		}
	}
	rejected, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	if rejected != nil {
		rejected.CloseNow()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusConflict {
		t.Fatalf("revoked reviewer replacement was not rejected before upgrade: response=%v err=%v", response, err)
	}
	if err := runForeign("has-session", "-t", "="+reviewer.Reconciliation.Reviewer.Session); err != nil {
		t.Fatalf("same-name replacement was touched after revocation: %v", err)
	}
}
