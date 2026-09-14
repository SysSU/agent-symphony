package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
	"github.com/coder/websocket"
)

func TestDashboardReviewerTerminalRejectsSameNameReplacement(t *testing.T) {
	root, tmux, run := dashboardTerminalTestTmux(t)

	const issue, attempt = 23, 2
	const repository = "o/r"
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, repository, issue, attempt)
	reviewer, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, repository, issue, attempt)
	status := orchestrator.RecoveryStatus{Repository: repository, Issue: issue, Attempt: attempt, State: "active", Session: implementation, Sessions: []orchestrator.AttemptSession{
		{Role: agentruntime.SessionRoleImplementation, Name: implementation, State: "completed"},
		{Role: agentruntime.SessionRoleReviewer, Name: reviewer, State: "running", Mode: agentruntime.ReviewModePlan, Target: "o/r#23 plan sha256:" + strings.Repeat("a", 64), Current: true},
	}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	if _, err := (&dashboardServer{stateRoot: root}).projectedSession(issue, attempt, agentruntime.SessionRoleReviewer); err != nil {
		t.Fatalf("reviewer was not projected as current: %v", err)
	}

	run("new-session", "-d", "-s", "keeper", "cat")
	run("new-session", "-d", "-s", reviewer, "cat")
	firstID := run("display-message", "-p", "-t", agentruntime.PaneTarget(reviewer), "#{session_id}")
	run("kill-session", "-t", "="+reviewer)

	const readyChannel = "foreign-reviewer-ready"
	foreignInput := filepath.Join(root, "foreign-input")
	foreignScript := filepath.Join(root, "foreign-reviewer")
	script := "#!/bin/sh\nprintf 'FOREIGN_REVIEWER_S2\\r\\n'\n" + strconv.Quote(tmux) + " wait-for -U " + readyChannel + "\nIFS= read -r line\nprintf '%s\\n' \"$line\" > " + strconv.Quote(foreignInput) + "\nprintf 'PANE_RECEIVED:%s\\r\\n' \"$line\"\nexec cat\n"
	if err := os.WriteFile(foreignScript, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	run("wait-for", "-L", readyChannel)
	run("new-session", "-d", "-s", reviewer, foreignScript)
	ready, cancelReady := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelReady()
	if output, err := exec.CommandContext(ready, tmux, "wait-for", "-L", readyChannel).CombinedOutput(); err != nil {
		t.Fatalf("replacement pane did not reach its ready barrier: %v: %s", err, output)
	}
	run("wait-for", "-U", readyChannel)
	secondID := run("display-message", "-p", "-t", agentruntime.PaneTarget(reviewer), "#{session_id}")
	if secondID == firstID || !strings.Contains(run("capture-pane", "-p", "-t", agentruntime.PaneTarget(reviewer)), "FOREIGN_REVIEWER_S2") {
		t.Fatalf("replacement was not a ready, distinct tmux session: S1=%s S2=%s", firstID, secondID)
	}

	assertReviewerTerminalRejectsUnboundPane(t, root, tmux, "FOREIGN_REVIEWER_S2", foreignInput)
	if run("display-message", "-p", "-t", agentruntime.PaneTarget(reviewer), "#{session_id}") != secondID {
		t.Fatal("replacement session was changed while rejecting terminal attachment")
	}
}

func TestDashboardReviewerTerminalRejectsUnboundLegacyV1Pane(t *testing.T) {
	root, tmux, run := dashboardTerminalTestTmux(t)
	const issue, attempt = 23, 2
	const repository = "o/r"
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, repository, issue, attempt)
	reviewer, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, repository, issue, attempt)
	target := "o/r#23 plan sha256:" + strings.Repeat("a", 64)
	status := orchestrator.RecoveryStatus{Repository: repository, Issue: issue, Attempt: attempt, State: "active", Session: implementation, Sessions: []orchestrator.AttemptSession{
		{Role: agentruntime.SessionRoleImplementation, Name: implementation, State: "completed"},
		{Role: agentruntime.SessionRoleReviewer, Name: reviewer, State: "running", Mode: agentruntime.ReviewModePlan, Target: target, Current: true},
	}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Version: 1, Repository: repository, Issue: issue, Attempt: attempt, Session: implementation, ReviewState: "running", ReviewSession: reviewer, ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target}
	manifestDir := filepath.Join(root, "attempts", internalgithub.RepositoryIdentifier(repository), "23-2")
	if err := os.MkdirAll(manifestDir, 0o700); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(manifestDir, "manifest.json"), encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := (&dashboardServer{stateRoot: root}).projectedSession(issue, attempt, agentruntime.SessionRoleReviewer); err != nil {
		t.Fatalf("legacy reviewer was not projected as current: %v", err)
	}

	const marker, readyChannel = "UNBOUND_V1_REVIEWER", "legacy-reviewer-ready"
	foreignInput := filepath.Join(root, "legacy-input")
	foreignScript := filepath.Join(root, "legacy-reviewer")
	script := "#!/bin/sh\nprintf '" + marker + "\\r\\n'\n" + strconv.Quote(tmux) + " wait-for -U " + readyChannel + "\nIFS= read -r line\nprintf '%s\\n' \"$line\" > " + strconv.Quote(foreignInput) + "\nprintf 'PANE_RECEIVED:%s\\r\\n' \"$line\"\nexec cat\n"
	if err := os.WriteFile(foreignScript, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	run("new-session", "-d", "-s", "keeper", "cat")
	run("wait-for", "-L", readyChannel)
	run("new-session", "-d", "-s", reviewer, foreignScript)
	ready, cancelReady := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancelReady()
	if output, err := exec.CommandContext(ready, tmux, "wait-for", "-L", readyChannel).CombinedOutput(); err != nil {
		t.Fatalf("legacy reviewer pane did not reach its ready barrier: %v: %s", err, output)
	}
	run("wait-for", "-U", readyChannel)
	before := run("display-message", "-p", "-t", agentruntime.PaneTarget(reviewer), "#{session_id}")
	if !strings.Contains(run("capture-pane", "-p", "-t", agentruntime.PaneTarget(reviewer)), marker) {
		t.Fatal("legacy reviewer pane was not visibly ready")
	}

	assertReviewerTerminalRejectsUnboundPane(t, root, tmux, marker, foreignInput)
	if after := run("display-message", "-p", "-t", agentruntime.PaneTarget(reviewer), "#{session_id}"); after != before {
		t.Fatalf("legacy reviewer session changed while rejecting attachment: before=%s after=%s", before, after)
	}
}

func TestDashboardReviewerTerminalBindsOwnedPaneAcrossReplacement(t *testing.T) {
	for _, timing := range []string{"before-admission", "between-admission-and-attach", "owner-revoked"} {
		t.Run(timing, func(t *testing.T) {
			root, tmux, run := dashboardTerminalTestTmux(t)
			request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
			owner, snapshot, request := reconciliationEffectTestOwner(t, request)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			identity := ownerReconciliationEffectIdentity(*effect)
			if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
				t.Fatal(err)
			}
			reviewer := request.Reviewer.Session
			ownedInput := filepath.Join(root, "owned-input")
			ownedScript := filepath.Join(root, "review-"+effect.ID)
			body := "#!/bin/sh\nprintf 'OWNED_REVIEWER_S1\\r\\n'\nIFS= read -r line\nprintf '%s\\n' \"$line\" > " + strconv.Quote(ownedInput) + "\nprintf 'OWNED_RECEIVED:%s\\r\\n' \"$line\"\nexec cat\n"
			if err := os.WriteFile(ownedScript, []byte(body), 0o700); err != nil {
				t.Fatal(err)
			}
			run("new-session", "-d", "-s", "keeper", "cat")
			run("new-session", "-d", "-s", reviewer, ownedScript)
			fields := strings.Split(run("display-message", "-p", "-t", agentruntime.PaneTarget(reviewer), reviewerTerminalIdentityFormat), "|")
			if len(fields) != 12 {
				t.Fatalf("reviewer identity fields=%q", fields)
			}
			panePID, _ := strconv.Atoi(fields[6])
			serverPID, _ := strconv.Atoi(fields[7])
			startTime, _ := strconv.ParseUint(fields[8], 10, 64)
			certificate := reviewerTerminalIdentity{ServerPID: serverPID, StartTime: startTime, SessionID: fields[5], Name: fields[9], PaneID: fields[10], PanePID: panePID}
			if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: panePID, Terminal: certificate}); err != nil {
				t.Fatal(err)
			}
			server := newProjectDashboardServer(t.Context(), owner.stateRoot, request.Repository, nil, tmux, nil, nil, false, "")
			server.operator = &operatorMutationService{owner: owner}
			server.capacity = 1
			proof, err := server.reviewerTerminalProof(t.Context(), request.Repository, request.Issue, request.Attempt)
			if err != nil {
				t.Fatalf("current reviewer proof: %v", err)
			}
			if result := run("if-shell", "-F", "-t", certificate.PaneID, reviewerTerminalGuardCondition(proof), "display-message -p GUARD_OK", "display-message -p GUARD_DENIED"); result != "GUARD_OK" {
				t.Fatalf("current reviewer guard=%q condition=%q", result, reviewerTerminalGuardCondition(proof))
			}
			httpServer := httptest.NewServer(server.webHandler())
			defer httpServer.Close()
			endpoint := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/reviewer/terminal?repository=o%2Fr&issue=" + strconv.Itoa(request.Issue) + "&attempt=" + strconv.Itoa(request.Attempt)
			dial := func(ctx context.Context) (*websocket.Conn, *http.Response, error) {
				return websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{httpServer.URL}}})
			}
			connection, response, err := dial(t.Context())
			if err != nil {
				var body []byte
				if response != nil {
					body, _ = io.ReadAll(response.Body)
				}
				t.Fatalf("owned reviewer terminal rejected: response=%v body=%q err=%v", response, body, err)
			}
			readCtx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			var output strings.Builder
			for !strings.Contains(output.String(), "OWNED_REVIEWER_S1") {
				_, message, readErr := connection.Read(readCtx)
				if readErr != nil {
					t.Fatalf("owned reviewer output=%q err=%v", output.String(), readErr)
				}
				output.Write(message)
			}
			if err := connection.Write(readCtx, websocket.MessageBinary, []byte("operator-reaches-owned-pane\n")); err != nil {
				t.Fatal(err)
			}
			for !strings.Contains(output.String(), "OWNED_RECEIVED:operator-reaches-owned-pane") {
				_, message, readErr := connection.Read(readCtx)
				if readErr != nil {
					t.Fatalf("owned reviewer input output=%q err=%v", output.String(), readErr)
				}
				output.Write(message)
			}
			clients := strings.Split(run("list-clients", "-F", reviewerTerminalClientFormat), "|")
			if len(clients) != 6 || clients[3] != certificate.SessionID || clients[5] != certificate.PaneID {
				t.Fatalf("live reviewer client inventory=%q", clients)
			}
			clientPID, err := strconv.Atoi(clients[0])
			if err != nil || clientPID < 2 {
				t.Fatalf("live reviewer client PID=%q err=%v", clients[0], err)
			}
			for _, invalid := range []struct {
				name   string
				change func(*reviewerProcessProof)
			}{
				{"server", func(value *reviewerProcessProof) { value.Terminal.ServerPID++ }},
				{"start", func(value *reviewerProcessProof) { value.Terminal.StartTime++ }},
				{"session", func(value *reviewerProcessProof) { value.Terminal.SessionID = "$99999" }},
				{"name", func(value *reviewerProcessProof) { value.Terminal.Name = "foreign-reviewer" }},
				{"pane", func(value *reviewerProcessProof) { value.Terminal.PaneID = "%99999" }},
			} {
				t.Run("reject-"+invalid.name, func(t *testing.T) {
					changed := proof
					invalid.change(&changed)
					if attached, err := server.reviewerTerminalClientAttached(t.Context(), changed, clientPID); err == nil || attached {
						t.Fatalf("foreign identity passed client inventory: attached=%t err=%v", attached, err)
					}
				})
			}
			if input := readForeignInput(t, ownedInput); input != "operator-reaches-owned-pane\n" {
				t.Fatalf("owned reviewer input=%q", input)
			}
			if timing == "owner-revoked" {
				if _, err := owner.diagnoseReconciliationEffect(t.Context(), diagnoseReconciliationEffectCommand{Identity: identity, Action: request.Action, Diagnostic: "unrelated diagnostic commit"}); err != nil {
					t.Fatal(err)
				}
				if err := connection.Write(readCtx, websocket.MessageBinary, []byte("operator-after-unrelated-commit\n")); err != nil {
					t.Fatalf("unrelated commit closed valid reviewer terminal: %v", err)
				}
				for !strings.Contains(output.String(), "operator-after-unrelated-commit") {
					_, message, readErr := connection.Read(readCtx)
					if readErr != nil {
						t.Fatalf("unrelated commit interrupted valid reviewer terminal: %v", readErr)
					}
					output.Write(message)
				}
				if _, err := owner.proveReviewerDead(t.Context(), proveReviewerDeadCommand{Identity: identity, GroupPID: panePID}); err != nil {
					t.Fatal(err)
				}
				for {
					_, _, readErr := connection.Read(readCtx)
					if readErr != nil {
						if errors.Is(readCtx.Err(), context.DeadlineExceeded) {
							t.Fatal("idle revoked reviewer terminal stayed connected")
						}
						break
					}
				}
				connection.CloseNow()
				cancel()
				if connection, response, err := dial(t.Context()); err == nil || response == nil || response.StatusCode != http.StatusNotFound {
					if connection != nil {
						connection.CloseNow()
					}
					t.Fatalf("revoked reviewer reattached: response=%v err=%v", response, err)
				}
				return
			}
			cancel()
			connection.CloseNow()

			foreignInput := filepath.Join(root, "foreign-input")
			foreignScript := filepath.Join(root, "foreign-reviewer")
			foreignBody := "#!/bin/sh\nprintf 'FOREIGN_REVIEWER_S2\\r\\n'\nIFS= read -r line\nprintf '%s\\n' \"$line\" > " + strconv.Quote(foreignInput) + "\nprintf 'FOREIGN_RECEIVED:%s\\r\\n' \"$line\"\nexec cat\n"
			if err := os.WriteFile(foreignScript, []byte(foreignBody), 0o700); err != nil {
				t.Fatal(err)
			}
			blockedTmux := tmux
			if timing == "between-admission-and-attach" {
				const gate = "reviewer-terminal-attach-gate"
				run("wait-for", "-L", gate)
				blockedTmux = filepath.Join(root, "blocked-tmux")
				wrapper := "#!/bin/sh\nif [ \"$1\" = if-shell ]; then\n" + strconv.Quote(tmux) + " wait-for -U reviewer-terminal-attach-ready\n" + strconv.Quote(tmux) + " wait-for -L " + gate + "\nfi\nexec " + strconv.Quote(tmux) + " \"$@\"\n"
				if err := os.WriteFile(blockedTmux, []byte(wrapper), 0o700); err != nil {
					t.Fatal(err)
				}
				server.tmux = blockedTmux
			}
			if timing == "before-admission" {
				run("kill-session", "-t", "="+reviewer)
				run("new-session", "-d", "-s", reviewer, foreignScript)
			}
			result := make(chan error, 1)
			go func() {
				connection, response, err := dial(t.Context())
				if connection != nil {
					connection.CloseNow()
				}
				if err == nil || response == nil || response.StatusCode != http.StatusConflict {
					result <- errors.New("stale reviewer terminal was not rejected before WebSocket upgrade")
					return
				}
				result <- nil
			}()
			if timing == "between-admission-and-attach" {
				ready, cancelReady := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancelReady()
				if output, err := exec.CommandContext(ready, tmux, "wait-for", "-L", "reviewer-terminal-attach-ready").CombinedOutput(); err != nil {
					t.Fatalf("terminal did not reach guarded attach barrier: %v: %s", err, output)
				}
				run("kill-session", "-t", "="+reviewer)
				run("new-session", "-d", "-s", reviewer, foreignScript)
				run("wait-for", "-U", "reviewer-terminal-attach-gate")
			}
			select {
			case err := <-result:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("stale reviewer terminal did not reject")
			}
			if run("display-message", "-p", "-t", agentruntime.PaneTarget(reviewer), "#{session_id}") == certificate.SessionID {
				t.Fatal("foreign reviewer did not replace certified session")
			}
			if input := readForeignInput(t, foreignInput); input != "" {
				t.Fatalf("foreign reviewer received input: %q", input)
			}
		})
	}
}

func assertReviewerTerminalRejectsUnboundPane(t *testing.T, root, tmux, marker, foreignInput string) {
	t.Helper()
	server := httptest.NewServer(newProjectDashboardHandlerWithOptions(t.Context(), root, "o/r", nil, tmux, nil, false, ""))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/reviewer/terminal?repository=o%2Fr&issue=23&attempt=2"
	connection, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	if err != nil {
		if response == nil || response.StatusCode < 400 || response.StatusCode >= 500 {
			t.Fatalf("reviewer terminal rejection response=%v err=%v", response, err)
		}
	} else {
		defer connection.CloseNow()
		readContext, cancelRead := context.WithTimeout(t.Context(), 3*time.Second)
		defer cancelRead()
		var visible strings.Builder
		inputSent := false
		for {
			_, message, readErr := connection.Read(readContext)
			if readErr != nil {
				if errors.Is(readErr, context.DeadlineExceeded) || errors.Is(readContext.Err(), context.DeadlineExceeded) {
					t.Fatalf("terminal stayed attached to unbound pane; exposed=%t input=%q", strings.Contains(visible.String(), marker), readForeignInput(t, foreignInput))
				}
				break
			}
			visible.Write(message)
			if strings.Contains(visible.String(), marker) && !inputSent {
				if err := connection.Write(readContext, websocket.MessageBinary, []byte("operator-must-not-reach-pane\n")); err != nil {
					t.Fatalf("unbound pane was exposed but input write failed: %v", err)
				}
				inputSent = true
			}
			if strings.Contains(visible.String(), "PANE_RECEIVED") {
				break
			}
		}
		if strings.Contains(visible.String(), marker) {
			t.Fatalf("terminal disclosed unbound pane %s; input_sent=%t received=%t input=%q", marker, inputSent, strings.Contains(visible.String(), "PANE_RECEIVED"), readForeignInput(t, foreignInput))
		}
	}
	if input := readForeignInput(t, foreignInput); input != "" {
		t.Fatalf("operator input reached unbound pane: %q", input)
	}
}

func dashboardTerminalTestTmux(t *testing.T) (string, string, func(...string) string) {
	t.Helper()
	tmuxBinary, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	root, err := os.MkdirTemp("/tmp", "as311-terminal-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(root, "tmux.sock")
	tmux := filepath.Join(root, "tmux")
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
	return root, tmux, run
}

func readForeignInput(t *testing.T, path string) string {
	t.Helper()
	body, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}
