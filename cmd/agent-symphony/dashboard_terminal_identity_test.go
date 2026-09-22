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
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
	"github.com/coder/websocket"
)

// A projected session name is not proof that a newly-created pane belongs to
// the attempt. This must fail before WebSocket upgrade, without touching S2.
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
