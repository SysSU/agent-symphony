package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func cleanupControlSocket(t *testing.T, root string) {
	t.Helper()
	t.Cleanup(func() { _ = os.Remove(controlSocketPath(root)) })
}

func TestControlServeHelper(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_CONTROL_SERVE_HELPER") != "1" {
		return
	}
	githubAPI = os.Getenv("AGENT_SYMPHONY_CONTROL_GITHUB_URL")
	githubClient = http.DefaultClient
	var args []string
	if json.Unmarshal([]byte(os.Getenv("AGENT_SYMPHONY_CONTROL_ARGS")), &args) != nil {
		os.Exit(2)
	}
	os.Exit(run(args, io.Discard, io.Discard))
}

func TestControlRequestRequiresExactIdentityAndConfirmation(t *testing.T) {
	valid := controlRequest{Version: 1, RequestID: "request-1", Repository: "o/r", Action: "recover", Issue: 7, Attempt: 2}
	if !validControlRequest(valid, "o/r") {
		t.Fatal("exact recover request was rejected")
	}
	for name, mutate := range map[string]func(*controlRequest){
		"foreign repository": func(r *controlRequest) { r.Repository = "other/repo" },
		"missing attempt":    func(r *controlRequest) { r.Attempt = 0 },
		"unknown action":     func(r *controlRequest) { r.Action = "force-merge" },
		"unsafe request id":  func(r *controlRequest) { r.RequestID = "secret\nvalue" },
		"unexpected confirm": func(r *controlRequest) { r.Confirm = true },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			if validControlRequest(request, "o/r") {
				t.Fatalf("accepted %#v", request)
			}
		})
	}
	for _, action := range []string{"archive", "abandon"} {
		request := valid
		request.Action = action
		if validControlRequest(request, "o/r") {
			t.Fatalf("%s accepted without explicit confirmation", action)
		}
		request.Confirm = true
		if !validControlRequest(request, "o/r") {
			t.Fatalf("confirmed %s was rejected", action)
		}
	}
}

func TestRunningDaemonControlWorksWhileDaemonLockIsHeldAndReportsBusy(t *testing.T) {
	root := t.TempDir()
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 7, 2)
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 7, Attempt: 2, State: "failed", Retryable: true, Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "failed", Current: true}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	lock, err := acquireDaemonLock(filepath.Join(root, "daemon.lock"))
	if err != nil {
		t.Fatal(err)
	}
	defer releaseDaemonLock(lock)
	operationMu := &sync.Mutex{}
	recovered := 0
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", operationMu, func(_ context.Context, issue, attempt int) error {
		if issue != 7 || attempt != 2 {
			t.Fatalf("recovered wrong attempt %d/%d", issue, attempt)
		}
		recovered++
		return nil
	}, nil, nil, nil, false, "canary-dashboard-password")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := startControlServer(ctx, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	request := controlRequest{Version: 1, RequestID: "recover-7-2", Repository: "o/r", Action: "recover", Issue: 7, Attempt: 2}
	result, err := callRunningDaemon(t.Context(), root, request)
	if err != nil || !result.OK || result.Status != http.StatusOK || recovered != 1 {
		t.Fatalf("result=%#v recovered=%d err=%v", result, recovered, err)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "canary-dashboard-password") {
		t.Fatal("dashboard credential escaped into the control result")
	}

	operationMu.Lock()
	request.RequestID = "busy-7-2"
	busy, err := callRunningDaemon(t.Context(), root, request)
	operationMu.Unlock()
	if err != nil || busy.OK || !busy.Retryable || busy.Status != http.StatusServiceUnavailable || recovered != 1 {
		t.Fatalf("busy=%#v recovered=%d err=%v", busy, recovered, err)
	}
	if err := os.Chmod(controlSocketPath(root), 0o666); err != nil {
		t.Fatal(err)
	}
	request.RequestID = "unsafe-socket"
	if _, err := callRunningDaemon(t.Context(), root, request); err == nil || !strings.Contains(err.Error(), "unavailable or unsafe") {
		t.Fatalf("group-accessible socket error=%v", err)
	}
}

func TestCompiledServeProcessAcceptsControlWhileOwningDaemonLock(t *testing.T) {
	root := gitRepository(t)
	runGit(t, root, "config", "user.email", "test@example.invalid")
	runGit(t, root, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("test repository\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "README.md")
	runGit(t, root, "commit", "-m", "initial")
	stateRoot := filepath.Join(root, "runtime")
	cleanupControlSocket(t, stateRoot)
	cfg := config.Default("o/r")
	cfg.Commands.Orchestrator, cfg.Commands.OrchestratorAudit = nil, nil
	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "pr-state.json")
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var githubReads atomic.Int32
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		githubReads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/user":
			_, _ = io.WriteString(w, `{"id":42,"login":"coordinator"}`)
		case "/repos/o/r":
			_, _ = io.WriteString(w, `{"full_name":"o/r","default_branch":"main","permissions":{"pull":true}}`)
		case "/repos/o/r/branches/main":
			_, _ = io.WriteString(w, `{"commit":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)
		default:
			_, _ = io.WriteString(w, `[]`)
		}
	}))
	defer github.Close()
	args := []string{"serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", "127.0.0.1:0", "--interval", "60s"}
	encodedArgs, _ := json.Marshal(args)
	command := exec.Command(os.Args[0], "-test.run=^TestControlServeHelper$")
	command.Dir = root
	command.Env = append(os.Environ(),
		"AGENT_SYMPHONY_CONTROL_SERVE_HELPER=1",
		"AGENT_SYMPHONY_CONTROL_GITHUB_URL="+github.URL,
		"AGENT_SYMPHONY_CONTROL_ARGS="+string(encodedArgs),
		"CODEX_HOME="+t.TempDir(),
	)
	var childOutput bytes.Buffer
	command.Stdout, command.Stderr = &childOutput, &childOutput
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			_ = command.Process.Kill()
			_ = command.Wait()
		}
	})
	deadline := time.Now().Add(10 * time.Second)
	for {
		info, err := os.Lstat(controlSocketPath(stateRoot))
		if err == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if command.ProcessState != nil || time.Now().After(deadline) {
			t.Fatalf("serve did not publish control socket: %v output=%q", err, childOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lock, err := acquireDaemonLock(filepath.Join(stateRoot, "daemon.lock")); err == nil {
		releaseDaemonLock(lock)
		t.Fatal("compiled serve process did not retain the daemon lock")
	}
	var result controlResult
	var err error
	for attempt := 1; ; attempt++ {
		result, err = callRunningDaemon(t.Context(), stateRoot, controlRequest{Version: 1, RequestID: fmt.Sprintf("compiled-reconcile-%d", attempt), Repository: "o/r", Action: "reconcile"})
		if err != nil || !result.Retryable || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if err != nil || !result.OK || result.Status != http.StatusNoContent {
		t.Fatalf("control result=%#v err=%v output=%q", result, err, childOutput.String())
	}
	if githubReads.Load() < 3 {
		t.Fatalf("control reconcile did not reach fake GitHub boundary: reads=%d", githubReads.Load())
	}
	if err := command.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatalf("serve shutdown: %v output=%q", err, childOutput.String())
	}
	stopped = true
}

func TestControlAndDashboardReturnTheSameRefusal(t *testing.T) {
	root := t.TempDir()
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 8, 1)
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 8, Attempt: 1, State: "active", Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "running", Current: true}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, func(context.Context, int, int) error { return nil }, nil, nil, nil, false, "")
	request := controlRequest{Version: 1, RequestID: "ineligible-8-1", Repository: "o/r", Action: "recover", Issue: 8, Attempt: 1}
	control := performDashboardControl(t.Context(), project, request)
	dashboardRequest := httptest.NewRequest(http.MethodPost, "http://localhost/actions/recover?repository=o%2Fr&issue=8&attempt=1", nil)
	dashboardRequest.Host = "localhost"
	dashboardRequest.Header.Set("Origin", "http://localhost")
	dashboard := httptest.NewRecorder()
	project.webHandler().ServeHTTP(dashboard, dashboardRequest)
	if control.Status != dashboard.Code || control.Status != http.StatusConflict || strings.TrimSpace(dashboard.Body.String()) != control.Error {
		t.Fatalf("control=%#v dashboard=%d/%q", control, dashboard.Code, dashboard.Body.String())
	}
}

func TestControlMapsPlanReconcileAndOrchestratorActionsToExistingGuards(t *testing.T) {
	root := t.TempDir()
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	activeSession, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 10, 1)
	blockedSession, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 11, 2)
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 10, Attempt: 1, State: "active", Session: activeSession},
		{Repository: "o/r", Issue: 11, Attempt: 2, State: "blocked", Session: blockedSession},
	}); err != nil {
		t.Fatal(err)
	}
	service := &fakeDashboardOrchestrator{status: orchestratoragent.Status{Enabled: true, State: "running"}}
	reconciles, reviews := 0, 0
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, func(_ context.Context, issue, attempt int) error {
		if issue != 10 || attempt != 1 {
			t.Fatalf("reviewed wrong attempt %d/%d", issue, attempt)
		}
		reviews++
		return nil
	}, func(context.Context) error {
		reconciles++
		return nil
	}, service, false, "")
	requests := []controlRequest{
		{Version: 1, RequestID: "reconcile", Repository: "o/r", Action: "reconcile"},
		{Version: 1, RequestID: "review-plan", Repository: "o/r", Action: "review-plan", Issue: 10, Attempt: 1},
		{Version: 1, RequestID: "investigate", Repository: "o/r", Action: "orchestrator-investigate", Issue: 11, Attempt: 2},
		{Version: 1, RequestID: "recover", Repository: "o/r", Action: "orchestrator-recover"},
		{Version: 1, RequestID: "clear", Repository: "o/r", Action: "orchestrator-clear"},
		{Version: 1, RequestID: "rebuild", Repository: "o/r", Action: "orchestrator-rebuild"},
	}
	for _, request := range requests {
		result := performDashboardControl(t.Context(), project, request)
		if !result.OK {
			t.Fatalf("%s result=%#v", request.Action, result)
		}
	}
	if reconciles != 1 || reviews != 1 || strings.Join(service.actions, ",") != "investigate:11:2,recover,clear,rebuild" {
		t.Fatalf("reconciles=%d reviews=%d orchestrator=%v", reconciles, reviews, service.actions)
	}
}

func TestControlConfirmedCleanupUsesExactDashboardMutation(t *testing.T) {
	for _, test := range []struct {
		action, state, manifestState, reason string
	}{
		{"archive", "completed", "completed", "archived"},
		{"abandon", "orphaned", "failed", "abandoned"},
	} {
		t.Run(test.action, func(t *testing.T) {
			root, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			if err := bindDeployment(root, "o/r"); err != nil {
				t.Fatal(err)
			}
			manifest := writeDashboardManifest(t, root, 12, 1, test.manifestState)
			status := orchestrator.RecoveryStatus{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, State: test.state, Branch: manifest.Branch, Worktree: manifest.Worktree, Session: manifest.Session}
			if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
				t.Fatal(err)
			}
			cleaned := ""
			project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, nil, nil, nil, false, "")
			project.cleanup = func(_ context.Context, action string, got agentruntime.Manifest) error {
				if got.Repository != "o/r" || got.Issue != 12 || got.Attempt != 1 {
					t.Fatalf("cleaned wrong manifest %#v", got)
				}
				cleaned = action
				return nil
			}
			request := controlRequest{Version: 1, RequestID: test.action + "-12-1", Repository: "o/r", Action: test.action, Issue: 12, Attempt: 1, Confirm: true}
			result := performDashboardControl(t.Context(), project, request)
			if !result.OK || cleaned != test.action {
				t.Fatalf("result=%#v cleaned=%q", result, cleaned)
			}
			state, err := project.readState()
			if err != nil || len(state.Hidden) != 1 || state.Hidden[0].Reason != test.reason {
				t.Fatalf("state=%#v err=%v", state, err)
			}
		})
	}
}

func TestControlRestartKeepsDismissIdempotent(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 9, 1)
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 9, Attempt: 1, State: "failed", IssueClosed: true, Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "failed"}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, nil, nil, nil, false, "")
	project.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	request := controlRequest{Version: 1, RequestID: "dismiss-9-1", Repository: "o/r", Action: "dismiss", Issue: 9, Attempt: 1}
	for cycle := 0; cycle < 2; cycle++ {
		ctx, cancel := context.WithCancel(t.Context())
		if err := startControlServer(ctx, project, &bytes.Buffer{}); err != nil {
			t.Fatal(err)
		}
		result, err := callRunningDaemon(t.Context(), root, request)
		if err != nil || !result.OK {
			t.Fatalf("cycle %d result=%#v err=%v", cycle, result, err)
		}
		cancel()
		time.Sleep(10 * time.Millisecond)
	}
	state, err := project.readState()
	if err != nil || len(state.Hidden) != 1 || state.Hidden[0].Reason != "dismissed" {
		t.Fatalf("state=%#v err=%v", state, err)
	}
}

func TestOldDaemonShutdownDoesNotRemoveReplacementControlSocket(t *testing.T) {
	root := t.TempDir()
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, nil, func(context.Context) error { return nil }, nil, false, "")
	oldContext, stopOld := context.WithCancel(t.Context())
	if err := startControlServer(oldContext, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	newContext, stopNew := context.WithCancel(t.Context())
	defer stopNew()
	if err := startControlServer(newContext, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	stopOld()
	time.Sleep(20 * time.Millisecond)
	request := controlRequest{Version: 1, RequestID: "replacement-reconcile", Repository: "o/r", Action: "reconcile"}
	result, err := callRunningDaemon(t.Context(), root, request)
	if err != nil || !result.OK {
		t.Fatalf("replacement result=%#v err=%v", result, err)
	}
}

func TestControlCLIEmitsVersionedResult(t *testing.T) {
	root := t.TempDir()
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, nil, func(context.Context) error { return nil }, nil, false, "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := startControlServer(ctx, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"control", "--repository", "o/r", "--action", "reconcile", "--runtime-state", root, "--request-id", "cli-reconcile-1", "--json"}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	var response envelope
	if json.Unmarshal(stdout.Bytes(), &response) != nil || response.Version != 1 || response.Command != "control" || !response.OK {
		t.Fatalf("response=%q", stdout.String())
	}
}

func TestChatSelectsExactReviewerAndRunningDaemonOrchestrator(t *testing.T) {
	root := t.TempDir()
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 23, 4)
	reviewer, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 23, 4)
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 23, Attempt: 4, State: "active", Session: implementation, Sessions: []orchestrator.AttemptSession{
		{Role: agentruntime.SessionRoleImplementation, Name: implementation, State: "completed"},
		{Role: agentruntime.SessionRoleReviewer, Name: reviewer, State: "running", Mode: agentruntime.ReviewModePlan, Target: "o/r#23 plan sha256:" + strings.Repeat("a", 64), Current: true},
	}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	orchestratorSession := "as-o-o-r"
	service := &fakeDashboardOrchestrator{target: orchestratoragent.AttachTarget{Session: orchestratorSession}}
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, nil, nil, service, false, "")
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := startControlServer(ctx, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	script := "#!/bin/sh\ncase $1 in\n" +
		"display-message) printf '0\\n';;\n" +
		"attach-session) IFS= read -r input; printf '%s:%s\\n' \"$3\" \"$input\";;\n" +
		"*) exit 2;;\nesac\n"
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	for _, test := range []struct {
		role           string
		issue, attempt int
		input, session string
	}{
		{agentruntime.SessionRoleReviewer, 23, 4, "review question", reviewer},
		{"orchestrator", 0, 0, "operator question", orchestratorSession},
	} {
		var stdout bytes.Buffer
		if err := chatExactSession(root, "o/r", test.role, test.issue, test.attempt, time.Second, strings.NewReader(test.input+"\n"), &stdout, &bytes.Buffer{}); err != nil {
			t.Fatalf("%s chat: %v", test.role, err)
		}
		if stdout.String() != "="+test.session+":"+test.input+"\n" {
			t.Fatalf("%s output=%q", test.role, stdout.String())
		}
	}
}
