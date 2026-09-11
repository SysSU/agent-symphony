package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
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

func resolvedTempDir(t *testing.T) string {
	t.Helper()
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return root
}

type ownerTestFileInfo struct {
	os.FileInfo
	stat syscall.Stat_t
}

func (i ownerTestFileInfo) Sys() any { return &i.stat }

func TestControlServeHelper(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_CONTROL_SERVE_HELPER") == "" {
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
	for _, action := range []string{"archive", "abandon", "remove"} {
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

func TestControlHandlerRejectsInvalidDeadline(t *testing.T) {
	request := controlRequest{Version: 1, RequestID: "deadline", Repository: "o/r", Action: "reconcile"}
	body, _ := json.Marshal(request)
	var calls atomic.Int32
	project := newProjectDashboardServer(t.Context(), t.TempDir(), "o/r", nil, "tmux", func(context.Context) error {
		calls.Add(1)
		return nil
	}, nil, false, "")
	for name, values := range map[string][]string{
		"missing":   nil,
		"malformed": {"later"},
		"unbounded": {strconv.FormatInt(time.Now().Add(3*time.Minute).UnixNano(), 10)},
		"duplicate": {strconv.FormatInt(time.Now().Add(time.Minute).UnixNano(), 10), strconv.FormatInt(time.Now().Add(time.Minute).UnixNano(), 10)},
	} {
		t.Run(name, func(t *testing.T) {
			httpRequest := httptest.NewRequest(http.MethodPost, "http://unix/v1/action", bytes.NewReader(body))
			for _, value := range values {
				httpRequest.Header.Add(controlDeadline, value)
			}
			response := httptest.NewRecorder()
			controlHandler(project).ServeHTTP(response, httpRequest)
			var result controlResult
			if json.Unmarshal(response.Body.Bytes(), &result) != nil || result.Status != http.StatusBadRequest || calls.Load() != 0 {
				t.Fatalf("result=%#v calls=%d body=%q", result, calls.Load(), response.Body.String())
			}
		})
	}
}

func TestControlOwnershipRejectsAnotherLocalIdentity(t *testing.T) {
	info, err := os.Stat(t.TempDir())
	if err != nil || !ownedByCurrentUser(info) {
		t.Fatalf("current owner rejected: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Skip("platform does not expose Unix ownership")
	}
	foreign := *stat
	foreign.Uid = uint32(os.Geteuid() + 1)
	if ownedByCurrentUser(ownerTestFileInfo{FileInfo: info, stat: foreign}) {
		t.Fatal("foreign local identity was accepted")
	}
}

func TestCompiledServeProcessAcceptsControlWhileOwningDaemonLock(t *testing.T) {
	root := gitRepository(t)
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "config", "user.email", "test@example.invalid")
	runGit(t, root, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("test repository\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "README.md")
	runGit(t, root, "commit", "-m", "initial")
	base := runGit(t, root, "rev-parse", "HEAD")
	runGit(t, root, "update-ref", "refs/remotes/origin/main", base)
	stateRoot := filepath.Join(root, "runtime")
	cleanupControlSocket(t, stateRoot)
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	legacyFiles := map[string]any{
		"dashboard-state.json": dashboardState{Version: dashboardStateVersion, Hidden: []dashboardHiddenAttempt{}},
		"removal-state.json":   dashboardRemovalState{Version: removalStateVersion, Intents: []dashboardRemovalIntent{}},
		controlReceiptsFile:    controlReceiptState{Version: controlVersion, Receipts: []controlReceipt{}},
	}
	legacyBytes := make(map[string][]byte, len(legacyFiles))
	for name, value := range legacyFiles {
		writeLegacyStateFixture(t, stateRoot, name, value)
		legacyBytes[name], _ = os.ReadFile(filepath.Join(stateRoot, name))
	}
	if err := writeDashboardStatusSnapshot(stateRoot, dashboardStatusSnapshot{UpdatedAt: time.Unix(1, 0), ReconciliationError: "legacy canary"}); err != nil {
		t.Fatal(err)
	}
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
	var showIssue atomic.Bool
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		githubReads.Add(1)
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/user":
			_, _ = io.WriteString(w, `{"id":42,"login":"coordinator"}`)
		case "/repos/o/r":
			_, _ = io.WriteString(w, `{"full_name":"o/r","default_branch":"main","permissions":{"pull":true}}`)
		case "/repos/o/r/branches/main":
			_, _ = fmt.Fprintf(w, `{"commit":{"sha":%q}}`, base)
		case "/repos/o/r/issues":
			if showIssue.Load() {
				_, _ = io.WriteString(w, `[{"number":9,"title":"Externally visible issue","body":"incomplete contract","state":"open","created_at":"2026-09-09T12:00:00Z"}]`)
			} else {
				_, _ = io.WriteString(w, `[]`)
			}
		case "/repos/o/r/issues/9":
			_, _ = io.WriteString(w, `{"number":9,"node_id":"I_9","title":"Externally visible issue","body":"incomplete contract","state":"open","created_at":"2026-09-09T12:00:00Z","user":{"id":42},"labels":[]}`)
		case "/repos/o/r/issues/9/comments", "/repos/o/r/issues/9/timeline":
			_, _ = io.WriteString(w, `[]`)
		default:
			_, _ = io.WriteString(w, `[]`)
		}
	}))
	defer github.Close()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	dashboardAddress := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", dashboardAddress, "--interval", "200ms"}
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
	readStatuses := func() ([]orchestrator.RecoveryStatus, error) {
		response, err := http.Get("http://" + dashboardAddress + "/status.json")
		if err != nil {
			return nil, err
		}
		defer response.Body.Close()
		var snapshot dashboardStatusSnapshot
		if response.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("dashboard status=%d", response.StatusCode)
		}
		if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
			return nil, err
		}
		return snapshot.Statuses, nil
	}
	for {
		statuses, err := readStatuses()
		if err == nil && len(statuses) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("compiled dashboard did not serve initial projection: statuses=%#v err=%v output=%q", statuses, err, childOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	changedAt := time.Now()
	showIssue.Store(true)
	propagationDeadline := changedAt.Add(2200 * time.Millisecond)
	for {
		statuses, err := readStatuses()
		if err == nil && len(statuses) == 1 && statuses[0].Issue == 9 && statuses[0].Title == "Externally visible issue" {
			if elapsed := time.Since(changedAt); elapsed > 2200*time.Millisecond {
				t.Fatalf("compiled dashboard propagation=%s", elapsed)
			} else {
				t.Logf("compiled serve-loop dashboard propagation=%s", elapsed)
			}
			break
		}
		if time.Now().After(propagationDeadline) {
			ledger, _ := os.ReadFile(filepath.Join(stateRoot, runtimeOwnerStateFile))
			statusFile, _ := os.ReadFile(filepath.Join(stateRoot, "status.json"))
			t.Fatalf("external issue did not reach compiled served dashboard: statuses=%#v err=%v output=%q ledger=%s status=%s", statuses, err, childOutput.String(), ledger, statusFile)
		}
		time.Sleep(20 * time.Millisecond)
	}
	beforeControl, err := readRuntimeOwnerState(stateRoot, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	var result controlResult
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
	afterControl, err := readRuntimeOwnerState(stateRoot, "o/r")
	if err != nil || afterControl.Revision <= beforeControl.Revision || afterControl.CycleOutcomeID <= beforeControl.CycleOutcomeID {
		t.Fatalf("reconcile did not commit through the owner: before=%#v after=%#v err=%v", beforeControl, afterControl, err)
	}
	if result, err := callRunningDaemon(t.Context(), stateRoot, controlRequest{Version: 1, RequestID: "compiled-orchestrator-clear", Repository: "o/r", Action: "orchestrator-clear"}); err != nil || result.OK || result.Status != http.StatusConflict {
		t.Fatalf("disabled orchestrator control result=%#v err=%v", result, err)
	}
	for name, want := range legacyBytes {
		got, readErr := os.ReadFile(filepath.Join(stateRoot, name))
		if readErr != nil || !bytes.Equal(got, want) {
			t.Fatalf("v2 production changed legacy authority %s: err=%v got=%q want=%q", name, readErr, got, want)
		}
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

func TestOldDaemonShutdownDoesNotRemoveReplacementControlSocket(t *testing.T) {
	root := resolvedTempDir(t)
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", func(context.Context) error { return nil }, nil, false, "")
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
	if err != nil || result.OK || result.Status != http.StatusConflict {
		t.Fatalf("replacement result=%#v err=%v", result, err)
	}
}

func TestControlCLIEmitsVersionedResult(t *testing.T) {
	owner, _ := operatorTestOwner(t, 12, "active", false)
	root := owner.stateRoot
	cleanupControlSocket(t, root)
	identity, _ := json.Marshal(deploymentIdentity{Version: deploymentIdentityVersion, Repository: "o/r"})
	if err := writeImmutable(filepath.Join(root, "deployment.json"), append(identity, '\n')); err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	project, err := newProjectDashboardServerV2(t.Context(), root, "o/r", nil, "tmux", service, false, "")
	if err != nil {
		t.Fatal(err)
	}
	project.reconcile = func(context.Context) error { return nil }
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
	owner, _ := operatorTestOwner(t, 12, "active", false)
	root := owner.stateRoot
	cleanupControlSocket(t, root)
	identity, _ := json.Marshal(deploymentIdentity{Version: deploymentIdentityVersion, Repository: "o/r"})
	if err := writeImmutable(filepath.Join(root, "deployment.json"), append(identity, '\n')); err != nil {
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
	operator := operatorTestMutationService(t, owner)
	project, err := newProjectDashboardServerV2(t.Context(), root, "o/r", nil, "tmux", operator, false, "")
	if err != nil {
		t.Fatal(err)
	}
	project.orchestrator = service
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
