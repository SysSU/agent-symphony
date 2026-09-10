package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
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
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
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

func TestRunningDaemonControlWorksWhileDaemonLockIsHeldAndQueuesOperations(t *testing.T) {
	root := resolvedTempDir(t)
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
	replayed, err := callRunningDaemon(t.Context(), root, request)
	if err != nil || !replayed.OK || replayed.Status != result.Status || recovered != 1 {
		t.Fatalf("replayed=%#v recovered=%d err=%v", replayed, recovered, err)
	}
	mismatched := request
	mismatched.Action = "review-plan"
	conflict, err := callRunningDaemon(t.Context(), root, mismatched)
	if err != nil || conflict.OK || conflict.Status != http.StatusConflict || recovered != 1 {
		t.Fatalf("mismatched replay=%#v recovered=%d err=%v", conflict, recovered, err)
	}
	encoded, _ := json.Marshal(result)
	if strings.Contains(string(encoded), "canary-dashboard-password") {
		t.Fatal("dashboard credential escaped into the control result")
	}

	operationMu.Lock()
	request.RequestID = "busy-7-2"
	queued := make(chan controlResult, 1)
	queuedErr := make(chan error, 1)
	go func() {
		result, err := callRunningDaemon(t.Context(), root, request)
		queued <- result
		queuedErr <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if recovered != 1 {
		t.Fatalf("recovery ran while reconciliation owned the operation lock: %d", recovered)
	}
	operationMu.Unlock()
	completed, err := <-queued, <-queuedErr
	if err != nil || !completed.OK || completed.Status != http.StatusOK || recovered != 2 {
		t.Fatalf("completed=%#v recovered=%d err=%v", completed, recovered, err)
	}
	replayedRetry, err := callRunningDaemon(t.Context(), root, request)
	if err != nil || !replayedRetry.OK || replayedRetry.Status != http.StatusOK || recovered != 2 {
		t.Fatalf("replayed retry=%#v recovered=%d err=%v", replayedRetry, recovered, err)
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
			t.Fatalf("external issue did not reach compiled served dashboard: statuses=%#v err=%v output=%q", statuses, err, childOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
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

func TestCompiledCLIInvokesEveryGuardedActionOnActualServeProcess(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is unavailable")
	}
	sourceDir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	repositoryRoot, err := os.MkdirTemp("/tmp", "as-251-serve-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(repositoryRoot) })
	repositoryRoot, err = filepath.EvalSymlinks(repositoryRoot)
	if err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command("git", "-C", repositoryRoot, "init", "-q", "-b", "main").CombinedOutput(); err != nil {
		t.Fatalf("git init: %v: %s", err, output)
	}
	runGit(t, repositoryRoot, "config", "user.email", "test@example.invalid")
	runGit(t, repositoryRoot, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(repositoryRoot, "README.md"), []byte("control integration\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repositoryRoot, "add", "README.md")
	runGit(t, repositoryRoot, "commit", "-m", "initial")
	base := runGit(t, repositoryRoot, "rev-parse", "HEAD")
	stateRoot := filepath.Join(repositoryRoot, "runtime")
	cleanupControlSocket(t, stateRoot)
	cfg := config.Default("o/r")
	cfg.Commands.Orchestrator = []string{"sh", "-c", "exec sleep 300"}
	cfg.Commands.OrchestratorAudit = nil
	configPath := filepath.Join(repositoryRoot, config.DefaultPath)
	if err := config.Write(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(repositoryRoot, "pr-state.json")
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
			_, _ = fmt.Fprintf(w, `{"commit":{"sha":%q}}`, base)
		case "/repos/o/r/issues/34":
			_, _ = io.WriteString(w, `{"number":34,"state":"closed"}`)
		case "/repos/o/r/issues", "/repos/o/r/pulls":
			_, _ = io.WriteString(w, `[]`)
		case "/graphql":
			_, _ = io.WriteString(w, `{"data":{"repository":{"issue":null}}}`)
		default:
			http.Error(w, `{"message":"unexpected test endpoint"}`, http.StatusNotFound)
		}
	}))
	defer github.Close()
	binary := filepath.Join(t.TempDir(), "agent-symphony")
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Dir = sourceDir
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build CLI: %v: %s", err, output)
	}
	curl, err := exec.LookPath("curl")
	if err != nil {
		t.Skip("curl is unavailable")
	}
	fakeBin := t.TempDir()
	gh := filepath.Join(fakeBin, "gh")
	ghScript := fmt.Sprintf(`#!/bin/sh
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
if [ "$input" -eq 1 ]; then
  exec %q -sS -i -X "$method" --data-binary @- "$FAKE_GITHUB_URL$endpoint"
fi
exec %q -sS -i -X "$method" "$FAKE_GITHUB_URL$endpoint"
`, curl, curl)
	if err := os.WriteFile(gh, []byte(ghScript), 0o700); err != nil {
		t.Fatal(err)
	}
	password := filepath.Join(repositoryRoot, "dashboard-password")
	if err := os.WriteFile(password, []byte("dashboard-secret-canary\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	server := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", "127.0.0.1:0", "--dashboard-password-file", password, "--interval", "60s")
	server.Dir = repositoryRoot
	server.Env = append(os.Environ(), "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GITHUB_URL="+github.URL, "GH_TOKEN=process-secret-canary", "CODEX_HOME="+t.TempDir())
	var serverOutput bytes.Buffer
	server.Stdout, server.Stderr = &serverOutput, &serverOutput
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	process, _ := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(server.Process.Pid)).Output()
	if strings.Contains(string(process), "process-secret-canary") || strings.Contains(string(process), "dashboard-secret-canary") {
		t.Fatalf("credential appeared in serve process listing: %q", process)
	}
	stopped := false
	tmuxEnv := append(os.Environ(), "TMUX_TMPDIR="+projectTmuxRoot(stateRoot))
	sessions := []string{orchestratoragent.Session("o/r")}
	t.Cleanup(func() {
		if !stopped {
			_ = server.Process.Kill()
			_ = server.Wait()
		}
		for _, session := range sessions {
			command := exec.Command("tmux", "kill-session", "-t", "="+session)
			command.Env = tmuxEnv
			_ = command.Run()
		}
	})
	deadline := time.Now().Add(20 * time.Second)
	for {
		if info, socketErr := os.Lstat(controlSocketPath(stateRoot)); socketErr == nil && info.Mode()&os.ModeSocket != 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("serve did not publish control socket: %q", serverOutput.String())
		}
		time.Sleep(20 * time.Millisecond)
	}
	if lock, err := acquireDaemonLock(filepath.Join(stateRoot, "daemon.lock")); err == nil {
		releaseDaemonLock(lock)
		t.Fatal("serve process did not retain daemon lock")
	}
	invokeControl := func(name, requestID string, issue int, confirm bool) (string, error) {
		t.Helper()
		args := []string{"control", "--repository", "o/r", "--runtime-state", stateRoot, "--action", name, "--request-id", requestID, "--timeout", "2m", "--json"}
		if issue > 0 {
			args = append(args, "--issue", strconv.Itoa(issue), "--attempt", "1")
		}
		if confirm {
			args = append(args, "--confirm")
		}
		command := exec.Command(binary, args...)
		command.Env = append(os.Environ(), "GITHUB_TOKEN=process-secret-canary")
		var output bytes.Buffer
		command.Stdout, command.Stderr = &output, &output
		err := command.Run()
		if strings.Contains(output.String(), "process-secret-canary") || strings.Contains(output.String(), "dashboard-secret-canary") {
			t.Fatalf("%s output=%q", name, output.String())
		}
		return output.String(), err
	}
	for attempt := 0; ; attempt++ {
		output, err := invokeControl("reconcile", "serve-reconcile-"+strconv.Itoa(attempt), 0, false)
		if err == nil && strings.Contains(output, `"ok":true`) {
			break
		}
		if attempt == 20 || !strings.Contains(output, `"retryable":true`) {
			t.Fatalf("serve never completed initial reconciliation: err=%v output=%s", err, output)
		}
		time.Sleep(50 * time.Millisecond)
	}
	completed := writeDashboardManifest(t, stateRoot, 32, 1, "completed")
	orphaned := writeDashboardManifest(t, stateRoot, 33, 1, "failed")
	for index := range []agentruntime.Manifest{completed, orphaned} {
		manifest := &completed
		if index == 1 {
			manifest = &orphaned
		}
		if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"init", "-b", manifest.Branch}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"commit", "--allow-empty", "-m", "attempt"}} {
			runGit(t, manifest.Worktree, args...)
		}
		if manifest.State == "completed" {
			manifest.ReviewHead = runGit(t, manifest.Worktree, "rev-parse", "HEAD")
			body, _ := json.Marshal(manifest)
			manifestPath := filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier("o/r"), fmt.Sprintf("%d-1", manifest.Issue), "manifest.json")
			if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
				t.Fatal(err)
			}
		}
		sessions = append(sessions, manifest.Session)
		command := exec.Command("tmux", "new-session", "-d", "-s", manifest.Session, "-c", manifest.Worktree)
		command.Env = tmuxEnv
		if output, err := command.CombinedOutput(); err != nil {
			t.Fatalf("start cleanup session: %v: %s", err, output)
		}
	}
	status := func(issue, attempt int, state string) orchestrator.RecoveryStatus {
		session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", issue, attempt)
		return orchestrator.RecoveryStatus{Repository: "o/r", Issue: issue, Attempt: attempt, State: state, Session: session}
	}
	recoverable := status(30, 1, "failed")
	recoverable.Retryable = true
	active := status(31, 1, "active")
	archive := status(32, 1, "completed")
	archive.Branch, archive.Worktree, archive.Session = completed.Branch, completed.Worktree, completed.Session
	abandon := status(33, 1, "orphaned")
	abandon.Branch, abandon.Worktree, abandon.Session = orphaned.Branch, orphaned.Worktree, orphaned.Session
	dismiss := status(34, 1, "failed")
	dismiss.IssueClosed = true
	investigate := status(35, 1, "blocked")
	statuses := []orchestrator.RecoveryStatus{recoverable, active, archive, abandon, dismiss, investigate}
	if err := writeStatusSnapshot(stateRoot, statuses); err != nil {
		t.Fatal(err)
	}
	type action struct {
		name    string
		issue   int
		confirm bool
		ok      bool
	}
	actions := []action{
		{name: "dismiss", issue: 34, ok: true},
		{name: "archive", issue: 32, confirm: true, ok: true},
		{name: "abandon", issue: 33, confirm: true, ok: true},
		{name: "orchestrator-recover", ok: true},
		{name: "orchestrator-clear", ok: true},
		{name: "orchestrator-rebuild", ok: true},
		{name: "recover", issue: 30},
		{name: "review-plan", issue: 31},
		{name: "orchestrator-investigate", issue: 35},
	}
	for _, action := range actions {
		if !action.ok {
			if err := writeStatusSnapshot(stateRoot, statuses); err != nil {
				t.Fatal(err)
			}
		}
		output, err := invokeControl(action.name, "serve-"+action.name, action.issue, action.confirm)
		if action.ok && err != nil || !action.ok && err == nil {
			t.Fatalf("%s: err=%v output=%s", action.name, err, output)
		}
		want := `"ok":true`
		if !action.ok {
			want = `"ok":false`
		}
		if !strings.Contains(output, want) {
			t.Fatalf("%s output=%q", action.name, output)
		}
	}
	if githubReads.Load() < 4 {
		t.Fatalf("serve controls did not reach fake GitHub boundary: reads=%d", githubReads.Load())
	}
	for _, manifest := range []agentruntime.Manifest{completed, orphaned} {
		if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cleanup retained %s: %v", manifest.Worktree, err)
		}
		command := exec.Command("tmux", "has-session", "-t", "="+manifest.Session)
		command.Env = tmuxEnv
		if err := command.Run(); err == nil {
			t.Fatalf("cleanup retained tmux session %s", manifest.Session)
		}
	}
	stored, err := (&agentruntime.Runtime{Root: productionAttemptRoot(stateRoot), StateRoot: stateRoot}).Discover()
	if err != nil || len(stored) != 1 || stored[0].Issue != completed.Issue {
		t.Fatalf("post-cleanup manifests=%#v err=%v", stored, err)
	}
	var dashboard dashboardState
	dashboardBody, err := os.ReadFile(filepath.Join(stateRoot, "dashboard-state.json"))
	if err != nil || json.Unmarshal(dashboardBody, &dashboard) != nil || len(dashboard.Hidden) != 3 {
		t.Fatalf("dashboard cleanup state=%s err=%v", dashboardBody, err)
	}
	orchestratorSession := exec.Command("tmux", "has-session", "-t", "="+orchestratoragent.Session("o/r"))
	orchestratorSession.Env = tmuxEnv
	if output, err := orchestratorSession.CombinedOutput(); err != nil {
		t.Fatalf("orchestrator session was not live after rebuild: %v: %s", err, output)
	}
	if err := server.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("control process shutdown: %v output=%q", err, serverOutput.String())
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
	githubReads := 0
	request := controlRequest{Version: 1, RequestID: "dismiss-9-1", Repository: "o/r", Action: "dismiss", Issue: 9, Attempt: 1}
	for cycle := 0; cycle < 2; cycle++ {
		project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, nil, nil, nil, false, "")
		project.issueClosed = func(context.Context, string, int) (bool, error) { githubReads++; return true, nil }
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
	project := &dashboardServer{stateRoot: root, repository: "o/r"}
	state, err := project.readState()
	if err != nil || len(state.Hidden) != 1 || state.Hidden[0].Reason != "dismissed" || githubReads != 1 {
		t.Fatalf("state=%#v github_reads=%d err=%v", state, githubReads, err)
	}
}

func TestControlReceiptCapacityAndPendingOutcomeRefuseReplay(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	state := controlReceiptState{Version: controlVersion, Receipts: make([]controlReceipt, maxControlReceipts)}
	for index := range state.Receipts {
		state.Receipts[index] = controlReceipt{Request: controlRequest{Version: 1, RequestID: fmt.Sprintf("pending-%d", index), Repository: "o/r", Action: "reconcile"}, State: "pending"}
	}
	calls := 0
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", &sync.Mutex{}, nil, nil, func(context.Context) error { calls++; return nil }, nil, false, "")
	if err := project.writeControlReceipts(state); err != nil {
		t.Fatal(err)
	}
	pending := project.performRecordedControl(t.Context(), state.Receipts[0].Request)
	if pending.OK || pending.Status != http.StatusConflict || calls != 0 || !strings.Contains(pending.Error, "replay was refused") {
		t.Fatalf("pending=%#v calls=%d", pending, calls)
	}
	full := project.performRecordedControl(t.Context(), controlRequest{Version: 1, RequestID: "new-request", Repository: "o/r", Action: "reconcile"})
	if full.OK || !full.Retryable || full.Status != http.StatusServiceUnavailable || calls != 0 {
		t.Fatalf("full=%#v calls=%d", full, calls)
	}
	if err := os.Chmod(filepath.Join(root, controlReceiptsFile), 0o644); err != nil {
		t.Fatal(err)
	}
	unsafe := project.performRecordedControl(t.Context(), controlRequest{Version: 1, RequestID: "unsafe-receipts", Repository: "o/r", Action: "reconcile"})
	if unsafe.OK || unsafe.Status != http.StatusInternalServerError || calls != 0 {
		t.Fatalf("unsafe=%#v calls=%d", unsafe, calls)
	}
}

func TestOldDaemonShutdownDoesNotRemoveReplacementControlSocket(t *testing.T) {
	root := resolvedTempDir(t)
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

func TestControlRecoveryQueuesBehindReconciliationAfterRestart(t *testing.T) {
	root := resolvedTempDir(t)
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 9, 1)
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 9, Attempt: 1, State: "failed", Retryable: true, Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "failed", Current: true}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}

	operation := &sync.Mutex{}
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", operation, func(context.Context, int, int) error { return nil }, nil, nil, nil, false, "")
	oldContext, stopOld := context.WithCancel(t.Context())
	if err := startControlServer(oldContext, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	stopOld()
	newContext, stopNew := context.WithCancel(t.Context())
	defer stopNew()
	if err := startControlServer(newContext, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}

	started := make(chan struct{})
	reconcileContext, stopReconcile := context.WithCancel(t.Context())
	reconcileDone := make(chan error, 1)
	var reconciling atomic.Bool
	var overlaps atomic.Int32
	var recoveries atomic.Int32
	go func() {
		reconcileDone <- orchestrator.ReconcileLoop(reconcileContext, time.Millisecond, func(context.Context) error {
			operation.Lock()
			defer operation.Unlock()
			reconciling.Store(true)
			defer reconciling.Store(false)
			select {
			case started <- struct{}{}:
			default:
			}
			time.Sleep(5 * time.Millisecond)
			return nil
		})
	}()
	<-started
	project.recover = func(context.Context, int, int) error {
		recoveries.Add(1)
		if reconciling.Load() {
			overlaps.Add(1)
		}
		return nil
	}

	var stdout, stderr bytes.Buffer
	code := run([]string{"control", "--repository", "o/r", "--action", "recover", "--issue", "9", "--attempt", "1", "--runtime-state", root, "--request-id", "restart-recovery", "--timeout", "250ms", "--json"}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), `"ok":true`) || recoveries.Load() != 1 || overlaps.Load() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stopReconcile()
	if err := <-reconcileDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("reconcile loop: %v", err)
	}
	stopNew()
}

func TestControlCLIEmitsVersionedResult(t *testing.T) {
	root := resolvedTempDir(t)
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

func TestControlCLIBoundsBusyRetryByTimeout(t *testing.T) {
	root := resolvedTempDir(t)
	cleanupControlSocket(t, root)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	operation := &sync.Mutex{}
	operation.Lock()
	defer operation.Unlock()
	project := newProjectDashboardServer(t.Context(), root, "o/r", nil, "tmux", operation, nil, nil, func(context.Context) error { return nil }, nil, false, "")
	ctx, cancel := context.WithCancel(t.Context())
	if err := startControlServer(ctx, project, &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	started := time.Now()
	code := run([]string{"control", "--repository", "o/r", "--action", "reconcile", "--runtime-state", root, "--request-id", "bounded-busy", "--timeout", "25ms"}, &stdout, &stderr)
	cancel()
	time.Sleep(50 * time.Millisecond)
	if code != 1 || time.Since(started) > time.Second || !strings.Contains(stderr.String(), "remained busy until --timeout") {
		t.Fatalf("code=%d elapsed=%s stdout=%q stderr=%q", code, time.Since(started), stdout.String(), stderr.String())
	}
}

func TestChatSelectsExactReviewerAndRunningDaemonOrchestrator(t *testing.T) {
	root := resolvedTempDir(t)
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
