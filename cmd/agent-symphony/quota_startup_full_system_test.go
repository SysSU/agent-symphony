package main

import (
	"encoding/json"
	"fmt"
	"io"
	"maps"
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
)

func TestQuotaDegradedStartupFullSystem(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_E2E") != "1" {
		t.Skip("set AGENT_SYMPHONY_FULL_SYSTEM_E2E=1 for the compiled daemon gate")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("curl is unavailable")
	}
	root := resolvedTempDir(t)
	repository := filepath.Join(root, "repository")
	origin := filepath.Join(root, "origin.git")
	runExternal(t, "", "git", "init", "-q", "--bare", origin)
	runExternal(t, "", "git", "init", "-q", "-b", "main", repository)
	runExternal(t, repository, "git", "config", "user.name", "Quota fixture")
	runExternal(t, repository, "git", "config", "user.email", "quota@example.invalid")
	if err := os.WriteFile(filepath.Join(repository, "README.md"), []byte("base\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runExternal(t, repository, "git", "add", "README.md")
	runExternal(t, repository, "git", "commit", "-qm", "base")
	runExternal(t, repository, "git", "remote", "add", "origin", origin)
	runExternal(t, repository, "git", "push", "-q", "-u", "origin", "main")
	base := strings.TrimSpace(runExternal(t, repository, "git", "rev-parse", "HEAD"))

	var quota atomic.Bool
	quota.Store(true)
	quotaResponse := make(chan struct{})
	quotaRejected := make(chan struct{}, 1)
	requests := make(chan string, 128)
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case requests <- r.Method + " " + r.URL.Path:
		default:
		}
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/user" && quota.Load() {
			select {
			case <-quotaResponse:
			case <-r.Context().Done():
				return
			}
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(5*time.Second).Unix(), 10))
			w.WriteHeader(http.StatusForbidden)
			_, _ = io.WriteString(w, `{"message":"API rate limit exceeded"}`)
			quotaRejected <- struct{}{}
			return
		}
		w.Header().Set("X-RateLimit-Remaining", "4000")
		w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(time.Now().Add(time.Hour).Unix(), 10))
		switch r.URL.Path {
		case "/user":
			_, _ = io.WriteString(w, `{"id":42,"login":"coordinator"}`)
		case "/repos/o/r":
			_, _ = io.WriteString(w, `{"full_name":"o/r","default_branch":"main","permissions":{"pull":true}}`)
		case "/repos/o/r/branches/main":
			_, _ = fmt.Fprintf(w, `{"commit":{"sha":%q}}`, base)
		default:
			_, _ = io.WriteString(w, `[]`)
		}
	}))
	defer func() {
		select {
		case <-quotaResponse:
		default:
			close(quotaResponse)
		}
		github.Close()
	}()

	binDir := filepath.Join(root, "bin")
	if err := os.Mkdir(binDir, 0o700); err != nil {
		t.Fatal(err)
	}
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
	buildNativeCodexFixture(t, filepath.Join(binDir, "codex"), `#!/bin/sh
if [ "$1" = --version ]; then printf '%s\n' 'codex-cli 0.153.4'; exit 0; fi
exit 0
`)
	configPath := filepath.Join(repository, config.DefaultPath)
	cfg := config.Default("o/r")
	cfg.Commands.Orchestrator, cfg.Commands.OrchestratorAudit = nil, nil
	if err := config.Write(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	statePath, stateRoot := filepath.Join(repository, "pr-state.json"), filepath.Join(root, "runtime")
	passwordPath := filepath.Join(root, "dashboard-password")
	if err := os.WriteFile(passwordPath, []byte("quota-fixture-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	source, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(binDir, "agent-symphony")
	buildArgs := []string{"build", "-o", binary, "."}
	if os.Getenv("AGENT_SYMPHONY_FULL_SYSTEM_RACE") == "1" {
		buildArgs = []string{"build", "-race", "-o", binary, "."}
	}
	runExternal(t, source, "go", buildArgs...)
	address := freeAddress(t)
	server := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", address, "--dashboard-password-file", passwordPath, "--disable-periodic-reconciliation")
	server.Dir = repository
	server.Env = append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_GITHUB_URL="+github.URL, "CODEX_HOME="+filepath.Join(root, "codex-home"), "TMUX_TMPDIR="+filepath.Join(root, "tmux"))
	output := &synchronizedBuffer{}
	server.Stdout, server.Stderr = output, output
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	t.Cleanup(func() {
		if !stopped {
			if err := stopFullSystemDaemon(server); err != nil {
				t.Errorf("stop quota fixture: %v", err)
			}
		}
	})
	select {
	case got := <-requests:
		if got != "GET /user" {
			t.Fatalf("first GitHub request = %q", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("auth request missing: %s", output.String())
	}
	dashboardRequest := func(method, path string, authenticated bool) (*http.Response, error) {
		request, err := http.NewRequest(method, "http://"+address+path, nil)
		if err != nil {
			return nil, err
		}
		request.Header.Set("Origin", "http://"+address)
		if authenticated {
			request.SetBasicAuth("agent-symphony", "quota-fixture-secret")
		}
		return http.DefaultClient.Do(request)
	}
	response, err := dashboardRequest(http.MethodGet, "/status.json", false)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("dashboard did not require authentication: HTTP %d", response.StatusCode)
	}
	response, err = dashboardRequest(http.MethodGet, "/status.json", true)
	if err != nil {
		t.Fatal(err)
	}
	var degraded dashboardStatusSnapshot
	if err := json.NewDecoder(response.Body).Decode(&degraded); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !degraded.ReadOnly || degraded.ReconciliationError == "" || len(degraded.Statuses) != 0 {
		t.Fatalf("degraded status=%d snapshot=%#v", response.StatusCode, degraded)
	}
	for _, route := range []string{"/actions/reconcile", "/actions/dismiss?repository=o/r&issue=1&attempt=1", "/terminal?repository=o/r&issue=1&attempt=1"} {
		method := http.MethodPost
		if strings.HasPrefix(route, "/terminal") {
			method = http.MethodGet
		}
		response, err := dashboardRequest(method, route, true)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusServiceUnavailable {
			t.Fatalf("degraded %s: HTTP %d", route, response.StatusCode)
		}
	}
	if _, err := os.Lstat(controlSocketPath(stateRoot)); !os.IsNotExist(err) {
		t.Fatalf("control socket exists before auth: %v", err)
	}
	browser := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/quota-degraded-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "playwright"))
	browser.Dir = source
	browser.Env = append(os.Environ(), "AGENT_SYMPHONY_QUOTA_E2E_URL=http://"+address)
	if browserOutput, err := browser.CombinedOutput(); err != nil {
		t.Fatalf("degraded dashboard browser: %v\n%s\nserve:\n%s", err, browserOutput, output.String())
	}
	close(quotaResponse)
	select {
	case <-quotaRejected:
	case <-time.After(20 * time.Second):
		t.Fatalf("quota response was not sent: %s", output.String())
	}
	response, err = dashboardRequest(http.MethodGet, "/status.json", true)
	if err != nil {
		t.Fatal(err)
	}
	degraded = dashboardStatusSnapshot{}
	if err := json.NewDecoder(response.Body).Decode(&degraded); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !degraded.ReadOnly {
		t.Fatalf("dashboard after quota response=%d snapshot=%#v", response.StatusCode, degraded)
	}
	quota.Store(false)
	liveBrowser := exec.Command("npm", "exec", "--prefix", "dashboard", "--", "playwright", "test", "browser/quota-degraded-full-system.spec.js", "--reporter=line", "--output", filepath.Join(root, "live-playwright"))
	liveBrowser.Dir = source
	liveBrowser.Env = append(os.Environ(), "AGENT_SYMPHONY_QUOTA_E2E_URL=http://"+address, "AGENT_SYMPHONY_QUOTA_E2E_PHASE=live")
	if browserOutput, err := liveBrowser.CombinedOutput(); err != nil {
		t.Fatalf("live dashboard browser: %v\n%s\nserve:\n%s", err, browserOutput, output.String())
	}
	response, err = dashboardRequest(http.MethodGet, "/status.json", true)
	if err != nil {
		t.Fatal(err)
	}
	var live dashboardStatusSnapshot
	if err := json.NewDecoder(response.Body).Decode(&live); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || live.ReadOnly || live.OwnerEpoch == 0 {
		t.Fatalf("live status=%d snapshot=%#v", response.StatusCode, live)
	}
	if _, err := os.Lstat(controlSocketPath(stateRoot)); err != nil {
		t.Fatalf("control socket absent after recovery: %v", err)
	}
	for len(requests) > 0 {
		<-requests
	}
	response, err = dashboardRequest(http.MethodPost, "/actions/reconcile", true)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("live reconciliation: HTTP %d output=%s", response.StatusCode, output.String())
	}
	counts := map[string]int{}
	for len(requests) > 0 {
		counts[<-requests]++
	}
	wantCounts := map[string]int{"GET /repos/o/r/pulls": 1, "GET /repos/o/r": 1, "GET /repos/o/r/branches/main": 1, "GET /repos/o/r/issues": 1}
	if !maps.Equal(counts, wantCounts) {
		t.Fatalf("empty-project reconciliation requests=%v want=%v", counts, wantCounts)
	}
	if err := server.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := server.Wait(); err != nil {
		t.Fatalf("graceful shutdown: %v output=%s", err, output.String())
	}
	stopped = true
	for len(requests) > 0 {
		<-requests
	}
	quota.Store(true)
	restartAddress := freeAddress(t)
	restart := exec.Command(binary, "serve", "--config", configPath, "--state", statePath, "--runtime-state", stateRoot, "--dashboard-address", restartAddress, "--dashboard-password-file", passwordPath, "--disable-periodic-reconciliation")
	restart.Dir, restart.Env = repository, server.Env
	restartOutput := &synchronizedBuffer{}
	restart.Stdout, restart.Stderr = restartOutput, restartOutput
	if err := restart.Start(); err != nil {
		t.Fatal(err)
	}
	restartStopped := false
	t.Cleanup(func() {
		if !restartStopped {
			if err := stopFullSystemDaemon(restart); err != nil {
				t.Errorf("stop quota restart fixture: %v", err)
			}
		}
	})
	select {
	case got := <-requests:
		if got != "GET /user" {
			t.Fatalf("restart GitHub request = %q", got)
		}
	case <-time.After(20 * time.Second):
		t.Fatalf("restart auth request missing: %s", restartOutput.String())
	}
	request, _ := http.NewRequest(http.MethodGet, "http://"+restartAddress+"/status.json", nil)
	request.SetBasicAuth("agent-symphony", "quota-fixture-secret")
	response, err = http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	degraded = dashboardStatusSnapshot{}
	if err := json.NewDecoder(response.Body).Decode(&degraded); err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK || !degraded.ReadOnly || degraded.OwnerEpoch == 0 {
		t.Fatalf("persisted degraded status=%d snapshot=%#v", response.StatusCode, degraded)
	}
	if err := restart.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if err := restart.Wait(); err != nil {
		t.Fatalf("cancel quota wait: %v output=%s", err, restartOutput.String())
	}
	restartStopped = true
	if _, err := os.Lstat(controlSocketPath(stateRoot)); !os.IsNotExist(err) {
		t.Fatalf("control socket survived degraded shutdown: %v", err)
	}
}
