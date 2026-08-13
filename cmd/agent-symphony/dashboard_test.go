package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
	"github.com/coder/websocket"
)

func TestDashboardServesEmbeddedNextPageAndStatus(t *testing.T) {
	root := t.TempDir()
	want := `{"updated_at":"2026-08-11T00:00:00Z","statuses":[]}`
	if err := os.WriteFile(filepath.Join(root, "status.json"), []byte(want), 0o600); err != nil {
		t.Fatal(err)
	}
	handler := dashboardHandler(root)

	for _, test := range []struct {
		method, path string
		status       int
		contains     string
	}{
		{http.MethodGet, "/", http.StatusOK, "Agent Symphony"},
		{http.MethodGet, "/status.json", http.StatusOK, want},
		{http.MethodHead, "/status.json", http.StatusOK, ""},
		{http.MethodPost, "/status.json", http.StatusMethodNotAllowed, "method not allowed"},
	} {
		request := httptest.NewRequest(test.method, test.path, nil)
		request.Host = "127.0.0.1"
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != test.status || !strings.Contains(response.Body.String(), test.contains) {
			t.Fatalf("%s %s: status=%d body=%q", test.method, test.path, response.Code, response.Body.String())
		}
	}
}

func TestDashboardEmbedsNextAssets(t *testing.T) {
	paths, err := fs.Glob(dashboardFiles, "dashboard/out/_next/static/chunks/*.js")
	if err != nil || len(paths) == 0 {
		t.Fatalf("embedded Next assets=%v err=%v", paths, err)
	}
	request := httptest.NewRequest(http.MethodGet, "/"+strings.TrimPrefix(paths[0], "dashboard/out/"), nil)
	request.Host = "127.0.0.1"
	response := httptest.NewRecorder()
	dashboardHandler(t.TempDir()).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("embedded asset %s status=%d", request.URL.Path, response.Code)
	}
}

func TestDashboardStatusRejectsMissingAndSymlinkFiles(t *testing.T) {
	root := t.TempDir()
	handler := dashboardHandler(root)
	request := httptest.NewRequest(http.MethodGet, "/status.json", nil)
	request.Host = "127.0.0.1"
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("missing status=%d", response.Code)
	}
	outside := filepath.Join(t.TempDir(), "status.json")
	if err := os.WriteFile(outside, []byte(`{"secret":"canary"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "status.json")); err != nil {
		t.Fatal(err)
	}
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "canary") {
		t.Fatalf("symlink status=%d body=%q", response.Code, response.Body.String())
	}
}

func TestDashboardRejectsNonLoopbackRequestHost(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "http://attacker.example/", nil)
	response := httptest.NewRecorder()
	dashboardHandler(t.TempDir()).ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("non-loopback Host status=%d", response.Code)
	}
}

func TestDashboardUsesLoopbackAndStopsWithContext(t *testing.T) {
	if _, err := startDashboard(t.Context(), "0.0.0.0:0", t.TempDir(), nil, false, "", io.Discard); err == nil {
		t.Fatal("non-loopback dashboard address accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	var log bytes.Buffer
	url, err := startDashboard(ctx, "127.0.0.1:0", t.TempDir(), nil, false, "", &log)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(url)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("dashboard response=%v err=%v", response, err)
	}
	response.Body.Close()
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		response, err = http.Get(url)
		if err != nil {
			return
		}
		response.Body.Close()
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("dashboard remained available after cancellation")
}

func TestDashboardUnsafeNetworkRequiresPassword(t *testing.T) {
	if _, err := startDashboard(t.Context(), "0.0.0.0:0", t.TempDir(), nil, true, "", io.Discard); err == nil || !strings.Contains(err.Error(), "--dashboard-password is required") {
		t.Fatalf("unsafe dashboard without password error=%v", err)
	}

	password := "test-dashboard-password"
	handler := newDashboardHandlerWithOptions(t.Context(), t.TempDir(), "tmux", &sync.Mutex{}, true, password)
	for _, test := range []struct {
		name, method, path, username, password string
		status                                 int
	}{
		{"missing credentials", http.MethodGet, "/", "", "", http.StatusUnauthorized},
		{"wrong username", http.MethodGet, "/", "operator", password, http.StatusUnauthorized},
		{"wrong password", http.MethodGet, "/", "agent-symphony", "wrong", http.StatusUnauthorized},
		{"correct credentials", http.MethodGet, "/", "agent-symphony", password, http.StatusOK},
		{"protected action", http.MethodPost, "/actions/abandon?issue=1&attempt=1", "", "", http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(test.method, "http://192.0.2.10:8080"+test.path, nil)
			if test.username != "" {
				request.SetBasicAuth(test.username, test.password)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.status {
				t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
			}
			if test.status == http.StatusUnauthorized && response.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("authentication challenge is missing")
			}
		})
	}

	server := httptest.NewServer(handler)
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/terminal?issue=1&attempt=1"
	if connection, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}}); err == nil || response == nil || response.StatusCode != http.StatusUnauthorized {
		if connection != nil {
			connection.CloseNow()
		}
		t.Fatalf("unauthenticated terminal response=%v err=%v", response, err)
	}
	authorized := httptest.NewRequest(http.MethodGet, server.URL, nil)
	authorized.Header.Set("Origin", server.URL)
	authorized.SetBasicAuth("agent-symphony", password)
	if connection, response, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: authorized.Header}); err == nil || response == nil || response.StatusCode != http.StatusNotFound {
		if connection != nil {
			connection.CloseNow()
		}
		t.Fatalf("authenticated terminal response=%v err=%v", response, err)
	}
}

func TestDashboardUnsafeNetworkBindingWarnsAndAcceptsAuthentication(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	var log bytes.Buffer
	boundURL, err := startDashboard(ctx, "0.0.0.0:0", t.TempDir(), nil, true, "test-dashboard-password", &log)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := url.Parse(boundURL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(parsed.Host)
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "http://127.0.0.1:"+port, nil)
	if err != nil {
		t.Fatal(err)
	}
	request.SetBasicAuth("agent-symphony", "test-dashboard-password")
	response, err := http.DefaultClient.Do(request)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("dashboard response=%v err=%v", response, err)
	}
	response.Body.Close()
	if !strings.Contains(log.String(), "WARNING: unsafe dashboard network access enabled") || !strings.Contains(log.String(), "unencrypted") {
		t.Fatalf("unsafe dashboard warning=%q", log.String())
	}
}

func TestServeUnsafeNetworkFlagRequiresPassword(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "--state", "pr-state.json", "--runtime-state", t.TempDir(), "--allow-unsafe-dashboard-network"}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "--dashboard-password is required with --allow-unsafe-dashboard-network") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDashboardArchivesCompletedAndAbandonsOrphanedAttempts(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	completed := writeDashboardManifest(t, root, 21, 1, "completed")
	orphaned := writeDashboardManifest(t, root, 22, 1, "failed")
	statuses := []orchestrator.RecoveryStatus{
		{Repository: completed.Repository, Issue: completed.Issue, Attempt: completed.Attempt, State: "completed", Branch: completed.Branch, Worktree: completed.Worktree, Session: completed.Session},
		{Repository: orphaned.Repository, Issue: orphaned.Issue, Attempt: orphaned.Attempt, State: "orphaned", Branch: orphaned.Branch, Worktree: orphaned.Worktree, Session: orphaned.Session},
	}
	if err := writeStatusSnapshot(root, statuses); err != nil {
		t.Fatal(err)
	}
	var actions []string
	operationMu := &sync.Mutex{}
	locked := true
	server := &dashboardServer{ctx: t.Context(), stateRoot: root, tmux: "tmux", mu: operationMu}
	server.cleanup = func(_ context.Context, action string, manifest agentruntime.Manifest) error {
		if operationMu.TryLock() {
			operationMu.Unlock()
			locked = false
		}
		actions = append(actions, action+":"+strconv.Itoa(manifest.Issue))
		return nil
	}
	assets, _ := fs.Sub(dashboardFiles, "dashboard/out")
	handler := server.handler(http.FileServer(http.FS(assets)))

	requestAction := func(path, origin string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+path, nil)
		request.Header.Set("Origin", origin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := requestAction("/actions/archive?issue=21&attempt=1", "https://evil.example"); response.Code != http.StatusForbidden {
		t.Fatalf("cross-origin archive status=%d", response.Code)
	}
	if response := requestAction("/actions/archive?issue=22&attempt=1", "http://127.0.0.1"); response.Code != http.StatusConflict {
		t.Fatalf("wrong-state archive status=%d body=%q", response.Code, response.Body.String())
	}
	operationMu.Lock()
	response := requestAction("/actions/archive?issue=21&attempt=1", "http://127.0.0.1")
	operationMu.Unlock()
	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Retry-After") != "1" || !strings.Contains(response.Body.String(), "reconciliation is in progress") || len(actions) != 0 {
		t.Fatalf("busy reconciliation status=%d retry=%q body=%q actions=%v", response.Code, response.Header().Get("Retry-After"), response.Body.String(), actions)
	}
	if response := requestAction("/actions/archive?issue=21&attempt=1", "http://127.0.0.1"); response.Code != http.StatusOK {
		t.Fatalf("archive status=%d body=%q", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Join(filepath.Dir(completed.LogPath), "manifest.json")); err != nil {
		t.Fatalf("archive removed retained manifest: %v", err)
	}
	if response := requestAction("/actions/abandon?issue=22&attempt=1", "http://127.0.0.1"); response.Code != http.StatusOK {
		t.Fatalf("abandon status=%d body=%q", response.Code, response.Body.String())
	}
	if _, err := os.Stat(filepath.Dir(orphaned.LogPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandon retained attempt record: %v", err)
	}
	if strings.Join(actions, ",") != "archive:21,abandon:22" {
		t.Fatalf("cleanup actions=%v", actions)
	}
	if !locked {
		t.Fatal("cleanup ran outside the shared reconciliation lock")
	}
	state, err := server.readState()
	if err != nil || len(state.Hidden) != 2 || state.Hidden[0].Reason != "archived" || state.Hidden[1].Reason != "abandoned" {
		t.Fatalf("dashboard state=%#v err=%v", state, err)
	}
}

func TestDashboardCleanupRejectsProjectionIdentityDrift(t *testing.T) {
	root, _ := filepath.EvalSymlinks(t.TempDir())
	manifest := writeDashboardManifest(t, root, 23, 1, "completed")
	status := orchestrator.RecoveryStatus{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, State: "completed", Branch: "substituted", Worktree: manifest.Worktree, Session: manifest.Session}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	called := false
	server := &dashboardServer{ctx: t.Context(), stateRoot: root, cleanup: func(context.Context, string, agentruntime.Manifest) error {
		called = true
		return nil
	}}
	assets, _ := fs.Sub(dashboardFiles, "dashboard/out")
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/archive?issue=23&attempt=1", nil)
	request.Header.Set("Origin", "http://127.0.0.1")
	response := httptest.NewRecorder()
	server.handler(http.FileServer(http.FS(assets))).ServeHTTP(response, request)
	if response.Code != http.StatusConflict || called {
		t.Fatalf("identity drift status=%d cleanup_called=%v", response.Code, called)
	}
}

func writeDashboardManifest(t *testing.T, stateRoot string, issue, attemptNumber int, state string) agentruntime.Manifest {
	t.Helper()
	repository := "o/r"
	attemptRoot := filepath.Join(stateRoot, "worktrees")
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	attempt := agentruntime.Attempt{Repository: repository, Issue: issue, Number: attemptNumber, BaseSHA: strings.Repeat("a", 40)}
	manifest, err := agentruntime.AttemptIdentity(attemptRoot, attempt)
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier(repository), fmt.Sprintf("%d-%d", issue, attemptNumber))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest.LogPath = filepath.Join(dir, "agent.log")
	manifest.State = state
	manifest.CreatedAt, manifest.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	if state == "completed" {
		manifest.ReviewHead = strings.Repeat("b", 40)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifest.LogPath, []byte("diagnostic"), 0o600); err != nil {
		t.Fatal(err)
	}
	return manifest
}

func TestDashboardTerminalAttachesOnlyProjectedSameOriginSession(t *testing.T) {
	root := t.TempDir()
	repository, issue, attempt := "o/r", 23, 2
	session := "as-" + internalgithub.RepositoryIdentifier(repository) + "-23-2"
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{{Repository: repository, Issue: issue, Attempt: attempt, State: "running", Session: session}}); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "tmux")
	body := "#!/bin/sh\ncase $1 in\n" +
		"has-session) test \"$2\" = -t && test \"$3\" = \"=$EXPECTED_SESSION\";;\n" +
		"attach-session) test \"$2\" = -t && test \"$3\" = \"=$EXPECTED_SESSION\" || exit 2; printf 'ready\\r\\n'; IFS= read -r input; stty size; printf 'got:%s\\r\\n' \"$input\"; sleep 30;;\n" +
		"*) exit 2;;\nesac\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXPECTED_SESSION", session)
	server := httptest.NewServer(newDashboardHandler(t.Context(), root, script))
	defer server.Close()

	dial := func(origin string, selectedIssue int) (*websocket.Conn, *http.Response, error) {
		endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/terminal?issue=" + strconv.Itoa(selectedIssue) + "&attempt=2"
		return websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{origin}}})
	}
	if connection, response, err := dial("https://evil.example", issue); err == nil || response == nil || response.StatusCode != http.StatusForbidden {
		if connection != nil {
			connection.CloseNow()
		}
		t.Fatalf("cross-origin terminal response=%v err=%v", response, err)
	}
	if connection, response, err := dial(server.URL, issue+1); err == nil || response == nil || response.StatusCode != http.StatusNotFound {
		if connection != nil {
			connection.CloseNow()
		}
		t.Fatalf("unprojected terminal response=%v err=%v", response, err)
	}

	connection, response, err := dial(server.URL, issue)
	if err != nil {
		t.Fatalf("terminal dial response=%v err=%v", response, err)
	}
	defer connection.CloseNow()
	if err := connection.Write(t.Context(), websocket.MessageText, []byte(`{"type":"resize","cols":120,"rows":40}`)); err != nil {
		t.Fatal(err)
	}
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte("hello\n")); err != nil {
		t.Fatal(err)
	}
	var output strings.Builder
	deadline, cancel := context.WithTimeout(t.Context(), 3*time.Second)
	defer cancel()
	for !strings.Contains(output.String(), "got:hello") {
		kind, message, err := connection.Read(deadline)
		if err != nil || kind != websocket.MessageBinary {
			t.Fatalf("terminal output=%q kind=%v err=%v", output.String(), kind, err)
		}
		output.Write(message)
	}
	if !strings.Contains(output.String(), "40 120") {
		t.Fatalf("terminal did not resize: %q", output.String())
	}
}

func TestDashboardTerminalRejectsTamperedIdentityAndInvalidMessages(t *testing.T) {
	root := t.TempDir()
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 8, Attempt: 1, State: "running", Session: "as-arbitrary"}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	if _, err := (&dashboardServer{stateRoot: root}).terminalStatus(8, 1); err == nil {
		t.Fatal("tampered session identity accepted")
	}

	status.Session = "as-" + internalgithub.RepositoryIdentifier(status.Repository) + "-8-1"
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "tmux")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncase $1 in has-session) exit 0;; attach-session) sleep 30;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(newDashboardHandler(t.Context(), root, script))
	defer server.Close()
	parsed, _ := url.Parse(server.URL)
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/terminal?issue=8&attempt=1"
	connection, _, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := connection.Write(t.Context(), websocket.MessageText, []byte(`{"type":"resize","cols":0,"rows":40}`)); err != nil {
		t.Fatal(err)
	}
	_, _, err = connection.Read(t.Context())
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("invalid resize from %s close=%v err=%v", parsed.Host, websocket.CloseStatus(err), err)
	}

	connection, _, err = websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	if err != nil {
		t.Fatal(err)
	}
	defer connection.CloseNow()
	if err := connection.Write(t.Context(), websocket.MessageBinary, make([]byte, maxTerminalInputBytes+1)); err != nil {
		t.Fatal(err)
	}
	_, _, err = connection.Read(t.Context())
	if websocket.CloseStatus(err) != websocket.StatusMessageTooBig {
		t.Fatalf("oversized input close=%v err=%v", websocket.CloseStatus(err), err)
	}
}

func TestDashboardTerminalRequiresLiveSession(t *testing.T) {
	root := t.TempDir()
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 8, Attempt: 1, State: "orphaned", Session: "as-" + internalgithub.RepositoryIdentifier("o/r") + "-8-1"}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "tmux")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/terminal?issue=8&attempt=1", nil)
	request.Header.Set("Origin", "http://127.0.0.1")
	response := httptest.NewRecorder()
	newDashboardHandler(t.Context(), root, script).ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("missing session status=%d body=%q", response.Code, response.Body.String())
	}
}
