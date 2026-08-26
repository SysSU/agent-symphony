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
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
	"github.com/coder/websocket"
)

func TestDashboardServesEmbeddedNextPageAndStatus(t *testing.T) {
	requireDashboardBuild(t)
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
		{http.MethodGet, "/orchestrator.json", http.StatusOK, `"state":"disabled"`},
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
	requireDashboardBuild(t)
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

func requireDashboardBuild(t *testing.T) {
	t.Helper()
	if _, err := fs.Stat(dashboardFiles, "dashboard/out/index.html"); err != nil {
		t.Skip("dashboard output has not been built")
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
	if _, err := startDashboard(t.Context(), "0.0.0.0:0", t.TempDir(), nil, nil, nil, nil, false, "", io.Discard); err == nil {
		t.Fatal("non-loopback dashboard address accepted")
	}
	ctx, cancel := context.WithCancel(t.Context())
	var log bytes.Buffer
	service := &fakeDashboardOrchestrator{status: orchestratoragent.Status{Version: 1, Enabled: true, State: "running", Session: "as-o-test"}}
	url, err := startDashboard(ctx, "127.0.0.1:0", t.TempDir(), nil, nil, nil, service, false, "", &log)
	if err != nil {
		t.Fatal(err)
	}
	response, err := http.Get(url)
	if err != nil || response.StatusCode != http.StatusOK {
		t.Fatalf("dashboard response=%v err=%v", response, err)
	}
	response.Body.Close()
	response, err = http.Get(url + "/orchestrator.json")
	if err != nil {
		t.Fatal(err)
	}
	body, readErr := io.ReadAll(response.Body)
	response.Body.Close()
	if readErr != nil || response.StatusCode != http.StatusOK || !strings.Contains(string(body), `"session":"as-o-test"`) {
		t.Fatalf("orchestrator response=%q status=%d err=%v", body, response.StatusCode, readErr)
	}
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
	if _, err := startDashboard(t.Context(), "0.0.0.0:0", t.TempDir(), nil, nil, nil, nil, true, "", io.Discard); err == nil || !strings.Contains(err.Error(), "dashboard password is required") {
		t.Fatalf("unsafe dashboard without password error=%v", err)
	}

	password := "test-dashboard-password"
	handler := newDashboardHandlerWithOptions(t.Context(), t.TempDir(), "tmux", &sync.Mutex{}, nil, nil, nil, true, password)
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
	boundURL, err := startDashboard(ctx, "0.0.0.0:0", t.TempDir(), nil, nil, nil, nil, true, "test-dashboard-password", &log)
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

func TestDashboardReconcileActionRunsUnderSharedLock(t *testing.T) {
	operationMu := &sync.Mutex{}
	calls := 0
	failing := false
	server := &dashboardServer{mu: operationMu, reconcile: func(context.Context) error {
		if operationMu.TryLock() {
			operationMu.Unlock()
			t.Fatal("reconciliation ran outside the shared operation lock")
		}
		calls++
		if failing {
			return errors.New("token=canary")
		}
		return nil
	}}
	handler := server.handler(http.NotFoundHandler())
	request := func(method, path, origin string, body io.Reader) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "http://127.0.0.1"+path, body)
		r.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}

	if response := request(http.MethodGet, "/actions/reconcile", "http://127.0.0.1", nil); response.Code != http.StatusMethodNotAllowed || response.Header().Get("Allow") != "POST" {
		t.Fatalf("GET status=%d allow=%q", response.Code, response.Header().Get("Allow"))
	}
	if response := request(http.MethodPost, "/actions/reconcile", "https://evil.example", nil); response.Code != http.StatusForbidden {
		t.Fatalf("cross-origin status=%d", response.Code)
	}
	if response := request(http.MethodPost, "/actions/reconcile", "http://127.0.0.1", strings.NewReader("payload")); response.Code != http.StatusBadRequest {
		t.Fatalf("request body status=%d", response.Code)
	}
	if response := request(http.MethodPost, "/actions/reconcile?again=1", "http://127.0.0.1", nil); response.Code != http.StatusBadRequest {
		t.Fatalf("query status=%d", response.Code)
	}
	operationMu.Lock()
	busy := request(http.MethodPost, "/actions/reconcile", "http://127.0.0.1", nil)
	operationMu.Unlock()
	if busy.Code != http.StatusServiceUnavailable || busy.Header().Get("Retry-After") != "1" || calls != 0 {
		t.Fatalf("busy status=%d retry=%q calls=%d", busy.Code, busy.Header().Get("Retry-After"), calls)
	}
	if response := request(http.MethodPost, "/actions/reconcile", "http://127.0.0.1", nil); response.Code != http.StatusNoContent || calls != 1 {
		t.Fatalf("success status=%d body=%q calls=%d", response.Code, response.Body.String(), calls)
	}
	failing = true
	response := request(http.MethodPost, "/actions/reconcile", "http://127.0.0.1", nil)
	if response.Code != http.StatusInternalServerError || strings.Contains(response.Body.String(), "canary") || !strings.Contains(response.Body.String(), "[REDACTED]") || calls != 2 {
		t.Fatalf("failure status=%d body=%q calls=%d", response.Code, response.Body.String(), calls)
	}
}

type fakeDashboardOrchestrator struct {
	status  orchestratoragent.Status
	target  orchestratoragent.AttachTarget
	err     error
	actions []string
}

type fakeDashboardMessageService struct {
	*fakeDashboardOrchestrator
	proposal  orchestratoragent.MessageProposal
	confirmed orchestratoragent.MessageProposal
	confirms  int
	consumes  int
}

func (f *fakeDashboardMessageService) MessageProposal(context.Context) (orchestratoragent.MessageProposal, error) {
	if f.proposal.Binding == "" {
		return orchestratoragent.MessageProposal{}, orchestratoragent.ErrNoMessageProposal
	}
	return f.proposal, nil
}

func (f *fakeDashboardMessageService) ConsumeMessageProposal(_ context.Context, binding string) error {
	if binding != f.proposal.Binding {
		return errors.New("binding changed")
	}
	f.consumes++
	f.proposal = orchestratoragent.MessageProposal{}
	return nil
}

func (f *fakeDashboardMessageService) ConfirmMessage(_ context.Context, proposal orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error) {
	f.confirmed = proposal
	f.confirms++
	return internalgithub.PrepareOperatorMessage(proposal.Repository, proposal.Issue, proposal.Attempt, proposal.Message)
}

func TestDashboardQueuesAuthenticatedWorkerFollowUpForExactProjectedAttempt(t *testing.T) {
	root := t.TempDir()
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 131, 3)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{{Repository: "o/r", Issue: 131, Attempt: 3, State: "active", Session: session}}); err != nil {
		t.Fatal(err)
	}
	service := &fakeDashboardMessageService{fakeDashboardOrchestrator: &fakeDashboardOrchestrator{}}
	withoutPassword := (&dashboardServer{stateRoot: root, messages: service, mu: &sync.Mutex{}}).handler(http.NotFoundHandler())
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/attempt/message?issue=131&attempt=3", strings.NewReader(`{"message":"Continue the focused fix."}`))
	request.Header.Set("Origin", "http://127.0.0.1")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	withoutPassword.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden || service.confirms != 0 {
		t.Fatalf("unauthenticated status=%d confirms=%d", response.Code, service.confirms)
	}

	server := &dashboardServer{stateRoot: root, password: "password", messages: service, mu: &sync.Mutex{}}
	handler := server.handler(http.NotFoundHandler())
	navigate := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/", nil)
	navigate.Header.Set("Sec-Fetch-Mode", "navigate")
	navigate.Header.Set("Sec-Fetch-Dest", "document")
	navigate.SetBasicAuth("agent-symphony", "password")
	navigation := httptest.NewRecorder()
	handler.ServeHTTP(navigation, navigate)
	sessionCookie := navigation.Result().Cookies()[0]

	request = httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/attempt/message?issue=131&attempt=3", strings.NewReader(`{"message":"Continue the focused fix."}`))
	request.Header.Set("Origin", "http://127.0.0.1")
	request.Header.Set("Content-Type", "application/json")
	request.SetBasicAuth("agent-symphony", "password")
	request.AddCookie(sessionCookie)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	want := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 131, Attempt: 3, Message: "Continue the focused fix."}
	if response.Code != http.StatusOK || service.confirms != 1 || service.confirmed != want || !strings.Contains(response.Body.String(), `"state":"queued"`) {
		t.Fatalf("status=%d confirms=%d proposal=%#v body=%q", response.Code, service.confirms, service.confirmed, response.Body.String())
	}
}

func TestDashboardWorkerMessageRequiresAuthenticationAndExactConfirmationBinding(t *testing.T) {
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 131, Attempt: 3, Message: "Run the race test.", Binding: "exact-binding"}
	service := &fakeDashboardMessageService{fakeDashboardOrchestrator: &fakeDashboardOrchestrator{status: orchestratoragent.Status{Enabled: true, State: "running"}}, proposal: proposal}
	withoutPassword := newDashboardHandlerWithOptions(t.Context(), t.TempDir(), "tmux", &sync.Mutex{}, nil, nil, service, false, "")
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/orchestrator/proposal.json", nil)
	response := httptest.NewRecorder()
	withoutPassword.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("unauthenticated message feature status=%d", response.Code)
	}

	handler := newDashboardHandlerWithOptions(t.Context(), t.TempDir(), "tmux", &sync.Mutex{}, nil, nil, service, false, "password")
	navigate := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/", nil)
	navigate.Header.Set("Sec-Fetch-Mode", "navigate")
	navigate.Header.Set("Sec-Fetch-Dest", "document")
	navigate.SetBasicAuth("agent-symphony", "password")
	navigation := httptest.NewRecorder()
	handler.ServeHTTP(navigation, navigate)
	cookies := navigation.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != dashboardSessionCookie || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("browser session cookies=%#v", cookies)
	}
	session := cookies[0]
	service.proposal = orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 131, Attempt: 3, Action: orchestratoragent.ProposalActionRetry, RequestID: "retry-1", Binding: "retry-binding"}
	request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1/orchestrator/proposal.json", nil)
	request.SetBasicAuth("agent-symphony", "password")
	request.AddCookie(session)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("automatic retry was exposed as a message confirmation: status=%d body=%q", response.Code, response.Body.String())
	}
	service.proposal = proposal
	request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1/orchestrator/proposal.json", nil)
	request.SetBasicAuth("agent-symphony", "password")
	request.AddCookie(session)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	nonce := response.Header().Get(dashboardNonceHeader)
	if response.Code != http.StatusOK || nonce == "" || !strings.Contains(response.Body.String(), proposal.Message) || !strings.Contains(response.Body.String(), proposal.Binding) {
		t.Fatalf("proposal status=%d body=%q", response.Code, response.Body.String())
	}

	post := func(submitted orchestratoragent.MessageProposal, browser bool) *httptest.ResponseRecorder {
		body, _ := json.Marshal(submitted)
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/orchestrator/message-confirm", bytes.NewReader(body))
		request.Header.Set("Origin", "http://127.0.0.1")
		request.Header.Set("Content-Type", "application/json")
		request.SetBasicAuth("agent-symphony", "password")
		if browser {
			request.AddCookie(session)
			request.Header.Set(dashboardNonceHeader, nonce)
		} else {
			request.AddCookie(&http.Cookie{Name: dashboardSessionCookie, Value: "forged-session"})
			request.Header.Set(dashboardNonceHeader, "forged-nonce")
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	if response := post(proposal, false); response.Code != http.StatusForbidden || service.confirms != 0 {
		t.Fatalf("authenticated non-browser status=%d confirms=%d", response.Code, service.confirms)
	}
	changed := proposal
	changed.Message = "Different bytes."
	if response := post(changed, true); response.Code != http.StatusConflict || service.confirms != 0 {
		t.Fatalf("changed proposal status=%d confirms=%d", response.Code, service.confirms)
	}
	service.proposal.Binding = "replacement-binding"
	if response := post(service.proposal, true); response.Code != http.StatusForbidden || service.confirms != 0 {
		t.Fatalf("nonce reused for replacement proposal status=%d confirms=%d", response.Code, service.confirms)
	}
	service.proposal = proposal
	response = post(proposal, true)
	if response.Code != http.StatusOK || service.confirms != 1 || service.consumes != 1 || strings.Contains(response.Body.String(), proposal.Message) {
		t.Fatalf("confirmed status=%d confirms=%d consumes=%d body=%q", response.Code, service.confirms, service.consumes, response.Body.String())
	}
	request = httptest.NewRequest(http.MethodGet, "http://127.0.0.1/orchestrator/proposal.json", nil)
	request.SetBasicAuth("agent-symphony", "password")
	request.AddCookie(session)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("consumed proposal status=%d body=%q", response.Code, response.Body.String())
	}
}

func (f *fakeDashboardOrchestrator) Status(context.Context) (orchestratoragent.Status, error) {
	return f.status, f.err
}
func (f *fakeDashboardOrchestrator) AttachTarget(context.Context) (orchestratoragent.AttachTarget, error) {
	return f.target, f.err
}
func (f *fakeDashboardOrchestrator) action(name string) (orchestratoragent.Status, error) {
	f.actions = append(f.actions, name)
	if name == "clear" || name == "rebuild" {
		f.status.Generation++
		f.status.ContextMode = name
		f.status.RebuiltAt = time.Now().UTC()
	}
	if name == "recover" {
		f.status.State = "running"
	}
	return f.status, f.err
}
func (f *fakeDashboardOrchestrator) Recover(context.Context) (orchestratoragent.Status, error) {
	return f.action("recover")
}
func (f *fakeDashboardOrchestrator) Clear(context.Context) (orchestratoragent.Status, error) {
	return f.action("clear")
}
func (f *fakeDashboardOrchestrator) Rebuild(context.Context) (orchestratoragent.Status, error) {
	return f.action("rebuild")
}
func (f *fakeDashboardOrchestrator) Investigate(_ context.Context, issue, attempt int) (orchestratoragent.Status, error) {
	return f.action(fmt.Sprintf("investigate:%d:%d", issue, attempt))
}

func TestDashboardOrchestratorStatusAndActions(t *testing.T) {
	root := t.TempDir()
	statuses := []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 31, Attempt: 2, State: "blocked", Session: "as-" + internalgithub.RepositoryIdentifier("o/r") + "-31-2"},
		{Repository: "o/r", Issue: 32, Attempt: 1, State: "active", Session: "as-" + internalgithub.RepositoryIdentifier("o/r") + "-32-1"},
	}
	if err := writeStatusSnapshot(root, statuses); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	service := &fakeDashboardOrchestrator{status: orchestratoragent.Status{Version: 1, UpdatedAt: now, Enabled: true, State: "running", Session: "as-o-test", Generation: 2, ContextMode: "rebuild", RebuiltAt: now, Diagnostic: "password=hunter2", NextAction: "none"}}
	operationMu := &sync.Mutex{}
	handler := newDashboardHandlerWithOptions(t.Context(), root, "tmux", operationMu, nil, nil, service, false, "")

	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/orchestrator.json", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"generation":2`) || strings.Contains(response.Body.String(), "hunter2") {
		t.Fatalf("status=%d body=%q", response.Code, response.Body.String())
	}

	post := func(path, origin string) *httptest.ResponseRecorder {
		request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1"+path, nil)
		request.Header.Set("Origin", origin)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		return response
	}
	request = httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/orchestrator/recover", io.NopCloser(strings.NewReader("x")))
	request.ContentLength = -1
	request.Header.Set("Origin", "http://127.0.0.1")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unknown-length body status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/orchestrator/recover", nil)
	request.TransferEncoding = []string{"chunked"}
	request.Header.Set("Origin", "http://127.0.0.1")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("transfer-encoded body status=%d", response.Code)
	}
	if got := post("/actions/orchestrator/recover", "https://evil.example"); got.Code != http.StatusForbidden {
		t.Fatalf("cross-origin recover status=%d", got.Code)
	}
	if got := post("/actions/orchestrator/recover?command=sh", "http://127.0.0.1"); got.Code != http.StatusBadRequest {
		t.Fatalf("caller-selected command status=%d", got.Code)
	}
	if got := post("/actions/orchestrator/investigate?issue=32&attempt=1", "http://127.0.0.1"); got.Code != http.StatusConflict {
		t.Fatalf("ineligible investigate status=%d body=%q", got.Code, got.Body.String())
	}
	service.status.State = "starting"
	if got := post("/actions/orchestrator/investigate?issue=31&attempt=2", "http://127.0.0.1"); got.Code != http.StatusConflict {
		t.Fatalf("non-running investigate status=%d body=%q", got.Code, got.Body.String())
	}
	service.status.State = "running"
	operationMu.Lock()
	got := post("/actions/orchestrator/recover", "http://127.0.0.1")
	operationMu.Unlock()
	if got.Code != http.StatusServiceUnavailable || got.Header().Get("Retry-After") != "1" {
		t.Fatalf("busy recover status=%d retry=%q", got.Code, got.Header().Get("Retry-After"))
	}
	if got := post("/actions/orchestrator/recover", "http://127.0.0.1"); got.Code != http.StatusOK || !strings.Contains(got.Body.String(), `"ok":true`) {
		t.Fatalf("recover status=%d body=%q", got.Code, got.Body.String())
	}
	if got := post("/actions/orchestrator/investigate?issue=31&attempt=2", "http://127.0.0.1"); got.Code != http.StatusOK {
		t.Fatalf("investigate status=%d body=%q", got.Code, got.Body.String())
	}
	if strings.Join(service.actions, ",") != "recover,investigate:31:2" {
		t.Fatalf("actions=%v", service.actions)
	}
}

func TestDashboardRemoteProxyInvestigationRequiresExactForwardedOrigin(t *testing.T) {
	root := t.TempDir()
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{{Repository: "o/r", Issue: 31, Attempt: 2, State: "blocked", Session: "as-" + internalgithub.RepositoryIdentifier("o/r") + "-31-2"}}); err != nil {
		t.Fatal(err)
	}
	service := &fakeDashboardOrchestrator{status: orchestratoragent.Status{Version: 1, Enabled: true, State: "running"}}
	handler := newDashboardHandlerWithOptions(t.Context(), root, "tmux", &sync.Mutex{}, nil, nil, service, false, "password")
	request := func(origin, forwardedHost, forwardedProto, login, remoteAddr string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/orchestrator/investigate?issue=31&attempt=2", nil)
		r.Header.Set("Origin", origin)
		r.Header.Set("X-Forwarded-Host", forwardedHost)
		r.Header.Set("X-Forwarded-Proto", forwardedProto)
		r.Header.Set("Tailscale-User-Login", login)
		r.RemoteAddr = remoteAddr
		r.SetBasicAuth("agent-symphony", "password")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response
	}

	if got := request("https://machine.example.ts.net", "machine.example.ts.net", "https", "user@example.com", "127.0.0.1:54321"); got.Code != http.StatusOK {
		t.Fatalf("remote proxy investigation status=%d body=%q", got.Code, got.Body.String())
	}
	for _, test := range []struct {
		name, origin, forwardedHost, forwardedProto, login, remoteAddr string
	}{
		{"mismatched origin", "https://other.example.ts.net", "machine.example.ts.net", "https", "user@example.com", "127.0.0.1:54321"},
		{"spoofed proxy headers", "https://machine.example.ts.net", "machine.example.ts.net", "https", "", "127.0.0.1:54321"},
		{"spoofed proxy identity", "https://machine.example.ts.net", "machine.example.ts.net", "https", "user@example.com", "192.0.2.10:54321"},
		{"mismatched scheme", "https://machine.example.ts.net", "machine.example.ts.net", "http", "user@example.com", "127.0.0.1:54321"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := request(test.origin, test.forwardedHost, test.forwardedProto, test.login, test.remoteAddr); got.Code != http.StatusForbidden {
				t.Fatalf("status=%d body=%q", got.Code, got.Body.String())
			}
		})
	}
	if got := request("http://127.0.0.1", "", "", "", "127.0.0.1:54321"); got.Code != http.StatusOK {
		t.Fatalf("localhost investigation status=%d body=%q", got.Code, got.Body.String())
	}
	if strings.Join(service.actions, ",") != "investigate:31:2,investigate:31:2" {
		t.Fatalf("actions=%v", service.actions)
	}
}

func TestDashboardOrchestratorClearAndRebuildAreBodylessPOSTsWithContextTransitions(t *testing.T) {
	service := &fakeDashboardOrchestrator{status: orchestratoragent.Status{Version: 1, Enabled: true, State: "running", Generation: 4, ContextMode: "rebuild"}}
	handler := newDashboardHandlerWithOptions(t.Context(), t.TempDir(), "tmux", &sync.Mutex{}, nil, nil, service, false, "")

	for _, action := range []string{"clear", "rebuild"} {
		request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/actions/orchestrator/"+action, nil)
		request.Header.Set("Origin", "http://127.0.0.1")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusMethodNotAllowed {
			t.Fatalf("GET %s status=%d", action, response.Code)
		}

		before := service.status.Generation
		request = httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/orchestrator/"+action, nil)
		request.Header.Set("Origin", "http://127.0.0.1")
		response = httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		var result struct {
			OK     bool                     `json:"ok"`
			Status orchestratoragent.Status `json:"status"`
		}
		if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.OK {
			t.Fatalf("POST %s status=%d body=%q", action, response.Code, response.Body.String())
		}
		if result.Status.Generation != before+1 || result.Status.ContextMode != action || result.Status.RebuiltAt.IsZero() {
			t.Fatalf("POST %s status=%+v", action, result.Status)
		}
	}
	if strings.Join(service.actions, ",") != "clear,rebuild" {
		t.Fatalf("actions=%v", service.actions)
	}
}

func TestDashboardOrchestratorTerminalIsExactAndLoopbackOnly(t *testing.T) {
	root := t.TempDir()
	session := "as-o-test-repository"
	service := &fakeDashboardOrchestrator{target: orchestratoragent.AttachTarget{Session: session}}
	script := filepath.Join(t.TempDir(), "tmux")
	body := "#!/bin/sh\ncase $1 in\n" +
		"has-session) test \"$2\" = -t && test \"$3\" = \"=$EXPECTED_SESSION\";;\n" +
		"attach-session) test \"$2\" = -t && test \"$3\" = \"=$EXPECTED_SESSION\" || exit 2; printf 'orchestrator-ready\\r\\n'; sleep 30;;\n" +
		"*) exit 2;;\nesac\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXPECTED_SESSION", session)

	unsafe := newDashboardHandlerWithOptions(t.Context(), root, script, &sync.Mutex{}, nil, nil, service, true, "password")
	request := httptest.NewRequest(http.MethodGet, "http://192.0.2.10/orchestrator/terminal", nil)
	request.Header.Set("Origin", "http://192.0.2.10")
	request.SetBasicAuth("agent-symphony", "password")
	response := httptest.NewRecorder()
	unsafe.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("non-loopback orchestrator terminal status=%d", response.Code)
	}

	server := httptest.NewServer(newDashboardHandlerWithOptions(t.Context(), root, script, &sync.Mutex{}, nil, nil, service, false, ""))
	defer server.Close()
	endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + "/orchestrator/terminal"
	connection, responseHTTP, err := websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	if err != nil {
		t.Fatalf("terminal dial response=%v err=%v", responseHTTP, err)
	}
	defer connection.CloseNow()
	kind, message, err := connection.Read(t.Context())
	if err != nil || kind != websocket.MessageBinary || !strings.Contains(string(message), "orchestrator-ready") {
		t.Fatalf("terminal kind=%v output=%q err=%v", kind, message, err)
	}
}

func TestDashboardOriginAcceptsExactHTTPAndHTTPSHosts(t *testing.T) {
	for _, origin := range []string{"http://localhost:8080", "https://machine.example.ts.net"} {
		request := httptest.NewRequest(http.MethodGet, origin+"/orchestrator/terminal", nil)
		request.Header.Set("Origin", origin)
		if !sameDashboardOrigin(request) {
			t.Errorf("exact origin %q was rejected", origin)
		}
	}

	request := httptest.NewRequest(http.MethodGet, "https://machine.example.ts.net/orchestrator/terminal", nil)
	request.Header.Set("Origin", "https://other.example.ts.net")
	if sameDashboardOrigin(request) {
		t.Fatal("mismatched origin was accepted")
	}
}

func TestDashboardLoopbackRequestAcceptsTailscaleServeIdentity(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "https://machine.example.ts.net/orchestrator/terminal", nil)
	request.RemoteAddr = "127.0.0.1:54321"
	if dashboardRequestLoopback(request) {
		t.Fatal("non-loopback Host without Tailscale identity was accepted")
	}

	request.Header.Set("Tailscale-User-Login", "user@example.com")
	if !dashboardRequestLoopback(request) {
		t.Fatal("loopback Tailscale Serve request was rejected")
	}

	request.RemoteAddr = "192.0.2.10:54321"
	if dashboardRequestLoopback(request) {
		t.Fatal("non-loopback peer with spoofed Tailscale identity was accepted")
	}
}

func TestServeUnsafeNetworkFlagRequiresPasswordFile(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := run([]string{"serve", "--state", "pr-state.json", "--runtime-state", t.TempDir(), "--allow-unsafe-dashboard-network"}, &stdout, &stderr)
	if code != 2 || !strings.Contains(stderr.String(), "--dashboard-password-file is required with --allow-unsafe-dashboard-network") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestDashboardPasswordLoadsOnlyFromPrivateCoordinatorFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "dashboard-password")
	if err := os.WriteFile(path, []byte("private-password\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if password, err := readDashboardPassword(path); err != nil || password != "private-password" {
		t.Fatalf("password=%q err=%v", password, err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readDashboardPassword(path); err == nil {
		t.Fatal("world-readable dashboard password was accepted")
	}
	link := filepath.Join(dir, "dashboard-password-link")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if _, err := readDashboardPassword(link); err == nil {
		t.Fatal("symlinked dashboard password was accepted")
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

func TestDashboardRecoverRequiresSameOriginFreshRetryableProjection(t *testing.T) {
	root := t.TempDir()
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 24, Attempt: 2, State: "failed", Retryable: true, Session: "as-" + internalgithub.RepositoryIdentifier("o/r") + "-24-2"}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	operationMu := &sync.Mutex{}
	calls := 0
	server := &dashboardServer{ctx: t.Context(), stateRoot: root, tmux: "tmux", mu: operationMu, recover: func(_ context.Context, issue, attempt int) error {
		calls++
		if issue != 24 || attempt != 2 {
			t.Fatalf("recovered %d/%d", issue, attempt)
		}
		return nil
	}}
	assets, _ := fs.Sub(dashboardFiles, "dashboard/out")
	handler := server.handler(http.FileServer(http.FS(assets)))
	request := func(origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/recover?issue=24&attempt=2", nil)
		r.Header.Set("Origin", origin)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if response := request("https://evil.example"); response.Code != http.StatusForbidden || calls != 0 {
		t.Fatalf("cross-origin status=%d calls=%d", response.Code, calls)
	}
	operationMu.Lock()
	busy := request("http://127.0.0.1")
	operationMu.Unlock()
	if busy.Code != http.StatusServiceUnavailable || calls != 0 {
		t.Fatalf("busy status=%d calls=%d", busy.Code, calls)
	}
	if response := request("http://127.0.0.1"); response.Code != http.StatusOK || calls != 1 {
		t.Fatalf("recover status=%d body=%q calls=%d", response.Code, response.Body.String(), calls)
	}
	status.Retryable = false
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	if response := request("http://127.0.0.1"); response.Code != http.StatusConflict || calls != 1 {
		t.Fatalf("stale status=%d calls=%d", response.Code, calls)
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
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{{Repository: repository, Issue: issue, Attempt: attempt, State: "running", Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "running", Current: true}}}}); err != nil {
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

func TestDashboardReviewerTerminalIsRoleBoundAndReadOnly(t *testing.T) {
	root := t.TempDir()
	repository, issue, attempt := "o/r", 23, 2
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, repository, issue, attempt)
	reviewer, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, repository, issue, attempt)
	status := orchestrator.RecoveryStatus{Repository: repository, Issue: issue, Attempt: attempt, State: "active", Session: implementation, Sessions: []orchestrator.AttemptSession{
		{Role: agentruntime.SessionRoleImplementation, Name: implementation, State: "completed"},
		{Role: agentruntime.SessionRoleReviewer, Name: reviewer, State: "running", Current: true},
	}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "tmux")
	body := "#!/bin/sh\ncase $1 in\n" +
		"has-session) test \"$2\" = -t && test \"$3\" = \"=$EXPECTED_SESSION\";;\n" +
		"attach-session) test \"$2\" = -r && test \"$3\" = -t && test \"$4\" = \"=$EXPECTED_SESSION\" || exit 2; printf 'review-ready\\r\\n'; sleep 30;;\n" +
		"*) exit 2;;\nesac\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("EXPECTED_SESSION", reviewer)
	server := httptest.NewServer(newDashboardHandler(t.Context(), root, script))
	defer server.Close()
	dial := func(path, extraQuery string) (*websocket.Conn, *http.Response, error) {
		endpoint := "ws" + strings.TrimPrefix(server.URL, "http") + path + "?issue=23&attempt=2"
		if extraQuery != "" {
			endpoint += "&" + extraQuery
		}
		return websocket.Dial(t.Context(), endpoint, &websocket.DialOptions{HTTPHeader: http.Header{"Origin": []string{server.URL}}})
	}
	if connection, response, err := dial("/terminal", "role=future"); err == nil || response == nil || response.StatusCode != http.StatusNotFound {
		if connection != nil {
			connection.CloseNow()
		}
		t.Fatalf("unknown role response=%v err=%v", response, err)
	}
	connection, response, err := dial("/reviewer/terminal", "")
	if err != nil {
		t.Fatalf("review terminal dial response=%v err=%v", response, err)
	}
	defer connection.CloseNow()
	if err := connection.Write(t.Context(), websocket.MessageBinary, []byte("must not reach reviewer")); err != nil {
		t.Fatal(err)
	}
	for {
		_, _, err = connection.Read(t.Context())
		if err != nil {
			break
		}
	}
	if websocket.CloseStatus(err) != websocket.StatusPolicyViolation {
		t.Fatalf("reviewer input close=%v err=%v", websocket.CloseStatus(err), err)
	}

	status.Sessions[1].Name = "as-r-forged"
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	if _, err := (&dashboardServer{stateRoot: root}).projectedSession(issue, attempt, agentruntime.SessionRoleReviewer); err == nil {
		t.Fatal("tampered reviewer identity accepted")
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

func TestDashboardReviewerTerminalRequiresLiveSession(t *testing.T) {
	root := t.TempDir()
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 8, 1)
	reviewer, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 8, 1)
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 8, Attempt: 1, State: "active", Session: implementation, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: implementation, State: "completed"}, {Role: agentruntime.SessionRoleReviewer, Name: reviewer, State: "running", Current: true}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	script := filepath.Join(t.TempDir(), "tmux")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://127.0.0.1/reviewer/terminal?issue=8&attempt=1", nil)
	request.Header.Set("Origin", "http://127.0.0.1")
	response := httptest.NewRecorder()
	newDashboardHandler(t.Context(), root, script).ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("missing session status=%d body=%q", response.Code, response.Body.String())
	}
}
