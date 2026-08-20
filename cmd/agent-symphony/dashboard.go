package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
	"github.com/coder/websocket"
	"github.com/creack/pty"
)

const (
	maxDashboardStatusBytes = 4 << 20
	maxDashboardStateBytes  = 1 << 20
	maxTerminalInputBytes   = 64 << 10
	dashboardStateVersion   = 1
	dashboardSessionCookie  = "agent-symphony-confirmation"
	dashboardNonceHeader    = "X-Agent-Symphony-Confirmation-Nonce"
)

//go:embed all:dashboard/out
var dashboardFiles embed.FS

type dashboardServer struct {
	ctx          context.Context
	stateRoot    string
	tmux         string
	allowNet     bool
	password     string
	orchestrator orchestratoragent.Service
	messages     orchestratorMessageService
	cleanup      func(context.Context, string, agentruntime.Manifest) error
	recover      func(context.Context, int, int) error
	reconcile    func(context.Context) error
	mu           *sync.Mutex
	localMu      sync.Mutex
	sessionOnce  sync.Once
	sessionKey   [32]byte
	sessionErr   error
}

type orchestratorMessageService interface {
	MessageProposal(context.Context) (orchestratoragent.MessageProposal, error)
	ConsumeMessageProposal(context.Context, string) error
	ConfirmMessage(context.Context, orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error)
}

type dashboardHiddenAttempt struct {
	Repository string `json:"repository"`
	Issue      int    `json:"issue"`
	Attempt    int    `json:"attempt"`
	Reason     string `json:"reason"`
}

type dashboardState struct {
	Version int                      `json:"version"`
	Hidden  []dashboardHiddenAttempt `json:"hidden"`
}

func dashboardHandler(stateRoot string) http.Handler {
	return newDashboardHandler(context.Background(), stateRoot, "tmux")
}

func newDashboardHandler(ctx context.Context, stateRoot, tmux string) http.Handler {
	return newDashboardHandlerWithMutex(ctx, stateRoot, tmux, &sync.Mutex{})
}

func newDashboardHandlerWithMutex(ctx context.Context, stateRoot, tmux string, operationMu *sync.Mutex) http.Handler {
	return newDashboardHandlerWithOptions(ctx, stateRoot, tmux, operationMu, nil, nil, nil, false, "")
}

func newDashboardHandlerWithOptions(ctx context.Context, stateRoot, tmux string, operationMu *sync.Mutex, recover func(context.Context, int, int) error, reconcile func(context.Context) error, service orchestratoragent.Service, allowNet bool, password string) http.Handler {
	assets, err := fs.Sub(dashboardFiles, "dashboard/out")
	if err != nil {
		panic(err)
	}
	static := http.FileServer(http.FS(assets))
	server := &dashboardServer{ctx: ctx, stateRoot: stateRoot, tmux: tmux, allowNet: allowNet, password: password, orchestrator: service, recover: recover, reconcile: reconcile, mu: operationMu}
	server.messages, _ = service.(orchestratorMessageService)
	server.cleanup = server.cleanupAttempt
	return server.handler(static)
}

func (s *dashboardServer) handler(static http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		if !dashboardHostAllowed(r.Host, s.allowNet) {
			http.Error(w, "dashboard requires a loopback host", http.StatusForbidden)
			return
		}
		if !s.authenticate(w, r) {
			return
		}
		s.issueBrowserSession(w, r)
		if r.URL.Path == "/actions/archive" || r.URL.Path == "/actions/abandon" || r.URL.Path == "/actions/recover" {
			s.serveAction(w, r, strings.TrimPrefix(r.URL.Path, "/actions/"))
			return
		}
		if r.URL.Path == "/actions/reconcile" {
			s.serveReconcileAction(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/actions/orchestrator/") {
			s.serveOrchestratorAction(w, r, strings.TrimPrefix(r.URL.Path, "/actions/orchestrator/"))
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/status.json" {
			serveDashboardJSON(w, r, filepath.Join(s.stateRoot, "status.json"), maxDashboardStatusBytes, "status is not available yet", "status snapshot is unavailable")
			return
		}
		if r.URL.Path == "/dashboard-state.json" {
			s.serveState(w, r)
			return
		}
		if r.URL.Path == "/orchestrator.json" {
			s.serveOrchestratorStatus(w, r)
			return
		}
		if r.URL.Path == "/orchestrator/proposal.json" {
			s.serveOrchestratorProposal(w, r)
			return
		}
		if r.URL.Path == "/orchestrator/terminal" {
			s.serveOrchestratorTerminal(w, r)
			return
		}
		if r.URL.Path == "/terminal" {
			s.serveTerminal(w, r)
			return
		}
		static.ServeHTTP(w, r)
	})
}

func (s *dashboardServer) serveOrchestratorProposal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.Header().Set("Allow", "GET, HEAD")
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if s.password == "" {
		http.Error(w, "worker message confirmation requires dashboard authentication", http.StatusForbidden)
		return
	}
	if s.messages == nil {
		http.Error(w, "orchestrator message proposals are unavailable", http.StatusConflict)
		return
	}
	session, ok := s.browserSession(r)
	if !ok {
		http.Error(w, "worker message confirmation requires a dashboard browser session", http.StatusForbidden)
		return
	}
	proposal, err := s.messages.MessageProposal(r.Context())
	if errors.Is(err, orchestratoragent.ErrNoMessageProposal) {
		w.Header().Set("Cache-Control", "no-store")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		http.Error(w, "orchestrator message proposal is invalid", http.StatusUnprocessableEntity)
		return
	}
	body, _ := json.Marshal(proposal)
	w.Header().Set(dashboardNonceHeader, s.confirmationNonce(session, proposal.Binding))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func (s *dashboardServer) serveReconcileAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "action requires POST", http.StatusMethodNotAllowed)
		return
	}
	if !sameDashboardOrigin(r) {
		http.Error(w, "action requires the dashboard origin", http.StatusForbidden)
		return
	}
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || len(r.URL.Query()) != 0 {
		http.Error(w, "invalid reconciliation action", http.StatusBadRequest)
		return
	}
	if s.reconcile == nil {
		http.Error(w, "reconciliation is unavailable", http.StatusConflict)
		return
	}
	operationMu := s.mu
	if operationMu == nil {
		operationMu = &s.localMu
	}
	if !operationMu.TryLock() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "reconciliation is in progress", http.StatusServiceUnavailable)
		return
	}
	defer operationMu.Unlock()
	if err := s.reconcile(r.Context()); err != nil {
		http.Error(w, internalgithub.Redact(err.Error()), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *dashboardServer) serveOrchestratorStatus(w http.ResponseWriter, r *http.Request) {
	status := orchestratoragent.Status{Version: 1, UpdatedAt: time.Now().UTC(), State: "disabled"}
	if s.orchestrator != nil {
		var err error
		status, err = s.orchestrator.Status(r.Context())
		if err != nil {
			http.Error(w, "orchestrator status is unavailable", http.StatusServiceUnavailable)
			return
		}
		status.Diagnostic = internalgithub.Redact(status.Diagnostic)
	}
	body, err := json.Marshal(status)
	if err != nil {
		http.Error(w, "orchestrator status is unavailable", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func (s *dashboardServer) authenticate(w http.ResponseWriter, r *http.Request) bool {
	if s.password == "" {
		return true
	}
	username, password, ok := r.BasicAuth()
	want, got := sha256.Sum256([]byte(s.password)), sha256.Sum256([]byte(password))
	if !ok || username != "agent-symphony" || subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
		w.Header().Set("WWW-Authenticate", `Basic realm="Agent Symphony", charset="UTF-8"`)
		http.Error(w, "dashboard authentication required", http.StatusUnauthorized)
		return false
	}
	return true
}

func (s *dashboardServer) issueBrowserSession(w http.ResponseWriter, r *http.Request) {
	if s.password == "" || r.Method != http.MethodGet || r.Header.Get("Sec-Fetch-Mode") != "navigate" || r.Header.Get("Sec-Fetch-Dest") != "document" {
		return
	}
	key, ok := s.browserSessionKey()
	if !ok {
		return
	}
	value := make([]byte, 32)
	if _, err := rand.Read(value); err != nil {
		return
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(value)
	encoded := base64.RawURLEncoding.EncodeToString(value) + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
	w.Header().Set("Cache-Control", "no-store")
	http.SetCookie(w, &http.Cookie{Name: dashboardSessionCookie, Value: encoded, Path: "/", HttpOnly: true, SameSite: http.SameSiteStrictMode})
}

func (s *dashboardServer) browserSessionKey() ([]byte, bool) {
	s.sessionOnce.Do(func() { _, s.sessionErr = rand.Read(s.sessionKey[:]) })
	return s.sessionKey[:], s.sessionErr == nil
}

func (s *dashboardServer) browserSession(r *http.Request) (string, bool) {
	cookie, err := r.Cookie(dashboardSessionCookie)
	if err != nil {
		return "", false
	}
	value, signature, ok := strings.Cut(cookie.Value, ".")
	if !ok {
		return "", false
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	provided, signatureErr := base64.RawURLEncoding.DecodeString(signature)
	key, keyOK := s.browserSessionKey()
	if err != nil || signatureErr != nil || !keyOK || len(raw) != 32 {
		return "", false
	}
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write(raw)
	if !hmac.Equal(provided, mac.Sum(nil)) {
		return "", false
	}
	return value, true
}

func (s *dashboardServer) confirmationNonce(session, binding string) string {
	key, ok := s.browserSessionKey()
	if !ok {
		return ""
	}
	mac := hmac.New(sha256.New, key)
	_, _ = io.WriteString(mac, "confirm\x00"+session+"\x00"+binding)
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func serveDashboardJSON(w http.ResponseWriter, r *http.Request, path string, limit int64, missing, unavailable string) {
	body, err := readDashboardFile(path, limit)
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, missing, http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, unavailable, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func readDashboardStatus(path string) ([]byte, error) {
	return readDashboardFile(path, maxDashboardStatusBytes)
}

func readDashboardFile(path string, limit int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, os.ErrNotExist
	}
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > limit {
		return nil, errors.New("unsafe dashboard file")
	}
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(info, opened) {
		return nil, errors.New("status snapshot changed while opening")
	}
	body, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil || int64(len(body)) != opened.Size() || int64(len(body)) > limit {
		return nil, errors.New("dashboard file changed while reading")
	}
	return body, nil
}

func (s *dashboardServer) serveState(w http.ResponseWriter, r *http.Request) {
	state, err := s.readState()
	if err != nil {
		http.Error(w, "dashboard state is unavailable", http.StatusInternalServerError)
		return
	}
	body, _ := json.Marshal(state)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func (s *dashboardServer) readState() (dashboardState, error) {
	state := dashboardState{Version: dashboardStateVersion, Hidden: []dashboardHiddenAttempt{}}
	body, err := readDashboardFile(filepath.Join(s.stateRoot, "dashboard-state.json"), maxDashboardStateBytes)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return dashboardState{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF || state.Version != dashboardStateVersion || len(state.Hidden) > 10_000 {
		return dashboardState{}, errors.New("invalid dashboard state")
	}
	for _, hidden := range state.Hidden {
		if hidden.Repository == "" || hidden.Issue < 1 || hidden.Attempt < 1 || (hidden.Reason != "archived" && hidden.Reason != "abandoned") {
			return dashboardState{}, errors.New("invalid dashboard state entry")
		}
	}
	return state, nil
}

func (s *dashboardServer) writeState(state dashboardState) error {
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil || len(body) > maxDashboardStateBytes {
		return errors.New("dashboard state is too large")
	}
	path := filepath.Join(s.stateRoot, "dashboard-state.json")
	root, err := filepath.EvalSymlinks(s.stateRoot)
	if err != nil || root != filepath.Clean(s.stateRoot) {
		return errors.New("dashboard state root is unsafe")
	}
	if info, statErr := os.Lstat(path); statErr == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return errors.New("dashboard state file is unsafe")
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	temporary, err := os.CreateTemp(root, ".dashboard-state-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(body, '\n')); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	return os.Rename(name, path)
}

func (s *dashboardServer) serveAction(w http.ResponseWriter, r *http.Request, action string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "action requires POST", http.StatusMethodNotAllowed)
		return
	}
	if !sameDashboardOrigin(r) {
		http.Error(w, "action requires the dashboard origin", http.StatusForbidden)
		return
	}
	issue, issueErr := strconv.Atoi(r.URL.Query().Get("issue"))
	attempt, attemptErr := strconv.Atoi(r.URL.Query().Get("attempt"))
	if issueErr != nil || attemptErr != nil || issue < 1 || attempt < 1 || (action != "archive" && action != "abandon" && action != "recover") {
		http.Error(w, "invalid action", http.StatusBadRequest)
		return
	}
	operationMu := s.mu
	if operationMu == nil {
		operationMu = &s.localMu
	}
	if !operationMu.TryLock() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "reconciliation is in progress", http.StatusServiceUnavailable)
		return
	}
	defer operationMu.Unlock()
	status, err := s.projectedStatus(issue, attempt)
	if action == "recover" {
		if err != nil || !status.Retryable || status.PR > 0 || s.recover == nil {
			http.Error(w, "attempt is not eligible for recovery", http.StatusConflict)
			return
		}
		if err := s.recover(r.Context(), issue, attempt); err != nil {
			http.Error(w, internalgithub.Redact(err.Error()), http.StatusConflict)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = io.WriteString(w, `{"ok":true}`)
		return
	}
	wantState := "completed"
	if action == "abandon" {
		wantState = "orphaned"
	}
	if err != nil || status.State != wantState {
		http.Error(w, "attempt is not eligible for this action", http.StatusConflict)
		return
	}
	runtimeState := &agentruntime.Runtime{Root: productionAttemptRoot(s.stateRoot), StateRoot: s.stateRoot}
	manifests, err := runtimeState.Discover()
	if err != nil {
		http.Error(w, "local attempt state is unavailable", http.StatusConflict)
		return
	}
	matches := make([]agentruntime.Manifest, 0, 1)
	for _, manifest := range manifests {
		if manifest.Repository == status.Repository && manifest.Issue == issue && manifest.Attempt == attempt {
			matches = append(matches, manifest)
		}
	}
	if len(matches) != 1 || status.Branch != matches[0].Branch || status.Worktree != matches[0].Worktree || status.Session != matches[0].Session {
		http.Error(w, "local attempt identity does not match the projection", http.StatusConflict)
		return
	}
	if action == "archive" && matches[0].State != "completed" {
		http.Error(w, "completed projection does not have a completed local attempt", http.StatusConflict)
		return
	}
	if err := s.cleanup(r.Context(), action, matches[0]); err != nil {
		http.Error(w, "attempt cleanup was refused", http.StatusConflict)
		return
	}
	if action == "abandon" {
		if err := runtimeState.Forget(matches[0]); err != nil {
			http.Error(w, "attempt resources were cleaned but its record could not be removed", http.StatusInternalServerError)
			return
		}
	}
	state, err := s.readState()
	if err != nil {
		http.Error(w, "attempt resources were cleaned but dashboard state could not be updated", http.StatusInternalServerError)
		return
	}
	reason := "archived"
	if action == "abandon" {
		reason = "abandoned"
	}
	hidden := dashboardHiddenAttempt{Repository: status.Repository, Issue: issue, Attempt: attempt, Reason: reason}
	if !slices.ContainsFunc(state.Hidden, func(entry dashboardHiddenAttempt) bool {
		return entry.Repository == hidden.Repository && entry.Issue == hidden.Issue && entry.Attempt == hidden.Attempt
	}) {
		state.Hidden = append(state.Hidden, hidden)
	}
	if err := s.writeState(state); err != nil {
		http.Error(w, "attempt resources were cleaned but dashboard state could not be updated", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, `{"ok":true}`)
}

func (s *dashboardServer) cleanupAttempt(ctx context.Context, action string, manifest agentruntime.Manifest) error {
	operation := "cleanup"
	if action == "abandon" {
		operation = "abandon"
	}
	body, _ := json.Marshal(manifest)
	_, err := implementationBoundary(s.stateRoot).call(ctx, operation, agentruntime.Command{Stdin: strings.NewReader(string(body))})
	return err
}

func (s *dashboardServer) serveOrchestratorAction(w http.ResponseWriter, r *http.Request, action string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "action requires POST", http.StatusMethodNotAllowed)
		return
	}
	if !sameDashboardOrigin(r) {
		http.Error(w, "action requires the dashboard origin", http.StatusForbidden)
		return
	}
	if action == "message-confirm" || action == "message-cancel" {
		s.serveOrchestratorMessageAction(w, r, action)
		return
	}
	if r.ContentLength != 0 || len(r.TransferEncoding) != 0 || !slices.Contains([]string{"recover", "clear", "rebuild", "investigate"}, action) {
		http.Error(w, "invalid orchestrator action", http.StatusBadRequest)
		return
	}
	if s.orchestrator == nil {
		http.Error(w, "orchestrator is disabled", http.StatusConflict)
		return
	}
	issue, attempt := 0, 0
	if action == "investigate" {
		var issueErr, attemptErr error
		issue, issueErr = strconv.Atoi(r.URL.Query().Get("issue"))
		attempt, attemptErr = strconv.Atoi(r.URL.Query().Get("attempt"))
		query := r.URL.Query()
		if issueErr != nil || attemptErr != nil || issue < 1 || attempt < 1 || len(query) != 2 || len(query["issue"]) != 1 || len(query["attempt"]) != 1 {
			http.Error(w, "invalid orchestrator action", http.StatusBadRequest)
			return
		}
	} else if len(r.URL.Query()) != 0 {
		http.Error(w, "invalid orchestrator action", http.StatusBadRequest)
		return
	}
	operationMu := s.mu
	if operationMu == nil {
		operationMu = &s.localMu
	}
	if !operationMu.TryLock() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "reconciliation is in progress", http.StatusServiceUnavailable)
		return
	}
	defer operationMu.Unlock()

	var result orchestratoragent.Status
	var err error
	switch action {
	case "recover":
		result, err = s.orchestrator.Recover(r.Context())
	case "clear":
		result, err = s.orchestrator.Clear(r.Context())
	case "rebuild":
		result, err = s.orchestrator.Rebuild(r.Context())
	case "investigate":
		orchestratorStatus, statusErr := s.orchestrator.Status(r.Context())
		if statusErr != nil || !orchestratorStatus.Enabled || orchestratorStatus.State != "running" {
			http.Error(w, "orchestrator is not running", http.StatusConflict)
			return
		}
		status, statusErr := s.projectedStatus(issue, attempt)
		if statusErr != nil || !orchestratorAttentionState(status.State) {
			http.Error(w, "attempt is not eligible for investigation", http.StatusConflict)
			return
		}
		result, err = s.orchestrator.Investigate(r.Context(), status.Issue, status.Attempt)
	}
	if err != nil {
		http.Error(w, "orchestrator action was refused", http.StatusConflict)
		return
	}
	result.Diagnostic = internalgithub.Redact(result.Diagnostic)
	body, _ := json.Marshal(struct {
		OK     bool                     `json:"ok"`
		Status orchestratoragent.Status `json:"status"`
	}{true, result})
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *dashboardServer) serveOrchestratorMessageAction(w http.ResponseWriter, r *http.Request, action string) {
	if s.password == "" {
		http.Error(w, "worker message confirmation requires dashboard authentication", http.StatusForbidden)
		return
	}
	if s.messages == nil || r.Header.Get("Content-Type") != "application/json" || len(r.URL.Query()) != 0 || len(r.TransferEncoding) != 0 || r.ContentLength <= 0 || r.ContentLength > maxTerminalInputBytes {
		http.Error(w, "invalid orchestrator message action", http.StatusBadRequest)
		return
	}
	var submitted orchestratoragent.MessageProposal
	decoder := json.NewDecoder(io.LimitReader(r.Body, maxTerminalInputBytes+1))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&submitted) != nil || decoder.Decode(&struct{}{}) != io.EOF || submitted.Binding == "" {
		http.Error(w, "invalid orchestrator message action", http.StatusBadRequest)
		return
	}
	current, err := s.messages.MessageProposal(r.Context())
	if err != nil || current != submitted {
		http.Error(w, "orchestrator message proposal changed before confirmation", http.StatusConflict)
		return
	}
	session, ok := s.browserSession(r)
	wantNonce := s.confirmationNonce(session, submitted.Binding)
	gotNonce := r.Header.Get(dashboardNonceHeader)
	if !ok || wantNonce == "" || subtle.ConstantTimeCompare([]byte(wantNonce), []byte(gotNonce)) != 1 {
		http.Error(w, "worker message confirmation requires a dashboard browser nonce", http.StatusForbidden)
		return
	}
	operationMu := s.mu
	if operationMu == nil {
		operationMu = &s.localMu
	}
	if !operationMu.TryLock() {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "reconciliation is in progress", http.StatusServiceUnavailable)
		return
	}
	defer operationMu.Unlock()
	// Re-read under the coordinator operation lock so confirmation binds the
	// exact bytes the operator reviewed, not a replaced proposal.
	current, err = s.messages.MessageProposal(r.Context())
	if err != nil || current != submitted {
		http.Error(w, "orchestrator message proposal changed before confirmation", http.StatusConflict)
		return
	}
	if action == "message-cancel" {
		if err := s.messages.ConsumeMessageProposal(r.Context(), submitted.Binding); err != nil {
			http.Error(w, "orchestrator message cancellation failed", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusNoContent)
		return
	}
	message, err := s.messages.ConfirmMessage(r.Context(), submitted)
	if err != nil {
		http.Error(w, internalgithub.Redact(err.Error()), http.StatusConflict)
		return
	}
	if err := s.messages.ConsumeMessageProposal(r.Context(), submitted.Binding); err != nil {
		http.Error(w, "message was recorded, but the proposal could not be cleared", http.StatusInternalServerError)
		return
	}
	body, _ := json.Marshal(struct {
		ID         string `json:"id"`
		Repository string `json:"repository"`
		Issue      int    `json:"issue"`
		Attempt    int    `json:"attempt"`
		State      string `json:"state"`
	}{message.ID, message.Repository, message.Issue, message.Attempt, message.State})
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func orchestratorAttentionState(state string) bool {
	return slices.Contains([]string{"blocked", "failed", "conflicting", "orphaned"}, state)
}

func sameDashboardOrigin(r *http.Request) bool {
	origin, err := url.Parse(r.Header.Get("Origin"))
	if err != nil || (origin.Scheme != "http" && origin.Scheme != "https") {
		return false
	}
	return strings.EqualFold(origin.Host, r.Host) ||
		(strings.TrimSpace(r.Header.Get("Tailscale-User-Login")) != "" && dashboardRequestLoopback(r) &&
			strings.EqualFold(origin.Host, strings.TrimSpace(r.Header.Get("X-Forwarded-Host"))) &&
			strings.EqualFold(origin.Scheme, strings.TrimSpace(r.Header.Get("X-Forwarded-Proto"))))
}

func dashboardHostAllowed(value string, allowNet bool) bool {
	if allowNet {
		return strings.TrimSpace(value) != ""
	}
	host := value
	if parsed, _, err := net.SplitHostPort(value); err == nil {
		host = parsed
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func dashboardRequestLoopback(r *http.Request) bool {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}
	ip := net.ParseIP(strings.Trim(host, "[]"))
	if ip == nil || !ip.IsLoopback() {
		return false
	}
	return dashboardHostAllowed(r.Host, false) || strings.TrimSpace(r.Header.Get("Tailscale-User-Login")) != ""
}

func (s *dashboardServer) serveTerminal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "terminal requires GET", http.StatusMethodNotAllowed)
		return
	}
	if !sameDashboardOrigin(r) {
		http.Error(w, "terminal requires the dashboard origin", http.StatusForbidden)
		return
	}
	query := r.URL.Query()
	issue, issueErr := strconv.Atoi(query.Get("issue"))
	attempt, attemptErr := strconv.Atoi(query.Get("attempt"))
	role := query.Get("role")
	if role == "" {
		role = agentruntime.SessionRoleImplementation
	}
	session, err := s.projectedSession(issue, attempt, role)
	wantKeys := 3
	if query.Get("role") == "" {
		wantKeys = 2
	}
	if issueErr != nil || attemptErr != nil || err != nil || len(query) != wantKeys || len(query["issue"]) != 1 || len(query["attempt"]) != 1 || query.Get("role") != "" && len(query["role"]) != 1 {
		http.Error(w, "terminal session is not available", http.StatusNotFound)
		return
	}
	s.serveTerminalSession(w, r, session.Name, role == agentruntime.SessionRoleReviewer)
}

func (s *dashboardServer) serveOrchestratorTerminal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "terminal requires GET", http.StatusMethodNotAllowed)
		return
	}
	if !sameDashboardOrigin(r) {
		http.Error(w, "terminal requires the dashboard origin", http.StatusForbidden)
		return
	}
	if !dashboardRequestLoopback(r) {
		http.Error(w, "orchestrator terminal requires loopback access", http.StatusForbidden)
		return
	}
	if s.orchestrator == nil {
		http.Error(w, "orchestrator terminal is not available", http.StatusNotFound)
		return
	}
	target, err := s.orchestrator.AttachTarget(r.Context())
	if err != nil || target.Session == "" || strings.ContainsAny(target.Session, "\x00\r\n") {
		http.Error(w, "orchestrator terminal is not available", http.StatusConflict)
		return
	}
	s.serveTerminalSession(w, r, target.Session, false)
}

func (s *dashboardServer) serveTerminalSession(w http.ResponseWriter, r *http.Request, session string, readOnly bool) {
	if exec.CommandContext(r.Context(), s.tmux, "has-session", "-t", "="+session).Run() != nil {
		http.Error(w, "terminal session is not running", http.StatusConflict)
		return
	}
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(maxTerminalInputBytes)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	args := []string{"attach-session"}
	if readOnly {
		args = append(args, "-r")
	}
	command := exec.CommandContext(ctx, s.tmux, append(args, "-t", "="+session)...)
	command.Env = append(os.Environ(), "TERM=xterm-256color")
	terminal, err := pty.StartWithSize(command, &pty.Winsize{Rows: 24, Cols: 80})
	if err != nil {
		_ = conn.Close(websocket.StatusInternalError, "cannot attach terminal")
		return
	}
	defer terminal.Close()
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer cancel()
		defer conn.CloseNow()
		buffer := make([]byte, 32<<10)
		for {
			n, readErr := terminal.Read(buffer)
			if n > 0 && conn.Write(ctx, websocket.MessageBinary, buffer[:n]) != nil {
				return
			}
			if readErr != nil {
				return
			}
		}
	}()
	for {
		kind, message, readErr := conn.Read(ctx)
		if readErr != nil {
			break
		}
		if kind == websocket.MessageBinary {
			if readOnly {
				_ = conn.Close(websocket.StatusPolicyViolation, "reviewer terminal is read-only")
				break
			}
			if _, err := terminal.Write(message); err != nil {
				break
			}
			continue
		}
		var resize struct {
			Type       string `json:"type"`
			Cols, Rows uint16
		}
		decoder := json.NewDecoder(strings.NewReader(string(message)))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&resize) != nil || decoder.Decode(&struct{}{}) != io.EOF || resize.Type != "resize" || resize.Cols < 2 || resize.Rows < 2 || resize.Cols > 500 || resize.Rows > 300 {
			_ = conn.Close(websocket.StatusPolicyViolation, "invalid terminal message")
			break
		}
		if pty.Setsize(terminal, &pty.Winsize{Cols: resize.Cols, Rows: resize.Rows}) != nil {
			break
		}
	}
	cancel()
	_ = terminal.Close()
	<-done
	_ = command.Wait()
}

func (s *dashboardServer) projectedStatus(issue, attempt int) (orchestrator.RecoveryStatus, error) {
	if issue < 1 || attempt < 1 {
		return orchestrator.RecoveryStatus{}, errors.New("invalid attempt")
	}
	body, err := readDashboardStatus(filepath.Join(s.stateRoot, "status.json"))
	if err != nil {
		return orchestrator.RecoveryStatus{}, err
	}
	var snapshot struct {
		UpdatedAt time.Time                     `json:"updated_at"`
		Statuses  []orchestrator.RecoveryStatus `json:"statuses"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&snapshot) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return orchestrator.RecoveryStatus{}, errors.New("invalid status snapshot")
	}
	var found *orchestrator.RecoveryStatus
	for i := range snapshot.Statuses {
		status := snapshot.Statuses[i]
		expected, nameErr := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, status.Repository, issue, attempt)
		validSessions := nameErr == nil && status.Session == expected
		seen := map[string]bool{}
		for _, session := range status.Sessions {
			want, sessionErr := agentruntime.AttemptSessionName(session.Role, status.Repository, issue, attempt)
			if sessionErr != nil || session.Name != want || session.State == "" || seen[session.Role] {
				validSessions = false
				break
			}
			seen[session.Role] = true
		}
		if len(status.Sessions) > 0 && !seen[agentruntime.SessionRoleImplementation] {
			validSessions = false
		}
		if status.Issue == issue && status.Attempt == attempt && validSessions {
			if found != nil {
				return orchestrator.RecoveryStatus{}, errors.New("ambiguous attempt")
			}
			found = &snapshot.Statuses[i]
		}
	}
	if found != nil {
		return *found, nil
	}
	return orchestrator.RecoveryStatus{}, errors.New("attempt not found")
}

func (s *dashboardServer) projectedSession(issue, attempt int, role string) (orchestrator.AttemptSession, error) {
	status, err := s.projectedStatus(issue, attempt)
	if err != nil {
		return orchestrator.AttemptSession{}, err
	}
	want, err := agentruntime.AttemptSessionName(role, status.Repository, issue, attempt)
	if err != nil {
		return orchestrator.AttemptSession{}, err
	}
	if len(status.Sessions) == 0 && role == agentruntime.SessionRoleImplementation {
		return orchestrator.AttemptSession{Role: role, Name: status.Session, State: status.State}, nil
	}
	index := slices.IndexFunc(status.Sessions, func(session orchestrator.AttemptSession) bool { return session.Role == role })
	if index < 0 || status.Sessions[index].Name != want {
		return orchestrator.AttemptSession{}, errors.New("attempt session not found")
	}
	return status.Sessions[index], nil
}

func (s *dashboardServer) terminalStatus(issue, attempt int) (orchestrator.RecoveryStatus, error) {
	return s.projectedStatus(issue, attempt)
}

func startDashboard(ctx context.Context, address, stateRoot string, operationMu *sync.Mutex, recover func(context.Context, int, int) error, reconcile func(context.Context) error, service orchestratoragent.Service, allowNet bool, password string, log io.Writer) (string, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("dashboard address: %w", err)
	}
	loopback := host == "localhost"
	if !loopback {
		ip := net.ParseIP(host)
		loopback = ip != nil && ip.IsLoopback()
		if !loopback && !allowNet {
			return "", errors.New("dashboard address must use localhost or a loopback IP")
		}
	}
	if allowNet && password == "" {
		return "", errors.New("dashboard password is required with --allow-unsafe-dashboard-network")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", fmt.Errorf("listen for dashboard on %s: %w", address, err)
	}
	server := &http.Server{Handler: newDashboardHandlerWithOptions(ctx, stateRoot, "tmux", operationMu, recover, reconcile, service, allowNet, password), ReadHeaderTimeout: 5 * time.Second}
	if allowNet {
		fmt.Fprintln(log, "WARNING: unsafe dashboard network access enabled; direct HTTP is unencrypted, the password and session data are exposed in transit, and anyone with the password can use terminals and cleanup controls")
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
	}()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(log, "dashboard: "+err.Error())
		}
	}()
	return "http://" + listener.Addr().String(), nil
}
