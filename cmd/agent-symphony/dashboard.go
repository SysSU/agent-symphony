package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
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
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
	"github.com/coder/websocket"
	"github.com/creack/pty"
)

const (
	maxDashboardStatusBytes = 4 << 20
	maxDashboardStateBytes  = 1 << 20
	maxTerminalInputBytes   = 64 << 10
	dashboardStateVersion   = 1
)

//go:embed all:dashboard/out
var dashboardFiles embed.FS

type dashboardServer struct {
	ctx       context.Context
	stateRoot string
	tmux      string
	allowNet  bool
	password  string
	cleanup   func(context.Context, string, agentruntime.Manifest) error
	mu        *sync.Mutex
	localMu   sync.Mutex
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
	return newDashboardHandlerWithOptions(ctx, stateRoot, tmux, operationMu, false, "")
}

func newDashboardHandlerWithOptions(ctx context.Context, stateRoot, tmux string, operationMu *sync.Mutex, allowNet bool, password string) http.Handler {
	assets, err := fs.Sub(dashboardFiles, "dashboard/out")
	if err != nil {
		panic(err)
	}
	static := http.FileServer(http.FS(assets))
	server := &dashboardServer{ctx: ctx, stateRoot: stateRoot, tmux: tmux, allowNet: allowNet, password: password, mu: operationMu}
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
		if r.URL.Path == "/actions/archive" || r.URL.Path == "/actions/abandon" {
			s.serveAction(w, r, strings.TrimPrefix(r.URL.Path, "/actions/"))
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
		if r.URL.Path == "/terminal" {
			s.serveTerminal(w, r)
			return
		}
		static.ServeHTTP(w, r)
	})
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
	if issueErr != nil || attemptErr != nil || issue < 1 || attempt < 1 || (action != "archive" && action != "abandon") {
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

func sameDashboardOrigin(r *http.Request) bool {
	origin, err := url.Parse(r.Header.Get("Origin"))
	return err == nil && origin.Scheme == "http" && strings.EqualFold(origin.Host, r.Host)
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

func (s *dashboardServer) serveTerminal(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "terminal requires GET", http.StatusMethodNotAllowed)
		return
	}
	if !sameDashboardOrigin(r) {
		http.Error(w, "terminal requires the dashboard origin", http.StatusForbidden)
		return
	}
	issue, issueErr := strconv.Atoi(r.URL.Query().Get("issue"))
	attempt, attemptErr := strconv.Atoi(r.URL.Query().Get("attempt"))
	status, err := s.projectedStatus(issue, attempt)
	if issueErr != nil || attemptErr != nil || err != nil {
		http.Error(w, "terminal session is not available", http.StatusNotFound)
		return
	}
	if exec.CommandContext(r.Context(), s.tmux, "has-session", "-t", "="+status.Session).Run() != nil {
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
	command := exec.CommandContext(ctx, s.tmux, "attach-session", "-t", "="+status.Session)
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
		expected := fmt.Sprintf("as-%s-%d-%d", internalgithub.RepositoryIdentifier(status.Repository), issue, attempt)
		if status.Issue == issue && status.Attempt == attempt && status.Session == expected {
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

func (s *dashboardServer) terminalStatus(issue, attempt int) (orchestrator.RecoveryStatus, error) {
	return s.projectedStatus(issue, attempt)
}

func startDashboard(ctx context.Context, address, stateRoot string, operationMu *sync.Mutex, allowNet bool, password string, log io.Writer) (string, error) {
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
		return "", errors.New("--dashboard-password is required with --allow-unsafe-dashboard-network")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", fmt.Errorf("listen for dashboard on %s: %w", address, err)
	}
	server := &http.Server{Handler: newDashboardHandlerWithOptions(ctx, stateRoot, "tmux", operationMu, allowNet, password), ReadHeaderTimeout: 5 * time.Second}
	if allowNet {
		fmt.Fprintln(log, "WARNING: unsafe dashboard network access enabled; direct HTTP is unencrypted, the password may be visible in process listings, and anyone with it can use terminals and cleanup controls")
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
