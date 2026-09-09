package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

const (
	controlVersion      = 1
	maxControlBodyBytes = 16 << 10
)

var controlRequestIDPattern = regexp.MustCompile(`^[A-Za-z0-9._:-]{1,128}$`)

type controlRequest struct {
	Version    int    `json:"version"`
	RequestID  string `json:"request_id"`
	Repository string `json:"repository"`
	Action     string `json:"action"`
	Issue      int    `json:"issue,omitempty"`
	Attempt    int    `json:"attempt,omitempty"`
	Confirm    bool   `json:"confirm,omitempty"`
}

type controlResult struct {
	Version   int             `json:"version"`
	RequestID string          `json:"request_id"`
	Action    string          `json:"action"`
	OK        bool            `json:"ok"`
	Retryable bool            `json:"retryable,omitempty"`
	Status    int             `json:"status"`
	Data      json.RawMessage `json:"data,omitempty"`
	Error     string          `json:"error,omitempty"`
}

func controlSocketPath(stateRoot string) string {
	digest := sha256.Sum256([]byte(filepath.Clean(stateRoot)))
	return filepath.Join("/tmp", "agent-symphony-control-"+hex.EncodeToString(digest[:12])+".sock")
}

func newControlRequestID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func startControlServer(ctx context.Context, project *dashboardServer, log io.Writer) error {
	path := controlSocketPath(project.stateRoot)
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 || !ownedByCurrentUser(info) {
			return errors.New("running-daemon control path is unsafe")
		}
		if err := os.Remove(path); err != nil {
			return fmt.Errorf("remove stale running-daemon control socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect running-daemon control socket: %w", err)
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return fmt.Errorf("listen for running-daemon control: %w", err)
	}
	if unix, ok := listener.(*net.UnixListener); ok {
		unix.SetUnlinkOnClose(false)
	}
	if err := os.Chmod(path, 0o600); err != nil {
		_ = listener.Close()
		_ = os.Remove(path)
		return fmt.Errorf("secure running-daemon control socket: %w", err)
	}
	socketInfo, err := os.Lstat(path)
	if err != nil || socketInfo.Mode()&os.ModeSocket == 0 || !ownedByCurrentUser(socketInfo) {
		_ = listener.Close()
		_ = os.Remove(path)
		return errors.New("running-daemon control socket is unsafe")
	}
	server := &http.Server{Handler: controlHandler(project), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(shutdown)
		_ = listener.Close()
	}()
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(log, "control: "+internalgithub.Redact(err.Error()))
		}
	}()
	return nil
}

func controlHandler(project *dashboardServer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		if r.Method != http.MethodPost || r.URL.Path != "/v1/action" || len(r.URL.Query()) != 0 {
			writeControlResult(w, controlResult{Version: controlVersion, Status: http.StatusBadRequest, Error: "invalid control request"})
			return
		}
		var request controlRequest
		r.Body = http.MaxBytesReader(w, r.Body, maxControlBodyBytes)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || !validControlRequest(request, project.repository) {
			writeControlResult(w, controlResult{Version: controlVersion, RequestID: request.RequestID, Action: request.Action, Status: http.StatusBadRequest, Error: "invalid control request"})
			return
		}
		response := performDashboardControl(r.Context(), project, request)
		response.Version, response.RequestID, response.Action = controlVersion, request.RequestID, request.Action
		writeControlResult(w, response)
	})
}

func validControlRequest(request controlRequest, repository string) bool {
	if request.Version != controlVersion || request.Repository != repository || !controlRequestIDPattern.MatchString(request.RequestID) {
		return false
	}
	switch request.Action {
	case "reconcile", "orchestrator-recover", "orchestrator-clear", "orchestrator-rebuild", "orchestrator-session":
		return request.Issue == 0 && request.Attempt == 0 && !request.Confirm
	case "recover", "review-plan", "dismiss", "orchestrator-investigate":
		return request.Issue > 0 && request.Attempt > 0 && !request.Confirm
	case "archive", "abandon":
		return request.Issue > 0 && request.Attempt > 0 && request.Confirm
	default:
		return false
	}
}

func performDashboardControl(ctx context.Context, project *dashboardServer, request controlRequest) controlResult {
	path := "/actions/" + request.Action
	if strings.HasPrefix(request.Action, "orchestrator-") {
		path = "/actions/orchestrator/" + strings.TrimPrefix(request.Action, "orchestrator-")
	}
	if request.Action == "orchestrator-session" {
		if project.orchestrator == nil {
			return controlResult{Status: http.StatusConflict, Error: "orchestrator is disabled"}
		}
		target, err := project.orchestrator.AttachTarget(ctx)
		if err != nil || target.Session == "" || strings.ContainsAny(target.Session, "\x00\r\n") {
			return controlResult{Status: http.StatusConflict, Error: "orchestrator terminal is not available"}
		}
		body, _ := json.Marshal(target)
		return controlResult{OK: true, Status: http.StatusOK, Data: body}
	}
	query := url.Values{}
	if request.Issue > 0 {
		query.Set("repository", request.Repository)
		query.Set("issue", strconv.Itoa(request.Issue))
		query.Set("attempt", strconv.Itoa(request.Attempt))
		path += "?" + query.Encode()
	}
	dashboardRequest := httptest.NewRequestWithContext(ctx, http.MethodPost, "http://localhost"+path, nil)
	dashboardRequest.Host = "localhost"
	dashboardRequest.Header.Set("Origin", "http://localhost")
	if project.password != "" {
		dashboardRequest.SetBasicAuth("agent-symphony", project.password)
	}
	recorder := httptest.NewRecorder()
	project.handler(http.NotFoundHandler()).ServeHTTP(recorder, dashboardRequest)
	body := bytes.TrimSpace(recorder.Body.Bytes())
	result := controlResult{OK: recorder.Code >= 200 && recorder.Code < 300, Retryable: recorder.Code == http.StatusServiceUnavailable, Status: recorder.Code}
	if result.OK {
		if json.Valid(body) {
			result.Data = append(json.RawMessage(nil), body...)
		}
		return result
	}
	result.Error = internalgithub.Redact(strings.TrimSpace(string(body)))
	if result.Error == "" {
		result.Error = http.StatusText(recorder.Code)
	}
	return result
}

func writeControlResult(w http.ResponseWriter, result controlResult) {
	status := result.Status
	if result.Version == controlVersion && result.RequestID != "" && result.Action != "" {
		status = http.StatusOK
	}
	if status < 100 {
		status = http.StatusInternalServerError
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(result)
}

func callRunningDaemon(ctx context.Context, stateRoot string, request controlRequest) (controlResult, error) {
	rootInfo, err := os.Lstat(stateRoot)
	if err != nil || rootInfo.Mode()&os.ModeSymlink != 0 || !rootInfo.IsDir() || !ownedByCurrentUser(rootInfo) {
		return controlResult{}, errors.New("runtime state root is unsafe")
	}
	identity, err := readDeploymentIdentity(stateRoot)
	if err != nil {
		return controlResult{}, fmt.Errorf("read deployment identity: %w", err)
	}
	if identity.Repository != request.Repository {
		return controlResult{}, fmt.Errorf("runtime state is bound to project %s, not %s", identity.Repository, request.Repository)
	}
	path := controlSocketPath(stateRoot)
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSymlink != 0 || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm()&0o077 != 0 || !ownedByCurrentUser(info) {
		return controlResult{}, errors.New("running-daemon control socket is unavailable or unsafe")
	}
	body, _ := json.Marshal(request)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://unix/v1/action", bytes.NewReader(body))
	if err != nil {
		return controlResult{}, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := client.Do(httpRequest)
	if err != nil {
		return controlResult{}, fmt.Errorf("contact running daemon: %w", err)
	}
	defer response.Body.Close()
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxControlBodyBytes+1))
	decoder.DisallowUnknownFields()
	var result controlResult
	if response.StatusCode != http.StatusOK || decoder.Decode(&result) != nil || decoder.Decode(&struct{}{}) != io.EOF || result.Version != controlVersion || result.RequestID != request.RequestID || result.Action != request.Action || result.Status < 100 {
		return controlResult{}, errors.New("running daemon returned an invalid control result")
	}
	return result, nil
}

func ownedByCurrentUser(info os.FileInfo) bool {
	stat, ok := info.Sys().(*syscall.Stat_t)
	return !ok || int(stat.Uid) == os.Geteuid()
}
