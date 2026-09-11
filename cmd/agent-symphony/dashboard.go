package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"embed"
	"encoding/hex"
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
	removalStateVersion     = 1
)

//go:embed all:dashboard/out
var dashboardFiles embed.FS

type dashboardServer struct {
	ctx          context.Context
	stateRoot    string
	repository   string
	peerProjects []string
	tmux         string
	allowNet     bool
	password     string
	orchestrator orchestratoragent.Service
	reconcile    func(context.Context) error
	issueClosed  func(context.Context, string, int) (bool, error)
	operator     *operatorMutationService
}

type dashboardHiddenAttempt struct {
	Repository string `json:"repository"`
	Issue      int    `json:"issue"`
	Attempt    int    `json:"attempt"`
	Reason     string `json:"reason"`
}

type dashboardState struct {
	Version       int                      `json:"version"`
	OwnerRevision uint64                   `json:"owner_revision,omitempty"`
	Hidden        []dashboardHiddenAttempt `json:"hidden"`
}

type dashboardRemovalIntent struct {
	Manifest       agentruntime.Manifest `json:"manifest"`
	PublishedHead  string                `json:"published_head"`
	CleanupStarted bool                  `json:"cleanup_started,omitempty"`
}

type dashboardRemovalState struct {
	Version int                      `json:"version"`
	Intents []dashboardRemovalIntent `json:"intents"`
}

type dashboardStatusSnapshot struct {
	UpdatedAt             time.Time                     `json:"updated_at"`
	OwnerEpoch            uint64                        `json:"owner_epoch,omitempty"`
	OwnerRevision         uint64                        `json:"owner_revision,omitempty"`
	Statuses              []orchestrator.RecoveryStatus `json:"statuses"`
	ReconciliationError   string                        `json:"reconciliation_error,omitempty"`
	ReconciliationErrorAt time.Time                     `json:"reconciliation_error_at,omitzero"`
}

type dashboardProject struct {
	Version    int                      `json:"version"`
	Repository string                   `json:"repository,omitempty"`
	URL        string                   `json:"url,omitempty"`
	Local      bool                     `json:"local,omitempty"`
	Snapshot   *dashboardStatusSnapshot `json:"snapshot,omitempty"`
	State      *dashboardState          `json:"state,omitempty"`
	Error      string                   `json:"error,omitempty"`
}

func dashboardHandler(stateRoot string) http.Handler {
	return newDashboardHandler(context.Background(), stateRoot, "tmux")
}

func newDashboardHandler(ctx context.Context, stateRoot, tmux string) http.Handler {
	return newDashboardHandlerWithOptions(ctx, stateRoot, tmux, nil, false, "")
}

func newDashboardHandlerWithOptions(ctx context.Context, stateRoot, tmux string, service orchestratoragent.Service, allowNet bool, password string) http.Handler {
	return newProjectDashboardHandlerWithOptions(ctx, stateRoot, "", nil, tmux, service, allowNet, password)
}

func newProjectDashboardHandlerWithOptions(ctx context.Context, stateRoot, repository string, peerProjects []string, tmux string, service orchestratoragent.Service, allowNet bool, password string) http.Handler {
	return newProjectDashboardServer(ctx, stateRoot, repository, peerProjects, tmux, nil, service, allowNet, password).webHandler()
}

func newProjectDashboardServer(ctx context.Context, stateRoot, repository string, peerProjects []string, tmux string, reconcile func(context.Context) error, service orchestratoragent.Service, allowNet bool, password string) *dashboardServer {
	return &dashboardServer{ctx: ctx, stateRoot: stateRoot, repository: repository, peerProjects: peerProjects, tmux: tmux, allowNet: allowNet, password: password, orchestrator: service, reconcile: reconcile, issueClosed: currentGitHubIssueClosed}
}

func (s *dashboardServer) webHandler() http.Handler {
	assets, err := fs.Sub(dashboardFiles, "dashboard/out")
	if err != nil {
		panic(err)
	}
	static := http.FileServer(http.FS(assets))
	return s.handler(static)
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
		if r.URL.Path == "/actions/archive" || r.URL.Path == "/actions/abandon" || r.URL.Path == "/actions/dismiss" || r.URL.Path == "/actions/remove" || r.URL.Path == "/actions/cancel" || r.URL.Path == "/actions/recover" || r.URL.Path == "/actions/review-plan" {
			if s.operator == nil {
				http.Error(w, "runtime mutation authority is unavailable", http.StatusConflict)
				return
			}
			s.serveOperatorAction(w, r, strings.TrimPrefix(r.URL.Path, "/actions/"))
			return
		}
		if r.URL.Path == "/actions/reconcile" {
			s.serveReconcileAction(w, r)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/actions/orchestrator/") {
			if s.operator == nil {
				http.Error(w, "runtime mutation authority is unavailable", http.StatusConflict)
				return
			}
			s.serveOrchestratorAction(w, r, strings.TrimPrefix(r.URL.Path, "/actions/orchestrator/"))
			return
		}
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.URL.Path == "/status.json" {
			s.serveStatus(w, r)
			return
		}
		if r.URL.Path == "/project.json" {
			s.serveProject(w, r)
			return
		}
		if r.URL.Path == "/projects.json" {
			s.serveProjects(w, r)
			return
		}
		if r.URL.Path == "/release.json" {
			serveDashboardRelease(w, r)
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
		if r.URL.Path == "/orchestrator/terminal" {
			s.serveOrchestratorTerminal(w, r)
			return
		}
		if r.URL.Path == "/reviewer/terminal" {
			s.serveTerminal(w, r, agentruntime.SessionRoleReviewer)
			return
		}
		if r.URL.Path == "/terminal" {
			s.serveTerminal(w, r, agentruntime.SessionRoleImplementation)
			return
		}
		static.ServeHTTP(w, r)
	})
}

func serveDashboardRelease(w http.ResponseWriter, r *http.Request) {
	body, _ := json.Marshal(struct {
		Release string `json:"release"`
	}{releaseVersion()})
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
	if s.operator == nil || s.reconcile == nil {
		http.Error(w, "reconciliation is unavailable", http.StatusConflict)
		return
	}
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

func validateDashboardProjectURLs(values []string) ([]string, error) {
	if len(values) > 16 {
		return nil, errors.New("--dashboard-project may be repeated at most 16 times")
	}
	result := make([]string, 0, len(values))
	seen := map[string]bool{}
	for _, value := range values {
		project, err := url.Parse(value)
		if err != nil || (project.Scheme != "http" && project.Scheme != "https") || project.Host == "" || project.User != nil || project.RawQuery != "" || project.Fragment != "" || project.Path != "" && project.Path != "/" {
			return nil, fmt.Errorf("invalid --dashboard-project %q: use an HTTP(S) origin without credentials, path, query, or fragment", value)
		}
		project.Path = ""
		normalized := project.String()
		if seen[normalized] {
			return nil, fmt.Errorf("duplicate --dashboard-project %q", normalized)
		}
		seen[normalized] = true
		result = append(result, normalized)
	}
	return result, nil
}

func (s *dashboardServer) readStatus() (dashboardStatusSnapshot, error) {
	if s.operator != nil && s.operator.owner != nil {
		snapshot, err := s.operator.owner.snapshot(s.ctx)
		if err != nil {
			return dashboardStatusSnapshot{}, err
		}
		return projectOwnerStatus(snapshot, maxReconciliationAttemptCount, time.Now())
	}
	body, err := readDashboardStatus(filepath.Join(s.stateRoot, "status.json"))
	if err != nil {
		return dashboardStatusSnapshot{}, err
	}
	var snapshot dashboardStatusSnapshot
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&snapshot) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return dashboardStatusSnapshot{}, errors.New("invalid status snapshot")
	}
	if s.repository != "" && slices.ContainsFunc(snapshot.Statuses, func(status orchestrator.RecoveryStatus) bool { return status.Repository != s.repository }) {
		return dashboardStatusSnapshot{}, errors.New("status snapshot contains another project")
	}
	return snapshot, nil
}

func (s *dashboardServer) project() (dashboardProject, error) {
	snapshot, err := s.readStatus()
	if err != nil {
		return dashboardProject{}, err
	}
	state, err := s.readState()
	if err != nil {
		return dashboardProject{}, err
	}
	repository := s.repository
	if repository == "" && len(snapshot.Statuses) > 0 {
		repository = snapshot.Statuses[0].Repository
	}
	if repository == "" {
		return dashboardProject{}, errors.New("project identity is unavailable")
	}
	return dashboardProject{Version: 1, Repository: repository, Snapshot: &snapshot, State: &state}, nil
}

func (s *dashboardServer) serveStatus(w http.ResponseWriter, r *http.Request) {
	if s.repository == "" {
		serveDashboardJSON(w, r, filepath.Join(s.stateRoot, "status.json"), maxDashboardStatusBytes, "status is not available yet", "status snapshot is unavailable")
		return
	}
	snapshot, err := s.readStatus()
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, "status is not available yet", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "status snapshot is unavailable", http.StatusInternalServerError)
		return
	}
	body, _ := json.Marshal(snapshot)
	serveDashboardBody(w, r, body)
}

func (s *dashboardServer) serveProject(w http.ResponseWriter, r *http.Request) {
	project, err := s.project()
	if errors.Is(err, os.ErrNotExist) {
		http.Error(w, "status is not available yet", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, "project status is unavailable", http.StatusInternalServerError)
		return
	}
	body, _ := json.Marshal(project)
	serveDashboardBody(w, r, body)
}

func serveDashboardBody(w http.ResponseWriter, r *http.Request, body []byte) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", fmt.Sprint(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

func (s *dashboardServer) serveProjects(w http.ResponseWriter, r *http.Request) {
	local, err := s.project()
	if err != nil {
		local = dashboardProject{Version: 1, Repository: s.repository, Local: true, Error: "status is not available yet"}
	}
	local.Local = true
	projects := make([]dashboardProject, len(s.peerProjects)+1)
	projects[0] = local
	var wait sync.WaitGroup
	for i, projectURL := range s.peerProjects {
		wait.Add(1)
		go func() {
			defer wait.Done()
			projects[i+1] = fetchDashboardProject(r.Context(), projectURL)
		}()
	}
	wait.Wait()
	seen := map[string]bool{}
	for i := range projects {
		if projects[i].Repository == "" || !seen[projects[i].Repository] {
			seen[projects[i].Repository] = projects[i].Repository != ""
			continue
		}
		projects[i].Repository, projects[i].Snapshot, projects[i].State, projects[i].Error = "", nil, nil, "duplicate project deployment"
	}
	body, _ := json.Marshal(struct {
		Version  int                `json:"version"`
		Projects []dashboardProject `json:"projects"`
	}{1, projects})
	serveDashboardBody(w, r, body)
}

func fetchDashboardProject(ctx context.Context, projectURL string) dashboardProject {
	project := dashboardProject{Version: 1, URL: projectURL}
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, projectURL+"/project.json", nil)
	if err != nil {
		project.Error = "project status is unavailable"
		return project
	}
	client := http.Client{Timeout: 2 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		project.Error = "project status is unavailable"
		return project
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		project.Error = "project status is unavailable"
		return project
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, maxDashboardStatusBytes+maxDashboardStateBytes+1))
	decoder.DisallowUnknownFields()
	var remote dashboardProject
	if decoder.Decode(&remote) != nil || decoder.Decode(&struct{}{}) != io.EOF || remote.Version != 1 || remote.Repository == "" || remote.Snapshot == nil || !validDashboardState(remote.State, remote.Repository) || remote.URL != "" || remote.Local || remote.Error != "" || slices.ContainsFunc(remote.Snapshot.Statuses, func(status orchestrator.RecoveryStatus) bool { return status.Repository != remote.Repository }) {
		project.Error = "project status is invalid"
		return project
	}
	remote.URL = projectURL
	return remote
}

func validDashboardState(state *dashboardState, repository string) bool {
	return state != nil && state.Version == dashboardStateVersion && len(state.Hidden) <= 10_000 && !slices.ContainsFunc(state.Hidden, func(hidden dashboardHiddenAttempt) bool {
		return hidden.Repository != repository || hidden.Issue < 1 || hidden.Attempt < 1 || hidden.Reason != "archived" && hidden.Reason != "abandoned" && hidden.Reason != "dismissed" && hidden.Reason != "removed"
	})
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
	if s.operator != nil {
		snapshot, err := s.operator.owner.snapshot(s.ctx)
		if err != nil {
			return dashboardState{}, err
		}
		state := dashboardState{Version: dashboardStateVersion, OwnerRevision: snapshot.State.Revision, Hidden: []dashboardHiddenAttempt{}}
		for _, tombstone := range snapshot.State.Tombstones {
			state.Hidden = append(state.Hidden, dashboardHiddenAttempt{Repository: tombstone.Repository, Issue: tombstone.Issue, Attempt: tombstone.Attempt, Reason: tombstone.Action})
		}
		slices.SortFunc(state.Hidden, func(a, b dashboardHiddenAttempt) int {
			if ordered := strings.Compare(a.Repository, b.Repository); ordered != 0 {
				return ordered
			}
			if a.Issue != b.Issue {
				return a.Issue - b.Issue
			}
			return a.Attempt - b.Attempt
		})
		return state, nil
	}
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
		if hidden.Repository == "" || s.repository != "" && hidden.Repository != s.repository || hidden.Issue < 1 || hidden.Attempt < 1 || (hidden.Reason != "archived" && hidden.Reason != "abandoned" && hidden.Reason != "dismissed" && hidden.Reason != "removed") {
			return dashboardState{}, errors.New("invalid dashboard state entry")
		}
	}
	return state, nil
}

func (s *dashboardServer) readRemovalState() (dashboardRemovalState, error) {
	state := dashboardRemovalState{Version: removalStateVersion, Intents: []dashboardRemovalIntent{}}
	body, err := readDashboardFile(filepath.Join(s.stateRoot, "removal-state.json"), maxDashboardStateBytes)
	if errors.Is(err, os.ErrNotExist) {
		return state, nil
	}
	if err != nil {
		return dashboardRemovalState{}, err
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF || state.Version != removalStateVersion || len(state.Intents) > 100 {
		return dashboardRemovalState{}, errors.New("invalid permanent removal journal")
	}
	seen := map[string]bool{}
	for _, intent := range state.Intents {
		manifest := intent.Manifest
		attempt := agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
		want, identityErr := agentruntime.AttemptIdentity(productionAttemptRoot(s.stateRoot), attempt)
		wantLog := filepath.Join(s.stateRoot, "attempts", internalgithub.RepositoryIdentifier(manifest.Repository), fmt.Sprintf("%d-%d", manifest.Issue, manifest.Attempt), "agent.log")
		key := fmt.Sprintf("%s#%d/%d", manifest.Repository, manifest.Issue, manifest.Attempt)
		if identityErr != nil || !preflightObjectID.MatchString(intent.PublishedHead) || manifest.Version != want.Version || manifest.Branch != want.Branch || manifest.Worktree != want.Worktree || manifest.Session != want.Session || manifest.LogPath != wantLog || !slices.Contains([]string{"completed", "failed", "cancelled"}, manifest.State) || seen[key] {
			return dashboardRemovalState{}, errors.New("invalid permanent removal journal")
		}
		seen[key] = true
	}
	return state, nil
}

func canDismissClosedAttempt(status orchestrator.RecoveryStatus) bool {
	return status.Repository != "" && status.Issue > 0 && status.Attempt > 0 && status.IssueClosed && slices.Contains([]string{"completed", "failed", "orphaned", "cancelled"}, status.State)
}

func currentGitHubIssueClosed(ctx context.Context, repository string, issue int) (bool, error) {
	return githubIssueClosed(ctx, internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}, repository, issue)
}

func githubIssueClosed(ctx context.Context, api internalgithub.API, repository string, issue int) (bool, error) {
	if repository == "" || issue < 1 {
		return false, errors.New("invalid issue identity")
	}
	var current struct {
		Number int
		State  string
	}
	if _, _, err := api.Read(ctx, fmt.Sprintf("/repos/%s/issues/%d", repository, issue), "", &current); err != nil {
		return false, err
	}
	if current.Number != issue || current.State != "open" && current.State != "closed" {
		return false, errors.New("invalid GitHub issue state")
	}
	return current.State == "closed", nil
}

func cleanupAttemptReviewResources(ctx context.Context, stateRoot string, boundary boundaryCaller, manifest agentruntime.Manifest, remove bool) error {
	attempt := agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
	snapshotRoot := productionSnapshotRoot(stateRoot)
	if root, err := filepath.EvalSymlinks(snapshotRoot); err == nil {
		if root != filepath.Clean(snapshotRoot) {
			return errors.New("review snapshot root is unsafe")
		}
		info, statErr := os.Lstat(snapshotRoot)
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("review snapshot root is unsafe")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.New("review snapshot root is unsafe")
	}
	expectedSnapshot, expectedSession := reviewIdentity(attempt, snapshotRoot)
	if manifest.ReviewSnapshot != "" && manifest.ReviewSnapshot != expectedSnapshot || manifest.ReviewSession != "" && manifest.ReviewSession != expectedSession || !belowRoot(expectedSnapshot, snapshotRoot) {
		return errors.New("persisted reviewer cleanup identity mismatch")
	}
	if info, err := os.Lstat(expectedSnapshot); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("review snapshot cleanup path is invalid")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	entries, err := os.ReadDir(snapshotRoot)
	if errors.Is(err, os.ErrNotExist) {
		entries = nil
	}
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	prefix := filepath.Base(expectedSnapshot) + ".result-"
	resultPaths := make([]string, 0)
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, prefix) || len(name) != len(prefix)+16 {
			continue
		}
		if _, err := hex.DecodeString(strings.TrimPrefix(name, prefix)); err != nil || entry.Type()&os.ModeSymlink != 0 || !entry.IsDir() {
			return errors.New("review result cleanup path is invalid")
		}
		resultPaths = append(resultPaths, filepath.Join(snapshotRoot, name))
	}
	if !remove {
		return nil
	}
	if err := cleanupReviewResources(ctx, boundary, nil, attempt, manifest.ReviewHead, manifest.ReviewTarget, expectedSnapshot, expectedSession, snapshotRoot); err != nil {
		return err
	}
	for _, path := range resultPaths {
		if err := os.RemoveAll(path); err != nil {
			return err
		}
	}
	return nil
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
		if issueErr != nil || attemptErr != nil || issue < 1 || attempt < 1 || !s.validProjectQuery(query, "issue", "attempt") {
			http.Error(w, "invalid orchestrator action", http.StatusBadRequest)
			return
		}
	} else if len(r.URL.Query()) != 0 {
		http.Error(w, "invalid orchestrator action", http.StatusBadRequest)
		return
	}
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

func orchestratorAttentionState(state string) bool {
	return slices.Contains([]string{"blocked", "failed", "conflicting", "orphaned"}, state)
}

func (s *dashboardServer) validProjectQuery(query url.Values, keys ...string) bool {
	want := len(keys)
	for _, key := range keys {
		if len(query[key]) != 1 {
			return false
		}
	}
	if s.repository == "" {
		return len(query) == want
	}
	return len(query) == want+1 && len(query["repository"]) == 1 && query.Get("repository") == s.repository
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

func (s *dashboardServer) serveTerminal(w http.ResponseWriter, r *http.Request, role string) {
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
	session, err := s.projectedSession(issue, attempt, role)
	if issueErr != nil || attemptErr != nil || err != nil || !s.validProjectQuery(query, "issue", "attempt") {
		http.Error(w, "terminal session is not available", http.StatusNotFound)
		return
	}
	s.serveTerminalSession(w, r, session.Name)
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
	s.serveTerminalSession(w, r, target.Session)
}

func (s *dashboardServer) serveTerminalSession(w http.ResponseWriter, r *http.Request, session string) {
	conn, err := websocket.Accept(w, r, nil)
	if err != nil {
		return
	}
	defer conn.CloseNow()
	if !tmuxPaneLive(r.Context(), s.tmux, session) {
		_ = conn.Close(websocket.StatusNormalClosure, "Session ended.")
		return
	}
	conn.SetReadLimit(maxTerminalInputBytes)
	ctx, cancel := context.WithCancel(s.ctx)
	defer cancel()
	command := exec.CommandContext(ctx, s.tmux, "attach-session", "-t", "="+session)
	command.Dir = "/tmp"
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
				_ = conn.Close(websocket.StatusNormalClosure, "Session ended.")
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

func tmuxPaneLive(ctx context.Context, tmux, session string) bool {
	command := exec.CommandContext(ctx, tmux, "display-message", "-p", "-t", agentruntime.PaneTarget(session), "#{pane_dead}")
	command.Dir = "/tmp"
	output, err := command.Output()
	return err == nil && strings.TrimSpace(string(output)) == "0"
}

func (s *dashboardServer) projectedStatus(issue, attempt int) (orchestrator.RecoveryStatus, error) {
	if issue < 1 || attempt < 1 {
		return orchestrator.RecoveryStatus{}, errors.New("invalid attempt")
	}
	snapshot, err := s.readStatus()
	if err != nil {
		return orchestrator.RecoveryStatus{}, err
	}
	var found *orchestrator.RecoveryStatus
	for i := range snapshot.Statuses {
		status := snapshot.Statuses[i]
		expected, nameErr := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, status.Repository, issue, attempt)
		validSessions := nameErr == nil && status.Session == expected
		seen := map[string]bool{}
		for _, session := range status.Sessions {
			want, sessionErr := agentruntime.AttemptSessionName(session.Role, status.Repository, issue, attempt)
			metadataValid := session.Role != agentruntime.SessionRoleReviewer && session.Mode == "" && session.Target == "" || session.Role == agentruntime.SessionRoleReviewer && agentruntime.ValidReviewTarget(session.Mode, session.Target, status.Repository, status.Issue)
			if sessionErr != nil || session.Name != want || session.State == "" || !metadataValid || seen[session.Role] {
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
		return orchestrator.AttemptSession{}, errors.New("attempt session is not projected as current")
	}
	index := slices.IndexFunc(status.Sessions, func(session orchestrator.AttemptSession) bool { return session.Role == role })
	if index < 0 || status.Sessions[index].Name != want || status.Sessions[index].State != "running" || !status.Sessions[index].Current {
		return orchestrator.AttemptSession{}, errors.New("attempt session not found")
	}
	return status.Sessions[index], nil
}

func (s *dashboardServer) terminalStatus(issue, attempt int) (orchestrator.RecoveryStatus, error) {
	return s.projectedStatus(issue, attempt)
}

func startDashboard(ctx context.Context, address, stateRoot string, service orchestratoragent.Service, allowNet bool, password string, log io.Writer) (string, error) {
	return startProjectDashboard(ctx, address, stateRoot, "", nil, nil, service, allowNet, password, log)
}

func startProjectDashboard(ctx context.Context, address, stateRoot, repository string, peerProjects []string, reconcile func(context.Context) error, service orchestratoragent.Service, allowNet bool, password string, log io.Writer) (string, error) {
	project := newProjectDashboardServer(ctx, stateRoot, repository, peerProjects, "tmux", reconcile, service, allowNet, password)
	url, running, err := startDashboardServerWaitable(address, project, allowNet, password, log)
	if err != nil {
		return "", err
	}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = running.shutdown(shutdown)
	}()
	return url, nil
}

type dashboardServerLifecycle struct {
	server   *http.Server
	listener net.Listener
	control  *controlServerLifecycle
	done     chan struct{}
}

func startDashboardServerWaitable(address string, project *dashboardServer, allowNet bool, password string, log io.Writer) (string, *dashboardServerLifecycle, error) {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return "", nil, fmt.Errorf("dashboard address: %w", err)
	}
	loopback := host == "localhost"
	if !loopback {
		ip := net.ParseIP(host)
		loopback = ip != nil && ip.IsLoopback()
		if !loopback && !allowNet {
			return "", nil, errors.New("dashboard address must use localhost or a loopback IP")
		}
	}
	if allowNet && password == "" {
		return "", nil, errors.New("dashboard password is required with --allow-unsafe-dashboard-network")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return "", nil, fmt.Errorf("listen for dashboard on %s: %w", address, err)
	}
	control, err := startControlServerWaitable(project, log)
	if err != nil {
		_ = listener.Close()
		return "", nil, err
	}
	server := &http.Server{Handler: project.webHandler(), ReadHeaderTimeout: 5 * time.Second}
	if allowNet {
		fmt.Fprintln(log, "WARNING: unsafe dashboard network access enabled; direct HTTP is unencrypted, the password and session data are exposed in transit, and anyone with the password can use terminals and cleanup controls")
	}
	running := &dashboardServerLifecycle{server: server, listener: listener, control: control, done: make(chan struct{})}
	go func() {
		defer close(running.done)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(log, "dashboard: "+err.Error())
		}
	}()
	return "http://" + listener.Addr().String(), running, nil
}

func (s *dashboardServerLifecycle) shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	controlErr := s.control.shutdown(ctx)
	err := s.server.Shutdown(ctx)
	_ = s.listener.Close()
	select {
	case <-s.done:
	case <-ctx.Done():
		return errors.Join(controlErr, err, ctx.Err())
	}
	return errors.Join(controlErr, err)
}
