// Package runtime owns one local implementation attempt's Git and tmux resources.
package runtime

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
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	stdruntime "runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

// WorkerResultMaxBytes is the hard implementation stdout capture ceiling.
const WorkerResultMaxBytes = 64 << 10

// ManifestVersion2 identifies manifests with a durable implementation launch binding.
const ManifestVersion2 = boundManifestVersion

const (
	manifestVersion         = 1
	boundManifestVersion    = 2
	maxResourceName         = 64
	maxPathLength           = 4096
	historyLimit            = "5000"
	workerPrivateDir        = ".agent-symphony"
	workerResultName        = "result.json"
	workerStatusName        = "status.json"
	WorkerResultEnvironment = "AGENT_SYMPHONY_IMPLEMENTATION_RESULT"
	WorkerStatusEnvironment = "AGENT_SYMPHONY_STATUS_REQUEST"
	WorkerGenerationEnv     = "AGENT_SYMPHONY_WORKER_GENERATION"
	WorkerLaunchIDEnv       = "AGENT_SYMPHONY_WORKER_LAUNCH_ID"
	PaneExitStatusOption    = "@agent-symphony-exit-status"
	PaneExitSignalOption    = "@agent-symphony-exit-signal"
	PaneStatusFormat        = "#{pane_dead}|#{pane_dead_status}|#{pane_dead_signal}|#{@agent-symphony-exit-status}|#{@agent-symphony-exit-signal}"
)

const (
	SessionRoleImplementation = "implementation"
	SessionRoleReviewer       = "reviewer"
	ReviewModePlan            = "plan-review"
	ReviewModeImplementation  = "implementation-review"
)

var component = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
var commitID = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)
var signalName = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,31}$`)

type Command struct {
	Name           string
	Args           []string
	Dir            string
	Env            []string
	Stdin          io.Reader
	MaxOutputBytes int
}

type Result struct {
	Output string
	Code   int
	Exited bool
}

type PaneStatus struct {
	Dead       bool
	Ready      bool
	ExitStatus int
	Signal     string
}

type Runner interface {
	Run(context.Context, Command) (Result, error)
}

type ExecRunner struct{}

func (ExecRunner) Run(ctx context.Context, command Command) (Result, error) {
	cmd := exec.CommandContext(ctx, command.Name, command.Args...)
	cmd.Dir, cmd.Env, cmd.Stdin = command.Dir, command.Env, command.Stdin
	var out []byte
	var err error
	if command.MaxOutputBytes > 0 {
		bounded := tailBuffer{limit: command.MaxOutputBytes}
		cmd.Stdout, cmd.Stderr = &bounded, &bounded
		err = cmd.Run()
		out = bounded.bytes()
	} else {
		out, err = cmd.CombinedOutput()
	}
	result := Result{Output: string(out)}
	if err == nil {
		return result, nil
	}
	var exit *exec.ExitError
	if errors.As(err, &exit) {
		result.Code, result.Exited = exit.ExitCode(), true
	}
	return result, err
}

type tailBuffer struct {
	mu    sync.Mutex
	body  []byte
	limit int
}

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	n := len(p)
	if n >= b.limit {
		b.body = append(b.body[:0], p[n-b.limit:]...)
		return n, nil
	}
	if overflow := len(b.body) + n - b.limit; overflow > 0 {
		copy(b.body, b.body[overflow:])
		b.body = b.body[:len(b.body)-overflow]
	}
	b.body = append(b.body, p...)
	return n, nil
}

func (b *tailBuffer) bytes() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]byte(nil), b.body...)
}

type Attempt struct {
	Repository  string
	Issue       int
	Number      int
	BaseSHA     string
	Context     string
	Command     []string
	Env         []string
	Interactive bool
	Eligible    func() bool
}

type Manifest struct {
	Version             int       `json:"version"`
	LaunchToken         string    `json:"launch_token,omitempty"`
	LaunchID            string    `json:"launch_id,omitempty"`
	Repository          string    `json:"repository"`
	Issue               int       `json:"issue"`
	Attempt             int       `json:"attempt"`
	Branch              string    `json:"branch"`
	Worktree            string    `json:"worktree"`
	Session             string    `json:"session"`
	BaseSHA             string    `json:"base_sha"`
	LogPath             string    `json:"log_path"`
	State               string    `json:"state"`
	Diagnostic          string    `json:"diagnostic,omitempty"`
	Interactive         bool      `json:"interactive,omitempty"`
	ImplementationAgent string    `json:"implementation_agent,omitempty"`
	ReviewAgent         string    `json:"review_agent,omitempty"`
	ReviewState         string    `json:"review_state,omitempty"`
	ReviewDiagnostic    string    `json:"review_diagnostic,omitempty"`
	ReviewInvalidated   bool      `json:"review_invalidated,omitempty"`
	ReviewMode          string    `json:"review_mode,omitempty"`
	ReviewTarget        string    `json:"review_target,omitempty"`
	ReviewRunID         string    `json:"review_run_id,omitempty"`
	ReviewRunCleaned    bool      `json:"review_run_cleaned,omitempty"`
	ReviewBase          string    `json:"review_base,omitempty"`
	ReviewHead          string    `json:"review_head,omitempty"`
	ReviewSnapshot      string    `json:"review_snapshot,omitempty"`
	ReviewSession       string    `json:"review_session,omitempty"`
	ReviewFindings      []string  `json:"review_findings,omitempty"`
	ReviewHandoffQueued bool      `json:"review_handoff_queued,omitempty"`
	ReviewHandoffAck    bool      `json:"review_handoff_ack,omitempty"`
	WorkerStatus        string    `json:"worker_status,omitempty"`
	WorkerStatusReason  string    `json:"worker_status_reason,omitempty"`
	WorkerStatusSeq     uint64    `json:"worker_status_sequence,omitempty"`
	WorkerStatusApplied uint64    `json:"worker_status_applied_sequence,omitempty"`
	WorkerGeneration    uint64    `json:"worker_generation,omitempty"`
	WorkerProfileDigest string    `json:"worker_profile_digest,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

type workerStatusRequest struct {
	Type       string `json:"type"`
	Generation uint64 `json:"generation"`
	LaunchID   string `json:"launch_id"`
	Sequence   uint64 `json:"sequence"`
	Status     string `json:"status"`
	Reason     string `json:"reason"`
}

func observeWorkerStatus(manifest Manifest, generation uint64) (Manifest, error) {
	path := StatusPath(manifest.Worktree)
	listed, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return manifest, nil
	}
	if err != nil || !listed.Mode().IsRegular() || listed.Mode()&os.ModeSymlink != 0 || listed.Mode().Perm()&0o077 != 0 || listed.Size() < 1 || listed.Size() > WorkerResultMaxBytes {
		return manifest, errors.New("worker status request is not a bounded private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return manifest, errors.New("worker status request is unavailable")
	}
	opened, statErr := file.Stat()
	body, readErr := io.ReadAll(io.LimitReader(file, WorkerResultMaxBytes+1))
	closeErr := file.Close()
	current, finalErr := os.Lstat(path)
	if statErr != nil || readErr != nil || closeErr != nil || finalErr != nil || len(body) > WorkerResultMaxBytes || !os.SameFile(listed, opened) || !os.SameFile(opened, current) || opened.Size() != current.Size() || !opened.ModTime().Equal(current.ModTime()) {
		return manifest, errors.New("worker status request changed while reading")
	}
	var request workerStatusRequest
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&request) != nil || decoder.Decode(&struct{}{}) != io.EOF || request.Type != "agent-symphony-status-v1" {
		return manifest, errors.New("worker status request is invalid or stale")
	}
	if request.Generation != generation || request.LaunchID != manifest.LaunchID {
		return manifest, nil
	}
	if request.Sequence == 0 || request.Sequence > 1<<53 || !slices.Contains([]string{"needs-attention", "clear"}, request.Status) || strings.TrimSpace(request.Reason) == "" || len(request.Reason) > 1024 || strings.ContainsRune(request.Reason, 0) {
		return manifest, errors.New("worker status request is invalid or stale")
	}
	if request.Sequence <= manifest.WorkerStatusSeq {
		return manifest, nil
	}
	manifest.WorkerStatus, manifest.WorkerStatusReason, manifest.WorkerStatusSeq = request.Status, strings.TrimSpace(request.Reason), request.Sequence
	return manifest, nil
}

func (r *Runtime) RecordReview(attempt Attempt, state, mode, target, base, head, snapshot, session string) (Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version == boundManifestVersion {
		return Manifest{}, errors.New("direct bound review mutation is unavailable; submit an owner Review effect")
	}
	manifest.ReviewState, manifest.ReviewMode, manifest.ReviewTarget = state, mode, target
	manifest.ReviewRunID = ""
	manifest.ReviewRunCleaned = false
	manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = base, head, snapshot, session
	if state != "findings-queued" {
		manifest.ReviewFindings, manifest.ReviewHandoffQueued, manifest.ReviewHandoffAck = nil, false, false
	}
	manifest.UpdatedAt = time.Now().UTC()
	return manifest, r.writeManifest(attempt, manifest)
}

func (r *Runtime) RecordReviewFindings(attempt Attempt, head string, findings []string, queued, acknowledged bool) (Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version == boundManifestVersion {
		return Manifest{}, errors.New("direct bound review mutation is unavailable; submit an owner Review effect")
	}
	if manifest.ReviewHandoffQueued {
		if manifest.ReviewHead != head || !slices.Equal(manifest.ReviewFindings, findings) {
			return Manifest{}, errors.New("queued review handoff is immutable")
		}
		queued, acknowledged = true, manifest.ReviewHandoffAck || acknowledged
	}
	manifest.ReviewState, manifest.ReviewHead = "findings-queued", head
	manifest.ReviewFindings, manifest.ReviewHandoffQueued, manifest.ReviewHandoffAck = append([]string(nil), findings...), queued, acknowledged
	if acknowledged && manifest.State == "completed" {
		manifest.State, manifest.Diagnostic = "running", ""
	}
	manifest.UpdatedAt = time.Now().UTC()
	return manifest, r.writeManifest(attempt, manifest)
}

// ResumeHandoff refreshes source refs and returns a completed attempt to the
// normal monitored worker lifecycle without granting the worker a remote.
func (r *Runtime) ResumeHandoff(context.Context, Attempt) (Manifest, error) {
	return Manifest{}, errors.New("direct implementation relaunch is unavailable; submit an owner Handoff effect")
}

type Runtime struct {
	Root       string
	StateRoot  string
	Source     string
	Git        string
	Tmux       string
	Helper     string
	Runner     Runner
	AllowEnv   []string
	WorkerHome string
	// WorkerProfileDigest binds a launched worker to the owner-approved
	// confinement configuration. An empty value preserves fail-closed legacy
	// behavior for manifests created before confinement was enforced.
	WorkerProfileDigest string
	StopWait            time.Duration
	// VerifyWorker verifies execution through the provisioned agent-host identity.
	// agent-host supplies the target account HOME; the coordinator never does.
	VerifyWorker func(context.Context) error
	mu           sync.Mutex
}

var (
	ErrWorktreeMissing      = errors.New("worktree is missing")
	ErrWorktreeUnsafe       = errors.New("worktree is not a safe directory")
	ErrWorktreeNonCanonical = errors.New("worktree path is not canonical")
)

func PaneTarget(session string) string { return "=" + session + ":0.0" }

func ValidReviewMetadata(mode, target string) bool {
	if mode == "" && target == "" { // Accept manifests created before review metadata existed.
		return true
	}
	return slices.Contains([]string{ReviewModePlan, ReviewModeImplementation}, mode) && strings.TrimSpace(target) != "" && len(target) <= 512 && !strings.ContainsAny(target, "\x00\r\n")
}

// ValidReviewTarget validates the mode-specific target grammar and public identity.
func ValidReviewTarget(mode, target, repository string, issue int) bool {
	switch mode {
	case ReviewModePlan:
		prefix := fmt.Sprintf("%s#%d plan sha256:", repository, issue)
		digest := strings.TrimPrefix(target, prefix)
		return strings.HasPrefix(target, prefix) && len(digest) == 64 && commitID.MatchString(digest)
	case ReviewModeImplementation:
		base, head, ok := strings.Cut(target, "..")
		return ok && commitID.MatchString(base) && commitID.MatchString(head) && !strings.EqualFold(base, head)
	default:
		return false
	}
}

// ValidReviewBinding validates a persisted target against its attested commits.
func ValidReviewBinding(mode, target, repository string, issue int, base, head, attemptBase string) bool {
	if !ValidReviewTarget(mode, target, repository, issue) || !commitID.MatchString(base) || !commitID.MatchString(head) {
		return false
	}
	if mode == ReviewModePlan {
		return strings.EqualFold(base, head) && strings.EqualFold(base, attemptBase)
	}
	targetBase, targetHead, _ := strings.Cut(target, "..")
	return strings.EqualFold(targetBase, base) && strings.EqualFold(targetHead, head)
}

// AttemptSessionName returns the deterministic tmux name for a bounded role.
func AttemptSessionName(role, repository string, issue, attempt int) (string, error) {
	parts := strings.Split(repository, "/")
	if len(parts) != 2 || !component.MatchString(parts[0]) || !component.MatchString(parts[1]) || issue < 1 || attempt < 1 {
		return "", errors.New("invalid attempt session identity")
	}
	var name string
	switch role {
	case SessionRoleImplementation:
		name = fmt.Sprintf("as-%s-%d-%d", internalgithub.RepositoryIdentifier(repository), issue, attempt)
	case SessionRoleReviewer:
		sum := sha256.Sum256([]byte(repository))
		name = fmt.Sprintf("as-r-%x-%d-%d", sum[:8], issue, attempt)
	default:
		return "", fmt.Errorf("unknown attempt session role %q", role)
	}
	if len(name) > maxResourceName {
		return "", fmt.Errorf("attempt session name exceeds %d bytes", maxResourceName)
	}
	return name, nil
}

// ReviewSessionName binds a reviewer tmux name to the full attempt and target
// identity while staying below tmux's resource-name limit.
func ReviewSessionName(repository string, issue, attempt int, target string) (string, error) {
	base, err := AttemptSessionName(SessionRoleReviewer, repository, issue, attempt)
	if err != nil || strings.TrimSpace(target) == "" || len(target) > 512 || strings.ContainsAny(target, "\x00\r\n") {
		return "", errors.New("invalid target-bound reviewer session identity")
	}
	digest := sha256.Sum256([]byte(base + "\x00" + target))
	return "as-r-" + hex.EncodeToString(digest[:24]), nil
}

// ReviewRunSessionName binds a reviewer tmux name to one never-reused owner run.
func ReviewRunSessionName(repository string, issue, attempt int, target, runID string) (string, error) {
	base, err := AttemptSessionName(SessionRoleReviewer, repository, issue, attempt)
	if err != nil || strings.TrimSpace(target) == "" || len(target) > 4096 || strings.ContainsAny(target, "\x00\r\n") || len(runID) != 64 {
		return "", errors.New("invalid reviewer run identity")
	}
	if _, err := hex.DecodeString(runID); err != nil {
		return "", errors.New("invalid reviewer run identity")
	}
	digest := sha256.Sum256([]byte(base + "\x00target\x00" + target + "\x00run\x00" + runID))
	return "as-r-" + hex.EncodeToString(digest[:24]), nil
}

func ResultPath(worktree string) string {
	return filepath.Join(worktree, workerPrivateDir, workerResultName)
}

func StatusPath(worktree string) string {
	return filepath.Join(worktree, workerPrivateDir, workerStatusName)
}

func PrivatePath(worktree string) string { return filepath.Join(worktree, workerPrivateDir) }

// PromptCommand runs command through the descriptor-owning capture helper.
func PromptCommand(helper, tmux, buffer, resultPath string, command []string) []string {
	return append([]string{helper, "worker-capture", tmux, buffer, resultPath, "--"}, command...)
}

// BoundPromptCommand runs v2 capture directly as the bound tmux pane process.
func BoundPromptCommand(helper, tmux, buffer, resultPath string, manifest Manifest, command []string) []string {
	return append([]string{helper, "worker-capture-bound", tmux, buffer, resultPath, manifest.LogPath, manifest.Worktree, manifest.Session, manifest.LaunchToken, manifest.LaunchID, "--"}, command...)
}

// HandoffPromptCommand records and signals worker-owned launch after the
// replacement worker produces its first observable output.
func HandoffPromptCommand(helper, tmux, buffer, resultPath, launchedPath, recipient, signal string, command []string) []string {
	return append([]string{helper, "worker-capture-handoff-ready", tmux, buffer, resultPath, launchedPath, recipient, signal, "--"}, command...)
}

func BoundHandoffPromptCommand(helper, tmux, buffer, resultPath, launchedPath, recipient, signal string, manifest Manifest, command []string) []string {
	return append([]string{helper, "worker-capture-handoff-ready-bound", tmux, buffer, resultPath, launchedPath, recipient, signal, manifest.LogPath, manifest.Worktree, manifest.Session, manifest.LaunchToken, manifest.LaunchID, "--"}, command...)
}

// PaneExitStatusCommand preserves a command's exit status in the pane before
// the pane process exits. tmux 3.4 can otherwise leave pane_dead_status blank.
func PaneExitStatusCommand(helper, tmux string, command []string) []string {
	return append([]string{helper, "pane-exit-status", tmux, "--"}, command...)
}

func BoundPaneExitStatusCommand(helper, tmux string, manifest Manifest, command []string) []string {
	return append([]string{helper, "pane-exit-status-bound", tmux, manifest.LogPath, manifest.Worktree, manifest.Session, manifest.LaunchToken, manifest.LaunchID, "--"}, command...)
}

func (r *Runtime) PrepareAndStart(context.Context, Attempt) (Manifest, error) {
	return Manifest{}, errors.New("direct implementation launch is unavailable; submit an owner Start effect")
}

func (r *Runtime) Monitor(ctx context.Context, attempt Attempt) (Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canonicalizeStateRoot(); err != nil {
		return Manifest{}, err
	}
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version == boundManifestVersion {
		return Manifest{}, errors.New("direct bound monitor is unavailable; submit an owner Monitor effect")
	}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	if attempt.Eligible != nil && !attempt.Eligible() {
		return r.cancel(ctx, attempt, manifest, "attempt is no longer eligible")
	}
	if manifest.Version != boundManifestVersion {
		live, err := r.session(ctx, manifest.Session)
		if err != nil {
			return manifest, err
		}
		if !live {
			return manifest, nil // Absence is safe to observe; no completed state is inferred.
		}
		return manifest, errors.New("legacy implementation pane has no durable observation identity")
	}
	var result Result
	var runErr error
	if manifest.Version == boundManifestVersion {
		result, runErr = r.observeBoundCommand(ctx, manifest, func(pane ImplementationPane) []string {
			return []string{"display-message", "-p", "-t", pane.PaneID, PaneStatusFormat}
		})
	} else {
		result, runErr = r.run(ctx, r.tmux(), []string{"display-message", "-p", "-t", PaneTarget(manifest.Session), PaneStatusFormat}, "", []string{}, nil)
	}
	if runErr != nil {
		return manifest, fmt.Errorf("observe tmux session: %w", runErr)
	}
	pane, err := ParsePaneStatus(result.Output)
	if err != nil {
		return manifest, fmt.Errorf("observe tmux session: %w", err)
	}
	if pane.Dead && !pane.Ready {
		return manifest, nil
	}
	if pane.Dead {
		env, envErr := r.agentEnvironment(manifest.Repository, attempt.Env...)
		var capture Result
		var captureErr error
		if manifest.Version == boundManifestVersion {
			capture, captureErr = r.observeBoundCommand(ctx, manifest, func(pane ImplementationPane) []string {
				return []string{"capture-pane", "-p", "-S", "-", "-t", pane.PaneID}
			})
		} else {
			capture, captureErr = r.run(ctx, r.tmux(), []string{"capture-pane", "-p", "-S", "-", "-t", PaneTarget(manifest.Session)}, "", []string{}, nil)
		}
		if envErr != nil {
			captureErr = errors.Join(captureErr, envErr)
		} else if captureErr != nil {
			captureErr = errors.New(internalgithub.RedactEnvironment(captureErr.Error(), env))
		}
		var logErr error
		if captureErr == nil {
			logErr = os.WriteFile(manifest.LogPath, []byte(internalgithub.RedactEnvironment(capture.Output, env)), 0o600)
		}
		if captureErr != nil || logErr != nil {
			cause := errors.Join(captureErr, logErr)
			manifest.State, manifest.Diagnostic = "failed", "agent exited; output was not preserved: "+diagnostic(cause)
			manifest.UpdatedAt = time.Now().UTC()
			return manifest, errors.Join(cause, r.writeManifest(attempt, manifest))
		} else if pane.Signal != "" {
			manifest.State, manifest.Diagnostic = "failed", fmt.Sprintf("agent terminated by signal %s; output preserved in %s", pane.Signal, manifest.LogPath)
		} else if pane.ExitStatus == 0 {
			if manifest.Interactive {
				info, err := os.Lstat(ResultPath(manifest.Worktree))
				if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() == 0 || info.Size() > WorkerResultMaxBytes {
					manifest.State, manifest.Diagnostic = "failed", "agent exited without a valid private result artifact"
				} else {
					manifest.State = "completed"
				}
			} else {
				manifest.State = "completed"
			}
		} else {
			manifest.State, manifest.Diagnostic = "failed", fmt.Sprintf("agent exited with status %d; output preserved in %s", pane.ExitStatus, manifest.LogPath)
		}
	}
	manifest.UpdatedAt = time.Now().UTC()
	return manifest, r.writeManifest(attempt, manifest)
}
func (r *Runtime) agentEnvironment(repository string, extra ...string) ([]string, error) {
	environment := append(os.Environ(), extra...)
	if r.WorkerHome != "" {
		environment = append(environment, "CODEX_HOME="+r.WorkerHome)
	}
	return internalgithub.WorkerEnvironmentWith(environment, r.AllowEnv...)
}

func workspaceEnvironment(environment []string, manifest Manifest, generation uint64) ([]string, error) {
	for _, name := range []string{".agents", ".codex"} {
		path := filepath.Join(manifest.Worktree, name)
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("prepare denied worker configuration path: %w", err)
		}
	}
	private := PrivatePath(manifest.Worktree)
	if err := os.Mkdir(private, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		return nil, fmt.Errorf("prepare worker-private directory: %w", err)
	}
	info, err := os.Lstat(private)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o077 != 0 {
		return nil, errors.New("worker-private directory is unsafe")
	}
	paths := map[string]string{
		"TMPDIR":           filepath.Join(private, "tmp"),
		"XDG_CACHE_HOME":   filepath.Join(private, "cache"),
		"GOCACHE":          filepath.Join(private, "go-cache"),
		"npm_config_cache": filepath.Join(private, "npm-cache"),
	}
	for _, path := range paths {
		if err := os.Mkdir(path, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("prepare worker-private cache: %w", err)
		}
	}
	managed := map[string]bool{}
	for name := range paths {
		managed[name] = true
	}
	managed[WorkerStatusEnvironment], managed[WorkerGenerationEnv], managed[WorkerLaunchIDEnv] = true, true, true
	filtered := environment[:0]
	for _, entry := range environment {
		name, _, _ := strings.Cut(entry, "=")
		if !managed[name] && !internalgithub.GitHubCLIEnvironmentVariable(name) {
			filtered = append(filtered, entry)
		}
	}
	for name, path := range paths {
		filtered = append(filtered, name+"="+path)
	}
	return append(filtered,
		WorkerStatusEnvironment+"="+StatusPath(manifest.Worktree),
		WorkerGenerationEnv+"="+strconv.FormatUint(generation, 10),
		WorkerLaunchIDEnv+"="+manifest.LaunchID,
	), nil
}

func (r *Runtime) startSession(ctx context.Context, manifest Manifest, env []string, effectID string, command []string) error {
	if manifest.Version != boundManifestVersion || !ValidManifestVersion(manifest) {
		return errors.New("implementation launch requires owner-committed token")
	}
	if strings.TrimSpace(r.Helper) == "" {
		return errors.New("implementation gate helper is required")
	}
	env = append(slices.Clone(env), "GIT_CONFIG_NOSYSTEM=1", "GIT_TERMINAL_PROMPT=0")
	args := TmuxNewSessionArgs(manifest.Session, manifest.Worktree, env)
	args = slices.Insert(args, 7, "-P", "-F", ImplementationPaneFormat)
	if len(command) == 0 {
		command = []string{"/bin/sh"}
	}
	channel := ImplementationGateChannel(effectID)
	args = append(args, r.Helper, "implementation-gate", r.tmux(), manifest.LogPath, manifest.Worktree, manifest.Session, manifest.LaunchToken, effectID, "--")
	args = append(args, command...)
	args = append([]string{"wait-for", "-L", channel, ";"}, args...)
	target := PaneTarget(manifest.Session)
	args = append(args, ";", "set-option", "-p", "-t", target, "@agent-symphony-launch-token", manifest.LaunchToken,
		";", "set-option", "-w", "-t", target, "remain-on-exit", "on",
		";", "set-option", "-w", "-t", target, "history-limit", historyLimit,
		";", "set-option", "-p", "-t", target, PaneExitStatusOption, "",
		";", "set-option", "-p", "-t", target, PaneExitSignalOption, "")
	created, err := r.run(ctx, r.tmux(), args, "", env, nil)
	if err != nil {
		return err
	}
	initial, err := ParseImplementationPane(created.Output)
	if err != nil || initial.SessionName != manifest.Session || initial.StartPath != manifest.Worktree || initial.Token != "" {
		return errors.New("created implementation pane identity is unavailable")
	}
	observed, err := r.run(ctx, r.tmux(), []string{"display-message", "-p", "-t", target, ImplementationPaneFormat}, "", []string{}, nil)
	if err != nil {
		return err
	}
	pane, err := ParseImplementationPane(observed.Output)
	if err != nil || pane.ServerPID != initial.ServerPID || pane.ServerStart != initial.ServerStart || pane.SessionID != initial.SessionID || pane.PaneID != initial.PaneID {
		return errors.New("implementation pane changed before durable binding")
	}
	role := "unknown"
	if len(command) > 1 {
		switch command[1] {
		case "worker-capture-bound", "worker-capture-handoff-ready-bound":
			role = "capture"
		case "pane-exit-status-bound":
			role = "interactive"
		}
	}
	binding, err := BindImplementationPane(manifest, effectID, role, pane)
	if err != nil {
		return err
	}
	return WriteImplementationBinding(manifest, binding)
}

func (r *Runtime) launchAgent(ctx context.Context, manifest Manifest) error {
	binding, pane, err := r.observeBound(ctx, manifest)
	if err != nil {
		return err
	}
	if !implementationPermitMatches(manifest, binding) {
		return errors.New("implementation launch lacks owner permit")
	}
	if err := WriteImplementationRelease(manifest, binding); err != nil {
		return err
	}
	channel := ImplementationGateChannel(manifest.LaunchID)
	result, err := r.guardedBoundResult(ctx, binding, pane, "wait-for -U "+channel, nil)
	if err == nil {
		return nil
	}
	if !result.Exited || result.Code != 1 || strings.TrimSpace(result.Output) != "channel "+channel+" not locked" || !implementationReleaseMatches(manifest, binding) {
		return err
	}
	if _, _, proofErr := r.observeBound(ctx, manifest); proofErr != nil {
		return errors.Join(err, proofErr)
	}
	return nil
}

// TmuxNewSessionArgs imports only the supplied environment names from the
// client. Values remain in the process environment instead of tmux argv.
func TmuxNewSessionArgs(session, dir string, environment []string) []string {
	names := make([]string, 0, len(environment))
	seen := map[string]bool{}
	for _, entry := range environment {
		name, _, ok := strings.Cut(entry, "=")
		if ok && name != "" && !seen[name] {
			names = append(names, name)
			seen[name] = true
		}
	}
	return []string{"set-option", "-g", "update-environment", strings.Join(names, " "), ";", "new-session", "-d", "-s", session, "-c", dir}
}

// ParsePaneStatus parses PaneStatusFormat. A dead pane is not ready until tmux
// has published either its normal exit status or terminating signal.
func ParsePaneStatus(output string) (PaneStatus, error) {
	fields := strings.Split(strings.TrimSpace(output), "|")
	if len(fields) != 5 || (fields[0] != "0" && fields[0] != "1") {
		return PaneStatus{}, fmt.Errorf("invalid pane status %q", strings.TrimSpace(output))
	}
	var recordedStatus *int
	if fields[3] != "" {
		status, err := strconv.Atoi(fields[3])
		if err != nil || status < 0 || status > 255 {
			return PaneStatus{}, fmt.Errorf("invalid recorded pane exit status %q", fields[3])
		}
		recordedStatus = &status
	}
	var recordedSignal *int
	if fields[4] != "" {
		number, err := strconv.Atoi(fields[4])
		if err != nil || number < 1 || number > 127 {
			return PaneStatus{}, fmt.Errorf("invalid recorded pane signal %q", fields[4])
		}
		recordedSignal = &number
	}
	if recordedStatus != nil && recordedSignal != nil {
		return PaneStatus{}, fmt.Errorf("conflicting recorded pane exit status and signal")
	}
	if fields[0] == "0" {
		if fields[1] != "" || fields[2] != "" {
			return PaneStatus{}, fmt.Errorf("invalid live pane status %q", strings.TrimSpace(output))
		}
		return PaneStatus{}, nil
	}
	pane := PaneStatus{Dead: true}
	if fields[1] == "" && fields[2] == "" {
		if recordedStatus != nil {
			pane.Ready, pane.ExitStatus = true, *recordedStatus
		} else if recordedSignal != nil {
			pane.Ready, pane.Signal = true, strconv.Itoa(*recordedSignal)
		}
		return pane, nil
	}
	if fields[1] != "" && fields[2] != "" {
		return PaneStatus{}, fmt.Errorf("ambiguous dead pane status %q", strings.TrimSpace(output))
	}
	if fields[2] != "" {
		signal := strings.ToLower(fields[2])
		if number, err := strconv.Atoi(signal); err == nil {
			if number < 1 || number > 127 {
				return PaneStatus{}, fmt.Errorf("invalid pane signal %q", fields[2])
			}
			signal = strconv.Itoa(number)
		} else if !signalName.MatchString(signal) {
			return PaneStatus{}, fmt.Errorf("invalid pane signal %q", fields[2])
		}
		if recordedStatus != nil || recordedSignal != nil && !nativePaneSignalMatches(signal, *recordedSignal) {
			return PaneStatus{}, fmt.Errorf("pane signal conflicts with recorded result")
		}
		pane.Ready, pane.Signal = true, signal
		return pane, nil
	}
	status, err := strconv.Atoi(fields[1])
	if err != nil || status < 0 {
		return PaneStatus{}, fmt.Errorf("invalid exit status %q", fields[1])
	}
	if recordedStatus != nil && status != *recordedStatus {
		return PaneStatus{}, fmt.Errorf("pane exit status conflicts with recorded status %d", *recordedStatus)
	}
	if recordedSignal != nil {
		if status == 0 {
			return PaneStatus{}, fmt.Errorf("pane exit status conflicts with recorded signal %d", *recordedSignal)
		}
		// The native status belongs to the Go wrapper, which can exit nonzero
		// instead of re-raising synchronous signals such as SIGSEGV. The pane
		// option records the child signal before that wrapper exits.
		pane.Ready, pane.Signal = true, strconv.Itoa(*recordedSignal)
		return pane, nil
	}
	pane.Ready, pane.ExitStatus = true, status
	return pane, nil
}

func nativePaneSignalMatches(native string, recorded int) bool {
	if native == strconv.Itoa(recorded) {
		return true
	}
	known := map[string]syscall.Signal{
		"hup": syscall.SIGHUP, "int": syscall.SIGINT, "quit": syscall.SIGQUIT,
		"ill": syscall.SIGILL, "trap": syscall.SIGTRAP, "abrt": syscall.SIGABRT,
		"iot": syscall.SIGABRT, "bus": syscall.SIGBUS, "fpe": syscall.SIGFPE,
		"kill": syscall.SIGKILL, "segv": syscall.SIGSEGV, "pipe": syscall.SIGPIPE,
		"alrm": syscall.SIGALRM, "term": syscall.SIGTERM, "usr1": syscall.SIGUSR1,
		"usr2": syscall.SIGUSR2, "chld": syscall.SIGCHLD, "cont": syscall.SIGCONT,
		"stop": syscall.SIGSTOP, "tstp": syscall.SIGTSTP, "ttin": syscall.SIGTTIN,
		"ttou": syscall.SIGTTOU, "sys": syscall.SIGSYS, "urg": syscall.SIGURG,
		"xcpu": syscall.SIGXCPU, "xfsz": syscall.SIGXFSZ, "vtalrm": syscall.SIGVTALRM,
		"prof": syscall.SIGPROF, "winch": syscall.SIGWINCH, "io": syscall.SIGIO,
	}
	name := strings.TrimPrefix(native, "sig")
	value, ok := known[name]
	if !ok && stdruntime.GOOS == "darwin" {
		value, ok = map[string]syscall.Signal{"emt": 7, "info": 29}[name]
	}
	if !ok && stdruntime.GOOS == "linux" {
		value, ok = map[string]syscall.Signal{"cld": 17, "poll": 29, "pwr": 30, "stkflt": 16, "unused": 31}[name]
	}
	return ok && int(value) == recorded
}

// Deliver sends one control-plane-framed handoff through the verified worker
// boundary. The server never invokes the worker's tmux server directly.
func (r *Runtime) Deliver(ctx context.Context, manifest Manifest, payload []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.VerifyWorker == nil {
		return errors.New("worker identity verification hook is required")
	}
	if err := r.VerifyWorker(ctx); err != nil {
		return fmt.Errorf("verify worker identity: %w", err)
	}
	attempt := Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return err
	}
	if manifest.Version != boundManifestVersion {
		return errors.New("legacy implementation pane has no durable handoff identity")
	}
	buffer := "as-handoff-" + fmt.Sprintf("%x", sha256.Sum256(payload))[:16]
	binding, pane, err := r.observeBound(ctx, manifest)
	if err != nil {
		return err
	}
	load, err := TmuxCommandString([]string{"load-buffer", "-b", buffer, "-"})
	if err != nil {
		return err
	}
	if _, err := r.guardedBoundResult(ctx, binding, pane, load, bytes.NewReader(payload)); err != nil {
		return err
	}
	for _, command := range [][]string{{"paste-buffer", "-d", "-b", buffer, "-t"}, {"send-keys", "-t"}} {
		binding, pane, err = r.observeBound(ctx, manifest)
		if err != nil {
			return err
		}
		command = append(command, pane.PaneID)
		if command[0] == "send-keys" {
			command = append(command, "Enter")
		}
		nested, err := TmuxCommandString(command)
		if err != nil {
			return err
		}
		if err := r.guardedBound(ctx, binding, pane, nested); err != nil {
			return err
		}
	}
	return nil
}

// VerifyOwned checks exact branch/session identity through the worker boundary
// without comparing mutable worktree HEAD.
func (r *Runtime) VerifyOwned(ctx context.Context, manifest Manifest) error {
	return r.verifyActive(ctx, manifest, "", false, true, false)
}

// VerifyActive checks exact branch/head/session identity through the worker
// boundary; it performs no coordinator-side worker command.
func (r *Runtime) VerifyActive(ctx context.Context, manifest Manifest, head string) error {
	return r.verifyActive(ctx, manifest, head, true, true, false)
}

// VerifyRetained checks a completed worktree and result without requiring its
// previous tmux session; a follow-up implementation turn creates a fresh session.
func (r *Runtime) VerifyRetained(ctx context.Context, manifest Manifest, head string) error {
	return r.verifyActive(ctx, manifest, head, head != "", false, true)
}

// VerifyWorkspace checks an active worktree without claiming ownership of its
// process. It is safe for an isolated plan review of an unbound legacy attempt.
func (r *Runtime) VerifyWorkspace(ctx context.Context, manifest Manifest) error {
	return r.verifyActive(ctx, manifest, "", false, false, false)
}

func (r *Runtime) verifyActive(ctx context.Context, manifest Manifest, head string, verifyHead, verifySession, verifyResult bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.VerifyWorker == nil {
		return errors.New("worker identity verification hook is required")
	}
	if err := r.VerifyWorker(ctx); err != nil {
		return fmt.Errorf("verify worker identity: %w", err)
	}
	attempt := Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return err
	}
	info, err := os.Lstat(manifest.Worktree)
	if err != nil {
		return ErrWorktreeMissing
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return ErrWorktreeUnsafe
	}
	abs, err := filepath.Abs(manifest.Worktree)
	if err != nil || abs != filepath.Clean(manifest.Worktree) {
		return ErrWorktreeNonCanonical
	}
	branch, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "branch", "--show-current"}, "", nil, nil)
	if err != nil || strings.TrimSpace(branch.Output) != manifest.Branch {
		return errors.New("worktree branch does not match manifest")
	}
	if verifyHead {
		got, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "rev-parse", "HEAD"}, "", nil, nil)
		if err != nil {
			return errors.New("worktree HEAD is unreadable")
		}
		current := strings.TrimSpace(got.Output)
		if !strings.EqualFold(current, head) {
			if !strings.EqualFold(head, manifest.BaseSHA) {
				return errors.New("worktree HEAD does not match GitHub")
			}
			if _, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "merge-base", "--is-ancestor", manifest.BaseSHA, current}, "", nil, nil); err != nil {
				return errors.New("worktree HEAD is not descended from the approved base")
			}
		}
	}
	if !verifySession {
		if verifyResult {
			result, err := os.Lstat(ResultPath(manifest.Worktree))
			if err != nil || !result.Mode().IsRegular() || result.Mode()&os.ModeSymlink != 0 {
				return errors.New("retained worker result is missing or unsafe")
			}
		}
		return nil
	}
	if manifest.Version == boundManifestVersion {
		if _, err := r.observeBoundCommand(ctx, manifest, func(pane ImplementationPane) []string {
			return []string{"display-message", "-p", "-t", pane.PaneID, "#{pane_dead}"}
		}); err != nil {
			return errors.New("exact bound implementation session is unavailable")
		}
		return nil
	}
	return errors.New("legacy implementation session has no durable launch identity")
}

func (r *Runtime) Cancel(ctx context.Context, attempt Attempt, reason string) (Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canonicalizeStateRoot(); err != nil {
		return Manifest{}, err
	}
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version == boundManifestVersion {
		return Manifest{}, errors.New("direct bound cancel is unavailable; submit an owner Stop effect")
	}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	return r.cancel(ctx, attempt, manifest, reason)
}

func (r *Runtime) cancel(ctx context.Context, attempt Attempt, manifest Manifest, reason string) (Manifest, error) {
	if err := r.stop(ctx, manifest); err != nil {
		return manifest, err
	}
	manifest.State, manifest.Diagnostic, manifest.UpdatedAt = "cancelled", reason, time.Now().UTC()
	return manifest, r.writeManifest(attempt, manifest)
}

func (r *Runtime) Discover() ([]Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canonicalizeStateRoot(); err != nil {
		return nil, err
	}
	attempts := filepath.Join(r.StateRoot, "attempts")
	paths, err := manifestPaths(r.StateRoot, attempts)
	if err != nil {
		return nil, err
	}
	manifests := make([]Manifest, 0, len(paths))
	repositories := make(map[string]string)
	for _, path := range paths {
		if err := rejectSymlinkPath(r.StateRoot, path, false); err != nil {
			return nil, err
		}
		manifest, err := readManifest(path)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", path, err)
		}
		attempt := Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
		if err := r.validateManifest(attempt, manifest); err != nil {
			return nil, fmt.Errorf("validate %s: %w", path, err)
		}
		folded := strings.ToLower(manifest.Repository)
		if previous, ok := repositories[folded]; ok && previous != manifest.Repository {
			return nil, fmt.Errorf("repository identity case collision: %q and %q", previous, manifest.Repository)
		}
		repositories[folded] = manifest.Repository
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

// Forget removes one exact retained attempt record after its worker resources
// have already been cleaned up by the implementation boundary.
func (r *Runtime) Forget(manifest Manifest) error {
	if manifest.Version == boundManifestVersion {
		return errors.New("direct bound forget is unavailable; submit an owner Cleanup effect")
	}
	return r.forget(manifest, false)
}

// ForgetCompatibility removes a retained v1 manifest/log record after v2 has
// already proved its worker resources absent. A native v2 attempt may have a
// log directory without a v1 manifest, which is also safe to remove here.
func (r *Runtime) ForgetCompatibility(manifest Manifest) error {
	return r.forget(manifest, true)
}

// VerifyResourcesGone checks the implementation session/worktree/result
// postconditions without mutating them.
func (r *Runtime) VerifyResourcesGone(ctx context.Context, manifest Manifest) error {
	return r.verifyResourcesGone(ctx, manifest)
}

func (r *Runtime) forget(manifest Manifest, allowMissingManifest bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canonicalizeStateRoot(); err != nil {
		return err
	}
	attempt := Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return err
	}
	stored, err := r.readManifest(attempt)
	if errors.Is(err, os.ErrNotExist) {
		dir := filepath.Dir(r.manifestPath(attempt))
		info, statErr := os.Lstat(dir)
		if !errors.Is(statErr, os.ErrNotExist) {
			if statErr == nil && (!allowMissingManifest || !info.IsDir() || info.Mode()&os.ModeSymlink != 0) {
				return errors.New("attempt record is incomplete")
			}
			if statErr != nil {
				return statErr
			}
		}
		for _, path := range []string{manifest.Worktree, ResultPath(manifest.Worktree)} {
			if _, statErr := os.Lstat(path); !errors.Is(statErr, os.ErrNotExist) {
				if statErr == nil {
					return fmt.Errorf("attempt worker resource still exists: %s", path)
				}
				return statErr
			}
		}
		if errors.Is(statErr, os.ErrNotExist) {
			return nil
		}
		if err := rejectSymlinkPath(r.StateRoot, dir, false); err != nil {
			return err
		}
		return os.RemoveAll(dir)
	}
	if err != nil {
		return err
	}
	if err := r.validateManifest(attempt, stored); err != nil {
		return err
	}
	for _, path := range []string{stored.Worktree, ResultPath(stored.Worktree)} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			if err == nil {
				return fmt.Errorf("attempt worker resource still exists: %s", path)
			}
			return err
		}
	}
	dir := filepath.Dir(r.manifestPath(attempt))
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("attempt record is not a non-symlink directory")
	}
	if err := rejectSymlinkPath(r.StateRoot, dir, false); err != nil {
		return err
	}
	return os.RemoveAll(dir)
}

func (r *Runtime) identify(a Attempt) (Manifest, error) {
	manifest, err := AttemptIdentity(r.Root, a)
	if err != nil {
		return Manifest{}, err
	}
	repoID := internalgithub.RepositoryIdentifier(a.Repository)
	logPath := filepath.Join(r.StateRoot, "attempts", repoID, fmt.Sprintf("%d-%d", a.Issue, a.Number), "agent.log")
	if !filepath.IsAbs(r.StateRoot) || len(logPath) > maxPathLength {
		return Manifest{}, fmt.Errorf("attempt path must be absolute and at most %d bytes", maxPathLength)
	}
	now := time.Now().UTC()
	manifest.LogPath, manifest.CreatedAt, manifest.UpdatedAt = logPath, now, now
	return manifest, nil
}

func newLaunchToken() (string, error) {
	var token [16]byte
	if _, err := rand.Read(token[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(token[:]), nil
}

// NewLaunchToken mints a candidate before an owner commits a launch effect.
func NewLaunchToken() (string, error) { return newLaunchToken() }

func ValidLaunchToken(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 16
}

// ValidManifestVersion accepts historical unbound records and new bound ones.
func ValidManifestVersion(manifest Manifest) bool {
	if manifest.Version == manifestVersion {
		return manifest.LaunchToken == "" && manifest.LaunchID == ""
	}
	decoded, err := hex.DecodeString(manifest.LaunchToken)
	if manifest.Version != boundManifestVersion || err != nil || len(decoded) != 16 {
		return false
	}
	if manifest.LaunchID == "" {
		return true
	}
	decoded, err = hex.DecodeString(manifest.LaunchID)
	return err == nil && len(decoded) == 16
}

// AttemptIdentity returns the exact boundary-visible resources for an attempt.
func AttemptIdentity(root string, a Attempt) (Manifest, error) {
	if !filepath.IsAbs(root) {
		return Manifest{}, errors.New("runtime root must be absolute")
	}
	canonical, err := filepath.EvalSymlinks(root)
	if err != nil {
		return Manifest{}, fmt.Errorf("resolve runtime root: %w", err)
	}
	return attemptIdentity(canonical, a)
}

func (r *Runtime) rejectCaseCollision(repository string) error {
	paths, err := manifestPaths(r.StateRoot, filepath.Join(r.StateRoot, "attempts"))
	if err != nil {
		return err
	}
	for _, path := range paths {
		if err := rejectSymlinkPath(r.StateRoot, path, false); err != nil {
			return err
		}
		manifest, err := readManifest(path)
		if err != nil {
			return err
		}
		if strings.EqualFold(manifest.Repository, repository) && manifest.Repository != repository {
			return fmt.Errorf("repository identity case collision: %q and %q", manifest.Repository, repository)
		}
	}
	return nil
}

func manifestPaths(root, attempts string) ([]string, error) {
	if err := rejectSymlinkPath(root, attempts, true); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	repositories, err := os.ReadDir(attempts)
	if err != nil {
		return nil, err
	}
	var paths []string
	for _, repository := range repositories {
		repositoryPath := filepath.Join(attempts, repository.Name())
		if repository.Name() == "source.bundle" && repository.Type()&os.ModeSymlink == 0 {
			info, err := repository.Info()
			if err != nil {
				return nil, err
			}
			if info.Mode().IsRegular() {
				continue
			}
		}
		if repository.Type()&os.ModeSymlink != 0 || !repository.IsDir() {
			return nil, fmt.Errorf("state repository component must be a non-symlink directory: %s", repositoryPath)
		}
		attemptEntries, err := os.ReadDir(repositoryPath)
		if err != nil {
			return nil, err
		}
		for _, attempt := range attemptEntries {
			attemptPath := filepath.Join(repositoryPath, attempt.Name())
			if attempt.Type()&os.ModeSymlink != 0 || !attempt.IsDir() {
				return nil, fmt.Errorf("state attempt component must be a non-symlink directory: %s", attemptPath)
			}
			manifestPath := filepath.Join(attemptPath, "manifest.json")
			if _, err := os.Lstat(manifestPath); err == nil {
				if err := rejectSymlinkPath(root, manifestPath, false); err != nil {
					return nil, err
				}
				paths = append(paths, manifestPath)
			} else if !errors.Is(err, os.ErrNotExist) {
				return nil, err
			}
		}
	}
	return paths, nil
}

func (r *Runtime) validateManifest(attempt Attempt, manifest Manifest) error {
	want, err := r.identify(attempt)
	if err != nil {
		return err
	}
	return validateManifestIdentity(want, manifest)
}

func validateManifestIdentity(want, manifest Manifest) error {
	if !ValidManifestVersion(manifest) || manifest.Repository != want.Repository || manifest.Issue != want.Issue || manifest.Attempt != want.Attempt ||
		manifest.Branch != want.Branch || manifest.Worktree != want.Worktree || manifest.Session != want.Session || manifest.BaseSHA != want.BaseSHA || manifest.LogPath != want.LogPath {
		return fmt.Errorf("manifest does not match deterministic attempt resources")
	}
	if (manifest.WorkerGeneration == 0) != (manifest.WorkerProfileDigest == "") || manifest.WorkerProfileDigest != "" && !ValidEffectRequestDigest(manifest.WorkerProfileDigest) {
		return errors.New("worker confinement proof is invalid")
	}
	switch manifest.ReviewState {
	case "":
		if manifest.ReviewMode != "" || manifest.ReviewTarget != "" || manifest.ReviewRunID != "" || manifest.ReviewRunCleaned || manifest.ReviewSession != "" {
			return errors.New("review metadata has no lifecycle state")
		}
	case "preparing", "running", "clean", "findings-queued", "failed":
		legacy := manifest.ReviewMode == "" && manifest.ReviewTarget == ""
		if !legacy && (!ValidReviewMetadata(manifest.ReviewMode, manifest.ReviewTarget) || !ValidReviewTarget(manifest.ReviewMode, manifest.ReviewTarget, manifest.Repository, manifest.Issue)) {
			return errors.New("review mode or target is invalid")
		}
		if !legacy && !ValidReviewBinding(manifest.ReviewMode, manifest.ReviewTarget, manifest.Repository, manifest.Issue, manifest.ReviewBase, manifest.ReviewHead, manifest.BaseSHA) {
			return errors.New("review target does not match persisted identity")
		}
		if manifest.ReviewState == "failed" && (manifest.ReviewDiagnostic == "" || len(manifest.ReviewDiagnostic) > 4096) {
			return errors.New("failed review requires a bounded diagnostic")
		}
	default:
		return fmt.Errorf("invalid review state %q", manifest.ReviewState)
	}
	if manifest.ReviewState != "failed" && manifest.ReviewDiagnostic != "" {
		return errors.New("review diagnostic requires failed state")
	}
	if manifest.ReviewInvalidated && manifest.ReviewState != "failed" {
		return errors.New("invalidated review requires failed state")
	}
	if manifest.ReviewBase != "" && !commitID.MatchString(manifest.ReviewBase) {
		return errors.New("review base is invalid")
	}
	if manifest.ReviewHead != "" && !commitID.MatchString(manifest.ReviewHead) {
		return errors.New("review head is invalid")
	}
	if manifest.ReviewSession != "" {
		if manifest.ReviewRunID != "" {
			runReview, runErr := ReviewRunSessionName(manifest.Repository, manifest.Issue, manifest.Attempt, manifest.ReviewTarget, manifest.ReviewRunID)
			if runErr != nil || manifest.ReviewSession != runReview {
				return errors.New("review session does not match deterministic run resources")
			}
		} else {
			wantReview, err := AttemptSessionName(SessionRoleReviewer, manifest.Repository, manifest.Issue, manifest.Attempt)
			targetReview, targetErr := ReviewSessionName(manifest.Repository, manifest.Issue, manifest.Attempt, manifest.ReviewTarget)
			if targetErr != nil || manifest.ReviewSession != targetReview && (err != nil || manifest.ReviewSession != wantReview) {
				return errors.New("review session does not match deterministic attempt resources")
			}
		}
	}
	if manifest.ReviewRunCleaned && (manifest.ReviewRunID != "" || manifest.ReviewSnapshot != "" || manifest.ReviewSession != "" || !slices.Contains([]string{"clean", "findings-queued", "failed"}, manifest.ReviewState)) {
		return errors.New("cleaned review run retains physical identity")
	}
	switch manifest.State {
	case "preparing", "running", "completed", "failed", "cancelled":
		return nil
	default:
		return fmt.Errorf("invalid manifest state %q", manifest.State)
	}
}

// BindWorkerConfinement records the exact generation and profile the owner
// authorizes before any implementation process may be released.
func BindWorkerConfinement(manifest Manifest, generation uint64, profileDigest string) (Manifest, error) {
	if generation == 0 || !ValidEffectRequestDigest(profileDigest) || manifest.WorkerGeneration != 0 || manifest.WorkerProfileDigest != "" {
		return Manifest{}, errors.New("worker confinement binding is invalid")
	}
	manifest.WorkerGeneration, manifest.WorkerProfileDigest = generation, profileDigest
	return manifest, nil
}

// WorkerConfinementMatches proves that a launched worker was admitted under
// the current rootless profile for this exact attempt generation.
func WorkerConfinementMatches(manifest Manifest, generation uint64, profileDigest string) bool {
	return WorkerConfinementBound(manifest, generation, profileDigest) && manifest.LaunchID != ""
}

// WorkerConfinementBound validates the durable owner authorization even when
// a generation-invalidating action raced a Start before its LaunchID result
// could be committed.
func WorkerConfinementBound(manifest Manifest, generation uint64, profileDigest string) bool {
	return manifest.Version == boundManifestVersion && generation != 0 && manifest.WorkerGeneration == generation &&
		manifest.WorkerProfileDigest == profileDigest && ValidEffectRequestDigest(profileDigest)
}

// ValidateManifest verifies a persisted manifest against deterministic
// identities rooted at caller-canonicalized paths. It performs no I/O.
func ValidateManifest(root, stateRoot string, manifest Manifest) error {
	attempt := Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
	want, err := attemptIdentity(root, attempt)
	if err != nil {
		return err
	}
	if !filepath.IsAbs(stateRoot) || filepath.Clean(stateRoot) != stateRoot {
		return errors.New("runtime state root must be canonical and absolute")
	}
	want.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository), fmt.Sprintf("%d-%d", attempt.Issue, attempt.Number), "agent.log")
	if len(want.LogPath) > maxPathLength {
		return fmt.Errorf("attempt path must be absolute and at most %d bytes", maxPathLength)
	}
	return validateManifestIdentity(want, manifest)
}

func attemptIdentity(root string, a Attempt) (Manifest, error) {
	if !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return Manifest{}, errors.New("attempt root must be canonical and absolute")
	}
	parts := strings.Split(a.Repository, "/")
	if len(parts) != 2 || !component.MatchString(parts[0]) || !component.MatchString(parts[1]) || a.Issue < 1 || a.Number < 1 || !commitID.MatchString(a.BaseSHA) {
		return Manifest{}, fmt.Errorf("invalid attempt identity or base SHA")
	}
	repoID := internalgithub.RepositoryIdentifier(a.Repository)
	name := fmt.Sprintf("%s-%d-%d", repoID, a.Issue, a.Number)
	branch, err := internalgithub.AttemptBranch(a.Repository, a.Issue, a.Number)
	if err != nil {
		return Manifest{}, err
	}
	session, err := AttemptSessionName(SessionRoleImplementation, a.Repository, a.Issue, a.Number)
	if err != nil {
		return Manifest{}, err
	}
	if len(name) > maxResourceName || len(branch) > maxResourceName || len(session) > maxResourceName {
		return Manifest{}, fmt.Errorf("attempt resource name exceeds %d bytes", maxResourceName)
	}
	worktree := filepath.Join(root, name)
	rel, err := filepath.Rel(root, worktree)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return Manifest{}, errors.New("attempt path escapes canonical root")
	}
	if len(worktree) > maxPathLength || len(ResultPath(worktree)) > maxPathLength {
		return Manifest{}, fmt.Errorf("attempt path must be absolute and at most %d bytes", maxPathLength)
	}
	return Manifest{Version: manifestVersion, Repository: a.Repository, Issue: a.Issue, Attempt: a.Number, Branch: branch, Worktree: worktree, Session: session, BaseSHA: a.BaseSHA, Interactive: a.Interactive}, nil
}

func (r *Runtime) manifestPath(a Attempt) string {
	return filepath.Join(r.StateRoot, "attempts", internalgithub.RepositoryIdentifier(a.Repository), fmt.Sprintf("%d-%d", a.Issue, a.Number), "manifest.json")
}

func (r *Runtime) writeManifest(a Attempt, manifest Manifest) error {
	path := r.manifestPath(a)
	if err := rejectSymlinkPath(r.StateRoot, path, false); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	b, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".manifest-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(b, '\n')); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, path)
}

func (r *Runtime) readManifest(a Attempt) (Manifest, error) {
	path := r.manifestPath(a)
	if err := rejectSymlinkPath(r.StateRoot, path, false); err != nil {
		return Manifest{}, err
	}
	return readManifest(path)
}

func readManifest(path string) (Manifest, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return Manifest{}, err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return Manifest{}, fmt.Errorf("manifest must be a regular non-symlink file: %s", path)
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return Manifest{}, err
	}
	var manifest Manifest
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&manifest); err != nil {
		return Manifest{}, err
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return Manifest{}, fmt.Errorf("manifest contains multiple JSON values")
	}
	if !ValidManifestVersion(manifest) || manifest.Repository == "" || manifest.Issue < 1 || manifest.Attempt < 1 {
		return Manifest{}, fmt.Errorf("invalid manifest")
	}
	return manifest, nil
}

func (r *Runtime) canonicalizeStateRoot() error {
	if !filepath.IsAbs(r.StateRoot) {
		return errors.New("state root must be absolute")
	}
	root, err := filepath.EvalSymlinks(r.StateRoot)
	if err != nil {
		return fmt.Errorf("resolve state root: %w", err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("state root must be an existing directory: %w", err)
	}
	r.StateRoot = root
	return nil
}

func mkdirBelow(root, path string, mode os.FileMode) error {
	if err := rejectSymlinkPath(root, path, true); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("state path escapes canonical root")
	}
	current := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		if err := os.Mkdir(current, mode); err != nil && !errors.Is(err, os.ErrExist) {
			return err
		}
		info, err := os.Lstat(current)
		if err != nil || info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return fmt.Errorf("state path component must be a non-symlink directory: %s", current)
		}
	}
	return nil
}

func rejectSymlinkPath(root, path string, finalDirectory bool) error {
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return errors.New("state path escapes canonical root")
	}
	current := root
	parts := strings.Split(rel, string(filepath.Separator))
	for i, part := range parts {
		if part == "." {
			continue
		}
		current = filepath.Join(current, part)
		info, err := os.Lstat(current)
		if err != nil {
			return err
		}
		if info.Mode()&os.ModeSymlink != 0 || (i < len(parts)-1 || finalDirectory) && !info.IsDir() {
			return fmt.Errorf("state path contains symlink or non-directory component: %s", current)
		}
	}
	return nil
}

func (r *Runtime) session(ctx context.Context, session string) (bool, error) {
	result, err := r.run(ctx, r.tmux(), []string{"has-session", "-t", "=" + session}, "", []string{}, nil)
	if err == nil {
		return true, nil
	}
	if result.Exited && result.Code == 1 {
		return false, nil
	}
	return false, err
}

func (r *Runtime) stop(ctx context.Context, manifest Manifest) error {
	return r.stopGeneration(ctx, manifest, 0)
}

func (r *Runtime) stopGeneration(ctx context.Context, manifest Manifest, generation uint64) error {
	if manifest.Version != boundManifestVersion {
		return errors.New("legacy implementation session has no durable launch identity")
	}
	confined := WorkerConfinementMatches(manifest, generation, r.WorkerProfileDigest)
	binding, err := ReadImplementationBinding(manifest)
	if err != nil {
		return err
	}
	live, err := r.session(ctx, manifest.Session)
	if err != nil {
		return err
	}
	if !live {
		// A renamed or unlinked session can keep the original worker
		// alive. The inventory must come from the exact original server.
		absent, probeErr := r.boundPaneAbsent(ctx, binding)
		if probeErr == nil && absent {
			if confined {
				return nil
			}
			gone, groupErr := ImplementationWorkerGone(manifest, binding)
			if groupErr == nil && gone {
				return nil
			}
			return errors.Join(groupErr, errors.New("implementation worker group termination is unconfirmed"))
		}
		return errors.Join(probeErr, errors.New("bound implementation pane may still exist"))
	}
	return r.stopBound(ctx, manifest, confined)
}

func (r *Runtime) boundPaneAbsent(ctx context.Context, binding ImplementationLaunchBinding) (bool, error) {
	result, err := r.run(ctx, r.tmux(), []string{"list-panes", "-a", "-F", ImplementationInventoryFormat}, "", []string{}, nil)
	if err != nil {
		if ImplementationOriginalServerGone(binding) {
			return true, nil
		}
		return false, err
	}
	absent, err := ImplementationPaneAbsentFromInventory(result.Output, binding)
	if err != nil && ImplementationOriginalServerGone(binding) {
		return true, nil
	}
	return absent, err
}

func (r *Runtime) observeBound(ctx context.Context, manifest Manifest) (ImplementationLaunchBinding, ImplementationPane, error) {
	binding, err := ReadImplementationBinding(manifest)
	if err != nil {
		return ImplementationLaunchBinding{}, ImplementationPane{}, err
	}
	result, err := r.run(ctx, r.tmux(), []string{"display-message", "-p", "-t", PaneTarget(manifest.Session), ImplementationPaneFormat}, "", []string{}, nil)
	if err != nil {
		return ImplementationLaunchBinding{}, ImplementationPane{}, err
	}
	pane, err := ParseImplementationPane(result.Output)
	if err != nil || !binding.Matches(manifest, pane) {
		return ImplementationLaunchBinding{}, ImplementationPane{}, errors.New("implementation pane no longer matches durable launch identity")
	}
	return binding, pane, nil
}

func (r *Runtime) guardedBound(ctx context.Context, binding ImplementationLaunchBinding, pane ImplementationPane, command string) error {
	result, err := r.guardedBoundResult(ctx, binding, pane, command, nil)
	if err != nil {
		return err
	}
	if strings.TrimSpace(result.Output) != "" {
		return errors.New("guarded implementation command returned unexpected output")
	}
	return nil
}

func (r *Runtime) guardedBoundResult(ctx context.Context, binding ImplementationLaunchBinding, pane ImplementationPane, command string, stdin io.Reader) (Result, error) {
	args, err := GuardedImplementationArgs(binding, pane, command)
	if err != nil {
		return Result{}, err
	}
	result, err := r.run(ctx, r.tmux(), args, "", []string{}, stdin)
	if err != nil {
		return result, err
	}
	if strings.TrimSpace(result.Output) == ImplementationGuardMismatch {
		return result, errors.New("implementation pane changed before guarded command")
	}
	return result, nil
}

func (r *Runtime) observeBoundCommand(ctx context.Context, manifest Manifest, build func(ImplementationPane) []string) (Result, error) {
	binding, pane, err := r.observeBound(ctx, manifest)
	if err != nil {
		return Result{}, err
	}
	nested, err := TmuxCommandString(build(pane))
	if err != nil {
		return Result{}, err
	}
	return r.guardedBoundResult(ctx, binding, pane, nested, nil)
}

func (r *Runtime) stopBound(ctx context.Context, manifest Manifest, confined bool) error {
	binding, pane, err := r.observeBound(ctx, manifest)
	if err != nil {
		return err
	}
	if err := r.guardedBound(ctx, binding, pane, "send-keys -t "+pane.PaneID+" C-c"); err != nil {
		return err
	}
	want := r.StopWait
	if want <= 0 {
		want = 2 * time.Second
	}
	deadline := time.Now().Add(want)
	for time.Now().Before(deadline) {
		result, err := r.observeBoundCommand(ctx, manifest, func(pane ImplementationPane) []string {
			return []string{"display-message", "-p", "-t", pane.PaneID, "#{pane_dead}"}
		})
		if err != nil {
			return err
		}
		if strings.TrimSpace(result.Output) == "1" {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Millisecond):
		}
	}
	_, pane, err = r.observeBound(ctx, manifest)
	if err != nil {
		return err
	}
	if err := r.guardedBound(ctx, binding, pane, "kill-pane -t "+pane.PaneID); err != nil {
		return err
	}
	absent, inventoryErr := r.boundPaneAbsent(ctx, binding)
	if inventoryErr != nil {
		return inventoryErr
	}
	if !absent {
		return errors.New("bound implementation pane remained after guarded stop")
	}
	if confined {
		return nil
	}
	workerGone, groupErr := ImplementationWorkerGone(manifest, binding)
	if groupErr != nil || !workerGone {
		return errors.Join(groupErr, errors.New("implementation worker group termination is unconfirmed"))
	}
	return nil
}

func (r *Runtime) run(ctx context.Context, name string, args []string, dir string, env []string, stdin io.Reader) (Result, error) {
	runner := r.Runner
	if runner == nil {
		runner = ExecRunner{}
	}
	result, err := runner.Run(ctx, Command{Name: name, Args: args, Dir: dir, Env: env, Stdin: stdin})
	if err != nil {
		redact := func(value string) string { return internalgithub.RedactEnvironment(value, env) }
		return result, fmt.Errorf("%s %s failed: %s: %s", filepath.Base(name), redact(fmt.Sprint(args)), redact(strings.TrimSpace(result.Output)), redact(err.Error()))
	}
	return result, nil
}

func (r *Runtime) git() string {
	if r.Git != "" {
		return r.Git
	}
	return "git"
}
func (r *Runtime) tmux() string {
	if r.Tmux != "" {
		return r.Tmux
	}
	return "tmux"
}
func diagnostic(err error) string {
	if err == nil {
		return ""
	}
	return internalgithub.RedactEnvironment(err.Error(), os.Environ())
}
