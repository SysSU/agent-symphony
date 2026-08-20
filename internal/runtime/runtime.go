// Package runtime owns one local implementation attempt's Git and tmux resources.
package runtime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

// WorkerResultMaxBytes is the hard implementation stdout capture ceiling.
const WorkerResultMaxBytes = 64 << 10

const (
	manifestVersion    = 1
	maxResourceName    = 64
	maxPathLength      = 4096
	historyLimit       = "5000"
	workerResultSuffix = ".result.json"
)

const (
	SessionRoleImplementation = "implementation"
	SessionRoleReviewer       = "reviewer"
)

var component = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$`)
var commitID = regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)

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
	Repository string
	Issue      int
	Number     int
	BaseSHA    string
	Context    string
	Command    []string
	Env        []string
	Eligible   func() bool
}

type Manifest struct {
	Version             int       `json:"version"`
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
	ImplementationAgent string    `json:"implementation_agent,omitempty"`
	ReviewAgent         string    `json:"review_agent,omitempty"`
	ReviewState         string    `json:"review_state,omitempty"`
	ReviewHead          string    `json:"review_head,omitempty"`
	ReviewSnapshot      string    `json:"review_snapshot,omitempty"`
	ReviewSession       string    `json:"review_session,omitempty"`
	ReviewFindings      []string  `json:"review_findings,omitempty"`
	ReviewHandoffQueued bool      `json:"review_handoff_queued,omitempty"`
	ReviewHandoffAck    bool      `json:"review_handoff_ack,omitempty"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
}

func (r *Runtime) RecordReview(attempt Attempt, state, head, snapshot, session string) (Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	manifest.ReviewState, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = state, head, snapshot, session
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
func (r *Runtime) ResumeHandoff(ctx context.Context, attempt Attempt) (Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canonicalizeStateRoot(); err != nil {
		return Manifest{}, err
	}
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	if manifest.State != "completed" && manifest.State != "running" {
		return Manifest{}, fmt.Errorf("cannot resume handoff from %s attempt", manifest.State)
	}
	if manifest.State == "completed" {
		if strings.TrimSpace(r.Source) == "" {
			return Manifest{}, errors.New("handoff source bundle is required")
		}
		if _, err := r.run(ctx, r.git(), []string{"-C", manifest.Worktree, "fetch", "--no-tags", r.Source, "+refs/heads/*:refs/remotes/agent-symphony/*"}, "", nil, nil); err != nil {
			return Manifest{}, fmt.Errorf("refresh handoff source refs: %w", err)
		}
	}
	if attempt.Eligible != nil && !attempt.Eligible() {
		return Manifest{}, errors.New("attempt is no longer eligible")
	}
	manifest.State, manifest.Diagnostic, manifest.UpdatedAt = "running", "", time.Now().UTC()
	return manifest, r.writeManifest(attempt, manifest)
}

type Runtime struct {
	Root      string
	StateRoot string
	Source    string
	Git       string
	Tmux      string
	Helper    string
	Runner    Runner
	AllowEnv  []string
	StopWait  time.Duration
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

func ResultPath(worktree string) string { return worktree + workerResultSuffix }

// PromptCommand runs command through the descriptor-owning capture helper.
func PromptCommand(helper, tmux, buffer, resultPath string, command []string) []string {
	return append([]string{helper, "worker-capture", tmux, buffer, resultPath, "--"}, command...)
}

// HandoffPromptCommand records and signals worker-owned launch after the
// replacement worker starts.
func HandoffPromptCommand(helper, tmux, buffer, resultPath, launchedPath, recipient, signal string, command []string) []string {
	return append([]string{helper, "worker-capture-handoff", tmux, buffer, resultPath, launchedPath, recipient, signal, "--"}, command...)
}

func (r *Runtime) PrepareAndStart(ctx context.Context, attempt Attempt) (Manifest, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.canonicalizeStateRoot(); err != nil {
		return Manifest{}, err
	}
	if r.VerifyWorker == nil {
		return Manifest{}, errors.New("worker identity verification hook is required")
	}
	if err := r.VerifyWorker(ctx); err != nil {
		return Manifest{}, fmt.Errorf("verify worker identity: %w", err)
	}
	if len(attempt.Command) == 0 || strings.TrimSpace(attempt.Command[0]) == "" {
		return Manifest{}, errors.New("attempt command is required")
	}
	if attempt.Context != "" && strings.TrimSpace(r.Helper) == "" {
		return Manifest{}, errors.New("attempt capture helper is required")
	}
	env, err := internalgithub.AgentEnvironmentWith(append(os.Environ(), attempt.Env...), r.AllowEnv...)
	if err != nil {
		return Manifest{}, fmt.Errorf("build worker environment: %w", err)
	}
	manifest, err := r.identify(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if attempt.Eligible != nil && !attempt.Eligible() {
		return Manifest{}, fmt.Errorf("attempt is no longer eligible")
	}
	if existing, err := r.readManifest(attempt); err == nil {
		return existing, fmt.Errorf("attempt resources already exist")
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	if err := r.rejectCaseCollision(attempt.Repository); err != nil {
		return Manifest{}, err
	}
	if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, fmt.Errorf("worktree already exists: %s", manifest.Worktree)
	}
	if _, err := os.Lstat(ResultPath(manifest.Worktree)); err == nil {
		return Manifest{}, fmt.Errorf("worker result already exists: %s", ResultPath(manifest.Worktree))
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	if live, err := r.session(ctx, manifest.Session); err != nil {
		return Manifest{}, err
	} else if live {
		return Manifest{}, fmt.Errorf("tmux session already exists: %s", manifest.Session)
	}
	if err := os.MkdirAll(filepath.Dir(manifest.Worktree), 0o770); err != nil {
		return Manifest{}, err
	}
	if err := mkdirBelow(r.StateRoot, filepath.Dir(r.manifestPath(attempt)), 0o700); err != nil {
		return Manifest{}, err
	}
	manifest.State = "preparing"
	if err := r.writeManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	fail := func(stage string, cause error) (Manifest, error) {
		manifest.State, manifest.Diagnostic, manifest.UpdatedAt = "failed", stage+": "+diagnostic(cause), time.Now().UTC()
		_ = r.writeManifest(attempt, manifest)
		return manifest, fmt.Errorf("%s: %w", stage, cause)
	}
	failStop := func(stage string, cause error) (Manifest, error) {
		return fail(stage, errors.Join(cause, r.stop(ctx, manifest.Session)))
	}
	if _, err := r.run(ctx, r.git(), []string{"clone", "--no-local", "--no-checkout", r.Source, manifest.Worktree}, "", nil, nil); err != nil {
		return fail("clone", err)
	}
	git := func(args ...string) error {
		_, err := r.run(ctx, r.git(), append([]string{"-C", manifest.Worktree}, args...), "", nil, nil)
		return err
	}
	if err := git("checkout", "--detach", attempt.BaseSHA); err != nil {
		return fail("checkout base", err)
	}
	if err := git("switch", "-c", manifest.Branch); err != nil {
		return fail("create branch", err)
	}
	if err := git("remote", "remove", "origin"); err != nil {
		return fail("remove remote", err)
	}
	if err := git("config", "--local", "credential.helper", ""); err != nil {
		return fail("disable credentials", err)
	}
	if attempt.Eligible != nil && !attempt.Eligible() {
		manifest.State, manifest.Diagnostic = "cancelled", "attempt became ineligible before launch"
		_ = r.writeManifest(attempt, manifest)
		return manifest, fmt.Errorf("attempt became ineligible before launch")
	}
	args := []string{"new-session", "-d", "-s", manifest.Session, "-c", manifest.Worktree, "-e", "GIT_CONFIG_NOSYSTEM=1", "-e", "GIT_TERMINAL_PROMPT=0"}
	for _, value := range env {
		args = append(args, "-e", value)
	}
	if _, err := r.run(ctx, r.tmux(), args, "", []string{}, nil); err != nil {
		return failStop("launch tmux", err)
	}
	target := PaneTarget(manifest.Session)
	if _, err := r.run(ctx, r.tmux(), []string{"set-option", "-w", "-t", target, "remain-on-exit", "on"}, "", []string{}, nil); err != nil {
		return failStop("configure tmux", err)
	}
	if _, err := r.run(ctx, r.tmux(), []string{"set-option", "-w", "-t", target, "history-limit", historyLimit}, "", []string{}, nil); err != nil {
		return failStop("configure tmux history", err)
	}
	if attempt.Context != "" {
		if _, err := r.run(ctx, r.tmux(), []string{"load-buffer", "-b", manifest.Session, "-"}, "", []string{}, strings.NewReader(attempt.Context)); err != nil {
			return failStop("load agent context", err)
		}
	}
	command := attempt.Command
	if attempt.Context != "" {
		command = PromptCommand(r.Helper, r.tmux(), manifest.Session, ResultPath(manifest.Worktree), command)
	}
	if _, err := r.run(ctx, r.tmux(), append([]string{"respawn-pane", "-k", "-t", target, "--"}, command...), "", []string{}, nil); err != nil {
		return failStop("start agent", err)
	}
	manifest.State, manifest.UpdatedAt = "running", time.Now().UTC()
	return manifest, r.writeManifest(attempt, manifest)
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
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	if attempt.Eligible != nil && !attempt.Eligible() {
		return r.cancel(ctx, attempt, manifest, "attempt is no longer eligible")
	}
	result, runErr := r.run(ctx, r.tmux(), []string{"display-message", "-p", "-t", PaneTarget(manifest.Session), "#{pane_dead} #{pane_dead_status}"}, "", []string{}, nil)
	if runErr != nil {
		return manifest, fmt.Errorf("observe tmux session: %w", runErr)
	}
	dead, status, err := ParsePaneStatus(result.Output)
	if err != nil {
		return manifest, fmt.Errorf("observe tmux session: %w", err)
	}
	if dead {
		capture, captureErr := r.run(ctx, r.tmux(), []string{"capture-pane", "-p", "-S", "-", "-t", PaneTarget(manifest.Session)}, "", []string{}, nil)
		var logErr error
		if captureErr == nil {
			logErr = os.WriteFile(manifest.LogPath, []byte(capture.Output), 0o600)
		}
		if captureErr != nil || logErr != nil {
			cause := errors.Join(captureErr, logErr)
			manifest.State, manifest.Diagnostic = "failed", "agent exited; output was not preserved: "+diagnostic(cause)
			manifest.UpdatedAt = time.Now().UTC()
			return manifest, errors.Join(cause, r.writeManifest(attempt, manifest))
		} else if status == 0 {
			manifest.State = "completed"
		} else {
			manifest.State, manifest.Diagnostic = "failed", fmt.Sprintf("agent exited with status %d; output preserved in %s", status, manifest.LogPath)
		}
	}
	manifest.UpdatedAt = time.Now().UTC()
	return manifest, r.writeManifest(attempt, manifest)
}

// ParsePaneStatus parses tmux's pane_dead and pane_dead_status format.
func ParsePaneStatus(output string) (bool, int, error) {
	fields := strings.Fields(output)
	if len(fields) == 1 && fields[0] == "0" {
		return false, 0, nil
	}
	if len(fields) != 2 || fields[0] != "1" {
		return false, 0, fmt.Errorf("invalid pane status %q", strings.TrimSpace(output))
	}
	status, err := strconv.Atoi(fields[1])
	if err != nil || status < 0 {
		return false, 0, fmt.Errorf("invalid exit status %q", fields[1])
	}
	return true, status, nil
}

// Deliver sends one coordinator-framed handoff through the verified worker
// boundary. The coordinator never invokes the worker's tmux server directly.
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
	buffer := "as-handoff-" + fmt.Sprintf("%x", sha256.Sum256(payload))[:16]
	if _, err := r.run(ctx, r.tmux(), []string{"load-buffer", "-b", buffer, "-"}, "", []string{}, bytes.NewReader(payload)); err != nil {
		return err
	}
	if _, err := r.run(ctx, r.tmux(), []string{"paste-buffer", "-d", "-b", buffer, "-t", PaneTarget(manifest.Session)}, "", []string{}, nil); err != nil {
		return err
	}
	_, err := r.run(ctx, r.tmux(), []string{"send-keys", "-t", PaneTarget(manifest.Session), "Enter"}, "", []string{}, nil)
	return err
}

// VerifyOwned checks exact branch/session identity through the worker boundary
// without comparing mutable worktree HEAD.
func (r *Runtime) VerifyOwned(ctx context.Context, manifest Manifest) error {
	return r.verifyActive(ctx, manifest, "", false)
}

// VerifyActive checks exact branch/head/session identity through the worker
// boundary; it performs no coordinator-side worker command.
func (r *Runtime) VerifyActive(ctx context.Context, manifest Manifest, head string) error {
	return r.verifyActive(ctx, manifest, head, true)
}

func (r *Runtime) verifyActive(ctx context.Context, manifest Manifest, head string, verifyHead bool) error {
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
	if _, err := r.run(ctx, r.tmux(), []string{"has-session", "-t", "=" + manifest.Session}, "", nil, nil); err != nil {
		return errors.New("exact tmux session is not live")
	}
	return nil
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
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	return r.cancel(ctx, attempt, manifest, reason)
}

func (r *Runtime) cancel(ctx context.Context, attempt Attempt, manifest Manifest, reason string) (Manifest, error) {
	if err := r.stop(ctx, manifest.Session); err != nil {
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

// AttemptIdentity returns the exact boundary-visible resources for an attempt.
func AttemptIdentity(root string, a Attempt) (Manifest, error) {
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
	worktree, err := below(root, name)
	if err != nil {
		return Manifest{}, err
	}
	if len(worktree) > maxPathLength || len(ResultPath(worktree)) > maxPathLength {
		return Manifest{}, fmt.Errorf("attempt path must be absolute and at most %d bytes", maxPathLength)
	}
	return Manifest{Version: manifestVersion, Repository: a.Repository, Issue: a.Issue, Attempt: a.Number,
		Branch: branch, Worktree: worktree, Session: session, BaseSHA: a.BaseSHA}, nil
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
	if manifest.Version != want.Version || manifest.Repository != want.Repository || manifest.Issue != want.Issue || manifest.Attempt != want.Attempt ||
		manifest.Branch != want.Branch || manifest.Worktree != want.Worktree || manifest.Session != want.Session || manifest.BaseSHA != want.BaseSHA || manifest.LogPath != want.LogPath {
		return fmt.Errorf("manifest does not match deterministic attempt resources")
	}
	switch manifest.ReviewState {
	case "":
		if manifest.ReviewSession != "" {
			return errors.New("review session has no lifecycle state")
		}
	case "preparing", "running", "clean", "findings-queued":
	default:
		return fmt.Errorf("invalid review state %q", manifest.ReviewState)
	}
	if manifest.ReviewSession != "" {
		wantReview, err := AttemptSessionName(SessionRoleReviewer, manifest.Repository, manifest.Issue, manifest.Attempt)
		if err != nil || manifest.ReviewSession != wantReview {
			return errors.New("review session does not match deterministic attempt resources")
		}
	}
	switch manifest.State {
	case "preparing", "running", "completed", "failed", "cancelled":
		return nil
	default:
		return fmt.Errorf("invalid manifest state %q", manifest.State)
	}
}

func below(root, name string) (string, error) {
	if !filepath.IsAbs(root) {
		return "", fmt.Errorf("runtime root must be absolute")
	}
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		return "", fmt.Errorf("resolve runtime root: %w", err)
	}
	path := filepath.Join(root, name)
	rel, err := filepath.Rel(root, path)
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("attempt path escapes runtime root")
	}
	return path, nil
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
	if manifest.Version != manifestVersion || manifest.Repository == "" || manifest.Issue < 1 || manifest.Attempt < 1 {
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

func (r *Runtime) stop(ctx context.Context, session string) error {
	if live, err := r.session(ctx, session); err != nil {
		return err
	} else if !live {
		return nil
	}
	pane := PaneTarget(session)
	if _, err := r.run(ctx, r.tmux(), []string{"send-keys", "-t", pane, "C-c"}, "", []string{}, nil); err != nil {
		return err
	}
	want := r.StopWait
	if want <= 0 {
		want = 2 * time.Second
	}
	deadline := time.Now().Add(want)
	for time.Now().Before(deadline) {
		result, err := r.run(ctx, r.tmux(), []string{"display-message", "-p", "-t", pane, "#{pane_dead}"}, "", []string{}, nil)
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
	if _, err := r.run(ctx, r.tmux(), []string{"kill-session", "-t", "=" + session}, "", []string{}, nil); err != nil {
		if live, probeErr := r.session(ctx, session); probeErr != nil || live {
			return errors.Join(err, probeErr)
		}
	}
	live, err := r.session(ctx, session)
	if err != nil {
		return err
	}
	if live {
		return errors.New("tmux session remained after cancellation")
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
		return result, fmt.Errorf("%s %q failed: %s: %w", filepath.Base(name), args, strings.TrimSpace(result.Output), err)
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
	return err.Error()
}
