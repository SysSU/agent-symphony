package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"regexp"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const outputVersion = 1

var releaseMetadata = "agent-symphony-release-version:devel"

var (
	githubAPI          = "https://api.github.com"
	githubClient       = http.DefaultClient
	reconcileGitHubRun = reconcileGitHub
	reviewSnapshotRoot = ""
	runningOnWSL       = func() bool { return runtime.GOOS == "linux" && isWSL() }
	immutableCreate    = os.CreateTemp
	immutableWrite     = func(f *os.File, body []byte) error { _, err := io.Copy(f, bytes.NewReader(body)); return err }
	immutableFileSync  = (*os.File).Sync
	immutableInstall   = os.Link
	immutableDirSync   = func(path string) error {
		d, err := os.Open(path)
		if err != nil {
			return err
		}
		defer d.Close()
		return d.Sync()
	}
)

type diagnostic struct {
	Name    string `json:"name"`
	Status  string `json:"status"`
	Message string `json:"message"`
	Action  string `json:"action,omitempty"`
}

type envelope struct {
	Version     int          `json:"version"`
	Command     string       `json:"command"`
	OK          bool         `json:"ok"`
	Data        any          `json:"data,omitempty"`
	Diagnostics []diagnostic `json:"diagnostics,omitempty"`
	Error       string       `json:"error,omitempty"`
}

type environmentToken string

type workerBoundaryRunner struct {
	Command string
	Args    []string
	Env     []string
}

type boundaryCaller interface {
	call(context.Context, string, agentruntime.Command) (agentruntime.Result, error)
}
type limitedBuffer struct {
	bytes.Buffer
	Limit int
}

func (w *limitedBuffer) Write(p []byte) (int, error) {
	if w.Len()+len(p) > w.Limit {
		return 0, errors.New("worker boundary response exceeds limit")
	}
	return w.Buffer.Write(p)
}

type boundaryCommand struct {
	Name  string   `json:"name"`
	Args  []string `json:"args,omitempty"`
	Env   []string `json:"env,omitempty"`
	Dir   string   `json:"dir,omitempty"`
	Input []byte   `json:"input,omitempty"`
}

func (b workerBoundaryRunner) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if strings.TrimSpace(b.Command) == "" {
		return agentruntime.Result{}, errors.New("AGENT_SYMPHONY_WORKER_BOUNDARY is required")
	}
	var input []byte
	if command.Stdin != nil {
		var err error
		input, err = io.ReadAll(io.LimitReader(command.Stdin, 1<<20+1))
		if err != nil || len(input) > 1<<20 {
			return agentruntime.Result{}, errors.New("worker boundary input exceeds limit")
		}
	}
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{operation, boundaryCommand{command.Name, command.Args, command.Env, command.Dir, input}})
	cmd := exec.CommandContext(ctx, b.Command, b.Args...)
	cmd.Env = append(minimalBoundaryEnvironment(), b.Env...)
	cmd.Stdin = bytes.NewReader(payload)
	out := limitedBuffer{Limit: 24 << 20}
	cmd.Stdout = &out
	err := cmd.Run()
	if err != nil {
		return agentruntime.Result{}, fmt.Errorf("worker boundary %s: %w", operation, err)
	}
	var result agentruntime.Result
	decoder := json.NewDecoder(bytes.NewReader(out.Bytes()))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&result); err != nil {
		return agentruntime.Result{}, errors.New("worker boundary returned an invalid result")
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return agentruntime.Result{}, errors.New("worker boundary returned trailing data")
	}
	if result.Code != 0 {
		return result, fmt.Errorf("worker boundary command exited %d", result.Code)
	}
	return result, nil
}

func minimalBoundaryEnvironment() []string {
	var env []string
	for _, name := range []string{"PATH", "TMPDIR", "SYSTEMROOT"} {
		if value := os.Getenv(name); value != "" {
			env = append(env, name+"="+value)
		}
	}
	return env
}
func (b workerBoundaryRunner) Run(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	return b.call(ctx, "run", command)
}

func implementationBoundary(stateRoot string) workerBoundaryRunner {
	if command := os.Getenv("AGENT_SYMPHONY_WORKER_BOUNDARY"); command != "" {
		return workerBoundaryRunner{Command: command}
	}
	binary, _ := os.Executable()
	if !hostIsolationInstalled() {
		return workerBoundaryRunner{Command: binary, Args: []string{"agent-host", "implementation"}, Env: []string{"AGENT_SYMPHONY_LOCAL_ROOT=" + localAttemptRoot(stateRoot)}}
	}
	return workerBoundaryRunner{Command: "sudo", Args: []string{"-n", "-u", workerUser, "-g", attemptGroup, binary, "agent-host", "implementation"}}
}

func reviewBoundary(stateRoot string) workerBoundaryRunner {
	if command := os.Getenv("AGENT_SYMPHONY_REVIEW_BOUNDARY"); command != "" {
		return workerBoundaryRunner{Command: command}
	}
	binary, _ := os.Executable()
	if !hostIsolationInstalled() {
		return workerBoundaryRunner{Command: binary, Args: []string{"agent-host", "review"}, Env: []string{"AGENT_SYMPHONY_LOCAL_ROOT=" + localSnapshotRoot(stateRoot)}}
	}
	return workerBoundaryRunner{Command: "sudo", Args: []string{"-n", "-u", reviewerUser, "-g", snapshotGroup, binary, "agent-host", "review"}}
}

// productionAttemptRoot and productionSnapshotRoot are the single source of
// truth for where implementation/review boundaries operate: the test seam
// (reviewSnapshotRoot), then the zero-admin default under the runtime state
// root, then the advanced host-isolated provisioned root.
func productionAttemptRoot(stateRoot string) string {
	if reviewSnapshotRoot != "" {
		return filepath.Join(filepath.Dir(reviewSnapshotRoot), "attempts")
	}
	if !hostIsolationInstalled() {
		return localAttemptRoot(stateRoot)
	}
	root := "/var/lib/agent-symphony/attempts"
	if runtime.GOOS == "darwin" {
		root = "/var/db/agent-symphony/attempts"
	}
	return root
}

func productionSnapshotRoot(stateRoot string) string {
	if reviewSnapshotRoot != "" {
		return reviewSnapshotRoot
	}
	if !hostIsolationInstalled() {
		return localSnapshotRoot(stateRoot)
	}
	root := "/var/lib/agent-symphony/snapshots"
	if runtime.GOOS == "darwin" {
		root = "/var/db/agent-symphony/snapshots"
	}
	return root
}

func (t environmentToken) Token(context.Context) (internalgithub.InstallationToken, error) {
	if t == "" {
		return internalgithub.InstallationToken{}, errors.New("GITHUB_TOKEN is required")
	}
	return internalgithub.InstallationToken{Value: string(t), ExpiresAt: time.Now().Add(time.Hour)}, nil
}

// githubTokenSource resolves how the coordinator authenticates to the GitHub
// API. When AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH and
// AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID are both set, it mints and
// auto-refreshes installation tokens from the App's own JWT — the only way to
// run serve continuously, since installation tokens expire after an hour and
// nothing can refresh a static GITHUB_TOKEN inside an already-running
// process. Otherwise it falls back to a static GITHUB_TOKEN, sufficient for
// short reconcile/doctor/pr-governance runs. Call this once per process
// invocation, not once per reconcile cycle, so InstallationTokens' internal
// cache is actually reused instead of re-minting a token every cycle.
func githubTokenSource(appID int64) (internalgithub.TokenSource, error) {
	keyPath := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH"))
	installationRaw := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID"))
	if keyPath == "" && installationRaw == "" {
		token := os.Getenv("GITHUB_TOKEN")
		if token == "" {
			return nil, errors.New("GITHUB_TOKEN is required, or set AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH and AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID for auto-refreshing credentials")
		}
		return environmentToken(token), nil
	}
	if keyPath == "" || installationRaw == "" {
		return nil, errors.New("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH and AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID must both be set")
	}
	installationID, err := strconv.ParseInt(installationRaw, 10, 64)
	if err != nil || installationID <= 0 {
		return nil, errors.New("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID must be a positive integer")
	}
	pemBytes, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read GitHub App private key: %w", err)
	}
	key, err := internalgithub.ParsePrivateKeyPEM(pemBytes)
	if err != nil {
		return nil, err
	}
	return &internalgithub.InstallationTokens{BaseURL: githubAPI, InstallationID: installationID, JWTs: internalgithub.AppJWT{AppID: strconv.FormatInt(appID, 10), Key: key}, HTTP: githubClient}, nil
}

// startWebhookListener starts an HTTP server that turns valid signed GitHub
// webhook deliveries into a coalesced early-wake signal for serve's
// reconcile loop. It only starts when both AGENT_SYMPHONY_WEBHOOK_ADDR and
// AGENT_SYMPHONY_WEBHOOK_SECRET are set; without them it returns a nil wake
// channel and a no-op shutdown, and serve continues to rely on periodic
// polling alone exactly as it always has. Periodic reconciliation remains
// the authoritative recovery path either way — a webhook only wakes it up
// sooner; it never replaces it.
func startWebhookListener(ctx context.Context, api internalgithub.API, repository string, stderr io.Writer) (<-chan struct{}, func(context.Context) error, error) {
	noop := func(context.Context) error { return nil }
	addr := strings.TrimSpace(os.Getenv("AGENT_SYMPHONY_WEBHOOK_ADDR"))
	secret := os.Getenv("AGENT_SYMPHONY_WEBHOOK_SECRET")
	if addr == "" && secret == "" {
		return nil, noop, nil
	}
	if addr == "" || secret == "" {
		return nil, nil, errors.New("AGENT_SYMPHONY_WEBHOOK_ADDR and AGENT_SYMPHONY_WEBHOOK_SECRET must both be set")
	}
	installationID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID")
	if err != nil {
		return nil, nil, fmt.Errorf("webhook requires %w", err)
	}
	// Cheap local checks before any network round-trip: fail fast on an
	// unbindable address rather than waiting on a retried API call first.
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, fmt.Errorf("webhook listener: %w", err)
	}
	repositoryID, err := api.RepositoryID(ctx, repository)
	if err != nil {
		_ = listener.Close()
		return nil, nil, err
	}
	hints := make(chan internalgithub.Hint, 64)
	wake := make(chan struct{}, 1)
	go func() {
		for range hints {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
	}()
	handler := internalgithub.Webhook{Secret: []byte(secret), RepositoryID: repositoryID, InstallationID: installationID, Hints: hints, Deliveries: internalgithub.NewDeliveryCache(1024)}
	server := &http.Server{Handler: handler}
	go func() {
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(stderr, "webhook listener: "+err.Error())
		}
	}()
	shutdown := func(shutdownCtx context.Context) error {
		err := server.Shutdown(shutdownCtx)
		close(hints) // safe only after Shutdown returns: no handler can still be sending
		return err
	}
	return wake, shutdown, nil
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && args[0] == "--version" {
		fmt.Fprintln(stdout, strings.TrimPrefix(releaseMetadata, "agent-symphony-release-version:"))
		return 0
	}
	if len(args) == 0 {
		usage(stdout)
		return 0
	}
	command := args[0]
	if command == "worker-capture" {
		if len(args) < 6 || args[4] != "--" {
			return misuse(stderr, false, command, "invalid internal worker capture invocation")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		code, err := agentruntime.CaptureWorker(ctx, args[1], args[2], args[3], args[5:], stdout, stderr)
		if err != nil {
			fmt.Fprintln(stderr, "error: "+err.Error())
		}
		return code
	}
	wantsJSON := hasJSONFlag(args[1:])
	if command == "help" || command == "--help" || command == "-h" {
		if len(args) != 1 {
			return misuse(stderr, wantsJSON, command, "help accepts no arguments")
		}
		usage(stdout)
		return 0
	}
	subcommand := ""
	flagArgs := args[1:]
	if command == "config" && len(flagArgs) > 0 && flagArgs[0] == "view" {
		subcommand = "view"
		flagArgs = flagArgs[1:]
	}
	fs := flag.NewFlagSet(command, flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	path := fs.String("config", config.DefaultPath, "configuration path")
	jsonOutput := fs.Bool("json", false, "emit versioned JSON")
	statePath := fs.String("state", "", "pull-request recovery state file")
	attemptsPath := fs.String("attempts", "", "authoritative attempt facts file")
	runtimeState := fs.String("runtime-state", "", "local runtime state root")
	issueNumber := fs.Int("issue", 0, "issue number to inspect")
	interval := fs.Duration("interval", orchestrator.MaxReconcileInterval, "serve reconciliation interval (maximum 60s)")
	offline := fs.Bool("offline", false, "skip network diagnostics")
	coordinator := fs.String("coordinator", "", "coordinator OS user")
	if err := fs.Parse(flagArgs); err != nil {
		return misuse(stderr, wantsJSON, command, err.Error())
	}

	switch command {
	case "install-host":
		if fs.NArg() != 0 || *coordinator == "" || !onlyFlags(fs, "coordinator", "json") {
			return misuse(stderr, wantsJSON, command, "usage: agent-symphony install-host --coordinator USER")
		}
		if err := installHost(*coordinator); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		return success(stdout, *jsonOutput, command, nil, "host isolation installed")
	case "agent-host":
		if fs.NArg() != 1 || fs.NFlag() != 0 {
			return misuse(stderr, wantsJSON, command, "usage: agent-symphony agent-host implementation|review")
		}
		if err := agentHost(context.Background(), fs.Arg(0), os.Stdin, stdout); err != nil {
			return fail(stderr, false, command, err.Error())
		}
		return 0
	case "serve":
		if fs.NArg() != 0 || *statePath == "" || *runtimeState == "" {
			return misuse(stderr, wantsJSON, command, "serve requires --state and --runtime-state")
		}
		if *interval <= 0 || *interval > orchestrator.MaxReconcileInterval {
			return misuse(stderr, wantsJSON, command, "--interval must be greater than zero and no more than 60s")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if runningOnWSL() {
			root, rootErr := config.GitRoot()
			if rootErr == nil {
				rootErr = validateWSLFilesystem(root, filepath.Join(root, c.WorktreeRoot), *runtimeState, "/proc/mounts")
			}
			if rootErr != nil {
				return fail(stderr, *jsonOutput, command, rootErr.Error())
			}
		}
		appID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		// Built once for the whole serve invocation, not once per reconcile
		// cycle: InstallationTokens caches and only re-mints near real expiry.
		tokens, err := githubTokenSource(appID)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		lock, err := acquireDaemonLock(filepath.Join(*runtimeState, "daemon.lock"))
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		defer releaseDaemonLock(lock)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		api := internalgithub.API{BaseURL: githubAPI, Tokens: tokens, HTTP: githubClient}
		wake, shutdownWebhook, err := startWebhookListener(ctx, api, c.Repository, stderr)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = shutdownWebhook(shutdownCtx)
		}()
		reconcile := func(ctx context.Context) error {
			_, err := reconcileGitHub(ctx, *path, *statePath, *runtimeState, true, tokens)
			if err != nil {
				fmt.Fprintln(stderr, "reconcile: "+internalgithub.Redact(err.Error()))
			}
			return err
		}
		if err := orchestrator.ReconcileLoop(ctx, *interval, wake, reconcile); err != nil && !errors.Is(err, context.Canceled) {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		return 0
	case "status", "list", "inspect", "reconcile":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, command+" accepts no positional arguments")
		}
		var statuses []orchestrator.RecoveryStatus
		var err error
		if *attemptsPath == "" {
			if *statePath == "" || *runtimeState == "" {
				return misuse(stderr, wantsJSON, command, command+" requires --state and --runtime-state unless --attempts is supplied")
			}
			appID, appErr := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
			if appErr != nil {
				return fail(stderr, *jsonOutput, command, appErr.Error())
			}
			tokens, tokenErr := githubTokenSource(appID)
			if tokenErr != nil {
				return fail(stderr, *jsonOutput, command, tokenErr.Error())
			}
			if command == "reconcile" {
				lock, lockErr := acquireDaemonLock(filepath.Join(*runtimeState, "daemon.lock"))
				if lockErr != nil {
					return fail(stderr, *jsonOutput, command, lockErr.Error())
				}
				defer releaseDaemonLock(lock)
			}
			statuses, err = reconcileGitHubRun(context.Background(), *path, *statePath, *runtimeState, command == "reconcile", tokens)
		} else {
			statuses, err = recoveryStatuses(*attemptsPath, *runtimeState)
		}
		if err != nil {
			return fail(stderr, *jsonOutput, command, internalgithub.Redact(err.Error()))
		}
		if command == "inspect" {
			if *issueNumber <= 0 {
				return misuse(stderr, wantsJSON, command, "inspect requires --issue")
			}
			statuses = slices.DeleteFunc(statuses, func(status orchestrator.RecoveryStatus) bool { return status.Issue != *issueNumber })
			if len(statuses) == 0 {
				return fail(stderr, *jsonOutput, command, "issue was not found in authoritative attempt facts")
			}
		}
		if *jsonOutput {
			return writeJSON(stdout, envelope{Version: outputVersion, Command: command, OK: true, Data: statuses})
		}
		for _, status := range statuses {
			fmt.Fprintf(stdout, "%-11s %s#%d attempt %d", strings.ToUpper(status.State), status.Repository, status.Issue, status.Attempt)
			if status.PR > 0 {
				fmt.Fprintf(stdout, " PR #%d", status.PR)
			}
			fmt.Fprintln(stdout)
			fmt.Fprintf(stdout, "  priority: P%d  dependencies: %v\n", status.Priority, status.Dependencies)
			fmt.Fprintf(stdout, "  agents: implementation=%s review=%s\n", firstNonempty(status.ImplementationAgent, "-"), firstNonempty(status.ReviewAgent, "-"))
			fmt.Fprintf(stdout, "  tmux: %s  worktree: %s\n", firstNonempty(status.Session, "-"), firstNonempty(status.Worktree, "-"))
			fmt.Fprintf(stdout, "  branch: %s  head: %s  checks: %v\n", firstNonempty(status.Branch, "-"), firstNonempty(status.HeadSHA, "-"), status.Checks)
			if len(status.Blockers) > 0 {
				fmt.Fprintln(stdout, "  blockers: "+strings.Join(status.Blockers, "; "))
			}
			if status.Diagnostic != "" {
				fmt.Fprintln(stdout, "  diagnostic: "+status.Diagnostic)
			}
			if status.Action != "" {
				fmt.Fprintln(stdout, "  action: "+status.Action)
			}
		}
		return 0
	case "init":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "init accepts no positional arguments")
		}
		if _, err := config.ValidateLocation(*path); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if _, err := os.Stat(*path); err == nil {
			return fail(stderr, *jsonOutput, command, *path+" already exists")
		} else if !errors.Is(err, os.ErrNotExist) {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		repository, err := repositoryFromGit()
		if err != nil {
			return fail(stderr, *jsonOutput, command, "cannot determine GitHub repository: "+err.Error())
		}
		if err := config.Write(*path, config.Default(repository)); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		return success(stdout, *jsonOutput, command, map[string]string{"path": *path}, "created "+*path)

	case "validate":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "validate accepts no positional arguments")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		return success(stdout, *jsonOutput, command, c, *path+" is valid")

	case "config":
		if subcommand != "view" || fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "usage: agent-symphony config view [--config path] [--json]")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if *jsonOutput {
			return writeJSON(stdout, envelope{Version: outputVersion, Command: "config view", OK: true, Data: c})
		}
		b, _ := json.MarshalIndent(c, "", "  ")
		fmt.Fprintln(stdout, string(b))
		return 0

	case "pr-governance":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, "pr-governance accepts no positional arguments")
		}
		if *statePath == "" {
			return fail(stderr, *jsonOutput, command, "--state is required")
		}
		if info, err := os.Lstat(*statePath); err == nil && !info.Mode().IsRegular() || err != nil && !errors.Is(err, os.ErrNotExist) {
			return fail(stderr, *jsonOutput, command, "--state must name a regular recovery file or an absent file in an existing directory")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		appID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		actorID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID")
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		actor := int(actorID)
		if int64(actor) != actorID {
			return fail(stderr, *jsonOutput, command, "AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID is out of range")
		}
		tokens, err := githubTokenSource(appID)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		api := internalgithub.API{BaseURL: githubAPI, Tokens: tokens, HTTP: githubClient}
		prConfig := internalgithub.PRAdapterConfig{
			Repository: c.Repository, ReadyLabel: c.Labels.Ready, HumanReviewLabel: c.CompletionPolicies.HumanReview,
			AutonomousMergeLabel: c.CompletionPolicies.AutonomousMerge, MergeMethod: "squash", PriorityP1Label: c.Labels.PriorityP1,
			PriorityP2Label: c.Labels.PriorityP2, PriorityP3Label: c.Labels.PriorityP3, DependencySection: c.Dependencies.Section,
			DefaultCompletion: c.CompletionPolicies.Default, ApprovalCommand: "/agent-symphony approve", CancelCommand: "/agent-symphony cancel",
			RetryCommand: "/agent-symphony retry", AppID: appID, AppActorID: actor,
		}
		if err := internalgithub.RunPRReconciliation(context.Background(), api, prConfig, *statePath); err != nil {
			return fail(stderr, *jsonOutput, command, internalgithub.Redact(err.Error()))
		}
		return success(stdout, *jsonOutput, command, map[string]string{"state": *statePath}, "pull-request governance reconciliation complete")

	case "doctor", "diagnostics":
		if fs.NArg() != 0 {
			return misuse(stderr, wantsJSON, command, command+" accepts no positional arguments")
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		diagnostics := doctor(c, *offline, *runtimeState)
		ok := true
		for _, d := range diagnostics {
			if d.Status == "fail" {
				ok = false
			}
		}
		if *jsonOutput {
			code := writeJSON(stdout, envelope{Version: outputVersion, Command: command, OK: ok, Diagnostics: diagnostics})
			if !ok {
				return 1
			}
			return code
		}
		for _, d := range diagnostics {
			fmt.Fprintf(stdout, "%-5s %-14s %s\n", strings.ToUpper(d.Status), d.Name, d.Message)
			if d.Action != "" {
				fmt.Fprintln(stdout, "      action: "+d.Action)
			}
		}
		if !ok {
			return 1
		}
		return 0
	default:
		return misuse(stderr, wantsJSON, command, fmt.Sprintf("unknown command %q", command))
	}
}

func onlyFlags(fs *flag.FlagSet, allowed ...string) bool {
	want := make(map[string]bool, len(allowed))
	for _, name := range allowed {
		want[name] = true
	}
	ok := true
	fs.Visit(func(f *flag.Flag) { ok = ok && want[f.Name] })
	return ok
}

func acquireDaemonLock(path string) (*os.File, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, err
	}
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() {
		f.Close()
		return nil, errors.New("daemon lock must be a regular non-symlink file")
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, errors.New("another agent-symphony instance owns this runtime")
	}
	return f, nil
}

func releaseDaemonLock(f *os.File) { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); _ = f.Close() }

func reconcileGitHub(ctx context.Context, configPath, statePath, stateRoot string, transition bool, tokens internalgithub.TokenSource) ([]orchestrator.RecoveryStatus, error) {
	started := time.Now()
	ctx, cancel := context.WithDeadline(ctx, started.Add(2*time.Minute))
	defer cancel()
	c, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	root, err := config.GitRoot()
	if err != nil {
		return nil, err
	}
	if runningOnWSL() {
		if err := validateWSLFilesystem(root, filepath.Join(root, c.WorktreeRoot), stateRoot, "/proc/mounts"); err != nil {
			return nil, err
		}
	}
	appID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
	if err != nil {
		return nil, err
	}
	actorID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID")
	if err != nil {
		return nil, err
	}
	if actorID > int64(^uint(0)>>1) {
		return nil, errors.New("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID is out of range")
	}
	api := internalgithub.API{BaseURL: githubAPI, Tokens: tokens, HTTP: githubClient}
	if err := api.VerifyInstallation(ctx, appID, c.Repository); err != nil {
		return nil, err
	}
	remote, err := internalgithub.FetchAttemptFacts(ctx, api, c.Repository, appID, int(actorID))
	if err != nil {
		return nil, err
	}
	facts := make([]orchestrator.AttemptFact, len(remote))
	for i, f := range remote {
		facts[i] = orchestrator.AttemptFact{Repository: f.Repository, Issue: f.Issue, Attempt: f.Attempt, BaseSHA: f.BaseSHA, HeadSHA: f.HeadSHA, PR: f.PR, State: f.State, Checks: f.Checks}
	}
	prConfig := internalgithub.PRAdapterConfig{Repository: c.Repository, ReadyLabel: c.Labels.Ready, HumanReviewLabel: c.CompletionPolicies.HumanReview, AutonomousMergeLabel: c.CompletionPolicies.AutonomousMerge, MergeMethod: "squash", PriorityP1Label: c.Labels.PriorityP1, PriorityP2Label: c.Labels.PriorityP2, PriorityP3Label: c.Labels.PriorityP3, DependencySection: c.Dependencies.Section, DefaultCompletion: c.CompletionPolicies.Default, ApprovalCommand: "/agent-symphony approve", CancelCommand: "/agent-symphony cancel", RetryCommand: "/agent-symphony retry", AppID: appID, AppActorID: int(actorID)}
	issues, err := internalgithub.FetchIssueFacts(ctx, api, prConfig, remote, transition)
	if err != nil {
		return nil, err
	}
	for i := range issues {
		if binding := issues[i].ActiveAttempt; binding != nil && !slices.ContainsFunc(remote, func(f internalgithub.RecoveryAttemptFact) bool {
			return f.Repository == binding.Repository && f.Issue == binding.Issue && f.Attempt == binding.Attempt
		}) {
			remote = append(remote, *binding)
			facts = append(facts, orchestrator.AttemptFact{Repository: binding.Repository, Issue: binding.Issue, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: binding.State})
		}
	}
	boundary := implementationBoundary(stateRoot)
	binary, err := os.Executable()
	if err != nil {
		return nil, err
	}
	attemptRoot := productionAttemptRoot(stateRoot)
	source, err := seedAttemptSource(ctx, root, attemptRoot)
	if err != nil {
		return nil, err
	}
	r := agentruntime.Runtime{Root: attemptRoot, StateRoot: stateRoot, Source: source, Helper: binary, Runner: boundary, AllowEnv: c.Commands.Environment, VerifyWorker: func(ctx context.Context) error {
		_, err := boundary.call(ctx, "verify", agentruntime.Command{})
		return err
	}}
	manifests, err := r.Discover()
	if err != nil {
		return nil, err
	}
	if transition {
		manifests, err = resumeBoundAttempts(ctx, &r, c, issues, manifests, remote)
		if err != nil {
			return nil, err
		}
	}
	for i := range issues {
		issues[i].Active = issues[i].Active || slices.ContainsFunc(manifests, func(m agentruntime.Manifest) bool {
			return m.Repository == issues[i].Repository && m.Issue == issues[i].Issue && (m.State == "preparing" || m.State == "running")
		})
		for _, manifest := range manifests {
			if manifest.Repository == issues[i].Repository && manifest.Issue == issues[i].Issue && (manifest.State == "failed" || manifest.State == "cancelled" || manifest.State == "completed") {
				bound := issues[i].ActiveAttempt != nil && issues[i].ActiveAttempt.Attempt == manifest.Attempt
				if bound {
					continue
				}
				if issues[i].Retry && issues[i].Attempt > manifest.Attempt {
					issues[i].Attempt = max(issues[i].Attempt, manifest.Attempt+1)
				} else {
					issues[i].Eligible = false
					issues[i].Blockers = append(issues[i].Blockers, "local terminal attempt awaits or has durable GitHub outcome")
				}
			}
		}
	}
	statuses := orchestrator.Recover(facts, manifests)
	statuses, decisions := joinIssueProjection(statuses, issues, c.Concurrency)
	if !transition {
		return statuses, nil
	}
	statuses = orchestrator.RecoverChecked(ctx, facts, manifests, func(ctx context.Context, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
		head := fact.HeadSHA
		if head == "" {
			head = fact.BaseSHA
		}
		return r.VerifyActive(ctx, manifest, head)
	})
	statuses, _ = joinIssueProjection(statuses, issues, c.Concurrency)
	// Governance may mutate GitHub; production only runs it after the verified
	// installation read and authoritative duplicate suppression above.
	if slices.ContainsFunc(statuses, func(s orchestrator.RecoveryStatus) bool {
		return s.State == "blocked" && strings.Contains(s.Diagnostic, "duplicate")
	}) {
		return statuses, nil
	}
	if err := internalgithub.RunPRReconciliation(ctx, api, prConfig, statePath); err != nil {
		return statuses, err
	}
	if err := monitorAttempts(ctx, &r, statuses, manifests, issues); err != nil {
		return statuses, err
	}
	if err := monitorQueuedAttempts(ctx, api, &r, c, issues, manifests, remote, stateRoot); err != nil {
		return statuses, err
	}
	if err := dispatchIssues(ctx, api, &r, c, prConfig, issues, decisions); err != nil {
		return statuses, err
	}
	if err := resumeHandoffs(ctx, &r, statePath, stateRoot, statuses, manifests); err != nil {
		return statuses, err
	}
	if err := ctx.Err(); err != nil {
		return statuses, fmt.Errorf("reconciliation exceeded the two-minute recovery target; retry on the next bounded backoff cycle: %w", err)
	}
	return statuses, nil
}

func joinIssueProjection(statuses []orchestrator.RecoveryStatus, issues []internalgithub.RecoveryIssueFact, capacity int) ([]orchestrator.RecoveryStatus, []orchestrator.Decision) {
	scheduled := make([]orchestrator.Issue, len(issues))
	for i, issue := range issues {
		scheduled[i] = orchestrator.Issue{Repository: issue.Repository, Number: issue.Issue, Priority: issue.Priority, CreatedAt: issue.CreatedAt, Dependencies: issue.Dependencies, Eligible: issue.Eligible, Blockers: issue.Blockers, Active: issue.Active, Completed: issue.Completed}
	}
	decisions := orchestrator.Schedule(scheduled, orchestrator.Capacity{Global: capacity, Repositories: map[string]int{}})
	for _, issue := range issues {
		found := false
		for j := range statuses {
			if statuses[j].Repository == issue.Repository && statuses[j].Issue == issue.Issue {
				statuses[j].Priority, statuses[j].Dependencies = issue.Priority, issue.Dependencies
				statuses[j].Blockers = append(statuses[j].Blockers, issue.Blockers...)
				found = true
			}
		}
		if found {
			continue
		}
		decisionIndex := slices.IndexFunc(decisions, func(d orchestrator.Decision) bool { return d.Repository == issue.Repository && d.Number == issue.Issue })
		if decisionIndex < 0 {
			continue
		}
		decision := decisions[decisionIndex]
		statuses = append(statuses, orchestrator.RecoveryStatus{Repository: issue.Repository, Issue: issue.Issue, Attempt: issue.Attempt, State: string(decision.State), Priority: issue.Priority, Dependencies: issue.Dependencies, Blockers: issue.Blockers, Action: decision.Explanation})
	}
	return statuses, decisions
}

func seedAttemptSource(ctx context.Context, repository, attemptRoot string) (string, error) {
	mode := os.FileMode(0o770)
	if !hostIsolationInstalled() {
		mode = 0o700
	}
	if err := os.MkdirAll(attemptRoot, mode); err != nil {
		return "", fmt.Errorf("open provisioned attempt root: %w", err)
	}
	tmp, err := os.CreateTemp(attemptRoot, ".source-*.bundle")
	if err != nil {
		return "", err
	}
	name := tmp.Name()
	if err := tmp.Close(); err != nil {
		return "", err
	}
	defer os.Remove(name)
	if out, err := exec.CommandContext(ctx, "git", "-C", repository, "bundle", "create", name, "--all").CombinedOutput(); err != nil {
		return "", fmt.Errorf("seed worker source bundle: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if err := os.Chmod(name, 0o640); err != nil {
		return "", err
	}
	path := filepath.Join(attemptRoot, "source.bundle")
	if err := os.Rename(name, path); err != nil {
		return "", err
	}
	return path, nil
}

func startIssueAttempt(ctx context.Context, runtime *agentruntime.Runtime, cfg config.Config, issue internalgithub.RecoveryIssueFact) (agentruntime.Manifest, error) {
	prompt := implementationPrompt(issue)
	return runtime.PrepareAndStart(ctx, agentruntime.Attempt{Repository: issue.Repository, Issue: issue.Issue, Number: issue.Attempt, BaseSHA: issue.BaseSHA, Context: prompt, Command: cfg.Commands.Implementation, Eligible: func() bool { return issue.DispatchAuthorized }})
}

func resumeBoundAttempts(ctx context.Context, runtime *agentruntime.Runtime, cfg config.Config, issues []internalgithub.RecoveryIssueFact, manifests []agentruntime.Manifest, remote []internalgithub.RecoveryAttemptFact) ([]agentruntime.Manifest, error) {
	for _, issue := range issues {
		binding := issue.ActiveAttempt
		remoteConflict := slices.ContainsFunc(remote, func(fact internalgithub.RecoveryAttemptFact) bool {
			return fact.PR > 0 && fact.Repository == issue.Repository && fact.Issue == issue.Issue && (fact.State == "active" || fact.State == "review-ready" || fact.State == "completed")
		})
		if binding == nil || !issue.DispatchAuthorized || remoteConflict || slices.ContainsFunc(manifests, func(manifest agentruntime.Manifest) bool {
			return manifest.Repository == binding.Repository && manifest.Issue == binding.Issue && manifest.Attempt == binding.Attempt
		}) {
			continue
		}
		issue.Attempt, issue.BaseSHA = binding.Attempt, binding.BaseSHA
		manifest, err := startIssueAttempt(ctx, runtime, cfg, issue)
		if err != nil {
			return manifests, fmt.Errorf("resume bound %s#%d attempt %d: %w", issue.Repository, issue.Issue, issue.Attempt, err)
		}
		manifests = append(manifests, manifest)
	}
	return manifests, nil
}

func dispatchIssues(ctx context.Context, api internalgithub.API, runtime *agentruntime.Runtime, cfg config.Config, prConfig internalgithub.PRAdapterConfig, issues []internalgithub.RecoveryIssueFact, decisions []orchestrator.Decision) error {
	for _, decision := range decisions {
		if decision.State != orchestrator.Runnable {
			continue
		}
		index := slices.IndexFunc(issues, func(issue internalgithub.RecoveryIssueFact) bool {
			return issue.Repository == decision.Repository && issue.Issue == decision.Number
		})
		if index < 0 {
			return errors.New("scheduler returned an unknown issue")
		}
		issue := issues[index]
		if err := internalgithub.EnsureActiveAttempt(ctx, api, prConfig, issue.Issue, issue.Attempt, issue.BaseSHA); err != nil {
			return fmt.Errorf("bind dispatch %s#%d attempt %d: %w", issue.Repository, issue.Issue, issue.Attempt, err)
		}
		issue.DispatchAuthorized = issue.Eligible
		if _, err := startIssueAttempt(ctx, runtime, cfg, issue); err != nil {
			return fmt.Errorf("dispatch %s#%d attempt %d: %w", issue.Repository, issue.Issue, issue.Attempt, err)
		}
	}
	return nil
}

func implementationPrompt(issue internalgithub.RecoveryIssueFact) string {
	return fmt.Sprintf("Repository: %s\nIssue: #%d\nAttempt: %d\n\n%s\n\nCompletion contract: Make stdout exactly one JSON line of at most 64 KiB with nonempty validation and documentation evidence; progress and diagnostics belong on stderr. Agent Symphony captures stdout outside the worktree. Do not wrap it in Markdown fences or emit another stdout object.\n{\"type\":\"agent-symphony-result-v1\",\"validation\":\"tests run and results\",\"documentation\":\"documentation impact or none\"}", issue.Repository, issue.Issue, issue.Attempt, issue.Body)
}

type workerResult struct {
	Type          string `json:"type"`
	Validation    string `json:"validation"`
	Documentation string `json:"documentation"`
	Decisions     string `json:"decisions,omitempty"`
}

type workerExport struct {
	Type         string       `json:"type"`
	Repository   string       `json:"repository"`
	Branch       string       `json:"branch"`
	BaseSHA      string       `json:"base_sha"`
	HeadSHA      string       `json:"head_sha"`
	BundleSHA256 string       `json:"bundle_sha256"`
	Clean        bool         `json:"clean"`
	Result       workerResult `json:"result"`
	Bundle       string       `json:"bundle"`
}

func importWorkerExport(ctx context.Context, boundary workerBoundaryRunner, manifest agentruntime.Manifest) (workerResult, string, string, error) {
	request, _ := json.Marshal(manifest)
	response, err := boundary.call(ctx, "export", agentruntime.Command{Stdin: bytes.NewReader(request)})
	if err != nil {
		return workerResult{}, "", "", err
	}
	var exported workerExport
	decoder := json.NewDecoder(strings.NewReader(response.Output))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&exported) != nil || decoder.Decode(&struct{}{}) != io.EOF || exported.Type != "agent-symphony-export-v1" || exported.Repository != manifest.Repository || exported.Branch != manifest.Branch || exported.BaseSHA != manifest.BaseSHA || !exported.Clean || exported.Result.Type != "agent-symphony-result-v1" || strings.TrimSpace(exported.Result.Validation) == "" || strings.TrimSpace(exported.Result.Documentation) == "" {
		return workerResult{}, "", "", errors.New("worker boundary returned invalid attested metadata")
	}
	bundle, err := base64.StdEncoding.DecodeString(exported.Bundle)
	if err != nil || len(bundle) == 0 || len(bundle) > 16<<20 || fmt.Sprintf("%x", sha256.Sum256(bundle)) != exported.BundleSHA256 {
		return workerResult{}, "", "", errors.New("worker boundary returned invalid or oversized bundle")
	}
	temp, err := os.MkdirTemp("", "agent-symphony-import-")
	if err != nil {
		return workerResult{}, "", "", err
	}
	defer os.RemoveAll(temp)
	bundlePath := filepath.Join(temp, "attempt.bundle")
	if err := os.WriteFile(bundlePath, bundle, 0o600); err != nil {
		return workerResult{}, "", "", err
	}
	importedRepo := filepath.Join(temp, "repository.git")
	if err := scanGit(ctx, temp, nil, []string{"init", "--bare", importedRepo}, nil); err != nil {
		return workerResult{}, "", "", fmt.Errorf("create temporary import repository: %w", err)
	}
	if err := scanGit(ctx, importedRepo, nil, []string{"bundle", "verify", bundlePath}, nil); err != nil {
		return workerResult{}, "", "", errors.New("worker bundle verification failed")
	}
	advertised := false
	if err := scanGit(ctx, importedRepo, nil, []string{"bundle", "list-heads", bundlePath}, func(line []byte) error {
		fields := bytes.Fields(line)
		if len(fields) != 2 || !preflightObjectID.Match(fields[0]) {
			return errors.New("invalid bundle ref")
		}
		advertised = advertised || string(fields[0]) == exported.HeadSHA
		return nil
	}); err != nil || !advertised {
		return workerResult{}, "", "", errors.New("worker head is not advertised by bundle")
	}
	if err := preflightBundle(ctx, bundle, bundlePath, importedRepo); err != nil {
		return workerResult{}, "", "", fmt.Errorf("worker bundle object bounds: %w", err)
	}
	if err := scanGit(ctx, importedRepo, nil, []string{"fetch", "--no-tags", bundlePath, exported.HeadSHA}, nil); err != nil {
		return workerResult{}, "", "", fmt.Errorf("import worker bundle: %w", err)
	}
	head, err := gitSingleLine(ctx, importedRepo, "rev-parse", "FETCH_HEAD")
	if err != nil || head != exported.HeadSHA || !regexp.MustCompile(`^[0-9a-f]{40,64}$`).MatchString(head) {
		return workerResult{}, "", "", errors.New("imported worker head changed")
	}
	if err := scanGit(ctx, importedRepo, nil, []string{"merge-base", "--is-ancestor", manifest.BaseSHA, head}, nil); err != nil || strings.EqualFold(head, manifest.BaseSHA) {
		return workerResult{}, "", "", errors.New("worker head is not a new descendant of approved base")
	}
	if err := validateWorkerTree(ctx, importedRepo, head); err != nil {
		return workerResult{}, "", "", errors.New("worker bundle contains a symlink or result marker")
	}
	root, err := config.GitRoot()
	if err != nil {
		return workerResult{}, "", "", err
	}
	if err := scanGit(ctx, root, nil, []string{"fetch", "--no-tags", importedRepo, head}, nil); err != nil {
		return workerResult{}, "", "", fmt.Errorf("import verified worker head: %w", err)
	}
	return exported.Result, head, root, nil
}

const (
	preflightMaxObjects = 100000
	preflightMaxBytes   = 32 << 20
	preflightMaxObject  = 8 << 20
	preflightMaxLine    = 1024
	preflightMaxOutput  = 64 << 20
	preflightMaxStderr  = 64 << 10
	preflightMaxEntries = 100000
)

var preflightObjectID = regexp.MustCompile(`^[0-9a-f]{40,64}$`)

type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if remaining := b.limit - b.Len(); remaining > 0 {
		_, _ = b.Buffer.Write(p[:min(len(p), remaining)])
	}
	return len(p), nil
}

func scanGit(ctx context.Context, repo string, stdin io.Reader, args []string, line func([]byte) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", "-c", "core.hooksPath=/dev/null", "-C", repo}, args...)...)
	cmd.Stdin = stdin
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr := cappedBuffer{limit: preflightMaxStderr}
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		return err
	}
	abort := func(err error) error {
		cancel()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		return err
	}
	scanner := bufio.NewScanner(stdout)
	scanner.Buffer(make([]byte, preflightMaxLine), preflightMaxLine)
	var outputBytes int64
	for scanner.Scan() {
		outputBytes += int64(len(scanner.Bytes()) + 1)
		if outputBytes > preflightMaxOutput {
			return abort(errors.New("git output byte limit exceeded"))
		}
		if line != nil {
			if err := line(scanner.Bytes()); err != nil {
				return abort(err)
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return abort(err)
	}
	if err := cmd.Wait(); err != nil {
		return fmt.Errorf("git %s: %w: %s", args[0], err, strings.TrimSpace(stderr.String()))
	}
	return nil
}

func gitSingleLine(ctx context.Context, repo string, args ...string) (string, error) {
	var result string
	err := scanGit(ctx, repo, nil, args, func(line []byte) error {
		if result != "" {
			return errors.New("unexpected multi-line git output")
		}
		result = string(line)
		return nil
	})
	return result, err
}

func validateWorkerTree(ctx context.Context, repo, head string) error {
	var count, total int64
	return scanGit(ctx, repo, nil, []string{"ls-tree", "-rl", head}, func(line []byte) error {
		count++
		if count > preflightMaxEntries {
			return errors.New("tree entry count exceeded")
		}
		tab := bytes.IndexByte(line, '\t')
		if tab < 0 {
			return errors.New("invalid tree entry")
		}
		fields, path := bytes.Fields(line[:tab]), line[tab+1:]
		if len(fields) != 4 || (string(fields[0]) != "100644" && string(fields[0]) != "100755") || string(fields[1]) != "blob" || !preflightObjectID.Match(fields[2]) || len(path) == 0 || path[0] == '/' || bytes.ContainsAny(path, "\\\n\r") || string(path) != filepath.Clean(string(path)) {
			return errors.New("invalid tree mode or path")
		}
		size, err := strconv.ParseInt(string(fields[3]), 10, 64)
		if err != nil || size < 0 || size > preflightMaxObject || total > preflightMaxBytes-size {
			return errors.New("tree declared size exceeded")
		}
		total += size
		return nil
	})
}

func preflightBundle(ctx context.Context, bundle []byte, bundlePath, repo string) error {
	start := bytes.Index(bundle, []byte("\nPACK"))
	if start < 0 {
		return errors.New("pack payload missing")
	}
	pack := filepath.Join(repo, "objects", "pack", "incoming.pack")
	if err := os.WriteFile(pack, bundle[start+1:], 0o600); err != nil {
		return err
	}
	if err := scanGit(ctx, repo, nil, []string{"index-pack", "--strict", pack}, nil); err != nil {
		return errors.New("invalid pack")
	}
	var count, total int64
	err := scanGit(ctx, repo, nil, []string{"verify-pack", "-v", strings.TrimSuffix(pack, ".pack") + ".idx"}, func(line []byte) error {
		fields := bytes.Fields(line)
		if len(fields) < 5 || !preflightObjectID.Match(fields[0]) {
			return nil
		}
		count++
		if count > preflightMaxObjects {
			return errors.New("expanded object count or bytes exceeded")
		}
		size, parseErr := strconv.ParseInt(string(fields[2]), 10, 64)
		if parseErr != nil || size < 0 || size > preflightMaxObject {
			return errors.New("oversized expanded object")
		}
		total += size
		if total > preflightMaxBytes {
			return errors.New("expanded object count or bytes exceeded")
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("pack verification failed: %w", err)
	}
	if count == 0 {
		return errors.New("empty pack")
	}
	var refs []string
	err = scanGit(ctx, repo, nil, []string{"bundle", "list-heads", bundlePath}, func(line []byte) error {
		if len(refs) >= 1024 {
			return errors.New("bundle ref count exceeded")
		}
		fields := bytes.Fields(line)
		if len(fields) != 2 || !preflightObjectID.Match(fields[0]) {
			return errors.New("invalid bundle ref")
		}
		ref := fmt.Sprintf("refs/preflight/%d", len(refs))
		if err := scanGit(ctx, repo, nil, []string{"update-ref", ref, string(fields[0])}, nil); err != nil {
			return errors.New("bundle ref object missing")
		}
		refs = append(refs, ref)
		return nil
	})
	if err != nil || len(refs) == 0 {
		return errors.New("bundle refs missing")
	}
	objects, err := os.CreateTemp(repo, "preflight-objects-")
	if err != nil {
		return err
	}
	defer os.Remove(objects.Name())
	defer objects.Close()
	count = 0
	err = scanGit(ctx, repo, nil, append([]string{"rev-list", "--objects"}, refs...), func(line []byte) error {
		fields := bytes.Fields(line)
		if len(fields) == 0 || !preflightObjectID.Match(fields[0]) {
			return errors.New("malformed reachable object")
		}
		count++
		if count > preflightMaxObjects {
			return errors.New("reachable object count exceeded")
		}
		_, err := objects.Write(append(fields[0], '\n'))
		return err
	})
	if err != nil || count == 0 {
		return errors.New("bundle reachability failed")
	}
	if _, err := objects.Seek(0, io.SeekStart); err != nil {
		return err
	}
	count, total = 0, 0
	err = scanGit(ctx, repo, objects, []string{"cat-file", "--batch-check=%(objectname) %(objecttype) %(objectsize)"}, func(line []byte) error {
		fields := bytes.Fields(line)
		if len(fields) != 3 {
			return errors.New("reachable object batch check malformed")
		}
		count++
		if count > preflightMaxObjects {
			return errors.New("reachable expanded object bounds exceeded")
		}
		size, parseErr := strconv.ParseInt(string(fields[2]), 10, 64)
		if parseErr != nil || size < 0 || size > preflightMaxObject || total > preflightMaxBytes-size {
			return errors.New("reachable expanded object bounds exceeded")
		}
		total += size
		return nil
	})
	if err != nil {
		return fmt.Errorf("reachable object batch check failed: %w", err)
	}
	return nil
}

func monitorQueuedAttempts(ctx context.Context, api internalgithub.API, runtime *agentruntime.Runtime, cfg config.Config, issues []internalgithub.RecoveryIssueFact, manifests []agentruntime.Manifest, remote []internalgithub.RecoveryAttemptFact, stateRoot string) error {
	for _, manifest := range manifests {
		remoteIndex := slices.IndexFunc(remote, func(f internalgithub.RecoveryAttemptFact) bool {
			return f.Repository == manifest.Repository && f.Issue == manifest.Issue && f.Attempt == manifest.Attempt
		})
		if remoteIndex < 0 || remote[remoteIndex].PR > 0 || remote[remoteIndex].BaseSHA != manifest.BaseSHA {
			continue
		}
		index := slices.IndexFunc(issues, func(i internalgithub.RecoveryIssueFact) bool {
			return i.Repository == manifest.Repository && i.Issue == manifest.Issue && i.Attempt == manifest.Attempt
		})
		if index < 0 {
			continue
		}
		issue := issues[index]
		if !issue.DispatchAuthorized {
			continue
		}
		current := manifest
		if manifest.State == "preparing" || manifest.State == "running" {
			continue // monitorAttempts owns live bound attempts from the same snapshot.
		}
		if current.State == "completed" {
			pending, err := publishWorkerResult(ctx, api, runtime, cfg, issue, current, stateRoot)
			if err != nil {
				return durableAttemptFailure(ctx, api, issue, current, err)
			}
			if pending {
				continue
			}
		} else if current.State == "failed" || current.State == "cancelled" {
			if err := durableAttemptFailure(ctx, api, issue, current, errors.New(current.Diagnostic)); err != nil {
				return err
			}
		}
	}
	return nil
}

func publishWorkerResult(ctx context.Context, api internalgithub.API, runtimeState *agentruntime.Runtime, cfg config.Config, issue internalgithub.RecoveryIssueFact, manifest agentruntime.Manifest, stateRoot string) (bool, error) {
	boundary := implementationBoundary(stateRoot)
	snapshotRoot := productionSnapshotRoot(stateRoot)
	reviewEnv, err := configuredAgentEnvironment(cfg.Commands.Environment)
	if err != nil {
		return false, err
	}
	result, head, root, err := importWorkerExport(ctx, boundary, manifest)
	if err != nil {
		return false, err
	}
	review := independentReviewResult{Type: "agent-symphony-review-v1", Status: "clean"}
	pending := false
	attempt := agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA, Command: cfg.Commands.Implementation}
	if (manifest.ReviewState == "clean" || manifest.ReviewState == "findings-queued") && manifest.ReviewHead == head && (manifest.ReviewSnapshot != "" || manifest.ReviewSession != "") {
		var cleanupErr error
		manifest, cleanupErr = cleanupReviewOutcome(ctx, runtimeState, attempt, reviewBoundary(stateRoot), reviewEnv, manifest, snapshotRoot)
		if cleanupErr != nil {
			return true, nil
		}
	}
	if manifest.ReviewState == "findings-queued" && manifest.ReviewHead == head {
		return returnReviewFindings(ctx, runtimeState, boundary, attempt, manifest, head, manifest.ReviewFindings, cfg.Commands.Implementation)
	}
	if manifest.ReviewState != "clean" || manifest.ReviewHead != head {
		review, pending, err = runIndependentReview(ctx, runtimeState, attempt, reviewBoundary(stateRoot), reviewEnv, cfg.Commands.Reviewer, issue, manifest, root, head, snapshotRoot)
	}
	if err != nil {
		return false, fmt.Errorf("independent review: %w", err)
	}
	if pending {
		return true, nil
	}
	if len(review.Findings) > 0 {
		queued, err := runtimeState.RecordReviewFindings(attempt, head, review.Findings, false, false)
		if err != nil {
			return false, err
		}
		return returnReviewFindings(ctx, runtimeState, boundary, attempt, queued, head, review.Findings, cfg.Commands.Implementation)
	}
	if manifest.ReviewState != "clean" {
		if _, err := runtimeState.RecordReview(attempt, "clean", head, "", ""); err != nil {
			return false, err
		}
	}
	run := func(dir string, args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", "-c", "core.hooksPath=/dev/null", "-C", dir}, args...)...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	if _, err := run(root, "push", "origin", "FETCH_HEAD:refs/heads/"+manifest.Branch); err != nil {
		return false, fmt.Errorf("publish verified worker head: %w", err)
	}
	body, err := internalgithub.PullRequestBody(issue.Issue, issue.Attempt, result.Validation, result.Documentation, result.Decisions)
	if err != nil {
		return false, err
	}
	mutation := internalgithub.Mutation{Issue: issue.Issue, Attempt: issue.Attempt}
	appID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
	if err != nil {
		return false, err
	}
	actorID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID")
	if err != nil {
		return false, err
	}
	pr, currentBody, err := internalgithub.FindPublishedAttempt(ctx, api, issue.Repository, manifest.Branch, head, appID, int(actorID))
	if err != nil {
		return false, err
	}
	if pr.Number == 0 {
		pr, err = api.CreatePullRequest(ctx, issue.Repository, issue.Title, manifest.Branch, issue.BaseBranch, body, mutation)
		if err != nil {
			// An ambiguous create is recovered by deterministic head lookup.
			pr, currentBody, _ = internalgithub.FindPublishedAttempt(ctx, api, issue.Repository, manifest.Branch, head, appID, int(actorID))
			if pr.Number == 0 {
				return false, err
			}
		}
	}
	bound, err := internalgithub.BindPullRequestBody(body, issue.Issue, issue.Attempt, manifest.Branch, head, pr.Number)
	if err != nil {
		return false, err
	}
	if currentBody != bound {
		// Re-read before mutation so a restart or concurrent edit cannot create a second PR.
		fresh, freshBody, findErr := internalgithub.FindPublishedAttempt(ctx, api, issue.Repository, manifest.Branch, head, appID, int(actorID))
		if findErr != nil || fresh.Number != pr.Number {
			return false, errors.New("pull request identity changed before binding")
		}
		currentBody = freshBody
	}
	if currentBody != bound {
		if err := api.UpdatePullRequest(ctx, issue.Repository, pr.Number, bound, mutation); err != nil {
			return false, err
		}
	}
	marker, _ := internalgithub.AttemptMarker(issue.Issue, issue.Attempt, manifest.Branch, head, pr.Number, "review")
	comment, _ := internalgithub.AttributedBody(issue.Issue, issue.Attempt, "Attempt published for review.")
	present, err := internalgithub.HasAttemptComment(ctx, api, issue.Repository, issue.Issue, marker, appID, int(actorID))
	if err != nil {
		return false, err
	}
	if present {
		return false, nil
	}
	return false, api.CreateIssueComment(ctx, issue.Repository, issue.Issue, comment+"\n\n"+marker, mutation)
}

func returnReviewFindings(ctx context.Context, runtimeState *agentruntime.Runtime, boundary workerBoundaryRunner, attempt agentruntime.Attempt, manifest agentruntime.Manifest, head string, findings, command []string) (bool, error) {
	key, outcomePath := "independent-review-"+head, manifest.LogPath+".review-outcome"
	handoff, _ := json.Marshal(struct{ Type, Key, Findings string }{"agent-symphony-handoff-v1", key, strings.Join(findings, "\n")})
	accept := func() error {
		request, _ := json.Marshal(struct {
			Manifest     agentruntime.Manifest `json:"manifest"`
			Handoff      json.RawMessage       `json:"handoff"`
			OutcomePath  string                `json:"outcome_path"`
			OutcomeToken string                `json:"outcome_token"`
			Command      []string              `json:"command"`
		}{manifest, handoff, outcomePath, head, command})
		accepted, err := boundary.call(ctx, "accept-handoff", agentruntime.Command{Stdin: bytes.NewReader(request)})
		if err != nil {
			return err
		}
		var ack struct{ Type, Key, OutcomePath, OutcomeToken string }
		decoder := json.NewDecoder(strings.NewReader(accepted.Output))
		decoder.DisallowUnknownFields()
		if decoder.Decode(&ack) != nil || decoder.Decode(&struct{}{}) != io.EOF || ack.Type != "agent-symphony-handoff-executed-v1" || ack.Key != key || ack.OutcomePath != outcomePath || ack.OutcomeToken != head {
			return errors.New("review findings handoff acceptance binding mismatch")
		}
		return nil
	}
	if len(command) == 0 {
		return false, errors.New("implementation command is missing")
	}
	if !manifest.ReviewHandoffAck {
		if err := accept(); err != nil {
			if manifest.ReviewHandoffQueued {
				return true, nil
			}
			return false, err
		}
		if !manifest.ReviewHandoffQueued {
			var err error
			manifest, err = runtimeState.RecordReviewFindings(attempt, head, findings, true, false)
			if err != nil {
				return false, err
			}
		}
		if _, err := runtimeState.RecordReviewFindings(attempt, head, findings, true, true); err != nil {
			return false, err
		}
	}
	return false, nil
}

func cleanupReviewOutcome(ctx context.Context, runtimeState *agentruntime.Runtime, attempt agentruntime.Attempt, boundary boundaryCaller, env []string, manifest agentruntime.Manifest, snapshotRoot string) (agentruntime.Manifest, error) {
	if err := cleanupReviewResources(ctx, boundary, env, attempt, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession, snapshotRoot); err != nil {
		return manifest, err
	}
	return runtimeState.RecordReview(attempt, manifest.ReviewState, manifest.ReviewHead, "", "")
}

func reviewIdentity(attempt agentruntime.Attempt, snapshotRoot string) (string, string) {
	return filepath.Join(snapshotRoot, fmt.Sprintf("%s-%d-%d", strings.ReplaceAll(attempt.Repository, "/", "-"), attempt.Issue, attempt.Number)), fmt.Sprintf("as-review-%d-%d", attempt.Issue, attempt.Number)
}

func reviewResultPath(snapshot, head string) string {
	sum := sha256.Sum256([]byte(head))
	return filepath.Join(fmt.Sprintf("%s.result-%x", snapshot, sum[:8]), "result.json")
}

func cleanupReviewResources(ctx context.Context, boundary boundaryCaller, env []string, attempt agentruntime.Attempt, head, snapshot, session, snapshotRoot string) error {
	expectedSnapshot, expectedSession := reviewIdentity(attempt, snapshotRoot)
	if (snapshot != "" && snapshot != expectedSnapshot) || (session != "" && session != expectedSession) {
		return errors.New("persisted reviewer cleanup identity mismatch")
	}
	if snapshot != "" {
		info, err := os.Lstat(snapshot)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		if (err == nil && info.Mode()&os.ModeSymlink != 0) || !belowRoot(snapshot, filepath.Dir(expectedSnapshot)) {
			return errors.New("review snapshot cleanup path is not a non-symlink descendant of the snapshot root")
		}
	}
	if session != "" {
		cleanupCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		result, err := boundary.call(cleanupCtx, "run", agentruntime.Command{Name: "tmux", Args: []string{"kill-session", "-t", "=" + session}, Dir: filepath.Dir(snapshot), Env: env})
		if err != nil && !(result.Exited && result.Code == 1) {
			return err
		}
	}
	resultPath := reviewResultPath(expectedSnapshot, head)
	resultRoot := filepath.Dir(resultPath)
	if info, err := os.Lstat(resultRoot); err == nil {
		if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || os.RemoveAll(resultRoot) != nil {
			return errors.New("review result cleanup path is invalid")
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if snapshot != "" {
		_ = filepath.WalkDir(snapshot, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o750)
			}
			return nil
		})
		if err := os.RemoveAll(snapshot); err != nil {
			return err
		}
	}
	return nil
}

type independentReviewResult struct {
	Type     string   `json:"type"`
	Status   string   `json:"status"`
	Findings []string `json:"findings"`
	Snapshot string   `json:"-"`
	Session  string   `json:"-"`
}

func reviewPrompt(issue internalgithub.RecoveryIssueFact, base string) string {
	return fmt.Sprintf("Review only the exact attested change at %s..HEAD for issue #%d attempt %d. Inspect that entire diff and its affected code for correctness, regressions, security, and missing behavioral tests. Make the entire final response exactly one bounded JSON object on stdout: {\"type\":\"agent-symphony-review-v1\",\"status\":\"clean\",\"findings\":[]} or status findings with actionable finding strings. Do not wrap it in Markdown, emit prose, or emit another object.\n\n%s", base, issue.Issue, issue.Attempt, issue.Body)
}

func runIndependentReview(ctx context.Context, runtimeState *agentruntime.Runtime, attempt agentruntime.Attempt, boundary boundaryCaller, env, command []string, issue internalgithub.RecoveryIssueFact, manifest agentruntime.Manifest, source, head, snapshotRoot string) (independentReviewResult, bool, error) {
	if len(command) == 0 {
		return independentReviewResult{}, false, errors.New("reviewer command is missing")
	}
	if !preflightObjectID.MatchString(attempt.BaseSHA) {
		return independentReviewResult{}, false, errors.New("review base is missing or invalid")
	}
	snapshot, session := reviewIdentity(attempt, snapshotRoot)
	if manifest.ReviewState == "running" && manifest.ReviewHead == head && manifest.ReviewSnapshot == snapshot && manifest.ReviewSession == session {
		result, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"display-message", "-p", "-t", agentruntime.PaneTarget(session), "#{pane_dead} #{pane_dead_status}"}, Dir: snapshot, Env: env})
		if err == nil {
			dead, status, statusErr := agentruntime.ParsePaneStatus(result.Output)
			if statusErr == nil && !dead {
				return independentReviewResult{Snapshot: snapshot, Session: session}, true, nil
			}
			if statusErr != nil {
				// The reviewer is temporarily unobservable; rebuild below.
			} else if status != 0 {
				return independentReviewResult{}, false, fmt.Errorf("reviewer exited %d", status)
			} else {
				request, _ := json.Marshal(reviewResultRequest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Head: head})
				artifact, err := boundary.call(ctx, "review-result", agentruntime.Command{Stdin: bytes.NewReader(request)})
				if err != nil {
					if artifact.Exited && artifact.Code == reviewResultInvalidCode {
						return independentReviewResult{}, false, err
					}
					return independentReviewResult{Snapshot: snapshot, Session: session}, true, nil
				}
				parsed, err := parseIndependentReview(artifact.Output)
				if err != nil {
					return independentReviewResult{}, false, err
				}
				if runtimeState != nil {
					var persisted agentruntime.Manifest
					if parsed.Status == "findings" {
						persisted, err = runtimeState.RecordReviewFindings(attempt, head, parsed.Findings, false, false)
					} else {
						persisted, err = runtimeState.RecordReview(attempt, "clean", head, snapshot, session)
					}
					if err != nil {
						return independentReviewResult{}, false, err
					}
					if _, err := cleanupReviewOutcome(ctx, runtimeState, attempt, boundary, env, persisted, snapshotRoot); err != nil {
						return parsed, true, nil
					}
				} else {
					if err := cleanupReviewResources(ctx, boundary, env, attempt, head, snapshot, session, snapshotRoot); err != nil {
						return parsed, true, nil
					}
				}
				return parsed, false, nil
			}
		}
	}
	if runtimeState != nil {
		if _, err := runtimeState.RecordReview(attempt, "preparing", head, snapshot, session); err != nil {
			return independentReviewResult{}, false, err
		}
	}
	if err := cleanupReviewResources(ctx, boundary, env, attempt, head, snapshot, session, snapshotRoot); err != nil {
		return independentReviewResult{}, true, nil
	}
	if out, err := exec.CommandContext(ctx, "git", "clone", "--no-local", "--no-checkout", source, snapshot).CombinedOutput(); err != nil {
		return independentReviewResult{}, false, fmt.Errorf("create snapshot: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if out, err := exec.CommandContext(ctx, "git", "-C", snapshot, "fetch", "--no-tags", source, head+":refs/agent-symphony/attested-review").CombinedOutput(); err != nil {
		return independentReviewResult{}, false, fmt.Errorf("transfer attested head: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if out, err := exec.CommandContext(ctx, "git", "-C", snapshot, "checkout", "--detach", head).CombinedOutput(); err != nil {
		return independentReviewResult{}, false, fmt.Errorf("checkout attested head: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if got, err := gitSingleLine(ctx, snapshot, "rev-parse", "HEAD"); err != nil || got != head {
		return independentReviewResult{}, false, errors.New("review snapshot HEAD differs from attested worker head")
	}
	if err := scanGit(ctx, snapshot, nil, []string{"merge-base", "--is-ancestor", attempt.BaseSHA, "HEAD"}, nil); err != nil || strings.EqualFold(attempt.BaseSHA, head) {
		return independentReviewResult{}, false, errors.New("review base is unavailable or not an ancestor of attested HEAD")
	}
	_ = exec.CommandContext(ctx, "git", "-C", snapshot, "remote", "remove", "origin").Run()
	_ = exec.CommandContext(ctx, "git", "-C", snapshot, "update-ref", "-d", "refs/agent-symphony/attested-review").Run()
	_ = exec.CommandContext(ctx, "git", "-C", snapshot, "config", "--local", "credential.helper", "").Run()
	reviewGID := -1
	if reviewSnapshotRoot == "" && hostIsolationInstalled() {
		group, err := user.LookupGroup(snapshotGroup)
		if err != nil {
			return independentReviewResult{}, false, err
		}
		reviewGID, _ = strconv.Atoi(group.Gid)
	}
	if err := filepath.WalkDir(snapshot, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if reviewGID >= 0 {
			if err := os.Chown(path, -1, reviewGID); err != nil {
				return err
			}
		}
		if entry.IsDir() {
			return os.Chmod(path, 0o550)
		}
		return os.Chmod(path, 0o440)
	}); err != nil {
		return independentReviewResult{}, false, err
	}
	resultPath := reviewResultPath(snapshot, head)
	resultRoot := filepath.Dir(resultPath)
	if err := os.Mkdir(resultRoot, 0o770); err != nil || reviewGID >= 0 && os.Chown(resultRoot, -1, reviewGID) != nil || os.Chmod(resultRoot, 0o770) != nil {
		_ = os.RemoveAll(resultRoot)
		return independentReviewResult{}, false, errors.New("prepare review result artifact")
	}
	args := []string{"new-session", "-d", "-s", session, "-c", snapshot}
	for _, value := range env {
		args = append(args, "-e", value)
	}
	if _, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: args, Dir: snapshot, Env: env}); err != nil {
		return independentReviewResult{}, false, err
	}
	if _, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"set-option", "-w", "-t", agentruntime.PaneTarget(session), "remain-on-exit", "on"}, Dir: snapshot, Env: env}); err != nil {
		return independentReviewResult{}, false, err
	}
	prompt := reviewPrompt(issue, attempt.BaseSHA)
	if _, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"load-buffer", "-b", session, "-"}, Dir: snapshot, Env: env, Stdin: strings.NewReader(prompt)}); err != nil {
		return independentReviewResult{}, false, err
	}
	binary, err := os.Executable()
	if err != nil {
		return independentReviewResult{}, false, err
	}
	command = agentruntime.PromptCommand(binary, "tmux", session, resultPath, command)
	if _, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: append([]string{"respawn-pane", "-k", "-t", agentruntime.PaneTarget(session), "--"}, command...), Dir: snapshot, Env: env}); err != nil {
		return independentReviewResult{}, false, err
	}
	if runtimeState != nil {
		if _, err := runtimeState.RecordReview(attempt, "running", head, snapshot, session); err != nil {
			return independentReviewResult{}, false, err
		}
	}
	return independentReviewResult{Snapshot: snapshot, Session: session}, true, nil
}

func configuredAgentEnvironment(allow []string) ([]string, error) {
	env, err := internalgithub.AgentEnvironmentWith(os.Environ(), allow...)
	return slices.DeleteFunc(env, func(value string) bool { return strings.HasPrefix(value, "GIT_CONFIG_") }), err
}

func parseIndependentReview(output string) (independentReviewResult, error) {
	if len(output) == 0 || len(output) > maxReviewResultSize {
		return independentReviewResult{}, errors.New("review result is missing or exceeds 64 KiB")
	}
	var result independentReviewResult
	d := json.NewDecoder(strings.NewReader(output))
	d.DisallowUnknownFields()
	if d.Decode(&result) != nil || d.Decode(&struct{}{}) != io.EOF || result.Type != "agent-symphony-review-v1" {
		return independentReviewResult{}, errors.New("reviewer returned invalid structured clean/findings result")
	}
	if (result.Status != "clean" && result.Status != "findings") || (result.Status == "clean") != (len(result.Findings) == 0) || len(result.Findings) > 100 {
		return independentReviewResult{}, errors.New("reviewer returned invalid structured clean/findings result")
	}
	for _, finding := range result.Findings {
		if strings.TrimSpace(finding) == "" || len(finding) > 4096 {
			return independentReviewResult{}, errors.New("review finding is invalid or oversized")
		}
	}
	return result, nil
}

func durableAttemptFailure(ctx context.Context, api internalgithub.API, issue internalgithub.RecoveryIssueFact, manifest agentruntime.Manifest, cause error) error {
	body, err := internalgithub.AttributedBody(issue.Issue, issue.Attempt, "Attempt failed closed: "+internalgithub.Redact(cause.Error()))
	if err != nil {
		return err
	}
	marker, err := internalgithub.TerminalFailureMarker(issue.Issue, issue.Attempt, manifest.UpdatedAt)
	if err != nil {
		return err
	}
	appID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
	if err != nil {
		return err
	}
	actorID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID")
	if err != nil {
		return err
	}
	present, err := internalgithub.HasAttemptComment(ctx, api, issue.Repository, issue.Issue, marker, appID, int(actorID))
	if err != nil {
		return err
	}
	if present {
		return nil
	}
	if err := api.CreateIssueComment(ctx, issue.Repository, issue.Issue, body+"\n\n"+marker, internalgithub.Mutation{Issue: issue.Issue, Attempt: issue.Attempt}); err != nil {
		return err
	}
	return nil
}

func monitorAttempts(ctx context.Context, runtime *agentruntime.Runtime, statuses []orchestrator.RecoveryStatus, manifests []agentruntime.Manifest, issues []internalgithub.RecoveryIssueFact) error {
	for _, status := range statuses {
		if status.Action != "resume monitoring the matching attempt" {
			continue
		}
		var base string
		for _, manifest := range manifests {
			if manifest.Repository == status.Repository && manifest.Issue == status.Issue && manifest.Attempt == status.Attempt {
				base = manifest.BaseSHA
				break
			}
		}
		authorized := slices.ContainsFunc(issues, func(issue internalgithub.RecoveryIssueFact) bool {
			return issue.Repository == status.Repository && issue.Issue == status.Issue && issue.DispatchAuthorized
		})
		_, err := runtime.Monitor(ctx, agentruntime.Attempt{Repository: status.Repository, Issue: status.Issue, Number: status.Attempt, BaseSHA: base, Eligible: func() bool { return authorized }})
		if err != nil {
			return fmt.Errorf("monitor %s#%d attempt %d: %w", status.Repository, status.Issue, status.Attempt, err)
		}
	}
	return nil
}

func resumeHandoffs(ctx context.Context, runtime *agentruntime.Runtime, statePath, stateRoot string, statuses []orchestrator.RecoveryStatus, manifests []agentruntime.Manifest) error {
	info, err := os.Lstat(statePath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("safe durable handoff state is unavailable")
	}
	live := map[string]agentruntime.Manifest{}
	for _, status := range statuses {
		if (status.State == "active" || status.State == "review-ready") && status.Action == "resume monitoring the matching attempt" {
			for _, manifest := range manifests {
				if manifest.Repository == status.Repository && manifest.Issue == status.Issue && manifest.Attempt == status.Attempt {
					live[fmt.Sprintf("%s#%d/%d", status.Repository, status.Issue, status.Attempt)] = manifest
				}
			}
		}
	}
	recovery := &internalgithub.FileRecovery{Path: statePath}
	outcomeRoot := filepath.Join(stateRoot, "handoff-outcomes")
	if err := os.MkdirAll(outcomeRoot, 0o700); err != nil {
		return err
	}
	if err := completeHandoffOutcomes(ctx, recovery, outcomeRoot); err != nil {
		return err
	}
	// Claim only after a safe owner is proven; otherwise work remains queued.
	if len(live) == 0 {
		return nil
	}
	owners := make(map[string]bool, len(live))
	for key := range live {
		owners[key] = true
	}
	handoffs, err := recovery.ClaimHandoffsFor(ctx, owners)
	if err != nil {
		return err
	}
	for _, handoff := range handoffs {
		manifest, ok := live[fmt.Sprintf("%s#%d/%d", handoff.Repository, handoff.Issue, handoff.Attempt)]
		if !ok {
			return errors.New("claimed handoff has no verified owning runtime; refusing execution")
		}
		payload, _ := json.Marshal(struct {
			Type, Key  string
			PR         int
			HeadSHA    string
			Validation bool
			Feedback   []internalgithub.Feedback
		}{"agent-symphony-handoff-v1", handoff.Key, handoff.PR, handoff.HeadSHA, handoff.Validation, handoff.Feedback})
		boundary, ok := runtime.Runner.(workerBoundaryRunner)
		if !ok {
			return errors.New("trusted boundary does not support durable handoff acceptance")
		}
		manifestBody, _ := json.Marshal(manifest)
		outcomePath := filepath.Join(outcomeRoot, handoff.Key+".json")
		outcomeToken := fmt.Sprintf("%x", sha256.Sum256([]byte("handoff-outcome\x00"+handoff.Key)))
		request, _ := json.Marshal(struct {
			Manifest     json.RawMessage `json:"manifest"`
			Handoff      json.RawMessage `json:"handoff"`
			OutcomePath  string          `json:"outcome_path"`
			OutcomeToken string          `json:"outcome_token"`
		}{manifestBody, payload, outcomePath, outcomeToken})
		accepted, err := boundary.call(ctx, "accept-handoff", agentruntime.Command{Stdin: bytes.NewReader(request)})
		var ack struct {
			Type         string `json:"type"`
			Key          string `json:"key"`
			OutcomePath  string `json:"outcome_path"`
			OutcomeToken string `json:"outcome_token"`
		}
		decoder := json.NewDecoder(strings.NewReader(accepted.Output))
		decoder.DisallowUnknownFields()
		if err != nil {
			return fmt.Errorf("worker-owned handoff acceptance: %w", err)
		}
		if decoder.Decode(&ack) != nil || decoder.Decode(&struct{}{}) != io.EOF || ack.Type != "agent-symphony-handoff-executed-v1" || ack.Key != handoff.Key || ack.OutcomePath != outcomePath || ack.OutcomeToken != outcomeToken {
			return errors.New("worker-owned handoff acceptance binding mismatch")
		}
		if err := recovery.ReceiptHandoff(ctx, handoff); err != nil {
			return err
		}
	}
	return nil
}

func writeImmutable(path string, body []byte) error {
	dir := filepath.Dir(path)
	f, err := immutableCreate(dir, ".agent-symphony-immutable-")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer os.Remove(tmp)
	if err = immutableWrite(f, body); err == nil {
		err = immutableFileSync(f)
	}
	if closeErr := f.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = immutableInstall(tmp, path); err != nil && !errors.Is(err, os.ErrExist) {
		return err
	}
	if err == nil {
		return immutableDirSync(dir)
	}
	existing, readErr := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if readErr != nil {
		return errors.New("immutable handoff inbox key collision")
	}
	old, readErr := io.ReadAll(io.LimitReader(existing, int64(len(body))+1))
	if closeErr := existing.Close(); readErr == nil {
		readErr = closeErr
	}
	if readErr != nil || !bytes.Equal(old, body) {
		return errors.New("immutable handoff inbox key collision")
	}
	return immutableDirSync(dir)
}

func completeHandoffOutcomes(ctx context.Context, recovery *internalgithub.FileRecovery, root string) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return err
	}
	if len(entries) > 10000 {
		return errors.New("handoff outcome count exceeded")
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return errors.New("unsafe handoff outcome file")
		}
		info, statErr := file.Stat()
		if statErr != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			file.Close()
			return errors.New("unsafe or oversized handoff outcome file")
		}
		b, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		var record struct {
			Handoff      internalgithub.RecoveryHandoff `json:"handoff"`
			Outcome      internalgithub.HandoffOutcome  `json:"outcome"`
			OutcomeToken string                         `json:"outcome_token"`
		}
		decoder := json.NewDecoder(bytes.NewReader(b))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil {
			return fmt.Errorf("decode handoff outcome: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); err != io.EOF {
			return errors.New("handoff outcome contains trailing data")
		}
		wantName := record.Handoff.Key + ".json"
		wantToken := fmt.Sprintf("%x", sha256.Sum256([]byte("handoff-outcome\x00"+record.Handoff.Key)))
		if entry.Name() != wantName || record.Outcome.Key != record.Handoff.Key || record.OutcomeToken != wantToken {
			return errors.New("handoff outcome destination or token mismatch")
		}
		if err := recovery.CompleteHandoffOutcome(ctx, record.Handoff, record.Outcome); err != nil {
			return err
		}
		if err := os.Remove(path); err != nil {
			return err
		}
	}
	return nil
}

func positiveEnvironmentInt64(name string) (int64, error) {
	value, err := strconv.ParseInt(os.Getenv(name), 10, 64)
	if err != nil || value <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return value, nil
}

func recoveryStatuses(attemptsPath, stateRoot string) ([]orchestrator.RecoveryStatus, error) {
	if attemptsPath == "" {
		return nil, errors.New("--attempts is required")
	}
	info, err := os.Lstat(attemptsPath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("--attempts must name a regular non-symlink file")
	}
	b, err := os.ReadFile(attemptsPath)
	if err != nil {
		return nil, err
	}
	var facts []orchestrator.AttemptFact
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&facts); err != nil {
		return nil, fmt.Errorf("read authoritative attempt facts: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); err != io.EOF {
		return nil, errors.New("authoritative attempt facts contain multiple JSON values")
	}
	sha := regexp.MustCompile(`^[0-9a-fA-F]{7,64}$`)
	for _, fact := range facts {
		if fact.Repository == "" || fact.Issue <= 0 || fact.Attempt <= 0 || !sha.MatchString(fact.BaseSHA) || !slices.Contains([]string{"queued", "active", "blocked", "review-ready", "completed", "failed", "cancelled"}, fact.State) {
			return nil, errors.New("authoritative attempt facts contain an invalid identity, base SHA, or state")
		}
	}
	var manifests []agentruntime.Manifest
	if stateRoot != "" {
		root, err := config.GitRoot()
		if err != nil {
			return nil, err
		}
		r := agentruntime.Runtime{Root: filepath.Join(root, ".worktrees"), StateRoot: stateRoot}
		manifests, err = r.Discover()
		if err != nil {
			return nil, fmt.Errorf("discover local attempts: %w", err)
		}
	}
	return orchestrator.Recover(facts, manifests), nil
}

// defaultStateRoot mirrors the state-root resolution documented in
// docs/architecture.md for callers, such as doctor, that treat --runtime-state
// as optional. serve/status/list/inspect/reconcile still require an explicit
// --runtime-state and never fall back here.
func defaultStateRoot() string {
	if runtime.GOOS == "darwin" {
		current, err := user.Current()
		if err != nil {
			return ""
		}
		return filepath.Join(current.HomeDir, "Library", "Application Support", "agent-symphony")
	}
	if value := os.Getenv("XDG_STATE_HOME"); value != "" {
		return filepath.Join(value, "agent-symphony")
	}
	current, err := user.Current()
	if err != nil {
		return ""
	}
	return filepath.Join(current.HomeDir, ".local", "state", "agent-symphony")
}

func doctor(c config.Config, offline bool, stateRoot string) []diagnostic {
	var result []diagnostic
	if runtime.GOOS == "darwin" || runtime.GOOS == "linux" {
		result = append(result, diagnostic{"platform", "pass", runtime.GOOS + "/" + runtime.GOARCH, ""})
	} else {
		result = append(result, diagnostic{"platform", "fail", runtime.GOOS + " is unsupported", "Use macOS, Linux, or WSL2."})
	}
	if stateRoot == "" {
		stateRoot = defaultStateRoot()
	}
	if runningOnWSL() {
		root, rootErr := config.GitRoot()
		filesystem, _ := mountedFilesystem(root, "/proc/mounts")
		validationErr := validateWSLFilesystem(root, filepath.Join(root, c.WorktreeRoot), stateRoot, "/proc/mounts")
		if rootErr != nil || validationErr != nil {
			message := firstNonempty(errorText(rootErr), errorText(validationErr))
			result = append(result, diagnostic{"wsl-filesystem", "fail", message, "Run inside a Git repository on the WSL Linux filesystem."})
		} else {
			result = append(result, diagnostic{"wsl-filesystem", "pass", "Git root is on " + filesystem, ""})
		}
	}
	result = append(result, executableDiagnostic("git", []string{"--version"}))
	result = append(result, tmuxDiagnostic())
	for _, configured := range []struct {
		name    string
		command []string
	}{{"implementation", c.Commands.Implementation}, {"reviewer", c.Commands.Reviewer}} {
		name, command := configured.name, configured.command
		if path, err := exec.LookPath(command[0]); err != nil {
			result = append(result, diagnostic{name + " command", "fail", command[0] + " was not found", "Install it or update commands." + name + "."})
		} else {
			result = append(result, diagnostic{name + " command", "pass", path, ""})
		}
	}
	if out, err := exec.Command("git", "rev-parse", "--show-toplevel").CombinedOutput(); err != nil {
		result = append(result, diagnostic{"repository", "fail", strings.TrimSpace(string(out)), "Run doctor inside the configured Git repository."})
	} else {
		result = append(result, diagnostic{"repository", "pass", strings.TrimSpace(string(out)), ""})
	}
	if repository, err := repositoryFromGit(); err != nil {
		result = append(result, diagnostic{"repository identity", "fail", err.Error(), "Configure origin as the GitHub repository named in configuration."})
	} else if repository != c.Repository {
		result = append(result, diagnostic{"repository identity", "fail", "origin is " + repository + ", configuration names " + c.Repository, "Run init in the intended repository or correct repository."})
	} else {
		result = append(result, diagnostic{"repository identity", "pass", repository, ""})
	}
	if offline {
		result = append(result, diagnostic{"GitHub permissions", "warn", "offline: connectivity and effective access were not probed", "Run doctor without --offline before serving work."})
	} else {
		result = append(result, githubDiagnostics(c.Repository)...)
	}
	result = append(result, hostDiagnostic(stateRoot))
	result = append(result, diagnostic{"GitHub policy", "warn", "the required agent-symphony/policy check and merge-permission gating are not yet implemented", "Configure branch protection manually until the policy check exists; an optional webhook listener for early wake-up can be configured with AGENT_SYMPHONY_WEBHOOK_ADDR/AGENT_SYMPHONY_WEBHOOK_SECRET, but periodic reconciliation remains the authoritative recovery path either way."})
	return result
}

func executableDiagnostic(name string, args []string) diagnostic {
	path, err := exec.LookPath(name)
	if err != nil {
		return diagnostic{name, "fail", name + " was not found", "Install " + name + " and ensure it is on PATH."}
	}
	out, err := exec.Command(path, args...).CombinedOutput()
	if err != nil {
		return diagnostic{name, "fail", strings.TrimSpace(string(out)), "Repair the " + name + " installation."}
	}
	return diagnostic{name, "pass", strings.TrimSpace(string(out)), ""}
}

func tmuxDiagnostic() diagnostic {
	path, err := exec.LookPath("tmux")
	if err != nil {
		return diagnostic{"tmux", "fail", "tmux was not found", "Install tmux and ensure it is on PATH."}
	}
	version, err := exec.Command(path, "-V").CombinedOutput()
	if err != nil {
		return diagnostic{"tmux", "fail", strings.TrimSpace(string(version)), "Repair the tmux installation."}
	}
	socket := fmt.Sprintf("agent-symphony-doctor-%d-%d", os.Getpid(), time.Now().UnixNano())
	if out, err := exec.Command(path, "-L", socket, "new-session", "-d", "-s", "doctor").CombinedOutput(); err != nil {
		return diagnostic{"tmux", "fail", strings.TrimSpace(string(out)), "Ensure tmux can create detached sessions for this user."}
	}
	defer exec.Command(path, "-L", socket, "kill-server").Run()
	return diagnostic{"tmux", "pass", strings.TrimSpace(string(version)) + "; detached session lifecycle works", ""}
}

func githubDiagnostics(repository string) []diagnostic {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if firstNonempty(os.Getenv("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH"), os.Getenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID")) != "" {
		appID, err := positiveEnvironmentInt64("AGENT_SYMPHONY_GITHUB_APP_ID")
		if err != nil {
			return []diagnostic{{"GitHub connectivity", "fail", err.Error(), "Set matching AGENT_SYMPHONY_GITHUB_APP_ID, AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH, and AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID values."}}
		}
		tokens, err := githubTokenSource(appID)
		if err != nil {
			return []diagnostic{{"GitHub connectivity", "fail", err.Error(), "Set matching AGENT_SYMPHONY_GITHUB_APP_ID, AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH, and AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID values."}}
		}
		api := internalgithub.API{BaseURL: githubAPI, Tokens: tokens, HTTP: githubClient}
		if err := api.VerifyInstallation(ctx, appID, repository); err != nil {
			return []diagnostic{{"GitHub connectivity", "fail", err.Error(), "Verify the configured GitHub App installation includes this repository."}}
		}
		return []diagnostic{
			{"GitHub connectivity", "pass", "connected to " + repository, ""},
			{"GitHub permissions", "warn", "configured GitHub App installation can access the repository", "Verify issue, pull-request, checks, webhook, rules, and installation permissions in GitHub."},
		}
	}
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, githubAPI+"/repos/"+repository, nil)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2022-11-28")
	if token := firstNonempty(os.Getenv("GITHUB_TOKEN"), os.Getenv("GH_TOKEN")); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := githubClient.Do(req)
	if err != nil {
		return []diagnostic{{"GitHub connectivity", "fail", err.Error(), "Check DNS, proxy, and access to api.github.com."}}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return []diagnostic{{"GitHub connectivity", "fail", resp.Status, "Check repository name and provide GITHUB_TOKEN or GH_TOKEN for private repositories."}}
	}
	var body struct {
		Permissions map[string]bool `json:"permissions"`
	}
	_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&body)
	if len(body.Permissions) == 0 {
		return []diagnostic{
			{"GitHub connectivity", "pass", "connected to " + repository, ""},
			{"GitHub permissions", "warn", "effective repository permissions are unavailable without authenticated metadata", "Set GITHUB_TOKEN or GH_TOKEN for this read-only probe; downstream App setup must still verify feature-specific permissions."},
		}
	}
	var granted []string
	for _, name := range []string{"admin", "maintain", "push", "triage", "pull"} {
		if body.Permissions[name] {
			granted = append(granted, name)
		}
	}
	return []diagnostic{
		{"GitHub connectivity", "pass", "connected to " + repository, ""},
		{"GitHub permissions", "warn", "effective repository access: " + strings.Join(granted, ", "), "Downstream App setup must verify issue, pull-request, checks, webhook, rules, and installation permissions."},
	}
}

func repositoryFromGit() (string, error) {
	out, err := exec.Command("git", "config", "--get", "remote.origin.url").Output()
	if err != nil {
		return "", err
	}
	return parseGitHubRemote(strings.TrimSpace(string(out)))
}

func parseGitHubRemote(remote string) (string, error) {
	if strings.HasPrefix(remote, "git@github.com:") {
		remote = strings.TrimPrefix(remote, "git@github.com:")
	} else {
		u, err := url.Parse(remote)
		if err != nil || u.Hostname() != "github.com" {
			return "", fmt.Errorf("origin is not a GitHub URL")
		}
		remote = strings.TrimPrefix(u.Path, "/")
	}
	remote = strings.TrimSuffix(remote, ".git")
	if len(strings.Split(remote, "/")) != 2 {
		return "", fmt.Errorf("origin must identify one owner/repository")
	}
	if err := config.Default(remote).Validate(); err != nil {
		return "", fmt.Errorf("invalid GitHub origin: %w", err)
	}
	return remote, nil
}

func isWSL() bool {
	b, _ := os.ReadFile("/proc/sys/kernel/osrelease")
	return strings.Contains(strings.ToLower(string(b)), "microsoft")
}

func validateWSLFilesystem(root, worktree, stateRoot, mountsPath string) error {
	for _, path := range []string{root, worktree, stateRoot} {
		clean := filepath.Clean(path)
		if path == "" || clean == "/mnt" || strings.HasPrefix(clean, "/mnt/") {
			return errors.New("repository, worktree, or state root is on a Windows-mounted filesystem; move all three paths into the WSL Linux filesystem")
		}
	}
	filesystem, err := mountedFilesystem(root, mountsPath)
	if err != nil {
		return err
	}
	if filesystem == "drvfs" || filesystem == "9p" {
		return errors.New("repository, worktree, or state root is on a Windows-mounted filesystem; move all three paths into the WSL Linux filesystem")
	}
	return nil
}

func mountedFilesystem(path, mountsPath string) (string, error) {
	b, err := os.ReadFile(mountsPath)
	if err != nil {
		return "", err
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", err
	}
	bestMount, bestType := "", ""
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		mount := decodeMountField(fields[1])
		if resolved, err := filepath.EvalSymlinks(mount); err == nil {
			mount = resolved
		}
		if (path == mount || strings.HasPrefix(path, strings.TrimSuffix(mount, "/")+"/")) && len(mount) > len(bestMount) {
			bestMount, bestType = mount, fields[2]
		}
	}
	if bestMount == "" {
		return "", fmt.Errorf("no /proc/mounts entry contains Git root %q", path)
	}
	return bestType, nil
}

func decodeMountField(value string) string {
	return strings.NewReplacer(`\040`, " ", `\011`, "\t", `\012`, "\n", `\134`, `\`).Replace(value)
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

func success(w io.Writer, jsonOutput bool, command string, data any, human string) int {
	if jsonOutput {
		return writeJSON(w, envelope{Version: outputVersion, Command: command, OK: true, Data: data})
	}
	fmt.Fprintln(w, human)
	return 0
}

func fail(w io.Writer, jsonOutput bool, command, message string) int {
	if jsonOutput {
		writeJSON(w, envelope{Version: outputVersion, Command: command, OK: false, Error: message})
	} else {
		fmt.Fprintln(w, "error: "+message)
	}
	return 1
}

func misuse(w io.Writer, jsonOutput bool, command, message string) int {
	if jsonOutput {
		writeJSON(w, envelope{Version: outputVersion, Command: command, OK: false, Error: message})
	} else {
		fmt.Fprintln(w, "error: "+message)
	}
	return 2
}

func hasJSONFlag(args []string) bool {
	for _, arg := range args {
		if arg == "--json" {
			return true
		}
		if strings.HasPrefix(arg, "--json=") {
			value, err := strconv.ParseBool(strings.TrimPrefix(arg, "--json="))
			if err == nil && value {
				return true
			}
		}
	}
	return false
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func writeJSON(w io.Writer, value envelope) int {
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(value); err != nil {
		return 1
	}
	return 0
}

func usage(w io.Writer) {
	fmt.Fprintln(w, `usage: agent-symphony <command> [options]

commands:
	install-host  provision the native worker/reviewer boundary (run as root)
	agent-host    execute the installed implementation or review boundary
	init          create .agent-symphony.yaml with safe defaults
  validate      validate configuration
  config view   print validated configuration
  serve         reconcile at startup and at most every 60 seconds
  status        show recovered work
  list          alias for status
  inspect       show one issue (--issue number)
  reconcile     fetch GitHub facts and reconcile now
  doctor        diagnose local prerequisites and GitHub access
  diagnostics   alias for doctor
  pr-governance reconcile pull-request governance once

options:
  --config path use another configuration file
  --state path  durable PR-governance/handoff state
  --runtime-state path  bounded runtime manifest root
  --json        emit a versioned JSON envelope`)
}
