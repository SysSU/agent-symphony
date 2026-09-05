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
	"sync"
	"syscall"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const (
	outputVersion              = 1
	humanInstructionPrecedence = "Confirmed human instructions amend the issue contract. Apply them in listed chronological order; a later human instruction supersedes conflicting earlier issue text or human instructions. Automated review feedback never supersedes a conflicting human instruction."
)

var releaseMetadata = "agent-symphony-release-version:devel"

func releaseVersion() string {
	return strings.TrimPrefix(releaseMetadata, "agent-symphony-release-version:")
}

var (
	githubAPI          = "https://api.github.com"
	githubClient       = &http.Client{Transport: internalgithub.CLITransport{}}
	reconcileGitHubRun = reconcileGitHub
	reconcileRetryRun  = reconcileGitHubWith
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

type workerBoundaryRunner struct {
	Command string
	Args    []string
	Env     []string
}

type boundaryCaller interface {
	call(context.Context, string, agentruntime.Command) (agentruntime.Result, error)
}

type operationCancellation struct {
	mu     sync.Mutex
	cancel context.CancelFunc
}

func (o *operationCancellation) begin(ctx context.Context) (context.Context, func()) {
	operationCtx, cancel := context.WithCancel(ctx)
	o.mu.Lock()
	o.cancel = cancel
	o.mu.Unlock()
	return operationCtx, func() {
		cancel()
		o.mu.Lock()
		o.cancel = nil
		o.mu.Unlock()
	}
}

func (o *operationCancellation) interrupt() {
	o.mu.Lock()
	if o.cancel != nil {
		o.cancel()
	}
	o.mu.Unlock()
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

type handoffReceipt struct {
	Type         string `json:"type"`
	Key          string `json:"key"`
	OutcomePath  string `json:"outcome_path"`
	OutcomeToken string `json:"outcome_token"`
}

type handoffRequest struct {
	Manifest     agentruntime.Manifest `json:"manifest"`
	Handoff      json.RawMessage       `json:"handoff"`
	OutcomePath  string                `json:"outcome_path"`
	OutcomeToken string                `json:"outcome_token"`
	Command      []string              `json:"command"`
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
	for _, name := range []string{"PATH", "TMPDIR", "SYSTEMROOT", "CODEX_HOME"} {
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
		return workerBoundaryRunner{Command: binary, Args: []string{"agent-host", "implementation"}, Env: []string{"AGENT_SYMPHONY_LOCAL_ROOT=" + localAttemptRoot(stateRoot), "TMUX_TMPDIR=" + projectTmuxRoot(stateRoot)}}
	}
	return workerBoundaryRunner{Command: "sudo", Args: []string{"-n", "-u", workerUser, "-g", attemptGroup, binary, "agent-host", "implementation"}}
}

func reviewBoundary(stateRoot string) workerBoundaryRunner {
	if command := os.Getenv("AGENT_SYMPHONY_REVIEW_BOUNDARY"); command != "" {
		return workerBoundaryRunner{Command: command}
	}
	binary, _ := os.Executable()
	if !hostIsolationInstalled() {
		return workerBoundaryRunner{Command: binary, Args: []string{"agent-host", "review"}, Env: []string{"AGENT_SYMPHONY_LOCAL_ROOT=" + localSnapshotRoot(stateRoot), "TMUX_TMPDIR=" + projectTmuxRoot(stateRoot)}}
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

func projectTmuxRoot(stateRoot string) string { return filepath.Join(stateRoot, "tmux") }

var agentCodexAssets = []string{"auth.json", "config.toml", "AGENTS.md", "rules", "skills", "plugins", "cache", "installation_id"}

func configureAgentCodexHome(stateRoot string) error {
	source := strings.TrimSpace(os.Getenv("CODEX_HOME"))
	if source == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return fmt.Errorf("resolve user home for Codex: %w", err)
		}
		source = filepath.Join(home, ".codex")
	}
	source, err := filepath.Abs(source)
	if err != nil {
		return fmt.Errorf("resolve Codex home: %w", err)
	}
	target, err := filepath.Abs(filepath.Join(stateRoot, "codex-home"))
	if err != nil {
		return fmt.Errorf("resolve agent Codex home: %w", err)
	}
	if filepath.Clean(source) == filepath.Clean(target) {
		return os.Setenv("CODEX_HOME", target)
	}
	if err := ensureLocalRoot(target); err != nil {
		return fmt.Errorf("prepare agent Codex home: %w", err)
	}
	for _, name := range agentCodexAssets {
		from, to := filepath.Join(source, name), filepath.Join(target, name)
		if _, err := os.Lstat(from); errors.Is(err, os.ErrNotExist) {
			continue
		} else if err != nil {
			return fmt.Errorf("inspect Codex capability %s: %w", name, err)
		}
		if destination, err := os.Readlink(to); err == nil && destination == from {
			continue
		} else if err == nil || !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("agent Codex capability %s conflicts with managed link", name)
		}
		if err := os.Symlink(from, to); err != nil {
			return fmt.Errorf("link Codex capability %s: %w", name, err)
		}
	}
	if err := os.Setenv("CODEX_HOME", target); err != nil {
		return fmt.Errorf("configure agent Codex home: %w", err)
	}
	return nil
}

func configureProjectTmux(stateRoot string) error {
	root, err := filepath.Abs(projectTmuxRoot(stateRoot))
	if err != nil {
		return err
	}
	if err := ensureLocalRoot(root); err != nil {
		return fmt.Errorf("prepare project tmux root: %w", err)
	}
	return os.Setenv("TMUX_TMPDIR", root)
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func readDashboardPassword(path string) (string, error) {
	info, err := os.Lstat(path)
	if err != nil || !privateDashboardPasswordFile(info) {
		return "", errors.New("dashboard password file must be a private regular file")
	}
	file, err := os.Open(path)
	if err != nil {
		return "", errors.New("dashboard password file is unavailable")
	}
	opened, statErr := file.Stat()
	body, readErr := io.ReadAll(io.LimitReader(file, 4097))
	final, finalStatErr := file.Stat()
	closeErr := file.Close()
	if statErr != nil || finalStatErr != nil || !privateDashboardPasswordFile(opened) || !privateDashboardPasswordFile(final) || !os.SameFile(info, opened) || !os.SameFile(opened, final) || opened.Size() != final.Size() || !opened.ModTime().Equal(final.ModTime()) || readErr != nil || closeErr != nil || len(body) > 4096 {
		return "", errors.New("dashboard password file changed while reading")
	}
	password := strings.TrimSuffix(strings.TrimSuffix(string(body), "\n"), "\r")
	if password == "" || strings.ContainsAny(password, "\r\n") {
		return "", errors.New("dashboard password file must contain one nonempty line")
	}
	return password, nil
}

func privateDashboardPasswordFile(info os.FileInfo) bool {
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return false
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	return !ok || int(stat.Uid) == os.Geteuid()
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 1 && args[0] == "--version" {
		fmt.Fprintln(stdout, releaseVersion())
		return 0
	}
	if len(args) == 0 {
		usage(stdout)
		return 0
	}
	command := args[0]
	if command == "worker-capture" || command == "worker-capture-replace" || command == "worker-capture-handoff" || command == "worker-capture-handoff-ready" {
		if command == "worker-capture-handoff" || command == "worker-capture-handoff-ready" {
			if len(args) < 9 || args[7] != "--" {
				return misuse(stderr, false, command, "invalid internal worker capture invocation")
			}
			ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
			defer stop()
			signalLaunch := func() error {
				command := exec.CommandContext(context.Background(), args[1], "wait-for", "-S", args[6])
				command.Dir = "/tmp"
				return command.Run()
			}
			code, err := agentruntime.CaptureWorkerReplacingResultAfterStart(ctx, args[1], args[2], args[3], args[8:], stdout, stderr, func() error {
				if err := writeImmutable(args[4], []byte(args[5])); err != nil {
					return err
				}
				if err := signalLaunch(); err != nil {
					signalErr := fmt.Errorf("launch signal: %w", err)
					if removeErr := os.Remove(args[4]); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
						return errors.Join(signalErr, fmt.Errorf("roll back launch marker: %w", removeErr))
					}
					if syncErr := immutableDirSync(filepath.Dir(args[4])); syncErr != nil {
						return errors.Join(signalErr, fmt.Errorf("sync launch marker rollback: %w", syncErr))
					}
					return signalErr
				}
				return nil
			})
			if err != nil {
				if command == "worker-capture-handoff-ready" {
					_ = signalLaunch()
				}
				fmt.Fprintln(stderr, "error: "+err.Error())
			}
			return code
		}
		if len(args) < 6 || args[4] != "--" {
			return misuse(stderr, false, command, "invalid internal worker capture invocation")
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		capture := agentruntime.CaptureWorker
		if command == "worker-capture-replace" {
			capture = agentruntime.CaptureWorkerReplacingResult
		}
		code, err := capture(ctx, args[1], args[2], args[3], args[5:], stdout, stderr)
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
	interval := fs.Duration("interval", orchestrator.MaxReconcileInterval, "override configured serve reconciliation interval (maximum 60s)")
	dashboardAddress := fs.String("dashboard-address", "127.0.0.1:8080", "dashboard loopback listen address")
	allowUnsafeDashboardNetwork := fs.Bool("allow-unsafe-dashboard-network", false, "allow password-protected dashboard access outside loopback")
	dashboardPasswordFile := fs.String("dashboard-password-file", "", "coordinator-only file containing the dashboard HTTP Basic authentication password")
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
			return misuse(stderr, wantsJSON, command, "usage: agent-symphony agent-host implementation|review|orchestrator|orchestrator-proposal|orchestrator-proposal-status")
		}
		if err := agentHost(context.Background(), fs.Arg(0), os.Stdin, stdout); err != nil {
			return fail(stderr, false, command, internalgithub.RedactEnvironment(err.Error(), os.Environ()))
		}
		return 0
	case "serve":
		if fs.NArg() != 0 || *statePath == "" || *runtimeState == "" {
			return misuse(stderr, wantsJSON, command, "serve requires --state and --runtime-state")
		}
		if *interval <= 0 || *interval > orchestrator.MaxReconcileInterval {
			return misuse(stderr, wantsJSON, command, "--interval must be greater than zero and no more than 60s")
		}
		if *allowUnsafeDashboardNetwork && *dashboardPasswordFile == "" {
			return misuse(stderr, wantsJSON, command, "--dashboard-password-file is required with --allow-unsafe-dashboard-network")
		}
		dashboardPassword := ""
		if *dashboardPasswordFile != "" {
			var passwordErr error
			dashboardPassword, passwordErr = readDashboardPassword(*dashboardPasswordFile)
			if passwordErr != nil {
				return fail(stderr, *jsonOutput, command, passwordErr.Error())
			}
		}
		c, err := config.Load(*path)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		*interval = effectiveServeInterval(fs, c.ReconciliationIntervalSeconds, *interval)
		if runningOnWSL() {
			root, rootErr := config.GitRoot()
			if rootErr == nil {
				rootErr = validateWSLFilesystem(root, filepath.Join(root, c.WorktreeRoot), *runtimeState, "/proc/mounts")
			}
			if rootErr != nil {
				return fail(stderr, *jsonOutput, command, rootErr.Error())
			}
		}
		if err := configureProjectTmux(*runtimeState); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if !hostIsolationInstalled() {
			if err := configureAgentCodexHome(*runtimeState); err != nil {
				return fail(stderr, *jsonOutput, command, err.Error())
			}
		}
		api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}
		if _, err := api.AuthenticatedUser(context.Background()); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if err := api.VerifyRepository(context.Background(), c.Repository); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		lock, err := acquireDaemonLock(filepath.Join(*runtimeState, "daemon.lock"))
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		defer releaseDaemonLock(lock)
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		agent, err := newOrchestratorAgent(c, *runtimeState)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		operationMu := &sync.Mutex{}
		deliveryMu := &sync.Mutex{}
		recoverAttempt := func(ctx context.Context, issue, attempt int) error {
			return recoverDashboardAttempt(ctx, *path, *statePath, *runtimeState, issue, attempt)
		}
		operations := &operationCancellation{}
		reconcile := func(ctx context.Context) error {
			cycleCtx, finish := operations.begin(ctx)
			defer finish()
			statuses, err := reconcileGitHub(cycleCtx, *path, *statePath, *runtimeState, true)
			if err != nil && !errors.Is(err, context.Canceled) {
				fmt.Fprintln(stderr, "reconcile: "+internalgithub.Redact(err.Error()))
			}
			if errors.Is(err, context.Canceled) && ctx.Err() == nil {
				return err
			}
			_, agentErr := agent.ObserveCycle(ctx, statuses, err)
			if agentErr != nil {
				fmt.Fprintln(stderr, "orchestrator agent: "+internalgithub.Redact(agentErr.Error()))
			}
			return err
		}
		dashboardAgent := dashboardOrchestratorService{Service: agent, supervisor: agent, confirm: func(ctx context.Context, proposal orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error) {
			return confirmOperatorMessage(ctx, *path, *runtimeState, proposal)
		}, accepted: func(message internalgithub.OperatorMessage) {
			operations.interrupt()
			go func() {
				deliveryMu.Lock()
				defer deliveryMu.Unlock()
				operationMu.Lock()
				defer operationMu.Unlock()
				if ctx.Err() != nil {
					return
				}
				if err := writeOperatorMessageStatus(*runtimeState, message); err != nil {
					fmt.Fprintln(stderr, "operator message projection: "+internalgithub.Redact(err.Error()))
				}
				deliveryCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
				defer cancel()
				delivered, err := deliverOperatorMessageWithRetry(deliveryCtx, *path, *runtimeState, message)
				if err != nil {
					fmt.Fprintln(stderr, "operator message delivery: "+internalgithub.Redact(err.Error()))
				}
				if projectionErr := writeOperatorMessageStatus(*runtimeState, delivered); projectionErr != nil {
					fmt.Fprintln(stderr, "operator message projection: "+internalgithub.Redact(projectionErr.Error()))
				}
			}()
		}}
		dashboardURL, err := startDashboard(ctx, *dashboardAddress, *runtimeState, operationMu, recoverAttempt, reconcile, dashboardAgent, *allowUnsafeDashboardNetwork, dashboardPassword, stderr)
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		fmt.Fprintln(stderr, "dashboard: "+dashboardURL)
		deliveryMu.Lock()
		retryCtx, retryCancel := context.WithTimeout(ctx, 90*time.Second)
		if err := retryProjectedOperatorMessages(retryCtx, *path, *runtimeState); err != nil && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(stderr, "operator message startup recovery: "+internalgithub.Redact(err.Error()))
		}
		retryCancel()
		deliveryMu.Unlock()
		lockedReconcile := func(ctx context.Context) error {
			operationMu.Lock()
			defer operationMu.Unlock()
			return reconcile(ctx)
		}
		go watchOrchestratorProposals(ctx, agent, operationMu, operations, *path, *statePath, *runtimeState, stderr)
		if err := orchestrator.ReconcileLoop(ctx, *interval, lockedReconcile); err != nil && !errors.Is(err, context.Canceled) {
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
			if err := configureProjectTmux(*runtimeState); err != nil {
				return fail(stderr, *jsonOutput, command, err.Error())
			}
			if !hostIsolationInstalled() {
				if err := configureAgentCodexHome(*runtimeState); err != nil {
					return fail(stderr, *jsonOutput, command, err.Error())
				}
			}
			if command == "reconcile" {
				lock, lockErr := acquireDaemonLock(filepath.Join(*runtimeState, "daemon.lock"))
				if lockErr != nil {
					return fail(stderr, *jsonOutput, command, lockErr.Error())
				}
				defer releaseDaemonLock(lock)
			}
			statuses, err = reconcileGitHubRun(context.Background(), *path, *statePath, *runtimeState, command == "reconcile")
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
			fmt.Fprintf(stdout, "  phase: %s  worktree: %s\n", firstNonempty(status.CurrentPhase, "-"), firstNonempty(status.Worktree, "-"))
			if len(status.Sessions) == 0 {
				fmt.Fprintln(stdout, "  sessions: -")
			}
			for _, session := range status.Sessions {
				current := ""
				if session.Current {
					current = " current"
				}
				fmt.Fprintf(stdout, "  session: %s%s %s %s\n", session.Role, current, session.State, session.Name)
			}
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
		api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}
		user, err := api.AuthenticatedUser(context.Background())
		if err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		if err := api.VerifyRepository(context.Background(), c.Repository); err != nil {
			return fail(stderr, *jsonOutput, command, err.Error())
		}
		prConfig := githubPRConfig(c, user.ID)
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

func watchOrchestratorProposals(ctx context.Context, agent *orchestratoragent.Supervisor, operationMu *sync.Mutex, operations *operationCancellation, configPath, statePath, stateRoot string, log io.Writer) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		processOrchestratorProposal(ctx, agent, operationMu, operations, configPath, statePath, stateRoot, log)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func processOrchestratorProposal(ctx context.Context, agent *orchestratoragent.Supervisor, operationMu *sync.Mutex, operations *operationCancellation, configPath, statePath, stateRoot string, log io.Writer) {
	proposal, err := agent.MessageProposal(ctx)
	if errors.Is(err, orchestratoragent.ErrNoMessageProposal) {
		return
	}
	if err != nil {
		fmt.Fprintln(log, "orchestrator proposal: "+internalgithub.Redact(err.Error()))
		return
	}
	if !slices.Contains([]string{orchestratoragent.ProposalActionRetry, orchestratoragent.ProposalActionRecover, orchestratoragent.ProposalActionAttention}, proposal.Action) || !operationMu.TryLock() {
		return
	}
	defer operationMu.Unlock()
	operationCtx, finish := operations.begin(ctx)
	defer finish()
	controlCtx, cancel := context.WithTimeout(operationCtx, 10*time.Minute)
	defer cancel()
	running := false
	switch proposal.Action {
	case orchestratoragent.ProposalActionRetry:
		_, err = reconcileRetryRun(controlCtx, configPath, statePath, stateRoot, reconcileOptions{
			transition: true,
			timeout:    10 * time.Minute,
			authorize: func(statuses []orchestrator.RecoveryStatus) error {
				if validateErr := validateTransitionRetry(proposal, statuses); validateErr != nil {
					return transitionRetryRefusal{validateErr}
				}
				if validateErr := agent.ValidateAttentionProposal(proposal, statuses); validateErr != nil {
					return transitionRetryRefusal{validateErr}
				}
				if resolveErr := agent.ResolveMessageProposal(controlCtx, proposal.Binding, "running", "the coordinator validated the exact completed attempt and is running bounded reconciliation"); resolveErr != nil {
					return fmt.Errorf("record running retry: %w", resolveErr)
				}
				running = true
				return nil
			},
		})
	case orchestratoragent.ProposalActionRecover, orchestratoragent.ProposalActionAttention:
		var statuses []orchestrator.RecoveryStatus
		statuses, err = reconcileGitHubRun(controlCtx, configPath, statePath, stateRoot, false)
		if err == nil {
			if validateErr := agent.ValidateAttentionProposal(proposal, statuses); validateErr != nil {
				err = transitionRetryRefusal{validateErr}
			}
		}
		if err == nil {
			detail := "the coordinator re-verified the exact attention target"
			if proposal.Action == orchestratoragent.ProposalActionRecover {
				detail += " and is running guarded attempt recovery"
			}
			err = agent.ResolveMessageProposal(controlCtx, proposal.Binding, "running", detail)
			running = err == nil
		}
		if err == nil && proposal.Action == orchestratoragent.ProposalActionRecover {
			err = recoverDashboardAttempt(controlCtx, configPath, statePath, stateRoot, proposal.Issue, proposal.Attempt)
		}
	}
	if err != nil {
		resolution := "failed"
		var refusal transitionRetryRefusal
		if errors.As(err, &refusal) {
			resolution = "refused"
		}
		resolveCtx, resolveCancel := context.WithTimeout(context.Background(), 5*time.Second)
		resolveErr := agent.ResolveMessageProposal(resolveCtx, proposal.Binding, resolution, err.Error())
		resolveCancel()
		if resolveErr != nil {
			fmt.Fprintln(log, "orchestrator proposal outcome: "+internalgithub.Redact(resolveErr.Error()))
		}
		fmt.Fprintln(log, "orchestrator proposal "+resolution+": "+internalgithub.Redact(err.Error()))
		return
	}
	if !running {
		fmt.Fprintln(log, "orchestrator proposal failed: coordinator processing skipped authorization")
		return
	}
	detail := "the validated bounded coordinator action completed; waiting for a fresh projection"
	if proposal.Action == orchestratoragent.ProposalActionAttention {
		detail = proposal.Detail
	}
	if err := agent.ResolveMessageProposal(controlCtx, proposal.Binding, "succeeded", detail); err != nil {
		fmt.Fprintln(log, "orchestrator proposal outcome: "+internalgithub.Redact(err.Error()))
	}
}

type transitionRetryRefusal struct{ error }

func validateTransitionRetry(proposal orchestratoragent.MessageProposal, statuses []orchestrator.RecoveryStatus) error {
	matches := make([]orchestrator.RecoveryStatus, 0, 1)
	for _, status := range statuses {
		if status.Repository == proposal.Repository && status.Issue == proposal.Issue && status.Attempt == proposal.Attempt {
			matches = append(matches, status)
		}
	}
	if len(matches) != 1 {
		return errors.New("retry target is not the exact current issue attempt")
	}
	target := matches[0]
	completedImplementation := slices.ContainsFunc(target.Sessions, func(session orchestrator.AttemptSession) bool {
		return session.Role == agentruntime.SessionRoleImplementation && session.State == "completed"
	})
	if !target.DispatchAuthorized || !slices.Contains([]string{"active", "review-ready"}, target.State) || !slices.Contains([]string{"validation", "publication"}, target.CurrentPhase) || len(target.Blockers) != 0 || !completedImplementation {
		return errors.New("retry target is not an authorized completed result awaiting validation or publication")
	}
	return nil
}

func confirmOperatorMessage(ctx context.Context, configPath, stateRoot string, proposal orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return internalgithub.OperatorMessage{}, err
	}
	message, err := internalgithub.PrepareOperatorMessage(proposal.Repository, proposal.Issue, proposal.Attempt, proposal.Message)
	if err != nil || proposal.Version != 1 || proposal.Repository != cfg.Repository || proposal.Action != "" && proposal.Action != orchestratoragent.ProposalActionMessage {
		return internalgithub.OperatorMessage{}, errors.New("operator message target or body is invalid")
	}
	api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return internalgithub.OperatorMessage{}, err
	}
	if err := api.VerifyRepository(ctx, cfg.Repository); err != nil {
		return internalgithub.OperatorMessage{}, err
	}
	runtimeState := newOperatorMessageRuntime(stateRoot)
	manifests, err := runtimeState.Discover()
	if err != nil {
		return internalgithub.OperatorMessage{}, err
	}
	if err := validateOperatorMessageAdmission(ctx, proposal, &runtimeState, manifests); err != nil {
		return internalgithub.OperatorMessage{}, err
	}
	return internalgithub.RecordOperatorMessage(ctx, api, githubPRConfig(cfg, user.ID), message)
}

func validateOperatorMessageAdmission(ctx context.Context, proposal orchestratoragent.MessageProposal, runtimeState *agentruntime.Runtime, manifests []agentruntime.Manifest) error {
	matching := make([]agentruntime.Manifest, 0, 1)
	for _, manifest := range manifests {
		if manifest.Repository == proposal.Repository && manifest.Issue == proposal.Issue && manifest.Attempt == proposal.Attempt {
			matching = append(matching, manifest)
		}
	}
	if len(matching) != 1 {
		return errors.New("operator message target does not have a verified exact runtime owner")
	}
	manifest := matching[0]
	var err error
	if manifest.State == "completed" {
		err = runtimeState.VerifyRetained(ctx, manifest, "")
	} else if manifest.State == "running" {
		err = runtimeState.VerifyOwned(ctx, manifest)
		if err != nil {
			err = runtimeState.VerifyRetained(ctx, manifest, "")
		}
	} else {
		err = errors.New("runtime is not eligible")
	}
	if err != nil {
		return errors.New("operator message target does not have a verified exact runtime owner")
	}
	return nil
}

func fetchOperatorMessageTarget(ctx context.Context, api internalgithub.API, cfg internalgithub.PRAdapterConfig, proposal orchestratoragent.MessageProposal) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, error) {
	api = api.WithReadSnapshot()
	remote, err := internalgithub.FetchOperatorAttemptFacts(ctx, api, cfg.Repository, cfg.ActorID, proposal.Issue, proposal.Attempt)
	if err != nil {
		return nil, nil, err
	}
	issues, err := internalgithub.FetchOperatorIssueFacts(ctx, api, cfg, remote, proposal.Issue)
	return issues, remote, err
}

func deliverOperatorMessage(ctx context.Context, configPath, stateRoot string, accepted internalgithub.OperatorMessage) (internalgithub.OperatorMessage, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return accepted, err
	}
	api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return accepted, err
	}
	if err := api.VerifyRepository(ctx, cfg.Repository); err != nil {
		return accepted, err
	}
	prConfig := githubPRConfig(cfg, user.ID)
	messages, err := internalgithub.FetchOperatorMessages(ctx, api, prConfig, accepted.Issue)
	if err != nil {
		return accepted, err
	}
	index := slices.IndexFunc(messages, func(message internalgithub.OperatorMessage) bool { return message.ID == accepted.ID })
	if index < 0 {
		return accepted, errors.New("operator message is not durably accepted")
	}
	accepted = messages[index]
	if accepted.State != "queued" && accepted.State != "claimed" {
		return accepted, nil
	}
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: accepted.Repository, Issue: accepted.Issue, Attempt: accepted.Attempt, Message: accepted.Message}
	issues, remote, err := fetchOperatorMessageTarget(ctx, api, prConfig, proposal)
	if err != nil {
		return accepted, err
	}
	runtimeState := newOperatorMessageRuntime(stateRoot)
	runtimeState.Source = filepath.Join(productionAttemptRoot(stateRoot), internalgithub.RepositoryIdentifier(cfg.Repository)+".source.bundle")
	runtimeState.AllowEnv = cfg.Commands.Environment
	manifests, err := runtimeState.Discover()
	if err != nil {
		return accepted, err
	}
	check := func(ctx context.Context, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
		return verifyOperatorMessageRuntime(ctx, &runtimeState, manifest, fact)
	}
	refresh := func(ctx context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
		currentManifests, err := runtimeState.Discover()
		return issues, remote, currentManifests, err
	}
	accept := func(ctx context.Context, proposal orchestratoragent.MessageProposal, expected operatorAttemptBinding) (bool, error) {
		currentIssues, currentRemote, err := fetchOperatorMessageTarget(ctx, api, prConfig, proposal)
		if err != nil {
			return false, err
		}
		return matchesOperatorMessageBinding(proposal, expected, currentIssues, currentRemote), nil
	}
	key := fmt.Sprintf("%s#%d/%d", accepted.Repository, accepted.Issue, accepted.Attempt)
	byAttempt := map[string][]internalgithub.OperatorMessage{key: {accepted}}
	if err := resumeOperatorMessages(ctx, api, prConfig, &runtimeState, implementationBoundary(stateRoot), issues, remote, manifests, byAttempt, cfg.Commands.Implementation, check, refresh, accept); err != nil {
		return byAttempt[key][0], err
	}
	return byAttempt[key][0], nil
}

func deliverOperatorMessageWithRetry(ctx context.Context, configPath, stateRoot string, message internalgithub.OperatorMessage) (internalgithub.OperatorMessage, error) {
	var err error
	for range 2 {
		message, err = deliverOperatorMessage(ctx, configPath, stateRoot, message)
		if err == nil || ctx.Err() != nil {
			return message, err
		}
	}
	return message, err
}

func retryProjectedOperatorMessages(ctx context.Context, configPath, stateRoot string) error {
	body, err := readDashboardStatus(filepath.Join(stateRoot, "status.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var snapshot struct {
		UpdatedAt time.Time                     `json:"updated_at"`
		Statuses  []orchestrator.RecoveryStatus `json:"statuses"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&snapshot) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid status snapshot")
	}
	pending := make(map[int]map[string]bool)
	pendingCount := 0
	for _, status := range snapshot.Statuses {
		for _, message := range status.OperatorMessages {
			if message.State != "queued" {
				continue
			}
			pendingCount++
			if pendingCount > 64 {
				return errors.New("operator message startup recovery exceeds limit")
			}
			if pending[status.Issue] == nil {
				pending[status.Issue] = make(map[string]bool)
			}
			pending[status.Issue][message.ID] = true
		}
	}
	if len(pending) == 0 {
		return nil
	}
	cfg, err := config.Load(configPath)
	if err != nil {
		return err
	}
	api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	if err := api.VerifyRepository(ctx, cfg.Repository); err != nil {
		return err
	}
	prConfig := githubPRConfig(cfg, user.ID)
	for issue, ids := range pending {
		messages, err := internalgithub.FetchOperatorMessages(ctx, api, prConfig, issue)
		if err != nil {
			return err
		}
		for _, message := range messages {
			if !ids[message.ID] || message.State != "queued" && message.State != "claimed" {
				continue
			}
			delivered, err := deliverOperatorMessageWithRetry(ctx, configPath, stateRoot, message)
			if projectionErr := writeOperatorMessageStatus(stateRoot, delivered); projectionErr != nil {
				return projectionErr
			}
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func verifyOperatorMessageRuntime(ctx context.Context, runtimeState *agentruntime.Runtime, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
	if manifest.State == "running" {
		if err := runtimeState.VerifyOwned(ctx, manifest); err == nil {
			return nil
		}
	}
	head := fact.HeadSHA
	if head == "" {
		head = fact.BaseSHA
	}
	return runtimeState.VerifyRetained(ctx, manifest, head)
}

func newOperatorMessageRuntime(stateRoot string) agentruntime.Runtime {
	boundary := implementationBoundary(stateRoot)
	return agentruntime.Runtime{Root: productionAttemptRoot(stateRoot), StateRoot: stateRoot, Runner: boundary, VerifyWorker: func(ctx context.Context) error {
		_, err := boundary.call(ctx, "verify", agentruntime.Command{})
		return err
	}}
}

func validateOperatorMessageTarget(ctx context.Context, proposal orchestratoragent.MessageProposal, issues []internalgithub.RecoveryIssueFact, remote []internalgithub.RecoveryAttemptFact, manifests []agentruntime.Manifest, check orchestrator.RuntimeCheck) error {
	if err := validateOperatorMessageAuthority(proposal, issues, remote); err != nil {
		return err
	}
	issueIndex := slices.IndexFunc(issues, func(issue internalgithub.RecoveryIssueFact) bool {
		return issue.Repository == proposal.Repository && issue.Issue == proposal.Issue
	})
	issue := issues[issueIndex]
	matching := make([]agentruntime.Manifest, 0, 1)
	for _, manifest := range manifests {
		if manifest.Repository == proposal.Repository && manifest.Issue == proposal.Issue && manifest.Attempt == proposal.Attempt {
			matching = append(matching, manifest)
		}
	}
	if len(matching) != 1 {
		return errors.New("operator message target runtime is missing or ambiguous")
	}
	manifest := matching[0]
	if binding := issue.ActiveAttempt; binding != nil && (binding.BaseSHA != manifest.BaseSHA || !slices.Contains([]string{"active", "review-ready"}, binding.State)) {
		return errors.New("operator message target does not match the issue's exact base binding")
	}
	facts := make([]orchestrator.AttemptFact, 0, len(remote)+1)
	for _, fact := range remote {
		if fact.Repository == proposal.Repository && fact.Issue == proposal.Issue && fact.Attempt == proposal.Attempt && issue.ActiveAttempt != nil && fact.BaseSHA != issue.ActiveAttempt.BaseSHA {
			return errors.New("operator message target has conflicting active base bindings")
		}
		facts = append(facts, orchestrator.AttemptFact{Repository: fact.Repository, Issue: fact.Issue, Attempt: fact.Attempt, BaseSHA: fact.BaseSHA, HeadSHA: fact.HeadSHA, PR: fact.PR, State: fact.State, Checks: fact.Checks, Diagnostic: fact.Diagnostic})
	}
	if binding := issue.ActiveAttempt; binding != nil && !slices.ContainsFunc(remote, func(fact internalgithub.RecoveryAttemptFact) bool {
		return fact.Repository == binding.Repository && fact.Issue == binding.Issue && fact.Attempt == binding.Attempt
	}) {
		facts = append(facts, orchestrator.AttemptFact{Repository: binding.Repository, Issue: binding.Issue, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, HeadSHA: binding.HeadSHA, PR: binding.PR, State: binding.State, Checks: binding.Checks, Diagnostic: binding.Diagnostic})
	}
	factIndex := slices.IndexFunc(facts, func(fact orchestrator.AttemptFact) bool {
		return fact.Repository == proposal.Repository && fact.Issue == proposal.Issue && fact.Attempt == proposal.Attempt
	})
	if factIndex < 0 || check == nil || check(ctx, manifest, facts[factIndex]) != nil {
		return errors.New("operator message target does not have a verified exact runtime owner")
	}
	statuses := orchestrator.RecoverChecked(ctx, facts, []agentruntime.Manifest{manifest}, func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil })
	statusIndex := slices.IndexFunc(statuses, func(status orchestrator.RecoveryStatus) bool {
		return status.Repository == proposal.Repository && status.Issue == proposal.Issue && status.Attempt == proposal.Attempt
	})
	if statusIndex < 0 || !slices.Contains([]string{"active", "review-ready"}, statuses[statusIndex].State) || !slices.Contains([]string{"implementation", "validation", "review", "findings-handoff", "publication"}, statuses[statusIndex].CurrentPhase) {
		return errors.New("operator message target does not have a verified exact runtime owner")
	}
	return nil
}

func validateOperatorMessageAuthority(proposal orchestratoragent.MessageProposal, issues []internalgithub.RecoveryIssueFact, remote []internalgithub.RecoveryAttemptFact) error {
	issueIndex := slices.IndexFunc(issues, func(issue internalgithub.RecoveryIssueFact) bool {
		return issue.Repository == proposal.Repository && issue.Issue == proposal.Issue
	})
	if issueIndex < 0 || issues[issueIndex].Cancelled || issues[issueIndex].Completed || !issues[issueIndex].DispatchAuthorized {
		return errors.New("operator message target is not an active authorized issue attempt")
	}
	if binding := issues[issueIndex].ActiveAttempt; binding != nil && (binding.Repository != proposal.Repository || binding.Issue != proposal.Issue || binding.Attempt != proposal.Attempt) {
		return errors.New("operator message target conflicts with the issue's active attempt binding")
	}
	if slices.ContainsFunc(remote, func(fact internalgithub.RecoveryAttemptFact) bool {
		return fact.Repository == proposal.Repository && fact.Issue == proposal.Issue && fact.Attempt == proposal.Attempt && !slices.Contains([]string{"active", "review-ready"}, fact.State)
	}) {
		return errors.New("operator message target attempt is not active")
	}
	return nil
}

type operatorAttemptBinding struct {
	Repository       string
	Issue, Attempt   int
	PR               int
	BaseSHA, HeadSHA string
}

func operatorMessageBinding(proposal orchestratoragent.MessageProposal, issues []internalgithub.RecoveryIssueFact, remote []internalgithub.RecoveryAttemptFact) (operatorAttemptBinding, error) {
	if err := validateOperatorMessageAuthority(proposal, issues, remote); err != nil {
		return operatorAttemptBinding{}, err
	}
	issue := issues[slices.IndexFunc(issues, func(issue internalgithub.RecoveryIssueFact) bool {
		return issue.Repository == proposal.Repository && issue.Issue == proposal.Issue
	})]
	matches := make([]internalgithub.RecoveryAttemptFact, 0, 1)
	for _, fact := range remote {
		if fact.Repository == proposal.Repository && fact.Issue == proposal.Issue && fact.Attempt == proposal.Attempt {
			matches = append(matches, fact)
		}
	}
	if len(matches) > 1 {
		return operatorAttemptBinding{}, errors.New("operator message target has ambiguous remote bindings")
	}
	if len(matches) == 1 {
		fact := matches[0]
		if fact.BaseSHA == "" || fact.PR > 0 && fact.HeadSHA == "" || issue.ActiveAttempt != nil && issue.ActiveAttempt.BaseSHA != fact.BaseSHA {
			return operatorAttemptBinding{}, errors.New("operator message target has an incomplete or conflicting remote binding")
		}
		return operatorAttemptBinding{fact.Repository, fact.Issue, fact.Attempt, fact.PR, fact.BaseSHA, fact.HeadSHA}, nil
	}
	if binding := issue.ActiveAttempt; binding != nil && binding.Repository == proposal.Repository && binding.Issue == proposal.Issue && binding.Attempt == proposal.Attempt && binding.BaseSHA != "" && slices.Contains([]string{"active", "review-ready"}, binding.State) {
		return operatorAttemptBinding{binding.Repository, binding.Issue, binding.Attempt, binding.PR, binding.BaseSHA, binding.HeadSHA}, nil
	}
	return operatorAttemptBinding{}, errors.New("operator message target has no exact active binding")
}

func matchesOperatorMessageBinding(proposal orchestratoragent.MessageProposal, expected operatorAttemptBinding, issues []internalgithub.RecoveryIssueFact, remote []internalgithub.RecoveryAttemptFact) bool {
	current, err := operatorMessageBinding(proposal, issues, remote)
	return err == nil && current == expected
}

func githubPRConfig(c config.Config, actorID int) internalgithub.PRAdapterConfig {
	return internalgithub.PRAdapterConfig{Repository: c.Repository, ReadyLabel: c.Labels.Ready, IssueFilterLabel: c.Labels.IssueFilter, HumanReviewLabel: c.CompletionPolicies.HumanReview, AutonomousMergeLabel: c.CompletionPolicies.AutonomousMerge, MergeMethod: "squash", PriorityP1Label: c.Labels.PriorityP1, PriorityP2Label: c.Labels.PriorityP2, PriorityP3Label: c.Labels.PriorityP3, DependencySection: c.Dependencies.Section, DefaultCompletion: c.CompletionPolicies.Default, ApprovalCommand: "/agent-symphony approve", CancelCommand: "/agent-symphony cancel", RetryCommand: "/agent-symphony retry", ActorID: actorID}
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

func effectiveServeInterval(fs *flag.FlagSet, configuredSeconds int, override time.Duration) time.Duration {
	interval := time.Duration(configuredSeconds) * time.Second
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "interval" {
			interval = override
		}
	})
	return interval
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

type reconcileOptions struct {
	transition bool
	intake     bool
	timeout    time.Duration
	authorize  func([]orchestrator.RecoveryStatus) error
}

func reconcileGitHub(ctx context.Context, configPath, statePath, stateRoot string, transition bool) ([]orchestrator.RecoveryStatus, error) {
	return reconcileGitHubWith(ctx, configPath, statePath, stateRoot, reconcileOptions{transition: transition, intake: transition, timeout: 5 * time.Minute})
}

func reconcileGitHubWith(ctx context.Context, configPath, statePath, stateRoot string, options reconcileOptions) (result []orchestrator.RecoveryStatus, resultErr error) {
	started := time.Now()
	if options.timeout <= 0 {
		options.timeout = 2 * time.Minute
	}
	ctx, cancel := context.WithDeadline(ctx, started.Add(options.timeout))
	defer cancel()
	cache, err := internalgithub.LoadReadCache(filepath.Join(stateRoot, "github-etag-cache.json"))
	if err != nil {
		return nil, fmt.Errorf("load GitHub cache: %w", err)
	}
	defer func() {
		if resultErr != nil {
			return
		}
		if err := cache.Save(); err != nil {
			resultErr = fmt.Errorf("save GitHub cache: %w", err)
		}
	}()
	c, err := config.Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load configuration: %w", err)
	}
	root, err := config.GitRoot()
	if err != nil {
		return nil, fmt.Errorf("resolve repository root: %w", err)
	}
	if runningOnWSL() {
		if err := validateWSLFilesystem(root, filepath.Join(root, c.WorktreeRoot), stateRoot, "/proc/mounts"); err != nil {
			return nil, fmt.Errorf("validate WSL filesystem: %w", err)
		}
	}
	api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient, Cache: cache}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return nil, fmt.Errorf("authenticate GitHub: %w", err)
	}
	if err := api.VerifyRepository(ctx, c.Repository); err != nil {
		return nil, fmt.Errorf("verify GitHub repository: %w", err)
	}
	remote, err := internalgithub.FetchAttemptFacts(ctx, api, c.Repository, user.ID)
	if err != nil {
		return nil, fmt.Errorf("fetch pull request attempts: %w", err)
	}
	prConfig := githubPRConfig(c, user.ID)
	issues, err := internalgithub.FetchIssueFacts(ctx, api, prConfig, remote, options.intake)
	if err != nil {
		return nil, fmt.Errorf("fetch issue controls: %w", err)
	}
	remote, facts := recoveryAttemptFacts(remote, issues)
	boundary := implementationBoundary(stateRoot)
	binary, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve coordinator executable: %w", err)
	}
	attemptRoot := productionAttemptRoot(stateRoot)
	baseBranch, baseSHA := "", ""
	if len(issues) > 0 {
		baseBranch, baseSHA = issues[0].BaseBranch, issues[0].BaseSHA
	}
	source, err := seedAttemptSource(ctx, root, c.Repository, attemptRoot, baseBranch, baseSHA)
	if err != nil {
		return nil, fmt.Errorf("refresh attempt source: %w", err)
	}
	r := agentruntime.Runtime{Root: attemptRoot, StateRoot: stateRoot, Source: source, Helper: binary, Runner: boundary, AllowEnv: c.Commands.Environment, VerifyWorker: func(ctx context.Context) error {
		_, err := boundary.call(ctx, "verify", agentruntime.Command{})
		return err
	}}
	manifests, err := r.Discover()
	if err != nil {
		return nil, fmt.Errorf("discover attempt manifests: %w", err)
	}
	deferBoundResume := options.transition && options.authorize != nil
	if options.transition && !deferBoundResume {
		manifests, err = resumeBoundAttempts(ctx, &r, c, issues, manifests, remote)
		if err != nil {
			return nil, fmt.Errorf("resume bound attempts: %w", err)
		}
	}
	checkRuntime := func(ctx context.Context, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
		head := fact.HeadSHA
		if head == "" {
			head = fact.BaseSHA
		}
		return r.VerifyActive(ctx, manifest, head)
	}
	statuses, decisions := projectRecoveryStatuses(ctx, facts, issues, manifests, c.Concurrency, checkRuntime)
	if err := writeStatusSnapshot(stateRoot, statuses); err != nil {
		return statuses, fmt.Errorf("write status projection: %w", err)
	}
	if options.authorize != nil {
		if err := options.authorize(statuses); err != nil {
			return statuses, fmt.Errorf("authorize requested transition: %w", err)
		}
	}
	if !options.transition {
		operatorMessages, err := fetchOperatorMessages(ctx, api, prConfig, issues, remote, manifests)
		if err != nil {
			return statuses, fmt.Errorf("fetch operator messages: %w", err)
		}
		attachOperatorMessageStatuses(statuses, operatorMessages)
		if err := writeStatusSnapshot(stateRoot, statuses); err != nil {
			return statuses, fmt.Errorf("write status projection: %w", err)
		}
		return statuses, nil
	}
	operatorMessages := map[string][]internalgithub.OperatorMessage{}
	refreshProjection := func(currentFacts []orchestrator.AttemptFact, currentIssues []internalgithub.RecoveryIssueFact) ([]agentruntime.Manifest, []orchestrator.Decision, error) {
		currentManifests, discoverErr := r.Discover()
		if discoverErr != nil {
			return nil, nil, discoverErr
		}
		statuses, decisions = projectRecoveryStatuses(ctx, currentFacts, currentIssues, currentManifests, c.Concurrency, checkRuntime)
		attachOperatorMessageStatuses(statuses, operatorMessages)
		return currentManifests, decisions, writeStatusSnapshot(stateRoot, statuses)
	}
	// Governance may mutate GitHub only after authenticated repository access,
	// exact proposal authorization, and the duplicate suppression below.
	if slices.ContainsFunc(statuses, func(s orchestrator.RecoveryStatus) bool {
		return s.State == "blocked" && strings.Contains(s.Diagnostic, "duplicate")
	}) {
		if options.authorize != nil {
			return statuses, errors.New("requested transition is blocked by a duplicate attempt projection")
		}
		return statuses, nil
	}
	if deferBoundResume {
		manifests, err = resumeBoundAttempts(ctx, &r, c, issues, manifests, remote)
		if err != nil {
			return statuses, fmt.Errorf("resume bound attempts: %w", err)
		}
	}
	dispatchErr := dispatchIssues(ctx, api, &r, c, prConfig, issues, decisions)
	remote, facts = recoveryAttemptFacts(remote, issues)
	if _, _, err = refreshProjection(facts, issues); err != nil {
		return statuses, errors.Join(fmt.Errorf("write dispatched status projection: %w", err), dispatchErr)
	}
	if dispatchErr != nil {
		return statuses, fmt.Errorf("dispatch eligible issues: %w", dispatchErr)
	}
	operatorMessages, err = fetchOperatorMessages(ctx, api, prConfig, issues, remote, manifests)
	if err != nil {
		return statuses, fmt.Errorf("fetch operator messages: %w", err)
	}
	attachOperatorMessageStatuses(statuses, operatorMessages)
	if err := writeStatusSnapshot(stateRoot, statuses); err != nil {
		return statuses, fmt.Errorf("write operator message status projection: %w", err)
	}
	if err := resumeHandoffs(ctx, &r, boundary, statePath, stateRoot, statuses, manifests, c.Commands.Implementation); err != nil {
		return statuses, fmt.Errorf("resume durable handoffs: %w", err)
	}
	queuedManifests, err := r.Discover()
	if err != nil {
		return statuses, fmt.Errorf("rediscover queued attempts: %w", err)
	}
	monitorErr := monitorAttempts(ctx, &r, statuses, queuedManifests, issues)
	queuedManifests, decisions, err = refreshProjection(facts, issues)
	if err != nil {
		return statuses, errors.Join(fmt.Errorf("refresh monitored attempt projection: %w", err), monitorErr)
	}
	if monitorErr != nil {
		return statuses, fmt.Errorf("monitor live attempts: %w", monitorErr)
	}
	transitionErr := monitorQueuedAttempts(ctx, api, &r, c, issues, queuedManifests, remote, operatorMessages, statePath, stateRoot, func() error {
		_, _, err := refreshProjection(facts, issues)
		return err
	})
	queuedManifests, decisions, err = refreshProjection(facts, issues)
	if err != nil {
		return statuses, errors.Join(fmt.Errorf("refresh completed attempt projection: %w", err), transitionErr)
	}
	if transitionErr != nil {
		return statuses, fmt.Errorf("process completed worker results: %w", transitionErr)
	}
	if err := cleanupCompletedAttempts(ctx, boundary, facts, queuedManifests); err != nil {
		return statuses, fmt.Errorf("clean completed attempts: %w", err)
	}
	if err := ensurePublishedEvidence(ctx, api, facts, queuedManifests, user.ID); err != nil {
		return statuses, fmt.Errorf("repair published evidence: %w", err)
	}
	if err := internalgithub.RunPRReconciliation(ctx, api, prConfig, statePath); err != nil {
		return statuses, fmt.Errorf("reconcile pull request governance: %w", err)
	}
	// Re-read GitHub after governance and monitoring. A merge or cancellation
	// observed here wins over every queued operator message.
	freshRemote, err := internalgithub.FetchAttemptFacts(ctx, api, c.Repository, user.ID)
	if err != nil {
		return statuses, fmt.Errorf("refresh pull request attempts: %w", err)
	}
	freshIssues, err := internalgithub.FetchIssueFacts(ctx, api, prConfig, freshRemote, false)
	if err != nil {
		return statuses, fmt.Errorf("refresh issue controls: %w", err)
	}
	freshRemote, freshFacts := recoveryAttemptFacts(freshRemote, freshIssues)
	if err := cleanupCompletedAttempts(ctx, boundary, freshFacts, queuedManifests); err != nil {
		return statuses, fmt.Errorf("clean newly completed attempts: %w", err)
	}
	operatorCheck := func(ctx context.Context, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
		head := fact.HeadSHA
		if head == "" {
			head = fact.BaseSHA
		}
		return r.VerifyActive(ctx, manifest, head)
	}
	refreshOperatorTarget := func(ctx context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
		attempts, err := internalgithub.FetchAttemptFacts(ctx, api, c.Repository, user.ID)
		if err != nil {
			return nil, nil, nil, err
		}
		currentIssues, err := internalgithub.FetchIssueFacts(ctx, api, prConfig, attempts, false)
		if err != nil {
			return nil, nil, nil, err
		}
		currentManifests, err := r.Discover()
		return currentIssues, attempts, currentManifests, err
	}
	acceptOperatorTarget := func(ctx context.Context, proposal orchestratoragent.MessageProposal, expected operatorAttemptBinding) (bool, error) {
		attempts, err := internalgithub.FetchAttemptFacts(ctx, api, c.Repository, user.ID)
		if err != nil {
			return false, err
		}
		currentIssues, err := internalgithub.FetchIssueFacts(ctx, api, prConfig, attempts, false)
		if err != nil {
			return false, err
		}
		return matchesOperatorMessageBinding(proposal, expected, currentIssues, attempts), nil
	}
	validatedManifests, decisions, err := refreshProjection(freshFacts, freshIssues)
	if err != nil {
		return statuses, fmt.Errorf("write fresh status projection: %w", err)
	}
	operatorErr := resumeOperatorMessages(ctx, api, prConfig, &r, boundary, freshIssues, freshRemote, validatedManifests, operatorMessages, c.Commands.Implementation, operatorCheck, refreshOperatorTarget, acceptOperatorTarget)
	_, decisions, err = refreshProjection(freshFacts, freshIssues)
	if err != nil {
		return statuses, errors.Join(fmt.Errorf("write final status projection: %w", err), operatorErr)
	}
	if operatorErr != nil {
		return statuses, fmt.Errorf("resume operator messages: %w", operatorErr)
	}
	if err := ctx.Err(); err != nil {
		return statuses, fmt.Errorf("reconciliation exceeded the %s recovery target: %w", options.timeout, err)
	}
	return statuses, nil
}

func recoveryAttemptFacts(remote []internalgithub.RecoveryAttemptFact, issues []internalgithub.RecoveryIssueFact) ([]internalgithub.RecoveryAttemptFact, []orchestrator.AttemptFact) {
	remote = slices.Clone(remote)
	facts := make([]orchestrator.AttemptFact, len(remote))
	for i, fact := range remote {
		facts[i] = orchestrator.AttemptFact{Repository: fact.Repository, Issue: fact.Issue, Attempt: fact.Attempt, BaseSHA: fact.BaseSHA, HeadSHA: fact.HeadSHA, PR: fact.PR, State: fact.State, Checks: fact.Checks, Diagnostic: fact.Diagnostic}
	}
	for i := range issues {
		if binding := issues[i].ActiveAttempt; binding != nil && !slices.ContainsFunc(remote, func(fact internalgithub.RecoveryAttemptFact) bool {
			return fact.Repository == binding.Repository && fact.Issue == binding.Issue && fact.Attempt == binding.Attempt
		}) {
			remote = append(remote, *binding)
			facts = append(facts, orchestrator.AttemptFact{Repository: binding.Repository, Issue: binding.Issue, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: binding.State, Diagnostic: binding.Diagnostic})
		}
		for _, binding := range issues[i].TerminalAttempts {
			if !slices.ContainsFunc(facts, func(fact orchestrator.AttemptFact) bool {
				return fact.Repository == binding.Repository && fact.Issue == binding.Issue && fact.Attempt == binding.Attempt && fact.BaseSHA == binding.BaseSHA && fact.State == binding.State
			}) {
				facts = append(facts, orchestrator.AttemptFact{Repository: binding.Repository, Issue: binding.Issue, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: binding.State, Diagnostic: binding.Diagnostic})
			}
		}
	}
	return remote, facts
}

func projectRecoveryStatuses(ctx context.Context, facts []orchestrator.AttemptFact, issues []internalgithub.RecoveryIssueFact, manifests []agentruntime.Manifest, capacity int, check orchestrator.RuntimeCheck) ([]orchestrator.RecoveryStatus, []orchestrator.Decision) {
	issues = slices.Clone(issues)
	for i := range issues {
		issues[i].Blockers = slices.Clone(issues[i].Blockers)
		issues[i].Active = issues[i].Active || slices.ContainsFunc(manifests, func(manifest agentruntime.Manifest) bool {
			return manifest.Repository == issues[i].Repository && manifest.Issue == issues[i].Issue && (manifest.State == "preparing" || manifest.State == "running")
		})
	}
	addTerminalAttemptBlockers(issues, manifests, facts)
	return joinIssueProjection(orchestrator.RecoverChecked(ctx, facts, manifests, check), issues, capacity)
}

func cleanupCompletedAttempts(ctx context.Context, boundary boundaryCaller, facts []orchestrator.AttemptFact, manifests []agentruntime.Manifest) error {
	for _, fact := range facts {
		if fact.State != "completed" {
			continue
		}
		index := slices.IndexFunc(manifests, func(manifest agentruntime.Manifest) bool {
			if manifest.State != "completed" && manifest.State != "preparing" && manifest.State != "running" {
				return false
			}
			published := manifest
			published.State = "completed"
			return orchestrator.MatchesPublishedAttempt(published, fact)
		})
		if index < 0 {
			continue
		}
		operation := "cleanup"
		if manifests[index].State == "preparing" || manifests[index].State == "running" {
			operation = "abandon"
		}
		body, _ := json.Marshal(manifests[index])
		if _, err := boundary.call(ctx, operation, agentruntime.Command{Stdin: bytes.NewReader(body)}); err != nil {
			return fmt.Errorf("clean up completed %s#%d attempt %d: %w", fact.Repository, fact.Issue, fact.Attempt, err)
		}
	}
	return nil
}

func ensurePublishedEvidence(ctx context.Context, api internalgithub.API, facts []orchestrator.AttemptFact, manifests []agentruntime.Manifest, actorID int) error {
	for _, fact := range facts {
		if fact.State != "active" && fact.State != "review-ready" {
			continue
		}
		if !slices.ContainsFunc(manifests, func(manifest agentruntime.Manifest) bool {
			return orchestrator.MatchesPublishedAttempt(manifest, fact)
		}) {
			continue
		}
		if err := api.EnsureEvidence(ctx, fact.Repository, fact.Issue, fact.Attempt, fact.HeadSHA, actorID); err != nil {
			return err
		}
	}
	return nil
}

func addTerminalAttemptBlockers(issues []internalgithub.RecoveryIssueFact, manifests []agentruntime.Manifest, facts []orchestrator.AttemptFact) {
	for i := range issues {
		for _, manifest := range manifests {
			if manifest.Repository == issues[i].Repository && manifest.Issue == issues[i].Issue && (manifest.State == "failed" || manifest.State == "cancelled" || manifest.State == "completed") {
				bound := (issues[i].ActiveAttempt != nil && issues[i].ActiveAttempt.Attempt == manifest.Attempt) || slices.ContainsFunc(issues[i].TerminalAttempts, func(attempt internalgithub.RecoveryAttemptFact) bool {
					return attempt.Attempt == manifest.Attempt
				})
				published := slices.ContainsFunc(facts, func(fact orchestrator.AttemptFact) bool {
					return orchestrator.MatchesPublishedAttempt(manifest, fact)
				})
				if bound || published {
					continue
				}
				if issues[i].Retry && issues[i].Attempt > manifest.Attempt {
					issues[i].Attempt = max(issues[i].Attempt, manifest.Attempt+1)
				} else {
					issues[i].Eligible = false
					issues[i].RecoveryAuthorized = false
					issues[i].Blockers = append(issues[i].Blockers, "local terminal attempt awaits or has durable GitHub outcome")
				}
			}
		}
	}
}

func joinIssueProjection(statuses []orchestrator.RecoveryStatus, issues []internalgithub.RecoveryIssueFact, capacity int) ([]orchestrator.RecoveryStatus, []orchestrator.Decision) {
	scheduled := make([]orchestrator.Issue, len(issues))
	for i, issue := range issues {
		scheduled[i] = orchestrator.Issue{Repository: issue.Repository, Number: issue.Issue, Priority: issue.Priority, CreatedAt: issue.CreatedAt, Dependencies: issue.Dependencies, Paths: issue.Paths, Eligible: issue.Eligible, Blockers: issue.Blockers, Active: issue.Active, Completed: issue.Completed}
	}
	decisions := orchestrator.Schedule(scheduled, orchestrator.Capacity{Global: capacity, Repositories: map[string]int{}})
	for _, issue := range issues {
		currentAttempt := issue.Attempt
		if issue.CurrentAttempt > 0 {
			currentAttempt = issue.CurrentAttempt
		}
		found := false
		for j := range statuses {
			if statuses[j].Repository == issue.Repository && statuses[j].Issue == issue.Issue {
				statuses[j].Title, statuses[j].Priority, statuses[j].Dependencies = issue.Title, issue.Priority, issue.Dependencies
				statuses[j].DispatchAuthorized = issue.DispatchAuthorized
				statuses[j].NeedsAttention = statuses[j].Attempt == currentAttempt && issue.NeedsAttention
				if statuses[j].Attempt == currentAttempt {
					statuses[j].Blockers = append(statuses[j].Blockers, issue.Blockers...)
				}
				if statuses[j].State == "failed" {
					statuses[j].Retryable = statuses[j].Retryable && issue.RecoveryAuthorized && issue.RecoveryAttempt == statuses[j].Attempt
				} else {
					statuses[j].Retryable = statuses[j].Retryable && issue.RecoveryAuthorized
				}
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
		statuses = append(statuses, orchestrator.RecoveryStatus{Repository: issue.Repository, Issue: issue.Issue, Title: issue.Title, Attempt: issue.Attempt, State: string(decision.State), CurrentPhase: string(decision.State), Priority: issue.Priority, Dependencies: issue.Dependencies, Blockers: issue.Blockers, Action: decision.Explanation, NeedsAttention: issue.NeedsAttention})
	}
	return statuses, decisions
}

func writeStatusSnapshot(stateRoot string, statuses []orchestrator.RecoveryStatus) error {
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		return err
	}
	path := filepath.Join(stateRoot, "status.json")
	if info, err := os.Lstat(path); err == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0) {
		return errors.New("status state must be a regular non-symlink file")
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	body, err := json.MarshalIndent(struct {
		UpdatedAt time.Time                     `json:"updated_at"`
		Statuses  []orchestrator.RecoveryStatus `json:"statuses"`
	}{time.Now().UTC(), statuses}, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(stateRoot, ".status-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close()
		return err
	}
	if _, err := tmp.Write(append(body, '\n')); err != nil {
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

func writeOperatorMessageStatus(stateRoot string, message internalgithub.OperatorMessage) error {
	body, err := readDashboardStatus(filepath.Join(stateRoot, "status.json"))
	if err != nil {
		return err
	}
	var snapshot struct {
		UpdatedAt time.Time                     `json:"updated_at"`
		Statuses  []orchestrator.RecoveryStatus `json:"statuses"`
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&snapshot) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return errors.New("invalid status snapshot")
	}
	index := slices.IndexFunc(snapshot.Statuses, func(status orchestrator.RecoveryStatus) bool {
		return status.Repository == message.Repository && status.Issue == message.Issue && status.Attempt == message.Attempt
	})
	if index < 0 {
		return errors.New("operator message target is missing from the status snapshot")
	}
	state := message.State
	if state == "claimed" {
		state = "queued"
	}
	status := orchestrator.OperatorMessageStatus{ID: message.ID, State: state, UpdatedAt: message.UpdatedAt, Diagnostic: message.Diagnostic}
	messages := &snapshot.Statuses[index].OperatorMessages
	if messageIndex := slices.IndexFunc(*messages, func(current orchestrator.OperatorMessageStatus) bool { return current.ID == message.ID }); messageIndex >= 0 {
		(*messages)[messageIndex] = status
	} else {
		*messages = append(*messages, status)
	}
	return writeStatusSnapshot(stateRoot, snapshot.Statuses)
}

func seedAttemptSource(ctx context.Context, checkout, repositoryName, attemptRoot, baseBranch, baseSHA string) (string, error) {
	mode := os.FileMode(0o770)
	if !hostIsolationInstalled() {
		mode = 0o700
	}
	if err := os.MkdirAll(attemptRoot, mode); err != nil {
		return "", fmt.Errorf("open provisioned attempt root: %w", err)
	}
	if baseBranch != "" || baseSHA != "" {
		if baseBranch == "" || !preflightObjectID.MatchString(baseSHA) {
			return "", errors.New("seed worker source requires a valid base branch and commit")
		}
		if out, err := exec.CommandContext(ctx, "git", "check-ref-format", "--branch", baseBranch).CombinedOutput(); err != nil {
			return "", fmt.Errorf("validate worker source branch: %w: %s", err, strings.TrimSpace(string(out)))
		}
		remoteRef := "refs/remotes/origin/" + baseBranch
		if exec.CommandContext(ctx, "git", "-C", checkout, "merge-base", "--is-ancestor", baseSHA, remoteRef).Run() != nil {
			refspec := "+refs/heads/" + baseBranch + ":" + remoteRef
			if out, err := exec.CommandContext(ctx, "git", "-C", checkout, "fetch", "--no-tags", "origin", refspec).CombinedOutput(); err != nil {
				return "", fmt.Errorf("refresh worker source base: %w: %s", err, strings.TrimSpace(string(out)))
			}
			if err := exec.CommandContext(ctx, "git", "-C", checkout, "merge-base", "--is-ancestor", baseSHA, remoteRef).Run(); err != nil {
				return "", errors.New("refreshed worker source does not contain the selected base")
			}
		}
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
	if out, err := exec.CommandContext(ctx, "git", "-C", checkout, "bundle", "create", name, "--all").CombinedOutput(); err != nil {
		return "", fmt.Errorf("seed worker source bundle: %w: %s", err, strings.TrimSpace(string(out)))
	}
	if err := os.Chmod(name, 0o640); err != nil {
		return "", err
	}
	path := filepath.Join(attemptRoot, internalgithub.RepositoryIdentifier(repositoryName)+".source.bundle")
	if err := os.Rename(name, path); err != nil {
		return "", err
	}
	return path, nil
}

func startIssueAttempt(ctx context.Context, runtime *agentruntime.Runtime, cfg config.Config, issue internalgithub.RecoveryIssueFact) (agentruntime.Manifest, error) {
	attempt := agentruntime.Attempt{Repository: issue.Repository, Issue: issue.Issue, Number: issue.Attempt, BaseSHA: issue.BaseSHA, Command: cfg.Commands.Implementation, Eligible: func() bool { return issue.DispatchAuthorized }}
	identity, err := agentruntime.AttemptIdentity(runtime.Root, attempt)
	if err != nil {
		return agentruntime.Manifest{}, err
	}
	attempt.Context = implementationPrompt(issue, identity)
	return runtime.PrepareAndStart(ctx, attempt)
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
		if !issue.Eligible || !issue.DispatchAuthorized {
			return fmt.Errorf("dispatch %s#%d attempt %d: issue is not eligible", issue.Repository, issue.Issue, issue.Attempt)
		}
		identity, err := agentruntime.AttemptIdentity(runtime.Root, agentruntime.Attempt{Repository: issue.Repository, Issue: issue.Issue, Number: issue.Attempt, BaseSHA: issue.BaseSHA})
		if err != nil {
			return fmt.Errorf("identify dispatch %s#%d attempt %d: %w", issue.Repository, issue.Issue, issue.Attempt, err)
		}
		detail := fmt.Sprintf("Implementation session reserved.\n\n- Project: `%s`\n- Branch: `%s`\n- Worktree: `%s`\n- Session: `%s`", issue.Repository, identity.Branch, identity.Worktree, identity.Session)
		if err := internalgithub.EnsureActiveAttempt(ctx, api, prConfig, issue.Issue, issue.Attempt, issue.BaseSHA, detail); err != nil {
			return fmt.Errorf("bind dispatch %s#%d attempt %d: %w", issue.Repository, issue.Issue, issue.Attempt, err)
		}
		binding := internalgithub.RecoveryAttemptFact{Repository: issue.Repository, Issue: issue.Issue, Attempt: issue.Attempt, BaseSHA: issue.BaseSHA, State: "active"}
		issues[index].Active, issues[index].Eligible, issues[index].ActiveAttempt = true, false, &binding
		issue.DispatchAuthorized = issue.Eligible
		if _, err := startIssueAttempt(ctx, runtime, cfg, issue); err != nil {
			return fmt.Errorf("dispatch %s#%d attempt %d: %w", issue.Repository, issue.Issue, issue.Attempt, err)
		}
	}
	return nil
}

func implementationPrompt(issue internalgithub.RecoveryIssueFact, identity agentruntime.Manifest) string {
	return fmt.Sprintf("Project: %s\nIssue: #%d\nAttempt: %d\nBase branch: %s\nBranch: %s\nWorktree: %s\nSession: %s\n\n%s\n\nDirect status: use the installed gh CLI to post one unedited comment on issue #%d or its pull request: `/agent-symphony status needs-attention: REASON` or `/agent-symphony status clear: REASON`. A nonempty reason is required. Pair the comment with the `needs-attention` label on the bound issue: add it when setting status and remove it when clearing. Re-read both the comment and label before reporting the status changed; authentication, authorization, or partial-update errors are failures, never success. Other issue-authorized comments, labels, and Markdown links may also be updated directly with gh.\n\nCompletion contract: Make stdout exactly one JSON line of at most 64 KiB with nonempty validation and documentation evidence; progress and diagnostics belong on stderr. Agent Symphony captures stdout outside the worktree. Do not wrap it in Markdown fences or emit another stdout object.\n{\"type\":\"agent-symphony-result-v1\",\"validation\":\"tests run and results\",\"documentation\":\"documentation impact or none\"}", issue.Repository, issue.Issue, issue.Attempt, issue.BaseBranch, identity.Branch, identity.Worktree, identity.Session, issue.Body, issue.Issue)
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
	if strings.EqualFold(head, manifest.BaseSHA) {
		return workerResult{}, "", "", errors.New("worker produced no repository changes")
	}
	if err := scanGit(ctx, importedRepo, nil, []string{"merge-base", "--is-ancestor", manifest.BaseSHA, head}, nil); err != nil {
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
		if len(fields) != 4 || (string(fields[0]) != "100644" && string(fields[0]) != "100755") || string(fields[1]) != "blob" || !preflightObjectID.Match(fields[2]) || len(path) == 0 || path[0] == '/' || bytes.ContainsAny(path, "\\\n\r") || string(path) != filepath.Clean(string(path)) || string(path) == ".agent-symphony" || strings.HasPrefix(string(path), ".agent-symphony/") {
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

func amendIssueWithHumanInstructions(issue internalgithub.RecoveryIssueFact, messages []internalgithub.OperatorMessage, feedback []internalgithub.Feedback) (internalgithub.RecoveryIssueFact, []string, bool) {
	type instruction struct {
		created time.Time
		key     string
		body    string
	}
	var instructions []instruction
	operatorDelivered := false
	for _, message := range messages {
		want, err := internalgithub.PrepareOperatorMessage(message.Repository, message.Issue, message.Attempt, message.Message)
		if err == nil && want.ID == message.ID && message.State == "delivered" && message.Repository == issue.Repository && message.Issue == issue.Issue && message.Attempt == issue.Attempt {
			instructions = append(instructions, instruction{message.UpdatedAt, "operator:" + message.ID, message.Message})
			operatorDelivered = true
		}
	}
	for _, item := range feedback {
		if item.ID > 0 && item.Authorized && strings.TrimSpace(item.Body) != "" && slices.Contains([]string{"issue", "conversation", "inline", "review"}, item.Source) {
			instructions = append(instructions, instruction{item.CreatedAt, fmt.Sprintf("%s:%d", item.Source, item.ID), strings.TrimSpace(item.Body)})
		}
	}
	slices.SortFunc(instructions, func(a, b instruction) int {
		if compared := a.created.Compare(b.created); compared != 0 {
			return compared
		}
		return strings.Compare(a.key, b.key)
	})
	bodies := make([]string, len(instructions))
	for i, item := range instructions {
		bodies[i] = item.key + "\n" + item.body
	}
	if len(instructions) > 0 {
		encoded, _ := json.Marshal(bodies)
		issue.Body += "\n\n" + humanInstructionPrecedence + "\n" + string(encoded)
	}
	return issue, bodies, operatorDelivered
}

func monitorQueuedAttempts(ctx context.Context, api internalgithub.API, runtime *agentruntime.Runtime, cfg config.Config, issues []internalgithub.RecoveryIssueFact, manifests []agentruntime.Manifest, remote []internalgithub.RecoveryAttemptFact, operatorMessages map[string][]internalgithub.OperatorMessage, statePath, stateRoot string, refreshProjection func() error) error {
	recovery := &internalgithub.FileRecovery{Path: statePath}
	for _, manifest := range manifests {
		var prepared internalgithub.PreparedPublication
		preparedOK := false
		if statePath != "" {
			var err error
			prepared, preparedOK, err = recovery.PreparedHandoffPublication(ctx, manifest.Repository, manifest.Issue, manifest.Attempt)
			if err != nil {
				return err
			}
		}
		remoteIndex := slices.IndexFunc(remote, func(f internalgithub.RecoveryAttemptFact) bool {
			return f.Repository == manifest.Repository && f.Issue == manifest.Issue && f.Attempt == manifest.Attempt
		})
		issueIndex := slices.IndexFunc(issues, func(i internalgithub.RecoveryIssueFact) bool {
			return i.Repository == manifest.Repository && i.Issue == manifest.Issue
		})
		if issueIndex < 0 {
			continue
		}
		issue := issues[issueIndex]
		var bound internalgithub.RecoveryAttemptFact
		if remoteIndex >= 0 {
			bound = remote[remoteIndex]
		} else if issue.ActiveAttempt != nil {
			bound = *issue.ActiveAttempt
		}
		if !preparedOK && (bound.Repository != manifest.Repository || bound.Issue != manifest.Issue || bound.Attempt != manifest.Attempt || bound.BaseSHA != manifest.BaseSHA) {
			continue
		}
		issue.Attempt = manifest.Attempt
		attemptMessages := operatorMessages[fmt.Sprintf("%s#%d/%d", manifest.Repository, manifest.Issue, manifest.Attempt)]
		_, _, operatorDelivered := amendIssueWithHumanInstructions(issue, attemptMessages, nil)
		var (
			handoff internalgithub.RecoveryHandoff
			err     error
		)
		if preparedOK {
			handoff = prepared.Handoff
		} else if bound.PR > 0 {
			published := bound
			var received bool
			handoff, received, err = recovery.ReceivedHandoff(ctx, manifest.Repository, published.PR, manifest.Issue, manifest.Attempt, published.HeadSHA)
			if err != nil {
				return err
			}
			if !received {
				if !operatorDelivered {
					continue
				}
				handoff = internalgithub.RecoveryHandoff{}
			}
		}
		if !issue.DispatchAuthorized {
			continue
		}
		issue, humanInstructions, _ := amendIssueWithHumanInstructions(issue, attemptMessages, handoff.Feedback)
		current := manifest
		if manifest.State == "preparing" || manifest.State == "running" {
			continue // monitorAttempts owns live bound attempts from the same snapshot.
		}
		if current.State == "completed" {
			var completed internalgithub.PreparedPublication
			var prepare func(string) error
			if handoff.PR > 0 {
				prepare = func(head string) error {
					outcome := internalgithub.HandoffOutcome{Key: handoff.Key}
					if handoff.Validation {
						outcome.ValidationResult = "blocked"
						outcome.ValidationEvidence = "pull request head changed to " + head + "; validation must run against the published feedback head"
					}
					for _, feedback := range handoff.Feedback {
						outcome.Feedback = append(outcome.Feedback, internalgithub.FeedbackOutcome{ID: feedback.ID, Source: feedback.Source, State: internalgithub.FeedbackAddressed, Evidence: "published in head " + head})
					}
					completed = internalgithub.PreparedPublication{Handoff: handoff, Outcome: outcome, HeadSHA: head}
					return recovery.PrepareHandoffPublication(ctx, handoff, head, outcome)
				}
			} else if bound.PR > 0 {
				prepare = func(head string) error {
					if head == bound.HeadSHA {
						return nil
					}
					var err error
					completed, err = recovery.PrepareAttemptPublication(ctx, manifest.Repository, bound.PR, manifest.Issue, manifest.Attempt, bound.HeadSHA, head)
					return err
				}
			}
			pending, err := publishWorkerResult(ctx, api, runtime, cfg, issue, current, humanInstructions, stateRoot, prepare, refreshProjection)
			if err != nil {
				return errors.Join(err, durableAttemptFailure(ctx, api, issue, current, err))
			}
			if pending {
				continue
			}
			if completed.HeadSHA == "" {
				continue
			}
			if err := recovery.CompleteHandoffPublication(ctx, completed); err != nil {
				return err
			}
		} else if current.State == "failed" || current.State == "cancelled" {
			if err := durableAttemptFailure(ctx, api, issue, current, errors.New(current.Diagnostic)); err != nil {
				return err
			}
		}
	}
	return nil
}

func publishWorkerResult(ctx context.Context, api internalgithub.API, runtimeState *agentruntime.Runtime, cfg config.Config, issue internalgithub.RecoveryIssueFact, manifest agentruntime.Manifest, humanInstructions []string, stateRoot string, preparePublication func(string) error, refreshProjection func() error) (bool, error) {
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
		if refreshProjection != nil {
			if err := refreshProjection(); err != nil {
				return false, err
			}
		}
	}
	if manifest.ReviewState == "findings-queued" && manifest.ReviewHead == head {
		return returnReviewFindings(ctx, runtimeState, boundary, attempt, manifest, head, manifest.ReviewFindings, humanInstructions, cfg.Commands.Implementation)
	}
	reviewBase := manifest.BaseSHA
	if preflightObjectID.MatchString(issue.BaseSHA) {
		if issue.BaseSHA != manifest.BaseSHA && scanGit(ctx, root, nil, []string{"merge-base", "--is-ancestor", issue.BaseSHA, head}, nil) != nil {
			findings := []string{fmt.Sprintf("Integrate current `%s` at exact commit `%s`, resolve conflicts, and rerun the relevant validation.", issue.BaseBranch, issue.BaseSHA)}
			queued, err := runtimeState.RecordReviewFindings(attempt, head, findings, false, false)
			if err != nil {
				return false, err
			}
			return returnReviewFindings(ctx, runtimeState, boundary, attempt, queued, head, findings, humanInstructions, cfg.Commands.Implementation)
		}
		reviewBase = issue.BaseSHA
	}
	if manifest.ReviewState != "clean" || manifest.ReviewBase != reviewBase || manifest.ReviewHead != head {
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
		return returnReviewFindings(ctx, runtimeState, boundary, attempt, queued, head, review.Findings, humanInstructions, cfg.Commands.Implementation)
	}
	if manifest.ReviewState != "clean" {
		if _, err := runtimeState.RecordReview(attempt, "clean", reviewBase, head, "", ""); err != nil {
			return false, err
		}
		if refreshProjection != nil {
			if err := refreshProjection(); err != nil {
				return false, err
			}
		}
	}
	result.Validation = fmt.Sprintf("Independent review passed for exact head `%s`.", head)
	run := func(dir string, args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, "git", append([]string{"--no-optional-locks", "-c", "core.hooksPath=/dev/null", "-c", "credential.helper=", "-c", "credential.helper=!gh auth git-credential", "-C", dir}, args...)...).CombinedOutput()
		return strings.TrimSpace(string(out)), err
	}
	if preparePublication != nil {
		if err := preparePublication(head); err != nil {
			return false, err
		}
	}
	if _, err := run(root, "push", "origin", "FETCH_HEAD:refs/heads/"+manifest.Branch); err != nil {
		return false, fmt.Errorf("publish verified worker head: %w", err)
	}
	body, err := internalgithub.PullRequestBody(issue.Issue, issue.Attempt, result.Validation, result.Documentation, result.Decisions)
	if err != nil {
		return false, err
	}
	mutation := internalgithub.Mutation{Issue: issue.Issue, Attempt: issue.Attempt}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return false, err
	}
	pr, currentBody, err := internalgithub.FindPublishedAttempt(ctx, api, issue.Repository, manifest.Branch, head, user.ID)
	if err != nil {
		return false, err
	}
	if pr.Number == 0 {
		pr, err = api.CreatePullRequest(ctx, issue.Repository, issue.Title, manifest.Branch, issue.BaseBranch, body, mutation)
		if err != nil {
			// An ambiguous create is recovered by deterministic head lookup.
			pr, currentBody, _ = internalgithub.FindPublishedAttempt(ctx, api, issue.Repository, manifest.Branch, head, user.ID)
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
		fresh, freshBody, findErr := internalgithub.FindPublishedAttempt(ctx, api, issue.Repository, manifest.Branch, head, user.ID)
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
	if err := api.EnsureEvidence(ctx, issue.Repository, issue.Issue, issue.Attempt, head, user.ID); err != nil {
		return false, err
	}
	marker, _ := internalgithub.AttemptMarker(issue.Issue, issue.Attempt, manifest.Branch, head, pr.Number, "review")
	comment, _ := internalgithub.AttributedBody(issue.Issue, issue.Attempt, "Attempt published for review.")
	present, err := internalgithub.HasAttemptComment(ctx, api, issue.Repository, issue.Issue, marker, user.ID)
	if err != nil {
		return false, err
	}
	if present {
		return false, nil
	}
	return false, api.CreateIssueComment(ctx, issue.Repository, issue.Issue, comment+"\n\n"+marker, mutation)
}

func returnReviewFindings(ctx context.Context, runtimeState *agentruntime.Runtime, boundary workerBoundaryRunner, attempt agentruntime.Attempt, manifest agentruntime.Manifest, head string, findings, humanInstructions, command []string) (bool, error) {
	key := "independent-review-" + head
	outcomePath := handoffReceiptPath(manifest.Worktree, key)
	handoff, _ := json.Marshal(struct {
		Type, Key, Findings string
		HumanInstructions   []string `json:"human_instructions,omitempty"`
	}{"agent-symphony-handoff-v1", key, strings.Join(findings, "\n"), humanInstructions})
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
		var ack handoffReceipt
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
	return runtimeState.RecordReview(attempt, manifest.ReviewState, manifest.ReviewBase, manifest.ReviewHead, "", "")
}

func reviewIdentity(attempt agentruntime.Attempt, snapshotRoot string) (string, string) {
	repository := internalgithub.RepositoryIdentifier(attempt.Repository)
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, attempt.Repository, attempt.Issue, attempt.Number)
	return filepath.Join(snapshotRoot, fmt.Sprintf("%s-%d-%d", repository, attempt.Issue, attempt.Number)), session
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
	return fmt.Sprintf("Review only the exact attested change at %s..HEAD for issue #%d attempt %d. Inspect that entire diff and its affected code for correctness, regressions, security, and missing behavioral tests. Use the installed gh CLI to post direct status on the issue or pull request as one unedited `/agent-symphony status needs-attention: REASON` or `/agent-symphony status clear: REASON` comment; pair it with adding or removing the bound issue's `needs-attention` label. A nonempty reason and a fresh re-read of both comment and label are required before reporting the status changed. Authentication, authorization, or partial-update errors are failures, never success. Make the entire final response exactly one bounded JSON object on stdout: {\"type\":\"agent-symphony-review-v1\",\"status\":\"clean\",\"findings\":[]} or status findings with actionable finding strings. Do not wrap it in Markdown, emit prose, or emit another object.\n\n%s", base, issue.Issue, issue.Attempt, issue.Body)
}

func runIndependentReview(ctx context.Context, runtimeState *agentruntime.Runtime, attempt agentruntime.Attempt, boundary boundaryCaller, env, command []string, issue internalgithub.RecoveryIssueFact, manifest agentruntime.Manifest, source, head, snapshotRoot string) (independentReviewResult, bool, error) {
	if len(command) == 0 {
		return independentReviewResult{}, false, errors.New("reviewer command is missing")
	}
	env = append(slices.Clone(env), "GH_REPO="+issue.Repository)
	reviewBase := attempt.BaseSHA
	if preflightObjectID.MatchString(issue.BaseSHA) {
		reviewBase = issue.BaseSHA
	}
	if !preflightObjectID.MatchString(reviewBase) {
		return independentReviewResult{}, false, errors.New("review base is missing or invalid")
	}
	snapshot, session := reviewIdentity(attempt, snapshotRoot)
	if manifest.ReviewState == "running" && manifest.ReviewBase == reviewBase && manifest.ReviewHead == head && manifest.ReviewSnapshot == snapshot && manifest.ReviewSession == session {
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
						persisted, err = runtimeState.RecordReview(attempt, "clean", reviewBase, head, snapshot, session)
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
		if _, err := runtimeState.RecordReview(attempt, "preparing", reviewBase, head, snapshot, session); err != nil {
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
	if err := scanGit(ctx, snapshot, nil, []string{"merge-base", "--is-ancestor", reviewBase, "HEAD"}, nil); err != nil || strings.EqualFold(reviewBase, head) {
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
	args := agentruntime.TmuxNewSessionArgs(session, snapshot, env)
	if _, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: args, Dir: snapshot, Env: env}); err != nil {
		return independentReviewResult{}, false, err
	}
	if _, err := boundary.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"set-option", "-w", "-t", agentruntime.PaneTarget(session), "remain-on-exit", "on"}, Dir: snapshot, Env: env}); err != nil {
		return independentReviewResult{}, false, err
	}
	prompt := reviewPrompt(issue, reviewBase)
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
		if _, err := runtimeState.RecordReview(attempt, "running", reviewBase, head, snapshot, session); err != nil {
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
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	present, err := internalgithub.HasAttemptComment(ctx, api, issue.Repository, issue.Issue, marker, user.ID)
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

func recoverDashboardAttempt(ctx context.Context, configPath, statePath, stateRoot string, issueNumber, attemptNumber int) error {
	if issueNumber < 1 || attemptNumber < 1 {
		return errors.New("invalid recovery attempt")
	}
	statuses, err := reconcileGitHubRun(ctx, configPath, statePath, stateRoot, false)
	if err != nil {
		return err
	}
	c, err := config.Load(configPath)
	if err != nil {
		return err
	}
	matches := slices.DeleteFunc(slices.Clone(statuses), func(status orchestrator.RecoveryStatus) bool {
		return status.Repository != c.Repository || status.Issue != issueNumber || status.Attempt != attemptNumber
	})
	if len(matches) != 1 || !matches[0].Retryable || matches[0].PR > 0 || (matches[0].State != "failed" && matches[0].State != "blocked") {
		return errors.New("fresh authoritative state does not permit recovery")
	}
	status := matches[0]
	if status.State == "blocked" && !slices.Equal(status.Blockers, []string{"runtime liveness mismatch"}) {
		return errors.New("only an exact runtime liveness mismatch can be recovered")
	}
	boundary := implementationBoundary(stateRoot)
	runtimeState := &agentruntime.Runtime{Root: productionAttemptRoot(stateRoot), StateRoot: stateRoot, Runner: boundary}
	manifests, err := runtimeState.Discover()
	if err != nil {
		return err
	}
	local := slices.DeleteFunc(slices.Clone(manifests), func(manifest agentruntime.Manifest) bool {
		return manifest.Repository != c.Repository || manifest.Issue != issueNumber || manifest.Attempt != attemptNumber
	})
	if len(local) != 1 || local[0].Branch != status.Branch || local[0].Worktree != status.Worktree || local[0].Session != status.Session {
		return errors.New("local attempt identity no longer matches the fresh projection")
	}
	manifest := local[0]
	api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return err
	}
	prConfig := internalgithub.PRAdapterConfig{Repository: c.Repository, CancelCommand: "/agent-symphony cancel", RetryCommand: "/agent-symphony retry", ActorID: user.ID}
	if status.State == "blocked" {
		manifest, err = runtimeState.Cancel(ctx, agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}, "dashboard recovery: "+status.Diagnostic)
		if err != nil {
			return err
		}
		issue := internalgithub.RecoveryIssueFact{Repository: c.Repository, Issue: issueNumber, Attempt: attemptNumber}
		if err := durableAttemptFailure(ctx, api, issue, manifest, errors.New(status.Diagnostic)); err != nil {
			return err
		}
	}
	return internalgithub.EnsureRetryCommand(ctx, api, prConfig, issueNumber, attemptNumber)
}

func monitorAttempts(ctx context.Context, runtime *agentruntime.Runtime, statuses []orchestrator.RecoveryStatus, manifests []agentruntime.Manifest, issues []internalgithub.RecoveryIssueFact) error {
	for _, status := range statuses {
		if status.State != "active" && status.State != "review-ready" && !(status.State == "blocked" && slices.Equal(status.Blockers, []string{"runtime liveness mismatch"})) {
			continue
		}
		manifestIndex := slices.IndexFunc(manifests, func(manifest agentruntime.Manifest) bool {
			return manifest.Repository == status.Repository && manifest.Issue == status.Issue && manifest.Attempt == status.Attempt && manifest.State == "running"
		})
		if manifestIndex < 0 {
			continue
		}
		manifest := manifests[manifestIndex]
		authorized := slices.ContainsFunc(issues, func(issue internalgithub.RecoveryIssueFact) bool {
			return issue.Repository == status.Repository && issue.Issue == status.Issue && issue.DispatchAuthorized
		})
		_, err := runtime.Monitor(ctx, agentruntime.Attempt{Repository: status.Repository, Issue: status.Issue, Number: status.Attempt, BaseSHA: manifest.BaseSHA, Eligible: func() bool { return authorized }})
		if err != nil {
			return fmt.Errorf("monitor %s#%d attempt %d: %w", status.Repository, status.Issue, status.Attempt, err)
		}
	}
	return nil
}

func fetchOperatorMessages(ctx context.Context, api internalgithub.API, cfg internalgithub.PRAdapterConfig, issues []internalgithub.RecoveryIssueFact, attempts []internalgithub.RecoveryAttemptFact, manifests []agentruntime.Manifest) (map[string][]internalgithub.OperatorMessage, error) {
	result := map[string][]internalgithub.OperatorMessage{}
	seen := map[int]bool{}
	fetch := func(repository string, issue int) error {
		if repository != cfg.Repository || seen[issue] {
			return nil
		}
		seen[issue] = true
		messages, err := internalgithub.FetchOperatorMessages(ctx, api, cfg, issue)
		if err != nil {
			return err
		}
		for _, message := range messages {
			key := fmt.Sprintf("%s#%d/%d", message.Repository, message.Issue, message.Attempt)
			result[key] = append(result[key], message)
		}
		return nil
	}
	for _, issue := range issues {
		if err := fetch(issue.Repository, issue.Issue); err != nil {
			return nil, err
		}
	}
	for _, attempt := range attempts {
		if err := fetch(attempt.Repository, attempt.Issue); err != nil {
			return nil, err
		}
	}
	for _, manifest := range manifests {
		if err := fetch(manifest.Repository, manifest.Issue); err != nil {
			return nil, err
		}
	}
	return result, nil
}

func attachOperatorMessageStatuses(statuses []orchestrator.RecoveryStatus, messages map[string][]internalgithub.OperatorMessage) {
	for i := range statuses {
		statuses[i].OperatorMessages = nil
		key := fmt.Sprintf("%s#%d/%d", statuses[i].Repository, statuses[i].Issue, statuses[i].Attempt)
		for _, message := range messages[key] {
			state := message.State
			if state == "claimed" {
				state = "queued"
			}
			statuses[i].OperatorMessages = append(statuses[i].OperatorMessages, orchestrator.OperatorMessageStatus{ID: message.ID, State: state, UpdatedAt: message.UpdatedAt, Diagnostic: message.Diagnostic})
		}
	}
}

type operatorTargetRefresh func(context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error)
type operatorAcceptanceCheck func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error)

func resumeOperatorMessages(ctx context.Context, api internalgithub.API, cfg internalgithub.PRAdapterConfig, runtimeState *agentruntime.Runtime, boundary boundaryCaller, issues []internalgithub.RecoveryIssueFact, remote []internalgithub.RecoveryAttemptFact, manifests []agentruntime.Manifest, messages map[string][]internalgithub.OperatorMessage, command []string, check orchestrator.RuntimeCheck, refresh operatorTargetRefresh, accept operatorAcceptanceCheck) error {
	if len(command) == 0 {
		return errors.New("implementation command is missing")
	}
	started := map[string]bool{}
	for key, queued := range messages {
		for index := range queued {
			message := &queued[index]
			if message.State != "queued" && message.State != "claimed" {
				continue
			}
			remoteIndex := slices.IndexFunc(remote, func(fact internalgithub.RecoveryAttemptFact) bool {
				return fact.Repository == message.Repository && fact.Issue == message.Issue && fact.Attempt == message.Attempt
			})
			if remoteIndex >= 0 && remote[remoteIndex].State == "completed" {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "attempt completed or merged before delivery"); err != nil {
					return err
				}
				continue
			}
			issueIndex := slices.IndexFunc(issues, func(issue internalgithub.RecoveryIssueFact) bool {
				return issue.Repository == message.Repository && issue.Issue == message.Issue
			})
			if issueIndex < 0 || issues[issueIndex].Cancelled || issues[issueIndex].Completed || !issues[issueIndex].DispatchAuthorized {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "attempt is no longer active and authorized"); err != nil {
					return err
				}
				continue
			}
			manifestIndex := slices.IndexFunc(manifests, func(manifest agentruntime.Manifest) bool {
				return manifest.Repository == message.Repository && manifest.Issue == message.Issue && manifest.Attempt == message.Attempt
			})
			handoffKey := "operator-message-" + message.ID
			if manifestIndex >= 0 && runtimeState != nil {
				recovered, changed, err := runtimeState.RecoverInterruptedHandoff(ctx, manifests[manifestIndex], handoffKey)
				if err != nil {
					return err
				}
				if changed {
					manifests[manifestIndex] = recovered
				}
			}
			if manifestIndex >= 0 && manifests[manifestIndex].State == "completed" && (manifests[manifestIndex].ReviewState == "preparing" || manifests[manifestIndex].ReviewState == "running") {
				continue
			}
			proposal := orchestratoragent.MessageProposal{Version: 1, Repository: message.Repository, Issue: message.Issue, Attempt: message.Attempt, Message: message.Message}
			ownershipCheck := check
			if runtimeState != nil {
				ownershipCheck = func(ctx context.Context, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
					if manifest.State == "running" {
						if err := runtimeState.VerifyOwned(ctx, manifest); err == nil {
							return nil
						}
					}
					if check != nil {
						return check(ctx, manifest, fact)
					}
					return verifyOperatorMessageRuntime(ctx, runtimeState, manifest, fact)
				}
			} else if ownershipCheck == nil {
				ownershipCheck = func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
			}
			if err := validateOperatorMessageTarget(ctx, proposal, issues, remote, manifests, ownershipCheck); err != nil {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "exact verified attempt ownership is missing"); err != nil {
					return err
				}
				continue
			}
			if manifestIndex < 0 {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "exact attempt runtime is missing"); err != nil {
					return err
				}
				continue
			}
			manifest := manifests[manifestIndex]
			outcomePath := handoffReceiptPath(manifest.Worktree, handoffKey)
			outcomeToken := fmt.Sprintf("%x", sha256.Sum256([]byte("handoff-outcome\x00"+handoffKey)))
			expectedReceipt := handoffReceipt{"agent-symphony-handoff-executed-v1", handoffKey, outcomePath, outcomeToken}
			payload, _ := json.Marshal(struct {
				Type, Key, Kind, Repository, Message string
				Issue, Attempt                       int
			}{"agent-symphony-handoff-v1", handoffKey, "operator-message", message.Repository, message.Message, message.Issue, message.Attempt})
			requestBody := func(manifest agentruntime.Manifest) []byte {
				request, _ := json.Marshal(handoffRequest{manifest, payload, outcomePath, outcomeToken, command})
				return request
			}
			verifyDelivery := func(manifest agentruntime.Manifest) (bool, error) {
				if message.State != "claimed" {
					return false, nil
				}
				observed, verifyErr := boundary.call(ctx, "verify-handoff", agentruntime.Command{Stdin: bytes.NewReader(requestBody(manifest))})
				if verifyErr != nil {
					return false, fmt.Errorf("verify claimed operator message delivery: %w", verifyErr)
				}
				if observed.Output != "" {
					var receipt handoffReceipt
					decoder := json.NewDecoder(strings.NewReader(observed.Output))
					decoder.DisallowUnknownFields()
					if decoder.Decode(&receipt) != nil || decoder.Decode(&struct{}{}) != io.EOF || receipt != expectedReceipt {
						return false, errors.New("verified operator message receipt binding mismatch")
					}
					return true, nil
				}
				return false, nil
			}
			switch manifest.State {
			case "preparing":
				continue // The accepted message remains durably queued.
			case "running":
				if message.State == "queued" {
					if runtimeState == nil {
						continue
					}
					superseded, err := runtimeState.InterruptLegacyOperatorHandoff(ctx, manifest, 2*time.Minute)
					if err != nil {
						return err
					}
					if superseded {
						break
					}
					monitored, err := runtimeState.Monitor(ctx, agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA})
					if err != nil {
						return err
					}
					manifests[manifestIndex], manifest = monitored, monitored
					switch monitored.State {
					case "running":
						continue
					case "failed":
						if err := recordOperatorOutcome(ctx, api, cfg, message, "failed", "attempt runtime failed before delivery"); err != nil {
							return err
						}
						continue
					case "cancelled":
						if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "attempt was cancelled before delivery"); err != nil {
							return err
						}
						continue
					case "completed":
					default:
						return errors.New("monitored operator message runtime state is invalid")
					}
					break
				}
				verified, verifyErr := verifyDelivery(manifest)
				if verifyErr != nil {
					return verifyErr
				}
				if !verified {
					continue
				}
				if err := recordOperatorOutcome(ctx, api, cfg, message, "delivered", ""); err != nil {
					return err
				}
				started[key] = true
				continue
			case "failed":
				if err := recordOperatorOutcome(ctx, api, cfg, message, "failed", "attempt runtime failed before delivery"); err != nil {
					return err
				}
				continue
			case "cancelled":
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "attempt was cancelled before delivery"); err != nil {
					return err
				}
				continue
			case "completed":
				if manifest.ReviewState == "preparing" || manifest.ReviewState == "running" {
					continue
				}
			default:
				if err := recordOperatorOutcome(ctx, api, cfg, message, "failed", "attempt runtime state is invalid"); err != nil {
					return err
				}
				continue
			}
			if started[key] {
				continue
			}
			if message.State == "queued" {
				claimed, err := internalgithub.RecordOperatorMessageClaim(ctx, api, cfg, *message)
				if err != nil {
					return err
				}
				*message = claimed
			}
			if refresh == nil {
				return errors.New("authoritative operator message target refresh is unavailable")
			}
			currentIssues, currentRemote, currentManifests, err := refresh(ctx)
			if err != nil {
				return err
			}
			if err := validateOperatorMessageTarget(ctx, proposal, currentIssues, currentRemote, currentManifests, check); err != nil {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "attempt became terminal or lost exact ownership before delivery"); err != nil {
					return err
				}
				continue
			}
			expectedBinding, err := operatorMessageBinding(proposal, currentIssues, currentRemote)
			if err != nil {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "attempt became terminal or lost exact ownership before delivery"); err != nil {
					return err
				}
				continue
			}
			manifestIndex = slices.IndexFunc(currentManifests, func(candidate agentruntime.Manifest) bool {
				return candidate.Repository == message.Repository && candidate.Issue == message.Issue && candidate.Attempt == message.Attempt
			})
			manifest = currentManifests[manifestIndex]
			// ResumeHandoff owns the final authoritative proof so terminal cleanup
			// wins without first changing the retained manifest back to running.
			if accept == nil {
				return errors.New("authoritative operator message acceptance check is unavailable")
			}
			var authorized bool
			var acceptanceErr error
			resumed, err := runtimeState.ResumeHandoff(ctx, agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA, Eligible: func() bool {
				authorized, acceptanceErr = accept(ctx, proposal, expectedBinding)
				return acceptanceErr == nil && authorized
			}})
			if acceptanceErr != nil {
				return acceptanceErr
			}
			if !authorized {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "rejected", "attempt became terminal or lost exact ownership at delivery acceptance"); err != nil {
					return err
				}
				continue
			}
			if err != nil {
				return err
			}
			verified, verifyErr := verifyDelivery(resumed)
			if verifyErr != nil {
				return verifyErr
			}
			if verified {
				if err := recordOperatorOutcome(ctx, api, cfg, message, "delivered", ""); err != nil {
					return err
				}
				started[key] = true
				continue
			}
			accepted, err := boundary.call(ctx, "accept-operator-handoff", agentruntime.Command{Stdin: bytes.NewReader(requestBody(resumed))})
			if err != nil {
				return fmt.Errorf("worker-owned operator message acceptance: %w", err)
			}
			var ack handoffReceipt
			decoder := json.NewDecoder(strings.NewReader(accepted.Output))
			decoder.DisallowUnknownFields()
			if decoder.Decode(&ack) != nil || decoder.Decode(&struct{}{}) != io.EOF || ack.Type != "agent-symphony-handoff-executed-v1" || ack.Key != handoffKey || ack.OutcomePath != outcomePath || ack.OutcomeToken != outcomeToken {
				return errors.New("worker-owned operator message acceptance binding mismatch")
			}
			if err := recordOperatorOutcome(ctx, api, cfg, message, "delivered", ""); err != nil {
				return err
			}
			started[key] = true
		}
		messages[key] = queued
	}
	return nil
}

func recordOperatorOutcome(ctx context.Context, api internalgithub.API, cfg internalgithub.PRAdapterConfig, message *internalgithub.OperatorMessage, state, diagnostic string) error {
	if err := internalgithub.RecordOperatorMessageOutcome(ctx, api, cfg, *message, state, diagnostic); err != nil {
		return err
	}
	message.State, message.Diagnostic = state, diagnostic
	return nil
}

func resumeHandoffs(ctx context.Context, runtimeState *agentruntime.Runtime, boundary boundaryCaller, statePath, stateRoot string, statuses []orchestrator.RecoveryStatus, manifests []agentruntime.Manifest, command []string) error {
	info, err := os.Lstat(statePath)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("safe durable handoff state is unavailable")
	}
	if len(command) == 0 {
		return errors.New("implementation command is missing")
	}
	live := map[string]agentruntime.Manifest{}
	for _, status := range statuses {
		if (status.State == "active" || status.State == "review-ready") && status.DispatchAuthorized && len(status.Blockers) == 0 {
			for _, manifest := range manifests {
				if manifest.Repository == status.Repository && manifest.Issue == status.Issue && manifest.Attempt == status.Attempt && (manifest.State == "running" || manifest.State == "completed") {
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
		if runtimeState != nil {
			manifest, err = runtimeState.ResumeHandoff(ctx, agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA})
			if err != nil {
				return err
			}
		}
		payload, _ := json.Marshal(struct {
			Type, Key  string
			PR         int
			HeadSHA    string
			Validation bool
			Feedback   []internalgithub.Feedback
		}{"agent-symphony-handoff-v1", handoff.Key, handoff.PR, handoff.HeadSHA, handoff.Validation, handoff.Feedback})
		manifestBody, _ := json.Marshal(manifest)
		outcomePath := handoffReceiptPath(manifest.Worktree, handoff.Key)
		outcomeToken := fmt.Sprintf("%x", sha256.Sum256([]byte("handoff-outcome\x00"+handoff.Key)))
		request, _ := json.Marshal(struct {
			Manifest     json.RawMessage `json:"manifest"`
			Handoff      json.RawMessage `json:"handoff"`
			OutcomePath  string          `json:"outcome_path"`
			OutcomeToken string          `json:"outcome_token"`
			Command      []string        `json:"command"`
		}{manifestBody, payload, outcomePath, outcomeToken, command})
		accepted, err := boundary.call(ctx, "accept-handoff", agentruntime.Command{Stdin: bytes.NewReader(request)})
		var ack handoffReceipt
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
	path, err := exec.LookPath("gh")
	if err != nil {
		return []diagnostic{{"GitHub CLI", "fail", "gh was not found", "Install GitHub CLI and run gh auth login."}}
	}
	version, err := exec.CommandContext(ctx, path, "--version").Output()
	if err != nil {
		return []diagnostic{{"GitHub CLI", "fail", internalgithub.Redact(err.Error()), "Repair GitHub CLI and run gh auth login."}}
	}
	api := internalgithub.API{BaseURL: githubAPI, HTTP: githubClient}
	user, err := api.AuthenticatedUser(ctx)
	if err != nil {
		return []diagnostic{{"GitHub CLI", "fail", internalgithub.Redact(err.Error()), "Run gh auth login and retry."}}
	}
	if err := api.VerifyRepository(ctx, repository); err != nil {
		return []diagnostic{{"GitHub connectivity", "fail", internalgithub.Redact(err.Error()), "Grant the authenticated gh account access to " + repository + "."}}
	}
	var body struct {
		Permissions map[string]bool `json:"permissions"`
	}
	if _, _, err := api.Read(ctx, "/repos/"+repository, "", &body); err != nil {
		return []diagnostic{{"GitHub permissions", "fail", internalgithub.Redact(err.Error()), "Grant the authenticated gh account repository access."}}
	}
	var granted []string
	for _, name := range []string{"admin", "maintain", "push", "triage", "pull"} {
		if body.Permissions[name] {
			granted = append(granted, name)
		}
	}
	return []diagnostic{
		{"GitHub CLI", "pass", strings.Split(strings.TrimSpace(string(version)), "\n")[0] + "; authenticated as " + user.Login, ""},
		{"GitHub connectivity", "pass", "connected to " + repository, ""},
		{"GitHub permissions", "pass", "effective repository access: " + strings.Join(granted, ", "), ""},
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
	agent-host    execute the implementation, review, or orchestrator boundary
	init          create .agent-symphony.yaml with project defaults
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
	help          show this help

options:
	--config path use another configuration file
	--state path  durable PR-governance/handoff state
	--runtime-state path  bounded runtime manifest root
	--attempts path  offline authoritative attempt facts
	--issue number  issue to inspect
	--interval duration  override configured serve reconciliation interval (maximum 60s)
	--dashboard-address address  dashboard listen address (serve only; loopback by default)
	--allow-unsafe-dashboard-network  permit non-loopback dashboard binding (requires password)
	--dashboard-password-file path  coordinator-only HTTP Basic password file; username is agent-symphony
	--offline     skip the GitHub probe in doctor or diagnostics
	--coordinator user  coordinator OS user for install-host
	--json        emit a versioned JSON envelope
	-h, --help    show this help (top level only)
	--version     show the release version`)
}
