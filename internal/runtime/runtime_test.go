package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

type fakeRunner struct {
	mu              sync.Mutex
	sessions        map[string]*fakeSession
	buffers         map[string]string
	fail            string
	failOutput      string
	failErr         error
	sessionAuth     bool
	validAuth       string
	failCode        map[string]int
	ignoreInterrupt bool
	keepAfterKill   bool
	seen            []Command
}

type fakeSession struct {
	dead, stopped bool
	status        int
	output        string
	context       string
	agent         []string
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{sessions: map[string]*fakeSession{}, buffers: map[string]string{}, failCode: map[string]int{}}
}

func TestExecRunnerBoundsOutputToTail(t *testing.T) {
	result, err := (ExecRunner{}).Run(t.Context(), Command{
		Name:           "sh",
		Args:           []string{"-c", "printf prefix; printf '%*s' 1048576 ''; printf tail"},
		MaxOutputBytes: 128,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Output) != 128 || !strings.HasSuffix(result.Output, "tail") || strings.Contains(result.Output, "prefix") {
		t.Fatalf("bounded output length=%d suffix=%q", len(result.Output), result.Output[len(result.Output)-8:])
	}
}

func TestExecRunnerCapturesConcurrentStdoutAndStderr(t *testing.T) {
	result, err := (ExecRunner{}).Run(t.Context(), Command{
		Name:           "sh",
		Args:           []string{"-c", `(i=0; while [ "$i" -lt 1000 ]; do printf 'stdout-%04d\n' "$i"; i=$((i+1)); done) & (i=0; while [ "$i" -lt 1000 ]; do printf 'stderr-%04d\n' "$i" >&2; i=$((i+1)); done) & wait`},
		MaxOutputBytes: 64 << 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(result.Output, "stdout-") || !strings.Contains(result.Output, "stderr-") {
		t.Fatalf("concurrent capture omitted a stream: %q", result.Output)
	}
}

func (f *fakeRunner) Run(ctx context.Context, command Command) (Result, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.seen = append(f.seen, command)
	if command.Name == "git" {
		cmd := exec.CommandContext(ctx, command.Name, command.Args...)
		cmd.Dir, cmd.Env, cmd.Stdin = command.Dir, command.Env, command.Stdin
		out, err := cmd.CombinedOutput()
		result := Result{Output: string(out)}
		var exit *exec.ExitError
		if errors.As(err, &exit) {
			result.Code, result.Exited = exit.ExitCode(), true
		}
		return result, err
	}
	if len(command.Args) == 0 {
		return Result{}, errors.New("missing tmux operation")
	}
	args := command.Args
	if offset := slices.Index(args, ";"); offset >= 0 && offset+1 < len(args) {
		args = args[offset+1:]
	}
	op := args[0]
	if f.fail == op {
		output, err := f.failOutput, f.failErr
		if output == "" {
			output = "canary failure detail"
		}
		if err == nil {
			err = errors.New("fake failure")
		}
		return Result{Output: output}, err
	}
	if code, ok := f.failCode[op]; ok {
		return Result{Output: "canary failure detail", Code: code, Exited: true}, errors.New("fake exit")
	}
	session := valueAfter(args, "-s")
	if session == "" {
		target := valueAfter(args, "-t")
		if target != "" && !strings.HasPrefix(target, "=") {
			return Result{}, errors.New("inexact tmux target")
		}
		session = strings.TrimPrefix(target, "=")
		session = strings.TrimSuffix(session, ":0.0")
	}
	switch op {
	case "has-session":
		if _, ok := f.sessions[session]; !ok {
			return Result{Code: 1, Exited: true}, errors.New("missing session")
		}
	case "new-session":
		if f.sessionAuth {
			token := environmentValue(command.Env, "GH_TOKEN")
			if token == "" {
				return Result{}, errors.New("GitHub CLI authentication is missing")
			}
			if token != f.validAuth {
				return Result{Output: "GitHub CLI authentication failed " + token}, errors.New("GitHub CLI authentication failed " + token)
			}
		}
		if _, ok := f.sessions[session]; ok {
			return Result{}, errors.New("existing session")
		}
		f.sessions[session] = &fakeSession{}
	case "set-option":
		if (slices.Contains(args, "remain-on-exit") || slices.Contains(args, "history-limit")) && !slices.Contains(args, "-w") {
			return Result{}, errors.New("window option missing -w")
		}
	case "respawn-pane":
		separator := slices.Index(args, "--")
		if separator < 0 || separator == len(args)-1 {
			return Result{}, errors.New("missing respawn command separator")
		}
		f.sessions[session].agent = slices.Clone(args[separator+1:])
		if slices.Contains(f.sessions[session].agent, "fast-exit") {
			f.sessions[session].dead, f.sessions[session].status = true, 42
		}
	case "load-buffer":
		b, _ := io.ReadAll(command.Stdin)
		f.buffers[valueAfter(args, "-b")] = string(b)
	case "paste-buffer":
		f.sessions[session].context = f.buffers[valueAfter(args, "-b")]
	case "display-message":
		s := f.sessions[session]
		if s == nil {
			return Result{}, errors.New("missing session")
		}
		if slices.Contains(args, "#{pane_start_command}") {
			return Result{Output: strings.Join(s.agent, " ")}, nil
		}
		if slices.Contains(args, "#{pane_dead} #{pane_dead_status}") {
			if s.dead {
				return Result{Output: "1 " + strconv.Itoa(s.status)}, nil
			}
			return Result{Output: "0 \n"}, nil
		}
		if s.dead {
			return Result{Output: "1"}, nil
		}
		return Result{Output: "0"}, nil
	case "capture-pane":
		return Result{Output: f.sessions[session].output}, nil
	case "send-keys":
		if slices.Contains(args, "C-c") && !f.ignoreInterrupt {
			f.sessions[session].dead, f.sessions[session].stopped = true, true
		}
	case "kill-session":
		if !f.keepAfterKill {
			delete(f.sessions, session)
		}
	}
	return Result{}, nil
}

func environmentValue(environment []string, name string) string {
	for _, entry := range environment {
		if key, value, ok := strings.Cut(entry, "="); ok && key == name {
			return value
		}
	}
	return ""
}

func TestParsePaneStatus(t *testing.T) {
	for _, test := range []struct {
		name    string
		output  string
		dead    bool
		status  int
		wantErr string
	}{
		{name: "live blank status", output: "0 \n"},
		{name: "dead zero", output: "1 0\n", dead: true},
		{name: "dead nonzero", output: "1 127\n", dead: true, status: 127},
		{name: "live with status", output: "0 0\n", wantErr: "invalid pane status"},
		{name: "dead blank status", output: "1 \n", wantErr: "invalid pane status"},
		{name: "negative exit", output: "1 -1\n", wantErr: "invalid exit status"},
		{name: "unknown state", output: "2 0\n", wantErr: "invalid pane status"},
		{name: "empty", wantErr: "invalid pane status"},
	} {
		t.Run(test.name, func(t *testing.T) {
			dead, status, err := ParsePaneStatus(test.output)
			if dead != test.dead || status != test.status || test.wantErr == "" && err != nil || test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("ParsePaneStatus(%q) = %v, %d, %v", test.output, dead, status, err)
			}
		})
	}
}

func TestInterruptLegacyOperatorHandoffStopsOnlyTheRetiredResumeCommand(t *testing.T) {
	for _, test := range []struct {
		name    string
		command []string
		want    bool
	}{
		{"old resume", []string{"agent-symphony", "worker-capture-handoff", "--", "codex", "exec", "resume", "--last", "--dangerously-bypass-approvals-and-sandbox", "-"}, true},
		{"old fresh", []string{"agent-symphony", "worker-capture-handoff", "--", "codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"}, true},
		{"ready protocol", []string{"agent-symphony", "worker-capture-handoff-ready", "--", "codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, fake, attempt, _ := testRuntime(t)
			manifest, err := r.PrepareAndStart(t.Context(), attempt)
			if err != nil {
				t.Fatal(err)
			}
			manifest.UpdatedAt = time.Now().Add(-3 * time.Minute)
			oldSession := fake.sessions[manifest.Session]
			oldSession.agent = test.command
			got, err := r.InterruptLegacyOperatorHandoff(t.Context(), manifest, 2*time.Minute)
			if err != nil || got != test.want || oldSession.stopped != test.want {
				t.Fatalf("interrupted=%v stopped=%v err=%v", got, oldSession.stopped, err)
			}
			if test.want && (fake.sessions[manifest.Session] == oldSession || fake.sessions[manifest.Session].dead) {
				t.Fatal("retired pane was not replaced with a fresh live session")
			}
		})
	}
}

func TestRecoverInterruptedHandoffRecreatesOnlyUnacknowledgedStartup(t *testing.T) {
	for _, test := range []struct {
		name     string
		missing  bool
		dead     bool
		noMarker bool
		want     bool
	}{
		{name: "missing session", missing: true, want: true},
		{name: "missing before launch marker", missing: true, noMarker: true, want: true},
		{name: "dead ready worker", dead: true, want: true},
		{name: "live ready worker"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, fake, attempt, _ := testRuntime(t)
			manifest, err := r.PrepareAndStart(t.Context(), attempt)
			if err != nil {
				t.Fatal(err)
			}
			key := "operator-message-" + strings.Repeat("a", 64)
			inbox := filepath.Join(manifest.Worktree, ".agent-symphony", "handoffs")
			if err := os.MkdirAll(inbox, 0o700); err != nil {
				t.Fatal(err)
			}
			if !test.noMarker {
				if err := os.WriteFile(filepath.Join(inbox, key+".launching"), []byte("recipient"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(ResultPath(manifest.Worktree), []byte(`{"type":"agent-symphony-result-v1","validation":"retained","documentation":"none"}`), 0o600); err != nil {
				t.Fatal(err)
			}
			old := fake.sessions[manifest.Session]
			old.agent = []string{"agent-symphony", "worker-capture-handoff-ready", "tmux", "buffer", "result", "launched", "recipient", "signal", "--", "codex", "exec", "-"}
			old.dead = test.dead
			if test.missing {
				delete(fake.sessions, manifest.Session)
			}
			got, recovered, err := r.RecoverInterruptedHandoff(t.Context(), manifest, key)
			if err != nil || recovered != test.want {
				t.Fatalf("recovered=%v manifest=%#v err=%v", recovered, got, err)
			}
			if test.want {
				if got.State != "completed" || fake.sessions[manifest.Session] == nil || fake.sessions[manifest.Session] == old {
					t.Fatalf("interrupted startup was not reset: %#v session=%#v", got, fake.sessions[manifest.Session])
				}
				stored, err := readManifest(r.manifestPath(attempt))
				if err != nil || stored.State != "completed" {
					t.Fatalf("recovery was not durable: %#v err=%v", stored, err)
				}
			}
		})
	}
}

func valueAfter(args []string, flag string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

func TestLifecycleCreatesCredentialedSessionWithoutCredentialedRepository(t *testing.T) {
	r, fake, attempt, primary := testRuntime(t)
	before := gitOutput(t, primary, "status", "--porcelain=v1", "--branch")
	t.Setenv("GITHUB_TOKEN", "credential-canary")
	manifest, err := r.PrepareAndStart(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.State != "running" || fake.buffers[manifest.Session] != attempt.Context {
		t.Fatalf("unexpected launch: %#v, %#v", manifest, fake.sessions[manifest.Session])
	}
	want := PromptCommand(r.Helper, "tmux", manifest.Session, ResultPath(manifest.Worktree), attempt.Command)
	if !slices.Equal(fake.sessions[manifest.Session].agent, want) {
		t.Fatalf("agent command = %#v, want %#v", fake.sessions[manifest.Session].agent, want)
	}
	if got := gitOutput(t, manifest.Worktree, "remote"); got != "" {
		t.Fatalf("agent repository has remote %q", got)
	}
	if got := gitOutput(t, manifest.Worktree, "config", "--local", "--get-all", "credential.helper"); got != "" {
		t.Fatalf("credential helper = %q", got)
	}
	if got := gitOutput(t, primary, "status", "--porcelain=v1", "--branch"); got != before {
		t.Fatalf("primary checkout changed: before %q after %q", before, got)
	}
	credentialAvailable, repositoryBound := false, false
	for _, command := range fake.seen {
		if command.Name != "tmux" {
			continue
		}
		args, env := strings.Join(command.Args, " "), strings.Join(command.Env, " ")
		if strings.Contains(args, "credential-canary") {
			t.Fatal("GitHub credential reached tmux argv")
		}
		credentialAvailable = credentialAvailable || strings.Contains(env, "GITHUB_TOKEN=credential-canary")
		repositoryBound = repositoryBound || strings.Contains(env, "GH_REPO=owner/repo")
	}
	if !credentialAvailable || !repositoryBound {
		t.Fatal("GitHub CLI authentication or repository binding was unavailable in the implementation session")
	}
	info, err := os.Stat(r.manifestPath(attempt))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("manifest mode: %v, %v", info, err)
	}
	if _, err := r.PrepareAndStart(context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "already exist") {
		t.Fatalf("duplicate start = %v", err)
	}
	got, err := r.Discover()
	if err != nil || len(got) != 1 || got[0].Session != manifest.Session {
		t.Fatalf("discover = %#v, %v", got, err)
	}
	manifest.Session = "as-tampered"
	if err := r.writeManifest(attempt, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Discover(); err == nil || !strings.Contains(err.Error(), "deterministic") {
		t.Fatalf("tampered manifest discovery = %v", err)
	}
}

func TestInteractiveLifecycleKeepsAgentOnTmuxAndRequiresResult(t *testing.T) {
	for _, test := range []struct {
		name       string
		result     string
		wantState  string
		wantReason string
	}{
		{name: "completed result", result: `{"type":"agent-symphony-result-v1","validation":"go test ./... passed","documentation":"none"}`, wantState: "completed"},
		{name: "missing result", wantState: "failed", wantReason: "valid private result artifact"},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, fake, attempt, _ := testRuntime(t)
			attempt.Command, attempt.Interactive = []string{"interactive-agent", "--tty"}, true
			manifest, err := r.PrepareAndStart(t.Context(), attempt)
			if err != nil {
				t.Fatal(err)
			}
			if !manifest.Interactive || fake.buffers[manifest.Session] != "" || !slices.Equal(fake.sessions[manifest.Session].agent, []string{"interactive-agent", "--tty", attempt.Context}) {
				t.Fatalf("interactive launch manifest=%#v session=%#v buffers=%#v", manifest, fake.sessions[manifest.Session], fake.buffers)
			}
			info, err := os.Lstat(ResultPath(manifest.Worktree))
			if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
				t.Fatalf("result artifact info=%v err=%v", info, err)
			}
			foundEnvironment := false
			for _, command := range fake.seen {
				foundEnvironment = foundEnvironment || environmentValue(command.Env, WorkerResultEnvironment) == ResultPath(manifest.Worktree)
			}
			if !foundEnvironment {
				t.Fatalf("%s was not imported into tmux", WorkerResultEnvironment)
			}
			if test.result != "" {
				if err := os.WriteFile(ResultPath(manifest.Worktree), []byte(test.result), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			fake.sessions[manifest.Session].dead = true
			got, err := r.Monitor(t.Context(), Attempt{Repository: attempt.Repository, Issue: attempt.Issue, Number: attempt.Number, BaseSHA: attempt.BaseSHA})
			if err != nil || got.State != test.wantState || !strings.Contains(got.Diagnostic, test.wantReason) {
				t.Fatalf("monitored manifest=%#v err=%v", got, err)
			}
		})
	}
}

func TestResumeHandoffRefreshesTrustedSourceRefs(t *testing.T) {
	r, fake, attempt, primary := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(primary, "README"), []byte("new source\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, primary, "add", "README")
	runGit(t, primary, "commit", "-qm", "new source")
	want := gitOutput(t, primary, "rev-parse", "HEAD")
	branch := gitOutput(t, primary, "branch", "--show-current")
	fake.sessions[manifest.Session].dead = true
	if _, err := r.Monitor(t.Context(), attempt); err != nil {
		t.Fatal(err)
	}
	resumed, err := r.ResumeHandoff(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != "running" || gitOutput(t, resumed.Worktree, "rev-parse", "refs/remotes/agent-symphony/"+branch) != want {
		t.Fatalf("resumed=%#v source=%s", resumed, want)
	}
	if got := gitOutput(t, resumed.Worktree, "remote"); got != "" {
		t.Fatalf("worker remote=%q", got)
	}
}

func TestResumeHandoffRecreatesMissingSessionBeforeStateTransition(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	if manifest, err = r.Monitor(t.Context(), attempt); err != nil || manifest.State != "completed" {
		t.Fatalf("completed manifest=%#v err=%v", manifest, err)
	}
	delete(fake.sessions, manifest.Session)
	resumed, err := r.ResumeHandoff(t.Context(), attempt)
	if err != nil || resumed.State != "running" || fake.sessions[manifest.Session] == nil {
		t.Fatalf("resumed=%#v session=%#v err=%v", resumed, fake.sessions[manifest.Session], err)
	}
}

func TestValidationTraversalExistingAndLaunchFailureDiagnostics(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	bad := attempt
	bad.Repository = "owner/../repo"
	if _, err := r.PrepareAndStart(context.Background(), bad); err == nil {
		t.Fatal("accepted traversal identity")
	}
	fake.fail = "new-session"
	manifest, err := r.PrepareAndStart(context.Background(), attempt)
	if err == nil || manifest.State != "failed" || !strings.Contains(manifest.Diagnostic, "canary failure detail") {
		t.Fatalf("launch failure = %#v, %v", manifest, err)
	}
	stored, readErr := readManifest(r.manifestPath(attempt))
	if readErr != nil || stored.Diagnostic != manifest.Diagnostic {
		t.Fatalf("stored diagnostic = %#v, %v", stored, readErr)
	}
}

func TestStaleWorkerResultBlocksLaunch(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	identity, err := AttemptIdentity(r.Root, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ResultPath(identity.Worktree), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err == nil || manifest.State != "" || !strings.Contains(err.Error(), "worker result already exists") {
		t.Fatalf("manifest=%#v err=%v", manifest, err)
	}
	if len(fake.sessions) != 0 {
		t.Fatalf("agent launched with stale result: %#v", fake.sessions)
	}
}

func TestAgentFailureCancelAndIneligibility(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	fake.sessions[manifest.Session].status = 7
	fake.sessions[manifest.Session].output = "useful failure output\n"
	recovered := Attempt{Repository: attempt.Repository, Issue: attempt.Issue, Number: attempt.Number, BaseSHA: attempt.BaseSHA}
	manifest, err = r.Monitor(context.Background(), recovered)
	if err != nil || manifest.State != "failed" || !strings.Contains(manifest.Diagnostic, "status 7") {
		t.Fatalf("monitor = %#v, %v", manifest, err)
	}
	if b, _ := os.ReadFile(manifest.LogPath); string(b) != "useful failure output\n" {
		t.Fatalf("log = %q", b)
	}

	r2, fake2, attempt2, _ := testRuntime(t)
	manifest2, err := r2.PrepareAndStart(context.Background(), attempt2)
	if err != nil {
		t.Fatal(err)
	}
	recovered2 := Attempt{Repository: attempt2.Repository, Issue: attempt2.Issue, Number: attempt2.Number, BaseSHA: attempt2.BaseSHA}
	manifest2, err = r2.Cancel(context.Background(), recovered2, "issue closed")
	if _, live := fake2.sessions[manifest2.Session]; err != nil || manifest2.State != "cancelled" || live {
		t.Fatalf("cancel = %#v, %v", manifest2, err)
	}

	r3, _, attempt3, _ := testRuntime(t)
	attempt3.Eligible = func() bool { return false }
	if _, err := r3.PrepareAndStart(context.Background(), attempt3); err == nil || !strings.Contains(err.Error(), "eligible") {
		t.Fatalf("ineligible = %v", err)
	}
}

func TestMonitorStopsAttemptThatBecomesIneligible(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	eligible := true
	attempt.Eligible = func() bool { return eligible }
	manifest, err := r.PrepareAndStart(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	eligible = false
	manifest, err = r.Monitor(context.Background(), attempt)
	if err != nil || manifest.State != "cancelled" {
		t.Fatalf("monitor = %#v, %v", manifest, err)
	}
	if _, live := fake.sessions[manifest.Session]; live {
		t.Fatal("ineligible attempt session remains live")
	}
}

func TestConcurrentDisjointAttempts(t *testing.T) {
	r, fake, first, _ := testRuntime(t)
	second := first
	second.Issue, second.Number, second.Context = 4, 1, "second"
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, attempt := range []Attempt{first, second} {
		wg.Add(1)
		go func(a Attempt) {
			defer wg.Done()
			_, err := r.PrepareAndStart(context.Background(), a)
			errs <- err
		}(attempt)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(fake.sessions) != 2 {
		t.Fatalf("sessions = %d, want 2", len(fake.sessions))
	}
	for name := range fake.sessions {
		if fake.buffers[name] == "" {
			t.Fatalf("session %s lost context", name)
		}
	}
}

func TestPromptCommandProvidesStdinBeforeFastConsumerStarts(t *testing.T) {
	dir := t.TempDir()
	workspace := filepath.Join(dir, "attempt")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	resultPath := ResultPath(workspace)
	canary := filepath.Join(dir, "outside-canary")
	if err := os.WriteFile(canary, []byte("unchanged"), 0o600); err != nil {
		t.Fatal(err)
	}
	private := t.TempDir()
	if err := os.WriteFile(filepath.Join(private, "coordinator-canary"), []byte("do not expose"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(private, 0); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(private, 0o700) })
	prompt := filepath.Join(dir, "prompt")
	if err := os.WriteFile(prompt, []byte("line one\nline two"), 0o600); err != nil {
		t.Fatal(err)
	}
	tmux := filepath.Join(dir, "tmux")
	deleted := filepath.Join(dir, "buffer-deleted")
	script := "#!/bin/sh\ncase $1 in\nsave-buffer) cat \"$FAKE_PROMPT\";;\ndelete-buffer) : >\"$FAKE_DELETED\";;\n*) exit 2;;\nesac\n"
	if err := os.WriteFile(tmux, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_PROMPT", prompt)
	t.Setenv("FAKE_DELETED", deleted)
	t.Setenv("TMPDIR", private)
	tempDir := t.TempDir()
	for _, name := range []string{"agent-symphony-status.attack", "agent-symphony-overflow.attack"} {
		if err := os.Symlink(canary, filepath.Join(tempDir, name)); err != nil {
			t.Fatal(err)
		}
	}
	result := `{"type":"agent-symphony-result-v1","validation":"tests passed","documentation":"none"}`
	consumer := `input=$(cat) || exit
test "$input" = "$1" && test "$TMPDIR" = /tmp || exit
test -e "$2" || exit
test -z "$(find "$3" -name 'agent-symphony-prompt-*' -print -quit)" || exit
ln -s "$4" "$5" || exit
printf '%s\n' progress >&2
printf '%s\n%s\n' "$6" "$6" >&2
printf %s "$6"`
	workspaceLink := filepath.Join(workspace, ".agent-symphony-result.json")
	var diagnostics strings.Builder
	code, err := captureWorker(t.Context(), tmux, "prompt-buffer", resultPath, []string{"sh", "-c", consumer, "consumer", "line one\nline two", deleted, tempDir, canary, workspaceLink, result}, io.Discard, &diagnostics, tempDir, false)
	if err != nil || code != 0 {
		t.Fatalf("fast stdin consumer: code=%d err=%v diagnostics=%s", code, err, diagnostics.String())
	}
	if !strings.Contains(diagnostics.String(), "progress") || strings.Count(diagnostics.String(), result) != 2 {
		t.Fatalf("stderr diagnostics were not preserved: %q", diagnostics.String())
	}
	if got, err := os.ReadFile(resultPath); err != nil || string(got) != result {
		t.Fatalf("captured stdout = %q, %v", got, err)
	}
	if info, err := os.Stat(resultPath); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("result mode = %v, %v", info, err)
	}
	if got, err := os.ReadFile(canary); err != nil || string(got) != "unchanged" {
		t.Fatalf("outside canary changed: %q, %v", got, err)
	}
	if target, err := os.Readlink(workspaceLink); err != nil || target != canary {
		t.Fatalf("workspace symlink = %q, %v", target, err)
	}
	for _, name := range []string{"agent-symphony-status.attack", "agent-symphony-overflow.attack"} {
		if target, err := os.Readlink(filepath.Join(tempDir, name)); err != nil || target != canary {
			t.Fatalf("scratch canary link = %q, %v", target, err)
		}
	}
	attackedResult := filepath.Join(dir, "attacked.result.json")
	if err := os.Symlink(canary, attackedResult); err != nil {
		t.Fatal(err)
	}
	if code, err := captureWorker(t.Context(), tmux, "prompt-buffer", attackedResult, []string{"sh", "-c", `printf replaced`}, io.Discard, io.Discard, tempDir, false); err == nil || code == 0 {
		t.Fatalf("result symlink was accepted: code=%d err=%v", code, err)
	}
	if got, err := os.ReadFile(canary); err != nil || string(got) != "unchanged" {
		t.Fatalf("result symlink canary changed: %q, %v", got, err)
	}

	var reviewerOut strings.Builder
	code, err = captureWorker(t.Context(), tmux, "prompt-buffer", "", []string{"sh", "-c", `test "$TMPDIR" = /tmp && test "$(cat)" = "$1" && printf reviewer`, "reviewer", "line one\nline two"}, &reviewerOut, io.Discard, tempDir, false)
	if err != nil || code != 0 || reviewerOut.String() != "reviewer" {
		t.Fatalf("reviewer capture: code=%d output=%q err=%v", code, reviewerOut.String(), err)
	}
}

func TestCaptureWorkerRedactsCredentialsBeforeEveryResultWrite(t *testing.T) {
	const canary = "capture-auth-canary"
	for _, mode := range []string{"initial", "replacement", "handoff"} {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			prompt := filepath.Join(dir, "prompt")
			if err := os.WriteFile(prompt, []byte("prompt"), 0o600); err != nil {
				t.Fatal(err)
			}
			tmux := filepath.Join(dir, "tmux")
			if err := os.WriteFile(tmux, []byte("#!/bin/sh\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer) :;; *) exit 2;; esac\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			resultPath := filepath.Join(dir, "result.json")
			if mode != "initial" {
				if err := os.WriteFile(resultPath, []byte(`{"type":"agent-symphony-result-v1","validation":"old","documentation":"safe"}`), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			ready, release := filepath.Join(dir, "ready"), filepath.Join(dir, "release")
			t.Setenv("FAKE_PROMPT", prompt)
			t.Setenv("GH_TOKEN", canary)
			t.Setenv("EXPECTED_TOKEN", canary)
			t.Setenv("READY", ready)
			t.Setenv("RELEASE", release)
			command := []string{"sh", "-c", `test "$GH_TOKEN" = "$EXPECTED_TOKEN" || exit 9
printf '{"type":"agent-symphony-result-v1","validation":"authenticated","documentation":"%s"}' "$GH_TOKEN"
: >"$READY"
while test ! -e "$RELEASE"; do sleep 0.01; done`}
			type captureResult struct {
				code int
				err  error
			}
			done := make(chan captureResult, 1)
			go func() {
				var code int
				var err error
				switch mode {
				case "initial":
					code, err = CaptureWorker(t.Context(), tmux, "buffer", resultPath, command, io.Discard, io.Discard)
				case "replacement":
					code, err = CaptureWorkerReplacingResult(t.Context(), tmux, "buffer", resultPath, command, io.Discard, io.Discard)
				case "handoff":
					code, err = CaptureWorkerReplacingResultAfterStart(t.Context(), tmux, "buffer", resultPath, command, io.Discard, io.Discard, func() error { return nil })
				}
				done <- captureResult{code, err}
			}()
			for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				if _, err := os.Stat(ready); err == nil {
					break
				} else if time.Now().After(deadline) {
					t.Fatalf("authenticated child did not produce output: %v", err)
				}
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			for _, entry := range entries {
				if body, err := os.ReadFile(filepath.Join(dir, entry.Name())); err == nil && bytes.Contains(body, []byte(canary)) {
					t.Fatalf("credential reached in-flight artifact %s", entry.Name())
				}
			}
			if err := os.WriteFile(release, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			captured := <-done
			body, err := os.ReadFile(resultPath)
			if captured.err != nil || captured.code != 0 || err != nil || bytes.Contains(body, []byte(canary)) || !bytes.Contains(body, []byte(`"documentation":"[REDACTED]"`)) {
				t.Fatalf("capture code=%d err=%v artifact=%q read=%v", captured.code, captured.err, body, err)
			}
		})
	}
}

func TestPromptCommandBoundsStdoutAndPreservesExitStatus(t *testing.T) {
	dir := t.TempDir()
	prompt := filepath.Join(dir, "prompt")
	if err := os.WriteFile(prompt, []byte("prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	tmux := filepath.Join(dir, "tmux")
	script := "#!/bin/sh\ncase $1 in\nsave-buffer) cat \"$FAKE_PROMPT\";;\ndelete-buffer) exit 0;;\n*) exit 2;;\nesac\n"
	if err := os.WriteFile(tmux, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_PROMPT", prompt)
	run := func(resultPath string, command []string) (int, error) {
		t.Helper()
		return captureWorker(t.Context(), tmux, "prompt-buffer", resultPath, command, io.Discard, io.Discard, dir, false)
	}

	exitResult := filepath.Join(dir, "exit.result.json")
	code, err := run(exitResult, []string{"sh", "-c", `cat >/dev/null; printf ok; exit 23`})
	if err != nil || code != 23 {
		t.Fatalf("consumer exit status = %d, %v", code, err)
	}
	if got, err := os.ReadFile(exitResult); err != nil || string(got) != "ok" {
		t.Fatalf("consumer stdout = %q, %v", got, err)
	}
	if code, err := run("", []string{"sh", "-c", `exit 23`}); err != nil || code != 23 {
		t.Fatalf("reviewer exit status = %d, %v", code, err)
	}

	boundedResult := filepath.Join(dir, "bounded.result.json")
	unrelated := exec.Command("sleep", "5")
	if err := unrelated.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unrelated.Process.Kill(); _ = unrelated.Wait() })
	started := time.Now()
	code, err = run(boundedResult, []string{"sh", "-c", `trap '' PIPE TERM; while :; do printf x || :; done`})
	if !errors.Is(err, ErrWorkerResultOverflow) || code == 0 || time.Since(started) > 2*time.Second {
		t.Fatalf("over-limit producer: code=%d elapsed=%v err=%v", code, time.Since(started), err)
	}
	if info, err := os.Stat(boundedResult); err != nil || info.Size() != WorkerResultMaxBytes {
		t.Fatalf("bounded result size = %v, %v", info, err)
	}
	if err := unrelated.Process.Signal(syscall.Signal(0)); err != nil {
		t.Fatalf("unrelated process group was terminated: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "agent-symphony-prompt-") {
			t.Fatalf("prompt scratch remained visible: %s", entry.Name())
		}
	}
}

func TestCaptureWorkerCancellationKillsAndReapsChildGroup(t *testing.T) {
	for _, captureResult := range []bool{true, false} {
		t.Run(strconv.FormatBool(captureResult), func(t *testing.T) {
			dir := t.TempDir()
			prompt := filepath.Join(dir, "prompt")
			if err := os.WriteFile(prompt, []byte("prompt"), 0o600); err != nil {
				t.Fatal(err)
			}
			tmux := filepath.Join(dir, "tmux")
			if err := os.WriteFile(tmux, []byte("#!/bin/sh\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer) exit 0;; *) exit 2;; esac\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FAKE_PROMPT", prompt)
			pidPath := filepath.Join(dir, "descendant.pid")
			resultPath := ""
			if captureResult {
				resultPath = filepath.Join(dir, "cancelled.result.json")
			}
			ctx, cancel := context.WithCancel(t.Context())
			done := make(chan error, 1)
			go func() {
				_, err := captureWorker(ctx, tmux, "prompt-buffer", resultPath, []string{"sh", "-c", `trap '' INT TERM; sleep 30 & echo $! >"$1"; wait`, "consumer", pidPath}, io.Discard, io.Discard, dir, false)
				done <- err
			}()
			var pid int
			for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				body, err := os.ReadFile(pidPath)
				if err == nil && strings.TrimSpace(string(body)) != "" {
					pid, err = strconv.Atoi(strings.TrimSpace(string(body)))
					if err != nil {
						t.Fatal(err)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("child descendant did not start")
				}
			}
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("capture cancellation = %v", err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("capture cancellation did not return promptly")
			}
			for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				if err := syscall.Kill(pid, 0); errors.Is(err, syscall.ESRCH) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("child descendant %d survived cancellation", pid)
				}
			}
		})
	}
}

func TestHandoffLaunchWaitsForWorkerOutput(t *testing.T) {
	dir := t.TempDir()
	prompt := filepath.Join(dir, "prompt")
	if err := os.WriteFile(prompt, []byte("prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	tmux := filepath.Join(dir, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer) exit 0;; *) exit 2;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_PROMPT", prompt)
	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	acknowledged := false
	code, err := CaptureWorkerReplacingResultAfterStart(ctx, tmux, "prompt-buffer", filepath.Join(dir, "result"), []string{"sh", "-c", "cat >/dev/null; sleep 5"}, io.Discard, io.Discard, func() error {
		acknowledged = true
		return nil
	})
	if code == 0 || !errors.Is(err, context.DeadlineExceeded) || acknowledged {
		t.Fatalf("code=%d err=%v acknowledged=%v", code, err, acknowledged)
	}
}

func TestCaptureWorkerCompletionTerminatesLateDescendants(t *testing.T) {
	for _, captureResult := range []bool{true, false} {
		t.Run(strconv.FormatBool(captureResult), func(t *testing.T) {
			dir := t.TempDir()
			prompt := filepath.Join(dir, "prompt")
			if err := os.WriteFile(prompt, []byte("prompt"), 0o600); err != nil {
				t.Fatal(err)
			}
			tmux := filepath.Join(dir, "tmux")
			if err := os.WriteFile(tmux, []byte("#!/bin/sh\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer) exit 0;; *) exit 2;; esac\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("FAKE_PROMPT", prompt)
			marker := filepath.Join(dir, "late-marker")
			resultPath, output := "", "reviewer"
			if captureResult {
				resultPath = filepath.Join(dir, "completed.result.json")
				output = `{"type":"agent-symphony-result-v1","validation":"ok","documentation":"none"}`
			}
			t.Setenv("AGENT_SYMPHONY_IN_GROUP_MARKER", marker)
			t.Setenv("AGENT_SYMPHONY_IN_GROUP_OUTPUT", output)
			identityBase := filepath.Join(dir, "identity")
			t.Setenv("AGENT_SYMPHONY_IN_GROUP_IDENTITY", identityBase)
			trigger := filepath.Join(dir, "post-return-trigger")
			t.Setenv("AGENT_SYMPHONY_IN_GROUP_TRIGGER", trigger)
			command := []string{os.Args[0], "-test.run=^TestCaptureWorkerInGroupStdoutHelper$"}
			unrelated := exec.Command("sleep", "5")
			if err := unrelated.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = unrelated.Process.Kill(); _ = unrelated.Wait() })
			var stdout strings.Builder
			devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer devNull.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()
			code, captureErr := captureWorker(ctx, tmux, "prompt-buffer", resultPath, command, &stdout, devNull, dir, false)
			configuredBody, err := os.ReadFile(identityBase + ".configured")
			if err != nil {
				t.Fatal(err)
			}
			childBody, err := os.ReadFile(identityBase + ".child")
			if err != nil {
				t.Fatal(err)
			}
			var wrapperPID, wrapperPGID, helperPID, helperPGID, childPID, childPGID int
			if _, err := fmt.Sscanf(string(configuredBody), "%d %d %d %d", &wrapperPID, &wrapperPGID, &helperPID, &helperPGID); err != nil {
				t.Fatal(err)
			}
			if _, err := fmt.Sscanf(string(childBody), "%d %d", &childPID, &childPGID); err != nil {
				t.Fatal(err)
			}
			t.Logf("worker identities: wrapper=%d/%d helper=%d/%d child=%d/%d", wrapperPID, wrapperPGID, helperPID, helperPGID, childPID, childPGID)
			if wrapperPID == helperPID || helperPID == childPID || helperPGID != wrapperPGID || childPGID != wrapperPGID {
				t.Fatalf("worker identities: wrapper=%d/%d helper=%d/%d child=%d/%d", wrapperPID, wrapperPGID, helperPID, helperPGID, childPID, childPGID)
			}
			if captureErr != nil || code != 0 {
				t.Fatalf("completion: code=%d err=%v", code, captureErr)
			}
			if !captureResult && stdout.String() != output {
				t.Fatalf("reviewer output = %q", stdout.String())
			}
			if err := unrelated.Process.Signal(syscall.Signal(0)); err != nil {
				t.Fatalf("unrelated process group was terminated: %v", err)
			}
			if err := os.WriteFile(trigger, []byte("go"), 0o600); err != nil {
				t.Fatal(err)
			}
			for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
				if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("late descendant mutated workspace: %v", err)
				}
				if time.Now().After(deadline) {
					break
				}
			}
		})
	}
}

func TestCaptureWorkerInGroupStdoutHelper(t *testing.T) {
	marker := os.Getenv("AGENT_SYMPHONY_IN_GROUP_MARKER")
	if marker == "" {
		return
	}
	devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	if err != nil {
		os.Exit(2)
	}
	wrapperPID := os.Getppid()
	wrapperPGID, err := syscall.Getpgid(wrapperPID)
	if err != nil {
		os.Exit(2)
	}
	identityBase := os.Getenv("AGENT_SYMPHONY_IN_GROUP_IDENTITY")
	configuredIdentity := fmt.Sprintf("%d %d %d %d", wrapperPID, wrapperPGID, os.Getpid(), syscall.Getpgrp())
	if err := os.WriteFile(identityBase+".configured", []byte(configuredIdentity), 0o600); err != nil {
		os.Exit(2)
	}
	child := exec.Command(os.Args[0], "-test.run=^TestCaptureWorkerLateMarkerHelper$")
	child.Env = append(os.Environ(), "AGENT_SYMPHONY_LATE_MARKER_CHILD="+identityBase+".child")
	child.Stdout, child.Stderr = os.Stdout, devNull
	if err := child.Start(); err != nil {
		os.Exit(2)
	}
	for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if body, err := os.ReadFile(identityBase + ".child"); err == nil && len(body) != 0 {
			break
		}
		if time.Now().After(deadline) {
			os.Exit(2)
		}
	}
	if _, err := io.WriteString(os.Stdout, os.Getenv("AGENT_SYMPHONY_IN_GROUP_OUTPUT")); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestCaptureWorkerLateMarkerHelper(t *testing.T) {
	identityPath := os.Getenv("AGENT_SYMPHONY_LATE_MARKER_CHILD")
	if identityPath == "" {
		return
	}
	identity := fmt.Sprintf("%d %d", os.Getpid(), syscall.Getpgrp())
	if err := os.WriteFile(identityPath, []byte(identity), 0o600); err != nil {
		os.Exit(2)
	}
	trigger := os.Getenv("AGENT_SYMPHONY_IN_GROUP_TRIGGER")
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		if _, err := os.Stat(trigger); err == nil {
			break
		}
		if time.Now().After(deadline) {
			os.Exit(2)
		}
	}
	if err := os.WriteFile(os.Getenv("AGENT_SYMPHONY_IN_GROUP_MARKER"), []byte("late"), 0o600); err != nil {
		os.Exit(2)
	}
	os.Exit(0)
}

func TestCaptureWorkerEscapedStdoutFailsPromptly(t *testing.T) {
	for _, captureResult := range []bool{true, false} {
		for _, cancelWorker := range []bool{true, false} {
			t.Run(fmt.Sprintf("result=%t/cancel=%t", captureResult, cancelWorker), func(t *testing.T) {
				dir := t.TempDir()
				prompt := filepath.Join(dir, "prompt")
				if err := os.WriteFile(prompt, []byte("prompt"), 0o600); err != nil {
					t.Fatal(err)
				}
				tmux := filepath.Join(dir, "tmux")
				if err := os.WriteFile(tmux, []byte("#!/bin/sh\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer) exit 0;; *) exit 2;; esac\n"), 0o700); err != nil {
					t.Fatal(err)
				}
				t.Setenv("FAKE_PROMPT", prompt)
				pidPath := filepath.Join(dir, "escaped.pid")
				t.Setenv("AGENT_SYMPHONY_ESCAPE_STDOUT", pidPath)
				resultPath, output := "", "reviewer"
				if captureResult {
					resultPath = filepath.Join(dir, "escaped.result.json")
					output = `{"type":"agent-symphony-result-v1","validation":"ok","documentation":"none"}`
				}
				mode := "normal"
				if cancelWorker {
					mode = "cancel"
				}
				command := []string{"sh", "-c", `set +m; "$1" -test.run=^TestCaptureWorkerEscapedStdoutHelper$ 2>/dev/null & while test ! -s "$4"; do sleep 0.01; done; printf %s "$2"; test "$3" = normal || sleep 30`, "consumer", os.Args[0], output, mode, pidPath}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				type outcome struct {
					code int
					err  error
				}
				var stdout strings.Builder
				devNull, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
				if err != nil {
					t.Fatal(err)
				}
				defer devNull.Close()
				done := make(chan outcome, 1)
				started := time.Now()
				go func() {
					code, err := captureWorker(ctx, tmux, "prompt-buffer", resultPath, command, &stdout, devNull, dir, false)
					done <- outcome{code: code, err: err}
				}()
				var pid int
				for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
					body, err := os.ReadFile(pidPath)
					if err == nil && strings.TrimSpace(string(body)) != "" {
						pid, err = strconv.Atoi(strings.TrimSpace(string(body)))
						if err != nil {
							t.Fatal(err)
						}
						break
					}
					if time.Now().After(deadline) {
						t.Fatal("escaped stdout holder did not start")
					}
				}
				t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
				if cancelWorker {
					cancel()
				}
				select {
				case got := <-done:
					if got.code == 0 || time.Since(started) > 2*time.Second {
						t.Fatalf("escaped capture: code=%d elapsed=%v err=%v", got.code, time.Since(started), got.err)
					}
					if cancelWorker && !errors.Is(got.err, context.Canceled) {
						t.Fatalf("escaped cancellation = %v", got.err)
					}
					if !cancelWorker && !errors.Is(got.err, ErrWorkerOutputOpen) {
						t.Fatalf("escaped completion = %v", got.err)
					}
				case <-time.After(2 * time.Second):
					t.Fatal("escaped stdout holder blocked helper return")
				}
			})
		}
	}
}

func TestCaptureWorkerEscapedStdoutHelper(t *testing.T) {
	pidPath := os.Getenv("AGENT_SYMPHONY_ESCAPE_STDOUT")
	if pidPath == "" {
		return
	}
	if _, err := syscall.Setsid(); err != nil {
		os.Exit(2)
	}
	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(os.Getpid())), 0o600); err != nil {
		os.Exit(2)
	}
	time.Sleep(30 * time.Second)
	os.Exit(0)
}

func TestPromptCommandDoesNotStartConsumerWhenBufferReadFails(t *testing.T) {
	dir := t.TempDir()
	tmux := filepath.Join(dir, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nexit 23\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "consumer-ran")
	resultPath := filepath.Join(dir, "attempt.result.json")
	if _, err := captureWorker(t.Context(), tmux, "missing-buffer", resultPath, []string{"sh", "-c", `touch "$1"`, "consumer", marker}, io.Discard, io.Discard, dir, false); err == nil {
		t.Fatal("buffer read failure was masked")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumer ran after buffer read failure: %v", err)
	}
	if _, err := os.Stat(resultPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("result was opened after buffer read failure: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), "agent-symphony-prompt-") {
			t.Fatalf("failed prompt scratch remained visible: %s", entry.Name())
		}
	}
}

func TestQueuedReviewHandoffTransitionsAreDurableAndImmutable(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "completed"
	if err := r.writeManifest(attempt, manifest); err != nil {
		t.Fatal(err)
	}
	findings := []string{"fix isolation"}
	for _, transition := range []struct {
		queued, acknowledged bool
		state                string
	}{{false, false, "completed"}, {true, false, "completed"}, {true, true, "running"}} {
		if _, err := r.RecordReviewFindings(attempt, "abcdef1", findings, transition.queued, transition.acknowledged); err != nil {
			t.Fatal(err)
		}
		restarted := &Runtime{StateRoot: r.StateRoot}
		stored, err := readManifest(restarted.manifestPath(attempt))
		if err != nil || stored.ReviewHandoffQueued != transition.queued || stored.ReviewHandoffAck != transition.acknowledged || stored.State != transition.state {
			t.Fatalf("transition %#v was not durable: %#v err=%v", transition, stored, err)
		}
	}
	if _, err := r.RecordReviewFindings(attempt, "abcdef1", []string{"different"}, true, true); err == nil {
		t.Fatal("queued handoff was mutable")
	}
}

func TestReviewModeAndTargetAreDurableAndValidated(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	target := attempt.BaseSHA + ".." + strings.Repeat("b", 40)
	reviewer, _ := AttemptSessionName(SessionRoleReviewer, attempt.Repository, attempt.Issue, attempt.Number)
	stored, err := r.RecordReview(attempt, "running", ReviewModeImplementation, target, attempt.BaseSHA, strings.Repeat("b", 40), "/review/snapshot", reviewer)
	if err != nil || stored.ReviewMode != ReviewModeImplementation || stored.ReviewTarget != target {
		t.Fatalf("review metadata was not persisted: %#v err=%v", stored, err)
	}
	for _, test := range []struct{ mode, target string }{
		{"ui-review", target},
		{ReviewModePlan, ""},
		{ReviewModePlan, "owner/repo#1 plan sha256:not-a-digest"},
		{ReviewModeImplementation, "garbage"},
		{ReviewModeImplementation, "unsafe\ntarget"},
	} {
		invalid := stored
		invalid.ReviewMode, invalid.ReviewTarget = test.mode, test.target
		if err := r.validateManifest(attempt, invalid); err == nil {
			t.Fatalf("invalid review metadata was accepted: %#v", test)
		}
	}
	invalid := stored
	invalid.ReviewTarget = strings.Repeat("c", 40) + ".." + strings.Repeat("b", 40)
	if err := r.validateManifest(attempt, invalid); err == nil {
		t.Fatal("review target detached from persisted base was accepted")
	}
	invalid = stored
	invalid.ReviewMode = ReviewModePlan
	invalid.ReviewTarget = fmt.Sprintf("%s#%d plan sha256:%s", attempt.Repository, attempt.Issue, strings.Repeat("c", 64))
	invalid.ReviewHead = strings.Repeat("b", 40)
	if err := r.validateManifest(attempt, invalid); err == nil {
		t.Fatal("plan target detached from persisted attempt base was accepted")
	}
	manifest.ReviewState = ""
	if err := r.validateManifest(attempt, manifest); err != nil {
		t.Fatalf("legacy manifest without review metadata was rejected: %v", err)
	}
}

func TestStopInterruptsPaneZeroWhenAnotherPaneIsActive(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.stop(t.Context(), manifest.Session); err != nil {
		t.Fatal(err)
	}
	interrupted := false
	for _, command := range fake.seen {
		if len(command.Args) > 0 && command.Args[0] == "send-keys" {
			interrupted = true
			if valueAfter(command.Args, "-t") != PaneTarget(manifest.Session) {
				t.Fatalf("interrupt targeted %q, want %q", valueAfter(command.Args, "-t"), PaneTarget(manifest.Session))
			}
		}
	}
	if !interrupted {
		t.Fatal("pane 0.0 did not receive C-c")
	}
}

func TestForgetRemovesOnlyCleanedAttemptRecord(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Forget(manifest); err == nil || !strings.Contains(err.Error(), "resource still exists") {
		t.Fatalf("forgot live resources: %v", err)
	}
	if err := os.RemoveAll(manifest.Worktree); err != nil {
		t.Fatal(err)
	}
	if err := r.Forget(manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Dir(manifest.LogPath)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("attempt record remains: %v", err)
	}
	if manifests, err := r.Discover(); err != nil || len(manifests) != 0 {
		t.Fatalf("discover after forget = %#v, %v", manifests, err)
	}
}

func TestCredentialedSessionLaunchFailureIsRedacted(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	canary := "credential-canary"
	t.Setenv("GITHUB_TOKEN", canary)
	fake.fail = "new-session"
	fake.failOutput, fake.failErr = "launch output "+canary, errors.New("launch failure "+canary)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err == nil || strings.Contains(err.Error(), canary) || strings.Contains(manifest.Diagnostic, canary) {
		t.Fatal("credentialed launch failure was not safely redacted")
	}
	stored, readErr := os.ReadFile(r.manifestPath(attempt))
	if readErr != nil || bytes.Contains(stored, []byte(canary)) {
		t.Fatalf("credential reached persisted manifest: read=%v", readErr)
	}
	for _, command := range fake.seen {
		if slices.Contains(command.Args, canary) || strings.Contains(strings.Join(command.Args, " "), canary) {
			t.Fatal("credential reached tmux argv")
		}
	}
	launch := fake.seen[slices.IndexFunc(fake.seen, func(command Command) bool { return slices.Contains(command.Args, "new-session") })]
	if !slices.Contains(launch.Env, "GITHUB_TOKEN="+canary) || !strings.Contains(strings.Join(launch.Args, " "), "GITHUB_TOKEN") {
		t.Fatal("tmux client did not receive the bounded credential environment")
	}
}

func TestImplementationAuthenticationCrossesRuntimeBoundary(t *testing.T) {
	for _, test := range []struct {
		name, token string
		ok          bool
	}{{"authenticated", "implementation-auth-canary", true}, {"missing", "", false}, {"invalid", "implementation-invalid-canary", false}} {
		t.Run(test.name, func(t *testing.T) {
			r, fake, attempt, _ := testRuntime(t)
			fake.sessionAuth, fake.validAuth = true, "implementation-auth-canary"
			t.Setenv("GH_TOKEN", test.token)
			manifest, err := r.PrepareAndStart(t.Context(), attempt)
			if test.ok {
				if err != nil || manifest.State != "running" {
					t.Fatal("authenticated implementation did not reach running state")
				}
			} else if err == nil || !strings.Contains(err.Error(), "GitHub CLI authentication") || test.token != "" && (strings.Contains(err.Error(), test.token) || strings.Contains(manifest.Diagnostic, test.token)) {
				t.Fatal("implementation authentication failure was unclear or exposed its credential")
			}
			for _, command := range fake.seen {
				if test.token != "" && strings.Contains(strings.Join(command.Args, " "), test.token) {
					t.Fatal("credential reached implementation command argv")
				}
			}
			body, readErr := os.ReadFile(r.manifestPath(attempt))
			if readErr != nil || test.token != "" && bytes.Contains(body, []byte(test.token)) {
				t.Fatalf("credential reached implementation manifest: read=%v", readErr)
			}
		})
	}
}

func TestCredentialedPaneOutputIsRedactedBeforeLogPersistence(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	canary := "pane-credential-canary"
	t.Setenv("GH_TOKEN", canary)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	fake.sessions[manifest.Session].output = "agent failure " + canary
	if _, err := r.Monitor(t.Context(), attempt); err != nil {
		t.Fatal(err)
	}
	log, err := os.ReadFile(manifest.LogPath)
	if err != nil || bytes.Contains(log, []byte(canary)) || !bytes.Contains(log, []byte("[REDACTED]")) {
		t.Fatalf("credentialed pane log was not redacted: read=%v", err)
	}
}

func TestTmuxSessionImportsAuthenticationWithoutPuttingValuesInArgv(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	for _, test := range []struct {
		name, token string
		want        string
	}{{"authenticated", "valid", "0"}, {"missing", "", "4"}, {"invalid", "invalid-canary", "5"}} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			bin := filepath.Join(root, "bin")
			tmuxRoot, err := os.MkdirTemp("/tmp", "as217-")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.RemoveAll(tmuxRoot) })
			if err := os.MkdirAll(bin, 0o700); err != nil {
				t.Fatal(err)
			}
			gh := filepath.Join(bin, "gh")
			if err := os.WriteFile(gh, []byte("#!/bin/sh\ncase \"$GH_TOKEN\" in valid) exit 0;; '') exit 4;; *) exit 5;; esac\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			home, err := os.UserHomeDir()
			if err != nil {
				t.Fatal(err)
			}
			env := []string{"HOME=" + home, "PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"), "TMUX_TMPDIR=" + tmuxRoot}
			if test.token != "" {
				env = append(env, "GH_TOKEN="+test.token)
			}
			session := fmt.Sprintf("issue217-%d", time.Now().UnixNano())
			cleanup := exec.Command(tmux, "-L", session, "kill-server")
			cleanup.Env = env
			t.Cleanup(func() { _ = cleanup.Run() })
			args := append([]string{"-L", session}, TmuxNewSessionArgs(session, root, env)...)
			if result, err := (ExecRunner{}).Run(t.Context(), Command{Name: tmux, Args: args, Env: env}); err != nil {
				t.Fatalf("create isolated tmux session: %v: %s", err, result.Output)
			}
			for _, arg := range args {
				if test.token != "" && strings.Contains(arg, test.token) {
					t.Fatal("credential reached tmux argv")
				}
			}
			status := filepath.Join(root, "status")
			command := exec.Command(tmux, "-L", session, "respawn-pane", "-k", "-t", "="+session+":0.0", "--", "sh", "-c", "gh api /user >/dev/null 2>&1; printf %s $? > \"$1\"", "sh", status)
			command.Env = env
			if output, err := command.CombinedOutput(); err != nil {
				t.Fatalf("start authentication probe: %v: %s", err, output)
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				body, err := os.ReadFile(status)
				if err == nil && len(body) > 0 {
					if string(body) != test.want {
						t.Fatalf("authentication result=%q want=%q", body, test.want)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("authentication probe did not finish")
				}
				time.Sleep(10 * time.Millisecond)
			}
		})
	}
}

func TestWorkerIdentityFailsClosedBeforeMutation(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	r.VerifyWorker = nil
	if _, err := r.PrepareAndStart(context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("missing hook = %v", err)
	}
	if len(fake.seen) != 0 {
		t.Fatalf("commands ran before identity verification: %#v", fake.seen)
	}
	if _, err := os.Stat(r.manifestPath(attempt)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest created before identity verification: %v", err)
	}
	r.VerifyWorker = func(context.Context) error { return errors.New("wrong uid") }
	if _, err := r.PrepareAndStart(context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "wrong uid") {
		t.Fatalf("failed hook = %v", err)
	}
	r.VerifyWorker = func(context.Context) error { return nil }
	missingCommand := attempt
	missingCommand.Command = nil
	if _, err := r.PrepareAndStart(context.Background(), missingCommand); err == nil || !strings.Contains(err.Error(), "command") {
		t.Fatalf("missing command = %v", err)
	}
	r.AllowEnv = []string{"SSH_AUTH_SOCK"}
	if _, err := r.PrepareAndStart(context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "environment") {
		t.Fatalf("environment filtering error = %v", err)
	}
	if len(fake.seen) != 0 {
		t.Fatalf("commands ran before environment validation: %#v", fake.seen)
	}
}

func TestExactTargetsExitCodesAndHistory(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	fake.sessions[manifest.Session].status = 127
	manifest, err = r.Monitor(context.Background(), attempt)
	if err != nil || manifest.State != "failed" || !strings.Contains(manifest.Diagnostic, "status 127") {
		t.Fatalf("large exit status = %#v, %v", manifest, err)
	}
	historyConfigured := false
	for _, command := range fake.seen {
		if command.Name != "tmux" {
			continue
		}
		if target := valueAfter(command.Args, "-t"); target != "" && target != "="+manifest.Session && target != PaneTarget(manifest.Session) {
			t.Fatalf("inexact target %q in %#v", target, command.Args)
		}
		if command.Args[0] == "set-option" && slices.Contains(command.Args, "history-limit") && slices.Contains(command.Args, historyLimit) {
			historyConfigured = true
		}
	}
	if !historyConfigured {
		t.Fatal("tmux history was not bounded")
	}
}

func TestVerifyActiveAcceptsOnlyApprovedUnpublishedAncestryOrPublishedHead(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := r.VerifyActive(t.Context(), manifest, attempt.BaseSHA); err != nil {
		t.Fatalf("base HEAD: %v", err)
	}
	runGit(t, manifest.Worktree, "config", "user.email", "test@example.invalid")
	runGit(t, manifest.Worktree, "config", "user.name", "Test")
	runGit(t, manifest.Worktree, "commit", "--allow-empty", "-qm", "descendant")
	descendant := gitOutput(t, manifest.Worktree, "rev-parse", "HEAD")
	if err := r.VerifyActive(t.Context(), manifest, attempt.BaseSHA); err != nil {
		t.Fatalf("unpublished descendant: %v", err)
	}
	if err := r.VerifyActive(t.Context(), manifest, descendant); err != nil {
		t.Fatalf("published exact head: %v", err)
	}
	runGit(t, manifest.Worktree, "commit", "--allow-empty", "-qm", "later")
	if err := r.VerifyActive(t.Context(), manifest, descendant); err == nil || !strings.Contains(err.Error(), "does not match GitHub") {
		t.Fatalf("published head drift: %v", err)
	}
	runGit(t, manifest.Worktree, "checkout", "--orphan", "unrelated")
	runGit(t, manifest.Worktree, "commit", "--allow-empty", "-qm", "unrelated")
	runGit(t, manifest.Worktree, "branch", "-M", manifest.Branch)
	if err := r.VerifyActive(t.Context(), manifest, attempt.BaseSHA); err == nil || !strings.Contains(err.Error(), "not descended") {
		t.Fatalf("unrelated unpublished head: %v", err)
	}
}

func TestResumeHandoffEligibilityOwnsStateTransition(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	if manifest, err = r.Monitor(t.Context(), attempt); err != nil || manifest.State != "completed" {
		t.Fatalf("completed manifest=%#v err=%v", manifest, err)
	}
	eligibilityChecked := false
	attempt.Eligible = func() bool {
		eligibilityChecked = true
		return false
	}
	if _, err := r.ResumeHandoff(t.Context(), attempt); err == nil || !strings.Contains(err.Error(), "no longer eligible") {
		t.Fatalf("ineligible resume=%v", err)
	}
	current, err := r.Discover()
	if !eligibilityChecked || err != nil || len(current) != 1 || current[0].State != "completed" {
		t.Fatalf("ineligible resume changed state: manifests=%#v err=%v", current, err)
	}
}

func TestLaunchConfiguresEmptySessionBeforeAgentAndRetainsFastExit(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	attempt.Command = []string{"fast-exit"}
	manifest, err := r.PrepareAndStart(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	var newIndex, remainIndex, historyIndex, respawnIndex = -1, -1, -1, -1
	for i, command := range fake.seen {
		if command.Name != "tmux" {
			continue
		}
		switch {
		case slices.Contains(command.Args, "new-session"):
			newIndex = i
			if slices.Contains(command.Args, "fast-exit") {
				t.Fatalf("new session contains agent command: %#v", command.Args)
			}
		case command.Args[0] == "set-option":
			if slices.Contains(command.Args, "remain-on-exit") {
				remainIndex = i
			}
			if slices.Contains(command.Args, "history-limit") {
				historyIndex = i
			}
		case command.Args[0] == "respawn-pane":
			respawnIndex = i
		}
	}
	if !(newIndex < remainIndex && remainIndex < historyIndex && historyIndex < respawnIndex) {
		t.Fatalf("launch order new=%d remain=%d history=%d respawn=%d", newIndex, remainIndex, historyIndex, respawnIndex)
	}
	got, err := r.Monitor(context.Background(), attempt)
	if err != nil || got.State != "failed" || !strings.Contains(got.Diagnostic, "status 42") || !fake.sessions[manifest.Session].dead {
		t.Fatalf("fast exit = %#v, %v", got, err)
	}
}

func TestStateContainmentRejectsEscapes(t *testing.T) {
	t.Run("creation component symlink", func(t *testing.T) {
		r, _, attempt, _ := testRuntime(t)
		if err := os.Mkdir(filepath.Join(r.StateRoot, "attempts"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(t.TempDir(), filepath.Join(r.StateRoot, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository))); err != nil {
			t.Fatal(err)
		}
		if _, err := r.PrepareAndStart(context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("creation escape = %v", err)
		}
	})
	t.Run("discovery repository symlink", func(t *testing.T) {
		r, _, attempt, _ := testRuntime(t)
		attempts := filepath.Join(r.StateRoot, "attempts")
		if err := os.Mkdir(attempts, 0o700); err != nil {
			t.Fatal(err)
		}
		outside := t.TempDir()
		if err := os.MkdirAll(filepath.Join(outside, "3-1"), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(outside, "3-1", "manifest.json"), []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, filepath.Join(attempts, internalgithub.RepositoryIdentifier(attempt.Repository))); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Discover(); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("discovery escape = %v", err)
		}
	})
	t.Run("read manifest symlink", func(t *testing.T) {
		r, _, attempt, _ := testRuntime(t)
		dir := filepath.Dir(r.manifestPath(attempt))
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		outside := filepath.Join(t.TempDir(), "manifest.json")
		if err := os.WriteFile(outside, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(outside, r.manifestPath(attempt)); err != nil {
			t.Fatal(err)
		}
		if _, err := r.Monitor(context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("read escape = %v", err)
		}
	})
}

func TestProbeAndCancellationErrorsPreserveState(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	_, err := r.PrepareAndStart(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.failCode["display-message"] = 23
	if got, err := r.Monitor(context.Background(), attempt); err == nil || got.State != "running" {
		t.Fatalf("observation error = %#v, %v", got, err)
	}
	delete(fake.failCode, "display-message")
	fake.ignoreInterrupt, fake.keepAfterKill = true, true
	if got, err := r.Cancel(context.Background(), attempt, "stop"); err == nil || got.State != "running" {
		t.Fatalf("uncertain cancellation = %#v, %v", got, err)
	}
	stored, err := readManifest(r.manifestPath(attempt))
	if err != nil || stored.State != "running" {
		t.Fatalf("stored state after cancellation error = %#v, %v", stored, err)
	}

	r2, fake2, attempt2, _ := testRuntime(t)
	if _, err := r2.PrepareAndStart(context.Background(), attempt2); err != nil {
		t.Fatal(err)
	}
	fake2.failCode["has-session"] = 2
	if got, err := r2.Cancel(context.Background(), attempt2, "stop"); err == nil || got.State != "running" {
		t.Fatalf("probe permission error = %#v, %v", got, err)
	}

	r3, fake3, attempt3, _ := testRuntime(t)
	r3.StopWait = time.Second
	if _, err := r3.PrepareAndStart(context.Background(), attempt3); err != nil {
		t.Fatal(err)
	}
	fake3.ignoreInterrupt = true
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := r3.Cancel(cancelled, attempt3, "stop"); !errors.Is(err, context.Canceled) || got.State != "running" {
		t.Fatalf("cancellation timeout = %#v, %v", got, err)
	}
}

func TestCancelValidatesManifestAndWinsConcurrentMonitor(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Session = "tampered"
	if err := r.writeManifest(attempt, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Cancel(context.Background(), attempt, "stop"); err == nil || !strings.Contains(err.Error(), "deterministic") {
		t.Fatalf("tampered cancel = %v", err)
	}

	r2, _, attempt2, _ := testRuntime(t)
	if _, err := r2.PrepareAndStart(context.Background(), attempt2); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = r2.Monitor(context.Background(), attempt2) }()
	go func() { defer wg.Done(); _, _ = r2.Cancel(context.Background(), attempt2, "stop") }()
	wg.Wait()
	stored, err := readManifest(r2.manifestPath(attempt2))
	if err != nil || stored.State != "cancelled" {
		t.Fatalf("concurrent final state = %#v, %v", stored, err)
	}
}

func TestRepositoryIdentityAndCaseCollision(t *testing.T) {
	r, _, first, _ := testRuntime(t)
	manifest, err := r.PrepareAndStart(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.manifestPath(first), internalgithub.RepositoryIdentifier(first.Repository)) || !strings.Contains(manifest.Session, internalgithub.RepositoryIdentifier(first.Repository)) {
		t.Fatalf("repository identity missing: %#v", manifest)
	}
	second := first
	second.Repository, second.Number = "Owner/Repo", 2
	if _, err := r.PrepareAndStart(context.Background(), second); err == nil || !strings.Contains(err.Error(), "case collision") {
		t.Fatalf("case collision = %v", err)
	}
	long := first
	long.Issue, long.Number = int(^uint(0)>>1), int(^uint(0)>>1)
	if _, err := r.identify(long); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("excessive names = %v", err)
	}
}

func TestCaptureAndLogErrorsAreHonest(t *testing.T) {
	for _, test := range []struct {
		name        string
		breakOutput func(*fakeRunner, Manifest) error
	}{
		{"capture", func(fake *fakeRunner, _ Manifest) error { fake.fail = "capture-pane"; return nil }},
		{"log", func(_ *fakeRunner, manifest Manifest) error { return os.Mkdir(manifest.LogPath, 0o700) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			r, fake, attempt, _ := testRuntime(t)
			manifest, err := r.PrepareAndStart(context.Background(), attempt)
			if err != nil {
				t.Fatal(err)
			}
			fake.sessions[manifest.Session].dead = true
			if err := test.breakOutput(fake, manifest); err != nil {
				t.Fatal(err)
			}
			got, err := r.Monitor(context.Background(), attempt)
			if err == nil || got.State != "failed" || !strings.Contains(got.Diagnostic, "not preserved") || strings.Contains(got.Diagnostic, "output preserved in") {
				t.Fatalf("output failure = %#v, %v", got, err)
			}
			stored, readErr := readManifest(r.manifestPath(attempt))
			if readErr != nil || stored.Diagnostic != got.Diagnostic {
				t.Fatalf("stored output diagnostic = %#v, %v", stored, readErr)
			}
		})
	}
}

func testRuntime(t *testing.T) (*Runtime, *fakeRunner, Attempt, string) {
	t.Helper()
	primary := filepath.Join(t.TempDir(), "primary")
	if err := os.Mkdir(primary, 0o755); err != nil {
		t.Fatal(err)
	}
	runGit(t, primary, "init", "-q")
	runGit(t, primary, "config", "user.email", "test@example.invalid")
	runGit(t, primary, "config", "user.name", "Test")
	if err := os.WriteFile(filepath.Join(primary, "README"), []byte("base\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	runGit(t, primary, "add", "README")
	runGit(t, primary, "commit", "-qm", "base")
	sha := gitOutput(t, primary, "rev-parse", "HEAD")
	base := t.TempDir()
	root, state := filepath.Join(base, "attempts"), filepath.Join(base, "state")
	if err := os.MkdirAll(root, 0o770); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(state, 0o700); err != nil {
		t.Fatal(err)
	}
	fake := newFakeRunner()
	r := &Runtime{Root: root, StateRoot: state, Source: primary, Git: "git", Tmux: "tmux", Helper: "agent-symphony-helper", Runner: fake, AllowEnv: []string{"PATH"}, StopWait: time.Millisecond, VerifyWorker: func(context.Context) error { return nil }}
	attempt := Attempt{Repository: "owner/repo", Issue: 3, Number: 1, BaseSHA: sha, Context: "issue context\n", Command: []string{"fake-agent"}}
	return r, fake, attempt, primary
}

func runGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %s: %v", args, out, err)
	}
}

func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(out), "key does not contain") {
		// git config --get uses exit 1 for an absent value.
		if !(len(args) > 0 && args[0] == "config") {
			t.Fatalf("git %v: %s: %v", args, out, err)
		}
	}
	return strings.TrimSpace(string(out))
}
