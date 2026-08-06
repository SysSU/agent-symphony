package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeRunner struct {
	mu              sync.Mutex
	sessions        map[string]*fakeSession
	buffers         map[string]string
	fail            string
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
	op := command.Args[0]
	if f.fail == op {
		return Result{Output: "canary failure detail"}, errors.New("fake failure")
	}
	if code, ok := f.failCode[op]; ok {
		return Result{Output: "canary failure detail", Code: code, Exited: true}, errors.New("fake exit")
	}
	session := valueAfter(command.Args, "-s")
	if session == "" {
		target := valueAfter(command.Args, "-t")
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
		if _, ok := f.sessions[session]; ok {
			return Result{}, errors.New("existing session")
		}
		f.sessions[session] = &fakeSession{}
	case "set-option":
		if (slices.Contains(command.Args, "remain-on-exit") || slices.Contains(command.Args, "history-limit")) && !slices.Contains(command.Args, "-w") {
			return Result{}, errors.New("window option missing -w")
		}
	case "respawn-pane":
		separator := slices.Index(command.Args, "--")
		if separator < 0 || separator == len(command.Args)-1 {
			return Result{}, errors.New("missing respawn command separator")
		}
		f.sessions[session].agent = slices.Clone(command.Args[separator+1:])
		if slices.Contains(f.sessions[session].agent, "fast-exit") {
			f.sessions[session].dead, f.sessions[session].status = true, 42
		}
	case "load-buffer":
		b, _ := io.ReadAll(command.Stdin)
		f.buffers[valueAfter(command.Args, "-b")] = string(b)
	case "paste-buffer":
		f.sessions[session].context = f.buffers[valueAfter(command.Args, "-b")]
	case "display-message":
		s := f.sessions[session]
		if s == nil {
			return Result{}, errors.New("missing session")
		}
		if slices.Contains(command.Args, "#{pane_dead} #{pane_dead_status}") {
			if s.dead {
				return Result{Output: "1 " + strconv.Itoa(s.status)}, nil
			}
			return Result{Output: "0 0"}, nil
		}
		if s.dead {
			return Result{Output: "1"}, nil
		}
		return Result{Output: "0"}, nil
	case "capture-pane":
		return Result{Output: f.sessions[session].output}, nil
	case "send-keys":
		if slices.Contains(command.Args, "C-c") && !f.ignoreInterrupt {
			f.sessions[session].dead, f.sessions[session].stopped = true, true
		}
	case "kill-session":
		if !f.keepAfterKill {
			delete(f.sessions, session)
		}
	}
	return Result{}, nil
}

func valueAfter(args []string, flag string) string {
	for i := 0; i+1 < len(args); i++ {
		if args[i] == flag {
			return args[i+1]
		}
	}
	return ""
}

func TestLifecycleCreatesUncredentialedRepositoryAndPreservesPrimary(t *testing.T) {
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
	want := PromptCommand("tmux", manifest.Session, attempt.Command)
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
	for _, command := range fake.seen {
		if command.Name != "tmux" {
			continue
		}
		joined := strings.Join(append(command.Args, command.Env...), " ")
		if strings.Contains(joined, "credential-canary") || strings.Contains(joined, "GITHUB_TOKEN") {
			t.Fatalf("credential reached agent command: %#v", command)
		}
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
	spoolRecord := filepath.Join(dir, "spool-path")
	script := "#!/bin/sh\ncase $1 in\nsave-buffer) printf %s \"$4\" >\"$FAKE_SPOOL\"; cp \"$FAKE_PROMPT\" \"$4\";;\ndelete-buffer) exit 0;;\n*) exit 2;;\nesac\n"
	if err := os.WriteFile(tmux, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	command := PromptCommand(tmux, "prompt-buffer", []string{"sh", "-c", `input=$(cat); test "$input" = "$1" && test "$TMPDIR" = /tmp`, "consumer", "line one\nline two"})
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "FAKE_PROMPT=" + prompt, "FAKE_SPOOL=" + spoolRecord, "TMPDIR=" + private}
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("fast stdin consumer: %v: %s", err, out)
	}
	spool, err := os.ReadFile(spoolRecord)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(string(spool)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prompt spool was not removed: %v", err)
	}
}

func TestPromptCommandDoesNotStartConsumerWhenBufferReadFails(t *testing.T) {
	dir := t.TempDir()
	tmux := filepath.Join(dir, "tmux")
	spoolRecord := filepath.Join(dir, "spool-path")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nprintf %s \"$4\" >\"$FAKE_SPOOL\"\nexit 23\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, "consumer-ran")
	command := PromptCommand(tmux, "missing-buffer", []string{"sh", "-c", `touch "$1"`, "consumer", marker})
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "FAKE_SPOOL=" + spoolRecord}
	if err := cmd.Run(); err == nil {
		t.Fatal("buffer read failure was masked")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("consumer ran after buffer read failure: %v", err)
	}
	spool, err := os.ReadFile(spoolRecord)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(string(spool)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed prompt spool was not removed: %v", err)
	}
}

func TestQueuedReviewHandoffTransitionsAreDurableAndImmutable(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	if _, err := r.PrepareAndStart(t.Context(), attempt); err != nil {
		t.Fatal(err)
	}
	findings := []string{"fix isolation"}
	for _, transition := range []struct {
		queued, acknowledged bool
	}{{false, false}, {true, false}, {true, true}} {
		if _, err := r.RecordReviewFindings(attempt, "abcdef1", findings, transition.queued, transition.acknowledged); err != nil {
			t.Fatal(err)
		}
		restarted := &Runtime{StateRoot: r.StateRoot}
		stored, err := readManifest(restarted.manifestPath(attempt))
		if err != nil || stored.ReviewHandoffQueued != transition.queued || stored.ReviewHandoffAck != transition.acknowledged {
			t.Fatalf("transition %#v was not durable: %#v err=%v", transition, stored, err)
		}
	}
	if _, err := r.RecordReviewFindings(attempt, "abcdef1", []string{"different"}, true, true); err == nil {
		t.Fatal("queued handoff was mutable")
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
	r.AllowEnv = []string{"GITHUB_TOKEN"}
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
		switch command.Args[0] {
		case "new-session":
			newIndex = i
			if slices.Contains(command.Args, "fast-exit") {
				t.Fatalf("new session contains agent command: %#v", command.Args)
			}
		case "set-option":
			if slices.Contains(command.Args, "remain-on-exit") {
				remainIndex = i
			}
			if slices.Contains(command.Args, "history-limit") {
				historyIndex = i
			}
		case "respawn-pane":
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
		if err := os.Symlink(t.TempDir(), filepath.Join(r.StateRoot, "attempts", repoIdentifier(attempt.Repository))); err != nil {
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
		if err := os.Symlink(outside, filepath.Join(attempts, repoIdentifier(attempt.Repository))); err != nil {
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
	if !strings.Contains(r.manifestPath(first), repoIdentifier(first.Repository)) || !strings.Contains(manifest.Session, repoIdentifier(first.Repository)) {
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
	r := &Runtime{Root: root, StateRoot: state, Source: primary, Git: "git", Tmux: "tmux", Runner: fake, AllowEnv: []string{"PATH"}, StopWait: time.Millisecond, VerifyWorker: func(context.Context) error { return nil }}
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
