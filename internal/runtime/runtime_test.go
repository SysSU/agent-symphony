package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	goruntime "runtime"
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

func TestWorkerStatusRequestIsGenerationAndLaunchBound(t *testing.T) {
	workspace := t.TempDir()
	if err := os.Mkdir(PrivatePath(workspace), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Worktree: workspace, LaunchID: strings.Repeat("a", 32)}
	write := func(generation, sequence uint64, status, reason string) {
		t.Helper()
		body, _ := json.Marshal(workerStatusRequest{Type: "agent-symphony-status-v1", Generation: generation, LaunchID: manifest.LaunchID, Sequence: sequence, Status: status, Reason: reason})
		if err := os.WriteFile(StatusPath(workspace), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	write(7, 1, "needs-attention", "operator decision required")
	observed, err := observeWorkerStatus(manifest, 7)
	if err != nil || observed.WorkerStatus != "needs-attention" || observed.WorkerStatusReason != "operator decision required" || observed.WorkerStatusSeq != 1 {
		t.Fatalf("observed=%#v err=%v", observed, err)
	}
	write(6, 2, "clear", "stale worker")
	unchanged, err := observeWorkerStatus(observed, 7)
	if err != nil || unchanged.WorkerStatus != observed.WorkerStatus || unchanged.WorkerStatusReason != observed.WorkerStatusReason || unchanged.WorkerStatusSeq != observed.WorkerStatusSeq {
		t.Fatalf("stale generation status blocked lifecycle or changed status: %#v err=%v", unchanged, err)
	}
	write(7, 1, "clear", "out of order")
	unchanged, err = observeWorkerStatus(observed, 7)
	if err != nil || unchanged.WorkerStatus != observed.WorkerStatus || unchanged.WorkerStatusSeq != observed.WorkerStatusSeq {
		t.Fatalf("out-of-order request changed status: %#v err=%v", unchanged, err)
	}
}

func TestWorkerEnvironmentUsesOnlyAttemptPrivateTempAndStatusPaths(t *testing.T) {
	workspace := t.TempDir()
	manifest := Manifest{Worktree: workspace, LaunchID: strings.Repeat("b", 32)}
	environment, err := workspaceEnvironment([]string{"PATH=/bin", "TMPDIR=/host/tmp", "GOCACHE=/host/cache", "GH_TOKEN=secret"}, manifest, 9)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range environment {
		if strings.Contains(entry, "/host/") || strings.HasPrefix(entry, "GH_TOKEN=") {
			t.Fatalf("host path or GitHub credential survived: %q", entry)
		}
	}
	for _, want := range []string{WorkerStatusEnvironment + "=" + StatusPath(workspace), WorkerGenerationEnv + "=9", WorkerLaunchIDEnv + "=" + manifest.LaunchID, "TMPDIR=" + filepath.Join(PrivatePath(workspace), "tmp")} {
		if !slices.Contains(environment, want) {
			t.Fatalf("missing managed worker environment %q in %#v", want, environment)
		}
	}
}

type inheritedEnvironmentRunner struct{}

func (inheritedEnvironmentRunner) Run(ctx context.Context, command Command) (Result, error) {
	command.Env = append(os.Environ(), command.Env...)
	return (ExecRunner{}).Run(ctx, command)
}

type swapOnGuardRunner struct {
	mu   sync.Mutex
	swap func() error
}

func (runner *swapOnGuardRunner) Run(ctx context.Context, command Command) (Result, error) {
	runner.mu.Lock()
	if len(command.Args) > 0 && command.Args[0] == "if-shell" && runner.swap != nil {
		swap := runner.swap
		runner.swap = nil
		runner.mu.Unlock()
		if err := swap(); err != nil {
			return Result{}, err
		}
	} else {
		runner.mu.Unlock()
	}
	return (inheritedEnvironmentRunner{}).Run(ctx, command)
}

type fakeSession struct {
	dead, stopped, pending  bool
	status                  int
	signal                  string
	output                  string
	context                 string
	agent                   []string
	startCommand            []string
	token, worktree, paneID string
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

func TestExecRunnerProtocolOutputFailsClosed(t *testing.T) {
	for _, test := range []struct {
		name, script, want string
		limit              int
		fail               bool
	}{
		{"stdout only", "printf report; printf noise >&2", "report", 6, false},
		{"overflow", "printf prefix; printf '%*s' 1024 ''; printf success", "", 6, true},
		{"failure", "printf report; exit 17", "", 64, true},
		{"unbounded", "printf report", "", 0, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			result, err := (ExecRunner{}).Run(t.Context(), Command{Name: "sh", Args: []string{"-c", test.script}, MaxOutputBytes: test.limit, StdoutOnly: true})
			if (err != nil) != test.fail || result.Output != test.want {
				t.Fatalf("result=%#v error=%v", result, err)
			}
		})
	}
}

func TestExecRunnerProtocolDoesNotWaitForDetachedWriter(t *testing.T) {
	root := t.TempDir()
	gate, pidPath := filepath.Join(root, "gate"), filepath.Join(root, "child-pid")
	if err := syscall.Mkfifo(gate, 0o600); err != nil {
		t.Fatal(err)
	}
	// The child holds stdout open while blocked on a FIFO; the direct shell
	// exits. Cleanup signals only this fixture's recorded child.
	defer func() {
		body, err := os.ReadFile(pidPath)
		if err != nil {
			t.Error(err)
			return
		}
		pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
		if err != nil || pid <= 0 {
			t.Errorf("invalid fixture PID %q: %v", body, err)
			return
		}
		_ = syscall.Kill(pid, syscall.SIGKILL)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	result, err := (ExecRunner{}).Run(ctx, Command{
		Name: "sh", Args: []string{"-c", `cat < "$1" & printf '%s\n' "$!" > "$2"; printf report`, "fixture", gate, pidPath},
		MaxOutputBytes: 64, StdoutOnly: true,
	})
	if !errors.Is(err, exec.ErrWaitDelay) || result.Output != "" || ctx.Err() != nil {
		t.Fatalf("retained stdout was accepted or waited until the process deadline: result=%#v error=%v context=%v", result, err, ctx.Err())
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
	if offset := slices.Index(args, "new-session"); offset >= 0 {
		args = args[offset:]
	} else if offset := slices.Index(args, ";"); offset >= 0 && offset+1 < len(args) {
		args = args[offset+1:]
	}
	op := args[0]
	guarded := op == "if-shell"
	guardSession := ""
	if guarded {
		_, suffix, _ := strings.Cut(args[4], "#{==:#{session_name},")
		guardSession, _, _ = strings.Cut(suffix, "}")
		args = parseFakeTmuxCommand(args[5])
		if len(args) == 0 {
			return Result{}, errors.New("invalid guarded tmux command")
		}
		op = args[0]
		f.seen = append(f.seen, Command{Name: command.Name, Args: args})
	}
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
	if guarded {
		session = guardSession
	}
	if session == "" {
		target := valueAfter(args, "-t")
		if strings.HasPrefix(target, "%") {
			for name, pane := range f.sessions {
				if pane.paneID == target {
					session = name
					break
				}
			}
		} else {
			if target != "" && !strings.HasPrefix(target, "=") {
				return Result{}, errors.New("inexact tmux target")
			}
			session = strings.TrimPrefix(target, "=")
			session = strings.TrimSuffix(session, ":0.0")
		}
	}
	switch op {
	case "list-panes":
		var names []string
		for name := range f.sessions {
			names = append(names, name)
		}
		slices.Sort(names)
		var rows strings.Builder
		if len(names) == 0 {
			// The fake server remains independently observable after its
			// attempt pane exits, so absence is not inferred from a name.
			rows.WriteString("300|400|%999999\n")
		}
		for _, name := range names {
			fmt.Fprintf(&rows, "300|400|%s\n", f.sessions[name].paneID)
		}
		return Result{Output: rows.String()}, nil
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
		launch := []string{"/bin/sh"}
		if index := slices.Index(args, "implementation-gate"); index >= 1 {
			index-- // Include the helper binary in the pane's immutable start command.
			end := slices.Index(args[index:], ";")
			if end >= 0 {
				launch = slices.Clone(args[index : index+end])
			}
		}
		s := &fakeSession{worktree: valueAfter(args, "-c"), agent: slices.Clone(launch), startCommand: slices.Clone(launch), paneID: fmt.Sprintf("%%%d", len(f.sessions))}
		if index := slices.Index(args, "@agent-symphony-launch-token"); index >= 0 && index+1 < len(args) {
			s.token = args[index+1]
		}
		f.sessions[session] = s
		if slices.Contains(args, "-P") {
			return Result{Output: fmt.Sprintf("%s|$0|%s|200|300|400|%s||%s", session, s.paneID, s.worktree, strings.Join(s.startCommand, " "))}, nil
		}
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
		f.sessions[session].startCommand = slices.Clone(f.sessions[session].agent)
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
			return Result{Code: 1, Exited: true}, errors.New("missing session")
		}
		if slices.Contains(args, "#{pane_start_command}") {
			return Result{Output: strings.Join(s.startCommand, " ")}, nil
		}
		if slices.Contains(args, ImplementationPaneFormat) {
			return Result{Output: fmt.Sprintf("%s|$0|%s|200|300|400|%s|%s|%s", session, s.paneID, s.worktree, s.token, strings.Join(s.startCommand, " "))}, nil
		}
		if slices.Contains(args, PaneStatusFormat) {
			if !s.dead {
				return Result{Output: "0||||\n"}, nil
			}
			if s.pending {
				return Result{Output: "1||||\n"}, nil
			}
			if s.signal != "" {
				return Result{Output: "1||" + s.signal + "||\n"}, nil
			}
			return Result{Output: "1|" + strconv.Itoa(s.status) + "|||\n"}, nil
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
	case "kill-pane":
		if !f.keepAfterKill {
			delete(f.sessions, session)
		}
	case "wait-for":
		if guarded && slices.Contains(args, "-U") {
			s := f.sessions[session]
			if s == nil {
				return Result{}, errors.New("missing session")
			}
			if index := slices.Index(s.agent, "implementation-gate"); index >= 0 {
				separator := slices.Index(s.agent[index:], "--")
				if separator < 0 {
					return Result{}, errors.New("missing implementation gate command")
				}
				s.agent = slices.Clone(s.agent[index+separator+1:])
				if slices.Contains(s.agent, "fast-exit") {
					s.dead, s.status = true, 42
				}
			}
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

func parseFakeTmuxCommand(input string) []string {
	var args []string
	var word strings.Builder
	quoted, escaped, started := false, false, false
	for _, char := range input {
		switch {
		case escaped:
			word.WriteRune(char)
			escaped = false
		case char == '\\' && !quoted:
			escaped, started = true, true
		case char == '\'':
			quoted, started = !quoted, true
		case char == ' ' && !quoted:
			if started {
				args = append(args, word.String())
				word.Reset()
				started = false
			}
		default:
			word.WriteRune(char)
			started = true
		}
	}
	if quoted || escaped {
		return nil
	}
	if started {
		args = append(args, word.String())
	}
	return args
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
		want    PaneStatus
		wantErr string
	}{
		{name: "live", output: "0||||\n"},
		{name: "live with prior recorded status", output: "0|||17|\n"},
		{name: "dead status not ready", output: "1||||\n", want: PaneStatus{Dead: true}},
		{name: "dead recorded zero", output: "1|||0|\n", want: PaneStatus{Dead: true, Ready: true}},
		{name: "dead recorded nonzero", output: "1|||42|\n", want: PaneStatus{Dead: true, Ready: true, ExitStatus: 42}},
		{name: "dead recorded signal", output: "1||||15\n", want: PaneStatus{Dead: true, Ready: true, Signal: "15"}},
		{name: "dead zero", output: "1|0|||\n", want: PaneStatus{Dead: true, Ready: true}},
		{name: "dead nonzero", output: "1|127|||\n", want: PaneStatus{Dead: true, Ready: true, ExitStatus: 127}},
		{name: "dead matching native and recorded", output: "1|17||17|\n", want: PaneStatus{Dead: true, Ready: true, ExitStatus: 17}},
		{name: "dead signal", output: "1||term||\n", want: PaneStatus{Dead: true, Ready: true, Signal: "term"}},
		{name: "dead numeric signal", output: "1||15||\n", want: PaneStatus{Dead: true, Ready: true, Signal: "15"}},
		{name: "dead matching native signal and recorded", output: "1||term||15\n", want: PaneStatus{Dead: true, Ready: true, Signal: "term"}},
		{name: "dead matching native segv and recorded", output: "1||segv||11\n", want: PaneStatus{Dead: true, Ready: true, Signal: "segv"}},
		{name: "dead uppercase signal", output: "1||TERM||\n", want: PaneStatus{Dead: true, Ready: true, Signal: "term"}},
		{name: "legacy ambiguous", output: "1\n", wantErr: "invalid pane status"},
		{name: "live with status", output: "0|0|||\n", wantErr: "invalid live pane status"},
		{name: "dead status and signal", output: "1|17|term||\n", wantErr: "ambiguous dead pane status"},
		{name: "negative exit", output: "1|-1|||\n", wantErr: "invalid exit status"},
		{name: "conflicting recorded exit", output: "1|17||42|\n", wantErr: "conflicts"},
		{name: "signaled child with nonzero wrapper status", output: "1|2|||11\n", want: PaneStatus{Dead: true, Ready: true, Signal: "11"}},
		{name: "signaled child with shell-style wrapper status", output: "1|143|||15\n", want: PaneStatus{Dead: true, Ready: true, Signal: "15"}},
		{name: "conflicting recorded signal and successful wrapper", output: "1|0|||15\n", wantErr: "conflicts"},
		{name: "conflicting recorded status and native signal", output: "1||term|143|\n", wantErr: "conflicts"},
		{name: "conflicting recorded signals", output: "1||term||9\n", wantErr: "conflicts"},
		{name: "conflicting recorded segv", output: "1||segv||15\n", wantErr: "conflicts"},
		{name: "conflicting recorded status and signal", output: "1|||143|15\n", wantErr: "conflicting recorded"},
		{name: "negative recorded exit", output: "1|||-1|\n", wantErr: "invalid recorded pane exit status"},
		{name: "out of range recorded exit", output: "1|||256|\n", wantErr: "invalid recorded pane exit status"},
		{name: "garbage recorded exit", output: "1|||x|\n", wantErr: "invalid recorded pane exit status"},
		{name: "zero recorded signal", output: "1||||0", wantErr: "invalid recorded pane signal"},
		{name: "out of range recorded signal", output: "1||||128", wantErr: "invalid recorded pane signal"},
		{name: "garbage recorded signal", output: "1||||x", wantErr: "invalid recorded pane signal"},
		{name: "zero signal", output: "1||0||", wantErr: "invalid pane signal"},
		{name: "out of range signal", output: "1||128||", wantErr: "invalid pane signal"},
		{name: "garbage signal", output: "1||15x||", wantErr: "invalid pane signal"},
		{name: "unknown state", output: "2||||\n", wantErr: "invalid pane status"},
		{name: "empty", wantErr: "invalid pane status"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := ParsePaneStatus(test.output)
			if got != test.want || test.wantErr == "" && err != nil || test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("ParsePaneStatus(%q) = %#v, %v", test.output, got, err)
			}
		})
	}
}

func TestParsePaneStatusFromRealTmux(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	root, err := os.MkdirTemp("/tmp", "as-pane-status-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	env := append(os.Environ(), "TMUX_TMPDIR="+root)
	session := fmt.Sprintf("pane-status-%d", time.Now().UnixNano())
	run := func(args ...string) string {
		t.Helper()
		command := exec.Command(tmux, append([]string{"-L", session, "-f", "/dev/null"}, args...)...)
		command.Env = env
		output, err := command.CombinedOutput()
		if err != nil {
			t.Fatalf("tmux %v: %v: %s", args, err, output)
		}
		return string(output)
	}
	t.Cleanup(func() {
		command := exec.Command(tmux, "-L", session, "kill-server")
		command.Env = env
		_ = command.Run()
	})
	run("new-session", "-d", "-s", session, "sleep", "30")
	target := PaneTarget(session)
	if pane, err := ParsePaneStatus(run("display-message", "-p", "-t", target, PaneStatusFormat)); err != nil || pane != (PaneStatus{}) {
		t.Fatalf("live pane: status=%#v err=%v", pane, err)
	}
	run("set-option", "-w", "-t", target, "remain-on-exit", "on")
	helper := filepath.Join(t.TempDir(), "pane-helper")
	helperScript := fmt.Sprintf("#!/bin/sh\nAGENT_SYMPHONY_PANE_HELPER=1 exec %q -test.run=^TestPaneExitStatusProcessHelper$ -- \"$@\"\n", os.Args[0])
	if err := os.WriteFile(helper, []byte(helperScript), 0o700); err != nil {
		t.Fatal(err)
	}
	awaitPaneDead := func(label string, command []string) PaneStatus {
		t.Helper()
		channel := session + "-" + label
		run("wait-for", "-L", channel)
		for _, option := range []string{PaneExitStatusOption, PaneExitSignalOption} {
			run("set-option", "-p", "-t", target, option, "")
		}
		run(slices.Concat(command[:5], []string{"env", "AGENT_SYMPHONY_PANE_TEST_WAKE=" + channel}, command[5:])...)
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()
		wait := exec.CommandContext(ctx, tmux, "-L", session, "-f", "/dev/null", "wait-for", "-L", channel)
		wait.Env = env
		if output, err := wait.CombinedOutput(); err != nil {
			t.Fatalf("tmux pane %s did not finish: %v: %s; version=%q pane=%q", label, err, output, run("-V"), run("display-message", "-p", "-t", target, PaneStatusFormat))
		}
		run("wait-for", "-U", channel)
		for {
			pane, err := ParsePaneStatus(run("display-message", "-p", "-t", target, PaneStatusFormat))
			if err != nil {
				t.Fatalf("tmux %s returned invalid pane status: %v; raw=%q pane=%q", label, err, run("display-message", "-p", "-t", target, PaneStatusFormat), run("capture-pane", "-p", "-S", "-", "-t", target))
			}
			if pane.Ready {
				return pane
			}
			if ctx.Err() != nil {
				t.Fatalf("tmux %s did not publish pane exit after helper completion: %v; version=%q pane=%#v", label, ctx.Err(), run("-V"), pane)
			}
		}
	}
	if pane := awaitPaneDead("exit-17", append([]string{"respawn-pane", "-k", "-t", target, "--"}, PaneExitStatusCommand(helper, tmux, []string{"sh", "-c", "exit 17"})...)); pane != (PaneStatus{Dead: true, Ready: true, ExitStatus: 17}) {
		t.Fatalf("dead pane status=%#v", pane)
	}
	if pane, err := ParsePaneStatus(run("display-message", "-p", "-t", target, "#{pane_dead}|||#{"+PaneExitStatusOption+"}|")); err != nil || pane != (PaneStatus{Dead: true, Ready: true, ExitStatus: 17}) {
		t.Fatalf("recorded fallback: status=%#v err=%v", pane, err)
	}
	if pane := awaitPaneDead("signal-term", append([]string{"respawn-pane", "-k", "-t", target, "--"}, PaneExitStatusCommand(helper, tmux, []string{"sh", "-c", "kill -TERM $$"})...)); pane != (PaneStatus{Dead: true, Ready: true, Signal: "term"}) && pane != (PaneStatus{Dead: true, Ready: true, Signal: "15"}) {
		t.Fatalf("signaled pane status=%#v", pane)
	}
	if pane, err := ParsePaneStatus(run("display-message", "-p", "-t", target, "#{pane_dead}||||#{"+PaneExitSignalOption+"}")); err != nil || pane != (PaneStatus{Dead: true, Ready: true, Signal: "15"}) {
		t.Fatalf("recorded signal fallback: status=%#v err=%v", pane, err)
	}
	if pane := awaitPaneDead("signal-segv", append([]string{"respawn-pane", "-k", "-t", target, "--"}, PaneExitStatusCommand(helper, tmux, []string{"sh", "-c", "kill -SEGV $$"})...)); pane != (PaneStatus{Dead: true, Ready: true, Signal: "segv"}) && pane != (PaneStatus{Dead: true, Ready: true, Signal: "11"}) {
		t.Fatalf("segfault pane status=%#v", pane)
	}
	if pane, err := ParsePaneStatus(run("display-message", "-p", "-t", target, "#{pane_dead}||||#{"+PaneExitSignalOption+"}")); err != nil || pane != (PaneStatus{Dead: true, Ready: true, Signal: "11"}) {
		t.Fatalf("recorded segfault fallback: status=%#v err=%v", pane, err)
	}
	if pane := awaitPaneDead("exit-143", append([]string{"respawn-pane", "-k", "-t", target, "--"}, PaneExitStatusCommand(helper, tmux, []string{"sh", "-c", "exit 143"})...)); pane != (PaneStatus{Dead: true, Ready: true, ExitStatus: 143}) {
		t.Fatalf("explicit exit 143 status=%#v", pane)
	}
}

func TestPaneExitStatusProcessHelper(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_PANE_HELPER") != "1" {
		return
	}
	separator := slices.Index(os.Args, "--")
	if separator < 0 || separator+4 > len(os.Args) || os.Args[separator+1] != "pane-exit-status" || os.Args[separator+3] != "--" {
		os.Exit(125)
	}
	code, childSignal, err := RunPaneCommand(context.Background(), os.Args[separator+2], os.Args[separator+4:], os.Stdin, os.Stdout, os.Stderr)
	if err != nil {
		os.Exit(126)
	}
	if channel := os.Getenv("AGENT_SYMPHONY_PANE_TEST_WAKE"); channel != "" {
		if exec.Command("tmux", "wait-for", "-U", channel).Run() != nil {
			os.Exit(127)
		}
	}
	if childSignal != 0 {
		signal.Reset(childSignal)
		if syscall.Kill(os.Getpid(), childSignal) == nil {
			select {}
		}
	}
	os.Exit(code)
}

func TestLifecycleCreatesUncredentialedSessionWithoutCredentialedRepository(t *testing.T) {
	r, fake, attempt, primary := testRuntime(t)
	before := gitOutput(t, primary, "status", "--porcelain=v1", "--branch")
	t.Setenv("GITHUB_TOKEN", "credential-canary")
	manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	buffer := "as-start-" + manifest.LaunchID
	if manifest.State != "running" || fake.buffers[buffer] != attempt.Context || fake.buffers[manifest.Session] != "" {
		t.Fatalf("unexpected launch: %#v, %#v", manifest, fake.sessions[manifest.Session])
	}
	want := BoundPromptCommand(r.Helper, "tmux", buffer, ResultPath(manifest.Worktree), manifest, attempt.Command)
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
		args, env := strings.Join(command.Args, " "), strings.Join(command.Env, " ")
		if strings.Contains(args, "credential-canary") {
			t.Fatal("GitHub credential reached tmux argv")
		}
		if strings.Contains(env, "GITHUB_TOKEN=") || strings.Contains(env, "GH_REPO=") {
			t.Fatal("GitHub authority reached the implementation session")
		}
	}
	info, err := os.Stat(r.manifestPath(attempt))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("manifest mode: %v, %v", info, err)
	}
	if _, err := prepareAndStartFixture(t, r, context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "already exist") {
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

func TestDirectBoundRuntimeMutatorsCannotBypassOwner(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(r.manifestPath(attempt))
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func() error{
		"prepare-start": func() error { _, err := r.PrepareAndStart(t.Context(), attempt); return err },
		"handoff":       func() error { _, err := r.ResumeHandoff(t.Context(), attempt); return err },
		"monitor":       func() error { _, err := r.Monitor(t.Context(), attempt); return err },
		"cancel":        func() error { _, err := r.Cancel(t.Context(), attempt, "operator cancel"); return err },
		"review": func() error {
			_, err := r.RecordReview(attempt, "running", ReviewModePlan, "target", "", "", "", "")
			return err
		},
		"findings": func() error {
			_, err := r.RecordReviewFindings(attempt, manifest.BaseSHA, []string{"finding"}, true, false)
			return err
		},
		"forget": func() error { return r.Forget(manifest) },
	} {
		t.Run(name, func(t *testing.T) {
			if err := mutate(); err == nil {
				t.Fatal("direct V2 mutation bypassed owner")
			}
			stored, err := os.ReadFile(r.manifestPath(attempt))
			if err != nil || !bytes.Equal(stored, before) || fake.sessions[manifest.Session] == nil {
				t.Fatalf("direct V2 mutation changed runtime: manifest=%q err=%v session=%#v", stored, err, fake.sessions[manifest.Session])
			}
		})
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
			manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
			if err != nil {
				t.Fatal(err)
			}
			want := BoundPaneExitStatusCommand(r.Helper, "tmux", manifest, []string{"interactive-agent", "--tty", attempt.Context})
			if !manifest.Interactive || fake.buffers["as-start-"+manifest.LaunchID] != "" || !slices.Equal(fake.sessions[manifest.Session].agent, want) {
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
			got, err := monitorFixture(t, r, t.Context(), Attempt{Repository: attempt.Repository, Issue: attempt.Issue, Number: attempt.Number, BaseSHA: attempt.BaseSHA})
			if err != nil || got.State != test.wantState || !strings.Contains(got.Diagnostic, test.wantReason) {
				t.Fatalf("monitored manifest=%#v err=%v", got, err)
			}
		})
	}
}

func TestResumeHandoffRefreshesTrustedSourceRefs(t *testing.T) {
	r, fake, attempt, primary := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
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
	if _, err := monitorFixture(t, r, t.Context(), attempt); err != nil {
		t.Fatal(err)
	}
	resumed, err := resumeHandoffFixture(t, r, t.Context(), attempt)
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

func TestReviewSessionNameIsTargetUniqueAndBounded(t *testing.T) {
	maximum := int(^uint(0) >> 1)
	first, err := ReviewSessionName("owner/repository", maximum, maximum, strings.Repeat("a", 40)+".."+strings.Repeat("b", 40))
	if err != nil || len(first) > maxResourceName {
		t.Fatalf("largest reviewer identity name=%q length=%d err=%v", first, len(first), err)
	}
	second, err := ReviewSessionName("owner/repository", maximum, maximum, strings.Repeat("a", 40)+".."+strings.Repeat("c", 40))
	if err != nil || first == second {
		t.Fatalf("distinct targets shared reviewer identity: first=%q second=%q err=%v", first, second, err)
	}
}

func TestReviewRunSessionNameBindsTargetAndNeverReusedRun(t *testing.T) {
	maximum := int(^uint(0) >> 1)
	first, err := ReviewRunSessionName("owner/repository", maximum, maximum, "owner/repository#1 plan sha256:"+strings.Repeat("a", 64), strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	differentTarget, err := ReviewRunSessionName("owner/repository", maximum, maximum, "owner/repository#1 plan sha256:"+strings.Repeat("c", 64), strings.Repeat("b", 64))
	if err != nil {
		t.Fatal(err)
	}
	differentRun, err := ReviewRunSessionName("owner/repository", maximum, maximum, "owner/repository#1 plan sha256:"+strings.Repeat("a", 64), strings.Repeat("d", 64))
	if err != nil {
		t.Fatal(err)
	}
	if first == differentTarget || first == differentRun || differentTarget == differentRun || len(first) > maxResourceName {
		t.Fatalf("run sessions are not unique and bounded: %q %q %q", first, differentTarget, differentRun)
	}
}

func TestResumeHandoffRecreatesMissingSessionBeforeStateTransition(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	if manifest, err = monitorFixture(t, r, t.Context(), attempt); err != nil || manifest.State != "completed" {
		t.Fatalf("completed manifest=%#v err=%v", manifest, err)
	}
	delete(fake.sessions, manifest.Session)
	fake.sessions["keeper"] = &fakeSession{paneID: "%999"}
	resumed, err := resumeHandoffFixture(t, r, t.Context(), attempt)
	if err != nil || resumed.State != "running" || fake.sessions[manifest.Session] == nil {
		t.Fatalf("resumed=%#v session=%#v err=%v", resumed, fake.sessions[manifest.Session], err)
	}
	cancelled, err := cancelFixture(t, r, t.Context(), attempt, "operator stopped handoff")
	if err != nil || cancelled.State != "cancelled" || fake.sessions[manifest.Session] != nil || fake.sessions["keeper"] == nil {
		t.Fatalf("handoff cleanup left a running session: manifest=%#v session=%#v err=%v", cancelled, fake.sessions[manifest.Session], err)
	}
}

func TestPersistedParkedBoundGateReleasesOnlyExactCandidate(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	if manifest, err = monitorFixture(t, r, t.Context(), attempt); err != nil || manifest.State != "completed" {
		t.Fatalf("completed manifest=%#v err=%v", manifest, err)
	}
	delete(fake.sessions, manifest.Session)
	manifest.LaunchToken, err = newLaunchToken()
	if err != nil {
		t.Fatal(err)
	}
	manifest.LaunchID = manifest.LaunchToken
	command := BoundPaneExitStatusCommand(r.Helper, r.tmux(), manifest, []string{"/bin/sh"})
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, command); err != nil {
		t.Fatal(err)
	}
	if err := r.writeManifest(attempt, manifest); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("fixture did not park the candidate gate")
	}
	fake.sessions[manifest.Session].token = strings.Repeat("f", 32)
	if _, err := resumeHandoffFixture(t, r, t.Context(), attempt); err == nil {
		t.Fatal("resume released a same-name pane without the bound token")
	}
	if !slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("foreign pane observation released the candidate gate")
	}
	fake.sessions[manifest.Session].token = manifest.LaunchToken
	binding, err := ReadImplementationBinding(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := WriteImplementationPermit(manifest, binding); err != nil {
		t.Fatal(err)
	}
	if err := r.launchAgent(t.Context(), manifest); err != nil {
		t.Fatalf("release exact parked gate: %v", err)
	}
	if slices.Contains(fake.sessions[manifest.Session].agent, "implementation-gate") {
		t.Fatal("resume reported running while the worker gate remained parked")
	}
}

func TestBoundHandoffShellStopsOnRealTmux(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-bound-handoff-shell-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	helper := filepath.Join(t.TempDir(), "agent-symphony")
	if output, err := exec.Command("go", "build", "-o", helper, "../../cmd/agent-symphony").CombinedOutput(); err != nil {
		t.Fatalf("build bound helper: %v: %s", err, output)
	}
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: ManifestVersion2, Session: "as-bound-handoff-shell", Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	r := &Runtime{Tmux: tmux, Helper: helper, Runner: inheritedEnvironmentRunner{}, StopWait: time.Second}
	if output, err := exec.Command(tmux, "new-session", "-d", "-s", "keeper").CombinedOutput(); err != nil {
		t.Fatalf("create unrelated keeper: %v: %s", err, output)
	}
	const ready = "as-bound-handoff-shell-ready"
	if output, err := exec.Command(tmux, "wait-for", "-L", ready).CombinedOutput(); err != nil {
		t.Fatalf("lock worker ready barrier: %v: %s", err, output)
	}
	command := BoundPaneExitStatusCommand(helper, tmux, manifest, []string{"/bin/sh", "-c", `"$1" wait-for -U "$2"; exec /bin/sh`, "worker", tmux, ready})
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, command); err != nil {
		t.Fatal(err)
	}
	binding, err := ReadImplementationBinding(manifest)
	if err != nil || binding.Role != "interactive" {
		t.Fatalf("handoff shell binding = %#v, %v", binding, err)
	}
	// Retire the isolated tmux server and join its exact bound wrapper before
	// TempDir cleanup can race the wrapper's terminal proof write.
	t.Cleanup(func() {
		_ = exec.Command(tmux, "kill-server").Run()
		deadline := time.Now().Add(5 * time.Second)
		for {
			if err := syscall.Kill(binding.PanePID, 0); errors.Is(err, syscall.ESRCH) {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("bound handoff wrapper %d did not exit", binding.PanePID)
				return
			}
			goruntime.Gosched()
		}
	})
	if err := WriteImplementationPermit(manifest, binding); err != nil {
		t.Fatal(err)
	}
	if err := r.launchAgent(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(ctx, tmux, "wait-for", "-L", ready).CombinedOutput(); err != nil {
		t.Fatalf("bound shell did not reach worker start: %v: %s", err, output)
	}
	if err := r.stop(t.Context(), manifest); err == nil || !strings.Contains(err.Error(), "descendants remain unproved") {
		t.Fatalf("bound handoff shell incorrectly claimed physical cleanup: %v", err)
	}
	if _, exists := ReadImplementationBinding(manifest); exists != nil {
		t.Fatalf("durable launch binding was lost after stop: %v", exists)
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "=keeper").CombinedOutput(); err != nil {
		t.Fatalf("unrelated keeper was stopped: %v: %s", err, output)
	}
}

func TestBoundLastPaneStopReplaysAfterOriginalServerExits(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-bound-last-pane-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: ManifestVersion2, Session: "as-bound-last-pane", Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	r := &Runtime{Tmux: tmux, Runner: inheritedEnvironmentRunner{}, StopWait: 20 * time.Millisecond}
	// The initial gate is parked, so no worker group was ever released.
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, []string{"unused-helper", "pane-exit-status-bound"}); err != nil {
		t.Fatal(err)
	}
	binding, err := ReadImplementationBinding(manifest)
	if err != nil || binding.Role != "interactive" {
		t.Fatalf("parked gate binding = %#v, %v", binding, err)
	}
	_ = r.stop(t.Context(), manifest) // The guarded kill may precede server exit proof.
	if err := syscall.Kill(binding.ServerPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("fixture original tmux server is not proved dead: PID=%d err=%v", binding.ServerPID, err)
	}
	// A fresh runtime must complete exact cleanup without the vanished socket;
	// a generic has-session exit status alone is never a sufficient proof.
	restarted := &Runtime{Tmux: tmux, Runner: inheritedEnvironmentRunner{}}
	if err := restarted.stop(t.Context(), manifest); err != nil {
		t.Fatalf("restart could not finish stopped last-pane gate: %v", err)
	}
}

func TestValidationTraversalExistingAndLaunchFailureDiagnostics(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	bad := attempt
	bad.Repository = "owner/../repo"
	if _, err := prepareAndStartFixture(t, r, context.Background(), bad); err == nil {
		t.Fatal("accepted traversal identity")
	}
	fake.fail = "new-session"
	manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err == nil || manifest.State != "preparing" || !strings.Contains(err.Error(), "canary failure detail") {
		t.Fatalf("launch failure = %#v, %v", manifest, err)
	}
	stored, readErr := readManifest(r.manifestPath(attempt))
	if readErr != nil || stored.State != "preparing" {
		t.Fatalf("ambiguous Start was falsely terminalized: %#v, %v", stored, readErr)
	}
}

func TestStaleWorkerResultBlocksLaunch(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Unix(7, 0))
	if err != nil {
		t.Fatal(err)
	}
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	prepare := effectTestRequest(t, executor, EffectRequest{Action: EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}, "a")
	prepared, err := executor.Execute(t.Context(), prepare)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(PrivatePath(prepared.Manifest.Worktree), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ResultPath(prepared.Manifest.Worktree), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	start := effectTestRequest(t, executor, EffectRequest{Action: EffectStart, Attempt: attempt, Manifest: prepared.Manifest, Eligible: true}, "b")
	result, err := executor.Execute(t.Context(), start)
	if err == nil || result.Manifest.State != "preparing" || !strings.Contains(err.Error(), "worker result already exists") {
		t.Fatalf("manifest=%#v err=%v", result.Manifest, err)
	}
	if len(fake.sessions) != 0 {
		t.Fatalf("agent launched with stale result: %#v", fake.sessions)
	}
}

func TestAgentFailureCancelAndIneligibility(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	fake.sessions[manifest.Session].status = 7
	fake.sessions[manifest.Session].output = "useful failure output\n"
	recovered := Attempt{Repository: attempt.Repository, Issue: attempt.Issue, Number: attempt.Number, BaseSHA: attempt.BaseSHA}
	manifest, err = monitorFixture(t, r, context.Background(), recovered)
	if err != nil || manifest.State != "failed" || !strings.Contains(manifest.Diagnostic, "status 7") {
		t.Fatalf("monitor = %#v, %v", manifest, err)
	}
	if b, _ := os.ReadFile(manifest.LogPath); string(b) != "useful failure output\n" {
		t.Fatalf("log = %q", b)
	}

	r2, fake2, attempt2, _ := testRuntime(t)
	manifest2, err := prepareAndStartFixture(t, r2, context.Background(), attempt2)
	if err != nil {
		t.Fatal(err)
	}
	recovered2 := Attempt{Repository: attempt2.Repository, Issue: attempt2.Issue, Number: attempt2.Number, BaseSHA: attempt2.BaseSHA}
	manifest2, err = cancelFixture(t, r2, context.Background(), recovered2, "issue closed")
	if _, live := fake2.sessions[manifest2.Session]; err != nil || manifest2.State != "cancelled" || live {
		t.Fatalf("cancel = %#v, %v", manifest2, err)
	}

	r3, _, attempt3, _ := testRuntime(t)
	attempt3.Eligible = func() bool { return false }
	if _, err := prepareAndStartFixture(t, r3, context.Background(), attempt3); err == nil || !strings.Contains(err.Error(), "input is invalid") {
		t.Fatalf("ineligible = %v", err)
	}
}

func TestMonitorStopsAttemptThatBecomesIneligible(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	fake.sessions["keeper"] = &fakeSession{paneID: "%999"}
	eligible := true
	attempt.Eligible = func() bool { return eligible }
	manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	session := manifest.Session
	eligible = false
	manifest, err = monitorFixture(t, r, context.Background(), attempt)
	if err == nil || !strings.Contains(err.Error(), "input is invalid") {
		t.Fatalf("ineligible monitor should defer to owner Stop: %#v, %v", manifest, err)
	}
	if _, live := fake.sessions[session]; !live {
		t.Fatal("ineligible monitor stopped a live attempt without owner invalidation")
	}
}

func TestConcurrentDisjointAttempts(t *testing.T) {
	r, fake, first, _ := testRuntime(t)
	second := first
	second.Issue, second.Number, second.Context = 4, 1, "second"
	var wg sync.WaitGroup
	type launched struct {
		manifest Manifest
		err      error
	}
	results := make(chan launched, 2)
	for _, attempt := range []Attempt{first, second} {
		wg.Add(1)
		go func(a Attempt) {
			defer wg.Done()
			manifest, err := prepareAndStartFixture(t, r, context.Background(), a)
			results <- launched{manifest, err}
		}(attempt)
	}
	wg.Wait()
	close(results)
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if fake.buffers["as-start-"+result.manifest.LaunchID] == "" {
			t.Fatalf("session %s lost context", result.manifest.Session)
		}
	}
	if len(fake.sessions) != 2 {
		t.Fatalf("sessions = %d, want 2", len(fake.sessions))
	}
}

func TestPromptCommandProvidesStdinBeforeFastConsumerStarts(t *testing.T) {
	dir := t.TempDir()
	workspace := filepath.Join(dir, "attempt")
	if err := os.Mkdir(workspace, 0o700); err != nil {
		t.Fatal(err)
	}
	resultPath := ResultPath(workspace)
	if err := os.Mkdir(PrivatePath(workspace), 0o700); err != nil {
		t.Fatal(err)
	}
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
	if !errors.Is(err, ErrWorkerResultOverflow) || code == 0 || time.Since(started) > 5*time.Second {
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
			defer cancel()
			ready := make(chan int, 1)
			done := make(chan error, 1)
			go func() {
				_, err := captureWorkerAfterStart(ctx, tmux, "prompt-buffer", resultPath, []string{"sh", "-c", `trap '' INT TERM; sleep 30 & echo $! >"$1"; printf ready >&2; wait`, "consumer", pidPath}, io.Discard, io.Discard, dir, false, func() error {
					body, err := os.ReadFile(pidPath)
					if err != nil {
						return err
					}
					pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
					if err == nil {
						ready <- pid
					}
					return err
				})
				done <- err
			}()
			var pid int
			select {
			case pid = <-ready:
			case err := <-done:
				t.Fatalf("child descendant did not start: %v", err)
			}
			started := time.Now()
			cancel()
			select {
			case err := <-done:
				if !errors.Is(err, context.Canceled) || time.Since(started) > 2*time.Second {
					t.Fatalf("capture cancellation elapsed=%v err=%v", time.Since(started), err)
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
				releasePath := filepath.Join(dir, "release")
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
				command := []string{"sh", "-c", `set +m; "$1" -test.run=^TestCaptureWorkerEscapedStdoutHelper$ 2>/dev/null & while test ! -s "$4"; do sleep 0.01; done; printf ready >&2; while test ! -e "$5"; do sleep 0.01; done; printf %s "$2"; test "$3" = normal || sleep 30`, "consumer", os.Args[0], output, mode, pidPath, releasePath}
				ctx, cancel := context.WithCancel(t.Context())
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
				ready := make(chan int, 1)
				go func() {
					code, err := captureWorkerAfterStart(ctx, tmux, "prompt-buffer", resultPath, command, &stdout, devNull, dir, false, func() error {
						body, err := os.ReadFile(pidPath)
						if err != nil {
							return err
						}
						pid, err := strconv.Atoi(strings.TrimSpace(string(body)))
						if err == nil {
							ready <- pid
						}
						return err
					})
					done <- outcome{code: code, err: err}
				}()
				var pid int
				select {
				case pid = <-ready:
				case got := <-done:
					t.Fatalf("escaped stdout holder did not start: code=%d err=%v", got.code, got.err)
				}
				t.Cleanup(func() { _ = syscall.Kill(pid, syscall.SIGKILL) })
				started := time.Now()
				if cancelWorker {
					cancel()
				} else if err := os.WriteFile(releasePath, nil, 0o600); err != nil {
					t.Fatal(err)
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
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
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
		if _, err := reviewFindingsFixture(t, r, attempt, "abcdef1", findings, transition.queued, transition.acknowledged); err != nil {
			t.Fatal(err)
		}
		restarted := &Runtime{StateRoot: r.StateRoot}
		stored, err := readManifest(restarted.manifestPath(attempt))
		if err != nil || stored.ReviewHandoffQueued != transition.queued || stored.ReviewHandoffAck != transition.acknowledged || stored.State != transition.state {
			t.Fatalf("transition %#v was not durable: %#v err=%v", transition, stored, err)
		}
	}
	if _, err := reviewFindingsFixture(t, r, attempt, "abcdef1", []string{"different"}, true, true); err == nil {
		t.Fatal("queued handoff was mutable")
	}
}

func TestReviewModeAndTargetAreDurableAndValidated(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	target := attempt.BaseSHA + ".." + strings.Repeat("b", 40)
	reviewer, _ := AttemptSessionName(SessionRoleReviewer, attempt.Repository, attempt.Issue, attempt.Number)
	stored, err := reviewFixture(t, r, attempt, ReviewTransition{State: "running", Mode: ReviewModeImplementation, Target: target, Base: attempt.BaseSHA, Head: strings.Repeat("b", 40), Snapshot: "/review/snapshot", Session: reviewer})
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
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	wantPaneID := fake.sessions[manifest.Session].paneID
	if err := r.stop(t.Context(), manifest); err != nil {
		t.Fatal(err)
	}
	interrupted := false
	for _, command := range fake.seen {
		if len(command.Args) > 0 && command.Args[0] == "send-keys" {
			interrupted = true
			if valueAfter(command.Args, "-t") != wantPaneID {
				t.Fatalf("interrupt targeted %q, want bound pane %q", valueAfter(command.Args, "-t"), wantPaneID)
			}
		}
	}
	if !interrupted {
		t.Fatal("pane 0.0 did not receive C-c")
	}
}

func TestStopDoesNotKillReplacementOnReusedTmuxName(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-implementation-replacement-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	const session = "implementation-replacement"
	run := func(args ...string) string {
		t.Helper()
		output, err := exec.Command(tmux, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("tmux %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: boundManifestVersion, Session: session, Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	r := &Runtime{Tmux: tmux, Runner: inheritedEnvironmentRunner{}}
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, nil); err != nil {
		t.Fatal(err)
	}
	run("new-session", "-d", "-s", "replacement-anchor", "sleep", "30")
	run("kill-session", "-t", "="+session)
	run("new-session", "-d", "-s", session, "sleep", "30")
	if err := r.stop(t.Context(), manifest); err == nil {
		t.Fatal("stop accepted an unbound replacement")
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "="+session).CombinedOutput(); err != nil {
		t.Fatalf("foreign replacement was killed: %v: %s", err, output)
	}
}

func TestTmuxMissingExactPaneCanFallBackToAnotherPane(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-missing-pane-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	for _, name := range []string{"keep", "target"} {
		if output, err := exec.Command(tmux, "new-session", "-d", "-s", name).CombinedOutput(); err != nil {
			t.Fatalf("create %s: %v: %s", name, err, output)
		}
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "=keep").CombinedOutput(); err != nil {
		t.Fatalf("control session exited before exact absence check: %v: %s", err, output)
	}
	output, err := exec.Command(tmux, "display-message", "-p", "-t", PaneTarget("target"), ImplementationPaneFormat).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	pane, err := ParseImplementationPane(string(output))
	if err != nil {
		t.Fatal(err)
	}
	paneID := pane.PaneID
	if paneID == "" {
		t.Fatal("target pane ID was empty")
	}
	if output, err := exec.Command(tmux, "kill-session", "-t", "=target").CombinedOutput(); err != nil {
		t.Fatalf("kill target: %v: %s", err, output)
	}
	command := exec.Command(tmux, "display-message", "-p", "-t", paneID, ImplementationPaneFormat)
	output, err = command.CombinedOutput()
	if err != nil || strings.Contains(string(output), paneID) {
		t.Fatalf("tmux missing-pane fallback changed: %v, output=%s", err, output)
	}
	inventory, err := exec.Command(tmux, "list-panes", "-a", "-F", ImplementationInventoryFormat).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	missing, err := ImplementationPaneAbsentFromInventory(string(inventory), ImplementationLaunchBinding{ServerPID: pane.ServerPID, ServerStart: pane.ServerStart, PaneID: paneID})
	if err != nil || !missing {
		t.Fatalf("same-server exact pane absence was not proved: %v, output=%s", err, inventory)
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "=keep").CombinedOutput(); err != nil {
		t.Fatalf("control server did not remain live: %v: %s", err, output)
	}
}

func TestTmuxLinkedPaneSurvivesSessionKillUntilExactPaneKill(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-linked-pane-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	run := func(args ...string) string {
		t.Helper()
		output, err := exec.Command(tmux, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("tmux %v: %v: %s", args, err, output)
		}
		return string(output)
	}
	run("new-session", "-d", "-s", "keeper")
	run("new-session", "-d", "-s", "target")
	pane, err := ParseImplementationPane(run("display-message", "-p", "-t", PaneTarget("target"), ImplementationPaneFormat))
	if err != nil {
		t.Fatal(err)
	}
	binding := ImplementationLaunchBinding{ServerPID: pane.ServerPID, ServerStart: pane.ServerStart, PaneID: pane.PaneID}
	run("link-window", "-s", "=target:0", "-t", "=keeper:1")
	run("kill-session", "-t", "=target")
	inventory := run("list-panes", "-a", "-F", ImplementationInventoryFormat)
	missing, err := ImplementationPaneAbsentFromInventory(inventory, binding)
	if err != nil || missing {
		t.Fatalf("linked worker vanished after only session kill: missing=%t err=%v inventory=%s", missing, err, inventory)
	}
	run("kill-pane", "-t", pane.PaneID)
	inventory = run("list-panes", "-a", "-F", ImplementationInventoryFormat)
	missing, err = ImplementationPaneAbsentFromInventory(inventory, binding)
	if err != nil || !missing {
		t.Fatalf("exact linked pane remained after pane kill: missing=%t err=%v inventory=%s", missing, err, inventory)
	}
}

func TestStopRejectsBoundServerAfterSocketPathMoves(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-bound-socket-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: boundManifestVersion, Session: "bound-socket", Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	r := &Runtime{Tmux: tmux, Runner: inheritedEnvironmentRunner{}}
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, nil); err != nil {
		t.Fatal(err)
	}
	oldSocket := filepath.Join(socketRoot, fmt.Sprintf("tmux-%d", os.Getuid()), "default")
	orphanSocket := filepath.Join(socketRoot, "orphaned-s1")
	if err := os.Rename(oldSocket, orphanSocket); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exec.Command(tmux, "-S", orphanSocket, "kill-server").Run() })
	if output, err := exec.Command(tmux, "-S", orphanSocket, "has-session", "-t", "="+manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("old S1 was not live after socket move: %v: %s", err, output)
	}
	if err := r.stop(t.Context(), manifest); err == nil {
		t.Fatal("socket absence falsely certified the live old S1 worker as stopped")
	}
	if output, err := exec.Command(tmux, "new-session", "-d", "-s", "foreign-s2").CombinedOutput(); err != nil {
		t.Fatalf("create replacement S2: %v: %s", err, output)
	}
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	if err := r.stop(t.Context(), manifest); err == nil {
		t.Fatal("replacement S2 inventory falsely certified the live old S1 worker as stopped")
	}
	if output, err := exec.Command(tmux, "-S", orphanSocket, "has-session", "-t", "="+manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("old S1 was touched by wrong-server cleanup: %v: %s", err, output)
	}
}

func TestBoundImplementationRejectsSamePaneRespawn(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-same-pane-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	attemptRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	stateRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	attempt := Attempt{Repository: "o/r", Issue: 310, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	manifest, err := AttemptIdentity(attemptRoot, attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Version, manifest.State = boundManifestVersion, "running"
	manifest.LaunchToken, manifest.LaunchID = strings.Repeat("a", 32), strings.Repeat("b", 32)
	manifest.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository), "310-1", "agent.log")
	r := &Runtime{Root: attemptRoot, StateRoot: stateRoot, Tmux: tmux, Runner: inheritedEnvironmentRunner{}, VerifyWorker: func(context.Context) error { return nil }}
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(manifest.LogPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := r.writeManifest(attempt, manifest); err != nil {
		t.Fatal(err)
	}
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, nil); err != nil {
		t.Fatal(err)
	}
	initial, err := ReadImplementationBinding(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command(tmux, "respawn-pane", "-k", "-t", PaneTarget(manifest.Session), "sleep", "30").CombinedOutput(); err != nil {
		t.Fatalf("replace same pane: %v: %s", err, out)
	}
	if result, err := exec.Command(tmux, "display-message", "-p", "-t", PaneTarget(manifest.Session), ImplementationPaneFormat).CombinedOutput(); err != nil {
		t.Fatal(err)
	} else if pane, err := ParseImplementationPane(string(result)); err != nil || pane.PaneID != initial.PaneID || pane.PanePID == initial.PanePID {
		t.Fatalf("replacement did not reuse pane with a new process: %#v, %v", pane, err)
	}
	if err := r.stop(t.Context(), manifest); err == nil {
		t.Fatal("stop accepted a same-pane replacement")
	}
	if _, err := monitorFixture(t, r, t.Context(), attempt); err == nil {
		t.Fatal("monitor accepted a same-pane replacement")
	}
	if err := r.Deliver(t.Context(), manifest, []byte("handoff")); err == nil {
		t.Fatal("handoff delivery accepted a same-pane replacement")
	}
	if out, err := exec.Command(tmux, "has-session", "-t", "="+manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("same-pane replacement was killed: %v: %s", err, out)
	}
}

func TestNewImplementationSessionBindsBeforeAgentLaunch(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-implementation-binding-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	state := t.TempDir()
	manifest := Manifest{Version: boundManifestVersion, Session: "implementation-binding", Worktree: worktree, LogPath: filepath.Join(state, "agent.log"), LaunchToken: strings.Repeat("a", 32)}
	helper := filepath.Join(t.TempDir(), "agent-symphony")
	if output, err := exec.Command("go", "build", "-o", helper, "../../cmd/agent-symphony").CombinedOutput(); err != nil {
		t.Fatalf("build bound helper: %v: %s", err, output)
	}
	r := &Runtime{Tmux: tmux, Helper: helper, Runner: inheritedEnvironmentRunner{}}
	manifest.LaunchID = strings.Repeat("b", 32)
	marker := filepath.Join(worktree, "worker-released")
	const signal = "implementation-worker-released"
	worker := []string{"sh", "-c", fmt.Sprintf("printf 'quoted\\nargument' > %q; tmux wait-for -U %s; exec sleep 30", marker, signal)}
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, worker); err != nil {
		if output, probeErr := exec.Command(tmux, "display-message", "-p", "-t", PaneTarget(manifest.Session), ImplementationPaneFormat).CombinedOutput(); probeErr == nil {
			t.Logf("pane after failed binding: %q", output)
		}
		t.Fatal(err)
	}
	binding, err := ReadImplementationBinding(manifest)
	if err != nil || binding.Token != manifest.LaunchToken || binding.EffectID != manifest.LaunchID {
		t.Fatalf("durable binding = %#v, %v", binding, err)
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker ran before durable binding and owner release: %v", err)
	}
	if output, err := exec.Command(tmux, "wait-for", "-L", signal).CombinedOutput(); err != nil {
		t.Fatalf("lock worker completion signal: %v: %s", err, output)
	}
	t.Cleanup(func() { _ = exec.Command(tmux, "wait-for", "-U", signal).Run() })
	if err := WriteImplementationPermit(manifest, binding); err != nil {
		t.Fatal(err)
	}
	if err := r.launchAgent(t.Context(), manifest); err != nil {
		t.Fatalf("guarded agent release: %v", err)
	}
	completion, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if output, err := exec.CommandContext(completion, tmux, "wait-for", "-L", signal).CombinedOutput(); err != nil {
		t.Fatalf("wait for worker execution: %v: %s", err, output)
	}
	if body, err := os.ReadFile(marker); err != nil || string(body) != "quoted\nargument" {
		t.Fatalf("worker output after release = %q, %v", body, err)
	}
	if output, err := exec.Command("ps", "-p", strconv.Itoa(binding.PanePID), "-o", "comm=").CombinedOutput(); err != nil || !strings.Contains(string(output), "sleep") {
		t.Fatalf("gate did not exec worker with same PID %d: %q, %v", binding.PanePID, output, err)
	}
	if err := r.launchAgent(t.Context(), manifest); err != nil {
		t.Fatalf("replaying release for same bound worker: %v", err)
	}
	status, err := r.observeBoundCommand(t.Context(), manifest, func(pane ImplementationPane) []string {
		return []string{"display-message", "-p", "-t", pane.PaneID, PaneStatusFormat}
	})
	if err != nil || strings.TrimSpace(status.Output) != "0||||" {
		t.Fatalf("guarded status observation = %q, %v", status.Output, err)
	}
	bound, pane, err := r.observeBound(t.Context(), manifest)
	if err != nil {
		t.Fatal(err)
	}
	load, err := TmuxCommandString([]string{"load-buffer", "-b", "implementation-binding-test", "-"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := r.guardedBoundResult(t.Context(), bound, pane, load, strings.NewReader("guarded payload")); err != nil {
		t.Fatalf("guarded buffer load: %v", err)
	}
	if output, err := exec.Command(tmux, "show-buffer", "-b", "implementation-binding-test").CombinedOutput(); err != nil || string(output) != "guarded payload" {
		t.Fatalf("guarded buffer contents = %q, %v", output, err)
	}
	if _, pane, err := r.observeBound(t.Context(), manifest); err != nil || pane.PanePID != binding.PanePID || pane.Command != binding.Command || !strings.Contains(pane.Command, "quoted") {
		t.Fatalf("released worker lost gate pane identity: %#v, %v", pane, err)
	}
	if err := r.stop(t.Context(), manifest); err == nil {
		t.Fatal("raw test worker without a bound group was falsely certified stopped")
	}
}

func TestImplementationIdentityRetainsBoundedWorkerContext(t *testing.T) {
	dir := t.TempDir()
	manifest := Manifest{
		Version:     boundManifestVersion,
		LaunchToken: strings.Repeat("a", 32),
		LaunchID:    strings.Repeat("b", 32),
		Session:     "bounded-worker-context",
		Worktree:    dir,
		LogPath:     filepath.Join(dir, "attempt", "agent.log"),
	}
	if err := os.MkdirAll(filepath.Dir(manifest.LogPath), 0o700); err != nil {
		t.Fatal(err)
	}
	binding := ImplementationLaunchBinding{
		Version: 1, Role: "interactive", Token: manifest.LaunchToken, EffectID: manifest.LaunchID,
		ServerPID: 10, ServerStart: 11, SessionName: manifest.Session, SessionID: "$1",
		PaneID: "%1", PanePID: 12, StartPath: manifest.Worktree, Command: strings.Repeat("worker-context-", 512),
	}
	if err := WriteImplementationBinding(manifest, binding); err != nil {
		t.Fatalf("write bounded launch identity: %v", err)
	}
	if got, err := ReadImplementationBinding(manifest); err != nil || got != binding {
		t.Fatalf("read bounded launch identity = %#v, %v", got, err)
	}
	if err := WriteImplementationPermit(manifest, binding); err != nil {
		t.Fatalf("write bounded launch permit: %v", err)
	}
	if err := WriteImplementationRelease(manifest, binding); err != nil {
		t.Fatalf("write bounded launch release: %v", err)
	}
}

func TestGuardedStopDoesNotSignalReplacementBetweenProbeAndCommand(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-implementation-guard-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: boundManifestVersion, Session: "implementation-guard", Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	runner := &swapOnGuardRunner{}
	r := &Runtime{Tmux: tmux, Runner: runner}
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, nil); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(tmux, "new-session", "-d", "-s", "guard-anchor", "sleep", "30").CombinedOutput(); err != nil {
		t.Fatalf("create guard anchor: %v: %s", err, output)
	}
	runner.swap = func() error {
		for _, args := range [][]string{{"kill-session", "-t", "=" + manifest.Session}, {"new-session", "-d", "-s", manifest.Session, "sleep", "30"}} {
			if output, err := exec.Command(tmux, args...).CombinedOutput(); err != nil {
				return fmt.Errorf("tmux %v: %w: %s", args, err, output)
			}
		}
		return nil
	}
	if err := r.stop(t.Context(), manifest); err == nil {
		t.Fatal("guarded stop accepted a replacement")
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "="+manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("guard killed foreign replacement: %v: %s", err, output)
	}
}

func TestNewImplementationSessionCollisionNeverTagsForeignPane(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-implementation-collision-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	const session = "implementation-collision"
	if output, err := exec.Command(tmux, "-f", "/dev/null", "new-session", "-d", "-s", session, "sleep", "30").CombinedOutput(); err != nil {
		t.Fatalf("create foreign pane: %v: %s", err, output)
	}
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: boundManifestVersion, Session: session, Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	r := &Runtime{Tmux: tmux, Runner: inheritedEnvironmentRunner{}}
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, nil); err == nil {
		t.Fatal("new-session collision was accepted")
	}
	probe, err := exec.Command(tmux, "display-message", "-p", "-t", "="+session, "#{@agent-symphony-launch-token}").CombinedOutput()
	if err != nil || strings.TrimSpace(string(probe)) != "" {
		t.Fatalf("foreign pane was tagged: %q, %v", probe, err)
	}
	if _, err := os.Lstat(ImplementationBindingPath(manifest, manifest.LaunchID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign pane acquired binding: %v", err)
	}
}

func TestGuardedObservationRejectsReplacementBetweenProbeAndRead(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-implementation-observation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	worktree, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := Manifest{Version: boundManifestVersion, Session: "implementation-observation", Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	runner := &swapOnGuardRunner{}
	r := &Runtime{Tmux: tmux, Runner: runner}
	if err := startSessionFixture(t, r, t.Context(), manifest, nil, manifest.LaunchID, nil); err != nil {
		t.Fatal(err)
	}
	if output, err := exec.Command(tmux, "new-session", "-d", "-s", "observation-anchor", "sleep", "30").CombinedOutput(); err != nil {
		t.Fatalf("create observation anchor: %v: %s", err, output)
	}
	runner.swap = func() error {
		for _, args := range [][]string{{"kill-session", "-t", "=" + manifest.Session}, {"new-session", "-d", "-s", manifest.Session, "sleep", "30"}} {
			if output, err := exec.Command(tmux, args...).CombinedOutput(); err != nil {
				return fmt.Errorf("tmux %v: %w: %s", args, err, output)
			}
		}
		return nil
	}
	if result, err := r.observeBoundCommand(t.Context(), manifest, func(pane ImplementationPane) []string {
		return []string{"display-message", "-p", "-t", pane.PaneID, PaneStatusFormat}
	}); err == nil || strings.TrimSpace(result.Output) == "0||||" {
		t.Fatalf("stale status was accepted: %q, %v", result.Output, err)
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "="+manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("foreign replacement was killed by observation: %v: %s", err, output)
	}
}

func TestForgetRemovesOnlyCleanedAttemptRecord(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := forgetFixture(t, r, manifest); err == nil || !strings.Contains(err.Error(), "resources remain") {
		t.Fatalf("forgot live resources: %v", err)
	}
	if err := os.RemoveAll(manifest.Worktree); err != nil {
		t.Fatal(err)
	}
	delete(fake.sessions, manifest.Session)
	if err := forgetFixture(t, r, manifest); err != nil {
		t.Fatal(err)
	}
	if err := forgetFixture(t, r, manifest); err != nil {
		t.Fatalf("idempotent forget after restart-style retry: %v", err)
	}
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := forgetFixture(t, r, manifest); err == nil || !strings.Contains(err.Error(), "resources remain") {
		t.Fatalf("missing record accepted a reappeared worker resource: %v", err)
	}
	if err := os.RemoveAll(manifest.Worktree); err != nil {
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
	t.Setenv("MODEL_API_KEY", canary)
	r.AllowEnv = append(r.AllowEnv, "MODEL_API_KEY")
	fake.fail = "new-session"
	fake.failOutput, fake.failErr = "launch output "+canary, errors.New("launch failure "+canary)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
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
	if !slices.Contains(launch.Env, "MODEL_API_KEY="+canary) || !strings.Contains(strings.Join(launch.Args, " "), "MODEL_API_KEY") {
		t.Fatal("tmux client did not receive the bounded model credential environment")
	}
}

func TestGitHubAuthenticationDoesNotCrossRuntimeBoundary(t *testing.T) {
	for _, test := range []struct{ name, token string }{{"present", "implementation-auth-canary"}, {"missing", ""}} {
		t.Run(test.name, func(t *testing.T) {
			r, fake, attempt, _ := testRuntime(t)
			t.Setenv("GH_TOKEN", test.token)
			manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
			if err != nil || manifest.State != "running" {
				t.Fatalf("uncredentialed implementation did not reach running state: %#v %v", manifest, err)
			}
			for _, command := range fake.seen {
				if strings.Contains(strings.Join(command.Args, " ")+strings.Join(command.Env, " "), "GH_TOKEN=") || test.token != "" && strings.Contains(strings.Join(command.Args, " ")+strings.Join(command.Env, " "), test.token) {
					t.Fatal("GitHub credential reached implementation process")
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
	t.Setenv("MODEL_API_KEY", canary)
	r.AllowEnv = append(r.AllowEnv, "MODEL_API_KEY")
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	fake.sessions[manifest.Session].output = "agent failure " + canary
	if _, err := monitorFixture(t, r, t.Context(), attempt); err != nil {
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
	if _, err := prepareAndStartFixture(t, r, context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "identity") {
		t.Fatalf("missing hook = %v", err)
	}
	if len(fake.seen) != 0 {
		t.Fatalf("commands ran before identity verification: %#v", fake.seen)
	}
	if _, err := os.Stat(r.manifestPath(attempt)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("manifest created before identity verification: %v", err)
	}
	r.VerifyWorker = func(context.Context) error { return errors.New("wrong uid") }
	if _, err := prepareAndStartFixture(t, r, context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "wrong uid") {
		t.Fatalf("failed hook = %v", err)
	}
	r.VerifyWorker = func(context.Context) error { return nil }
	missingCommand := attempt
	missingCommand.Command = nil
	if _, err := prepareAndStartFixture(t, r, context.Background(), missingCommand); err == nil || !strings.Contains(err.Error(), "input is invalid") {
		t.Fatalf("missing command = %v", err)
	}
	r.AllowEnv = []string{"SSH_AUTH_SOCK"}
	if _, err := prepareAndStartFixture(t, r, context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "environment") {
		t.Fatalf("environment filtering error = %v", err)
	}
	if len(fake.seen) != 0 {
		t.Fatalf("commands ran before environment validation: %#v", fake.seen)
	}
}

func TestExactTargetsExitCodesAndHistory(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	fake.sessions[manifest.Session].status = 127
	manifest, err = monitorFixture(t, r, context.Background(), attempt)
	if err != nil || manifest.State != "failed" || !strings.Contains(manifest.Diagnostic, "status 127") {
		t.Fatalf("large exit status = %#v, %v", manifest, err)
	}
	historyConfigured := false
	for _, command := range fake.seen {
		if command.Name != "tmux" {
			continue
		}
		if target := valueAfter(command.Args, "-t"); target != "" && target != "="+manifest.Session && target != PaneTarget(manifest.Session) && target != "%0" {
			t.Fatalf("inexact target %q in %#v", target, command.Args)
		}
		if slices.Contains(command.Args, "new-session") && slices.Contains(command.Args, "history-limit") && slices.Contains(command.Args, historyLimit) {
			historyConfigured = true
		}
	}
	if !historyConfigured {
		t.Fatal("tmux history was not bounded")
	}
}

func TestVerifyActiveAcceptsOnlyApprovedUnpublishedAncestryOrPublishedHead(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
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
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.sessions[manifest.Session].dead = true
	if manifest, err = monitorFixture(t, r, t.Context(), attempt); err != nil || manifest.State != "completed" {
		t.Fatalf("completed manifest=%#v err=%v", manifest, err)
	}
	eligibilityChecked := false
	attempt.Eligible = func() bool {
		eligibilityChecked = true
		return false
	}
	if _, err := resumeHandoffFixture(t, r, t.Context(), attempt); err == nil || !strings.Contains(err.Error(), "input is invalid") {
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
	manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	var newIndex, releaseIndex = -1, -1
	for i, command := range fake.seen {
		if command.Name != "tmux" {
			continue
		}
		switch {
		case slices.Contains(command.Args, "new-session"):
			newIndex = i
			if !slices.Contains(command.Args, "remain-on-exit") || !slices.Contains(command.Args, "history-limit") {
				t.Fatalf("session setup was not atomic with creation: %#v", command.Args)
			}
			if !slices.Contains(command.Args, "implementation-gate") || !slices.Contains(command.Args, "fast-exit") {
				t.Fatalf("new session did not park the authorized agent command: %#v", command.Args)
			}
		case command.Args[0] == "if-shell" && slices.Contains(command.Args, "wait-for -U "+ImplementationGateChannel(manifest.LaunchID)):
			releaseIndex = i
		}
	}
	if !(newIndex >= 0 && newIndex < releaseIndex) {
		t.Fatalf("launch order new=%d release=%d", newIndex, releaseIndex)
	}
	got, err := monitorFixture(t, r, context.Background(), attempt)
	if err != nil || got.State != "failed" || !strings.Contains(got.Diagnostic, "status 42") || !fake.sessions[manifest.Session].dead {
		t.Fatalf("fast exit = %#v, %v", got, err)
	}
}

func TestMonitorWaitsForPaneStatusThenReportsSignal(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	session := fake.sessions[manifest.Session]
	session.dead, session.pending = true, true
	pending, err := monitorFixture(t, r, t.Context(), attempt)
	if err != nil || pending.State != "running" || !pending.UpdatedAt.Equal(manifest.UpdatedAt) {
		t.Fatalf("pending pane status changed progress: manifest=%#v err=%v", pending, err)
	}
	session.pending, session.signal, session.output = false, "15", "terminated output\n"
	failed, err := monitorFixture(t, r, t.Context(), attempt)
	if err != nil || failed.State != "failed" || !strings.Contains(failed.Diagnostic, "signal 15") {
		t.Fatalf("signaled pane status=%#v err=%v", failed, err)
	}
	if output, err := os.ReadFile(failed.LogPath); err != nil || string(output) != session.output {
		t.Fatalf("signaled pane log=%q err=%v", output, err)
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
		if _, err := prepareAndStartFixture(t, r, context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "symlink") {
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
		if _, err := monitorFixture(t, r, context.Background(), attempt); err == nil || !strings.Contains(err.Error(), "symlink") {
			t.Fatalf("read escape = %v", err)
		}
	})
}

func TestProbeAndCancellationErrorsPreserveState(t *testing.T) {
	r, fake, attempt, _ := testRuntime(t)
	_, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	fake.failCode["display-message"] = 23
	if got, err := monitorFixture(t, r, context.Background(), attempt); err == nil || got.State != "running" {
		t.Fatalf("observation error = %#v, %v", got, err)
	}
	delete(fake.failCode, "display-message")
	fake.ignoreInterrupt, fake.keepAfterKill = true, true
	if got, err := cancelFixture(t, r, context.Background(), attempt, "stop"); err == nil || got.State != "running" {
		t.Fatalf("uncertain cancellation = %#v, %v", got, err)
	}
	stored, err := readManifest(r.manifestPath(attempt))
	if err != nil || stored.State != "running" {
		t.Fatalf("stored state after cancellation error = %#v, %v", stored, err)
	}

	r2, fake2, attempt2, _ := testRuntime(t)
	if _, err := prepareAndStartFixture(t, r2, context.Background(), attempt2); err != nil {
		t.Fatal(err)
	}
	fake2.failCode["has-session"] = 2
	if got, err := cancelFixture(t, r2, context.Background(), attempt2, "stop"); err == nil || got.State != "running" {
		t.Fatalf("probe permission error = %#v, %v", got, err)
	}

	r3, fake3, attempt3, _ := testRuntime(t)
	r3.StopWait = time.Second
	if _, err := prepareAndStartFixture(t, r3, context.Background(), attempt3); err != nil {
		t.Fatal(err)
	}
	fake3.ignoreInterrupt = true
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if got, err := cancelFixture(t, r3, cancelled, attempt3, "stop"); !errors.Is(err, context.Canceled) || got.State != "running" {
		t.Fatalf("cancellation timeout = %#v, %v", got, err)
	}
}

func TestCancelValidatesManifest(t *testing.T) {
	r, _, attempt, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Session = "tampered"
	if err := r.writeManifest(attempt, manifest); err != nil {
		t.Fatal(err)
	}
	if _, err := cancelFixture(t, r, context.Background(), attempt, "stop"); err == nil || !strings.Contains(err.Error(), "deterministic") {
		t.Fatalf("tampered cancel = %v", err)
	}

}

func TestRepositoryIdentityAndCaseCollision(t *testing.T) {
	r, _, first, _ := testRuntime(t)
	manifest, err := prepareAndStartFixture(t, r, context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(r.manifestPath(first), internalgithub.RepositoryIdentifier(first.Repository)) || !strings.Contains(manifest.Session, internalgithub.RepositoryIdentifier(first.Repository)) {
		t.Fatalf("repository identity missing: %#v", manifest)
	}
	second := first
	second.Repository, second.Number = "Owner/Repo", 2
	if _, err := prepareAndStartFixture(t, r, context.Background(), second); err == nil || !strings.Contains(err.Error(), "case collision") {
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
			manifest, err := prepareAndStartFixture(t, r, context.Background(), attempt)
			if err != nil {
				t.Fatal(err)
			}
			fake.sessions[manifest.Session].dead = true
			if err := test.breakOutput(fake, manifest); err != nil {
				t.Fatal(err)
			}
			got, err := monitorFixture(t, r, context.Background(), attempt)
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
	root, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	state, err = filepath.EvalSymlinks(state)
	if err != nil {
		t.Fatal(err)
	}
	fake := newFakeRunner()
	r := &Runtime{Root: root, StateRoot: state, Source: primary, Git: "git", Tmux: "tmux", Helper: "agent-symphony-helper", Runner: fake, AllowEnv: []string{"PATH"}, StopWait: time.Millisecond, VerifyWorker: func(context.Context) error { return nil }}
	attempt := Attempt{Repository: "owner/repo", Issue: 3, Number: 1, BaseSHA: sha, Context: "issue context\n", Command: []string{"fake-agent"}}
	return r, fake, attempt, primary
}

// prepareAndStartFixture exercises the runtime through typed effects while
// emulating the owner grant only inside runtime-package tests. Production
// callers must submit an owner Start intent and cannot use this fixture.
func prepareAndStartFixture(t *testing.T, r *Runtime, ctx context.Context, attempt Attempt) (Manifest, error) {
	t.Helper()
	r.mu.Lock()
	canonicalState, stateErr := filepath.EvalSymlinks(r.StateRoot)
	if stateErr != nil {
		r.mu.Unlock()
		return Manifest{}, stateErr
	}
	canonicalRoot, err := filepath.EvalSymlinks(r.Root)
	if err != nil {
		r.mu.Unlock()
		return Manifest{}, err
	}
	if r.StateRoot != canonicalState {
		r.StateRoot = canonicalState
	}
	if r.Root != canonicalRoot {
		r.Root = canonicalRoot
	}
	r.mu.Unlock()
	manifest, err := PreparingManifest(r.Root, r.StateRoot, attempt, time.Now())
	if err != nil {
		return Manifest{}, err
	}
	if existing, err := r.readManifest(attempt); err == nil {
		return existing, errors.New("attempt resources already exist")
	} else if !errors.Is(err, os.ErrNotExist) {
		return Manifest{}, err
	}
	eligible := attempt.Eligible == nil || attempt.Eligible()
	effectAttempt := attempt
	effectAttempt.Eligible = nil
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	bind := func(action EffectAction, current Manifest) (EffectRequest, error) {
		request, err := executor.BindRequest(EffectRequest{Action: action, Attempt: effectAttempt, Manifest: current, Eligible: eligible})
		if err != nil {
			return EffectRequest{}, err
		}
		id, err := newLaunchToken()
		if err != nil {
			return EffectRequest{}, err
		}
		request.Identity = EffectIdentity{Repository: current.Repository, Issue: current.Issue, Attempt: current.Attempt, Epoch: 1, SourceRevision: 2, IssueGeneration: 1, AttemptGeneration: 1, EffectID: id}
		if action == EffectStart {
			request.GateNonce = request.Identity.EffectID
		}
		request.Identity.RequestDigest, err = EffectRequestDigest(request)
		return request, err
	}
	prepare, err := bind(EffectPrepare, manifest)
	if err != nil {
		return Manifest{}, err
	}
	prepared, err := executor.Execute(ctx, prepare)
	if prepared.Manifest.Repository == "" {
		return Manifest{}, err
	}
	if writeErr := r.writeManifest(attempt, prepared.Manifest); writeErr != nil {
		return Manifest{}, errors.Join(err, writeErr)
	}
	if err != nil || prepared.Manifest.State != "preparing" {
		return prepared.Manifest, err
	}
	start, err := bind(EffectStart, prepared.Manifest)
	if err != nil {
		return prepared.Manifest, err
	}
	started, err := executor.Execute(ctx, start)
	if started.Manifest.Repository == "" {
		return Manifest{}, err
	}
	if writeErr := r.writeManifest(attempt, started.Manifest); writeErr != nil {
		return started.Manifest, errors.Join(err, writeErr)
	}
	return started.Manifest, err
}

func monitorFixture(t *testing.T, r *Runtime, ctx context.Context, attempt Attempt) (Manifest, error) {
	t.Helper()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version != boundManifestVersion {
		return r.Monitor(ctx, attempt)
	}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	eligible := attempt.Eligible == nil || attempt.Eligible()
	effectAttempt := attempt
	effectAttempt.Eligible = nil
	executor := EffectExecutor{Runtime: r}
	request, err := executor.BindRequest(EffectRequest{Action: EffectMonitor, Attempt: effectAttempt, Manifest: manifest, Eligible: eligible})
	if err != nil {
		return manifest, err
	}
	id, err := newLaunchToken()
	if err != nil {
		return manifest, err
	}
	request.Identity = EffectIdentity{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Epoch: 1, SourceRevision: 2, IssueGeneration: 1, AttemptGeneration: 1, EffectID: id}
	request.Identity.RequestDigest, err = EffectRequestDigest(request)
	if err != nil {
		return manifest, err
	}
	result, effectErr := executor.Execute(ctx, request)
	if result.Manifest.Repository == "" {
		return manifest, effectErr
	}
	if writeErr := r.writeManifest(attempt, result.Manifest); writeErr != nil {
		return result.Manifest, errors.Join(effectErr, writeErr)
	}
	return result.Manifest, effectErr
}

func cancelFixture(t *testing.T, r *Runtime, ctx context.Context, attempt Attempt, reason string) (Manifest, error) {
	t.Helper()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version != boundManifestVersion {
		return r.Cancel(ctx, attempt, reason)
	}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	effectAttempt := attempt
	effectAttempt.Eligible = nil
	executor := EffectExecutor{Runtime: r}
	request, err := executor.BindRequest(EffectRequest{Action: EffectStop, Attempt: effectAttempt, Manifest: manifest, Reason: reason})
	if err != nil {
		return manifest, err
	}
	id, err := newLaunchToken()
	if err != nil {
		return manifest, err
	}
	request.Identity = EffectIdentity{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Epoch: 1, SourceRevision: 2, IssueGeneration: 1, AttemptGeneration: 1, EffectID: id}
	request.Identity.RequestDigest, err = EffectRequestDigest(request)
	if err != nil {
		return manifest, err
	}
	result, effectErr := executor.Execute(ctx, request)
	if result.Manifest.Repository == "" {
		return manifest, effectErr
	}
	if effectErr == nil {
		if writeErr := r.writeManifest(attempt, result.Manifest); writeErr != nil {
			return result.Manifest, writeErr
		}
	}
	return result.Manifest, effectErr
}

func reviewFixture(t *testing.T, r *Runtime, attempt Attempt, review ReviewTransition) (Manifest, error) {
	t.Helper()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version != boundManifestVersion {
		return r.RecordReview(attempt, review.State, review.Mode, review.Target, review.Base, review.Head, review.Snapshot, review.Session)
	}
	executor := EffectExecutor{Runtime: r}
	request, err := executor.BindRequest(EffectRequest{Action: EffectReview, Attempt: attempt, Manifest: manifest, Review: review})
	if err != nil {
		return manifest, err
	}
	id, err := newLaunchToken()
	if err != nil {
		return manifest, err
	}
	request.Identity = EffectIdentity{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Epoch: 1, SourceRevision: 2, IssueGeneration: 1, AttemptGeneration: 1, EffectID: id}
	request.Identity.RequestDigest, err = EffectRequestDigest(request)
	if err != nil {
		return manifest, err
	}
	result, effectErr := executor.Execute(t.Context(), request)
	if effectErr != nil {
		return manifest, effectErr
	}
	return result.Manifest, r.writeManifest(attempt, result.Manifest)
}

func reviewFindingsFixture(t *testing.T, r *Runtime, attempt Attempt, head string, findings []string, queued, acknowledged bool) (Manifest, error) {
	t.Helper()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if manifest.Version != boundManifestVersion {
		return r.RecordReviewFindings(attempt, head, findings, queued, acknowledged)
	}
	return reviewFixture(t, r, attempt, ReviewTransition{State: "findings-queued", Mode: manifest.ReviewMode, Target: manifest.ReviewTarget, Base: manifest.ReviewBase, Head: head, Snapshot: manifest.ReviewSnapshot, Session: manifest.ReviewSession, Findings: slices.Clone(findings), HandoffQueued: queued, HandoffAcknowledged: acknowledged})
}

func forgetFixture(t *testing.T, r *Runtime, manifest Manifest) error {
	t.Helper()
	if manifest.Version != boundManifestVersion {
		return r.Forget(manifest)
	}
	executor := EffectExecutor{
		Runtime: r,
		Cleanup: func(ctx context.Context, request EffectRequest) error {
			if err := r.VerifyResourcesGone(ctx, request.Manifest); err != nil {
				return err
			}
			return r.ForgetCompatibility(request.Manifest)
		},
		VerifyCleanup: func(ctx context.Context, request EffectRequest) (bool, error) {
			return r.VerifyResourcesGone(ctx, request.Manifest) == nil, nil
		},
	}
	request, err := executor.BindRequest(EffectRequest{Action: EffectCleanup, Attempt: Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}, Manifest: manifest, Cleanup: EffectCleanupPolicy{Action: "abandon"}})
	if err != nil {
		return err
	}
	id, err := newLaunchToken()
	if err != nil {
		return err
	}
	request.Identity = EffectIdentity{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Epoch: 1, SourceRevision: 2, IssueGeneration: 1, AttemptGeneration: 1, EffectID: id}
	request.Identity.RequestDigest, err = EffectRequestDigest(request)
	if err != nil {
		return err
	}
	_, err = executor.Execute(t.Context(), request)
	return err
}

func resumeHandoffFixture(t *testing.T, r *Runtime, ctx context.Context, attempt Attempt) (Manifest, error) {
	t.Helper()
	manifest, err := r.readManifest(attempt)
	if err != nil {
		return Manifest{}, err
	}
	if err := r.validateManifest(attempt, manifest); err != nil {
		return Manifest{}, err
	}
	eligible := attempt.Eligible == nil || attempt.Eligible()
	effectAttempt := attempt
	effectAttempt.Eligible = nil
	executor := EffectExecutor{Runtime: r, AuthorizeLaunch: func(context.Context, EffectRequest) error { return nil }}
	request, err := executor.BindRequest(EffectRequest{Action: EffectHandoff, Attempt: effectAttempt, Manifest: manifest, Eligible: eligible, CandidateLaunchToken: strings.Repeat("c", 32)})
	if err != nil {
		return Manifest{}, err
	}
	request.Identity = EffectIdentity{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Epoch: 1, SourceRevision: 2, IssueGeneration: 1, AttemptGeneration: 1, EffectID: strings.Repeat("d", 32)}
	request.Identity.RequestDigest, err = EffectRequestDigest(request)
	if err != nil {
		return Manifest{}, err
	}
	result, err := executor.Execute(ctx, request)
	if err != nil {
		return result.Manifest, err
	}
	return result.Manifest, r.writeManifest(attempt, result.Manifest)
}

func startSessionFixture(t *testing.T, r *Runtime, ctx context.Context, manifest Manifest, env []string, effectID string, command []string) error {
	t.Helper()
	if r.Helper == "" {
		r.Helper = filepath.Join(t.TempDir(), "agent-symphony")
		if output, err := exec.Command("go", "build", "-o", r.Helper, "../../cmd/agent-symphony").CombinedOutput(); err != nil {
			t.Fatalf("build gate helper: %v: %s", err, output)
		}
	}
	return r.startSession(ctx, manifest, env, effectID, command)
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
