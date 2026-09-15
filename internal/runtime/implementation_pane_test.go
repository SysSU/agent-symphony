package runtime

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func boundPaneProcessFixture(t *testing.T, helper string) (Manifest, ImplementationLaunchBinding) {
	t.Helper()
	worktree := t.TempDir()
	manifest := Manifest{Version: ManifestVersion2, Session: "as-aaaaaaaaaaaaaaaa-310-1", Worktree: worktree, LogPath: filepath.Join(t.TempDir(), "agent.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	t.Setenv("TMUX_PANE", "%1")
	role := "interactive"
	if strings.HasPrefix(helper, "worker-capture-bound") {
		role = "capture"
	}
	binding := ImplementationLaunchBinding{Version: 1, Role: role, Token: manifest.LaunchToken, EffectID: manifest.LaunchID, ServerPID: os.Getpid(), ServerStart: 1, SessionName: manifest.Session, SessionID: "$1", PaneID: "%1", PanePID: os.Getpid(), StartPath: worktree, Command: helper}
	if err := WriteImplementationBinding(manifest, binding); err != nil {
		t.Fatal(err)
	}
	return manifest, binding
}

func TestBoundInteractivePreservesSignalVersusExitStatus(t *testing.T) {
	for _, tc := range []struct {
		name    string
		command string
		code    int
		signal  syscall.Signal
	}{
		{name: "explicit exit 143", command: "exit 143", code: 143},
		{name: "TERM signal", command: "kill -TERM $$", code: 143, signal: syscall.SIGTERM},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manifest, binding := boundPaneProcessFixture(t, "pane-exit-status-bound")
			code, gotSignal, err := RunBoundPaneCommand(t.Context(), manifest, "/usr/bin/true", []string{"/bin/sh", "-c", tc.command}, nil, io.Discard, io.Discard)
			if err != nil || code != tc.code || gotSignal != tc.signal {
				t.Fatalf("bound exit = code %d signal %d err %v; want %d/%d", code, gotSignal, err, tc.code, tc.signal)
			}
			if gone, err := ImplementationWorkerGone(manifest, binding); err == nil || gone {
				t.Fatalf("worker descendants were certified dead from group status: %t, %v", gone, err)
			}
		})
	}
}

func TestBoundInteractiveCancellationKillsForkedWorker(t *testing.T) {
	manifest, binding := boundPaneProcessFixture(t, "pane-exit-status-bound")
	path := filepath.Join(t.TempDir(), "wait.fifo")
	if err := syscall.Mkfifo(path, 0o600); err != nil {
		t.Fatal(err)
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, _, err := RunBoundPaneCommand(ctx, manifest, "/usr/bin/true", []string{"/bin/sh", "-c", `sleep 30 & printf '%d\n' "$!"; exec cat "$1"`, "worker", path}, nil, writer, io.Discard)
		finished <- err
		_ = writer.Close()
	}()
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	workerPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || workerPID < 2 {
		t.Fatalf("forked worker PID %q: %v", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(workerPID, syscall.SIGKILL) })
	cancel()
	if err := <-finished; err != nil {
		t.Fatalf("bound cancellation group cleanup: %v", err)
	}
	state, err := exec.Command("ps", "-p", strconv.Itoa(workerPID), "-o", "state=").CombinedOutput()
	if err == nil && strings.TrimSpace(string(state)) != "" && !strings.HasPrefix(strings.TrimSpace(string(state)), "Z") {
		t.Fatalf("forked worker survived bound cancellation: PID=%d state=%s", workerPID, state)
	}
	if gone, err := ImplementationWorkerGone(manifest, binding); err == nil || gone {
		t.Fatalf("worker descendants were certified dead from group status: %t, %v", gone, err)
	}
}

func TestBoundInteractiveProofConflictPreventsWorkerExecution(t *testing.T) {
	manifest, binding := boundPaneProcessFixture(t, "pane-exit-status-bound")
	path := implementationGroupPath(manifest, "interactive", "start")
	if err := os.WriteFile(path, []byte("conflicting proof"), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "worker-ran")
	_, _, err := RunBoundPaneCommand(t.Context(), manifest, "/usr/bin/true", []string{"/bin/sh", "-c", `printf ran > "$1"`, "worker", marker}, nil, io.Discard, io.Discard)
	if err == nil {
		t.Fatal("bound helper released worker despite conflicting durable proof")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker ran before launch proof: %v", err)
	}
	if gone, err := ImplementationWorkerGone(manifest, binding); err == nil || gone {
		t.Fatalf("conflicting evidence was accepted as worker absence: %t, %v", gone, err)
	}
}

func TestBoundCapturePersistsWorkerGroupBeforeExecution(t *testing.T) {
	manifest, binding := boundPaneProcessFixture(t, "worker-capture-bound")
	result := filepath.Join(t.TempDir(), "result.json")
	code, err := CaptureBoundWorker(t.Context(), manifest, "/usr/bin/true", "prompt", result, []string{"/bin/sh", "-c", `printf captured`}, io.Discard, io.Discard, false, nil)
	if err != nil || code != 0 {
		t.Fatalf("bound capture = code %d, err %v", code, err)
	}
	if body, err := os.ReadFile(result); err != nil || string(body) != "captured" {
		t.Fatalf("captured result = %q, %v", body, err)
	}
	if start, err := readImplementationGroup(manifest, binding, "capture", "start"); err != nil || start.GroupPID != start.WorkerPID {
		t.Fatalf("bound capture group = %#v, %v", start, err)
	}
	if gone, err := ImplementationWorkerGone(manifest, binding); err == nil || gone {
		t.Fatalf("capture descendants were certified dead from group status: %t, %v", gone, err)
	}
}

func TestBoundCaptureProofConflictPreventsWorkerExecution(t *testing.T) {
	manifest, binding := boundPaneProcessFixture(t, "worker-capture-bound")
	if err := os.WriteFile(implementationGroupPath(manifest, "capture", "start"), []byte("conflicting proof"), 0o600); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(t.TempDir(), "worker-ran")
	_, err := CaptureBoundWorker(t.Context(), manifest, "/usr/bin/true", "prompt", filepath.Join(t.TempDir(), "result"), []string{"/bin/sh", "-c", `printf ran > "$1"`, "worker", marker}, io.Discard, io.Discard, false, nil)
	if err == nil {
		t.Fatal("bound capture released worker despite conflicting durable proof")
	}
	if _, err := os.Stat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker ran before durable group proof: %v", err)
	}
	if gone, err := ImplementationWorkerGone(manifest, binding); err == nil || gone {
		t.Fatalf("conflicting capture proof was accepted: %t, %v", gone, err)
	}
}

func TestBoundCaptureWorkerArgumentCannotChangeGroupRole(t *testing.T) {
	manifest, binding := boundPaneProcessFixture(t, "worker-capture-bound -- worker-argument=pane-exit-status-bound")
	groupPID, err := syscall.Getpgid(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteImplementationGroupStart(manifest, binding, "capture", groupPID, os.Getpid()); err != nil {
		t.Fatal(err)
	}
	if gone, err := ImplementationWorkerGone(manifest, binding); err == nil && gone {
		t.Fatal("arbitrary worker argument changed capture role and certified a live group absent")
	}
}

func TestBoundGroupProofRejectsRoleMismatch(t *testing.T) {
	manifest, binding := boundPaneProcessFixture(t, "worker-capture-bound")
	groupPID, err := syscall.Getpgid(0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := WriteImplementationGroupStart(manifest, binding, "interactive", groupPID, os.Getpid()); err == nil {
		t.Fatal("capture binding accepted interactive group start")
	}
	record := ImplementationGroupStart{Version: 1, Binding: binding, Role: "interactive", GroupPID: groupPID, WorkerPID: os.Getpid()}
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeImmutableImplementationGroup(implementationGroupPath(manifest, "interactive", "start"), body); err != nil {
		t.Fatal(err)
	}
	if gone, err := ImplementationWorkerGone(manifest, binding); err == nil || gone {
		t.Fatalf("mismatched durable group role certified capture absent: %t, %v", gone, err)
	}
}

func TestBoundDetachedChildProcessHelper(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_BOUND_DETACHED_HELPER") != "1" {
		return
	}
	detached := exec.Command("/bin/sleep", "30")
	detached.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	if err := detached.Start(); err != nil {
		os.Exit(125)
	}
	_, _ = fmt.Fprintf(os.Stdout, "%d\n", detached.Process.Pid)
	_ = detached.Wait()
}

func TestBoundCleanupRejectsEscapedWorkerChild(t *testing.T) {
	manifest, binding := boundPaneProcessFixture(t, "pane-exit-status-bound")
	t.Setenv("AGENT_SYMPHONY_BOUND_DETACHED_HELPER", "1")
	reader, writer := io.Pipe()
	defer reader.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		_, _, err := RunBoundPaneCommand(ctx, manifest, "/usr/bin/true", []string{os.Args[0], "-test.run=^TestBoundDetachedChildProcessHelper$"}, nil, writer, io.Discard)
		finished <- err
		_ = writer.Close()
	}()
	line, err := bufio.NewReader(reader).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	detachedPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || detachedPID < 2 {
		t.Fatalf("detached worker PID %q: %v", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(detachedPID, syscall.SIGKILL) })
	start, err := readImplementationGroup(manifest, binding, "interactive", "start")
	if err != nil {
		t.Fatal(err)
	}
	if group, err := syscall.Getpgid(detachedPID); err != nil || group == start.GroupPID {
		t.Fatalf("fixture did not escape bound group: detached group=%d original=%d err=%v", group, start.GroupPID, err)
	}
	cancel()
	if err := <-finished; err != nil {
		t.Fatalf("bound group cleanup failed: %v", err)
	}
	if err := syscall.Kill(detachedPID, 0); err != nil {
		t.Fatalf("detached worker unexpectedly gone; fixture cannot prove escape: %v", err)
	}
	if gone, err := ImplementationWorkerGone(manifest, binding); err == nil && gone {
		t.Fatal("cleanup falsely certified all worker descendants dead while detached child is live")
	}
}
