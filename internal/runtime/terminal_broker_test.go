package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestTerminalBrokerIdentityFailureKillsExactGatedChild(t *testing.T) {
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sh", "-c", terminalBrokerWrapper, "terminal-broker", "/bin/cat")
	command.ExtraFiles = []*os.File{gateReader, holdReader}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = gateReader.Close()
	_ = holdReader.Close()
	done := make(chan error, 1)
	go func() {
		stopUnverifiedTerminalBrokerChild(command, gateWriter, holdWriter)
		done <- nil
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("exact gated child did not exit after identity failure cleanup")
	}
}

func TestTerminalBrokerWrapperPinsGroupAfterCommandExit(t *testing.T) {
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer gateWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer holdWriter.Close()
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer statusReader.Close()
	command := exec.Command("/bin/sh", "-c", terminalBrokerWrapper, "terminal-broker", "/bin/sh", "-c", "exit 0")
	command.ExtraFiles = []*os.File{gateReader, holdReader, statusWriter}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = gateReader.Close()
	_ = holdReader.Close()
	_ = statusWriter.Close()
	if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
		t.Fatal(err)
	}
	_ = gateWriter.Close()
	if status, err := readTerminalBrokerWorkerStatus(statusReader); err != nil || status != 0 {
		t.Fatalf("worker status=%d err=%v", status, err)
	}
	if err := syscall.Kill(-command.Process.Pid, 0); err != nil {
		t.Fatalf("process group identity was not pinned after command exit: %v", err)
	}
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = holdWriter.Close()
	if err := command.Wait(); err == nil {
		t.Fatal("group anchor was not killed")
	}
	terminated, err := implementationGroupTerminated(command.Process.Pid)
	if err != nil || !terminated {
		t.Fatalf("pinned process group terminated=%t err=%v", terminated, err)
	}
}

func TestTerminalBrokerWrapperAnchorSurvivesGracefulGroupStop(t *testing.T) {
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer gateWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer holdWriter.Close()
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer statusReader.Close()
	ready := filepath.Join(t.TempDir(), "ready")
	command := exec.Command("/bin/sh", "-c", terminalBrokerWrapper, "terminal-broker", "/bin/sh", "-c", `trap 'exit 0' TERM; : >"$1"; while :; do sleep 1; done`, "reviewer", ready)
	command.ExtraFiles = []*os.File{gateReader, holdReader, statusWriter}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = gateReader.Close()
	_ = holdReader.Close()
	_ = statusWriter.Close()
	t.Cleanup(func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_ = holdWriter.Close()
	})
	if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
		t.Fatal(err)
	}
	_ = gateWriter.Close()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		} else if !errors.Is(err, os.ErrNotExist) {
			t.Fatal(err)
		}
		if time.Now().After(deadline) {
			t.Fatal("reviewer did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGTERM); err != nil {
		t.Fatal(err)
	}
	if status, err := readTerminalBrokerWorkerStatus(statusReader); err != nil || status != 0 {
		t.Fatalf("worker status=%d err=%v", status, err)
	}
	if err := syscall.Kill(-command.Process.Pid, 0); err != nil && !errors.Is(err, syscall.EPERM) {
		t.Fatalf("graceful group stop removed the process-group anchor: %v", err)
	}
	if err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = holdWriter.Close()
	if err := command.Wait(); err == nil {
		t.Fatal("group anchor was not killed")
	}
	terminated, err := implementationGroupTerminated(command.Process.Pid)
	if err != nil || !terminated {
		t.Fatalf("pinned process group terminated=%t err=%v", terminated, err)
	}
}

func TestTerminalBrokerWrapperPinsGroupAfterWorkerSelfKill(t *testing.T) {
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer gateWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer holdWriter.Close()
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer statusReader.Close()
	command := exec.Command("/bin/sh", "-c", terminalBrokerWrapper, "terminal-broker", "/bin/sh", "-c", `group=$(ps -o pgid= -p $$); kill -KILL -"$group"`)
	command.ExtraFiles = []*os.File{gateReader, holdReader, statusWriter}
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	_ = gateReader.Close()
	_ = holdReader.Close()
	_ = statusWriter.Close()
	t.Cleanup(func() {
		_ = syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		_ = holdWriter.Close()
	})
	if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
		t.Fatal(err)
	}
	_ = gateWriter.Close()
	if _, err := readTerminalBrokerWorkerStatus(statusReader); err == nil {
		t.Fatal("self-killed worker reported a normal status")
	}
	if err := syscall.Kill(-command.Process.Pid, 0); err != nil && !errors.Is(err, syscall.EPERM) {
		t.Fatalf("unreaped group leader did not pin the self-killed PGID: %v", err)
	}
	_ = holdWriter.Close()
	if err := command.Wait(); err == nil {
		t.Fatal("self-killed group leader reported success")
	}
	terminated, err := implementationGroupTerminated(command.Process.Pid)
	if err != nil || !terminated {
		t.Fatalf("pinned process group terminated=%t err=%v", terminated, err)
	}
}

func startTerminalBrokerFixture(t *testing.T, command ...string) (TerminalBrokerBinding, string, <-chan error, context.CancelFunc) {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "as-terminal-broker-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	record := filepath.Join(root, "broker.json")
	sockets := filepath.Join(root, "sockets")
	if err := os.Mkdir(sockets, 0o700); err != nil {
		t.Fatal(err)
	}
	outerReader, outerWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		outerReader.Close()
		outerWriter.Close()
	})
	started := make(chan TerminalBrokerBinding, 1)
	done := make(chan error, 1)
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	go func() {
		_, _, err := RunTerminalBroker(ctx, record, sockets, command, outerReader, io.Discard, io.Discard, func(binding TerminalBrokerBinding) error {
			started <- binding
			return nil
		})
		done <- err
	}()
	select {
	case binding := <-started:
		return binding, record, done, cancel
	case err := <-done:
		t.Fatalf("terminal broker failed to start: %v", err)
		return TerminalBrokerBinding{}, "", nil, nil
	case <-time.After(5 * time.Second):
		t.Fatal("terminal broker did not start")
		return TerminalBrokerBinding{}, "", nil, nil
	}
}

func TestTerminalBrokerStartedFailureRecordsExactDeath(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "as-terminal-broker-start-failure-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	record, sockets := filepath.Join(root, "broker.json"), filepath.Join(root, "sockets")
	if err := os.Mkdir(sockets, 0o700); err != nil {
		t.Fatal(err)
	}
	outerReader, outerWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outerReader.Close()
	defer outerWriter.Close()
	var binding TerminalBrokerBinding
	code, _, err := RunTerminalBroker(t.Context(), record, sockets, []string{"/bin/cat"}, outerReader, io.Discard, io.Discard, func(started TerminalBrokerBinding) error {
		binding = started
		return errors.New("owner persistence failed")
	})
	if code != 125 || err == nil || !strings.Contains(err.Error(), "owner persistence failed") {
		t.Fatalf("code=%d err=%v", code, err)
	}
	if stored, readErr := ReadTerminalBrokerBinding(record); readErr != nil || stored != binding {
		t.Fatalf("stored=%#v binding=%#v err=%v", stored, binding, readErr)
	}
	if dead, deadErr := TerminalBrokerDead(record, binding); deadErr != nil || !dead {
		t.Fatalf("dead=%t err=%v", dead, deadErr)
	}
}

func TestTerminalBrokerReleaseFailureIsStable(t *testing.T) {
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	reader.Close()
	writer.Close()
	broker := &terminalBroker{gate: writer}
	first, second := broker.releaseInner(), broker.releaseInner()
	if first == nil || second == nil || first.Error() != second.Error() {
		t.Fatalf("release errors changed: first=%v second=%v", first, second)
	}
}

func TestTerminalBrokerStopWaitsForExactDeathProof(t *testing.T) {
	binding, record, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `trap '' TERM HUP; while :; do sleep 1; done`)
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	if err := StopAndProveTerminalBroker(t.Context(), record, binding); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTerminalBrokerWorkerSelfKillRecordsExactDeath(t *testing.T) {
	binding, record, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `group=$(ps -o pgid= -p $$); kill -KILL -"$group"`)
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("self-killed worker group was not reaped")
	}
	if dead, err := TerminalBrokerDead(record, binding); err != nil || !dead {
		t.Fatalf("dead=%t err=%v", dead, err)
	}
}

func TestTerminalBrokerReplayInputReconnectAndExactStop(t *testing.T) {
	binding, record, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `printf 'READY\n'; while IFS= read -r line; do printf 'ECHO:%s\n' "$line"; done`)
	if stored, err := ReadTerminalBrokerBinding(record); err != nil || stored != binding {
		t.Fatalf("stored binding=%#v err=%v", stored, err)
	}
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	read := func(client *TerminalBrokerClient, marker string) {
		t.Helper()
		result := new(bytes.Buffer)
		for !strings.Contains(result.String(), marker) {
			chunk, err := client.ReadOutput()
			if err != nil {
				t.Fatalf("read %q: %v output=%q", marker, err, result.String())
			}
			result.Write(chunk)
		}
	}
	first, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Start(); err != nil {
		t.Fatal(err)
	}
	read(first, "READY")
	if err := first.WriteInput([]byte("one\n")); err != nil {
		t.Fatal(err)
	}
	read(first, "ECHO:one")
	first.Close()
	second, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Start(); err != nil {
		t.Fatal(err)
	}
	read(second, "ECHO:one")
	second.Close()
	if err := StopTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("terminal broker did not stop")
	}
	if dead, err := TerminalBrokerDead(record, binding); err != nil || !dead {
		t.Fatalf("dead=%t err=%v", dead, err)
	}
}

func TestTerminalBrokerDeliversRapidExitTail(t *testing.T) {
	for range 10 {
		binding, _, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `printf 'FINAL-TAIL\n'`)
		client, err := DialTerminalBroker(t.Context(), binding)
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Start(); err != nil {
			t.Fatal(err)
		}
		if err := client.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
			t.Fatal(err)
		}
		if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
			t.Fatal(err)
		}
		var output bytes.Buffer
		for {
			chunk, err := client.ReadOutput()
			if err != nil {
				break
			}
			output.Write(chunk)
		}
		_ = client.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(output.String(), "FINAL-TAIL") {
			t.Fatalf("rapid-exit terminal tail was lost: %q", output.String())
		}
	}
}

func TestTerminalBrokerDrainsLateAttachReplay(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "as-terminal-replay-drain-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	trigger := filepath.Join(root, "trigger")
	command := []string{"/bin/sh", "-c", `head -c $((3 * 1024 * 1024)) /dev/zero; printf 'REPLAY-END\n'; while [ ! -e "$1" ]; do sleep 0.01; done`, "broker", trigger}
	binding, _, done, _ := startTerminalBrokerFixture(t, command...)
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	probe, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := probe.Start(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	for !strings.Contains(output.String(), "REPLAY-END") {
		chunk, err := probe.ReadOutput()
		if err != nil {
			t.Fatal(err)
		}
		output.Write(chunk)
	}
	probe.Close()

	client, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(trigger, []byte("go\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	output.Reset()
	for !strings.Contains(output.String(), "REPLAY-END") {
		chunk, err := client.ReadOutput()
		if err != nil {
			t.Fatalf("late attach lost replay during broker exit: %v", err)
		}
		output.Write(chunk)
	}
	client.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTerminalBrokerRejectsAttachAfterDrainStarts(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "as-terminal-drain-admission-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	trigger := filepath.Join(root, "trigger")
	command := []string{"/bin/sh", "-c", `head -c $((1024 * 1024)) /dev/zero; printf 'REPLAY-READY\n'; while [ ! -e "$1" ]; do sleep 0.01; done`, "broker", trigger}
	binding, _, done, _ := startTerminalBrokerFixture(t, command...)
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	probe, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := probe.Start(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	for !strings.Contains(output.String(), "REPLAY-READY") {
		chunk, err := probe.ReadOutput()
		if err != nil {
			t.Fatal(err)
		}
		output.Write(chunk)
	}
	probe.Close()
	held, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(trigger, []byte("go\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		client, err := DialTerminalBroker(t.Context(), binding)
		if client != nil {
			client.Close()
		}
		if err != nil && strings.Contains(err.Error(), "rejected attachment") {
			break
		}
		if time.Now().After(deadline) {
			held.Close()
			t.Fatalf("terminal broker did not close attach admission: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	held.Close()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTerminalBrokerResizeAndInputLimit(t *testing.T) {
	binding, _, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `printf 'READY\n'; while IFS= read -r line; do stty size; printf 'ECHO:%s\n' "$line"; done`)
	defer func() {
		_ = StopTerminalBroker(context.Background(), binding)
		<-done
	}()
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	client, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	readUntil := func(marker string) {
		t.Helper()
		var output bytes.Buffer
		for !strings.Contains(output.String(), marker) {
			chunk, err := client.ReadOutput()
			if err != nil {
				t.Fatalf("read %q: %v output=%q", marker, err, output.String())
			}
			output.Write(chunk)
		}
	}
	readUntil("READY")
	if err := client.Resize(123, 45); err != nil {
		t.Fatal(err)
	}
	if err := client.WriteInput(make([]byte, terminalFrameLimit+1)); err == nil || !strings.Contains(err.Error(), "exceeds limit") {
		t.Fatalf("oversized input err=%v", err)
	}
	if err := client.WriteInput([]byte("after-resize\n")); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	for !strings.Contains(output.String(), "45 123") || !strings.Contains(output.String(), "ECHO:after-resize") {
		chunk, err := client.ReadOutput()
		if err != nil {
			t.Fatalf("read resized output: %v output=%q", err, output.String())
		}
		output.Write(chunk)
	}
}

func TestTerminalBrokerRejectsSocketIdentityAndPeerMismatch(t *testing.T) {
	binding, _, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `while :; do sleep 1; done`)
	defer func() {
		_ = StopTerminalBroker(context.Background(), binding)
		<-done
	}()
	connection, err := net.Dial("unix", binding.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	unix := connection.(*net.UnixConn)
	if err := verifyTerminalPeerPID(unix, binding.OuterPID+1); err == nil {
		t.Fatal("accepted mismatched peer PID")
	}
	unix.Close()
	tampered := binding
	tampered.SocketIno++
	if _, err := DialTerminalBroker(t.Context(), tampered); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("replacement err=%v", err)
	}
}

func TestTerminalBrokerRejectsOverflowedLateAttach(t *testing.T) {
	command := []string{"/bin/sh", "-c", `head -c $((5 * 1024 * 1024)) /dev/zero; while :; do sleep 1; done`}
	binding, _, done, _ := startTerminalBrokerFixture(t, command...)
	defer func() {
		_ = StopTerminalBroker(context.Background(), binding)
		<-done
	}()
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		client, err := DialTerminalBroker(t.Context(), binding)
		if err != nil && strings.Contains(err.Error(), "replay limit exceeded") {
			break
		}
		if client != nil {
			client.Close()
		}
		if time.Now().After(deadline) {
			t.Fatalf("late attach was not rejected: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTerminalBrokerKeepsPreOverflowAdmissionComplete(t *testing.T) {
	root, err := os.MkdirTemp("/tmp", "as-terminal-admission-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	trigger := filepath.Join(root, "trigger")
	command := []string{"/bin/sh", "-c", `head -c $((4 * 1024 * 1024 - 512 * 1024)) /dev/zero; printf 'PHASE-ONE\n'; while [ ! -e "$1" ]; do sleep 0.01; done; head -c $((600 * 1024)) /dev/zero; printf 'ADMITTED-DONE\n'; exec sleep 30`, "broker", trigger}
	binding, _, done, _ := startTerminalBrokerFixture(t, command...)
	defer func() {
		_ = StopTerminalBroker(context.Background(), binding)
		<-done
	}()
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		probe, err := DialTerminalBroker(t.Context(), binding)
		if err != nil {
			t.Fatal(err)
		}
		if err := probe.Start(); err != nil {
			probe.Close()
			if time.Now().After(deadline) {
				t.Fatalf("broker did not settle its bounded replay prefix: %v", err)
			}
			continue
		}
		var prefix bytes.Buffer
		for !strings.Contains(prefix.String(), "PHASE-ONE") {
			chunk, err := probe.ReadOutput()
			if err != nil {
				break
			}
			prefix.Write(chunk)
		}
		probe.Close()
		if strings.Contains(prefix.String(), "PHASE-ONE") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("broker did not settle its bounded replay prefix")
		}
	}
	client, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if err := os.WriteFile(trigger, []byte("go\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(5 * time.Second)
	for {
		probe, err := DialTerminalBroker(t.Context(), binding)
		if probe != nil {
			probe.Close()
		}
		if err != nil && strings.Contains(err.Error(), "replay limit exceeded") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("broker did not cross the replay limit: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := client.Start(); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	for !strings.Contains(output.String(), "ADMITTED-DONE") {
		chunk, err := client.ReadOutput()
		if err != nil {
			t.Fatalf("pre-overflow admission lost its complete stream: %v", err)
		}
		output.Write(chunk)
	}
}

func TestTerminalBrokerSlowReaderDoesNotBlockOtherClients(t *testing.T) {
	binding, _, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `head -c $((12 * 1024 * 1024)) /dev/zero; printf 'FAST-CLIENT-DONE\n'`)
	slow, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	defer slow.Close()
	if err := slow.Start(); err != nil {
		t.Fatal(err)
	}
	fast, err := DialTerminalBroker(t.Context(), binding)
	if err != nil {
		t.Fatal(err)
	}
	defer fast.Close()
	if err := fast.Start(); err != nil {
		t.Fatal(err)
	}
	if err := ReleaseTerminalBroker(t.Context(), binding); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		chunk, err := fast.ReadOutput()
		if err != nil {
			t.Fatalf("fast client disconnected behind slow reader: %v", err)
		}
		if bytes.Contains(chunk, []byte("FAST-CLIENT-DONE")) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("slow reader blocked broker output to the fast client")
		}
	}
	if err := slow.conn.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	for {
		if _, err := slow.ReadOutput(); err != nil {
			var networkError net.Error
			if errors.As(err, &networkError) && networkError.Timeout() {
				t.Fatal("slow reader was not evicted after exceeding its queue")
			}
			break
		}
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow reader prevented the broker from reaping its child")
	}
}

func TestTerminalBrokerRejectsSameUIDSocketReplacementAndLeavesItAlive(t *testing.T) {
	binding, _, done, cancel := startTerminalBrokerFixture(t, "/bin/sh", "-c", `while :; do sleep 1; done`)
	if err := os.Remove(binding.SocketPath); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnix("unix", &net.UnixAddr{Name: binding.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer replacement.Close()
	if err := os.Chmod(binding.SocketPath, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := DialTerminalBroker(t.Context(), binding); err == nil || !strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("replacement socket was admitted: %v", err)
	}
	accepted := make(chan error, 1)
	go func() {
		conn, acceptErr := replacement.AcceptUnix()
		if conn != nil {
			_ = conn.Close()
		}
		accepted <- acceptErr
	}()
	connection, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: binding.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("replacement socket did not survive rejection: %v", err)
	}
	_ = connection.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("replacement socket did not remain live: %v", err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original broker did not stop")
	}
}

func TestTerminalBrokerRejectsReplacementByKernelPeerPID(t *testing.T) {
	if runtime.GOOS != "darwin" && runtime.GOOS != "linux" {
		t.Skip("kernel peer PID verification is unsupported")
	}
	binding, _, done, cancel := startTerminalBrokerFixture(t, "/bin/sh", "-c", `while :; do sleep 1; done`)
	if err := os.Remove(binding.SocketPath); err != nil {
		t.Fatal(err)
	}
	ready := binding.SocketPath + ".ready"
	ctx, stopHelper := context.WithTimeout(t.Context(), 10*time.Second)
	defer stopHelper()
	helper := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestTerminalBrokerReplacementPeerHelper$")
	helper.Env = append(os.Environ(), "AGENT_SYMPHONY_TERMINAL_REPLACEMENT_HELPER=1", "AGENT_SYMPHONY_TERMINAL_REPLACEMENT_SOCKET="+binding.SocketPath, "AGENT_SYMPHONY_TERMINAL_REPLACEMENT_READY="+ready)
	if err := helper.Start(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, err := os.Stat(ready); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("replacement helper did not become ready")
		}
		time.Sleep(10 * time.Millisecond)
	}
	info, err := os.Lstat(binding.SocketPath)
	if err != nil {
		t.Fatal(err)
	}
	binding.SocketDev, binding.SocketIno, err = terminalSocketIdentity(info)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := DialTerminalBroker(t.Context(), binding); err == nil || !strings.Contains(err.Error(), "peer PID mismatch") {
		t.Fatalf("replacement peer PID was admitted: %v", err)
	}
	connection, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: binding.SocketPath, Net: "unix"})
	if err != nil {
		t.Fatalf("replacement did not survive peer rejection: %v", err)
	}
	_ = connection.Close()
	if err := helper.Wait(); err != nil {
		t.Fatalf("replacement helper: %v", err)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestTerminalBrokerReplacementPeerHelper(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_TERMINAL_REPLACEMENT_HELPER") != "1" {
		return
	}
	path, ready := os.Getenv("AGENT_SYMPHONY_TERMINAL_REPLACEMENT_SOCKET"), os.Getenv("AGENT_SYMPHONY_TERMINAL_REPLACEMENT_READY")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(path, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(ready, []byte("ready\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		connection, err := listener.AcceptUnix()
		if err != nil {
			t.Fatal(err)
		}
		_ = connection.Close()
	}
}

func TestTerminalBrokerMalformedBindingFailsClosed(t *testing.T) {
	_, err := DialTerminalBroker(t.Context(), TerminalBrokerBinding{})
	if err == nil || !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("malformed binding err=%v", err)
	}
}
