package runtime

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
	binding, _, done, _ := startTerminalBrokerFixture(t, "/bin/sh", "-c", `head -c $((3 * 1024 * 1024)) /dev/zero; printf 'FAST-CLIENT-DONE\n'`)
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

func TestTerminalBrokerMalformedBindingFailsClosed(t *testing.T) {
	_, err := DialTerminalBroker(t.Context(), TerminalBrokerBinding{})
	if err == nil || !errors.Is(err, os.ErrNotExist) && !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("malformed binding err=%v", err)
	}
}
