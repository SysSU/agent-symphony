package runtime

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/creack/pty"
)

func TestTerminalBrokerPTYCanKeepWorkerInBoundProcessGroup(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if err := pty.Setsize(master, &pty.Winsize{Rows: 22, Cols: 44}); err != nil {
		t.Fatal(err)
	}
	command := exec.Command("/bin/sh", "-c", `printf 'INNER:%s:%s\n' "$(tty)" "$(stty size)"; if (exec 3</dev/tty) 2>/dev/null; then printf 'OUTER:%s:%s\n' "$(tty </dev/tty)" "$(stty size </dev/tty)"; else echo NO_CONTROLLING_TTY; fi; IFS= read -r line; printf 'RECEIVED:%s\n' "$line"`)
	command.Stdin, command.Stdout, command.Stderr = slave, slave, slave
	if err := command.Start(); err != nil {
		slave.Close()
		t.Fatal(err)
	}
	slave.Close()
	defer command.Process.Kill()
	parentGroup, err := syscall.Getpgid(os.Getpid())
	if err != nil {
		t.Fatal(err)
	}
	childGroup, err := syscall.Getpgid(command.Process.Pid)
	if err != nil || childGroup != parentGroup {
		t.Fatalf("child group=%d parent group=%d err=%v", childGroup, parentGroup, err)
	}
	reader := bufio.NewReader(master)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		t.Log(strings.TrimSpace(line))
		if strings.Contains(line, "NO_CONTROLLING_TTY") || strings.Contains(line, "OUTER:") {
			break
		}
	}
	if _, err := master.Write([]byte("broker-input\n")); err != nil {
		t.Fatal(err)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(line, "RECEIVED:broker-input") {
			break
		}
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
}

func TestTerminalBrokerPTYWithExistingForegroundWorkerAttributes(t *testing.T) {
	master, slave, err := pty.Open()
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	defer slave.Close()
	worker := exec.Command("/bin/sh", "-c", "exit 0")
	worker.Stdin, worker.Stdout, worker.Stderr = slave, slave, slave
	worker.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Foreground: true, Ctty: 0}
	err = worker.Run()
	t.Logf("existing foreground worker attributes with broker slave: %v", err)
	if err != nil {
		t.Fatalf("existing bound interactive worker cannot start on broker slave: %v", err)
	}
}

// This is a transport probe, not a production broker. The child deliberately
// gets its own controlling PTY/session; exact dual-group cleanup is unproven.
func TestTerminalBrokerNativePTYPrototype(t *testing.T) {
	if socket := os.Getenv("AS311_BROKER_SOCKET"); socket != "" {
		runTerminalBrokerPrototype(t, socket, os.Getenv("AS311_BROKER_TOKEN"), os.Getenv("AS311_BROKER_TMUX"))
		return
	}
	tmuxBinary, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	root, err := os.MkdirTemp("/tmp", "as311-native-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	tmuxSocket, brokerSocket := filepath.Join(root, "tmux.sock"), filepath.Join(root, "broker.sock")
	tmux := filepath.Join(root, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nexec "+strconv.Quote(tmuxBinary)+" -S "+strconv.Quote(tmuxSocket)+" -f /dev/null \"$@\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) string {
		t.Helper()
		output, err := exec.Command(tmux, args...).CombinedOutput()
		if err != nil {
			t.Fatalf("tmux %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	run("new-session", "-d", "-s", "keeper", "cat")
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	run("wait-for", "-L", "as311-broker-ready")
	token, err := NewLaunchToken()
	if err != nil {
		t.Fatal(err)
	}
	helper := filepath.Join(root, "helper")
	script := "#!/bin/sh\nAS311_BROKER_SOCKET=" + strconv.Quote(brokerSocket) + " AS311_BROKER_TOKEN=" + strconv.Quote(token) + " AS311_BROKER_TMUX=" + strconv.Quote(tmux) + " exec " + strconv.Quote(os.Args[0]) + " -test.run '^TestTerminalBrokerNativePTYPrototype$' -test.v\n"
	if err := os.WriteFile(helper, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	run("new-session", "-d", "-x", "80", "-y", "24", "-s", "broker", helper)
	ready, cancelReady := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancelReady()
	if output, err := exec.CommandContext(ready, tmux, "wait-for", "-L", "as311-broker-ready").CombinedOutput(); err != nil {
		t.Fatalf("broker was not ready: %v: %s; pane=%s", err, output, run("capture-pane", "-p", "-t", "=broker:0.0"))
	}
	wrong, err := net.Dial("unix", brokerSocket)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(wrong, "wrong-token\n"); err != nil {
		t.Fatal(err)
	}
	_ = wrong.SetReadDeadline(time.Now().Add(3 * time.Second))
	var rejected [1]byte
	if _, err := wrong.Read(rejected[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("wrong nonce was not rejected: %v", err)
	}
	wrong.Close()
	client, err := net.Dial("unix", brokerSocket)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := io.WriteString(client, token+"\n"); err != nil {
		t.Fatal(err)
	}
	output := new(bytes.Buffer)
	readUntil := func(marker string) {
		t.Helper()
		_ = client.SetReadDeadline(time.Now().Add(8 * time.Second))
		chunk := make([]byte, 4096)
		for !strings.Contains(output.String(), marker) {
			n, err := client.Read(chunk)
			if err != nil {
				t.Fatalf("waiting for %q: %v; broker output=%q; pane=%s", marker, err, output.String(), run("capture-pane", "-p", "-t", "=broker:0.0"))
			}
			output.Write(chunk[:n])
		}
	}
	readUntil("RAW_WAIT") // READY was printed before this late connection.
	if !strings.Contains(output.String(), "READY:24 80") {
		t.Fatalf("late replay missed initial inner-PTY output: %q", output.String())
	}
	run("send-keys", "-t", "=broker:0.0", "-l", "x")
	readUntil("RAW_BYTE:x")
	run("wait-for", "-L", "as311-resized")
	run("resize-window", "-t", "=broker:0", "-x", "60", "-y", "16")
	resize, cancelResize := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancelResize()
	if output, err := exec.CommandContext(resize, tmux, "wait-for", "-L", "as311-resized").CombinedOutput(); err != nil {
		t.Fatalf("inner PTY did not inherit outer resize: %v: %s", err, output)
	}
	run("send-keys", "-t", "=broker:0.0", "-l", "size")
	run("send-keys", "-t", "=broker:0.0", "Enter")
	readUntil("SIZE:16 60")
	if _, err := io.WriteString(client, "browser-input\n"); err != nil {
		t.Fatal(err)
	}
	readUntil("BROWSER:browser-input")
	run("send-keys", "-t", "=broker:0.0", "C-c")
	readUntil("INT_OK")
	t.Logf("late replay and live inner-PTY output: %q", output.String())
}

func runTerminalBrokerPrototype(t *testing.T, socket, token, tmux string) {
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := os.Chmod(socket, 0o600); err != nil {
		t.Fatal(err)
	}
	save := exec.Command("stty", "-g")
	save.Stdin = os.Stdin
	saved, err := save.Output()
	if err != nil {
		t.Fatal(err)
	}
	raw := exec.Command("stty", "raw", "-echo")
	raw.Stdin = os.Stdin
	if err := raw.Run(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		restore := exec.Command("stty", strings.TrimSpace(string(saved)))
		restore.Stdin = os.Stdin
		_ = restore.Run()
	}()
	child := exec.Command("/bin/sh", "-c", `printf 'READY:%s\n' "$(stty size </dev/tty)"; stty raw -echo; printf 'RAW_WAIT\n'; key=$(dd bs=1 count=1 2>/dev/null); stty sane; printf 'RAW_BYTE:%s\n' "$key"; IFS= read -r line; printf 'SIZE:%s\n' "$(stty size </dev/tty)"; IFS= read -r line; printf 'BROWSER:%s\n' "$line"; trap 'printf "INT_OK\n"; exit 0' INT; printf 'INT_WAIT\n'; while :; do IFS= read -r line || :; done`)
	master, err := pty.StartWithSize(child, &pty.Winsize{Rows: 24, Cols: 80})
	if err != nil {
		t.Fatal(err)
	}
	defer master.Close()
	if group, err := syscall.Getpgid(child.Process.Pid); err != nil || group != child.Process.Pid {
		t.Fatalf("inner PTY did not create its own session/group: pid=%d pgid=%d err=%v", child.Process.Pid, group, err)
	}
	var mu sync.Mutex
	var replay bytes.Buffer
	var client net.Conn
	var ready sync.Once
	pumpDone := make(chan struct{})
	go func() {
		defer close(pumpDone)
		buffer := make([]byte, 4096)
		for {
			n, err := master.Read(buffer)
			if n > 0 {
				_, _ = os.Stdout.Write(buffer[:n])
				mu.Lock()
				replay.Write(buffer[:n])
				if client != nil {
					_, _ = client.Write(buffer[:n])
				}
				if bytes.Contains(replay.Bytes(), []byte("RAW_WAIT")) {
					ready.Do(func() { _ = exec.Command(tmux, "wait-for", "-U", "as311-broker-ready").Run() })
				}
				mu.Unlock()
			}
			if err != nil {
				return
			}
		}
	}()
	go func() { _, _ = io.Copy(master, os.Stdin) }()
	winsize := make(chan os.Signal, 1)
	signal.Notify(winsize, syscall.SIGWINCH)
	defer signal.Stop(winsize)
	go func() {
		for range winsize {
			if pty.InheritSize(os.Stdin, master) == nil {
				if rows, cols, err := pty.Getsize(master); err == nil && rows == 16 && cols == 60 {
					_ = exec.Command(tmux, "wait-for", "-U", "as311-resized").Run()
				}
			}
		}
	}()
	for {
		connection, err := listener.Accept()
		if err != nil {
			t.Fatal(err)
		}
		_ = connection.SetReadDeadline(time.Now().Add(3 * time.Second))
		reader := bufio.NewReader(connection)
		proof, err := reader.ReadString('\n')
		if err != nil || strings.TrimSpace(proof) != token {
			connection.Close()
			continue
		}
		_ = connection.SetReadDeadline(time.Time{})
		mu.Lock()
		_, err = io.Copy(connection, bytes.NewReader(replay.Bytes()))
		if err == nil {
			client = connection
		}
		mu.Unlock()
		if err != nil {
			connection.Close()
			continue
		}
		go func() { _, _ = io.Copy(master, reader) }()
		break
	}
	if err := child.Wait(); err != nil {
		t.Fatal(fmt.Errorf("inner child: %w", err))
	}
	<-pumpDone
	mu.Lock()
	if client != nil {
		client.Close()
	}
	mu.Unlock()
}
