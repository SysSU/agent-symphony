package runtime

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestReviewerGateAndCancellationKillStubbornGroup(t *testing.T) {
	root := t.TempDir()
	started, blocked := filepath.Join(root, "started"), filepath.Join(root, "blocked")
	late := filepath.Join(root, "late")
	for _, path := range []string{started, blocked} {
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer gateReader.Close()
	defer gateWriter.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pidReady := make(chan int, 1)
	finished := make(chan error, 1)
	command := []string{"/bin/sh", "-c", `trap '' TERM HUP; printf ready > "$1"; IFS= read -r _ < "$2"; printf late > "$3"`, "reviewer", started, blocked, late}
	go func() {
		_, _, runErr := RunPaneCommandAfterStart(ctx, "tmux", command, nil, io.Discard, io.Discard, func(pid int) error {
			pidReady <- pid
			return nil
		}, gateReader)
		finished <- runErr
	}()
	var groupPID int
	select {
	case groupPID = <-pidReady:
	case <-time.After(5 * time.Second):
		t.Fatal("reviewer gate child did not start")
	}
	if _, err := os.Lstat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer ran before owner opened gate: %v", err)
	}
	startedReader := make(chan *os.File, 1)
	go func() { file, _ := os.OpenFile(started, os.O_RDONLY, 0); startedReader <- file }()
	if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
		t.Fatal(err)
	}
	select {
	case file := <-startedReader:
		if file == nil {
			t.Fatal("reviewer did not reach start barrier")
		}
		if body, err := io.ReadAll(file); err != nil || string(body) != "ready" {
			t.Fatalf("reviewer start barrier = %q, %v", body, err)
		}
		file.Close()
	case <-time.After(5 * time.Second):
		t.Fatal("reviewer did not reach start barrier")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("stubborn reviewer group did not terminate")
	}
	if err := syscall.Kill(-groupPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("reviewer process group remains after cancellation: %v", err)
	}
	if _, err := os.Lstat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer wrote after cancellation: %v", err)
	}
}

func TestReviewerNormalExitKillsLateWritingDescendant(t *testing.T) {
	root := t.TempDir()
	started, blocked := filepath.Join(root, "started"), filepath.Join(root, "blocked")
	observed, late := filepath.Join(root, "observed"), filepath.Join(root, "late")
	for _, path := range []string{started, blocked} {
		if err := syscall.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	tmux := filepath.Join(root, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TMUX_PANE", "%123")
	gateReader, gateWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer gateReader.Close()
	defer gateWriter.Close()
	if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
		t.Fatal(err)
	}
	var groupPID int
	script := `(/bin/sh -c 'printf ready > "$1"; IFS= read -r _ < "$2"; printf late > "$3"' child "$1" "$2" "$3") & IFS= read -r _ < "$1"; printf observed > "$4"`
	code, signal, err := RunPaneCommandAfterStart(t.Context(), tmux, []string{"/bin/sh", "-c", script, "reviewer", started, blocked, late, observed}, nil, io.Discard, io.Discard, func(pid int) error {
		groupPID = pid
		return nil
	}, gateReader)
	if err != nil || code != 0 || signal != 0 {
		t.Fatalf("reviewer exit=%d signal=%d err=%v", code, signal, err)
	}
	if body, err := os.ReadFile(observed); err != nil || string(body) != "observed" {
		t.Fatalf("reviewer parent did not observe live descendant: %q %v", body, err)
	}
	if err := syscall.Kill(-groupPID, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("reviewer descendant group survives parent exit: %v", err)
	}
	if _, err := os.Lstat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant wrote after reviewer success: %v", err)
	}
}
