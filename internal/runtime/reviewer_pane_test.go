package runtime

import (
	"context"
	"errors"
	"fmt"
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
	outputReader, outputWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outputReader.Close()
	defer outputWriter.Close()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pidReady := make(chan int, 1)
	finished := make(chan error, 1)
	command := []string{"/bin/sh", "-c", `trap '' TERM HUP; printf ready > "$1"; IFS= read -r _ < "$2"; printf late > "$3"`, "reviewer", started, blocked, late}
	go func() {
		_, _, runErr := RunPaneCommandAfterStart(ctx, "tmux", command, nil, outputWriter, io.Discard, func(pid int) error {
			pidReady <- pid
			return nil
		}, gateReader)
		finished <- runErr
	}()
	select {
	case <-pidReady:
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
	if err := outputWriter.Close(); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled reviewer error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stubborn reviewer group did not terminate")
	}
	assertReviewerOutputClosed(t, outputReader)
	if _, err := os.Lstat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("reviewer wrote after cancellation: %v", err)
	}
}

func TestReviewerGroupAbsenceRequiresESRCH(t *testing.T) {
	for _, tc := range []struct {
		name   string
		probe  error
		proved bool
	}{
		{"absent", syscall.ESRCH, true},
		{"live or zombie", nil, false},
		{"permission denied", syscall.EPERM, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := reviewerGroupAbsenceError(tc.probe)
			if (err == nil) != tc.proved {
				t.Fatalf("group absence proved=%t for probe %v, want %t", err == nil, tc.probe, tc.proved)
			}
			if !tc.proved && !errors.Is(err, errReviewerGroupAbsenceUnproved) {
				t.Fatalf("unproved probe error = %v", err)
			}
		})
	}
}

func TestReviewerNormalExitKillsLateWritingDescendant(t *testing.T) {
	root := t.TempDir()
	started, blocked := filepath.Join(root, "started"), filepath.Join(root, "blocked")
	barrier, release := filepath.Join(root, "barrier"), filepath.Join(root, "release")
	observed, late := filepath.Join(root, "observed"), filepath.Join(root, "late")
	for _, path := range []string{started, blocked, barrier, release} {
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
	outputReader, outputWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer outputReader.Close()
	defer outputWriter.Close()
	if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
		t.Fatal(err)
	}
	type result struct {
		code   int
		signal syscall.Signal
		err    error
	}
	finished := make(chan result, 1)
	launched := make(chan struct{}, 1)
	script := `(/bin/sh -c 'printf ready > "$1"; IFS= read -r _ < "$2"; printf late > "$3"' child "$1" "$2" "$3") & IFS= read -r _ < "$1"; printf observed > "$4"; printf ready > "$5"; IFS= read -r _ < "$6"`
	go func() {
		code, signal, err := RunPaneCommandAfterStart(t.Context(), tmux, []string{"/bin/sh", "-c", script, "reviewer", started, blocked, late, observed, barrier, release}, nil, outputWriter, io.Discard, func(int) error {
			launched <- struct{}{}
			return nil
		}, gateReader)
		finished <- result{code, signal, err}
	}()
	barrierReady := make(chan error, 1)
	go func() {
		file, err := os.OpenFile(barrier, os.O_RDONLY, 0)
		if err == nil {
			var body []byte
			body, err = io.ReadAll(file)
			err = errors.Join(err, file.Close())
			if err == nil && string(body) != "ready" {
				err = errors.New("reviewer did not reach normal-exit barrier")
			}
		}
		barrierReady <- err
	}()
	select {
	case err := <-barrierReady:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reviewer did not reach normal-exit barrier")
	}
	select {
	case <-launched:
	case <-time.After(5 * time.Second):
		t.Fatal("reviewer wrapper did not start")
	}
	if err := outputWriter.Close(); err != nil {
		t.Fatal(err)
	}
	released := make(chan error, 1)
	go func() {
		file, err := os.OpenFile(release, os.O_WRONLY, 0)
		if err == nil {
			_, err = io.WriteString(file, "go\n")
			err = errors.Join(err, file.Close())
		}
		released <- err
	}()
	select {
	case err := <-released:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reviewer did not accept normal-exit release")
	}
	select {
	case got := <-finished:
		if got.code != 0 || got.signal != 0 || (got.err != nil && !errors.Is(got.err, errReviewerGroupAbsenceUnproved)) {
			t.Fatalf("reviewer exit=%d signal=%d err=%v", got.code, got.signal, got.err)
		}
		if got.err != nil {
			t.Logf("controlled writer exited; process-group absence remains unproved: %v", got.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reviewer did not finish after normal-exit release")
	}
	assertReviewerOutputClosed(t, outputReader)
	if body, err := os.ReadFile(observed); err != nil || string(body) != "observed" {
		t.Fatalf("reviewer parent did not observe live descendant: %q %v", body, err)
	}
	if _, err := os.Lstat(late); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("descendant wrote after reviewer completion: %v", err)
	}
}

func TestReviewerUnprovedGroupPreservesCommandAndPaneExit(t *testing.T) {
	root := t.TempDir()
	tmux := filepath.Join(root, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$AS_TEST_TMUX_ARGS\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	argsPath := filepath.Join(root, "tmux-args")
	t.Setenv("AS_TEST_TMUX_ARGS", argsPath)
	t.Setenv("TMUX_PANE", "%123")
	for _, exitCode := range []int{0, 7} {
		t.Run(fmt.Sprint(exitCode), func(t *testing.T) {
			gateReader, gateWriter, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			defer gateReader.Close()
			defer gateWriter.Close()
			if _, err := io.WriteString(gateWriter, "go\n"); err != nil {
				t.Fatal(err)
			}
			code, signal, err := runReviewerPaneCommand(t.Context(), tmux, []string{"/bin/sh", "-c", "exit " + fmt.Sprint(exitCode)}, nil, io.Discard, io.Discard, nil, []*os.File{gateReader}, func(int) error { return nil })
			if code != exitCode || signal != 0 || !errors.Is(err, errReviewerGroupAbsenceUnproved) {
				t.Fatalf("reviewer command code=%d signal=%d cleanup=%v, want code=%d and typed unproved", code, signal, err, exitCode)
			}
			args, err := os.ReadFile(argsPath)
			want := fmt.Sprintf("set-option\n-p\n-t\n%%123\n%s\n%d\n", PaneExitStatusOption, exitCode)
			if err != nil || string(args) != want {
				t.Fatalf("recorded pane exit option = %q, %v; want %q", args, err, want)
			}
		})
	}
}

func assertReviewerOutputClosed(t *testing.T, reader *os.File) {
	t.Helper()
	closed := make(chan error, 1)
	go func() {
		body, err := io.ReadAll(reader)
		if err == nil && len(body) != 0 {
			err = errors.New("reviewer wrote unexpected output")
		}
		closed <- err
	}()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("reviewer output: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("reviewer output writer remained open after completion")
	}
}
