package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type forbiddenReviewerBoundary struct{ calls int }

func (b *forbiddenReviewerBoundary) call(context.Context, string, agentruntime.Command) (agentruntime.Result, error) {
	b.calls++
	return agentruntime.Result{}, errors.New("replayed reviewer attempted boundary work")
}

type directReviewerSessionBoundary struct {
	newSessionError bool
	setupError      bool
	requested       bool
	directCommand   bool
	killed          bool
}

func (b *directReviewerSessionBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary command")
	}
	switch command.Args[0] {
	case "display-message":
		return agentruntime.Result{Output: "||||||||||"}, nil
	case "wait-for":
		return agentruntime.Result{}, nil
	case "set-option":
		if len(command.Args) > 5 && command.Args[4] == ";" && command.Args[5] == "new-session" {
			b.requested = true
			b.directCommand = strings.Contains(strings.Join(command.Args, " "), " review-pane tmux ")
			if b.newSessionError {
				return agentruntime.Result{}, errors.New("tmux new-session response failed")
			}
			return agentruntime.Result{}, nil
		}
		if b.setupError {
			return agentruntime.Result{}, errors.New("tmux set-option denied")
		}
		return agentruntime.Result{}, nil
	case "kill-session":
		b.killed = true
		return agentruntime.Result{}, nil
	default:
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary command")
	}
}

type reviewerSessionStopBoundary struct {
	status    agentruntime.Result
	err       error
	killErr   error
	killed    []string
	onKill    func() error
	inventory string
}

type tmuxReviewerGuardBoundary struct {
	binary         string
	socket         string
	beforeGuard    func() error
	afterGuard     func() error
	maskMissing    bool
	inventoryError error
	lastInventory  string
}

func (b *tmuxReviewerGuardBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" {
		return agentruntime.Result{}, errors.New("unexpected tmux guard boundary")
	}
	if len(command.Args) > 0 && command.Args[0] == "if-shell" && b.beforeGuard != nil {
		if err := b.beforeGuard(); err != nil {
			return agentruntime.Result{}, err
		}
		b.beforeGuard = nil
	}
	if len(command.Args) > 0 && command.Args[0] == "list-sessions" && b.inventoryError != nil {
		b.lastInventory = ""
		return agentruntime.Result{Exited: true, Code: 1, Output: "server exited unexpectedly"}, b.inventoryError
	}
	cmd := exec.CommandContext(ctx, b.binary, append([]string{"-S", b.socket, "-f", "/dev/null"}, command.Args...)...)
	output, err := cmd.CombinedOutput()
	result := agentruntime.Result{Output: string(output), Exited: true}
	if exit, ok := err.(*exec.ExitError); ok {
		result.Code = exit.ExitCode()
	}
	if len(command.Args) > 0 && command.Args[0] == "if-shell" && err == nil && b.afterGuard != nil {
		if hookErr := b.afterGuard(); hookErr != nil {
			return agentruntime.Result{}, hookErr
		}
		b.afterGuard = nil
	}
	if len(command.Args) > 0 && command.Args[0] == "list-sessions" && err != nil && b.maskMissing {
		result.Output = "server exited unexpectedly"
	}
	if len(command.Args) > 0 && command.Args[0] == "list-sessions" {
		b.lastInventory = result.Output
	}
	return result, err
}

func reviewerGuardTmux(t *testing.T) (*tmuxReviewerGuardBoundary, func(...string) (string, error)) {
	t.Helper()
	binary, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	root, err := os.MkdirTemp("/tmp", "as-reviewer-guard-")
	if err != nil {
		t.Fatal(err)
	}
	boundary := &tmuxReviewerGuardBoundary{binary: binary, socket: filepath.Join(root, "tmux.sock")}
	run := func(args ...string) (string, error) {
		cmd := exec.Command(binary, append([]string{"-S", boundary.socket, "-f", "/dev/null"}, args...)...)
		output, err := cmd.CombinedOutput()
		return string(output), err
	}
	t.Cleanup(func() { _, _ = run("kill-server"); _ = os.RemoveAll(root) })
	return boundary, run
}

func waitReviewerTmuxSocketDisconnected(socket string) error {
	deadline := time.Now().Add(5 * time.Second)
	for {
		connection, err := net.DialTimeout("unix", socket, 100*time.Millisecond)
		if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ECONNREFUSED) {
			return nil
		}
		if err != nil && !os.IsTimeout(err) {
			return err
		}
		if connection != nil {
			_ = connection.Close()
		}
		if time.Now().After(deadline) {
			return errors.New("old tmux server still accepts connections after kill-server")
		}
		runtime.Gosched()
	}
}

func TestGuardedReviewerKillDoesNotTouchReplacementTmuxSession(t *testing.T) {
	for _, replaceServer := range []bool{false, true} {
		t.Run(fmt.Sprintf("server_restart_%t", replaceServer), func(t *testing.T) {
			boundary, run := reviewerGuardTmux(t)
			session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 297, 1)
			if err != nil {
				t.Fatal(err)
			}
			if output, err := run("new-session", "-d", "-s", session); err != nil {
				t.Fatalf("create S1: %v: %s", err, output)
			}
			if !replaceServer {
				if output, err := run("new-session", "-d", "-s", "anchor"); err != nil {
					t.Fatalf("keep original server live: %v: %s", err, output)
				}
			}
			output, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
			if err != nil {
				t.Fatal(err)
			}
			pane, err := parseReviewerPaneIdentity(output)
			if err != nil {
				t.Fatal(err)
			}
			boundary.beforeGuard = func() error {
				if replaceServer {
					if output, err := run("kill-server"); err != nil {
						return fmt.Errorf("replace S1 server: %w: %s", err, output)
					}
					if err := waitReviewerTmuxSocketDisconnected(boundary.socket); err != nil {
						return err
					}
				} else if output, err := run("kill-session", "-t", "="+session); err != nil {
					return fmt.Errorf("replace S1 session: %w: %s", err, output)
				}
				if output, err := run("new-session", "-d", "-s", session); err != nil {
					return fmt.Errorf("create foreign S2: %w: %s", err, output)
				}
				replacementOutput, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
				if err != nil {
					return err
				}
				replacement, err := parseReviewerPaneIdentity(replacementOutput)
				if err != nil {
					return err
				}
				if replaceServer && (replacement.SessionID != pane.SessionID || replacement.ServerPID == pane.ServerPID && replacement.StartTime == pane.StartTime) || !replaceServer && (replacement.SessionID == pane.SessionID || replacement.ServerPID != pane.ServerPID) {
					return errors.New("fixture did not replace the expected tmux identity")
				}
				return nil
			}
			if err := guardedReviewerKillSession(t.Context(), boundary, pane, session, "", nil); err == nil || !strings.Contains(err.Error(), "changed before guarded stop") {
				t.Fatalf("replacement session was reported stopped or guard did not report false branch: %v", err)
			}
			if output, err := run("has-session", "-t", "="+session); err != nil {
				t.Fatalf("foreign S2 was killed: %v: %s", err, output)
			}
		})
	}
}

func TestGuardedReviewerKillStopsExactTmuxSession(t *testing.T) {
	boundary, run := reviewerGuardTmux(t)
	// WSL2 reports this instead of the usual missing-server text after the
	// real last-pane shutdown. The boundary still executes the real tmux call.
	boundary.maskMissing = true
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 298, 1)
	if err != nil {
		t.Fatal(err)
	}
	if output, err := run("new-session", "-d", "-s", session); err != nil {
		t.Fatalf("create exact session: %v: %s", err, output)
	}
	output, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
	if err != nil {
		t.Fatal(err)
	}
	pane, err := parseReviewerPaneIdentity(output)
	if err != nil {
		t.Fatal(err)
	}
	if !validReviewerGuardedKillArgs([]string{"if-shell", "-F", "-t", agentruntime.PaneTarget(session), reviewerGuardCondition(pane), "kill-session -t " + pane.SessionID, "display-message -p " + reviewerGuardMismatch}) {
		t.Fatal("host rejected the exact guarded reviewer stop")
	}
	if err := guardedReviewerKillSession(t.Context(), boundary, pane, session, "", nil); err == nil {
		if !reviewerSessionAbsentOnServer(boundary.lastInventory, pane, session) {
			t.Fatalf("last-pane stop lacked exact-server inventory proof: %q", boundary.lastInventory)
		}
	} else if !strings.Contains(err.Error(), "absence is unproved") {
		t.Fatalf("last-pane stop did not fail closed: %v", err)
	}
	if output, err := run("has-session", "-t", "="+session); err == nil {
		t.Fatalf("exact reviewer session remains: %s", output)
	}
}

func TestGuardedReviewerKillStopsExactTmuxSessionWithAnchor(t *testing.T) {
	boundary, run := reviewerGuardTmux(t)
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 315, 2)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"anchor", session} {
		if output, err := run("new-session", "-d", "-s", name); err != nil {
			t.Fatalf("create %s: %v: %s", name, err, output)
		}
	}
	output, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
	if err != nil {
		t.Fatal(err)
	}
	pane, err := parseReviewerPaneIdentity(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := guardedReviewerKillSession(t.Context(), boundary, pane, session, "", nil); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(pane.ServerPID, 0); err != nil {
		t.Fatalf("anchor server stopped: %v", err)
	}
	if output, err := run("has-session", "-t", "=anchor"); err != nil {
		t.Fatalf("anchor was killed: %v: %s", err, output)
	}
	if output, err := run("has-session", "-t", "="+session); err == nil {
		t.Fatalf("exact reviewer session remains: %s", output)
	}
}

func TestGuardedReviewerKillRejectsSameServerS2AfterGuard(t *testing.T) {
	boundary, run := reviewerGuardTmux(t)
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 315, 3)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"anchor", session} {
		if output, err := run("new-session", "-d", "-s", name); err != nil {
			t.Fatalf("create %s: %v: %s", name, err, output)
		}
	}
	output, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
	if err != nil {
		t.Fatal(err)
	}
	pane, err := parseReviewerPaneIdentity(output)
	if err != nil {
		t.Fatal(err)
	}
	boundary.afterGuard = func() error {
		if output, err := run("new-session", "-d", "-s", session); err != nil {
			return fmt.Errorf("create same-server foreign S2: %w: %s", err, output)
		}
		return nil
	}
	if err := guardedReviewerKillSession(t.Context(), boundary, pane, session, "", nil); err == nil {
		t.Fatal("same-server foreign S2 was certified absent")
	}
	if output, err := run("has-session", "-t", "="+session); err != nil {
		t.Fatalf("same-server foreign S2 was killed: %v: %s", err, output)
	}
}

func TestGuardedReviewerKillRejectsS2AfterLastServerExit(t *testing.T) {
	for _, failInventory := range []bool{false, true} {
		t.Run(fmt.Sprintf("inventory_failure_%t", failInventory), func(t *testing.T) {
			testGuardedReviewerKillRejectsS2AfterLastServerExit(t, failInventory)
		})
	}
}

func testGuardedReviewerKillRejectsS2AfterLastServerExit(t *testing.T, failInventory bool) {
	boundary, run := reviewerGuardTmux(t)
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 315, 4)
	if err != nil {
		t.Fatal(err)
	}
	server := exec.CommandContext(t.Context(), boundary.binary, "-D", "-S", boundary.socket, "-f", "/dev/null")
	var serverOutput bytes.Buffer
	server.Stderr = &serverOutput
	if err := server.Start(); err != nil {
		t.Fatal(err)
	}
	serverDone := make(chan error, 1)
	go func() { serverDone <- server.Wait() }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		connection, connectErr := net.DialTimeout("unix", boundary.socket, 100*time.Millisecond)
		if connectErr == nil {
			_ = connection.Close()
			break
		}
		select {
		case serverErr := <-serverDone:
			t.Fatalf("foreground S1 server exited before startup: %v: %s", serverErr, serverOutput.String())
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("foreground S1 server did not start: %v", connectErr)
		}
		runtime.Gosched()
	}
	if output, err := run("new-session", "-d", "-s", session); err != nil {
		t.Fatalf("create S1: %v: %s", err, output)
	}
	if output, err := run("set-option", "-g", "exit-empty", "on"); err != nil {
		t.Fatalf("restore last-pane server exit: %v: %s", err, output)
	}
	output, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
	if err != nil {
		t.Fatal(err)
	}
	pane, err := parseReviewerPaneIdentity(output)
	if err != nil {
		t.Fatal(err)
	}
	if pane.ServerPID != server.Process.Pid {
		t.Fatalf("foreground S1 server PID differs from captured identity: started=%d captured=%d", server.Process.Pid, pane.ServerPID)
	}
	gate := filepath.Join(filepath.Dir(boundary.socket), "s2-gate")
	if err := syscall.Mkfifo(gate, 0600); err != nil {
		t.Fatal(err)
	}
	boundary.afterGuard = func() error {
		<-serverDone // The exact last-pane guarded kill naturally exited S1.
		if err := syscall.Kill(pane.ServerPID, 0); !errors.Is(err, syscall.ESRCH) {
			return fmt.Errorf("S1 server death is unproved: %v", err)
		}
		if err := waitReviewerTmuxSocketDisconnected(boundary.socket); err != nil {
			return err
		}
		if err := os.Remove(boundary.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove disconnected S1 socket: %w", err)
		}
		if output, err := run("new-session", "-d", "-s", "anchor", "/bin/cat "+gate); err != nil {
			return fmt.Errorf("create S2 server anchor: %w: %s", err, output)
		}
		if output, err := run("set-option", "-gw", "remain-on-exit", "on"); err != nil {
			return fmt.Errorf("keep S2 fixture pane after exit: %w: %s", err, output)
		}
		if output, err := run("new-session", "-d", "-s", session, "/bin/cat "+gate); err != nil {
			return fmt.Errorf("create foreign S2: %w: %s", err, output)
		}
		if output, err := run("list-sessions", "-F", reviewerSessionsFormat); err != nil || !strings.Contains(output, "|"+session+"\n") {
			return fmt.Errorf("S2 inventory unavailable: %v: %s", err, output)
		}
		if failInventory {
			boundary.inventoryError = errors.New("injected inventory failure")
		}
		return nil
	}
	guardErr := guardedReviewerKillSession(t.Context(), boundary, pane, session, "", nil)
	if guardErr == nil {
		t.Fatal("foreign S2 was certified absent after original server death")
	}
	if !failInventory && !strings.Contains(boundary.lastInventory, "|"+session+"\n") || failInventory && !strings.Contains(guardErr.Error(), "inventory failure") {
		t.Fatalf("post-guard inventory did not observe foreign S2: %q (guard: %v)", boundary.lastInventory, guardErr)
	}
	if output, err := run("has-session", "-t", "="+session); err != nil {
		t.Fatalf("foreign S2 was killed: %v: %s", err, output)
	}
}

func TestReviewerSessionAbsentOnServerRequiresValidatedRow(t *testing.T) {
	pane := reviewerPaneIdentity{ServerPID: 123, StartTime: 456}
	for _, output := range []string{"", "124|456|anchor", "123|457|anchor", "123|456|reviewer", "123|456|anchor\n124|456|other", "123|456|", "123|456|anchor|extra", "123|456|anchor\n\n"} {
		if reviewerSessionAbsentOnServer(output, pane, "reviewer") {
			t.Fatalf("uncertain server inventory was accepted: %q", output)
		}
	}
	if !reviewerSessionAbsentOnServer("123|456|anchor", pane, "reviewer") {
		t.Fatal("matching live server with only anchor was rejected")
	}
}

func TestReviewerSessionAbsenceProofRequiresExactInventory(t *testing.T) {
	pane := reviewerPaneIdentity{ServerPID: 123, StartTime: 456}
	if !reviewerSessionAbsenceProved("123|456|anchor\n", nil, pane, "reviewer") {
		t.Fatal("exact same-server inventory was rejected")
	}
	if reviewerSessionAbsenceProved("123|456|anchor\n123|456|reviewer\n", nil, pane, "reviewer") {
		t.Fatal("positive same-name S2 inventory was certified absent")
	}
	if reviewerSessionAbsenceProved("", errors.New("tmux unavailable"), pane, "reviewer") || reviewerSessionAbsenceProved("123|456|anchor\n", errors.New("tmux failed"), pane, "reviewer") {
		t.Fatal("failed inventory was certified absent")
	}
}

func TestGuardedReviewerKillRejectsUnlinkedLiveServerAndReplacement(t *testing.T) {
	for _, replacement := range []bool{false, true} {
		t.Run(fmt.Sprintf("replacement_%t", replacement), func(t *testing.T) {
			boundary, run := reviewerGuardTmux(t)
			session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 315, 1)
			if err != nil {
				t.Fatal(err)
			}
			for _, name := range []string{"anchor", session} {
				if output, err := run("new-session", "-d", "-s", name); err != nil {
					t.Fatalf("create %s: %v: %s", name, err, output)
				}
			}
			output, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
			if err != nil {
				t.Fatal(err)
			}
			pane, err := parseReviewerPaneIdentity(output)
			if err != nil {
				t.Fatal(err)
			}
			oldSocket := boundary.socket + ".old"
			boundary.afterGuard = func() error {
				if err := os.Rename(boundary.socket, oldSocket); err != nil {
					return err
				}
				if err := syscall.Kill(pane.ServerPID, 0); err != nil {
					return fmt.Errorf("original server must remain live: %w", err)
				}
				if replacement {
					if output, err := run("new-session", "-d", "-s", session); err != nil {
						return fmt.Errorf("create foreign S2: %w: %s", err, output)
					}
				}
				return nil
			}
			t.Cleanup(func() {
				if output, err := exec.Command(boundary.binary, "-S", oldSocket, "-f", "/dev/null", "kill-server").CombinedOutput(); err != nil {
					t.Errorf("stop exact original fixture server: %v: %s", err, output)
				}
			})
			if err := guardedReviewerKillSession(t.Context(), boundary, pane, session, "", nil); err == nil {
				t.Fatal("live original server was certified absent")
			}
			if err := syscall.Kill(pane.ServerPID, 0); err != nil {
				t.Fatalf("original server no longer live: %v", err)
			}
			if replacement {
				if output, err := run("has-session", "-t", "="+session); err != nil {
					t.Fatalf("foreign S2 was killed: %v: %s", err, output)
				}
			}
		})
	}
}

func TestHostAllowsOnlyExactReviewerGuardedKill(t *testing.T) {
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 298, 1)
	if err != nil {
		t.Fatal(err)
	}
	pane := reviewerPaneIdentity{SessionID: "$0", PID: 23456, ServerPID: 12345, StartTime: 1789286706, Name: session}
	args := []string{"if-shell", "-F", "-t", agentruntime.PaneTarget(session), reviewerGuardCondition(pane), "kill-session -t " + pane.SessionID, "display-message -p " + reviewerGuardMismatch}
	if !validTmuxBoundaryArgs(args, nil, "", "") {
		t.Fatal("host denied exact server-side reviewer guard")
	}
	if !validTmuxBoundaryArgs([]string{"list-sessions", "-F", reviewerSessionsFormat}, nil, "", "") || validTmuxBoundaryArgs([]string{"list-sessions", "-F", "#{session_name}"}, nil, "", "") {
		t.Fatal("host did not allow only exact reviewer server inventory")
	}
	for _, change := range []func([]string){
		func(args []string) { args[3] = "=foreign:0.0" },
		func(args []string) { args[4] = "#{pane_pid}" },
		func(args []string) { args[5] = "run-shell true" },
		func(args []string) { args[6] = "display-message -p anything" },
		func(args []string) { args[1] = "-bF" },
	} {
		invalid := append([]string(nil), args...)
		change(invalid)
		if validTmuxBoundaryArgs(invalid, nil, "", "") {
			t.Fatalf("host admitted arbitrary tmux guard: %v", invalid)
		}
	}
}

func TestReviewerMissingSessionWithLiveTmuxServerNeedsGroupDeath(t *testing.T) {
	boundary, run := reviewerGuardTmux(t)
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 300, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"anchor", session} {
		if output, err := run("new-session", "-d", "-s", name); err != nil {
			t.Fatalf("create tmux session %s: %v: %s", name, err, output)
		}
	}
	if output, err := run("kill-session", "-t", "="+session); err != nil {
		t.Fatalf("remove reviewer session: %v: %s", err, output)
	}
	service := &operatorMutationService{reviewer: boundary, lifecycle: t.Context()}
	if _, err := service.stopReviewerSessionAt(t.Context(), session, strings.Repeat("a", 32), "", 0, false, false, "", "", 1, 1); err == nil {
		t.Fatal("unbound reviewer was certified from missing tmux session")
	}
	if _, err := service.stopReviewerSessionAt(t.Context(), session, strings.Repeat("a", 32), "", syscall.Getpgrp(), false, false, "", "", 1, 1); err == nil {
		t.Fatal("live or ambiguous group was certified from missing tmux session")
	}
	if observed, err := service.stopReviewerSessionAt(t.Context(), session, strings.Repeat("a", 32), "", 99999999, false, false, "", "", 1, 1); err != nil || observed.GroupPID != 99999999 {
		t.Fatalf("dead owner-bound group did not converge after session loss: observation=%#v err=%v", observed, err)
	}
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 300, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	snapshot, expectedSession := reviewIdentity(attempt, root)
	if expectedSession != session {
		t.Fatal("fixture reviewer session mismatch")
	}
	target := "o/r#300 plan sha256:" + digestText("missing pane")
	resultRoot := filepath.Dir(reviewResultPath(snapshot, target))
	for _, path := range []string{snapshot, resultRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	proof := reviewerProcessProof{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Target: target, Mode: agentruntime.ReviewModePlan, EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, GroupPID: 99999999, DeadProved: true}
	if err := cleanupCertifiedReviewResources(t.Context(), boundary, nil, attempt, attempt.BaseSHA, target, snapshot, session, root, proof); err != nil {
		t.Fatalf("certified cleanup did not converge with absent session and live server: %v", err)
	}
	for _, path := range []string{snapshot, resultRoot} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("certified cleanup retained %s: %v", path, err)
		}
	}
}

func TestDetachedReviewerChildProcessHelper(t *testing.T) {
	switch os.Getenv("AGENT_SYMPHONY_DETACHED_REVIEWER_HELPER") {
	case "reviewer":
		child := exec.Command(os.Args[0], "-test.run=^TestDetachedReviewerChildProcessHelper$")
		child.Env = append(os.Environ(), "AGENT_SYMPHONY_DETACHED_REVIEWER_HELPER=child")
		child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		child.Stdout, child.Stderr = io.Discard, io.Discard
		if child.Start() != nil {
			os.Exit(125)
		}
		_, _ = fmt.Fprintln(os.Stdout, child.Process.Pid)
		_ = child.Wait()
	case "child":
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
		<-signals
	}
}

func TestReviewerCleanupRejectsEscapedChild(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	reviewer := exec.Command(os.Args[0], "-test.run=^TestDetachedReviewerChildProcessHelper$")
	reviewer.Env = append(os.Environ(), "AGENT_SYMPHONY_DETACHED_REVIEWER_HELPER=reviewer")
	reviewer.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	output, err := reviewer.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := reviewer.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		_ = reviewer.Process.Kill()
		if !waited {
			_ = reviewer.Wait()
		}
	})
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil {
		t.Fatalf("reviewer did not report detached child: %v", err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || childPID < 2 {
		t.Fatalf("invalid detached reviewer PID %q: %v", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(childPID, syscall.SIGKILL) })
	groupPID := reviewer.Process.Pid
	if childGroup, err := syscall.Getpgid(childPID); err != nil || childGroup == groupPID {
		t.Fatalf("reviewer child did not escape original group: child=%d group=%d err=%v", childGroup, groupPID, err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: groupPID}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Kill(-groupPID, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = reviewer.Wait()
	waited = true
	if gone, err := reviewerGroupGone(groupPID); err != nil || !gone {
		t.Fatalf("original reviewer group remains: gone=%t err=%v", gone, err)
	}
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatalf("detached reviewer child is not live: %v", err)
	}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||"}}
	service := &operatorMutationService{lifecycle: t.Context(), reviewer: boundary}
	stopped, stopErr := service.stopReviewerSessionAt(t.Context(), request.Reviewer.Session, effect.ID, effect.RequestDigest, groupPID, true, true, "", "", effect.IssueGeneration, effect.AttemptGeneration)
	if stopErr == nil && stopped.GroupPID == groupPID {
		_, _ = owner.proveReviewerDead(t.Context(), proveReviewerDeadCommand{Identity: identity, GroupPID: groupPID})
	}
	proof := mustOwnerSnapshot(t, owner).State.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)]
	if proof.DeadProved {
		snapshotRoot := productionSnapshotRoot(owner.stateRoot)
		if err := os.MkdirAll(request.Reviewer.Snapshot, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := cleanupCertifiedReviewResources(t.Context(), boundary, nil, agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, request.Reviewer.HeadSHA, request.Reviewer.Target, request.Reviewer.Snapshot, request.Reviewer.Session, snapshotRoot, proof); err != nil {
			t.Fatalf("false death certificate did not reach cleanup: %v", err)
		}
		if err := syscall.Kill(childPID, 0); err != nil {
			t.Fatalf("detached reviewer child died before cleanup was checked: %v", err)
		}
		t.Fatal("reviewer cleanup deleted snapshot with detached child still live")
	}
}

func TestDismissWithPendingReviewerDoesNotCompletePhysicalCleanup(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	request.Reviewer.Mode = agentruntime.ReviewModeImplementation
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, reviewer, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*reviewer)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	liveReviewer := exec.Command(os.Args[0], "-test.run=^TestDetachedReviewerChildProcessHelper$")
	liveReviewer.Env = append(os.Environ(), "AGENT_SYMPHONY_DETACHED_REVIEWER_HELPER=child")
	liveReviewer.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := liveReviewer.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = liveReviewer.Process.Kill()
		_ = liveReviewer.Wait()
	})
	if group, err := syscall.Getpgid(liveReviewer.Process.Pid); err != nil || group != liveReviewer.Process.Pid {
		t.Fatalf("fixture reviewer is not its process-group leader: group=%d err=%v", group, err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: liveReviewer.Process.Pid}); err != nil {
		t.Fatal(err)
	}
	result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(request)
	sealTestReviewerResult(t, owner, *reviewer, result)
	if err := os.MkdirAll(request.Reviewer.Snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	input := reconciliationEffectObservationInput(request, "title")
	input.Issues[0].Closed = true
	input.Issues[0].ActiveAttempt = nil
	input.Issues[0].TerminalAttempts = nil
	input.Issues[0].Active = false
	input.Issues[0].DispatchAuthorized = false
	applyReconciliationInput(t, owner, input)
	before := mustOwnerSnapshot(t, owner)
	status, _, err := ownerOperatorStatus(before.State, request.Issue, request.Attempt)
	if err != nil || !canDismissClosedAttempt(status) {
		t.Fatalf("pending reviewer cannot reach Dismiss: status=%#v err=%v", status, err)
	}
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: request.Repository, operator: service}
	httpRequest := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost/actions/dismiss?repository=o%%2Fr&issue=%d&attempt=%d", request.Issue, request.Attempt), nil)
	httpRequest.Host = "localhost"
	httpRequest.Header.Set("Origin", "http://localhost")
	response := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(response, httpRequest)
	if response.Code != http.StatusAccepted {
		t.Fatalf("reachable Dismiss failed: status=%d body=%s", response.Code, response.Body.String())
	}
	committed := mustOwnerSnapshot(t, owner)
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	proofKey := reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)
	proof := committed.State.ReviewerProofs[proofKey]
	if err := syscall.Kill(liveReviewer.Process.Pid, 0); err != nil {
		t.Fatalf("pending reviewer did not remain live: %v", err)
	}
	if _, err := os.Lstat(request.Reviewer.Snapshot); err != nil {
		t.Fatalf("pending reviewer snapshot was removed: %v", err)
	}
	if _, exists := committed.State.Attempts[key]; exists || committed.State.Tombstones[key].Action != "dismissed" || committed.State.Tombstones[key].ReviewerLeaseID != reviewer.ID || proof.EffectID != reviewer.ID || proof.DeadProved {
		t.Fatalf("Dismiss lost logical invalidation or exact pending reviewer proof: tombstone=%#v proof=%#v", committed.State.Tombstones[key], proof)
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: result}); err == nil {
		t.Fatal("stale reviewer result resurrected dismissed attempt")
	}
	if _, err := owner.sealReviewerResult(t.Context(), sealReviewerResultCommand{Identity: identity, Result: result}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("dismissed reviewer accepted a stale business-result seal: %v", err)
	}
	if committed.State.Tombstones[key].CleanupPhase == "completed" {
		t.Fatal("Dismiss falsely completed cleanup with unresolved reviewer lease")
	}
	replay := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(replay, httpRequest.Clone(t.Context()))
	if replay.Code != http.StatusAccepted {
		t.Fatalf("duplicate Dismiss lost physical-pending response: status=%d body=%s", replay.Code, replay.Body.String())
	}
	blocked := cloneRuntimeOwnerState(committed.State)
	if err := applyAdvanceIssueGeneration(&blocked, advanceIssueGenerationCommand{Repository: request.Repository, Issue: request.Issue, ExpectedGeneration: blocked.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)]}); !errors.Is(err, errStateConflict) {
		t.Fatalf("unproved reviewer allowed same-issue generation advance: %v", err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, request.Repository)
	if err != nil || loaded.Tombstones[key].CleanupPhase == "completed" || loaded.ReviewerProofs[proofKey].EffectID != reviewer.ID {
		t.Fatalf("restart lost pending reviewer quarantine: tombstone=%#v proof=%#v err=%v", loaded.Tombstones[key], loaded.ReviewerProofs[proofKey], err)
	}
	restarted, err := startTestStateOwner(t, root, loaded, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, owner.attemptRoot, state)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	replayResult := operatorTestMutationService(t, restarted).perform(t.Context(), controlRequest{Version: controlVersion, RequestID: "dismiss-after-restart", Repository: request.Repository, Action: "dismiss", Issue: request.Issue, Attempt: request.Attempt})
	if replayResult.Status != http.StatusAccepted || !replayResult.OK {
		t.Fatalf("restart rejected idempotent pending Dismiss: %#v", replayResult)
	}
}

func TestDismissBeforeReviewerSessionRequestProvesNeverRan(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	_, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, reviewer, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	state := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	publication := bindEffectObservation(stateOwnerSnapshot{State: state}, reconciliationEffectCaseNamed(t, "github-publish").request)
	if _, err := applyBeginReconciliationEffect(owner.attemptRoot, owner.stateRoot, &state, beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(stateOwnerSnapshot{State: state}, publication), Request: publication}); !errors.Is(err, errStateConflict) {
		t.Fatalf("publication overlapped unlaunched reviewer gate: %v", err)
	}
	leaseID, err := retainReviewerLease(&state, request.Repository, request.Issue, request.Attempt, *reviewer)
	if err != nil || leaseID != "" {
		t.Fatalf("never-requested reviewer retained an unresolved lease: lease=%q err=%v", leaseID, err)
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	manifest := cloneManifest(state.Attempts[key].Manifest)
	if _, err := applyInvalidateAttempt(owner.attemptRoot, owner.stateRoot, &state, invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, ExpectedIssueGeneration: reviewer.IssueGeneration, ExpectedAttemptGeneration: reviewer.AttemptGeneration, Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest}); err != nil {
		t.Fatal(err)
	}
	state, _, err = finishRuntimeOwnerTransition(owner.attemptRoot, owner.stateRoot, state, nil)
	if err != nil {
		t.Fatal(err)
	}
	proof := state.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)]
	if state.Tombstones[key].CleanupPhase != "completed" || state.Tombstones[key].ReviewerLeaseID != "" || !proof.NeverRan || !proof.DeadProved {
		t.Fatalf("dismissed no-launch reviewer lacks exact no-run evidence: tombstone=%#v proof=%#v", state.Tombstones[key], proof)
	}
	if err := applyMarkReviewerSessionRequested(owner.stateRoot, &state, markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewer)}); err == nil {
		t.Fatal("reviewer session started after durable Dismiss")
	}
}

func TestRestartDemotesLegacyReviewerGroupDeathCertificate(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	state := cloneRuntimeOwnerState(snapshot.State)
	proof := reviewerProcessProof{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, Mode: request.Reviewer.Mode, Target: request.Reviewer.Target, EffectID: strings.Repeat("a", 32), IssueGeneration: state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: state.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)], GroupPID: 99999999, DeadProved: true}
	state.ReviewerSafetyMigrated = false // A pre-upgrade ledger has no migration marker.
	key := reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)
	state.ReviewerProofs[key] = proof
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(root, owner.attemptRoot, state); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, state, func(value runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, owner.attemptRoot, value)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	current := mustOwnerSnapshot(t, restarted).State
	if current.ReviewerProofs[key].DeadProved {
		t.Fatal("legacy group-only certificate survived restart as descendant-death proof")
	}
	if err := applyAdvanceIssueGeneration(&current, advanceIssueGenerationCommand{Repository: request.Repository, Issue: request.Issue, ExpectedGeneration: current.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)]}); !errors.Is(err, errStateConflict) {
		t.Fatalf("legacy reviewer lease allowed conflicting dispatch: %v", err)
	}
}

func TestLegacyCompletedCleanupWithoutReviewerIdentityQuarantinesOnlyItsIssue(t *testing.T) {
	for _, action := range []string{"dismissed", "archived", "abandoned", "removed"} {
		t.Run(action, func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
			root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
			state := cloneRuntimeOwnerState(snapshot.State)
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			publishedHead := ""
			if action == "removed" {
				publishedHead = strings.Repeat("a", 40)
			}
			if err := migrateLegacyTombstone(&state, request.Repository, request.Issue, request.Attempt, action, "completed", publishedHead, nil); err != nil {
				t.Fatal(err)
			}
			delete(state.Observations, ownerIssueKey(request.Repository, request.Issue))
			key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
			tombstone := state.Tombstones[key]
			tombstone.Revision = state.Revision
			state.Tombstones[key] = tombstone
			operatorAction := map[string]string{"dismissed": "dismiss", "archived": "archive", "abandoned": "abandon", "removed": "remove"}[action]
			legacyRequest := controlRequest{Version: controlVersion, RequestID: "legacy-" + operatorAction + "-receipt", Repository: request.Repository, Action: operatorAction, Issue: request.Issue, Attempt: request.Attempt, Confirm: operatorAction != "dismiss"}
			legacyReceipt := controlReceipt{Request: legacyRequest, State: "completed", Phase: operatorPhaseCompleted, EffectID: tombstone.EffectID, Result: successfulOperatorResult(legacyRequest, state.Revision)}
			state.ControlReceipts = append(state.ControlReceipts, legacyReceipt)
			state.ReviewerSafetyMigrated = false
			state.ReviewerProofs = map[string]reviewerProcessProof{} // Old cleanup discarded the only group certificate.
			before := cloneRuntimeOwnerState(state)
			if err := writeRuntimeOwnerState(root, owner.attemptRoot, state); err != nil {
				t.Fatal(err)
			}
			for restart := 0; restart < 2; restart++ {
				restarted, err := startTestStateOwner(t, root, state, func(value runtimeOwnerState) error {
					return writeRuntimeOwnerState(root, owner.attemptRoot, value)
				})
				if err != nil {
					t.Fatal(err)
				}
				state = mustOwnerSnapshot(t, restarted).State
				if err := restarted.close(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			if !reflect.DeepEqual(state.Tombstones[key], before.Tombstones[key]) {
				t.Fatal("migration rewrote a completed destructive tombstone")
			}
			for id, effect := range before.Effects {
				if state.Effects[id].State != effect.State || state.Effects[id].ID != effect.ID {
					t.Fatal("migration rewrote a completed effect")
				}
			}
			if !reflect.DeepEqual(state.ControlReceipts, before.ControlReceipts) {
				t.Fatal("migration rewrote a completed operator receipt")
			}
			warning := operatorResultForReceipt(stateOwnerSnapshot{State: state}, state.ControlReceipts[len(state.ControlReceipts)-1])
			if warning.Status != http.StatusAccepted || !strings.Contains(string(warning.Data), "physical-unverified") {
				t.Fatalf("duplicate historical action lacked quarantine warning: %#v", warning)
			}
			if state.LegacyReviewerQuarantines[ownerIssueKey(request.Repository, request.Issue)] == "" {
				t.Fatal("missing reviewer identity was silently considered clean")
			}
			if err := applyAdvanceIssueGeneration(&state, advanceIssueGenerationCommand{Repository: request.Repository, Issue: request.Issue, ExpectedGeneration: state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)]}); !errors.Is(err, errStateConflict) {
				t.Fatalf("quarantined issue admitted new work: %v", err)
			}
			status, err := projectOwnerStatus(stateOwnerSnapshot{State: state}, maxReconciliationAttemptCount, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			visible := false
			for _, entry := range status.Statuses {
				if entry.Issue == request.Issue && entry.Attempt == request.Attempt && entry.NeedsAttention && entry.CurrentPhase == "physical-unverified" && entry.Diagnostic != "" {
					visible = true
				}
			}
			if !visible {
				t.Fatal("quarantined historical attempt is absent from dashboard status")
			}
		})
	}
}

func TestLegacyCleanManifestWithoutReviewerHistoryBlocksPublication(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	state := cloneRuntimeOwnerState(snapshot.State)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	record := state.Attempts[key]
	record.Manifest.ReviewState = "clean"
	state.Attempts[key] = record
	state.ReviewerSafetyMigrated = false
	if err := writeRuntimeOwnerState(root, owner.attemptRoot, state); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, state, func(value runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, owner.attemptRoot, value)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	current := mustOwnerSnapshot(t, restarted).State
	if current.LegacyReviewerQuarantines[ownerIssueKey(request.Repository, request.Issue)] == "" || !issueHasUnprovedReviewer(current, request.Repository, request.Issue) {
		t.Fatal("old clean manifest without retained reviewer history was considered physically safe")
	}
	if err := applyAdvanceIssueGeneration(&current, advanceIssueGenerationCommand{Repository: request.Repository, Issue: request.Issue, ExpectedGeneration: current.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)]}); !errors.Is(err, errStateConflict) {
		t.Fatalf("old clean manifest allowed issue reuse: %v", err)
	}
	status, err := projectOwnerStatus(stateOwnerSnapshot{State: current}, maxReconciliationAttemptCount, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	visible := false
	for _, entry := range status.Statuses {
		if entry.Issue == request.Issue && entry.Attempt == request.Attempt && entry.NeedsAttention && !entry.DispatchAuthorized && entry.Diagnostic != "" {
			visible = true
		}
	}
	if !visible {
		t.Fatal("old clean manifest quarantine is not visible to the dashboard")
	}
}

func TestEscapedReviewerChildBlocksGitHubPublicationAndGovernance(t *testing.T) {
	reviewer := exec.Command(os.Args[0], "-test.run=^TestDetachedReviewerChildProcessHelper$")
	reviewer.Env = append(os.Environ(), "AGENT_SYMPHONY_DETACHED_REVIEWER_HELPER=reviewer")
	reviewer.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	output, err := reviewer.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := reviewer.Start(); err != nil {
		t.Fatal(err)
	}
	waited := false
	t.Cleanup(func() {
		_ = reviewer.Process.Kill()
		if !waited {
			_ = reviewer.Wait()
		}
	})
	line, err := bufio.NewReader(output).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || childPID < 2 {
		t.Fatalf("invalid escaped child PID: %q", line)
	}
	t.Cleanup(func() { _ = syscall.Kill(childPID, syscall.SIGKILL) })
	if group, err := syscall.Getpgid(childPID); err != nil || group == reviewer.Process.Pid {
		t.Fatalf("fixture child did not escape reviewer group: group=%d err=%v", group, err)
	}
	if err := syscall.Kill(-reviewer.Process.Pid, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	_ = reviewer.Wait()
	waited = true
	if gone, err := reviewerGroupGone(reviewer.Process.Pid); err != nil || !gone {
		t.Fatalf("old reviewer group still exists: gone=%t err=%v", gone, err)
	}
	if err := syscall.Kill(childPID, 0); err != nil {
		t.Fatalf("escaped child died before publication authorization: %v", err)
	}
	for _, name := range []string{"github-publish", "github-pr-governance", "issue-evidence"} {
		t.Run(name, func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, name).request
			owner, snapshot, request := reconciliationEffectTestOwner(t, request)
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			state := cloneRuntimeOwnerState(snapshot.State)
			key := reviewerProofKey(request.Repository, request.Issue, request.Attempt, agentruntime.ReviewModeImplementation, strings.Repeat("a", 40)+".."+strings.Repeat("b", 40))
			state.ReviewerProofs[key] = reviewerProcessProof{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, Mode: agentruntime.ReviewModeImplementation, Target: strings.Repeat("a", 40) + ".." + strings.Repeat("b", 40), EffectID: strings.Repeat("c", 32), IssueGeneration: state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: state.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)], GroupPID: reviewer.Process.Pid, LegacyUnverified: true}
			if _, err := applyBeginReconciliationEffect(owner.attemptRoot, owner.stateRoot, &state, beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request}); !errors.Is(err, errStateConflict) {
				t.Fatalf("escaped reviewer child permitted %s: %v", name, err)
			}
			if err := syscall.Kill(childPID, 0); err != nil {
				t.Fatalf("escaped child died during blocked %s: %v", name, err)
			}
		})
	}
}

func TestCertifiedReviewerCleanupGuardsDeadPaneAcrossServerReplacement(t *testing.T) {
	for _, mode := range []string{"exact", "replaced_before_guard", "replaced_after_guard_inventory_failed"} {
		t.Run(mode, func(t *testing.T) {
			boundary, run := reviewerGuardTmux(t)
			root := t.TempDir()
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 299, Number: 1, BaseSHA: strings.Repeat("a", 40)}
			snapshot, session := reviewIdentity(attempt, root)
			target := "o/r#299 plan sha256:" + digestText("guarded cleanup")
			resultRoot := filepath.Dir(reviewResultPath(snapshot, target))
			for _, path := range []string{snapshot, resultRoot} {
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: digestText("guarded reviewer"), GateProtocol: true, SessionRequested: true, ChildPID: 99999999}
			launchPath, terminalPath := reviewerLifecyclePaths(snapshot, target)
			if err := writeReviewerRecord(launchPath, identity); err != nil {
				t.Fatal(err)
			}
			proof := reviewerProcessProof{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Target: target, Mode: agentruntime.ReviewModePlan, EffectID: identity.EffectID, IssueGeneration: 1, AttemptGeneration: 1, GroupPID: identity.ChildPID, DeadProved: true}
			if output, err := run("new-session", "-d", "-s", session); err != nil {
				t.Fatalf("create reviewer S1: %v: %s", err, output)
			}
			if mode != "replaced_before_guard" {
				if output, err := run("new-session", "-d", "-s", "anchor"); err != nil {
					t.Fatalf("create S1 anchor: %v: %s", err, output)
				}
			}
			if output, err := run("set-option", "-w", "-t", agentruntime.PaneTarget(session), "remain-on-exit", "on"); err != nil {
				t.Fatalf("retain reviewer pane: %v: %s", err, output)
			}
			start := "false # agent-symphony review-pane tmux " + launchPath + " " + terminalPath + " " + reviewerSignal(identity) + " " + identity.RequestDigest
			if output, err := run("respawn-pane", "-k", "-t", agentruntime.PaneTarget(session), "--", start); err != nil {
				t.Fatalf("start reviewer pane: %v: %s", err, output)
			}
			deadline := time.Now().Add(5 * time.Second)
			for {
				output, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
				if err != nil {
					t.Fatal(err)
				}
				pane, err := parseReviewerPaneIdentity(output)
				if err != nil {
					t.Fatal(err)
				}
				if pane.Status.Dead {
					if pane.SessionID != "$0" {
						t.Fatalf("initial reviewer session was not $0: %#v", pane)
					}
					if !reviewerPaneStartMatches(pane.Start, launchPath, terminalPath, identity) {
						t.Fatalf("fixture pane is not owned: %q", pane.Start)
					}
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("reviewer pane did not exit")
				}
			}
			if mode == "replaced_before_guard" {
				boundary.beforeGuard = func() error {
					if output, err := run("kill-server"); err != nil {
						return fmt.Errorf("replace reviewer server: %w: %s", err, output)
					}
					if err := waitReviewerTmuxSocketDisconnected(boundary.socket); err != nil {
						return err
					}
					if output, err := run("new-session", "-d", "-s", session); err != nil {
						return fmt.Errorf("create foreign reviewer S2: %w: %s", err, output)
					}
					replacementOutput, err := run("display-message", "-p", "-t", agentruntime.PaneTarget(session), reviewerPaneIdentityFormat)
					if err != nil {
						return err
					}
					replacement, err := parseReviewerPaneIdentity(replacementOutput)
					if err != nil || replacement.SessionID != "$0" {
						return fmt.Errorf("foreign server did not reuse initial tmux session ID: pane=%#v err=%v", replacement, err)
					}
					return nil
				}
			} else if mode == "replaced_after_guard_inventory_failed" {
				boundary.afterGuard = func() error {
					if output, err := run("kill-server"); err != nil {
						return fmt.Errorf("stop original server after guard: %w: %s", err, output)
					}
					if err := waitReviewerTmuxSocketDisconnected(boundary.socket); err != nil {
						return err
					}
					if err := os.Remove(boundary.socket); err != nil && !errors.Is(err, os.ErrNotExist) {
						return err
					}
					if output, err := run("new-session", "-d", "-s", session); err != nil {
						return fmt.Errorf("create foreign reviewer S2: %w: %s", err, output)
					}
					boundary.inventoryError = errors.New("injected inventory failure")
					return nil
				}
			}
			err := cleanupCertifiedReviewResources(t.Context(), boundary, nil, attempt, attempt.BaseSHA, target, snapshot, session, root, proof)
			if mode != "exact" {
				if err == nil || mode == "replaced_before_guard" && !strings.Contains(err.Error(), "changed before guarded stop") || mode == "replaced_after_guard_inventory_failed" && !strings.Contains(err.Error(), "absence is unproved") {
					t.Fatalf("certified cleanup killed foreign S2 or guard did not report false branch: %v", err)
				}
				if output, err := run("has-session", "-t", "="+session); err != nil {
					t.Fatalf("foreign S2 was killed: %v: %s", err, output)
				}
				for _, path := range []string{snapshot, resultRoot} {
					if _, err := os.Lstat(path); err != nil {
						t.Fatalf("failed cleanup removed %s: %v", path, err)
					}
				}
			} else if err != nil {
				t.Fatalf("exact certified cleanup failed: %v", err)
			} else {
				for _, path := range []string{snapshot, resultRoot} {
					if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("exact cleanup retained %s: %v", path, err)
					}
				}
			}
		})
	}
}

func reviewerPaneTestOutput(status, session, id string, pid int, start string) string {
	return fmt.Sprintf("%s|%s|%d|99999|1789286706|%s|%s", status, id, pid, session, start)
}

type preRespawnReviewerBoundary struct {
	command *exec.Cmd
	start   string
	killed  bool
}

func (b *preRespawnReviewerBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
	}
	switch command.Args[0] {
	case "display-message":
		switch command.Args[len(command.Args)-1] {
		case agentruntime.PaneStatusFormat:
			return agentruntime.Result{Output: "0||||\n"}, nil
		case "#{pane_start_command}":
			return agentruntime.Result{Output: b.start + "\n"}, nil
		case "#{pane_pid}":
			return agentruntime.Result{Output: strconv.Itoa(b.command.Process.Pid)}, nil
		}
	case "kill-session":
		b.killed = true
		if err := b.command.Process.Kill(); err != nil {
			return agentruntime.Result{}, err
		}
		_ = b.command.Wait()
		return agentruntime.Result{}, nil
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer tmux command")
}

func TestPreRespawnReviewerShellCanStopWithoutForgingLaunchedProof(t *testing.T) {
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	launchPath, terminalPath := reviewerLifecyclePaths(filepath.Join(root, "snapshot"), "o/r#73 plan sha256:"+strings.Repeat("b", 64))
	for _, test := range []struct {
		name      string
		start     string
		wantError bool
	}{
		{name: "foreign default shell", start: "/bin/zsh", wantError: true},
		{name: "wrapper before launch marker", start: "agent-symphony review-pane tmux " + launchPath, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			process := exec.Command("sleep", "30")
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = process.Process.Kill(); _ = process.Wait() })
			boundary := &preRespawnReviewerBoundary{command: process, start: test.start}
			service := &operatorMutationService{reviewer: boundary, lifecycle: t.Context()}
			observed, err := service.stopReviewerSessionAt(t.Context(), session, strings.Repeat("a", 32), "", 0, false, false, launchPath, terminalPath, 1, 1)
			if (err != nil) != test.wantError || boundary.killed == test.wantError || !test.wantError && (!observed.NeverRan || observed.GroupPID != 0) {
				t.Fatalf("pre-respawn stop observation=%#v err=%v killed=%v wantError=%v", observed, err, boundary.killed, test.wantError)
			}
		})
	}
}

func (b *reviewerSessionStopBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) < 2 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
	}
	switch command.Args[0] {
	case "display-message":
		return b.status, b.err
	case "if-shell":
		b.killed = append(b.killed, strings.TrimPrefix(command.Args[5], "kill-session -t "))
		if pane, err := parseReviewerPaneIdentity(b.status.Output); err == nil {
			b.inventory = fmt.Sprintf("%d|%d|anchor\n", pane.ServerPID, pane.StartTime)
		}
		b.status = agentruntime.Result{Output: "||||||||||\n"}
		if b.onKill != nil {
			return agentruntime.Result{}, b.onKill()
		}
		return agentruntime.Result{}, b.killErr
	case "wait-for":
		return agentruntime.Result{}, nil
	case "list-sessions":
		return agentruntime.Result{Output: b.inventory}, nil
	case "has-session":
		name := strings.TrimPrefix(command.Args[2], "=")
		return agentruntime.Result{Exited: true, Code: 1, Output: "can't find session: " + name}, errors.New("reviewer session is absent")
	default:
		return agentruntime.Result{}, errors.New("unexpected tmux command")
	}
}

func TestReviewerSessionStopRequiresExactPaneEvidence(t *testing.T) {
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		status    agentruntime.Result
		err       error
		wantKill  bool
		wantError bool
	}{
		{name: "exact missing pane", status: agentruntime.Result{Output: "||||||||||\n"}},
		{name: "unverified live pane", status: agentruntime.Result{Output: "0||||\n"}, wantError: true},
		{name: "ambiguous tmux exit one", status: agentruntime.Result{Exited: true, Code: 1, Output: "permission denied"}, err: errors.New("tmux failed"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			boundary := &reviewerSessionStopBoundary{status: test.status, err: test.err}
			service := &operatorMutationService{reviewer: boundary, lifecycle: t.Context()}
			err := service.stopReviewerSession(t.Context(), session, strings.Repeat("a", 32), 99999999)
			if (err != nil) != test.wantError || (len(boundary.killed) != 0) != test.wantKill {
				t.Fatalf("err=%v killed=%v", err, boundary.killed)
			}
			if test.wantKill && boundary.killed[0] != "="+session {
				t.Fatalf("killed wrong session: %v", boundary.killed)
			}
		})
	}
}

func TestUnboundReviewerSessionLossCannotForgeNeverRanProof(t *testing.T) {
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
	if err != nil {
		t.Fatal(err)
	}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||\n"}}
	service := &operatorMutationService{reviewer: boundary, lifecycle: t.Context()}
	if _, err := service.stopReviewerSessionAt(t.Context(), session, strings.Repeat("a", 32), "", 0, false, false, "", "", 1, 1); err == nil {
		t.Fatal("missing tmux pane falsely proved an unbound reviewer never executed")
	}
	if len(boundary.killed) != 0 {
		t.Fatalf("unbound reviewer cleanup targeted another session: %v", boundary.killed)
	}
	root := t.TempDir()
	launchPath := filepath.Join(root, "launch.json")
	terminalPath := filepath.Join(root, "terminal.json")
	observed, err := service.stopReviewerSessionAt(t.Context(), session, strings.Repeat("a", 32), strings.Repeat("b", 64), 0, true, false, launchPath, terminalPath, 1, 1)
	if err != nil || !observed.NeverRan || observed.GroupPID != 0 || len(boundary.killed) != 0 {
		t.Fatalf("gated prebind crash did not recover exact NeverRan observation: observation=%#v err=%v boundary=%#v", observed, err, boundary)
	}
	if _, err := service.stopReviewerSessionAt(t.Context(), session, strings.Repeat("a", 32), strings.Repeat("b", 64), 0, true, true, launchPath, terminalPath, 1, 1); err == nil {
		t.Fatal("requested reviewer session vanished but was certified without owner-bound child identity")
	}
	forged := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64), GateProtocol: true, SessionRequested: true, ChildPID: 99999999}
	if err := writeReviewerRecord(launchPath, forged); err != nil {
		t.Fatal(err)
	}
	if _, err := service.stopReviewerSessionAt(t.Context(), session, forged.EffectID, forged.RequestDigest, 0, true, true, launchPath, terminalPath, 1, 1); err == nil || len(boundary.killed) != 0 {
		t.Fatalf("group-writable launch file forged a death certificate after pane loss: err=%v killed=%v", err, boundary.killed)
	}
}

func TestUnboundReviewerCannotKillBeforeOwnerPIDBinding(t *testing.T) {
	root := t.TempDir()
	reviewerID := strings.Repeat("a", 32)
	digest := strings.Repeat("b", 64)
	launchPath := filepath.Join(root, "launch.json")
	terminalPath := filepath.Join(root, "terminal.json")
	identity := reviewerLaunchIdentity{EffectID: reviewerID, IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: digest, GateProtocol: true, SessionRequested: true}
	child := exec.Command("sh", "-c", "read line")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = child.Process.Kill(); _ = child.Wait() })
	identity.ChildPID = child.Process.Pid
	if err := writeReviewerRecord(launchPath, identity); err != nil {
		t.Fatal(err)
	}
	start := "agent-symphony review-pane tmux " + launchPath + " " + terminalPath + " " + reviewerSignal(identity) + " " + digest
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
	if err != nil {
		t.Fatal(err)
	}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: reviewerPaneTestOutput("0||||", session, "$9", os.Getpid(), start)}}
	service := &operatorMutationService{reviewer: boundary, lifecycle: t.Context()}
	if observation, err := service.stopReviewerSessionAt(t.Context(), session, reviewerID, digest, 0, true, true, launchPath, terminalPath, 1, 1); err == nil || observation != (reviewerStopObservation{}) || len(boundary.killed) != 0 {
		t.Fatalf("unbound reviewer was killed before owner PID binding: observation=%#v err=%v killed=%v", observation, err, boundary.killed)
	}
}

func TestCancelReplaysDiskBoundReviewerAfterKillAck(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	owner, snapshot, review := reconciliationEffectTestOwner(t, test.request)
	manifest := *review.Manifest
	reviewRequest := operatorRequest("review-before-stop-crash", "review-plan", manifest, false)
	reviewCommand := operatorCommand(snapshot, reviewRequest, manifest)
	reviewCommand.Reconciliation = &beginReconciliationEffectCommand{Identity: reviewCommand.Identity, Request: review}
	_, reviewer, err := owner.beginOperatorMutation(t.Context(), reviewCommand)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewer)}); err != nil {
		t.Fatal(err)
	}
	cancelRequest := operatorRequest("cancel-stop-crash", "cancel", manifest, false)
	cancelCommand := operatorCommand(mustOwnerSnapshot(t, owner), cancelRequest, manifest)
	cancelCommand.Runtime = &beginRuntimeEffectCommand{Identity: cancelCommand.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "operator cancelled attempt", RequestDigest: strings.Repeat("b", 64)}
	_, stop, err := owner.beginOperatorMutation(t.Context(), cancelCommand)
	if err != nil {
		t.Fatal(err)
	}
	admitted := mustOwnerSnapshot(t, owner).State
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, admitted); err != nil {
		t.Fatal(err)
	}
	persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	durableOwner, err := startTestStateOwner(t, owner.stateRoot, persisted, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state)
	})
	if err != nil {
		t.Fatal(err)
	}
	child := exec.Command("sh", "-c", "read line")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = child.Process.Kill(); _ = child.Wait() })
	launchPath, terminalPath := reviewerLifecyclePaths(review.Reviewer.Snapshot, review.Reviewer.Target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	launch := reviewerLaunchIdentity{EffectID: reviewer.ID, IssueGeneration: reviewer.IssueGeneration, AttemptGeneration: reviewer.AttemptGeneration, RequestDigest: reviewer.RequestDigest, GateProtocol: true, SessionRequested: true, ChildPID: child.Process.Pid}
	if err := writeReviewerRecord(launchPath, launch); err != nil {
		t.Fatal(err)
	}
	start := "agent-symphony review-pane tmux " + launchPath + " " + terminalPath + " " + reviewerSignal(launch) + " " + launch.RequestDigest
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: reviewerPaneTestOutput("0||||", review.Reviewer.Session, "$9", os.Getpid(), start)}}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: durableOwner, active: map[string]*activeRuntimeEffect{}}
	service := &operatorMutationService{lifecycle: t.Context(), owner: durableOwner, effects: effects, reviewer: boundary}
	request, err := service.reconstructRuntimeRequest(mustOwnerSnapshot(t, durableOwner), *stop)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.stopBoundReviewer(t.Context(), request, reviewer.ID); err == nil || len(boundary.killed) != 1 {
		t.Fatalf("tmux kill ACK falsely completed Stop while wrapper and child lived: err=%v killed=%v", err, boundary.killed)
	}
	bound := mustOwnerSnapshot(t, durableOwner)
	if gone, err := reviewerGroupGone(child.Process.Pid); err != nil || gone {
		t.Fatalf("ACK test child group was not live: gone=%v err=%v", gone, err)
	}
	if effect := bound.State.Effects[stop.ID]; effect.SupersededReviewerGroupPID != child.Process.Pid || effect.ReviewerStopped {
		t.Fatalf("group was not bound before kill boundary: %#v", effect)
	}
	if receipt, ok := operatorReceiptByID(bound.State, cancelRequest.RequestID); !ok || receipt.State != "pending" {
		t.Fatalf("tmux ACK falsely completed Cancel receipt: %#v", receipt)
	}
	if err := durableOwner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	stored, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	proofKey := reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, review.Reviewer.Mode, review.Reviewer.Target)
	if stored.Effects[stop.ID].SupersededReviewerGroupPID != child.Process.Pid || stored.Effects[stop.ID].ReviewerStopped || stored.ReviewerProofs[proofKey].GroupPID != child.Process.Pid || stored.ReviewerProofs[proofKey].DeadProved {
		t.Fatalf("disk did not preserve pending reviewer group binding: effect=%#v proof=%#v", stored.Effects[stop.ID], stored.ReviewerProofs[proofKey])
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, stored, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	missing := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||\n"}}
	restartService := &operatorMutationService{lifecycle: t.Context(), owner: restarted, effects: &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, active: map[string]*activeRuntimeEffect{}}, reviewer: missing}
	if err := restartService.stopBoundReviewer(t.Context(), request, reviewer.ID); err == nil {
		t.Fatal("restart completed Cancel while exact child group remained live")
	}
	if err := child.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := child.Wait(); err == nil {
		t.Fatal("test child unexpectedly exited successfully after SIGKILL")
	}
	if err := restartService.stopBoundReviewer(t.Context(), request, reviewer.ID); err != nil {
		t.Fatalf("restart could not prove bound group death: %v", err)
	}
	cancelled := manifest
	cancelled.State, cancelled.Diagnostic, cancelled.UpdatedAt = "cancelled", cancelCommand.Runtime.Reason, time.Unix(50, 0).UTC()
	if _, err := restarted.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stop)), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
		t.Fatalf("Cancel receipt could not complete after group death proof: %v", err)
	}
	receipt, _ := operatorReceiptByID(mustOwnerSnapshot(t, restarted).State, cancelRequest.RequestID)
	if receipt.State != "completed" {
		t.Fatalf("Cancel receipt remained pending after exact group death: %#v", receipt)
	}
	if err := restarted.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	finished, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if receipt, ok := operatorReceiptByID(finished, cancelRequest.RequestID); !ok || receipt.State != "completed" || !finished.ReviewerProofs[proofKey].DeadProved {
		t.Fatalf("disk lost terminal Cancel receipt or group-death proof: receipt=%#v proof=%#v", receipt, finished.ReviewerProofs[proofKey])
	}
}

type forgedTerminalStopBoundary struct {
	parentPID  int
	reviewerID string
	session    string
	kills      int
}

func (b *forgedTerminalStopBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
	}
	switch command.Args[0] {
	case "display-message":
		return agentruntime.Result{Output: reviewerPaneTestOutput("0||||", b.session, "$1", b.parentPID, "agent-symphony review-pane tmux /tmp/launch /tmp/terminal review-"+b.reviewerID)}, nil
	case "if-shell":
		b.kills++ // The fake tmux ACK deliberately does not terminate the child.
		return agentruntime.Result{}, nil
	case "has-session":
		return agentruntime.Result{Exited: true, Code: 1, Output: "can't find session: " + b.session}, errors.New("session is absent")
	case "wait-for":
		return agentruntime.Result{}, nil
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer tmux command")
}

func TestReviewerStopDoesNotTrustTmuxKillAck(t *testing.T) {
	reviewerID := strings.Repeat("a", 32)
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyReader.Close()
	defer readyWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer holdReader.Close()
	defer holdWriter.Close()
	child := exec.Command("/bin/sh", "-c", `printf ready >&3; IFS= read -r _`)
	child.Stdin = holdReader
	child.ExtraFiles = []*os.File{readyWriter}
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-child.Process.Pid, syscall.SIGKILL); _ = child.Wait() }()
	ready := make(chan string, 1)
	go func() {
		buffer := make([]byte, 5)
		n, _ := readyReader.Read(buffer)
		ready <- string(buffer[:n])
	}()
	select {
	case value := <-ready:
		if value != "ready" {
			t.Fatalf("child readiness = %q", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child did not start")
	}
	boundary := &forgedTerminalStopBoundary{parentPID: os.Getpid(), reviewerID: reviewerID}
	service := &operatorMutationService{lifecycle: t.Context(), reviewer: boundary}
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
	if err != nil {
		t.Fatal(err)
	}
	boundary.session = session
	if err := service.stopReviewerSession(t.Context(), session, reviewerID, child.Process.Pid); err == nil || boundary.kills != 1 {
		t.Fatalf("forged terminal or kill ACK completed cancellation: err=%v kills=%d", err, boundary.kills)
	}
	if err := syscall.Kill(-child.Process.Pid, 0); err != nil {
		t.Fatalf("fake did not preserve live reviewer group: %v", err)
	}
}

type liveReviewerArtifactBoundary struct{ artifactCalls int }

func (b *liveReviewerArtifactBoundary) call(_ context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" {
		return agentruntime.Result{Output: "0||||\n"}, nil
	}
	if operation == "review-result" {
		b.artifactCalls++
		return agentruntime.Result{Exited: true, Code: reviewResultInvalidCode}, errors.New("forged clean result")
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
}

func TestPlanReviewerForgedTerminalCannotFinishLivePane(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	target := "o/r#73 plan sha256:" + digestText(issue.Body)
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running", ReviewState: "running", ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target, ReviewBase: attempt.BaseSHA, ReviewHead: attempt.BaseSHA, ReviewSnapshot: snapshot, ReviewSession: session}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	launchPath, terminalPath := reviewerLifecyclePaths(snapshot, target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(launchPath, identity); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	boundary := &liveReviewerArtifactBoundary{}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if !pending || err != nil || boundary.artifactCalls != 0 {
		t.Fatalf("forged terminal overrode live reviewer: pending=%v err=%v artifact_calls=%d", pending, err, boundary.artifactCalls)
	}
}

func TestReviewCleanupFailsClosedOnAmbiguousTmuxFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    agentruntime.Result
		err       error
		wantError bool
		wantKill  bool
	}{
		{name: "exact missing", status: agentruntime.Result{Output: "||||||||||\n"}},
		{name: "no server", status: agentruntime.Result{Exited: true, Code: 1, Output: "no server running on /private/tmp/tmux-501/test"}, err: errors.New("worker boundary command exited 1")},
		{name: "live", status: agentruntime.Result{Output: "0|||||$1|12345|/bin/sh\n"}, wantError: true},
		{name: "exit one is ambiguous", status: agentruntime.Result{Exited: true, Code: 1, Output: "permission denied"}, err: errors.New("tmux unavailable"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: strings.Repeat("a", 40)}
			snapshot, session := reviewIdentity(attempt, root)
			target := "o/r#73 plan sha256:" + digestText("body")
			resultRoot := filepath.Dir(reviewResultPath(snapshot, target))
			boundary := &reviewerSessionStopBoundary{status: test.status, err: test.err}
			err := cleanupReviewResources(t.Context(), boundary, nil, attempt, attempt.BaseSHA, target, snapshot, session, root)
			if (err != nil) != test.wantError || (len(boundary.killed) != 0) != test.wantKill {
				t.Fatalf("cleanup err=%v killed=%v", err, boundary.killed)
			}
			for _, path := range []string{snapshot, resultRoot} {
				_, statErr := os.Lstat(path)
				if !test.wantError && !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("cleanup path %s stat=%v", path, statErr)
				}
			}
		})
	}
}

func TestReviewCleanupRetainsUnboundSnapshotWhenSessionIsMissing(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	snapshot, session := reviewIdentity(attempt, root)
	if err := os.MkdirAll(snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||\n"}}
	if err := cleanupReviewResources(t.Context(), boundary, nil, attempt, attempt.BaseSHA, "", snapshot, session, root); err == nil {
		t.Fatal("unbound reviewer snapshot was deleted from a missing pane")
	}
	if _, err := os.Lstat(snapshot); err != nil {
		t.Fatalf("unbound reviewer snapshot was removed: %v", err)
	}
}

func TestReviewCleanupWithHistoricalMetadataButNoResourcesSucceeds(t *testing.T) {
	stateRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 91, Attempt: 1, BaseSHA: strings.Repeat("a", 40), ReviewState: "clean"}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||\n"}}
	if err := cleanupAttemptReviewResourcesProved(t.Context(), stateRoot, boundary, manifest, true, nil); err != nil {
		t.Fatalf("historical review metadata with no session or resources blocked cleanup: %v", err)
	}
	if len(boundary.killed) != 0 {
		t.Fatalf("cleanup killed an unrelated reviewer session: %v", boundary.killed)
	}
}

func TestReviewCleanupRetainsSnapshotWithNondeadOwnerProof(t *testing.T) {
	stateRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 92, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	snapshotRoot := productionSnapshotRoot(stateRoot)
	if err := os.MkdirAll(snapshotRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	snapshot, session := reviewIdentity(attempt, snapshotRoot)
	if err := os.Mkdir(snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	target := "o/r#92 plan sha256:" + strings.Repeat("b", 64)
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, ReviewState: "running", ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target, ReviewSnapshot: snapshot, ReviewSession: session}
	proofs := map[string]reviewerProcessProof{target: {Target: target, GroupPID: 99999999}}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||\n"}}
	if err := cleanupAttemptReviewResourcesProved(t.Context(), stateRoot, boundary, manifest, true, proofs); err == nil {
		t.Fatal("reviewer snapshot was deleted without owner process-death proof")
	}
	if _, err := os.Stat(snapshot); err != nil {
		t.Fatalf("unproved reviewer snapshot changed: %v", err)
	}
}

func TestReviewCleanupLeavesForeignSameNameDeadPaneUntouched(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 74, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	snapshot, session := reviewIdentity(attempt, root)
	target := "o/r#74 plan sha256:" + digestText("body")
	resultRoot := filepath.Dir(reviewResultPath(snapshot, target))
	for _, path := range []string{snapshot, resultRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "1|||0||$9|12345|/bin/sh\n"}}
	if err := cleanupReviewResources(t.Context(), boundary, nil, attempt, attempt.BaseSHA, target, snapshot, session, root); err == nil || len(boundary.killed) != 0 {
		t.Fatalf("zero-certificate cleanup removed foreign pane: err=%v killed=%v", err, boundary.killed)
	}
	proof := reviewerProcessProof{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Target: target, Mode: agentruntime.ReviewModePlan, EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, DeadProved: true}
	if err := cleanupCertifiedReviewResources(t.Context(), boundary, nil, attempt, attempt.BaseSHA, target, snapshot, session, root, proof); err == nil || len(boundary.killed) != 0 {
		t.Fatalf("old reviewer certificate removed foreign replacement pane: err=%v killed=%v", err, boundary.killed)
	}
	for _, path := range []string{snapshot, resultRoot} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("foreign pane probe removed review resource %s: %v", path, err)
		}
	}
}

type blockedReviewerWatchBoundary struct {
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (b *blockedReviewerWatchBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" && command.Name == "tmux" && len(command.Args) >= 2 && command.Args[0] == "wait-for" {
		if command.Args[1] == "-U" {
			return agentruntime.Result{}, nil
		}
		if command.Args[1] == "-L" {
			b.once.Do(func() { close(b.entered) })
			<-ctx.Done()
			close(b.exited)
			return agentruntime.Result{}, ctx.Err()
		}
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
}

func TestInvalidatedReviewerWatcherExitsWithoutPaneSignal(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	boundary := &blockedReviewerWatchBoundary{entered: make(chan struct{}), exited: make(chan struct{})}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, reviewer: boundary, active: map[string]bool{}, released: map[string]chan struct{}{}}
	service.watchPlanReviewer("watch-cancel-test", effect.ID)
	<-boundary.entered
	service.cancelPlanWatcher(effect.ID)
	<-boundary.exited
}

func TestDuplicateReviewerWatchPreservesOriginalCancellation(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	boundary := &blockedReviewerWatchBoundary{entered: make(chan struct{}), exited: make(chan struct{})}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, reviewer: boundary, active: map[string]bool{}, released: map[string]chan struct{}{}}
	service.watchPlanReviewer("watch-duplicate-test", effect.ID)
	<-boundary.entered
	service.watchPlanReviewer("watch-duplicate-test", effect.ID)
	service.cancelPlanWatcher(effect.ID)
	select {
	case <-boundary.exited:
	case <-time.After(2 * time.Second):
		t.Fatal("duplicate registration lost the original watcher cancellation")
	}
}

func TestReviewerLaunchLeaseCancelsWhenEffectIsSupersededWithoutGenerationChange(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := coordinator.acquireKey(t.Context(), key, effect.IssueGeneration, effect.AttemptGeneration, request.ObservationGeneration, effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	invalidated, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, ExpectedIssueGeneration: effect.IssueGeneration, ExpectedAttemptGeneration: effect.AttemptGeneration, Action: "dismissed", CleanupPhase: "completed"})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.cancelInvalidated(invalidated)
	select {
	case <-run.ctx.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("superseded reviewer launch retained its external lease after owner invalidation")
	}
	coordinator.releaseKey(key, run)
}

func TestAmbiguousReviewerPaneProbeKeepsPendingIntentWithDiagnostic(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*effect)}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: ownerReconciliationEffectIdentity(*effect), GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	current := mustOwnerSnapshot(t, owner)
	current.State.ControlReceipts = append(current.State.ControlReceipts, controlReceipt{Request: operatorRequest("probe-reviewer", "review-plan", *request.Manifest, false), State: "pending", Phase: operatorPhaseReviewPending, EffectID: effect.ID})
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Exited: true, Code: 1, Output: "permission denied"}, err: errors.New("tmux socket unavailable")}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, reviewer: boundary}
	service.scanPendingPlanReviewers(t.Context(), current)
	after := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if after.State != "pending" || !strings.Contains(after.Diagnostic, "reviewer pane probe unavailable") || boundary.killed != nil {
		t.Fatalf("ambiguous probe falsely terminalized reviewer: %#v boundary=%#v", after, boundary)
	}
}

type invalidReviewArtifactBoundary struct{ calls int }

func (b *invalidReviewArtifactBoundary) call(_ context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" {
		return agentruntime.Result{Output: "1|0|||\n"}, nil
	}
	if operation == "review-result" {
		b.calls++
		return agentruntime.Result{Exited: true, Code: reviewResultInvalidCode}, errors.New("invalid reviewer result")
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
}

func TestReviewerPaneRecordsExactLaunchAndTerminalIdentity(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "tmux"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TMUX_PANE", "%123")
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 2, AttemptGeneration: 4, RequestDigest: strings.Repeat("b", 64)}
	launch, terminal := filepath.Join(root, "launch.json"), filepath.Join(root, "terminal.json")
	encoded := `{"effect_id":"` + identity.EffectID + `","issue_generation":2,"attempt_generation":4,"request_digest":"` + identity.RequestDigest + `"}`
	args := []string{"tmux", launch, terminal, reviewerSignal(identity), reviewerStartSignal(identity), encoded, "--", "sh", "-c", "exit 0"}
	code, signal, err := runReviewerPane(args, io.Discard, io.Discard)
	if err != nil || code != 0 || signal != 0 {
		t.Fatalf("reviewer wrapper result code=%d signal=%d err=%v", code, signal, err)
	}
	record, err := readReviewerTerminal(launch, terminal, identity)
	if err != nil || record == nil || record.ExitCode != 0 || record.Signal != 0 {
		t.Fatalf("exact terminal record=%#v err=%v", record, err)
	}
	changed := identity
	changed.AttemptGeneration++
	if _, err := readReviewerTerminal(launch, terminal, changed); err == nil {
		t.Fatal("different attempt generation accepted prior reviewer launch")
	}
}

func TestPlanReviewerReplayFailsClosedWithoutExactLaunch(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 72, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	target := "o/r#72 plan sha256:" + digestText(issue.Body)
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running", ReviewState: "running", ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target, ReviewBase: attempt.BaseSHA, ReviewHead: attempt.BaseSHA, ReviewSnapshot: snapshot, ReviewSession: session}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||"}}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if !pending || err == nil || len(boundary.killed) != 0 {
		t.Fatalf("missing launch replay pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
	// A new owner-reserved gate cannot release reviewer code until the owner
	// durably binds a PID. This remains true across a crash during git clone.
	partial := filepath.Join(snapshot, "partial-clone")
	if err := os.MkdirAll(partial, 0o700); err != nil {
		t.Fatal(err)
	}
	gated := identity
	gated.GateProtocol = true
	_, pending, err = runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &gated, true)
	if !pending || err == nil || len(boundary.killed) != 0 {
		t.Fatalf("gated prelaunch crash falsely certified an existing partial snapshot: pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
	if _, err := os.Stat(partial); err != nil {
		t.Fatalf("prelaunch crash removed partial resources before owner proof: %v", err)
	}
	launchPath, terminalPath := reviewerLifecyclePaths(snapshot, target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	wrong := identity
	wrong.AttemptGeneration++
	if err := writeReviewerRecord(launchPath, wrong); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: wrong}); err != nil {
		t.Fatal(err)
	}
	_, pending, err = runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if !pending || err == nil || len(boundary.killed) != 0 {
		t.Fatalf("mismatched launch replay pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
	identity.ChildPID = syscall.Getpgrp() // A live bound group may never be terminalized from a missing record.
	if err := os.Remove(launchPath); err != nil {
		t.Fatal(err)
	}
	_, pending, err = runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if !pending || err == nil || len(boundary.killed) != 0 {
		t.Fatalf("live bound reviewer group was terminalized without launch proof: pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
}

func TestPlanReviewerPriorCleanupFailureKeepsUnprovedIntentPending(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 74, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running"}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	snapshot, _ := reviewIdentity(attempt, root)
	oldPath := filepath.Join(snapshot, "older-uncertified-review")
	if err := os.MkdirAll(oldPath, 0o700); err != nil {
		t.Fatal(err)
	}
	boundary := &forbiddenReviewerBoundary{}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, false)
	if !pending || err == nil || errors.Is(err, errReviewerTerminal) || boundary.calls != 0 {
		t.Fatalf("failed prior cleanup created a new-effect no-run proof: pending=%v err=%v calls=%d", pending, err, boundary.calls)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("uncertified prior snapshot was removed: %v", err)
	}
	_, pending, err = runIndependentReviewCore(t.Context(), attempt, boundary, nil, nil, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, false)
	if !pending || err == nil || errors.Is(err, errReviewerTerminal) || boundary.calls != 0 {
		t.Fatalf("early validation failure certified old same-target resources: pending=%v err=%v calls=%d", pending, err, boundary.calls)
	}
	if _, err := os.Stat(oldPath); err != nil {
		t.Fatalf("early validation failure removed old resources: %v", err)
	}
}

func TestPlanReviewerDirectSessionSetupFailureRemainsPending(t *testing.T) {
	base, _, source, _ := testWorkerExportBoundary(t)
	root := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 75, Number: 1, BaseSHA: base}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, Body: "plan"}
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, State: "running"}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64), GateProtocol: true}
	boundary := &directReviewerSessionBoundary{setupError: true}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, source, base, root, agentruntime.ReviewModePlan, &identity, false, func() error { identity.SessionRequested = true; return nil })
	if !pending || err == nil || !boundary.requested || !boundary.directCommand || boundary.killed {
		t.Fatalf("setup error falsely completed or killed uncertain direct wrapper: pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
}

func TestPlanReviewerAmbiguousDirectSessionNeverKillsByName(t *testing.T) {
	base, _, source, _ := testWorkerExportBoundary(t)
	root := t.TempDir()
	t.Cleanup(func() {
		_ = filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 77, Number: 1, BaseSHA: base}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, Body: "plan"}
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, State: "running"}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64), GateProtocol: true}
	boundary := &directReviewerSessionBoundary{newSessionError: true}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, source, base, root, agentruntime.ReviewModePlan, &identity, false, func() error { identity.SessionRequested = true; return nil })
	if !pending || err == nil || !boundary.requested || !boundary.directCommand || boundary.killed {
		t.Fatalf("uncertain direct-wrapper creation was falsely terminalized or killed by name: pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
	_, pending, err = runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, source, base, root, agentruntime.ReviewModePlan, &identity, true)
	if !pending || err == nil || boundary.killed {
		t.Fatalf("restart after missing launch identity falsely completed or killed foreign session: pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
}

func TestPlanReviewerInvalidArtifactIsTerminalFailure(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	target := "o/r#73 plan sha256:" + digestText(issue.Body)
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running", ReviewState: "running", ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target, ReviewBase: attempt.BaseSHA, ReviewHead: attempt.BaseSHA, ReviewSnapshot: snapshot, ReviewSession: session}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64), ChildPID: 99999999}
	launchPath, terminalPath := reviewerLifecyclePaths(snapshot, target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(launchPath, identity); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	boundary := &invalidReviewArtifactBoundary{}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if pending || !errors.Is(err, errReviewerTerminal) || boundary.calls != 1 {
		t.Fatalf("invalid artifact replay pending=%v err=%v boundary_calls=%d", pending, err, boundary.calls)
	}
}
