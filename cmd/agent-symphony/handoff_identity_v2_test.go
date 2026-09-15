package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestBoundHandoffParksBeforeReleaseAndPreservesPaneIdentity(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-bound-handoff-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	oldExec, oldHelper := hostExecRunner, hostExecutable
	t.Cleanup(func() { hostExecRunner, hostExecutable = oldExec, oldHelper })
	hostExecRunner = (agentruntime.ExecRunner{}).Run
	helper := filepath.Join(t.TempDir(), "agent-symphony")
	build := exec.Command("go", "build", "-o", helper, ".")
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build handoff helper: %v: %s", err, output)
	}
	hostExecutable = func() (string, error) { return helper, nil }
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(root, "worktree")
	if err := os.MkdirAll(filepath.Join(worktree, ".agent-symphony"), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Version: agentruntime.ManifestVersion2, Session: "bound-handoff", Worktree: worktree, LogPath: filepath.Join(worktree, ".agent-symphony", "attempt.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	created, err := exec.Command(tmux, "new-session", "-d", "-P", "-F", agentruntime.ImplementationPaneFormat, "-s", manifest.Session, "-c", worktree, "--", "/bin/sh", "-c", "exec sleep 30").CombinedOutput()
	if err != nil {
		t.Fatalf("create old worker: %v: %s", err, created)
	}
	if output, err := exec.Command(tmux, "set-option", "-p", "-t", agentruntime.PaneTarget(manifest.Session), "@agent-symphony-launch-token", manifest.LaunchToken).CombinedOutput(); err != nil {
		t.Fatalf("tag old worker: %v: %s", err, output)
	}
	observed, err := exec.Command(tmux, "display-message", "-p", "-t", agentruntime.PaneTarget(manifest.Session), agentruntime.ImplementationPaneFormat).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	pane, err := agentruntime.ParseImplementationPane(string(observed))
	if err != nil {
		t.Fatal(err)
	}
	old, err := agentruntime.BindImplementationPane(manifest, manifest.LaunchID, "unknown", pane)
	if err != nil || agentruntime.WriteImplementationBinding(manifest, old) != nil {
		t.Fatalf("bind old worker: %v", err)
	}
	handoff := json.RawMessage(`{"type":"agent-symphony-handoff-v1","key":"test"}`)
	request := handoffRequest{Manifest: manifest, Handoff: handoff, OutcomePath: handoffReceiptPath(worktree, "test"), OutcomeToken: "head", Command: []string{"/bin/sh", "-c", `printf '{"type":"agent-symphony-result-v1","validation":"passed","documentation":"none"}\n'`}, CandidateLaunchToken: strings.Repeat("c", 32), CandidateLaunchID: strings.Repeat("d", 32)}
	input, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := prepareHandoffV2(t.Context(), input, root)
	if err != nil || prepared != request.CandidateLaunchID+":"+request.CandidateLaunchToken {
		t.Fatalf("prepare bound handoff = %q, %v", prepared, err)
	}
	candidate := handoffCandidateManifest(request)
	binding, err := agentruntime.ReadImplementationBinding(candidate)
	if err != nil || binding.PaneID != old.PaneID || binding.PanePID == old.PanePID {
		t.Fatalf("parked candidate binding = %#v, %v", binding, err)
	}
	if _, err := os.Stat(filepath.Join(worktree, ".agent-symphony", "handoffs", "test.launched")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("worker ran before owner-authorized release: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	ack, err := releaseHandoffV2(ctx, input, root)
	if err != nil || !validHandoffAck(ack, &handoffEffectRequest{Key: "test", OutcomePath: request.OutcomePath, OutcomeToken: request.OutcomeToken}) {
		t.Fatalf("release bound handoff = %q, %v", ack, err)
	}
	verified, err := verifyHandoff(ctx, input, root)
	if err != nil || verified != ack {
		t.Fatalf("verify bound handoff = %q, %v", verified, err)
	}
	current, err := exec.Command(tmux, "display-message", "-p", "-t", binding.PaneID, agentruntime.ImplementationPaneFormat).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	actual, err := agentruntime.ParseImplementationPane(string(current))
	if err != nil || !binding.Matches(candidate, actual) {
		t.Fatalf("candidate identity changed after exec: %#v, %v", actual, err)
	}
	if replay, err := releaseHandoffV2(ctx, input, root); err != nil || replay != ack {
		t.Fatalf("same-effect release replay = %q, %v", replay, err)
	}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: manifest}})
	result, err := compensateHandoffV2(ctx, cleanup, root)
	assertHandoffCompensationProof(t, result, err, request, false, "killed")
	if output, err := exec.Command(tmux, "has-session", "-t", "="+manifest.Session).CombinedOutput(); err == nil {
		t.Fatalf("compensated candidate still has a session: %s", output)
	}
}

func boundHandoffCompensationFixture(t *testing.T) (string, string, handoffRequest, []byte, agentruntime.ImplementationLaunchBinding) {
	t.Helper()
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-handoff-compensation-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	oldExec, oldHelper := hostExecRunner, hostExecutable
	t.Cleanup(func() { hostExecRunner, hostExecutable = oldExec, oldHelper })
	hostExecRunner = (agentruntime.ExecRunner{}).Run
	hostExecutable = func() (string, error) { return "/bin/true", nil }
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	worktree := filepath.Join(root, "worktree")
	if err := os.MkdirAll(filepath.Join(worktree, ".agent-symphony"), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Version: agentruntime.ManifestVersion2, Session: "bound-compensation", Worktree: worktree, LogPath: filepath.Join(worktree, ".agent-symphony", "attempt.log"), LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	created, err := exec.Command(tmux, "new-session", "-d", "-P", "-F", agentruntime.ImplementationPaneFormat, "-s", manifest.Session, "-c", worktree, "--", "/bin/sh", "-c", "exec sleep 30").CombinedOutput()
	if err != nil {
		t.Fatalf("create old worker: %v: %s", err, created)
	}
	if output, err := exec.Command(tmux, "set-option", "-p", "-t", agentruntime.PaneTarget(manifest.Session), "@agent-symphony-launch-token", manifest.LaunchToken).CombinedOutput(); err != nil {
		t.Fatalf("tag old worker: %v: %s", err, output)
	}
	observed, err := exec.Command(tmux, "display-message", "-p", "-t", agentruntime.PaneTarget(manifest.Session), agentruntime.ImplementationPaneFormat).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	pane, err := agentruntime.ParseImplementationPane(string(observed))
	if err != nil {
		t.Fatal(err)
	}
	old, err := agentruntime.BindImplementationPane(manifest, manifest.LaunchID, "unknown", pane)
	if err != nil {
		t.Fatal(err)
	}
	if err := agentruntime.WriteImplementationBinding(manifest, old); err != nil {
		t.Fatal(err)
	}
	request := handoffRequest{Manifest: manifest, Handoff: json.RawMessage(`{"type":"agent-symphony-handoff-v1","key":"test"}`), OutcomePath: handoffReceiptPath(worktree, "test"), OutcomeToken: "head", Command: []string{"/bin/sh", "-c", "printf worker"}, CandidateLaunchToken: strings.Repeat("c", 32), CandidateLaunchID: strings.Repeat("d", 32)}
	input, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return tmux, root, request, input, old
}

func TestBoundCleanupAndCompensationRejectUnlinkedOriginalServer(t *testing.T) {
	tmux, root, request, _, old := boundHandoffCompensationFixture(t)
	socketRoot := os.Getenv("TMUX_TMPDIR")
	oldSocket := filepath.Join(socketRoot, fmt.Sprintf("tmux-%d", os.Getuid()), "default")
	orphanSocket := filepath.Join(socketRoot, "orphaned-s1")
	if err := os.Rename(oldSocket, orphanSocket); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = exec.Command(tmux, "-S", orphanSocket, "kill-server").Run() })
	if err := stopAttemptSession(t.Context(), request.Manifest); err == nil {
		t.Fatal("cleanup certified absent S1 even though its bound worker remains live")
	}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}})
	if proof, err := compensateHandoffV2(t.Context(), cleanup, root); err == nil || proof != "" {
		t.Fatalf("compensation falsely certified inaccessible S1: proof=%q err=%v", proof, err)
	}
	if _, err := os.Stat(handoffCompensationProofPath(handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest})); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("false durable compensation proof: %v", err)
	}
	if output, err := exec.Command(tmux, "-S", orphanSocket, "display-message", "-p", "-t", old.PaneID, agentruntime.ImplementationPaneFormat).CombinedOutput(); err != nil {
		t.Fatalf("original S1 worker was touched: %v: %s", err, output)
	}
	if output, err := exec.Command(tmux, "new-session", "-d", "-s", "foreign-s2").CombinedOutput(); err != nil {
		t.Fatalf("create S2: %v: %s", err, output)
	}
	if err := stopAttemptSession(t.Context(), request.Manifest); err == nil {
		t.Fatal("cleanup accepted foreign S2 inventory as original worker absence")
	}
	if proof, err := compensateHandoffV2(t.Context(), cleanup, root); err == nil || proof != "" {
		t.Fatalf("compensation accepted foreign S2 inventory: proof=%q err=%v", proof, err)
	}
	if _, err := os.Stat(request.Manifest.Worktree); err != nil {
		t.Fatalf("unproved cleanup removed worker worktree: %v", err)
	}
}

func TestBoundHandoffDismissBeforeReleaseStopsCandidate(t *testing.T) {
	tmux, root, request, input, old := boundHandoffCompensationFixture(t)
	if _, err := prepareHandoffV2(t.Context(), input, root); err != nil {
		t.Fatal(err)
	}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}, PreserveOld: true})
	result, err := compensateHandoffV2(t.Context(), cleanup, root)
	assertHandoffCompensationProof(t, result, err, request, true, "killed")
	if output, err := exec.Command(tmux, "has-session", "-t", "="+request.Manifest.Session).CombinedOutput(); err == nil {
		t.Fatalf("dismissed candidate still has a session: %s", output)
	}
	if _, err := releaseHandoffV2(t.Context(), input, root); err == nil {
		t.Fatal("stale release executed after Dismiss compensation")
	}
	if _, err := os.Stat(filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", "test.launched")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dismissed worker executed: %v", err)
	}
	if old.PanePID < 2 {
		t.Fatal("fixture did not bind old pane")
	}
}

func TestBoundHandoffDismissMarkerWinsBeforeHostQueue(t *testing.T) {
	tmux, root, request, input, old := boundHandoffCompensationFixture(t)
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}, PreserveOld: true})
	result, err := compensateHandoffV2(t.Context(), cleanup, root)
	assertHandoffCompensationProof(t, result, err, request, true, "marked-old")
	if _, err := prepareHandoffV2(t.Context(), input, root); err == nil {
		t.Fatal("host retagged a Dismiss-invalidated old pane")
	}
	observed, err := exec.Command(tmux, "display-message", "-p", "-t", old.PaneID, agentruntime.ImplementationPaneFormat).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	pane, err := agentruntime.ParseImplementationPane(string(observed))
	if err != nil || !old.Matches(request.Manifest, pane) {
		t.Fatalf("Dismiss marker damaged old worker: %#v, %v", pane, err)
	}
}

func TestBoundHandoffCrashAfterRespawnBeforeBindingCanCompensate(t *testing.T) {
	tmux, root, request, input, _ := boundHandoffCompensationFixture(t)
	if _, err := prepareHandoffV2(t.Context(), input, root); err != nil {
		t.Fatal(err)
	}
	path := agentruntime.ImplementationBindingPath(handoffCandidateManifest(request), request.CandidateLaunchID)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}})
	result, err := compensateHandoffV2(t.Context(), cleanup, root)
	assertHandoffCompensationProof(t, result, err, request, false, "killed")
	if output, err := exec.Command(tmux, "has-session", "-t", "="+request.Manifest.Session).CombinedOutput(); err == nil {
		t.Fatalf("unbound parked candidate survived compensation: %s", output)
	}
}

func TestBoundHandoffRestartWithPhaseBeforeTmuxQueueReplaysSameIntent(t *testing.T) {
	_, root, request, input, old := boundHandoffCompensationFixture(t)
	if err := os.MkdirAll(filepath.Dir(handoffPhasePath(request, "test")), 0o700); err != nil {
		t.Fatal(err)
	}
	_, recipient := handoffBinding(request)
	phase, _ := json.Marshal(handoffLaunchPhase{Old: old, Candidate: request.CandidateLaunchToken, EffectID: request.CandidateLaunchID, Gate: handoffGateCommand(request, "test", recipient, "/bin/true")})
	if err := writeImmutable(handoffPhasePath(request, "test"), phase); err != nil {
		t.Fatal(err)
	}
	if prepared, err := prepareHandoffV2(t.Context(), input, root); err != nil || prepared != request.CandidateLaunchID+":"+request.CandidateLaunchToken {
		t.Fatalf("phase-before-queue replay = %q, %v", prepared, err)
	}
	if _, err := agentruntime.ReadImplementationBinding(handoffCandidateManifest(request)); err != nil {
		t.Fatalf("replay did not durably bind candidate: %v", err)
	}
}

func TestBoundHandoffRestartWithParkedGateBeforeSidecarRebinds(t *testing.T) {
	_, root, request, input, _ := boundHandoffCompensationFixture(t)
	if _, err := prepareHandoffV2(t.Context(), input, root); err != nil {
		t.Fatal(err)
	}
	candidate := handoffCandidateManifest(request)
	path := agentruntime.ImplementationBindingPath(candidate, request.CandidateLaunchID)
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if prepared, err := prepareHandoffV2(t.Context(), input, root); err != nil || prepared != request.CandidateLaunchID+":"+request.CandidateLaunchToken {
		t.Fatalf("parked-gate replay = %q, %v", prepared, err)
	}
	if _, err := agentruntime.ReadImplementationBinding(candidate); err != nil {
		t.Fatalf("parked gate did not regain durable sidecar: %v", err)
	}
	if _, err := os.Stat(filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", "test.launched")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("parked gate executed before owner release: %v", err)
	}
}

func TestBoundHandoffRenamedSessionIsNotMistakenForAbsence(t *testing.T) {
	tmux, root, request, _, old := boundHandoffCompensationFixture(t)
	if output, err := exec.Command(tmux, "rename-session", "-t", old.SessionID, "renamed-foreign").CombinedOutput(); err != nil {
		t.Fatalf("rename bound session: %v: %s", err, output)
	}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}, PreserveOld: true})
	if result, err := compensateHandoffV2(t.Context(), cleanup, root); err == nil || result != "" {
		t.Fatalf("renamed live pane was mistaken for absence: %q, %v", result, err)
	}
	if output, err := exec.Command(tmux, "has-session", "-t", old.SessionID).CombinedOutput(); err != nil {
		t.Fatalf("renamed pane was signalled: %v: %s", err, output)
	}
}

func TestBoundHandoffPrelockedChannelCannotDelayOldPaneGuard(t *testing.T) {
	tmux, root, request, input, old := boundHandoffCompensationFixture(t)
	channel := agentruntime.ImplementationGateChannel(request.CandidateLaunchID)
	if output, err := exec.Command(tmux, "wait-for", "-L", channel).CombinedOutput(); err != nil {
		t.Fatalf("prelock candidate channel: %v: %s", err, output)
	}
	t.Cleanup(func() { _ = exec.Command(tmux, "wait-for", "-U", channel).Run() })
	entered := make(chan struct{})
	baseRunner := hostExecRunner
	hostExecRunner = func(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		if len(command.Args) == 3 && command.Args[0] == "wait-for" && command.Args[1] == "-L" && command.Args[2] == channel {
			close(entered)
		}
		return baseRunner(ctx, command)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	prepared := make(chan error, 1)
	go func() { _, err := prepareHandoffV2(ctx, input, root); prepared <- err }()
	<-entered
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}, PreserveOld: true})
	result, err := compensateHandoffV2(t.Context(), cleanup, root)
	assertHandoffCompensationProof(t, result, err, request, true, "marked-old")
	cancel()
	if err := <-prepared; err == nil {
		t.Fatal("prelocked prepare unexpectedly succeeded")
	}
	if output, err := exec.Command(tmux, "wait-for", "-U", channel).CombinedOutput(); err != nil {
		t.Fatalf("unlock test channel: %v: %s", err, output)
	}
	observed, err := exec.Command(tmux, "display-message", "-p", "-t", old.PaneID, agentruntime.ImplementationPaneFormat).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	pane, err := agentruntime.ParseImplementationPane(string(observed))
	if err != nil || !old.Matches(request.Manifest, pane) {
		t.Fatalf("prelocked delayed launch touched dismissed old pane: %#v, %v", pane, err)
	}
}

func assertHandoffCompensationProof(t *testing.T, output string, resultErr error, request handoffRequest, preserveOld bool, disposition string) {
	t.Helper()
	old, err := agentruntime.ReadImplementationBinding(request.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	var proof handoffCompensationProof
	if resultErr != nil || json.Unmarshal([]byte(output), &proof) != nil || !validHandoffCompensationProof(proof, handoffCompensationRequest{Candidate: handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}, PreserveOld: preserveOld}, old) || proof.Disposition != disposition {
		t.Fatalf("compensation proof = %q, %v", output, resultErr)
	}
}
