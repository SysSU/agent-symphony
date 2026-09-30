package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestBoundHandoffUsesFreshOwnerCertifiedBroker(t *testing.T) {
	tmux, root, request, old, oldTerminal := boundBrokerHandoffFixture(t)
	input, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	defer cancel()
	output, err := prepareHandoffV2(ctx, input, root)
	var prepared handoffPreparedTerminal
	if err != nil || json.Unmarshal([]byte(output), &prepared) != nil {
		t.Fatalf("prepare bound handoff = %q, %v", output, err)
	}
	candidate := handoffCandidateManifest(request)
	if !agentruntime.ValidImplementationBinding(candidate, prepared.Implementation, candidate.LaunchID) || !agentruntime.ValidImplementationTerminalBinding(candidate, prepared.Terminal) || prepared.Terminal.OuterPID != prepared.Implementation.PanePID {
		t.Fatalf("prepared certificate is invalid: %#v", prepared)
	}
	if prepared.Implementation.ServerPID == old.ServerPID && prepared.Implementation.SessionID == old.SessionID || prepared.Implementation.PanePID == old.PanePID {
		t.Fatalf("handoff reused the old outer identity: old=%#v new=%#v", old, prepared.Implementation)
	}
	dead, err := agentruntime.TerminalBrokerDead(agentruntime.TerminalBrokerPath(request.Manifest), oldTerminal)
	if err != nil || !dead {
		t.Fatalf("old broker death is unproved: dead=%t err=%v", dead, err)
	}
	if _, err := os.Stat(filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", "test.launched")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("candidate ran before owner-persisted release: %v", err)
	}
	request.PreparedImplementation, request.PreparedTerminal = &prepared.Implementation, &prepared.Terminal
	releaseInput, _ := json.Marshal(request)
	ack, err := releaseHandoffV2(ctx, releaseInput, root)
	if err != nil || !validHandoffAck(ack, &handoffEffectRequest{Key: "test", OutcomePath: request.OutcomePath, OutcomeToken: request.OutcomeToken}) {
		t.Fatalf("release bound handoff = %q, %v", ack, err)
	}
	verified, err := verifyHandoff(ctx, releaseInput, root)
	if err != nil || verified != ack {
		t.Fatalf("verify bound handoff = %q, %v", verified, err)
	}
	invalidation := handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest, Implementation: &prepared.Implementation, Terminal: &prepared.Terminal}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: invalidation})
	proof, err := compensateHandoffV2(ctx, cleanup, root)
	assertHandoffCompensationProof(t, proof, err, invalidation, false, "killed")
	if output, err := exec.Command(tmux, "has-session", "-t", "="+request.Manifest.Session).CombinedOutput(); err == nil {
		t.Fatalf("compensated candidate still has a session: %s", output)
	}
}

func TestBoundHandoffRejectsMissingOwnerCertificate(t *testing.T) {
	_, root, request, _, _ := boundBrokerHandoffFixture(t)
	request.CurrentTerminal = nil
	input, _ := json.Marshal(request)
	if output, err := prepareHandoffV2(t.Context(), input, root); err == nil || output != "" {
		t.Fatalf("missing owner certificate was accepted: output=%q err=%v", output, err)
	}
}

func TestBoundHandoffCompensationRejectsUncertifiedCandidate(t *testing.T) {
	_, root, request, _, _ := boundBrokerHandoffFixture(t)
	input, _ := json.Marshal(request)
	if _, err := prepareHandoffV2(t.Context(), input, root); err != nil {
		t.Fatal(err)
	}
	invalidation := handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: invalidation})
	if proof, err := compensateHandoffV2(t.Context(), cleanup, root); err == nil || proof != "" || !strings.Contains(err.Error(), "without an owner certificate") {
		t.Fatalf("uncertified candidate was cleaned optimistically: proof=%q err=%v", proof, err)
	}
}

func TestBoundHandoffRenamedCertifiedBrokerIsNotMistakenForAbsence(t *testing.T) {
	tmux, root, request, _, _ := boundBrokerHandoffFixture(t)
	input, _ := json.Marshal(request)
	output, err := prepareHandoffV2(t.Context(), input, root)
	var prepared handoffPreparedTerminal
	if err != nil || json.Unmarshal([]byte(output), &prepared) != nil {
		t.Fatalf("prepare bound handoff = %q, %v", output, err)
	}
	if output, err := exec.Command(tmux, "rename-session", "-t", prepared.Implementation.SessionID, "renamed-certified-candidate").CombinedOutput(); err != nil {
		t.Fatalf("rename candidate session: %v: %s", err, output)
	}
	invalidation := handoffCandidateInvalidation{EffectID: request.CandidateLaunchID, Token: request.CandidateLaunchToken, Key: "test", Manifest: request.Manifest, Implementation: &prepared.Implementation, Terminal: &prepared.Terminal}
	cleanup, _ := json.Marshal(handoffCompensationRequest{Candidate: invalidation})
	if proof, err := compensateHandoffV2(t.Context(), cleanup, root); err == nil || proof != "" {
		t.Fatalf("renamed candidate was mistaken for absence: proof=%q err=%v", proof, err)
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "=renamed-certified-candidate").CombinedOutput(); err != nil {
		t.Fatalf("renamed candidate session was touched ambiguously: %v: %s", err, output)
	}
}

func boundBrokerHandoffFixture(t *testing.T) (string, string, handoffRequest, agentruntime.ImplementationLaunchBinding, agentruntime.TerminalBrokerBinding) {
	t.Helper()
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
	worktree, stateRoot := filepath.Join(root, "worktree"), filepath.Join(root, "state")
	logPath := filepath.Join(stateRoot, "attempts", "o-r", "1-1", "agent.log")
	if err := os.MkdirAll(filepath.Join(worktree, ".agent-symphony"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Version: agentruntime.ManifestVersion2, Repository: "o/r", Issue: 1, Attempt: 1, Session: "bound-handoff", Worktree: worktree, LogPath: logPath, LaunchToken: strings.Repeat("a", 32), LaunchID: strings.Repeat("b", 32)}
	runtime := &agentruntime.Runtime{Root: root, StateRoot: stateRoot, Tmux: tmux, Helper: helper}
	command := agentruntime.BoundPaneExitStatusCommand(helper, tmux, manifest, []string{"/bin/sh", "-c", "exec sleep 30"})
	old, terminal, err := runtime.PrepareBoundTerminalBroker(t.Context(), manifest, os.Environ(), command)
	if err != nil {
		t.Fatalf("prepare old broker: %v", err)
	}
	if err := agentruntime.ReleaseTerminalBroker(t.Context(), terminal); err != nil {
		t.Fatal(err)
	}
	request := handoffRequest{
		Manifest: manifest, Handoff: json.RawMessage(`{"type":"agent-symphony-handoff-v1","key":"test"}`),
		OutcomePath: handoffReceiptPath(worktree, "test"), OutcomeToken: "head",
		Command:              []string{"/bin/sh", "-c", "printf worker-started >&2; exec sleep 30"},
		CandidateLaunchToken: strings.Repeat("c", 32), CandidateLaunchID: strings.Repeat("d", 32),
		CurrentImplementation: &old, CurrentTerminal: &terminal,
	}
	return tmux, root, request, old, terminal
}

func assertHandoffCompensationProof(t *testing.T, output string, resultErr error, candidate handoffCandidateInvalidation, preserveOld bool, disposition string) {
	t.Helper()
	old, err := agentruntime.ReadImplementationBinding(candidate.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	request := handoffCompensationRequest{Candidate: candidate, PreserveOld: preserveOld}
	var proof handoffCompensationProof
	if resultErr != nil || json.Unmarshal([]byte(output), &proof) != nil || !validHandoffCompensationProof(proof, request, old) || proof.Disposition != disposition {
		t.Fatalf("compensation proof = %q, %v", output, resultErr)
	}
}
