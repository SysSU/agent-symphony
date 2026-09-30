package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// A phase marker is durable before the old pane is retagged. It identifies
// both sides of the transition if the host exits before binding the new gate.
type handoffLaunchPhase struct {
	Old       agentruntime.ImplementationLaunchBinding `json:"old"`
	Candidate string                                   `json:"candidate"`
	EffectID  string                                   `json:"effect_id"`
	Gate      []string                                 `json:"gate"`
}

func handoffCandidateManifest(request handoffRequest) agentruntime.Manifest {
	manifest := request.Manifest
	manifest.LaunchToken, manifest.LaunchID = request.CandidateLaunchToken, request.CandidateLaunchID
	return manifest
}

func readHandoffPhase(path string) ([]byte, error) {
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return nil, err
	}
	listed, listErr := os.Lstat(path)
	opened, statErr := file.Stat()
	body, readErr := io.ReadAll(io.LimitReader(file, maxReconciliationEffectBytes+1))
	closeErr := file.Close()
	if listErr != nil || statErr != nil || readErr != nil || closeErr != nil || !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || len(body) > maxReconciliationEffectBytes {
		return nil, errors.New("handoff phase file is unsafe")
	}
	return body, nil
}

func handoffCandidateBinding(ctx context.Context, request handoffRequest) (agentruntime.ImplementationLaunchBinding, agentruntime.ImplementationPane, error) {
	manifest := handoffCandidateManifest(request)
	binding, err := agentruntime.ReadImplementationBinding(manifest)
	if err != nil {
		return binding, agentruntime.ImplementationPane{}, err
	}
	observed, err := runHostTmux(ctx, []string{"display-message", "-p", "-t", binding.PaneID, agentruntime.ImplementationPaneFormat}, nil)
	if err != nil {
		return binding, agentruntime.ImplementationPane{}, err
	}
	pane, err := agentruntime.ParseImplementationPane(observed.Output)
	if err != nil || !binding.Matches(manifest, pane) || binding.EffectID != request.CandidateLaunchID {
		return binding, pane, errors.New("handoff candidate pane identity changed")
	}
	return binding, pane, nil
}

func guardedHandoffCandidateArgs(binding agentruntime.ImplementationLaunchBinding, pane agentruntime.ImplementationPane, command string) ([]string, error) {
	condition, err := agentruntime.ImplementationGuardCondition(binding, pane)
	if err != nil || command == "" {
		return nil, errors.New("handoff candidate guard is invalid")
	}
	condition = "#{&&:" + condition + ",#{==:#{@agent-symphony-handoff-invalidated},}}"
	return []string{"if-shell", "-F", "-t", pane.PaneID, condition, command, "display-message -p " + agentruntime.ImplementationGuardMismatch}, nil
}

func prepareHandoffV2(ctx context.Context, input []byte, root string) (string, error) {
	request, handoff, err := decodeHandoffRequest(input, root)
	if err != nil || request.Manifest.Version != agentruntime.ManifestVersion2 {
		return "", errors.New("bound handoff request is invalid")
	}
	if err := os.MkdirAll(filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs"), 0o700); err != nil {
		return "", err
	}
	bindingBytes, recipient := handoffBinding(request)
	inbox := filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs")
	if err := writeImmutable(filepath.Join(inbox, handoff.Key+".json"), bindingBytes); err != nil {
		return "", err
	}
	helper, err := hostExecutable()
	if err != nil {
		return "", err
	}
	buffer := "as-handoff-" + recipient[:16]
	prompt := fmt.Appendf(nil, "Apply this authorized Agent Symphony handoff in the current worktree. It may contain review feedback or confirmed human instructions. %s Current source refs are available under refs/remotes/agent-symphony/. Do not push; Agent Symphony will publish the captured result.\n\n%s\n\nCompletion contract: Make stdout exactly one JSON line of at most 64 KiB with nonempty validation and documentation evidence; progress and diagnostics belong on stderr. Do not wrap it in Markdown fences or emit another stdout object.\n{\"type\":\"agent-symphony-result-v1\",\"validation\":\"tests run and results\",\"documentation\":\"documentation impact or none\"}", humanInstructionPrecedence, request.Handoff)
	if _, err := runHostTmux(ctx, []string{"load-buffer", "-b", buffer, "-"}, bytes.NewReader(prompt)); err != nil {
		return "", err
	}
	candidateManifest := handoffCandidateManifest(request)
	if binding, _, candidateErr := handoffCandidateBinding(ctx, request); candidateErr == nil {
		terminal, terminalErr := agentruntime.ReadTerminalBrokerBinding(agentruntime.TerminalBrokerPath(candidateManifest))
		if terminalErr != nil || !agentruntime.ValidImplementationTerminalBinding(candidateManifest, terminal) || terminal.OuterPID != binding.PanePID {
			return "", errors.Join(terminalErr, errors.New("prepared handoff broker identity is unavailable"))
		}
		prepared, _ := json.Marshal(handoffPreparedTerminal{binding, terminal})
		return string(prepared), nil
	} else if !errors.Is(candidateErr, os.ErrNotExist) {
		return "", candidateErr
	}
	oldBinding, oldTerminal := *request.CurrentImplementation, *request.CurrentTerminal
	stateRoot := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(request.Manifest.LogPath))))
	runtime := &agentruntime.Runtime{Root: root, StateRoot: stateRoot, Tmux: "tmux", Helper: helper}
	keeper := "as-handoff-keeper-" + request.CandidateLaunchID
	if _, err := runHostTmux(ctx, []string{"new-session", "-d", "-s", keeper, "--", "/bin/sh", "-c", "sleep 30"}, nil); err != nil {
		return "", err
	}
	defer func() {
		_, _ = runHostTmux(context.WithoutCancel(ctx), []string{"kill-session", "-t", "=" + keeper}, nil)
	}()
	if err := runtime.StopCertifiedImplementation(ctx, request.Manifest, oldBinding, oldTerminal); err != nil {
		return "", fmt.Errorf("stop current handoff worker: %w", err)
	}
	launchedPath := filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", handoff.Key+".launched")
	signal := buffer + "-launched"
	worker := agentruntime.BoundHandoffPromptCommand(helper, "tmux", buffer, agentruntime.ResultPath(request.Manifest.Worktree), launchedPath, recipient, signal, candidateManifest, request.Command)
	binding, terminal, err := runtime.PrepareBoundTerminalBroker(ctx, candidateManifest, os.Environ(), worker)
	if err != nil {
		return "", err
	}
	prepared, _ := json.Marshal(handoffPreparedTerminal{binding, terminal})
	return string(prepared), nil
}

func releaseHandoffV2(ctx context.Context, input []byte, root string) (string, error) {
	request, handoff, err := decodeHandoffRequest(input, root)
	if err != nil || request.Manifest.Version != agentruntime.ManifestVersion2 {
		return "", errors.New("bound handoff request is invalid")
	}
	binding, pane, err := handoffCandidateBinding(ctx, request)
	if err != nil {
		return "", err
	}
	manifest := handoffCandidateManifest(request)
	terminal, err := agentruntime.ReadTerminalBrokerBinding(agentruntime.TerminalBrokerPath(manifest))
	if err != nil || request.PreparedImplementation == nil || request.PreparedTerminal == nil || binding != *request.PreparedImplementation || terminal != *request.PreparedTerminal || terminal.OuterPID != pane.PanePID {
		return "", errors.Join(err, errors.New("owner-certified handoff broker identity changed"))
	}
	if err := agentruntime.ReleaseTerminalBroker(ctx, terminal); err != nil {
		return "", err
	}
	_, recipient := handoffBinding(request)
	launchedPath := filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", handoff.Key+".launched")
	launched, err := immutableMarkerMatches(launchedPath, []byte(recipient))
	if err != nil {
		return "", err
	}
	if !launched {
		if _, err := runHostTmux(ctx, []string{"wait-for", "as-handoff-" + recipient[:16] + "-launched"}, nil); err != nil {
			return "", err
		}
		launched, err = immutableMarkerMatches(launchedPath, []byte(recipient))
		if err != nil || !launched {
			return "", errors.New("handoff worker did not produce startup output")
		}
	}
	binding, pane, err = handoffCandidateBinding(ctx, request)
	if err != nil {
		return "", err
	}
	option := "@agent-symphony-handoff-" + recipient[:16]
	set, _ := agentruntime.TmuxCommandString([]string{"set-option", "-p", "-t", pane.PaneID, option, recipient})
	args, err := guardedHandoffCandidateArgs(binding, pane, set)
	if err != nil {
		return "", err
	}
	result, err := runHostTmux(ctx, args, nil)
	if err != nil || strings.Contains(result.Output, agentruntime.ImplementationGuardMismatch) {
		return "", errors.Join(err, errors.New("handoff candidate guard rejected receipt"))
	}
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", handoff.Key, request.OutcomePath, request.OutcomeToken})
	if err := writeImmutable(request.OutcomePath, ack); err != nil {
		return "", err
	}
	return string(ack), nil
}

type handoffPreparedTerminal struct {
	Implementation agentruntime.ImplementationLaunchBinding `json:"implementation"`
	Terminal       agentruntime.TerminalBrokerBinding       `json:"terminal"`
}
