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
	"reflect"
	"strings"
	"syscall"
	"time"

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

func handoffPhasePath(request handoffRequest, key string) string {
	return filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", key+"-"+request.CandidateLaunchID+".phase")
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

func handoffGateCommand(request handoffRequest, key, recipient, helper string) []string {
	buffer := "as-handoff-" + recipient[:16]
	launchedPath := filepath.Join(request.Manifest.Worktree, ".agent-symphony", "handoffs", key+".launched")
	signal := buffer + "-launched"
	bound := handoffCandidateManifest(request)
	worker := agentruntime.BoundHandoffPromptCommand(helper, "tmux", buffer, agentruntime.ResultPath(request.Manifest.Worktree), launchedPath, recipient, signal, bound, request.Command)
	return append([]string{helper, "implementation-gate", "tmux", bound.LogPath, bound.Worktree, bound.Session, bound.LaunchToken, bound.LaunchID, "--"}, worker...)
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
	prepared := request.CandidateLaunchID + ":" + request.CandidateLaunchToken
	if _, _, err := handoffCandidateBinding(ctx, request); err == nil {
		return prepared, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	oldBinding, err := agentruntime.ReadImplementationBinding(request.Manifest)
	if err != nil {
		return "", fmt.Errorf("old handoff launch is unbound: %w", err)
	}
	observed, err := runHostTmux(ctx, []string{"display-message", "-p", "-t", oldBinding.PaneID, agentruntime.ImplementationPaneFormat}, nil)
	if err != nil {
		return "", err
	}
	oldPane, err := agentruntime.ParseImplementationPane(observed.Output)
	if err != nil {
		return "", err
	}
	phasePath := handoffPhasePath(request, handoff.Key)
	phaseBody, phaseErr := readHandoffPhase(phasePath)
	phaseExists := phaseErr == nil
	if phaseErr != nil && !errors.Is(phaseErr, os.ErrNotExist) {
		return "", phaseErr
	}
	var prior handoffLaunchPhase
	if phaseExists {
		if json.Unmarshal(phaseBody, &prior) != nil || prior.Old != oldBinding || prior.Candidate != request.CandidateLaunchToken || prior.EffectID != request.CandidateLaunchID || len(prior.Gate) < 10 || !reflect.DeepEqual(prior.Gate, handoffGateCommand(request, handoff.Key, recipient, prior.Gate[0])) {
			return "", errors.New("handoff phase identity conflicts with owner intent")
		}
		if oldPane.Token == request.CandidateLaunchToken && oldPane.ServerPID == oldBinding.ServerPID && oldPane.ServerStart == oldBinding.ServerStart && oldPane.SessionID == oldBinding.SessionID && oldPane.PaneID == oldBinding.PaneID && oldPane.PanePID != oldBinding.PanePID && strings.Contains(oldPane.Command, request.CandidateLaunchID) && strings.Contains(oldPane.Command, "implementation-gate") {
			candidate, err := agentruntime.BindImplementationPane(handoffCandidateManifest(request), request.CandidateLaunchID, "capture", oldPane)
			if err != nil {
				return "", err
			}
			if err := agentruntime.WriteImplementationBinding(handoffCandidateManifest(request), candidate); err != nil {
				return "", err
			}
			return prepared, nil
		}
	}
	guardBinding := oldBinding
	if !oldBinding.Matches(request.Manifest, oldPane) {
		if !phaseExists || oldPane.Token != request.CandidateLaunchToken || oldPane.PanePID != oldBinding.PanePID || oldPane.Command != oldBinding.Command || oldPane.ServerPID != oldBinding.ServerPID || oldPane.ServerStart != oldBinding.ServerStart || oldPane.SessionID != oldBinding.SessionID || oldPane.PaneID != oldBinding.PaneID {
			return "", errors.New("old handoff pane identity changed")
		}
		guardBinding.Token = request.CandidateLaunchToken // Retag completed; respawn did not.
	}
	helper, err := hostExecutable()
	if err != nil {
		return "", err
	}
	gate := handoffGateCommand(request, handoff.Key, recipient, helper)
	if phaseExists {
		gate = prior.Gate
	}
	phase, _ := json.Marshal(handoffLaunchPhase{Old: oldBinding, Candidate: request.CandidateLaunchToken, EffectID: request.CandidateLaunchID, Gate: gate})
	buffer := "as-handoff-" + recipient[:16]
	prompt := fmt.Appendf(nil, "Apply this authorized Agent Symphony handoff in the current worktree. It may contain review feedback or confirmed human instructions. %s Current source refs are available under refs/remotes/agent-symphony/. Do not push; Agent Symphony will publish the captured result.\n\n%s\n\nCompletion contract: Make stdout exactly one JSON line of at most 64 KiB with nonempty validation and documentation evidence; progress and diagnostics belong on stderr. Do not wrap it in Markdown fences or emit another stdout object.\n{\"type\":\"agent-symphony-result-v1\",\"validation\":\"tests run and results\",\"documentation\":\"documentation impact or none\"}", humanInstructionPrecedence, request.Handoff)
	if _, err := runHostTmux(ctx, []string{"load-buffer", "-b", buffer, "-"}, bytes.NewReader(prompt)); err != nil {
		return "", err
	}
	if !phaseExists {
		if err := writeImmutable(phasePath, phase); err != nil {
			return "", err
		}
	} else {
		// No candidate gate exists in either old-pane phase. A crashed host
		// may have left this exact channel locked; retire it before reacquiring.
		unlocked, unlockErr := runHostTmux(ctx, []string{"wait-for", "-U", agentruntime.ImplementationGateChannel(request.CandidateLaunchID)}, nil)
		if unlockErr != nil && (!unlocked.Exited || unlocked.Code != 1 || strings.TrimSpace(unlocked.Output) != "channel "+agentruntime.ImplementationGateChannel(request.CandidateLaunchID)+" not locked") {
			return "", unlockErr
		}
	}
	// Acquire the gate before checking the old pane. A blocked lock must never
	// resume inside a previously authorized tmux if-shell branch.
	if _, err := runHostTmux(ctx, []string{"wait-for", "-L", agentruntime.ImplementationGateChannel(request.CandidateLaunchID)}, nil); err != nil {
		return "", err
	}
	condition, err := agentruntime.ImplementationGuardCondition(guardBinding, oldPane)
	if err != nil {
		return "", err
	}
	condition = "#{&&:" + condition + ",#{==:#{@agent-symphony-handoff-invalidated},}}"
	commands := [][]string{
		{"set-option", "-w", "-t", oldPane.PaneID, "remain-on-exit", "on"},
		{"set-option", "-p", "-t", oldPane.PaneID, "@agent-symphony-launch-token", request.CandidateLaunchToken},
		{"set-option", "-p", "-t", oldPane.PaneID, agentruntime.PaneExitStatusOption, ""},
		{"set-option", "-p", "-t", oldPane.PaneID, agentruntime.PaneExitSignalOption, ""},
		append([]string{"respawn-pane", "-k", "-t", oldPane.PaneID, "-c", request.Manifest.Worktree, "--"}, gate...),
	}
	parts := make([]string, 0, len(commands))
	for _, command := range commands {
		part, err := agentruntime.TmuxCommandString(command)
		if err != nil {
			return "", err
		}
		parts = append(parts, part)
	}
	queued, err := runHostTmux(ctx, []string{"if-shell", "-F", "-t", oldPane.PaneID, condition, strings.Join(parts, " ; "), "display-message -p " + agentruntime.ImplementationGuardMismatch}, nil)
	if strings.Contains(queued.Output, agentruntime.ImplementationGuardMismatch) {
		// The false branch proves no gate was created, so this exact lock can
		// be retired. On ambiguous command failure, retain it for recovery.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_, unlockErr := runHostTmux(cleanupCtx, []string{"wait-for", "-U", agentruntime.ImplementationGateChannel(request.CandidateLaunchID)}, nil)
		return "", errors.Join(err, unlockErr, errors.New("handoff pane guard rejected candidate transition"))
	}
	if err != nil {
		return "", errors.Join(err, errors.New("handoff pane guard rejected candidate transition"))
	}
	observed, err = runHostTmux(ctx, []string{"display-message", "-p", "-t", oldPane.PaneID, agentruntime.ImplementationPaneFormat}, nil)
	if err != nil {
		return "", err
	}
	pane, err := agentruntime.ParseImplementationPane(observed.Output)
	if err != nil || pane.ServerPID != oldBinding.ServerPID || pane.ServerStart != oldBinding.ServerStart || pane.SessionID != oldBinding.SessionID || pane.PaneID != oldBinding.PaneID || pane.PanePID == oldBinding.PanePID || pane.Token != request.CandidateLaunchToken || !strings.Contains(pane.Command, request.CandidateLaunchID) || !strings.Contains(pane.Command, "implementation-gate") {
		return "", errors.New("parked handoff gate identity is unavailable")
	}
	candidate, err := agentruntime.BindImplementationPane(handoffCandidateManifest(request), request.CandidateLaunchID, "capture", pane)
	if err != nil {
		return "", err
	}
	if err := agentruntime.WriteImplementationBinding(handoffCandidateManifest(request), candidate); err != nil {
		return "", err
	}
	return prepared, nil
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
	priorRelease := agentruntime.ImplementationReleaseMatches(manifest, binding)
	if err := agentruntime.WriteImplementationPermit(manifest, binding); err != nil {
		return "", err
	}
	if err := agentruntime.WriteImplementationRelease(manifest, binding); err != nil {
		return "", err
	}
	channel := agentruntime.ImplementationGateChannel(request.CandidateLaunchID)
	unlock, _ := agentruntime.TmuxCommandString([]string{"wait-for", "-U", channel})
	args, err := guardedHandoffCandidateArgs(binding, pane, unlock)
	if err != nil {
		return "", err
	}
	result, err := runHostTmux(ctx, args, nil)
	if err != nil && (!priorRelease || !result.Exited || result.Code != 1 || strings.TrimSpace(result.Output) != "channel "+channel+" not locked") {
		return "", err
	}
	if strings.Contains(result.Output, agentruntime.ImplementationGuardMismatch) {
		return "", errors.New("handoff candidate guard rejected release")
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
	args, err = guardedHandoffCandidateArgs(binding, pane, set)
	if err != nil {
		return "", err
	}
	result, err = runHostTmux(ctx, args, nil)
	if err != nil || strings.Contains(result.Output, agentruntime.ImplementationGuardMismatch) {
		return "", errors.Join(err, errors.New("handoff candidate guard rejected receipt"))
	}
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", handoff.Key, request.OutcomePath, request.OutcomeToken})
	if err := writeImmutable(request.OutcomePath, ack); err != nil {
		return "", err
	}
	return string(ack), nil
}
