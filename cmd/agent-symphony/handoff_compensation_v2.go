package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type handoffCompensationRequest struct {
	Candidate   handoffCandidateInvalidation `json:"candidate"`
	PreserveOld bool                         `json:"preserve_old"`
}

type handoffCompensationProof struct {
	EffectID     string `json:"effect_id"`
	Token        string `json:"token"`
	OldSessionID string `json:"old_session_id"`
	OldPaneID    string `json:"old_pane_id"`
	PreserveOld  bool   `json:"preserve_old"`
	Disposition  string `json:"disposition"`
}

func handoffCompensationProofPath(candidate handoffCandidateInvalidation) string {
	return filepath.Join(candidate.Manifest.Worktree, ".agent-symphony", "handoffs", candidate.Key+"-"+candidate.EffectID+".compensated")
}

func validHandoffCompensationProof(proof handoffCompensationProof, request handoffCompensationRequest, old agentruntime.ImplementationLaunchBinding) bool {
	return proof.EffectID == request.Candidate.EffectID && proof.Token == request.Candidate.Token && proof.OldSessionID == old.SessionID && proof.OldPaneID == old.PaneID && proof.PreserveOld == request.PreserveOld && (proof.Disposition == "absent" || proof.Disposition == "marked-old" && request.PreserveOld || proof.Disposition == "killed")
}

func writeHandoffCompensationProof(request handoffCompensationRequest, old agentruntime.ImplementationLaunchBinding, disposition string) (string, error) {
	proof := handoffCompensationProof{request.Candidate.EffectID, request.Candidate.Token, old.SessionID, old.PaneID, request.PreserveOld, disposition}
	if !validHandoffCompensationProof(proof, request, old) {
		return "", errors.New("handoff compensation proof is invalid")
	}
	body, _ := json.Marshal(proof)
	path := handoffCompensationProofPath(request.Candidate)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", err
	}
	if err := writeImmutable(path, body); err != nil {
		return "", err
	}
	return string(body), nil
}

func compensateHandoffV2(ctx context.Context, input []byte, root string) (string, error) {
	var request handoffCompensationRequest
	if err := json.Unmarshal(input, &request); err != nil || !validHandoffCandidateInvalidation(request.Candidate, request.Candidate.Manifest) || !belowRoot(request.Candidate.Manifest.Worktree, root) {
		return "", errors.New("handoff compensation identity is invalid")
	}
	candidate := request.Candidate
	old, oldErr := agentruntime.ReadImplementationBinding(candidate.Manifest)
	if oldErr != nil {
		return "", fmt.Errorf("handoff old binding is unavailable: %w", oldErr)
	}
	if prior, err := readHandoffPhase(handoffCompensationProofPath(candidate)); err == nil {
		var proof handoffCompensationProof
		if json.Unmarshal(prior, &proof) != nil || !validHandoffCompensationProof(proof, request, old) {
			return "", errors.New("handoff compensation proof conflicts with candidate")
		}
		return string(prior), nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	status, err := runHostTmux(ctx, []string{"has-session", "-t", old.SessionID}, nil)
	if err != nil {
		if exactTmuxSessionAbsent(status, old.SessionID) {
			// Name absence is not process absence: a pane can outlive a
			// renamed/unlinked session, and a replaced socket can hide S1.
			absent, paneErr := hostBoundImplementationPaneAbsent(ctx, old)
			if paneErr == nil && absent {
				return writeHandoffCompensationProof(request, old, "absent")
			}
			return "", errors.Join(paneErr, errors.New("bound handoff pane may still exist"))
		}
		return "", err
	}
	observed, err := runHostTmux(ctx, []string{"display-message", "-p", "-t", old.PaneID, agentruntime.ImplementationPaneFormat}, nil)
	if err != nil {
		return "", err
	}
	pane, err := agentruntime.ParseImplementationPane(observed.Output)
	if err != nil || pane.ServerPID != old.ServerPID || pane.ServerStart != old.ServerStart || pane.SessionID != old.SessionID || pane.PaneID != old.PaneID || pane.SessionName != old.SessionName {
		return "", errors.New("handoff pane replacement is ambiguous")
	}
	guard := old
	oldWorker := old.Matches(candidate.Manifest, pane)
	if !oldWorker {
		if pane.Token != candidate.Token {
			return "", errors.New("handoff pane has foreign launch token")
		}
		candidateManifest := candidate.Manifest
		candidateManifest.LaunchToken, candidateManifest.LaunchID = candidate.Token, candidate.EffectID
		bound, bindErr := agentruntime.ReadImplementationBinding(candidateManifest)
		if bindErr == nil {
			if !bound.Matches(candidateManifest, pane) {
				return "", errors.New("handoff candidate binding changed")
			}
			guard = bound
		} else if !errors.Is(bindErr, os.ErrNotExist) {
			return "", bindErr
		} else {
			phasePath := filepath.Join(candidate.Manifest.Worktree, ".agent-symphony", "handoffs", candidate.Key+"-"+candidate.EffectID+".phase")
			body, readErr := readHandoffPhase(phasePath)
			var phase handoffLaunchPhase
			if readErr != nil || json.Unmarshal(body, &phase) != nil || phase.Old != old || phase.Candidate != candidate.Token || phase.EffectID != candidate.EffectID {
				return "", errors.New("handoff phase evidence is unavailable")
			}
			if pane.PanePID == old.PanePID && pane.Command == old.Command {
				// Retag committed but respawn did not: preserve the old worker on Dismiss.
				guard.Token = candidate.Token
			} else if pane.PanePID != old.PanePID && strings.Contains(pane.Command, candidate.EffectID) && strings.Contains(pane.Command, "implementation-gate") && len(phase.Gate) >= 9 && reflect.DeepEqual(phase.Gate[1:9], []string{"implementation-gate", "tmux", candidate.Manifest.LogPath, candidate.Manifest.Worktree, candidate.Manifest.Session, candidate.Token, candidate.EffectID, "--"}) {
				// The immutable pre-respawn phase plus exact token/server/pane/gate
				// identifies a parked candidate even if binding was interrupted.
				guard.Token, guard.PanePID, guard.Command = candidate.Token, pane.PanePID, pane.Command
			} else {
				return "", errors.New("unbound handoff candidate is ambiguous")
			}
		}
	}
	mark, _ := agentruntime.TmuxCommandString([]string{"set-option", "-p", "-t", pane.PaneID, "@agent-symphony-handoff-invalidated", candidate.EffectID})
	command := mark
	killCandidate := !request.PreserveOld || !oldWorker && pane.PanePID != old.PanePID
	if killCandidate {
		kill, _ := agentruntime.TmuxCommandString([]string{"kill-pane", "-t", pane.PaneID})
		command += " ; " + kill
	}
	args, err := agentruntime.GuardedImplementationArgs(guard, pane, command)
	if err != nil {
		return "", err
	}
	result, err := runHostTmux(ctx, args, nil)
	if err != nil || strings.Contains(result.Output, agentruntime.ImplementationGuardMismatch) {
		return "", errors.Join(err, errors.New("handoff compensation guard rejected pane"))
	}
	if killCandidate {
		// Confirm exact pane absence on the original server, or prove the
		// original server PID is gone if its last pane ended the server.
		absent, goneErr := hostBoundImplementationPaneAbsent(ctx, old)
		if goneErr != nil {
			return "", goneErr
		}
		if !absent {
			return "", errors.New("handoff candidate pane remained after guarded kill")
		}
		workerManifest := candidate.Manifest
		if guard.EffectID == candidate.EffectID {
			workerManifest.LaunchToken, workerManifest.LaunchID = candidate.Token, candidate.EffectID
		}
		if guard.EffectID == workerManifest.LaunchID && guard.Token == workerManifest.LaunchToken {
			gone, groupErr := agentruntime.ImplementationWorkerGone(workerManifest, guard)
			if groupErr != nil || !gone {
				return "", errors.Join(groupErr, errors.New("handoff worker group termination is unconfirmed"))
			}
		} else if !strings.Contains(guard.Command, "implementation-gate") {
			return "", errors.New("handoff worker release identity is unavailable")
		}
		return writeHandoffCompensationProof(request, old, "killed")
	}
	read, _ := agentruntime.TmuxCommandString([]string{"show-options", "-pqv", "-t", pane.PaneID, "@agent-symphony-handoff-invalidated"})
	args, err = agentruntime.GuardedImplementationArgs(guard, pane, read)
	if err != nil {
		return "", err
	}
	marked, err := runHostTmux(ctx, args, nil)
	if err != nil || strings.TrimSpace(marked.Output) != candidate.EffectID {
		return "", errors.Join(err, errors.New("handoff old pane invalidation is unconfirmed"))
	}
	return writeHandoffCompensationProof(request, old, "marked-old")
}
