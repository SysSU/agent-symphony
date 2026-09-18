package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// operatorCleanupExecutor performs the policy-bound external deletion after
// the owner has durably tombstoned the attempt.
type operatorCleanupExecutor struct {
	stateRoot      string
	owner          *stateOwner
	implementation boundaryCaller
	reviewer       boundaryCaller
	runtime        *agentruntime.Runtime
}

func (e operatorCleanupExecutor) bindPolicy(request agentruntime.EffectRequest) (agentruntime.EffectRequest, error) {
	if request.Cleanup.Action == "dismiss" {
		return request, nil
	}
	manifestSeen, logSeen, err := compatibilityResources(request.Manifest)
	if err != nil {
		return agentruntime.EffectRequest{}, err
	}
	request.Cleanup.CompatibilityManifestSeen = manifestSeen
	request.Cleanup.CompatibilityLogSeen = logSeen
	return request, nil
}

func (e operatorCleanupExecutor) validate(ctx context.Context, request agentruntime.EffectRequest) error {
	if e.stateRoot == "" || e.implementation == nil || e.runtime == nil || request.Action != agentruntime.EffectCleanup {
		return errors.New("operator cleanup executor is invalid")
	}
	if request.Cleanup.Action != "dismiss" {
		manifestSeen, logSeen, err := compatibilityResources(request.Manifest)
		if err != nil || manifestSeen != request.Cleanup.CompatibilityManifestSeen || logSeen != request.Cleanup.CompatibilityLogSeen {
			if err == nil {
				err = errors.New("compatibility resources changed before cleanup admission")
			}
			return err
		}
	}
	if err := cleanupAttemptReviewResources(ctx, e.stateRoot, e.reviewer, request.Manifest, false); err != nil {
		return err
	}
	if request.Cleanup.Action == "dismiss" {
		return nil
	}
	operation, body, err := cleanupBoundaryInput(request, true)
	if err != nil {
		return err
	}
	_, err = e.implementation.call(ctx, operation, agentruntime.Command{Stdin: bytes.NewReader(body)})
	return err
}

func (e operatorCleanupExecutor) execute(ctx context.Context, request agentruntime.EffectRequest) error {
	if e.reviewer == nil {
		return errors.New("review cleanup boundary is missing")
	}
	proofs := map[string]reviewerProcessProof{}
	if e.owner != nil {
		snapshot, err := e.owner.snapshot(ctx)
		if err != nil {
			return err
		}
		proofs, _, err = cleanupReviewerProofs(snapshot.State, request)
		if err != nil {
			return err
		}
		tombstone := snapshot.State.Tombstones[ownerAttemptKey(request.Identity.Repository, request.Identity.Issue, request.Identity.Attempt)]
		if tombstone.InvalidatedStart != nil && !agentruntime.WorkerConfinementBound(tombstone.InvalidatedStart.Manifest, tombstone.InvalidatedGeneration, e.runtime.WorkerProfileDigest) {
			return fmt.Errorf("start candidate cleanup remains unproved: %w", agentruntime.ErrRuntimeResourcesRemain)
		}
		if err := cleanupAttemptReviewResourcesBound(ctx, e.stateRoot, e.reviewer, request.Manifest, true, activeWorkerProfileDigest(snapshot.State), proofs); err != nil {
			return err
		}
	} else if err := cleanupAttemptReviewResourcesProved(ctx, e.stateRoot, e.reviewer, request.Manifest, true, proofs); err != nil {
		return err
	}
	if request.Cleanup.Action == "dismiss" {
		return nil
	}
	operation, body, err := cleanupBoundaryInput(request, false)
	if err != nil {
		return err
	}
	if _, err := e.implementation.call(ctx, operation, agentruntime.Command{Stdin: bytes.NewReader(body)}); err != nil {
		return err
	}
	if request.Cleanup.Action == "abandon" || request.Cleanup.Action == "remove" {
		return freshRuntime(e.runtime, e.runtime.Source).ForgetCompatibility(request.Manifest)
	}
	return nil
}

func (e operatorCleanupExecutor) verify(ctx context.Context, request agentruntime.EffectRequest) (bool, error) {
	if e.reviewer == nil || e.runtime == nil {
		return false, errors.New("operator cleanup verifier is invalid")
	}
	if e.owner != nil {
		snapshot, err := e.owner.snapshot(ctx)
		if err != nil {
			return false, err
		}
		proofs, _, proofErr := cleanupReviewerProofs(snapshot.State, request)
		if proofErr != nil {
			if errors.Is(proofErr, agentruntime.ErrRuntimeResourcesRemain) {
				return false, nil
			}
			return false, proofErr
		}
		tombstone := snapshot.State.Tombstones[ownerAttemptKey(request.Identity.Repository, request.Identity.Issue, request.Identity.Attempt)]
		if tombstone.InvalidatedStart != nil && !agentruntime.WorkerConfinementBound(tombstone.InvalidatedStart.Manifest, tombstone.InvalidatedGeneration, e.runtime.WorkerProfileDigest) {
			return false, nil
		}
		attempt := agentruntime.Attempt{Repository: request.Manifest.Repository, Issue: request.Manifest.Issue, Number: request.Manifest.Attempt, BaseSHA: request.Manifest.BaseSHA}
		snapshotRoot := productionSnapshotRoot(e.stateRoot)
		for target := range proofs {
			expectedSnapshot, expectedSession := persistedReviewIdentity(attempt, snapshotRoot, target, proofs[target].RunID)
			result, sessionErr := e.reviewer.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"has-session", "-t", "=" + expectedSession}, Dir: snapshotRoot})
			if sessionErr == nil || !exactTmuxSessionAbsent(result, expectedSession) {
				return false, nil
			}
			if _, statErr := os.Lstat(expectedSnapshot); statErr == nil {
				return false, nil
			} else if !errors.Is(statErr, os.ErrNotExist) {
				return false, statErr
			}
		}
	}
	attempt := agentruntime.Attempt{Repository: request.Manifest.Repository, Issue: request.Manifest.Issue, Number: request.Manifest.Attempt, BaseSHA: request.Manifest.BaseSHA}
	snapshotRoot := productionSnapshotRoot(e.stateRoot)
	if request.Manifest.ReviewTarget != "" {
		expectedSnapshot, expectedSession := persistedReviewIdentity(attempt, snapshotRoot, request.Manifest.ReviewTarget, request.Manifest.ReviewRunID)
		result, err := e.reviewer.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"has-session", "-t", "=" + expectedSession}, Dir: snapshotRoot})
		if err == nil {
			return false, nil
		}
		if !exactTmuxSessionAbsent(result, expectedSession) {
			return false, err
		}
		if _, err := os.Lstat(expectedSnapshot); err == nil {
			return false, nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	if request.Cleanup.Action == "dismiss" {
		entries, err := os.ReadDir(snapshotRoot)
		if err != nil && !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
		baseSnapshot, _ := reviewIdentity(attempt, snapshotRoot)
		for _, entry := range entries {
			name := entry.Name()
			if name == filepath.Base(baseSnapshot) || strings.HasPrefix(name, filepath.Base(baseSnapshot)+"-") || strings.HasPrefix(name, filepath.Base(baseSnapshot)+".result-") {
				return false, nil
			}
		}
		return true, nil
	}
	if err := freshRuntime(e.runtime, e.runtime.Source).VerifyResourcesGone(ctx, request.Manifest); err != nil {
		if errors.Is(err, agentruntime.ErrRuntimeResourcesRemain) {
			return false, nil
		}
		return false, err
	}
	entries, err := os.ReadDir(snapshotRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	baseSnapshot, _ := reviewIdentity(attempt, snapshotRoot)
	for _, entry := range entries {
		name := entry.Name()
		if name == filepath.Base(baseSnapshot) || strings.HasPrefix(name, filepath.Base(baseSnapshot)+"-") || strings.HasPrefix(name, filepath.Base(baseSnapshot)+".result-") {
			return false, nil
		}
	}
	manifestSeen, logSeen, err := compatibilityResources(request.Manifest)
	if err != nil {
		return false, err
	}
	if request.Cleanup.Action == "archive" {
		return manifestSeen == request.Cleanup.CompatibilityManifestSeen && logSeen == request.Cleanup.CompatibilityLogSeen, nil
	}
	directory := filepath.Dir(request.Manifest.LogPath)
	if _, err := os.Lstat(directory); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return !manifestSeen && !logSeen, nil
}

func cleanupReviewerProofs(state runtimeOwnerState, request agentruntime.EffectRequest) (map[string]reviewerProcessProof, runtimeTombstone, error) {
	effect, ok := state.Effects[request.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.Action != string(agentruntime.EffectCleanup) || effect.RequestDigest != request.Identity.RequestDigest || effect.IssueGeneration != request.Identity.IssueGeneration || effect.AttemptGeneration != request.Identity.AttemptGeneration || effect.Repository != request.Identity.Repository || effect.Issue != request.Identity.Issue || effect.Attempt != request.Identity.Attempt {
		return nil, runtimeTombstone{}, errStaleStateResult
	}
	tombstone, ok := state.Tombstones[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)]
	if !ok || tombstone.EffectID != effect.ID || tombstone.Generation != effect.AttemptGeneration || tombstone.InvalidatedGeneration+1 != tombstone.Generation || tombstone.InvalidatedHandoff != nil && !tombstone.HandoffCompensated || tombstone.InvalidatedStart != nil || state.LegacyReviewerQuarantines[ownerIssueKey(effect.Repository, effect.Issue)] != "" {
		return nil, runtimeTombstone{}, agentruntime.ErrRuntimeResourcesRemain
	}
	activeProfileDigest := activeWorkerProfileDigest(state)
	proofs := map[string]reviewerProcessProof{}
	for _, proof := range state.ReviewerProofs {
		if proof.Repository != effect.Repository || proof.Issue != effect.Issue || proof.Attempt != effect.Attempt {
			continue
		}
		if !proof.NeverRan && !validDigest(proof.RunID) || proof.AttemptGeneration == 0 || proof.AttemptGeneration > tombstone.InvalidatedGeneration || !reviewerCleanupAuthorized(proof, activeProfileDigest) {
			return nil, runtimeTombstone{}, agentruntime.ErrRuntimeResourcesRemain
		}
		if proof.Target == request.Manifest.ReviewTarget && request.Manifest.ReviewRunID != "" && proof.RunID != request.Manifest.ReviewRunID {
			return nil, runtimeTombstone{}, agentruntime.ErrRuntimeResourcesRemain
		}
		proofs[proof.Target] = proof
	}
	return proofs, tombstone, nil
}

func compatibilityResources(manifest agentruntime.Manifest) (bool, bool, error) {
	present := func(path string) (bool, error) {
		info, err := os.Lstat(path)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return false, fmt.Errorf("compatibility record is unsafe: %s", path)
		}
		return true, nil
	}
	manifestSeen, err := present(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"))
	if err != nil {
		return false, false, err
	}
	logSeen, err := present(manifest.LogPath)
	return manifestSeen, logSeen, err
}

func cleanupBoundaryInput(request agentruntime.EffectRequest, validate bool) (string, []byte, error) {
	var operation string
	var value any = request.Manifest
	switch request.Cleanup.Action {
	case "archive":
		operation = "cleanup"
		if validate {
			operation = "validate-cleanup"
		}
	case "abandon":
		operation = "abandon"
		if validate {
			operation = "validate-abandon"
		}
	case "remove":
		operation = "remove"
		if validate {
			operation = "validate-remove"
		}
		value = permanentRemovalRequest{Manifest: request.Manifest, PublishedHead: request.Cleanup.PublishedHead}
	default:
		return "", nil, errors.New("unknown cleanup policy")
	}
	body, err := json.Marshal(value)
	return operation, body, err
}
