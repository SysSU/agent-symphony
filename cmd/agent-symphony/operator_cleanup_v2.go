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
	implementation boundaryCaller
	reviewer       boundaryCaller
	runtime        *agentruntime.Runtime
}

func (e operatorCleanupExecutor) bindPolicy(request agentruntime.EffectRequest) (agentruntime.EffectRequest, error) {
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
	manifestSeen, logSeen, err := compatibilityResources(request.Manifest)
	if err != nil || manifestSeen != request.Cleanup.CompatibilityManifestSeen || logSeen != request.Cleanup.CompatibilityLogSeen {
		if err == nil {
			err = errors.New("compatibility resources changed before cleanup admission")
		}
		return err
	}
	if err := cleanupAttemptReviewResources(ctx, e.stateRoot, e.reviewer, request.Manifest, false); err != nil {
		return err
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
	if err := cleanupAttemptReviewResources(ctx, e.stateRoot, e.reviewer, request.Manifest, true); err != nil {
		return err
	}
	operation, body, err := cleanupBoundaryInput(request, false)
	if err != nil {
		return err
	}
	if _, err := e.implementation.call(ctx, operation, agentruntime.Command{Stdin: bytes.NewReader(body)}); err != nil {
		return err
	}
	if request.Cleanup.Action == "abandon" || request.Cleanup.Action == "remove" {
		return e.runtime.ForgetCompatibility(request.Manifest)
	}
	return nil
}

func (e operatorCleanupExecutor) verify(ctx context.Context, request agentruntime.EffectRequest) (bool, error) {
	if e.reviewer == nil || e.runtime == nil {
		return false, errors.New("operator cleanup verifier is invalid")
	}
	if err := e.runtime.VerifyResourcesGone(ctx, request.Manifest); err != nil {
		if errors.Is(err, agentruntime.ErrRuntimeResourcesRemain) {
			return false, nil
		}
		return false, err
	}
	attempt := agentruntime.Attempt{Repository: request.Manifest.Repository, Issue: request.Manifest.Issue, Number: request.Manifest.Attempt, BaseSHA: request.Manifest.BaseSHA}
	snapshotRoot := productionSnapshotRoot(e.stateRoot)
	expectedSnapshot, expectedSession := reviewIdentity(attempt, snapshotRoot)
	result, err := e.reviewer.call(ctx, "run", agentruntime.Command{Name: "tmux", Args: []string{"has-session", "-t", "=" + expectedSession}, Dir: e.stateRoot})
	if err == nil {
		return false, nil
	}
	if !result.Exited || result.Code != 1 {
		return false, err
	}
	if _, err := os.Lstat(expectedSnapshot); err == nil {
		return false, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	entries, err := os.ReadDir(snapshotRoot)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	prefix := filepath.Base(expectedSnapshot) + ".result-"
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) {
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
