package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"syscall"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

type handoffOutcomeFile struct {
	Handoff      internalgithub.RecoveryHandoff `json:"handoff"`
	Outcome      internalgithub.HandoffOutcome  `json:"outcome"`
	OutcomeToken string                         `json:"outcome_token"`
}

func collectHandoffOutcomePlans(snapshot stateOwnerSnapshot, stateRoot string) ([]reconciliationPlannedEffect, map[string]string, error) {
	root := filepath.Join(stateRoot, "handoff-outcomes")
	info, statErr := os.Lstat(root)
	if errors.Is(statErr, os.ErrNotExist) {
		return nil, map[string]string{}, nil
	}
	if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return nil, nil, errors.New("handoff outcome directory is unsafe")
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) > 10000 {
		return nil, nil, errors.New("handoff outcome directory is unavailable or exceeds bounds")
	}
	var plans []reconciliationPlannedEffect
	paths := map[string]string{}
	seenAttempts := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(root, entry.Name())
		record, err := readHandoffOutcomeFile(path)
		if err != nil {
			return nil, nil, err
		}
		if entry.Name() != record.Handoff.Key+".json" || record.Outcome.Key != record.Handoff.Key || record.OutcomeToken != digestText("handoff-outcome\x00"+record.Handoff.Key) {
			return nil, nil, errors.New("handoff outcome identity mismatch")
		}
		key := ownerAttemptKey(record.Handoff.Repository, record.Handoff.Issue, record.Handoff.Attempt)
		if seenAttempts[key] {
			return nil, nil, errors.New("multiple handoff outcomes target one attempt")
		}
		seenAttempts[key] = true
		recovery, ok := snapshot.State.Recoveries[key]
		attempt, owned := snapshot.State.Attempts[key]
		observation := snapshot.State.Observations[ownerIssueKey(record.Handoff.Repository, record.Handoff.Issue)]
		if !ok || !owned || !observation.Present || !recovery.State.HandoffReceipts[record.Handoff.Key] {
			continue
		}
		request := reconciliationEffectRequest{Action: reconciliationHandoffDeliver, Repository: record.Handoff.Repository, Issue: record.Handoff.Issue, Attempt: record.Handoff.Attempt, Manifest: ptrManifest(attempt.Manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, Handoff: &handoffEffectRequest{Kind: "recovery", Key: record.Handoff.Key, Recovery: &record.Handoff, Outcome: &record.Outcome, OutcomePath: handoffReceiptPath(attempt.Manifest.Worktree, record.Handoff.Key), OutcomeToken: record.OutcomeToken}}
		request.ExecutionDigest = handoffOutcomeExecutionDigest(request, path)
		plans = append(plans, reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request})
		paths[record.Handoff.Key] = path
	}
	slices.SortFunc(plans, func(a, b reconciliationPlannedEffect) int {
		return strings.Compare(a.Request.Handoff.Key, b.Request.Handoff.Key)
	})
	return plans, paths, nil
}

func handoffOutcomeExecutionDigest(request reconciliationEffectRequest, path string) string {
	request.ExecutionDigest = ""
	body, _ := json.Marshal(struct {
		Request reconciliationEffectRequest
		Path    string
	}{request, path})
	return digestText(string(body))
}

func (c *runtimeEffectCoordinator) executeHandoffOutcome(_ context.Context, plan reconciliationPlannedEffect, path string) (reconciliationEffectResult, error) {
	request := plan.Request
	if request.Action != reconciliationHandoffDeliver || request.Handoff == nil || request.Handoff.Outcome == nil || handoffOutcomeExecutionDigest(request, path) != request.ExecutionDigest {
		return reconciliationEffectResult{}, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	record, err := readHandoffOutcomeFile(path)
	if err != nil || !reflect.DeepEqual(record.Handoff, *request.Handoff.Recovery) || !reflect.DeepEqual(record.Outcome, *request.Handoff.Outcome) || record.OutcomeToken != request.Handoff.OutcomeToken {
		if err == nil {
			err = errStaleStateResult
		}
		return reconciliationEffectResult{}, err
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	result := reconciliationEffectResult{Action: request.Action, Handoff: &handoffEffectResult{Kind: request.Handoff.Kind, Key: request.Handoff.Key, OutcomePath: request.Handoff.OutcomePath, OutcomeToken: request.Handoff.OutcomeToken, Observed: true}}
	if err := writeReconciliationEffectMarker(c.owner.stateRoot, plan.Identity, request, result); err != nil {
		return reconciliationEffectResult{}, err
	}
	if _, err := c.owner.finishReconciliationEffect(c.lifecycle, finishReconciliationEffectCommand{Identity: plan.Identity, Result: result}); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, cleanupCompletedHandoffOutcome(c.owner.stateRoot, request)
}

func cleanupCompletedHandoffOutcome(stateRoot string, request reconciliationEffectRequest) error {
	if request.Action != reconciliationHandoffDeliver || request.Handoff == nil || request.Handoff.Outcome == nil {
		return nil
	}
	path := filepath.Join(stateRoot, "handoff-outcomes", request.Handoff.Key+".json")
	record, err := readHandoffOutcomeFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil || !reflect.DeepEqual(record.Handoff, *request.Handoff.Recovery) || !reflect.DeepEqual(record.Outcome, *request.Handoff.Outcome) || record.OutcomeToken != request.Handoff.OutcomeToken {
		if err == nil {
			err = errors.New("handoff outcome changed before cleanup")
		}
		return err
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	directory, err := os.Open(filepath.Dir(path))
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func readHandoffOutcomeFile(path string) (handoffOutcomeFile, error) {
	listed, err := os.Lstat(path)
	if err != nil {
		return handoffOutcomeFile{}, err
	}
	file, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NOFOLLOW, 0)
	if err != nil {
		return handoffOutcomeFile{}, errors.New("unsafe handoff outcome file")
	}
	defer file.Close()
	opened, err := file.Stat()
	if err != nil || !os.SameFile(listed, opened) || !opened.Mode().IsRegular() || opened.Mode().Perm() != 0o600 || !ownedByCurrentUser(opened) || opened.Size() < 2 || opened.Size() > 1<<20 {
		return handoffOutcomeFile{}, errors.New("unsafe handoff outcome file")
	}
	body, err := io.ReadAll(io.LimitReader(file, 1<<20+1))
	if err != nil || len(body) > 1<<20 {
		return handoffOutcomeFile{}, errors.New("oversized handoff outcome file")
	}
	var record handoffOutcomeFile
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&record) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return handoffOutcomeFile{}, errors.New("invalid handoff outcome file")
	}
	return record, nil
}
