package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

func TestHandoffOutcomeFinishPersistsBeforeFileRemovalAndRecovers(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "handoff-recovery-outcome")
	var failFinish atomic.Bool
	root, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, test.request, func(state runtimeOwnerState) error {
		if failFinish.Load() {
			for _, effect := range state.Effects {
				if effect.State == "completed" && effect.Reconciliation != nil && effect.Reconciliation.Handoff != nil && effect.Reconciliation.Handoff.Outcome != nil {
					return errors.New("persistence failed")
				}
			}
		}
		return nil
	})
	path := writeTestHandoffOutcome(t, root, snapshot, test.request)
	plans, paths, err := collectHandoffOutcomePlans(snapshot, root)
	if err != nil || len(plans) != 1 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	failFinish.Store(true)
	if _, err := coordinator.executeHandoffOutcome(t.Context(), plan, paths[plan.Request.Handoff.Key]); err == nil {
		t.Fatal("handoff outcome survived finish persistence failure")
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("outcome file removed before durable finish: %v", err)
	}
	current, _ := owner.snapshot(t.Context())
	effect := current.State.Effects[plan.Identity.EffectID]
	if effect.State != "pending" {
		t.Fatalf("failed finish committed: %#v", effect)
	}
	failFinish.Store(false)
	if _, err := coordinator.verifyPendingReconciliation(t.Context(), effect); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("recovered durable finish did not remove outcome: %v", err)
	}
}

func TestCompletedHandoffOutcomeCrashCleanupUsesExactProof(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "handoff-recovery-outcome")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	path := writeTestHandoffOutcome(t, root, snapshot, test.request)
	plans, paths, err := collectHandoffOutcomePlans(snapshot, root)
	if err != nil || len(plans) != 1 || paths[plans[0].Request.Handoff.Key] != path {
		t.Fatalf("plans=%#v paths=%#v err=%v", plans, paths, err)
	}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	result := reconciliationEffectResult{Action: reconciliationHandoffDeliver, Handoff: &handoffEffectResult{Kind: plan.Request.Handoff.Kind, Key: plan.Request.Handoff.Key, OutcomePath: plan.Request.Handoff.OutcomePath, OutcomeToken: plan.Request.Handoff.OutcomeToken, Observed: true}}
	if err := writeReconciliationEffectMarker(root, plan.Identity, plan.Request, result); err != nil {
		t.Fatal(err)
	}
	finished, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: plan.Identity, Result: result})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("test did not preserve post-finish crash file: %v", err)
	}
	if _, err := coordinator.verifyPendingReconciliation(t.Context(), finished.State.Effects[plan.Identity.EffectID]); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("completed marker recovery did not clean exact file: %v", err)
	}
}

func TestHandoffOutcomeCollectionRejectsAmbiguousAndUnsafeFiles(t *testing.T) {
	root := resolvedTempDir(t)
	directory := filepath.Join(root, "handoff-outcomes")
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	test := reconciliationEffectCaseNamed(t, "handoff-recovery-outcome")
	record := handoffOutcomeFile{Handoff: *test.request.Handoff.Recovery, Outcome: *test.request.Handoff.Outcome}
	for _, key := range []string{"one", "two"} {
		copy := record
		copy.Handoff.Key, copy.Outcome.Key = key, key
		copy.OutcomeToken = digestText("handoff-outcome\x00" + key)
		writeOutcomeRecord(t, filepath.Join(directory, key+".json"), copy)
	}
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	if _, _, err := collectHandoffOutcomePlans(stateOwnerSnapshot{State: state}, root); err == nil {
		t.Fatal("multiple outcome files for one attempt were accepted")
	}
	if err := os.Remove(filepath.Join(directory, "two.json")); err != nil {
		t.Fatal(err)
	}
	one := filepath.Join(directory, "one.json")
	if err := os.Chmod(one, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readHandoffOutcomeFile(one); err == nil {
		t.Fatal("public outcome file was accepted")
	}
	if err := os.Remove(one); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(directory, "target")
	writeOutcomeRecord(t, target, record)
	if err := os.Symlink(target, one); err != nil {
		t.Fatal(err)
	}
	if _, err := readHandoffOutcomeFile(one); err == nil {
		t.Fatal("symlink outcome file was accepted")
	}
}

func writeTestHandoffOutcome(t *testing.T, root string, snapshot stateOwnerSnapshot, request reconciliationEffectRequest) string {
	t.Helper()
	directory := filepath.Join(root, "handoff-outcomes")
	if err := os.Mkdir(directory, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
		t.Fatal(err)
	}
	recovery := snapshot.State.Recoveries[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].State
	handoff, _, err := internalgithub.ClaimHandoffState(&recovery)
	if err != nil {
		t.Fatal(err)
	}
	outcome := *request.Handoff.Outcome
	outcome.Key = handoff.Key
	record := handoffOutcomeFile{Handoff: handoff, Outcome: outcome, OutcomeToken: digestText("handoff-outcome\x00" + handoff.Key)}
	path := filepath.Join(directory, handoff.Key+".json")
	writeOutcomeRecord(t, path, record)
	return path
}

func writeOutcomeRecord(t *testing.T, path string, record handoffOutcomeFile) {
	t.Helper()
	body, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
}
