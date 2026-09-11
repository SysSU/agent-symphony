package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestReconciliationMarkersRoundTripEveryClosedVariant(t *testing.T) {
	for _, test := range reconciliationEffectCases(t) {
		t.Run(test.name, func(t *testing.T) {
			root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
			request := bindEffectObservation(snapshot, test.request)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			identity, result := ownerReconciliationEffectIdentity(*effect), test.result(request)
			if err := writeReconciliationEffectMarker(root, identity, request, result); err != nil {
				t.Fatal(err)
			}
			loaded, err := readReconciliationEffectMarker(root, identity, request)
			if err != nil || loaded == nil || !reflect.DeepEqual(*loaded, result) {
				t.Fatalf("loaded=%#v want=%#v err=%v", loaded, result, err)
			}
		})
	}
}

func TestReconciliationMarkerDirectorySyncsParentAfterFirstLink(t *testing.T) {
	root := resolvedTempDir(t)
	previous := immutableDirSync
	t.Cleanup(func() { immutableDirSync = previous })
	called := false
	immutableDirSync = func(path string) error {
		called = true
		if path != root {
			t.Fatalf("synced %q want %q", path, root)
		}
		if info, err := os.Lstat(filepath.Join(root, "reconciliation-effects")); err != nil || !info.IsDir() {
			t.Fatalf("marker directory was not linked before parent sync: info=%v err=%v", info, err)
		}
		return errors.New("injected parent sync failure")
	}
	if _, err := reconciliationMarkerDirectory(root, true); err == nil || !strings.Contains(err.Error(), "injected parent sync failure") || !called {
		t.Fatalf("called=%v err=%v", called, err)
	}
}

func TestProductionMarkerDirectoriesSyncEachNewParentLink(t *testing.T) {
	root := resolvedTempDir(t)
	previous := immutableDirSync
	t.Cleanup(func() { immutableDirSync = previous })
	var calls int
	immutableDirSync = func(path string) error {
		calls++
		if path != root {
			t.Fatalf("synced %q want %q", path, root)
		}
		if info, err := os.Lstat(filepath.Join(root, "reconciliation-effects")); err != nil || !info.IsDir() {
			t.Fatalf("reconciliation marker directory missing before sync: info=%v err=%v", info, err)
		}
		if calls == 2 {
			if info, err := os.Lstat(filepath.Join(root, "runtime-effects")); err != nil || !info.IsDir() {
				t.Fatalf("runtime marker directory missing before sync: info=%v err=%v", info, err)
			}
		}
		return nil
	}
	if err := prepareProductionMarkerDirectories(root); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("parent sync calls=%d want 2", calls)
	}
	if err := prepareProductionMarkerDirectories(root); err != nil {
		t.Fatal(err)
	}
	if calls != 2 {
		t.Fatalf("existing directories were synced again: calls=%d", calls)
	}
}

func TestReconciliationMarkerIsImmutableAndRejectsUnsafeFile(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	request := bindEffectObservation(snapshot, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity, result := ownerReconciliationEffectIdentity(*effect), test.result(request)
	if err := writeReconciliationEffectMarker(root, identity, request, result); err != nil {
		t.Fatal(err)
	}
	conflict := cloneReconciliationResult(result)
	conflict.Reviewer.Status, conflict.Reviewer.Findings = "findings", []string{"changed"}
	if err := writeReconciliationEffectMarker(root, identity, request, conflict); err == nil {
		t.Fatal("immutable marker accepted a conflicting valid result")
	}
	path := filepath.Join(root, "reconciliation-effects", identity.EffectID+".done")
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := readReconciliationEffectMarker(root, identity, request); err == nil {
		t.Fatal("unsafe marker mode was accepted")
	}
}

func TestReconciliationMarkerFinishesAfterPersistenceFailureAndRestart(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "issue-control-snapshot")
	var persisted runtimeOwnerState
	failFinish := true
	root, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, test.request, func(state runtimeOwnerState) error {
		for _, effect := range state.Effects {
			if effect.Reconciliation != nil && effect.State == "completed" && failFinish {
				return errors.New("persistence failed")
			}
		}
		persisted = cloneRuntimeOwnerState(state)
		return nil
	})
	request := bindEffectObservation(snapshot, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity, result := ownerReconciliationEffectIdentity(*effect), test.result(request)
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	if err := coordinator.finishReconciliationWithMarker(identity, request, result); err == nil {
		t.Fatal("finish unexpectedly survived persistence failure")
	}
	current, _ := owner.snapshot(t.Context())
	if current.State.Effects[effect.ID].State != "pending" {
		t.Fatalf("failed finish committed in memory: %#v", current.State.Effects[effect.ID])
	}
	if err := owner.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	failFinish = false
	restarted, err := startTestStateOwner(t, root, persisted, func(state runtimeOwnerState) error {
		persisted = cloneRuntimeOwnerState(state)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	restartCoordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, active: map[string]*activeRuntimeEffect{}}
	recovered, err := restartCoordinator.verifyPendingReconciliation(t.Context(), persisted.Effects[effect.ID])
	if err != nil || recovered == nil || !reflect.DeepEqual(*recovered, result) {
		t.Fatalf("recovered=%#v err=%v", recovered, err)
	}
	finished, _ := restarted.snapshot(t.Context())
	if got := finished.State.Effects[effect.ID]; got.State != "completed" || got.Diagnostic != "" || !reflect.DeepEqual(got.ReconciliationResult, &result) {
		t.Fatalf("restart did not commit marked result: %#v", got)
	}
}

func TestReconciliationResultCancellationAfterPersistenceDispatchStillCommits(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "github-bind")
	persistEntered, persistRelease := make(chan struct{}), make(chan struct{})
	var once sync.Once
	root, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, test.request, func(state runtimeOwnerState) error {
		for _, effect := range state.Effects {
			if effect.State == "completed" && effect.Reconciliation != nil {
				once.Do(func() { close(persistEntered) })
				<-persistRelease
				break
			}
		}
		return nil
	})
	request := bindEffectObservation(snapshot, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, cancel := context.WithCancel(t.Context())
	coordinator := runtimeEffectCoordinator{lifecycle: lifecycle, owner: owner, active: map[string]*activeRuntimeEffect{}}
	done := make(chan error, 1)
	go func() {
		done <- coordinator.finishReconciliationWithMarker(ownerReconciliationEffectIdentity(*effect), request, test.result(request))
	}()
	<-persistEntered
	if _, err := os.Lstat(filepath.Join(root, "reconciliation-effects", effect.ID+".done")); err != nil {
		t.Fatalf("result marker was not durable before owner finish: %v", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("finish caller err=%v want cancellation", err)
	}
	close(persistRelease)
	for committed := range owner.commits {
		if committed.State.Effects[effect.ID].State == "completed" {
			break
		}
	}
	current := mustOwnerSnapshot(t, owner)
	if current.State.Effects[effect.ID].State != "completed" {
		t.Fatalf("linearized finish did not commit after cancellation: %#v", current.State.Effects[effect.ID])
	}
}

func TestReconciliationEffectDiagnosticIsDurableAndBounded(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "issue-control-snapshot")
	_, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	request := bindEffectObservation(snapshot, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	updated, err := owner.diagnoseReconciliationEffect(t.Context(), diagnoseReconciliationEffectCommand{Identity: identity, Action: request.Action, Diagnostic: "execution input is unavailable"})
	if err != nil || updated.State.Effects[effect.ID].Diagnostic == "" {
		t.Fatalf("diagnostic=%#v err=%v", updated.State.Effects[effect.ID], err)
	}
	if _, err := owner.diagnoseReconciliationEffect(t.Context(), diagnoseReconciliationEffectCommand{Identity: identity, Action: request.Action, Diagnostic: string(make([]byte, maxReconciliationStringBytes+1))}); err == nil {
		t.Fatal("oversized diagnostic was accepted")
	}
}

func TestReconciliationMarkerCannotFinishInvalidatedIntent(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "issue-control-snapshot")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	request := bindEffectObservation(snapshot, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity, result := ownerReconciliationEffectIdentity(*effect), test.result(request)
	if err := writeReconciliationEffectMarker(root, identity, request, result); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: request.Repository, Issue: request.Issue, ExpectedGeneration: effect.IssueGeneration}); err != nil {
		t.Fatal(err)
	}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	if _, err := coordinator.verifyPendingReconciliation(t.Context(), *effect); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("invalidated marker finish err=%v", err)
	}
	current, _ := owner.snapshot(t.Context())
	if _, exists := current.State.Effects[effect.ID]; exists {
		t.Fatalf("invalidated marker recreated effect: %#v", current.State.Effects[effect.ID])
	}
}
