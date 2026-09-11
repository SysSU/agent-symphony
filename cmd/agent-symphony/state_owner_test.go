package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestRuntimeOwnerMigratesLegacyStateWithoutInstallingLedger(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	manifest := writeDashboardManifest(t, root, 41, 1, "failed")
	server := dashboardServer{stateRoot: root, repository: "o/r"}
	if err := server.writeState(dashboardState{Version: dashboardStateVersion, Hidden: []dashboardHiddenAttempt{{Repository: "o/r", Issue: 41, Attempt: 1, Reason: "dismissed"}}}); err != nil {
		t.Fatal(err)
	}
	if err := server.writeRemovalState(dashboardRemovalState{Version: removalStateVersion, Intents: []dashboardRemovalIntent{{Manifest: manifest, PublishedHead: manifest.BaseSHA, CleanupStarted: true}}}); err != nil {
		t.Fatal(err)
	}
	receipt := controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "migration-41-1", Repository: "o/r", Action: "remove", Issue: 41, Attempt: 1, Confirm: true}, State: "pending"}
	if err := server.writeControlReceipts(controlReceiptState{Version: controlVersion, Receipts: []controlReceipt{receipt}}); err != nil {
		t.Fatal(err)
	}

	state, migrated, err := loadOrMigrateRuntimeOwnerState(root, "o/r")
	if err != nil || !migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
	}
	key := ownerAttemptKey("o/r", 41, 1)
	tombstone := state.Tombstones[key]
	if tombstone.Action != "removed" || tombstone.CleanupPhase != "cleanup-started" || tombstone.PublishedHead != manifest.BaseSHA || tombstone.Manifest == nil || !sameAttemptIdentity(*tombstone.Manifest, manifest) || state.AttemptGenerations[key] != 3 || tombstone.InvalidatedGeneration != 2 {
		t.Fatalf("tombstone=%#v generation=%d", tombstone, state.AttemptGenerations[key])
	}
	if len(state.Attempts) != 0 || len(state.Effects) != 1 || len(state.ControlReceipts) != 1 || !reflect.DeepEqual(state.ControlReceipts[0], receipt) {
		t.Fatalf("state=%#v", state)
	}
	for _, effect := range state.Effects {
		if effect.State != "pending" || effect.Action != "remove" || effect.AttemptGeneration != 3 || effect.IntentRevision != 1 {
			t.Fatalf("effect=%#v", effect)
		}
	}
	if _, err := os.Lstat(filepath.Join(root, runtimeOwnerStateFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dormant migration installed a ledger: %v", err)
	}
}

func TestRuntimeOwnerMigrationMissingMalformedConflictingAndInterruptedState(t *testing.T) {
	t.Run("missing optional files", func(t *testing.T) {
		root := resolvedTempDir(t)
		if err := bindDeployment(root, "o/r"); err != nil {
			t.Fatal(err)
		}
		state, migrated, err := loadOrMigrateRuntimeOwnerState(root, "o/r")
		if err != nil || !migrated || len(state.Attempts) != 0 || len(state.Tombstones) != 0 || state.Epoch != 0 || state.Revision != 0 {
			t.Fatalf("state=%#v migrated=%v err=%v", state, migrated, err)
		}
	})

	t.Run("malformed legacy file", func(t *testing.T) {
		root := resolvedTempDir(t)
		if err := bindDeployment(root, "o/r"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, "dashboard-state.json"), []byte(`{"version":1,"hidden":[],"extra":true}`), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadOrMigrateRuntimeOwnerState(root, "o/r"); err == nil || !strings.Contains(err.Error(), "dashboard") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("conflicting tombstones", func(t *testing.T) {
		root := resolvedTempDir(t)
		if err := bindDeployment(root, "o/r"); err != nil {
			t.Fatal(err)
		}
		server := dashboardServer{stateRoot: root, repository: "o/r"}
		state := dashboardState{Version: dashboardStateVersion, Hidden: []dashboardHiddenAttempt{
			{Repository: "o/r", Issue: 42, Attempt: 1, Reason: "archived"},
			{Repository: "o/r", Issue: 42, Attempt: 1, Reason: "dismissed"},
		}}
		if err := server.writeState(state); err != nil {
			t.Fatal(err)
		}
		if _, _, err := loadOrMigrateRuntimeOwnerState(root, "o/r"); err == nil || !strings.Contains(err.Error(), "conflicting") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("interrupted install", func(t *testing.T) {
		root := resolvedTempDir(t)
		if err := bindDeployment(root, "o/r"); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, ".runtime-state-interrupted"), []byte("partial"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, migrated, err := loadOrMigrateRuntimeOwnerState(root, "o/r"); err != nil || !migrated {
			t.Fatalf("migrated=%v err=%v", migrated, err)
		}
	})
}

func TestRuntimeOwnerExistingLedgerIsOneWayAndRestartIncrementsEpoch(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	initial, _, err := loadOrMigrateRuntimeOwnerState(root, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	owner, err := startTestStateOwner(t, root, initial, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	first, migrated, err := loadOrMigrateRuntimeOwnerState(root, "o/r")
	if err != nil || migrated || first.Epoch != 1 || first.Revision != 1 {
		t.Fatalf("first=%#v migrated=%v err=%v", first, migrated, err)
	}
	if err := os.WriteFile(filepath.Join(root, "dashboard-state.json"), []byte("malformed"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, migrated, err := loadOrMigrateRuntimeOwnerState(root, "o/r")
	if err != nil || migrated || !reflect.DeepEqual(loaded, first) {
		t.Fatalf("one-way loaded=%#v migrated=%v err=%v", loaded, migrated, err)
	}
	owner, err = startTestStateOwner(t, root, loaded, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := readRuntimeOwnerState(root, "o/r")
	if err != nil || restarted.Epoch != 2 || restarted.Revision != 2 {
		t.Fatalf("restarted=%#v err=%v", restarted, err)
	}
}

func TestStateOwnerPersistenceFailureKeepsCommittedSnapshotAndDispatchesNothing(t *testing.T) {
	root := resolvedTempDir(t)
	initial := newRuntimeOwnerState("o/r")
	manifest := ownerTestManifest(t, root, 43, 1, "running")
	issueKey, attemptKey := ownerIssueKey("o/r", 43), ownerAttemptKey("o/r", 43, 1)
	initial.IssueGenerations[issueKey], initial.AttemptGenerations[attemptKey] = 1, 1
	initial.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	entered, release := make(chan struct{}), make(chan struct{})
	writes := 0
	owner, err := startTestStateOwner(t, root, initial, func(runtimeOwnerState) error {
		writes++
		if writes == 1 {
			return nil
		}
		close(entered)
		<-release
		return errors.New("injected persistence failure")
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	result := make(chan error, 1)
	go func() {
		_, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 43, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "dismissed", CleanupPhase: "pending", Manifest: &manifest, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
		result <- err
	}()
	<-entered
	snapshot, err := owner.snapshot(t.Context())
	if err != nil || snapshot.State.Revision != 1 || len(snapshot.State.Tombstones) != 0 || len(snapshot.State.Effects) != 0 || len(snapshot.State.Attempts) != 1 {
		t.Fatalf("visible snapshot=%#v err=%v manifest=%#v", snapshot, err, manifest)
	}
	close(release)
	if err := <-result; err == nil || !strings.Contains(err.Error(), "injected persistence failure") {
		t.Fatalf("err=%v", err)
	}
	snapshot, err = owner.snapshot(t.Context())
	if err != nil || snapshot.State.Revision != 1 || len(snapshot.State.Tombstones) != 0 || len(snapshot.State.Effects) != 0 || len(snapshot.State.Attempts) != 1 {
		t.Fatalf("committed snapshot=%#v err=%v", snapshot, err)
	}
}

func TestStateOwnerRevisionGenerationTombstoneAndOutOfOrderResults(t *testing.T) {
	root := resolvedTempDir(t)
	var persisted runtimeOwnerState
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(state runtimeOwnerState) error {
		persisted = cloneRuntimeOwnerState(state)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	manifest := ownerTestManifest(t, root, 44, 1, "running")
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	issueKey, attemptKey := ownerIssueKey("o/r", 44), ownerAttemptKey("o/r", 44, 1)
	if created.State.Revision != 2 || created.State.IssueGenerations[issueKey] != 1 || created.State.AttemptGenerations[attemptKey] != 1 {
		t.Fatalf("created=%#v", created)
	}
	cycleOne, _ := owner.reconciliationSnapshot(t.Context())
	cycleTwo, _ := owner.reconciliationSnapshot(t.Context())
	newer := cloneManifest(manifest)
	newer.Diagnostic = "newer"
	identity := stateResultIdentity{Epoch: cycleTwo.State.Epoch, SourceRevision: cycleTwo.State.Revision, CycleID: cycleTwo.CycleID, IssueGeneration: 1, AttemptGeneration: 1}
	if _, err := owner.applyAttemptResult(t.Context(), applyAttemptResultCommand{Identity: identity, Manifest: newer}); err != nil {
		t.Fatal(err)
	}
	older := cloneManifest(manifest)
	older.Diagnostic = "older"
	identity.CycleID = cycleOne.CycleID
	if _, err := owner.applyAttemptResult(t.Context(), applyAttemptResultCommand{Identity: identity, Manifest: older}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("out-of-order err=%v", err)
	}
	beforeDelete, _ := owner.snapshot(t.Context())
	deleted, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 44, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "dismissed", CleanupPhase: "pending", Manifest: &newer, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
	if err != nil || effect == nil || effect.State != "pending" || deleted.State.Revision != 4 || deleted.State.AttemptGenerations[attemptKey] != 2 || deleted.State.Tombstones[attemptKey].InvalidatedGeneration != 1 {
		t.Fatalf("deleted=%#v effect=%#v err=%v persisted=%#v", deleted, effect, err, persisted)
	}
	replayed, replayedEffect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 44, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "dismissed", CleanupPhase: "pending", Manifest: &newer, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
	if err != nil || replayed.State.Revision != deleted.State.Revision || replayedEffect == nil || replayedEffect.ID != effect.ID {
		t.Fatalf("replayed=%#v effect=%#v err=%v", replayed, replayedEffect, err)
	}
	identity = stateResultIdentity{Epoch: beforeDelete.State.Epoch, SourceRevision: beforeDelete.State.Revision, CycleID: cycleTwo.CycleID + 1, IssueGeneration: 1, AttemptGeneration: 1}
	if _, err := owner.applyAttemptResult(t.Context(), applyAttemptResultCommand{Identity: identity, Manifest: manifest}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("invalidated result err=%v", err)
	}
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 2}); !errors.Is(err, errAttemptTombstoned) {
		t.Fatalf("resurrection err=%v", err)
	}
	alias, _ := owner.snapshot(t.Context())
	alias.State.AttemptGenerations[attemptKey] = 99
	alias.State.Tombstones[attemptKey] = runtimeTombstone{}
	alias.State.ControlReceipts = append(alias.State.ControlReceipts, controlReceipt{})
	stable, _ := owner.snapshot(t.Context())
	if stable.State.AttemptGenerations[attemptKey] != 2 || stable.State.Tombstones[attemptKey].Action != "dismissed" || len(stable.State.ControlReceipts) != 0 {
		t.Fatalf("snapshot aliases owner state: %#v", stable)
	}
}

func TestStateOwnerSynchronousAttemptChangeInvalidatesOlderResult(t *testing.T) {
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	manifest := ownerTestManifest(t, root, 45, 1, "running")
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	cycle, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	changed := cloneManifest(manifest)
	changed.Diagnostic = "user mutation"
	committed, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: changed, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1})
	if err != nil || committed.State.AttemptGenerations[ownerAttemptKey("o/r", 45, 1)] != 2 {
		t.Fatalf("committed=%#v err=%v", committed, err)
	}
	identity := stateResultIdentity{Epoch: created.State.Epoch, SourceRevision: cycle.State.Revision, CycleID: cycle.CycleID, IssueGeneration: 1, AttemptGeneration: 1}
	if _, err := owner.applyAttemptResult(t.Context(), applyAttemptResultCommand{Identity: identity, Manifest: manifest}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("older result err=%v", err)
	}
}

func TestStateOwnerRejectsEffectCompletionAfterIssueGenerationChanges(t *testing.T) {
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	manifest := ownerTestManifest(t, root, 46, 1, "running")
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	identity := stateResultIdentity{Epoch: created.State.Epoch, SourceRevision: created.State.Revision, IssueGeneration: 1, AttemptGeneration: 1}
	_, effect, err := owner.recordEffect(t.Context(), recordEffectCommand{Identity: identity, Action: "monitor", Manifest: manifest})
	if err != nil || effect == nil {
		t.Fatalf("effect=%#v err=%v", effect, err)
	}
	if _, err := owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: 46, ExpectedGeneration: 1}); err != nil {
		t.Fatal(err)
	}
	identity.EffectID = effect.ID
	if _, err := owner.completeEffect(t.Context(), completeEffectCommand{Identity: identity}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("completion err=%v", err)
	}
	state, _ := owner.snapshot(t.Context())
	if len(state.State.Effects) != 0 || validateRuntimeOwnerState(state.State, runtimeOwnerAttemptRoot(root), root, true) != nil {
		t.Fatalf("state=%#v", state)
	}
}

func TestStateOwnerCompletesTombstoneCleanupAfterIssueGenerationChanges(t *testing.T) {
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	manifest := ownerTestManifest(t, root, 48, 1, "failed")
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	deleted, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 48, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "abandoned", CleanupPhase: "cleanup-started", Manifest: &manifest, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
	if err != nil || effect == nil {
		t.Fatalf("deleted=%#v effect=%#v err=%v", deleted, effect, err)
	}
	advanced, err := owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: 48, ExpectedGeneration: 1})
	if err != nil || advanced.State.Effects[effect.ID].State != "pending" || advanced.State.Tombstones[ownerAttemptKey("o/r", 48, 1)].EffectID != effect.ID {
		t.Fatalf("advanced=%#v err=%v", advanced, err)
	}
	identity := stateResultIdentity{Epoch: advanced.State.Epoch, EffectID: effect.ID, IssueGeneration: 1, AttemptGeneration: 2}
	completed, err := owner.completeEffect(t.Context(), completeEffectCommand{Identity: identity, Diagnostic: "cleanup verified"})
	if err != nil {
		t.Fatal(err)
	}
	tombstone := completed.State.Tombstones[ownerAttemptKey("o/r", 48, 1)]
	if tombstone.CleanupPhase != "completed" || completed.State.Effects[effect.ID].State != "completed" || validateRuntimeOwnerState(completed.State, runtimeOwnerAttemptRoot(root), root, true) != nil {
		t.Fatalf("completed=%#v", completed)
	}
}

func TestRuntimeOwnerValidationRejectsEffectWithStaleIssueGeneration(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 47, 1, "running")
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 2
	issueKey, attemptKey := ownerIssueKey("o/r", 47), ownerAttemptKey("o/r", 47, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 2, 1
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	effect := runtimeEffectIntent{Action: "monitor", Repository: "o/r", Issue: 47, Attempt: 1, IssueGeneration: 1, AttemptGeneration: 1, IntentRevision: 2, State: "pending"}
	effect.ID = runtimeEffectID(effect)
	state.Effects[effect.ID] = effect
	if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(root), root, true); err == nil {
		t.Fatal("effect with stale issue generation was accepted")
	}
}

func TestStateOwnerRejectsUnrecoverablePendingTombstones(t *testing.T) {
	for _, test := range []struct {
		name          string
		action        string
		manifest      bool
		publishedHead string
		want          string
	}{
		{name: "missing resource identity", action: "abandoned", want: "resource identity"},
		{name: "invalid permanent removal head", action: "removed", manifest: true, publishedHead: "not-an-object-id", want: "published head"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDir(t)
			manifest := ownerTestManifest(t, root, 49, 1, "failed")
			initial := newRuntimeOwnerState("o/r")
			issueKey, attemptKey := ownerIssueKey("o/r", 49), ownerAttemptKey("o/r", 49, 1)
			initial.IssueGenerations[issueKey], initial.AttemptGenerations[attemptKey] = 1, 1
			initial.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
			owner, err := startTestStateOwner(t, root, initial, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			var identity *agentruntime.Manifest
			if test.manifest {
				identity = &manifest
			}
			_, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 49, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: test.action, CleanupPhase: "pending", PublishedHead: test.publishedHead, Manifest: identity, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
			if err == nil || !strings.Contains(err.Error(), test.want) || effect != nil {
				t.Fatalf("effect=%#v err=%v", effect, err)
			}
			snapshot, snapshotErr := owner.snapshot(t.Context())
			if snapshotErr != nil || snapshot.State.Revision != 1 || len(snapshot.State.Tombstones) != 0 || len(snapshot.State.Effects) != 0 {
				t.Fatalf("snapshot=%#v err=%v", snapshot, snapshotErr)
			}
		})
	}
}

func TestStateOwnerShutdownDrainsWriteAndRejectsAcceptedQueue(t *testing.T) {
	root := resolvedTempDir(t)
	entered, release := make(chan struct{}), make(chan struct{})
	writes := 0
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error {
		writes++
		if writes == 2 {
			close(entered)
			<-release
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	first := stateOwnerCommand{kind: stateOwnerUpsertAttempt, upsert: upsertAttemptCommand{Manifest: ownerTestManifest(t, root, 50, 1, "running")}, reply: make(chan stateOwnerResult, 1)}
	owner.commands <- first
	<-entered

	const queued = 8
	requests := make([]stateOwnerCommand, queued)
	for index := range requests {
		requests[index] = stateOwnerCommand{kind: stateOwnerUpsertAttempt, upsert: upsertAttemptCommand{Manifest: ownerTestManifest(t, root, 51+index, 1, "running")}, reply: make(chan stateOwnerResult, 1)}
		owner.commands <- requests[index]
	}
	stopReply := make(chan error, 1)
	owner.stop <- stateOwnerStopRequest{reply: stopReply}
	close(release)
	if result := <-first.reply; result.err != nil || result.snapshot.State.Revision != 2 {
		t.Fatalf("in-flight result=%#v", result)
	}
	if err := <-stopReply; err != nil {
		t.Fatal(err)
	}
	for _, request := range requests {
		if result := <-request.reply; !errors.Is(result.err, errStateOwnerStopped) {
			t.Fatalf("queued result=%#v", result)
		}
	}
	if _, err := owner.snapshot(t.Context()); !errors.Is(err, errStateOwnerStopped) {
		t.Fatalf("snapshot after shutdown err=%v", err)
	}
	if _, err := owner.submit(t.Context(), stateOwnerCommand{kind: stateOwnerStart}); !errors.Is(err, errStateOwnerStopped) {
		t.Fatalf("command after shutdown err=%v", err)
	}
}

func TestRuntimeOwnerWriterRoundTripsPrivateValidatedState(t *testing.T) {
	root := resolvedTempDir(t)
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	if err := writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, "o/r")
	if err != nil || !reflect.DeepEqual(loaded, state) {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
	info, err := os.Stat(filepath.Join(root, runtimeOwnerStateFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("info=%v err=%v", info, err)
	}
	if err := os.WriteFile(filepath.Join(root, runtimeOwnerStateFile), []byte(`{"version":2}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRuntimeOwnerState(root, "o/r"); err == nil {
		t.Fatal("malformed installed ledger was accepted")
	}
}

func TestStateOwnerControlReceiptTransitionIsDurableAndIdempotent(t *testing.T) {
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	request := controlRequest{Version: controlVersion, RequestID: "owner-receipt", Repository: "o/r", Action: "reconcile"}
	pending, err := owner.recordControlReceipt(t.Context(), controlReceipt{Request: request, State: "pending"})
	if err != nil || pending.State.Revision != 2 {
		t.Fatalf("pending=%#v err=%v", pending, err)
	}
	result := controlResult{Version: controlVersion, RequestID: request.RequestID, Action: request.Action, OK: true, Status: 200, Data: json.RawMessage(`{"ok":true}`)}
	completedReceipt := controlReceipt{Request: request, State: "completed", Result: &result}
	completed, err := owner.recordControlReceipt(t.Context(), completedReceipt)
	if err != nil || completed.State.Revision != 3 {
		t.Fatalf("completed=%#v err=%v", completed, err)
	}
	replayed, err := owner.recordControlReceipt(t.Context(), completedReceipt)
	if err != nil || replayed.State.Revision != completed.State.Revision {
		t.Fatalf("replayed=%#v err=%v", replayed, err)
	}
	completedReceipt.Result.Data[0] = '['
	snapshot, _ := owner.snapshot(t.Context())
	if string(snapshot.State.ControlReceipts[0].Result.Data) != `{"ok":true}` {
		t.Fatalf("receipt aliased caller data: %q", snapshot.State.ControlReceipts[0].Result.Data)
	}
}

func TestStateOwnerRejectsManifestRootChosenByCaller(t *testing.T) {
	root, foreign := resolvedTempDir(t), resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	manifest := ownerTestManifest(t, foreign, 60, 1, "running")
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err == nil {
		t.Fatal("manifest selected its own trust root")
	}
}

func TestStateOwnerTransitionsUseBoundIdentityWithoutFilesystemAccess(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 61, 1, "running")
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	if err := os.RemoveAll(root); err != nil {
		t.Fatal(err)
	}
	snapshot, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	key := ownerAttemptKey("o/r", 61, 1)
	if err != nil || snapshot.State.Revision != 2 || snapshot.State.Attempts[key].Manifest.Worktree != manifest.Worktree {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}

func ownerTestManifest(t *testing.T, stateRoot string, issue, attempt int, state string) agentruntime.Manifest {
	t.Helper()
	return writeDashboardManifest(t, stateRoot, issue, attempt, state)
}

func startTestStateOwner(t *testing.T, stateRoot string, state runtimeOwnerState, persist func(runtimeOwnerState) error) (*stateOwner, error) {
	t.Helper()
	attemptRoot := productionAttemptRoot(stateRoot)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	canonical, err := filepath.EvalSymlinks(attemptRoot)
	if err != nil {
		t.Fatal(err)
	}
	return startStateOwner(t.Context(), stateRoot, canonical, state, persist)
}

func TestRuntimeOwnerRejectsInvalidIssueGenerationKey(t *testing.T) {
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	state.IssueGenerations["o/r#0"] = 1
	root := resolvedTempDir(t)
	if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(root), root, true); err == nil {
		t.Fatal("issue zero generation was accepted")
	}
}

func TestStateOwnerConcurrentSnapshotsDoNotAlias(t *testing.T) {
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	var wait sync.WaitGroup
	for range 32 {
		wait.Add(1)
		go func() {
			defer wait.Done()
			snapshot, err := owner.snapshot(t.Context())
			if err != nil {
				t.Error(err)
				return
			}
			snapshot.State.IssueGenerations["o/r#999"] = 1
		}()
	}
	wait.Wait()
	snapshot, err := owner.snapshot(t.Context())
	if err != nil || len(snapshot.State.IssueGenerations) != 0 {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}

func TestRuntimeOwnerJSONRejectsUnknownFields(t *testing.T) {
	root := resolvedTempDir(t)
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	body = append(body[:len(body)-1], []byte(`,"unknown":true}`)...)
	if err := os.WriteFile(filepath.Join(root, runtimeOwnerStateFile), body, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRuntimeOwnerState(root, "o/r"); err == nil {
		t.Fatal("unknown ledger field was accepted")
	}
}
