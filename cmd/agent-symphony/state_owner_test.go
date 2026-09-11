package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func writeLegacyStateFixture(t *testing.T, root, name string, value any) {
	t.Helper()
	body, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, name), append(body, '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeOwnerMigratesLegacyStateWithoutInstallingLedger(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	manifest := writeDashboardManifest(t, root, 41, 1, "failed")
	writeLegacyStateFixture(t, root, "dashboard-state.json", dashboardState{Version: dashboardStateVersion, Hidden: []dashboardHiddenAttempt{{Repository: "o/r", Issue: 41, Attempt: 1, Reason: "dismissed"}}})
	writeLegacyStateFixture(t, root, "removal-state.json", dashboardRemovalState{Version: removalStateVersion, Intents: []dashboardRemovalIntent{{Manifest: manifest, PublishedHead: manifest.BaseSHA, CleanupStarted: true}}})
	state, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
	if err != nil || !migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
	}
	key := ownerAttemptKey("o/r", 41, 1)
	tombstone := state.Tombstones[key]
	if tombstone.Action != "removed" || tombstone.CleanupPhase != "cleanup-started" || tombstone.PublishedHead != manifest.BaseSHA || tombstone.Manifest == nil || !sameAttemptIdentity(*tombstone.Manifest, manifest) || tombstone.CleanupPolicy == nil || tombstone.CleanupPolicy.Action != "remove" || state.AttemptGenerations[key] != 2 || tombstone.InvalidatedGeneration != 1 {
		t.Fatalf("tombstone=%#v generation=%d", tombstone, state.AttemptGenerations[key])
	}
	if len(state.Attempts) != 0 || len(state.Effects) != 1 || len(state.ControlReceipts) != 1 {
		t.Fatalf("state=%#v", state)
	}
	for _, effect := range state.Effects {
		if effect.State != "pending" || effect.Action != string(agentruntime.EffectCleanup) || effect.AttemptGeneration != 2 || effect.IntentEpoch != 1 || effect.IntentRevision != 1 || !agentruntime.ValidEffectRequestDigest(effect.RequestDigest) {
			t.Fatalf("effect=%#v", effect)
		}
	}
	if receipt := state.ControlReceipts[0]; receipt.State != "pending" || receipt.Phase != operatorPhaseCleanupStarted || receipt.EffectID != tombstone.EffectID || receipt.Request.Action != "remove" {
		t.Fatalf("receipt=%#v", receipt)
	}
	if _, err := os.Lstat(filepath.Join(root, runtimeOwnerStateFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("dormant migration installed a ledger: %v", err)
	}
}

func TestRuntimeOwnerMigratesCompletedLegacyRemovalWithoutResourceIdentity(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	writeLegacyStateFixture(t, root, "dashboard-state.json", dashboardState{Version: dashboardStateVersion, Hidden: []dashboardHiddenAttempt{{Repository: "o/r", Issue: 42, Attempt: 1, Reason: "removed"}}})
	state, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
	if err != nil || !migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
	}
	key := ownerAttemptKey("o/r", 42, 1)
	tombstone := state.Tombstones[key]
	if !bareCompletedRemoval(tombstone) || state.AttemptGenerations[key] != tombstone.Generation || len(state.Effects) != 0 || len(state.ControlReceipts) != 0 {
		t.Fatalf("state=%#v tombstone=%#v", state, tombstone)
	}
	state.Epoch, state.Revision, tombstone.Revision = 1, 1, 1
	state.Tombstones[key] = tombstone
	if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(root), root, true); err != nil {
		t.Fatalf("persisted terminal removal: %v", err)
	}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 42, Attempt: 1, PR: 9, BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), State: "active", Checks: []string{}}
	issue := issueFact(42, "stale")
	issue.Active, issue.ActiveAttempt = true, &remote
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	accepted := applyReconciliationInput(t, owner, input)
	projected, err := projectOwnerStatus(accepted, 1, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if slices.ContainsFunc(projected.Statuses, func(status orchestrator.RecoveryStatus) bool { return status.Issue == 42 && status.Attempt == 1 }) {
		t.Fatalf("completed removal was resurrected: %#v", projected.Statuses)
	}
}

func TestRuntimeOwnerMigratedPendingRemovalResumesTypedCleanup(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	manifest := writeDashboardManifest(t, root, 43, 1, "failed")
	writeLegacyStateFixture(t, root, "removal-state.json", dashboardRemovalState{Version: removalStateVersion, Intents: []dashboardRemovalIntent{{Manifest: manifest, PublishedHead: manifest.BaseSHA, CleanupStarted: true}}})
	state, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
	if err != nil || !migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
	}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	if err := os.MkdirAll(productionSnapshotRoot(root), 0o700); err != nil {
		t.Fatal(err)
	}
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	requestID := mustOwnerSnapshot(t, owner).State.ControlReceipts[0].Request.RequestID
	if err := service.resumeReceipt(t.Context(), requestID); err != nil {
		t.Fatal(err)
	}
	final := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey("o/r", 43, 1)
	tombstone := final.Tombstones[key]
	if tombstone.CleanupPhase != "completed" || final.Effects[tombstone.EffectID].State != "completed" || len(final.ControlReceipts) != 1 || final.ControlReceipts[0].State != "completed" {
		t.Fatalf("state=%#v", final)
	}
}

func refreshOwnerObservation(t *testing.T, owner *stateOwner, issue int) stateOwnerSnapshot {
	t.Helper()
	snapshot, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	issueKey := ownerIssueKey(snapshot.State.Repository, issue)
	observation, ok := snapshot.State.Observations[issueKey]
	if !ok {
		t.Fatalf("issue %d has no observation", issue)
	}
	group := reconciliationIssueGroup{Fact: cloneReconciliationIssueFact(observation.Fact), IssueUpdates: slices.Clone(observation.IssueUpdates)}
	issueGenerations := map[string]uint64{issueKey: snapshot.State.IssueGenerations[issueKey]}
	attemptGenerations := map[string]uint64{}
	for key, accepted := range observation.Attempts {
		group.Attempts = append(group.Attempts, cloneReconciliationAttemptFact(accepted.Fact))
		attemptGenerations[key] = snapshot.State.AttemptGenerations[key]
	}
	committed, err := owner.applyReconciliation(t.Context(), reconciliationCollection{Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, CycleID: snapshot.CycleID}, Scope: issueScope(issue), Complete: true, IssueGenerations: issueGenerations, AttemptGenerations: attemptGenerations, Issues: []reconciliationIssueGroup{group}})
	if err != nil {
		t.Fatal(err)
	}
	return committed
}

func TestRuntimeOwnerMigrationMissingMalformedConflictingAndInterruptedState(t *testing.T) {
	t.Run("missing optional files", func(t *testing.T) {
		root := resolvedTempDir(t)
		if err := bindDeployment(root, "o/r"); err != nil {
			t.Fatal(err)
		}
		state, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
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
		if _, _, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r"); err == nil || !strings.Contains(err.Error(), "dashboard") {
			t.Fatalf("err=%v", err)
		}
	})

	t.Run("conflicting tombstones", func(t *testing.T) {
		root := resolvedTempDir(t)
		if err := bindDeployment(root, "o/r"); err != nil {
			t.Fatal(err)
		}
		state := dashboardState{Version: dashboardStateVersion, Hidden: []dashboardHiddenAttempt{
			{Repository: "o/r", Issue: 42, Attempt: 1, Reason: "archived"},
			{Repository: "o/r", Issue: 42, Attempt: 1, Reason: "dismissed"},
		}}
		writeLegacyStateFixture(t, root, "dashboard-state.json", state)
		if _, _, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r"); err == nil || !strings.Contains(err.Error(), "conflicting") {
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
		if _, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r"); err != nil || !migrated {
			t.Fatalf("migrated=%v err=%v", migrated, err)
		}
	})
}

func TestRuntimeOwnerMigrationRejectsUnprovedLegacyWork(t *testing.T) {
	for _, test := range []struct {
		name  string
		setup func(*testing.T, string)
		want  string
	}{
		{
			name: "pending control receipt",
			setup: func(t *testing.T, root string) {
				receipt := controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "migration-41-1", Repository: "o/r", Action: "remove", Issue: 41, Attempt: 1, Confirm: true}, State: "pending"}
				writeLegacyStateFixture(t, root, controlReceiptsFile, controlReceiptState{Version: controlVersion, Receipts: []controlReceipt{receipt}})
			},
			want: "pending without a v2 effect proof",
		},
		{
			name: "active reviewer",
			setup: func(t *testing.T, root string) {
				manifest := writeDashboardManifest(t, root, 41, 1, "completed")
				manifest.ReviewState = "running"
				body, _ := json.Marshal(manifest)
				if err := os.WriteFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"), body, 0o600); err != nil {
					t.Fatal(err)
				}
			},
			want: "active reviewer",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDir(t)
			if err := bindDeployment(root, "o/r"); err != nil {
				t.Fatal(err)
			}
			test.setup(t, root)
			if _, _, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r"); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestRuntimeOwnerMigrationImportsLegacyPRRecoveryOnce(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	manifest := writeDashboardManifest(t, root, 41, 1, "running")
	legacy := []internalgithub.PRState{{Repository: "o/r", Number: 7, Issue: 41, Attempt: 1, HeadSHA: manifest.BaseSHA}}
	body, _ := json.Marshal(legacy)
	path := filepath.Join(root, "pr-state.json")
	if err := os.WriteFile(path, body, 0o600); err != nil {
		t.Fatal(err)
	}
	state, migrated, err := loadOrMigrateRuntimeOwnerState(root, path, "o/r")
	key := ownerAttemptKey("o/r", 41, 1)
	if err != nil || !migrated || !reflect.DeepEqual(state.Recoveries[key].State, legacy[0]) || state.Recoveries[key].IssueGeneration != 1 || state.Recoveries[key].AttemptGeneration != 1 {
		t.Fatalf("state=%#v migrated=%v err=%v", state, migrated, err)
	}
	owner, err := startTestStateOwner(t, root, state, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("malformed"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted, migrated, err := loadOrMigrateRuntimeOwnerState(root, path, "o/r")
	if err != nil || migrated || !reflect.DeepEqual(restarted.Recoveries[key].State, legacy[0]) {
		t.Fatalf("state=%#v migrated=%v err=%v", restarted, migrated, err)
	}
}

func TestDeploymentFenceFailsOldBinaryClosedAndResumesBeforeLedger(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	if err := installDeploymentFence(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	if _, err := readDeploymentIdentityVersion(root, legacyDeploymentIdentityVersion); err == nil {
		t.Fatal("v1 deployment reader accepted the v2 fence")
	}
	fenceBefore, err := os.ReadFile(filepath.Join(root, "deployment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := bindDeployment(root, "o/r"); err == nil {
		t.Fatal("legacy deployment writer gate accepted the v2 fence")
	}
	fenceAfter, err := os.ReadFile(filepath.Join(root, "deployment.json"))
	if err != nil || !slices.Equal(fenceBefore, fenceAfter) {
		t.Fatalf("legacy writer changed the v2 fence: err=%v", err)
	}
	if _, err := os.Lstat(filepath.Join(root, runtimeOwnerStateFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("fence crash window unexpectedly has a ledger: %v", err)
	}
	initial, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
	if err != nil || !migrated {
		t.Fatalf("migrated=%v err=%v", migrated, err)
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
	if _, err := readRuntimeOwnerState(root, "o/r"); err != nil {
		t.Fatalf("fenced restart did not install the ledger: %v", err)
	}
	if err := installDeploymentFence(root, "o/r"); err != nil {
		t.Fatalf("fence replay: %v", err)
	}
}

func TestRuntimeOwnerExistingLedgerIsOneWayAndRestartIncrementsEpoch(t *testing.T) {
	root := resolvedTempDir(t)
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	initial, _, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
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
	first, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
	if err != nil || migrated || first.Epoch != 1 || first.Revision != 1 {
		t.Fatalf("first=%#v migrated=%v err=%v", first, migrated, err)
	}
	if err := os.WriteFile(filepath.Join(root, "dashboard-state.json"), []byte("malformed"), 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, migrated, err := loadOrMigrateRuntimeOwnerState(root, filepath.Join(root, "pr-state.json"), "o/r")
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
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
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
		_, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 43, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "abandoned", CleanupPhase: "pending", Manifest: &manifest, CleanupPolicy: &agentruntime.EffectCleanupPolicy{Action: "abandon"}, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
		result <- err
	}()
	<-entered
	snapshot, err := owner.snapshot(t.Context())
	if err != nil || snapshot.State.Revision != 1 || len(snapshot.State.Tombstones) != 0 || len(snapshot.State.Effects) != 0 || len(snapshot.State.Attempts) != 1 {
		t.Fatalf("visible snapshot=%#v err=%v manifest=%#v", snapshot, err, manifest)
	}
	unblock()
	if err := <-result; err == nil || !strings.Contains(err.Error(), "injected persistence failure") {
		t.Fatalf("err=%v", err)
	}
	snapshot, err = owner.snapshot(t.Context())
	if err != nil || snapshot.State.Revision != 1 || len(snapshot.State.Tombstones) != 0 || len(snapshot.State.Effects) != 0 || len(snapshot.State.Attempts) != 1 {
		t.Fatalf("committed snapshot=%#v err=%v", snapshot, err)
	}
}

func TestStateOwnerCancellationAfterPersistenceDispatchStillCommits(t *testing.T) {
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
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	<-owner.commits
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() {
		_, err := owner.recordControlReceipt(ctx, controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "admitted", Repository: "o/r", Action: "reconcile"}, State: "pending"})
		result <- err
	}()
	<-entered
	cancel()
	select {
	case err := <-result:
		t.Fatalf("caller returned before durable result: %v", err)
	default:
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatalf("caller result=%v", err)
	}
	committed := <-owner.commits
	if receipt, ok := operatorReceiptByID(committed.State, "admitted"); !ok || receipt.State != "pending" {
		t.Fatalf("dispatched command was not committed: %#v", committed.State)
	}
	if writes != 2 {
		t.Fatalf("writes=%d", writes)
	}
}

func TestStateOwnerCancellationWhileQueuedSkipsPersistence(t *testing.T) {
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
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	first := make(chan error, 1)
	go func() {
		_, err := owner.recordControlReceipt(context.Background(), controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "first", Repository: "o/r", Action: "reconcile"}, State: "pending"})
		first <- err
	}()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	admitted := make(chan struct{})
	queued := stateOwnerCommand{
		kind:    stateOwnerRecordControlReceipt,
		receipt: recordControlReceiptCommand{Receipt: controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "canceled", Repository: "o/r", Action: "reconcile"}, State: "pending"}},
	}
	queuedResult := make(chan error, 1)
	go func() {
		_, err := owner.submitWithAdmission(ctx, queued, admitted)
		queuedResult <- err
	}()
	<-admitted
	cancel()
	if err := <-queuedResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("queued result=%v", err)
	}
	select {
	case err := <-first:
		t.Fatalf("unrelated persistence completed before release: %v", err)
	default:
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	snapshot := mustOwnerSnapshot(t, owner)
	if writes != 2 || snapshot.State.Revision != 2 || len(snapshot.State.ControlReceipts) != 1 || snapshot.State.ControlReceipts[0].Request.RequestID != "first" || len(snapshot.State.Effects) != 0 || len(snapshot.State.Tombstones) != 0 {
		t.Fatalf("writes=%d state=%#v", writes, snapshot.State)
	}
}

func TestStateOwnerInstalledPersistenceErrorPoisonsAdmission(t *testing.T) {
	root := resolvedTempDir(t)
	writes := 0
	installed := statePersistenceInstalledError{err: errors.New("injected directory sync failure")}
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error {
		writes++
		if writes == 2 {
			return installed
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	manifest := ownerTestManifest(t, root, 53, 1, "running")
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); !errors.Is(err, installed.err) {
		t.Fatalf("installed error=%v", err)
	}
	if _, err := owner.snapshot(t.Context()); !errors.Is(err, installed.err) {
		t.Fatalf("snapshot after installed error=%v", err)
	}
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); !errors.Is(err, installed.err) {
		t.Fatalf("mutation after installed error=%v", err)
	}
	if writes != 2 {
		t.Fatalf("poisoned owner performed %d writes", writes)
	}
}

func TestStateOwnerGenerationTombstoneAndSnapshotIsolation(t *testing.T) {
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
	if created.State.IssueGenerations[issueKey] != 1 || created.State.AttemptGenerations[attemptKey] != 1 {
		t.Fatalf("created=%#v", created)
	}
	beforeDelete, _ := owner.snapshot(t.Context())
	policy := &agentruntime.EffectCleanupPolicy{Action: "abandon"}
	deleted, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 44, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "abandoned", CleanupPhase: "pending", Manifest: &manifest, CleanupPolicy: policy, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
	if err != nil || effect == nil || effect.State != "pending" || deleted.State.Revision != beforeDelete.State.Revision+1 || deleted.State.AttemptGenerations[attemptKey] != 2 || deleted.State.Tombstones[attemptKey].InvalidatedGeneration != 1 {
		t.Fatalf("deleted=%#v effect=%#v err=%v persisted=%#v", deleted, effect, err, persisted)
	}
	replayed, replayedEffect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 44, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "abandoned", CleanupPhase: "pending", Manifest: &manifest, CleanupPolicy: policy, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
	if err != nil || replayed.State.Revision != deleted.State.Revision || replayedEffect == nil || replayedEffect.ID != effect.ID {
		t.Fatalf("replayed=%#v effect=%#v err=%v", replayed, replayedEffect, err)
	}
	current := mustOwnerSnapshot(t, owner)
	identity := stateResultIdentity{Epoch: current.State.Epoch, SourceRevision: current.State.Revision, IssueGeneration: 1, AttemptGeneration: 2}
	preparing := cloneManifest(manifest)
	preparing.State, preparing.Diagnostic = "preparing", ""
	if _, _, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectPrepare, Manifest: preparing, RequestDigest: strings.Repeat("f", 64)}); !errors.Is(err, errAttemptTombstoned) {
		t.Fatalf("resurrection err=%v", err)
	}
	alias, _ := owner.snapshot(t.Context())
	alias.State.AttemptGenerations[attemptKey] = 99
	alias.State.Tombstones[attemptKey] = runtimeTombstone{}
	alias.State.ControlReceipts = append(alias.State.ControlReceipts, controlReceipt{})
	stable, _ := owner.snapshot(t.Context())
	if stable.State.AttemptGenerations[attemptKey] != 2 || stable.State.Tombstones[attemptKey].Action != "abandoned" || len(stable.State.ControlReceipts) != 0 {
		t.Fatalf("snapshot aliases owner state: %#v", stable)
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
	manifest.Diagnostic = "test failure"
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	deleted, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 48, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "abandoned", CleanupPhase: "cleanup-started", Manifest: &manifest, CleanupPolicy: &agentruntime.EffectCleanupPolicy{Action: "abandon"}, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
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
		{name: "invalid permanent removal head", action: "removed", manifest: true, publishedHead: "not-an-object-id", want: "cleanup policy"},
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
			policyAction := map[string]string{"abandoned": "abandon", "removed": "remove"}[test.action]
			_, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 49, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: test.action, CleanupPhase: "pending", PublishedHead: test.publishedHead, Manifest: identity, CleanupPolicy: &agentruntime.EffectCleanupPolicy{Action: policyAction, PublishedHead: test.publishedHead}, EffectAction: "cleanup", EffectRequestDigest: strings.Repeat("a", 64)})
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
	first := stateOwnerCommand{kind: stateOwnerRecordControlReceipt, receipt: recordControlReceiptCommand{Receipt: controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "first", Repository: "o/r", Action: "reconcile"}, State: "pending"}}, reply: make(chan stateOwnerResult, 1)}
	owner.commands <- first
	<-entered

	const queued = 8
	requests := make([]stateOwnerCommand, queued)
	for index := range requests {
		requests[index] = stateOwnerCommand{kind: stateOwnerRecordControlReceipt, receipt: recordControlReceiptCommand{Receipt: controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: fmt.Sprintf("queued-%d", index), Repository: "o/r", Action: "reconcile"}, State: "pending"}}, reply: make(chan stateOwnerResult, 1)}
		owner.commands <- requests[index]
	}
	closed := make(chan error, 1)
	go func() { closed <- owner.close(context.Background()) }()
	for _, request := range requests {
		if result := <-request.reply; !errors.Is(result.err, errStateOwnerStopped) {
			t.Fatalf("queued result=%#v", result)
		}
	}
	close(release)
	if result := <-first.reply; result.err != nil || result.snapshot.State.Revision != 2 {
		t.Fatalf("in-flight result=%#v", result)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if _, err := owner.snapshot(t.Context()); !errors.Is(err, errStateOwnerStopped) {
		t.Fatalf("snapshot after shutdown err=%v", err)
	}
	if _, err := owner.submit(t.Context(), stateOwnerCommand{kind: stateOwnerStart}); !errors.Is(err, errStateOwnerStopped) {
		t.Fatalf("command after shutdown err=%v", err)
	}
}

func TestStateOwnerShutdownStartsWithAlreadyCanceledWaitContext(t *testing.T) {
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
	result := make(chan error, 1)
	go func() {
		_, err := owner.recordControlReceipt(t.Context(), controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "in-flight", Repository: "o/r", Action: "reconcile"}, State: "pending"})
		result <- err
	}()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := owner.close(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-owner.done:
		t.Fatal("owner stopped before the admitted write finished")
	default:
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	select {
	case <-owner.done:
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
}

func TestStateOwnerConcurrentCloseIsIdempotent(t *testing.T) {
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
	committed := make(chan error, 1)
	go func() {
		_, err := owner.recordControlReceipt(context.Background(), controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "drain", Repository: "o/r", Action: "reconcile"}, State: "pending"})
		committed <- err
	}()
	<-entered
	const callers = 16
	ready, start, results := make(chan struct{}, callers), make(chan struct{}), make(chan error, callers)
	for range callers {
		go func() {
			ready <- struct{}{}
			<-start
			results <- owner.close(context.Background())
		}()
	}
	for range callers {
		<-ready
	}
	close(start)
	close(release)
	if err := <-committed; err != nil {
		t.Fatal(err)
	}
	for range callers {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if writes != 2 {
		t.Fatalf("writes=%d", writes)
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
	if err != nil || snapshot.State.Revision <= 1 || snapshot.State.Attempts[key].Manifest.Worktree != manifest.Worktree {
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

// upsertAttempt is test-only compatibility for fixtures that predate the
// production cutover. It builds authority through the typed runtime path; the
// untyped production owner command no longer exists.
func (o *stateOwner) upsertAttempt(ctx context.Context, command upsertAttemptCommand) (stateOwnerSnapshot, error) {
	snapshot, err := o.snapshot(ctx)
	if err != nil {
		return stateOwnerSnapshot{}, err
	}
	key := ownerAttemptKey(command.Manifest.Repository, command.Manifest.Issue, command.Manifest.Attempt)
	if _, exists := snapshot.State.Attempts[key]; exists {
		return stateOwnerSnapshot{}, errors.New("test fixture cannot replace existing attempt authority")
	}
	target := cloneManifest(command.Manifest)
	manifest := cloneManifest(target)
	manifest.State, manifest.Diagnostic = "preparing", ""
	identity := stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)]}
	committed, effect, err := o.beginRuntimeEffect(ctx, beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectPrepare, Manifest: manifest, RequestDigest: strings.Repeat("1", 64)})
	if err != nil {
		return stateOwnerSnapshot{}, err
	}
	committed, err = o.finishRuntimeEffect(ctx, finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectPrepare, Manifest: manifest})
	if err != nil || target.State == "preparing" {
		return committed, err
	}
	identity = stateResultIdentity{Epoch: committed.State.Epoch, SourceRevision: committed.State.Revision, IssueGeneration: committed.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)], AttemptGeneration: committed.State.AttemptGenerations[key]}
	_, effect, err = o.beginRuntimeEffect(ctx, beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectStart, Manifest: manifest, RequestDigest: strings.Repeat("2", 64)})
	if err != nil {
		return stateOwnerSnapshot{}, err
	}
	running := cloneManifest(target)
	running.State, running.Diagnostic = "running", ""
	committed, err = o.finishRuntimeEffect(ctx, finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectStart, Manifest: running})
	if err != nil || target.State == "running" {
		return committed, err
	}
	identity = stateResultIdentity{Epoch: committed.State.Epoch, SourceRevision: committed.State.Revision, IssueGeneration: committed.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)], AttemptGeneration: committed.State.AttemptGenerations[key]}
	action, reason := agentruntime.EffectMonitor, ""
	if target.State == "cancelled" {
		action, reason = agentruntime.EffectStop, target.Diagnostic
		if reason == "" {
			reason = "test cancellation"
			target.Diagnostic = reason
		}
	}
	_, effect, err = o.beginRuntimeEffect(ctx, beginRuntimeEffectCommand{Identity: identity, Action: action, Manifest: running, Reason: reason, RequestDigest: strings.Repeat("3", 64)})
	if err != nil {
		return stateOwnerSnapshot{}, err
	}
	return o.finishRuntimeEffect(ctx, finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: action, Manifest: target})
}

func advanceTestAttemptGeneration(t *testing.T, owner *stateOwner, manifest agentruntime.Manifest) stateOwnerSnapshot {
	t.Helper()
	snapshot := mustOwnerSnapshot(t, owner)
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	identity := stateResultIdentity{
		Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision,
		IssueGeneration:   snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)],
		AttemptGeneration: snapshot.State.AttemptGenerations[key],
	}
	committed, _, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "test generation advance", RequestDigest: strings.Repeat("4", 64)})
	if err != nil {
		t.Fatal(err)
	}
	return committed
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
