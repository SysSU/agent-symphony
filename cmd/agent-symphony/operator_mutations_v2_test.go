package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestOperatorDismissCommitsCompletedTombstoneReceiptWithoutCleanup(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 301, "completed", true)
	snapshot := mustOwnerSnapshot(t, owner)
	request := operatorRequest("dismiss-one", "dismiss", manifest, false)
	committed, effect, err := owner.beginOperatorMutation(t.Context(), operatorCommand(snapshot, request, manifest))
	if err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	tombstone := committed.State.Tombstones[key]
	receipt, ok := operatorReceiptByID(committed.State, request.RequestID)
	if effect != nil || !ok || receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || receipt.EffectID != "" || receipt.Result == nil || receipt.Result.OwnerRevision != committed.State.Revision {
		t.Fatalf("effect=%#v receipt=%#v revision=%d", effect, receipt, committed.State.Revision)
	}
	if tombstone.Action != "dismissed" || tombstone.CleanupPhase != "completed" || tombstone.Manifest == nil || !reflect.DeepEqual(*tombstone.Manifest, manifest) || len(committed.State.Effects) != 0 {
		t.Fatalf("tombstone=%#v effects=%#v", tombstone, committed.State.Effects)
	}
}

func TestOperatorSameSnapshotDismissAndCleanupRequestsConverge(t *testing.T) {
	t.Run("dismiss", func(t *testing.T) {
		owner, manifest := operatorTestOwner(t, 302, "completed", true)
		snapshot := mustOwnerSnapshot(t, owner)
		commands := []beginOperatorMutationCommand{
			operatorCommand(snapshot, operatorRequest("dismiss-a", "dismiss", manifest, false), manifest),
			operatorCommand(snapshot, operatorRequest("dismiss-b", "dismiss", manifest, false), manifest),
		}
		results := submitOperatorCommands(t, owner, commands)
		for _, err := range results {
			if err != nil {
				t.Fatal(err)
			}
		}
		state := mustOwnerSnapshot(t, owner).State
		if len(state.Tombstones) != 1 || len(state.Effects) != 0 || len(state.ControlReceipts) != 2 || state.AttemptGenerations[ownerAttemptKey("o/r", 302, 1)] != 2 {
			t.Fatalf("state=%#v", state)
		}
	})

	t.Run("archive cleanup", func(t *testing.T) {
		owner, manifest := operatorTestOwner(t, 303, "completed", false)
		snapshot := mustOwnerSnapshot(t, owner)
		digest := strings.Repeat("a", 64)
		first := operatorCommand(snapshot, operatorRequest("archive-a", "archive", manifest, true), manifest)
		first.CleanupDigest = digest
		second := operatorCommand(snapshot, operatorRequest("archive-b", "archive", manifest, true), manifest)
		second.CleanupDigest = digest
		results := submitOperatorCommands(t, owner, []beginOperatorMutationCommand{first, second})
		for _, err := range results {
			if err != nil {
				t.Fatal(err)
			}
		}
		state := mustOwnerSnapshot(t, owner).State
		if len(state.Tombstones) != 1 || len(state.Effects) != 1 || len(state.ControlReceipts) != 2 {
			t.Fatalf("state=%#v", state)
		}
		var effectID string
		for _, receipt := range state.ControlReceipts {
			if receipt.Phase != operatorPhaseCleanupPending || receipt.EffectID == "" {
				t.Fatalf("receipt=%#v", receipt)
			}
			if effectID == "" {
				effectID = receipt.EffectID
			} else if effectID != receipt.EffectID {
				t.Fatalf("receipts do not share one effect: %#v", state.ControlReceipts)
			}
		}
		conflict := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("archive-c", "archive", manifest, true), manifest)
		conflict.CleanupDigest = strings.Repeat("b", 64)
		if _, _, err := owner.beginOperatorMutation(t.Context(), conflict); !errors.Is(err, errStateConflict) {
			t.Fatalf("different cleanup digest err=%v", err)
		}
	})
}

func TestOperatorSameSnapshotCancelRequestsShareOneGenerationAndEffect(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 304, "active", false)
	snapshot := mustOwnerSnapshot(t, owner)
	digest, reason := strings.Repeat("c", 64), "operator cancelled attempt"
	makeCommand := func(id string) beginOperatorMutationCommand {
		command := operatorCommand(snapshot, operatorRequest(id, "cancel", manifest, false), manifest)
		command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: reason, RequestDigest: digest}
		return command
	}
	results := submitOperatorCommands(t, owner, []beginOperatorMutationCommand{makeCommand("cancel-a"), makeCommand("cancel-b")})
	for _, err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey("o/r", 304, 1)
	if state.AttemptGenerations[key] != 2 || len(state.Effects) != 1 || len(state.ControlReceipts) != 2 {
		t.Fatalf("state=%#v", state)
	}
	if state.ControlReceipts[0].EffectID == "" || state.ControlReceipts[0].EffectID != state.ControlReceipts[1].EffectID {
		t.Fatalf("receipts=%#v", state.ControlReceipts)
	}
}

func TestOperatorDifferentAttemptsAdmitFromOneSnapshot(t *testing.T) {
	root := resolvedTempDir(t)
	manifests := []agentruntime.Manifest{ownerTestManifest(t, root, 305, 1, "completed"), ownerTestManifest(t, root, 306, 1, "completed")}
	state := newRuntimeOwnerState("o/r")
	for _, manifest := range manifests {
		key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
		state.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)] = 1
		state.AttemptGenerations[key] = 1
		state.Attempts[key] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
		addOperatorObservation(&state, manifest, "completed", false)
	}
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	snapshot := mustOwnerSnapshot(t, owner)
	commands := make([]beginOperatorMutationCommand, len(manifests))
	for index, manifest := range manifests {
		commands[index] = operatorCommand(snapshot, operatorRequest("archive-"+string(rune('a'+index)), "archive", manifest, true), manifest)
		commands[index].CleanupDigest = strings.Repeat(string(rune('d'+index)), 64)
	}
	for _, err := range submitOperatorCommands(t, owner, commands) {
		if err != nil {
			t.Fatal(err)
		}
	}
	committed := mustOwnerSnapshot(t, owner).State
	if len(committed.Tombstones) != 2 || len(committed.Effects) != 2 || len(committed.ControlReceipts) != 2 {
		t.Fatalf("state=%#v", committed)
	}
}

func TestOperatorReceiptReplaySurvivesLaterRevisionAndRestart(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 307, 1, "completed")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "completed", true)
	state.Epoch, state.Revision = 1, 1
	var persisted runtimeOwnerState
	persist := func(state runtimeOwnerState) error { persisted = cloneRuntimeOwnerState(state); return nil }
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	refreshOperatorObservation(t, owner)
	snapshot := mustOwnerSnapshot(t, owner)
	request := operatorRequest("dismiss-restart", "dismiss", manifest, false)
	command := operatorCommand(snapshot, request, manifest)
	committed, _, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: manifest.Issue, ExpectedGeneration: committed.State.IssueGenerations[ownerIssueKey("o/r", manifest.Issue)]}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := owner.beginOperatorMutation(t.Context(), command); err != nil {
		t.Fatalf("later-revision replay: %v", err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, persisted, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	replayed, _, err := restarted.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatalf("restart replay: %v", err)
	}
	receipt, ok := operatorReceiptByID(replayed.State, request.RequestID)
	if !ok || receipt.Result == nil || receipt.Result.OwnerRevision == 0 {
		t.Fatalf("receipt=%#v", receipt)
	}
}

func TestOperatorCompletedReceiptRequiresCommittedOwnerRevision(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 308, "completed", true)
	command := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("dismiss-revision", "dismiss", manifest, false), manifest)
	committed, _, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateRuntimeOwnerState(committed.State, owner.attemptRoot, owner.stateRoot, true); err != nil {
		t.Fatal(err)
	}
	for _, revision := range []uint64{0, committed.State.Revision + 1} {
		corrupt := cloneRuntimeOwnerState(committed.State)
		corrupt.ControlReceipts[0].Result.OwnerRevision = revision
		if err := validateRuntimeOwnerState(corrupt, owner.attemptRoot, owner.stateRoot, true); err == nil {
			t.Fatalf("accepted completed receipt owner revision %d", revision)
		}
	}
}

func TestRuntimeOwnerRejectsInvalidTombstoneCleanupProof(t *testing.T) {
	t.Run("dismissed carries no cleanup", func(t *testing.T) {
		owner, manifest := operatorTestOwner(t, 318, "completed", true)
		command := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("dismiss-proof", "dismiss", manifest, false), manifest)
		committed, _, err := owner.beginOperatorMutation(t.Context(), command)
		if err != nil {
			t.Fatal(err)
		}
		key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
		corrupt := cloneRuntimeOwnerState(committed.State)
		policy := agentruntime.EffectCleanupPolicy{Action: "archive"}
		tombstone := corrupt.Tombstones[key]
		tombstone.CleanupPolicy = &policy
		corrupt.Tombstones[key] = tombstone
		if err := validateRuntimeOwnerState(corrupt, owner.attemptRoot, owner.stateRoot, true); err == nil {
			t.Fatal("dismissed tombstone with cleanup policy was accepted")
		}
	})

	t.Run("destructive completion retains policy and effect proof", func(t *testing.T) {
		owner, manifest := operatorTestOwner(t, 319, "completed", false)
		command := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("archive-proof", "archive", manifest, true), manifest)
		command.CleanupDigest = strings.Repeat("9", 64)
		committed, effect, err := owner.beginOperatorMutation(t.Context(), command)
		if err != nil {
			t.Fatal(err)
		}
		key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
		for _, mutate := range []func(*runtimeOwnerState){
			func(state *runtimeOwnerState) {
				tombstone := state.Tombstones[key]
				tombstone.CleanupPolicy = nil
				state.Tombstones[key] = tombstone
			},
			func(state *runtimeOwnerState) {
				tombstone := state.Tombstones[key]
				tombstone.CleanupPolicy.Action = "remove"
				state.Tombstones[key] = tombstone
			},
			func(state *runtimeOwnerState) {
				tombstone := state.Tombstones[key]
				tombstone.CleanupPhase, tombstone.EffectID = "completed", ""
				state.Tombstones[key] = tombstone
				completed := state.Effects[effect.ID]
				completed.State = "completed"
				state.Effects[effect.ID] = completed
			},
		} {
			corrupt := cloneRuntimeOwnerState(committed.State)
			mutate(&corrupt)
			if err := validateRuntimeOwnerState(corrupt, owner.attemptRoot, owner.stateRoot, true); err == nil {
				t.Fatalf("accepted corrupt tombstone: %#v", corrupt.Tombstones[key])
			}
		}
	})
}

func TestOperatorReceiptCapacityEvictsOnlyUnreferencedCompletedEffect(t *testing.T) {
	t.Run("stale generation effect", func(t *testing.T) {
		owner, manifest := operatorTestOwner(t, 309, "active", false)
		snapshot := mustOwnerSnapshot(t, owner)
		reason := "operator cancelled attempt"
		command := operatorCommand(snapshot, operatorRequest("cancel-capacity", "cancel", manifest, false), manifest)
		command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: reason, RequestDigest: strings.Repeat("e", 64)}
		_, effect, err := owner.beginOperatorMutation(t.Context(), command)
		if err != nil {
			t.Fatal(err)
		}
		cancelled := manifest
		cancelled.State, cancelled.Diagnostic = "cancelled", reason
		if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
			t.Fatal(err)
		}
		state := mustOwnerSnapshot(t, owner).State
		key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
		state.AttemptGenerations[key]++
		record := state.Attempts[key]
		record.Generation = state.AttemptGenerations[key]
		state.Attempts[key] = record
		fillCompletedLegacyReceipts(&state)
		if err := appendOperatorReceipt(&state, completedLegacyReceipt("capacity-new", state.Repository)); err != nil {
			t.Fatal(err)
		}
		if _, ok := state.Effects[effect.ID]; ok {
			t.Fatal("evicted receipt left an unreferenced stale completed effect")
		}
		if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(owner.stateRoot), owner.stateRoot, true); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("tombstone cleanup proof", func(t *testing.T) {
		owner, manifest := operatorTestOwner(t, 310, "completed", false)
		snapshot := mustOwnerSnapshot(t, owner)
		command := operatorCommand(snapshot, operatorRequest("archive-capacity", "archive", manifest, true), manifest)
		command.CleanupDigest = strings.Repeat("f", 64)
		_, effect, err := owner.beginOperatorMutation(t.Context(), command)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectCleanup, Manifest: manifest}}); err != nil {
			t.Fatal(err)
		}
		state := mustOwnerSnapshot(t, owner).State
		fillCompletedLegacyReceipts(&state)
		if err := appendOperatorReceipt(&state, completedLegacyReceipt("capacity-tombstone-new", state.Repository)); err != nil {
			t.Fatal(err)
		}
		if _, ok := state.Effects[effect.ID]; !ok {
			t.Fatal("receipt eviction deleted tombstone cleanup proof")
		}
		if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(owner.stateRoot), owner.stateRoot, true); err != nil {
			t.Fatal(err)
		}
	})
}

func TestOperatorBlockedRecoveryAdvancesThroughDurablePhases(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 311, "active", false)
	snapshot := mustOwnerSnapshot(t, owner)
	reason := "dashboard recovery: runtime liveness mismatch"
	request := operatorRequest("recover-phases", "recover", manifest, false)
	command := operatorCommand(snapshot, request, manifest)
	command.LivenessFailed = true
	command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: reason, RequestDigest: strings.Repeat("a", 64)}
	_, stop, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		status, _, _ := ownerOperatorStatus(snapshot.State, manifest.Issue, manifest.Attempt)
		t.Fatalf("status=%#v err=%v", status, err)
	}
	assertOperatorReceiptPhase(t, owner, request.RequestID, operatorPhaseStopPending, "pending")
	cancelled := manifest
	cancelled.State, cancelled.Diagnostic, cancelled.UpdatedAt = "cancelled", reason, time.Unix(50, 0).UTC()
	if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stop)), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
		t.Fatal(err)
	}
	assertOperatorReceiptPhase(t, owner, request.RequestID, operatorPhaseTerminalAwait, "pending")

	active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 311, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
	issue := issueFact(311, "recover")
	issue.Attempt, issue.CurrentAttempt, issue.Active, issue.ActiveAttempt = 1, 1, true, &active
	terminal := applyAndPlanRecover(t, owner, repositoryInput(true, issue), []internalgithub.RecoveryAttemptFact{active}, githubIssueTerminalFailure)
	terminalEffect := advanceOperatorRecoverPlan(t, owner, request.RequestID, terminal)
	assertOperatorReceiptPhase(t, owner, request.RequestID, operatorPhaseTerminal, "pending")
	terminalResult := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueTerminalFailure, Observed: true}}
	if _, err := owner.finishOperatorReconciliationEffect(t.Context(), finishOperatorReconciliationEffectCommand{Finish: finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*terminalEffect), Result: terminalResult}}); err != nil {
		t.Fatal(err)
	}
	assertOperatorReceiptPhase(t, owner, request.RequestID, operatorPhaseRetryAwait, "pending")

	failed := active
	failed.State = "failed"
	terminalIssue := issueFact(311, "recover")
	terminalIssue.Attempt, terminalIssue.CurrentAttempt, terminalIssue.RecoveryAuthorized = 1, 1, true
	terminalIssue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
	retry := applyAndPlanRecover(t, owner, repositoryInput(true, terminalIssue), []internalgithub.RecoveryAttemptFact{failed}, githubIssueRetry)
	retryEffect := advanceOperatorRecoverPlan(t, owner, request.RequestID, retry)
	assertOperatorReceiptPhase(t, owner, request.RequestID, operatorPhaseRetryPending, "pending")
	retryResult := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueRetry, Observed: true}}
	completed, err := owner.finishOperatorReconciliationEffect(t.Context(), finishOperatorReconciliationEffectCommand{Finish: finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*retryEffect), Result: retryResult}})
	if err != nil {
		t.Fatal(err)
	}
	receipt, _ := operatorReceiptByID(completed.State, request.RequestID)
	if receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || receipt.Result == nil || receipt.Result.OwnerRevision != completed.State.Revision {
		t.Fatalf("receipt=%#v revision=%d", receipt, completed.State.Revision)
	}
}

func TestOperatorCanceledBeforeSubmitDoesNotCommit(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 312, "completed", true)
	before := mustOwnerSnapshot(t, owner)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := owner.beginOperatorMutation(ctx, operatorCommand(before, operatorRequest("dismiss-cancelled", "dismiss", manifest, false), manifest)); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	after := mustOwnerSnapshot(t, owner)
	if after.State.Revision != before.State.Revision || len(after.State.ControlReceipts) != 0 || len(after.State.Tombstones) != 0 {
		t.Fatalf("canceled command committed: %#v", after.State)
	}
}

func TestOperatorAdmissionRejectsStaleSameIssueObservationProof(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 313, "completed", true)
	stale := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("dismiss-stale-proof", "dismiss", manifest, false), manifest)
	terminal := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 313, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "completed", Checks: []string{}}
	issue := issueFact(313, "reopened")
	issue.Attempt, issue.CurrentAttempt, issue.Completed, issue.Closed = 1, 1, true, false
	issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{terminal}
	applyReconciliationInput(t, owner, reconciliationInput{Scope: issueScope(313), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{terminal}})
	if _, _, err := owner.beginOperatorMutation(t.Context(), stale); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("stale observation proof err=%v", err)
	}
}

func TestOperatorAdmissionAndStatusRejectPreviousEpochObservation(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 315, "completed", true)
	persisted := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	snapshot := mustOwnerSnapshot(t, restarted)
	command := operatorCommand(snapshot, operatorRequest("dismiss-before-recollect", "dismiss", manifest, false), manifest)
	if _, _, err := restarted.beginOperatorMutation(t.Context(), command); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("previous-epoch admission err=%v", err)
	}
	status, err := projectOwnerStatus(snapshot, 1, time.Unix(1, 0))
	if err != nil || len(status.Statuses) != 1 || status.Statuses[0].State != "orphaned" || status.Statuses[0].DispatchAuthorized {
		t.Fatalf("status=%#v err=%v", status, err)
	}
}

func TestOwnerProjectionAndAdmissionRejectRemoteFailedLocalCompletedRecovery(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 314, 1, "completed")
	state := runtimeEffectInitialState(manifest)
	failed := reconciliationAttemptFact{Repository: "o/r", Issue: 314, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}}
	issueKey, attemptKey := ownerIssueKey("o/r", 314), ownerAttemptKey("o/r", 314, 1)
	state.Observations[issueKey] = reconciliationObservation{Present: true, Generation: 1, OwnerGeneration: 1, ObservationEpoch: 1, LastCycleID: 1, InputDigest: digestText("observation"), Fact: reconciliationIssueFact{Repository: "o/r", Issue: 314, Attempt: 1, CurrentAttempt: 1, BodyDigest: digestText("body"), RecoveryAuthorized: true, Dependencies: []int{}, SatisfiedDependencies: []int{}, Paths: []string{}, Blockers: []string{}, TerminalAttempts: []reconciliationAttemptFact{failed}}, Attempts: map[string]reconciliationAttemptObservation{attemptKey: {Present: true, Generation: 1, OwnerGeneration: 1, SourceIssueGeneration: 1, ObservationEpoch: 1, LastCycleID: 1, Fact: failed}}}
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	snapshot := mustOwnerSnapshot(t, owner)
	projected, err := projectOwnerStatus(snapshot, 1, time.Unix(1, 0))
	if err != nil || len(projected.Statuses) != 1 || projected.Statuses[0].Retryable || projected.Statuses[0].Action != "inspect inconsistent completed local attempt before recovery" {
		t.Fatalf("projected=%#v err=%v", projected, err)
	}
	command := operatorCommand(snapshot, operatorRequest("recover-completed", "recover", manifest, false), manifest)
	if _, _, err := owner.beginOperatorMutation(t.Context(), command); !errors.Is(err, errStateConflict) {
		t.Fatalf("completed local recovery err=%v", err)
	}
}

func TestOperatorCancelSupersedesPendingPlanReview(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	owner, snapshot, review := reconciliationEffectTestOwner(t, test.request)
	manifest := *review.Manifest
	reviewRequest := operatorRequest("review-before-cancel", "review-plan", manifest, false)
	reviewCommand := operatorCommand(snapshot, reviewRequest, manifest)
	reviewCommand.Reconciliation = &beginReconciliationEffectCommand{Identity: reviewCommand.Identity, Request: review}
	_, reviewEffect, err := owner.beginOperatorMutation(t.Context(), reviewCommand)
	if err != nil {
		t.Fatal(err)
	}

	cancelSnapshot := mustOwnerSnapshot(t, owner)
	cancelRequest := operatorRequest("cancel-review", "cancel", manifest, false)
	cancelCommand := operatorCommand(cancelSnapshot, cancelRequest, manifest)
	cancelCommand.Runtime = &beginRuntimeEffectCommand{Identity: cancelCommand.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "operator cancelled attempt", RequestDigest: strings.Repeat("b", 64)}
	committed, stop, err := owner.beginOperatorMutation(t.Context(), cancelCommand)
	if err != nil {
		t.Fatal(err)
	}
	old, _ := operatorReceiptByID(committed.State, reviewRequest.RequestID)
	if old.State != "completed" || old.Phase != operatorPhaseCompleted || old.EffectID != "" || old.Result == nil || old.Result.Status != http.StatusConflict || old.Result.OwnerRevision != committed.State.Revision {
		t.Fatalf("superseded receipt=%#v", old)
	}
	if _, exists := committed.State.Effects[reviewEffect.ID]; exists || stop == nil || committed.State.Effects[stop.ID].State != "pending" {
		t.Fatalf("effects=%#v stop=%#v", committed.State.Effects, stop)
	}
	if _, effect, err := owner.beginOperatorMutation(t.Context(), reviewCommand); err != nil || effect != nil {
		t.Fatalf("superseded replay effect=%#v err=%v", effect, err)
	}
}

func applyAndPlanRecover(t *testing.T, owner *stateOwner, input reconciliationInput, attempts []internalgithub.RecoveryAttemptFact, kind githubIssueUpdateKind) reconciliationPlannedEffect {
	t.Helper()
	input.Attempts = attempts
	snapshot := applyReconciliationInput(t, owner, input)
	plans, err := planReconciliationAttemptIssueUpdates(snapshot, reconciliationV2Batch{Input: input}, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42, RetryCommand: "/agent-symphony retry"})
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		if plan.Request.GitHubIssueUpdate != nil && plan.Request.GitHubIssueUpdate.Kind == kind {
			return plan
		}
	}
	t.Fatalf("missing %s plan: %#v", kind, plans)
	return reconciliationPlannedEffect{}
}

func advanceOperatorRecoverPlan(t *testing.T, owner *stateOwner, requestID string, plan reconciliationPlannedEffect) *runtimeEffectIntent {
	t.Helper()
	_, effect, err := owner.advanceOperatorRecovery(t.Context(), advanceOperatorRecoveryCommand{RequestID: requestID, Identity: plan.Identity, Reconciliation: beginReconciliationEffectCommand{Identity: plan.Identity, Request: plan.Request}})
	if err != nil {
		t.Fatal(err)
	}
	return effect
}

func assertOperatorReceiptPhase(t *testing.T, owner *stateOwner, requestID, phase, state string) {
	t.Helper()
	receipt, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, requestID)
	if !ok || receipt.Phase != phase || receipt.State != state {
		t.Fatalf("receipt=%#v", receipt)
	}
}

func fillCompletedLegacyReceipts(state *runtimeOwnerState) {
	for len(state.ControlReceipts) < maxControlReceipts {
		state.ControlReceipts = append(state.ControlReceipts, completedLegacyReceipt(fmt.Sprintf("legacy-%03d", len(state.ControlReceipts)), state.Repository))
	}
}

func completedLegacyReceipt(id, repository string) controlReceipt {
	request := controlRequest{Version: controlVersion, RequestID: id, Repository: repository, Action: "reconcile"}
	return controlReceipt{Request: request, State: "completed", Result: successfulOperatorResult(request, 0)}
}

func operatorTestOwner(t *testing.T, issue int, status string, closed bool) (*stateOwner, agentruntime.Manifest) {
	t.Helper()
	root := resolvedTempDir(t)
	manifestState := "running"
	if status == "completed" {
		manifestState = "completed"
	}
	manifest := ownerTestManifest(t, root, issue, 1, manifestState)
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, status, closed)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	return owner, manifest
}

func refreshOperatorObservation(t *testing.T, owner *stateOwner) {
	t.Helper()
	snapshot := mustOwnerSnapshot(t, owner)
	input := reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: snapshot.State.Repository}, Complete: true}
	for _, observation := range snapshot.State.Observations {
		if !observation.Present {
			continue
		}
		input.Issues = append(input.Issues, expandIssueFact(observation.Fact))
		for _, attempt := range observation.Attempts {
			if attempt.Present {
				input.Attempts = append(input.Attempts, expandAttemptFact(attempt.Fact))
			}
		}
	}
	applyReconciliationInput(t, owner, input)
}

func addOperatorObservation(state *runtimeOwnerState, manifest agentruntime.Manifest, status string, closed bool) {
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	fact := reconciliationAttemptFact{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, BaseSHA: manifest.BaseSHA, State: status, Checks: []string{}}
	issue := reconciliationIssueFact{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, CurrentAttempt: manifest.Attempt, BodyDigest: digestText("body"), Closed: closed, Dependencies: []int{}, SatisfiedDependencies: []int{}, Paths: []string{}, Blockers: []string{}, TerminalAttempts: []reconciliationAttemptFact{}}
	if status == "active" {
		issue.Active, issue.DispatchAuthorized, issue.RecoveryAuthorized = true, true, true
		issue.ActiveAttempt = &fact
	} else {
		issue.Completed = status == "completed"
		issue.TerminalAttempts = []reconciliationAttemptFact{fact}
	}
	state.Observations[issueKey] = reconciliationObservation{Present: true, Generation: 1, OwnerGeneration: state.IssueGenerations[issueKey], ObservationEpoch: 1, LastCycleID: 1, InputDigest: digestText("observation"), Fact: issue, IssueUpdates: []reconciliationIssueUpdateProposal{}, Attempts: map[string]reconciliationAttemptObservation{
		attemptKey: {Present: true, Generation: 1, OwnerGeneration: state.AttemptGenerations[attemptKey], SourceIssueGeneration: 1, ObservationEpoch: 1, LastCycleID: 1, Fact: fact},
	}}
}

func operatorRequest(id, action string, manifest agentruntime.Manifest, confirm bool) controlRequest {
	return controlRequest{Version: controlVersion, RequestID: id, Repository: manifest.Repository, Action: action, Issue: manifest.Issue, Attempt: manifest.Attempt, Confirm: confirm}
}

func operatorCommand(snapshot stateOwnerSnapshot, request controlRequest, manifest agentruntime.Manifest) beginOperatorMutationCommand {
	observation := snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
	command := beginOperatorMutationCommand{Request: request, Manifest: manifest, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest, IssueClosed: request.Action == "dismiss", CleanupValid: slices.Contains([]string{"archive", "abandon", "remove"}, request.Action), Identity: stateResultIdentity{
		Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision,
		IssueGeneration:   snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)],
		AttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)],
	}}
	if command.CleanupValid {
		command.CleanupPolicy.Action = request.Action
	}
	return command
}

func submitOperatorCommands(t *testing.T, owner *stateOwner, commands []beginOperatorMutationCommand) []error {
	t.Helper()
	start := make(chan struct{})
	errs := make([]error, len(commands))
	var wait sync.WaitGroup
	for index := range commands {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, _, errs[index] = owner.beginOperatorMutation(t.Context(), commands[index])
		}()
	}
	close(start)
	wait.Wait()
	return errs
}
