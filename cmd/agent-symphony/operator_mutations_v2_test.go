package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestOperatorDismissCommitsReviewerOnlyCleanupIntent(t *testing.T) {
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
	if effect == nil || !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseCleanupPending || receipt.EffectID != effect.ID || receipt.Result != nil {
		t.Fatalf("effect=%#v receipt=%#v revision=%d", effect, receipt, committed.State.Revision)
	}
	if tombstone.Action != "dismissed" || tombstone.CleanupPhase != "pending" || tombstone.Manifest == nil || !reflect.DeepEqual(*tombstone.Manifest, manifest) || tombstone.EffectID != effect.ID || tombstone.CleanupPolicy == nil || tombstone.CleanupPolicy.Action != "dismiss" {
		t.Fatalf("tombstone=%#v effects=%#v", tombstone, committed.State.Effects)
	}
}

func TestOperatorWithoutTombstoneStillRejectsMissingObservation(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 371, "completed", false)
	before := mustOwnerSnapshot(t, owner)
	command := operatorCommand(before, operatorRequest("archive-stale-observation", "archive", manifest, true), manifest)
	applyReconciliationInput(t, owner, reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: manifest.Repository}, Complete: true})
	if _, _, err := owner.beginOperatorMutation(t.Context(), command); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("missing observation without tombstone err=%v", err)
	}
	state := mustOwnerSnapshot(t, owner).State
	if _, exists := state.Tombstones[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)]; exists {
		t.Fatal("stale request created tombstone")
	}
}

func TestDismissPendingStartKeepsPhysicalCleanupPendingAcrossRestart(t *testing.T) {
	owner, manifest := operatorOwnerWithPendingStart(t, 397, "completed", true)
	request := operatorRequest("dismiss-start-candidate", "dismiss", manifest, false)
	committed, effect, err := owner.beginOperatorMutation(t.Context(), operatorCommand(mustOwnerSnapshot(t, owner), request, manifest))
	if err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	receipt, ok := operatorReceiptByID(committed.State, request.RequestID)
	if effect == nil || !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseStartCleanup || receipt.EffectID != effect.ID || committed.State.Tombstones[key].InvalidatedStart == nil {
		t.Fatalf("effect=%#v receipt=%#v tombstone=%#v", effect, receipt, committed.State.Tombstones[key])
	}
	if _, exists := committed.State.Attempts[key]; exists {
		t.Fatal("dismissed attempt remained live")
	}
	if got := operatorResultForReceipt(committed, receipt); got.Status != http.StatusAccepted || !got.OK {
		t.Fatalf("dismiss reported physical cleanup complete: %#v", got)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := loaded.Attempts[key]; exists || loaded.Tombstones[key].InvalidatedStart == nil {
		t.Fatalf("restart resurrected dismissed attempt or lost candidate: %#v", loaded)
	}
}

func TestDismissConfinedPendingStartSettlesAndRemainsRevokedAfterRestart(t *testing.T) {
	owner, manifest := operatorOwnerWithConfinedPendingStart(t, 329, "completed", true)
	request := operatorRequest("dismiss-confined-start", "dismiss", manifest, false)
	committed, effect, err := owner.beginOperatorMutation(t.Context(), operatorCommand(mustOwnerSnapshot(t, owner), request, manifest))
	if err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	receipt, ok := operatorReceiptByID(committed.State, request.RequestID)
	if effect == nil || !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseCleanupPending || receipt.EffectID != effect.ID || committed.State.Tombstones[key].InvalidatedStart != nil {
		t.Fatalf("confined dismissal did not settle: effect=%#v receipt=%#v tombstone=%#v", effect, receipt, committed.State.Tombstones[key])
	}
	if implementationLeaseBlocksGitHub(committed.State, reconciliationGitHubIssueUpdate, manifest.Repository, manifest.Issue) {
		t.Fatal("revoked confined Start retained GitHub authority")
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := loaded.Attempts[key]; exists || loaded.Tombstones[key].InvalidatedStart != nil || implementationLeaseBlocksGitHub(loaded, reconciliationGitHubIssueUpdate, manifest.Repository, manifest.Issue) {
		t.Fatalf("restart restored confined worker authority: %#v", loaded)
	}
}

func TestAbandonAndRemovePendingStartNeverRunPhysicalCleanupWithoutProof(t *testing.T) {
	for _, action := range []string{"abandon", "remove"} {
		t.Run(action, func(t *testing.T) {
			owner, manifest := operatorOwnerWithPendingStart(t, 398, "completed", true)
			snapshot := mustOwnerSnapshot(t, owner)
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			published := ""
			if action == "remove" {
				published = manifest.BaseSHA
			}
			policy := agentruntime.EffectCleanupPolicy{Action: action, PublishedHead: published}
			committed, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, ExpectedIssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)], ExpectedAttemptGeneration: snapshot.State.AttemptGenerations[key], Action: operatorTombstoneAction(action), CleanupPhase: "pending", PublishedHead: published, Manifest: &manifest, CleanupPolicy: &policy, EffectAction: string(agentruntime.EffectCleanup), EffectRequestDigest: strings.Repeat("f", 64)})
			if err != nil || effect == nil || committed.State.Tombstones[key].InvalidatedStart == nil {
				t.Fatalf("invalidation effect=%#v err=%v tombstone=%#v", effect, err, committed.State.Tombstones[key])
			}
			boundary := &forbiddenReviewerBoundary{}
			cleanup := operatorCleanupExecutor{owner: owner, reviewer: boundary, implementation: boundary, runtime: &agentruntime.Runtime{}}
			request := agentruntime.EffectRequest{Action: agentruntime.EffectCleanup, Manifest: manifest, Identity: effectRequestIdentity(*effect), Cleanup: policy}
			if err := cleanup.execute(t.Context(), request); !errors.Is(err, agentruntime.ErrRuntimeResourcesRemain) || boundary.calls != 0 {
				t.Fatalf("cleanup improperly ran: err=%v calls=%d", err, boundary.calls)
			}
			if complete, err := cleanup.verify(t.Context(), request); err != nil || complete {
				t.Fatalf("cleanup improperly verified: complete=%t err=%v", complete, err)
			}
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			loaded, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
			if err != nil {
				t.Fatal(err)
			}
			if loaded.Tombstones[key].InvalidatedStart == nil || loaded.Tombstones[key].CleanupPhase == "completed" {
				t.Fatalf("restart lost pending candidate cleanup: %#v", loaded.Tombstones[key])
			}
		})
	}
}

func TestStopPendingStartCannotClaimCancelledWithoutCandidateProof(t *testing.T) {
	owner, manifest := operatorOwnerWithPendingStart(t, 399, "active", false)
	snapshot := mustOwnerSnapshot(t, owner)
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	_, stop, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)], AttemptGeneration: snapshot.State.AttemptGenerations[key]}, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "operator cancelled attempt", RequestDigest: strings.Repeat("c", 64)})
	if err != nil || stop == nil || stop.InvalidatedStart == nil {
		t.Fatalf("Stop lost Start candidate: effect=%#v err=%v", stop, err)
	}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: &agentruntime.Runtime{}})
	if err != nil {
		t.Fatal(err)
	}
	result, err := coordinator.executeOperator(agentruntime.EffectRequest{Action: agentruntime.EffectStop, Manifest: manifest, Identity: effectRequestIdentity(*stop)})
	if !errors.Is(err, agentruntime.ErrRuntimeResourcesRemain) || result.Disposition != agentruntime.EffectResultAmbiguous {
		t.Fatalf("Stop claimed terminal result before physical proof: result=%#v err=%v", result, err)
	}
	current := mustOwnerSnapshot(t, owner).State
	if current.Effects[stop.ID].State != "pending" || current.Attempts[key].Manifest.State != manifest.State {
		t.Fatalf("Stop claimed cancelled: effect=%#v attempt=%#v", current.Effects[stop.ID], current.Attempts[key])
	}
}

func operatorOwnerWithPendingStart(t *testing.T, issue int, status string, closed bool) (*stateOwner, agentruntime.Manifest) {
	t.Helper()
	base, manifest := operatorTestOwner(t, issue, status, closed, true)
	state := cloneRuntimeOwnerState(mustOwnerSnapshot(t, base).State)
	if err := base.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	effect := runtimeEffectIntent{Action: string(agentruntime.EffectStart), Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, IssueGeneration: state.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)], AttemptGeneration: state.AttemptGenerations[key], IntentEpoch: state.Epoch, IntentRevision: state.Revision, State: "pending", RequestDigest: strings.Repeat("e", 64), StartGateNonce: strings.Repeat("d", 32), StartMayRun: true, StartCandidates: []startGateCandidate{{Nonce: strings.Repeat("d", 32), MayRun: true}}}
	effect.ID = runtimeEffectID(effect)
	state.Effects[effect.ID] = effect
	root := base.stateRoot
	owner, err := startTestStateOwner(t, root, state, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, productionAttemptRoot(root), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	return owner, manifest
}

func operatorOwnerWithConfinedPendingStart(t *testing.T, issue int, status string, closed bool) (*stateOwner, agentruntime.Manifest) {
	t.Helper()
	owner, manifest := operatorOwnerWithPendingStart(t, issue, status, closed)
	state := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	generation := state.AttemptGenerations[key]
	manifest.WorkerGeneration, manifest.WorkerProfileDigest = generation, config.WorkerProfileDigest()
	record := state.Attempts[key]
	record.Manifest = manifest
	state.Attempts[key] = record
	restarted, err := startTestStateOwner(t, owner.stateRoot, state, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, productionAttemptRoot(owner.stateRoot), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	refreshOperatorObservation(t, restarted)
	return restarted, manifest
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
		if len(state.Tombstones) != 1 || len(state.Effects) != 1 || len(state.ControlReceipts) != 2 || state.AttemptGenerations[ownerAttemptKey("o/r", 302, 1)] != 2 {
			t.Fatalf("state=%#v", state)
		}
		for _, receipt := range state.ControlReceipts {
			if receipt.State != "pending" || receipt.Phase != operatorPhaseCleanupPending || receipt.EffectID == "" || receipt.EffectID != state.Tombstones[ownerAttemptKey("o/r", 302, 1)].EffectID {
				t.Fatalf("concurrent Dismiss did not converge on one cleanup effect: %#v", state.ControlReceipts)
			}
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
	if !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseCleanupPending || receipt.EffectID == "" || receipt.Result != nil {
		t.Fatalf("receipt=%#v", receipt)
	}
}

func TestOperatorCompletedReceiptRequiresCommittedOwnerRevision(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 308, "completed", true)
	command := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("dismiss-revision", "dismiss", manifest, false), manifest)
	committed, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if effect == nil {
		t.Fatal("Dismiss did not admit reviewer-only cleanup effect")
	}
	committed, err = owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectCleanup, Manifest: manifest}})
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
	activeMarker, err := internalgithub.ActiveAttemptMarker("o/r", manifest.Issue, manifest.Attempt, manifest.BaseSHA)
	if err != nil {
		t.Fatal(err)
	}
	terminalMarker, err := internalgithub.TerminalFailureMarker(manifest.Issue, manifest.Attempt, cancelled.UpdatedAt)
	if err != nil {
		t.Fatal(err)
	}
	comment := func(id int, body string, created time.Time) map[string]any {
		return map[string]any{"id": id, "body": body, "created_at": created, "updated_at": created, "user": map[string]any{"id": 42}}
	}
	comments := []map[string]any{
		comment(1, activeMarker, time.Unix(1, 0).UTC()),
		comment(2, terminalMarker, cancelled.UpdatedAt),
		comment(3, "/agent-symphony retry", cancelled.UpdatedAt.Add(time.Second)),
	}
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != fmt.Sprintf("/repos/o/r/issues/%d/comments", manifest.Issue) {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(comments)
	}))
	t.Cleanup(github.Close)
	applied, err := internalgithub.RetryCommandApplied(t.Context(), internalgithub.API{BaseURL: github.URL, HTTP: github.Client(), Retries: -1}, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42, CancelCommand: "/agent-symphony cancel", RetryCommand: "/agent-symphony retry"}, manifest.Issue, manifest.Attempt, cancelled.UpdatedAt)
	if err != nil || !applied {
		t.Fatalf("exact retry proof applied=%v err=%v", applied, err)
	}
	retryResult := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueRetry, Observed: true}}
	// Reconciliation can observe the proven command before the owner commits it.
	proofReady, releaseFinish := make(chan struct{}), make(chan struct{})
	type finishResult struct {
		snapshot stateOwnerSnapshot
		err      error
	}
	finished := make(chan finishResult, 1)
	go func() {
		close(proofReady)
		<-releaseFinish
		completed, err := owner.finishOperatorReconciliationEffect(t.Context(), finishOperatorReconciliationEffectCommand{Finish: finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*retryEffect), Result: retryResult}})
		finished <- finishResult{completed, err}
	}()
	<-proofReady
	provisional := terminalIssue
	provisional.RecoveryAuthorized = false
	provisional.Blockers = []string{"control snapshot update is pending"}
	applyReconciliationInput(t, owner, repositoryInput(true, provisional))
	settled := terminalIssue
	settled.Attempt, settled.Retry = 2, true
	applyReconciliationInput(t, owner, repositoryInput(true, settled))
	close(releaseFinish)
	finish := <-finished
	completed, err := finish.snapshot, finish.err
	if err != nil {
		t.Fatal(err)
	}
	receipt, _ := operatorReceiptByID(completed.State, request.RequestID)
	if receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || receipt.Result == nil || receipt.Result.OwnerRevision != completed.State.Revision {
		t.Fatalf("receipt=%#v revision=%d", receipt, completed.State.Revision)
	}
}

func TestVerifiedStopDurablyRevokesExactWorkerAuthorityForGitHubProgress(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 339, 1, "running")
	manifest, _ = boundRuntimeEffectTestManifest(t, manifest)
	manifest.WorkerGeneration, manifest.WorkerProfileDigest = 1, config.WorkerProfileDigest()
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	persist := func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
	}
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	current := mustOwnerSnapshot(t, owner)
	_, stop, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{
		Identity: stateResultIdentity{
			Epoch: current.State.Epoch, SourceRevision: current.State.Revision,
			IssueGeneration:   current.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)],
			AttemptGeneration: current.State.AttemptGenerations[key],
		},
		Action: agentruntime.EffectStop, Manifest: manifest, Reason: "operator cancelled attempt", RequestDigest: strings.Repeat("c", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	cancelled := manifest
	cancelled.State, cancelled.Diagnostic = "cancelled", stop.Reason
	if _, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stop)), Action: agentruntime.EffectStop, Manifest: cancelled}); err != nil {
		t.Fatal(err)
	}
	committed := mustOwnerSnapshot(t, owner)
	record := committed.State.Attempts[key]
	if !revokedWorkerCredentialCurrent(record) || record.Manifest.LaunchID != manifest.LaunchID || record.Manifest.WorkerGeneration != manifest.WorkerGeneration || implementationLeaseBlocksGitHub(committed.State, reconciliationGitHubIssueUpdate, manifest.Repository, manifest.Issue) {
		t.Fatalf("verified Stop did not preserve and revoke the exact worker authority: %#v", record)
	}

	active := internalgithub.RecoveryAttemptFact{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
	issue := issueFact(manifest.Issue, "cancelled")
	issue.Attempt, issue.CurrentAttempt, issue.Active, issue.ActiveAttempt = manifest.Attempt, manifest.Attempt, true, &active
	plan := applyAndPlanRecover(t, owner, repositoryInput(true, issue), []internalgithub.RecoveryAttemptFact{active}, githubIssueTerminalFailure)
	if _, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plan.Identity, Request: plan.Request}); err != nil || effect == nil {
		t.Fatalf("terminal failure was not admitted after verified Stop: effect=%#v err=%v", effect, err)
	}
	if _, retained := mustOwnerSnapshot(t, owner).State.Effects[stop.ID]; retained {
		t.Fatal("terminal admission retained the completed Stop effect instead of relying on the durable owner credential")
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, manifest.Repository)
	if err != nil || !revokedWorkerCredentialCurrent(loaded.Attempts[key]) || implementationLeaseBlocksGitHub(loaded, reconciliationGitHubIssueUpdate, manifest.Repository, manifest.Issue) {
		t.Fatalf("restart lost exact worker revocation: record=%#v err=%v", loaded.Attempts[key], err)
	}
	for _, action := range []string{"dismiss", "archive", "abandon", "remove"} {
		t.Run(action, func(t *testing.T) {
			candidate := cloneRuntimeOwnerState(committed.State)
			destructive, err := startTestStateOwner(t, root, candidate, persist)
			if err != nil {
				t.Fatal(err)
			}
			current := mustOwnerSnapshot(t, destructive)
			currentManifest := current.State.Attempts[key].Manifest
			published := ""
			if action == "remove" {
				published = currentManifest.BaseSHA
			}
			policy := agentruntime.EffectCleanupPolicy{Action: action, PublishedHead: published}
			committed, _, err := destructive.invalidateAttempt(t.Context(), invalidateAttemptCommand{
				Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt,
				ExpectedIssueGeneration:   current.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)],
				ExpectedAttemptGeneration: current.State.AttemptGenerations[key], Action: operatorTombstoneAction(action), CleanupPhase: "pending",
				PublishedHead: published, Manifest: &currentManifest, CleanupPolicy: &policy, EffectAction: string(agentruntime.EffectCleanup), EffectRequestDigest: strings.Repeat("f", 64),
			})
			if err != nil {
				t.Fatal(err)
			}
			tombstone := committed.State.Tombstones[key]
			if !revokedWorkerTombstoneCredentialCurrent(tombstone) || implementationLeaseBlocksGitHub(committed.State, reconciliationGitHubIssueUpdate, manifest.Repository, manifest.Issue) {
				t.Fatalf("%s lost exact worker revocation: %#v", action, tombstone)
			}
			if err := destructive.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			restarted, err := readRuntimeOwnerState(root, manifest.Repository)
			if err != nil || !revokedWorkerTombstoneCredentialCurrent(restarted.Tombstones[key]) || implementationLeaseBlocksGitHub(restarted, reconciliationGitHubIssueUpdate, manifest.Repository, manifest.Issue) {
				t.Fatalf("%s restart lost exact worker revocation: tombstone=%#v err=%v", action, restarted.Tombstones[key], err)
			}
		})
	}

	legacy := runtimeEffectInitialState(cancelled)
	legacy.AttemptGenerations[key] = 2
	legacy.Attempts[key] = runtimeAttemptRecord{Generation: 2, Manifest: cancelled}
	if !implementationLeaseBlocksGitHub(legacy, reconciliationGitHubIssueUpdate, manifest.Repository, manifest.Issue) {
		t.Fatal("cancelled V2 manifest without an owner revocation credential gained GitHub authority")
	}
	for name, mutate := range map[string]func(*runtimeAttemptRecord){
		"generation": func(record *runtimeAttemptRecord) { record.Generation++ },
		"launch":     func(record *runtimeAttemptRecord) { record.Manifest.LaunchID = strings.Repeat("d", 32) },
		"profile":    func(record *runtimeAttemptRecord) { record.Manifest.WorkerProfileDigest = strings.Repeat("e", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			tampered := record
			mutate(&tampered)
			if revokedWorkerCredentialCurrent(tampered) {
				t.Fatalf("tampered worker authority retained revocation: %#v", tampered)
			}
		})
	}
	t.Run("noncurrent profile cannot mint", func(t *testing.T) {
		candidate := cloneRuntimeOwnerState(state)
		unconfined := candidate.Attempts[key]
		unconfined.Manifest.WorkerProfileDigest = strings.Repeat("e", 64)
		candidate.Attempts[key] = unconfined
		unconfinedOwner, err := startTestStateOwner(t, root, candidate, func(runtimeOwnerState) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = unconfinedOwner.close(context.Background()) }()
		current := mustOwnerSnapshot(t, unconfinedOwner)
		_, stop, err := unconfinedOwner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{
			Identity: stateResultIdentity{
				Epoch: current.State.Epoch, SourceRevision: current.State.Revision,
				IssueGeneration:   current.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)],
				AttemptGeneration: current.State.AttemptGenerations[key],
			},
			Action: agentruntime.EffectStop, Manifest: unconfined.Manifest, Reason: "operator cancelled attempt", RequestDigest: strings.Repeat("d", 64),
		})
		if err != nil {
			t.Fatal(err)
		}
		result := unconfined.Manifest
		result.State, result.Diagnostic = "cancelled", stop.Reason
		if _, err := unconfinedOwner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stop)), Action: agentruntime.EffectStop, Manifest: result}); !errors.Is(err, errStateConflict) {
			t.Fatalf("Stop minted revocation for a noncurrent worker profile: %v", err)
		}
	})
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
	if err != nil || len(status.Statuses) != 1 || status.Statuses[0].State != "orphaned" || status.Statuses[0].DispatchAuthorized || !status.Statuses[0].OperatorBlocked {
		t.Fatalf("status=%#v err=%v", status, err)
	}
}

func TestRestartedLocalOrphansHideDestructiveControlsUntilCurrentObservation(t *testing.T) {
	for _, localState := range []string{"running", "failed"} {
		t.Run(localState, func(t *testing.T) {
			owner, manifest := operatorTestOwner(t, 316, "orphaned", false)
			persisted := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			record := persisted.Attempts[key]
			record.Manifest.State = localState
			persisted.Attempts[key] = record
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.close(context.Background()) })
			status, err := projectOwnerStatus(mustOwnerSnapshot(t, restarted), 1, time.Unix(1, 0))
			if err != nil || len(status.Statuses) != 1 || !status.Statuses[0].OperatorBlocked {
				t.Fatalf("status=%#v err=%v", status, err)
			}
		})
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
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewEffect)}); err != nil {
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
	if !stop.SupersededReviewerGateProtocol || !stop.SupersededReviewerSessionRequested || stop.SupersededReviewerRequestDigest != reviewEffect.RequestDigest || stop.SupersededReviewerID != reviewEffect.ID || stop.SupersededReviewerIssueGeneration != reviewEffect.IssueGeneration || stop.SupersededReviewerAttemptGeneration != reviewEffect.AttemptGeneration {
		t.Fatalf("Cancel lost pre-deletion reviewer lifecycle identity: reviewer=%#v stop=%#v", reviewEffect, stop)
	}
	if _, effect, err := owner.beginOperatorMutation(t.Context(), reviewCommand); err != nil || effect != nil {
		t.Fatalf("superseded replay effect=%#v err=%v", effect, err)
	}
}

func TestCancelBindsObservedReviewerGroupBeforeStopAndSurvivesRestart(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	owner, snapshot, review := reconciliationEffectTestOwner(t, test.request)
	manifest := *review.Manifest
	reviewRequest := operatorRequest("review-before-group-stop", "review-plan", manifest, false)
	reviewCommand := operatorCommand(snapshot, reviewRequest, manifest)
	reviewCommand.Reconciliation = &beginReconciliationEffectCommand{Identity: reviewCommand.Identity, Request: review}
	_, reviewer, err := owner.beginOperatorMutation(t.Context(), reviewCommand)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewer)}); err != nil {
		t.Fatal(err)
	}
	admission := mustOwnerSnapshot(t, owner)
	cancel := operatorCommand(admission, operatorRequest("cancel-group-stop", "cancel", manifest, false), manifest)
	cancel.Runtime = &beginRuntimeEffectCommand{Identity: cancel.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "operator cancelled attempt", RequestDigest: strings.Repeat("b", 64)}
	_, stop, err := owner.beginOperatorMutation(t.Context(), cancel)
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerEffectIdentity(effectRequestIdentity(*stop))
	proofKey := reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, review.Reviewer.Mode, review.Reviewer.Target)
	prior := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	bindingOwner, err := startTestStateOwner(t, owner.stateRoot, prior, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bindingOwner.markReviewerStopped(t.Context(), markReviewerStoppedCommand{Identity: identity, Observation: reviewerStopObservation{GroupPID: 12345}}); !errors.Is(err, errStateConflict) {
		t.Fatalf("stop completed before durable group bind: %v", err)
	}
	bound, err := bindingOwner.bindReviewerStopping(t.Context(), bindReviewerStoppingCommand{Identity: identity, GroupPID: 12345})
	if err != nil {
		t.Fatal(err)
	}
	proof := bound.State.ReviewerProofs[proofKey]
	if bound.State.Effects[stop.ID].SupersededReviewerGroupPID != 12345 || proof.GroupPID != 12345 || proof.DeadProved || proof.EffectID != reviewer.ID {
		t.Fatalf("pre-kill owner binding=%#v proof=%#v", bound.State.Effects[stop.ID], proof)
	}
	if err := bindingOwner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, bound.State, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	if _, err := restarted.markReviewerStopped(t.Context(), markReviewerStoppedCommand{Identity: identity, Observation: reviewerStopObservation{GroupPID: 12345}}); err != nil {
		t.Fatalf("exact confined reviewer group death did not complete after restart: %v", err)
	}
	current := mustOwnerSnapshot(t, restarted)
	proof = current.State.ReviewerProofs[proofKey]
	if !proof.DeadProved || !reviewerCleanupAuthorized(proof, activeWorkerProfileDigest(current.State)) || !current.State.Effects[stop.ID].ReviewerStopped || current.State.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].StopEffectID != stop.ID {
		t.Fatalf("restart lost exact stopped reviewer proof: proof=%#v attempt=%#v effect=%#v", proof, current.State.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)], current.State.Effects[stop.ID])
	}
	projected, err := projectOwnerStatus(current, 1, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if len(projected.Statuses) != 1 || projected.Statuses[0].CurrentPhase != "stop-pending" {
		t.Fatalf("runtime stop was not projected after reviewer authority ended: %#v", projected.Statuses)
	}
}

func TestRecoverLivenessBindsPendingReviewerBeforeAttemptAdvance(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 338, "active", false)
	initial := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	attempt := expandAttemptFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"number":338,"state":"open","body":"body"}`)
	}))
	defer github.Close()
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{API: internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
	reviewRequest := operatorRequest("review-before-recover", "review-plan", manifest, false)
	reviewCommand, _, err := service.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), reviewRequest)
	if err != nil {
		t.Fatal(err)
	}
	_, reviewer, err := owner.beginOperatorMutation(t.Context(), reviewCommand)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewer)}); err != nil {
		t.Fatal(err)
	}
	before := mustOwnerSnapshot(t, owner)
	recoverRequest := operatorRequest("recover-with-reviewer", "recover", manifest, false)
	command := operatorCommand(before, recoverRequest, manifest)
	command.LivenessFailed = true
	command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "dashboard recovery: runtime liveness mismatch", RequestDigest: strings.Repeat("b", 64)}
	committed, stop, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		status, _, _ := ownerOperatorStatus(before.State, manifest.Issue, manifest.Attempt)
		t.Fatalf("Recover with pending reviewer status=%#v err=%v", status, err)
	}
	if stop == nil || stop.SupersededReviewerID != reviewer.ID || !stop.SupersededReviewerSessionRequested || stop.SupersededReviewerRequestDigest != reviewer.RequestDigest {
		t.Fatalf("Recover advanced without exact reviewer stop binding: %#v", stop)
	}
	if pending, ok := committed.State.Effects[reviewer.ID]; ok && pending.State == "pending" {
		t.Fatalf("Recover left old reviewer eligible: %#v", pending)
	}
	cancelled := manifest
	cancelled.State, cancelled.Diagnostic, cancelled.UpdatedAt = "cancelled", command.Runtime.Reason, time.Unix(50, 0).UTC()
	if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stop)), Action: agentruntime.EffectStop, Manifest: cancelled}}); !errors.Is(err, errStateConflict) {
		t.Fatalf("Recover completed before reviewer death proof: %v", err)
	}
}

func TestAbandonCapturesPendingReviewerIdentityBeforeTombstone(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	owner, snapshot, review := reconciliationEffectTestOwner(t, test.request)
	manifest := *review.Manifest
	request := operatorRequest("review-before-abandon", "review-plan", manifest, false)
	command := operatorCommand(snapshot, request, manifest)
	command.Reconciliation = &beginReconciliationEffectCommand{Identity: command.Identity, Request: review}
	_, reviewer, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewer)}); err != nil {
		t.Fatal(err)
	}
	state := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	applyReconciliationInput(t, restarted, repositoryInput(true))
	before := mustOwnerSnapshot(t, restarted)
	abandon := operatorCommand(before, operatorRequest("abandon-pending-review", "abandon", manifest, true), manifest)
	abandon.CleanupDigest = strings.Repeat("c", 64)
	committed, cleanup, err := restarted.beginOperatorMutation(t.Context(), abandon)
	if err != nil {
		status, _, _ := ownerOperatorStatus(before.State, manifest.Issue, manifest.Attempt)
		t.Fatalf("Abandon pending reviewer status=%#v err=%v", status, err)
	}
	if cleanup == nil || cleanup.SupersededReviewerID != reviewer.ID || !cleanup.SupersededReviewerGateProtocol || !cleanup.SupersededReviewerSessionRequested || cleanup.SupersededReviewerRequestDigest != reviewer.RequestDigest || cleanup.SupersededReviewerIssueGeneration != reviewer.IssueGeneration || cleanup.SupersededReviewerAttemptGeneration != reviewer.AttemptGeneration {
		t.Fatalf("Abandon lost exact pre-deletion reviewer identity: %#v", cleanup)
	}
	if _, ok := committed.State.Effects[reviewer.ID]; ok {
		t.Fatal("Abandon left superseded reviewer effect runnable")
	}
	if committed.State.Tombstones[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].EffectID != cleanup.ID {
		t.Fatalf("Abandon did not bind pending cleanup to tombstone: %#v", committed.State.Tombstones)
	}
	proofKey := reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, review.Reviewer.Mode, review.Reviewer.Target)
	prior := cloneRuntimeOwnerState(committed.State)
	priorProof := reviewerProcessProof{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Mode: review.Reviewer.Mode, Target: review.Reviewer.Target, RunID: digestText("different prior run"), EffectID: strings.Repeat("f", 32), IssueGeneration: reviewer.IssueGeneration, AttemptGeneration: reviewer.AttemptGeneration, GroupPID: 7777, DeadProved: true, ProfileDigest: activeWorkerProfileDigest(prior)}
	prior.ReviewerProofs[proofKey] = priorProof
	if err := restarted.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	bindingOwner, err := startTestStateOwner(t, restarted.stateRoot, prior, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = bindingOwner.close(context.Background()) })
	if _, err := bindingOwner.bindReviewerStopping(t.Context(), bindReviewerStoppingCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*cleanup)), GroupPID: 12345}); !errors.Is(err, errStateConflict) {
		t.Fatalf("Abandon cleanup replaced a different never-reused reviewer proof: %v", err)
	}
	if proof := mustOwnerSnapshot(t, bindingOwner).State.ReviewerProofs[proofKey]; !reflect.DeepEqual(proof, priorProof) {
		t.Fatalf("rejected Abandon binding changed the prior proof: %#v", proof)
	}
}

func TestBindReviewerStoppingRequiresPriorRunProofToBeForgotten(t *testing.T) {
	review := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	const oldID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const newID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const stopID = "cccccccccccccccccccccccccccccccc"
	for _, action := range []string{string(agentruntime.EffectStop), string(agentruntime.EffectCleanup), string(reconciliationReviewer), "implementation run-observe"} {
		t.Run(action, func(t *testing.T) {
			review := review
			review.Reviewer.RunID = digestText("new run " + action)
			if action == "implementation run-observe" {
				review.Reviewer.Mode = agentruntime.ReviewModeImplementation
			}
			state := newRuntimeOwnerState(review.Repository)
			key := reviewerProofKey(review.Repository, review.Issue, review.Attempt, review.Reviewer.Mode, review.Reviewer.Target)
			state.WorkerProfileDigest = digestText("active profile")
			old := reviewerProcessProof{Repository: review.Repository, Issue: review.Issue, Attempt: review.Attempt, Mode: review.Reviewer.Mode, Target: review.Reviewer.Target, RunID: digestText("old run " + action), EffectID: oldID, IssueGeneration: 1, AttemptGeneration: 1, GroupPID: 11111, DeadProved: true, ProfileDigest: state.WorkerProfileDigest}
			state.ReviewerProofs[key] = old
			effect := runtimeEffectIntent{ID: stopID, Action: action, Repository: review.Repository, Issue: review.Issue, Attempt: review.Attempt, IssueGeneration: 2, AttemptGeneration: 2, IntentEpoch: 1, IntentRevision: 3, State: "pending", RequestDigest: strings.Repeat("d", 64), ReviewerGateProtocol: true, ReviewerSessionRequested: true, ReviewerProfileDigest: state.WorkerProfileDigest}
			if action == string(reconciliationReviewer) || action == "implementation run-observe" {
				effect.Action = string(reconciliationReviewer)
				effect.ID = newID
				effect.Reconciliation = &review
			} else {
				effect.SupersededReviewerID = newID
				effect.SupersededReviewerGateProtocol = true
				effect.SupersededReviewerSessionRequested = true
				effect.SupersededReviewerMode = review.Reviewer.Mode
				effect.SupersededReviewerTarget = review.Reviewer.Target
				effect.SupersededReviewerRunID = review.Reviewer.RunID
				effect.SupersededReviewerProfileDigest = state.WorkerProfileDigest
				effect.SupersededReviewerIssueGeneration = 2
				effect.SupersededReviewerAttemptGeneration = 2
			}
			state.Effects[effect.ID] = effect
			command := bindReviewerStoppingCommand{Identity: ownerEffectIdentity(effectRequestIdentity(effect)), GroupPID: 22222}
			if err := applyBindReviewerStopping(&state, command); !errors.Is(err, errStateConflict) {
				t.Fatalf("different prior run proof was replaced: %v", err)
			}
			if !reflect.DeepEqual(state.ReviewerProofs[key], old) {
				t.Fatalf("rejected binding changed the prior run proof: %#v", state.ReviewerProofs[key])
			}
			delete(state.ReviewerProofs, key) // Physical cleanup plus owner forget occurs before a new run can bind.
			if err := applyBindReviewerStopping(&state, command); err != nil {
				t.Fatalf("new reviewer could not bind after prior proof was forgotten: %v", err)
			}
			bound := state.ReviewerProofs[key]
			if bound.RunID != review.Reviewer.RunID || bound.GroupPID != 22222 || bound.DeadProved {
				t.Fatalf("new live reviewer proof was not bound exactly: %#v", bound)
			}
			if err := applyBindReviewerStopping(&state, command); err != nil || !reflect.DeepEqual(state.ReviewerProofs[key], bound) {
				t.Fatalf("exact reviewer binding replay changed state: proof=%#v err=%v", state.ReviewerProofs[key], err)
			}
		})
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

func operatorTestOwner(t *testing.T, issue int, status string, closed bool, bound ...bool) (*stateOwner, agentruntime.Manifest) {
	t.Helper()
	root := resolvedTempDir(t)
	manifestState := "running"
	if status == "completed" {
		manifestState = "completed"
	}
	manifest := ownerTestManifest(t, root, issue, 1, manifestState)
	if len(bound) != 0 && bound[0] {
		manifest, _ = boundRuntimeEffectTestManifest(t, manifest)
	}
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
	command := beginOperatorMutationCommand{Request: request, Manifest: manifest, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest, IssueClosed: request.Action == "dismiss", CleanupValid: slices.Contains([]string{"dismiss", "archive", "abandon", "remove"}, request.Action), Identity: stateResultIdentity{
		Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision,
		IssueGeneration:   snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)],
		AttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)],
	}}
	if command.CleanupValid {
		command.CleanupPolicy.Action = request.Action
		if request.Action == "dismiss" {
			command.CleanupDigest = strings.Repeat("d", 64)
		}
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
