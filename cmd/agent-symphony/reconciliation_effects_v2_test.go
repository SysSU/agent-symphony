package main

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestReconciliationEffectVariantsAreExactIdempotentAndConflictSafe(t *testing.T) {
	for _, test := range reconciliationEffectCases(t) {
		t.Run(test.name, func(t *testing.T) {
			owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
			identity := reconciliationBeginIdentity(snapshot, request)
			firstState, first, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: request})
			if err != nil || first == nil {
				t.Fatalf("begin effect=%#v err=%v", first, err)
			}
			replayedState, replayed, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: request})
			if err != nil || replayed == nil || replayed.ID != first.ID || replayedState.State.Revision != firstState.State.Revision {
				t.Fatalf("replay state=%#v effect=%#v err=%v", replayedState.State, replayed, err)
			}
			conflict := cloneReconciliationRequest(request)
			conflict.ExecutionDigest = strings.Repeat("b", 64)
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: conflict}); !errors.Is(err, errStateConflict) {
				t.Fatalf("conflicting begin err=%v", err)
			}
			finishIdentity := reconciliationIntentIdentity(*first)
			if err := owner.authorizeReconciliationEffect(t.Context(), authorizeReconciliationEffectCommand{Identity: finishIdentity, Action: request.Action}); err != nil {
				t.Fatalf("authorize effect: %v", err)
			}
			result := test.result(request)
			completed, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: finishIdentity, Result: result})
			if err != nil {
				t.Fatal(err)
			}
			verifyReconciliationEffectOutcome(t, completed.State, request, result)
			replayedCompletion, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: finishIdentity, Result: result})
			if err != nil || replayedCompletion.State.Revision != completed.State.Revision {
				t.Fatalf("completion replay revisions got=%d want=%d err=%v", replayedCompletion.State.Revision, completed.State.Revision, err)
			}
			invalid := reconciliationEffectResult{Action: request.Action}
			if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: finishIdentity, Result: invalid}); !errors.Is(err, errStateConflict) {
				t.Fatalf("conflicting completion err=%v", err)
			}
		})
	}
}

func verifyReconciliationEffectOutcome(t *testing.T, state runtimeOwnerState, request reconciliationEffectRequest, result reconciliationEffectResult) {
	t.Helper()
	if request.Attempt == 0 {
		return
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	manifest := state.Attempts[key].Manifest
	switch request.Action {
	case reconciliationGitHubPublish:
		recovery, ok := state.Recoveries[key]
		if !ok || recovery.State.Number != result.GitHubPublish.PR || recovery.State.HeadSHA != result.GitHubPublish.HeadSHA || recovery.State.PreparedPublication != nil {
			t.Fatalf("publication recovery=%#v", recovery)
		}
	case reconciliationReviewer:
		if request.Reviewer.Phase == "cleanup" {
			if manifest.ReviewSnapshot != "" || manifest.ReviewSession != "" {
				t.Fatalf("cleanup retained reviewer resources: %#v", manifest)
			}
		} else if manifest.ReviewState != result.Reviewer.Status || manifest.ReviewSnapshot != request.Reviewer.Snapshot || manifest.ReviewSession != request.Reviewer.Session {
			t.Fatalf("review result was not applied: %#v", manifest)
		}
	case reconciliationHandoffDeliver:
		if request.Handoff.Kind == "review-findings" {
			if !manifest.ReviewHandoffQueued || !manifest.ReviewHandoffAck || manifest.State != "running" {
				t.Fatalf("review handoff was not applied: %#v", manifest)
			}
			return
		}
		recovery := state.Recoveries[key].State
		if request.Handoff.Outcome == nil && !recovery.HandoffReceipts[request.Handoff.Key] {
			t.Fatalf("recovery handoff receipt missing: %#v", recovery)
		}
		if request.Handoff.Outcome != nil && (recovery.ValidationInFlightSHA != "" || recovery.ValidationResult != request.Handoff.Outcome.ValidationResult || recovery.ValidationEvidence != request.Handoff.Outcome.ValidationEvidence) {
			t.Fatalf("recovery outcome was not applied: %#v", recovery)
		}
	}
}

func TestReconciliationEffectClonesAndLoadsTypedPayload(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-findings").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	identity := reconciliationBeginIdentity(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: request})
	if err != nil {
		t.Fatal(err)
	}
	request.GitHubIssueUpdate.Findings[0] = "mutated"
	stored, _ := owner.snapshot(t.Context())
	if got := stored.State.Effects[effect.ID].Reconciliation.GitHubIssueUpdate.Findings[0]; got != "finding" {
		t.Fatalf("request aliases caller: %q", got)
	}
	result := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueFindings, Observed: true}}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: result}); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, "o/r")
	if err != nil || loaded.Effects[effect.ID].ReconciliationResult.GitHubIssueUpdate.Kind != githubIssueFindings {
		t.Fatalf("loaded effect=%#v err=%v", loaded.Effects[effect.ID], err)
	}
}

func TestReconciliationEffectSurvivesIdenticalCycleAndRejectsChangedObservation(t *testing.T) {
	test := reconciliationEffectCases(t)[1]
	owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identical := reconciliationEffectObservationInput(request, "title")
	applyReconciliationInput(t, owner, identical)
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: test.result(request)}); err != nil {
		t.Fatalf("identical later cycle invalidated effect: %v", err)
	}

	owner, snapshot, request = reconciliationEffectTestOwner(t, test.request)
	_, effect, err = owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	changed := reconciliationEffectObservationInput(request, "changed")
	applyReconciliationInput(t, owner, changed)
	state, _ := owner.snapshot(t.Context())
	if _, exists := state.State.Effects[effect.ID]; exists {
		t.Fatal("changed observation retained stale pending effect")
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: test.result(request)}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("changed observation finish err=%v", err)
	}
}

func TestReconciliationEffectRejectsUnauthorizedLifecycle(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runtimeOwnerState, *reconciliationEffectRequest)
	}{
		{"github-bind", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.State = "running" })},
		{"github-publish", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewState = "findings-queued" })},
		{"github-publish-prepared", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			state.Recoveries[effectAttemptKey(*request)] = runtimePRRecovery{}
		}},
		{"issue-terminal-failure", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.State = "running" })},
		{"issue-evidence", removeEffectRemoteAttempt},
		{"issue-findings", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewHandoffAck = true })},
		{"issue-retry", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.State = "running" })},
		{"issue-control-snapshot", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
			observation.Present = false
			state.Observations[ownerIssueKey(request.Repository, request.Issue)] = observation
		}},
		{"issue-dependency-clear", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
			observation.Fact.SatisfiedDependencies = nil
			state.Observations[ownerIssueKey(request.Repository, request.Issue)] = observation
		}},
		{"github-pr-governance", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			recovery := state.Recoveries[effectAttemptKey(*request)]
			recovery.State.HeadSHA = strings.Repeat("c", 40)
			state.Recoveries[effectAttemptKey(*request)] = recovery
		}},
		{"reviewer-run-observe", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewState = "clean" })},
		{"reviewer-cleanup", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewState = "running" })},
		{"handoff-review-findings", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewHandoffAck = true })},
		{"handoff-recovery", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			recovery := state.Recoveries[effectAttemptKey(*request)]
			recovery.State.ValidationInFlightSHA = ""
			state.Recoveries[effectAttemptKey(*request)] = recovery
		}},
		{"handoff-recovery-outcome", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			recovery := state.Recoveries[effectAttemptKey(*request)]
			delete(recovery.State.HandoffReceipts, request.Handoff.Key)
			state.Recoveries[effectAttemptKey(*request)] = recovery
		}},
		{"retire-completed", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
			attempt := observation.Attempts[effectAttemptKey(*request)]
			attempt.Fact.State = "active"
			observation.Attempts[effectAttemptKey(*request)] = attempt
			if observation.Fact.ActiveAttempt != nil {
				observation.Fact.ActiveAttempt.State = "active"
			}
			observation.Fact.TerminalAttempts = nil
			state.Observations[ownerIssueKey(request.Repository, request.Issue)] = observation
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, owner, snapshot := reconciliationEffectPersistentOwner(t, reconciliationEffectCaseNamed(t, test.name).request)
			request := bindEffectObservation(snapshot, reconciliationEffectCaseNamed(t, test.name).request)
			request = configureEffectFixture(root, request, manifestForRequest(snapshot.State, request), strings.Repeat("b", 40))
			request = bindEffectObservation(snapshot, request)
			state := cloneRuntimeOwnerState(snapshot.State)
			test.mutate(&state, &request)
			if reconciliationObservationCurrent(state, request) && validReconciliationEffectStateBindings(root, state, request) {
				t.Fatal("unauthorized lifecycle remained admissible")
			}
			_ = owner
		})
	}
}

func TestReconciliationEffectRejectsUnobservedIssueUpdateProposal(t *testing.T) {
	for _, name := range []string{"issue-control-snapshot", "issue-dependency-clear"} {
		t.Run(name, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, name)
			owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
			if request.GitHubIssueUpdate.Kind == githubIssueControlSnapshot {
				request.GitHubIssueUpdate.ControlSnapshotDigest = strings.Repeat("d", 64)
			} else {
				request.GitHubIssueUpdate.PullRequest++
			}
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request}); !errors.Is(err, errStaleStateResult) {
				t.Fatalf("unobserved proposal err=%v", err)
			}
		})
	}
}

func effectAttemptKey(request reconciliationEffectRequest) string {
	return ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
}

func manifestForRequest(state runtimeOwnerState, request reconciliationEffectRequest) *agentruntime.Manifest {
	manifest := state.Attempts[effectAttemptKey(request)].Manifest
	return &manifest
}

func mutateEffectManifest(change func(*agentruntime.Manifest)) func(*runtimeOwnerState, *reconciliationEffectRequest) {
	return func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
		key := effectAttemptKey(*request)
		record := state.Attempts[key]
		change(&record.Manifest)
		state.Attempts[key] = record
		manifest := cloneManifest(record.Manifest)
		request.Manifest = &manifest
	}
}

func removeEffectRemoteAttempt(state *runtimeOwnerState, request *reconciliationEffectRequest) {
	key := ownerIssueKey(request.Repository, request.Issue)
	observation := state.Observations[key]
	delete(observation.Attempts, effectAttemptKey(*request))
	observation.Fact.ActiveAttempt, observation.Fact.TerminalAttempts = nil, nil
	state.Observations[key] = observation
}

func TestFirstAttemptReservationInvalidatesIssueScopedEffect(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-control-snapshot").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	// Rebuild this owner without an attempt reservation: issue-scoped updates can precede it.
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	applyReconciliationInput(t, owner, reconciliationInput{Scope: issueScope(190), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issueFact(190, "title")}, IssueUpdates: []reconciliationIssueUpdateProposal{{Repository: "o/r", Issue: 190, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: strings.Repeat("c", 64)}}})
	snapshot, _ = owner.snapshot(t.Context())
	observation := snapshot.State.Observations[ownerIssueKey("o/r", 190)]
	request.Attempt, request.Manifest = 0, nil
	request.ObservationGeneration, request.ObservationCycleID, request.BodyDigest = observation.Generation, observation.LastCycleID, observation.Fact.BodyDigest
	request.GitHubIssueUpdate = &githubIssueUpdateEffectRequest{Kind: githubIssueControlSnapshot, ControlSnapshotDigest: strings.Repeat("c", 64)}
	request.Action, request.GitHubPRGovernance = reconciliationGitHubIssueUpdate, nil
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey("o/r", 190)]}, Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueControlSnapshot, Observed: true}}}); err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, 190, 1, "preparing")
	reserved, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest, ExpectedIssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey("o/r", 190)]})
	if err != nil || len(reserved.State.Effects) != 0 || reserved.State.IssueGenerations[ownerIssueKey("o/r", 190)] != 2 {
		t.Fatalf("reservation=%#v err=%v", reserved.State, err)
	}
}

func TestRuntimePRRecoveryClonesLoadsAndRejectsInvalidState(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 191, 1, "running")
	key := ownerAttemptKey("o/r", 191, 1)
	feedback := internalgithub.Feedback{ID: 9, Source: "issue", ActorID: 1, Body: "fix it", State: internalgithub.FeedbackPending, Execution: internalgithub.FeedbackInFlight, Authorized: true}
	pr := internalgithub.PRState{Repository: "o/r", Number: 8, Issue: 191, Attempt: 1, HeadSHA: strings.Repeat("b", 40), Facts: internalgithub.PRFacts{Feedback: []internalgithub.Feedback{feedback}}, HandoffReceipts: map[string]bool{}}
	handoff := internalgithub.RecoveryHandoff{Repository: "o/r", PR: 8, Issue: 191, Attempt: 1, HeadSHA: pr.HeadSHA, Feedback: []internalgithub.Feedback{feedback}}
	handoff.Key = recoveryHandoffKey(handoff)
	pr.HandoffReceipts[handoff.Key] = true
	state := newRuntimeOwnerState("o/r")
	state.IssueGenerations[ownerIssueKey("o/r", 191)] = 1
	state.AttemptGenerations[key] = 1
	state.Attempts[key] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	state.Recoveries[key] = runtimePRRecovery{IssueGeneration: 1, AttemptGeneration: 1, State: pr}
	owner, err := startTestStateOwner(t, root, state, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
	})
	if err != nil {
		t.Fatal(err)
	}
	pr.Facts.Feedback[0].Body = "caller mutation"
	delete(pr.HandoffReceipts, handoff.Key)
	snapshot, err := owner.snapshot(t.Context())
	if err != nil || snapshot.State.Recoveries[key].State.Facts.Feedback[0].Body != "fix it" || !snapshot.State.Recoveries[key].State.HandoffReceipts[handoff.Key] {
		t.Fatalf("recovery aliases input: %#v err=%v", snapshot.State.Recoveries[key], err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, "o/r")
	if err != nil || loaded.Recoveries[key].State.Facts.Feedback[0].Body != "fix it" || !loaded.Recoveries[key].State.HandoffReceipts[handoff.Key] {
		t.Fatalf("loaded recovery=%#v err=%v", loaded.Recoveries[key], err)
	}

	invalid := []struct {
		name   string
		mutate func(*runtimeOwnerState)
	}{
		{"stale generation", func(candidate *runtimeOwnerState) {
			recovery := candidate.Recoveries[key]
			recovery.AttemptGeneration++
			candidate.Recoveries[key] = recovery
		}},
		{"tombstoned", func(candidate *runtimeOwnerState) { candidate.Tombstones[key] = runtimeTombstone{} }},
		{"wrong key", func(candidate *runtimeOwnerState) {
			recovery := candidate.Recoveries[key]
			delete(candidate.Recoveries, key)
			candidate.Recoveries[ownerAttemptKey("o/r", 191, 2)] = recovery
		}},
		{"oversized feedback", func(candidate *runtimeOwnerState) {
			recovery := candidate.Recoveries[key]
			recovery.State.Facts.Feedback[0].Body = strings.Repeat("x", maxReconciliationBodyBytes+1)
			candidate.Recoveries[key] = recovery
		}},
		{"duplicate PR", func(candidate *runtimeOwnerState) {
			secondKey := ownerAttemptKey("o/r", 191, 2)
			second := ownerTestManifest(t, root, 191, 2, "running")
			candidate.AttemptGenerations[secondKey] = 1
			candidate.Attempts[secondKey] = runtimeAttemptRecord{Generation: 1, Manifest: second}
			recovery := candidate.Recoveries[key]
			recovery.State.Attempt = 2
			candidate.Recoveries[secondKey] = recovery
		}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneRuntimeOwnerState(loaded)
			test.mutate(&candidate)
			if validateRuntimePRRecoveries(candidate) == nil {
				t.Fatal("invalid recovery state was accepted")
			}
		})
	}
}

func TestOwnerGenerationChangesDeletePRRecovery(t *testing.T) {
	for _, operation := range []string{"issue", "attempt", "tombstone"} {
		t.Run(operation, func(t *testing.T) {
			_, owner, snapshot := reconciliationEffectPersistentOwner(t, reconciliationEffectCaseNamed(t, "github-pr-governance").request)
			key := ownerAttemptKey("o/r", 190, 1)
			if _, ok := snapshot.State.Recoveries[key]; !ok {
				t.Fatal("fixture lacks PR recovery")
			}
			var (
				result stateOwnerSnapshot
				err    error
			)
			switch operation {
			case "issue":
				result, err = owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: 190, ExpectedGeneration: 1})
			case "attempt":
				result, err = owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: snapshot.State.Attempts[key].Manifest, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1})
			case "tombstone":
				result, _, err = owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 190, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "dismissed", CleanupPhase: "completed"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := result.State.Recoveries[key]; ok {
				t.Fatal("generation change retained stale PR recovery")
			}
		})
	}
}

func TestAcceptedPublishedAttemptHydratesOwnerRecovery(t *testing.T) {
	root := resolvedTempDir(t)
	state := newRuntimeOwnerState("o/r")
	manifest := ownerTestManifest(t, root, 196, 1, "running")
	issueKey, attemptKey := ownerIssueKey("o/r", 196), ownerAttemptKey("o/r", 196, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	issue := issueFact(196, "published")
	issue.Attempt, issue.CurrentAttempt = 1, 1
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 196, Attempt: 1, PR: 9, BaseSHA: manifest.BaseSHA, HeadSHA: strings.Repeat("b", 40), State: "active", PublicationConfirmed: true}
	issue.ActiveAttempt = &remote
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	accepted := applyReconciliationInput(t, owner, input)
	if got := accepted.State.Recoveries[attemptKey]; got.State.Number != 9 || got.State.HeadSHA != remote.HeadSHA || got.IssueGeneration != 1 || got.AttemptGeneration != 1 {
		t.Fatalf("accepted attempt did not hydrate owner recovery: %#v", got)
	}

	nonPublishedOwner := newReconciliationTestOwner(t)
	nonPublished := repositoryInput(true, issueFact(197, "not published"))
	nonPublished.Attempts = []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 197, Attempt: 1, PR: 10, BaseSHA: manifest.BaseSHA, HeadSHA: remote.HeadSHA, State: "active"}}
	result := applyReconciliationInput(t, nonPublishedOwner, nonPublished)
	if len(result.State.Recoveries) != 0 {
		t.Fatalf("unconfirmed attempt hydrated recovery: %#v", result.State.Recoveries)
	}
}

func TestOwnerAttemptRecoveryMutatesOnlyCurrentGovernanceEffect(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	recovery := ownerAttemptRecovery{owner: owner, identity: reconciliationIntentIdentity(*effect)}
	state, err := recovery.PullRequestState(t.Context(), "o/r", 7, request.Issue, request.Attempt, request.GitHubPRGovernance.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	feedback := internalgithub.Feedback{ID: 12, Source: "issue", ActorID: 5, Body: "fix", CreatedAt: time.Unix(10, 0), Authorized: true}
	if claimed, err := recovery.ClaimFeedback(t.Context(), state, feedback); err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if claimed, err := recovery.ClaimFeedback(t.Context(), state, feedback); err != nil || claimed {
		t.Fatalf("duplicate claim=%v err=%v", claimed, err)
	}
	if err := recovery.QueueValidation(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	if err := recovery.QueueValidation(t.Context(), state); err != nil {
		t.Fatalf("duplicate queue: %v", err)
	}
	current, _ := owner.snapshot(t.Context())
	got := current.State.Recoveries[ownerAttemptKey("o/r", request.Issue, request.Attempt)].State
	if got.ValidationGeneration != 1 || got.ValidationQueuedSHA != got.HeadSHA || len(got.Facts.Feedback) != 1 || got.Facts.Feedback[0].Execution != internalgithub.FeedbackClaimed {
		t.Fatalf("owner recovery mutation=%#v", got)
	}
	if _, err := (ownerAttemptRecovery{}).ClaimFeedback(t.Context(), state, feedback); err == nil {
		t.Fatal("zero-value recovery claim panicked or succeeded")
	}
	if err := (ownerAttemptRecovery{}).QueueValidation(t.Context(), state); err == nil {
		t.Fatal("zero-value recovery queue succeeded")
	}
}

func TestAttemptInvalidationRevokesIssueScopedDependencyClear(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-dependency-clear").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	invalidated, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: request.Issue, Attempt: request.GitHubIssueUpdate.AttributionAttempt, ExpectedIssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey("o/r", request.Issue)], ExpectedAttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey("o/r", request.Issue, request.GitHubIssueUpdate.AttributionAttempt)], Action: "dismissed", CleanupPhase: "completed"})
	if err != nil {
		t.Fatal(err)
	}
	observation := invalidated.State.Observations[ownerIssueKey("o/r", request.Issue)]
	if slices.ContainsFunc(observation.IssueUpdates, func(proposal reconciliationIssueUpdateProposal) bool {
		return proposal.Kind == githubIssueDependencyClear
	}) {
		t.Fatalf("dependency proposal survived attempt invalidation: %#v", observation.IssueUpdates)
	}
	if _, exists := invalidated.State.Effects[effect.ID]; exists {
		t.Fatalf("dependency effect survived attempt invalidation: %#v", invalidated.State.Effects[effect.ID])
	}
	if err := owner.authorizeReconciliationEffect(t.Context(), authorizeReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Action: request.Action}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("invalidated dependency effect authorization err=%v", err)
	}
}

func TestStaleCollectionCannotRestoreInvalidatedDependencyClear(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-dependency-clear").request
	owner, _, request := reconciliationEffectTestOwner(t, request)
	before, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stale := mustCollection(t, before, reconciliationEffectObservationInput(request, "title"))
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{
		Repository:                request.Repository,
		Issue:                     request.Issue,
		Attempt:                   request.GitHubIssueUpdate.AttributionAttempt,
		ExpectedIssueGeneration:   before.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)],
		ExpectedAttemptGeneration: before.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.GitHubIssueUpdate.AttributionAttempt)],
		Action:                    "dismissed",
		CleanupPhase:              "completed",
	}); err != nil {
		t.Fatal(err)
	}
	applied := applyCollection(t, owner, stale)
	observation := applied.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	if slices.ContainsFunc(observation.IssueUpdates, func(proposal reconciliationIssueUpdateProposal) bool {
		return proposal.Kind == githubIssueDependencyClear
	}) {
		t.Fatalf("stale collection restored invalidated proposal: %#v", observation.IssueUpdates)
	}
	request = bindEffectObservation(applied, request)
	if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(applied, request), Request: request}); err == nil {
		t.Fatal("effect admission accepted an invalidated dependency proposal")
	}
}

type reconciliationEffectCase struct {
	name    string
	request reconciliationEffectRequest
	result  func(reconciliationEffectRequest) reconciliationEffectResult
}

func reconciliationEffectCaseNamed(t *testing.T, name string) reconciliationEffectCase {
	t.Helper()
	for _, test := range reconciliationEffectCases(t) {
		if test.name == name {
			return test
		}
	}
	t.Fatalf("missing reconciliation effect case %q", name)
	return reconciliationEffectCase{}
}

func reconciliationEffectCases(t *testing.T) []reconciliationEffectCase {
	t.Helper()
	base := strings.Repeat("a", 40)
	digest := strings.Repeat("a", 64)
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 190, Attempt: 1, BaseSHA: base, Branch: "agent/190-1", State: "running", ReviewHead: base}
	common := func(action reconciliationEffectAction) reconciliationEffectRequest {
		copy := manifest
		return reconciliationEffectRequest{Action: action, Repository: "o/r", Issue: 190, Attempt: 1, Manifest: &copy, ExecutionDigest: digest}
	}
	observed := func(action reconciliationEffectAction, value any) func(reconciliationEffectRequest) reconciliationEffectResult {
		return func(request reconciliationEffectRequest) reconciliationEffectResult {
			result := reconciliationEffectResult{Action: action}
			switch action {
			case reconciliationGitHubBind:
				result.GitHubBind = &githubBindEffectResult{Observed: true}
			case reconciliationGitHubIssueUpdate:
				result.GitHubIssueUpdate = &githubIssueUpdateEffectResult{Kind: request.GitHubIssueUpdate.Kind, Observed: true}
			case reconciliationGitHubPRGovernance:
				result.GitHubPRGovernance = &githubGovernanceEffectResult{PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA, Observed: true}
			case reconciliationHandoffDeliver:
				result.Handoff = &handoffEffectResult{Kind: request.Handoff.Kind, Key: request.Handoff.Key, OutcomePath: request.Handoff.OutcomePath, OutcomeToken: request.Handoff.OutcomeToken, Observed: true}
			case reconciliationRetireCompleted:
				result.Retire = &retireCompletedEffectResult{ResourcesGone: true}
			}
			_ = value
			return result
		}
	}
	bind := common(reconciliationGitHubBind)
	bind.GitHubBind = &githubBindEffectRequest{BaseSHA: base, Branch: manifest.Branch, Detail: "reserved"}
	publish := common(reconciliationGitHubPublish)
	publish.GitHubPublish = &githubPublishEffectRequest{Title: "title", BaseBranch: "main", HeadSHA: base, Validation: "tests", Documentation: "none"}
	publishPrepared := cloneReconciliationRequest(publish)
	publishPrepared.GitHubPublish.Prepared = &internalgithub.PreparedPublication{Handoff: internalgithub.RecoveryHandoff{Repository: "o/r", PR: 7, Issue: 190, Attempt: 1, HeadSHA: strings.Repeat("b", 40)}, HeadSHA: strings.Repeat("c", 40)}
	publishResult := func(request reconciliationEffectRequest) reconciliationEffectResult {
		return reconciliationEffectResult{Action: request.Action, GitHubPublish: &githubPublishEffectResult{PR: 7, HeadSHA: request.GitHubPublish.HeadSHA, BoundBodyDigest: expectedPublishedBodyDigest(request, 7), Evidence: true, PublishedComment: true}}
	}
	update := func(kind githubIssueUpdateKind, payload githubIssueUpdateEffectRequest) reconciliationEffectRequest {
		request := common(reconciliationGitHubIssueUpdate)
		payload.Kind = kind
		request.GitHubIssueUpdate = &payload
		if kind == githubIssueControlSnapshot || kind == githubIssueDependencyClear {
			request.Attempt, request.Manifest = 0, nil
		}
		return request
	}
	governance := common(reconciliationGitHubPRGovernance)
	governance.GitHubPRGovernance = &githubGovernanceEffectRequest{PR: 7, HeadSHA: base, Policy: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}
	reviewer := func(phase string) reconciliationEffectRequest {
		request := common(reconciliationReviewer)
		target := fmt.Sprintf("o/r#190 plan sha256:%s", digest)
		snapshot, session := reviewIdentity(agentruntime.Attempt{Repository: "o/r", Issue: 190, Number: 1}, filepath.Join("/tmp", "reviews"))
		request.Reviewer = &reviewerEffectRequest{Phase: phase, Mode: agentruntime.ReviewModePlan, Target: target, BaseSHA: base, HeadSHA: base, Snapshot: snapshot, Session: session}
		return request
	}
	reviewerResult := func(request reconciliationEffectRequest) reconciliationEffectResult {
		value := request.Reviewer
		status := "clean"
		if value.Phase == "cleanup" {
			status = "cleaned"
		}
		return reconciliationEffectResult{Action: request.Action, Reviewer: &reviewerEffectResult{Phase: value.Phase, Status: status, Mode: value.Mode, Target: value.Target, BaseSHA: value.BaseSHA, HeadSHA: value.HeadSHA, Snapshot: value.Snapshot, Session: value.Session}}
	}
	reviewHandoff := common(reconciliationHandoffDeliver)
	reviewHandoff.Handoff = &handoffEffectRequest{Kind: "review-findings", HeadSHA: base, Key: "review", Findings: []string{"finding"}, OutcomePath: handoffReceiptPath("/tmp/worktree", "review"), OutcomeToken: base}
	reviewHandoff.Manifest.Worktree = "/tmp/worktree"
	recoveryHandoff := common(reconciliationHandoffDeliver)
	recovery := internalgithub.RecoveryHandoff{Key: "recovery", Repository: "o/r", PR: 7, Issue: 190, Attempt: 1, HeadSHA: base, Validation: true}
	token := fmt.Sprintf("%x", sha256Sum("handoff-outcome\x00"+recovery.Key))
	recoveryHandoff.Handoff = &handoffEffectRequest{Kind: "recovery", Key: recovery.Key, Recovery: &recovery, OutcomePath: handoffReceiptPath(recoveryHandoff.Manifest.Worktree, recovery.Key), OutcomeToken: token}
	recoveryOutcome := cloneReconciliationRequest(recoveryHandoff)
	recoveryOutcome.Handoff.Outcome = &internalgithub.HandoffOutcome{Key: recovery.Key, ValidationResult: "passed", ValidationEvidence: "tests passed"}
	retire := common(reconciliationRetireCompleted)
	retire.Retire = &retireCompletedEffectRequest{Mode: "abandon", HeadSHA: base}
	return []reconciliationEffectCase{
		{"github-bind", bind, observed(reconciliationGitHubBind, nil)},
		{"github-publish", publish, publishResult},
		{"github-publish-prepared", publishPrepared, publishResult},
		{"issue-terminal-failure", update(githubIssueTerminalFailure, githubIssueUpdateEffectRequest{Diagnostic: "failed", FailedAtUnixNano: 1}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-evidence", update(githubIssueEvidence, githubIssueUpdateEffectRequest{HeadSHA: base}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-findings", update(githubIssueFindings, githubIssueUpdateEffectRequest{HeadSHA: base, Findings: []string{"finding"}}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-retry", update(githubIssueRetry, githubIssueUpdateEffectRequest{FailedAtUnixNano: 1}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-control-snapshot", update(githubIssueControlSnapshot, githubIssueUpdateEffectRequest{ControlSnapshotDigest: strings.Repeat("c", 64)}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-dependency-clear", update(githubIssueDependencyClear, githubIssueUpdateEffectRequest{AttributionAttempt: 1, Dependency: 3, PullRequest: 7}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"github-pr-governance", governance, observed(reconciliationGitHubPRGovernance, nil)},
		{"reviewer-run-observe", reviewer("run-observe"), reviewerResult},
		{"reviewer-cleanup", reviewer("cleanup"), reviewerResult},
		{"handoff-review-findings", reviewHandoff, observed(reconciliationHandoffDeliver, nil)},
		{"handoff-recovery", recoveryHandoff, observed(reconciliationHandoffDeliver, nil)},
		{"handoff-recovery-outcome", recoveryOutcome, observed(reconciliationHandoffDeliver, nil)},
		{"retire-completed", retire, observed(reconciliationRetireCompleted, nil)},
	}
}

func reconciliationEffectTestOwner(t *testing.T, request reconciliationEffectRequest) (*stateOwner, stateOwnerSnapshot, reconciliationEffectRequest) {
	t.Helper()
	_, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, request, func(runtimeOwnerState) error { return nil })
	return owner, snapshot, bindEffectObservation(snapshot, request)
}

func reconciliationEffectPersistentOwner(t *testing.T, request reconciliationEffectRequest) (string, *stateOwner, stateOwnerSnapshot) {
	t.Helper()
	var root string
	root, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, request, nil)
	return root, owner, snapshot
}

func reconciliationEffectPersistentOwnerWithPersist(t *testing.T, request reconciliationEffectRequest, persist func(runtimeOwnerState) error) (string, *stateOwner, stateOwnerSnapshot) {
	t.Helper()
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, request.Issue, max(1, request.Attempt), "running")
	head := strings.Repeat("b", 40)
	request = configureEffectFixture(root, request, &manifest, head)
	state := newRuntimeOwnerState("o/r")
	if request.Attempt > 0 {
		issueKey, attemptKey := ownerIssueKey("o/r", request.Issue), ownerAttemptKey("o/r", request.Issue, request.Attempt)
		state.IssueGenerations[issueKey] = 1
		state.AttemptGenerations[attemptKey] = 1
		state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
		if request.Action == reconciliationGitHubPRGovernance || request.Handoff != nil && request.Handoff.Kind == "recovery" || request.GitHubPublish != nil && request.GitHubPublish.Prepared != nil {
			recovery := internalgithub.PRState{Repository: "o/r", Number: 7, Issue: request.Issue, Attempt: request.Attempt, HeadSHA: head, HandoffReceipts: map[string]bool{}}
			if request.Handoff != nil && request.Handoff.Kind == "recovery" {
				recovery.ValidationInFlightSHA, recovery.ValidationGeneration = head, 1
				if request.Handoff.Outcome != nil {
					recovery.HandoffReceipts[request.Handoff.Key] = true
				}
			}
			if request.GitHubPublish != nil && request.GitHubPublish.Prepared != nil {
				recovery.PreparedPublication = clonePreparedPublication(request.GitHubPublish.Prepared)
			}
			state.Recoveries[attemptKey] = runtimePRRecovery{IssueGeneration: 1, AttemptGeneration: 1, State: recovery}
		}
	}
	if persist == nil {
		persist = func(state runtimeOwnerState) error {
			return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
		}
	}
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	input := reconciliationEffectObservationInput(request, "title")
	applyReconciliationInput(t, owner, input)
	snapshot, _ := owner.snapshot(t.Context())
	request.Manifest = nil
	if request.Attempt > 0 {
		stored := snapshot.State.Attempts[ownerAttemptKey("o/r", request.Issue, request.Attempt)].Manifest
		request.Manifest = &stored
	}
	return root, owner, snapshot
}

func configureEffectFixture(root string, request reconciliationEffectRequest, manifest *agentruntime.Manifest, head string) reconciliationEffectRequest {
	manifest.ReviewState, manifest.ReviewMode, manifest.ReviewTarget = "", "", ""
	manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = "", "", "", ""
	manifest.ReviewFindings, manifest.ReviewHandoffQueued, manifest.ReviewHandoffAck = nil, false, false
	switch request.Action {
	case reconciliationGitHubBind:
		manifest.State = "preparing"
	case reconciliationGitHubPublish:
		manifest.State, manifest.ReviewState, manifest.ReviewMode = "completed", "clean", agentruntime.ReviewModeImplementation
		publishedHead := head
		if request.GitHubPublish.Prepared != nil {
			publishedHead = request.GitHubPublish.Prepared.HeadSHA
		}
		manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = manifest.BaseSHA, publishedHead, manifest.BaseSHA+".."+publishedHead
		request.GitHubPublish.HeadSHA = publishedHead
	case reconciliationGitHubPRGovernance:
		request.GitHubPRGovernance.HeadSHA = head
	case reconciliationReviewer:
		snapshot, session := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, productionSnapshotRoot(root))
		request.Reviewer.Snapshot, request.Reviewer.Session = snapshot, session
		if request.Reviewer.Mode == agentruntime.ReviewModeImplementation {
			manifest.State = "completed"
			request.Reviewer.BaseSHA, request.Reviewer.HeadSHA = manifest.BaseSHA, head
			request.Reviewer.Target = manifest.BaseSHA + ".." + head
		} else {
			manifest.State = "running"
			request.Reviewer.BaseSHA, request.Reviewer.HeadSHA = manifest.BaseSHA, manifest.BaseSHA
			request.Reviewer.Target = fmt.Sprintf("%s#%d plan sha256:%x", request.Repository, request.Issue, sha256Sum("body"))
		}
		if request.Reviewer.Phase == "cleanup" {
			manifest.ReviewState, manifest.ReviewMode, manifest.ReviewTarget = "clean", request.Reviewer.Mode, request.Reviewer.Target
			manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = request.Reviewer.BaseSHA, request.Reviewer.HeadSHA, snapshot, session
		}
	case reconciliationHandoffDeliver:
		if request.Handoff.Kind == "review-findings" {
			manifest.State, manifest.ReviewState, manifest.ReviewMode = "completed", "findings-queued", agentruntime.ReviewModeImplementation
			manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = manifest.BaseSHA, head, manifest.BaseSHA+".."+head
			manifest.ReviewFindings = slices.Clone(request.Handoff.Findings)
			request.Handoff.HeadSHA, request.Handoff.Key, request.Handoff.OutcomeToken = head, "independent-review-"+head, head
		} else {
			request.Handoff.Recovery.HeadSHA = head
			request.Handoff.Recovery.ValidationGeneration = 1
			request.Handoff.Recovery.Key = recoveryHandoffKey(*request.Handoff.Recovery)
			request.Handoff.Key = request.Handoff.Recovery.Key
			request.Handoff.OutcomeToken = fmt.Sprintf("%x", sha256Sum("handoff-outcome\x00"+request.Handoff.Key))
			if request.Handoff.Outcome != nil {
				request.Handoff.Outcome.Key = request.Handoff.Key
			}
		}
	case reconciliationRetireCompleted:
		manifest.State, manifest.ReviewHead = "running", head
		request.Retire.HeadSHA = head
	case reconciliationGitHubIssueUpdate:
		switch request.GitHubIssueUpdate.Kind {
		case githubIssueTerminalFailure, githubIssueRetry:
			manifest.State = "failed"
		case githubIssueEvidence:
			manifest.State, manifest.ReviewHead = "completed", head
			request.GitHubIssueUpdate.HeadSHA = head
		case githubIssueFindings:
			manifest.State, manifest.ReviewState, manifest.ReviewMode = "completed", "findings-queued", agentruntime.ReviewModeImplementation
			manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = manifest.BaseSHA, head, manifest.BaseSHA+".."+head
			manifest.ReviewFindings = slices.Clone(request.GitHubIssueUpdate.Findings)
			request.GitHubIssueUpdate.HeadSHA = head
		}
	}
	request.Manifest = manifest
	return request
}

func reconciliationEffectObservationInput(request reconciliationEffectRequest, title string) reconciliationInput {
	issue := issueFact(request.Issue, title)
	issue.BaseBranch, issue.DispatchAuthorized, issue.Attempt, issue.CurrentAttempt = "main", true, request.Attempt, request.Attempt
	if request.Manifest != nil {
		issue.BaseSHA = request.Manifest.BaseSHA
	}
	if request.GitHubIssueUpdate != nil && request.GitHubIssueUpdate.Kind == githubIssueDependencyClear {
		issue.Dependencies = []int{request.GitHubIssueUpdate.Dependency}
		issue.SatisfiedDependencies = []int{request.GitHubIssueUpdate.Dependency}
		issue.Attempt, issue.CurrentAttempt = 1, 1
		active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: request.Issue, Attempt: 1, State: "active"}
		issue.ActiveAttempt = &active
	}
	input := repositoryInput(true, issue)
	if request.GitHubIssueUpdate != nil && (request.GitHubIssueUpdate.Kind == githubIssueControlSnapshot || request.GitHubIssueUpdate.Kind == githubIssueDependencyClear) {
		input.IssueUpdates = []reconciliationIssueUpdateProposal{{Repository: request.Repository, Issue: request.Issue, Kind: request.GitHubIssueUpdate.Kind, ControlSnapshotDigest: request.GitHubIssueUpdate.ControlSnapshotDigest, AttributionAttempt: request.GitHubIssueUpdate.AttributionAttempt, Dependency: request.GitHubIssueUpdate.Dependency, PullRequest: request.GitHubIssueUpdate.PullRequest}}
		if request.GitHubIssueUpdate.Kind == githubIssueDependencyClear {
			input.Attempts = []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: request.Issue, Attempt: 1, PR: request.GitHubIssueUpdate.PullRequest, State: "active"}}
		}
	}
	if request.Attempt > 0 {
		head := strings.Repeat("b", 40)
		fact := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: request.Issue, Attempt: request.Attempt, PR: 7, BaseSHA: request.Manifest.BaseSHA, HeadSHA: head, State: "completed"}
		if request.Action == reconciliationGitHubPRGovernance || request.Action == reconciliationHandoffDeliver {
			fact.State, fact.PublicationConfirmed = "active", true
		}
		if request.Action == reconciliationGitHubBind || request.Reviewer != nil && request.Reviewer.Mode == agentruntime.ReviewModePlan {
			fact.HeadSHA, fact.State = "", "active"
		}
		if request.GitHubIssueUpdate != nil && (request.GitHubIssueUpdate.Kind == githubIssueTerminalFailure || request.GitHubIssueUpdate.Kind == githubIssueRetry || request.GitHubIssueUpdate.Kind == githubIssueFindings) {
			issue.ActiveAttempt, issue.TerminalAttempts = nil, nil
			input.Issues[0] = issue
			return input
		}
		issue.ActiveAttempt, issue.TerminalAttempts = &fact, []internalgithub.RecoveryAttemptFact{fact}
		input.Issues[0] = issue
		input.Attempts = []internalgithub.RecoveryAttemptFact{fact}
	}
	return input
}

func recoveryHandoffKey(handoff internalgithub.RecoveryHandoff) string {
	identities := make([]string, len(handoff.Feedback))
	for index, feedback := range handoff.Feedback {
		identities[index] = fmt.Sprintf("%s:%d", feedback.Source, feedback.ID)
	}
	slices.Sort(identities)
	return fmt.Sprintf("%x", sha256Sum(fmt.Sprintf("%s\x00%d\x00%d\x00%d\x00%s\x00%d\x00%s", handoff.Repository, handoff.PR, handoff.Issue, handoff.Attempt, handoff.HeadSHA, handoff.ValidationGeneration, strings.Join(identities, ","))))
}

func bindEffectObservation(snapshot stateOwnerSnapshot, request reconciliationEffectRequest) reconciliationEffectRequest {
	observation := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	request.ObservationGeneration, request.ObservationCycleID, request.BodyDigest = observation.Generation, observation.LastCycleID, observation.Fact.BodyDigest
	if request.Manifest != nil {
		stored := snapshot.State.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].Manifest
		request.Manifest = &stored
	}
	if request.GitHubBind != nil {
		request.GitHubBind.BaseSHA, request.GitHubBind.Branch = request.Manifest.BaseSHA, request.Manifest.Branch
	}
	if request.GitHubIssueUpdate != nil && (request.GitHubIssueUpdate.Kind == githubIssueTerminalFailure || request.GitHubIssueUpdate.Kind == githubIssueRetry) {
		request.GitHubIssueUpdate.FailedAtUnixNano = request.Manifest.UpdatedAt.UnixNano()
	}
	if request.Reviewer != nil && request.Reviewer.Mode == agentruntime.ReviewModePlan {
		request.Reviewer.Target = fmt.Sprintf("%s#%d plan sha256:%s", request.Repository, request.Issue, request.BodyDigest)
	}
	if request.Handoff != nil && request.Handoff.Kind == "review-findings" {
		request.Handoff.HeadSHA, request.Handoff.OutcomeToken = request.Manifest.ReviewHead, request.Manifest.ReviewHead
		request.Handoff.OutcomePath = handoffReceiptPath(request.Manifest.Worktree, request.Handoff.Key)
	}
	if request.Handoff != nil && request.Handoff.Kind == "recovery" {
		request.Handoff.OutcomePath = handoffReceiptPath(request.Manifest.Worktree, request.Handoff.Key)
	}
	return request
}

func reconciliationBeginIdentity(snapshot stateOwnerSnapshot, request reconciliationEffectRequest) stateResultIdentity {
	return stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]}
}

func reconciliationIntentIdentity(effect runtimeEffectIntent) stateResultIdentity {
	return stateResultIdentity{Epoch: effect.IntentEpoch, SourceRevision: effect.IntentRevision, IssueGeneration: effect.IssueGeneration, AttemptGeneration: effect.AttemptGeneration, EffectID: effect.ID, RequestDigest: effect.RequestDigest}
}

func sha256Sum(value string) [32]byte { return sha256.Sum256([]byte(value)) }
