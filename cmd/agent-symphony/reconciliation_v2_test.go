package main

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

func TestReconciliationCompleteScopeControlsAbsence(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	applyReconciliationInput(t, owner, repositoryInput(true, issueFact(71, "one"), issueFact(72, "two")))

	applyReconciliationInput(t, owner, reconciliationInput{Scope: issueScope(71), Complete: true})
	state, _ := owner.snapshot(t.Context())
	if state.State.Observations[ownerIssueKey("o/r", 71)].Present || !state.State.Observations[ownerIssueKey("o/r", 72)].Present {
		t.Fatalf("targeted absence affected the wrong issue: %#v", state.State.Observations)
	}

	applyReconciliationInput(t, owner, reconciliationInput{Scope: issueScope(72), Complete: false})
	state, _ = owner.snapshot(t.Context())
	if !state.State.Observations[ownerIssueKey("o/r", 72)].Present {
		t.Fatal("incomplete collection applied absence")
	}
}

func TestReconciliationCompleteScopeRecordsUnobservedOwnerAbsence(t *testing.T) {
	for _, scope := range []string{"repository", "issue"} {
		t.Run(scope, func(t *testing.T) {
			root := resolvedTempDir(t)
			owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			manifest := ownerTestManifest(t, root, 170, 1, "running")
			if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
				t.Fatal(err)
			}
			input := repositoryInput(true)
			if scope == "issue" {
				input.Scope = issueScope(170)
			}
			applyReconciliationInput(t, owner, input)
			state, _ := owner.snapshot(t.Context())
			observation, ok := state.State.Observations[ownerIssueKey("o/r", 170)]
			if !ok || observation.Present || observation.OwnerGeneration != 1 {
				t.Fatalf("unobserved owner issue absence=%#v", observation)
			}
		})
	}
}

func TestReconciliationRepositoryScopeIgnoresHistoricalGenerationOnlyKeys(t *testing.T) {
	root := resolvedTempDir(t)
	state := newRuntimeOwnerState("o/r")
	for issue := 1; issue <= maxReconciliationIssueCount+1; issue++ {
		state.IssueGenerations[ownerIssueKey("o/r", 2000+issue)] = 1
	}
	manifest := ownerTestManifest(t, root, 170, 1, "running")
	issueKey, attemptKey := ownerIssueKey("o/r", 170), ownerAttemptKey("o/r", 170, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	snapshot, _ := owner.reconciliationSnapshot(t.Context())
	collection, err := collectionFromSnapshot(snapshot, repositoryInput(true))
	if err != nil || len(collection.IssueGenerations) != 1 || len(collection.AttemptGenerations) != 1 {
		t.Fatalf("historical generations leaked into repository collection: issues=%d attempts=%d err=%v", len(collection.IssueGenerations), len(collection.AttemptGenerations), err)
	}
	applied := applyCollection(t, owner, collection)
	if observation := applied.State.Observations[issueKey]; observation.Present || observation.OwnerGeneration != 1 {
		t.Fatalf("current attempt issue absence=%#v", observation)
	}
}

func TestReconciliationOverlappingCyclesAreOrderedAndExact(t *testing.T) {
	for _, order := range []string{"older-first", "newer-first"} {
		t.Run(order, func(t *testing.T) {
			owner := newReconciliationTestOwner(t)
			olderSnapshot, err := owner.reconciliationSnapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			newerSnapshot, err := owner.reconciliationSnapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			older := mustCollection(t, olderSnapshot, repositoryInput(true, issueFact(73, "older")))
			newer := mustCollection(t, newerSnapshot, repositoryInput(true, issueFact(73, "newer")))
			if order == "older-first" {
				applyCollection(t, owner, older)
				applyCollection(t, owner, newer)
			} else {
				applyCollection(t, owner, newer)
				applyCollection(t, owner, older)
			}
			state, _ := owner.snapshot(t.Context())
			if got := state.State.Observations[ownerIssueKey("o/r", 73)].Fact.Title; got != "newer" {
				t.Fatalf("out-of-order result won: %q", got)
			}
		})
	}

	owner := newReconciliationTestOwner(t)
	snapshot, _ := owner.reconciliationSnapshot(t.Context())
	collection := mustCollection(t, snapshot, repositoryInput(true, issueFact(74, "exact")))
	first := applyCollection(t, owner, collection)
	replayed := applyCollection(t, owner, collection)
	if replayed.State.Revision != first.State.Revision {
		t.Fatalf("exact replay committed a new revision: %d -> %d", first.State.Revision, replayed.State.Revision)
	}
	conflict := cloneReconciliationCollection(collection)
	conflict.Issues[0].Fact.Title = "conflict"
	if _, err := owner.applyReconciliation(t.Context(), conflict); !errors.Is(err, errStateConflict) {
		t.Fatalf("same-cycle conflict err=%v", err)
	}

	for _, order := range []string{"present-first", "absence-first"} {
		t.Run(order, func(t *testing.T) {
			owner := newReconciliationTestOwner(t)
			applyReconciliationInput(t, owner, repositoryInput(true, issueFact(74, "initial")))
			presentSnapshot, _ := owner.reconciliationSnapshot(t.Context())
			absenceSnapshot, _ := owner.reconciliationSnapshot(t.Context())
			present := mustCollection(t, presentSnapshot, reconciliationInput{Scope: issueScope(74), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issueFact(74, "old")}})
			absence := mustCollection(t, absenceSnapshot, reconciliationInput{Scope: issueScope(74), Complete: true})
			if order == "present-first" {
				applyCollection(t, owner, present)
				applyCollection(t, owner, absence)
			} else {
				applyCollection(t, owner, absence)
				applyCollection(t, owner, present)
			}
			state, _ := owner.snapshot(t.Context())
			if state.State.Observations[ownerIssueKey("o/r", 74)].Present {
				t.Fatal("older present result resurrected a newer absence")
			}
		})
	}
}

func TestReconciliationNewerIdenticalCycleIsDurableNoopButAdvancesOrdering(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	initial := applyReconciliationInput(t, owner, repositoryInput(true, issueFact(74, "same")))
	olderSnapshot, _ := owner.reconciliationSnapshot(t.Context())
	newerSnapshot, _ := owner.reconciliationSnapshot(t.Context())
	olderChanged := mustCollection(t, olderSnapshot, repositoryInput(true, issueFact(74, "stale change")))
	newerIdentical := mustCollection(t, newerSnapshot, repositoryInput(true, issueFact(74, "same")))
	noop := applyCollection(t, owner, newerIdentical)
	if noop.State.Revision != initial.State.Revision {
		t.Fatalf("identical newer cycle changed revision: %d -> %d", initial.State.Revision, noop.State.Revision)
	}
	applyCollection(t, owner, olderChanged)
	current, _ := owner.snapshot(t.Context())
	if current.State.Revision != initial.State.Revision+1 || current.State.StaleReconciliations != 1 || current.State.Observations[ownerIssueKey("o/r", 74)].Fact.Title != "same" {
		t.Fatalf("older changed result applied after identical high-water: %#v", current.State.Observations[ownerIssueKey("o/r", 74)])
	}
}

func TestReconciliationIssueChangeInvalidatesOlderAttempts(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	first := repositoryInput(true, issueFact(75, "first"))
	first.Attempts = []internalgithub.RecoveryAttemptFact{attemptFact(75, 1, "old")}
	applyReconciliationInput(t, owner, first)

	second := repositoryInput(false, issueFact(75, "changed"))
	applyReconciliationInput(t, owner, second)
	state, _ := owner.snapshot(t.Context())
	issue := state.State.Observations[ownerIssueKey("o/r", 75)]
	if issue.Generation != 2 || len(issue.Attempts) != 0 || state.State.IssueGenerations[ownerIssueKey("o/r", 75)] != 1 {
		t.Fatalf("issue change retained mixed-cycle attempts: %#v", issue)
	}
}

func TestReconciliationRejectsStaleEpochAndDiscardsStaleGeneration(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	snapshot, _ := owner.reconciliationSnapshot(t.Context())
	staleEpoch := mustCollection(t, snapshot, repositoryInput(true, issueFact(76, "stale")))
	staleEpoch.Identity.Epoch++
	if _, err := owner.applyReconciliation(t.Context(), staleEpoch); err != nil {
		t.Fatalf("stale epoch err=%v", err)
	}
	futureSnapshot, _ := owner.reconciliationSnapshot(t.Context())
	staleSource := mustCollection(t, futureSnapshot, repositoryInput(true, issueFact(76, "stale source")))
	staleSource.Identity.SourceRevision++
	if _, err := owner.applyReconciliation(t.Context(), staleSource); err != nil {
		t.Fatalf("stale source err=%v", err)
	}

	collection := mustCollection(t, snapshot, repositoryInput(true, issueFact(76, "stale")))
	if _, err := owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: 76}); err != nil {
		t.Fatal(err)
	}
	applyCollection(t, owner, collection)
	state, _ := owner.snapshot(t.Context())
	if _, exists := state.State.Observations[ownerIssueKey("o/r", 76)]; exists {
		t.Fatal("stale issue generation restored an observation")
	}
	if state.State.StaleReconciliations != 3 {
		t.Fatalf("stale issue result count=%d", state.State.StaleReconciliations)
	}

	root := resolvedTempDir(t)
	attemptOwner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = attemptOwner.close(context.Background()) })
	manifest := ownerTestManifest(t, root, 176, 1, "running")
	if _, err := attemptOwner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	attemptSnapshot, _ := attemptOwner.reconciliationSnapshot(t.Context())
	attemptInput := repositoryInput(true, issueFact(176, "issue"))
	attemptInput.Attempts = []internalgithub.RecoveryAttemptFact{attemptFact(176, 1, "stale")}
	attemptCollection := mustCollection(t, attemptSnapshot, attemptInput)
	advanceTestAttemptGeneration(t, attemptOwner, manifest)
	applyCollection(t, attemptOwner, attemptCollection)
	attemptState, _ := attemptOwner.snapshot(t.Context())
	if len(attemptState.State.Observations[ownerIssueKey("o/r", 176)].Attempts) != 0 {
		t.Fatal("stale attempt generation restored an observation")
	}
	if attemptState.State.StaleReconciliations != 1 {
		t.Fatalf("stale attempt result count=%d", attemptState.State.StaleReconciliations)
	}
}

func TestCompleteAttemptAbsenceWithChangedGenerationCountsStaleOnce(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 178, 1, "running")
	issueKey, attemptKey := ownerIssueKey("o/r", 178), ownerAttemptKey("o/r", 178, 1)
	fact := attemptFact(178, 1, "old generation")
	issue := issueFact(178, "issue")
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 2
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 2, Manifest: manifest}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	initial := repositoryInput(true, issue)
	initial.Attempts = []internalgithub.RecoveryAttemptFact{fact}
	applyReconciliationInput(t, owner, initial)
	snapshot, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	collection := mustCollection(t, snapshot, repositoryInput(true, issue))
	collection.AttemptGenerations[attemptKey] = 1 // The complete scan captured the prior attempt generation.
	applied := applyCollection(t, owner, collection)
	if applied.State.StaleReconciliations != 1 {
		t.Fatalf("stale complete-absence count=%d want 1", applied.State.StaleReconciliations)
	}
	if !applied.State.Observations[issueKey].Attempts[attemptKey].Present {
		t.Fatal("stale complete absence removed the newer-generation attempt observation")
	}
}

func TestReconciliationTombstoneMasksAttempt(t *testing.T) {
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	manifest := ownerTestManifest(t, root, 77, 1, "running")
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, _ := owner.reconciliationSnapshot(t.Context())
	remote := attemptFact(77, 1, "remote")
	issue := issueFact(77, "issue")
	issue.ActiveAttempt = &remote
	issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{remote}
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	collection := mustCollection(t, snapshot, input)
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 77, Attempt: 1, ExpectedIssueGeneration: created.State.IssueGenerations[ownerIssueKey("o/r", 77)], ExpectedAttemptGeneration: 1, Action: "dismissed", CleanupPhase: "completed"}); err != nil {
		t.Fatal(err)
	}
	applyCollection(t, owner, collection)
	state, _ := owner.snapshot(t.Context())
	observation := state.State.Observations[ownerIssueKey("o/r", 77)]
	if len(observation.Attempts) != 0 || observation.Fact.ActiveAttempt != nil || len(observation.Fact.TerminalAttempts) != 0 {
		t.Fatalf("tombstoned attempt was observed: %#v", observation)
	}
}

func TestReconciliationGenerationAdvanceMasksNestedAttemptFacts(t *testing.T) {
	for _, nested := range []string{"active", "terminal"} {
		t.Run(nested, func(t *testing.T) {
			root := resolvedTempDir(t)
			owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			manifest := ownerTestManifest(t, root, 177, 1, "running")
			if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
				t.Fatal(err)
			}
			snapshot, _ := owner.reconciliationSnapshot(t.Context())
			remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 177, Attempt: 1, PR: 9, BaseSHA: manifest.BaseSHA, HeadSHA: strings.Repeat("b", 40), State: "completed"}
			issue := issueFact(177, "stale")
			issue.Attempt, issue.CurrentAttempt, issue.BaseSHA = 1, 1, manifest.BaseSHA
			if nested == "active" {
				issue.ActiveAttempt = &remote
			} else {
				issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{remote}
			}
			collection := mustCollection(t, snapshot, repositoryInput(true, issue))
			advanceTestAttemptGeneration(t, owner, manifest)
			manifest.State, manifest.ReviewHead = "completed", remote.HeadSHA
			applied := applyCollection(t, owner, collection)
			observation := applied.State.Observations[ownerIssueKey("o/r", 177)]
			if observation.Fact.ActiveAttempt != nil || len(observation.Fact.TerminalAttempts) != 0 {
				t.Fatalf("stale nested attempt survived: %#v", observation.Fact)
			}
			request := reconciliationEffectRequest{Action: reconciliationGitHubIssueUpdate, Repository: "o/r", Issue: 177, Attempt: 1, Manifest: &manifest, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, ExecutionDigest: strings.Repeat("a", 64), GitHubIssueUpdate: &githubIssueUpdateEffectRequest{Kind: githubIssueEvidence, HeadSHA: remote.HeadSHA}}
			identity := stateResultIdentity{Epoch: applied.State.Epoch, SourceRevision: applied.State.Revision, IssueGeneration: 1, AttemptGeneration: 2}
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: request}); !errors.Is(err, errStaleStateResult) {
				t.Fatalf("stale nested fact admitted an effect: %v", err)
			}
		})
	}
}

func TestReconciliationFactsAreBoundedValidatedAndDeepCloned(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	snapshot, _ := owner.reconciliationSnapshot(t.Context())
	oversizedBody := issueFact(78, "large")
	oversizedBody.Body = strings.Repeat("x", maxReconciliationBodyBytes+1)
	if _, err := collectionFromSnapshot(snapshot, repositoryInput(true, oversizedBody)); err == nil {
		t.Fatal("oversized body was accepted")
	}
	malformed := issueFact(78, "bad\x00title")
	if _, err := collectionFromSnapshot(snapshot, repositoryInput(true, malformed)); err == nil {
		t.Fatal("malformed title was accepted")
	}
	orphanAttempt := repositoryInput(true)
	orphanAttempt.Attempts = []internalgithub.RecoveryAttemptFact{attemptFact(78, 1, "orphan")}
	orphanCollection, err := collectionFromSnapshot(snapshot, orphanAttempt)
	if err != nil || len(orphanCollection.Issues) != 0 {
		t.Fatalf("attempt without an in-scope issue group was not safely excluded: %#v err=%v", orphanCollection, err)
	}

	large := repositoryInput(true)
	block := strings.Repeat("x", maxReconciliationStringBytes)
	for issue := 1; issue <= 5; issue++ {
		fact := issueFact(80+issue, "large")
		fact.Blockers = make([]string, maxReconciliationRelationCount)
		for index := range fact.Blockers {
			fact.Blockers[index] = block
		}
		large.Issues = append(large.Issues, fact)
	}
	if _, err := collectionFromSnapshot(snapshot, large); err == nil || !strings.Contains(err.Error(), "durable bounds") {
		t.Fatalf("aggregate bound err=%v", err)
	}

	input := repositoryInput(true, issueFact(78, "clone"))
	input.Issues[0].Paths = []string{"a/b"}
	input.IssueUpdates = []reconciliationIssueUpdateProposal{{Repository: "o/r", Issue: 78, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: strings.Repeat("a", 64)}}
	collection := mustCollection(t, snapshot, input)
	applyCollection(t, owner, collection)
	collection.Issues[0].Fact.Paths[0] = "mutated"
	collection.Issues[0].IssueUpdates[0].ControlSnapshotDigest = strings.Repeat("b", 64)
	state, _ := owner.snapshot(t.Context())
	state.State.Observations[ownerIssueKey("o/r", 78)].Fact.Paths[0] = "aliased"
	stable, _ := owner.snapshot(t.Context())
	if got := stable.State.Observations[ownerIssueKey("o/r", 78)].Fact.Paths[0]; got != "a/b" {
		t.Fatalf("observation aliases caller: %q", got)
	}
	if got := stable.State.Observations[ownerIssueKey("o/r", 78)].IssueUpdates[0].ControlSnapshotDigest; got != strings.Repeat("a", 64) {
		t.Fatalf("proposal aliases caller: %q", got)
	}
	validationRoot := resolvedTempDir(t)
	if err := validateRuntimeOwnerState(stable.State, runtimeOwnerAttemptRoot(validationRoot), validationRoot, true); err != nil {
		t.Fatalf("persisted observation validation: %v", err)
	}
	if err := writeRuntimeOwnerState(validationRoot, runtimeOwnerAttemptRoot(validationRoot), stable.State); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(validationRoot, "o/r")
	if err != nil || loaded.Observations[ownerIssueKey("o/r", 78)].Fact.Paths[0] != "a/b" || loaded.Observations[ownerIssueKey("o/r", 78)].IssueUpdates[0].ControlSnapshotDigest != strings.Repeat("a", 64) {
		t.Fatalf("loaded observation=%#v err=%v", loaded.Observations, err)
	}
	inconsistent := cloneRuntimeOwnerState(loaded)
	observation := inconsistent.Observations[ownerIssueKey("o/r", 78)]
	observation.IssueUpdates = append(observation.IssueUpdates, reconciliationIssueUpdateProposal{Repository: "o/r", Issue: 78, Kind: githubIssueDependencyClear, Dependency: 9})
	inconsistent.Observations[ownerIssueKey("o/r", 78)] = observation
	if err := validateRuntimeOwnerState(inconsistent, runtimeOwnerAttemptRoot(validationRoot), validationRoot, true); err == nil {
		t.Fatal("fact-inconsistent persisted dependency proposal was accepted")
	}

	tooManyGenerations := mustCollection(t, snapshot, repositoryInput(false))
	for issue := 1; issue <= maxReconciliationIssueCount+1; issue++ {
		tooManyGenerations.IssueGenerations[ownerIssueKey("o/r", issue)] = 1
	}
	if err := validateReconciliationCollection("o/r", tooManyGenerations); err == nil {
		t.Fatal("oversized generation map was accepted")
	}

	historical := cloneRuntimeOwnerState(snapshot.State)
	for issue := 1; issue <= maxReconciliationIssueCount+1; issue++ {
		historical.IssueGenerations[ownerIssueKey("o/r", 2000+issue)] = 1
	}
	historicalSnapshot := stateOwnerSnapshot{State: historical, CycleID: snapshot.CycleID}
	targeted, err := collectionFromSnapshot(historicalSnapshot, reconciliationInput{Scope: issueScope(78), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issueFact(78, "targeted")}})
	if err != nil || len(targeted.IssueGenerations) != 0 {
		t.Fatalf("unrelated historical generations leaked into targeted collection: %#v err=%v", targeted.IssueGenerations, err)
	}

	manyChecks := attemptFact(78, 1, "checks")
	manyChecks.Checks = make([]string, 1100)
	for index := range manyChecks.Checks {
		manyChecks.Checks[index] = "success"
	}
	issueWithPaths := issueFact(78, "checks")
	issueWithPaths.Paths = make([]string, 300)
	for index := range issueWithPaths.Paths {
		issueWithPaths.Paths[index] = "path/to/file"
	}
	checksInput := reconciliationInput{Scope: issueScope(78), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issueWithPaths}, Attempts: []internalgithub.RecoveryAttemptFact{manyChecks}}
	if _, err := collectionFromSnapshot(snapshot, checksInput); err != nil {
		t.Fatalf("source-compatible check count rejected: %v", err)
	}

	tooManyProposals := repositoryInput(true, issueFact(78, "proposals"))
	tooManyProposals.IssueUpdates = make([]reconciliationIssueUpdateProposal, maxReconciliationProposalCount+1)
	if _, err := collectionFromSnapshot(snapshot, tooManyProposals); err == nil {
		t.Fatal("oversized proposal collection was accepted")
	}
}

func TestReconciliationRunnerRejectsMissingDependencies(t *testing.T) {
	if _, err := (reconciliationRunner{}).run(t.Context()); err == nil {
		t.Fatal("incomplete runner was accepted")
	}
	owner := newReconciliationTestOwner(t)
	if _, err := (reconciliationRunner{owner: owner}).run(t.Context()); err == nil {
		t.Fatal("runner without collector was accepted")
	}
}

func TestReconciliationCollectionDoesNotBlockOwnerMutations(t *testing.T) {
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	entered, release := make(chan struct{}), make(chan struct{})
	manifest := ownerTestManifest(t, root, 80, 1, "running")
	runner := reconciliationRunner{owner: owner, collect: func(_ context.Context, snapshot stateOwnerSnapshot) (reconciliationInput, error) {
		close(entered)
		snapshot.State.Repository = "mutated"
		snapshot.State.IssueGenerations[ownerIssueKey("o/r", 79)] = 99
		<-release
		return repositoryInput(true, issueFact(79, "collected")), nil
	}}
	result := make(chan error, 1)
	go func() {
		_, err := runner.run(t.Context())
		result <- err
	}()
	<-entered
	mutation := make(chan error, 1)
	go func() {
		_, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
		mutation <- err
	}()
	if err := <-mutation; err != nil {
		t.Fatalf("mutation blocked by collection: %v", err)
	}
	close(release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	state, _ := owner.snapshot(t.Context())
	if state.State.Repository != "o/r" || state.State.IssueGenerations[ownerIssueKey("o/r", 79)] != 1 {
		t.Fatalf("collector mutated its immutable snapshot source: %#v", state.State)
	}
}

func TestRecoverAdmissionRemainsResponsiveDuringBlockedReconciliation(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 78, "active", false)
	entered, release := make(chan struct{}), make(chan struct{})
	runner := reconciliationRunner{owner: owner, collect: func(ctx context.Context, _ stateOwnerSnapshot) (reconciliationInput, error) {
		close(entered)
		select {
		case <-release:
			return repositoryInput(true), nil
		case <-ctx.Done():
			return reconciliationInput{}, ctx.Err()
		}
	}}
	done := make(chan error, 1)
	go func() { _, err := runner.run(t.Context()); done <- err }()
	<-entered
	service := operatorTestMutationService(t, owner)
	service.effects.stopped = true
	result := service.perform(t.Context(), operatorRequest("recover-during-reconcile", "recover", manifest, false))
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("recover result=%#v", result)
	}
	close(release)
	if err := <-done; err != nil && !errors.Is(err, errStaleStateResult) {
		t.Fatal(err)
	}
}

func TestReconciliationTriggersCoalesceToOneRerun(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	entered := make(chan int, 3)
	release := []chan struct{}{make(chan struct{}), make(chan struct{}), make(chan struct{})}
	run := 0
	triggered, err := newReconciliationTriggerRunner(t.Context(), reconciliationRunner{owner: owner, collect: func(ctx context.Context, _ stateOwnerSnapshot) (reconciliationInput, error) {
		run++
		current := run
		entered <- current
		select {
		case <-release[current-1]:
			return repositoryInput(true, issueFact(80, "collected")), nil
		case <-ctx.Done():
			return reconciliationInput{}, ctx.Err()
		}
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := triggered.trigger(); err != nil {
		t.Fatal(err)
	}
	if got := <-entered; got != 1 {
		t.Fatalf("first run=%d", got)
	}
	for range 20 {
		if err := triggered.trigger(); err != nil {
			t.Fatal(err)
		}
	}
	close(release[0])
	if got := <-entered; got != 2 {
		t.Fatalf("rerun=%d", got)
	}
	close(release[1])
	if err := triggered.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	triggered.mu.Lock()
	runs := triggered.runs
	triggered.mu.Unlock()
	if runs != 2 {
		t.Fatalf("coalesced burst ran %d times, want 2", runs)
	}
	if err := triggered.trigger(); !errors.Is(err, errStateOwnerStopped) {
		t.Fatalf("post-shutdown trigger err=%v", err)
	}
}

func TestReconciliationTriggerShutdownCancelsAndDrains(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	entered := make(chan struct{})
	triggered, err := newReconciliationTriggerRunner(context.Background(), reconciliationRunner{owner: owner, collect: func(ctx context.Context, _ stateOwnerSnapshot) (reconciliationInput, error) {
		close(entered)
		<-ctx.Done()
		return reconciliationInput{}, ctx.Err()
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := triggered.trigger(); err != nil {
		t.Fatal(err)
	}
	<-entered
	if err := triggered.trigger(); err != nil {
		t.Fatal(err)
	}
	if err := triggered.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	triggered.mu.Lock()
	runs, lastErr := triggered.runs, triggered.lastErr
	triggered.mu.Unlock()
	if runs != 1 || !errors.Is(lastErr, context.Canceled) {
		t.Fatalf("shutdown runs=%d err=%v", runs, lastErr)
	}
}

func TestReconciliationWaiterDoesNotReturnWhenWakeIsOnlyConsumed(t *testing.T) {
	consumed, release, ran := make(chan struct{}), make(chan struct{}), make(chan struct{})
	triggered, err := newReconciliationTrigger(t.Context(), func(context.Context) error {
		close(ran)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	triggered.beforeRun = func() {
		close(consumed)
		<-release
	}
	result := make(chan error, 1)
	go func() { result <- triggered.triggerAndWait(t.Context()) }()
	<-consumed
	select {
	case err := <-result:
		t.Fatalf("wait returned before satisfying cycle ran: %v", err)
	default:
	}
	close(release)
	<-ran
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := triggered.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestReconciliationWaiterFollowsRequiredFreshCycle(t *testing.T) {
	first, release, second := make(chan struct{}), make(chan struct{}), make(chan struct{})
	runs := 0
	triggered, err := newReconciliationTrigger(t.Context(), func(context.Context) error {
		runs++
		if runs == 1 {
			close(first)
			<-release
			return errReconciliationRecollect
		}
		close(second)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- triggered.triggerAndWait(t.Context()) }()
	<-first
	close(release)
	<-second
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if err := triggered.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestCanceledReconciliationWaiterUnregistersWhileCycleRemainsBlocked(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	triggered, err := newReconciliationTrigger(t.Context(), func(context.Context) error {
		close(entered)
		<-release
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- triggered.triggerAndWait(ctx) }()
	<-entered
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiter result=%v", err)
	}
	triggered.mu.Lock()
	waiters, running := len(triggered.waiters), triggered.running
	triggered.mu.Unlock()
	if waiters != 0 || !running {
		t.Fatalf("waiters=%d running=%v", waiters, running)
	}
	close(release)
	if err := triggered.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func newReconciliationTestOwner(t *testing.T) *stateOwner {
	t.Helper()
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	return owner
}

func repositoryInput(complete bool, issues ...internalgithub.RecoveryIssueFact) reconciliationInput {
	return reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: "o/r"}, Complete: complete, Issues: issues}
}

func issueScope(issue int) reconciliationScope {
	return reconciliationScope{Kind: reconciliationIssueScope, Repository: "o/r", Issue: issue}
}

func issueFact(issue int, title string) internalgithub.RecoveryIssueFact {
	return internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: issue, Title: title, Body: "body"}
}

func attemptFact(issue, attempt int, diagnostic string) internalgithub.RecoveryAttemptFact {
	return internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: issue, Attempt: attempt, Diagnostic: diagnostic, Checks: []string{"ok"}}
}

func applyReconciliationInput(t *testing.T, owner *stateOwner, input reconciliationInput) stateOwnerSnapshot {
	t.Helper()
	snapshot, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return applyCollection(t, owner, mustCollection(t, snapshot, input))
}

func mustCollection(t *testing.T, snapshot stateOwnerSnapshot, input reconciliationInput) reconciliationCollection {
	t.Helper()
	collection, err := collectionFromSnapshot(snapshot, input)
	if err != nil {
		t.Fatal(err)
	}
	return collection
}

func applyCollection(t *testing.T, owner *stateOwner, collection reconciliationCollection) stateOwnerSnapshot {
	t.Helper()
	snapshot, err := owner.applyReconciliation(t.Context(), collection)
	if err != nil {
		t.Fatalf("apply reconciliation: %v collection=%#v", err, collection)
	}
	return snapshot
}
