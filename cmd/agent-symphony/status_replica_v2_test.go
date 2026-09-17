package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestOwnerStatusReplicaIsRevisionTaggedAndRejectsOlderWrites(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	accepted := applyReconciliationInput(t, owner, repositoryInput(true, issueFact(185, "status")))
	newer, err := projectOwnerStatus(accepted, 1, time.Unix(20, 0))
	if err != nil || newer.OwnerEpoch != accepted.State.Epoch || newer.OwnerRevision != accepted.State.Revision {
		t.Fatalf("replica=%#v err=%v", newer, err)
	}
	root := resolvedTempDir(t)
	writer := ownerStatusReplicaWriter{stateRoot: root}
	if err := writer.write(newer); err != nil {
		t.Fatal(err)
	}
	older := newer
	older.OwnerRevision--
	older.UpdatedAt = time.Unix(10, 0)
	if err := writer.write(older); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(root, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	var stored dashboardStatusSnapshot
	if err := json.Unmarshal(body, &stored); err != nil || stored.OwnerRevision != newer.OwnerRevision || !stored.UpdatedAt.Equal(newer.UpdatedAt) {
		t.Fatalf("stored replica=%#v err=%v", stored, err)
	}
}

func TestOwnerStatusProjectionMatchesRecoveryProjection(t *testing.T) {
	for _, lifecycle := range []string{"preparing", "running", "failed", "cancelled", "completed"} {
		t.Run(lifecycle, func(t *testing.T) {
			root := resolvedTempDir(t)
			manifest := ownerTestManifest(t, root, 186, 1, lifecycle)
			if lifecycle == "failed" {
				manifest.Diagnostic = "test failure"
			}
			if lifecycle == "cancelled" {
				manifest.Diagnostic = "test cancellation"
			}
			state := runtimeEffectInitialState(manifest)
			state.Epoch, state.Revision = 1, 1
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			issue := issueFact(186, "projection")
			issue.Eligible = true
			input := repositoryInput(true, issue)
			if lifecycle == "running" || lifecycle == "preparing" {
				attempt := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 186, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active"}
				input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
				input.Issues[0].ActiveAttempt = &attempt
			}
			snapshot := applyReconciliationInput(t, owner, input)
			got, err := projectOwnerStatus(snapshot, 1, time.Unix(20, 0))
			if err != nil {
				t.Fatal(err)
			}
			_, facts := recoveryAttemptFacts(input.Attempts, input.Issues)
			committedLiveness := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
			want, _ := projectRecoveryStatuses(context.Background(), facts, input.Issues, []agentruntime.Manifest{manifest}, 1, committedLiveness)
			for index := range got.Statuses {
				if !validDigest(got.Statuses[index].OwnerCausalityToken) {
					t.Fatalf("owner projection lacks causality: %#v", got.Statuses[index])
				}
				got.Statuses[index].IssueGeneration, got.Statuses[index].AttemptGeneration, got.Statuses[index].MachineStatusSequence, got.Statuses[index].OwnerCausalityToken = 0, 0, 0, ""
			}
			if !reflect.DeepEqual(got.Statuses, want) {
				t.Fatalf("owner projection=%#v\nv1 projection=%#v", got.Statuses, want)
			}
		})
	}
}

func TestOwnerStatusProjectionPreservesIssueFactsAndOrdering(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	zero := issueFact(188, "zero")
	zero.CreatedAt = time.Time{}
	active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 188, Attempt: 2, State: "active", Checks: []string{"check"}}
	terminal := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 188, Attempt: 1, State: "failed", Diagnostic: "failed"}
	zero.ActiveAttempt, zero.TerminalAttempts = &active, []internalgithub.RecoveryAttemptFact{terminal}
	reduced, err := reduceIssueFact("o/r", zero)
	if err != nil {
		t.Fatal(err)
	}
	expanded := expandIssueFact(reduced)
	if !expanded.CreatedAt.IsZero() || expanded.ActiveAttempt == nil || expanded.ActiveAttempt.Attempt != 2 || len(expanded.TerminalAttempts) != 1 || expanded.TerminalAttempts[0].Attempt != 1 {
		t.Fatalf("issue fact lost projection fields: %#v", expanded)
	}
	input := repositoryInput(true, issueFact(189, "later"), zero)
	input.Attempts = []internalgithub.RecoveryAttemptFact{active, terminal}
	snapshot := applyReconciliationInput(t, owner, input)

	first, err := projectOwnerStatus(snapshot, 2, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	for range 20 {
		next, err := projectOwnerStatus(snapshot, 2, time.Unix(20, 0))
		if err != nil || !reflect.DeepEqual(next, first) {
			t.Fatalf("projection order changed: %#v != %#v err=%v", next, first, err)
		}
	}
}

func TestOwnerStatusProjectionMasksTombstonedAttempts(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-dependency-clear").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	masked, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.GitHubIssueUpdate.AttributionAttempt, ExpectedIssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], ExpectedAttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.GitHubIssueUpdate.AttributionAttempt)], Action: "dismissed", CleanupPhase: "completed"})
	if err != nil {
		current := mustOwnerSnapshot(t, owner)
		t.Fatalf("invalidate tombstoned projection fixture: %v attempts=%#v effects=%#v proofs=%#v", err, current.State.Attempts, current.State.Effects, current.State.ReviewerProofs)
	}
	status, err := projectOwnerStatus(masked, 1, time.Unix(20, 0))
	if err != nil {
		t.Fatal(err)
	}
	for _, projected := range status.Statuses {
		if projected.Issue == request.Issue && projected.Attempt == request.GitHubIssueUpdate.AttributionAttempt && projected.PR != 0 {
			t.Fatalf("tombstoned remote attempt was projected: %#v", projected)
		}
	}
}

func TestOwnerStatusProjectionKeepsDismissedAttemptHiddenWithUnresolvedGitHubMutation(t *testing.T) {
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	issueKey, attemptKey := ownerIssueKey("o/r", 330), ownerAttemptKey("o/r", 330, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 2, 2
	state.Tombstones[attemptKey] = runtimeTombstone{Repository: "o/r", Issue: 330, Attempt: 1, Action: "dismissed", CleanupPhase: "completed", InvalidatedGeneration: 1, Generation: 2, Revision: 1}
	request := reconciliationEffectRequest{Action: reconciliationGitHubPublish, Repository: "o/r", Issue: 330, Attempt: 1, GitHubPublish: &githubPublishEffectRequest{}}
	state.Effects["ambiguous"] = runtimeEffectIntent{Repository: "o/r", Issue: 330, Attempt: 1, State: "invalidated", Dispatched: true, Reconciliation: &request}

	projected, err := projectOwnerStatus(stateOwnerSnapshot{State: state}, 1, time.Unix(2, 0))
	if err != nil || len(projected.Statuses) != 0 {
		t.Fatalf("quarantine projection=%#v err=%v", projected.Statuses, err)
	}
}

func TestOwnerStatusProjectionDoesNotSynthesizeTombstonedAttemptFromStaleIssue(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 320, 1, "running")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(candidate runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), candidate)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	before := mustOwnerSnapshot(t, owner)
	projected, err := projectOwnerStatus(before, 1, time.Unix(1, 0))
	if err != nil || len(projected.Statuses) != 1 || projected.Statuses[0].Attempt != manifest.Attempt {
		t.Fatalf("initial status=%#v err=%v", projected.Statuses, err)
	}
	committed, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{
		Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt,
		ExpectedIssueGeneration:   before.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)],
		ExpectedAttemptGeneration: before.State.AttemptGenerations[key], Action: "dismissed", CleanupPhase: "completed",
	})
	if err != nil || committed.State.Tombstones[key].Action != "dismissed" {
		t.Fatalf("tombstone=%#v err=%v", committed.State.Tombstones[key], err)
	}
	projected, err = projectOwnerStatus(committed, 1, time.Unix(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	assertAbsent := func(statuses []orchestrator.RecoveryStatus) {
		t.Helper()
		for _, status := range statuses {
			if status.Repository == manifest.Repository && status.Issue == manifest.Issue && status.Attempt == manifest.Attempt {
				t.Fatalf("tombstoned attempt was synthesized from stale issue: %#v", status)
			}
		}
	}
	assertAbsent(projected.Statuses)
	if err := (&ownerStatusReplicaWriter{stateRoot: owner.stateRoot}).write(projected); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(filepath.Join(owner.stateRoot, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	var written dashboardStatusSnapshot
	if err := json.Unmarshal(body, &written); err != nil {
		t.Fatal(err)
	}
	assertAbsent(written.Statuses)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil || persisted.Tombstones[key].Action != "dismissed" {
		t.Fatalf("persisted tombstone=%#v err=%v", persisted.Tombstones[key], err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	stale := internalgithub.RecoveryAttemptFact{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, BaseSHA: manifest.BaseSHA, State: "active"}
	issue := issueFact(manifest.Issue, "stale GitHub marker")
	issue.Attempt, issue.CurrentAttempt, issue.Active, issue.ActiveAttempt = manifest.Attempt, manifest.Attempt, true, &stale
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{stale}
	recollected := applyReconciliationInput(t, restarted, input)
	if recollected.State.Tombstones[key].Action != "dismissed" {
		t.Fatalf("recollection lost tombstone: %#v", recollected.State.Tombstones[key])
	}
	projected, err = projectOwnerStatus(recollected, 1, time.Unix(3, 0))
	if err != nil {
		t.Fatal(err)
	}
	assertAbsent(projected.Statuses)
	replacement := stale
	replacement.Attempt++
	issue.Attempt, issue.CurrentAttempt, issue.ActiveAttempt = replacement.Attempt, replacement.Attempt, &replacement
	input = repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{replacement}
	advanced := applyReconciliationInput(t, restarted, input)
	projected, err = projectOwnerStatus(advanced, 1, time.Unix(4, 0))
	if err != nil {
		t.Fatal(err)
	}
	assertAbsent(projected.Statuses)
	if !slices.ContainsFunc(projected.Statuses, func(status orchestrator.RecoveryStatus) bool {
		return status.Repository == manifest.Repository && status.Issue == manifest.Issue && status.Attempt == replacement.Attempt
	}) {
		t.Fatalf("new distinct attempt was hidden: %#v", projected.Statuses)
	}
}

func TestOwnerStatusBlocksControlsWhenRemoteAndLocalCompletionDoNotMatch(t *testing.T) {
	for _, test := range []struct {
		name, localState, remoteState string
		owned                         bool
	}{{"remote-only-failed", "failed", "failed", false}, {"local-running-remote-completed", "running", "completed", true}, {"local-failed-remote-completed", "failed", "completed", true}} {
		t.Run(test.name, func(t *testing.T) {
			manifest := ownerTestManifest(t, resolvedTempDir(t), 318, 1, test.localState)
			state := runtimeEffectInitialState(manifest)
			addOperatorObservation(&state, manifest, test.remoteState, true)
			if !test.owned {
				delete(state.Attempts, ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt))
			}
			state.Epoch, state.Revision = 1, 1
			projected, err := projectOwnerStatus(stateOwnerSnapshot{State: state}, 1, time.Unix(1, 0))
			if err != nil || len(projected.Statuses) != 1 || !projected.Statuses[0].OperatorBlocked {
				t.Fatalf("status=%#v err=%v", projected, err)
			}
		})
	}
}
