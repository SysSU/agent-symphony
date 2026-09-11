package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
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
			owner := newReconciliationTestOwner(t)
			manifest := ownerTestManifest(t, owner.stateRoot, 186, 1, lifecycle)
			if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
				t.Fatal(err)
			}
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
			want, _ := projectRecoveryStatuses(context.Background(), facts, input.Issues, []agentruntime.Manifest{manifest}, 1, nil)
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
		t.Fatal(err)
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
