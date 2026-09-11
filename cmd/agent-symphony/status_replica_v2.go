package main

import (
	"cmp"
	"context"
	"errors"
	"slices"
	"sync"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type ownerStatusReplicaWriter struct {
	stateRoot           string
	mu                  sync.Mutex
	epoch, lastRevision uint64
}

func projectOwnerStatus(snapshot stateOwnerSnapshot, capacity int, now time.Time) (dashboardStatusSnapshot, error) {
	if snapshot.State.Epoch == 0 || snapshot.State.Revision == 0 || snapshot.State.Repository == "" || now.IsZero() {
		return dashboardStatusSnapshot{}, errors.New("owner status snapshot is invalid")
	}
	var remote []internalgithub.RecoveryAttemptFact
	var issues []internalgithub.RecoveryIssueFact
	for issueKey, observation := range snapshot.State.Observations {
		if !observation.Present || observation.OwnerGeneration != snapshot.State.IssueGenerations[issueKey] {
			continue
		}
		issue := expandIssueFact(observation.Fact)
		if active := issue.ActiveAttempt; active != nil {
			key := ownerAttemptKey(active.Repository, active.Issue, active.Attempt)
			if _, tombstoned := snapshot.State.Tombstones[key]; tombstoned {
				issue.ActiveAttempt = nil
			}
		}
		issue.TerminalAttempts = slices.DeleteFunc(issue.TerminalAttempts, func(attempt internalgithub.RecoveryAttemptFact) bool {
			_, tombstoned := snapshot.State.Tombstones[ownerAttemptKey(attempt.Repository, attempt.Issue, attempt.Attempt)]
			return tombstoned
		})
		issues = append(issues, issue)
		for attemptKey, accepted := range observation.Attempts {
			if !accepted.Present || accepted.SourceIssueGeneration != observation.Generation || accepted.OwnerGeneration != snapshot.State.AttemptGenerations[attemptKey] {
				continue
			}
			if _, tombstoned := snapshot.State.Tombstones[attemptKey]; !tombstoned {
				remote = append(remote, expandAttemptFact(accepted.Fact))
			}
		}
	}
	var manifests []agentruntime.Manifest
	for key, record := range snapshot.State.Attempts {
		if _, tombstoned := snapshot.State.Tombstones[key]; !tombstoned && record.Generation == snapshot.State.AttemptGenerations[key] {
			manifests = append(manifests, cloneManifest(record.Manifest))
		}
	}
	slices.SortFunc(issues, func(a, b internalgithub.RecoveryIssueFact) int { return cmp.Compare(a.Issue, b.Issue) })
	slices.SortFunc(remote, func(a, b internalgithub.RecoveryAttemptFact) int {
		if ordered := cmp.Compare(a.Issue, b.Issue); ordered != 0 {
			return ordered
		}
		return cmp.Compare(a.Attempt, b.Attempt)
	})
	_, facts := recoveryAttemptFacts(remote, issues)
	statuses, _ := projectRecoveryStatuses(context.Background(), facts, issues, manifests, capacity, nil)
	for _, effect := range snapshot.State.Effects {
		if effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Phase != "run-observe" {
			continue
		}
		reviewer := effect.Reconciliation.Reviewer
		for index := range statuses {
			status := &statuses[index]
			if status.Repository != effect.Repository || status.Issue != effect.Issue || status.Attempt != effect.Attempt || slices.ContainsFunc(status.Sessions, func(session orchestrator.AttemptSession) bool {
				return session.Role == agentruntime.SessionRoleReviewer
			}) {
				continue
			}
			status.Sessions = append(status.Sessions, orchestrator.AttemptSession{Role: agentruntime.SessionRoleReviewer, Name: reviewer.Session, State: "preparing", Mode: reviewer.Mode, Target: reviewer.Target, Current: true})
		}
	}
	for index := range statuses {
		status := &statuses[index]
		key := ownerAttemptKey(status.Repository, status.Issue, status.Attempt)
		if record, ok := snapshot.State.Attempts[key]; ok && record.Manifest.State == "completed" && (status.State == "failed" || status.State == "cancelled" || status.Retryable) {
			status.Retryable = false
			status.Action = "inspect inconsistent completed local attempt before recovery"
		}
	}
	slices.SortFunc(statuses, func(a, b orchestrator.RecoveryStatus) int {
		if ordered := cmp.Compare(a.Repository, b.Repository); ordered != 0 {
			return ordered
		}
		if ordered := cmp.Compare(a.Issue, b.Issue); ordered != 0 {
			return ordered
		}
		return cmp.Compare(a.Attempt, b.Attempt)
	})
	return dashboardStatusSnapshot{UpdatedAt: now.UTC(), OwnerEpoch: snapshot.State.Epoch, OwnerRevision: snapshot.State.Revision, Statuses: statuses}, nil
}

func expandIssueFact(fact reconciliationIssueFact) internalgithub.RecoveryIssueFact {
	var createdAt time.Time
	if fact.CreatedAtUnixNano != 0 {
		createdAt = time.Unix(0, fact.CreatedAtUnixNano)
	}
	result := internalgithub.RecoveryIssueFact{Repository: fact.Repository, Title: fact.Title, BaseSHA: fact.BaseSHA, BaseBranch: fact.BaseBranch, Issue: fact.Issue, Attempt: fact.Attempt, CurrentAttempt: fact.CurrentAttempt, Priority: fact.Priority, CreatedAt: createdAt, Dependencies: slices.Clone(fact.Dependencies), SatisfiedDependencies: slices.Clone(fact.SatisfiedDependencies), Paths: slices.Clone(fact.Paths), Blockers: slices.Clone(fact.Blockers), Eligible: fact.Eligible, Active: fact.Active, Completed: fact.Completed, Retry: fact.Retry, Cancelled: fact.Cancelled, Closed: fact.Closed, DispatchAuthorized: fact.DispatchAuthorized, RecoveryAuthorized: fact.RecoveryAuthorized, RecoveryAttempt: fact.RecoveryAttempt, NeedsAttention: fact.NeedsAttention}
	if fact.ActiveAttempt != nil {
		active := expandAttemptFact(*fact.ActiveAttempt)
		result.ActiveAttempt = &active
	}
	for _, terminal := range fact.TerminalAttempts {
		result.TerminalAttempts = append(result.TerminalAttempts, expandAttemptFact(terminal))
	}
	return result
}

func (w *ownerStatusReplicaWriter) write(snapshot dashboardStatusSnapshot) error {
	if w == nil || w.stateRoot == "" || snapshot.OwnerEpoch == 0 || snapshot.OwnerRevision == 0 {
		return errors.New("owner status writer is invalid")
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if snapshot.OwnerEpoch < w.epoch || snapshot.OwnerEpoch == w.epoch && snapshot.OwnerRevision <= w.lastRevision {
		return nil
	}
	if err := writeDashboardStatusSnapshot(w.stateRoot, snapshot); err != nil {
		return err
	}
	w.epoch, w.lastRevision = snapshot.OwnerEpoch, snapshot.OwnerRevision
	return nil
}
