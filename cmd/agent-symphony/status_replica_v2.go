package main

import (
	"cmp"
	"context"
	"errors"
	"io"
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

type ownerStatusReplica struct {
	done chan struct{}
}

func startOwnerStatusReplica(ctx context.Context, owner *stateOwner, capacity int, log io.Writer) (*ownerStatusReplica, error) {
	if ctx == nil || owner == nil || capacity < 1 || log == nil {
		return nil, errors.New("owner status replica is incomplete")
	}
	replica := &ownerStatusReplica{done: make(chan struct{})}
	writer := &ownerStatusReplicaWriter{stateRoot: owner.stateRoot}
	initial, err := owner.snapshot(ctx)
	if err != nil {
		return nil, err
	}
	status, err := projectOwnerStatus(initial, capacity, time.Now().UTC())
	if err != nil {
		return nil, err
	}
	if err := writer.write(status); err != nil {
		return nil, err
	}
	go func() {
		defer close(replica.done)
		for {
			select {
			case <-ctx.Done():
				return
			case snapshot, ok := <-owner.commits:
				if !ok {
					return
				}
				status, err := projectOwnerStatus(snapshot, capacity, time.Now().UTC())
				if err == nil {
					err = writer.write(status)
				}
				if err != nil {
					_, _ = io.WriteString(log, "status projection: "+internalgithub.Redact(err.Error())+"\n")
				}
			}
		}
	}()
	return replica, nil
}

func (r *ownerStatusReplica) wait(ctx context.Context) error {
	if r == nil {
		return nil
	}
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func projectOwnerStatus(snapshot stateOwnerSnapshot, capacity int, now time.Time) (dashboardStatusSnapshot, error) {
	if snapshot.State.Epoch == 0 || snapshot.State.Revision == 0 || snapshot.State.Repository == "" || now.IsZero() {
		return dashboardStatusSnapshot{}, errors.New("owner status snapshot is invalid")
	}
	var remote []internalgithub.RecoveryAttemptFact
	var issues []internalgithub.RecoveryIssueFact
	for issueKey, observation := range snapshot.State.Observations {
		if !observation.Present || observation.ObservationEpoch != snapshot.State.Epoch || observation.OwnerGeneration != snapshot.State.IssueGenerations[issueKey] {
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
	// A generation-current owner manifest is the last committed local runtime
	// observation. Slow liveness checks run as monitor effects and update that
	// manifest; projection must not reinterpret the absence of I/O here as a
	// failed liveness check.
	committedLiveness := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
	statuses, _ := projectRecoveryStatuses(context.Background(), facts, issues, manifests, capacity, committedLiveness)
	// Issue-level scheduling can synthesize a status from a stale scalar Attempt
	// even after its active binding and local/remote facts were removed.
	statuses = slices.DeleteFunc(statuses, func(status orchestrator.RecoveryStatus) bool {
		_, tombstoned := snapshot.State.Tombstones[ownerAttemptKey(status.Repository, status.Issue, status.Attempt)]
		return tombstoned
	})
	for _, effect := range snapshot.State.Effects {
		if effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Phase != "run-observe" {
			continue
		}
		reviewer := effect.Reconciliation.Reviewer
		for index := range statuses {
			status := &statuses[index]
			if status.Repository != effect.Repository || status.Issue != effect.Issue || status.Attempt != effect.Attempt {
				continue
			}
			phase := "preparing"
			if effect.ReviewerLaunched {
				phase = "running"
			}
			session := orchestrator.AttemptSession{Role: agentruntime.SessionRoleReviewer, Name: reviewer.Session, State: phase, Mode: reviewer.Mode, Target: reviewer.Target, Current: true}
			replaced := false
			for i := range status.Sessions {
				status.Sessions[i].Current = false
				if status.Sessions[i].Role == agentruntime.SessionRoleReviewer {
					status.Sessions[i] = session
					replaced = true
				}
			}
			if !replaced {
				status.Sessions = append(status.Sessions, session)
			}
			status.CurrentPhase = "review"
			status.Diagnostic = ""
			if len(status.Blockers) == 0 && (status.State == "active" || status.State == "review-ready") {
				status.Action = "monitor the independent reviewer session"
			}
		}
	}
	for index := range statuses {
		status := &statuses[index]
		key := ownerAttemptKey(status.Repository, status.Issue, status.Attempt)
		issueKey := ownerIssueKey(status.Repository, status.Issue)
		status.IssueGeneration = snapshot.State.IssueGenerations[issueKey]
		status.AttemptGeneration = snapshot.State.AttemptGenerations[key]
		status.MachineStatusSequence = snapshot.State.MachineStatuses[issueKey].Sequence
		observation := snapshot.State.Observations[issueKey]
		record, owned := snapshot.State.Attempts[key]
		accepted := observation.Attempts[key]
		status.OperatorBlocked = observation.ObservationEpoch != snapshot.State.Epoch || observation.OwnerGeneration != snapshot.State.IssueGenerations[issueKey] || !observation.Present && !owned ||
			(!owned || record.Generation != snapshot.State.AttemptGenerations[key]) && (!accepted.Present || accepted.OwnerGeneration != snapshot.State.AttemptGenerations[key] || accepted.ObservationEpoch != snapshot.State.Epoch) ||
			!owned && accepted.Fact.State != "completed" || owned && status.State == "completed" && record.Manifest.State != "completed"
		if record, ok := snapshot.State.Attempts[key]; ok && record.Manifest.State == "completed" && (status.State == "failed" || status.State == "cancelled" || status.Retryable) {
			status.Retryable = false
			status.Action = "inspect inconsistent completed local attempt before recovery"
		}
		for id, effect := range snapshot.State.Effects {
			if id == effect.ID && effect.State == "pending" && effect.Action == string(agentruntime.EffectStart) && effect.Repository == status.Repository && effect.Issue == status.Issue && effect.Attempt == status.Attempt && effect.IssueGeneration == snapshot.State.IssueGenerations[issueKey] && effect.AttemptGeneration == snapshot.State.AttemptGenerations[key] && owned && record.Generation == effect.AttemptGeneration && (effect.Diagnostic == "legacy launch identity unproved; manual migration required" || effect.Diagnostic == "pending Start launch identity or worker absence is unproved") {
				status.Diagnostic = effect.Diagnostic
				if effect.Diagnostic == "legacy launch identity unproved; manual migration required" {
					status.Action = "manually migrate the legacy implementation launch identity"
				} else {
					status.Action = "inspect the unproved implementation launch before retry"
				}
				status.NeedsAttention = true
				break
			}
		}
		if attemptHasUnprovedReviewer(snapshot.State, status.Repository, status.Issue, status.Attempt) {
			status.NeedsAttention = true
			status.DispatchAuthorized = false
			status.Retryable = false
			status.Diagnostic = "reviewer descendant absence is unproved; physical cleanup remains pending"
			status.Action = "archive, abandon, remove, or dismiss can hide the attempt while physical cleanup remains pending"
		}
		if diagnostic := snapshot.State.LegacyReviewerQuarantines[issueKey]; diagnostic != "" {
			status.NeedsAttention = true
			status.OperatorBlocked = true
			status.DispatchAuthorized = false
			status.Retryable = false
			status.CurrentPhase = "physical-unverified"
			status.Diagnostic = diagnostic
			status.Action = "inspect legacy reviewer descendants before reusing this issue"
		}
		if record.StopEffectID != "" {
			status.State = "blocked"
			status.CurrentPhase = "stop-pending"
			status.NeedsAttention = true
			status.OperatorBlocked = true
			status.Retryable = false
			status.DispatchAuthorized = false
			status.Blockers = append(status.Blockers, "physical stop remains pending")
			status.Diagnostic = "stop requested; reviewer descendant absence is unproved"
			status.Action = "wait for verified physical cleanup before retrying or dispatching"
		}
	}
	// A historical cleanup may have removed the attempt from ordinary recovery
	// projection. Keep its unresolved physical safety lease visible anyway.
	for _, tombstone := range snapshot.State.Tombstones {
		diagnostic := legacyReviewerDiagnostic(snapshot.State, tombstone.Repository, tombstone.Issue, tombstone.Attempt)
		if tombstone.ReviewerLeaseID != "" || attemptHasUnprovedReviewer(snapshot.State, tombstone.Repository, tombstone.Issue, tombstone.Attempt) {
			if diagnostic == "" {
				diagnostic = "reviewer descendant absence is unproved; physical cleanup remains pending"
			}
		}
		if tombstone.InvalidatedStart != nil {
			if diagnostic != "" {
				diagnostic += "; "
			}
			diagnostic += "implementation start candidate absence is unproved; physical cleanup remains pending"
		}
		if tombstone.InvalidatedHandoff != nil && !tombstone.HandoffCompensated {
			if diagnostic != "" {
				diagnostic += "; "
			}
			diagnostic += "implementation handoff compensation remains pending"
		}
		if diagnostic == "" {
			continue
		}
		found := false
		for index := range statuses {
			if statuses[index].Repository == tombstone.Repository && statuses[index].Issue == tombstone.Issue && statuses[index].Attempt == tombstone.Attempt {
				statuses[index].NeedsAttention = true
				statuses[index].OperatorBlocked = true
				statuses[index].DispatchAuthorized = false
				statuses[index].CurrentPhase = "physical-unverified"
				statuses[index].Diagnostic = diagnostic
				found = true
			}
		}
		if !found {
			statuses = append(statuses, orchestrator.RecoveryStatus{Repository: tombstone.Repository, Issue: tombstone.Issue, Attempt: tombstone.Attempt, State: "blocked", CurrentPhase: "physical-unverified", NeedsAttention: true, OperatorBlocked: true, Diagnostic: diagnostic, Action: "inspect legacy reviewer descendants before reusing this issue"})
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
	return dashboardStatusSnapshot{UpdatedAt: now.UTC(), OwnerEpoch: snapshot.State.Epoch, OwnerRevision: snapshot.State.Revision, Statuses: statuses, ReconciliationError: snapshot.State.CycleDiagnostic, ReconciliationErrorAt: snapshot.State.CycleDiagnosticAt}, nil
}

func legacyReviewerDiagnostic(state runtimeOwnerState, repository string, issue, attempt int) string {
	if diagnostic := state.LegacyReviewerQuarantines[ownerIssueKey(repository, issue)]; diagnostic != "" {
		return diagnostic
	}
	for _, proof := range state.ReviewerProofs {
		if proof.Repository == repository && proof.Issue == issue && proof.Attempt == attempt && proof.LegacyUnverified {
			return "legacy reviewer descendant absence is unverified; physical cleanup cannot be certified"
		}
	}
	return ""
}

func expandIssueFact(fact reconciliationIssueFact) internalgithub.RecoveryIssueFact {
	var createdAt time.Time
	if fact.CreatedAtUnixNano != 0 {
		createdAt = time.Unix(0, fact.CreatedAtUnixNano)
	}
	result := internalgithub.RecoveryIssueFact{Repository: fact.Repository, Title: fact.Title, BaseSHA: fact.BaseSHA, BaseBranch: fact.BaseBranch, Issue: fact.Issue, Attempt: fact.Attempt, CurrentAttempt: fact.CurrentAttempt, Priority: fact.Priority, CreatedAt: createdAt, Dependencies: slices.Clone(fact.Dependencies), SatisfiedDependencies: slices.Clone(fact.SatisfiedDependencies), Paths: slices.Clone(fact.Paths), Blockers: slices.Clone(fact.Blockers), Eligible: fact.Eligible, Active: fact.Active, Completed: fact.Completed, Retry: fact.Retry, Cancelled: fact.Cancelled, Closed: fact.Closed, DispatchAuthorized: fact.DispatchAuthorized, RecoveryAuthorized: fact.RecoveryAuthorized, RecoveryAttempt: fact.RecoveryAttempt, NeedsAttention: fact.NeedsAttention, MachineStatusProtocol: fact.MachineStatusProtocol, MachineStatusAttempt: fact.MachineStatusAttempt, MachineStatusSequence: fact.MachineStatusSequence, MachineStatusNeedsAttention: fact.MachineStatusNeedsAttention, MachineStatusReason: fact.MachineStatusReason}
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
