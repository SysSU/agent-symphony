package main

import (
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const (
	operatorPhaseAdmissionPending = "admission-pending"
	operatorPhaseCleanupPending   = "cleanup-pending"
	operatorPhaseCleanupStarted   = "cleanup-started"
	operatorPhaseStopPending      = "stop-pending"
	operatorPhaseTerminalAwait    = "terminal-awaiting"
	operatorPhaseTerminal         = "terminal-pending"
	operatorPhaseRetryAwait       = "retry-awaiting"
	operatorPhaseRetryPending     = "retry-pending"
	operatorPhaseReviewPending    = "review-pending"
	operatorPhaseHandoffCleanup   = "handoff-cleanup-pending"
	operatorPhaseStartCleanup     = "start-cleanup-pending"
	operatorPhaseCompleted        = "completed"
)

func applyReserveOperatorAdmission(state *runtimeOwnerState, command reserveOperatorAdmissionCommand) error {
	request := command.Request
	if !validOperatorRequest(request, state.Repository) || !slices.Contains([]string{"dismiss", "archive", "abandon", "remove", "cancel", "recover"}, request.Action) {
		return errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if (request.Action == "cancel" || request.Action == "recover") && state.Attempts[key].Manifest.State == "preparing" {
		return errStateConflict
	}
	if receipt, ok := operatorReceiptByID(*state, request.RequestID); ok {
		if receipt.Request != request {
			return errStateConflict
		}
		return nil
	}
	for _, receipt := range state.ControlReceipts {
		if receipt.State == "pending" && receipt.Phase == operatorPhaseAdmissionPending && ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt) == key {
			if receipt.Request.Action != request.Action || receipt.Admission == nil {
				return errStateConflict
			}
			admission := *receipt.Admission
			admission.Manifest = cloneManifest(admission.Manifest)
			return appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: operatorPhaseAdmissionPending, Admission: &admission})
		}
	}
	record, ok := state.Attempts[key]
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	if !ok {
		if !slices.Contains([]string{"archive", "dismiss"}, request.Action) {
			return errStaleStateResult
		}
		observation, observed := state.Observations[issueKey]
		attempt, accepted := observation.Attempts[key]
		if !observed || !observation.Present || observation.ObservationEpoch > state.Epoch || observation.OwnerGeneration != state.IssueGenerations[issueKey] || observation.Fact.CurrentAttempt != request.Attempt || request.Action == "dismiss" && !observation.Fact.Closed || !accepted || !attempt.Present || attempt.ObservationEpoch > state.Epoch || attempt.SourceIssueGeneration != observation.Generation || attempt.OwnerGeneration != state.AttemptGenerations[key] || attempt.Fact.State != "completed" || attempt.Fact.Repository != request.Repository || attempt.Fact.Issue != request.Issue || attempt.Fact.Attempt != request.Attempt {
			return errStateConflict
		}
		admission := &operatorAdmission{Epoch: state.Epoch, IssueGeneration: state.IssueGenerations[issueKey], AttemptGeneration: state.AttemptGenerations[key], IssueClosed: request.Action == "dismiss", RemoteOnly: true, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest}
		return appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: operatorPhaseAdmissionPending, Admission: admission})
	}
	if record.Generation == 0 || record.Generation != state.AttemptGenerations[key] {
		return errStaleStateResult
	}
	status, statuses, err := ownerOperatorStatus(*state, request.Issue, request.Attempt)
	if err != nil {
		return errors.Join(err, errStateConflict)
	}
	switch request.Action {
	case "dismiss":
		observation, observed := state.Observations[issueKey]
		if !observed || observation.ObservationEpoch > state.Epoch {
			return errStaleStateResult
		}
		status.IssueClosed = true
		if !canDismissClosedAttempt(status) {
			return errStateConflict
		}
	case "archive", "abandon", "remove":
		if !validDestructiveOperatorStatus(request.Action, status, statuses) {
			return errStateConflict
		}
	}
	admission := &operatorAdmission{Epoch: state.Epoch, IssueGeneration: state.IssueGenerations[issueKey], AttemptGeneration: record.Generation, Manifest: cloneManifest(record.Manifest)}
	if request.Action == "recover" || request.Action == "dismiss" {
		observation := state.Observations[issueKey]
		admission.ObservationGeneration, admission.ObservationCycleID, admission.ObservationBodyDigest = observation.Generation, observation.LastCycleID, observation.Fact.BodyDigest
	}
	return appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: operatorPhaseAdmissionPending, Admission: admission})
}

func applyAbortOperatorAdmission(state *runtimeOwnerState, command abortOperatorAdmissionCommand) error {
	for index, receipt := range state.ControlReceipts {
		if receipt.Request.RequestID != command.Request.RequestID {
			continue
		}
		if receipt.Request != command.Request || receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil {
			return errStateConflict
		}
		state.ControlReceipts = slices.Delete(state.ControlReceipts, index, index+1)
		return nil
	}
	return nil
}

func applyFailOperatorAdmission(state *runtimeOwnerState, command failOperatorAdmissionCommand) error {
	var admission *operatorAdmission
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.Request.RequestID != command.Request.RequestID {
			continue
		}
		if receipt.Request != command.Request || receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil || state.Revision == ^uint64(0) || command.Result.OK || command.Result.Status < 400 || !validRecordedControlResult(command.Result, command.Request) {
			return errStateConflict
		}
		copy := *receipt.Admission
		admission = &copy
		break
	}
	if admission == nil {
		return errStaleStateResult
	}
	key := ownerAttemptKey(command.Request.Repository, command.Request.Issue, command.Request.Attempt)
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil || receipt.Request.Action != command.Request.Action || ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt) != key || !reflect.DeepEqual(*receipt.Admission, *admission) {
			continue
		}
		result := command.Result
		result.RequestID, result.Action, result.OwnerRevision = receipt.Request.RequestID, receipt.Request.Action, state.Revision+1
		receipt.State, receipt.Phase, receipt.Admission, receipt.Result = "completed", operatorPhaseCompleted, nil, &result
	}
	return nil
}

func matchingOperatorAdmissions(state runtimeOwnerState, request controlRequest) []controlRequest {
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	if !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil {
		return nil
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	requests := []controlRequest{}
	for _, candidate := range state.ControlReceipts {
		if candidate.State == "pending" && candidate.Phase == operatorPhaseAdmissionPending && candidate.Admission != nil && candidate.Request.Action == request.Action && ownerAttemptKey(candidate.Request.Repository, candidate.Request.Issue, candidate.Request.Attempt) == key && reflect.DeepEqual(candidate.Admission, receipt.Admission) {
			requests = append(requests, candidate.Request)
		}
	}
	return requests
}

func operatorAdmissionLeader(state runtimeOwnerState, receipt controlReceipt) bool {
	for _, candidate := range state.ControlReceipts {
		if candidate.State == "pending" && candidate.Phase == operatorPhaseAdmissionPending && candidate.Admission != nil && candidate.Request.Action == receipt.Request.Action && ownerAttemptKey(candidate.Request.Repository, candidate.Request.Issue, candidate.Request.Attempt) == ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt) && reflect.DeepEqual(candidate.Admission, receipt.Admission) {
			return candidate.Request.RequestID == receipt.Request.RequestID
		}
	}
	return false
}

func hasObservationSensitiveAdmission(state runtimeOwnerState, issueKey string) bool {
	return slices.ContainsFunc(state.ControlReceipts, func(receipt controlReceipt) bool {
		return receipt.State == "pending" && receipt.Phase == operatorPhaseAdmissionPending && observationSensitiveOperatorAdmission(receipt) && ownerIssueKey(receipt.Request.Repository, receipt.Request.Issue) == issueKey
	})
}

func observationSensitiveOperatorAdmission(receipt controlReceipt) bool {
	return receipt.Admission != nil && (slices.Contains([]string{"dismiss", "recover"}, receipt.Request.Action) || receipt.Request.Action == "archive" && receipt.Admission.RemoteOnly)
}

func remoteOnlyOperatorAdmissionEligible(state runtimeOwnerState, receipt controlReceipt, observation reconciliationObservation) bool {
	admission := receipt.Admission
	issueKey := ownerIssueKey(receipt.Request.Repository, receipt.Request.Issue)
	attemptKey := ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt)
	attempt, accepted := observation.Attempts[attemptKey]
	_, owned := state.Attempts[attemptKey]
	_, invalidated := state.Tombstones[attemptKey]
	return admission != nil && admission.RemoteOnly && slices.Contains([]string{"archive", "dismiss"}, receipt.Request.Action) && !owned && !invalidated &&
		observation.Present && observation.ObservationEpoch <= state.Epoch && observation.OwnerGeneration == admission.IssueGeneration && observation.Fact.CurrentAttempt == receipt.Request.Attempt &&
		(receipt.Request.Action != "dismiss" || admission.IssueClosed && observation.Fact.Closed) && accepted && attempt.Present && attempt.ObservationEpoch <= state.Epoch &&
		attempt.SourceIssueGeneration == observation.Generation && attempt.OwnerGeneration == admission.AttemptGeneration && attempt.Fact.State == "completed" &&
		attempt.Fact.Repository == receipt.Request.Repository && attempt.Fact.Issue == receipt.Request.Issue && attempt.Fact.Attempt == receipt.Request.Attempt &&
		!attemptHasReviewerProof(state, receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt) && state.LegacyReviewerQuarantines[issueKey] == ""
}

func reconcileOperatorAdmissions(state *runtimeOwnerState, collection reconciliationCollection) {
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || !observationSensitiveOperatorAdmission(*receipt) {
			continue
		}
		issueKey := ownerIssueKey(receipt.Request.Repository, receipt.Request.Issue)
		if !reconciliationScopeContains(collection.Scope, issueKey) {
			continue
		}
		observation, observed := state.Observations[issueKey]
		if !observed || observation.ObservationEpoch != collection.Identity.Epoch || observation.LastCycleID != collection.Identity.CycleID {
			continue
		}
		if receipt.Admission.ObservationGeneration == observation.Generation && receipt.Admission.ObservationCycleID == observation.LastCycleID && receipt.Admission.ObservationBodyDigest == observation.Fact.BodyDigest {
			continue
		}
		if receipt.Admission.RemoteOnly && !remoteOnlyOperatorAdmissionEligible(*state, *receipt, observation) || !receipt.Admission.RemoteOnly && (receipt.Request.Action == "recover" && !observation.Present || receipt.Request.Action == "dismiss" && observation.Present && !observation.Fact.Closed) {
			result := operatorErrorResult(receipt.Request, http.StatusConflict, "attempt or issue changed; refresh and retry")
			result.OwnerRevision = state.Revision + 1
			receipt.State, receipt.Phase, receipt.Admission, receipt.Result = "completed", operatorPhaseCompleted, nil, &result
			continue
		}
		receipt.Admission.ObservationGeneration = observation.Generation
		receipt.Admission.ObservationCycleID = observation.LastCycleID
		receipt.Admission.ObservationBodyDigest = observation.Fact.BodyDigest
	}
}

func validateOperatorAdmissionRefresh(state runtimeOwnerState, command applyReconciliationCommand) error {
	receipt, ok := operatorReceiptByID(state, command.OperatorRequestID)
	if !ok || receipt.Request.Action != "recover" || receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil {
		return errStaleStateResult
	}
	admission := receipt.Admission
	issueKey := ownerIssueKey(receipt.Request.Repository, receipt.Request.Issue)
	attemptKey := ownerAttemptKey(receipt.Request.Repository, receipt.Request.Issue, receipt.Request.Attempt)
	observation, observed := state.Observations[issueKey]
	identity := command.Collection.Identity
	if command.Collection.Scope != (reconciliationScope{Kind: reconciliationIssueScope, Repository: receipt.Request.Repository, Issue: receipt.Request.Issue}) || !command.Collection.Complete || identity.Epoch != admission.Epoch || identity.SourceRevision == 0 || identity.SourceRevision > state.Revision || identity.CycleID == 0 || admission.IssueGeneration != state.IssueGenerations[issueKey] || admission.AttemptGeneration != state.AttemptGenerations[attemptKey] || command.Collection.IssueGenerations[issueKey] != admission.IssueGeneration || command.Collection.AttemptGenerations[attemptKey] != admission.AttemptGeneration || !observed || observation.Generation != admission.ObservationGeneration || observation.LastCycleID != admission.ObservationCycleID || observation.Fact.BodyDigest != admission.ObservationBodyDigest {
		return errStaleStateResult
	}
	return nil
}

func bindOperatorAdmissionRefresh(state *runtimeOwnerState, command applyReconciliationCommand) error {
	receipt, ok := operatorReceiptByID(*state, command.OperatorRequestID)
	if !ok || receipt.Admission == nil {
		return errStaleStateResult
	}
	issueKey := ownerIssueKey(receipt.Request.Repository, receipt.Request.Issue)
	observation, observed := state.Observations[issueKey]
	group := slices.IndexFunc(command.Collection.Issues, func(group reconciliationIssueGroup) bool {
		return group.Fact.Repository == receipt.Request.Repository && group.Fact.Issue == receipt.Request.Issue
	})
	if !observed || !observation.Present || observation.ObservationEpoch > state.Epoch || group < 0 || !reflect.DeepEqual(observation.Fact, command.Collection.Issues[group].Fact) {
		return errStaleStateResult
	}
	previous := *receipt.Admission
	for index := range state.ControlReceipts {
		candidate := &state.ControlReceipts[index]
		if candidate.State == "pending" && candidate.Phase == operatorPhaseAdmissionPending && candidate.Request.Action == "recover" && candidate.Admission != nil && reflect.DeepEqual(*candidate.Admission, previous) {
			candidate.Admission.ObservationGeneration = observation.Generation
			candidate.Admission.ObservationCycleID = observation.LastCycleID
			candidate.Admission.ObservationBodyDigest = observation.Fact.BodyDigest
		}
	}
	return nil
}

func applyBeginOperatorMutation(attemptRoot, stateRoot string, state *runtimeOwnerState, command beginOperatorMutationCommand) (*runtimeEffectIntent, error) {
	request := command.Request
	admitted := false
	if !validOperatorRequest(request, state.Repository) {
		return nil, errors.Join(errors.New("validate operator request"), errStateConflict)
	}
	if receipt, ok := operatorReceiptByID(*state, request.RequestID); ok {
		if receipt.Request != request {
			return nil, errStateConflict
		}
		if receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil {
			return effectForOperatorReceipt(*state, receipt)
		}
		admission := receipt.Admission
		if admission.Epoch != state.Epoch || admission.IssueGeneration != state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] || admission.AttemptGeneration != state.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)] || !reflect.DeepEqual(admission.Manifest, command.Manifest) || admission.RemoteOnly != command.RemoteOnly {
			return nil, errStaleStateResult
		}
		if observationSensitiveOperatorAdmission(receipt) && (command.ObservationGeneration != admission.ObservationGeneration || command.ObservationCycleID != admission.ObservationCycleID || command.ObservationBodyDigest != admission.ObservationBodyDigest) {
			return nil, errStaleStateResult
		}
		command.Identity = stateResultIdentity{Epoch: admission.Epoch, SourceRevision: state.Revision, IssueGeneration: admission.IssueGeneration, AttemptGeneration: admission.AttemptGeneration}
		admitted = true
		if command.Runtime != nil {
			if !operatorAdmissionIdentityCurrent(command.Runtime.Identity, *admission, state.Revision) {
				return nil, errStaleStateResult
			}
			runtime := *command.Runtime
			runtime.Identity = command.Identity
			command.Runtime = &runtime
		}
		if command.Reconciliation != nil {
			if !operatorAdmissionIdentityCurrent(command.Reconciliation.Identity, *admission, state.Revision) {
				return nil, errStaleStateResult
			}
			reconciliation := *command.Reconciliation
			reconciliation.Identity = command.Identity
			command.Reconciliation = &reconciliation
		}
		state.ControlReceipts = slices.DeleteFunc(state.ControlReceipts, func(candidate controlReceipt) bool { return candidate.Request.RequestID == request.RequestID })
		if observation, exists := state.Observations[ownerIssueKey(request.Repository, request.Issue)]; exists {
			command.ObservationGeneration, command.ObservationCycleID, command.ObservationBodyDigest = observation.Generation, observation.LastCycleID, observation.Fact.BodyDigest
		}
	}
	if command.Identity.Epoch != state.Epoch || command.Identity.SourceRevision == 0 || command.Identity.SourceRevision > state.Revision {
		return nil, errStaleStateResult
	}
	issueKey, attemptKey := ownerIssueKey(request.Repository, request.Issue), ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if command.Identity.IssueGeneration != state.IssueGenerations[issueKey] {
		return nil, errStaleStateResult
	}
	observation, ok := state.Observations[issueKey]
	if command.RemoteOnly {
		return applyRemoteOnlyOperatorMutation(attemptRoot, stateRoot, state, command, observation, ok, admitted)
	}
	manifest := cloneManifest(command.Manifest)
	if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, manifest); err != nil || manifest.Issue != request.Issue || manifest.Attempt != request.Attempt {
		return nil, errors.Join(errors.New("validate operator manifest"), errStateConflict)
	}
	tombstone, tombstoned := state.Tombstones[attemptKey]
	if tombstoned {
		// A same-action replay is bound to the durable tombstone, not a
		// later GitHub observation that may already have disappeared.
		if command.Identity.AttemptGeneration != tombstone.InvalidatedGeneration && command.Identity.AttemptGeneration != tombstone.Generation {
			return nil, errStaleStateResult
		}
		return replayOperatorTombstone(state, request, manifest, command.PublishedHead, command.CleanupDigest, command.CleanupPolicy, tombstone)
	}
	absentOrphan := ok && !observation.Present && (request.Action == "dismiss" && command.IssueClosed || request.Action == "abandon") && observation.ObservationEpoch <= state.Epoch
	if !ok || !observation.Present && !absentOrphan || command.ObservationGeneration != observation.Generation || command.ObservationCycleID != observation.LastCycleID || command.ObservationBodyDigest != observation.Fact.BodyDigest {
		return nil, errStaleStateResult
	}
	if observation.ObservationEpoch != state.Epoch && (!admitted || observation.ObservationEpoch > state.Epoch) {
		return nil, errStaleStateResult
	}
	if request.Action == "recover" {
		if effect, attached, err := attachOperatorRecovery(state, command, manifest); attached || err != nil {
			return effect, err
		}
	}
	if request.Action == "cancel" || request.Action == "recover" {
		if effect := matchingPendingOperatorStop(*state, command); effect != nil {
			if err := appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: operatorPhaseStopPending, EffectID: effect.ID}); err != nil {
				return nil, err
			}
			return effect, nil
		}
	}
	if command.Identity.AttemptGeneration != state.AttemptGenerations[attemptKey] {
		return nil, errStaleStateResult
	}
	record, ok := state.Attempts[attemptKey]
	if !ok || record.Generation != command.Identity.AttemptGeneration || !reflect.DeepEqual(record.Manifest, manifest) && (request.Action != "review-plan" || !sameManifestExceptUpdatedAt(record.Manifest, manifest)) {
		return nil, errStaleStateResult
	}
	status, statuses, err := ownerOperatorStatus(*state, request.Issue, request.Attempt)
	if err != nil {
		return nil, errors.Join(errors.New("project current operator status"), errStateConflict)
	}
	if absentOrphan {
		if status.State != "orphaned" {
			return nil, errStateConflict
		}
		status.IssueClosed = command.IssueClosed
	}
	var effect *runtimeEffectIntent
	phase := operatorPhaseCompleted
	switch request.Action {
	case "dismiss":
		// The admission's direct GitHub read is newer than the status replica.
		status.IssueClosed = command.IssueClosed
		if !command.IssueClosed || !canDismissClosedAttempt(status) || !command.CleanupValid || !agentruntime.ValidEffectRequestDigest(command.CleanupDigest) || command.CleanupPolicy.Action != request.Action || command.CleanupPolicy.PublishedHead != "" || command.PublishedHead != "" || command.Runtime != nil || command.Reconciliation != nil {
			return nil, errStateConflict
		}
		reviewer, reviewerErr := pendingReviewerEffect(*state, request.Repository, request.Issue, request.Attempt)
		if reviewerErr != nil {
			return nil, reviewerErr
		}
		leaseID, leaseErr := retainReviewerLease(state, request.Repository, request.Issue, request.Attempt, reviewer)
		if leaseErr != nil {
			return nil, leaseErr
		}
		policy := command.CleanupPolicy
		effect, err = applyInvalidateAttempt(attemptRoot, stateRoot, state, invalidateAttemptCommand{
			Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt,
			ExpectedIssueGeneration: command.Identity.IssueGeneration, ExpectedAttemptGeneration: command.Identity.AttemptGeneration,
			Action: "dismissed", CleanupPhase: "pending", Manifest: &manifest, CleanupPolicy: &policy,
			EffectAction: string(agentruntime.EffectCleanup), EffectRequestDigest: command.CleanupDigest,
		})
		if err == nil && effect != nil {
			supersedePendingOperatorWorkflows(state, request.Repository, request.Issue, request.Attempt, request.Action)
			bindSupersededReviewer(effect, reviewer)
			tombstone := state.Tombstones[attemptKey]
			tombstone.ReviewerLeaseID = leaseID
			tombstone.Diagnostic = "reviewer cleanup remains pending"
			state.Tombstones[attemptKey] = tombstone
			if tombstone.InvalidatedStart != nil {
				phase = operatorPhaseStartCleanup
			} else if tombstone.InvalidatedHandoff != nil {
				phase = operatorPhaseHandoffCleanup
			} else {
				phase = operatorPhaseCleanupPending
			}
		}
	case "archive", "abandon", "remove":
		if !command.CleanupValid || !validDestructiveOperatorStatus(request.Action, status, statuses) || !agentruntime.ValidEffectRequestDigest(command.CleanupDigest) || command.CleanupPolicy.Action != request.Action || command.CleanupPolicy.PublishedHead != command.PublishedHead || command.Runtime != nil || command.Reconciliation != nil || request.Action == "archive" && manifest.State != "completed" {
			return nil, errStateConflict
		}
		publishedHead := ""
		if request.Action == "remove" {
			publishedHead = command.PublishedHead
			if !preflightObjectID.MatchString(publishedHead) || publishedHead != firstNonempty(status.HeadSHA, manifest.BaseSHA) {
				return nil, errStateConflict
			}
		} else if command.PublishedHead != "" {
			return nil, errStateConflict
		}
		reviewer, reviewerErr := pendingReviewerEffect(*state, request.Repository, request.Issue, request.Attempt)
		if reviewerErr != nil {
			return nil, reviewerErr
		}
		policy := command.CleanupPolicy
		effect, err = applyInvalidateAttempt(attemptRoot, stateRoot, state, invalidateAttemptCommand{
			Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt,
			ExpectedIssueGeneration: command.Identity.IssueGeneration, ExpectedAttemptGeneration: command.Identity.AttemptGeneration,
			Action: operatorTombstoneAction(request.Action), CleanupPhase: "pending", PublishedHead: publishedHead, Manifest: &manifest, CleanupPolicy: &policy,
			EffectAction: string(agentruntime.EffectCleanup), EffectRequestDigest: command.CleanupDigest,
		})
		if err == nil && effect != nil {
			supersedePendingOperatorWorkflows(state, request.Repository, request.Issue, request.Attempt, request.Action)
			bindSupersededReviewer(effect, reviewer)
		}
		phase = operatorPhaseCleanupPending
	case "cancel":
		if manifest.State != "running" || command.Runtime == nil || command.Reconciliation != nil || command.CleanupDigest != "" || command.PublishedHead != "" || command.Runtime.Action != agentruntime.EffectStop || !reflect.DeepEqual(command.Runtime.Manifest, manifest) {
			return nil, errStateConflict
		}
		begin := *command.Runtime
		if !sameOperatorBeginIdentity(begin.Identity, command.Identity) {
			return nil, errStaleStateResult
		}
		reviewer, reviewerErr := pendingReviewerEffect(*state, request.Repository, request.Issue, request.Attempt)
		if reviewerErr != nil {
			return nil, reviewerErr
		}
		begin.Identity.SourceRevision = state.Revision
		begin.SupersededReviewerID = reviewer.ID
		effect, err = applyBeginRuntimeEffect(attemptRoot, stateRoot, state, begin)
		if err == nil {
			supersedePendingOperatorWorkflows(state, request.Repository, request.Issue, request.Attempt, request.Action)
		}
		phase = operatorPhaseStopPending
	case "recover":
		if command.CleanupDigest != "" || command.PublishedHead != "" {
			return nil, errStateConflict
		}
		if command.Runtime != nil {
			livenessRecoverable := status.State == "active" || status.State == "review-ready" || status.State == "blocked" && status.Retryable && slices.Equal(status.Blockers, []string{"runtime liveness mismatch"})
			if manifest.State != "running" || !livenessRecoverable || status.PR > 0 || !command.LivenessFailed || command.Reconciliation != nil || command.Runtime.Action != agentruntime.EffectStop || !strings.HasPrefix(command.Runtime.Reason, "dashboard recovery: ") || strings.TrimSpace(strings.TrimPrefix(command.Runtime.Reason, "dashboard recovery: ")) == "" || !reflect.DeepEqual(command.Runtime.Manifest, manifest) {
				return nil, errStateConflict
			}
			begin := *command.Runtime
			if !sameOperatorBeginIdentity(begin.Identity, command.Identity) {
				return nil, errStaleStateResult
			}
			reviewer, reviewerErr := pendingReviewerEffect(*state, request.Repository, request.Issue, request.Attempt)
			if reviewerErr != nil {
				return nil, reviewerErr
			}
			begin.Identity.SourceRevision = state.Revision
			begin.SupersededReviewerID = reviewer.ID
			effect, err = applyBeginRuntimeEffect(attemptRoot, stateRoot, state, begin)
			if err == nil {
				supersedePendingOperatorWorkflows(state, request.Repository, request.Issue, request.Attempt, request.Action)
			}
			phase = operatorPhaseStopPending
		} else {
			if command.LivenessFailed || !status.Retryable || status.PR > 0 || !slices.Contains([]string{"failed", "cancelled"}, manifest.State) || command.Reconciliation == nil || command.Reconciliation.Request.Action != reconciliationGitHubIssueUpdate || command.Reconciliation.Request.GitHubIssueUpdate == nil || command.Reconciliation.Request.GitHubIssueUpdate.Kind != githubIssueRetry {
				return nil, errStateConflict
			}
			begin := *command.Reconciliation
			if !sameOperatorBeginIdentity(begin.Identity, command.Identity) {
				return nil, errStaleStateResult
			}
			begin.Identity.SourceRevision = state.Revision
			effect, err = applyBeginReconciliationEffect(attemptRoot, stateRoot, state, begin)
			phase = operatorPhaseRetryPending
		}
	case "review-plan":
		if command.Reconciliation == nil || command.Runtime != nil || command.CleanupDigest != "" || command.PublishedHead != "" {
			return nil, errors.Join(errors.New("review-plan command shape"), errStateConflict)
		}
		if manifest.State != "running" || command.Reconciliation.Request.Action != reconciliationReviewer || command.Reconciliation.Request.Reviewer == nil || command.Reconciliation.Request.Reviewer.Mode != agentruntime.ReviewModePlan {
			return nil, errors.Join(errors.New("review-plan runtime state"), errStateConflict)
		}
		begin := *command.Reconciliation
		if !sameOperatorBeginIdentity(begin.Identity, command.Identity) {
			return nil, errStaleStateResult
		}
		if state.Revision == ^uint64(0) {
			return nil, errors.New("runtime revision overflow")
		}
		admissionRevision := state.Revision + 1
		reviewer := *begin.Request.Reviewer
		reviewer.RunID = reviewerRunID(state.Epoch, admissionRevision, command.Identity.IssueGeneration, command.Identity.AttemptGeneration, request.Repository, request.Issue, request.Attempt, reviewer.Mode, reviewer.Target)
		reviewer.Snapshot, reviewer.Session = reviewRunIdentity(operatorEffectAttempt(manifest), productionSnapshotRoot(stateRoot), reviewer.Target, reviewer.RunID)
		begin.Request.Reviewer = &reviewer
		begin.ReviewerSourceRevision = admissionRevision
		begin.Identity.SourceRevision = state.Revision
		effect, err = applyBeginReconciliationEffect(attemptRoot, stateRoot, state, begin)
		if err != nil {
			return nil, errors.Join(errors.New("finalize current reviewer admission"), err)
		}
		phase = operatorPhaseReviewPending
	default:
		return nil, errStateConflict
	}
	if err != nil {
		return nil, err
	}
	receipt := controlReceipt{Request: request, State: "pending", Phase: phase}
	if phase == operatorPhaseCompleted {
		receipt.State = "completed"
		receipt.Result = successfulOperatorResult(request, 0)
		if request.Action == "dismiss" {
			if tombstone := state.Tombstones[attemptKey]; tombstone.ReviewerLeaseID != "" {
				receipt.Result = pendingReviewerResult(request, 0, tombstone.Diagnostic)
			}
		}
	}
	if err := appendOperatorReceipt(state, receipt); err != nil {
		return nil, err
	}
	return effect, nil
}

func operatorAdmissionIdentityCurrent(identity stateResultIdentity, admission operatorAdmission, revision uint64) bool {
	return identity.Epoch == admission.Epoch && identity.SourceRevision > 0 && identity.SourceRevision <= revision && identity.IssueGeneration == admission.IssueGeneration && identity.AttemptGeneration == admission.AttemptGeneration && identity.CycleID == 0 && identity.EffectID == "" && identity.RequestDigest == ""
}

func applyRemoteOnlyOperatorMutation(attemptRoot, stateRoot string, state *runtimeOwnerState, command beginOperatorMutationCommand, observation reconciliationObservation, observed, admitted bool) (*runtimeEffectIntent, error) {
	request := command.Request
	issueKey, attemptKey := ownerIssueKey(request.Repository, request.Issue), ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if tombstone, exists := state.Tombstones[attemptKey]; exists {
		if !slices.Contains([]string{"archive", "dismiss"}, request.Action) || tombstone.Action != operatorTombstoneAction(request.Action) || tombstone.Manifest != nil || tombstone.CleanupPhase != "completed" || tombstone.EffectID != "" || tombstone.CleanupPolicy != nil || tombstone.PublishedHead != "" || command.Identity.AttemptGeneration != tombstone.Generation || state.AttemptGenerations[attemptKey] != tombstone.Generation || !reflect.DeepEqual(command.Manifest, agentruntime.Manifest{}) {
			return nil, errStateConflict
		}
		return nil, appendOperatorReceipt(state, controlReceipt{Request: request, State: "completed", Phase: operatorPhaseCompleted, Result: successfulOperatorResult(request, 0)})
	}
	attempt, accepted := observation.Attempts[attemptKey]
	if !slices.Contains([]string{"archive", "dismiss"}, request.Action) || request.Action == "dismiss" && !command.IssueClosed || command.IssueClosed != observation.Fact.Closed || !reflect.DeepEqual(command.Manifest, agentruntime.Manifest{}) || command.CleanupValid || command.CleanupDigest != "" || command.CleanupPolicy != (agentruntime.EffectCleanupPolicy{}) || command.Runtime != nil || command.Reconciliation != nil || command.PublishedHead != "" || !observed || !observation.Present || observation.ObservationEpoch != state.Epoch && (!admitted || observation.ObservationEpoch > state.Epoch) || observation.Fact.CurrentAttempt != request.Attempt || observation.Generation != command.ObservationGeneration || observation.LastCycleID != command.ObservationCycleID || observation.Fact.BodyDigest != command.ObservationBodyDigest || !accepted || !attempt.Present || attempt.ObservationEpoch != state.Epoch && (!admitted || attempt.ObservationEpoch > state.Epoch) || attempt.SourceIssueGeneration != observation.Generation || attempt.OwnerGeneration != command.Identity.AttemptGeneration || attempt.Fact.State != "completed" || attempt.Fact.Repository != request.Repository || attempt.Fact.Issue != request.Issue || attempt.Fact.Attempt != request.Attempt || state.AttemptGenerations[attemptKey] != command.Identity.AttemptGeneration {
		return nil, errStaleStateResult
	}
	if _, exists := state.Attempts[attemptKey]; exists {
		return nil, errStateConflict
	}
	if _, exists := state.Tombstones[attemptKey]; exists {
		return nil, errStateConflict
	}
	if _, err := applyInvalidateAttempt(attemptRoot, stateRoot, state, invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, ExpectedIssueGeneration: state.IssueGenerations[issueKey], ExpectedAttemptGeneration: command.Identity.AttemptGeneration, Action: operatorTombstoneAction(request.Action), CleanupPhase: "completed"}); err != nil {
		return nil, err
	}
	receipt := controlReceipt{Request: request, State: "completed", Phase: operatorPhaseCompleted, Result: successfulOperatorResult(request, 0)}
	return nil, appendOperatorReceipt(state, receipt)
}

func applyStartOperatorCleanup(state *runtimeOwnerState, command startOperatorCleanupCommand) error {
	identity := command.Identity
	effect, ok := state.Effects[identity.EffectID]
	if !ok || effect.Action != string(agentruntime.EffectCleanup) || effect.State != "pending" {
		return errStaleStateResult
	}
	if err := applyAuthorizeRuntimeEffect(state, authorizeRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectCleanup}); err != nil {
		return err
	}
	key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	tombstone, ok := state.Tombstones[key]
	if !ok || tombstone.EffectID != effect.ID || tombstone.CleanupPhase != "pending" && tombstone.CleanupPhase != "cleanup-started" {
		return errStateConflict
	}
	if tombstone.CleanupPhase == "cleanup-started" {
		return nil
	}
	tombstone.CleanupPhase = "cleanup-started"
	state.Tombstones[key] = tombstone
	for index := range state.ControlReceipts {
		if state.ControlReceipts[index].EffectID == effect.ID && state.ControlReceipts[index].State == "pending" {
			state.ControlReceipts[index].Phase = operatorPhaseCleanupStarted
			state.ControlReceipts[index].Diagnostic = ""
		}
	}
	return nil
}

func applyFinishOperatorRuntimeEffect(attemptRoot, stateRoot string, state *runtimeOwnerState, command finishOperatorRuntimeEffectCommand) error {
	if err := applyFinishRuntimeEffect(attemptRoot, stateRoot, state, command.Finish); err != nil {
		return err
	}
	if !advanceRecoverReceipts(state, command.Finish.Identity.EffectID, operatorPhaseStopPending, operatorPhaseTerminalAwait) {
		completeOperatorReceipts(state, command.Finish.Identity.EffectID)
	}
	return nil
}

func applyFinishOperatorReconciliationEffect(stateRoot string, state *runtimeOwnerState, command finishOperatorReconciliationEffectCommand) error {
	return applyFinishReconciliationEffect(stateRoot, state, command.Finish)
}

func applyAdvanceOperatorRecovery(attemptRoot, stateRoot string, state *runtimeOwnerState, command advanceOperatorRecoveryCommand) (*runtimeEffectIntent, error) {
	receipt, ok := operatorReceiptByID(*state, command.RequestID)
	if !ok || receipt.Request.Action != "recover" || receipt.State != "pending" || command.Identity.Epoch != state.Epoch || command.Identity.SourceRevision == 0 || command.Identity.SourceRevision > state.Revision {
		return nil, errStaleStateResult
	}
	predecessor, ok := state.Effects[receipt.EffectID]
	if !ok || predecessor.State != "completed" || !operatorReceiptMatchesEffect(receipt, predecessor) {
		return nil, errStateConflict
	}
	request := command.Reconciliation.Request
	wantKind := githubIssueTerminalFailure
	if receipt.Phase == operatorPhaseRetryAwait {
		wantKind = githubIssueRetry
	} else if receipt.Phase != operatorPhaseTerminalAwait {
		return nil, errStateConflict
	}
	if request.Action != reconciliationGitHubIssueUpdate || request.GitHubIssueUpdate == nil || request.GitHubIssueUpdate.Kind != wantKind || request.Repository != receipt.Request.Repository || request.Issue != receipt.Request.Issue || request.Attempt != receipt.Request.Attempt {
		return nil, errStateConflict
	}
	issueKey, attemptKey := ownerIssueKey(request.Repository, request.Issue), ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if command.Identity.IssueGeneration != state.IssueGenerations[issueKey] || command.Identity.AttemptGeneration != state.AttemptGenerations[attemptKey] {
		return nil, errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return nil, errAttemptTombstoned
	}
	begin := command.Reconciliation
	if !sameOperatorBeginIdentity(begin.Identity, command.Identity) {
		return nil, errStaleStateResult
	}
	begin.Identity.SourceRevision = state.Revision
	effect, err := applyBeginReconciliationEffect(attemptRoot, stateRoot, state, begin)
	if err != nil {
		return nil, err
	}
	phase := operatorPhaseTerminal
	if wantKind == githubIssueRetry {
		phase = operatorPhaseRetryPending
	}
	for index := range state.ControlReceipts {
		candidate := &state.ControlReceipts[index]
		if candidate.Request.Action == "recover" && candidate.State == "pending" && candidate.Phase == receipt.Phase && candidate.EffectID == receipt.EffectID {
			candidate.Phase = phase
			candidate.EffectID = ""
			candidate.Diagnostic = ""
		}
	}
	return effect, nil
}

func advanceRecoverReceipts(state *runtimeOwnerState, effectID, from, to string) bool {
	advanced := false
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.Request.Action == "recover" && receipt.State == "pending" && receipt.Phase == from && receipt.EffectID == effectID {
			receipt.Phase = to
			receipt.Diagnostic = ""
			advanced = true
		}
	}
	return advanced
}

func completeOperatorReceipts(state *runtimeOwnerState, effectID string) {
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.EffectID == effectID && receipt.State == "pending" {
			receipt.State, receipt.Phase = "completed", operatorPhaseCompleted
			receipt.Diagnostic = ""
			receipt.Result = successfulOperatorResult(receipt.Request, state.Revision+1)
		}
	}
}

func failOperatorReceipts(state *runtimeOwnerState, effectID, diagnostic string) {
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.EffectID != effectID || receipt.State != "pending" {
			continue
		}
		receipt.State, receipt.Phase = "completed", operatorPhaseCompleted
		receipt.Diagnostic = ""
		receipt.Result = &controlResult{Version: controlVersion, RequestID: receipt.Request.RequestID, Action: receipt.Request.Action, Status: http.StatusInternalServerError, Error: diagnostic, OwnerRevision: state.Revision + 1}
	}
}

func supersedePendingOperatorWorkflows(state *runtimeOwnerState, repository string, issue, attempt int, action string) {
	remove := map[string]bool{}
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.State != "pending" || receipt.Request.Repository != repository || receipt.Request.Issue != issue || receipt.Request.Attempt != attempt || receipt.Request.Action == action {
			continue
		}
		remove[receipt.EffectID] = true
		receipt.State, receipt.Phase, receipt.EffectID = "completed", operatorPhaseCompleted, ""
		receipt.Diagnostic = ""
		receipt.Result = &controlResult{Version: controlVersion, RequestID: receipt.Request.RequestID, Action: receipt.Request.Action, Status: http.StatusConflict, Error: "operator workflow superseded by " + action, OwnerRevision: state.Revision + 1}
	}
	for effectID := range remove {
		if effectID != "" && !effectReferencedByReceipt(*state, effectID) && !tombstoneReferencesEffect(*state, effectID) {
			delete(state.Effects, effectID)
		}
	}
}

func pendingReviewerEffect(state runtimeOwnerState, repository string, issue, attempt int) (runtimeEffectIntent, error) {
	issueGeneration := state.IssueGenerations[ownerIssueKey(repository, issue)]
	attemptGeneration := state.AttemptGenerations[ownerAttemptKey(repository, issue, attempt)]
	var selected runtimeEffectIntent
	for _, effect := range state.Effects {
		if effect.Repository != repository || effect.Issue != issue || effect.Attempt != attempt || effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Phase != "run-observe" {
			continue
		}
		if selected.ID != "" || effect.IssueGeneration != issueGeneration || effect.AttemptGeneration != attemptGeneration {
			return runtimeEffectIntent{}, errStateConflict
		}
		selected = effect
	}
	return selected, nil
}

func retainReviewerLease(state *runtimeOwnerState, repository string, issue, attempt int, pending runtimeEffectIntent) (string, error) {
	if pending.ID != "" {
		reviewer := pending.Reconciliation.Reviewer
		key := reviewerProofKey(repository, issue, attempt, reviewer.Mode, reviewer.Target)
		proof, exists := state.ReviewerProofs[key]
		if exists && proof.EffectID != pending.ID {
			return "", errStateConflict
		}
		if !exists {
			neverRan := pending.ReviewerGateProtocol && !pending.ReviewerSessionRequested && !pending.ReviewerLaunched && pending.ReviewerGroupPID == 0
			proof = reviewerProcessProof{Repository: repository, Issue: issue, Attempt: attempt, Mode: reviewer.Mode, Target: reviewer.Target, RunID: reviewer.RunID, EffectID: pending.ID, IssueGeneration: pending.IssueGeneration, AttemptGeneration: pending.AttemptGeneration, GroupPID: pending.ReviewerGroupPID, DeadProved: neverRan, NeverRan: neverRan, ProfileDigest: pending.ReviewerProfileDigest, ConfinementVersion: pending.ReviewerConfinementVersion}
			state.ReviewerProofs[key] = proof
		}
	}
	leaseID := ""
	for key, proof := range state.ReviewerProofs {
		if proof.Repository != repository || proof.Issue != issue || proof.Attempt != attempt || reviewerCleanupAuthorized(proof, activeWorkerProfileDigest(*state)) {
			continue
		}
		if !reviewerCleanupAuthorized(proof, activeWorkerProfileDigest(*state)) && proof.DeadProved && proof.LegacyUnverified {
			proof.DeadProved = false // Old group-only certificates do not prove descendant death.
			state.ReviewerProofs[key] = proof
		}
		if leaseID == "" || proof.EffectID < leaseID {
			leaseID = proof.EffectID
		}
	}
	return leaseID, nil
}

func bindSupersededReviewer(effect *runtimeEffectIntent, reviewer runtimeEffectIntent) {
	if reviewer.ID == "" {
		return
	}
	effect.SupersededReviewerID = reviewer.ID
	effect.SupersededReviewerGroupPID = reviewer.ReviewerGroupPID
	effect.SupersededReviewerGateProtocol = reviewer.ReviewerGateProtocol
	effect.SupersededReviewerSessionRequested = reviewer.ReviewerSessionRequested
	effect.SupersededReviewerRequestDigest = reviewer.RequestDigest
	effect.SupersededReviewerProfileDigest = reviewer.ReviewerProfileDigest
	effect.SupersededReviewerConfinementVersion = reviewer.ReviewerConfinementVersion
	effect.SupersededReviewerMode = reviewer.Reconciliation.Reviewer.Mode
	effect.SupersededReviewerTarget = reviewer.Reconciliation.Reviewer.Target
	effect.SupersededReviewerRunID = reviewer.Reconciliation.Reviewer.RunID
	effect.SupersededReviewerIssueGeneration = reviewer.IssueGeneration
	effect.SupersededReviewerAttemptGeneration = reviewer.AttemptGeneration
}

func bindOperatorReceipt(state *runtimeOwnerState, requestID string, effect *runtimeEffectIntent, revision uint64) {
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.Request.RequestID != requestID {
			continue
		}
		if effect != nil {
			receipt.EffectID = effect.ID
		}
		if receipt.Result != nil {
			receipt.Result.OwnerRevision = revision
		}
		return
	}
}

func replayOperatorTombstone(state *runtimeOwnerState, request controlRequest, manifest agentruntime.Manifest, publishedHead, cleanupDigest string, cleanupPolicy agentruntime.EffectCleanupPolicy, tombstone runtimeTombstone) (*runtimeEffectIntent, error) {
	if tombstone.Action != operatorTombstoneAction(request.Action) || tombstone.Manifest == nil || !reflect.DeepEqual(*tombstone.Manifest, manifest) || tombstone.PublishedHead != publishedHead {
		return nil, errStateConflict
	}
	if request.Action == "dismiss" {
		if tombstone.InvalidatedStart != nil {
			if err := appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: operatorPhaseStartCleanup, EffectID: tombstone.EffectID}); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if tombstone.InvalidatedHandoff != nil && !tombstone.HandoffCompensated {
			if err := appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: operatorPhaseHandoffCleanup, EffectID: tombstone.EffectID}); err != nil {
				return nil, err
			}
			return nil, nil
		}
		if tombstone.EffectID == "" {
			if cleanupDigest != "" || cleanupPolicy != (agentruntime.EffectCleanupPolicy{}) || tombstone.CleanupPolicy != nil || tombstone.CleanupPhase != "completed" {
				return nil, errStateConflict
			}
			return nil, appendOperatorReceipt(state, controlReceipt{Request: request, State: "completed", Phase: operatorPhaseCompleted, Result: successfulOperatorResult(request, 0)})
		}
	}
	effect, ok := state.Effects[tombstone.EffectID]
	if !ok || effect.Action != string(agentruntime.EffectCleanup) || effect.RequestDigest != cleanupDigest || tombstone.CleanupPolicy == nil || *tombstone.CleanupPolicy != cleanupPolicy {
		return nil, errStateConflict
	}
	if tombstone.CleanupPhase == "completed" {
		result := successfulOperatorResult(request, 0)
		if diagnostic := legacyReviewerDiagnostic(*state, request.Repository, request.Issue, request.Attempt); diagnostic != "" {
			result = pendingReviewerResult(request, 0, diagnostic)
		}
		receipt := controlReceipt{Request: request, State: "completed", Phase: operatorPhaseCompleted, EffectID: tombstone.EffectID, Result: result}
		return nil, appendOperatorReceipt(state, receipt)
	}
	if effect.State != "pending" {
		return nil, errStateConflict
	}
	phase := operatorPhaseCleanupPending
	if tombstone.CleanupPhase == "cleanup-started" {
		phase = operatorPhaseCleanupStarted
	}
	if err := appendOperatorReceipt(state, controlReceipt{Request: request, State: "pending", Phase: phase, EffectID: effect.ID}); err != nil {
		return nil, err
	}
	return cloneEffect(&effect), nil
}

func operatorReceiptByID(state runtimeOwnerState, requestID string) (controlReceipt, bool) {
	index := slices.IndexFunc(state.ControlReceipts, func(receipt controlReceipt) bool { return receipt.Request.RequestID == requestID })
	if index < 0 {
		return controlReceipt{}, false
	}
	return cloneControlReceipt(state.ControlReceipts[index]), true
}

func matchingPendingOperatorStop(state runtimeOwnerState, command beginOperatorMutationCommand) *runtimeEffectIntent {
	if command.Runtime == nil || command.Runtime.Action != agentruntime.EffectStop || !sameOperatorBeginIdentity(command.Runtime.Identity, command.Identity) {
		return nil
	}
	for _, effect := range state.Effects {
		if effect.State == "pending" && effect.Action == string(agentruntime.EffectStop) && effect.Repository == command.Request.Repository && effect.Issue == command.Request.Issue && effect.Attempt == command.Request.Attempt && effect.IssueGeneration == command.Identity.IssueGeneration && effect.AttemptGeneration == state.AttemptGenerations[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)] && effect.RequestDigest == command.Runtime.RequestDigest && effect.Reason == command.Runtime.Reason && reflect.DeepEqual(command.Runtime.Manifest, command.Manifest) {
			return cloneEffect(&effect)
		}
	}
	return nil
}

func attachOperatorRecovery(state *runtimeOwnerState, command beginOperatorMutationCommand, manifest agentruntime.Manifest) (*runtimeEffectIntent, bool, error) {
	if command.CleanupDigest != "" || command.PublishedHead != "" || command.Identity.Epoch != state.Epoch || command.Identity.SourceRevision == 0 || command.Identity.SourceRevision > state.Revision {
		return nil, false, nil
	}
	key := ownerAttemptKey(command.Request.Repository, command.Request.Issue, command.Request.Attempt)
	record, ok := state.Attempts[key]
	if !ok || !sameRuntimeEffectManifestBase(record.Manifest, manifest, "") {
		return nil, false, nil
	}
	for _, receipt := range state.ControlReceipts {
		if receipt.Request.Action != "recover" || receipt.State != "pending" || receipt.Request.Repository != command.Request.Repository || receipt.Request.Issue != command.Request.Issue || receipt.Request.Attempt != command.Request.Attempt {
			continue
		}
		if receipt.Phase == operatorPhaseAdmissionPending {
			continue
		}
		effect, ok := state.Effects[receipt.EffectID]
		generation := state.AttemptGenerations[key]
		captured := command.Identity.AttemptGeneration
		if !ok || !operatorReceiptMatchesEffect(receipt, effect) || command.Identity.IssueGeneration != effect.IssueGeneration || effect.AttemptGeneration != generation || captured != generation && (captured == ^uint64(0) || captured+1 != generation) {
			return nil, true, errStateConflict
		}
		if err := appendOperatorReceipt(state, controlReceipt{Request: command.Request, State: "pending", Phase: receipt.Phase, EffectID: effect.ID}); err != nil {
			return nil, true, err
		}
		return cloneEffect(&effect), true, nil
	}
	if effect := currentRetryEffect(*state, command.Request.Repository, command.Request.Issue, command.Request.Attempt); effect != nil {
		if command.Identity.IssueGeneration != effect.IssueGeneration || command.Identity.AttemptGeneration != effect.AttemptGeneration ||
			effect.Reconciliation.Manifest == nil || !sameRuntimeEffectManifestBase(*effect.Reconciliation.Manifest, manifest, "") {
			return nil, true, errStateConflict
		}
		receipt := controlReceipt{Request: command.Request, State: "pending", Phase: operatorPhaseRetryPending, EffectID: effect.ID}
		if effect.State == "completed" {
			receipt.State, receipt.Phase = "completed", operatorPhaseCompleted
			receipt.Result = successfulOperatorResult(command.Request, state.Revision+1)
		}
		if err := appendOperatorReceipt(state, receipt); err != nil {
			return nil, true, err
		}
		return cloneEffect(effect), true, nil
	}
	return nil, false, nil
}

func effectForOperatorReceipt(state runtimeOwnerState, receipt controlReceipt) (*runtimeEffectIntent, error) {
	if receipt.State == "completed" {
		return nil, nil
	}
	effect, ok := state.Effects[receipt.EffectID]
	if !ok || effect.State != operatorReceiptEffectState(receipt) || !operatorReceiptMatchesEffect(receipt, effect) {
		return nil, errStateConflict
	}
	return cloneEffect(&effect), nil
}

func appendOperatorReceipt(state *runtimeOwnerState, receipt controlReceipt) error {
	if len(state.ControlReceipts) == maxControlReceipts {
		completed := slices.IndexFunc(state.ControlReceipts, func(existing controlReceipt) bool { return existing.State == "completed" })
		if completed < 0 {
			return errStateConflict
		}
		evictedEffect := state.ControlReceipts[completed].EffectID
		state.ControlReceipts = slices.Delete(state.ControlReceipts, completed, completed+1)
		if evictedEffect != "" && !effectReferencedByReceipt(*state, evictedEffect) && !tombstoneReferencesEffect(*state, evictedEffect) {
			if effect, ok := state.Effects[evictedEffect]; ok && effect.State == "completed" {
				delete(state.Effects, evictedEffect)
			}
		}
	}
	state.ControlReceipts = append(state.ControlReceipts, cloneControlReceipt(receipt))
	return nil
}

func successfulOperatorResult(request controlRequest, revision uint64) *controlResult {
	return &controlResult{Version: controlVersion, RequestID: request.RequestID, Action: request.Action, OK: true, Status: http.StatusOK, OwnerRevision: revision}
}

func pendingReviewerResult(request controlRequest, revision uint64, diagnostic string) *controlResult {
	return &controlResult{Version: controlVersion, RequestID: request.RequestID, Action: request.Action, OK: true, Status: http.StatusAccepted, OwnerRevision: revision, Data: []byte(`{"phase":"cleanup-pending","diagnostic":` + strconv.Quote(diagnostic) + `}`)}
}

func validOperatorRequest(request controlRequest, repository string) bool {
	return validControlRequest(request, repository) && slices.Contains([]string{"dismiss", "archive", "abandon", "remove", "cancel", "recover", "review-plan"}, request.Action)
}

func operatorTombstoneAction(action string) string {
	switch action {
	case "archive":
		return "archived"
	case "abandon":
		return "abandoned"
	case "dismiss":
		return "dismissed"
	case "remove":
		return "removed"
	default:
		return ""
	}
}

func validTombstoneCleanupPolicy(tombstone runtimeTombstone) bool {
	if tombstone.Action == "dismissed" {
		if tombstone.PublishedHead != "" {
			return false
		}
		if tombstone.EffectID == "" {
			return tombstone.CleanupPolicy == nil && tombstone.CleanupPhase == "completed" && tombstone.ReviewerLeaseID == ""
		}
		return tombstone.CleanupPolicy != nil && tombstone.CleanupPolicy.Action == "dismiss" && tombstone.CleanupPolicy.PublishedHead == ""
	}
	if tombstone.ReviewerLeaseID != "" {
		return false
	}
	if bareCompletedRemoval(tombstone) || bareCompletedArchive(tombstone) {
		return true
	}
	if tombstone.CleanupPolicy == nil || tombstone.EffectID == "" {
		return false
	}
	want := map[string]string{"archived": "archive", "abandoned": "abandon", "removed": "remove"}[tombstone.Action]
	return want != "" && tombstone.CleanupPolicy.Action == want && tombstone.CleanupPolicy.PublishedHead == tombstone.PublishedHead && (want == "remove" && preflightObjectID.MatchString(tombstone.PublishedHead) || want != "remove" && tombstone.PublishedHead == "")
}

func bareCompletedArchive(tombstone runtimeTombstone) bool {
	return tombstone.Action == "archived" && tombstone.CleanupPhase == "completed" && tombstone.PublishedHead == "" && tombstone.Manifest == nil && tombstone.CleanupPolicy == nil && tombstone.EffectID == ""
}

func bareCompletedRemoval(tombstone runtimeTombstone) bool {
	return tombstone.Action == "removed" && tombstone.CleanupPhase == "completed" && tombstone.PublishedHead == "" && tombstone.Manifest == nil && tombstone.CleanupPolicy == nil && tombstone.EffectID == ""
}

func ownerOperatorStatus(state runtimeOwnerState, issue, attempt int) (orchestrator.RecoveryStatus, []orchestrator.RecoveryStatus, error) {
	snapshot, err := projectOwnerStatus(stateOwnerSnapshot{State: cloneRuntimeOwnerState(state)}, maxReconciliationAttemptCount, time.Unix(1, 0))
	if err != nil {
		return orchestrator.RecoveryStatus{}, nil, err
	}
	matches := slices.DeleteFunc(slices.Clone(snapshot.Statuses), func(status orchestrator.RecoveryStatus) bool {
		return status.Repository != state.Repository || status.Issue != issue || status.Attempt != attempt
	})
	if len(matches) != 1 {
		return orchestrator.RecoveryStatus{}, snapshot.Statuses, errors.New("attempt projection is unavailable or ambiguous")
	}
	return matches[0], snapshot.Statuses, nil
}

func validDestructiveOperatorStatus(action string, status orchestrator.RecoveryStatus, statuses []orchestrator.RecoveryStatus) bool {
	switch action {
	case "archive":
		return status.State == "completed"
	case "abandon":
		return status.State == "orphaned"
	case "remove":
		return slices.Contains([]string{"failed", "orphaned", "cancelled"}, status.State) && !status.Retryable && slices.ContainsFunc(statuses, func(candidate orchestrator.RecoveryStatus) bool {
			return candidate.Repository == status.Repository && candidate.Issue == status.Issue && candidate.Attempt > status.Attempt
		})
	default:
		return false
	}
}

func validOperatorReceiptBinding(receipt controlReceipt) bool {
	if receipt.Phase == "" {
		return receipt.EffectID == "" && receipt.Diagnostic == "" && receipt.Admission == nil
	}
	if !slices.Contains([]string{operatorPhaseAdmissionPending, operatorPhaseCleanupPending, operatorPhaseCleanupStarted, operatorPhaseStopPending, operatorPhaseTerminalAwait, operatorPhaseTerminal, operatorPhaseRetryAwait, operatorPhaseRetryPending, operatorPhaseReviewPending, operatorPhaseHandoffCleanup, operatorPhaseStartCleanup, operatorPhaseCompleted}, receipt.Phase) {
		return false
	}
	if receipt.State == "completed" {
		return receipt.Phase == operatorPhaseCompleted && receipt.Result != nil && receipt.Diagnostic == "" && receipt.Admission == nil
	}
	if receipt.Phase == operatorPhaseAdmissionPending {
		return receipt.EffectID == "" && receipt.Result == nil && receipt.Diagnostic == "" && receipt.Admission != nil
	}
	legacyHandoff := receipt.Request.Action == "dismiss" && receipt.Phase == operatorPhaseHandoffCleanup && receipt.EffectID == ""
	return receipt.State == "pending" && receipt.Phase != operatorPhaseCompleted && (receipt.EffectID != "" || legacyHandoff) && receipt.Result == nil && receipt.Admission == nil && boundedText(receipt.Diagnostic, maxReconciliationStringBytes, false)
}

func sameOperatorBeginIdentity(begin, operator stateResultIdentity) bool {
	return begin.Epoch == operator.Epoch && begin.SourceRevision == operator.SourceRevision && begin.IssueGeneration == operator.IssueGeneration && begin.AttemptGeneration == operator.AttemptGeneration && begin.CycleID == 0 && begin.EffectID == "" && begin.RequestDigest == ""
}

func operatorReceiptMatchesEffect(receipt controlReceipt, effect runtimeEffectIntent) bool {
	if effect.Repository != receipt.Request.Repository || effect.Issue != receipt.Request.Issue || effect.Attempt != receipt.Request.Attempt {
		return false
	}
	switch receipt.Request.Action {
	case "dismiss", "archive", "abandon", "remove":
		return effect.Action == string(agentruntime.EffectCleanup)
	case "cancel":
		return effect.Action == string(agentruntime.EffectStop)
	case "recover":
		switch receipt.Phase {
		case operatorPhaseStopPending, operatorPhaseTerminalAwait:
			return effect.Action == string(agentruntime.EffectStop)
		case operatorPhaseTerminal, operatorPhaseRetryAwait:
			return effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationGitHubIssueUpdate && effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueTerminalFailure
		case operatorPhaseRetryPending, operatorPhaseCompleted:
			return effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationGitHubIssueUpdate && effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueRetry
		default:
			return false
		}
	case "review-plan":
		return effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil && effect.Reconciliation.Reviewer.Mode == agentruntime.ReviewModePlan
	default:
		return false
	}
}

func operatorReceiptEffectState(receipt controlReceipt) string {
	if receipt.State == "completed" || receipt.Phase == operatorPhaseTerminalAwait || receipt.Phase == operatorPhaseRetryAwait {
		return "completed"
	}
	return "pending"
}

func effectReferencedByReceipt(state runtimeOwnerState, effectID string) bool {
	return slices.ContainsFunc(state.ControlReceipts, func(receipt controlReceipt) bool { return receipt.EffectID == effectID })
}

func tombstoneReferencesEffect(state runtimeOwnerState, effectID string) bool {
	for _, tombstone := range state.Tombstones {
		if tombstone.EffectID == effectID {
			return true
		}
	}
	return false
}
