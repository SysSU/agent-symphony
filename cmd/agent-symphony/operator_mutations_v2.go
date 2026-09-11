package main

import (
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const (
	operatorPhaseCleanupPending = "cleanup-pending"
	operatorPhaseCleanupStarted = "cleanup-started"
	operatorPhaseStopPending    = "stop-pending"
	operatorPhaseTerminalAwait  = "terminal-awaiting"
	operatorPhaseTerminal       = "terminal-pending"
	operatorPhaseRetryAwait     = "retry-awaiting"
	operatorPhaseRetryPending   = "retry-pending"
	operatorPhaseReviewPending  = "review-pending"
	operatorPhaseCompleted      = "completed"
)

func applyBeginOperatorMutation(attemptRoot, stateRoot string, state *runtimeOwnerState, command beginOperatorMutationCommand) (*runtimeEffectIntent, error) {
	request := command.Request
	if !validOperatorRequest(request, state.Repository) {
		return nil, errStateConflict
	}
	if receipt, ok := operatorReceiptByID(*state, request.RequestID); ok {
		if receipt.Request != request {
			return nil, errStateConflict
		}
		return effectForOperatorReceipt(*state, receipt)
	}
	if command.Identity.Epoch != state.Epoch || command.Identity.SourceRevision == 0 || command.Identity.SourceRevision > state.Revision {
		return nil, errStaleStateResult
	}
	manifest := cloneManifest(command.Manifest)
	if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, manifest); err != nil || manifest.Issue != request.Issue || manifest.Attempt != request.Attempt {
		return nil, errStateConflict
	}
	issueKey, attemptKey := ownerIssueKey(request.Repository, request.Issue), ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if command.Identity.IssueGeneration != state.IssueGenerations[issueKey] {
		return nil, errStaleStateResult
	}
	observation, ok := state.Observations[issueKey]
	if !ok || !observation.Present || command.ObservationGeneration != observation.Generation || command.ObservationCycleID != observation.LastCycleID || command.ObservationBodyDigest != observation.Fact.BodyDigest {
		return nil, errStaleStateResult
	}
	tombstone, tombstoned := state.Tombstones[attemptKey]
	if observation.ObservationEpoch != state.Epoch && (!tombstoned || tombstone.CleanupPhase != "completed") {
		return nil, errStaleStateResult
	}
	if request.Action == "recover" {
		if effect, attached, err := attachOperatorRecovery(state, command, manifest); attached || err != nil {
			return effect, err
		}
	}
	if tombstoned {
		if command.Identity.AttemptGeneration != tombstone.InvalidatedGeneration && command.Identity.AttemptGeneration != tombstone.Generation {
			return nil, errStaleStateResult
		}
		return replayOperatorTombstone(state, request, manifest, command.PublishedHead, command.CleanupDigest, command.CleanupPolicy, tombstone)
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
	if !ok || record.Generation != command.Identity.AttemptGeneration || !reflect.DeepEqual(record.Manifest, manifest) {
		return nil, errStaleStateResult
	}
	status, statuses, err := ownerOperatorStatus(*state, request.Issue, request.Attempt)
	if err != nil {
		return nil, errStateConflict
	}
	var effect *runtimeEffectIntent
	phase := operatorPhaseCompleted
	switch request.Action {
	case "dismiss":
		if !command.IssueClosed || !canDismissClosedAttempt(status) || command.CleanupDigest != "" || command.CleanupPolicy != (agentruntime.EffectCleanupPolicy{}) || command.PublishedHead != "" || command.Runtime != nil || command.Reconciliation != nil {
			return nil, errStateConflict
		}
		supersedePendingOperatorWorkflows(state, request.Repository, request.Issue, request.Attempt, request.Action)
		_, err = applyInvalidateAttempt(attemptRoot, stateRoot, state, invalidateAttemptCommand{
			Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt,
			ExpectedIssueGeneration: command.Identity.IssueGeneration, ExpectedAttemptGeneration: command.Identity.AttemptGeneration,
			Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest,
		})
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
		supersedePendingOperatorWorkflows(state, request.Repository, request.Issue, request.Attempt, request.Action)
		policy := command.CleanupPolicy
		effect, err = applyInvalidateAttempt(attemptRoot, stateRoot, state, invalidateAttemptCommand{
			Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt,
			ExpectedIssueGeneration: command.Identity.IssueGeneration, ExpectedAttemptGeneration: command.Identity.AttemptGeneration,
			Action: operatorTombstoneAction(request.Action), CleanupPhase: "pending", PublishedHead: publishedHead, Manifest: &manifest, CleanupPolicy: &policy,
			EffectAction: string(agentruntime.EffectCleanup), EffectRequestDigest: command.CleanupDigest,
		})
		phase = operatorPhaseCleanupPending
	case "cancel":
		if command.Runtime == nil || command.Reconciliation != nil || command.CleanupDigest != "" || command.PublishedHead != "" || command.Runtime.Action != agentruntime.EffectStop || !reflect.DeepEqual(command.Runtime.Manifest, manifest) {
			return nil, errStateConflict
		}
		begin := *command.Runtime
		if !sameOperatorBeginIdentity(begin.Identity, command.Identity) {
			return nil, errStaleStateResult
		}
		supersedePendingOperatorWorkflows(state, request.Repository, request.Issue, request.Attempt, request.Action)
		begin.Identity.SourceRevision = state.Revision
		effect, err = applyBeginRuntimeEffect(attemptRoot, stateRoot, state, begin)
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
			begin.Identity.SourceRevision = state.Revision
			effect, err = applyBeginRuntimeEffect(attemptRoot, stateRoot, state, begin)
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
			return nil, errStateConflict
		}
		if manifest.State != "running" || command.Reconciliation.Request.Action != reconciliationReviewer || command.Reconciliation.Request.Reviewer == nil || command.Reconciliation.Request.Reviewer.Mode != agentruntime.ReviewModePlan {
			return nil, errStateConflict
		}
		begin := *command.Reconciliation
		if !sameOperatorBeginIdentity(begin.Identity, command.Identity) {
			return nil, errStaleStateResult
		}
		begin.Identity.SourceRevision = state.Revision
		effect, err = applyBeginReconciliationEffect(attemptRoot, stateRoot, state, begin)
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
	}
	if err := appendOperatorReceipt(state, receipt); err != nil {
		return nil, err
	}
	return effect, nil
}

func applyStartOperatorCleanup(state *runtimeOwnerState, command startOperatorCleanupCommand) error {
	identity := command.Identity
	effect, ok := state.Effects[identity.EffectID]
	if !ok || effect.Action != string(agentruntime.EffectCleanup) || effect.State != "pending" {
		return errStaleStateResult
	}
	if err := applyAuthorizeRuntimeEffect(*state, authorizeRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectCleanup}); err != nil {
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
	if err := applyFinishReconciliationEffect(stateRoot, state, command.Finish); err != nil {
		return err
	}
	effect := state.Effects[command.Finish.Identity.EffectID]
	if effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationGitHubIssueUpdate && effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueTerminalFailure && advanceRecoverReceipts(state, effect.ID, operatorPhaseTerminal, operatorPhaseRetryAwait) {
		return nil
	}
	completeOperatorReceipts(state, command.Finish.Identity.EffectID)
	return nil
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
			receipt.Result = successfulOperatorResult(receipt.Request, state.Revision+1)
		}
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
		receipt.Result = &controlResult{Version: controlVersion, RequestID: receipt.Request.RequestID, Action: receipt.Request.Action, Status: http.StatusConflict, Error: "operator workflow superseded by " + action, OwnerRevision: state.Revision + 1}
	}
	for effectID := range remove {
		if effectID != "" && !effectReferencedByReceipt(*state, effectID) && !tombstoneReferencesEffect(*state, effectID) {
			delete(state.Effects, effectID)
		}
	}
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
		if cleanupDigest != "" || cleanupPolicy != (agentruntime.EffectCleanupPolicy{}) || tombstone.EffectID != "" || tombstone.CleanupPolicy != nil {
			return nil, errStateConflict
		}
	} else {
		effect, ok := state.Effects[tombstone.EffectID]
		if !ok || effect.Action != string(agentruntime.EffectCleanup) || effect.RequestDigest != cleanupDigest || tombstone.CleanupPolicy == nil || *tombstone.CleanupPolicy != cleanupPolicy {
			return nil, errStateConflict
		}
	}
	if tombstone.CleanupPhase == "completed" {
		receipt := controlReceipt{Request: request, State: "completed", Phase: operatorPhaseCompleted, EffectID: tombstone.EffectID, Result: successfulOperatorResult(request, 0)}
		return nil, appendOperatorReceipt(state, receipt)
	}
	effect, ok := state.Effects[tombstone.EffectID]
	if !ok || effect.State != "pending" || effect.Action != string(agentruntime.EffectCleanup) {
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
	if !ok || !sameRuntimeEffectManifestBase(record.Manifest, manifest, false) {
		return nil, false, nil
	}
	for _, receipt := range state.ControlReceipts {
		if receipt.Request.Action != "recover" || receipt.State != "pending" || receipt.Request.Repository != command.Request.Repository || receipt.Request.Issue != command.Request.Issue || receipt.Request.Attempt != command.Request.Attempt {
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
		return tombstone.CleanupPolicy == nil && tombstone.PublishedHead == "" && tombstone.CleanupPhase == "completed" && tombstone.EffectID == ""
	}
	if bareCompletedRemoval(tombstone) {
		return true
	}
	if tombstone.CleanupPolicy == nil || tombstone.EffectID == "" {
		return false
	}
	want := map[string]string{"archived": "archive", "abandoned": "abandon", "removed": "remove"}[tombstone.Action]
	return want != "" && tombstone.CleanupPolicy.Action == want && tombstone.CleanupPolicy.PublishedHead == tombstone.PublishedHead && (want == "remove" && preflightObjectID.MatchString(tombstone.PublishedHead) || want != "remove" && tombstone.PublishedHead == "")
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
		return receipt.EffectID == ""
	}
	if !slices.Contains([]string{operatorPhaseCleanupPending, operatorPhaseCleanupStarted, operatorPhaseStopPending, operatorPhaseTerminalAwait, operatorPhaseTerminal, operatorPhaseRetryAwait, operatorPhaseRetryPending, operatorPhaseReviewPending, operatorPhaseCompleted}, receipt.Phase) {
		return false
	}
	if receipt.State == "completed" {
		return receipt.Phase == operatorPhaseCompleted && receipt.Result != nil
	}
	return receipt.State == "pending" && receipt.Phase != operatorPhaseCompleted && receipt.EffectID != "" && receipt.Result == nil
}

func sameOperatorBeginIdentity(begin, operator stateResultIdentity) bool {
	return begin.Epoch == operator.Epoch && begin.SourceRevision == operator.SourceRevision && begin.IssueGeneration == operator.IssueGeneration && begin.AttemptGeneration == operator.AttemptGeneration && begin.CycleID == 0 && begin.EffectID == "" && begin.RequestDigest == ""
}

func operatorReceiptMatchesEffect(receipt controlReceipt, effect runtimeEffectIntent) bool {
	if effect.Repository != receipt.Request.Repository || effect.Issue != receipt.Request.Issue || effect.Attempt != receipt.Request.Attempt {
		return false
	}
	switch receipt.Request.Action {
	case "archive", "abandon", "remove":
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
