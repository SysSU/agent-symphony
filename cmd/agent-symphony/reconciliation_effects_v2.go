package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"reflect"
	"slices"
	"strings"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const maxReconciliationEffectBytes = 1 << 20

const maxPRRecoveryEntries = 4096

var errPRFeedbackAlreadyClaimed = errors.New("PR feedback already claimed")

type prRecoveryMutationKind string

const (
	prRecoveryClaimFeedback   prRecoveryMutationKind = "claim-feedback"
	prRecoveryQueueValidation prRecoveryMutationKind = "queue-validation"
)

type reconciliationEffectAction string

const (
	reconciliationGitHubBind         reconciliationEffectAction = "github-bind"
	reconciliationGitHubPublish      reconciliationEffectAction = "github-publish"
	reconciliationGitHubIssueUpdate  reconciliationEffectAction = "github-issue-update"
	reconciliationGitHubPRGovernance reconciliationEffectAction = "github-pr-governance"
	reconciliationReviewer           reconciliationEffectAction = "reviewer"
	reconciliationHandoffDeliver     reconciliationEffectAction = "handoff-deliver"
	reconciliationRetireCompleted    reconciliationEffectAction = "retire-completed"
	reconciliationMonitoringCheckIn  reconciliationEffectAction = "monitoring-check-in"
)

type githubIssueUpdateKind string

const (
	githubIssueTerminalFailure githubIssueUpdateKind = "terminal-failure"
	githubIssueEvidence        githubIssueUpdateKind = "evidence"
	githubIssueFindings        githubIssueUpdateKind = "findings"
	githubIssueRetry           githubIssueUpdateKind = "retry"
	githubIssueControlSnapshot githubIssueUpdateKind = "control-snapshot"
	githubIssueDependencyClear githubIssueUpdateKind = "dependency-clear"
	// The persisted wire name predates the shared owner ordering domain.
	githubIssueMachineStatus githubIssueUpdateKind = "worker-status"
	githubIssueWorkerStatus                        = githubIssueMachineStatus
)

type reconciliationEffectRequest struct {
	Action                reconciliationEffectAction      `json:"action"`
	Repository            string                          `json:"repository"`
	Issue                 int                             `json:"issue"`
	Attempt               int                             `json:"attempt,omitempty"`
	Manifest              *agentruntime.Manifest          `json:"manifest,omitempty"`
	ObservationGeneration uint64                          `json:"observation_generation"`
	ObservationCycleID    uint64                          `json:"observation_cycle_id"`
	BodyDigest            string                          `json:"body_digest"`
	ExecutionDigest       string                          `json:"execution_digest"`
	ControlGeneration     uint64                          `json:"control_generation,omitempty"`
	ControlRepair         bool                            `json:"control_repair,omitempty"`
	GitHubBind            *githubBindEffectRequest        `json:"github_bind,omitempty"`
	GitHubPublish         *githubPublishEffectRequest     `json:"github_publish,omitempty"`
	GitHubIssueUpdate     *githubIssueUpdateEffectRequest `json:"github_issue_update,omitempty"`
	GitHubPRGovernance    *githubGovernanceEffectRequest  `json:"github_pr_governance,omitempty"`
	Reviewer              *reviewerEffectRequest          `json:"reviewer,omitempty"`
	Handoff               *handoffEffectRequest           `json:"handoff,omitempty"`
	Retire                *retireCompletedEffectRequest   `json:"retire,omitempty"`
	CheckIn               *monitoringCheckInEffectRequest `json:"check_in,omitempty"`
}

type githubBindEffectRequest struct{ BaseSHA, Branch, Detail string }

type githubPublishEffectRequest struct {
	Title, BaseBranch, HeadSHA, Validation, Documentation, Decisions string
	Prepared                                                         *internalgithub.PreparedPublication
}

type githubIssueUpdateEffectRequest struct {
	Kind                    githubIssueUpdateKind
	HeadSHA, Diagnostic     string
	Findings                []string
	FailedAtUnixNano        int64
	ControlSnapshotDigest   string
	ControlSnapshotBody     string
	AttributionAttempt      int
	Dependency, PullRequest int
	Status, StatusReason    string
	StatusSequence          uint64
	StatusSource            string
	StatusSourceSequence    uint64
}

type githubGovernanceEffectRequest struct {
	PR      int
	HeadSHA string
	Policy  internalgithub.PRAdapterConfig
}

type reviewerEffectRequest struct {
	Phase                          string
	Mode, Target, BaseSHA, HeadSHA string
	Snapshot, Session              string
	DigestVersion                  int `json:"digest_version,omitempty"`
}

type handoffEffectRequest struct {
	Kind                        string
	HeadSHA, Key                string
	Findings, HumanInstructions []string
	Recovery                    *internalgithub.RecoveryHandoff
	Outcome                     *internalgithub.HandoffOutcome
	OutcomePath, OutcomeToken   string
	CandidateLaunchToken        string `json:",omitempty"`
}

type retireCompletedEffectRequest struct {
	Mode, HeadSHA string
}

type monitoringCheckInEffectRequest struct {
	Session, Binding, Payload string
}

type reconciliationEffectResult struct {
	Action             reconciliationEffectAction     `json:"action"`
	GitHubBind         *githubBindEffectResult        `json:"github_bind,omitempty"`
	GitHubPublish      *githubPublishEffectResult     `json:"github_publish,omitempty"`
	GitHubIssueUpdate  *githubIssueUpdateEffectResult `json:"github_issue_update,omitempty"`
	GitHubPRGovernance *githubGovernanceEffectResult  `json:"github_pr_governance,omitempty"`
	Reviewer           *reviewerEffectResult          `json:"reviewer,omitempty"`
	Handoff            *handoffEffectResult           `json:"handoff,omitempty"`
	Retire             *retireCompletedEffectResult   `json:"retire,omitempty"`
	CheckIn            *monitoringCheckInEffectResult `json:"check_in,omitempty"`
}

type githubBindEffectResult struct{ Observed bool }
type githubPublishEffectResult struct {
	PR                         int
	HeadSHA, BoundBodyDigest   string
	Evidence, PublishedComment bool
}
type githubIssueUpdateEffectResult struct {
	Kind     githubIssueUpdateKind
	Observed bool
}
type githubGovernanceEffectResult struct {
	PR       int
	HeadSHA  string
	Observed bool
}
type reviewerEffectResult struct {
	Phase, Status, Mode, Target, BaseSHA, HeadSHA, Snapshot, Session string
	Findings                                                         []string
	Diagnostic                                                       string
}
type handoffEffectResult struct {
	Kind, Key, OutcomePath, OutcomeToken string
	Observed                             bool
	LaunchToken                          string `json:",omitempty"`
	LaunchID                             string `json:",omitempty"`
}
type retireCompletedEffectResult struct{ ResourcesGone bool }
type monitoringCheckInEffectResult struct {
	Session  string
	Observed bool
}

func (o *stateOwner) beginReconciliationEffect(ctx context.Context, command beginReconciliationEffectCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	command.Request = cloneReconciliationRequest(command.Request)
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerBeginReconciliationEffect, beginReconciliation: command})
	return result.snapshot, result.effect, err
}

func ownerReconciliationEffectIdentity(effect runtimeEffectIntent) stateResultIdentity {
	return stateResultIdentity{Epoch: effect.IntentEpoch, SourceRevision: effect.IntentRevision, IssueGeneration: effect.IssueGeneration, AttemptGeneration: effect.AttemptGeneration, EffectID: effect.ID, RequestDigest: effect.RequestDigest}
}

func ownerReconciliationBeginIdentity(snapshot stateOwnerSnapshot, request reconciliationEffectRequest) stateResultIdentity {
	return stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]}
}

func (o *stateOwner) authorizeReconciliationEffect(ctx context.Context, command authorizeReconciliationEffectCommand) error {
	_, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerAuthorizeReconciliationEffect, authorizeReconciliation: command})
	return err
}

func (o *stateOwner) resolveInvalidatedReconciliationEffect(ctx context.Context, command resolveInvalidatedReconciliationEffectCommand) error {
	_, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerResolveInvalidatedReconciliationEffect, resolveInvalidatedReconcile: command})
	return err
}

func (o *stateOwner) finishReconciliationEffect(ctx context.Context, command finishReconciliationEffectCommand) (stateOwnerSnapshot, error) {
	command.Result = cloneReconciliationResult(command.Result)
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerFinishReconciliationEffect, finishReconciliation: command})
	return result.snapshot, err
}

func (o *stateOwner) diagnoseReconciliationEffect(ctx context.Context, command diagnoseReconciliationEffectCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerDiagnoseReconciliationEffect, diagnoseReconciliation: command})
	return result.snapshot, err
}

func (o *stateOwner) mutatePRRecovery(ctx context.Context, command mutatePRRecoveryCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerMutatePRRecovery, mutatePRRecovery: command})
	return result.snapshot, err
}

func (o *stateOwner) mutateGovernancePhase(ctx context.Context, command mutateGovernancePhaseCommand) error {
	_, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerMutateGovernancePhase, mutateGovernancePhase: command})
	return err
}

func applyMutateGovernancePhase(stateRoot string, state *runtimeOwnerState, command mutateGovernancePhaseCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationGitHubPRGovernance || !reconciliationEffectIdentityMatches(effect, command.Identity) || !validGovernancePhase(effect, command.Phase) {
		return errStaleStateResult
	}
	if err := reconciliationEffectCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	for i, phase := range effect.GovernancePhases {
		if phase.ID != command.Phase.ID {
			continue
		}
		expected := command.Phase
		expected.State = phase.State
		if phase != expected || phase.State != "admitted" && phase.State != "completed" {
			return errStateConflict
		}
		if command.Complete {
			effect.GovernancePhases[i].State = "completed"
			state.Effects[effect.ID] = effect
		}
		return nil
	}
	if command.Complete || len(effect.GovernancePhases) >= maxPRRecoveryEntries {
		return errStateConflict
	}
	command.Phase.State = "admitted"
	effect.Dispatched = true
	effect.GovernancePhases = append(effect.GovernancePhases, command.Phase)
	state.Effects[effect.ID] = effect
	return nil
}

func validGovernancePhase(effect runtimeEffectIntent, phase internalgithub.GovernancePhase) bool {
	state := phase.State
	phase.State = ""
	request := effect.Reconciliation
	if request == nil || request.GitHubPRGovernance == nil || state != "" && state != "admitted" && state != "completed" || !phase.Valid() || !boundedText(phase.Payload, maxReconciliationBodyBytes, true) {
		return false
	}
	return phase.Repository == effect.Repository && phase.PR == request.GitHubPRGovernance.PR && phase.Issue == effect.Issue && phase.Attempt == effect.Attempt && phase.HeadSHA == request.GitHubPRGovernance.HeadSHA && phase.Epoch == effect.IntentEpoch && phase.SourceRevision == effect.IntentRevision && phase.IssueGeneration == effect.IssueGeneration && phase.AttemptGeneration == effect.AttemptGeneration && slices.Contains([]string{"review-label", "decision-comment", "feedback-disposition-comment", "feedback-delegation", "validation-queue", "policy-status", "policy-failure-comment", "merge-prepared-comment", "merge-dispatched-comment", "merge-resolved-comment", "merge"}, phase.Kind)
}

func applyMutatePRRecovery(stateRoot string, state *runtimeOwnerState, command mutatePRRecoveryCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationGitHubPRGovernance || !reconciliationEffectIdentityMatches(effect, command.Identity) {
		return errStaleStateResult
	}
	if err := reconciliationEffectCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	request := effect.Reconciliation
	recovery, ok := currentPRRecovery(*state, *request)
	if !ok || !samePRRecoveryAttempt(recovery.State, command.State) || recovery.State.HeadSHA != command.State.HeadSHA {
		return errStaleStateResult
	}
	switch command.Kind {
	case prRecoveryClaimFeedback:
		claimed, err := internalgithub.ClaimFeedbackState(&recovery.State, command.Feedback)
		if err != nil {
			return errStateConflict
		}
		if !claimed {
			return errPRFeedbackAlreadyClaimed
		}
	case prRecoveryQueueValidation:
		if err := internalgithub.QueueValidationState(&recovery.State, command.State.HeadSHA); err != nil {
			return errStateConflict
		}
	default:
		return errStateConflict
	}
	state.Recoveries[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)] = recovery
	return nil
}

func samePRRecoveryAttempt(left, right internalgithub.PRState) bool {
	return left.Repository == right.Repository && left.Number == right.Number && left.Issue == right.Issue && left.Attempt == right.Attempt
}

func applyBeginReconciliationEffect(attemptRoot, stateRoot string, state *runtimeOwnerState, command beginReconciliationEffectCommand) (*runtimeEffectIntent, error) {
	request, identity := cloneReconciliationRequest(command.Request), command.Identity
	if identity.Epoch != state.Epoch || identity.SourceRevision == 0 || identity.SourceRevision > state.Revision || identity.EffectID != "" || !validReconciliationEffectRequest(state.Repository, request) {
		return nil, errStaleStateResult
	}
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	if state.IssueGenerations[issueKey] != identity.IssueGeneration {
		return nil, errStaleStateResult
	}
	if !machineStatusRequest(request) && (implementationLeaseBlocksGitHub(*state, request.Action, request.Repository, request.Issue) || reconciliationMutatesGitHub(request.Action) && (issueHasUnprovedReviewer(*state, request.Repository, request.Issue) || issueHasPendingReviewer(*state, request.Repository, request.Issue))) {
		return nil, errStateConflict
	}
	if issueHasUnprovedReviewer(*state, request.Repository, request.Issue) && request.Action == reconciliationReviewer {
		return nil, errStateConflict
	}
	if request.Action == reconciliationReviewer {
		for _, effect := range state.Effects {
			if effect.Repository == request.Repository && effect.Issue == request.Issue && effect.State == "pending" && effect.Reconciliation != nil && reconciliationMutatesGitHub(effect.Reconciliation.Action) {
				return nil, errStateConflict
			}
		}
	}
	if !machineStatusRequest(request) && (!reconciliationObservationCurrent(*state, request) || state.Observations[issueKey].LastCycleID != request.ObservationCycleID) || !validReconciliationEffectStateBindings(stateRoot, *state, request) {
		return nil, errStaleStateResult
	}
	attemptGeneration := uint64(0)
	if request.Manifest != nil {
		manifest := cloneManifest(*request.Manifest)
		if err := validateOwnerManifest(state.Repository, attemptRoot, stateRoot, manifest); err != nil || manifest.Repository != request.Repository || manifest.Issue != request.Issue || manifest.Attempt != request.Attempt {
			return nil, errStateConflict
		}
		attemptKey := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
		if state.AttemptGenerations[attemptKey] != identity.AttemptGeneration || identity.AttemptGeneration == 0 {
			return nil, errStaleStateResult
		}
		if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
			return nil, errAttemptTombstoned
		}
		record, exists := state.Attempts[attemptKey]
		if !exists || record.StopEffectID != "" || record.Generation != identity.AttemptGeneration || !sameReconciliationManifest(request, record.Manifest, manifest) {
			return nil, errStaleStateResult
		}
		attemptGeneration = identity.AttemptGeneration
	} else if identity.AttemptGeneration != 0 {
		return nil, errStaleStateResult
	}
	digest := reconciliationEffectDigest(request)
	for _, effect := range state.Effects {
		if effect.Repository != request.Repository || effect.Issue != request.Issue || effect.Attempt != request.Attempt || effect.State != "pending" {
			continue
		}
		if effect.Action == string(agentruntime.EffectMonitor) && request.Action == reconciliationReviewer && request.Reviewer != nil && request.Reviewer.Mode == agentruntime.ReviewModePlan {
			if identity.SourceRevision != state.Revision || effect.IssueGeneration != identity.IssueGeneration || effect.AttemptGeneration != attemptGeneration {
				return nil, errStaleStateResult
			}
			delete(state.Effects, effect.ID)
			continue
		}
		if effect.Action == string(request.Action) && effect.RequestDigest == digest && effect.IssueGeneration == identity.IssueGeneration && effect.AttemptGeneration == attemptGeneration {
			return cloneEffect(&effect), nil
		}
		// Retry authorization is about one failed attempt, not the collection
		// cycle that happened to observe it. A dashboard Recover and the
		// background reconciler must converge on the already-durable retry.
		if samePendingRetryEffect(effect, request, identity.IssueGeneration, attemptGeneration) {
			return cloneEffect(&effect), nil
		}
		return nil, errStateConflict
	}
	if identity.SourceRevision != state.Revision {
		return nil, errStaleStateResult
	}
	if request.Attempt > 0 {
		pruneCompletedAttemptEffects(state, request.Repository, request.Issue, request.Attempt)
	} else {
		pruneCompletedIssueScopedEffects(state, request.Repository, request.Issue)
	}
	if err := applyReconciliationBeginTransition(state, request); err != nil {
		return nil, err
	}
	gateProtocol := request.Action == reconciliationReviewer && request.Reviewer != nil && request.Reviewer.Phase == "run-observe"
	return &runtimeEffectIntent{Action: string(request.Action), Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, IssueGeneration: identity.IssueGeneration, AttemptGeneration: attemptGeneration, IntentEpoch: state.Epoch, State: "pending", RequestDigest: digest, Reconciliation: &request, ReviewerGateProtocol: gateProtocol}, nil
}

func samePendingRetryEffect(effect runtimeEffectIntent, request reconciliationEffectRequest, issueGeneration, attemptGeneration uint64) bool {
	return effect.State == "pending" && effect.Reconciliation != nil &&
		effect.Action == string(reconciliationGitHubIssueUpdate) && request.Action == reconciliationGitHubIssueUpdate &&
		effect.Reconciliation.GitHubIssueUpdate != nil && request.GitHubIssueUpdate != nil &&
		effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueRetry && request.GitHubIssueUpdate.Kind == githubIssueRetry &&
		effect.Repository == request.Repository && effect.Issue == request.Issue && effect.Attempt == request.Attempt &&
		effect.IssueGeneration == issueGeneration && effect.AttemptGeneration == attemptGeneration &&
		reflect.DeepEqual(effect.Reconciliation.Manifest, request.Manifest) &&
		effect.Reconciliation.GitHubIssueUpdate.FailedAtUnixNano == request.GitHubIssueUpdate.FailedAtUnixNano &&
		effect.Reconciliation.BodyDigest == request.BodyDigest && effect.Reconciliation.ExecutionDigest == request.ExecutionDigest
}

func applyReconciliationBeginTransition(state *runtimeOwnerState, request reconciliationEffectRequest) error {
	if request.Attempt == 0 {
		return nil
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	recovery, ok := state.Recoveries[key]
	if request.Action == reconciliationHandoffDeliver && request.Handoff.Kind == "recovery" && request.Handoff.Outcome == nil {
		if !ok {
			return errStaleStateResult
		}
		handoff, runnable, err := internalgithub.ClaimHandoffState(&recovery.State)
		if err != nil || !runnable || !reflect.DeepEqual(handoff, *request.Handoff.Recovery) {
			return errStateConflict
		}
		state.Recoveries[key] = recovery
	}
	if request.Action == reconciliationGitHubPublish && request.GitHubPublish.Prepared != nil {
		if !ok || internalgithub.PreparePublicationState(&recovery.State, *request.GitHubPublish.Prepared) != nil {
			return errStateConflict
		}
		state.Recoveries[key] = recovery
	}
	return nil
}

func applyAuthorizeReconciliationEffect(stateRoot string, state *runtimeOwnerState, command authorizeReconciliationEffectCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.Reconciliation == nil || effect.Action != string(command.Action) || !reconciliationEffectIdentityMatches(effect, command.Identity) || effect.State != "pending" {
		return errStaleStateResult
	}
	if err := reconciliationEffectCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	if reconciliationMutatesGitHub(effect.Reconciliation.Action) && !effect.Dispatched {
		effect.Dispatched = true
		state.Effects[effect.ID] = effect
	}
	return nil
}

func applyResolveInvalidatedReconciliationEffect(state *runtimeOwnerState, command resolveInvalidatedReconciliationEffectCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "invalidated" || !effect.Dispatched || effect.Reconciliation == nil || !reconciliationMutatesGitHub(effect.Reconciliation.Action) || !reconciliationEffectIdentityMatches(effect, command.Identity) || !validInvalidatedExternalOutcome(effect, command.Outcome) {
		return errStaleStateResult
	}
	key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if effect.Attempt == 0 {
		request := effect.Reconciliation.GitHubIssueUpdate
		if request != nil && request.Kind == githubIssueMachineStatus {
			status, current := state.MachineStatuses[ownerIssueKey(effect.Repository, effect.Issue)]
			if !current || request.StatusSequence >= status.Sequence || command.Outcome.StatusSequence != status.Sequence {
				return errStaleStateResult
			}
			status.AppliedSequence = status.Sequence
			state.MachineStatuses[ownerIssueKey(effect.Repository, effect.Issue)] = status
		} else if effect.Reconciliation.ControlGeneration >= controlGeneration(*state, ownerIssueKey(effect.Repository, effect.Issue)) {
			return errStaleStateResult
		}
	} else {
		tombstone, ok := state.Tombstones[key]
		if !ok || tombstone.InvalidatedGeneration != effect.AttemptGeneration {
			return errStaleStateResult
		}
		if tombstone.ExternalOutcomes == nil {
			tombstone.ExternalOutcomes = map[string]invalidatedExternalOutcome{}
		}
		tombstone.ExternalOutcomes[effect.ID] = command.Outcome
		state.Tombstones[key] = tombstone
	}
	effect.State, effect.Diagnostic = "invalidated-resolved", ""
	state.Effects[effect.ID] = effect
	return nil
}

func validInvalidatedExternalOutcome(effect runtimeEffectIntent, outcome invalidatedExternalOutcome) bool {
	if effect.Reconciliation == nil || outcome.Action != effect.Reconciliation.Action || !outcome.Observed {
		return false
	}
	switch outcome.Action {
	case reconciliationGitHubBind:
		return !outcome.Merged && outcome.PR == 0 && outcome.HeadSHA == "" && outcome.StatusSequence == 0
	case reconciliationGitHubIssueUpdate:
		if effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			return !outcome.Merged && outcome.PR == 0 && outcome.HeadSHA == "" && outcome.StatusSequence > effect.Reconciliation.GitHubIssueUpdate.StatusSequence
		}
		return !outcome.Merged && outcome.PR == 0 && outcome.HeadSHA == "" && outcome.StatusSequence == 0
	case reconciliationGitHubPublish:
		return !outcome.Merged && outcome.PR > 0 && effect.Reconciliation.GitHubPublish != nil && outcome.HeadSHA == effect.Reconciliation.GitHubPublish.HeadSHA
	case reconciliationGitHubPRGovernance:
		return outcome.Merged != outcome.Superseded && outcome.PR > 0 && effect.Reconciliation.GitHubPRGovernance != nil && outcome.PR == effect.Reconciliation.GitHubPRGovernance.PR && outcome.HeadSHA == effect.Reconciliation.GitHubPRGovernance.HeadSHA
	default:
		return false
	}
}

func applyMarkReviewerSessionRequested(stateRoot string, state *runtimeOwnerState, command markReviewerSessionRequestedCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || !effect.ReviewerGateProtocol || effect.Reconciliation == nil || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Phase != "run-observe" || !reconciliationEffectIdentityMatches(effect, command.Identity) {
		return errStaleStateResult
	}
	if effect.ReviewerSessionRequested {
		return nil
	}
	if effect.ReviewerLaunched || effect.ReviewerGroupPID != 0 {
		return errStateConflict
	}
	if err := reconciliationEffectCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	effect.ReviewerSessionRequested = true
	state.Effects[effect.ID] = effect
	return nil
}

func applyMarkPlanReviewRunning(stateRoot string, state *runtimeOwnerState, command markPlanReviewRunningCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.Reconciliation == nil || !reconciliationEffectIdentityMatches(effect, command.Identity) {
		return errStaleStateResult
	}
	request := *effect.Reconciliation
	if request.Action != reconciliationReviewer || request.Reviewer == nil || request.Reviewer.Phase != "run-observe" || request.Manifest == nil || command.GroupPID < 2 || effect.ReviewerGateProtocol && !effect.ReviewerSessionRequested {
		return errStateConflict
	}
	if err := reconciliationEffectFinishCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	if effect.ReviewerLaunched {
		if effect.ReviewerGroupPID != command.GroupPID {
			return errStateConflict
		}
		return nil
	}
	if state.ReviewerProofs == nil {
		state.ReviewerProofs = map[string]reviewerProcessProof{}
	}
	key := reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, request.Reviewer.Mode, request.Reviewer.Target)
	if old, exists := state.ReviewerProofs[key]; exists && !old.DeadProved && old.EffectID != effect.ID {
		return errStateConflict
	}
	state.ReviewerProofs[key] = reviewerProcessProof{Repository: effect.Repository, Issue: effect.Issue, Attempt: effect.Attempt, Mode: request.Reviewer.Mode, Target: request.Reviewer.Target, EffectID: effect.ID, IssueGeneration: effect.IssueGeneration, AttemptGeneration: effect.AttemptGeneration, GroupPID: command.GroupPID}
	effect.ReviewerLaunched = true
	effect.ReviewerGroupPID = command.GroupPID
	state.Effects[effect.ID] = effect
	return nil
}

func applyProveReviewerDead(state *runtimeOwnerState, command proveReviewerDeadCommand) error {
	// The original process group can disappear while a reviewer descendant
	// remains alive in another group. Only the gated never-ran case proves that
	// no reviewer process was launched.
	if !command.NeverRan {
		return errStateConflict
	}
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || !reconciliationEffectIdentityMatches(effect, command.Identity) {
		return errStaleStateResult
	}
	if command.NeverRan != (command.GroupPID == 0) || command.NeverRan && (!effect.ReviewerGateProtocol || effect.ReviewerSessionRequested) || effect.ReviewerLaunched && (command.NeverRan || effect.ReviewerGroupPID != command.GroupPID) || !effect.ReviewerLaunched && !command.NeverRan && command.GroupPID < 2 {
		return errStateConflict
	}
	key := reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, effect.Reconciliation.Reviewer.Mode, effect.Reconciliation.Reviewer.Target)
	proof, ok := state.ReviewerProofs[key]
	if ok && (proof.EffectID != effect.ID || proof.GroupPID != command.GroupPID || proof.IssueGeneration != effect.IssueGeneration || proof.AttemptGeneration != effect.AttemptGeneration) || !ok && effect.ReviewerLaunched {
		return errStateConflict
	}
	if !ok {
		proof = reviewerProcessProof{Repository: effect.Repository, Issue: effect.Issue, Attempt: effect.Attempt, Mode: effect.Reconciliation.Reviewer.Mode, Target: effect.Reconciliation.Reviewer.Target, EffectID: effect.ID, IssueGeneration: effect.IssueGeneration, AttemptGeneration: effect.AttemptGeneration, GroupPID: command.GroupPID, NeverRan: command.NeverRan}
	}
	proof.DeadProved = true
	state.ReviewerProofs[key] = proof
	if !effect.ReviewerLaunched && command.GroupPID > 1 {
		effect.ReviewerLaunched, effect.ReviewerGroupPID = true, command.GroupPID
		state.Effects[effect.ID] = effect
	}
	return nil
}

func reviewerResultDigest(result reconciliationEffectResult) string {
	body, _ := json.Marshal(result)
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}

func applySealReviewerResult(stateRoot string, state *runtimeOwnerState, command sealReviewerResultCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Phase != "run-observe" || !effect.ReviewerLaunched || effect.ReviewerGroupPID < 2 || !reconciliationEffectIdentityMatches(effect, command.Identity) || !validReconciliationEffectResult(*effect.Reconciliation, command.Result) {
		return errStaleStateResult
	}
	if err := reconciliationEffectFinishCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	reviewer := effect.Reconciliation.Reviewer
	proof, ok := state.ReviewerProofs[reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, reviewer.Mode, reviewer.Target)]
	if !ok || proof.EffectID != effect.ID || proof.GroupPID != effect.ReviewerGroupPID || proof.IssueGeneration != effect.IssueGeneration || proof.AttemptGeneration != effect.AttemptGeneration || proof.NeverRan {
		return errStateConflict
	}
	launchPath, terminalPath := reviewerLifecyclePaths(reviewer.Snapshot, reviewer.Target)
	if !command.Pane.Status.Dead || !command.Pane.Status.Ready || command.Pane.Name != reviewer.Session || command.Pane.SessionID == "" || command.Pane.PID < 2 || command.Pane.ServerPID < 2 || command.Pane.StartTime == 0 || !reviewerPaneStartMatches(command.Pane.Start, launchPath, terminalPath, reviewerIdentity(command.Identity)) {
		return errStateConflict
	}
	expected := reviewerIdentity(command.Identity)
	expected.GateProtocol, expected.SessionRequested, expected.ChildPID = effect.ReviewerGateProtocol, effect.ReviewerSessionRequested, effect.ReviewerGroupPID
	if command.Terminal.Identity != expected || command.Terminal.ExitCode == 0 && command.Terminal.Signal != 0 || command.Result.Reviewer.Status != "failed" && (command.Terminal.ExitCode != 0 || command.Terminal.Signal != 0) {
		return errStateConflict
	}
	digest := reviewerResultDigest(command.Result)
	if effect.ReviewerResultDigest != "" && effect.ReviewerResultDigest != digest {
		return errStateConflict
	}
	effect.ReviewerResultDigest = digest
	state.Effects[effect.ID] = effect
	return nil
}

func applySupersedePlanReview(state *runtimeOwnerState, command supersedePlanReviewCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok {
		return errStaleStateResult
	}
	if !reconciliationEffectIdentityMatches(effect, command.Identity) || effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Phase != "run-observe" {
		return errStaleStateResult
	}
	if effect.State == "completed" {
		return nil // A terminal receipt is immutable.
	}
	reviewer := effect.Reconciliation.Reviewer
	if command.CurrentHeadSHA != "" && (reviewer.Mode != agentruntime.ReviewModeImplementation || !validOptionalObjectID(command.CurrentHeadSHA) || command.CurrentHeadSHA == reviewer.HeadSHA) {
		return errStateConflict
	}
	proof, ok := state.ReviewerProofs[reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, reviewer.Mode, reviewer.Target)]
	if !ok || proof.EffectID != effect.ID || proof.GroupPID != effect.ReviewerGroupPID || !proof.DeadProved || proof.NeverRan == effect.ReviewerLaunched {
		return errStateConflict
	}
	if reviewer.Mode == agentruntime.ReviewModePlan && !planReviewInvalidated(*state, effect) {
		return errStateConflict
	}
	request := *effect.Reconciliation
	issueKey := ownerIssueKey(effect.Repository, effect.Issue)
	attemptKey := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	invalid := effect.ReviewerRevoked || state.IssueGenerations[issueKey] != effect.IssueGeneration || state.AttemptGenerations[attemptKey] != effect.AttemptGeneration
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		invalid = true
	}
	if !invalid {
		observation, observed := state.Observations[issueKey]
		if !observed || observation.ObservationEpoch != state.Epoch {
			return errStateConflict // No fresh owner observation proves revocation.
		}
		fact := observation.Fact
		attempt, remotelyObserved := observedReconciliationAttempt(observation, request.Attempt)
		invalid = !reconciliationFinishObservationMatches(*state, request)
		if record, exists := state.Attempts[attemptKey]; !exists || request.Manifest == nil || !sameReconciliationManifest(request, record.Manifest, *request.Manifest) {
			invalid = true
		}
		if reviewer.Mode == agentruntime.ReviewModePlan {
			invalid = invalid || !fact.DispatchAuthorized || !remotelyObserved || attempt.State != "active" && attempt.State != "review-ready"
		} else if command.CurrentHeadSHA != "" && !invalid {
			invalid = fact.CurrentAttempt == request.Attempt && observation.Fact.BaseSHA == reviewer.BaseSHA
		}
	}
	if !invalid {
		return errStateConflict
	}
	receiptFound := false
	for index := range state.ControlReceipts {
		receipt := &state.ControlReceipts[index]
		if receipt.EffectID != effect.ID || receipt.State != "pending" || receipt.Request.Action != "review-plan" {
			continue
		}
		receiptFound = true
		receipt.State, receipt.Phase = "completed", operatorPhaseCompleted
		receipt.Diagnostic = ""
		receipt.Result = &controlResult{Version: controlVersion, RequestID: receipt.Request.RequestID, Action: receipt.Request.Action, Status: http.StatusConflict, Error: "plan review was invalidated by current issue state", OwnerRevision: state.Revision + 1}
	}
	if !receiptFound && reviewer.Mode == agentruntime.ReviewModePlan {
		return errStateConflict
	}
	diagnostic := "plan review was invalidated by current issue state"
	if reviewer.Mode == agentruntime.ReviewModeImplementation {
		diagnostic = "implementation review was invalidated by current issue or worker state"
	}
	result := reconciliationEffectResult{Action: reconciliationReviewer, Reviewer: &reviewerEffectResult{Phase: reviewer.Phase, Status: "failed", Mode: reviewer.Mode, Target: reviewer.Target, BaseSHA: reviewer.BaseSHA, HeadSHA: reviewer.HeadSHA, Snapshot: reviewer.Snapshot, Session: reviewer.Session, Diagnostic: diagnostic}}
	if record, present := state.Attempts[attemptKey]; present && state.IssueGenerations[issueKey] == effect.IssueGeneration && state.AttemptGenerations[attemptKey] == effect.AttemptGeneration && request.Manifest != nil && sameReconciliationManifest(request, record.Manifest, *request.Manifest) {
		if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
			present = false
		}
		if present {
			if err := applyReconciliationEffectOutcome(state, request, result); err != nil {
				return err
			}
			if reviewer.Mode == agentruntime.ReviewModeImplementation {
				record = state.Attempts[attemptKey]
				record.Manifest.ReviewInvalidated = true
				state.Attempts[attemptKey] = record
			}
		}
	}
	effect.State, effect.ReconciliationResult, effect.Diagnostic = "completed", &result, ""
	state.Effects[effect.ID] = effect
	return nil
}

// Only a fresh owner observation (or an exact generation/tombstone change)
// may revoke a receipt-bound Plan review that is still running.
func planReviewInvalidated(state runtimeOwnerState, effect runtimeEffectIntent) bool {
	if effect.ReviewerRevoked {
		return true
	}
	request := *effect.Reconciliation
	issueKey := ownerIssueKey(effect.Repository, effect.Issue)
	attemptKey := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if state.IssueGenerations[issueKey] != effect.IssueGeneration || state.AttemptGenerations[attemptKey] != effect.AttemptGeneration {
		return true
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return true
	}
	observation, ok := state.Observations[issueKey]
	if !ok || observation.ObservationEpoch != state.Epoch {
		return false
	}
	attempt, present := observedReconciliationAttempt(observation, request.Attempt)
	record, exists := state.Attempts[attemptKey]
	return !reconciliationFinishObservationMatches(state, request) || !exists || request.Manifest == nil || !sameReconciliationManifest(request, record.Manifest, *request.Manifest) || !observation.Fact.DispatchAuthorized || !present || attempt.State != "active" && attempt.State != "review-ready"
}

func applyFinishReconciliationEffect(stateRoot string, state *runtimeOwnerState, command finishReconciliationEffectCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.Reconciliation == nil || effect.Action != string(command.Result.Action) || !reconciliationEffectIdentityMatches(effect, command.Identity) {
		return errStaleStateResult
	}
	result := cloneReconciliationResult(command.Result)
	if !validReconciliationEffectResult(*effect.Reconciliation, result) {
		return errStateConflict
	}
	if effect.Action == string(reconciliationHandoffDeliver) && effect.Reconciliation.Manifest.Version == agentruntime.ManifestVersion2 && result.Handoff.LaunchID != effect.ID {
		return errStateConflict
	}
	if effect.State == "completed" {
		if reflect.DeepEqual(effect.ReconciliationResult, &result) {
			return nil
		}
		return errStateConflict
	}
	if err := reconciliationEffectFinishCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	if effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil && effect.Reconciliation.Reviewer.Phase == "run-observe" && result.Reviewer != nil && result.Reviewer.Status != "failed" && !effect.ReviewerLaunched {
		return errStateConflict // An old or forged result cannot bypass process binding.
	}
	if effect.ReviewerGateProtocol && !effect.ReviewerLaunched && effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil && effect.Reconciliation.Reviewer.Phase == "run-observe" {
		reviewer := effect.Reconciliation.Reviewer
		proof := state.ReviewerProofs[reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, reviewer.Mode, reviewer.Target)]
		if proof.EffectID != effect.ID || !proof.DeadProved || !proof.NeverRan || proof.GroupPID != 0 {
			return errStateConflict
		}
	}
	if effect.ReviewerLaunched && effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil {
		proof, ok := state.ReviewerProofs[reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, effect.Reconciliation.Reviewer.Mode, effect.Reconciliation.Reviewer.Target)]
		if !ok || proof.EffectID != effect.ID || proof.GroupPID != effect.ReviewerGroupPID || proof.IssueGeneration != effect.IssueGeneration || proof.AttemptGeneration != effect.AttemptGeneration || proof.NeverRan || effect.ReviewerResultDigest != reviewerResultDigest(result) {
			return errStateConflict
		}
	}
	if err := applyReconciliationEffectOutcome(state, *effect.Reconciliation, result); err != nil {
		return err
	}
	if effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil && effect.Reconciliation.Reviewer.Phase == "cleanup" {
		key := reviewerProofKey(effect.Repository, effect.Issue, effect.Attempt, effect.Reconciliation.Reviewer.Mode, effect.Reconciliation.Reviewer.Target)
		proof, ok := state.ReviewerProofs[key]
		if !ok || !proof.DeadProved {
			return errStateConflict
		}
		delete(state.ReviewerProofs, key)
	}
	effect.State, effect.ReconciliationResult, effect.Diagnostic = "completed", &result, ""
	state.Effects[effect.ID] = effect
	if effect.Reconciliation.Action == reconciliationGitHubIssueUpdate && effect.Reconciliation.GitHubIssueUpdate != nil &&
		effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueTerminalFailure &&
		advanceRecoverReceipts(state, effect.ID, operatorPhaseTerminal, operatorPhaseRetryAwait) {
		return nil
	}
	if result.Reviewer != nil && result.Reviewer.Status == "failed" {
		failOperatorReceipts(state, effect.ID, result.Reviewer.Diagnostic)
		return nil
	}
	completeOperatorReceipts(state, effect.ID)
	return nil
}

func applyDiagnoseReconciliationEffect(state *runtimeOwnerState, command diagnoseReconciliationEffectCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.State != "pending" || effect.Reconciliation == nil || effect.Action != string(command.Action) || !reconciliationEffectIdentityMatches(effect, command.Identity) {
		return errStaleStateResult
	}
	if !boundedText(command.Diagnostic, maxReconciliationStringBytes, true) {
		return errStateConflict
	}
	effect.Diagnostic = command.Diagnostic
	state.Effects[effect.ID] = effect
	return nil
}

func reconciliationEffectCurrent(stateRoot string, state runtimeOwnerState, effect runtimeEffectIntent) error {
	request := effect.Reconciliation
	if request != nil && !machineStatusRequest(*request) && reconciliationMutatesGitHub(request.Action) && (issueHasUnprovedReviewer(state, effect.Repository, effect.Issue) || issueHasPendingReviewer(state, effect.Repository, effect.Issue)) {
		return errStateConflict
	}
	if request == nil || effect.ReviewerRevoked || state.IssueGenerations[ownerIssueKey(effect.Repository, effect.Issue)] != effect.IssueGeneration || !validReconciliationEffectStateBindings(stateRoot, state, *request) {
		return errStaleStateResult
	}
	if !machineStatusRequest(*request) && implementationLeaseBlocksGitHub(state, request.Action, effect.Repository, effect.Issue) {
		return errStateConflict
	}
	compatibleV1 := request.Action == reconciliationReviewer && request.Reviewer != nil && request.Reviewer.DigestVersion == 1 && reconciliationFinishObservationMatches(state, *request)
	if request.Action == reconciliationReviewer && request.Reviewer != nil && request.Reviewer.DigestVersion == 1 && !compatibleV1 || !machineStatusRequest(*request) && !reconciliationObservationCurrent(state, *request) && !compatibleV1 || compatibleV1 && state.Observations[ownerIssueKey(effect.Repository, effect.Issue)].ObservationEpoch != state.Epoch {
		return errStaleStateResult
	}
	if request.Attempt == 0 {
		return nil
	}
	key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if state.AttemptGenerations[key] != effect.AttemptGeneration {
		return errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[key]; tombstoned {
		return errAttemptTombstoned
	}
	return nil
}

// A launched implementation can leave a child outside its original process
// group. Until there is positive descendant containment, no GitHub mutation
// may consume that potentially live worker result or its issue authority.
func implementationLeaseBlocksGitHub(state runtimeOwnerState, action reconciliationEffectAction, repository string, issue int) bool {
	switch action {
	case reconciliationGitHubBind, reconciliationGitHubPublish, reconciliationGitHubIssueUpdate, reconciliationGitHubPRGovernance:
	default:
		return false
	}
	for _, record := range state.Attempts {
		manifest := record.Manifest
		if manifest.Repository == repository && manifest.Issue == issue && manifest.Version == agentruntime.ManifestVersion2 && manifest.LaunchID != "" && !agentruntime.WorkerConfinementMatches(manifest, record.Generation, activeWorkerProfileDigest(state)) {
			return true
		}
	}
	for _, effect := range state.Effects {
		if effect.Repository == repository && effect.Issue == issue && effect.Action == string(agentruntime.EffectStart) && effect.StartMayRun {
			key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
			record, current := state.Attempts[key]
			tombstone, invalidated := state.Tombstones[key]
			if current && agentruntime.WorkerConfinementBound(record.Manifest, effect.AttemptGeneration, activeWorkerProfileDigest(state)) || invalidated && tombstone.InvalidatedStart != nil && agentruntime.WorkerConfinementBound(tombstone.InvalidatedStart.Manifest, tombstone.InvalidatedGeneration, activeWorkerProfileDigest(state)) {
				continue
			}
			return true
		}
	}
	for _, tombstone := range state.Tombstones {
		if tombstone.Repository != repository || tombstone.Issue != issue {
			continue
		}
		confined := tombstone.Manifest != nil && agentruntime.WorkerConfinementBound(*tombstone.Manifest, tombstone.InvalidatedGeneration, activeWorkerProfileDigest(state))
		if !confined && (tombstone.Manifest != nil && tombstone.Manifest.Version == agentruntime.ManifestVersion2 && tombstone.Manifest.LaunchID != "" || tombstone.InvalidatedHandoff != nil) {
			return true
		}
		if tombstone.InvalidatedStart != nil && !agentruntime.WorkerConfinementBound(tombstone.InvalidatedStart.Manifest, tombstone.InvalidatedGeneration, activeWorkerProfileDigest(state)) {
			for _, candidate := range tombstone.InvalidatedStart.Candidates {
				if candidate.MayRun {
					return true
				}
			}
		}
	}
	return false
}

// reconciliationEffectFinishCurrent intentionally does not require a current
// observation epoch. An immutable completion marker proves that the external
// effect finished before restart; generations and state bindings still prevent
// a marker from completing invalidated work.
func reconciliationEffectFinishCurrent(stateRoot string, state runtimeOwnerState, effect runtimeEffectIntent) error {
	request := effect.Reconciliation
	if request != nil && !machineStatusRequest(*request) && reconciliationMutatesGitHub(request.Action) && (issueHasUnprovedReviewer(state, effect.Repository, effect.Issue) || issueHasPendingReviewer(state, effect.Repository, effect.Issue)) {
		return errStateConflict
	}
	if request == nil || effect.ReviewerRevoked || state.IssueGenerations[ownerIssueKey(effect.Repository, effect.Issue)] != effect.IssueGeneration || !machineStatusRequest(*request) && !reconciliationFinishObservationMatches(state, *request) || !validReconciliationEffectStateBindings(stateRoot, state, *request) {
		return errStaleStateResult
	}
	if !machineStatusRequest(*request) && implementationLeaseBlocksGitHub(state, request.Action, effect.Repository, effect.Issue) {
		return errStateConflict
	}
	if request.Attempt == 0 {
		return nil
	}
	key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if state.AttemptGenerations[key] != effect.AttemptGeneration {
		return errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[key]; tombstoned {
		return errAttemptTombstoned
	}
	return nil
}

func machineStatusRequest(request reconciliationEffectRequest) bool {
	return request.Action == reconciliationGitHubIssueUpdate && request.GitHubIssueUpdate != nil && request.GitHubIssueUpdate.Kind == githubIssueMachineStatus
}

func reconciliationMutatesGitHub(action reconciliationEffectAction) bool {
	return action == reconciliationGitHubBind || action == reconciliationGitHubPublish || action == reconciliationGitHubIssueUpdate || action == reconciliationGitHubPRGovernance
}

func issueHasPendingReviewer(state runtimeOwnerState, repository string, issue int) bool {
	for _, effect := range state.Effects {
		if effect.Repository == repository && effect.Issue == issue && effect.State == "pending" && effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil && effect.Reconciliation.Reviewer.Phase == "run-observe" {
			return true
		}
	}
	return false
}

// A retry command changes its own issue observation. Once its exact GitHub
// postcondition is proven, only compatible owner state is needed to record
// completion; the original observation generation is still required before
// issuing the command.
func reconciliationFinishObservationMatches(state runtimeOwnerState, request reconciliationEffectRequest) bool {
	if request.Action == reconciliationReviewer && request.Reviewer != nil && request.Reviewer.Phase == "run-observe" {
		observation, ok := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
		if !ok || !observation.Present || observation.OwnerGeneration != state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] || observation.Fact.BodyDigest != request.BodyDigest || observation.Fact.Closed || observation.Fact.Cancelled || observation.Fact.CurrentAttempt != request.Attempt {
			return false
		}
		if request.Reviewer.Mode == agentruntime.ReviewModePlan {
			return observation.Fact.DispatchAuthorized
		}
		attempt, present := observedReconciliationAttempt(observation, request.Attempt)
		if request.Reviewer.DigestVersion == 0 && observation.Generation != request.ObservationGeneration {
			return false
		}
		return !present || attempt.BaseSHA == request.Reviewer.BaseSHA && (attempt.HeadSHA == "" || attempt.HeadSHA == request.Reviewer.HeadSHA) && slices.Contains([]string{"active", "review-ready", "completed"}, attempt.State)
	}
	if reconciliationObservationMatches(state, request) {
		return true
	}
	if request.GitHubIssueUpdate == nil || request.GitHubIssueUpdate.Kind != githubIssueRetry {
		return false
	}
	observation, ok := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
	if !ok || !observation.Present || observation.OwnerGeneration != state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] || observation.Fact.BodyDigest != request.BodyDigest {
		return false
	}
	fact := observation.Fact
	return fact.CurrentAttempt == request.Attempt && !fact.Closed && !fact.Cancelled && !fact.Active &&
		(fact.RecoveryAuthorized || slices.Equal(fact.Blockers, []string{"control snapshot update is pending"}))
}

func reconciliationObservationCurrent(state runtimeOwnerState, request reconciliationEffectRequest) bool {
	return reconciliationObservationMatches(state, request) && state.Observations[ownerIssueKey(request.Repository, request.Issue)].ObservationEpoch == state.Epoch
}

func reconciliationObservationMatches(state runtimeOwnerState, request reconciliationEffectRequest) bool {
	observation, ok := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
	return ok && observation.Present && observation.Generation == request.ObservationGeneration && observation.OwnerGeneration == state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] && observation.Fact.BodyDigest == request.BodyDigest
}

func reconciliationEffectIdentityMatches(effect runtimeEffectIntent, identity stateResultIdentity) bool {
	return identity.Epoch == effect.IntentEpoch && identity.SourceRevision == effect.IntentRevision && identity.IssueGeneration == effect.IssueGeneration && identity.AttemptGeneration == effect.AttemptGeneration && identity.RequestDigest == effect.RequestDigest
}

func validPersistedReconciliationEffect(state runtimeOwnerState, effect runtimeEffectIntent) bool {
	request := effect.Reconciliation
	if request == nil || effect.Review != nil || effect.Reason != "" || effect.SupersededReviewerID != "" || !boundedText(effect.Diagnostic, maxReconciliationStringBytes, false) || effect.Action != string(request.Action) || effect.Repository != request.Repository || effect.Issue != request.Issue || effect.Attempt != request.Attempt || effect.RequestDigest != reconciliationEffectDigest(*request) || !validReconciliationEffectRequest(state.Repository, *request) || effect.IntentEpoch == 0 || effect.IntentEpoch > state.Epoch || effect.ReviewerLaunched != (effect.ReviewerGroupPID > 1) || effect.ReviewerSessionRequested && !effect.ReviewerGateProtocol || (effect.ReviewerLaunched || effect.ReviewerGateProtocol) && (request.Action != reconciliationReviewer || request.Reviewer == nil || request.Reviewer.Phase != "run-observe") || effect.ReviewerRevoked && (request.Action != reconciliationReviewer || request.Reviewer == nil || request.Reviewer.Mode != agentruntime.ReviewModePlan || request.Reviewer.Phase != "run-observe") || effect.ReviewerResultDigest != "" && (!validDigest(effect.ReviewerResultDigest) || !effect.ReviewerLaunched) {
		return false
	}
	seenGovernancePhases := map[string]bool{}
	for _, phase := range effect.GovernancePhases {
		if seenGovernancePhases[phase.ID] || !validGovernancePhase(effect, phase) {
			return false
		}
		seenGovernancePhases[phase.ID] = true
	}
	if effect.State == "pending" {
		return effect.ReconciliationResult == nil
	}
	if effect.State == "invalidated" {
		if request.GitHubIssueUpdate != nil && request.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			status, ok := state.MachineStatuses[ownerIssueKey(effect.Repository, effect.Issue)]
			return effect.Dispatched && effect.ReconciliationResult == nil && ok && request.GitHubIssueUpdate.StatusSequence < status.Sequence
		}
		return effect.Dispatched && effect.ReconciliationResult == nil && reconciliationMutatesGitHub(request.Action) && (request.Attempt > 0 || request.ControlGeneration < controlGeneration(state, ownerIssueKey(effect.Repository, effect.Issue)))
	}
	if effect.State == "invalidated-resolved" {
		if !effect.Dispatched || effect.ReconciliationResult != nil || !reconciliationMutatesGitHub(request.Action) {
			return false
		}
		if request.Attempt == 0 {
			if request.GitHubIssueUpdate != nil && request.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
				status, ok := state.MachineStatuses[ownerIssueKey(effect.Repository, effect.Issue)]
				return ok && request.GitHubIssueUpdate.StatusSequence < status.Sequence && status.AppliedSequence == status.Sequence
			}
			return request.ControlGeneration < controlGeneration(state, ownerIssueKey(effect.Repository, effect.Issue))
		}
		_, ok := state.Tombstones[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)].ExternalOutcomes[effect.ID]
		return ok
	}
	return effect.Diagnostic == "" && effect.ReconciliationResult != nil && validReconciliationEffectResult(*request, *effect.ReconciliationResult) && (effect.ReviewerResultDigest == "" || effect.ReviewerResultDigest == reviewerResultDigest(*effect.ReconciliationResult))
}

func validReconciliationEffectRequest(repository string, request reconciliationEffectRequest) bool {
	if request.Repository != repository || request.Issue < 1 || request.ObservationGeneration == 0 || request.ObservationCycleID == 0 || !validDigest(request.BodyDigest) || !validDigest(request.ExecutionDigest) {
		return false
	}
	issueScoped := reconciliationEffectIssueScoped(request)
	if !issueScoped && request.ControlGeneration != 0 || request.ControlRepair && (!issueScoped || request.ControlGeneration == 0 || request.GitHubIssueUpdate.Kind != githubIssueControlSnapshot) {
		return false
	}
	if issueScoped != (request.Attempt == 0 && request.Manifest == nil) || !issueScoped && (request.Attempt < 1 || request.Manifest == nil) {
		return false
	}
	variants := []bool{request.GitHubBind != nil, request.GitHubPublish != nil, request.GitHubIssueUpdate != nil, request.GitHubPRGovernance != nil, request.Reviewer != nil, request.Handoff != nil, request.Retire != nil, request.CheckIn != nil}
	if countTrue(variants) != 1 {
		return false
	}
	valid := false
	switch request.Action {
	case reconciliationGitHubBind:
		valid = request.GitHubBind != nil && validGitHubBind(*request.GitHubBind)
	case reconciliationGitHubPublish:
		valid = request.GitHubPublish != nil && validGitHubPublish(*request.GitHubPublish)
	case reconciliationGitHubIssueUpdate:
		valid = request.GitHubIssueUpdate != nil && validGitHubIssueUpdate(*request.GitHubIssueUpdate, issueScoped)
	case reconciliationGitHubPRGovernance:
		valid = request.GitHubPRGovernance != nil && validGitHubGovernance(*request.GitHubPRGovernance)
	case reconciliationReviewer:
		valid = request.Reviewer != nil && validReviewerRequest(*request.Reviewer)
	case reconciliationHandoffDeliver:
		valid = request.Handoff != nil && validHandoffRequest(*request.Handoff)
	case reconciliationRetireCompleted:
		valid = request.Retire != nil && validRetireRequest(*request.Retire)
	case reconciliationMonitoringCheckIn:
		valid = request.CheckIn != nil && boundedText(request.CheckIn.Session, 512, true) && validDigest(request.CheckIn.Binding) && boundedText(request.CheckIn.Payload, 4096, true)
	}
	if valid {
		valid = validReconciliationEffectBindings(request)
	}
	body, err := json.Marshal(request)
	return valid && err == nil && len(body) <= maxReconciliationEffectBytes
}

func reconciliationEffectIssueScoped(request reconciliationEffectRequest) bool {
	return request.Action == reconciliationGitHubIssueUpdate && request.GitHubIssueUpdate != nil && slices.Contains([]githubIssueUpdateKind{githubIssueControlSnapshot, githubIssueDependencyClear, githubIssueMachineStatus}, request.GitHubIssueUpdate.Kind)
}

func validReconciliationEffectBindings(request reconciliationEffectRequest) bool {
	if request.Manifest != nil && (request.Manifest.Repository != request.Repository || request.Manifest.Issue != request.Issue || request.Manifest.Attempt != request.Attempt) {
		return false
	}
	switch request.Action {
	case reconciliationGitHubBind:
		return request.Manifest != nil && request.GitHubBind.BaseSHA == request.Manifest.BaseSHA && request.GitHubBind.Branch == request.Manifest.Branch
	case reconciliationGitHubPublish:
		return request.Manifest != nil
	case reconciliationGitHubPRGovernance:
		return request.GitHubPRGovernance.Policy.Repository == request.Repository
	case reconciliationReviewer:
		return request.Manifest != nil && agentruntime.ValidReviewTarget(request.Reviewer.Mode, request.Reviewer.Target, request.Repository, request.Issue)
	case reconciliationHandoffDeliver:
		if request.Manifest == nil || request.Handoff.OutcomePath != handoffReceiptPath(request.Manifest.Worktree, request.Handoff.Key) {
			return false
		}
		if request.Manifest.Version == agentruntime.ManifestVersion2 {
			if !agentruntime.ValidLaunchToken(request.Handoff.CandidateLaunchToken) || request.Handoff.CandidateLaunchToken == request.Manifest.LaunchToken {
				return false
			}
		} else if request.Handoff.CandidateLaunchToken != "" {
			return false
		}
		if request.Handoff.Kind == "recovery" {
			recovery := request.Handoff.Recovery
			token := fmt.Sprintf("%x", sha256.Sum256([]byte("handoff-outcome\x00"+request.Handoff.Key)))
			return recovery.Repository == request.Repository && recovery.Issue == request.Issue && recovery.Attempt == request.Attempt && recovery.HeadSHA != "" && request.Handoff.OutcomeToken == token && (request.Handoff.Outcome == nil || request.Handoff.Outcome.Key == recovery.Key)
		}
		return request.Handoff.Key == "independent-review-"+request.Handoff.HeadSHA && request.Handoff.HeadSHA == request.Manifest.ReviewHead
	case reconciliationRetireCompleted:
		if request.Manifest == nil {
			return false
		}
		mode := "cleanup"
		if request.Manifest.State == "preparing" || request.Manifest.State == "running" {
			mode = "abandon"
		}
		return request.Retire.Mode == mode
	case reconciliationMonitoringCheckIn:
		return request.Manifest != nil && request.CheckIn.Session == request.Manifest.Session
	default:
		return true
	}
}

// Monitor advances UpdatedAt without changing the plan-review target or
// implementation state. All other manifest fields must still match exactly.
func sameManifestExceptUpdatedAt(current, planned agentruntime.Manifest) bool {
	planned.UpdatedAt = current.UpdatedAt
	return reflect.DeepEqual(current, planned)
}

func sameReconciliationManifest(request reconciliationEffectRequest, current, planned agentruntime.Manifest) bool {
	return reflect.DeepEqual(current, planned) || request.Action == reconciliationReviewer && request.Reviewer != nil && request.Reviewer.Mode == agentruntime.ReviewModePlan && sameManifestExceptUpdatedAt(current, planned)
}

func validReconciliationEffectStateBindings(stateRoot string, state runtimeOwnerState, request reconciliationEffectRequest) bool {
	if !validReconciliationEffectBindings(request) {
		return false
	}
	observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
	if request.Attempt == 0 {
		if request.Action != reconciliationGitHubIssueUpdate || request.GitHubIssueUpdate == nil {
			return false
		}
		if request.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			status, ok := state.MachineStatuses[ownerIssueKey(request.Repository, request.Issue)]
			return ok && status.IssueGeneration == state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] && status.Sequence == request.GitHubIssueUpdate.StatusSequence && status.Attempt == request.GitHubIssueUpdate.AttributionAttempt && status.Status == request.GitHubIssueUpdate.Status && status.Reason == request.GitHubIssueUpdate.StatusReason && status.Source == request.GitHubIssueUpdate.StatusSource && status.SourceSequence == request.GitHubIssueUpdate.StatusSourceSequence
		}
		currentControl := controlGeneration(state, ownerIssueKey(request.Repository, request.Issue))
		if request.ControlGeneration != 0 && request.ControlGeneration != currentControl || request.ControlGeneration == 0 && currentControl != 1 {
			return false
		}
		if request.ControlRepair {
			repair, ok := state.ControlRepairs[ownerIssueKey(request.Repository, request.Issue)]
			return ok && repair.Generation == request.ControlGeneration && repair.Body == request.GitHubIssueUpdate.ControlSnapshotBody && digestText(repair.Body) == request.GitHubIssueUpdate.ControlSnapshotDigest
		}
		proposal := reconciliationIssueUpdateProposal{Repository: request.Repository, Issue: request.Issue, Kind: request.GitHubIssueUpdate.Kind, ControlSnapshotDigest: request.GitHubIssueUpdate.ControlSnapshotDigest, AttributionAttempt: request.GitHubIssueUpdate.AttributionAttempt, Dependency: request.GitHubIssueUpdate.Dependency, PullRequest: request.GitHubIssueUpdate.PullRequest}
		return slices.Contains(observation.IssueUpdates, proposal)
	}
	attemptKey := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	record, ok := state.Attempts[attemptKey]
	if !ok || request.Manifest == nil || !sameReconciliationManifest(request, record.Manifest, *request.Manifest) {
		return false
	}
	manifest := record.Manifest
	fact, remotelyObserved := observedReconciliationAttempt(observation, request.Attempt)
	switch request.Action {
	case reconciliationGitHubBind:
		matches, err := reconciliationBindMatchesObservation(state, observation, manifest)
		return err == nil && matches
	case reconciliationGitHubPublish:
		publish := request.GitHubPublish
		if manifest.State != "completed" || manifest.ReviewState != "clean" || manifest.ReviewMode != agentruntime.ReviewModeImplementation || manifest.ReviewHead != publish.HeadSHA || manifest.ReviewBase == "" || manifest.ReviewTarget != manifest.ReviewBase+".."+publish.HeadSHA || publish.Title != observation.Fact.Title || publish.BaseBranch != observation.Fact.BaseBranch {
			return false
		}
		if publish.Prepared == nil {
			return true
		}
		recovery, ok := currentPRRecovery(state, request)
		if !ok || publish.Prepared.Handoff.PR != recovery.State.Number || publish.Prepared.Handoff.HeadSHA != recovery.State.HeadSHA || publish.Prepared.HeadSHA != publish.HeadSHA {
			return false
		}
		candidate := clonePRState(recovery.State)
		return internalgithub.PreparePublicationState(&candidate, *publish.Prepared) == nil
	case reconciliationGitHubPRGovernance:
		recovery, ok := currentPRRecovery(state, request)
		return remotelyObserved && ok && (fact.State == "active" || fact.State == "review-ready") && fact.PublicationConfirmed && fact.BaseSHA == manifest.BaseSHA && fact.PR == request.GitHubPRGovernance.PR && fact.HeadSHA == request.GitHubPRGovernance.HeadSHA && recovery.State.Number == fact.PR && recovery.State.HeadSHA == fact.HeadSHA
	case reconciliationReviewer:
		reviewer := request.Reviewer
		expectedSnapshot, expectedSession := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, productionSnapshotRoot(stateRoot))
		if reviewer.Snapshot != expectedSnapshot || reviewer.Session != expectedSession {
			return false
		}
		if reviewer.Phase == "cleanup" {
			return (manifest.ReviewState == "clean" || manifest.ReviewState == "findings-queued" || manifest.ReviewState == "failed") && reviewManifestMatches(manifest, reviewer) && reviewerCleanupProved(state, request.Repository, request.Issue, request.Attempt, reviewer.Mode, reviewer.Target)
		}
		if reviewer.Mode == agentruntime.ReviewModePlan {
			if manifest.State != "running" || !observation.Fact.DispatchAuthorized || reviewer.Target != fmt.Sprintf("%s#%d plan sha256:%s", request.Repository, request.Issue, request.BodyDigest) || reviewer.BaseSHA != manifest.BaseSHA || reviewer.HeadSHA != manifest.BaseSHA || !remotelyObserved || fact.BaseSHA != manifest.BaseSHA || fact.State != "active" && fact.State != "review-ready" {
				return false
			}
		} else if manifest.State != "completed" || reviewer.Target != reviewer.BaseSHA+".."+reviewer.HeadSHA || reviewer.BaseSHA != observation.Fact.BaseSHA {
			return false
		}
		detached := manifest.ReviewSnapshot == "" && manifest.ReviewSession == ""
		switch manifest.ReviewState {
		case "":
			return true
		case "failed":
			return detached && (reviewer.Mode == agentruntime.ReviewModePlan || reviewer.Mode == agentruntime.ReviewModeImplementation && (manifest.ReviewHead != reviewer.HeadSHA || manifest.ReviewInvalidated && manifest.ReviewMode == agentruntime.ReviewModeImplementation))
		case "preparing", "running":
			return reviewManifestMatches(manifest, reviewer)
		case "findings-queued":
			return manifest.ReviewHandoffAck && detached && manifest.ReviewHead != reviewer.HeadSHA
		case "clean":
			return detached && reviewer.Mode == agentruntime.ReviewModeImplementation && (manifest.ReviewMode == agentruntime.ReviewModePlan || manifest.ReviewMode == agentruntime.ReviewModeImplementation) && manifest.ReviewHead != reviewer.HeadSHA && reviewer.HeadSHA != manifest.BaseSHA
		default:
			return false
		}
	case reconciliationMonitoringCheckIn:
		return manifest.State == "running" && observation.Fact.DispatchAuthorized && observation.Fact.NeedsAttention && remotelyObserved && (fact.State == "active" || fact.State == "review-ready") && fact.BaseSHA == manifest.BaseSHA
	case reconciliationHandoffDeliver:
		if request.Handoff.Kind == "review-findings" {
			return manifest.ReviewState == "findings-queued" && manifest.ReviewHead == request.Handoff.HeadSHA && slices.Equal(manifest.ReviewFindings, request.Handoff.Findings) && !manifest.ReviewHandoffAck
		}
		recovery, ok := currentPRRecovery(state, request)
		if !remotelyObserved || !ok || fact.PR != request.Handoff.Recovery.PR || fact.HeadSHA != request.Handoff.Recovery.HeadSHA || recovery.State.Number != fact.PR || recovery.State.HeadSHA != fact.HeadSHA {
			return false
		}
		candidate := clonePRState(recovery.State)
		if request.Handoff.Outcome == nil {
			handoff, runnable, err := internalgithub.ClaimHandoffState(&candidate)
			return err == nil && runnable && reflect.DeepEqual(handoff, *request.Handoff.Recovery)
		}
		return recovery.State.HandoffReceipts[request.Handoff.Key] && internalgithub.RecoveryHandoffCurrent(&candidate, *request.Handoff.Recovery) && internalgithub.ApplyHandoffOutcome(&candidate, *request.Handoff.Recovery, *request.Handoff.Outcome) == nil
	case reconciliationRetireCompleted:
		wantMode := "cleanup"
		if manifest.State == "preparing" || manifest.State == "running" {
			wantMode = "abandon"
		}
		return remotelyObserved && fact.State == "completed" && fact.PR > 0 && fact.BaseSHA == manifest.BaseSHA && fact.HeadSHA == request.Retire.HeadSHA && manifest.ReviewHead == fact.HeadSHA && manifest.ReviewSnapshot == "" && manifest.ReviewSession == "" && request.Retire.Mode == wantMode
	case reconciliationGitHubIssueUpdate:
		switch request.GitHubIssueUpdate.Kind {
		case githubIssueEvidence:
			return remotelyObserved && fact.PR > 0 && fact.HeadSHA == request.GitHubIssueUpdate.HeadSHA && manifest.State == "completed" && manifest.ReviewHead == fact.HeadSHA
		case githubIssueFindings:
			return manifest.ReviewState == "findings-queued" && manifest.ReviewHead == request.GitHubIssueUpdate.HeadSHA && slices.Equal(manifest.ReviewFindings, request.GitHubIssueUpdate.Findings) && !manifest.ReviewHandoffQueued && !manifest.ReviewHandoffAck
		case githubIssueTerminalFailure, githubIssueRetry:
			return (manifest.State == "failed" || manifest.State == "cancelled") && request.GitHubIssueUpdate.FailedAtUnixNano == manifest.UpdatedAt.UnixNano() && (observation.Fact.Attempt == request.Attempt || observation.Fact.CurrentAttempt == request.Attempt)
		case githubIssueDependencyClear:
			return slices.Contains(observation.Fact.Dependencies, request.GitHubIssueUpdate.Dependency) && slices.Contains(observation.Fact.SatisfiedDependencies, request.GitHubIssueUpdate.Dependency)
		case githubIssueMachineStatus:
			return false
		default:
			return true
		}
	default:
		return true
	}
}

func reviewManifestMatches(manifest agentruntime.Manifest, request *reviewerEffectRequest) bool {
	return request != nil && manifest.ReviewMode == request.Mode && manifest.ReviewTarget == request.Target && manifest.ReviewBase == request.BaseSHA && manifest.ReviewHead == request.HeadSHA && manifest.ReviewSnapshot == request.Snapshot && manifest.ReviewSession == request.Session
}

func reviewerCleanupProved(state runtimeOwnerState, repository string, issue, attempt int, mode, target string) bool {
	proof, ok := state.ReviewerProofs[reviewerProofKey(repository, issue, attempt, mode, target)]
	return ok && proof.DeadProved && proof.NeverRan && proof.IssueGeneration == state.IssueGenerations[ownerIssueKey(repository, issue)] && proof.AttemptGeneration == state.AttemptGenerations[ownerAttemptKey(repository, issue, attempt)]
}

func currentPRRecovery(state runtimeOwnerState, request reconciliationEffectRequest) (runtimePRRecovery, bool) {
	recovery, ok := state.Recoveries[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]
	return recovery, ok && recovery.IssueGeneration == state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] && recovery.AttemptGeneration == state.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]
}

func validateRuntimePRRecoveries(state runtimeOwnerState) error {
	if len(state.Recoveries) > maxPRRecoveryEntries {
		return errors.New("runtime owner PR recovery count is invalid")
	}
	seenPRs := map[int]bool{}
	for key, recovery := range state.Recoveries {
		pr := recovery.State
		expected := ownerAttemptKey(pr.Repository, pr.Issue, pr.Attempt)
		if key != expected || pr.Repository != state.Repository || pr.Number < 1 || seenPRs[pr.Number] || recovery.IssueGeneration == 0 || recovery.AttemptGeneration == 0 || recovery.IssueGeneration != state.IssueGenerations[ownerIssueKey(pr.Repository, pr.Issue)] || recovery.AttemptGeneration != state.AttemptGenerations[key] {
			return errors.New("runtime owner PR recovery identity is invalid")
		}
		if _, tombstoned := state.Tombstones[key]; tombstoned {
			return errors.New("runtime owner PR recovery is tombstoned")
		}
		if _, current := state.Attempts[key]; !current || !validPRState(pr) {
			return errors.New("runtime owner PR recovery state is invalid")
		}
		seenPRs[pr.Number] = true
	}
	return nil
}

func validPRState(state internalgithub.PRState) bool {
	if state.Repository == "" || state.Number < 1 || state.Issue < 1 || state.Attempt < 1 || !validOptionalObjectID(state.HeadSHA) || state.HeadSHA == "" || !validOptionalObjectID(state.CheckHead) || !validOptionalObjectID(state.ValidationQueuedSHA) || !validOptionalObjectID(state.ValidationInFlightSHA) || !validOptionalObjectID(state.MergeAttemptSHA) || len(state.Facts.Feedback) > maxPRRecoveryEntries || len(state.PendingDispositions) > maxPRRecoveryEntries || len(state.ConfirmedDispositions) > maxPRRecoveryEntries || len(state.Decisions) > maxPRRecoveryEntries || len(state.HandoffReceipts) > maxPRRecoveryEntries {
		return false
	}
	for _, value := range []string{state.PolicyStatus, state.ValidationResult, state.MergePhase} {
		if !boundedText(value, maxReconciliationStringBytes, false) {
			return false
		}
	}
	for _, value := range []string{state.ValidationEvidence, state.Facts.ValidationSHA, state.Facts.DocumentationSHA} {
		if !boundedText(value, maxReconciliationBodyBytes, false) {
			return false
		}
	}
	for _, feedback := range append(append(slices.Clone(state.Facts.Feedback), state.PendingDispositions...), state.ConfirmedDispositions...) {
		if !validRecoveryFeedback(feedback) {
			return false
		}
	}
	for _, decision := range state.Decisions {
		if !boundedText(decision.ID, maxReconciliationStringBytes, true) || !boundedText(decision.Body, maxReconciliationBodyBytes, true) {
			return false
		}
	}
	for key, present := range state.HandoffReceipts {
		if !present || !validDigest(key) {
			return false
		}
	}
	return state.PreparedPublication == nil || validPreparedPublication(state, *state.PreparedPublication)
}

func validRecoveryFeedback(feedback internalgithub.Feedback) bool {
	return feedback.ID > 0 && feedback.ActorID > 0 && slices.Contains([]string{"issue", "conversation", "inline", "review"}, feedback.Source) && boundedText(feedback.Body, maxReconciliationBodyBytes, true) && boundedText(feedback.Evidence, maxReconciliationBodyBytes, false) && slices.Contains([]internalgithub.FeedbackState{"", internalgithub.FeedbackPending, internalgithub.FeedbackAddressed, internalgithub.FeedbackBlocked}, feedback.State) && slices.Contains([]internalgithub.FeedbackExecutionState{"", internalgithub.FeedbackClaimed, internalgithub.FeedbackInFlight, internalgithub.FeedbackCompleted}, feedback.Execution)
}

func validRecoveryHandoff(handoff internalgithub.RecoveryHandoff) bool {
	if !validDigest(handoff.Key) || handoff.Repository == "" || handoff.PR < 1 || handoff.Issue < 1 || handoff.Attempt < 1 || !validOptionalObjectID(handoff.HeadSHA) || handoff.HeadSHA == "" || len(handoff.Feedback) > maxPRRecoveryEntries || !handoff.Validation && len(handoff.Feedback) == 0 {
		return false
	}
	for _, feedback := range handoff.Feedback {
		if !validRecoveryFeedback(feedback) || feedback.Execution != internalgithub.FeedbackInFlight {
			return false
		}
	}
	return true
}

func validHandoffOutcome(outcome internalgithub.HandoffOutcome, key string) bool {
	if outcome.Key != key || len(outcome.Feedback) > maxPRRecoveryEntries || !boundedText(outcome.ValidationResult, maxReconciliationStringBytes, false) || !boundedText(outcome.ValidationEvidence, maxReconciliationBodyBytes, false) {
		return false
	}
	for _, feedback := range outcome.Feedback {
		if feedback.ID <= 0 || !slices.Contains([]string{"issue", "conversation", "inline", "review"}, feedback.Source) || !slices.Contains([]internalgithub.FeedbackState{internalgithub.FeedbackAddressed, internalgithub.FeedbackBlocked}, feedback.State) || !boundedText(feedback.Evidence, maxReconciliationBodyBytes, true) {
			return false
		}
	}
	return true
}

func validPreparedPublication(state internalgithub.PRState, prepared internalgithub.PreparedPublication) bool {
	if !validOptionalObjectID(prepared.HeadSHA) || prepared.HeadSHA == "" || prepared.HeadSHA == state.HeadSHA || prepared.Handoff.Repository != state.Repository || prepared.Handoff.PR != state.Number || prepared.Handoff.Issue != state.Issue || prepared.Handoff.Attempt != state.Attempt || prepared.Handoff.HeadSHA != state.HeadSHA {
		return false
	}
	if prepared.Handoff.Key == "" {
		return !prepared.Handoff.Validation && prepared.Handoff.ValidationGeneration == 0 && prepared.Handoff.Feedback == nil && reflect.DeepEqual(prepared.Outcome, internalgithub.HandoffOutcome{})
	}
	return validRecoveryHandoff(prepared.Handoff) && state.HandoffReceipts[prepared.Handoff.Key] && validHandoffOutcome(prepared.Outcome, prepared.Handoff.Key)
}

func observedReconciliationAttempt(observation reconciliationObservation, attempt int) (reconciliationAttemptFact, bool) {
	entry, ok := observation.Attempts[ownerAttemptKey(observation.Fact.Repository, observation.Fact.Issue, attempt)]
	if ok && entry.Present && entry.SourceIssueGeneration == observation.Generation {
		return entry.Fact, true
	}
	if observation.Fact.ActiveAttempt != nil && observation.Fact.ActiveAttempt.Attempt == attempt {
		return *observation.Fact.ActiveAttempt, true
	}
	for _, terminal := range observation.Fact.TerminalAttempts {
		if terminal.Attempt == attempt {
			return terminal, true
		}
	}
	return reconciliationAttemptFact{}, false
}

func expectedPublishedBodyDigest(request reconciliationEffectRequest, pr int) string {
	if request.GitHubPublish == nil || request.Manifest == nil || pr < 1 {
		return ""
	}
	publish := request.GitHubPublish
	body, err := internalgithub.PullRequestBody(request.Issue, request.Attempt, publish.Validation, publish.Documentation, publish.Decisions)
	if err != nil {
		return ""
	}
	bound, err := internalgithub.BindPullRequestBody(body, request.Issue, request.Attempt, request.Manifest.Branch, publish.HeadSHA, pr)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256([]byte(bound))
	return hex.EncodeToString(digest[:])
}

func pruneCompletedIssueScopedEffects(state *runtimeOwnerState, repository string, issue int) {
	for id, effect := range state.Effects {
		if effect.Repository == repository && effect.Issue == issue && effect.Attempt == 0 && effect.State == "completed" {
			delete(state.Effects, id)
		}
	}
}

func deleteIssueRecoveries(state *runtimeOwnerState, repository string, issue int) {
	for key := range state.Recoveries {
		repo, number, _, ok := parseOwnerAttemptKey(key)
		if ok && repo == repository && number == issue {
			delete(state.Recoveries, key)
		}
	}
}

func deleteIssueScopedEffects(state *runtimeOwnerState, repository string, issue int) {
	for id, effect := range state.Effects {
		if !effectReferencedByReceipt(*state, id) && effect.Repository == repository && effect.Issue == issue && effect.Attempt == 0 {
			delete(state.Effects, id)
		}
	}
}

func validReconciliationEffectResult(request reconciliationEffectRequest, result reconciliationEffectResult) bool {
	if result.Action != request.Action || countTrue([]bool{result.GitHubBind != nil, result.GitHubPublish != nil, result.GitHubIssueUpdate != nil, result.GitHubPRGovernance != nil, result.Reviewer != nil, result.Handoff != nil, result.Retire != nil, result.CheckIn != nil}) != 1 {
		return false
	}
	valid := false
	switch request.Action {
	case reconciliationGitHubBind:
		valid = result.GitHubBind != nil && result.GitHubBind.Observed
	case reconciliationGitHubPublish:
		valid = result.GitHubPublish != nil && result.GitHubPublish.PR > 0 && result.GitHubPublish.HeadSHA == request.GitHubPublish.HeadSHA && result.GitHubPublish.BoundBodyDigest == expectedPublishedBodyDigest(request, result.GitHubPublish.PR) && result.GitHubPublish.Evidence && result.GitHubPublish.PublishedComment
	case reconciliationGitHubIssueUpdate:
		valid = result.GitHubIssueUpdate != nil && result.GitHubIssueUpdate.Kind == request.GitHubIssueUpdate.Kind && result.GitHubIssueUpdate.Observed
	case reconciliationGitHubPRGovernance:
		valid = result.GitHubPRGovernance != nil && result.GitHubPRGovernance.PR == request.GitHubPRGovernance.PR && result.GitHubPRGovernance.HeadSHA == request.GitHubPRGovernance.HeadSHA && result.GitHubPRGovernance.Observed
	case reconciliationReviewer:
		valid = result.Reviewer != nil && validReviewerResult(*request.Reviewer, *result.Reviewer)
	case reconciliationHandoffDeliver:
		valid = result.Handoff != nil && result.Handoff.Kind == request.Handoff.Kind && result.Handoff.Key == request.Handoff.Key && result.Handoff.OutcomePath == request.Handoff.OutcomePath && result.Handoff.OutcomeToken == request.Handoff.OutcomeToken && result.Handoff.Observed
		if valid && request.Manifest.Version == agentruntime.ManifestVersion2 {
			valid = result.Handoff.LaunchToken == request.Handoff.CandidateLaunchToken && result.Handoff.LaunchID != ""
		} else if valid {
			valid = result.Handoff.LaunchToken == "" && result.Handoff.LaunchID == ""
		}
	case reconciliationRetireCompleted:
		valid = result.Retire != nil && result.Retire.ResourcesGone
	case reconciliationMonitoringCheckIn:
		valid = result.CheckIn != nil && result.CheckIn.Observed && result.CheckIn.Session == request.CheckIn.Session
	}
	body, err := json.Marshal(result)
	return valid && err == nil && len(body) <= maxReconciliationEffectBytes
}

func applyReconciliationEffectOutcome(state *runtimeOwnerState, request reconciliationEffectRequest, result reconciliationEffectResult) error {
	if request.Attempt == 0 {
		if request.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			key := ownerIssueKey(request.Repository, request.Issue)
			status, ok := state.MachineStatuses[key]
			if !ok || status.Sequence != request.GitHubIssueUpdate.StatusSequence {
				return errStaleStateResult
			}
			status.AppliedSequence = status.Sequence
			state.MachineStatuses[key] = status
			if status.Source == "worker" {
				attemptKey := ownerAttemptKey(status.Repository, status.Issue, status.Attempt)
				if record, exists := state.Attempts[attemptKey]; exists && record.Generation == status.AttemptGeneration {
					record.Manifest.WorkerStatusApplied = status.SourceSequence
					state.Attempts[attemptKey] = record
				}
			}
			for id, effect := range state.Effects {
				if effect.Repository == request.Repository && effect.Issue == request.Issue && effect.State == "invalidated-resolved" && effect.Reconciliation != nil && effect.Reconciliation.GitHubIssueUpdate != nil && effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
					delete(state.Effects, id)
				}
			}
			return nil
		}
		if request.ControlRepair {
			key := ownerIssueKey(request.Repository, request.Issue)
			repair, ok := state.ControlRepairs[key]
			if !ok || repair.Generation != request.ControlGeneration || repair.Body != request.GitHubIssueUpdate.ControlSnapshotBody {
				return errStaleStateResult
			}
			for _, effect := range state.Effects {
				if effect.Repository == request.Repository && effect.Issue == request.Issue && effect.State == "invalidated" {
					return errStateConflict
				}
			}
			delete(state.ControlRepairs, key)
			for id, effect := range state.Effects {
				if effect.Repository == request.Repository && effect.Issue == request.Issue && effect.State == "invalidated-resolved" {
					delete(state.Effects, id)
				}
			}
		}
		return nil
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	record, ok := state.Attempts[key]
	if !ok {
		return errStaleStateResult
	}
	switch request.Action {
	case reconciliationGitHubPublish:
		if request.GitHubPublish.Prepared == nil {
			if _, exists := state.Recoveries[key]; exists {
				return errStateConflict
			}
			state.Recoveries[key] = runtimePRRecovery{IssueGeneration: state.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: state.AttemptGenerations[key], State: internalgithub.PRState{Repository: request.Repository, Number: result.GitHubPublish.PR, Issue: request.Issue, Attempt: request.Attempt, HeadSHA: result.GitHubPublish.HeadSHA, HandoffReceipts: map[string]bool{}}}
			return nil
		}
		recovery, ok := currentPRRecovery(*state, request)
		if !ok || !reflect.DeepEqual(recovery.State.PreparedPublication, request.GitHubPublish.Prepared) {
			return errStaleStateResult
		}
		if err := internalgithub.CompletePreparedPublication(&recovery.State, *request.GitHubPublish.Prepared); err != nil {
			return errStateConflict
		}
		state.Recoveries[key] = recovery
	case reconciliationReviewer:
		reviewer, outcome := request.Reviewer, result.Reviewer
		if reviewer.Phase == "cleanup" {
			record.Manifest.ReviewSnapshot, record.Manifest.ReviewSession = "", ""
		} else {
			record.Manifest.ReviewState, record.Manifest.ReviewMode, record.Manifest.ReviewTarget = outcome.Status, reviewer.Mode, reviewer.Target
			record.Manifest.ReviewInvalidated = false
			record.Manifest.ReviewBase, record.Manifest.ReviewHead = reviewer.BaseSHA, reviewer.HeadSHA
			record.Manifest.ReviewSnapshot, record.Manifest.ReviewSession = reviewer.Snapshot, reviewer.Session
			record.Manifest.ReviewFindings = slices.Clone(outcome.Findings)
			record.Manifest.ReviewDiagnostic = outcome.Diagnostic
			record.Manifest.ReviewHandoffQueued, record.Manifest.ReviewHandoffAck = false, false
			if outcome.Status == "findings" {
				record.Manifest.ReviewState = "findings-queued"
			}
		}
		state.Attempts[key] = record
	case reconciliationGitHubIssueUpdate:
		if request.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			return errStaleStateResult
		}
	case reconciliationHandoffDeliver:
		if request.Manifest.Version == agentruntime.ManifestVersion2 {
			record.Manifest.LaunchToken, record.Manifest.LaunchID = result.Handoff.LaunchToken, result.Handoff.LaunchID
		}
		if request.Handoff.Kind == "review-findings" {
			record.Manifest.ReviewHandoffQueued, record.Manifest.ReviewHandoffAck = true, true
			if record.Manifest.State == "completed" {
				record.Manifest.State, record.Manifest.Diagnostic = "running", ""
			}
			state.Attempts[key] = record
			return nil
		}
		recovery, ok := currentPRRecovery(*state, request)
		if !ok {
			return errStaleStateResult
		}
		var err error
		if request.Handoff.Outcome == nil {
			err = internalgithub.ReceiptHandoffState(&recovery.State, *request.Handoff.Recovery)
		} else {
			err = internalgithub.ApplyHandoffOutcome(&recovery.State, *request.Handoff.Recovery, *request.Handoff.Outcome)
		}
		if err != nil {
			return errStateConflict
		}
		state.Recoveries[key] = recovery
		state.Attempts[key] = record
	case reconciliationRetireCompleted:
		delete(state.Attempts, key)
		delete(state.Recoveries, key)
	}
	return nil
}

func validGitHubBind(request githubBindEffectRequest) bool {
	return validOptionalObjectID(request.BaseSHA) && request.BaseSHA != "" && boundedText(request.Branch, 4096, true) && boundedText(request.Detail, 64<<10, true)
}

func validGitHubPublish(request githubPublishEffectRequest) bool {
	return boundedText(request.Title, 4096, true) && boundedText(request.BaseBranch, 4096, true) && validOptionalObjectID(request.HeadSHA) && request.HeadSHA != "" && boundedText(request.Validation, 64<<10, true) && boundedText(request.Documentation, 64<<10, true) && boundedText(request.Decisions, 64<<10, false)
}

func validGitHubIssueUpdate(request githubIssueUpdateEffectRequest, issueScoped bool) bool {
	if request.Kind != githubIssueMachineStatus && (request.Status != "" || request.StatusReason != "" || request.StatusSequence != 0 || request.StatusSource != "" || request.StatusSourceSequence != 0) {
		return false
	}
	if request.Kind != githubIssueControlSnapshot && request.ControlSnapshotBody != "" {
		return false
	}
	switch request.Kind {
	case githubIssueTerminalFailure:
		return !issueScoped && request.FailedAtUnixNano != 0 && boundedText(request.Diagnostic, 4096, true) && request.HeadSHA == "" && request.Findings == nil && request.ControlSnapshotDigest == "" && request.AttributionAttempt == 0 && request.Dependency == 0 && request.PullRequest == 0
	case githubIssueEvidence:
		return !issueScoped && validOptionalObjectID(request.HeadSHA) && request.HeadSHA != "" && request.Diagnostic == "" && request.Findings == nil && request.FailedAtUnixNano == 0 && request.ControlSnapshotDigest == "" && request.AttributionAttempt == 0 && request.Dependency == 0 && request.PullRequest == 0
	case githubIssueFindings:
		return !issueScoped && validOptionalObjectID(request.HeadSHA) && request.HeadSHA != "" && validFindings(request.Findings) && request.Diagnostic == "" && request.FailedAtUnixNano == 0 && request.ControlSnapshotDigest == "" && request.AttributionAttempt == 0 && request.Dependency == 0 && request.PullRequest == 0
	case githubIssueRetry:
		return !issueScoped && request.FailedAtUnixNano != 0 && request.HeadSHA == "" && request.Diagnostic == "" && request.Findings == nil && request.ControlSnapshotDigest == "" && request.AttributionAttempt == 0 && request.Dependency == 0 && request.PullRequest == 0
	case githubIssueControlSnapshot:
		return issueScoped && validDigest(request.ControlSnapshotDigest) && (request.ControlSnapshotBody == "" || digestText(request.ControlSnapshotBody) == request.ControlSnapshotDigest) && request.HeadSHA == "" && request.Diagnostic == "" && request.Findings == nil && request.FailedAtUnixNano == 0 && request.AttributionAttempt == 0 && request.Dependency == 0 && request.PullRequest == 0
	case githubIssueDependencyClear:
		return issueScoped && request.AttributionAttempt > 0 && request.Dependency > 0 && request.PullRequest >= 0 && request.HeadSHA == "" && request.Diagnostic == "" && request.Findings == nil && request.FailedAtUnixNano == 0 && request.ControlSnapshotDigest == ""
	case githubIssueMachineStatus:
		return issueScoped && request.AttributionAttempt > 0 && request.Dependency == 0 && request.PullRequest == 0 && request.HeadSHA == "" && request.Diagnostic == "" && request.Findings == nil && request.FailedAtUnixNano == 0 && request.ControlSnapshotDigest == "" && slices.Contains([]string{"needs-attention", "clear"}, request.Status) && boundedText(request.StatusReason, 1024, true) && request.StatusSequence > 0 && request.StatusSourceSequence > 0 && slices.Contains([]string{"worker", "dependency", "orchestrator", "destructive"}, request.StatusSource)
	default:
		return false
	}
}

func validGitHubGovernance(request githubGovernanceEffectRequest) bool {
	return request.PR > 0 && validOptionalObjectID(request.HeadSHA) && request.HeadSHA != "" && request.Policy.Repository != "" && request.Policy.ActorID > 0
}

func validReviewerRequest(request reviewerEffectRequest) bool {
	if request.Phase != "run-observe" && request.Phase != "cleanup" || !slices.Contains([]string{agentruntime.ReviewModePlan, agentruntime.ReviewModeImplementation}, request.Mode) || request.DigestVersion < 0 || request.DigestVersion > 1 || request.DigestVersion == 1 && (request.Mode != agentruntime.ReviewModeImplementation || request.Phase != "run-observe") || !boundedText(request.Target, 4096, true) || !validOptionalObjectID(request.BaseSHA) || !validOptionalObjectID(request.HeadSHA) || !boundedText(request.Snapshot, 4096, true) || !boundedText(request.Session, 4096, true) {
		return false
	}
	if request.Phase == "cleanup" {
		return true
	}
	return request.BaseSHA != "" && request.HeadSHA != ""
}

func validHandoffRequest(request handoffEffectRequest) bool {
	if !slices.Contains([]string{"review-findings", "recovery"}, request.Kind) || !boundedText(request.Key, 4096, true) || !boundedText(request.OutcomePath, 4096, true) {
		return false
	}
	if request.Kind == "review-findings" {
		return request.Recovery == nil && request.Outcome == nil && validOptionalObjectID(request.HeadSHA) && request.HeadSHA != "" && request.OutcomeToken == request.HeadSHA && validFindings(request.Findings) && validStringList(request.HumanInstructions, 1000, 4096)
	}
	return request.Recovery != nil && validRecoveryHandoff(*request.Recovery) && request.Recovery.Key == request.Key && validDigest(request.OutcomeToken) && request.HeadSHA == "" && request.Findings == nil && request.HumanInstructions == nil && (request.Outcome == nil || validHandoffOutcome(*request.Outcome, request.Key))
}

func validRetireRequest(request retireCompletedEffectRequest) bool {
	return slices.Contains([]string{"cleanup", "abandon"}, request.Mode) && validOptionalObjectID(request.HeadSHA) && request.HeadSHA != ""
}

func validReviewerResult(request reviewerEffectRequest, result reviewerEffectResult) bool {
	if result.Phase != request.Phase || result.Mode != request.Mode || result.Target != request.Target || result.BaseSHA != request.BaseSHA || result.HeadSHA != request.HeadSHA || result.Snapshot != request.Snapshot || result.Session != request.Session {
		return false
	}
	if request.Phase == "cleanup" {
		return result.Status == "cleaned" && len(result.Findings) == 0 && result.Diagnostic == ""
	}
	if result.Status == "failed" {
		return len(result.Findings) == 0 && boundedText(result.Diagnostic, 4096, true)
	}
	if result.Diagnostic != "" {
		return false
	}
	if result.Status == "clean" {
		return len(result.Findings) == 0
	}
	return result.Status == "findings" && validFindings(result.Findings)
}

func validFindings(findings []string) bool {
	return len(findings) > 0 && len(findings) <= 100 && validStringList(findings, 100, 4096)
}

func boundedText(value string, limit int, required bool) bool {
	return (!required || strings.TrimSpace(value) != "") && len(value) <= limit && !strings.ContainsRune(value, 0)
}

func countTrue(values []bool) int {
	count := 0
	for _, value := range values {
		if value {
			count++
		}
	}
	return count
}

func reconciliationEffectDigest(request reconciliationEffectRequest) string {
	body, _ := json.Marshal(request)
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func cloneReconciliationEffectRequest(request *reconciliationEffectRequest) *reconciliationEffectRequest {
	if request == nil {
		return nil
	}
	clone := cloneReconciliationRequest(*request)
	return &clone
}

func cloneReconciliationRequest(request reconciliationEffectRequest) reconciliationEffectRequest {
	if request.Manifest != nil {
		manifest := cloneManifest(*request.Manifest)
		request.Manifest = &manifest
	}
	if request.GitHubIssueUpdate != nil {
		update := *request.GitHubIssueUpdate
		update.Findings = slices.Clone(update.Findings)
		request.GitHubIssueUpdate = &update
	}
	if request.GitHubBind != nil {
		value := *request.GitHubBind
		request.GitHubBind = &value
	}
	if request.GitHubPublish != nil {
		publish := *request.GitHubPublish
		publish.Prepared = clonePreparedPublication(publish.Prepared)
		request.GitHubPublish = &publish
	}
	if request.GitHubPRGovernance != nil {
		value := *request.GitHubPRGovernance
		request.GitHubPRGovernance = &value
	}
	if request.Reviewer != nil {
		value := *request.Reviewer
		request.Reviewer = &value
	}
	if request.Handoff != nil {
		handoff := *request.Handoff
		handoff.Findings = slices.Clone(handoff.Findings)
		handoff.HumanInstructions = slices.Clone(handoff.HumanInstructions)
		if handoff.Recovery != nil {
			recovery := *handoff.Recovery
			recovery.Feedback = slices.Clone(recovery.Feedback)
			handoff.Recovery = &recovery
		}
		if handoff.Outcome != nil {
			outcome := cloneHandoffOutcome(*handoff.Outcome)
			handoff.Outcome = &outcome
		}
		request.Handoff = &handoff
	}
	if request.Retire != nil {
		value := *request.Retire
		request.Retire = &value
	}
	if request.CheckIn != nil {
		value := *request.CheckIn
		request.CheckIn = &value
	}
	return request
}

func clonePRState(state internalgithub.PRState) internalgithub.PRState {
	state.Facts.Feedback = slices.Clone(state.Facts.Feedback)
	state.Decisions = slices.Clone(state.Decisions)
	state.PendingDispositions = slices.Clone(state.PendingDispositions)
	state.ConfirmedDispositions = slices.Clone(state.ConfirmedDispositions)
	receipts := state.HandoffReceipts
	state.HandoffReceipts = make(map[string]bool, len(receipts))
	for key, value := range receipts {
		state.HandoffReceipts[key] = value
	}
	state.PreparedPublication = clonePreparedPublication(state.PreparedPublication)
	return state
}

func cloneHandoffOutcome(outcome internalgithub.HandoffOutcome) internalgithub.HandoffOutcome {
	outcome.Feedback = slices.Clone(outcome.Feedback)
	return outcome
}

func clonePreparedPublication(prepared *internalgithub.PreparedPublication) *internalgithub.PreparedPublication {
	if prepared == nil {
		return nil
	}
	clone := *prepared
	clone.Handoff.Feedback = slices.Clone(prepared.Handoff.Feedback)
	clone.Outcome.Feedback = slices.Clone(prepared.Outcome.Feedback)
	return &clone
}

func cloneReconciliationEffectResult(result *reconciliationEffectResult) *reconciliationEffectResult {
	if result == nil {
		return nil
	}
	clone := cloneReconciliationResult(*result)
	return &clone
}

func cloneReconciliationResult(result reconciliationEffectResult) reconciliationEffectResult {
	if result.GitHubBind != nil {
		value := *result.GitHubBind
		result.GitHubBind = &value
	}
	if result.GitHubPublish != nil {
		value := *result.GitHubPublish
		result.GitHubPublish = &value
	}
	if result.GitHubIssueUpdate != nil {
		value := *result.GitHubIssueUpdate
		result.GitHubIssueUpdate = &value
	}
	if result.GitHubPRGovernance != nil {
		value := *result.GitHubPRGovernance
		result.GitHubPRGovernance = &value
	}
	if result.Reviewer != nil {
		reviewer := *result.Reviewer
		reviewer.Findings = slices.Clone(reviewer.Findings)
		result.Reviewer = &reviewer
	}
	if result.Handoff != nil {
		value := *result.Handoff
		result.Handoff = &value
	}
	if result.Retire != nil {
		value := *result.Retire
		result.Retire = &value
	}
	if result.CheckIn != nil {
		value := *result.CheckIn
		result.CheckIn = &value
	}
	return result
}
