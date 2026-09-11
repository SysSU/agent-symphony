package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
	AttributionAttempt      int
	Dependency, PullRequest int
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
}

type handoffEffectRequest struct {
	Kind                        string
	HeadSHA, Key                string
	Findings, HumanInstructions []string
	Recovery                    *internalgithub.RecoveryHandoff
	Outcome                     *internalgithub.HandoffOutcome
	OutcomePath, OutcomeToken   string
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
}
type handoffEffectResult struct {
	Kind, Key, OutcomePath, OutcomeToken string
	Observed                             bool
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
	if !reconciliationObservationCurrent(*state, request) || state.Observations[issueKey].LastCycleID != request.ObservationCycleID || !validReconciliationEffectStateBindings(stateRoot, *state, request) {
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
		if !exists || record.Generation != identity.AttemptGeneration || !reflect.DeepEqual(record.Manifest, manifest) {
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
	return &runtimeEffectIntent{Action: string(request.Action), Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, IssueGeneration: identity.IssueGeneration, AttemptGeneration: attemptGeneration, IntentEpoch: state.Epoch, State: "pending", RequestDigest: digest, Reconciliation: &request}, nil
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

func applyAuthorizeReconciliationEffect(stateRoot string, state runtimeOwnerState, command authorizeReconciliationEffectCommand) error {
	effect, ok := state.Effects[command.Identity.EffectID]
	if !ok || effect.Reconciliation == nil || effect.Action != string(command.Action) || !reconciliationEffectIdentityMatches(effect, command.Identity) || effect.State != "pending" {
		return errStaleStateResult
	}
	return reconciliationEffectCurrent(stateRoot, state, effect)
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
	if effect.State == "completed" {
		if reflect.DeepEqual(effect.ReconciliationResult, &result) {
			return nil
		}
		return errStateConflict
	}
	if err := reconciliationEffectFinishCurrent(stateRoot, *state, effect); err != nil {
		return err
	}
	if err := applyReconciliationEffectOutcome(state, *effect.Reconciliation, result); err != nil {
		return err
	}
	effect.State, effect.ReconciliationResult, effect.Diagnostic = "completed", &result, ""
	state.Effects[effect.ID] = effect
	if effect.Reconciliation.Action == reconciliationGitHubIssueUpdate && effect.Reconciliation.GitHubIssueUpdate != nil &&
		effect.Reconciliation.GitHubIssueUpdate.Kind == githubIssueTerminalFailure &&
		advanceRecoverReceipts(state, effect.ID, operatorPhaseTerminal, operatorPhaseRetryAwait) {
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
	if request == nil || state.IssueGenerations[ownerIssueKey(effect.Repository, effect.Issue)] != effect.IssueGeneration || !reconciliationObservationCurrent(state, *request) || !validReconciliationEffectStateBindings(stateRoot, state, *request) {
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

// reconciliationEffectFinishCurrent intentionally does not require a current
// observation epoch. An immutable completion marker proves that the external
// effect finished before restart; generations and state bindings still prevent
// a marker from completing invalidated work.
func reconciliationEffectFinishCurrent(stateRoot string, state runtimeOwnerState, effect runtimeEffectIntent) error {
	request := effect.Reconciliation
	if request == nil || state.IssueGenerations[ownerIssueKey(effect.Repository, effect.Issue)] != effect.IssueGeneration || !reconciliationObservationMatches(state, *request) || !validReconciliationEffectStateBindings(stateRoot, state, *request) {
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
	if request == nil || effect.Review != nil || effect.Reason != "" || !boundedText(effect.Diagnostic, maxReconciliationStringBytes, false) || effect.Action != string(request.Action) || effect.Repository != request.Repository || effect.Issue != request.Issue || effect.Attempt != request.Attempt || effect.RequestDigest != reconciliationEffectDigest(*request) || !validReconciliationEffectRequest(state.Repository, *request) || effect.IntentEpoch == 0 || effect.IntentEpoch > state.Epoch {
		return false
	}
	if effect.State == "pending" {
		return effect.ReconciliationResult == nil
	}
	return effect.Diagnostic == "" && effect.ReconciliationResult != nil && validReconciliationEffectResult(*request, *effect.ReconciliationResult)
}

func validReconciliationEffectRequest(repository string, request reconciliationEffectRequest) bool {
	if request.Repository != repository || request.Issue < 1 || request.ObservationGeneration == 0 || request.ObservationCycleID == 0 || !validDigest(request.BodyDigest) || !validDigest(request.ExecutionDigest) {
		return false
	}
	issueScoped := reconciliationEffectIssueScoped(request)
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
	return request.Action == reconciliationGitHubIssueUpdate && request.GitHubIssueUpdate != nil && slices.Contains([]githubIssueUpdateKind{githubIssueControlSnapshot, githubIssueDependencyClear}, request.GitHubIssueUpdate.Kind)
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

func validReconciliationEffectStateBindings(stateRoot string, state runtimeOwnerState, request reconciliationEffectRequest) bool {
	if !validReconciliationEffectBindings(request) {
		return false
	}
	observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
	if request.Attempt == 0 {
		if request.Action != reconciliationGitHubIssueUpdate || request.GitHubIssueUpdate == nil {
			return false
		}
		proposal := reconciliationIssueUpdateProposal{Repository: request.Repository, Issue: request.Issue, Kind: request.GitHubIssueUpdate.Kind, ControlSnapshotDigest: request.GitHubIssueUpdate.ControlSnapshotDigest, AttributionAttempt: request.GitHubIssueUpdate.AttributionAttempt, Dependency: request.GitHubIssueUpdate.Dependency, PullRequest: request.GitHubIssueUpdate.PullRequest}
		return slices.Contains(observation.IssueUpdates, proposal)
	}
	attemptKey := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	record, ok := state.Attempts[attemptKey]
	if !ok || request.Manifest == nil || !reflect.DeepEqual(record.Manifest, *request.Manifest) {
		return false
	}
	manifest := record.Manifest
	fact, remotelyObserved := observedReconciliationAttempt(observation, request.Attempt)
	switch request.Action {
	case reconciliationGitHubBind:
		return manifest.State == "preparing" && observation.Fact.DispatchAuthorized && observation.Fact.Attempt == request.Attempt && observation.Fact.BaseSHA == manifest.BaseSHA
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
		if reviewer.Mode == agentruntime.ReviewModePlan {
			if manifest.State != "running" || !observation.Fact.DispatchAuthorized || reviewer.Target != fmt.Sprintf("%s#%d plan sha256:%s", request.Repository, request.Issue, request.BodyDigest) || reviewer.BaseSHA != manifest.BaseSHA || reviewer.HeadSHA != manifest.BaseSHA || !remotelyObserved || fact.BaseSHA != manifest.BaseSHA || fact.State != "active" && fact.State != "review-ready" {
				return false
			}
		} else if manifest.State != "completed" || reviewer.Target != reviewer.BaseSHA+".."+reviewer.HeadSHA || reviewer.BaseSHA != observation.Fact.BaseSHA {
			return false
		}
		if reviewer.Phase == "cleanup" {
			return (manifest.ReviewState == "clean" || manifest.ReviewState == "findings-queued") && reviewManifestMatches(manifest, reviewer)
		}
		return manifest.ReviewState == "" || (manifest.ReviewState == "preparing" || manifest.ReviewState == "running") && reviewManifestMatches(manifest, reviewer) || manifest.ReviewState == "findings-queued" && manifest.ReviewHandoffAck && manifest.ReviewSnapshot == "" && manifest.ReviewSession == "" && manifest.ReviewHead != reviewer.HeadSHA
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
			record.Manifest.ReviewBase, record.Manifest.ReviewHead = reviewer.BaseSHA, reviewer.HeadSHA
			record.Manifest.ReviewSnapshot, record.Manifest.ReviewSession = reviewer.Snapshot, reviewer.Session
			record.Manifest.ReviewFindings = slices.Clone(outcome.Findings)
			record.Manifest.ReviewHandoffQueued, record.Manifest.ReviewHandoffAck = false, false
			if outcome.Status == "findings" {
				record.Manifest.ReviewState = "findings-queued"
			}
		}
		state.Attempts[key] = record
	case reconciliationHandoffDeliver:
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
		return issueScoped && validDigest(request.ControlSnapshotDigest) && request.HeadSHA == "" && request.Diagnostic == "" && request.Findings == nil && request.FailedAtUnixNano == 0 && request.AttributionAttempt == 0 && request.Dependency == 0 && request.PullRequest == 0
	case githubIssueDependencyClear:
		return issueScoped && request.AttributionAttempt > 0 && request.Dependency > 0 && request.PullRequest >= 0 && request.HeadSHA == "" && request.Diagnostic == "" && request.Findings == nil && request.FailedAtUnixNano == 0 && request.ControlSnapshotDigest == ""
	default:
		return false
	}
}

func validGitHubGovernance(request githubGovernanceEffectRequest) bool {
	return request.PR > 0 && validOptionalObjectID(request.HeadSHA) && request.HeadSHA != "" && request.Policy.Repository != "" && request.Policy.ActorID > 0
}

func validReviewerRequest(request reviewerEffectRequest) bool {
	if request.Phase != "run-observe" && request.Phase != "cleanup" || !slices.Contains([]string{agentruntime.ReviewModePlan, agentruntime.ReviewModeImplementation}, request.Mode) || !boundedText(request.Target, 4096, true) || !validOptionalObjectID(request.BaseSHA) || !validOptionalObjectID(request.HeadSHA) || !boundedText(request.Snapshot, 4096, true) || !boundedText(request.Session, 4096, true) {
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
		return result.Status == "cleaned" && len(result.Findings) == 0
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
