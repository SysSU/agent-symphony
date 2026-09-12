package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// operatorMutationService commits owner state before launching external work.
// It is the production path for dashboard, control, and orchestrator mutations.
type operatorMutationService struct {
	lifecycle         context.Context
	owner             *stateOwner
	effects           *runtimeEffectCoordinator
	cleanup           operatorCleanupExecutor
	collector         reconciliationV2Collector
	collect           func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error)
	reviewer          boundaryCaller
	reviewSource      string
	reviewEnvironment []string
	reviewCommand     []string
	issueClosed       func(context.Context, string, int) (bool, error)
	mu                sync.Mutex
	wg                sync.WaitGroup
	active            map[string]bool
	stopped           bool
}

type operatorWork struct {
	requestID string
	runtime   *agentruntime.EffectRequest
	plan      *reconciliationPlannedEffect
	reviewer  *reviewerExecutionMaterial
}

type operatorReceiptStatus struct {
	Phase      string `json:"phase"`
	EffectID   string `json:"effect_id,omitempty"`
	Diagnostic string `json:"diagnostic,omitempty"`
}

func newOperatorMutationService(lifecycle context.Context, owner *stateOwner, effects *runtimeEffectCoordinator, cleanup operatorCleanupExecutor, collector reconciliationV2Collector, reviewer boundaryCaller, reviewSource string, reviewEnvironment, reviewCommand []string) (*operatorMutationService, error) {
	if lifecycle == nil || owner == nil || effects == nil || effects.owner != owner || effects.executor.Runtime == nil || cleanup.runtime == nil || cleanup.stateRoot != owner.stateRoot || collector.API.HTTP == nil || collector.Config.Repository == "" || collector.Config.ActorID < 1 || reviewer == nil {
		return nil, errors.New("operator mutation service is incomplete")
	}
	snapshot, err := owner.snapshot(lifecycle)
	if err != nil || snapshot.State.Repository != collector.Config.Repository {
		return nil, errors.New("operator mutation service repository does not match owner")
	}
	cleanup.runtime = effects.executor.Runtime
	effects.executor.Cleanup = cleanup.execute
	effects.executor.VerifyCleanup = cleanup.verify
	service := &operatorMutationService{
		lifecycle: lifecycle, owner: owner, effects: effects, cleanup: cleanup, collector: collector, reviewer: reviewer,
		reviewSource: reviewSource, reviewEnvironment: slices.Clone(reviewEnvironment), reviewCommand: slices.Clone(reviewCommand), issueClosed: currentGitHubIssueClosed,
		active: map[string]bool{},
	}
	service.collect = func(ctx context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		bound := collector
		bound.Scope = reconciliationScope{Kind: reconciliationIssueScope, Repository: snapshot.State.Repository, Issue: issue}
		return bound.collect(ctx, snapshot)
	}
	return service, nil
}

func (s *operatorMutationService) perform(ctx context.Context, request controlRequest) controlResult {
	return s.performMode(ctx, request, false)
}

// performSynchronously uses the same durable operator command path without
// launching duplicate background work. It returns only after the receipt is
// terminal or execution reports that the durable intent remains pending.
func (s *operatorMutationService) performSynchronously(ctx context.Context, request controlRequest) controlResult {
	return s.performMode(ctx, request, true)
}

func (s *operatorMutationService) performMode(ctx context.Context, request controlRequest, synchronous bool) controlResult {
	if s == nil || s.owner == nil || s.effects == nil || ctx == nil || !validOperatorRequest(request, s.collector.Config.Repository) {
		return operatorErrorResult(request, http.StatusBadRequest, "invalid operator request")
	}
	if err := ctx.Err(); err != nil {
		return operatorErrorResult(request, http.StatusRequestTimeout, "operator request was cancelled before admission")
	}
	snapshot, err := s.owner.snapshot(ctx)
	if err != nil {
		return operatorResultForError(request, err)
	}
	if receipt, ok := operatorReceiptByID(snapshot.State, request.RequestID); ok {
		if receipt.Request != request {
			return operatorErrorResult(request, http.StatusConflict, "operator request identity was already used for different input")
		}
		if receipt.State == "pending" {
			if synchronous {
				if err := s.resumeReceipt(ctx, request.RequestID); err != nil {
					return operatorResultForError(request, err)
				}
				return s.currentReceiptResult(ctx, request)
			}
			s.dispatchResume(request.RequestID)
		}
		return operatorResultForReceipt(snapshot, receipt)
	}
	if replay, ok := s.tombstoneReplayCommand(snapshot, request); ok {
		committed, effect, err := s.owner.beginOperatorMutation(ctx, replay)
		if err != nil {
			return operatorResultForError(request, err)
		}
		if effect != nil && effect.State == "pending" {
			if synchronous {
				if err := s.resumeReceipt(ctx, request.RequestID); err != nil {
					return operatorResultForError(request, err)
				}
				return s.currentReceiptResult(ctx, request)
			}
			s.dispatchResume(request.RequestID)
		}
		receipt, found := operatorReceiptByID(committed.State, request.RequestID)
		if !found {
			return operatorErrorResult(request, http.StatusInternalServerError, "operator receipt was not committed")
		}
		return operatorResultForReceipt(committed, receipt)
	}
	if attach, ok := s.recoveryAttachCommand(snapshot, request); ok {
		committed, _, err := s.owner.beginOperatorMutation(ctx, attach)
		if err != nil {
			return operatorResultForError(request, err)
		}
		if synchronous {
			if err := s.resumeReceipt(ctx, request.RequestID); err != nil {
				return operatorResultForError(request, err)
			}
			return s.currentReceiptResult(ctx, request)
		}
		s.dispatchResume(request.RequestID)
		receipt, found := operatorReceiptByID(committed.State, request.RequestID)
		if !found {
			return operatorErrorResult(request, http.StatusInternalServerError, "operator receipt was not committed")
		}
		return operatorResultForReceipt(committed, receipt)
	}

	if (request.Action == "archive" || request.Action == "dismiss") && !ownerHasAttempt(snapshot.State, request) {
		fresh, batch, err := s.collectIssue(ctx, request.Issue)
		if err != nil {
			return operatorResultForError(request, err)
		}
		command, err := remoteOnlyOperatorCommand(fresh, batch, request)
		if err != nil {
			return operatorResultForError(request, err)
		}
		committed, _, err := s.owner.beginOperatorMutation(ctx, command)
		if err != nil {
			return operatorResultForError(request, err)
		}
		s.effects.cancelInvalidated(committed)
		receipt, ok := operatorReceiptByID(committed.State, request.RequestID)
		if !ok {
			return operatorErrorResult(request, http.StatusInternalServerError, "operator receipt was not committed")
		}
		return operatorResultForReceipt(committed, receipt)
	}
	command, work, err := s.prepareAdmission(ctx, snapshot, request)
	if err != nil {
		return operatorResultForError(request, err)
	}
	committed, effect, err := s.owner.beginOperatorMutation(ctx, command)
	if err != nil {
		return operatorResultForError(request, err)
	}
	s.effects.cancelInvalidated(committed)
	if effect != nil {
		work.requestID = request.RequestID
		bindOperatorWorkIdentity(&work, *effect)
		if synchronous {
			if err := s.executeOnce(work, ""); err != nil {
				return operatorResultForError(request, err)
			}
			return s.currentReceiptResult(ctx, request)
		}
		s.dispatch(work)
	}
	receipt, ok := operatorReceiptByID(committed.State, request.RequestID)
	if !ok {
		return operatorErrorResult(request, http.StatusInternalServerError, "operator receipt was not committed")
	}
	return operatorResultForReceipt(committed, receipt)
}

func ownerHasAttempt(state runtimeOwnerState, request controlRequest) bool {
	_, ok := state.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]
	return ok
}

func remoteOnlyOperatorCommand(snapshot stateOwnerSnapshot, batch reconciliationV2Batch, request controlRequest) (beginOperatorMutationCommand, error) {
	issueKey, attemptKey := ownerIssueKey(request.Repository, request.Issue), ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	observation, ok := snapshot.State.Observations[issueKey]
	attempt, accepted := observation.Attempts[attemptKey]
	issueFound := slices.ContainsFunc(batch.Input.Issues, func(fact internalgithub.RecoveryIssueFact) bool {
		return fact.Repository == request.Repository && fact.Issue == request.Issue && (request.Action == "archive" || fact.Closed) && fact.CurrentAttempt == request.Attempt
	})
	attemptFound := slices.ContainsFunc(batch.Input.Attempts, func(fact internalgithub.RecoveryAttemptFact) bool {
		reduced, err := reduceAttemptFact(request.Repository, fact)
		return err == nil && reflect.DeepEqual(reduced, attempt.Fact)
	})
	if !issueFound || !attemptFound || !ok || !observation.Present || observation.ObservationEpoch != snapshot.State.Epoch || request.Action == "dismiss" && !observation.Fact.Closed || observation.Fact.CurrentAttempt != request.Attempt || !accepted || !attempt.Present || attempt.ObservationEpoch != snapshot.State.Epoch || attempt.SourceIssueGeneration != observation.Generation || attempt.OwnerGeneration != snapshot.State.AttemptGenerations[attemptKey] || attempt.Fact.State != "completed" || attempt.Fact.Repository != request.Repository || attempt.Fact.Issue != request.Issue || attempt.Fact.Attempt != request.Attempt || ownerHasAttempt(snapshot.State, request) {
		return beginOperatorMutationCommand{}, errStateConflict
	}
	return beginOperatorMutationCommand{Request: request, RemoteOnly: true, IssueClosed: observation.Fact.Closed,
		ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest,
		Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[issueKey], AttemptGeneration: snapshot.State.AttemptGenerations[attemptKey]},
	}, nil
}

func (s *operatorMutationService) currentReceiptResult(ctx context.Context, request controlRequest) controlResult {
	snapshot, err := s.owner.snapshot(ctx)
	if err != nil {
		return operatorResultForError(request, err)
	}
	receipt, ok := operatorReceiptByID(snapshot.State, request.RequestID)
	if !ok {
		return operatorErrorResult(request, http.StatusInternalServerError, "operator receipt was not committed")
	}
	return operatorResultForReceipt(snapshot, receipt)
}

func (s *operatorMutationService) recoveryAttachCommand(snapshot stateOwnerSnapshot, request controlRequest) (beginOperatorMutationCommand, bool) {
	if request.Action != "recover" {
		return beginOperatorMutationCommand{}, false
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	record, ok := snapshot.State.Attempts[key]
	if !ok || record.Generation != snapshot.State.AttemptGenerations[key] {
		return beginOperatorMutationCommand{}, false
	}
	for _, receipt := range snapshot.State.ControlReceipts {
		if receipt.State != "pending" || receipt.Request.Action != "recover" || receipt.Request.Repository != request.Repository || receipt.Request.Issue != request.Issue || receipt.Request.Attempt != request.Attempt {
			continue
		}
		effect, exists := snapshot.State.Effects[receipt.EffectID]
		if !exists || !operatorReceiptMatchesEffect(receipt, effect) {
			return beginOperatorMutationCommand{Request: request}, true
		}
		observation := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
		return beginOperatorMutationCommand{
			Request: request, Manifest: cloneManifest(record.Manifest),
			ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest,
			Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: snapshot.State.AttemptGenerations[key]},
		}, true
	}
	if effect := currentRetryEffect(snapshot.State, request.Repository, request.Issue, request.Attempt); effect != nil {
		observation := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
		return beginOperatorMutationCommand{
			Request: request, Manifest: cloneManifest(record.Manifest),
			ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest,
			Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: snapshot.State.AttemptGenerations[key]},
		}, true
	}
	return beginOperatorMutationCommand{}, false
}

func (s *operatorMutationService) tombstoneReplayCommand(snapshot stateOwnerSnapshot, request controlRequest) (beginOperatorMutationCommand, bool) {
	tombstone, ok := snapshot.State.Tombstones[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]
	if !ok || tombstone.Manifest == nil {
		return beginOperatorMutationCommand{}, false
	}
	action := map[string]string{"dismissed": "dismiss", "archived": "archive", "abandoned": "abandon", "removed": "remove"}[tombstone.Action]
	if request.Action != action {
		return beginOperatorMutationCommand{Request: request}, true
	}
	observation := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	command := beginOperatorMutationCommand{Request: request, Manifest: cloneManifest(*tombstone.Manifest), PublishedHead: tombstone.PublishedHead, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest, Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: tombstone.Generation}}
	if tombstone.CleanupPolicy != nil {
		command.CleanupPolicy = *cloneCleanupPolicy(tombstone.CleanupPolicy)
	}
	if effect, exists := snapshot.State.Effects[tombstone.EffectID]; exists {
		command.CleanupDigest = effect.RequestDigest
	}
	return command, true
}

func (s *operatorMutationService) prepareAdmission(ctx context.Context, snapshot stateOwnerSnapshot, request controlRequest) (beginOperatorMutationCommand, operatorWork, error) {
	manifest, status, err := operatorAttempt(snapshot, request)
	if err != nil {
		return beginOperatorMutationCommand{}, operatorWork{}, err
	}
	observation := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	command := beginOperatorMutationCommand{
		Request: request, Manifest: manifest,
		ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest,
		Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]},
	}
	var work operatorWork
	if request.Action == "dismiss" && !observation.Present {
		if s.issueClosed == nil {
			return beginOperatorMutationCommand{}, operatorWork{}, errStateConflict
		}
		closed, err := s.issueClosed(ctx, request.Repository, request.Issue)
		if err != nil {
			return beginOperatorMutationCommand{}, operatorWork{}, err
		}
		if !closed {
			return beginOperatorMutationCommand{}, operatorWork{}, fmt.Errorf("GitHub issue is open: %w", errStateConflict)
		}
		command.IssueClosed = true
	}
	switch request.Action {
	case "dismiss":
		if !command.IssueClosed {
			if s.issueClosed == nil {
				return beginOperatorMutationCommand{}, operatorWork{}, errStateConflict
			}
			closed, err := s.issueClosed(ctx, request.Repository, request.Issue)
			if err != nil {
				return beginOperatorMutationCommand{}, operatorWork{}, err
			}
			if !closed {
				return beginOperatorMutationCommand{}, operatorWork{}, fmt.Errorf("GitHub issue is open: %w", errStateConflict)
			}
			command.IssueClosed = true
		}
	case "archive", "abandon", "remove":
		cleanup := agentruntime.EffectRequest{Action: agentruntime.EffectCleanup, Attempt: operatorEffectAttempt(manifest), Manifest: manifest, Cleanup: agentruntime.EffectCleanupPolicy{Action: request.Action}}
		if request.Action == "remove" {
			cleanup.Cleanup.PublishedHead = firstNonempty(status.HeadSHA, manifest.BaseSHA)
			command.PublishedHead = cleanup.Cleanup.PublishedHead
		}
		cleanup, err = s.cleanup.bindPolicy(cleanup)
		if err == nil {
			cleanup, err = s.effects.executor.BindRequest(cleanup)
		}
		if err == nil {
			err = s.effects.executor.ValidateRequest(cleanup)
		}
		if err == nil {
			err = s.cleanup.validate(ctx, cleanup)
		}
		if err != nil {
			return beginOperatorMutationCommand{}, operatorWork{}, err
		}
		digest, err := agentruntime.EffectRequestDigest(cleanup)
		if err != nil {
			return beginOperatorMutationCommand{}, operatorWork{}, err
		}
		command.CleanupValid, command.CleanupDigest, command.CleanupPolicy = true, digest, cleanup.Cleanup
		work.runtime = &cleanup
	case "cancel":
		effect, err := s.prepareStop(manifest, "operator cancelled attempt")
		if err != nil {
			return beginOperatorMutationCommand{}, operatorWork{}, err
		}
		command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: effect.Reason, RequestDigest: effect.Identity.RequestDigest}
		work.runtime = &effect
	case "recover":
		if manifest.State == "running" && (status.State == "active" || status.State == "review-ready" || status.State == "blocked" && status.Retryable) {
			if err := freshRuntime(s.effects.executor.Runtime, s.effects.executor.Runtime.Source).VerifyOwned(ctx, manifest); err == nil {
				return beginOperatorMutationCommand{}, operatorWork{}, errStateConflict
			} else if ctx.Err() != nil {
				return beginOperatorMutationCommand{}, operatorWork{}, ctx.Err()
			}
			reason := "dashboard recovery: " + firstNonempty(status.Diagnostic, "runtime liveness mismatch")
			effect, err := s.prepareStop(manifest, reason)
			if err != nil {
				return beginOperatorMutationCommand{}, operatorWork{}, err
			}
			command.LivenessFailed = true
			command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: effect.Reason, RequestDigest: effect.Identity.RequestDigest}
			work.runtime = &effect
		} else {
			fresh, batch, err := s.collectIssue(ctx, request.Issue)
			if err != nil {
				return beginOperatorMutationCommand{}, operatorWork{}, err
			}
			return s.prepareRecoveryAdmission(fresh, batch, request, githubIssueRetry)
		}
	case "review-plan":
		plan, material, err := s.preparePlanReview(ctx, snapshot, manifest)
		if err != nil {
			return beginOperatorMutationCommand{}, operatorWork{}, err
		}
		command.Reconciliation = &beginReconciliationEffectCommand{Identity: command.Identity, Request: plan.Request}
		work.plan, work.reviewer = &plan, &material
	default:
		return beginOperatorMutationCommand{}, operatorWork{}, errStateConflict
	}
	return command, work, nil
}

func (s *operatorMutationService) prepareStop(manifest agentruntime.Manifest, reason string) (agentruntime.EffectRequest, error) {
	request := agentruntime.EffectRequest{Action: agentruntime.EffectStop, Attempt: operatorEffectAttempt(manifest), Manifest: manifest, Reason: reason}
	bound, err := s.effects.executor.BindRequest(request)
	if err == nil {
		err = s.effects.executor.ValidateRequest(bound)
	}
	if err != nil {
		return agentruntime.EffectRequest{}, err
	}
	digest, err := agentruntime.EffectRequestDigest(bound)
	bound.Identity.RequestDigest = digest
	return bound, err
}

func operatorEffectAttempt(manifest agentruntime.Manifest) agentruntime.Attempt {
	return agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
}

func operatorAttempt(snapshot stateOwnerSnapshot, request controlRequest) (agentruntime.Manifest, orchestrator.RecoveryStatus, error) {
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	record, ok := snapshot.State.Attempts[key]
	observation, observed := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	absentOrphan := !observation.Present && (request.Action == "dismiss" || request.Action == "abandon") && observation.ObservationEpoch == snapshot.State.Epoch
	if !ok || !observed || !observation.Present && !absentOrphan || record.Generation != snapshot.State.AttemptGenerations[key] || observation.OwnerGeneration != snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] {
		return agentruntime.Manifest{}, orchestrator.RecoveryStatus{}, errStaleStateResult
	}
	status, _, err := ownerOperatorStatus(snapshot.State, request.Issue, request.Attempt)
	if absentOrphan && status.State != "orphaned" {
		return agentruntime.Manifest{}, orchestrator.RecoveryStatus{}, errStaleStateResult
	}
	return cloneManifest(record.Manifest), status, err
}

func (s *operatorMutationService) collectIssue(ctx context.Context, issue int) (stateOwnerSnapshot, reconciliationV2Batch, error) {
	snapshot, err := s.owner.reconciliationSnapshot(ctx)
	if err != nil {
		return stateOwnerSnapshot{}, reconciliationV2Batch{}, err
	}
	if s.collect == nil {
		return stateOwnerSnapshot{}, reconciliationV2Batch{}, errors.New("operator collector is unavailable")
	}
	batch, err := s.collect(ctx, snapshot, issue)
	if err != nil {
		return stateOwnerSnapshot{}, reconciliationV2Batch{}, err
	}
	collection, err := collectionFromSnapshot(snapshot, batch.Input)
	if err != nil {
		return stateOwnerSnapshot{}, reconciliationV2Batch{}, err
	}
	committed, err := s.owner.applyReconciliation(ctx, collection)
	if err != nil {
		return stateOwnerSnapshot{}, reconciliationV2Batch{}, err
	}
	s.effects.cancelInvalidated(committed)
	return committed, batch, nil
}

func (s *operatorMutationService) prepareRecoveryAdmission(snapshot stateOwnerSnapshot, batch reconciliationV2Batch, request controlRequest, kind githubIssueUpdateKind) (beginOperatorMutationCommand, operatorWork, error) {
	manifest, _, err := operatorAttempt(snapshot, request)
	if err != nil {
		return beginOperatorMutationCommand{}, operatorWork{}, err
	}
	plans, err := planReconciliationAttemptIssueUpdates(snapshot, batch, s.collector.Config)
	if err != nil {
		return beginOperatorMutationCommand{}, operatorWork{}, err
	}
	matches := slices.DeleteFunc(plans, func(plan reconciliationPlannedEffect) bool {
		return plan.Request.Repository != request.Repository || plan.Request.Issue != request.Issue || plan.Request.Attempt != request.Attempt || plan.Request.GitHubIssueUpdate == nil || plan.Request.GitHubIssueUpdate.Kind != kind
	})
	if len(matches) != 1 {
		return beginOperatorMutationCommand{}, operatorWork{}, errStateConflict
	}
	plan := matches[0]
	observation := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	command := beginOperatorMutationCommand{Request: request, Manifest: manifest, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, ObservationBodyDigest: observation.Fact.BodyDigest, Identity: plan.Identity, Reconciliation: &beginReconciliationEffectCommand{Identity: plan.Identity, Request: plan.Request}}
	return command, operatorWork{plan: &plan}, nil
}

func (s *operatorMutationService) preparePlanReview(ctx context.Context, snapshot stateOwnerSnapshot, manifest agentruntime.Manifest) (reconciliationPlannedEffect, reviewerExecutionMaterial, error) {
	if err := freshRuntime(s.effects.executor.Runtime, s.effects.executor.Runtime.Source).VerifyOwned(ctx, manifest); err != nil {
		return reconciliationPlannedEffect{}, reviewerExecutionMaterial{}, err
	}
	observation := snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
	issue := expandIssueFact(observation.Fact)
	issue.Attempt, issue.BaseSHA = manifest.Attempt, manifest.BaseSHA
	target := manifest.Repository + "#" + strconv.Itoa(manifest.Issue) + " plan sha256:" + observation.Fact.BodyDigest
	snapshotPath, session := reviewIdentity(agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt}, productionSnapshotRoot(s.owner.stateRoot))
	request := reconciliationEffectRequest{Action: reconciliationReviewer, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Manifest: ptrManifest(manifest), ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, Reviewer: &reviewerEffectRequest{Phase: "run-observe", Mode: agentruntime.ReviewModePlan, Target: target, BaseSHA: manifest.BaseSHA, HeadSHA: manifest.BaseSHA, Snapshot: snapshotPath, Session: session}}
	material := reviewerExecutionMaterial{Issue: issue, Source: s.reviewSource, HeadSHA: manifest.BaseSHA, Env: slices.Clone(s.reviewEnvironment), Command: slices.Clone(s.reviewCommand)}
	request.ExecutionDigest = reviewerExecutionDigest(request, material)
	if !validReconciliationEffectRequest(snapshot.State.Repository, request) || !validReconciliationEffectStateBindings(s.owner.stateRoot, snapshot.State, request) {
		return reconciliationPlannedEffect{}, reviewerExecutionMaterial{}, errStateConflict
	}
	return reconciliationPlannedEffect{Identity: ownerReconciliationBeginIdentity(snapshot, request), Request: request}, material, nil
}

func bindOperatorWorkIdentity(work *operatorWork, effect runtimeEffectIntent) {
	if work.runtime != nil {
		work.runtime.Identity = effectRequestIdentity(effect)
	}
	if work.plan != nil {
		work.plan.Identity = ownerReconciliationEffectIdentity(effect)
		work.plan.Request = cloneReconciliationRequest(*effect.Reconciliation)
	}
}

func currentRetryEffect(state runtimeOwnerState, repository string, issue, attempt int) *runtimeEffectIntent {
	issueGeneration := state.IssueGenerations[ownerIssueKey(repository, issue)]
	attemptGeneration := state.AttemptGenerations[ownerAttemptKey(repository, issue, attempt)]
	var found *runtimeEffectIntent
	for _, effect := range state.Effects {
		if effect.Repository != repository || effect.Issue != issue || effect.Attempt != attempt || effect.Reconciliation == nil ||
			effect.Reconciliation.Action != reconciliationGitHubIssueUpdate || effect.Reconciliation.GitHubIssueUpdate == nil ||
			effect.Reconciliation.GitHubIssueUpdate.Kind != githubIssueRetry || effect.IssueGeneration != issueGeneration ||
			effect.AttemptGeneration != attemptGeneration || effect.State != "pending" && effect.State != "completed" {
			continue
		}
		if found != nil {
			return nil
		}
		found = cloneEffect(&effect)
	}
	return found
}

func (s *operatorMutationService) dispatch(work operatorWork) {
	key := operatorWorkEffectID(work)
	s.start(key, func() {
		if err := s.execute(work); err != nil {
			s.classifyWorkerFailure(work.requestID, err)
		}
	})
}

func operatorWorkEffectID(work operatorWork) string {
	if work.runtime != nil {
		return work.runtime.Identity.EffectID
	}
	if work.plan != nil {
		return work.plan.Identity.EffectID
	}
	return ""
}

func (s *operatorMutationService) executeOnce(work operatorWork, reserved string) error {
	key := operatorWorkEffectID(work)
	if key == "" {
		return errStateConflict
	}
	if key == reserved {
		return s.execute(work)
	}
	reservedSuccessor := reserved != ""
	if reservedSuccessor && !s.reserveSuccessor(reserved, key) || !reservedSuccessor && !s.reserve(key) {
		return nil
	}
	defer s.release(key)
	return s.execute(work)
}

// reserveSuccessor lets work admitted before shutdown finish its already-
// durable multi-stage transition without opening admission for unrelated work.
func (s *operatorMutationService) reserveSuccessor(current, next string) bool {
	if current == "" || next == "" || current == next {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.active[current] || s.active[next] {
		return false
	}
	s.active[next] = true
	return true
}

func (s *operatorMutationService) execute(work operatorWork) error {
	return s.executeReserved(work, operatorWorkEffectID(work))
}

func (s *operatorMutationService) executeReserved(work operatorWork, reserved string) error {
	if work.runtime != nil {
		result, err := s.effects.executeOperator(*work.runtime)
		if result.Disposition == agentruntime.EffectResultReady && work.requestID != "" {
			err = errors.Join(err, s.resumeReceiptReserved(s.lifecycle, work.requestID, reserved))
		}
		return err
	}
	if work.plan == nil {
		return nil
	}
	if work.reviewer != nil {
		result, pending, err := s.effects.executeOperatorReviewer(s.reviewer, *work.plan, *work.reviewer)
		if !pending && result.Action != "" && work.requestID != "" {
			err = errors.Join(err, s.resumeReceiptReserved(s.lifecycle, work.requestID, reserved))
		}
		return err
	}
	result, err := s.effects.executeOperatorIssueUpdate(s.collector.API, *work.plan)
	if result.Action != "" && work.requestID != "" {
		err = errors.Join(err, s.resumeReceiptReserved(s.lifecycle, work.requestID, reserved))
	}
	return err
}

func (s *operatorMutationService) dispatchResume(requestID string) {
	key, reserved := "request:"+requestID, ""
	if snapshot, err := s.owner.snapshot(s.lifecycle); err == nil {
		if receipt, ok := operatorReceiptByID(snapshot.State, requestID); ok && receipt.EffectID != "" {
			key, reserved = receipt.EffectID, receipt.EffectID
		}
	}
	s.start(key, func() {
		if err := s.resumeReceiptReserved(s.lifecycle, requestID, reserved); err != nil {
			s.classifyWorkerFailure(requestID, err)
		}
	})
}

// classifyWorkerFailure makes one marker/postcondition check, then leaves an
// exact durable diagnostic on work that is still pending. It never retries the
// external mutation.
func (s *operatorMutationService) classifyWorkerFailure(requestID string, workErr error) {
	if requestID == "" || workErr == nil || s.lifecycle.Err() != nil {
		return
	}
	snapshot, err := s.owner.snapshot(s.lifecycle)
	if err != nil {
		return
	}
	receipt, ok := operatorReceiptByID(snapshot.State, requestID)
	if !ok || receipt.State != "pending" {
		return
	}
	effect, ok := snapshot.State.Effects[receipt.EffectID]
	if !ok || effect.State != "pending" {
		return
	}
	diagnostic := "operator worker stopped with pending durable intent: " + internalgithub.Redact(workErr.Error())
	if effect.Reconciliation != nil {
		result, verifyErr := s.effects.verifyPendingOperatorReconciliation(s.lifecycle, effect)
		if result != nil || verifyErr == nil {
			if result != nil {
				return
			}
		} else {
			diagnostic += "; marker verification failed: " + internalgithub.Redact(verifyErr.Error())
		}
		_, _ = s.owner.diagnoseReconciliationEffect(s.lifecycle, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: effect.Reconciliation.Action, Diagnostic: diagnostic})
		return
	}
	request, reconstructErr := s.reconstructRuntimeRequest(snapshot, effect)
	if reconstructErr == nil {
		verification, verifyErr := s.effects.verifyPendingOperator(s.lifecycle, snapshot, effect, request)
		if verifyErr == nil && verification.Disposition == agentruntime.EffectVerified {
			return
		}
		if verifyErr != nil {
			diagnostic += "; marker verification failed: " + internalgithub.Redact(verifyErr.Error())
		}
	} else {
		diagnostic += "; reconstruction failed: " + internalgithub.Redact(reconstructErr.Error())
	}
	_, _ = s.owner.diagnoseRuntimeEffect(s.lifecycle, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(effect)), Action: agentruntime.EffectAction(effect.Action), Diagnostic: diagnostic})
}

func (s *operatorMutationService) start(key string, work func()) bool {
	if key == "" {
		return false
	}
	s.mu.Lock()
	if s.stopped || s.active[key] {
		s.mu.Unlock()
		return false
	}
	if s.active == nil {
		s.active = map[string]bool{}
	}
	s.active[key] = true
	s.wg.Add(1)
	s.mu.Unlock()
	go func() {
		defer func() {
			s.release(key)
			s.wg.Done()
		}()
		work()
	}()
	return true
}

func (s *operatorMutationService) reserve(key string) bool {
	if key == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped || s.active[key] {
		return false
	}
	if s.active == nil {
		s.active = map[string]bool{}
	}
	s.active[key] = true
	return true
}

func (s *operatorMutationService) release(key string) {
	s.mu.Lock()
	delete(s.active, key)
	s.mu.Unlock()
}

func (s *operatorMutationService) shutdown(ctx context.Context) error {
	if s == nil {
		return nil
	}
	s.mu.Lock()
	s.stopped = true
	s.mu.Unlock()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func operatorResultForReceipt(snapshot stateOwnerSnapshot, receipt controlReceipt) controlResult {
	if receipt.State == "completed" {
		return *receipt.Result
	}
	body, _ := json.Marshal(operatorReceiptStatus{Phase: receipt.Phase, EffectID: receipt.EffectID, Diagnostic: receipt.Diagnostic})
	return controlResult{Version: controlVersion, RequestID: receipt.Request.RequestID, Action: receipt.Request.Action, OK: true, Status: http.StatusAccepted, OwnerRevision: snapshot.State.Revision, Data: body}
}

func operatorResultForError(request controlRequest, err error) controlResult {
	status := http.StatusInternalServerError
	message := "operator mutation failed"
	if errors.Is(err, errStateConflict) {
		status, message = http.StatusConflict, internalgithub.Redact(err.Error())
	} else if errors.Is(err, errStaleStateResult) {
		status, message = http.StatusConflict, "attempt or issue changed; refresh and retry"
	} else if errors.Is(err, errAttemptTombstoned) {
		status, message = http.StatusConflict, "attempt was already invalidated"
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status, message = http.StatusRequestTimeout, "operator request was cancelled before admission"
	}
	return operatorErrorResult(request, status, message)
}

func operatorErrorResult(request controlRequest, status int, message string) controlResult {
	return controlResult{Version: controlVersion, RequestID: request.RequestID, Action: request.Action, Status: status, Error: message}
}

func (s *operatorMutationService) resumeReceipt(ctx context.Context, requestID string) error {
	return s.resumeReceiptReserved(ctx, requestID, "")
}

func (s *operatorMutationService) resumeReceiptReserved(ctx context.Context, requestID, reserved string) error {
	if ctx == nil || s == nil {
		return errStateConflict
	}
	snapshot, err := s.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	receipt, ok := operatorReceiptByID(snapshot.State, requestID)
	if !ok || receipt.State == "completed" {
		return nil
	}
	if receipt.Phase == operatorPhaseTerminalAwait || receipt.Phase == operatorPhaseRetryAwait {
		kind := githubIssueTerminalFailure
		if receipt.Phase == operatorPhaseRetryAwait {
			kind = githubIssueRetry
		}
		fresh, batch, err := s.collectIssue(ctx, receipt.Request.Issue)
		if err != nil {
			return errors.Join(err, s.recordAwaitingDiagnostic(receipt, err))
		}
		command, work, err := s.prepareRecoveryAdmission(fresh, batch, receipt.Request, kind)
		if err != nil {
			return errors.Join(err, s.recordAwaitingDiagnostic(receipt, err))
		}
		committed, effect, err := s.owner.advanceOperatorRecovery(ctx, advanceOperatorRecoveryCommand{RequestID: requestID, Identity: command.Identity, Reconciliation: *command.Reconciliation})
		if err != nil {
			return err
		}
		s.effects.cancelInvalidated(committed)
		work.requestID = requestID
		bindOperatorWorkIdentity(&work, *effect)
		return s.executeOnce(work, reserved)
	}
	effect, ok := snapshot.State.Effects[receipt.EffectID]
	if !ok || effect.State != "pending" {
		return errStateConflict
	}
	if effect.Reconciliation != nil {
		verified, err := s.effects.verifyPendingOperatorReconciliation(ctx, effect)
		if err != nil {
			return err
		}
		if verified != nil {
			return s.resumeReceiptReserved(ctx, requestID, reserved)
		}
		resumeErr := s.resumeUnmarkedReconciliation(ctx, snapshot, receipt, effect, reserved)
		if resumeErr != nil {
			_, diagnosticErr := s.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: effect.Reconciliation.Action, Diagnostic: "pending operator effect reconstruction failed: " + internalgithub.Redact(resumeErr.Error())})
			return errors.Join(resumeErr, diagnosticErr)
		}
		return nil
	}
	request, err := s.reconstructRuntimeRequest(snapshot, effect)
	if err != nil {
		return err
	}
	verification, err := s.effects.verifyPendingOperator(ctx, snapshot, effect, request)
	if err != nil {
		return err
	}
	if verification.Disposition == agentruntime.EffectVerified {
		return s.resumeReceiptReserved(ctx, requestID, reserved)
	}
	if verification.Disposition == agentruntime.EffectPending {
		return nil
	}
	bound, err := s.effects.executor.BindRequest(request)
	if err != nil {
		return err
	}
	bound.Identity = effectRequestIdentity(effect)
	digest, err := agentruntime.EffectRequestDigest(bound)
	if err != nil || digest != effect.RequestDigest {
		return errStateConflict
	}
	return s.executeOnce(operatorWork{requestID: requestID, runtime: &bound}, reserved)
}

func (s *operatorMutationService) recordAwaitingDiagnostic(receipt controlReceipt, cause error) error {
	diagnostic := strings.ToValidUTF8("recovery admission remains pending: "+internalgithub.Redact(cause.Error()), "?")
	if len(diagnostic) > maxReconciliationStringBytes {
		diagnostic = diagnostic[:maxReconciliationStringBytes]
	}
	_, err := s.owner.recordOperatorDiagnostic(s.lifecycle, recordOperatorDiagnosticCommand{RequestID: receipt.Request.RequestID, Phase: receipt.Phase, EffectID: receipt.EffectID, Diagnostic: diagnostic})
	return err
}

func (s *operatorMutationService) reconstructRuntimeRequest(snapshot stateOwnerSnapshot, effect runtimeEffectIntent) (agentruntime.EffectRequest, error) {
	key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if tombstone, ok := snapshot.State.Tombstones[key]; ok && tombstone.EffectID == effect.ID && tombstone.Manifest != nil && tombstone.CleanupPolicy != nil {
		manifest := cloneManifest(*tombstone.Manifest)
		return agentruntime.EffectRequest{Identity: effectRequestIdentity(effect), Action: agentruntime.EffectCleanup, Attempt: operatorEffectAttempt(manifest), Manifest: manifest, Cleanup: *cloneCleanupPolicy(tombstone.CleanupPolicy)}, nil
	}
	record, ok := snapshot.State.Attempts[key]
	if !ok {
		return agentruntime.EffectRequest{}, errStaleStateResult
	}
	manifest := cloneManifest(record.Manifest)
	return agentruntime.EffectRequest{Identity: effectRequestIdentity(effect), Action: agentruntime.EffectAction(effect.Action), Attempt: operatorEffectAttempt(manifest), Manifest: manifest, Reason: effect.Reason}, nil
}

func (s *operatorMutationService) resumeUnmarkedReconciliation(ctx context.Context, snapshot stateOwnerSnapshot, receipt controlReceipt, effect runtimeEffectIntent, reserved string) error {
	if effect.Reconciliation == nil {
		return errStateConflict
	}
	if effect.Reconciliation.Action == reconciliationGitHubIssueUpdate {
		fresh, batch, err := s.collectIssue(ctx, receipt.Request.Issue)
		if err != nil {
			return err
		}
		plans, err := planReconciliationAttemptIssueUpdates(fresh, batch, s.collector.Config)
		if err != nil {
			return err
		}
		for _, plan := range plans {
			plan.Request.ObservationCycleID = effect.Reconciliation.ObservationCycleID
			material := plan.Material
			plan.Request.ExecutionDigest = issueUpdateExecutionDigest(plan.Request, material)
			if reflect.DeepEqual(plan.Request, *effect.Reconciliation) {
				plan.Identity = ownerReconciliationEffectIdentity(effect)
				return s.executeOnce(operatorWork{requestID: receipt.Request.RequestID, plan: &plan}, reserved)
			}
		}
		return errStateConflict
	}
	if effect.Reconciliation.Action == reconciliationReviewer {
		record, ok := snapshot.State.Attempts[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)]
		if !ok {
			return errStaleStateResult
		}
		plan, material, err := s.preparePlanReview(ctx, snapshot, record.Manifest)
		if err != nil || !reflect.DeepEqual(plan.Request, *effect.Reconciliation) {
			if err != nil {
				return err
			}
			plan.Request.ObservationCycleID = effect.Reconciliation.ObservationCycleID
			plan.Request.ExecutionDigest = reviewerExecutionDigest(plan.Request, material)
			if !reflect.DeepEqual(plan.Request, *effect.Reconciliation) {
				return errStateConflict
			}
		}
		plan.Identity = ownerReconciliationEffectIdentity(effect)
		return s.executeOnce(operatorWork{requestID: receipt.Request.RequestID, plan: &plan, reviewer: &material}, reserved)
	}
	return errStateConflict
}

func (s *operatorMutationService) resumePending(ctx context.Context) error {
	snapshot, err := s.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(snapshot.State.ControlReceipts))
	for _, receipt := range snapshot.State.ControlReceipts {
		if receipt.State == "pending" {
			ids = append(ids, receipt.Request.RequestID)
		}
	}
	slices.Sort(ids)
	for _, requestID := range ids {
		snapshot, err = s.owner.snapshot(ctx)
		if err != nil {
			return err
		}
		receipt, ok := operatorReceiptByID(snapshot.State, requestID)
		if !ok || receipt.State != "pending" {
			continue
		}
		if receipt.Phase == operatorPhaseTerminalAwait || receipt.Phase == operatorPhaseRetryAwait {
			if receipt.Diagnostic != "" {
				continue
			}
			s.dispatchResume(requestID)
			continue
		}
		effect, ok := snapshot.State.Effects[receipt.EffectID]
		if !ok || effect.State != "pending" {
			return errStateConflict
		}
		if effect.Reconciliation != nil {
			result, verifyErr := s.effects.verifyPendingOperatorReconciliation(ctx, effect)
			if verifyErr != nil {
				return verifyErr
			}
			if result != nil {
				s.dispatchResume(requestID)
				continue
			}
			s.dispatchResume(requestID)
			continue
		}
		marker := filepath.Join(s.owner.stateRoot, "runtime-effects", effect.ID+".done")
		if _, statErr := os.Lstat(marker); errors.Is(statErr, os.ErrNotExist) {
			s.dispatchResume(requestID)
			continue
		} else if statErr != nil {
			return statErr
		}
		request, reconstructErr := s.reconstructRuntimeRequest(snapshot, effect)
		if reconstructErr != nil {
			return reconstructErr
		}
		verification, verifyErr := s.effects.verifyPendingOperator(ctx, snapshot, effect, request)
		if verifyErr != nil {
			return verifyErr
		}
		if verification.Disposition == agentruntime.EffectVerified {
			s.dispatchResume(requestID)
		}
	}
	return nil
}

func newProjectDashboardServerV2(ctx context.Context, stateRoot, repository string, peerProjects []string, tmux string, service *operatorMutationService, capacity int, allowNet bool, password string) (*dashboardServer, error) {
	if service == nil || service.owner == nil || service.owner.stateRoot != stateRoot || service.collector.Config.Repository != repository || capacity < 1 {
		return nil, errors.New("v2 dashboard owner binding is invalid")
	}
	server := newProjectDashboardServer(ctx, stateRoot, repository, peerProjects, tmux, nil, nil, allowNet, password)
	server.operator = service
	server.capacity = capacity
	return server, nil
}

func (s *dashboardServer) serveOperatorAction(w http.ResponseWriter, r *http.Request, action string) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", "POST")
		http.Error(w, "action requires POST", http.StatusMethodNotAllowed)
		return
	}
	if !sameDashboardOrigin(r) {
		http.Error(w, "action requires the dashboard origin", http.StatusForbidden)
		return
	}
	query := r.URL.Query()
	issue, issueErr := strconv.Atoi(query.Get("issue"))
	attempt, attemptErr := strconv.Atoi(query.Get("attempt"))
	if issueErr != nil || attemptErr != nil || issue < 1 || attempt < 1 || !s.validProjectQuery(query, "issue", "attempt") || !slices.Contains([]string{"archive", "abandon", "dismiss", "remove", "cancel", "recover", "review-plan"}, action) || r.ContentLength != 0 || len(r.TransferEncoding) != 0 {
		http.Error(w, "invalid action", http.StatusBadRequest)
		return
	}
	requestID, err := newControlRequestID()
	if err != nil {
		http.Error(w, "operator request identity is unavailable", http.StatusInternalServerError)
		return
	}
	request := controlRequest{Version: controlVersion, RequestID: requestID, Repository: s.repository, Action: action, Issue: issue, Attempt: attempt, Confirm: slices.Contains([]string{"archive", "abandon", "remove"}, action)}
	result := s.operator.perform(r.Context(), request)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(result.Status)
	_ = json.NewEncoder(w).Encode(result)
}
