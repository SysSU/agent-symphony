package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"slices"
	"strconv"

	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// operatorMutationService is the dormant v2 request path. It commits owner
// state before launching any external work and never participates in the v1
// operation mutex or compatibility writers.
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
}

type operatorWork struct {
	requestID string
	runtime   *agentruntime.EffectRequest
	plan      *reconciliationPlannedEffect
	reviewer  *reviewerExecutionMaterial
}

type operatorReceiptStatus struct {
	Phase    string `json:"phase"`
	EffectID string `json:"effect_id,omitempty"`
}

func newOperatorMutationService(lifecycle context.Context, owner *stateOwner, effects *runtimeEffectCoordinator, cleanup operatorCleanupExecutor, collector reconciliationV2Collector, reviewer boundaryCaller, reviewSource string, reviewEnvironment, reviewCommand []string) (*operatorMutationService, error) {
	if lifecycle == nil || owner == nil || effects == nil || effects.owner != owner || effects.executor.Runtime == nil || cleanup.runtime != effects.executor.Runtime || cleanup.stateRoot != owner.stateRoot || collector.API.HTTP == nil || collector.Config.Repository == "" || collector.Config.ActorID < 1 || reviewer == nil {
		return nil, errors.New("operator mutation service is incomplete")
	}
	snapshot, err := owner.snapshot(lifecycle)
	if err != nil || snapshot.State.Repository != collector.Config.Repository {
		return nil, errors.New("operator mutation service repository does not match owner")
	}
	effects.executor.Cleanup = cleanup.execute
	effects.executor.VerifyCleanup = cleanup.verify
	service := &operatorMutationService{
		lifecycle: lifecycle, owner: owner, effects: effects, cleanup: cleanup, collector: collector, reviewer: reviewer,
		reviewSource: reviewSource, reviewEnvironment: slices.Clone(reviewEnvironment), reviewCommand: slices.Clone(reviewCommand), issueClosed: currentGitHubIssueClosed,
	}
	service.collect = func(ctx context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		bound := collector
		bound.Scope = reconciliationScope{Kind: reconciliationIssueScope, Repository: snapshot.State.Repository, Issue: issue}
		return bound.collect(ctx, snapshot)
	}
	return service, nil
}

func (s *operatorMutationService) perform(ctx context.Context, request controlRequest) controlResult {
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
		s.dispatchResume(request.RequestID)
		receipt, found := operatorReceiptByID(committed.State, request.RequestID)
		if !found {
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
		s.dispatch(work)
	}
	receipt, ok := operatorReceiptByID(committed.State, request.RequestID)
	if !ok {
		return operatorErrorResult(request, http.StatusInternalServerError, "operator receipt was not committed")
	}
	return operatorResultForReceipt(committed, receipt)
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
	switch request.Action {
	case "dismiss":
		if s.issueClosed == nil {
			return beginOperatorMutationCommand{}, operatorWork{}, errStateConflict
		}
		closed, err := s.issueClosed(ctx, request.Repository, request.Issue)
		if err != nil {
			return beginOperatorMutationCommand{}, operatorWork{}, err
		}
		if !closed {
			return beginOperatorMutationCommand{}, operatorWork{}, errStateConflict
		}
		command.IssueClosed = true
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
		if status.State == "blocked" {
			if err := s.effects.executor.Runtime.VerifyOwned(ctx, manifest); err == nil {
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
	if !ok || !observed || !observation.Present || record.Generation != snapshot.State.AttemptGenerations[key] || observation.OwnerGeneration != snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] {
		return agentruntime.Manifest{}, orchestrator.RecoveryStatus{}, errStaleStateResult
	}
	status, _, err := ownerOperatorStatus(snapshot.State, request.Issue, request.Attempt)
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
	if err := s.effects.executor.Runtime.VerifyOwned(ctx, manifest); err != nil {
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
	}
}

func (s *operatorMutationService) dispatch(work operatorWork) {
	go func() { _ = s.execute(work) }()
}

func (s *operatorMutationService) execute(work operatorWork) error {
	if work.runtime != nil {
		result, err := s.effects.executeOperator(*work.runtime)
		if result.Disposition == agentruntime.EffectResultReady && work.requestID != "" {
			err = errors.Join(err, s.resumeReceipt(s.lifecycle, work.requestID))
		}
		return err
	}
	if work.plan == nil {
		return nil
	}
	if work.reviewer != nil {
		result, pending, err := s.effects.executeOperatorReviewer(s.reviewer, *work.plan, *work.reviewer)
		if !pending && result.Action != "" && work.requestID != "" {
			err = errors.Join(err, s.resumeReceipt(s.lifecycle, work.requestID))
		}
		return err
	}
	result, err := s.effects.executeOperatorIssueUpdate(s.collector.API, *work.plan)
	if result.Action != "" && work.requestID != "" {
		err = errors.Join(err, s.resumeReceipt(s.lifecycle, work.requestID))
	}
	return err
}

func (s *operatorMutationService) dispatchResume(requestID string) {
	go func() { _ = s.resumeReceipt(s.lifecycle, requestID) }()
}

func operatorResultForReceipt(snapshot stateOwnerSnapshot, receipt controlReceipt) controlResult {
	if receipt.State == "completed" {
		return *receipt.Result
	}
	body, _ := json.Marshal(operatorReceiptStatus{Phase: receipt.Phase, EffectID: receipt.EffectID})
	return controlResult{Version: controlVersion, RequestID: receipt.Request.RequestID, Action: receipt.Request.Action, OK: true, Status: http.StatusAccepted, OwnerRevision: snapshot.State.Revision, Data: body}
}

func operatorResultForError(request controlRequest, err error) controlResult {
	status := http.StatusInternalServerError
	message := "operator mutation failed"
	if errors.Is(err, errStateConflict) || errors.Is(err, errStaleStateResult) || errors.Is(err, errAttemptTombstoned) {
		status, message = http.StatusConflict, "operator mutation conflicts with current state"
	} else if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		status, message = http.StatusRequestTimeout, "operator request was cancelled before admission"
	}
	return operatorErrorResult(request, status, message)
}

func operatorErrorResult(request controlRequest, status int, message string) controlResult {
	return controlResult{Version: controlVersion, RequestID: request.RequestID, Action: request.Action, Status: status, Error: message}
}

func (s *operatorMutationService) resumeReceipt(ctx context.Context, requestID string) error {
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
			return err
		}
		command, work, err := s.prepareRecoveryAdmission(fresh, batch, receipt.Request, kind)
		if err != nil {
			return err
		}
		committed, effect, err := s.owner.advanceOperatorRecovery(ctx, advanceOperatorRecoveryCommand{RequestID: requestID, Identity: command.Identity, Reconciliation: *command.Reconciliation})
		if err != nil {
			return err
		}
		s.effects.cancelInvalidated(committed)
		work.requestID = requestID
		bindOperatorWorkIdentity(&work, *effect)
		return s.execute(work)
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
			return s.resumeReceipt(ctx, requestID)
		}
		return s.resumeUnmarkedReconciliation(ctx, snapshot, receipt, effect)
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
		return s.resumeReceipt(ctx, requestID)
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
	return s.execute(operatorWork{requestID: requestID, runtime: &bound})
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

func (s *operatorMutationService) resumeUnmarkedReconciliation(ctx context.Context, snapshot stateOwnerSnapshot, receipt controlReceipt, effect runtimeEffectIntent) error {
	if effect.Reconciliation == nil {
		return errStateConflict
	}
	if effect.Reconciliation.Action == reconciliationGitHubIssueUpdate {
		fresh, err := s.owner.reconciliationSnapshot(ctx)
		if err != nil {
			return err
		}
		if s.collect == nil {
			return errors.New("operator collector is unavailable")
		}
		batch, err := s.collect(ctx, fresh, receipt.Request.Issue)
		if err != nil {
			return err
		}
		if _, err := collectionFromSnapshot(fresh, batch.Input); err != nil {
			return err
		}
		plans, err := planReconciliationAttemptIssueUpdates(fresh, batch, s.collector.Config)
		if err != nil {
			return err
		}
		for _, plan := range plans {
			if reflect.DeepEqual(plan.Request, *effect.Reconciliation) {
				plan.Identity = ownerReconciliationEffectIdentity(effect)
				return s.execute(operatorWork{requestID: receipt.Request.RequestID, plan: &plan})
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
			return errStateConflict
		}
		plan.Identity = ownerReconciliationEffectIdentity(effect)
		return s.execute(operatorWork{requestID: receipt.Request.RequestID, plan: &plan, reviewer: &material})
	}
	return errStateConflict
}

func (s *operatorMutationService) resumePending(ctx context.Context) error {
	snapshot, err := s.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	for _, receipt := range snapshot.State.ControlReceipts {
		if receipt.State == "pending" {
			s.dispatchResume(receipt.Request.RequestID)
		}
	}
	return nil
}

func newProjectDashboardServerV2(ctx context.Context, stateRoot, repository string, peerProjects []string, tmux string, service *operatorMutationService, allowNet bool, password string) (*dashboardServer, error) {
	if service == nil || service.owner == nil || service.owner.stateRoot != stateRoot || service.collector.Config.Repository != repository {
		return nil, errors.New("v2 dashboard owner binding is invalid")
	}
	server := newProjectDashboardServer(ctx, stateRoot, repository, peerProjects, tmux, nil, nil, nil, nil, nil, allowNet, password)
	server.operator = service
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
