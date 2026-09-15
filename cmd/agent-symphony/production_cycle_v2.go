package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type runtimeLifecyclePlan struct {
	Snapshot stateOwnerSnapshot
	Request  agentruntime.EffectRequest
}

func (p *productionReconciliation) selectedWorkerExport(ctx context.Context, record runtimeAttemptRecord) (workerResult, string, string, error) {
	if err := p.verifyWorkerExecutable(ctx); err != nil {
		return workerResult{}, "", "", err
	}
	if record.WorkerSeal != nil {
		selection := *record.WorkerSeal
		exported := workerExport{Repository: record.Manifest.Repository, Branch: record.Manifest.Branch, BaseSHA: record.Manifest.BaseSHA, HeadSHA: selection.HeadSHA, BundleSHA256: selection.BundleSHA256, Result: selection.Result}
		if !validWorkerSealSelection(p.stateRoot, record.Manifest, record.Generation, selection) || validateWorkerSeal(ctx, selection.Root, record.Generation, record.Manifest, exported) != nil {
			return workerResult{}, "", "", errors.New("selected worker seal is invalid")
		}
		return selection.Result, selection.HeadSHA, selection.Root, nil
	}
	result, head, root, err := importWorkerExport(ctx, p.implementation, p.stateRoot, record.Generation, record.Manifest)
	if err != nil {
		return workerResult{}, "", "", err
	}
	body, err := os.ReadFile(filepath.Join(root, "agent-symphony-seal.json"))
	if err != nil {
		return workerResult{}, "", "", err
	}
	var seal workerSeal
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&seal) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return workerResult{}, "", "", errors.New("worker seal metadata is invalid")
	}
	selection := workerSealSelection{Generation: record.Generation, HeadSHA: head, Root: root, BundleSHA256: seal.BundleSHA256, ProfileDigest: seal.ProfileDigest, Result: result}
	snapshot, err := p.owner.selectWorkerSeal(ctx, selectWorkerSealCommand{Repository: record.Manifest.Repository, Issue: record.Manifest.Issue, Attempt: record.Manifest.Attempt, ExpectedGeneration: record.Generation, Selection: selection})
	if err != nil {
		return workerResult{}, "", "", err
	}
	selected := snapshot.State.Attempts[ownerAttemptKey(record.Manifest.Repository, record.Manifest.Issue, record.Manifest.Attempt)].WorkerSeal
	if selected == nil {
		return workerResult{}, "", "", errors.New("worker seal selection was not committed")
	}
	return selected.Result, selected.HeadSHA, selected.Root, nil
}

// sweepPendingMarkers runs before production admission. It consumes immutable
// completion proof without reconstructing or repeating external work. Unmarked
// intents remain pending for exact reconstruction by the first fresh cycle.
func (p *productionReconciliation) sweepPendingMarkers(ctx context.Context) error {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	operatorEffects := make(map[string]bool, len(snapshot.State.ControlReceipts))
	for _, receipt := range snapshot.State.ControlReceipts {
		if receipt.State == "pending" && receipt.EffectID != "" {
			operatorEffects[receipt.EffectID] = true
		}
	}
	ids := make([]string, 0, len(snapshot.State.Effects))
	for id, effect := range snapshot.State.Effects {
		if effect.State == "completed" || effect.State == "pending" && !operatorEffects[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		effect := snapshot.State.Effects[id]
		if effect.State == "completed" {
			if effect.Reconciliation != nil {
				if err := cleanupCompletedHandoffOutcome(p.stateRoot, *effect.Reconciliation); err != nil {
					return err
				}
				if err := removeReconciliationEffectMarker(p.stateRoot, ownerReconciliationEffectIdentity(effect)); err != nil {
					return err
				}
			} else if err := p.effects.executor.Runtime.RemoveEffectMarker(effectRequestIdentity(effect)); err != nil {
				return err
			}
			continue
		}
		if effect.Reconciliation != nil {
			result, verifyErr := p.effects.verifyPendingReconciliation(ctx, effect)
			if verifyErr != nil {
				_, err = p.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: effect.Reconciliation.Action, Diagnostic: "startup marker verification failed: " + internalgithub.Redact(verifyErr.Error())})
			} else if result == nil {
				_, err = p.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: effect.Reconciliation.Action, Diagnostic: "waiting for exact fresh input reconstruction"})
			}
			if err != nil {
				return err
			}
			continue
		}
		action := agentruntime.EffectAction(effect.Action)
		identity := effectRequestIdentity(effect)
		markerPath := filepath.Join(p.stateRoot, "runtime-effects", effect.ID+".done")
		if _, statErr := os.Lstat(markerPath); errors.Is(statErr, os.ErrNotExist) {
			_, err = p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(identity), Action: action, Diagnostic: "waiting for exact fresh input reconstruction"})
		} else if statErr != nil {
			_, err = p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(identity), Action: action, Diagnostic: "startup marker inspection failed: " + internalgithub.Redact(statErr.Error())})
		} else {
			_, verifyErr := p.effects.verifyPending(ctx, snapshot, effect, agentruntime.EffectRequest{Identity: identity, Action: action})
			if verifyErr != nil {
				_, err = p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(identity), Action: action, Diagnostic: "startup marker verification failed: " + internalgithub.Redact(verifyErr.Error())})
			}
		}
		if err != nil {
			return err
		}
		snapshot, err = p.owner.snapshot(ctx)
		if err != nil {
			return err
		}
	}
	current, err := p.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	runtimePending := map[string]bool{}
	reconciliationPending := map[string]bool{}
	for id, effect := range current.State.Effects {
		if effect.State != "pending" {
			continue
		}
		if effect.Reconciliation == nil {
			runtimePending[id] = true
		} else {
			reconciliationPending[id] = true
		}
	}
	if p.effects.executor.Runtime == nil {
		if len(runtimePending) > 0 {
			return errors.New("runtime marker recovery is unavailable")
		}
	} else if err := p.effects.executor.Runtime.ReclaimOrphanEffectMarkers(runtimePending); err != nil {
		return err
	}
	return reclaimOrphanReconciliationMarkers(p.stateRoot, reconciliationPending)
}

var errReconciliationRecollect = errors.New("reconciliation requires fresh external observations")

type productionReconciliation struct {
	owner               *stateOwner
	effects             *runtimeEffectCoordinator
	collector           reconciliationV2Collector
	config              config.Config
	api                 internalgithub.API
	stateRoot           string
	attemptRoot         string
	checkout            string
	implementation      workerBoundaryRunner
	reviewer            workerBoundaryRunner
	operator            *operatorMutationService
	reviewEnv           []string
	wake                func() error
	supervisor          *orchestratoragent.Supervisor
	capacity            int
	workerProfileDigest string
	log                 io.Writer
}

func (p *productionReconciliation) verifyWorkerExecutable(ctx context.Context) error {
	if p.workerProfileDigest == "" { // Unit fixtures do not launch a production worker.
		return nil
	}
	if !validDigest(p.workerProfileDigest) || len(p.config.Commands.Implementation) == 0 {
		return errors.New("worker executable binding is unavailable")
	}
	return config.VerifyWorkerExecutable(ctx, p.config.Commands.Implementation[0], p.workerProfileDigest)
}

func (p *productionReconciliation) runCycle(ctx context.Context) error {
	if p == nil || p.owner == nil || p.effects == nil || p.collector.Config.Repository != p.config.Repository || p.stateRoot == "" || p.attemptRoot == "" || p.checkout == "" {
		return errors.New("production reconciliation is incomplete")
	}
	cycleSnapshot, err := p.owner.reconciliationSnapshot(ctx)
	if err != nil {
		return err
	}
	err = p.cycleFromSnapshot(ctx, cycleSnapshot)
	diagnostic := ""
	var at time.Time
	if err != nil && !errors.Is(err, errReconciliationRecollect) {
		diagnostic = internalgithub.Redact(err.Error())
		at = time.Now().UTC()
	}
	identity := stateResultIdentity{Epoch: cycleSnapshot.State.Epoch, SourceRevision: cycleSnapshot.State.Revision, CycleID: cycleSnapshot.CycleID}
	committed, outcomeErr := p.owner.recordCycleOutcome(ctx, recordCycleOutcomeCommand{Identity: identity, Diagnostic: diagnostic, At: at})
	if outcomeErr != nil {
		return errors.Join(err, outcomeErr)
	}
	if p.supervisor != nil && p.capacity > 0 {
		status, projectErr := projectOwnerStatus(committed, p.capacity, time.Now().UTC())
		observeErr := projectErr
		if observeErr == nil {
			cycleErr := err
			if errors.Is(cycleErr, errReconciliationRecollect) {
				cycleErr = nil
			}
			_, observeErr = p.supervisor.ObserveCycle(ctx, status.Statuses, cycleErr)
		}
		if observeErr != nil && p.log != nil {
			_, _ = fmt.Fprintln(p.log, "orchestrator agent: "+internalgithub.Redact(observeErr.Error()))
		}
	}
	return err
}

func (p *productionReconciliation) cycleFromSnapshot(ctx context.Context, cycleSnapshot stateOwnerSnapshot) error {
	api := p.api.WithReadSnapshot()
	collector := p.collector
	collector.API = api
	batch, err := collector.collect(ctx, cycleSnapshot)
	if err != nil {
		return err
	}
	collection, err := collectionFromSnapshot(cycleSnapshot, batch.Input)
	if err != nil {
		return err
	}
	applied, err := p.owner.applyReconciliation(ctx, collection)
	if err != nil {
		return err
	}
	applied, err = p.admitDependencyStatuses(ctx, applied)
	if err != nil {
		return err
	}
	p.effects.cancelInvalidated(applied)
	if resolved, err := p.resolveInvalidatedGitHubEffect(ctx, p.api); err != nil || resolved {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}
	if p.operator != nil {
		p.operator.cancelSupersededPlanWatchers(cycleSnapshot, applied)
		if superseded, err := p.supersedeInvalidPendingPlanReviewers(ctx, applied); err != nil || superseded {
			if err != nil {
				return err
			}
			return errReconciliationRecollect
		}
		p.operator.scanPendingPlanReviewers(ctx, applied)
	}
	if resumed, err := p.resumePendingReconciliation(ctx, api, batch); err != nil || resumed {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}
	if changed, err := p.runMachineStatusPhase(ctx, api); err != nil || changed {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}
	if changed, err := p.runIssueUpdatePhase(ctx, api, batch, false); err != nil || changed {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}

	baseBranch, baseSHA := "", ""
	if len(batch.Input.Issues) > 0 {
		baseBranch, baseSHA = batch.Input.Issues[0].BaseBranch, batch.Input.Issues[0].BaseSHA
	}
	source, err := seedImmutableAttemptSource(ctx, p.checkout, p.config.Repository, p.attemptRoot, baseBranch, baseSHA)
	if err != nil {
		return err
	}
	if err := p.resumePendingRuntime(ctx, batch, source); err != nil {
		return err
	}
	if err := p.runRuntimePhase(ctx, batch, source, agentruntime.EffectPrepare); err != nil {
		return err
	}
	if changed, err := p.runBindPhase(ctx, api); err != nil || changed {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}
	if err := p.runRuntimePhase(ctx, batch, source, agentruntime.EffectStart); err != nil {
		return err
	}
	if err := p.runRuntimePhase(ctx, batch, source, agentruntime.EffectMonitor); err != nil {
		return err
	}

	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	reviewers, publications, err := p.executionCandidates(ctx, snapshot, batch)
	if err != nil {
		return err
	}
	if err := p.runReviewerPhase(ctx, reviewers); err != nil {
		return err
	}
	if err := p.runHandoffOutcomePhase(ctx); err != nil {
		return err
	}
	if err := p.runHandoffPhase(ctx); err != nil {
		return err
	}
	if changed, err := p.runPublicationPhase(ctx, api, publications); err != nil || changed {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}
	if changed, err := p.runIssueUpdatePhase(ctx, api, batch, true); err != nil || changed {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}
	if changed, err := p.runGovernancePhase(ctx, api); err != nil || changed {
		if err != nil {
			return err
		}
		return errReconciliationRecollect
	}
	return p.runRetirementPhase(ctx)
}

func (p *productionReconciliation) admitDependencyStatuses(ctx context.Context, snapshot stateOwnerSnapshot) (stateOwnerSnapshot, error) {
	for issueKey, observation := range snapshot.State.Observations {
		if !observation.Present || !observation.Fact.NeedsAttention || observation.OwnerGeneration != snapshot.State.IssueGenerations[issueKey] {
			continue
		}
		for _, proposal := range observation.IssueUpdates {
			if proposal.Kind != githubIssueDependencyClear {
				continue
			}
			if current, exists := snapshot.State.MachineStatuses[issueKey]; exists && current.Source != "dependency" {
				continue
			}
			attemptGeneration := snapshot.State.AttemptGenerations[ownerAttemptKey(proposal.Repository, proposal.Issue, proposal.AttributionAttempt)]
			statusSequence := snapshot.State.MachineStatuses[issueKey].Sequence
			var err error
			snapshot, err = p.owner.admitMachineStatus(ctx, admitMachineStatusCommand{
				Repository: proposal.Repository, Issue: proposal.Issue, Attempt: proposal.AttributionAttempt,
				ExpectedIssueGeneration: observation.OwnerGeneration, ExpectedAttemptGeneration: attemptGeneration,
				ExpectedObservationGeneration: observation.Generation, ExpectedStatusSequence: statusSequence, Dependency: proposal.Dependency, PullRequest: proposal.PullRequest,
				Source: "dependency", SourceID: fmt.Sprintf("%d:%d:%d", observation.Generation, proposal.Dependency, proposal.PullRequest), Status: "clear", Reason: fmt.Sprintf("monitoring: dependency #%d is complete", proposal.Dependency),
			})
			if err != nil {
				return stateOwnerSnapshot{}, err
			}
		}
	}
	return snapshot, nil
}

func (p *productionReconciliation) runMachineStatusPhase(ctx context.Context, api internalgithub.API) (bool, error) {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return false, err
	}
	plans, err := planMachineStatusUpdates(snapshot, p.collector.Config)
	if err != nil || len(plans) == 0 {
		return false, err
	}
	plan, err := p.effects.beginReconciliation(ctx, plans[0])
	if err != nil {
		return false, err
	}
	_, err = p.effects.executeIssueUpdate(ctx, api, plan)
	return err == nil, err
}

func (p *productionReconciliation) resolveInvalidatedGitHubEffect(ctx context.Context, api internalgithub.API) (bool, error) {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return false, err
	}
	ids := make([]string, 0)
	for id, effect := range snapshot.State.Effects {
		if effect.State == "invalidated" && effect.Dispatched && effect.Reconciliation != nil && reconciliationMutatesGitHub(effect.Reconciliation.Action) {
			ids = append(ids, id)
		}
	}
	if len(ids) == 0 {
		return false, nil
	}
	slices.Sort(ids)
	resolved := false
	for _, id := range ids {
		changed, resolveErr := p.resolveOneInvalidatedGitHubEffect(ctx, api, snapshot.State.Effects[id])
		if resolveErr != nil {
			return resolved, resolveErr
		}
		resolved = resolved || changed
	}
	return resolved, nil
}

func (p *productionReconciliation) resolveOneInvalidatedGitHubEffect(ctx context.Context, api internalgithub.API, effect runtimeEffectIntent) (bool, error) {
	request := *effect.Reconciliation
	key, generation := ownerIssueKey(effect.Repository, effect.Issue), uint64(0)
	run, err := p.effects.acquireKey(ctx, key, effect.IssueGeneration, generation, request.ObservationGeneration, effect.ID)
	if err != nil {
		return false, err
	}
	defer p.effects.releaseKey(key, run)
	outcome := invalidatedExternalOutcome{Action: request.Action}
	switch request.Action {
	case reconciliationGitHubBind:
		outcome.Observed, err = observeGitHubBind(run.ctx, api, p.collector.Config, request)
	case reconciliationGitHubPublish:
		user, authErr := api.AuthenticatedUser(run.ctx)
		err = authErr
		if err == nil && user.ID != p.collector.Config.ActorID {
			err = errors.New("authenticated GitHub actor changed")
		}
		if err == nil {
			var pr internalgithub.PullRequest
			outcome.Observed, pr, err = verifyPublishedAttempt(run.ctx, api, request, user.ID)
			outcome.PR, outcome.HeadSHA = pr.Number, request.GitHubPublish.HeadSHA
		}
	case reconciliationGitHubIssueUpdate:
		if request.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			var snapshot stateOwnerSnapshot
			snapshot, err = p.owner.snapshot(run.ctx)
			current, ok := snapshot.State.MachineStatuses[ownerIssueKey(request.Repository, request.Issue)]
			if err == nil && (!ok || current.Sequence <= request.GitHubIssueUpdate.StatusSequence) {
				err = errStaleStateResult
			}
			if err == nil {
				err = api.EnsureOwnerStatus(run.ctx, current.Repository, current.Issue, current.Attempt, current.Sequence, current.Status == "needs-attention", current.Reason, p.collector.Config.ActorID)
				if err == nil {
					outcome.Observed, err = api.OwnerStatusApplied(run.ctx, current.Repository, current.Issue, current.Attempt, current.Sequence, current.Status == "needs-attention", current.Reason, p.collector.Config.ActorID)
					outcome.StatusSequence = current.Sequence
				}
			}
		} else if request.ControlRepair || reconciliationEffectIssueScoped(request) {
			outcome.Observed, err = internalgithub.ControlSnapshotRepairApplied(run.ctx, api, p.collector.Config, request.Repository, request.Issue, request.GitHubIssueUpdate.ControlSnapshotBody)
		} else if request.GitHubIssueUpdate.Kind == githubIssueRetry {
			outcome.Observed, err = internalgithub.EnsureRetrySuppressed(run.ctx, api, p.collector.Config, request.Issue, request.Attempt, time.Unix(0, request.GitHubIssueUpdate.FailedAtUnixNano))
		} else {
			outcome.Observed, err = attemptIssueUpdateApplied(run.ctx, api, request, p.collector.Config)
		}
	case reconciliationGitHubPRGovernance:
		// A pre-phase-ledger effect may already have reached merge under the old
		// coarse dispatch bit, so an empty ledger is conservatively merge-admitted.
		mergeAdmitted := len(effect.GovernancePhases) == 0 || slices.ContainsFunc(effect.GovernancePhases, func(phase internalgithub.GovernancePhase) bool { return phase.Kind == "merge" })
		if mergeAdmitted {
			outcome.Merged, err = api.PullRequestMerged(run.ctx, request.Repository, request.GitHubPRGovernance.PR)
			if err == nil && !outcome.Merged {
				// A 404 is only a point-in-time observation. An already accepted
				// exact-head merge can become visible later, so it cannot certify
				// that the invalidated effect was superseded.
				return false, nil
			}
		}
		if err == nil && mergeAdmitted {
			var facts []internalgithub.RecoveryAttemptFact
			facts, err = internalgithub.FetchAttemptFacts(run.ctx, api, request.Repository, request.GitHubPRGovernance.Policy.ActorID)
			outcome.Observed = exactGovernanceMergeObserved(request, facts)
		} else if err == nil {
			outcome.Observed = true
			for _, phase := range effect.GovernancePhases {
				if phase.Kind == "merge" {
					continue
				}
				observed, observeErr := internalgithub.GovernancePreMergeObserved(run.ctx, api, phase, request.GitHubPRGovernance.Policy.ActorID)
				if observeErr != nil {
					err = observeErr
					break
				}
				outcome.Observed = outcome.Observed && observed
			}
			outcome.Superseded = outcome.Observed
		}
		outcome.PR, outcome.HeadSHA = request.GitHubPRGovernance.PR, request.GitHubPRGovernance.HeadSHA
	default:
		err = errStateConflict
	}
	if err != nil {
		return false, err
	}
	if request.Action == reconciliationGitHubPRGovernance && (!outcome.Observed || !outcome.Merged && !outcome.Superseded) || request.Action != reconciliationGitHubPRGovernance && !outcome.Observed {
		return false, nil
	}
	if err := p.owner.resolveInvalidatedReconciliationEffect(ctx, resolveInvalidatedReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Outcome: outcome}); err != nil {
		return false, err
	}
	return true, nil
}

func exactGovernanceMergeObserved(request reconciliationEffectRequest, facts []internalgithub.RecoveryAttemptFact) bool {
	return request.GitHubPRGovernance != nil && slices.ContainsFunc(facts, func(fact internalgithub.RecoveryAttemptFact) bool {
		return fact.Repository == request.Repository && fact.PR == request.GitHubPRGovernance.PR && fact.Issue == request.Issue && fact.Attempt == request.Attempt && fact.HeadSHA == request.GitHubPRGovernance.HeadSHA && fact.State == "completed"
	})
}

// Receipt-bound Plan reviews are excluded from generic reconciliation replay.
// A fresh observation must revoke them here, including while their pane lives.
func (p *productionReconciliation) supersedeInvalidPendingPlanReviewers(ctx context.Context, snapshot stateOwnerSnapshot) (bool, error) {
	for _, receipt := range snapshot.State.ControlReceipts {
		if receipt.State != "pending" || receipt.Request.Action != "review-plan" {
			continue
		}
		effect, ok := snapshot.State.Effects[receipt.EffectID]
		if !ok || effect.State != "pending" || effect.Reconciliation == nil || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Mode != agentruntime.ReviewModePlan || !planReviewInvalidated(snapshot.State, effect) {
			continue
		}
		p.effects.cancelEffect(effect.ID)
		superseded, err := p.operator.supersedeInvalidPlanReview(ctx, effect)
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			diagnostic := "revoked Plan reviewer stop remains pending: " + internalgithub.Redact(err.Error())
			if len(diagnostic) > maxReconciliationStringBytes {
				diagnostic = diagnostic[:maxReconciliationStringBytes]
			}
			if _, diagnoseErr := p.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: reconciliationReviewer, Diagnostic: diagnostic}); diagnoseErr != nil && !errors.Is(diagnoseErr, errStaleStateResult) {
				return false, diagnoseErr
			}
			continue
		}
		if superseded {
			return true, nil
		}
	}
	return false, nil
}

func (p *productionReconciliation) resumePendingReconciliation(ctx context.Context, api internalgithub.API, batch reconciliationV2Batch) (bool, error) {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return false, err
	}
	operatorEffects := map[string]bool{}
	for _, receipt := range snapshot.State.ControlReceipts {
		if receipt.State == "pending" && receipt.EffectID != "" {
			operatorEffects[receipt.EffectID] = true
		}
	}
	ids := make([]string, 0, len(snapshot.State.Effects))
	for id, effect := range snapshot.State.Effects {
		if effect.State == "pending" && effect.Reconciliation != nil && !operatorEffects[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	diagnose := func(effect runtimeEffectIntent, prefix string, cause error) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		diagnostic := prefix + internalgithub.Redact(cause.Error())
		if len(diagnostic) > maxReconciliationStringBytes {
			diagnostic = diagnostic[:maxReconciliationStringBytes]
		}
		_, err := p.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: effect.Reconciliation.Action, Diagnostic: diagnostic})
		if errors.Is(err, errStaleStateResult) {
			return nil
		}
		return err
	}
	for _, id := range ids {
		effect := snapshot.State.Effects[id]
		if effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer != nil && effect.Reconciliation.Reviewer.Mode == agentruntime.ReviewModeImplementation && effect.Reconciliation.Reviewer.Phase == "run-observe" {
			stale := reconciliationEffectFinishCurrent(p.owner.stateRoot, snapshot.State, effect) != nil
			head := ""
			if !stale {
				record := snapshot.State.Attempts[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)]
				_, currentHead, _, importErr := p.selectedWorkerExport(ctx, record)
				if importErr != nil {
					if err := diagnose(effect, "pending reviewer export import failed: ", importErr); err != nil {
						return false, err
					}
					continue
				}
				if currentHead != effect.Reconciliation.Reviewer.HeadSHA {
					head = currentHead
				}
			}
			if stale || head != "" {
				if p.operator == nil {
					return false, errors.New("implementation reviewer supersession is unavailable")
				}
				p.effects.cancelEffect(effect.ID)
				if superseded, err := p.operator.supersedeInvalidPlanReview(ctx, effect, head); err != nil {
					if diagnoseErr := diagnose(effect, "pending reviewer stop remains pending: ", err); diagnoseErr != nil {
						return false, diagnoseErr
					}
					continue
				} else if superseded {
					return true, nil
				}
				continue
			}
		}
		if result, verifyErr := p.effects.verifyPendingReconciliation(ctx, effect); verifyErr != nil {
			if err := diagnose(effect, "pending effect marker verification failed: ", verifyErr); err != nil {
				return false, err
			}
			continue
		} else if result != nil {
			return true, nil
		}
		resumed, resumeErr := p.resumeUnmarkedReconciliation(ctx, api, batch, snapshot, effect)
		if resumeErr != nil {
			if err := diagnose(effect, "pending effect reconstruction failed: ", resumeErr); err != nil {
				return false, err
			}
			continue
		}
		if resumed {
			return true, nil
		}
	}
	return false, nil
}

func (p *productionReconciliation) resumeUnmarkedReconciliation(ctx context.Context, api internalgithub.API, batch reconciliationV2Batch, snapshot stateOwnerSnapshot, effect runtimeEffectIntent) (bool, error) {
	request := *cloneReconciliationEffectRequest(effect.Reconciliation)
	plan := reconciliationPlannedEffect{Identity: ownerReconciliationEffectIdentity(effect), Request: request}
	rawIssues := map[string]internalgithub.RecoveryIssueFact{}
	rawAttempts := map[string]internalgithub.RecoveryAttemptFact{}
	for _, issue := range batch.Input.Issues {
		rawIssues[ownerIssueKey(issue.Repository, issue.Issue)] = issue
	}
	for _, attempt := range batch.Input.Attempts {
		rawAttempts[ownerAttemptKey(attempt.Repository, attempt.Issue, attempt.Attempt)] = attempt
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	switch request.Action {
	case reconciliationGitHubBind:
		plan.Material.Config = p.collector.Config
		if githubBindExecutionDigest(request, p.collector.Config) != request.ExecutionDigest {
			return false, errStateConflict
		}
		_, err := p.effects.executeGitHubBind(ctx, api, plan)
		return err == nil, err
	case reconciliationGitHubPublish:
		record, ok := snapshot.State.Attempts[key]
		issue, supplied := rawIssues[ownerIssueKey(request.Repository, request.Issue)]
		if !ok || !supplied {
			return false, errStaleStateResult
		}
		result, head, root, err := p.selectedWorkerExport(ctx, record)
		if err != nil {
			return false, err
		}
		issue.Attempt = record.Manifest.Attempt
		material := publicationExecutionMaterial{Issue: issue, Config: p.collector.Config, Root: root, Head: head, Validation: result.Validation, Documentation: result.Documentation, Decisions: result.Decisions}
		if publicationExecutionDigest(request, material) != request.ExecutionDigest {
			return false, errStateConflict
		}
		_, err = p.effects.executePublication(ctx, api, plan, material)
		return err == nil, err
	case reconciliationGitHubIssueUpdate:
		if request.GitHubIssueUpdate.Kind == githubIssueMachineStatus {
			plan.Material = reconciliationIssueUpdateMaterial{Config: p.collector.Config}
			if issueUpdateExecutionDigest(request, plan.Material) != request.ExecutionDigest {
				return false, errStateConflict
			}
			_, err := p.effects.executeIssueUpdate(ctx, api, plan)
			return err == nil, err
		}
		if reconciliationEffectIssueScoped(request) {
			update := request.GitHubIssueUpdate
			accepted := reconciliationIssueUpdateProposal{Repository: request.Repository, Issue: request.Issue, Kind: update.Kind, ControlSnapshotDigest: update.ControlSnapshotDigest, AttributionAttempt: update.AttributionAttempt, Dependency: update.Dependency, PullRequest: update.PullRequest}
			for _, material := range batch.IssueUpdates {
				plan.Material = material
				if reflect.DeepEqual(material.Proposal, accepted) && issueUpdateExecutionDigest(request, material) == request.ExecutionDigest {
					_, err := p.effects.executeIssueUpdate(ctx, api, plan)
					return err == nil, err
				}
			}
			return false, errStaleStateResult
		}
		issue, issueOK := rawIssues[ownerIssueKey(request.Repository, request.Issue)]
		attempt, attemptOK := rawAttempts[key]
		if !issueOK {
			return false, errStaleStateResult
		}
		plan.Material = reconciliationIssueUpdateMaterial{Config: p.collector.Config, Issue: &issue}
		if attemptOK {
			plan.Material.Attempt = &attempt
		}
		if issueUpdateExecutionDigest(request, plan.Material) != request.ExecutionDigest {
			return false, errStateConflict
		}
		_, err := p.effects.executeIssueUpdate(ctx, api, plan)
		return err == nil, err
	case reconciliationGitHubPRGovernance:
		attempt, ok := rawAttempts[key]
		if !ok || governanceExecutionDigest(request, attempt) != request.ExecutionDigest {
			return false, errStaleStateResult
		}
		plan.Attempt = &attempt
		_, err := p.effects.executeGovernance(ctx, api, plan)
		return err == nil, err
	case reconciliationReviewer:
		record, ok := snapshot.State.Attempts[key]
		issue, supplied := rawIssues[ownerIssueKey(request.Repository, request.Issue)]
		if !ok || !supplied {
			return false, errStaleStateResult
		}
		_, head, source, err := p.selectedWorkerExport(ctx, record)
		if err != nil {
			return false, err
		}
		issue.Attempt = record.Manifest.Attempt
		material := reviewerExecutionMaterial{Issue: issue, Source: source, HeadSHA: head, Env: slices.Clone(p.reviewEnv), Command: slices.Clone(p.config.Commands.Reviewer), Replay: true}
		if reviewerExecutionDigest(request, material) != request.ExecutionDigest || digestText(issue.Body) != request.BodyDigest {
			return false, errStateConflict
		}
		_, pending, err := p.effects.executeReviewer(ctx, p.reviewer, plan, material)
		return err == nil && !pending, err
	case reconciliationHandoffDeliver:
		if request.Handoff.Outcome != nil {
			path := filepath.Join(p.stateRoot, "handoff-outcomes", request.Handoff.Key+".json")
			if handoffOutcomeExecutionDigest(request, path) != request.ExecutionDigest {
				return false, errStateConflict
			}
			_, err := p.effects.executeHandoffOutcome(ctx, plan, path)
			return err == nil, err
		}
		expanded, err := config.ExpandManagedWorkspace(p.config.Commands.Implementation, request.Manifest.Worktree)
		if err != nil {
			return false, err
		}
		material := handoffExecutionMaterial{Command: expanded}
		if handoffExecutionDigest(request, material) != request.ExecutionDigest {
			return false, errStateConflict
		}
		_, err = p.effects.executeHandoff(ctx, p.implementation, plan, material)
		return err == nil, err
	case reconciliationRetireCompleted:
		if retirementExecutionDigest(request) != request.ExecutionDigest {
			return false, errStateConflict
		}
		_, err := p.effects.executeRetirement(ctx, p.implementation, plan)
		return err == nil, err
	case reconciliationMonitoringCheckIn:
		return false, errors.New("check-in delivery has no safe unmarked completion proof")
	default:
		return false, errStateConflict
	}
}

func (p *productionReconciliation) resumePendingRuntime(ctx context.Context, batch reconciliationV2Batch, source string) error {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return err
	}
	operatorEffects := map[string]bool{}
	for _, receipt := range snapshot.State.ControlReceipts {
		if receipt.State == "pending" && receipt.EffectID != "" {
			operatorEffects[receipt.EffectID] = true
		}
	}
	raw := map[string]internalgithub.RecoveryIssueFact{}
	for _, issue := range batch.Input.Issues {
		raw[ownerIssueKey(issue.Repository, issue.Issue)] = issue
	}
	ids := make([]string, 0, len(snapshot.State.Effects))
	for id, effect := range snapshot.State.Effects {
		if effect.State == "pending" && effect.Reconciliation == nil && !operatorEffects[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		effect := snapshot.State.Effects[id]
		action := agentruntime.EffectAction(effect.Action)
		if !slices.Contains([]agentruntime.EffectAction{agentruntime.EffectPrepare, agentruntime.EffectStart, agentruntime.EffectMonitor}, action) || strings.HasPrefix(effect.Diagnostic, "startup marker verification failed") {
			continue
		}
		key := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
		record, ok := snapshot.State.Attempts[key]
		if action == agentruntime.EffectStart && ok && record.Manifest.Version != agentruntime.ManifestVersion2 {
			const diagnostic = "legacy launch identity unproved; manual migration required"
			if effect.Diagnostic != diagnostic {
				if _, err := p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(effect)), Action: action, Diagnostic: diagnostic}); err != nil {
					return err
				}
			}
			continue
		}
		observation := snapshot.State.Observations[ownerIssueKey(effect.Repository, effect.Issue)]
		issue, supplied := raw[ownerIssueKey(effect.Repository, effect.Issue)]
		if !ok || record.Generation != effect.AttemptGeneration || !supplied || !currentReconciliationObservation(snapshot.State, ownerIssueKey(effect.Repository, effect.Issue), observation) || digestText(issue.Body) != observation.Fact.BodyDigest {
			continue
		}
		manifest := cloneManifest(record.Manifest)
		if p.effects.effectActive(manifest, effect.ID) {
			continue
		}
		request := agentruntime.EffectRequest{Identity: effectRequestIdentity(effect), Action: action, Manifest: manifest, Eligible: true, CandidateLaunchToken: effect.CandidateLaunchToken, GateNonce: effect.StartGateNonce}
		if action == agentruntime.EffectPrepare || action == agentruntime.EffectStart {
			accepted := expandIssueFact(observation.Fact)
			accepted.Body, accepted.Attempt, accepted.BaseSHA = issue.Body, manifest.Attempt, manifest.BaseSHA
			request.Attempt, err = runtimeLaunchAttempt(p.config, accepted, manifest, p.attemptRoot)
		} else {
			request.Attempt = agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
		}
		if err != nil {
			return err
		}
		bound, executor, bindErr := p.effects.bindWithSource(request, source)
		if bindErr != nil {
			return bindErr
		}
		digest, digestErr := agentruntime.EffectRequestDigest(bound)
		if digestErr != nil || digest != effect.RequestDigest {
			_, err = p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: action, Diagnostic: "fresh input does not reproduce the durable request"})
			if err != nil {
				return err
			}
			continue
		}
		if action == agentruntime.EffectMonitor && manifest.Version != agentruntime.ManifestVersion2 {
			// A legacy session has no durable pane identity. Retire an old
			// pending monitor without inferring process state or redispatching it.
			if _, err := p.owner.finishRuntimeEffect(ctx, finishRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: action, Manifest: manifest}); err != nil {
				return err
			}
			continue
		}
		verification, verifyErr := executor.VerifyPending(ctx, bound)
		if verifyErr != nil {
			_, err = p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: action, Diagnostic: "pending effect verification failed: " + internalgithub.Redact(verifyErr.Error())})
			if err != nil {
				return err
			}
			continue
		}
		switch verification.Disposition {
		case agentruntime.EffectVerified:
			if verification.Result == nil {
				return errStateConflict
			}
			if _, err := p.owner.finishRuntimeEffect(ctx, finishRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: action, Manifest: verification.Result.Manifest}); err != nil {
				return err
			}
		case agentruntime.EffectRetry:
			if err := p.effects.dispatch(bound, func() {
				if p.wake != nil {
					_ = p.wake()
				}
			}); err != nil {
				return err
			}
		case agentruntime.EffectRotate:
			if action == agentruntime.EffectStart && effect.StartMayRun {
				// A permitted candidate may already have executed. Its missing
				// session is not proof of death, so never rotate or redispatch it.
				if effect.Diagnostic != "" {
					continue
				}
				if _, err := p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: action, Diagnostic: "pending Start launch identity or worker absence is unproved"}); err != nil {
					return err
				}
				continue
			}
			if action != agentruntime.EffectStart || bound.GateNonce == "" {
				return errStateConflict
			}
			_, rotated, err := p.owner.rotateStartGate(ctx, rotateStartGateCommand{Identity: ownerEffectIdentity(bound.Identity), OldNonce: bound.GateNonce})
			if err != nil {
				return err
			}
			bound.GateNonce = rotated.StartGateNonce
			if err := p.effects.dispatch(bound, func() {
				if p.wake != nil {
					_ = p.wake()
				}
			}); err != nil {
				return err
			}
		case agentruntime.EffectPending:
			if effect.Diagnostic != "" {
				continue
			}
			diagnostic := "external completion remains ambiguous"
			if action == agentruntime.EffectStart {
				diagnostic = "pending Start launch identity or worker absence is unproved"
			}
			_, err = p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: action, Diagnostic: diagnostic})
			if err != nil {
				return err
			}
		default:
			return errStateConflict
		}
		snapshot, err = p.owner.snapshot(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *productionReconciliation) runRuntimePhase(ctx context.Context, batch reconciliationV2Batch, source string, action agentruntime.EffectAction) error {
	for {
		snapshot, err := p.owner.snapshot(ctx)
		if err != nil {
			return err
		}
		plans, err := planRuntimeLifecycle(snapshot, batch, p.config, time.Now().UTC(), p.attemptRoot, p.stateRoot)
		if err != nil {
			return err
		}
		index := slices.IndexFunc(plans, func(plan runtimeLifecyclePlan) bool { return plan.Request.Action == action })
		if index < 0 {
			return nil
		}
		request, err := p.effects.beginWithSource(ctx, snapshot, plans[index].Request, source)
		if err != nil {
			return err
		}
		if action == agentruntime.EffectMonitor {
			if err := p.effects.dispatch(request, func() {
				if p.wake != nil {
					_ = p.wake()
				}
			}); err != nil {
				return err
			}
			continue
		}
		if _, err := p.effects.execute(ctx, request); err != nil {
			return err
		}
	}
}

func (p *productionReconciliation) runIssueUpdatePhase(ctx context.Context, api internalgithub.API, batch reconciliationV2Batch, attempts bool) (bool, error) {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return false, err
	}
	var plans []reconciliationPlannedEffect
	if attempts {
		plans, err = planReconciliationAttemptIssueUpdates(snapshot, batch, p.collector.Config)
	} else {
		plans, err = planControlSnapshotRepairs(snapshot, p.collector.Config)
		if err == nil && len(plans) == 0 {
			plans, err = planReconciliationIssueUpdates(snapshot, batch)
		}
	}
	if err != nil || len(plans) == 0 {
		return false, err
	}
	plan, err := p.effects.beginReconciliation(ctx, plans[0])
	if err != nil {
		return false, err
	}
	_, err = p.effects.executeIssueUpdate(ctx, api, plan)
	return err == nil, err
}

func (p *productionReconciliation) runBindPhase(ctx context.Context, api internalgithub.API) (bool, error) {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return false, err
	}
	plans, err := planReconciliationBinds(snapshot, p.collector.Config)
	if err != nil || len(plans) == 0 {
		return false, err
	}
	plan, err := p.effects.beginReconciliation(ctx, plans[0])
	if err != nil {
		return false, err
	}
	metrics := &internalgithub.CycleMetrics{}
	api.Metrics = metrics
	_, err = p.effects.executeGitHubBind(ctx, api, plan)
	return metrics.Mutated(), err
}

func (p *productionReconciliation) executionCandidates(ctx context.Context, snapshot stateOwnerSnapshot, batch reconciliationV2Batch) ([]reviewerExecutionMaterial, []publicationExecutionMaterial, error) {
	raw := map[string]internalgithub.RecoveryIssueFact{}
	for _, issue := range batch.Input.Issues {
		raw[ownerIssueKey(issue.Repository, issue.Issue)] = issue
	}
	var reviewers []reviewerExecutionMaterial
	var publications []publicationExecutionMaterial
	keys := make([]string, 0, len(snapshot.State.Attempts))
	for key := range snapshot.State.Attempts {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		record := snapshot.State.Attempts[key]
		if record.Generation != snapshot.State.AttemptGenerations[key] || record.Manifest.State != "completed" || pendingAttemptEffect(snapshot.State, key) {
			continue
		}
		issue, ok := raw[ownerIssueKey(record.Manifest.Repository, record.Manifest.Issue)]
		observation := snapshot.State.Observations[ownerIssueKey(record.Manifest.Repository, record.Manifest.Issue)]
		if !ok || !currentReconciliationObservation(snapshot.State, ownerIssueKey(record.Manifest.Repository, record.Manifest.Issue), observation) || digestText(issue.Body) != observation.Fact.BodyDigest {
			continue
		}
		result, head, root, err := p.selectedWorkerExport(ctx, record)
		if err != nil {
			if ctx.Err() != nil {
				return nil, nil, ctx.Err()
			}
			if p.log != nil {
				diagnostic := internalgithub.Redact(err.Error())
				if len(diagnostic) > maxReconciliationStringBytes {
					diagnostic = diagnostic[:maxReconciliationStringBytes]
				}
				_, _ = fmt.Fprintf(p.log, "attempt #%d/%d export import remains unavailable: %s\n", record.Manifest.Issue, record.Manifest.Attempt, diagnostic)
			}
			continue
		}
		issue.Attempt = record.Manifest.Attempt
		reviewers = append(reviewers, reviewerExecutionMaterial{Issue: issue, Source: root, HeadSHA: head, Env: slices.Clone(p.reviewEnv), Command: slices.Clone(p.config.Commands.Reviewer)})
		publications = append(publications, publicationExecutionMaterial{Issue: issue, Config: p.collector.Config, Root: root, Head: head, Validation: result.Validation, Documentation: result.Documentation, Decisions: result.Decisions})
	}
	return reviewers, publications, nil
}

func (p *productionReconciliation) runReviewerPhase(ctx context.Context, candidates []reviewerExecutionMaterial) error {
	for {
		if err := p.verifyWorkerExecutable(ctx); err != nil {
			return err
		}
		snapshot, err := p.owner.snapshot(ctx)
		if err != nil {
			return err
		}
		plans, material, err := planReconciliationReviewers(snapshot, p.stateRoot, candidates)
		if err != nil || len(plans) == 0 {
			return err
		}
		plan, err := p.effects.beginReconciliation(ctx, plans[0])
		if err != nil {
			return err
		}
		key := ownerAttemptKey(plan.Request.Repository, plan.Request.Issue, plan.Request.Attempt)
		if _, pending, runErr := p.effects.executeReviewer(ctx, p.reviewer, plan, material[key]); runErr != nil {
			diagnostic := "reviewer effect failed: " + internalgithub.Redact(runErr.Error())
			if len(diagnostic) > maxReconciliationStringBytes {
				diagnostic = diagnostic[:maxReconciliationStringBytes]
			}
			_, diagnoseErr := p.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: plan.Identity, Action: reconciliationReviewer, Diagnostic: diagnostic})
			if errors.Is(diagnoseErr, errStaleStateResult) {
				diagnoseErr = nil
			}
			return errors.Join(runErr, diagnoseErr)
		} else if pending {
			return nil
		}
	}
}

func (p *productionReconciliation) runHandoffOutcomePhase(ctx context.Context) error {
	for {
		snapshot, err := p.owner.snapshot(ctx)
		if err != nil {
			return err
		}
		plans, paths, err := collectHandoffOutcomePlans(snapshot, p.stateRoot)
		if err != nil || len(plans) == 0 {
			return err
		}
		plan, err := p.effects.beginReconciliation(ctx, plans[0])
		if err != nil {
			return err
		}
		if _, err := p.effects.executeHandoffOutcome(ctx, plan, paths[plan.Request.Handoff.Key]); err != nil {
			return err
		}
	}
}

func (p *productionReconciliation) runHandoffPhase(ctx context.Context) error {
	for {
		snapshot, err := p.owner.snapshot(ctx)
		if err != nil {
			return err
		}
		plans, material, err := planReconciliationHandoffs(snapshot, p.config.Commands.Implementation, nil)
		if err != nil || len(plans) == 0 {
			return err
		}
		plan, err := p.effects.beginReconciliation(ctx, plans[0])
		if err != nil {
			return err
		}
		key := ownerAttemptKey(plan.Request.Repository, plan.Request.Issue, plan.Request.Attempt)
		if _, err := p.effects.executeHandoff(ctx, p.implementation, plan, material[key]); err != nil {
			return err
		}
	}
}

func (p *productionReconciliation) runPublicationPhase(ctx context.Context, api internalgithub.API, candidates []publicationExecutionMaterial) (bool, error) {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return false, err
	}
	plans, material, err := planReconciliationPublications(snapshot, candidates)
	if err != nil || len(plans) == 0 {
		return false, err
	}
	plan, err := p.effects.beginReconciliation(ctx, plans[0])
	if err != nil {
		return false, err
	}
	key := ownerAttemptKey(plan.Request.Repository, plan.Request.Issue, plan.Request.Attempt)
	_, err = p.effects.executePublication(ctx, api, plan, material[key])
	return err == nil, err
}

func (p *productionReconciliation) runGovernancePhase(ctx context.Context, api internalgithub.API) (bool, error) {
	snapshot, err := p.owner.snapshot(ctx)
	if err != nil {
		return false, err
	}
	plans, err := planReconciliationGovernance(snapshot, p.collector.Config)
	if err != nil || len(plans) == 0 {
		return false, err
	}
	plan, err := p.effects.beginReconciliation(ctx, plans[0])
	if err != nil {
		return false, err
	}
	metrics := &internalgithub.CycleMetrics{}
	api.Metrics = metrics
	_, err = p.effects.executeGovernance(ctx, api, plan)
	return metrics.Mutated(), err
}

func (p *productionReconciliation) runRetirementPhase(ctx context.Context) error {
	for {
		snapshot, err := p.owner.snapshot(ctx)
		if err != nil {
			return err
		}
		plans := planReconciliationRetirements(snapshot)
		if len(plans) == 0 {
			return nil
		}
		plan, err := p.effects.beginReconciliation(ctx, plans[0])
		if err != nil {
			return err
		}
		if _, err := p.effects.executeRetirement(ctx, p.implementation, plan); err != nil {
			return err
		}
	}
}

// planRuntimeLifecycle is pure: it consumes only an immutable owner snapshot,
// the raw body material from the collection that produced it, and fixed daemon
// configuration. External work starts only after the returned request is bound
// and committed by runtimeEffectCoordinator.
func planRuntimeLifecycle(snapshot stateOwnerSnapshot, batch reconciliationV2Batch, cfg config.Config, now time.Time, attemptRoot, stateRoot string) ([]runtimeLifecyclePlan, error) {
	if snapshot.State.Repository == "" || snapshot.State.Repository != cfg.Repository || snapshot.CycleID != 0 || now.IsZero() {
		return nil, errors.New("runtime lifecycle input is invalid")
	}
	rawIssues := make(map[string]internalgithub.RecoveryIssueFact, len(batch.Input.Issues))
	for _, raw := range batch.Input.Issues {
		key := ownerIssueKey(raw.Repository, raw.Issue)
		if _, duplicate := rawIssues[key]; duplicate {
			return nil, errStateConflict
		}
		rawIssues[key] = raw
	}

	var scheduled []orchestrator.Issue
	for key, observation := range snapshot.State.Observations {
		if !currentReconciliationObservation(snapshot.State, key, observation) {
			continue
		}
		fact := observation.Fact
		var createdAt time.Time
		if fact.CreatedAtUnixNano != 0 {
			createdAt = time.Unix(0, fact.CreatedAtUnixNano)
		}
		scheduled = append(scheduled, orchestrator.Issue{
			Repository: fact.Repository, Number: fact.Issue, Priority: fact.Priority,
			CreatedAt: createdAt, Dependencies: slices.Clone(fact.Dependencies),
			SatisfiedDependencies: slices.Clone(fact.SatisfiedDependencies), Paths: slices.Clone(fact.Paths),
			Eligible: fact.Eligible, Blockers: slices.Clone(fact.Blockers), Active: fact.Active,
			Completed: fact.Completed, Cancelled: fact.Cancelled,
		})
	}
	decisions := orchestrator.Schedule(scheduled, orchestrator.Capacity{Global: cfg.Concurrency, Repositories: map[string]int{}})
	runnable := map[string]bool{}
	for _, decision := range decisions {
		runnable[ownerIssueKey(decision.Repository, decision.Number)] = decision.State == orchestrator.Runnable
	}

	var plans []runtimeLifecyclePlan
	for issueKey, observation := range snapshot.State.Observations {
		if !currentReconciliationObservation(snapshot.State, issueKey, observation) || !observation.Fact.DispatchAuthorized {
			continue
		}
		raw, ok := rawIssues[issueKey]
		if !ok || digestText(raw.Body) != observation.Fact.BodyDigest {
			continue
		}
		issue := expandIssueFact(observation.Fact)
		issue.Body = raw.Body
		attemptNumber := issue.Attempt
		if issue.ActiveAttempt != nil {
			attemptNumber = issue.ActiveAttempt.Attempt
		}
		if attemptNumber < 1 {
			continue
		}
		remote, remotelyBound := observedReconciliationAttempt(observation, attemptNumber)
		remotelyActive := remotelyBound && (remote.State == "active" || remote.State == "review-ready")
		initialKey := ownerAttemptKey(issue.Repository, issue.Issue, attemptNumber)
		currentRecord, locallyOwned := snapshot.State.Attempts[initialKey]
		locallyOwned = locallyOwned && currentRecord.Generation == snapshot.State.AttemptGenerations[initialKey]
		if !remotelyActive && !locallyOwned {
			replacement, err := currentPreparingReplacement(snapshot.State, observation, attemptNumber)
			if err != nil {
				return nil, err
			}
			if replacement > 0 {
				attemptNumber = replacement
			} else {
				attemptNumber = firstUnreservedAttempt(snapshot.State, issue.Repository, issue.Issue, attemptNumber)
			}
			if attemptNumber < 1 {
				continue
			}
			remote, remotelyBound = observedReconciliationAttempt(observation, attemptNumber)
		}
		attemptKey := ownerAttemptKey(issue.Repository, issue.Issue, attemptNumber)
		if _, tombstoned := snapshot.State.Tombstones[attemptKey]; tombstoned || pendingAttemptEffect(snapshot.State, attemptKey) {
			continue
		}
		record, exists := snapshot.State.Attempts[attemptKey]
		if !exists {
			if !runnable[issueKey] && (!remotelyBound || remote.State != "active" && remote.State != "review-ready") {
				continue
			}
			if snapshot.State.AttemptGenerations[attemptKey] != 0 {
				continue
			}
			issue.Attempt = attemptNumber
			if remotelyBound {
				issue.BaseSHA = remote.BaseSHA
			}
			attempt, err := runtimeLaunchAttempt(cfg, issue, agentruntime.Manifest{}, attemptRoot)
			if err != nil {
				return nil, err
			}
			manifest, err := agentruntime.PreparingManifest(attemptRoot, stateRoot, attempt, now)
			if err != nil {
				return nil, err
			}
			plans = append(plans, runtimeLifecyclePlan{Snapshot: snapshot, Request: agentruntime.EffectRequest{Action: agentruntime.EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}})
			continue
		}
		if record.Generation != snapshot.State.AttemptGenerations[attemptKey] {
			continue
		}
		manifest := cloneManifest(record.Manifest)
		attempt := agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA}
		switch manifest.State {
		case "preparing":
			if manifest.Version != agentruntime.ManifestVersion2 {
				// A historical pending Start cannot be relaunched by session name.
				continue
			}
			remote, bound := observedReconciliationAttempt(observation, manifest.Attempt)
			if !bound || remote.BaseSHA != manifest.BaseSHA || remote.State != "active" && remote.State != "review-ready" {
				continue
			}
			issue.Attempt, issue.BaseSHA = manifest.Attempt, manifest.BaseSHA
			var err error
			attempt, err = runtimeLaunchAttempt(cfg, issue, manifest, attemptRoot)
			if err != nil {
				return nil, err
			}
			plans = append(plans, runtimeLifecyclePlan{Snapshot: snapshot, Request: agentruntime.EffectRequest{Action: agentruntime.EffectStart, Attempt: attempt, Manifest: manifest, Eligible: true}})
		case "running":
			if manifest.Version != agentruntime.ManifestVersion2 {
				// Name alone cannot identify a legacy process. Leave its
				// process state unknown, without a recurring monitor wake.
				continue
			}
			plans = append(plans, runtimeLifecyclePlan{Snapshot: snapshot, Request: agentruntime.EffectRequest{Action: agentruntime.EffectMonitor, Attempt: attempt, Manifest: manifest, Eligible: true}})
		}
	}
	slices.SortFunc(plans, func(a, b runtimeLifecyclePlan) int {
		if ordered := cmp.Compare(a.Request.Manifest.Issue, b.Request.Manifest.Issue); ordered != 0 {
			return ordered
		}
		if ordered := cmp.Compare(a.Request.Manifest.Attempt, b.Request.Manifest.Attempt); ordered != 0 {
			return ordered
		}
		return cmp.Compare(a.Request.Action, b.Request.Action)
	})
	return plans, nil
}

func currentPreparingReplacement(state runtimeOwnerState, observation reconciliationObservation, proposed int) (int, error) {
	replacement := 0
	for key, record := range state.Attempts {
		manifest := record.Manifest
		if manifest.Repository != observation.Fact.Repository || manifest.Issue != observation.Fact.Issue || manifest.Attempt <= proposed || manifest.State != "preparing" || record.Generation != state.AttemptGenerations[key] {
			continue
		}
		if _, tombstoned := state.Tombstones[key]; tombstoned {
			continue
		}
		if replacement != 0 && replacement != manifest.Attempt {
			return 0, errStateConflict
		}
		replacement = manifest.Attempt
	}
	return replacement, nil
}

func reconciliationBindMatchesObservation(state runtimeOwnerState, observation reconciliationObservation, manifest agentruntime.Manifest) (bool, error) {
	if manifest.State != "preparing" || !observation.Fact.DispatchAuthorized || observation.Fact.BaseSHA != manifest.BaseSHA {
		return false, nil
	}
	if observation.Fact.Attempt == manifest.Attempt {
		return true, nil
	}
	replacement, err := currentPreparingReplacement(state, observation, observation.Fact.Attempt)
	if err != nil {
		return false, err
	}
	return replacement == manifest.Attempt && !reconciliationObservationHasActiveBinding(observation), nil
}

func firstUnreservedAttempt(state runtimeOwnerState, repository string, issue, proposed int) int {
	for proposed > 0 {
		key := ownerAttemptKey(repository, issue, proposed)
		if state.AttemptGenerations[key] == 0 {
			if _, owned := state.Attempts[key]; !owned {
				if _, tombstoned := state.Tombstones[key]; !tombstoned {
					return proposed
				}
			}
		}
		proposed++
	}
	return 0
}

func currentReconciliationObservation(state runtimeOwnerState, key string, observation reconciliationObservation) bool {
	return observation.Present && observation.OwnerGeneration != 0 && observation.OwnerGeneration == state.IssueGenerations[key] && observation.ObservationEpoch == state.Epoch
}

func pendingAttemptEffect(state runtimeOwnerState, attemptKey string) bool {
	for _, effect := range state.Effects {
		if effect.State == "pending" && ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt) == attemptKey {
			return true
		}
	}
	return false
}

func runtimeLaunchAttempt(cfg config.Config, issue internalgithub.RecoveryIssueFact, manifest agentruntime.Manifest, attemptRoot string) (agentruntime.Attempt, error) {
	command, interactive := interactiveImplementationCommand(cfg.Commands.Implementation)
	attempt := agentruntime.Attempt{Repository: issue.Repository, Issue: issue.Issue, Number: issue.Attempt, BaseSHA: issue.BaseSHA, Interactive: interactive}
	identity := manifest
	if identity.Repository == "" {
		var err error
		identity, err = agentruntime.AttemptIdentity(attemptRoot, attempt)
		if err != nil {
			return agentruntime.Attempt{}, err
		}
	}
	expanded, err := config.ExpandManagedWorkspace(command, identity.Worktree)
	if err != nil {
		return agentruntime.Attempt{}, err
	}
	attempt.Command = expanded
	attempt.Context = implementationPrompt(issue, identity, interactive)
	return attempt, nil
}
