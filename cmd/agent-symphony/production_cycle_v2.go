package main

import (
	"cmp"
	"context"
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
		if effect.State == "pending" && !operatorEffects[id] {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	for _, id := range ids {
		effect := snapshot.State.Effects[id]
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
	return nil
}

var errReconciliationRecollect = errors.New("reconciliation requires fresh external observations")

type productionReconciliation struct {
	owner          *stateOwner
	effects        *runtimeEffectCoordinator
	collector      reconciliationV2Collector
	config         config.Config
	api            internalgithub.API
	stateRoot      string
	attemptRoot    string
	checkout       string
	implementation workerBoundaryRunner
	reviewer       workerBoundaryRunner
	reviewEnv      []string
	wake           func() error
	supervisor     *orchestratoragent.Supervisor
	capacity       int
	log            io.Writer
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
	if _, err = p.owner.applyReconciliation(ctx, collection); err != nil {
		return err
	}
	if resumed, err := p.resumePendingReconciliation(ctx, api, batch); err != nil || resumed {
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
	for _, id := range ids {
		effect := snapshot.State.Effects[id]
		if result, verifyErr := p.effects.verifyPendingReconciliation(ctx, effect); verifyErr != nil {
			if _, err := p.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: effect.Reconciliation.Action, Diagnostic: "pending effect marker verification failed: " + internalgithub.Redact(verifyErr.Error())}); err != nil {
				return false, err
			}
			continue
		} else if result != nil {
			return true, nil
		}
		resumed, resumeErr := p.resumeUnmarkedReconciliation(ctx, api, batch, snapshot, effect)
		if resumeErr != nil {
			if _, err := p.owner.diagnoseReconciliationEffect(ctx, diagnoseReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: effect.Reconciliation.Action, Diagnostic: "pending effect reconstruction failed: " + internalgithub.Redact(resumeErr.Error())}); err != nil {
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
		result, head, root, err := importWorkerExport(ctx, p.implementation, record.Manifest)
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
		_, head, source, err := importWorkerExport(ctx, p.implementation, record.Manifest)
		if err != nil {
			return false, err
		}
		issue.Attempt = record.Manifest.Attempt
		material := reviewerExecutionMaterial{Issue: issue, Source: source, HeadSHA: head, Env: slices.Clone(p.reviewEnv), Command: slices.Clone(p.config.Commands.Reviewer)}
		if reviewerExecutionDigest(request, material) != request.ExecutionDigest || digestText(issue.Body) != request.BodyDigest {
			return false, errStateConflict
		}
		_, _, err = p.effects.executeReviewer(ctx, p.reviewer, plan, material)
		return err == nil, err
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
		observation := snapshot.State.Observations[ownerIssueKey(effect.Repository, effect.Issue)]
		issue, supplied := raw[ownerIssueKey(effect.Repository, effect.Issue)]
		if !ok || record.Generation != effect.AttemptGeneration || !supplied || !currentReconciliationObservation(snapshot.State, ownerIssueKey(effect.Repository, effect.Issue), observation) || digestText(issue.Body) != observation.Fact.BodyDigest {
			continue
		}
		manifest := cloneManifest(record.Manifest)
		request := agentruntime.EffectRequest{Identity: effectRequestIdentity(effect), Action: action, Manifest: manifest, Eligible: true}
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
		case agentruntime.EffectPending:
			_, err = p.owner.diagnoseRuntimeEffect(ctx, diagnoseRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: action, Diagnostic: "external completion remains ambiguous"})
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
		plans, err = planReconciliationIssueUpdates(snapshot, batch)
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
		result, head, root, err := importWorkerExport(ctx, p.implementation, record.Manifest)
		if err != nil {
			return nil, nil, fmt.Errorf("import worker export for %s: %w", key, err)
		}
		issue.Attempt = record.Manifest.Attempt
		reviewers = append(reviewers, reviewerExecutionMaterial{Issue: issue, Source: root, HeadSHA: head, Env: slices.Clone(p.reviewEnv), Command: slices.Clone(p.config.Commands.Reviewer)})
		publications = append(publications, publicationExecutionMaterial{Issue: issue, Config: p.collector.Config, Root: root, Head: head, Validation: result.Validation, Documentation: result.Documentation, Decisions: result.Decisions})
	}
	return reviewers, publications, nil
}

func (p *productionReconciliation) runReviewerPhase(ctx context.Context, candidates []reviewerExecutionMaterial) error {
	for {
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
		if _, pending, err := p.effects.executeReviewer(ctx, p.reviewer, plan, material[key]); err != nil {
			return err
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
		attemptKey := ownerAttemptKey(issue.Repository, issue.Issue, attemptNumber)
		if _, tombstoned := snapshot.State.Tombstones[attemptKey]; tombstoned || pendingAttemptEffect(snapshot.State, attemptKey) {
			continue
		}
		record, exists := snapshot.State.Attempts[attemptKey]
		if !exists {
			remote, remotelyBound := observedReconciliationAttempt(observation, attemptNumber)
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
