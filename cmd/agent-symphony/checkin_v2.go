package main

import (
	"encoding/json"

	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func planMonitoringCheckIn(snapshot stateOwnerSnapshot, proposal orchestratoragent.MessageProposal) (reconciliationPlannedEffect, error) {
	if proposal.Action != orchestratoragent.ProposalActionCheckIn || proposal.Repository != snapshot.State.Repository || proposal.Issue < 1 || proposal.Attempt < 1 || !validDigest(proposal.Binding) {
		return reconciliationPlannedEffect{}, errStateConflict
	}
	issueKey := ownerIssueKey(proposal.Repository, proposal.Issue)
	attemptKey := ownerAttemptKey(proposal.Repository, proposal.Issue, proposal.Attempt)
	observation := snapshot.State.Observations[issueKey]
	record, ok := snapshot.State.Attempts[attemptKey]
	fact, observed := observedReconciliationAttempt(observation, proposal.Attempt)
	if !ok || record.Generation != snapshot.State.AttemptGenerations[attemptKey] || !currentReconciliationObservation(snapshot.State, issueKey, observation) || !observed || pendingAttemptEffect(snapshot.State, attemptKey) {
		return reconciliationPlannedEffect{}, errStaleStateResult
	}
	manifest := cloneManifest(record.Manifest)
	if manifest.State != "running" || manifest.Session == "" || !observation.Fact.DispatchAuthorized || !observation.Fact.NeedsAttention || (fact.State != "active" && fact.State != "review-ready") || fact.BaseSHA != manifest.BaseSHA {
		return reconciliationPlannedEffect{}, errStateConflict
	}
	payload, _ := json.Marshal(struct {
		Type       string `json:"type"`
		Repository string `json:"repository"`
		Issue      int    `json:"issue"`
		Attempt    int    `json:"attempt"`
		Request    string `json:"request"`
	}{"agent-symphony-monitoring-check-in-v1", proposal.Repository, proposal.Issue, proposal.Attempt, "Report current progress and the next step in this session. Continue only the implementation you already own. If blocked, set needs-attention with a specific reason through the direct GitHub status contract; clear a prior monitoring status only after fresh evidence shows recovery."})
	checkIn := &monitoringCheckInEffectRequest{Session: manifest.Session, Binding: proposal.Binding, Payload: string(payload)}
	request := reconciliationEffectRequest{
		Action: reconciliationMonitoringCheckIn, Repository: proposal.Repository, Issue: proposal.Issue, Attempt: proposal.Attempt,
		Manifest: &manifest, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID,
		BodyDigest: observation.Fact.BodyDigest, CheckIn: checkIn,
	}
	request.ExecutionDigest = monitoringCheckInExecutionDigest(request)
	return reconciliationPlannedEffect{Identity: stateResultIdentity{
		Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision,
		IssueGeneration: snapshot.State.IssueGenerations[issueKey], AttemptGeneration: snapshot.State.AttemptGenerations[attemptKey],
	}, Request: request}, nil
}

func monitoringCheckInExecutionDigest(request reconciliationEffectRequest) string {
	if request.CheckIn == nil {
		return ""
	}
	return digestText(request.CheckIn.Session + "\x00" + request.CheckIn.Binding + "\x00" + request.CheckIn.Payload)
}

func (c *runtimeEffectCoordinator) executeMonitoringCheckIn(plan reconciliationPlannedEffect) (reconciliationEffectResult, error) {
	request := plan.Request
	if c == nil || c.owner == nil || c.executor.Runtime == nil || request.Action != reconciliationMonitoringCheckIn || request.CheckIn == nil || monitoringCheckInExecutionDigest(request) != request.ExecutionDigest {
		return reconciliationEffectResult{}, errStateConflict
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	run, err := c.acquireKey(c.lifecycle, key, plan.Identity.IssueGeneration, plan.Identity.AttemptGeneration, request.ObservationGeneration)
	if err != nil {
		return reconciliationEffectResult{}, err
	}
	defer c.releaseKey(key, run)
	base := c.executor.Runtime
	runtime := &agentruntime.Runtime{Root: base.Root, StateRoot: base.StateRoot, Tmux: base.Tmux, Runner: base.Runner, VerifyWorker: base.VerifyWorker}
	if err := runtime.VerifyOwned(run.ctx, *request.Manifest); err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := c.owner.authorizeReconciliationEffect(run.ctx, authorizeReconciliationEffectCommand{Identity: plan.Identity, Action: request.Action}); err != nil {
		return reconciliationEffectResult{}, err
	}
	if err := runtime.Deliver(run.ctx, *request.Manifest, []byte(request.CheckIn.Payload)); err != nil {
		return reconciliationEffectResult{}, err
	}
	result := reconciliationEffectResult{Action: request.Action, CheckIn: &monitoringCheckInEffectResult{Session: request.CheckIn.Session, Observed: true}}
	if err := c.finishReconciliationWithMarker(plan.Identity, request, result); err != nil {
		return reconciliationEffectResult{}, err
	}
	return result, nil
}
