package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestSupervisorRetryWaitsForCompletedCycleBeforeSuccess(t *testing.T) {
	owner, manifest := retryProposalOwner(t, 371)
	agent := proposalTestSupervisor(t)
	snapshot := mustOwnerSnapshot(t, owner)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Action: orchestratoragent.ProposalActionRetry, RequestID: "retry-371-1", OwnerCausalityToken: ownerAttemptCausalityToken(snapshot.State, manifest.Repository, manifest.Issue, manifest.Attempt)}
	writeProposalV2(t, agent, proposal)

	started, release := make(chan struct{}), make(chan struct{})
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(ctx context.Context) error {
		close(started)
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	service := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: &runtimeEffectCoordinator{}, operator: &operatorMutationService{}, trigger: trigger, capacity: 1}
	done := make(chan error, 1)
	go func() { done <- service.process(t.Context()) }()
	<-started
	if status := readProposalStatusV2(t, agent); status.Resolution != "running" || status.PendingBinding == "" {
		t.Fatalf("blocked retry status=%#v", status)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "succeeded" || status.ResolvedBinding == "" {
		t.Fatalf("completed retry status=%#v", status)
	}
}

func TestSupervisorInvalidRetryIsRefusedBeforeRunning(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 372, "active", false)
	agent := proposalTestSupervisor(t)
	writeProposalV2(t, agent, orchestratoragent.MessageProposal{Version: 1, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Action: orchestratoragent.ProposalActionRetry, RequestID: "retry-372-1"})
	triggered := false
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error { triggered = true; return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	service := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: &runtimeEffectCoordinator{}, operator: &operatorMutationService{}, trigger: trigger, capacity: 1}
	if err := service.process(t.Context()); err == nil {
		t.Fatal("invalid retry unexpectedly succeeded")
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "refused" || strings.Contains(status.Detail, "running") || triggered {
		t.Fatalf("invalid retry status=%#v triggered=%v", status, triggered)
	}
}

func TestSupervisorStatusProposalCannotRebindAfterDismiss(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 333, "active", false)
	before := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey("o/r", 333), ownerAttemptKey("o/r", 333, 1)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 333, Attempt: 1, Action: orchestratoragent.ProposalActionStatusSet, RequestID: "status-333-1", Detail: "monitoring: stale heartbeat", IssueGeneration: before.State.IssueGenerations[issueKey], AttemptGeneration: before.State.AttemptGenerations[attemptKey], OwnerCausalityToken: ownerAttemptCausalityToken(before.State, "o/r", 333, 1)}
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 333, Attempt: 1, ExpectedIssueGeneration: proposal.IssueGeneration, ExpectedAttemptGeneration: proposal.AttemptGeneration, Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest}); err != nil {
		t.Fatal(err)
	}
	agent := proposalTestSupervisor(t)
	writeProposalV2(t, agent, proposal)
	operator := operatorTestMutationService(t, owner)
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	service := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: operator.effects, operator: operator, trigger: trigger, capacity: 1}
	if err := service.process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "refused" {
		t.Fatalf("proposal status=%#v", status)
	}
	committed := mustOwnerSnapshot(t, owner).State.MachineStatuses[issueKey]
	if committed.Status != "clear" || committed.Source != "destructive" || committed.Sequence != 1 {
		t.Fatalf("stale proposal changed owner status: %#v", committed)
	}
}

func TestSupervisorStatusProposalCannotOverwriteNewerStatus(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 334, "active", false)
	before := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey("o/r", 334), ownerAttemptKey("o/r", 334, 1)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 334, Attempt: 1, Action: orchestratoragent.ProposalActionStatusSet, RequestID: "status-334-1", Detail: "monitoring: stale heartbeat", IssueGeneration: before.State.IssueGenerations[issueKey], AttemptGeneration: before.State.AttemptGenerations[attemptKey], MachineStatusSequence: 0, OwnerCausalityToken: ownerAttemptCausalityToken(before.State, "o/r", 334, 1)}
	if _, err := owner.admitMachineStatus(t.Context(), admitMachineStatusCommand{Repository: "o/r", Issue: 334, Attempt: 1, ExpectedIssueGeneration: proposal.IssueGeneration, ExpectedAttemptGeneration: proposal.AttemptGeneration, ExpectedStatusSequence: 0, Source: "worker", SourceID: "new-clear", SourceSequence: 1, Status: "clear", Reason: "monitoring: recovered"}); err != nil {
		t.Fatal(err)
	}
	agent := proposalTestSupervisor(t)
	writeProposalV2(t, agent, proposal)
	operator := operatorTestMutationService(t, owner)
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	service := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: operator.effects, operator: operator, trigger: trigger, capacity: 1}
	if err := service.process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "refused" {
		t.Fatalf("proposal status=%#v", status)
	}
	committed := mustOwnerSnapshot(t, owner).State.MachineStatuses[issueKey]
	if committed.Status != "clear" || committed.SourceID != "new-clear" || committed.Sequence != 1 || committed.Attempt != manifest.Attempt {
		t.Fatalf("stale supervisor changed status: %#v", committed)
	}
}

func TestSupervisorStatusAdmissionFailsTerminallyWithoutFreshObservation(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 336, 1, "running")
	state := runtimeEffectInitialState(manifest)
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	snapshot := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey("o/r", 336), ownerAttemptKey("o/r", 336, 1)
	agent := proposalTestSupervisor(t)
	writeProposalV2(t, agent, orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 336, Attempt: 1, Action: orchestratoragent.ProposalActionStatusSet, RequestID: "status-336-1", Detail: "monitoring: awaiting observation", IssueGeneration: snapshot.State.IssueGenerations[issueKey], AttemptGeneration: snapshot.State.AttemptGenerations[attemptKey], OwnerCausalityToken: ownerAttemptCausalityToken(snapshot.State, "o/r", 336, 1)})
	operator := operatorTestMutationService(t, owner)
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	service := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: operator.effects, operator: operator, trigger: trigger, capacity: 1}
	if err := service.process(t.Context()); err == nil {
		t.Fatal("status without a fresh observation unexpectedly succeeded")
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "failed" || status.ResolvedBinding == "" {
		t.Fatalf("durably admitted proposal was not terminally failed: %#v", status)
	}
	if status := mustOwnerSnapshot(t, owner).State.MachineStatuses[issueKey]; status.Status != "needs-attention" || status.Sequence != 1 {
		t.Fatalf("owner status=%#v", status)
	}
}

func TestSupervisorStatusExecutionFailureIsTerminal(t *testing.T) {
	owner, _ := operatorTestOwner(t, 337, "active", false)
	snapshot := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey("o/r", 337), ownerAttemptKey("o/r", 337, 1)
	agent := proposalTestSupervisor(t)
	writeProposalV2(t, agent, orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 337, Attempt: 1, Action: orchestratoragent.ProposalActionStatusSet, RequestID: "status-337-1", Detail: "monitoring: needs operator", IssueGeneration: snapshot.State.IssueGenerations[issueKey], AttemptGeneration: snapshot.State.AttemptGenerations[attemptKey], OwnerCausalityToken: ownerAttemptCausalityToken(snapshot.State, "o/r", 337, 1)})
	operator := operatorTestMutationService(t, owner)
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	service := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: operator.effects, operator: operator, trigger: trigger, capacity: 1}
	if err := service.process(t.Context()); err == nil {
		t.Fatal("failed GitHub execution unexpectedly succeeded")
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "failed" {
		t.Fatalf("execution failure was not terminal: %#v", status)
	}
}

func TestSupervisorStatusProposalRejectsSameGenerationLifecycleDrift(t *testing.T) {
	owner, _ := operatorTestOwner(t, 338, "active", false)
	before := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey("o/r", 338), ownerAttemptKey("o/r", 338, 1)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 338, Attempt: 1, Action: orchestratoragent.ProposalActionStatusSet, RequestID: "status-338-1", Detail: "monitoring: stale lifecycle", IssueGeneration: before.State.IssueGenerations[issueKey], AttemptGeneration: before.State.AttemptGenerations[attemptKey], OwnerCausalityToken: ownerAttemptCausalityToken(before.State, "o/r", 338, 1)}
	observation := before.State.Observations[issueKey]
	issue := expandIssueFact(observation.Fact)
	issue.Title += " changed"
	var attempts []internalgithub.RecoveryAttemptFact
	for _, attempt := range observation.Attempts {
		attempts = append(attempts, expandAttemptFact(attempt.Fact))
	}
	applyReconciliationInput(t, owner, reconciliationInput{Scope: issueScope(338), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: attempts})
	after := mustOwnerSnapshot(t, owner)
	if after.State.IssueGenerations[issueKey] != proposal.IssueGeneration || after.State.AttemptGenerations[attemptKey] != proposal.AttemptGeneration || after.State.MachineStatuses[issueKey].Sequence != proposal.MachineStatusSequence || ownerAttemptCausalityToken(after.State, "o/r", 338, 1) == proposal.OwnerCausalityToken {
		t.Fatalf("test did not create same-generation lifecycle drift")
	}
	if _, err := owner.admitMachineStatus(t.Context(), admitMachineStatusCommand{Repository: "o/r", Issue: 338, Attempt: 1, ExpectedIssueGeneration: proposal.IssueGeneration, ExpectedAttemptGeneration: proposal.AttemptGeneration, ExpectedStatusSequence: proposal.MachineStatusSequence, ExpectedCausalityToken: proposal.OwnerCausalityToken, Source: "orchestrator", SourceID: strings.Repeat("a", 64), Status: "needs-attention", Reason: proposal.Detail}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("owner accepted same-generation stale causality: %v", err)
	}
	agent := proposalTestSupervisor(t)
	writeProposalV2(t, agent, proposal)
	operator := operatorTestMutationService(t, owner)
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	service := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: operator.effects, operator: operator, trigger: trigger, capacity: 1}
	if err := service.process(t.Context()); err != nil {
		t.Fatal(err)
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "refused" {
		t.Fatalf("same-generation stale proposal status=%#v", status)
	}
}

func TestSupervisorRecoverWaitsForDurableReceipt(t *testing.T) {
	owner, manifest := operatorNeverLaunchedOwner(t, 373, "failed", "failed", func(runtimeOwnerState) error { return nil })
	snapshot, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	issueKey := ownerIssueKey(manifest.Repository, manifest.Issue)
	observation := snapshot.State.Observations[issueKey]
	observation.Fact.NeedsAttention = true
	collection := reconciliationCollection{Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, CycleID: snapshot.CycleID}, Scope: issueScope(manifest.Issue), Complete: true, IssueGenerations: map[string]uint64{issueKey: snapshot.State.IssueGenerations[issueKey]}, AttemptGenerations: map[string]uint64{ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt): snapshot.State.AttemptGenerations[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)]}, Issues: []reconciliationIssueGroup{{Fact: observation.Fact, Attempts: []reconciliationAttemptFact{observation.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact}, IssueUpdates: observation.IssueUpdates}}}
	if _, err := owner.applyReconciliation(t.Context(), collection); err != nil {
		t.Fatal(err)
	}

	service := operatorTestMutationService(t, owner)
	service.collector.Config.ActorID = 42
	service.collector.Config.RetryCommand = "/agent-symphony retry"
	var collections int
	service.collect = func(_ context.Context, _ stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		collections++
		failed := internalgithub.RecoveryAttemptFact{Repository: manifest.Repository, Issue: issue, Attempt: manifest.Attempt, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}}
		fact := issueFact(issue, "recover")
		fact.Attempt, fact.CurrentAttempt, fact.RecoveryAttempt, fact.NeedsAttention, fact.RecoveryAuthorized = manifest.Attempt, manifest.Attempt, manifest.Attempt, true, true
		fact.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
		return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{fact}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}}, nil
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	jsonResponse := func(request *http.Request, value any) *http.Response {
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), Request: request}
	}
	service.collector.API = internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.Path {
		case "/user":
			return jsonResponse(request, map[string]any{"id": 42, "login": "owner"}), nil
		case "/repos/o/r/pulls":
			return jsonResponse(request, []any{}), nil
		case "/repos/o/r":
			return jsonResponse(request, map[string]any{"full_name": "o/r", "default_branch": "main", "permissions": map[string]any{"pull": true}}), nil
		case "/repos/o/r/branches/main":
			return jsonResponse(request, map[string]any{"commit": map[string]any{"sha": manifest.BaseSHA}}), nil
		case "/repos/o/r/issues/373":
			return jsonResponse(request, map[string]any{"number": 373, "node_id": "I_373", "title": "recover", "body": "body", "state": "open", "created_at": "2026-09-14T00:00:00Z", "user": map[string]any{"id": 42}, "labels": []any{}}), nil
		case "/repos/o/r/issues/373/timeline":
			return jsonResponse(request, []any{}), nil
		}
		if !strings.Contains(request.URL.Path, "/comments") {
			return nil, fmt.Errorf("unexpected GitHub request %s", request.URL.String())
		}
		if first {
			first = false
			close(entered)
			select {
			case <-release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		}
		current, err := owner.snapshot(request.Context())
		if err != nil {
			return nil, err
		}
		failedAt := current.State.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Manifest.UpdatedAt
		marker, err := internalgithub.TerminalFailureMarker(manifest.Issue, manifest.Attempt, failedAt)
		if err != nil {
			return nil, err
		}
		comments := []map[string]any{{"id": 1, "body": marker, "created_at": failedAt, "updated_at": failedAt, "user": map[string]any{"id": 42}}, {"id": 2, "body": "/agent-symphony retry", "created_at": failedAt.Add(time.Second), "updated_at": failedAt.Add(time.Second), "user": map[string]any{"id": 42}}}
		body, _ := json.Marshal(comments)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
	})}}

	agent := proposalTestSupervisor(t)
	projection, err := projectOwnerStatus(mustOwnerSnapshot(t, owner), 1, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agent.Observe(t.Context(), projection.Statuses); err != nil {
		t.Fatal(err)
	}
	var handoff struct {
		ID string `json:"id"`
	}
	body, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.AttentionHandoffFile))
	if err != nil || json.Unmarshal(body, &handoff) != nil || handoff.ID == "" {
		t.Fatalf("handoff=%s err=%v statuses=%#v", body, err, projection.Statuses)
	}
	writeProposalV2(t, agent, orchestratoragent.MessageProposal{Version: 1, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Action: orchestratoragent.ProposalActionRecover, RequestID: "recover-373-1", HandoffID: handoff.ID})
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = trigger.shutdown(t.Context()) })
	proposalService := &supervisorProposalServiceV2{agent: agent, owner: owner, effects: service.effects, operator: service, trigger: trigger, capacity: 1}
	done := make(chan error, 1)
	go func() { done <- proposalService.process(t.Context()) }()
	<-entered
	if status := readProposalStatusV2(t, agent); status.Resolution != "running" {
		t.Fatalf("blocked recover status=%#v", status)
	}
	if receipt, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, "recover-373-1"); !ok || receipt.State != "pending" {
		t.Fatalf("blocked recover receipt=%#v exists=%v", receipt, ok)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if status := readProposalStatusV2(t, agent); status.Resolution != "succeeded" {
		t.Fatalf("completed recover status=%#v", status)
	}
	if receipt, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, "recover-373-1"); !ok || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusOK {
		t.Fatalf("completed recover receipt=%#v exists=%v", receipt, ok)
	}
}

func retryProposalOwner(t *testing.T, issue int) (*stateOwner, agentruntime.Manifest) {
	t.Helper()
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, issue, 1, "completed")
	manifest.ReviewState, manifest.ReviewHead = "clean", strings.Repeat("c", 40)
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, issue), ownerAttemptKey(manifest.Repository, issue, 1)
	observation := state.Observations[issueKey]
	accepted := observation.Attempts[attemptKey]
	accepted.Fact.HeadSHA, accepted.Fact.PR, accepted.Fact.PublicationConfirmed = manifest.ReviewHead, 7, true
	observation.Attempts[attemptKey] = accepted
	active := accepted.Fact
	observation.Fact.ActiveAttempt = &active
	state.Observations[issueKey] = observation
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(t.Context()) })
	refreshOperatorObservation(t, owner)
	snapshot := mustOwnerSnapshot(t, owner)
	projected, err := projectOwnerStatus(snapshot, 1, time.Now().UTC())
	if err != nil || validateTransitionRetry(orchestratoragent.MessageProposal{Repository: manifest.Repository, Issue: issue, Attempt: 1}, projected.Statuses) != nil {
		t.Fatalf("retry fixture statuses=%#v err=%v", projected.Statuses, err)
	}
	return owner, manifest
}

func proposalTestSupervisor(t *testing.T) *orchestratoragent.Supervisor {
	t.Helper()
	root := t.TempDir()
	agent := &orchestratoragent.Supervisor{Root: root, Workspace: filepath.Join(root, "orchestrator-o-r"), Repository: "o/r", Command: []string{"agent"}, ProposalCommand: []string{"proposal"}, ProposalStatusCommand: []string{"proposal-status"}, Runner: &orchestratorTestRunner{}}
	if _, err := agent.Observe(t.Context(), []orchestrator.RecoveryStatus{}); err != nil {
		t.Fatal(err)
	}
	return agent
}

func writeProposalV2(t *testing.T, agent *orchestratoragent.Supervisor, proposal orchestratoragent.MessageProposal) {
	t.Helper()
	body, err := json.Marshal(proposal)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalFile), body, 0o620); err != nil {
		t.Fatal(err)
	}
}

func readProposalStatusV2(t *testing.T, agent *orchestratoragent.Supervisor) orchestratoragent.MessageProposalStatus {
	t.Helper()
	body, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalStatusFile))
	if err != nil {
		t.Fatal(err)
	}
	var status orchestratoragent.MessageProposalStatus
	if err := json.Unmarshal(body, &status); err != nil {
		t.Fatal(err)
	}
	return status
}
