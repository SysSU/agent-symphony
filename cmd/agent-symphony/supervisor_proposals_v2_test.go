package main

import (
	"bytes"
	"context"
	"encoding/json"
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
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Action: orchestratoragent.ProposalActionRetry, RequestID: "retry-371-1"}
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

func TestSupervisorRecoverWaitsForTerminalDurableReceipt(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 373, "active", false)
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
		active := internalgithub.RecoveryAttemptFact{Repository: manifest.Repository, Issue: issue, Attempt: manifest.Attempt, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
		fact := issueFact(issue, "recover")
		fact.Attempt, fact.CurrentAttempt, fact.NeedsAttention = manifest.Attempt, manifest.Attempt, true
		if collections == 1 {
			fact.Active, fact.ActiveAttempt = true, &active
			return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{fact}, Attempts: []internalgithub.RecoveryAttemptFact{active}}}, nil
		}
		failed := active
		failed.State = "failed"
		fact.RecoveryAuthorized, fact.TerminalAttempts = true, []internalgithub.RecoveryAttemptFact{failed}
		return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{fact}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}}, nil
	}
	entered, release := make(chan struct{}), make(chan struct{})
	first := true
	service.collector.API = internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
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
