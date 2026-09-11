package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestV2DashboardDismissesWhileReconciliationCollectsAndIgnoresStaleFiles(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 320, "completed", true)
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}

	if err := writeDashboardStatusSnapshot(owner.stateRoot, dashboardStatusSnapshot{UpdatedAt: time.Unix(1, 0), Statuses: nil}); err != nil {
		t.Fatal(err)
	}
	if err := server.writeState(dashboardState{Version: dashboardStateVersion, Hidden: nil}); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	runner := reconciliationRunner{owner: owner, collect: func(_ context.Context, snapshot stateOwnerSnapshot) (reconciliationInput, error) {
		close(entered)
		<-release
		issue := expandIssueFact(snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
		return repositoryInput(true, issue), nil
	}}
	reconciled := make(chan error, 1)
	go func() { _, err := runner.run(t.Context()); reconciled <- err }()
	<-entered

	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=320&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result controlResult
	if json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.OK || result.OwnerRevision == 0 {
		t.Fatalf("result=%#v body=%s", result, response.Body.String())
	}
	once.Do(func() { close(release) })
	if err := <-reconciled; err != nil && !errors.Is(err, errStaleStateResult) {
		t.Fatal(err)
	}

	stateRequest := httptest.NewRequest(http.MethodGet, "http://localhost/dashboard-state.json", nil)
	stateRequest.Host = "localhost"
	stateResponse := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(stateResponse, stateRequest)
	var state dashboardState
	if stateResponse.Code != http.StatusOK || json.Unmarshal(stateResponse.Body.Bytes(), &state) != nil || state.OwnerRevision < result.OwnerRevision || len(state.Hidden) != 1 || state.Hidden[0].Reason != "dismissed" {
		t.Fatalf("status=%d state=%#v body=%s", stateResponse.Code, state, stateResponse.Body.String())
	}
	committed := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if _, exists := committed.Attempts[key]; exists || committed.Tombstones[key].Action != "dismissed" {
		t.Fatalf("stale reconciliation restored the attempt: %#v", committed)
	}
}

func TestV2DashboardCancelRespondsWhileReconciliationCollectsAndRejectsStaleResult(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 327, "active", false)
	service := operatorTestMutationService(t, owner)
	service.effects.stopped = true
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	runner := reconciliationRunner{owner: owner, collect: func(_ context.Context, snapshot stateOwnerSnapshot) (reconciliationInput, error) {
		close(entered)
		<-release
		issue := expandIssueFact(snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
		return repositoryInput(true, issue), nil
	}}
	reconciled := make(chan error, 1)
	go func() { _, err := runner.run(t.Context()); reconciled <- err }()
	<-entered

	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/cancel?repository=o%2Fr&issue=327&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var result controlResult
	if json.Unmarshal(response.Body.Bytes(), &result) != nil || !result.OK || result.OwnerRevision == 0 {
		t.Fatalf("result=%#v body=%s", result, response.Body.String())
	}
	once.Do(func() { close(release) })
	if err := <-reconciled; err != nil && !errors.Is(err, errStaleStateResult) {
		t.Fatalf("reconciliation err=%v", err)
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if state.AttemptGenerations[key] != 2 || state.Attempts[key].Generation != 2 || len(state.Effects) != 1 || state.ControlReceipts[0].Phase != operatorPhaseStopPending {
		t.Fatalf("cancel was overwritten: %#v", state)
	}
}

func TestV2ControlSocketCommitsOwnerReceiptWithoutOperationMutex(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 321, "completed", true)
	if err := bindDeployment(owner.stateRoot, manifest.Repository); err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	if err := startControlServer(ctx, server, os.Stderr); err != nil {
		t.Fatal(err)
	}
	request := operatorRequest("socket-dismiss", "dismiss", manifest, false)
	result, err := callRunningDaemon(t.Context(), owner.stateRoot, request)
	if err != nil || !result.OK || result.OwnerRevision == 0 {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	if !ok || receipt.State != "completed" || state.Tombstones[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Action != "dismissed" {
		t.Fatalf("state=%#v receipt=%#v", state, receipt)
	}
}

func TestV2ControlCLIReportsAcceptedPendingPhase(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 326, "active", false)
	if err := bindDeployment(owner.stateRoot, manifest.Repository); err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	service.effects.stopped = true
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	if err := startControlServer(ctx, server, os.Stderr); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	code := run([]string{"control", "--repository", "o/r", "--action", "cancel", "--issue", "326", "--attempt", "1", "--runtime-state", owner.stateRoot, "--request-id", "cli-pending-cancel"}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 || stdout.String() != "cancel accepted for o/r#326 attempt 1 (phase stop-pending)\n" {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestV2ControlSocketDeadlineCancelsBlockedAdmissionBeforeIntent(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 324, "completed", true)
	if err := bindDeployment(owner.stateRoot, manifest.Repository); err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	var calls int
	service.issueClosed = func(ctx context.Context, _ string, _ int) (bool, error) {
		calls++
		return true, nil
	}
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	serverContext, stop := context.WithCancel(t.Context())
	t.Cleanup(stop)
	if err := startControlServer(serverContext, server, os.Stderr); err != nil {
		t.Fatal(err)
	}
	request := operatorRequest("socket-deadline", "dismiss", manifest, false)
	body, _ := json.Marshal(request)
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", controlSocketPath(owner.stateRoot))
	}}
	t.Cleanup(transport.CloseIdleConnections)
	httpRequest, _ := http.NewRequest(http.MethodPost, "http://unix/v1/action", bytes.NewReader(body))
	httpRequest.Header.Set(controlDeadline, strconv.FormatInt(time.Unix(1, 0).UnixNano(), 10))
	response, err := (&http.Client{Transport: transport}).Do(httpRequest)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	var result controlResult
	if json.NewDecoder(response.Body).Decode(&result) != nil || result.Status != http.StatusRequestTimeout || result.OK {
		t.Fatalf("result=%#v", result)
	}
	state := mustOwnerSnapshot(t, owner).State
	if len(state.ControlReceipts) != 0 || len(state.Effects) != 0 || len(state.Tombstones) != 0 || len(state.Attempts) != 1 {
		t.Fatalf("deadline committed state: %#v", state)
	}
	if calls != 0 {
		t.Fatalf("expired request reached admission I/O %d times", calls)
	}
}

func TestOperatorServiceCancellationDuringBlockedAdmissionCommitsNothing(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 325, "completed", true)
	service := operatorTestMutationService(t, owner)
	entered := make(chan struct{})
	service.issueClosed = func(ctx context.Context, _ string, _ int) (bool, error) {
		close(entered)
		<-ctx.Done()
		return false, ctx.Err()
	}
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan controlResult, 1)
	go func() {
		result <- service.perform(ctx, operatorRequest("blocked-admission-cancel", "dismiss", manifest, false))
	}()
	<-entered
	cancel()
	got := <-result
	if got.Status != http.StatusRequestTimeout || got.OK {
		t.Fatalf("result=%#v", got)
	}
	state := mustOwnerSnapshot(t, owner).State
	if len(state.ControlReceipts) != 0 || len(state.Effects) != 0 || len(state.Tombstones) != 0 || len(state.Attempts) != 1 {
		t.Fatalf("cancelled admission committed state: %#v", state)
	}
}

func TestOperatorStopDigestBindsExactAttemptIdentity(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 328, "active", false)
	service := operatorTestMutationService(t, owner)
	request, err := service.prepareStop(manifest, "operator cancelled attempt")
	if err != nil {
		t.Fatal(err)
	}
	want := operatorEffectAttempt(manifest)
	if !reflect.DeepEqual(request.Attempt, want) {
		t.Fatalf("attempt=%#v want=%#v", request.Attempt, want)
	}
	digest, err := agentruntime.EffectRequestDigest(request)
	if err != nil || digest != request.Identity.RequestDigest {
		t.Fatalf("digest=%q identity=%q err=%v", digest, request.Identity.RequestDigest, err)
	}
	request.Attempt.BaseSHA = strings.Repeat("b", 40)
	changed, err := agentruntime.EffectRequestDigest(request)
	if err != nil || changed == digest {
		t.Fatalf("attempt identity was not digest-bound: changed=%q err=%v", changed, err)
	}
}

func TestOperatorBlockedRecoverSelfAdvancesToRetryCompletion(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 329, 1, "running")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	completed := make(chan struct{})
	var completion sync.Once
	persist := func(state runtimeOwnerState) error {
		if receipt, ok := operatorReceiptByID(state, "recover-self-advance"); ok && receipt.State == "completed" {
			completion.Do(func() { close(completed) })
		}
		return nil
	}
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	service := operatorTestMutationService(t, owner)
	service.collector.Config.ActorID = 42
	service.collector.Config.RetryCommand = "/agent-symphony retry"
	var collections atomic.Int32
	service.collect = func(_ context.Context, _ stateOwnerSnapshot, issueNumber int) (reconciliationV2Batch, error) {
		active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: issueNumber, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
		issue := issueFact(issueNumber, "recover")
		issue.Attempt, issue.CurrentAttempt = 1, 1
		if collections.Add(1) == 1 {
			issue.Active, issue.ActiveAttempt = true, &active
			return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issueNumber), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{active}}}, nil
		}
		failed := active
		failed.State = "failed"
		issue.RecoveryAuthorized = true
		issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
		return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issueNumber), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}}, nil
	}
	service.collector.API = internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if !strings.Contains(request.URL.Path, "/comments") {
			return nil, fmt.Errorf("unexpected GitHub request %s", request.URL.String())
		}
		current, snapshotErr := owner.snapshot(request.Context())
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		failedAt := current.State.Attempts[ownerAttemptKey("o/r", 329, 1)].Manifest.UpdatedAt
		marker, markerErr := internalgithub.TerminalFailureMarker(329, 1, failedAt)
		if markerErr != nil {
			return nil, markerErr
		}
		comments := []map[string]any{
			{"id": 1, "body": marker, "created_at": failedAt, "updated_at": failedAt, "user": map[string]any{"id": 42}},
			{"id": 2, "body": "/agent-symphony retry", "created_at": failedAt.Add(time.Second), "updated_at": failedAt.Add(time.Second), "user": map[string]any{"id": 42}},
		}
		body, _ := json.Marshal(comments)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
	})}}
	result := service.perform(t.Context(), operatorRequest("recover-self-advance", "recover", manifest, false))
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("result=%#v", result)
	}
	<-completed
	final := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(final, "recover-self-advance")
	if !ok || receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || receipt.Result == nil || receipt.Result.Status != http.StatusOK || collections.Load() != 2 {
		t.Fatalf("receipt=%#v collections=%d effects=%#v", receipt, collections.Load(), final.Effects)
	}
}

func TestOperatorServiceNewRecoveryRequestAttachesAcrossDurablePhases(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 342, "active", false)
	snapshot := mustOwnerSnapshot(t, owner)
	request := operatorRequest("recover-original", "recover", manifest, false)
	command := operatorCommand(snapshot, request, manifest)
	command.LivenessFailed = true
	command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "dashboard recovery: runtime liveness mismatch", RequestDigest: strings.Repeat("d", 64)}
	_, stopEffect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, cancel := context.WithCancel(t.Context())
	cancel()
	service := operatorTestMutationService(t, owner)
	service.lifecycle, service.effects.lifecycle = lifecycle, lifecycle
	assertAttached := func(id, phase string) {
		t.Helper()
		result := service.perform(t.Context(), operatorRequest(id, "recover", manifest, false))
		if !result.OK || result.Status != http.StatusAccepted {
			t.Fatalf("phase=%s result=%#v", phase, result)
		}
		state := mustOwnerSnapshot(t, owner).State
		receipt, ok := operatorReceiptByID(state, id)
		if !ok || receipt.Phase != phase {
			t.Fatalf("phase=%s receipt=%#v", phase, receipt)
		}
	}
	assertAttached("recover-stop-attach", operatorPhaseStopPending)
	cancelled := manifest
	cancelled.State, cancelled.Diagnostic, cancelled.UpdatedAt = "cancelled", command.Runtime.Reason, time.Unix(60, 0).UTC()
	if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stopEffect)), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
		t.Fatal(err)
	}
	manifest = cancelled
	assertAttached("recover-terminal-await-attach", operatorPhaseTerminalAwait)
	active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 342, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
	issue := issueFact(342, "recover")
	issue.Attempt, issue.CurrentAttempt, issue.Active, issue.ActiveAttempt = 1, 1, true, &active
	terminal := applyAndPlanRecover(t, owner, repositoryInput(true, issue), []internalgithub.RecoveryAttemptFact{active}, githubIssueTerminalFailure)
	terminalEffect := advanceOperatorRecoverPlan(t, owner, request.RequestID, terminal)
	assertAttached("recover-terminal-pending-attach", operatorPhaseTerminal)
	terminalResult := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueTerminalFailure, Observed: true}}
	if _, err := owner.finishOperatorReconciliationEffect(t.Context(), finishOperatorReconciliationEffectCommand{Finish: finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*terminalEffect), Result: terminalResult}}); err != nil {
		t.Fatal(err)
	}
	assertAttached("recover-retry-await-attach", operatorPhaseRetryAwait)
	failed := active
	failed.State = "failed"
	failedIssue := issueFact(342, "recover")
	failedIssue.Attempt, failedIssue.CurrentAttempt, failedIssue.RecoveryAuthorized = 1, 1, true
	failedIssue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
	retry := applyAndPlanRecover(t, owner, repositoryInput(true, failedIssue), []internalgithub.RecoveryAttemptFact{failed}, githubIssueRetry)
	retryEffect := advanceOperatorRecoverPlan(t, owner, request.RequestID, retry)
	assertAttached("recover-retry-pending-attach", operatorPhaseRetryPending)
	state := mustOwnerSnapshot(t, owner).State
	if len(state.ControlReceipts) != 6 {
		t.Fatalf("receipts=%#v", state.ControlReceipts)
	}
	for _, receipt := range state.ControlReceipts {
		if receipt.State != "pending" || receipt.Phase != operatorPhaseRetryPending || receipt.EffectID != retryEffect.ID {
			t.Fatalf("receipt did not converge on workflow: %#v", receipt)
		}
	}
}

func TestOperatorRestartRetriesUnmarkedTerminalEffectOnlyFromMatchingFreshInput(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 343, "active", false)
	snapshot := mustOwnerSnapshot(t, owner)
	request := operatorRequest("recover-unmarked-restart", "recover", manifest, false)
	command := operatorCommand(snapshot, request, manifest)
	command.LivenessFailed = true
	command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "dashboard recovery: runtime liveness mismatch", RequestDigest: strings.Repeat("e", 64)}
	_, stopEffect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	cancelled := manifest
	cancelled.State, cancelled.Diagnostic, cancelled.UpdatedAt = "cancelled", command.Runtime.Reason, time.Unix(70, 0).UTC()
	if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stopEffect)), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
		t.Fatal(err)
	}
	active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 343, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
	activeIssue := issueFact(343, "recover")
	activeIssue.Attempt, activeIssue.CurrentAttempt, activeIssue.Active, activeIssue.ActiveAttempt = 1, 1, true, &active
	activeInput := repositoryInput(true, activeIssue)
	activeInput.Attempts = []internalgithub.RecoveryAttemptFact{active}
	terminal := applyAndPlanRecover(t, owner, activeInput, []internalgithub.RecoveryAttemptFact{active}, githubIssueTerminalFailure)
	if _, _, err := owner.advanceOperatorRecovery(t.Context(), advanceOperatorRecoveryCommand{RequestID: request.RequestID, Identity: terminal.Identity, Reconciliation: beginReconciliationEffectCommand{Identity: terminal.Identity, Request: terminal.Request}}); err != nil {
		t.Fatal(err)
	}
	before := mustOwnerSnapshot(t, owner)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	completed := make(chan struct{})
	var completion sync.Once
	restarted, err := startTestStateOwner(t, owner.stateRoot, before.State, func(state runtimeOwnerState) error {
		if receipt, ok := operatorReceiptByID(state, request.RequestID); ok && receipt.State == "completed" {
			completion.Do(func() { close(completed) })
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	service := operatorTestMutationService(t, restarted)
	service.collector.Config.ActorID = 42
	service.collector.Config.RetryCommand = "/agent-symphony retry"
	tampered := activeInput
	tampered.Issues = slices.Clone(activeInput.Issues)
	tampered.Issues[0].Body = "changed after durable authorization"
	service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		return reconciliationV2Batch{Input: tampered}, nil
	}
	if err := service.resumeReceipt(t.Context(), request.RequestID); !errors.Is(err, errStateConflict) {
		t.Fatalf("tampered recovery err=%v", err)
	}
	stillPending, _ := operatorReceiptByID(mustOwnerSnapshot(t, restarted).State, request.RequestID)
	if stillPending.State != "pending" || stillPending.Phase != operatorPhaseTerminal {
		t.Fatalf("tampered recovery changed receipt: %#v", stillPending)
	}
	var collections atomic.Int32
	service.collect = func(_ context.Context, _ stateOwnerSnapshot, _ int) (reconciliationV2Batch, error) {
		if collections.Add(1) == 1 {
			return reconciliationV2Batch{Input: activeInput}, nil
		}
		failed := active
		failed.State = "failed"
		failedIssue := issueFact(343, "recover")
		failedIssue.Attempt, failedIssue.CurrentAttempt, failedIssue.RecoveryAuthorized = 1, 1, true
		failedIssue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
		return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(343), Complete: true, Issues: []internalgithub.RecoveryIssueFact{failedIssue}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}}, nil
	}
	service.collector.API = operatorRecoveryProofAPI(t, restarted, 343)
	if err := service.resumeReceipt(t.Context(), request.RequestID); err != nil {
		t.Fatal(err)
	}
	<-completed
	final := mustOwnerSnapshot(t, restarted).State
	receipt, _ := operatorReceiptByID(final, request.RequestID)
	if receipt.State != "completed" || collections.Load() != 2 {
		t.Fatalf("receipt=%#v collections=%d effects=%#v", receipt, collections.Load(), final.Effects)
	}
}

func operatorRecoveryProofAPI(t *testing.T, owner *stateOwner, issue int) internalgithub.API {
	t.Helper()
	return internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if !strings.Contains(request.URL.Path, "/comments") {
			return nil, fmt.Errorf("unexpected GitHub request %s", request.URL.String())
		}
		current, err := owner.snapshot(request.Context())
		if err != nil {
			return nil, err
		}
		failedAt := current.State.Attempts[ownerAttemptKey("o/r", issue, 1)].Manifest.UpdatedAt
		marker, err := internalgithub.TerminalFailureMarker(issue, 1, failedAt)
		if err != nil {
			return nil, err
		}
		comments := []map[string]any{
			{"id": 1, "body": marker, "created_at": failedAt, "updated_at": failedAt, "user": map[string]any{"id": 42}},
			{"id": 2, "body": "/agent-symphony retry", "created_at": failedAt.Add(time.Second), "updated_at": failedAt.Add(time.Second), "user": map[string]any{"id": 42}},
		}
		body, _ := json.Marshal(comments)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
	})}}
}

func TestOperatorServiceReplaysCleanupFromDurableTombstoneWithoutPreflight(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 322, "completed", false)
	command := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("archive-first", "archive", manifest, true), manifest)
	command.CleanupDigest = strings.Repeat("a", 64)
	committed, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	for _, requestID := range []string{"archive-started", "archive-completed"} {
		if requestID == "archive-completed" {
			if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectCleanup, Manifest: manifest}}); err != nil {
				t.Fatal(err)
			}
		}
		request := operatorRequest(requestID, "archive", manifest, true)
		result := service.perform(t.Context(), request)
		if !result.OK || result.Status != map[bool]int{true: http.StatusOK, false: http.StatusAccepted}[requestID == "archive-completed"] {
			t.Fatalf("request=%s result=%#v", requestID, result)
		}
	}
	final := mustOwnerSnapshot(t, owner).State
	if final.Revision != committed.State.Revision+3 || len(final.Effects) != 1 || len(final.ControlReceipts) != 3 {
		got := final
		t.Fatalf("state=%#v", got)
	}
	conflict := service.perform(t.Context(), operatorRequest("remove-conflict", "remove", manifest, true))
	if conflict.Status != http.StatusConflict {
		t.Fatalf("conflict=%#v", conflict)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, final, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	restartService := operatorTestMutationService(t, restarted)
	replayed := restartService.perform(t.Context(), operatorRequest("archive-restarted", "archive", manifest, true))
	if !replayed.OK || replayed.Status != http.StatusOK {
		t.Fatalf("restart replay=%#v", replayed)
	}
}

func operatorTestMutationService(t *testing.T, owner *stateOwner) *operatorMutationService {
	t.Helper()
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: &barrierEffectRunner{}, VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}}
	return &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}}
}

func TestV2DashboardConstructionRejectsWrongOwnerBinding(t *testing.T) {
	owner, _ := operatorTestOwner(t, 323, "completed", true)
	service := operatorTestMutationService(t, owner)
	if _, err := newProjectDashboardServerV2(t.Context(), filepath.Join(owner.stateRoot, "wrong"), "o/r", nil, "tmux", service, false, ""); err == nil {
		t.Fatal("dashboard accepted a mismatched owner root")
	}
	runtimeState := service.effects.executor.Runtime
	coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}}
	cleanup := operatorCleanupExecutor{stateRoot: owner.stateRoot, implementation: absentSessionBoundary{}, reviewer: absentSessionBoundary{}, runtime: runtimeState}
	collector := reconciliationV2Collector{API: internalgithub.API{HTTP: http.DefaultClient}, Config: internalgithub.PRAdapterConfig{Repository: "wrong/repository", ActorID: 1}}
	if _, err := newOperatorMutationService(t.Context(), owner, coordinator, cleanup, collector, absentSessionBoundary{}, "source", nil, []string{"review"}); err == nil || !strings.Contains(err.Error(), "repository") {
		t.Fatalf("constructor repository mismatch err=%v", err)
	}
}

func TestOperatorCleanupExecutesExactArchiveAbandonAndRemovePolicies(t *testing.T) {
	for _, action := range []string{"archive", "abandon", "remove"} {
		t.Run(action, func(t *testing.T) {
			owner, manifest := operatorTestOwner(t, 330, "completed", false)
			snapshotRoot := productionSnapshotRoot(owner.stateRoot)
			expectedSnapshot, _ := reviewIdentity(operatorEffectAttempt(manifest), snapshotRoot)
			resultRoot := expectedSnapshot + ".result-0123456789abcdef"
			for _, path := range []string{expectedSnapshot, resultRoot} {
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			implementation := &operatorBoundaryRecorder{}
			reviewer := &operatorBoundaryRecorder{sessionLive: true}
			runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: &barrierEffectRunner{}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
			executor := operatorCleanupExecutor{stateRoot: owner.stateRoot, implementation: implementation, reviewer: reviewer, runtime: runtimeState}
			request := agentruntime.EffectRequest{Action: agentruntime.EffectCleanup, Attempt: operatorEffectAttempt(manifest), Manifest: manifest, Cleanup: agentruntime.EffectCleanupPolicy{Action: action}}
			if action == "remove" {
				request.Cleanup.PublishedHead = manifest.BaseSHA
			}
			var err error
			if request, err = executor.bindPolicy(request); err != nil {
				t.Fatal(err)
			}
			if !request.Cleanup.CompatibilityManifestSeen || !request.Cleanup.CompatibilityLogSeen {
				t.Fatalf("compatibility policy=%#v", request.Cleanup)
			}
			if err := executor.validate(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			if err := executor.execute(t.Context(), request); err != nil {
				t.Fatal(err)
			}
			complete, err := executor.verify(t.Context(), request)
			if err != nil || !complete {
				t.Fatalf("complete=%v err=%v", complete, err)
			}
			for _, path := range []string{expectedSnapshot, resultRoot} {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("review resource remains %s: %v", path, err)
				}
			}
			manifestPath := filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")
			if action == "archive" {
				if _, err := os.Lstat(manifestPath); err != nil {
					t.Fatalf("archive removed compatibility manifest: %v", err)
				}
				if _, err := os.Lstat(manifest.LogPath); err != nil {
					t.Fatalf("archive removed compatibility log: %v", err)
				}
			} else if _, err := os.Lstat(filepath.Dir(manifest.LogPath)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("%s retained compatibility state: %v", action, err)
			}
			wantOperation := map[string]string{"archive": "cleanup", "abandon": "abandon", "remove": "remove"}[action]
			if !slices.Equal(implementation.operations(), []string{"validate-" + wantOperation, wantOperation}) || !slices.Equal(reviewer.operations(), []string{"run", "run"}) {
				t.Fatalf("implementation=%v reviewer=%v", implementation.operations(), reviewer.operations())
			}
		})
	}
}

func TestOperatorCleanupResumesAfterRestartFromPendingAndStartedPhases(t *testing.T) {
	for _, phase := range []string{operatorPhaseCleanupPending, operatorPhaseCleanupStarted} {
		t.Run(phase, func(t *testing.T) {
			owner, manifest := operatorTestOwner(t, 331, "completed", false)
			if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.MkdirAll(productionSnapshotRoot(owner.stateRoot), 0o700); err != nil {
				t.Fatal(err)
			}
			lifecycle, stop := context.WithCancel(t.Context())
			blocking := &operatorCleanupBoundary{path: manifest.Worktree, entered: make(chan struct{}), exited: make(chan struct{})}
			service := operatorServiceWithCleanup(t, owner, lifecycle, blocking)
			request := operatorRequest("archive-restart-"+phase, "archive", manifest, true)
			if phase == operatorPhaseCleanupPending {
				snapshot := mustOwnerSnapshot(t, owner)
				command, _, err := service.prepareAdmission(t.Context(), snapshot, request)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := owner.beginOperatorMutation(t.Context(), command); err != nil {
					t.Fatal(err)
				}
			} else {
				result := service.perform(t.Context(), request)
				if !result.OK || result.Status != http.StatusAccepted {
					t.Fatalf("result=%#v", result)
				}
				<-blocking.entered
				stop()
				<-blocking.exited
			}
			before := mustOwnerSnapshot(t, owner)
			receipt, _ := operatorReceiptByID(before.State, request.RequestID)
			if receipt.Phase != phase || receipt.State != "pending" {
				t.Fatalf("before restart receipt=%#v", receipt)
			}
			if _, err := os.Lstat(manifest.Worktree); err != nil {
				t.Fatalf("cleanup unexpectedly completed before restart: %v", err)
			}
			stop()
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			completed := make(chan struct{})
			var once sync.Once
			restarted, err := startTestStateOwner(t, owner.stateRoot, before.State, func(state runtimeOwnerState) error {
				if receipt, ok := operatorReceiptByID(state, request.RequestID); ok && receipt.State == "completed" {
					once.Do(func() { close(completed) })
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.close(context.Background()) })
			restartService := operatorServiceWithCleanup(t, restarted, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
			if err := restartService.resumeReceipt(t.Context(), request.RequestID); err != nil {
				t.Fatal(err)
			}
			<-completed
			final := mustOwnerSnapshot(t, restarted).State
			receipt, _ = operatorReceiptByID(final, request.RequestID)
			tombstone := final.Tombstones[ownerAttemptKey("o/r", 331, 1)]
			if receipt.State != "completed" || tombstone.CleanupPhase != "completed" || final.Effects[tombstone.EffectID].State != "completed" {
				t.Fatalf("receipt=%#v tombstone=%#v effects=%#v", receipt, tombstone, final.Effects)
			}
			if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("worktree remains after recovery: %v", err)
			}
			for _, path := range []string{manifest.LogPath, filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")} {
				if _, err := os.Lstat(path); err != nil {
					t.Fatalf("archive removed compatibility resource %s: %v", path, err)
				}
			}
		})
	}
}

func TestOperatorStartupSweepDispatchesPendingCleanupAndShutdownLeavesItResumable(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 332, "completed", false)
	for _, path := range []string{manifest.Worktree, productionSnapshotRoot(owner.stateRoot)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	request := operatorRequest("startup-cleanup", "archive", manifest, true)
	admissionService := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	command, _, err := admissionService.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), request)
	if err != nil {
		t.Fatal(err)
	}
	committed, _, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, committed.State, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	lifecycle, stop := context.WithCancel(t.Context())
	blocking := &operatorCleanupBoundary{path: manifest.Worktree, entered: make(chan struct{}), exited: make(chan struct{})}
	service := operatorServiceWithCleanup(t, restarted, lifecycle, blocking)
	if err := service.resumePending(t.Context()); err != nil {
		t.Fatal(err)
	}
	<-blocking.entered
	started := mustOwnerSnapshot(t, restarted).State
	receipt, _ := operatorReceiptByID(started, request.RequestID)
	if receipt.Phase != operatorPhaseCleanupStarted || receipt.State != "pending" {
		t.Fatalf("receipt=%#v", receipt)
	}
	stop()
	<-blocking.exited
	remaining := mustOwnerSnapshot(t, restarted).State
	if err := validateRuntimeOwnerState(remaining, restarted.attemptRoot, restarted.stateRoot, true); err != nil {
		t.Fatal(err)
	}
	receipt, _ = operatorReceiptByID(remaining, request.RequestID)
	if receipt.Phase != operatorPhaseCleanupStarted || receipt.State != "pending" {
		t.Fatalf("shutdown lost resumable receipt: %#v", receipt)
	}
	if _, err := os.Lstat(manifest.Worktree); err != nil {
		t.Fatalf("cancelled cleanup removed worktree: %v", err)
	}
}

func TestV2PlanReviewHandlerCommitsExactOwnerEffectWithoutCompatibilityWrite(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 333, "active", false)
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	beforeManifest, err := os.ReadFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: "o/r", operator: service}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/review-plan?repository=o%2Fr&issue=333&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	state := mustOwnerSnapshot(t, owner).State
	if len(state.ControlReceipts) != 1 || state.ControlReceipts[0].Phase != operatorPhaseReviewPending || len(state.Effects) != 1 {
		t.Fatalf("state=%#v", state)
	}
	effect := state.Effects[state.ControlReceipts[0].EffectID]
	if effect.Reconciliation == nil || effect.Reconciliation.Action != reconciliationReviewer || effect.Reconciliation.Reviewer == nil || effect.Reconciliation.Reviewer.Mode != agentruntime.ReviewModePlan {
		t.Fatalf("effect=%#v", effect)
	}
	afterManifest, err := os.ReadFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"))
	if err != nil || !bytes.Equal(afterManifest, beforeManifest) {
		t.Fatalf("compatibility manifest changed err=%v", err)
	}
}

func TestV2ConcurrentSameAttemptDismissHandlersConverge(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 334, "completed", true)
	service := operatorTestMutationService(t, owner)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	service.issueClosed = func(context.Context, string, int) (bool, error) {
		entered <- struct{}{}
		<-release
		return true, nil
	}
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: "o/r", operator: service}
	responses := make(chan *httptest.ResponseRecorder, 2)
	for range 2 {
		go func() {
			request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=334&attempt=1", nil)
			request.Host = "localhost"
			request.Header.Set("Origin", "http://localhost")
			response := httptest.NewRecorder()
			server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
			responses <- response
		}()
	}
	<-entered
	<-entered
	close(release)
	for range 2 {
		response := <-responses
		if response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)
	if len(state.ControlReceipts) != 2 || len(state.Tombstones) != 1 || state.AttemptGenerations[key] != 2 || len(state.Effects) != 0 {
		t.Fatalf("state=%#v", state)
	}
}

func TestV2ConcurrentDifferentAttemptHandlersCommitIndependently(t *testing.T) {
	root := resolvedTempDir(t)
	manifests := []agentruntime.Manifest{ownerTestManifest(t, root, 338, 1, "completed"), ownerTestManifest(t, root, 339, 1, "completed")}
	state := newRuntimeOwnerState("o/r")
	for _, manifest := range manifests {
		issueKey, attemptKey := ownerIssueKey("o/r", manifest.Issue), ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)
		state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
		state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
		addOperatorObservation(&state, manifest, "completed", true)
	}
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	service := operatorTestMutationService(t, owner)
	entered := make(chan struct{}, len(manifests))
	release := make(chan struct{})
	service.issueClosed = func(context.Context, string, int) (bool, error) {
		entered <- struct{}{}
		<-release
		return true, nil
	}
	server := &dashboardServer{ctx: t.Context(), stateRoot: root, repository: "o/r", operator: service}
	responses := make(chan *httptest.ResponseRecorder, len(manifests))
	for _, manifest := range manifests {
		go func() {
			path := fmt.Sprintf("http://localhost/actions/dismiss?repository=o%%2Fr&issue=%d&attempt=1", manifest.Issue)
			request := httptest.NewRequest(http.MethodPost, path, nil)
			request.Host = "localhost"
			request.Header.Set("Origin", "http://localhost")
			response := httptest.NewRecorder()
			server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
			responses <- response
		}()
	}
	for range manifests {
		<-entered
	}
	close(release)
	for range manifests {
		if response := <-responses; response.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
	committed := mustOwnerSnapshot(t, owner).State
	if len(committed.ControlReceipts) != 2 || len(committed.Tombstones) != 2 {
		t.Fatalf("state=%#v", committed)
	}
}

func TestV2CancelAdmitsRemoteReviewReadyWithRunningManifest(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 335, 1, "running")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	issueKey, attemptKey := ownerIssueKey("o/r", 335), ownerAttemptKey("o/r", 335, 1)
	observation := state.Observations[issueKey]
	fact := observation.Attempts[attemptKey].Fact
	fact.State, fact.PR, fact.HeadSHA = "review-ready", 336, manifest.BaseSHA
	attemptObservation := observation.Attempts[attemptKey]
	attemptObservation.Fact = fact
	observation.Attempts[attemptKey], observation.Fact.ActiveAttempt = attemptObservation, &fact
	state.Observations[issueKey] = observation
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	service := operatorTestMutationService(t, owner)
	service.effects.stopped = true
	result := service.perform(t.Context(), operatorRequest("cancel-review-ready", "cancel", manifest, false))
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("result=%#v", result)
	}
}

func TestV2AbandonAndRemoveSurviveBlockedReconciliation(t *testing.T) {
	for _, action := range []string{"abandon", "remove"} {
		t.Run(action, func(t *testing.T) {
			var owner *stateOwner
			var manifest agentruntime.Manifest
			if action == "abandon" {
				owner, manifest = operatorTestOwner(t, 336, "orphaned", false)
			} else {
				root := resolvedTempDir(t)
				manifest = ownerTestManifest(t, root, 337, 1, "failed")
				newer := ownerTestManifest(t, root, 337, 2, "running")
				state := newRuntimeOwnerState("o/r")
				issueKey := ownerIssueKey("o/r", 337)
				firstKey, newerKey := ownerAttemptKey("o/r", 337, 1), ownerAttemptKey("o/r", 337, 2)
				state.IssueGenerations[issueKey] = 1
				state.AttemptGenerations[firstKey], state.AttemptGenerations[newerKey] = 1, 1
				state.Attempts[firstKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
				state.Attempts[newerKey] = runtimeAttemptRecord{Generation: 1, Manifest: newer}
				addOperatorObservation(&state, newer, "active", false)
				observation := state.Observations[issueKey]
				failed, err := reduceAttemptFact("o/r", internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 337, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}})
				if err != nil {
					t.Fatal(err)
				}
				observation.Fact.TerminalAttempts = []reconciliationAttemptFact{failed}
				observation.Attempts[firstKey] = reconciliationAttemptObservation{Present: true, Generation: 1, OwnerGeneration: 1, SourceIssueGeneration: observation.Generation, ObservationEpoch: 1, LastCycleID: observation.LastCycleID, Fact: failed}
				state.Observations[issueKey] = observation
				state.Epoch, state.Revision = 1, 1
				owner, err = startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = owner.close(context.Background()) })
			}
			if err := os.MkdirAll(productionSnapshotRoot(owner.stateRoot), 0o700); err != nil {
				t.Fatal(err)
			}
			status, statuses, err := ownerOperatorStatus(mustOwnerSnapshot(t, owner).State, manifest.Issue, manifest.Attempt)
			if err != nil || !validDestructiveOperatorStatus(action, status, statuses) {
				t.Fatalf("status=%#v all=%#v err=%v", status, statuses, err)
			}
			service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
			service.effects.stopped = true
			server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: "o/r", operator: service}
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			t.Cleanup(func() { once.Do(func() { close(release) }) })
			runner := reconciliationRunner{owner: owner, collect: func(_ context.Context, snapshot stateOwnerSnapshot) (reconciliationInput, error) {
				close(entered)
				<-release
				observation := snapshot.State.Observations[ownerIssueKey("o/r", manifest.Issue)]
				input := reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: "o/r"}, Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}}
				for _, attempt := range observation.Attempts {
					input.Attempts = append(input.Attempts, expandAttemptFact(attempt.Fact))
				}
				return input, nil
			}}
			reconciled := make(chan error, 1)
			go func() { _, err := runner.run(t.Context()); reconciled <- err }()
			<-entered
			path := fmt.Sprintf("http://localhost/actions/%s?repository=o%%2Fr&issue=%d&attempt=%d", action, manifest.Issue, manifest.Attempt)
			request := httptest.NewRequest(http.MethodPost, path, nil)
			request.Host = "localhost"
			request.Header.Set("Origin", "http://localhost")
			response := httptest.NewRecorder()
			server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
			if response.Code != http.StatusAccepted {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			once.Do(func() { close(release) })
			if err := <-reconciled; err != nil && !errors.Is(err, errStaleStateResult) {
				t.Fatal(err)
			}
			state := mustOwnerSnapshot(t, owner).State
			key := ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)
			if _, exists := state.Attempts[key]; exists || state.Tombstones[key].Action != operatorTombstoneAction(action) || state.AttemptGenerations[key] != 2 {
				t.Fatalf("stale reconciliation restored %s: %#v", action, state)
			}
		})
	}
}

func TestOperatorPersistenceFailureBeforeDispatchCommitsNothing(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 340, 1, "running")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	writes := 0
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error {
		writes++
		if writes == 2 {
			return errors.New("injected operator admission persistence failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	runner := &barrierEffectRunner{}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: root, Runner: runner, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}}
	result := service.perform(t.Context(), operatorRequest("persist-before-dispatch", "cancel", manifest, false))
	if result.Status != http.StatusInternalServerError || result.OK {
		t.Fatalf("result=%#v", result)
	}
	after := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey("o/r", 340, 1)
	if after.Revision != 2 || after.AttemptGenerations[key] != 1 || len(after.ControlReceipts) != 0 || len(after.Effects) != 0 || runner.calls.Load() != 0 {
		t.Fatalf("state=%#v calls=%d", after, runner.calls.Load())
	}
}

func TestOperatorMarkerSurvivesFinishPersistenceFailureAndFinalizesAfterRestart(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 341, 1, "running")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	writes := 0
	var persisted runtimeOwnerState
	owner, err := startTestStateOwner(t, root, state, func(candidate runtimeOwnerState) error {
		writes++
		if writes == 3 {
			return errors.New("injected operator result persistence failure")
		}
		persisted = cloneRuntimeOwnerState(candidate)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: root, Runner: &barrierEffectRunner{}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}}
	request, err := service.prepareStop(manifest, "operator cancelled attempt")
	if err != nil {
		t.Fatal(err)
	}
	command := operatorCommand(mustOwnerSnapshot(t, owner), operatorRequest("persist-after-marker", "cancel", manifest, false), manifest)
	command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: request.Action, Manifest: manifest, Reason: request.Reason, RequestDigest: request.Identity.RequestDigest}
	_, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	request.Identity = effectRequestIdentity(*effect)
	result, executeErr := effects.executeOperator(request)
	if executeErr == nil || !strings.Contains(executeErr.Error(), "injected operator result persistence failure") || result.Disposition != agentruntime.EffectResultReady {
		t.Fatalf("result=%#v err=%v", result, executeErr)
	}
	failed := mustOwnerSnapshot(t, owner).State
	receipt, _ := operatorReceiptByID(failed, command.Request.RequestID)
	if receipt.State != "pending" || failed.Effects[effect.ID].State != "pending" || failed.Attempts[ownerAttemptKey("o/r", 341, 1)].Manifest.State != "running" {
		t.Fatalf("state=%#v receipt=%#v", failed, receipt)
	}
	marker := filepath.Join(root, "runtime-effects", effect.ID+".done")
	if _, err := os.Lstat(marker); err != nil {
		t.Fatalf("completion marker missing: %v", err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, persisted, func(candidate runtimeOwnerState) error {
		persisted = cloneRuntimeOwnerState(candidate)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	restartRuntime := &agentruntime.Runtime{Root: restarted.attemptRoot, StateRoot: root, Runner: &barrierEffectRunner{}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	restartEffects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, executor: agentruntime.EffectExecutor{Runtime: restartRuntime}, active: map[string]*activeRuntimeEffect{}}
	restartService := &operatorMutationService{lifecycle: t.Context(), owner: restarted, effects: restartEffects, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}}
	restartSnapshot := mustOwnerSnapshot(t, restarted)
	restartEffect := restartSnapshot.State.Effects[effect.ID]
	reconstructed, err := restartService.reconstructRuntimeRequest(restartSnapshot, restartEffect)
	if err != nil {
		t.Fatal(err)
	}
	verification, err := restartEffects.verifyPendingOperator(t.Context(), restartSnapshot, restartEffect, reconstructed)
	if err != nil || verification.Disposition != agentruntime.EffectVerified {
		t.Fatalf("verification=%#v err=%v", verification, err)
	}
	final := mustOwnerSnapshot(t, restarted).State
	receipt, _ = operatorReceiptByID(final, command.Request.RequestID)
	if receipt.State != "completed" || final.Effects[effect.ID].State != "completed" || final.Attempts[ownerAttemptKey("o/r", 341, 1)].Manifest.State != "cancelled" {
		t.Fatalf("state=%#v receipt=%#v", final, receipt)
	}
}

type operatorOwnedRunner struct{ manifest agentruntime.Manifest }

func (r operatorOwnedRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if command.Name == "git" && slices.Contains(command.Args, "--show-current") {
		return agentruntime.Result{Output: r.manifest.Branch + "\n"}, nil
	}
	if command.Name == "tmux" && slices.Contains(command.Args, "has-session") {
		return agentruntime.Result{}, nil
	}
	return agentruntime.Result{}, fmt.Errorf("unexpected ownership command %s %v", command.Name, command.Args)
}

func operatorServiceWithCleanup(t *testing.T, owner *stateOwner, lifecycle context.Context, implementation boundaryCaller) *operatorMutationService {
	t.Helper()
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: &barrierEffectRunner{}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	cleanup := operatorCleanupExecutor{stateRoot: owner.stateRoot, implementation: implementation, reviewer: &operatorBoundaryRecorder{}, runtime: runtimeState}
	effects := &runtimeEffectCoordinator{lifecycle: lifecycle, owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState, Cleanup: cleanup.execute, VerifyCleanup: cleanup.verify}, active: map[string]*activeRuntimeEffect{}}
	return &operatorMutationService{lifecycle: lifecycle, owner: owner, effects: effects, cleanup: cleanup, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}}
}

type operatorCleanupBoundary struct {
	path            string
	entered, exited chan struct{}
	enterOnce       sync.Once
}

func (b *operatorCleanupBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if strings.HasPrefix(operation, "validate-") {
		return agentruntime.Result{}, nil
	}
	if operation != "cleanup" {
		return agentruntime.Result{}, fmt.Errorf("unexpected cleanup operation %q", operation)
	}
	if b.entered != nil {
		b.enterOnce.Do(func() { close(b.entered) })
		<-ctx.Done()
		if b.exited != nil {
			close(b.exited)
		}
		return agentruntime.Result{}, ctx.Err()
	}
	return agentruntime.Result{}, os.RemoveAll(b.path)
}

type operatorBoundaryRecorder struct {
	mu          sync.Mutex
	seen        []string
	sessionLive bool
}

func (r *operatorBoundaryRecorder) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, operation)
	if operation != "run" || len(command.Args) == 0 {
		return agentruntime.Result{}, nil
	}
	switch command.Args[0] {
	case "has-session":
		if r.sessionLive {
			return agentruntime.Result{}, nil
		}
		return agentruntime.Result{Exited: true, Code: 1}, errors.New("session absent")
	case "kill-session":
		r.sessionLive = false
		return agentruntime.Result{}, nil
	default:
		return agentruntime.Result{}, fmt.Errorf("unexpected tmux operation %q", command.Args[0])
	}
}

func (r *operatorBoundaryRecorder) operations() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.seen)
}
