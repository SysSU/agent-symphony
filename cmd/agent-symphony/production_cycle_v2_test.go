package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestPlanRuntimeLifecycleEmitsOneGenerationCurrentActionPerAttempt(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := filepath.Join(root, "worktrees")
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Default("o/r")
	cfg.Concurrency = 3
	now := time.Unix(1_700_000_000, 0).UTC()
	base := strings.Repeat("a", 40)
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	batch := reconciliationV2Batch{Input: reconciliationInput{Issues: []internalgithub.RecoveryIssueFact{}}}

	addIssue := func(issue, attempt int, active bool) reconciliationObservation {
		body := "issue body " + string(rune('a'+issue))
		paths := []string{"path/" + strconv.Itoa(issue)}
		fact := reconciliationIssueFact{Repository: "o/r", Issue: issue, Attempt: attempt, Priority: 1, CreatedAtUnixNano: now.UnixNano(), Eligible: !active, Active: active, DispatchAuthorized: true, BaseSHA: base, BaseBranch: "main", BodyDigest: digestText(body), Paths: paths}
		key := ownerIssueKey("o/r", issue)
		state.IssueGenerations[key] = 1
		observation := reconciliationObservation{Present: true, Generation: 1, OwnerGeneration: 1, ObservationEpoch: 1, LastCycleID: 1, Fact: fact, Attempts: map[string]reconciliationAttemptObservation{}}
		state.Observations[key] = observation
		batch.Input.Issues = append(batch.Input.Issues, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: issue, Attempt: attempt, Priority: 1, CreatedAt: now, Eligible: !active, Active: active, DispatchAuthorized: true, BaseSHA: base, BaseBranch: "main", Body: body, Paths: paths})
		return observation
	}

	addIssue(1, 1, false)
	preparingObservation := addIssue(2, 2, true)
	preparingAttempt := agentruntime.Attempt{Repository: "o/r", Issue: 2, Number: 1, BaseSHA: base, Interactive: true}
	preparing, err := agentruntime.PreparingManifest(attemptRoot, root, preparingAttempt, now)
	if err != nil {
		t.Fatal(err)
	}
	preparingKey := ownerAttemptKey("o/r", 2, 1)
	state.AttemptGenerations[preparingKey] = 1
	state.Attempts[preparingKey] = runtimeAttemptRecord{Generation: 1, Manifest: preparing}
	preparingObservation.Fact.ActiveAttempt = &reconciliationAttemptFact{Repository: "o/r", Issue: 2, Attempt: 1, BaseSHA: base, State: "active"}
	preparingObservation.Attempts[preparingKey] = reconciliationAttemptObservation{Present: true, Generation: 1, OwnerGeneration: 1, SourceIssueGeneration: 1, ObservationEpoch: 1, LastCycleID: 1, Fact: *preparingObservation.Fact.ActiveAttempt}
	state.Observations[ownerIssueKey("o/r", 2)] = preparingObservation

	addIssue(3, 1, true)
	runningAttempt := agentruntime.Attempt{Repository: "o/r", Issue: 3, Number: 1, BaseSHA: base}
	running, err := agentruntime.PreparingManifest(attemptRoot, root, runningAttempt, now)
	if err != nil {
		t.Fatal(err)
	}
	running.State = "running"
	runningKey := ownerAttemptKey("o/r", 3, 1)
	state.AttemptGenerations[runningKey] = 1
	state.Attempts[runningKey] = runtimeAttemptRecord{Generation: 1, Manifest: running}

	plans, err := planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, cfg, now, attemptRoot, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 3 || plans[0].Request.Action != agentruntime.EffectPrepare || plans[1].Request.Action != agentruntime.EffectStart || plans[2].Request.Action != agentruntime.EffectMonitor {
		t.Fatalf("plans=%#v", plans)
	}
	if plans[1].Request.Attempt.Number != 1 || plans[1].Request.Attempt.BaseSHA != preparing.BaseSHA {
		t.Fatalf("start request used issue next-attempt identity: %#v", plans[1].Request.Attempt)
	}
	for _, plan := range plans {
		if plan.Request.Attempt.Eligible != nil || !plan.Request.Eligible {
			t.Fatalf("request did not capture eligibility: %#v", plan.Request)
		}
	}

	state.Effects["pending"] = runtimeEffectIntent{ID: "pending", State: "pending", Repository: "o/r", Issue: 3, Attempt: 1}
	plans, err = planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, cfg, now, attemptRoot, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(plans) != 2 {
		t.Fatalf("pending attempt was planned again: %#v", plans)
	}
}

func TestProductionTriggerImmediatelyCoalescesRecollectWithoutFailure(t *testing.T) {
	started := make(chan int, 2)
	secondDone := make(chan struct{})
	runs := 0
	trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(context.Context) error {
		runs++
		started <- runs
		if runs == 1 {
			return errReconciliationRecollect
		}
		close(secondDone)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := trigger.trigger(); err != nil {
		t.Fatal(err)
	}
	if first, second := <-started, <-started; first != 1 || second != 2 {
		t.Fatalf("runs=%d,%d", first, second)
	}
	<-secondDone
	trigger.mu.Lock()
	lastErr, completed := trigger.lastErr, trigger.runs
	trigger.mu.Unlock()
	if lastErr != nil || completed != 2 {
		t.Fatalf("completed=%d lastErr=%v", completed, lastErr)
	}
	if err := trigger.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestResumePendingMonitorReconstructsExactlyAndRunsAsynchronously(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	manifest := ownerTestManifest(t, owner.stateRoot, 41, 1, "running")
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	issue := internalgithub.RecoveryIssueFact{
		Repository: "o/r", Issue: 41, Attempt: 1, Active: true,
		DispatchAuthorized: true, BaseSHA: manifest.BaseSHA, Body: "exact body",
	}
	applyReconciliationInput(t, owner, repositoryInput(true, issue))

	lifecycle, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner := &barrierEffectRunner{entered: make(chan struct{}, 1), release: make(chan struct{})}
	runtimeState := &agentruntime.Runtime{
		Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: runner, Tmux: "tmux",
		VerifyWorker: func(context.Context) error { return nil },
	}
	effects, err := newRuntimeEffectCoordinator(lifecycle, owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	batch := reconciliationV2Batch{Input: repositoryInput(true, issue)}
	plans, err := planRuntimeLifecycle(mustOwnerSnapshot(t, owner), batch, config.Default("o/r"), time.Now().UTC(), owner.attemptRoot, owner.stateRoot)
	if err != nil || len(plans) != 1 || plans[0].Request.Action != agentruntime.EffectMonitor {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	if _, err := effects.beginWithSource(t.Context(), plans[0].Snapshot, plans[0].Request, ""); err != nil {
		t.Fatal(err)
	}
	woke := make(chan struct{}, 1)
	production := &productionReconciliation{owner: owner, effects: effects, config: config.Default("o/r"), wake: func() error { woke <- struct{}{}; return nil }}
	if err := production.resumePendingRuntime(t.Context(), batch, ""); err != nil {
		t.Fatal(err)
	}
	<-runner.entered
	close(runner.release)
	<-woke
	if err := effects.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

func TestProductionCycleCollectsAppliesAndPlansWithoutLegacyWriters(t *testing.T) {
	checkout := gitRepository(t)
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "commit", "--allow-empty", "-m", "base")
	owner := newReconciliationTestOwner(t)
	writes := 0
	reads := 0
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			writes++
			return nil, errors.New("unexpected mutation")
		}
		reads++
		var value any
		switch request.URL.RequestURI() {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1":
			value = []any{}
		case "/repos/o/r":
			value = map[string]any{"default_branch": "main"}
		case "/repos/o/r/branches/main":
			value = map[string]any{"commit": map[string]any{"sha": strings.Repeat("a", 40)}}
		case "/repos/o/r/issues?state=open&per_page=100&page=1":
			value = []any{}
		default:
			return nil, fmt.Errorf("unexpected read %s", request.URL.String())
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
	})}}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: &barrierEffectRunner{}, VerifyWorker: func(context.Context) error { return nil }}
	effects, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default("o/r")
	production := &productionReconciliation{
		owner: owner, effects: effects, config: cfg, api: api, stateRoot: owner.stateRoot,
		attemptRoot: owner.attemptRoot, checkout: checkout,
		collector: reconciliationV2Collector{API: api, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: "o/r"}},
	}
	failureAt := time.Unix(1_700_000_100, 0).UTC()
	failureCycle, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	failureIdentity := stateResultIdentity{Epoch: failureCycle.State.Epoch, SourceRevision: failureCycle.State.Revision, CycleID: failureCycle.CycleID}
	failed, err := owner.recordCycleOutcome(t.Context(), recordCycleOutcomeCommand{Identity: failureIdentity, Diagnostic: "GitHub collection failed", At: failureAt})
	if err != nil {
		t.Fatal(err)
	}
	failedStatus, err := projectOwnerStatus(failed, 1, time.Now().UTC())
	if err != nil || failedStatus.ReconciliationError != "GitHub collection failed" || !failedStatus.ReconciliationErrorAt.Equal(failureAt) {
		t.Fatalf("failed status=%#v err=%v", failedStatus, err)
	}
	if err := production.runCycle(t.Context()); err != nil {
		t.Fatal(err)
	}
	cleared := mustOwnerSnapshot(t, owner)
	if cleared.State.CycleDiagnostic != "" || !cleared.State.CycleDiagnosticAt.IsZero() {
		t.Fatalf("successful fresh cycle retained failure: %#v", cleared.State)
	}
	if _, err := owner.recordCycleOutcome(t.Context(), recordCycleOutcomeCommand{Identity: failureIdentity, Diagnostic: "late stale failure", At: failureAt.Add(time.Second)}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("late cycle outcome err=%v", err)
	}
	afterLate := mustOwnerSnapshot(t, owner)
	if afterLate.State.CycleDiagnostic != "" || afterLate.State.CycleOutcomeID != cleared.State.CycleOutcomeID {
		t.Fatalf("late cycle overwrote newer success: %#v", afterLate.State)
	}
	if reads == 0 || writes != 0 {
		t.Fatalf("reads=%d writes=%d", reads, writes)
	}
	if files, err := filepath.Glob(filepath.Join(owner.attemptRoot, "*.source.bundle")); err != nil || len(files) != 1 {
		t.Fatalf("immutable sources=%v err=%v", files, err)
	}
}

func TestStartupMarkerSweepLeavesOperatorEffectsToReceiptRecovery(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 42, "completed", false)
	snapshot := mustOwnerSnapshot(t, owner)
	command := operatorCommand(snapshot, operatorRequest("startup-archive", "archive", manifest, true), manifest)
	command.CleanupDigest = strings.Repeat("d", 64)
	committed, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil || effect == nil {
		t.Fatalf("effect=%#v err=%v", effect, err)
	}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	production := &productionReconciliation{owner: owner, effects: effects, stateRoot: owner.stateRoot}
	if err := production.sweepPendingMarkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := mustOwnerSnapshot(t, owner)
	if after.State.Revision != committed.State.Revision || after.State.Effects[effect.ID].Diagnostic != "" {
		t.Fatalf("operator effect was consumed by generic sweep: %#v", after.State.Effects[effect.ID])
	}
}

func TestProductionRuntimeAdmitsOwnerMutationWhileInitialCollectionIsBlocked(t *testing.T) {
	checkout := gitRepository(t)
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "commit", "--allow-empty", "-m", "base")
	stateRoot := resolvedTempDir(t)
	if err := bindDeployment(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	if err := installDeploymentFence(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	if err := writeDashboardStatusSnapshot(stateRoot, dashboardStatusSnapshot{UpdatedAt: time.Now().UTC(), ReconciliationError: "legacy stale failure", ReconciliationErrorAt: time.Now().UTC(), Statuses: []orchestrator.RecoveryStatus{}}); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		select {
		case entered <- struct{}{}:
			select {
			case <-release:
			case <-request.Context().Done():
				return nil, request.Context().Err()
			}
		default:
		}
		var value any
		switch request.URL.RequestURI() {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1":
			value = []any{}
		case "/repos/o/r":
			value = map[string]any{"default_branch": "main"}
		case "/repos/o/r/branches/main":
			value = map[string]any{"commit": map[string]any{"sha": strings.Repeat("a", 40)}}
		case "/repos/o/r/issues?state=open&per_page=100&page=1":
			value = []any{}
		default:
			return nil, fmt.Errorf("unexpected read %s", request.URL.String())
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
	})}}
	lifecycle, cancel := context.WithCancel(t.Context())
	runtime, err := startProductionRuntimeV2(lifecycle, config.Default("o/r"), api, internalgithub.AuthenticatedUser{ID: 42}, stateRoot, filepath.Join(stateRoot, "legacy-pr-state.json"), checkout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		close(release)
		cancel()
		_ = runtime.shutdown(context.Background())
	}()
	<-entered
	status, err := (&dashboardServer{stateRoot: stateRoot, repository: "o/r"}).readStatus()
	if err != nil || status.OwnerEpoch == 0 || status.OwnerRevision == 0 || status.ReconciliationError != "" {
		t.Fatalf("initial owner status=%#v err=%v", status, err)
	}
	manifest := ownerTestManifest(t, stateRoot, 43, 1, "running")
	committed, err := runtime.owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil || committed.State.Attempts[ownerAttemptKey("o/r", 43, 1)].Manifest.State != "running" {
		t.Fatalf("mutation while collection blocked: snapshot=%#v err=%v", committed, err)
	}
}

func TestProductionRuntimeShutdownCancelsBlockedEffectBeforeJoiningTrigger(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	lifecycle, cancel := context.WithCancel(t.Context())
	effects := &runtimeEffectCoordinator{lifecycle: lifecycle, owner: owner, active: map[string]*activeRuntimeEffect{}}
	run, err := effects.acquireKey(t.Context(), ownerAttemptKey("o/r", 47, 1), 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	trigger, err := newProductionReconciliationTriggerRunner(lifecycle, func(context.Context) error {
		close(started)
		<-run.ctx.Done()
		effects.releaseKey(ownerAttemptKey("o/r", 47, 1), run)
		return run.ctx.Err()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := trigger.trigger(); err != nil {
		t.Fatal(err)
	}
	<-started
	runtime := &productionRuntimeV2{cancel: cancel, owner: owner, effects: effects, trigger: trigger}
	if err := runtime.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-run.done:
	default:
		t.Fatal("blocked effect was not joined")
	}
}

func TestV2DashboardReconcileAndServersDoNotUseLegacyOperationLock(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	service := operatorTestMutationService(t, owner)
	project, err := newProjectDashboardServerV2(t.Context(), owner.stateRoot, "o/r", nil, "tmux", service, false, "")
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	project.reconcile = func(context.Context) error { calls++; return nil }
	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/reconcile", nil)
	request.Header.Set("Origin", "http://127.0.0.1")
	response := httptest.NewRecorder()
	project.webHandler().ServeHTTP(response, request)
	if response.Code != http.StatusNoContent || calls != 1 {
		t.Fatalf("status=%d calls=%d body=%q", response.Code, calls, response.Body.String())
	}

	_, running, err := startDashboardServerWaitable("127.0.0.1:0", project, false, "", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if err := running.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(controlSocketPath(owner.stateRoot)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("control socket survived waitable shutdown: %v", err)
	}
}

func TestV2ControlReconcileAndOrchestratorNeverWriteLegacyReceipts(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	service := operatorTestMutationService(t, owner)
	project, err := newProjectDashboardServerV2(t.Context(), owner.stateRoot, "o/r", nil, "tmux", service, false, "")
	if err != nil {
		t.Fatal(err)
	}
	project.orchestrator = &fakeDashboardOrchestrator{status: orchestratoragent.Status{Enabled: true, State: "running"}}
	reconciles := 0
	project.reconcile = func(context.Context) error { reconciles++; return nil }
	for _, request := range []controlRequest{
		{Version: controlVersion, RequestID: "v2-reconcile", Repository: "o/r", Action: "reconcile"},
		{Version: controlVersion, RequestID: "v2-orchestrator-clear", Repository: "o/r", Action: "orchestrator-clear"},
	} {
		result := project.performRecordedControl(t.Context(), request)
		if !result.OK || result.Version != controlVersion || result.RequestID != request.RequestID || result.OwnerRevision == 0 {
			t.Fatalf("%s result=%#v", request.Action, result)
		}
	}
	if reconciles != 1 {
		t.Fatalf("reconciles=%d", reconciles)
	}
	for _, name := range []string{controlReceiptsFile, "dashboard-state.json", "removal-state.json"} {
		if _, err := os.Lstat(filepath.Join(owner.stateRoot, name)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("v2 control wrote legacy %s: %v", name, err)
		}
	}
}

func TestDashboardMutationRemainsResponsiveDuringBlockedSupervisorIO(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 44, "completed", true)
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	project, err := newProjectDashboardServerV2(t.Context(), owner.stateRoot, "o/r", nil, "tmux", service, false, "")
	if err != nil {
		t.Fatal(err)
	}
	cfg := config.Default("o/r")
	agent, err := newOrchestratorAgent(cfg, owner.stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, cancel := context.WithCancel(t.Context())
	defer cancel()
	if err := agent.BindLifecycle(lifecycle); err != nil {
		t.Fatal(err)
	}
	runner := &barrierEffectRunner{entered: make(chan struct{}, 1), release: make(chan struct{})}
	agent.Runner = runner
	supervisorDone := make(chan error, 1)
	go func() {
		_, err := agent.Recover(t.Context())
		supervisorDone <- err
	}()
	<-runner.entered

	request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/dismiss?repository=o%2Fr&issue=44&attempt=1", nil)
	request.Header.Set("Origin", "http://127.0.0.1")
	response := httptest.NewRecorder()
	project.webHandler().ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("dismiss status=%d body=%q", response.Code, response.Body.String())
	}
	snapshot := mustOwnerSnapshot(t, owner)
	if snapshot.State.Tombstones[ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)].Action != "dismissed" {
		t.Fatalf("dismiss did not commit while supervisor was blocked: %#v", snapshot.State)
	}
	cancel()
	if err := agent.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-supervisorDone; err == nil {
		t.Fatal("cancelled blocked supervisor returned success")
	}
}

func TestMonitoringCheckInIsGenerationBoundAndDurablyCompleted(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	manifest := ownerTestManifest(t, owner.stateRoot, 45, 1, "running")
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 45, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active"}
	issue := internalgithub.RecoveryIssueFact{
		Repository: "o/r", Issue: 45, Attempt: 1, Active: true, NeedsAttention: true,
		DispatchAuthorized: true, BaseSHA: manifest.BaseSHA, Body: "body", ActiveAttempt: &remote,
	}
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	applyReconciliationInput(t, owner, input)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 45, Attempt: 1, Action: orchestratoragent.ProposalActionCheckIn, Binding: strings.Repeat("b", 64)}
	plan, err := planMonitoringCheckIn(mustOwnerSnapshot(t, owner), proposal)
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingCheckInRunner{branch: manifest.Branch}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Git: "git", Tmux: "tmux", Runner: runner, VerifyWorker: func(context.Context) error { return nil }}
	effects, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	plan, err = effects.beginReconciliation(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := effects.executeMonitoringCheckIn(plan); err != nil {
		t.Fatal(err)
	}
	after := mustOwnerSnapshot(t, owner)
	if after.State.Effects[plan.Identity.EffectID].State != "completed" || runner.deliveries != 1 {
		t.Fatalf("effect=%#v deliveries=%d", after.State.Effects[plan.Identity.EffectID], runner.deliveries)
	}
	if marker, err := readReconciliationEffectMarker(owner.stateRoot, plan.Identity, plan.Request); err != nil || marker == nil || marker.CheckIn == nil || !marker.CheckIn.Observed {
		t.Fatalf("marker=%#v err=%v", marker, err)
	}
	current := mustOwnerSnapshot(t, owner)
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{
		Repository: "o/r", Issue: 45, Attempt: 1,
		ExpectedIssueGeneration: current.State.IssueGenerations[ownerIssueKey("o/r", 45)], ExpectedAttemptGeneration: current.State.AttemptGenerations[ownerAttemptKey("o/r", 45, 1)],
		Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := effects.executeMonitoringCheckIn(plan); !errors.Is(err, errStaleStateResult) || runner.deliveries != 1 {
		t.Fatalf("stale check-in err=%v deliveries=%d", err, runner.deliveries)
	}
}

func TestStartupSweepLeavesUnmarkedMonitoringCheckInPendingWithoutDelivery(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	manifest := ownerTestManifest(t, owner.stateRoot, 46, 1, "running")
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 46, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active"}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 46, Attempt: 1, Active: true, NeedsAttention: true, DispatchAuthorized: true, BaseSHA: manifest.BaseSHA, Body: "body", ActiveAttempt: &remote}
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	applyReconciliationInput(t, owner, input)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 46, Attempt: 1, Action: orchestratoragent.ProposalActionCheckIn, Binding: strings.Repeat("d", 64)}
	plan, err := planMonitoringCheckIn(mustOwnerSnapshot(t, owner), proposal)
	if err != nil {
		t.Fatal(err)
	}
	runner := &recordingCheckInRunner{branch: manifest.Branch}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Git: "git", Tmux: "tmux", Runner: runner, VerifyWorker: func(context.Context) error { return nil }}
	effects, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	plan, err = effects.beginReconciliation(t.Context(), plan)
	if err != nil {
		t.Fatal(err)
	}
	cycle := &productionReconciliation{owner: owner, effects: effects, stateRoot: owner.stateRoot}
	if err := cycle.sweepPendingMarkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	effect := mustOwnerSnapshot(t, owner).State.Effects[plan.Identity.EffectID]
	if effect.State != "pending" || effect.Diagnostic == "" || runner.deliveries != 0 {
		t.Fatalf("effect=%#v deliveries=%d", effect, runner.deliveries)
	}
}

type recordingCheckInRunner struct {
	branch     string
	deliveries int
}

func (r *recordingCheckInRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if command.Name == "git" {
		return agentruntime.Result{Output: r.branch + "\n"}, nil
	}
	if len(command.Args) > 0 && command.Args[0] == "load-buffer" {
		r.deliveries++
	}
	return agentruntime.Result{}, nil
}

func TestImmutableAttemptSourceNeverChangesInFlightBundle(t *testing.T) {
	repository := gitRepository(t)
	runGit(t, repository, "config", "user.email", "test@example.invalid")
	runGit(t, repository, "config", "user.name", "test")
	runGit(t, repository, "commit", "--allow-empty", "-m", "base")
	attemptRoot := filepath.Join(t.TempDir(), "attempts")
	first, err := seedImmutableAttemptSource(t.Context(), repository, "o/r", attemptRoot, "", "")
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	runGit(t, repository, "commit", "--allow-empty", "-m", "next")
	second, err := seedImmutableAttemptSource(t.Context(), repository, "o/r", attemptRoot, "", "")
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(first)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || !slices.Equal(before, after) {
		t.Fatalf("source paths first=%q second=%q changed=%v", first, second, !slices.Equal(before, after))
	}
}

func TestPlanRuntimeLifecycleRejectsStaleBodyAndAttemptGeneration(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := filepath.Join(root, "worktrees")
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(1_700_000_000, 0).UTC()
	base := strings.Repeat("a", 40)
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision = 1, 1
	issueKey, attemptKey := ownerIssueKey("o/r", 4), ownerAttemptKey("o/r", 4, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 2
	manifest, err := agentruntime.PreparingManifest(attemptRoot, root, agentruntime.Attempt{Repository: "o/r", Issue: 4, Number: 1, BaseSHA: base}, now)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "running"
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	body := "current"
	state.Observations[issueKey] = reconciliationObservation{Present: true, Generation: 1, OwnerGeneration: 1, ObservationEpoch: 1, Fact: reconciliationIssueFact{Repository: "o/r", Issue: 4, Attempt: 1, DispatchAuthorized: true, BodyDigest: digestText(body)}, Attempts: map[string]reconciliationAttemptObservation{}}
	batch := reconciliationV2Batch{Input: reconciliationInput{Issues: []internalgithub.RecoveryIssueFact{{Repository: "o/r", Issue: 4, Attempt: 1, Body: body}}}}
	plans, err := planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), now, attemptRoot, root)
	if err != nil || len(plans) != 0 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 2, Manifest: manifest}
	batch.Input.Issues[0].Body = "stale"
	plans, err = planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), now, attemptRoot, root)
	if err != nil || len(plans) != 0 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	batch.Input.Issues[0].Body = body
	plans, err = planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), now, attemptRoot, root)
	if err != nil || len(plans) != 1 || plans[0].Request.Action != agentruntime.EffectMonitor {
		t.Fatalf("current plans=%#v err=%v", plans, err)
	}
	state.Epoch = 2
	plans, err = planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), now, attemptRoot, root)
	if err != nil || len(plans) != 0 {
		t.Fatalf("pre-restart observation authorized work: plans=%#v err=%v", plans, err)
	}
}
