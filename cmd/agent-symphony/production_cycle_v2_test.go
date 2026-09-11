package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
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

func restartOwnerWithInput(t *testing.T, owner *stateOwner, input reconciliationInput) *stateOwner {
	t.Helper()
	before := mustOwnerSnapshot(t, owner)
	if err := owner.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, before.State); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(owner.stateRoot, before.State.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, loaded, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	after := applyReconciliationInput(t, restarted, input)
	if after.State.Epoch <= before.State.Epoch {
		t.Fatalf("owner epoch did not advance: before=%d after=%d", before.State.Epoch, after.State.Epoch)
	}
	return restarted
}

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

func TestPlanRuntimeLifecycleNeverReusesOwnedAttemptAfterRestart(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := filepath.Join(root, "worktrees")
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	base, body := strings.Repeat("a", 40), "stale attempt proposal"
	issueKey, attemptOne := ownerIssueKey("o/r", 10), ownerAttemptKey("o/r", 10, 1)
	state := newRuntimeOwnerState("o/r")
	state.Epoch, state.Revision, state.IssueGenerations[issueKey], state.AttemptGenerations[attemptOne] = 2, 3, 1, 2
	state.Tombstones[attemptOne] = runtimeTombstone{Repository: "o/r", Issue: 10, Attempt: 1, Generation: 2, InvalidatedGeneration: 1, Action: "abandoned", CleanupPhase: "completed"}
	state.Observations[issueKey] = reconciliationObservation{
		Present: true, Generation: 1, OwnerGeneration: 1, ObservationEpoch: 2, LastCycleID: 1,
		Fact:     reconciliationIssueFact{Repository: "o/r", Issue: 10, Attempt: 1, Priority: 1, CreatedAtUnixNano: time.Unix(1, 0).UnixNano(), Eligible: true, DispatchAuthorized: true, BaseSHA: base, BaseBranch: "main", BodyDigest: digestText(body)},
		Attempts: map[string]reconciliationAttemptObservation{},
	}
	batch := reconciliationV2Batch{Input: reconciliationInput{Issues: []internalgithub.RecoveryIssueFact{{Repository: "o/r", Issue: 10, Attempt: 1, Priority: 1, CreatedAt: time.Unix(1, 0), Eligible: true, DispatchAuthorized: true, BaseSHA: base, BaseBranch: "main", Body: body}}}}
	plans, err := planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), time.Unix(1, 0).UTC(), attemptRoot, root)
	if err != nil || len(plans) != 1 || plans[0].Request.Action != agentruntime.EffectPrepare || plans[0].Request.Attempt.Number != 2 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	replacement := plans[0].Request.Manifest
	attemptTwo := ownerAttemptKey("o/r", 10, 2)
	state.AttemptGenerations[attemptTwo] = 1
	state.Attempts[attemptTwo] = runtimeAttemptRecord{Generation: 1, Manifest: replacement}
	plans, err = planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), time.Unix(2, 0).UTC(), attemptRoot, root)
	if err != nil || len(plans) != 0 {
		t.Fatalf("preparing replacement allocated another attempt: %#v err=%v", plans, err)
	}
	binds, err := planReconciliationBinds(stateOwnerSnapshot{State: state}, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1})
	if err != nil || len(binds) != 1 || binds[0].Request.Attempt != 2 {
		t.Fatalf("replacement binds=%#v err=%v", binds, err)
	}

	duplicate := replacement
	duplicate.Attempt = 3
	duplicate.Branch = strings.TrimSuffix(duplicate.Branch, "-2") + "-3"
	duplicate.Worktree = strings.TrimSuffix(duplicate.Worktree, "-2") + "-3"
	duplicate.Session = strings.TrimSuffix(duplicate.Session, "-2") + "-3"
	attemptThree := ownerAttemptKey("o/r", 10, 3)
	state.AttemptGenerations[attemptThree] = 1
	state.Attempts[attemptThree] = runtimeAttemptRecord{Generation: 1, Manifest: duplicate}
	if _, err := planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), time.Unix(3, 0).UTC(), attemptRoot, root); !errors.Is(err, errStateConflict) {
		t.Fatalf("multiple replacements err=%v", err)
	}
	delete(state.AttemptGenerations, attemptThree)
	delete(state.Attempts, attemptThree)

	active := reconciliationAttemptFact{Repository: "o/r", Issue: 10, Attempt: 2, BaseSHA: base, State: "active"}
	observation := state.Observations[issueKey]
	observation.Fact.Attempt = 2
	observation.Fact.Active, observation.Fact.ActiveAttempt = true, &active
	observation.Attempts[attemptTwo] = reconciliationAttemptObservation{Present: true, Generation: 1, OwnerGeneration: 1, SourceIssueGeneration: 1, ObservationEpoch: 2, LastCycleID: 2, Fact: active}
	state.Observations[issueKey] = observation
	batch.Input.Issues[0].Attempt = 2
	batch.Input.Issues[0].Active = true
	batch.Input.Issues[0].ActiveAttempt = &internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 10, Attempt: 2, BaseSHA: base, State: "active"}
	batch.Input.Attempts = []internalgithub.RecoveryAttemptFact{*batch.Input.Issues[0].ActiveAttempt}
	plans, err = planRuntimeLifecycle(stateOwnerSnapshot{State: state}, batch, config.Default("o/r"), time.Unix(4, 0).UTC(), attemptRoot, root)
	if err != nil || len(plans) != 1 || plans[0].Request.Action != agentruntime.EffectStart || plans[0].Request.Attempt.Number != 2 {
		t.Fatalf("confirmed replacement start=%#v err=%v", plans, err)
	}
}

func TestReplacementLifecycleCommitsPrepareBindAndStartOnOneReservation(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := filepath.Join(root, "worktrees")
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	base, body := strings.Repeat("a", 40), "replacement proposal"
	issueKey, attemptOne := ownerIssueKey("o/r", 12), ownerAttemptKey("o/r", 12, 1)
	state := newRuntimeOwnerState("o/r")
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptOne] = 1, 2
	state.Tombstones[attemptOne] = runtimeTombstone{Repository: "o/r", Issue: 12, Attempt: 1, Generation: 2, InvalidatedGeneration: 1, Action: "removed", CleanupPhase: "completed"}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })

	proposal := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 12, Attempt: 1, Priority: 1, CreatedAt: time.Unix(1, 0).UTC(), Eligible: true, DispatchAuthorized: true, BaseSHA: base, BaseBranch: "main", Body: body}
	input := repositoryInput(true, proposal)
	accepted := applyReconciliationInput(t, owner, input)
	batch := reconciliationV2Batch{Input: input}
	plans, err := planRuntimeLifecycle(accepted, batch, config.Default("o/r"), time.Unix(2, 0).UTC(), attemptRoot, root)
	if err != nil || len(plans) != 1 || plans[0].Request.Action != agentruntime.EffectPrepare || plans[0].Request.Manifest.Attempt != 2 {
		t.Fatalf("prepare plans=%#v err=%v", plans, err)
	}
	prepare := plans[0]
	_, prepareEffect, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: stateResultIdentity{Epoch: prepare.Snapshot.State.Epoch, SourceRevision: prepare.Snapshot.State.Revision, IssueGeneration: prepare.Snapshot.State.IssueGenerations[issueKey]}, Action: agentruntime.EffectPrepare, Manifest: prepare.Request.Manifest, RequestDigest: strings.Repeat("1", 64)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*prepareEffect)), Action: agentruntime.EffectPrepare, Manifest: prepare.Request.Manifest}); err != nil {
		t.Fatal(err)
	}
	prepared := applyReconciliationInput(t, owner, input)
	if again, err := planRuntimeLifecycle(prepared, batch, config.Default("o/r"), time.Unix(3, 0).UTC(), attemptRoot, root); err != nil || len(again) != 0 {
		t.Fatalf("preparing reservation allocated another attempt: plans=%#v err=%v", again, err)
	}

	binds, err := planReconciliationBinds(prepared, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1})
	if err != nil || len(binds) != 1 || binds[0].Request.Attempt != 2 {
		t.Fatalf("bind plans=%#v err=%v", binds, err)
	}
	_, bindEffect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: binds[0].Identity, Request: binds[0].Request})
	if err != nil {
		t.Fatalf("replacement bind admission: %v", err)
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*bindEffect), Result: reconciliationEffectResult{Action: reconciliationGitHubBind, GitHubBind: &githubBindEffectResult{Observed: true}}}); err != nil {
		t.Fatalf("replacement bind finish: %v", err)
	}

	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 12, Attempt: 2, BaseSHA: base, State: "active"}
	confirmed := proposal
	confirmed.Attempt, confirmed.CurrentAttempt, confirmed.Active, confirmed.ActiveAttempt = 2, 2, true, &remote
	confirmedInput := repositoryInput(true, confirmed)
	confirmedInput.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	confirmedSnapshot := applyReconciliationInput(t, owner, confirmedInput)
	startPlans, err := planRuntimeLifecycle(confirmedSnapshot, reconciliationV2Batch{Input: confirmedInput}, config.Default("o/r"), time.Unix(4, 0).UTC(), attemptRoot, root)
	if err != nil || len(startPlans) != 1 || startPlans[0].Request.Action != agentruntime.EffectStart || startPlans[0].Request.Manifest.Attempt != 2 {
		t.Fatalf("start plans=%#v err=%v", startPlans, err)
	}
	start := startPlans[0]
	_, startEffect, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: stateResultIdentity{Epoch: start.Snapshot.State.Epoch, SourceRevision: start.Snapshot.State.Revision, IssueGeneration: start.Snapshot.State.IssueGenerations[issueKey], AttemptGeneration: start.Snapshot.State.AttemptGenerations[ownerAttemptKey("o/r", 12, 2)]}, Action: agentruntime.EffectStart, Manifest: start.Request.Manifest, RequestDigest: strings.Repeat("2", 64)})
	if err != nil {
		t.Fatal(err)
	}
	running := cloneManifest(start.Request.Manifest)
	running.State = "running"
	finished, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*startEffect)), Action: agentruntime.EffectStart, Manifest: running})
	if err != nil {
		t.Fatal(err)
	}
	if got := finished.State.Attempts[ownerAttemptKey("o/r", 12, 2)].Manifest.State; got != "running" {
		t.Fatalf("attempt 2 state=%q", got)
	}
	if _, exists := finished.State.AttemptGenerations[ownerAttemptKey("o/r", 12, 3)]; exists {
		t.Fatal("replacement lifecycle allocated attempt 3")
	}
}

func TestPreBindAbandonRestartReservesAttemptAndRejectsStaleWork(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 11, "orphaned", false)
	for _, path := range []string{manifest.Worktree, productionSnapshotRoot(owner.stateRoot)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	request := operatorRequest("pre-bind-abandon", "abandon", manifest, true)
	command, _, err := service.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), request)
	if err != nil {
		t.Fatal(err)
	}
	committed, _, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey("o/r", 11, 1)
	if committed.State.AttemptGenerations[key] != 2 || committed.State.Tombstones[key].Action != "abandoned" {
		t.Fatalf("abandon did not reserve attempt 1: %#v", committed.State)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, committed.State, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	body, base := "stale GitHub proposal", manifest.BaseSHA
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 11, Attempt: 1, Priority: 1, CreatedAt: time.Unix(1, 0).UTC(), Eligible: true, DispatchAuthorized: true, BaseSHA: base, BaseBranch: "main", Body: body}
	input := repositoryInput(true, issue)
	accepted := applyReconciliationInput(t, restarted, input)
	plans, err := planRuntimeLifecycle(accepted, reconciliationV2Batch{Input: input}, config.Default("o/r"), time.Unix(2, 0).UTC(), restarted.attemptRoot, restarted.stateRoot)
	if err != nil || len(plans) != 1 || plans[0].Request.Action != agentruntime.EffectPrepare || plans[0].Request.Attempt.Number != 2 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	stale := plans[0].Request
	stale.Attempt.Number = 1
	stale.Manifest, err = agentruntime.PreparingManifest(restarted.attemptRoot, restarted.stateRoot, stale.Attempt, time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	identity := stateResultIdentity{Epoch: accepted.State.Epoch, SourceRevision: accepted.State.Revision, IssueGeneration: accepted.State.IssueGenerations[ownerIssueKey("o/r", 11)], AttemptGeneration: accepted.State.AttemptGenerations[key]}
	if _, _, err := restarted.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectPrepare, Manifest: stale.Manifest, RequestDigest: strings.Repeat("f", 64)}); !errors.Is(err, errAttemptTombstoned) {
		t.Fatalf("stale attempt 1 begin err=%v", err)
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

func TestResumeUnmarkedBindUsesPersistedRequestWithoutReplanning(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "github-bind")
	_, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
	request := bindEffectObservation(snapshot, test.request)
	request.ExecutionDigest = githubBindExecutionDigest(request, cfg)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	owner = restartOwnerWithInput(t, owner, reconciliationEffectObservationInput(request, "title"))
	marker, _ := internalgithub.ActiveAttemptMarker(request.Repository, request.Issue, request.Attempt, request.GitHubBind.BaseSHA)
	api := issueUpdateAppliedAPI(t, map[int][]map[string]any{request.Issue: {{"id": 1, "body": marker, "created_at": time.Unix(1, 0).UTC(), "updated_at": time.Unix(1, 0).UTC(), "user": map[string]any{"id": 42}}}})
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	production := &productionReconciliation{owner: owner, effects: effects, collector: reconciliationV2Collector{Config: cfg}}
	resumed, err := production.resumePendingReconciliation(t.Context(), api, reconciliationV2Batch{})
	if err != nil || !resumed {
		t.Fatalf("resumed=%v err=%v", resumed, err)
	}
	finished := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if finished.State != "completed" || finished.ReconciliationResult == nil || finished.ReconciliationResult.GitHubBind == nil || !finished.ReconciliationResult.GitHubBind.Observed {
		t.Fatalf("effect=%#v", finished)
	}
}

func TestPendingReconciliationRecoveryRetainsEveryUnreconstructableVariant(t *testing.T) {
	for _, test := range reconciliationEffectCases(t) {
		t.Run(test.name, func(t *testing.T) {
			owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
			production := &productionReconciliation{
				owner: owner, effects: effects, stateRoot: owner.stateRoot,
				config: config.Default("o/r"), collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}},
			}
			resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{})
			if err != nil || resumed {
				t.Fatalf("resumed=%v err=%v", resumed, err)
			}
			current := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
			if current.State != "pending" || current.Diagnostic == "" {
				t.Fatalf("unreconstructable effect was lost or not diagnosed: %#v", current)
			}
		})
	}
}

func TestBindNoOpDoesNotRequestRecollection(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	manifest := ownerTestManifest(t, owner.stateRoot, 42, 1, "preparing")
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest}); err != nil {
		t.Fatal(err)
	}
	issue := issueFact(manifest.Issue, "already bound")
	issue.Attempt, issue.CurrentAttempt = manifest.Attempt, manifest.Attempt
	issue.Active, issue.DispatchAuthorized = true, true
	issue.BaseSHA = manifest.BaseSHA
	applyReconciliationInput(t, owner, repositoryInput(true, issue))

	cfg := internalgithub.PRAdapterConfig{Repository: manifest.Repository, ActorID: 42}
	marker, err := internalgithub.ActiveAttemptMarker(manifest.Repository, manifest.Issue, manifest.Attempt, manifest.BaseSHA)
	if err != nil {
		t.Fatal(err)
	}
	api := issueUpdateAppliedAPI(t, map[int][]map[string]any{manifest.Issue: {{"body": marker, "user": map[string]any{"id": cfg.ActorID}}}})
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	production := &productionReconciliation{owner: owner, effects: effects, collector: reconciliationV2Collector{Config: cfg}}
	changed, err := production.runBindPhase(t.Context(), api)
	if err != nil {
		t.Fatal(err)
	}
	if changed {
		t.Fatal("an already-observed bind requested another collection")
	}
	remote := internalgithub.RecoveryAttemptFact{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, BaseSHA: manifest.BaseSHA, State: "active"}
	issue.ActiveAttempt = &remote
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	applyReconciliationInput(t, owner, input)
	changed, err = production.runBindPhase(t.Context(), api)
	if err != nil || changed {
		t.Fatalf("fresh accepted bind observation changed=%v err=%v", changed, err)
	}
	markers, err := os.ReadDir(filepath.Join(owner.stateRoot, "reconciliation-effects"))
	if err != nil || len(markers) != 0 {
		t.Fatalf("bind marker growth=%d err=%v", len(markers), err)
	}
}

func TestGovernanceNoOpDoesNotRequestRecollectionOrGrowProofs(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 43, 1, "running")
	state := newRuntimeOwnerState("o/r")
	issueKey, attemptKey := ownerIssueKey("o/r", manifest.Issue), ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	head := strings.Repeat("b", 40)
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: manifest.Issue, Attempt: manifest.Attempt, PR: 8, BaseSHA: manifest.BaseSHA, HeadSHA: head, State: "active", PublicationConfirmed: true, Checks: []string{internalgithub.PolicyCheck + ":failure"}}
	issue := issueFact(manifest.Issue, "governed")
	issue.Attempt, issue.CurrentAttempt, issue.ActiveAttempt = manifest.Attempt, manifest.Attempt, &remote
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	applyReconciliationInput(t, owner, input)

	cfg := githubPRConfig(config.Default("o/r"), 42)
	baseBody, _ := internalgithub.PullRequestBody(manifest.Issue, manifest.Attempt, "tests", "none", "")
	pullBody, _ := internalgithub.BindPullRequestBody(baseBody, manifest.Issue, manifest.Attempt, manifest.Branch, head, remote.PR)
	activeMarker, _ := internalgithub.ActiveAttemptMarker(manifest.Repository, manifest.Issue, manifest.Attempt, manifest.BaseSHA)
	issueBody := "## Context\ncurrent\n## Acceptance criteria\ncurrent\n## Checklist\n- [ ] current\n## Validation\ncurrent\n## Dependencies\nNone.\n"
	created := time.Unix(1, 0).UTC()
	readyAt := time.Unix(2, 0).UTC()
	controls := internalgithub.Controls{Ready: true, Completion: "human-review"}
	provenance := []internalgithub.Provenance{
		{Name: "ready", Value: "true", Source: "timeline", EventID: 1, ActorID: cfg.ActorID, CreatedAt: readyAt},
		{Name: "priority", Value: "0", Source: "creation", ActorID: cfg.ActorID, CreatedAt: created},
		{Name: "completion", Value: "human-review", Source: "creation", ActorID: cfg.ActorID, CreatedAt: created},
		{Name: "closed", Value: "false", Source: "creation", ActorID: cfg.ActorID, CreatedAt: created},
		{Name: "cancelled", Value: "false", Source: "creation", ActorID: cfg.ActorID, CreatedAt: created},
		{Name: "retry", Value: "false", Source: "creation", ActorID: cfg.ActorID, CreatedAt: created},
	}
	snapshot, err := internalgithub.NewSnapshot(controls, issueBody, internalgithub.Anchor{IssueNodeID: "issue-node", CreatedAt: created, ChangedAt: created, AuthorID: cfg.ActorID}, internalgithub.Approval{}, provenance, cfg.ApprovalCommand, func(actor int) bool { return actor == cfg.ActorID }, func(event internalgithub.Provenance) bool { return slices.Contains(provenance, event) })
	if err != nil {
		t.Fatal(err)
	}
	comments := []any{
		map[string]any{"id": 1, "body": pullBody, "created_at": created, "updated_at": created, "user": map[string]any{"id": cfg.ActorID}},
		map[string]any{"id": 2, "body": activeMarker, "created_at": readyAt, "updated_at": readyAt, "user": map[string]any{"id": cfg.ActorID}},
		map[string]any{"id": 3, "body": internalgithub.SnapshotComment(snapshot), "created_at": readyAt, "updated_at": readyAt, "user": map[string]any{"id": cfg.ActorID}},
	}
	policyBody, _ := internalgithub.PolicyFailureBody(manifest.Issue, manifest.Attempt, head, "", []string{
		"documentation impact assessment is missing or stale",
		"feedback 2 is pending",
		"feedback issue:2 execution is claimed",
		"merge permission is unavailable",
		"validation evidence is missing or stale",
	})
	pull := map[string]any{
		"number": remote.PR, "body": pullBody, "state": "open", "merged": false, "mergeable": true,
		"user": map[string]any{"id": cfg.ActorID}, "head": map[string]any{"sha": head, "ref": manifest.Branch},
		"base": map[string]any{"sha": manifest.BaseSHA, "ref": "main"}, "labels": []any{},
	}
	mutations := 0
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet && request.Method != http.MethodPost {
			mutations++
			return nil, fmt.Errorf("unexpected governance mutation %s", request.URL.String())
		}
		if request.Method == http.MethodPost && request.URL.Path != "/graphql" {
			mutations++
			return nil, fmt.Errorf("unexpected governance mutation %s", request.URL.String())
		}
		var value any
		status := http.StatusOK
		switch request.URL.RequestURI() {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1":
			value = []any{pull}
		case fmt.Sprintf("/repos/o/r/issues/%d", manifest.Issue):
			value = map[string]any{"number": manifest.Issue, "node_id": "issue-node", "created_at": created, "state": "open", "title": "governed", "body": issueBody, "labels": []any{map[string]any{"name": cfg.ReadyLabel}}, "user": map[string]any{"id": cfg.ActorID}}
		case fmt.Sprintf("/repos/o/r/issues/%d/comments?per_page=100&page=1", manifest.Issue):
			value = comments
		case fmt.Sprintf("/repos/o/r/issues/%d/timeline?per_page=100&page=1", manifest.Issue):
			value = []any{map[string]any{"id": 1, "event": "labeled", "label": map[string]any{"name": cfg.ReadyLabel}, "created_at": readyAt, "actor": map[string]any{"id": cfg.ActorID}}}
		case "/repos/o/r":
			value = map[string]any{"full_name": "o/r", "default_branch": "main", "permissions": map[string]any{"pull": true}}
		case fmt.Sprintf("/user/%d", cfg.ActorID):
			value = map[string]any{"login": "owner"}
		case "/repos/o/r/collaborators/owner/permission":
			value = map[string]any{"permission": "maintain"}
		case fmt.Sprintf("/repos/o/r/commits/%s/check-runs?filter=latest&per_page=100&page=1", head), fmt.Sprintf("/repos/o/r/commits/%s/check-runs?filter=all&per_page=100&page=1", head):
			value = map[string]any{"check_runs": []any{}}
		case fmt.Sprintf("/repos/o/r/commits/%s/status", head):
			value = map[string]any{"statuses": []any{map[string]any{"context": internalgithub.PolicyCheck, "state": "failure", "creator": map[string]any{"id": cfg.ActorID}}}}
		case fmt.Sprintf("/repos/o/r/pulls/%d", remote.PR):
			value = pull
		case fmt.Sprintf("/repos/o/r/pulls/%d/comments?per_page=100&page=1", remote.PR), fmt.Sprintf("/repos/o/r/pulls/%d/reviews?per_page=100&page=1", remote.PR):
			value = []any{}
		case fmt.Sprintf("/repos/o/r/issues/%d/comments?per_page=100&page=1", remote.PR):
			value = []any{map[string]any{"id": 8, "body": policyBody, "user": map[string]any{"id": cfg.ActorID}}}
		case "/repos/o/r/issues/comments/2":
			value = comments[1]
		case fmt.Sprintf("/repos/o/r/commits/%s/statuses?per_page=100&page=1", head):
			value = []any{map[string]any{"context": internalgithub.PolicyCheck, "state": "failure", "creator": map[string]any{"id": cfg.ActorID}}}
		case "/repos/o/r/rules/branches/main?per_page=100&page=1":
			value = []any{}
		case "/repos/o/r/branches/main/protection":
			status, value = http.StatusNotFound, map[string]any{"message": "not protected"}
		case "/graphql":
			value = map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}, "pullRequest": map[string]any{"reviewDecision": nil}}}}
		default:
			return nil, fmt.Errorf("unexpected governance read %s %s", request.Method, request.URL.String())
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
	})}}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	production := &productionReconciliation{owner: owner, effects: effects, collector: reconciliationV2Collector{Config: cfg}}
	for cycle := 1; cycle <= 2; cycle++ {
		changed, err := production.runGovernancePhase(t.Context(), api)
		if err != nil || changed {
			t.Fatalf("cycle %d changed=%v mutations=%d err=%v", cycle, changed, mutations, err)
		}
	}
	issue.Title = "governed after restart"
	input = repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	current := applyReconciliationInput(t, owner, input)
	plans, err := planReconciliationGovernance(current, cfg)
	if err != nil || len(plans) != 1 {
		t.Fatalf("restart plans=%#v err=%v", plans, err)
	}
	if _, err := effects.beginReconciliation(t.Context(), plans[0]); err != nil {
		t.Fatal(err)
	}
	owner = restartOwnerWithInput(t, owner, input)
	effects.owner = owner
	production.owner = owner
	if resumed, err := production.resumePendingReconciliation(t.Context(), api, reconciliationV2Batch{Input: input}); err != nil || !resumed {
		t.Fatalf("restart resume=%v err=%v", resumed, err)
	}
	markers, err := os.ReadDir(filepath.Join(root, "reconciliation-effects"))
	if err != nil || len(markers) != 0 || mutations != 0 || len(mustOwnerSnapshot(t, owner).State.Effects) != 1 {
		t.Fatalf("markers=%d effects=%d mutations=%d err=%v", len(markers), len(mustOwnerSnapshot(t, owner).State.Effects), mutations, err)
	}
}

func TestUnmarkedPublicationAndReviewerReconstructExactWorkerExport(t *testing.T) {
	t.Run("publication", func(t *testing.T) {
		base, head, checkout, exportBoundary := testWorkerExportBoundary(t)
		owner, manifest, issue, snapshot := completedWorkerOwner(t, 44, base, head, true)
		implementation := exportBoundary(manifest.Branch)
		cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
		candidate := publicationExecutionMaterial{Issue: issue, Config: cfg, Root: checkout, Head: head, Validation: "ok", Documentation: "none"}
		plans, _, err := planReconciliationPublications(snapshot, []publicationExecutionMaterial{candidate})
		if err != nil || len(plans) != 1 {
			t.Fatalf("plans=%#v err=%v", plans, err)
		}
		coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
		plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
		if err != nil {
			t.Fatal(err)
		}
		input := repositoryInput(true, issue)
		owner = restartOwnerWithInput(t, owner, input)
		coordinator.owner = owner
		prBody, _ := internalgithub.PullRequestBody(issue.Issue, issue.Attempt, "ok", "none", "")
		prBody, _ = internalgithub.BindPullRequestBody(prBody, issue.Issue, issue.Attempt, manifest.Branch, head, 9)
		validation, _ := internalgithub.EvidenceBody(issue.Issue, issue.Attempt, "validation", head)
		documentation, _ := internalgithub.EvidenceBody(issue.Issue, issue.Attempt, "documentation", head)
		published, _ := internalgithub.AttemptMarker(issue.Issue, issue.Attempt, manifest.Branch, head, 9, "review")
		comments := []any{
			map[string]any{"body": validation, "user": map[string]any{"id": cfg.ActorID}},
			map[string]any{"body": documentation, "user": map[string]any{"id": cfg.ActorID}},
			map[string]any{"body": published, "user": map[string]any{"id": cfg.ActorID}},
		}
		api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
			var value any
			switch request.URL.RequestURI() {
			case "/user":
				value = map[string]any{"id": cfg.ActorID, "login": "owner"}
			case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1":
				value = []any{map[string]any{"number": 9, "body": prBody, "user": map[string]any{"id": cfg.ActorID}, "head": map[string]any{"sha": head, "ref": manifest.Branch}}}
			case fmt.Sprintf("/repos/o/r/issues/%d/comments?per_page=100&page=1", issue.Issue):
				value = comments
			default:
				return nil, fmt.Errorf("unexpected publication request %s %s", request.Method, request.URL.String())
			}
			body, _ := json.Marshal(value)
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
		})}}
		production := &productionReconciliation{owner: owner, effects: coordinator, implementation: implementation, collector: reconciliationV2Collector{Config: cfg}}
		batch := reconciliationV2Batch{Input: input}
		if resumed, err := production.resumePendingReconciliation(t.Context(), api, batch); err != nil || !resumed {
			t.Fatalf("resume=%v err=%v", resumed, err)
		}
		if mustOwnerSnapshot(t, owner).State.Effects[plan.Identity.EffectID].State != "completed" {
			t.Fatal("publication intent remained pending")
		}
	})

	t.Run("reviewer-run-observe", func(t *testing.T) {
		base, head, checkout, exportBoundary := testWorkerExportBoundary(t)
		owner, manifest, issue, snapshot := completedWorkerOwner(t, 45, base, head, false)
		implementation := exportBoundary(manifest.Branch)
		candidate := reviewerExecutionMaterial{Issue: issue, Source: checkout, HeadSHA: head, Env: []string{"REVIEW=1"}, Command: []string{"reviewer"}}
		plans, _, err := planReconciliationReviewers(snapshot, owner.stateRoot, []reviewerExecutionMaterial{candidate})
		if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "run-observe" {
			t.Fatalf("plans=%#v err=%v", plans, err)
		}
		coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
		plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
		if err != nil {
			t.Fatal(err)
		}
		input := repositoryInput(true, issue)
		owner = restartOwnerWithInput(t, owner, input)
		coordinator.owner = owner
		cfg := config.Default("o/r")
		cfg.Commands.Reviewer = []string{"reviewer"}
		reviewer := workerBoundaryRunner{Command: "/bin/sh", Args: []string{"-c", `payload=$(cat)
case "$payload" in
  *'"operation":"review-result"'*) printf %s '{"Output":"{\"type\":\"agent-symphony-review-v1\",\"status\":\"clean\",\"findings\":[]}"}' ;;
  *'display-message'*) printf %s '{"Output":"1|||0\n"}' ;;
  *) exit 1 ;;
esac`}}
		production := &productionReconciliation{owner: owner, effects: coordinator, implementation: implementation, reviewer: reviewer, config: cfg, reviewEnv: []string{"REVIEW=1"}}
		if resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{Input: input}); err != nil || !resumed {
			t.Fatalf("resume=%v err=%v effect=%#v", resumed, err, mustOwnerSnapshot(t, owner).State.Effects[plan.Identity.EffectID])
		}
		current := mustOwnerSnapshot(t, owner)
		if current.State.Effects[plan.Identity.EffectID].State != "completed" || current.State.Attempts[ownerAttemptKey("o/r", issue.Issue, issue.Attempt)].Manifest.ReviewState != "clean" {
			t.Fatalf("reviewer result was not committed: %#v", current.State.Effects[plan.Identity.EffectID])
		}
	})

	t.Run("reviewer-cleanup", func(t *testing.T) {
		base, head, checkout, exportBoundary := testWorkerExportBoundary(t)
		owner, _, issue, snapshot := completedWorkerOwner(t, 45, base, head, true)
		manifest := snapshot.State.Attempts[ownerAttemptKey("o/r", 45, 1)].Manifest
		implementation := exportBoundary(manifest.Branch)
		candidate := reviewerExecutionMaterial{Issue: issue, Source: checkout, HeadSHA: head, Env: []string{"REVIEW=1"}, Command: []string{"reviewer"}}
		plans, _, err := planReconciliationReviewers(snapshot, owner.stateRoot, []reviewerExecutionMaterial{candidate})
		if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "cleanup" {
			t.Fatalf("plans=%#v err=%v", plans, err)
		}
		coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
		plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
		if err != nil {
			t.Fatal(err)
		}
		input := repositoryInput(true, issue)
		owner = restartOwnerWithInput(t, owner, input)
		coordinator.owner = owner
		cfg := config.Default("o/r")
		cfg.Commands.Reviewer = []string{"reviewer"}
		production := &productionReconciliation{owner: owner, effects: coordinator, implementation: implementation, reviewer: workerBoundaryResultValue(t, agentruntime.Result{Exited: true, Code: 1}), config: cfg, reviewEnv: []string{"REVIEW=1"}}
		batch := reconciliationV2Batch{Input: input}
		if resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, batch); err != nil || !resumed {
			t.Fatalf("resume=%v err=%v effect=%#v", resumed, err, mustOwnerSnapshot(t, owner).State.Effects[plan.Identity.EffectID])
		}
		if mustOwnerSnapshot(t, owner).State.Effects[plan.Identity.EffectID].State != "completed" {
			t.Fatal("reviewer cleanup intent remained pending")
		}
	})
}

func completedWorkerOwner(t *testing.T, issueNumber int, base, head string, reviewed bool) (*stateOwner, agentruntime.Manifest, internalgithub.RecoveryIssueFact, stateOwnerSnapshot) {
	t.Helper()
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, issueNumber, 1, "completed")
	manifest.BaseSHA = base
	if reviewed {
		if err := os.MkdirAll(productionSnapshotRoot(root), 0o700); err != nil {
			t.Fatal(err)
		}
		manifest.ReviewState, manifest.ReviewMode = "clean", agentruntime.ReviewModeImplementation
		manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = base, head, base+".."+head
		manifest.ReviewSnapshot, manifest.ReviewSession = reviewIdentity(agentruntime.Attempt{Repository: "o/r", Issue: issueNumber, Number: 1}, productionSnapshotRoot(root))
	}
	state := newRuntimeOwnerState("o/r")
	issueKey, attemptKey := ownerIssueKey("o/r", issueNumber), ownerAttemptKey("o/r", issueNumber, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	issue := issueFact(issueNumber, "worker result")
	issue.Attempt, issue.CurrentAttempt, issue.BaseBranch, issue.BaseSHA = 1, 1, "main", base
	snapshot := applyReconciliationInput(t, owner, repositoryInput(true, issue))
	return owner, manifest, issue, snapshot
}

func testWorkerExportBoundary(t *testing.T) (base, head, checkout string, boundary func(string) workerBoundaryRunner) {
	t.Helper()
	checkout = resolvedTempDir(t)
	runGit(t, checkout, "init")
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(checkout, "file"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, checkout, "add", "file")
	runGit(t, checkout, "commit", "-m", "base")
	base = runGit(t, checkout, "rev-parse", "HEAD")
	worker := filepath.Join(resolvedTempDir(t), "worker")
	if output, err := exec.Command("git", "clone", "-q", checkout, worker).CombinedOutput(); err != nil {
		t.Fatalf("clone worker: %v: %s", err, output)
	}
	runGit(t, worker, "config", "user.email", "test@example.invalid")
	runGit(t, worker, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("head"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "commit", "-am", "head")
	head = runGit(t, worker, "rev-parse", "HEAD")
	bundlePath := filepath.Join(t.TempDir(), "attempt.bundle")
	runGit(t, worker, "bundle", "create", bundlePath, "HEAD")
	bundle, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(checkout); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	return base, head, checkout, func(branch string) workerBoundaryRunner {
		exported := workerExport{Type: "agent-symphony-export-v1", Repository: "o/r", Branch: branch, BaseSHA: base, HeadSHA: head, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "ok", Documentation: "none"}, Bundle: base64.StdEncoding.EncodeToString(bundle)}
		exportedJSON, _ := json.Marshal(exported)
		return workerBoundaryResult(t, string(exportedJSON))
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
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: &agentruntime.Runtime{StateRoot: owner.stateRoot, Root: owner.attemptRoot}}, active: map[string]*activeRuntimeEffect{}}
	production := &productionReconciliation{owner: owner, effects: effects, stateRoot: owner.stateRoot}
	if err := production.sweepPendingMarkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := mustOwnerSnapshot(t, owner)
	if after.State.Revision != committed.State.Revision || after.State.Effects[effect.ID].Diagnostic != "" {
		t.Fatalf("operator effect was consumed by generic sweep: %#v", after.State.Effects[effect.ID])
	}
}

func TestStartupMarkerSweepReclaimsOrphanProofAbsentFromLedger(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "issue-control-snapshot")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	request := bindEffectObservation(snapshot, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity, result := ownerReconciliationEffectIdentity(*effect), test.result(request)
	if err := writeReconciliationEffectMarker(root, identity, request, result); err != nil {
		t.Fatal(err)
	}
	if err := prepareProductionMarkerDirectories(root); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		filepath.Join(root, "reconciliation-effects", ".effect-12345"),
		filepath.Join(root, "runtime-effects", ".effect-67890"),
	} {
		if err := os.WriteFile(path, []byte("crash residue"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	finished, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: result})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	delete(finished.State.Effects, effect.ID) // Simulate ledger replacement after the effect was retired.
	restarted, err := startTestStateOwner(t, root, finished.State, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	runtimeState := &agentruntime.Runtime{Root: restarted.attemptRoot, StateRoot: root, Runner: &barrierEffectRunner{}, VerifyWorker: func(context.Context) error { return nil }}
	coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}}
	production := &productionReconciliation{owner: restarted, effects: coordinator, stateRoot: root}
	if err := production.sweepPendingMarkers(t.Context()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(filepath.Join(root, "reconciliation-effects", effect.ID+".done")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("orphan marker was not reclaimed after restart: %v", err)
	}
	for _, path := range []string{
		filepath.Join(root, "reconciliation-effects", ".effect-12345"),
		filepath.Join(root, "runtime-effects", ".effect-67890"),
	} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("crash temporary was not reclaimed after restart: %s: %v", path, err)
		}
	}
}

func TestProductionRuntimeFinishesOperatorMarkerBeforeAdmission(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 48, "running", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	request := operatorRequest("startup-marker", "cancel", manifest, false)
	command, work, err := service.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), request)
	if err != nil {
		t.Fatal(err)
	}
	_, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	work.requestID = request.RequestID
	bindOperatorWorkIdentity(&work, *effect)
	if result, err := service.effects.executor.Execute(t.Context(), *work.runtime); err != nil || result.Disposition != agentruntime.EffectResultReady {
		t.Fatalf("seed marker result=%#v err=%v", result, err)
	}
	before := mustOwnerSnapshot(t, owner)
	if before.State.Effects[effect.ID].State != "pending" {
		t.Fatal("seed marker unexpectedly finished owner state")
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, before.State); err != nil {
		t.Fatal(err)
	}
	if err := bindDeployment(owner.stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	if err := installDeploymentFence(owner.stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	checkout := gitRepository(t)
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "commit", "--allow-empty", "-m", "base")
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var value any
		switch request.URL.RequestURI() {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1", "/repos/o/r/issues?state=open&per_page=100&page=1":
			value = []any{}
		case "/repos/o/r":
			value = map[string]any{"default_branch": "main"}
		case "/repos/o/r/branches/main":
			value = map[string]any{"commit": map[string]any{"sha": strings.Repeat("a", 40)}}
		default:
			return nil, fmt.Errorf("unexpected read %s", request.URL.String())
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
	})}}
	lifecycle, cancel := context.WithCancel(t.Context())
	runtime, err := startProductionRuntimeV2(lifecycle, config.Default("o/r"), api, internalgithub.AuthenticatedUser{ID: 42}, owner.stateRoot, filepath.Join(owner.stateRoot, "legacy-pr-state.json"), checkout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = runtime.shutdown(context.Background())
	}()
	final := mustOwnerSnapshot(t, runtime.owner).State
	receipt, ok := operatorReceiptByID(final, request.RequestID)
	if !ok || receipt.State != "completed" || final.Effects[effect.ID].State != "completed" {
		t.Fatalf("constructor returned before marker completion: receipt=%#v effect=%#v", receipt, final.Effects[effect.ID])
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

func TestProductionRuntimeShutdownCancelsEffectsWhilePersistenceDrains(t *testing.T) {
	root := resolvedTempDir(t)
	persisted, releasePersistence := make(chan struct{}), make(chan struct{})
	writes := 0
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error {
		writes++
		if writes == 2 {
			close(persisted)
			<-releasePersistence
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	lifecycle, cancel := context.WithCancel(t.Context())
	effects := &runtimeEffectCoordinator{lifecycle: lifecycle, owner: owner, active: map[string]*activeRuntimeEffect{}}
	key := ownerAttemptKey("o/r", 49, 1)
	run, err := effects.acquireKey(t.Context(), key, 1, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		<-run.ctx.Done()
		effects.releaseKey(key, run)
	}()
	mutation := make(chan error, 1)
	go func() {
		_, err := owner.recordControlReceipt(t.Context(), controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "shutdown-persistence", Repository: "o/r", Action: "reconcile"}, State: "pending"})
		mutation <- err
	}()
	<-persisted
	runtime := &productionRuntimeV2{cancel: cancel, owner: owner, effects: effects}
	shutdown := make(chan error, 1)
	go func() { shutdown <- runtime.shutdown(t.Context()) }()
	<-run.done
	select {
	case err := <-shutdown:
		t.Fatalf("shutdown returned before dispatched persistence drained: %v", err)
	default:
	}
	close(releasePersistence)
	if err := <-mutation; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
}

func TestV2DashboardReconcileAndServersDoNotUseLegacyOperationLock(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	service := operatorTestMutationService(t, owner)
	project, err := newProjectDashboardServerV2(t.Context(), owner.stateRoot, "o/r", nil, "tmux", service, 1, false, "")
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
	project, err := newProjectDashboardServerV2(t.Context(), owner.stateRoot, "o/r", nil, "tmux", service, 1, false, "")
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
	project, err := newProjectDashboardServerV2(t.Context(), owner.stateRoot, "o/r", nil, "tmux", service, 1, false, "")
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
	if marker, err := readReconciliationEffectMarker(owner.stateRoot, plan.Identity, plan.Request); err != nil || marker != nil {
		t.Fatalf("committed check-in marker was not reclaimed: marker=%#v err=%v", marker, err)
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
	owner = restartOwnerWithInput(t, owner, input)
	effects.owner = owner
	cycle := &productionReconciliation{owner: owner, effects: effects, stateRoot: owner.stateRoot}
	if resumed, err := cycle.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{Input: input}); err != nil || resumed {
		t.Fatalf("check-in resume=%v err=%v", resumed, err)
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
