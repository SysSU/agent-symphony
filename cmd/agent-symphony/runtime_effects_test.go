package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestRuntimeEffectSupersededBeforeExecutionPerformsNoIO(t *testing.T) {
	coordinator, owner, runner, manifest := runtimeEffectTestCoordinator(t, 71)
	monitor := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
	stop := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectStop, manifest, "operator stop")
	if stop.Identity.AttemptGeneration != 2 {
		t.Fatalf("stop generation=%d", stop.Identity.AttemptGeneration)
	}
	if _, err := coordinator.execute(t.Context(), monitor); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("stale monitor err=%v", err)
	}
	if got := runner.calls.Load(); got != 0 {
		t.Fatalf("stale effect performed %d I/O calls", got)
	}
	snapshot, err := owner.snapshot(t.Context())
	if err != nil || len(snapshot.State.Effects) != 1 || snapshot.State.Effects[stop.Identity.EffectID].State != "pending" {
		t.Fatalf("snapshot=%#v err=%v", snapshot, err)
	}
}

func TestRuntimeEffectDispatchRegistersBeforeSpawnAndShutdownJoins(t *testing.T) {
	coordinator, owner, _, manifest := runtimeEffectTestCoordinator(t, 72)
	request := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
	if err := coordinator.dispatch(request, nil); err != nil {
		t.Fatal(err)
	}
	coordinator.mu.Lock()
	active := len(coordinator.active)
	coordinator.mu.Unlock()
	if active != 1 {
		t.Fatalf("dispatch returned with %d registered effects", active)
	}
	if err := coordinator.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	coordinator.mu.Lock()
	active = len(coordinator.active)
	coordinator.mu.Unlock()
	if active != 0 {
		t.Fatalf("shutdown left %d effects", active)
	}
}

func TestRuntimeEffectStopCancelsAuthorizedBlockedMonitor(t *testing.T) {
	coordinator, owner, runner, manifest := runtimeEffectTestCoordinator(t, 72)
	monitor := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
	done := make(chan error, 1)
	go func() {
		_, err := coordinator.execute(t.Context(), monitor)
		done <- err
	}()
	<-runner.entered
	stop := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectStop, manifest, "operator stop")
	if stop.Identity.AttemptGeneration != 2 {
		t.Fatalf("stop generation=%d", stop.Identity.AttemptGeneration)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("monitor err=%v", err)
	}
	if got := runner.calls.Load(); got != 1 {
		t.Fatalf("monitor I/O calls=%d", got)
	}
}

func TestRuntimeEffectDuplicateStopIsIdempotent(t *testing.T) {
	coordinator, owner, _, manifest := runtimeEffectTestCoordinator(t, 85)
	request := runtimeTestRequest(agentruntime.EffectStop, manifest, "operator stop")
	first := beginRuntimeTestEffectRequest(t, coordinator, owner, request)
	afterFirst := mustOwnerSnapshot(t, owner)
	second := beginRuntimeTestEffectRequest(t, coordinator, owner, request)
	afterSecond := mustOwnerSnapshot(t, owner)
	if first.Identity != second.Identity || afterSecond.State.Revision != afterFirst.State.Revision || afterSecond.State.AttemptGenerations[ownerAttemptKey("o/r", 85, 1)] != 2 || len(afterSecond.State.Effects) != 1 {
		t.Fatalf("first=%#v second=%#v first state=%#v second state=%#v", first.Identity, second.Identity, afterFirst.State, afterSecond.State)
	}
	conflict := runtimeTestRequest(agentruntime.EffectStop, manifest, "different stop")
	if _, err := coordinator.begin(t.Context(), afterSecond, conflict); !errors.Is(err, errStateConflict) {
		t.Fatalf("conflicting Stop err=%v", err)
	}
}

func TestStateOwnerDuplicateStopCommandIsIdempotent(t *testing.T) {
	coordinator, owner, _, manifest := runtimeEffectTestCoordinator(t, 87)
	request, err := coordinator.executor.BindRequest(runtimeTestRequest(agentruntime.EffectStop, manifest, "operator stop"))
	if err != nil {
		t.Fatal(err)
	}
	digest, err := agentruntime.EffectRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := mustOwnerSnapshot(t, owner)
	command := beginRuntimeEffectCommand{
		Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: 1, AttemptGeneration: 1},
		Action:   agentruntime.EffectStop, Manifest: manifest, Reason: "operator stop", RequestDigest: digest,
	}
	firstState, first, err := owner.beginRuntimeEffect(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	secondState, second, err := owner.beginRuntimeEffect(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if first == nil || second == nil || first.ID != second.ID || firstState.State.Revision != secondState.State.Revision || len(secondState.State.Effects) != 1 {
		t.Fatalf("first=%#v second=%#v first state=%#v second state=%#v", first, second, firstState.State, secondState.State)
	}
}

func TestRuntimeEffectConcurrentSameSnapshotStopCommitsOnce(t *testing.T) {
	coordinator, owner, _, manifest := runtimeEffectTestCoordinator(t, 86)
	snapshot := mustOwnerSnapshot(t, owner)
	request := runtimeTestRequest(agentruntime.EffectStop, manifest, "operator stop")
	results := make(chan agentruntime.EffectRequest, 2)
	errors := make(chan error, 2)
	start := make(chan struct{})
	for range 2 {
		go func() {
			<-start
			result, err := coordinator.begin(t.Context(), snapshot, request)
			results <- result
			errors <- err
		}()
	}
	close(start)
	first, second := <-results, <-results
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
	if err := <-errors; err != nil {
		t.Fatal(err)
	}
	committed := mustOwnerSnapshot(t, owner)
	if first.Identity != second.Identity || committed.State.Revision != snapshot.State.Revision+1 || committed.State.AttemptGenerations[ownerAttemptKey("o/r", 86, 1)] != 2 || len(committed.State.Effects) != 1 {
		t.Fatalf("first=%#v second=%#v committed=%#v", first.Identity, second.Identity, committed.State)
	}
}

func TestRuntimeEffectDaemonShutdownCancelsOutstandingIO(t *testing.T) {
	coordinator, owner, runner, manifest := runtimeEffectTestCoordinator(t, 82)
	lifecycle, cancel := context.WithCancel(t.Context())
	coordinator.lifecycle = lifecycle
	request := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
	done := make(chan error, 1)
	go func() {
		_, err := coordinator.execute(context.Background(), request)
		done <- err
	}()
	<-runner.entered
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown err=%v", err)
	}
	snapshot := mustOwnerSnapshot(t, owner)
	if snapshot.State.Effects[request.Identity.EffectID].State != "pending" {
		t.Fatalf("cancelled work was finalized: %#v", snapshot.State)
	}
}

func TestRuntimeEffectCancelledStartCleanupRemainsPending(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, 88, 1, "preparing")
	owner, err := startTestStateOwner(t, root, runtimeEffectInitialState(manifest), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	lifecycle, cancel := context.WithCancel(t.Context())
	runner := &cancelledStartRunner{entered: make(chan struct{})}
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Runner: runner, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	coordinator, err := newRuntimeEffectCoordinator(lifecycle, owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	request := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectStart, manifest, "")
	done := make(chan error, 1)
	go func() {
		_, err := coordinator.execute(t.Context(), request)
		done <- err
	}()
	<-runner.entered
	cancel()
	if err := <-done; err == nil {
		t.Fatal("cancelled partial start returned no error")
	}
	snapshot := mustOwnerSnapshot(t, owner)
	key := ownerAttemptKey("o/r", 88, 1)
	if snapshot.State.Effects[request.Identity.EffectID].State != "pending" || snapshot.State.Attempts[key].Manifest.State != "preparing" {
		t.Fatalf("partial start was finalized: %#v", snapshot.State)
	}
	if _, err := os.Lstat(filepath.Join(root, "runtime-effects", request.Identity.EffectID+".done")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ambiguous partial start marker err=%v", err)
	}
}

func TestRuntimeEffectRejectsInvalidRequestBeforeIntent(t *testing.T) {
	coordinator, owner, _, manifest := runtimeEffectTestCoordinator(t, 83)
	before := mustOwnerSnapshot(t, owner)
	request := runtimeTestRequest(agentruntime.EffectStart, manifest, "")
	request.Attempt.Command = nil
	if _, err := coordinator.begin(t.Context(), before, request); err == nil {
		t.Fatal("invalid start request was admitted")
	}
	after := mustOwnerSnapshot(t, owner)
	if after.State.Revision != before.State.Revision || len(after.State.Effects) != 0 {
		t.Fatalf("invalid request changed state: %#v", after.State)
	}
}

func TestRuntimeEffectsSerializeSameAttemptAndOverlapDifferentAttempts(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	first, second := ownerTestManifest(t, root, 73, 1, "running"), ownerTestManifest(t, root, 74, 1, "running")
	state := newRuntimeOwnerState("o/r")
	for _, manifest := range []agentruntime.Manifest{first, second} {
		issueKey, attemptKey := ownerIssueKey("o/r", manifest.Issue), ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)
		state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
		state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	runner := &barrierEffectRunner{entered: make(chan struct{}, 2), release: make(chan struct{})}
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Runner: runner, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState, Cleanup: func(context.Context, agentruntime.EffectRequest) error { return nil }, VerifyCleanup: func(context.Context, agentruntime.EffectRequest) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	firstRequest := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, first, "")
	if _, err := coordinator.begin(t.Context(), mustOwnerSnapshot(t, owner), runtimeTestRequest(agentruntime.EffectMonitor, first, "")); !errors.Is(err, errStateConflict) {
		t.Fatalf("same-attempt pending effect err=%v", err)
	}
	secondRequest := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, second, "")
	done := make(chan error, 2)
	go func() { _, err := coordinator.execute(t.Context(), firstRequest); done <- err }()
	go func() { _, err := coordinator.execute(t.Context(), secondRequest); done <- err }()
	<-runner.entered
	<-runner.entered
	close(runner.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeEffectPrepareAllocatesFirstGenerationsAtomically(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Source: filepath.Join(root, "source"), VerifyWorker: func(context.Context) error { return nil }}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 75, Number: 1, BaseSHA: "abcdef1", Command: []string{"worker"}}
	manifest, err := agentruntime.PreparingManifest(attemptRoot, root, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	request, err := coordinator.begin(t.Context(), mustOwnerSnapshot(t, owner), agentruntime.EffectRequest{Action: agentruntime.EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true})
	if err != nil {
		t.Fatal(err)
	}
	if request.Identity.IssueGeneration != 1 || request.Identity.AttemptGeneration != 1 {
		t.Fatalf("identity=%#v", request.Identity)
	}
	snapshot := mustOwnerSnapshot(t, owner)
	key := ownerAttemptKey("o/r", 75, 1)
	if snapshot.State.Attempts[key].Generation != 1 || snapshot.State.Effects[request.Identity.EffectID].State != "pending" {
		t.Fatalf("snapshot=%#v", snapshot)
	}
	if _, err := coordinator.begin(t.Context(), snapshot, agentruntime.EffectRequest{Action: agentruntime.EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true}); !errors.Is(err, errStateConflict) {
		t.Fatalf("duplicate prepare err=%v", err)
	}
}

func TestRuntimeMonitorRetriesDifferentObservationAfterFinalizationFailure(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, 76, 1, "running")
	state := newRuntimeOwnerState("o/r")
	state.IssueGenerations[ownerIssueKey("o/r", 76)] = 1
	state.AttemptGenerations[ownerAttemptKey("o/r", 76, 1)] = 1
	state.Attempts[ownerAttemptKey("o/r", 76, 1)] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	writes := 0
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error {
		writes++
		if writes == 3 {
			return errors.New("injected finalization failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	runner := &monitorSequenceRunner{}
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Runner: runner, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	request := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
	if _, err := coordinator.execute(t.Context(), request); err == nil || err.Error() != "injected finalization failure" {
		t.Fatalf("finalization err=%v", err)
	}
	snapshot := mustOwnerSnapshot(t, owner)
	if snapshot.State.Effects[request.Identity.EffectID].State != "pending" {
		t.Fatalf("effect was not recoverable: %#v", snapshot.State.Effects[request.Identity.EffectID])
	}
	marker := filepath.Join(root, "runtime-effects", request.Identity.EffectID+".done")
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("repeatable monitor wrote a marker: %v", err)
	}
	result, err := coordinator.execute(t.Context(), request)
	if err != nil || result.Manifest.State != "failed" || result.Manifest.Diagnostic == "" {
		t.Fatalf("retry result=%#v err=%v", result, err)
	}
	snapshot = mustOwnerSnapshot(t, owner)
	if snapshot.State.Effects[request.Identity.EffectID].State != "completed" || snapshot.State.Attempts[ownerAttemptKey("o/r", 76, 1)].Manifest.State != "failed" {
		t.Fatalf("retry was not committed: %#v", snapshot.State)
	}
}

func TestRuntimeEffectCommitsDefinitiveFailureButLeavesAmbiguousWorkPending(t *testing.T) {
	for _, test := range []struct {
		name        string
		ambiguous   bool
		failPersist bool
		wantState   string
		wantEffect  string
	}{
		{name: "terminal failure", wantState: "failed", wantEffect: "completed"},
		{name: "ambiguous failure", ambiguous: true, wantState: "running", wantEffect: "pending"},
		{name: "terminal persistence failure", failPersist: true, wantState: "running", wantEffect: "pending"},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDir(t)
			attemptRoot := productionAttemptRoot(root)
			if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			manifest := ownerTestManifest(t, root, 77, 1, "running")
			state := runtimeEffectInitialState(manifest)
			writes := 0
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error {
				writes++
				if test.failPersist && writes == 3 {
					return errors.New("injected terminal persistence failure")
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Runner: &monitorFailureRunner{ambiguous: test.ambiguous}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
			coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
			if err != nil {
				t.Fatal(err)
			}
			request := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
			result, executeErr := coordinator.execute(t.Context(), request)
			if executeErr == nil {
				t.Fatal("monitor failure was not returned")
			}
			if !test.ambiguous && result.Manifest.State != "failed" {
				t.Fatalf("terminal result=%#v", result)
			}
			snapshot := mustOwnerSnapshot(t, owner)
			key := ownerAttemptKey("o/r", 77, 1)
			if snapshot.State.Attempts[key].Manifest.State != test.wantState || snapshot.State.Effects[request.Identity.EffectID].State != test.wantEffect {
				t.Fatalf("state=%#v effect=%#v err=%v", snapshot.State.Attempts[key], snapshot.State.Effects[request.Identity.EffectID], executeErr)
			}
		})
	}
}

func TestRuntimeEffectRecoveryUsesIntentEpochAndExactMarker(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, 78, 1, "running")
	persisted := runtimeEffectInitialState(manifest)
	persist := func(state runtimeOwnerState) error {
		persisted = cloneRuntimeOwnerState(state)
		return nil
	}
	owner, err := startTestStateOwner(t, root, persisted, persist)
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, VerifyWorker: func(context.Context) error { return nil }}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	request := runtimeTestRequest(agentruntime.EffectReview, manifest, "")
	request.Review = agentruntime.ReviewTransition{State: "clean"}
	request, err = coordinator.begin(t.Context(), mustOwnerSnapshot(t, owner), request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := coordinator.executor.Execute(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	pending := mustOwnerSnapshot(t, owner)
	effect := pending.State.Effects[request.Identity.EffectID]
	oldEpoch := effect.IntentEpoch
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}

	restarted, err := startTestStateOwner(t, root, persisted, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	restartedSnapshot := mustOwnerSnapshot(t, restarted)
	if restartedSnapshot.State.Epoch != oldEpoch+1 {
		t.Fatalf("restart epoch=%d old=%d", restartedSnapshot.State.Epoch, oldEpoch)
	}
	bad := effectRequestIdentity(effect)
	bad.Epoch = oldEpoch + 1
	if err := restarted.authorizeRuntimeEffect(t.Context(), authorizeRuntimeEffectCommand{Identity: ownerEffectIdentity(bad), Action: agentruntime.EffectReview}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("mismatched effect epoch err=%v", err)
	}
	recovery, err := newRuntimeEffectCoordinator(t.Context(), restarted, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	changed := request
	changed.Attempt.Context = "changed-after-success"
	verification, err := recovery.verifyPending(t.Context(), restartedSnapshot, effect, changed)
	if err != nil || verification.Disposition != agentruntime.EffectVerified {
		t.Fatalf("verification=%#v err=%v", verification, err)
	}
	final := mustOwnerSnapshot(t, restarted)
	if final.State.Effects[effect.ID].State != "completed" || final.State.Attempts[ownerAttemptKey("o/r", 78, 1)].Manifest.ReviewState != "clean" {
		t.Fatalf("recovered state=%#v", final.State)
	}
}

func TestRuntimeEffectPersistsDigestsWithoutRawSecretInputs(t *testing.T) {
	const contextCanary = "context-canary-never-persist"
	const credentialCanary = "credential-canary-never-persist"
	t.Setenv("TEST_EFFECT_CANARY", credentialCanary)
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, 84, 1, "preparing")
	owner, err := startTestStateOwner(t, root, runtimeEffectInitialState(manifest), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	runner := &barrierEffectRunner{}
	runtimeState := &agentruntime.Runtime{
		Root: attemptRoot, StateRoot: root, Runner: runner, Tmux: "tmux", Helper: "helper",
		AllowEnv: []string{"TEST_EFFECT_CANARY"}, VerifyWorker: func(context.Context) error { return nil },
	}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	request := runtimeTestRequest(agentruntime.EffectStart, manifest, "")
	request.Attempt.Context = contextCanary
	request = beginRuntimeTestEffectRequest(t, coordinator, owner, request)
	ledger, err := json.Marshal(mustOwnerSnapshot(t, owner).State)
	if err != nil {
		t.Fatal(err)
	}
	assertNoRuntimeEffectCanary(t, ledger, contextCanary, credentialCanary)
	if !strings.Contains(strings.Join(request.Runtime.Environment, "\n"), credentialCanary) {
		t.Fatal("effective environment omitted the allowed credential canary")
	}
	if _, err := coordinator.executor.Execute(t.Context(), request); err != nil {
		t.Fatal(err)
	}
	marker, err := os.ReadFile(filepath.Join(root, "runtime-effects", request.Identity.EffectID+".done"))
	if err != nil {
		t.Fatal(err)
	}
	assertNoRuntimeEffectCanary(t, marker, contextCanary, credentialCanary)
	verification, err := coordinator.executor.VerifyPending(t.Context(), request)
	if err != nil || verification.Disposition != agentruntime.EffectVerified {
		t.Fatalf("verification=%#v err=%v", verification, err)
	}
}

func assertNoRuntimeEffectCanary(t *testing.T, body []byte, canaries ...string) {
	t.Helper()
	for _, canary := range canaries {
		if strings.Contains(string(body), canary) {
			t.Fatalf("persisted raw canary %q", canary)
		}
	}
}

func TestRuntimeEffectOwnerRejectsInvalidResultTransitions(t *testing.T) {
	coordinator, owner, _, manifest := runtimeEffectTestCoordinator(t, 79)
	request := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
	for _, mutate := range []func(*agentruntime.Manifest){
		func(result *agentruntime.Manifest) { result.ReviewState = "changes-requested" },
		func(result *agentruntime.Manifest) { result.Branch = "as-79-999" },
		func(result *agentruntime.Manifest) { result.State = "cancelled" },
		func(result *agentruntime.Manifest) { result.Diagnostic = "forged running diagnostic" },
		func(result *agentruntime.Manifest) {
			result.State, result.Diagnostic = "completed", "forged completed diagnostic"
		},
		func(result *agentruntime.Manifest) { result.State, result.Diagnostic = "failed", "" },
	} {
		result := manifest
		mutate(&result)
		if _, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: agentruntime.EffectMonitor, Manifest: result}); !errors.Is(err, errStateConflict) {
			t.Fatalf("invalid result=%#v err=%v", result, err)
		}
	}
}

func TestRuntimeEffectOwnerRejectsPrepareSuccessDiagnostic(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Source: filepath.Join(root, "source"), VerifyWorker: func(context.Context) error { return nil }}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 89, Number: 1, BaseSHA: "abcdef1", Command: []string{"worker"}}
	manifest, err := agentruntime.PreparingManifest(attemptRoot, root, attempt, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	request, err := coordinator.begin(t.Context(), mustOwnerSnapshot(t, owner), agentruntime.EffectRequest{Action: agentruntime.EffectPrepare, Attempt: attempt, Manifest: manifest, Eligible: true})
	if err != nil {
		t.Fatal(err)
	}
	manifest.Diagnostic = "forged prepare diagnostic"
	if _, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: agentruntime.EffectPrepare, Manifest: manifest}); !errors.Is(err, errStateConflict) {
		t.Fatalf("forged Prepare err=%v", err)
	}
}

func TestStateOwnerRejectsInvalidRuntimeEffectActionTransitions(t *testing.T) {
	for _, test := range []struct {
		name   string
		action agentruntime.EffectAction
		state  string
		reason string
		review *agentruntime.ReviewTransition
	}{
		{name: "Start on running", action: agentruntime.EffectStart, state: "running"},
		{name: "Monitor on preparing", action: agentruntime.EffectMonitor, state: "preparing"},
		{name: "Stop on completed", action: agentruntime.EffectStop, state: "completed", reason: "operator stop"},
		{name: "Handoff on failed", action: agentruntime.EffectHandoff, state: "failed"},
		{name: "malformed Review", action: agentruntime.EffectReview, state: "running", review: &agentruntime.ReviewTransition{State: "invalid"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDir(t)
			manifest := ownerTestManifest(t, root, 90, 1, test.state)
			state := runtimeEffectInitialState(manifest)
			state.Epoch, state.Revision = 1, 1
			_, err := applyBeginRuntimeEffect(runtimeOwnerAttemptRoot(root), root, &state, beginRuntimeEffectCommand{
				Identity: stateResultIdentity{Epoch: 1, SourceRevision: 1, IssueGeneration: 1, AttemptGeneration: 1},
				Action:   test.action, Manifest: manifest, Reason: test.reason, RequestDigest: strings.Repeat("a", 64), Review: test.review,
			})
			if !errors.Is(err, errStateConflict) {
				t.Fatalf("invalid transition err=%v", err)
			}
		})
	}
}

func TestRuntimeMonitorEffectsRemainBounded(t *testing.T) {
	coordinator, owner, runner, manifest := runtimeEffectTestCoordinator(t, 80)
	runner.entered = nil
	close(runner.release)
	for range 8 {
		request := beginRuntimeTestEffect(t, coordinator, owner, agentruntime.EffectMonitor, manifest, "")
		result, err := coordinator.execute(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		manifest = result.Manifest
		snapshot := mustOwnerSnapshot(t, owner)
		if len(snapshot.State.Effects) != 1 {
			t.Fatalf("effect count=%d", len(snapshot.State.Effects))
		}
	}
}

func TestRuntimeTombstoneCleanupUsesTypedAtomicIntent(t *testing.T) {
	coordinator, owner, runner, manifest := runtimeEffectTestCoordinator(t, 81)
	runner.entered = nil
	close(runner.release)
	request := runtimeTestRequest(agentruntime.EffectCleanup, manifest, "")
	before := mustOwnerSnapshot(t, owner)
	if _, err := coordinator.begin(t.Context(), before, request); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("ordinary cleanup admission err=%v", err)
	}
	if after := mustOwnerSnapshot(t, owner); after.State.Revision != before.State.Revision || len(after.State.Effects) != 0 {
		t.Fatalf("ordinary cleanup changed state: %#v", after.State)
	}
	forget := runtimeTestRequest(agentruntime.EffectAction("forget"), manifest, "")
	if _, err := coordinator.begin(t.Context(), before, forget); err == nil {
		t.Fatal("legacy forget was admitted to the v2 effect path")
	}
	request, err := coordinator.executor.BindRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	digest, err := agentruntime.EffectRequestDigest(request)
	if err != nil {
		t.Fatal(err)
	}
	snapshot := mustOwnerSnapshot(t, owner)
	deleted, effect, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{
		Repository: "o/r", Issue: 81, Attempt: 1,
		ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1,
		Action: "abandoned", CleanupPhase: "cleanup-started", Manifest: &manifest, CleanupPolicy: &agentruntime.EffectCleanupPolicy{Action: "abandon"},
		EffectAction: string(agentruntime.EffectCleanup), EffectRequestDigest: digest,
	})
	if err != nil || effect == nil || effect.IntentEpoch != snapshot.State.Epoch || effect.RequestDigest != digest {
		t.Fatalf("deleted=%#v effect=%#v err=%v", deleted, effect, err)
	}
	request.Identity = effectRequestIdentity(*effect)
	forged := manifest
	forged.Diagnostic = "forged cleanup result"
	if _, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: agentruntime.EffectCleanup, Manifest: forged}); !errors.Is(err, errStateConflict) {
		t.Fatalf("forged Cleanup err=%v", err)
	}
	result, err := coordinator.execute(t.Context(), request)
	if err != nil || result.Disposition != agentruntime.EffectResultReady {
		t.Fatalf("result=%#v err=%v", result, err)
	}
	completed := mustOwnerSnapshot(t, owner)
	key := ownerAttemptKey("o/r", 81, 1)
	if completed.State.Tombstones[key].CleanupPhase != "completed" || completed.State.Effects[effect.ID].State != "completed" {
		t.Fatalf("completed=%#v", completed.State)
	}
}

func TestRuntimeOwnerLoadRejectsMalformedReviewEffectTransition(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 92, 1, "running")
	state := runtimeEffectInitialState(manifest)
	state.Epoch, state.Revision = 1, 2
	effect := runtimeEffectIntent{
		Action: string(agentruntime.EffectReview), Repository: "o/r", Issue: 92, Attempt: 1,
		IssueGeneration: 1, AttemptGeneration: 1, IntentEpoch: 1, IntentRevision: 2,
		State: "pending", RequestDigest: strings.Repeat("a", 64), Review: &agentruntime.ReviewTransition{State: "invalid"},
	}
	effect.ID = runtimeEffectID(effect)
	state.Effects[effect.ID] = effect
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, runtimeOwnerStateFile), body, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readRuntimeOwnerState(root, "o/r"); err == nil || !strings.Contains(err.Error(), "review effect transition") {
		t.Fatalf("corrupt review ledger err=%v", err)
	}
}

type monitorFailureRunner struct{ ambiguous bool }

func (r *monitorFailureRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("missing operation")
	}
	switch command.Args[0] {
	case "display-message":
		if r.ambiguous {
			return agentruntime.Result{}, errors.New("observation unavailable")
		}
		return agentruntime.Result{Output: "1|9||9\n"}, nil
	case "capture-pane":
		return agentruntime.Result{}, errors.New("capture failed")
	default:
		return agentruntime.Result{}, nil
	}
}

type cancelledStartRunner struct{ entered chan struct{} }

func (r *cancelledStartRunner) Run(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if err := ctx.Err(); err != nil {
		return agentruntime.Result{}, err
	}
	if len(command.Args) > 0 && command.Args[0] == "respawn-pane" {
		close(r.entered)
		<-ctx.Done()
		return agentruntime.Result{}, ctx.Err()
	}
	return agentruntime.Result{}, nil
}

type monitorSequenceRunner struct{ displays atomic.Int32 }

func (r *monitorSequenceRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("missing operation")
	}
	switch command.Args[0] {
	case "display-message":
		if r.displays.Add(1) == 1 {
			return agentruntime.Result{Output: "0|||\n"}, nil
		}
		return agentruntime.Result{Output: "1|7||7\n"}, nil
	case "capture-pane":
		return agentruntime.Result{Output: "worker failed\n"}, nil
	default:
		return agentruntime.Result{}, nil
	}
}

type barrierEffectRunner struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
}

func (r *barrierEffectRunner) Run(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	r.calls.Add(1)
	if len(command.Args) > 0 && command.Args[0] == "has-session" {
		return agentruntime.Result{Code: 1, Exited: true}, errors.New("missing session")
	}
	if r.entered != nil {
		r.entered <- struct{}{}
	}
	if r.release != nil {
		select {
		case <-r.release:
		case <-ctx.Done():
			return agentruntime.Result{}, ctx.Err()
		}
	}
	return agentruntime.Result{Output: "0|||\n"}, nil
}

func runtimeEffectTestCoordinator(t *testing.T, issue int) (*runtimeEffectCoordinator, *stateOwner, *barrierEffectRunner, agentruntime.Manifest) {
	t.Helper()
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, issue, 1, "running")
	state := runtimeEffectInitialState(manifest)
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	runner := &barrierEffectRunner{entered: make(chan struct{}, 1), release: make(chan struct{})}
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Runner: runner, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	coordinator, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState, Cleanup: func(context.Context, agentruntime.EffectRequest) error { return nil }, VerifyCleanup: func(context.Context, agentruntime.EffectRequest) (bool, error) { return true, nil }})
	if err != nil {
		t.Fatal(err)
	}
	return coordinator, owner, runner, manifest
}

func runtimeEffectInitialState(manifest agentruntime.Manifest) runtimeOwnerState {
	state := newRuntimeOwnerState(manifest.Repository)
	state.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)] = 1
	state.AttemptGenerations[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)] = 1
	state.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	return state
}

func beginRuntimeTestEffect(t *testing.T, coordinator *runtimeEffectCoordinator, owner *stateOwner, action agentruntime.EffectAction, manifest agentruntime.Manifest, reason string) agentruntime.EffectRequest {
	t.Helper()
	return beginRuntimeTestEffectRequest(t, coordinator, owner, runtimeTestRequest(action, manifest, reason))
}

func beginRuntimeTestEffectRequest(t *testing.T, coordinator *runtimeEffectCoordinator, owner *stateOwner, request agentruntime.EffectRequest) agentruntime.EffectRequest {
	t.Helper()
	request, err := coordinator.begin(t.Context(), mustOwnerSnapshot(t, owner), request)
	if err != nil {
		t.Fatal(err)
	}
	return request
}

func runtimeTestRequest(action agentruntime.EffectAction, manifest agentruntime.Manifest, reason string) agentruntime.EffectRequest {
	request := agentruntime.EffectRequest{Action: action, Attempt: agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA, Command: []string{"worker"}, Interactive: manifest.Interactive}, Manifest: manifest, Eligible: true, Reason: reason}
	if action == agentruntime.EffectCleanup {
		request.Cleanup.Action = "abandon"
	}
	return request
}

func mustOwnerSnapshot(t *testing.T, owner *stateOwner) stateOwnerSnapshot {
	t.Helper()
	snapshot, err := owner.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}
