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
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	writeLegacyStateFixture(t, owner.stateRoot, "dashboard-state.json", dashboardState{Version: dashboardStateVersion, Hidden: nil})
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
	if response.Code != http.StatusAccepted {
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

func TestDismissReviewerOnlyCleanupCompletesWithoutDeletingAttemptArtifacts(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 398, "completed", true)
	for _, path := range []string{manifest.Worktree, filepath.Dir(manifest.LogPath)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(manifest.LogPath, []byte("kept\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	request := operatorRequest("dismiss-reviewer-only", "dismiss", manifest, false)
	result := service.performSynchronously(t.Context(), request)
	if !result.OK || result.Status != http.StatusOK {
		t.Fatalf("Dismiss did not finish reviewer-only cleanup: %#v", result)
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	tombstone := state.Tombstones[key]
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	if _, live := state.Attempts[key]; live || !ok || receipt.State != "completed" || receipt.EffectID == "" || tombstone.CleanupPhase != "completed" || tombstone.EffectID != receipt.EffectID || tombstone.ReviewerLeaseID != "" {
		t.Fatalf("Dismiss owner state is incomplete: receipt=%#v tombstone=%#v", receipt, tombstone)
	}
	for _, path := range []string{manifest.Worktree, manifest.LogPath} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("Dismiss deleted implementation artifact %s: %v", path, err)
		}
	}
}

func TestV2DashboardDismissSurvivesAliveMonitorDuringGitHubPreflight(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 350, 1, "running")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "orphaned", true)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, productionAttemptRoot(root), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	service := operatorTestMutationService(t, owner)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	service.issueClosed = func(ctx context.Context, repository string, issue int) (bool, error) {
		if repository != manifest.Repository || issue != manifest.Issue {
			return false, fmt.Errorf("unexpected GitHub issue %s#%d", repository, issue)
		}
		close(entered)
		select {
		case <-release:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	monitor, err := service.effects.begin(t.Context(), mustOwnerSnapshot(t, owner), runtimeTestRequest(agentruntime.EffectMonitor, manifest, ""))
	if err != nil {
		t.Fatal(err)
	}
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=350&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		server.handler(http.NotFoundHandler()).ServeHTTP(recorder, request)
		response <- recorder
	}()
	<-entered
	if _, err := service.effects.execute(t.Context(), monitor); err != nil {
		t.Fatal(err)
	}
	afterMonitor := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	once.Do(func() { close(release) })
	result := <-response
	if result.Code != http.StatusAccepted {
		t.Fatalf("alive Monitor made Dismiss fail: HTTP %d body=%s", result.Code, result.Body.String())
	}
	if !reflect.DeepEqual(afterMonitor.Attempts[key].Manifest, manifest) || afterMonitor.Effects[monitor.Identity.EffectID].State != "completed" {
		t.Fatalf("alive Monitor changed manifest or did not finish: manifest=%#v effect=%#v", afterMonitor.Attempts[key].Manifest, afterMonitor.Effects[monitor.Identity.EffectID])
	}
	persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil || persisted.Tombstones[key].Action != "dismissed" {
		t.Fatalf("Dismiss did not durably invalidate attempt: tombstone=%#v err=%v", persisted.Tombstones[key], err)
	}
	if _, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(monitor.Identity), Action: agentruntime.EffectMonitor, Manifest: manifest}); err == nil {
		t.Fatal("old Monitor result could finish after Dismiss invalidated the attempt")
	}
}

func TestOperatorActionsReserveExactManifestDuringPreflight(t *testing.T) {
	for index, action := range []string{"dismiss", "archive", "abandon", "remove", "cancel"} {
		t.Run(action, func(t *testing.T) {
			issue := 450 + index
			var owner *stateOwner
			var manifest agentruntime.Manifest
			switch action {
			case "dismiss":
				owner, manifest = operatorTestOwner(t, issue, "completed", true)
			case "cancel":
				owner, manifest = operatorTestOwner(t, issue, "active", false)
			default:
				owner, manifest = operatorCleanupRestartOwner(t, issue, action)
			}
			if err := os.MkdirAll(productionSnapshotRoot(owner.stateRoot), 0o700); err != nil {
				t.Fatal(err)
			}

			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			block := func() {
				once.Do(func() { close(entered) })
				<-release
			}
			boundary := &operatorAdmissionBarrier{block: block}
			service := operatorServiceWithCleanup(t, owner, t.Context(), boundary)
			service.effects.stopped = true
			service.issueClosed = func(context.Context, string, int) (bool, error) {
				if action == "dismiss" {
					block()
				}
				return true, nil
			}
			if action == "cancel" {
				service.beforeAdmission = block
			}
			var stale finishRuntimeEffectCommand
			if action == "abandon" {
				stale = prepareWorkerStatusManifestChange(t, owner, manifest.Repository, manifest.Issue, manifest.Attempt)
			} else {
				stale = prepareReviewManifestChange(t, owner, manifest.Repository, manifest.Issue, manifest.Attempt)
			}

			result := make(chan controlResult, 1)
			request := operatorRequest("manifest-change-"+action, action, manifest, action != "dismiss" && action != "cancel")
			go func() { result <- service.perform(t.Context(), request) }()
			<-entered

			before := mustOwnerSnapshot(t, owner).State
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			generation := before.AttemptGenerations[key]
			if receipt, ok := operatorReceiptByID(before, request.RequestID); !ok || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil {
				t.Fatalf("owner did not durably reserve operator admission: %#v", receipt)
			}
			if _, err := owner.finishRuntimeEffect(t.Context(), stale); !errors.Is(err, errStaleStateResult) {
				t.Fatalf("reserved manifest accepted stale reconciliation result: %v", err)
			}
			changed := mustOwnerSnapshot(t, owner).State
			if changed.AttemptGenerations[key] != generation {
				t.Fatalf("operator admission changed generation: before=%d after=%d", generation, changed.AttemptGenerations[key])
			}
			if !reflect.DeepEqual(changed.Attempts[key].Manifest, before.Attempts[key].Manifest) {
				t.Fatal("durable admission did not preserve its exact manifest")
			}
			close(release)

			got := <-result
			if !got.OK || got.Status != http.StatusAccepted {
				t.Fatalf("same-generation reconciliation change caused operator failure: %#v", got)
			}
			committed := mustOwnerSnapshot(t, owner).State
			receipt, ok := operatorReceiptByID(committed, request.RequestID)
			if !ok || receipt.State != "pending" || receipt.EffectID == "" {
				t.Fatalf("operator mutation was not durably committed: %#v", receipt)
			}
			if action == "cancel" {
				if committed.Effects[receipt.EffectID].Action != string(agentruntime.EffectStop) {
					t.Fatalf("cancel did not commit Stop: %#v", committed.Effects[receipt.EffectID])
				}
			} else if tombstone := committed.Tombstones[key]; tombstone.Action != operatorTombstoneAction(action) {
				t.Fatalf("%s did not invalidate the attempt: %#v", action, tombstone)
			}
		})
	}
}

func TestOperatorAdmissionPreservesEligibilityAndPersistenceFailure(t *testing.T) {
	t.Run("eligibility", func(t *testing.T) {
		owner, manifest := operatorTestOwner(t, 455, "completed", true)
		service := operatorTestMutationService(t, owner)
		var calls atomic.Int32
		service.issueClosed = func(context.Context, string, int) (bool, error) {
			calls.Add(1)
			return false, nil
		}
		got := service.perform(t.Context(), operatorRequest("stale-eligibility", "dismiss", manifest, false))
		if got.OK || got.Status != http.StatusConflict || !strings.Contains(got.Error, "GitHub issue is open") || calls.Load() != 1 {
			t.Fatalf("invalid eligibility was bypassed: result=%#v calls=%d", got, calls.Load())
		}
		state := mustOwnerSnapshot(t, owner).State
		receipt, ok := operatorReceiptByID(state, "stale-eligibility")
		if !ok || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusConflict || len(state.Tombstones) != 0 {
			t.Fatalf("invalid action did not durably record its rejected admission: receipt=%#v tombstones=%#v", receipt, state.Tombstones)
		}
	})

	t.Run("persistence", func(t *testing.T) {
		root := resolvedTempDir(t)
		manifest := ownerTestManifest(t, root, 456, 1, "completed")
		state := runtimeEffectInitialState(manifest)
		addOperatorObservation(&state, manifest, "completed", true)
		state.Epoch, state.Revision = 1, 1
		var fail atomic.Bool
		owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error {
			if fail.Load() {
				return errors.New("injected stale-retry persistence failure")
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = owner.close(context.Background()) })
		refreshOperatorObservation(t, owner)
		service := operatorTestMutationService(t, owner)
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		service.issueClosed = func(context.Context, string, int) (bool, error) {
			once.Do(func() { close(entered) })
			<-release
			return true, nil
		}
		stale := prepareReviewManifestChange(t, owner, manifest.Repository, manifest.Issue, manifest.Attempt)
		result := make(chan controlResult, 1)
		go func() {
			result <- service.perform(t.Context(), operatorRequest("stale-persistence", "dismiss", manifest, false))
		}()
		<-entered
		if _, err := owner.finishRuntimeEffect(t.Context(), stale); !errors.Is(err, errStaleStateResult) {
			t.Fatalf("reserved Dismiss accepted stale manifest result: %v", err)
		}
		fail.Store(true)
		close(release)
		got := <-result
		if got.OK || got.Status != http.StatusInternalServerError || !strings.Contains(got.Error, "injected stale-retry persistence failure") {
			t.Fatalf("persistence failure was hidden by stale recomputation: %#v", got)
		}
		state = mustOwnerSnapshot(t, owner).State
		receipt, ok := operatorReceiptByID(state, "stale-persistence")
		if !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil || len(state.Tombstones) != 0 {
			t.Fatalf("failed persistence did not retain the durable admission: receipt=%#v tombstones=%#v", receipt, state.Tombstones)
		}
	})
}

func TestOperatorAdmissionCommitsDespiteRepeatedSameGenerationResults(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 457, "active", false)
	service := operatorTestMutationService(t, owner)
	service.effects.stopped = true
	finish := prepareMonitorTimestampChange(t, owner, manifest.Repository, manifest.Issue, manifest.Attempt)
	var admissions atomic.Int32
	service.beforeAdmission = func() {
		for range 100 {
			admissions.Add(1)
			if _, err := owner.finishRuntimeEffect(t.Context(), finish); !errors.Is(err, errStaleStateResult) {
				t.Fatalf("reserved admission accepted stale result: %v", err)
			}
		}
	}
	result := service.perform(t.Context(), operatorRequest("continuous-churn", "cancel", manifest, false))
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("result=%#v", result)
	}
	if admissions.Load() != 100 {
		t.Fatalf("admissions=%d want=100", admissions.Load())
	}
	state := mustOwnerSnapshot(t, owner).State
	if receipt, ok := operatorReceiptByID(state, "continuous-churn"); !ok || receipt.Phase != operatorPhaseStopPending || receipt.EffectID == "" {
		t.Fatalf("admission did not commit after repeated stale results: %#v", state.ControlReceipts)
	}
}

func TestOwnerRejectsMonitorResultAfterCancelAdmission(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 461, "active", false)
	monitor := prepareMonitorTimestampChange(t, owner, manifest.Repository, manifest.Issue, manifest.Attempt)
	service := operatorTestMutationService(t, owner)
	service.effects.stopped = true
	request := operatorRequest("cancel-before-monitor", "cancel", manifest, false)
	result := service.perform(t.Context(), request)
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("Cancel admission failed: %#v", result)
	}
	if _, err := owner.finishRuntimeEffect(t.Context(), monitor); err == nil {
		t.Fatal("stale Monitor result overwrote an admitted Stop")
	}
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	if !ok || receipt.Phase != operatorPhaseStopPending || state.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].StopEffectID != receipt.EffectID {
		t.Fatalf("Stop admission was not preserved: receipt=%#v", receipt)
	}
}

func TestOperatorAdmissionCancellationReleasesReservation(t *testing.T) {
	owner, manifest := operatorCleanupRestartOwner(t, 458, "archive")
	entered := make(chan struct{})
	service := operatorServiceWithCleanup(t, owner, t.Context(), &cancellableAdmissionBarrier{entered: entered})
	service.effects.stopped = true
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan controlResult, 1)
	request := operatorRequest("cancel-admission", "archive", manifest, true)
	go func() { result <- service.perform(ctx, request) }()
	<-entered
	reserved := mustOwnerSnapshot(t, owner).State
	if receipt, ok := operatorReceiptByID(reserved, request.RequestID); !ok || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil {
		t.Fatalf("slow preflight was not durably reserved: %#v", receipt)
	}
	cancel()
	got := <-result
	if got.OK || got.Status != http.StatusRequestTimeout {
		t.Fatalf("cancelled admission result=%#v", got)
	}
	state := mustOwnerSnapshot(t, owner).State
	if _, ok := operatorReceiptByID(state, request.RequestID); ok || len(state.Tombstones) != 0 {
		t.Fatalf("cancelled admission remained reserved or mutated attempt: %#v", state)
	}
}

func TestConcurrentDestructiveAdmissionsReserveAttemptOnce(t *testing.T) {
	owner, manifest := operatorCleanupRestartOwner(t, 459, "archive")
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorAdmissionBarrier{block: func() {
		once.Do(func() { close(entered) })
		<-release
	}})
	service.effects.stopped = true
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	archive := operatorRequest("reserve-archive", "archive", manifest, true)
	first := make(chan controlResult, 1)
	go func() { first <- service.perform(t.Context(), archive) }()
	<-entered
	dismiss := service.perform(t.Context(), operatorRequest("reserve-dismiss", "dismiss", manifest, false))
	if dismiss.OK || dismiss.Status != http.StatusConflict {
		t.Fatalf("second destructive reservation was not rejected: %#v", dismiss)
	}
	close(release)
	if got := <-first; !got.OK || got.Status != http.StatusAccepted {
		t.Fatalf("first destructive admission failed: %#v", got)
	}
	state := mustOwnerSnapshot(t, owner).State
	if tombstone := state.Tombstones[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)]; tombstone.Action != "archived" {
		t.Fatalf("winning action was not committed exactly once: %#v", tombstone)
	}
}

func TestClosedLocalOrphanOperatorAdmissionAfterIssueFallsOutOfCollector(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 191, "orphaned", true)
	applyReconciliationInput(t, owner, reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: manifest.Repository}, Complete: true})
	snapshot := mustOwnerSnapshot(t, owner)
	if snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Present {
		t.Fatal("fixture did not reproduce absent issue observation")
	}
	for _, action := range []string{"dismiss", "abandon"} {
		_, _, err := operatorAttempt(snapshot, operatorRequest("orphan-"+action, action, manifest, action == "abandon"))
		if err != nil {
			t.Fatalf("%s of exact local orphan rejected: %v", action, err)
		}
	}
}

func TestHistoricalClosedLocalOrphanDashboardActionsCommitTombstones(t *testing.T) {
	for _, action := range []string{"dismiss", "abandon"} {
		t.Run(action, func(t *testing.T) {
			owner, manifest := operatorTestOwner(t, 191, "orphaned", true)
			applyReconciliationInput(t, owner, reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: manifest.Repository}, Complete: true})
			if err := os.MkdirAll(productionSnapshotRoot(owner.stateRoot), 0o700); err != nil {
				t.Fatal(err)
			}
			service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
			service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
			service.effects.stopped = true // Verify admission before external cleanup can run.
			server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
			request := httptest.NewRequest(http.MethodPost, fmt.Sprintf("http://localhost/actions/%s?repository=o%%2Fr&issue=191&attempt=1", action), nil)
			request.Host = "localhost"
			request.Header.Set("Origin", "http://localhost")
			response := httptest.NewRecorder()
			server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
			wantStatus := http.StatusAccepted
			if response.Code != wantStatus {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			state := mustOwnerSnapshot(t, owner).State
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			if _, exists := state.Attempts[key]; exists || state.Tombstones[key].Action != operatorTombstoneAction(action) {
				t.Fatalf("owner did not durably invalidate orphan: %#v", state)
			}
		})
	}
}

func TestOpenLocalOrphanDismissRejectsWithoutInvalidatingAndAbandonNeedsNoGitHubRead(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 193, "orphaned", false)
	applyReconciliationInput(t, owner, reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: manifest.Repository}, Complete: true})
	if err := os.MkdirAll(productionSnapshotRoot(owner.stateRoot), 0o700); err != nil {
		t.Fatal(err)
	}
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	service.effects.stopped = true
	reads := 0
	service.issueClosed = func(context.Context, string, int) (bool, error) { reads++; return false, nil }
	before := mustOwnerSnapshot(t, owner).State
	dismiss := service.perform(t.Context(), operatorRequest("open-orphan-dismiss", "dismiss", manifest, false))
	if dismiss.Status != http.StatusConflict || !strings.Contains(dismiss.Error, "GitHub issue is open") || reads != 1 {
		t.Fatalf("dismiss=%#v GitHub reads=%d", dismiss, reads)
	}
	after := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if after.AttemptGenerations[key] != before.AttemptGenerations[key] || !reflect.DeepEqual(after.Attempts[key], before.Attempts[key]) || len(after.Tombstones) != len(before.Tombstones) {
		t.Fatalf("rejected dismissal changed owner state: before=%#v after=%#v", before, after)
	}
	receipt, ok := operatorReceiptByID(after, "open-orphan-dismiss")
	if !ok || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusConflict {
		t.Fatalf("rejected dismissal was not durably recorded: %#v", receipt)
	}
	abandon := service.perform(t.Context(), operatorRequest("open-orphan-abandon", "abandon", manifest, true))
	if abandon.Status != http.StatusAccepted || !abandon.OK || reads != 1 || mustOwnerSnapshot(t, owner).State.Tombstones[key].Action != "abandoned" {
		t.Fatalf("abandon=%#v GitHub reads=%d", abandon, reads)
	}
}

func TestRemoteOnlyCompletedDashboardActionUsesPublishedAttemptIdentity(t *testing.T) {
	for _, test := range []struct {
		name, action string
		closed       bool
	}{{"closed-archive", "archive", true}, {"open-archive", "archive", false}, {"closed-dismiss", "dismiss", true}} {
		t.Run(test.name, func(t *testing.T) {
			root := resolvedTempDir(t)
			manifest := ownerTestManifest(t, root, 160, 1, "completed")
			state := runtimeEffectInitialState(manifest)
			addOperatorObservation(&state, manifest, "completed", test.closed)
			delete(state.Attempts, ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt))
			state.Epoch, state.Revision = 1, 1
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			refreshOperatorObservation(t, owner)
			service := operatorTestMutationService(t, owner)
			service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, issueNumber int) (reconciliationV2Batch, error) {
				observation := snapshot.State.Observations[ownerIssueKey(manifest.Repository, issueNumber)]
				attempt := observation.Attempts[ownerAttemptKey(manifest.Repository, issueNumber, 1)]
				return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: manifest.Repository, Issue: issueNumber}, Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(attempt.Fact)}}}, nil
			}
			before, err := projectOwnerStatus(mustOwnerSnapshot(t, owner), 1, time.Unix(1, 0))
			if err != nil || len(before.Statuses) != 1 || before.Statuses[0].Attempt != 1 || before.Statuses[0].State != "completed" {
				t.Fatalf("before=%#v err=%v", before, err)
			}
			result := service.perform(t.Context(), operatorRequest("remote-"+test.name, test.action, manifest, test.action == "archive"))
			if !result.OK || result.Status != http.StatusOK {
				t.Fatalf("result=%#v", result)
			}
			committed := mustOwnerSnapshot(t, owner)
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, 1)
			if tombstone := committed.State.Tombstones[key]; tombstone.Action != operatorTombstoneAction(test.action) || tombstone.Manifest != nil || tombstone.CleanupPhase != "completed" {
				t.Fatalf("remote-only tombstone=%#v", tombstone)
			}
			after, err := projectOwnerStatus(committed, 1, time.Unix(2, 0))
			if err != nil || len(after.Statuses) != 0 {
				t.Fatalf("after=%#v err=%v", after, err)
			}
		})
	}
}

func TestRemoteOnlyAdmissionRejectsOldCollectionAfterNewerInvalidatingObservation(t *testing.T) {
	for _, action := range []string{"archive", "dismiss"} {
		t.Run(action, func(t *testing.T) {
			root := resolvedTempDir(t)
			manifest := ownerTestManifest(t, root, 163, 1, "completed")
			state := runtimeEffectInitialState(manifest)
			addOperatorObservation(&state, manifest, "completed", true)
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			delete(state.Attempts, key)
			state.Epoch, state.Revision = 1, 1
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			refreshOperatorObservation(t, owner)
			service := operatorTestMutationService(t, owner)
			service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
			entered, release := make(chan struct{}), make(chan struct{})
			service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
				close(entered)
				<-release
				observation := snapshot.State.Observations[ownerIssueKey(manifest.Repository, issue)]
				attempt := observation.Attempts[key]
				return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: manifest.Repository, Issue: issue}, Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(attempt.Fact)}}}, nil
			}
			request := operatorRequest("remote-cycle-"+action, action, manifest, action == "archive")
			result := make(chan controlResult, 1)
			go func() { result <- service.perform(t.Context(), request) }()
			<-entered
			reserved := mustOwnerSnapshot(t, owner).State
			if receipt, ok := operatorReceiptByID(reserved, request.RequestID); !ok || receipt.Admission == nil || !receipt.Admission.RemoteOnly {
				t.Fatalf("remote action was not reserved: %#v", receipt)
			}
			cycle, err := owner.reconciliationSnapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			collection := mustCollection(t, cycle, reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: manifest.Repository, Issue: manifest.Issue}, Complete: true})
			if _, err := owner.applyReconciliation(t.Context(), collection); err != nil {
				t.Fatalf("newer invalidating observation was rejected: %v", err)
			}
			invalidated := mustOwnerSnapshot(t, owner).State
			receipt, ok := operatorReceiptByID(invalidated, request.RequestID)
			if !ok || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusConflict || invalidated.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Present {
				t.Fatalf("newer observation did not durably invalidate admission: receipt=%#v observation=%#v", receipt, invalidated.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)])
			}
			close(release)
			got := <-result
			if got.OK || got.Status != http.StatusConflict {
				t.Fatalf("old remote collection overrode newer observation: %#v", got)
			}
			committed := mustOwnerSnapshot(t, owner).State
			if _, exists := committed.Tombstones[key]; exists {
				t.Fatalf("stale remote action created tombstone: %#v", committed.Tombstones[key])
			}
		})
	}
}

func TestRemoteOnlyDismissReservationUsesOwnerCurrentObservation(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 164, 1, "completed")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "completed", false)
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	delete(state.Attempts, key)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)

	stale := mustOwnerSnapshot(t, owner)
	cycle, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	issueKey := ownerIssueKey(manifest.Repository, manifest.Issue)
	observation := cycle.State.Observations[issueKey]
	attempt := observation.Attempts[key]
	issue := expandIssueFact(observation.Fact)
	issue.Closed = true
	collection := mustCollection(t, cycle, reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(attempt.Fact)}})
	if _, err := owner.applyReconciliation(t.Context(), collection); err != nil {
		t.Fatal(err)
	}

	request := operatorRequest("remote-owner-current-dismiss", "dismiss", manifest, false)
	reserved, err := owner.reserveOperatorAdmission(t.Context(), reserveOperatorAdmissionCommand{Request: request})
	if err != nil {
		t.Fatalf("stale caller observation prevented owner-current reservation: stale=%#v err=%v", stale.State.Observations[issueKey], err)
	}
	receipt, ok := operatorReceiptByID(reserved.State, request.RequestID)
	current := reserved.State.Observations[issueKey]
	if !ok || receipt.Admission == nil || !receipt.Admission.RemoteOnly || !receipt.Admission.IssueClosed || receipt.Admission.ObservationCycleID != current.LastCycleID || receipt.Admission.ObservationCycleID == stale.State.Observations[issueKey].LastCycleID {
		t.Fatalf("reservation did not bind owner-current closed observation: receipt=%#v current=%#v stale=%#v", receipt, current, stale.State.Observations[issueKey])
	}
}

func TestRemoteOnlyAdmissionResumesAfterRestart(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 164, 1, "completed")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "completed", true)
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	delete(state.Attempts, key)
	state.Epoch, state.Revision = 1, 1
	persisted := cloneRuntimeOwnerState(state)
	persist := func(value runtimeOwnerState) error { persisted = cloneRuntimeOwnerState(value); return nil }
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	refreshOperatorObservation(t, owner)
	request := operatorRequest("remote-restart-admission", "archive", manifest, true)
	if _, err := owner.reserveOperatorAdmission(t.Context(), reserveOperatorAdmissionCommand{Request: request}); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, persisted, persist)
	if err != nil {
		t.Fatalf("restart with pending remote admission: %v", err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	service := operatorTestMutationService(t, restarted)
	service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		observation := snapshot.State.Observations[ownerIssueKey(manifest.Repository, issue)]
		attempt := observation.Attempts[key]
		return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(attempt.Fact)}}}, nil
	}
	if err := service.resumeReceipt(t.Context(), request.RequestID); err != nil {
		t.Fatal(err)
	}
	committed := mustOwnerSnapshot(t, restarted).State
	if receipt, ok := operatorReceiptByID(committed, request.RequestID); !ok || receipt.State != "completed" || committed.Tombstones[key].Action != "archived" {
		t.Fatalf("remote admission did not survive restart: receipt=%#v tombstone=%#v", receipt, committed.Tombstones[key])
	}
}

func TestRemoteOnlyActionStartsAfterRestartBeforeReconciliation(t *testing.T) {
	for _, action := range []string{"archive", "dismiss"} {
		t.Run(action, func(t *testing.T) {
			root := resolvedTempDir(t)
			manifest := ownerTestManifest(t, root, 165, 1, "completed")
			state := runtimeEffectInitialState(manifest)
			addOperatorObservation(&state, manifest, "completed", true)
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			delete(state.Attempts, key)
			state.Epoch, state.Revision = 1, 1
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			service := operatorTestMutationService(t, owner)
			service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
				observation := snapshot.State.Observations[ownerIssueKey(manifest.Repository, issue)]
				attempt := observation.Attempts[key]
				return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(attempt.Fact)}}}, nil
			}
			request := operatorRequest("remote-after-restart-"+action, action, manifest, action == "archive")
			if result := service.perform(t.Context(), request); !result.OK || result.Status != http.StatusOK {
				t.Fatalf("%s after restart=%#v", action, result)
			}
			committed := mustOwnerSnapshot(t, owner).State
			if committed.Tombstones[key].Action != operatorTombstoneAction(action) || len(committed.Attempts) != 0 {
				t.Fatalf("%s tombstone=%#v attempts=%#v", action, committed.Tombstones[key], committed.Attempts)
			}
		})
	}
}

func TestRemoteOnlyTombstoneReplaysAfterReceiptEvictionAndRestart(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 164, 1, "completed")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "completed", true)
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	delete(state.Attempts, key)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	refreshOperatorObservation(t, owner)
	service := operatorTestMutationService(t, owner)
	service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		observation := snapshot.State.Observations[ownerIssueKey(manifest.Repository, issue)]
		attempt := observation.Attempts[key]
		return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: manifest.Repository, Issue: issue}, Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(attempt.Fact)}}}, nil
	}
	request := operatorRequest("remote-original", "archive", manifest, true)
	if result := service.perform(t.Context(), request); !result.OK {
		t.Fatalf("first archive=%#v", result)
	}
	persisted := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	for index := range maxControlReceipts {
		dummy := operatorRequest(fmt.Sprintf("evict-%03d", index), "archive", manifest, true)
		if err := appendOperatorReceipt(&persisted, controlReceipt{Request: dummy, State: "completed", Phase: operatorPhaseCompleted, Result: successfulOperatorResult(dummy, persisted.Revision)}); err != nil {
			t.Fatal(err)
		}
	}
	if _, exists := operatorReceiptByID(persisted, request.RequestID); exists {
		t.Fatal("fixture did not evict original receipt")
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, persisted, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	replayService := operatorTestMutationService(t, restarted)
	replayService.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		t.Fatal("replay must not collect GitHub")
		return reconciliationV2Batch{}, errStateConflict
	}
	if result := replayService.perform(t.Context(), request); !result.OK || result.Status != http.StatusOK {
		t.Fatalf("same-action replay=%#v", result)
	}
	other := operatorRequest("remote-opposite", "dismiss", manifest, false)
	if result := replayService.perform(t.Context(), other); result.Status != http.StatusConflict || result.OK {
		t.Fatalf("cross-action replay=%#v", result)
	}
	final := mustOwnerSnapshot(t, restarted).State
	if final.Tombstones[key].Action != "archived" || final.AttemptGenerations[key] != 2 || len(final.Attempts) != 0 {
		t.Fatalf("replay changed tombstone=%#v", final.Tombstones[key])
	}
}

func TestLocalTombstoneReplayIgnoresLaterMissingObservation(t *testing.T) {
	for index, action := range []string{"dismiss", "archive", "abandon", "remove"} {
		t.Run(action, func(t *testing.T) {
			var owner *stateOwner
			var manifest agentruntime.Manifest
			if action == "dismiss" {
				owner, manifest = operatorTestOwner(t, 370+index, "completed", true)
			} else {
				owner, manifest = operatorCleanupRestartOwner(t, 370+index, action)
				if err := os.MkdirAll(productionSnapshotRoot(owner.stateRoot), 0o700); err != nil {
					t.Fatal(err)
				}
			}
			service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
			service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
			service.stopped = true // Keep cleanup pending so replay can prove it does not create another effect.
			confirm := action != "dismiss"
			first := service.perform(t.Context(), operatorRequest(action+"-first", action, manifest, confirm))
			want := http.StatusAccepted
			if !first.OK || first.Status != want {
				t.Fatalf("first action=%#v", first)
			}
			before := mustOwnerSnapshot(t, owner).State
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			if before.Tombstones[key].Action != operatorTombstoneAction(action) {
				t.Fatalf("first action did not tombstone attempt: %#v", before.Tombstones[key])
			}
			applyReconciliationInput(t, owner, reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: manifest.Repository}, Complete: true})
			if mustOwnerSnapshot(t, owner).State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Present {
				t.Fatal("fixture did not remove the prior issue observation")
			}
			replay := service.perform(t.Context(), operatorRequest(action+"-replay", action, manifest, confirm))
			if !replay.OK || replay.Status != want {
				t.Fatalf("same-action replay after observation loss=%#v", replay)
			}
			cross := service.perform(t.Context(), operatorRequest(action+"-cross", "cancel", manifest, false))
			if cross.OK || cross.Status != http.StatusConflict {
				t.Fatalf("cross-action replay=%#v", cross)
			}
			after := mustOwnerSnapshot(t, owner).State
			if !reflect.DeepEqual(after.Tombstones[key], before.Tombstones[key]) || after.AttemptGenerations[key] != before.AttemptGenerations[key] || !reflect.DeepEqual(after.Effects, before.Effects) || !reflect.DeepEqual(after.Attempts, before.Attempts) || !reflect.DeepEqual(after.MachineStatuses, before.MachineStatuses) {
				t.Fatalf("replay changed durable invalidation: before=%#v after=%#v", before, after)
			}
			if receipt, ok := operatorReceiptByID(after, action+"-replay"); !ok || receipt.EffectID != before.Tombstones[key].EffectID || receipt.State != "pending" {
				t.Fatalf("replay receipt=%#v exists=%t", receipt, ok)
			}
			command, ok := service.tombstoneReplayCommand(stateOwnerSnapshot{State: after}, operatorRequest(action+"-invalid", action, manifest, confirm))
			if !ok {
				t.Fatal("tombstone replay command missing")
			}
			for name, corrupt := range map[string]func(*beginOperatorMutationCommand){
				"epoch":              func(c *beginOperatorMutationCommand) { c.Identity.Epoch++ },
				"source revision":    func(c *beginOperatorMutationCommand) { c.Identity.SourceRevision = after.Revision + 1 },
				"issue generation":   func(c *beginOperatorMutationCommand) { c.Identity.IssueGeneration++ },
				"attempt generation": func(c *beginOperatorMutationCommand) { c.Identity.AttemptGeneration++ },
				"manifest":           func(c *beginOperatorMutationCommand) { c.Manifest.BaseSHA = strings.Repeat("f", 40) },
				"published head":     func(c *beginOperatorMutationCommand) { c.PublishedHead = strings.Repeat("f", 40) },
				"cleanup digest":     func(c *beginOperatorMutationCommand) { c.CleanupDigest = strings.Repeat("f", 64) },
				"policy": func(c *beginOperatorMutationCommand) {
					c.CleanupPolicy.Action = map[bool]string{true: "archive", false: "dismiss"}[action == "dismiss"]
				},
			} {
				t.Run(name, func(t *testing.T) {
					invalid := command
					invalid.Request.RequestID += "-" + name
					corrupt(&invalid)
					if _, _, err := owner.beginOperatorMutation(t.Context(), invalid); !errors.Is(err, errStaleStateResult) && !errors.Is(err, errStateConflict) {
						t.Fatalf("invalid replay error=%v", err)
					}
					unchanged := mustOwnerSnapshot(t, owner).State
					if unchanged.Revision != after.Revision || !reflect.DeepEqual(unchanged.Tombstones, after.Tombstones) || !reflect.DeepEqual(unchanged.Effects, after.Effects) || !reflect.DeepEqual(unchanged.ControlReceipts, after.ControlReceipts) {
						t.Fatal("invalid replay committed owner state")
					}
				})
			}
			if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, after); err != nil {
				t.Fatal(err)
			}
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(next runtimeOwnerState) error {
				return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, next)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.close(context.Background()) })
			replayService := operatorServiceWithCleanup(t, restarted, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
			replayService.stopped = true
			restartedReplay := replayService.perform(t.Context(), operatorRequest(action+"-restarted", action, manifest, confirm))
			if !restartedReplay.OK || restartedReplay.Status != want {
				t.Fatalf("restart replay=%#v", restartedReplay)
			}
			durable, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
			beforeTombstone, durableTombstone := before.Tombstones[key], durable.Tombstones[key]
			if len(beforeTombstone.ExternalOutcomes) == 0 {
				beforeTombstone.ExternalOutcomes = nil
			}
			if len(durableTombstone.ExternalOutcomes) == 0 {
				durableTombstone.ExternalOutcomes = nil
			}
			if err != nil || !reflect.DeepEqual(durableTombstone, beforeTombstone) || !reflect.DeepEqual(durable.Effects, before.Effects) || !reflect.DeepEqual(durable.MachineStatuses, before.MachineStatuses) {
				t.Fatalf("restart changed durable invalidation: err=%v state=%#v", err, durable)
			}
		})
	}
}

func TestLocalTombstoneReplayAdmitsCapturedCommandAfterObservationChanges(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 375, "completed", true)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	service.stopped = true
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	first := service.perform(t.Context(), operatorRequest("dismiss-before-body-change", "dismiss", manifest, false))
	if !first.OK || first.Status != http.StatusAccepted {
		t.Fatalf("first dismissal=%#v", first)
	}
	before := mustOwnerSnapshot(t, owner)
	request := operatorRequest("dismiss-captured-before-body-change", "dismiss", manifest, false)
	command, ok := service.tombstoneReplayCommand(before, request)
	if !ok {
		t.Fatal("service did not prepare tombstone replay")
	}
	issueKey := ownerIssueKey(manifest.Repository, manifest.Issue)
	issue := expandIssueFact(before.State.Observations[issueKey].Fact)
	issue.Body = "changed GitHub issue body"
	input := repositoryInput(true, issue)
	for _, attempt := range before.State.Observations[issueKey].Attempts {
		if attempt.Present {
			input.Attempts = append(input.Attempts, expandAttemptFact(attempt.Fact))
		}
	}
	changed := applyReconciliationInput(t, owner, input)
	if !changed.State.Observations[issueKey].Present || changed.State.Observations[issueKey].Fact.BodyDigest == before.State.Observations[issueKey].Fact.BodyDigest || changed.State.IssueGenerations[issueKey] != before.State.IssueGenerations[issueKey] {
		t.Fatalf("fixture did not change only the observation: before=%#v after=%#v", before.State.Observations[issueKey], changed.State.Observations[issueKey])
	}
	currentBeforeReplay := mustOwnerSnapshot(t, owner)
	committed, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatalf("captured replay rejected after GitHub body changed: %v", err)
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if effect == nil || effect.ID != before.State.Tombstones[key].EffectID || !reflect.DeepEqual(committed.State.Tombstones[key], currentBeforeReplay.State.Tombstones[key]) || committed.State.Attempts[key].Generation != 0 {
		t.Fatalf("replay changed invalidated attempt: effect=%#v state=%#v", effect, committed.State)
	}
	if receipt, ok := operatorReceiptByID(committed.State, request.RequestID); !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseCleanupPending || receipt.Result != nil {
		t.Fatalf("captured replay receipt=%#v exists=%t", receipt, ok)
	}
	fresh := service.perform(t.Context(), operatorRequest("dismiss-after-body-change", "dismiss", manifest, false))
	if !fresh.OK || fresh.Status != http.StatusAccepted {
		t.Fatalf("fresh request after changed body=%#v", fresh)
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
	if state.AttemptGenerations[key] != 2 || state.Attempts[key].Generation != 2 || len(state.Effects) != 1 || len(state.ControlReceipts) != 1 {
		t.Fatalf("cancel was overwritten: %#v", state)
	}
	receipt := state.ControlReceipts[0]
	effect, exists := state.Effects[receipt.EffectID]
	if !exists || effect.Action != string(agentruntime.EffectStop) || receipt.Request.Action != "cancel" {
		t.Fatalf("Cancel receipt lost its exact stop effect: receipt=%#v effect=%#v", receipt, effect)
	}
	switch {
	case receipt.State == "pending" && receipt.Phase == operatorPhaseStopPending:
		if effect.State != "pending" || receipt.Result != nil {
			t.Fatalf("pending Cancel was not durable: receipt=%#v effect=%#v", receipt, effect)
		}
	case receipt.State == "completed" && receipt.Phase == operatorPhaseCompleted:
		if effect.State != "completed" || state.Attempts[key].Manifest.State != "cancelled" || receipt.Result == nil || !receipt.Result.OK {
			t.Fatalf("completed Cancel lost its terminal outcome: receipt=%#v effect=%#v manifest=%#v", receipt, effect, state.Attempts[key].Manifest)
		}
	default:
		t.Fatalf("Cancel reached an invalid phase: receipt=%#v effect=%#v", receipt, effect)
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
	if !ok || receipt.State != "pending" || !slices.Contains([]string{operatorPhaseCleanupPending, operatorPhaseCleanupStarted}, receipt.Phase) || state.Tombstones[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Action != "dismissed" {
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

func TestOperatorFailedRecoverSelfAdvancesToRetryCompletion(t *testing.T) {
	completed := make(chan struct{})
	var completion sync.Once
	persist := func(state runtimeOwnerState) error {
		if receipt, ok := operatorReceiptByID(state, "recover-self-advance"); ok && receipt.State == "completed" {
			completion.Do(func() { close(completed) })
		}
		return nil
	}
	owner, manifest := operatorNeverLaunchedOwner(t, 329, "failed", "failed", persist)
	service := operatorTestMutationService(t, owner)
	service.collector.Config.ActorID = 42
	service.collector.Config.RetryCommand = "/agent-symphony retry"
	var collections atomic.Int32
	service.collect = func(_ context.Context, _ stateOwnerSnapshot, issueNumber int) (reconciliationV2Batch, error) {
		failed := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: issueNumber, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}}
		issue := issueFact(issueNumber, "recover")
		issue.Attempt, issue.CurrentAttempt, issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, 1, 1, true
		issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
		collections.Add(1)
		return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issueNumber), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}}, nil
	}
	service.collector.API = internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path == "/user" {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":42,"login":"owner"}`)), Request: request}, nil
		}
		if request.URL.Path == "/repos/o/r/pulls" {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
		}
		if request.URL.Path == "/repos/o/r" {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"full_name":"o/r","default_branch":"main","permissions":{"pull":true}}`)), Request: request}, nil
		}
		if request.URL.Path == "/repos/o/r/branches/main" {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"commit":{"sha":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}}`)), Request: request}, nil
		}
		if request.URL.Path == "/repos/o/r/issues/329" {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"number":329,"node_id":"I_329","title":"recover","body":"body","state":"open","created_at":"2026-09-14T00:00:00Z","user":{"id":42},"labels":[]}`)), Request: request}, nil
		}
		if request.URL.Path == "/repos/o/r/issues/329/timeline" {
			return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`[]`)), Request: request}, nil
		}
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
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		current := mustOwnerSnapshot(t, owner).State
		receipt, _ := operatorReceiptByID(current, "recover-self-advance")
		t.Fatalf("Recover did not reach a terminal receipt: receipt=%#v effects=%#v", receipt, current.Effects)
	}
	final := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(final, "recover-self-advance")
	if !ok || receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || receipt.Result == nil || receipt.Result.Status != http.StatusOK || collections.Load() != 1 {
		t.Fatalf("receipt=%#v collections=%d effects=%#v", receipt, collections.Load(), final.Effects)
	}
	if manifest.LaunchID != "" {
		t.Fatalf("never-launched recovery fixture acquired launch authority: %#v", manifest)
	}
}

func TestOperatorActiveNeverLaunchedRecoverStaysPendingWithoutStopProof(t *testing.T) {
	owner, manifest := operatorNeverLaunchedOwner(t, 330, "running", "active", func(runtimeOwnerState) error { return nil })
	service := operatorTestMutationService(t, owner)
	var githubRequests atomic.Int32
	service.collector.API = internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		githubRequests.Add(1)
		return nil, fmt.Errorf("unexpected GitHub request %s", request.URL.String())
	})}}
	result := service.perform(t.Context(), operatorRequest("recover-unproved-stop", "recover", manifest, false))
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("result=%#v", result)
	}
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	final := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(final, result.RequestID)
	effect := final.Effects[receipt.EffectID]
	if !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseStopPending || effect.State != "pending" || effect.Action != string(agentruntime.EffectStop) || githubRequests.Load() != 0 {
		t.Fatalf("unproved Stop advanced: receipt=%#v effect=%#v GitHub_requests=%d", receipt, effect, githubRequests.Load())
	}
}

func bindLiveReviewerForService(t *testing.T, owner *stateOwner, reviewer *runtimeEffectIntent) *reviewerSessionStopBoundary {
	t.Helper()
	child := exec.Command("sh", "-c", "read line")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	input, err := child.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = child.Process.Kill(); _ = child.Wait() })
	identity := ownerReconciliationEffectIdentity(*reviewer)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: child.Process.Pid}); err != nil {
		t.Fatal(err)
	}
	launchPath, terminalPath := reviewerLifecyclePaths(reviewer.Reconciliation.Reviewer.Snapshot, reviewer.Reconciliation.Reviewer.Target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	launch := reviewerLaunchIdentity{EffectID: reviewer.ID, RunID: reviewer.Reconciliation.Reviewer.RunID, IssueGeneration: reviewer.IssueGeneration, AttemptGeneration: reviewer.AttemptGeneration, RequestDigest: reviewer.RequestDigest, GateProtocol: true, SessionRequested: true, ChildPID: child.Process.Pid}
	if err := writeReviewerRecord(launchPath, launch); err != nil {
		t.Fatal(err)
	}
	start := "agent-symphony review-pane tmux " + launchPath + " " + terminalPath + " " + reviewerSignal(launch) + " " + launch.RequestDigest
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: reviewerPaneTestOutput("0||||", reviewer.Reconciliation.Reviewer.Session, "$9", os.Getpid(), start)}}
	boundary.onKill = func() error {
		if err := syscall.Kill(-child.Process.Pid, syscall.SIGKILL); err != nil {
			return err
		}
		_ = child.Wait()
		return nil
	}
	return boundary
}

func TestUnboundReviewerWrapperProcess(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_TEST_REVIEWER_WRAPPER") != "1" {
		return
	}
	child := exec.Command("sh", "-c", "read line")
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	input, err := child.StdinPipe()
	if err != nil || child.Start() != nil {
		os.Exit(2)
	}
	_, _ = fmt.Fprintln(os.Stdout, child.Process.Pid)
	var release [1]byte
	_, _ = os.Stdin.Read(release[:])
	_ = syscall.Kill(-child.Process.Pid, syscall.SIGKILL)
	_ = child.Wait()
	_ = input.Close()
}

func startUnboundReviewerForService(t *testing.T, owner *stateOwner, reviewer *runtimeEffectIntent) *reviewerSessionStopBoundary {
	t.Helper()
	wrapper := exec.Command(os.Args[0], "-test.run=^TestUnboundReviewerWrapperProcess$")
	wrapper.Env = append(os.Environ(), "AGENT_SYMPHONY_TEST_REVIEWER_WRAPPER=1")
	input, err := wrapper.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := wrapper.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := wrapper.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = wrapper.Process.Kill(); _ = wrapper.Wait() })
	var childPID int
	if _, err := fmt.Fscan(output, &childPID); err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*reviewer)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	launchPath, terminalPath := reviewerLifecyclePaths(reviewer.Reconciliation.Reviewer.Snapshot, reviewer.Reconciliation.Reviewer.Target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	launch := reviewerLaunchIdentity{EffectID: reviewer.ID, RunID: reviewer.Reconciliation.Reviewer.RunID, IssueGeneration: reviewer.IssueGeneration, AttemptGeneration: reviewer.AttemptGeneration, RequestDigest: reviewer.RequestDigest, ProfileDigest: reviewer.ReviewerProfileDigest, ConfinementVersion: reviewer.ReviewerConfinementVersion, GateProtocol: true, SessionRequested: true, ChildPID: childPID}
	if err := writeReviewerRecord(launchPath, launch); err != nil {
		t.Fatal(err)
	}
	start := "agent-symphony review-pane tmux " + launchPath + " " + terminalPath + " " + reviewerSignal(launch) + " " + launch.RequestDigest
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: reviewerPaneTestOutput("0||||", reviewer.Reconciliation.Reviewer.Session, "$9", wrapper.Process.Pid, start)}}
	boundary.onKill = func() error {
		if _, err := input.Write([]byte{'x'}); err != nil {
			return err
		}
		return wrapper.Wait()
	}
	return boundary
}

func TestFreshReconciliationStopsInvalidLivePlanReviewer(t *testing.T) {
	for _, change := range []string{"body", "closed", "body restored", "partial body"} {
		t.Run(change, func(t *testing.T) {
			owner, manifest := operatorTestOwner(t, 366, "active", false)
			service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
			reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
			boundary := bindLiveReviewerForService(t, owner, reviewer)
			service.reviewer = boundary
			key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			run, err := service.effects.acquireKey(t.Context(), key, reviewer.IssueGeneration, reviewer.AttemptGeneration, 0, reviewer.ID)
			if err != nil {
				t.Fatal(err)
			}
			released := make(chan struct{})
			go func() {
				<-run.ctx.Done()
				service.effects.releaseKey(key, run)
				close(released)
			}()
			pipeline := productionReconciliation{owner: owner, effects: service.effects, operator: service}
			if superseded, err := pipeline.supersedeInvalidPendingPlanReviewers(t.Context(), mustOwnerSnapshot(t, owner)); err != nil || superseded || run.ctx.Err() != nil {
				t.Fatalf("compatible live review was stopped: superseded=%v err=%v", superseded, err)
			}
			state := mustOwnerSnapshot(t, owner).State
			observation := state.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
			issue := expandIssueFact(observation.Fact)
			issue.Body = "body"
			if change == "body" || change == "body restored" || change == "partial body" {
				issue.Body = "changed body"
			} else {
				issue.Closed = true
			}
			attempt := expandAttemptFact(observation.Attempts[key].Fact)
			input := repositoryInput(change != "partial body", issue)
			input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
			applied := applyReconciliationInput(t, owner, input)
			if change == "body restored" {
				issue.Body = "body"
				input = repositoryInput(true, issue)
				input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
				applied = applyReconciliationInput(t, owner, input)
				if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, applied.State); err != nil {
					t.Fatal(err)
				}
				loaded, err := readRuntimeOwnerState(owner.stateRoot, applied.State.Repository)
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.close(t.Context()); err != nil {
					t.Fatal(err)
				}
				restarted, err := startTestStateOwner(t, owner.stateRoot, loaded, func(state runtimeOwnerState) error {
					return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state)
				})
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = restarted.close(context.Background()) })
				owner, service.owner, service.effects.owner = restarted, restarted, restarted
				applied = mustOwnerSnapshot(t, owner)
				stale := applied.State.Effects[reviewer.ID]
				result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(*stale.Reconciliation)
				if err := writeReconciliationEffectMarker(owner.stateRoot, ownerReconciliationEffectIdentity(stale), *stale.Reconciliation, result); err != nil {
					t.Fatal(err)
				}
				if _, err := service.effects.verifyPendingOperatorReconciliation(t.Context(), stale); !errors.Is(err, errStaleStateResult) {
					t.Fatalf("restored body accepted revoked reviewer marker: %v", err)
				}
				if current := mustOwnerSnapshot(t, owner).State; current.Effects[reviewer.ID].State != "pending" || current.Attempts[key].Manifest.ReviewState == "clean" {
					t.Fatalf("stale marker resurrected Plan review: effect=%#v manifest=%#v", current.Effects[reviewer.ID], current.Attempts[key].Manifest)
				}
				if err := os.Remove(filepath.Join(owner.stateRoot, "reconciliation-effects", reviewer.ID+".done")); err != nil {
					t.Fatal(err)
				}
			}
			var superseded bool
			if change == "body restored" {
				err = service.resumeReceipt(t.Context(), fmt.Sprintf("pending-plan-%d", manifest.Issue))
				superseded = err == nil
			} else {
				superseded, err = pipeline.supersedeInvalidPendingPlanReviewers(t.Context(), applied)
			}
			if !superseded || err != nil {
				t.Fatalf("fresh %s observation did not revoke the exact confined review run: superseded=%v err=%v", change, superseded, err)
			}
			<-released
			final := mustOwnerSnapshot(t, owner).State
			receipt, ok := operatorReceiptByID(final, fmt.Sprintf("pending-plan-%d", manifest.Issue))
			proof := final.ReviewerProofs[reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, agentruntime.ReviewModePlan, reviewer.Reconciliation.Reviewer.Target)]
			if !ok || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusConflict || final.Effects[reviewer.ID].State != "completed" || !final.Effects[reviewer.ID].ReviewerRevoked || !reviewerCleanupAuthorized(proof, activeWorkerProfileDigest(final)) || len(boundary.killed) != 1 {
				t.Fatalf("invalidated confined review did not become durably terminal: receipt=%#v effect=%#v proof=%#v killed=%v", receipt, final.Effects[reviewer.ID], proof, boundary.killed)
			}
			if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, final); err != nil {
				t.Fatal(err)
			}
			reloaded, err := readRuntimeOwnerState(owner.stateRoot, final.Repository)
			if err != nil || reloaded.Effects[reviewer.ID].State != "completed" || !reloaded.Effects[reviewer.ID].ReviewerRevoked {
				t.Fatalf("restart lost terminal reviewer invalidation: err=%v effect=%#v", err, reloaded.Effects[reviewer.ID])
			}
		})
	}
}

func TestPreupgradeInvalidPlanObservationRevokesBeforeRestartEpoch(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 477, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	identity := ownerReconciliationEffectIdentity(*reviewer)
	if _, err := owner.proveReviewerDead(t.Context(), proveReviewerDeadCommand{Identity: identity, NeverRan: true}); err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(state.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "changed body"
	attempt := expandAttemptFact(state.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	issue.Body = "body"
	input = repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	legacy := applyReconciliationInput(t, owner, input).State // Old ledger only retains restored A.
	effect := legacy.Effects[reviewer.ID]
	effect.ReviewerRevoked = false // Old binary did not track revocation.
	legacy.Effects[reviewer.ID] = effect
	legacy.ReviewerRevocationTracked = false
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, legacy); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(owner.stateRoot, legacy.Repository)
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
	if started := mustOwnerSnapshot(t, restarted).State; !started.Effects[reviewer.ID].ReviewerRevoked || !started.ReviewerRevocationTracked || started.Epoch <= loaded.Epoch {
		t.Fatalf("restart lost old-ledger revocation: effect=%#v epoch=%d", started.Effects[reviewer.ID], started.Epoch)
	}
	stale := mustOwnerSnapshot(t, restarted).State.Effects[reviewer.ID]
	result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(*stale.Reconciliation)
	if err := writeReconciliationEffectMarker(restarted.stateRoot, identity, *stale.Reconciliation, result); err != nil {
		t.Fatal(err)
	}
	service.owner, service.effects.owner = restarted, restarted
	if _, err := service.effects.verifyPendingOperatorReconciliation(t.Context(), stale); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("pre-upgrade stale marker completed after body restoration: %v", err)
	}
}

func TestPreupgradeValidPlanReviewerRevokedOnce(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 478, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	state := mustOwnerSnapshot(t, owner).State
	observation := state.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)]
	if observation.ObservationEpoch != reviewer.IntentEpoch || observation.LastCycleID != reviewer.Reconciliation.ObservationCycleID {
		t.Fatal("fixture observation is not the one that admitted the reviewer")
	}
	state.Epoch++ // Old daemon restarted without collecting a newer issue fact.
	state.ReviewerRevocationTracked = false
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(owner.stateRoot, state.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, loaded, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	if current := mustOwnerSnapshot(t, restarted).State; current.Effects[reviewer.ID].State != "pending" || !current.Effects[reviewer.ID].ReviewerRevoked || !current.ReviewerRevocationTracked {
		t.Fatalf("old pending reviewer was not conservatively revoked: %#v", current.Effects[reviewer.ID])
	}
	if err := restarted.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	stored, err := readRuntimeOwnerState(owner.stateRoot, state.Repository)
	if err != nil {
		t.Fatal(err)
	}
	second, err := startTestStateOwner(t, owner.stateRoot, stored, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.close(context.Background()) })
	if current := mustOwnerSnapshot(t, second).State; !current.ReviewerRevocationTracked || !current.Effects[reviewer.ID].ReviewerRevoked {
		t.Fatalf("migration was not durable across second restart: %#v", current.Effects[reviewer.ID])
	}
}

func TestTrackedValidPlanReviewerSurvivesRestart(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 479, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, mustOwnerSnapshot(t, owner).State); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	for restart := 0; restart < 2; restart++ {
		stored, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
		if err != nil {
			t.Fatal(err)
		}
		if !stored.ReviewerRevocationTracked || stored.Effects[reviewer.ID].ReviewerRevoked {
			t.Fatalf("valid tracked reviewer changed before restart %d: %#v", restart, stored.Effects[reviewer.ID])
		}
		current, err := startTestStateOwner(t, owner.stateRoot, stored, func(next runtimeOwnerState) error {
			return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, next)
		})
		if err != nil {
			t.Fatal(err)
		}
		if got := mustOwnerSnapshot(t, current).State.Effects[reviewer.ID]; got.State != "pending" || got.ReviewerRevoked {
			t.Fatalf("valid tracked reviewer revoked at restart %d: %#v", restart, got)
		}
		if err := current.close(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPlanRevocationMigrationPersistenceFailureDoesNotStart(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 480, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	legacy := mustOwnerSnapshot(t, owner).State
	legacy.ReviewerRevocationTracked = false
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, legacy); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	stored, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("injected owner persistence failure")
	if started, err := startTestStateOwner(t, owner.stateRoot, stored, func(runtimeOwnerState) error { return injected }); !errors.Is(err, injected) || started != nil {
		t.Fatalf("startup exposed failed migration: owner=%v err=%v", started, err)
	}
	unchanged, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Epoch != stored.Epoch || unchanged.ReviewerRevocationTracked || unchanged.Effects[reviewer.ID].ReviewerRevoked {
		t.Fatalf("failed migration partially persisted: %#v", unchanged.Effects[reviewer.ID])
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, unchanged, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	if current := mustOwnerSnapshot(t, restarted).State; !current.ReviewerRevocationTracked || !current.Effects[reviewer.ID].ReviewerRevoked {
		t.Fatalf("retry did not commit full migration: %#v", current.Effects[reviewer.ID])
	}
}

func TestImplementationSupersessionBindsAndStopsUnboundConfinedReviewer(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	request.Reviewer.Mode, request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, reviewer, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	boundary := startUnboundReviewerForService(t, owner, reviewer)
	launchPath, terminalPath := reviewerLifecyclePaths(request.Reviewer.Snapshot, request.Reviewer.Target)
	var launch reviewerLaunchIdentity
	found, launchErr := readReviewerRecord(launchPath, &launch)
	if !found || launchErr != nil || !reviewerPaneStartMatches(strings.SplitN(boundary.status.Output, "|", 8)[7], launchPath, terminalPath, launch) {
		t.Fatalf("fixture launch: found=%v err=%v launch=%#v pane=%q", found, launchErr, launch, boundary.status.Output)
	}
	service := operatorTestMutationService(t, owner)
	service.reviewer = boundary
	input := reconciliationEffectObservationInput(request, "changed")
	input.Issues[0].Body = "changed body"
	applyReconciliationInput(t, owner, input)
	current := mustOwnerSnapshot(t, owner).State.Effects[reviewer.ID]
	superseded, err := service.supersedeInvalidPlanReview(t.Context(), current)
	if !superseded || err != nil {
		t.Fatalf("unbound confined implementation reviewer did not stop exactly: superseded=%v err=%v", superseded, err)
	}
	final := mustOwnerSnapshot(t, owner).State
	effect := final.Effects[reviewer.ID]
	proof := final.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)]
	gone, groupErr := reviewerGroupGone(proof.GroupPID)
	if effect.State != "completed" || effect.ReviewerGroupPID < 2 || !reviewerCleanupAuthorized(proof, activeWorkerProfileDigest(final)) || proof.EffectID != reviewer.ID || proof.RunID != request.Reviewer.RunID || !gone || groupErr != nil || len(boundary.killed) != 1 {
		t.Fatalf("implementation reviewer did not retain exact confined stop proof: effect=%#v proof=%#v gone=%v groupErr=%v killed=%v", effect, proof, gone, groupErr, boundary.killed)
	}
}

func admitPendingGatedPlanReviewer(t *testing.T, owner *stateOwner, service *operatorMutationService, manifest agentruntime.Manifest) *runtimeEffectIntent {
	t.Helper()
	initial := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	attempt := expandAttemptFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, fmt.Sprintf(`{"number":%d,"state":"open","body":"body"}`, manifest.Issue))
	}))
	t.Cleanup(github.Close)
	service.collector.API = internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}
	service.reviewSource, service.reviewCommand = "source", []string{"review"}
	runtimeState := service.effects.executor.Runtime
	priorRunner, priorTmux, priorGit := runtimeState.Runner, runtimeState.Tmux, runtimeState.Git
	runtimeState.Runner, runtimeState.Tmux, runtimeState.Git = operatorOwnedRunner{manifest: manifest}, "tmux", "git"
	command, _, err := service.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), operatorRequest(fmt.Sprintf("pending-plan-%d", manifest.Issue), "review-plan", manifest, false))
	runtimeState.Runner, runtimeState.Tmux, runtimeState.Git = priorRunner, priorTmux, priorGit
	if err != nil {
		t.Fatal(err)
	}
	_, reviewer, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil || reviewer == nil || !reviewer.ReviewerGateProtocol || reviewer.ReviewerSessionRequested {
		t.Fatalf("pending gated reviewer admission: reviewer=%#v err=%v", reviewer, err)
	}
	return reviewer
}

func TestPlanReviewScannerHonorsAdmissionReservation(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 349, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	service.reviewer = absentSessionBoundary{}
	requestID := fmt.Sprintf("pending-plan-%d", manifest.Issue)
	requestKey := "request:" + requestID
	if !service.reserve(requestKey) {
		t.Fatal("could not reserve plan review before owner admission")
	}
	defer service.release(requestKey)
	effect := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	service.effects.mu.Lock()
	defer service.effects.mu.Unlock()
	snapshot := mustOwnerSnapshot(t, owner)
	service.scanPendingPlanReviewers(t.Context(), snapshot)
	service.mu.Lock()
	replayStarted := service.active[effect.ID]
	service.mu.Unlock()
	if replayStarted {
		t.Fatal("same-daemon scan reserved replay before the original admission dispatched")
	}
	current := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(current, requestID)
	if !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseReviewPending || current.Effects[effect.ID].ReviewerLaunched {
		t.Fatalf("scanner altered unlaunched admission: receipt=%#v effect=%#v", receipt, current.Effects[effect.ID])
	}
}

func TestPlanReviewAdmissionReservesBeforeEffectDispatch(t *testing.T) {
	root := resolvedTempDir(t)
	t.Cleanup(func() {
		_ = filepath.WalkDir(productionSnapshotRoot(root), func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})
	source := filepath.Join(root, "source")
	runExternal(t, "", "git", "init", "-q", "-b", "main", source)
	runExternal(t, source, "git", "config", "user.name", "Review fixture")
	runExternal(t, source, "git", "config", "user.email", "fixture@example.invalid")
	if err := os.WriteFile(filepath.Join(source, "README.md"), []byte("plan review\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runExternal(t, source, "git", "add", "README.md")
	runExternal(t, source, "git", "commit", "-qm", "base")
	manifest := ownerTestManifest(t, root, 348, 1, "running")
	manifest.BaseSHA = strings.TrimSpace(runExternal(t, source, "git", "rev-parse", "HEAD"))
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	initial := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	attempt := expandAttemptFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprintf(w, `{"number":%d,"state":"open","body":"body"}`, manifest.Issue)
	}))
	t.Cleanup(github.Close)
	lifecycle, stop := context.WithCancel(t.Context())
	defer stop()
	service := operatorServiceWithCleanup(t, owner, lifecycle, &operatorCleanupBoundary{path: manifest.Worktree})
	service.collector.API = internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}
	service.reviewSource, service.reviewCommand = source, []string{"review"}
	launch := &admissionReviewerBoundary{entered: make(chan struct{})}
	service.reviewer = launch
	service.effects.executor.Runtime.Runner = operatorOwnedRunner{manifest: manifest}
	service.effects.executor.Runtime.Tmux, service.effects.executor.Runtime.Git = "tmux", "git"
	request := operatorRequest("plan-admission-before-dispatch", "review-plan", manifest, false)
	service.effects.mu.Lock() // Hold only effect cancellation, after owner admission and before dispatch.
	locked := true
	defer func() {
		if locked {
			service.effects.mu.Unlock()
		}
	}()
	result := make(chan controlResult, 1)
	go func() { result <- service.perform(t.Context(), request) }()
	var committed stateOwnerSnapshot
	deadline := time.After(5 * time.Second)
	for {
		select {
		case committed = <-owner.commits:
			if receipt, ok := operatorReceiptByID(committed.State, request.RequestID); ok && receipt.State == "pending" {
				goto admitted
			}
		case got := <-result:
			t.Fatalf("plan review returned before owner admission: %#v", got)
		case <-deadline:
			t.Fatal("plan review did not commit before effect dispatch barrier")
		}
	}
admitted:
	receipt, _ := operatorReceiptByID(committed.State, request.RequestID)
	service.scanPendingPlanReviewers(t.Context(), committed)
	service.mu.Lock()
	replayStarted := service.active[receipt.EffectID]
	service.mu.Unlock()
	if replayStarted || committed.State.Effects[receipt.EffectID].ReviewerLaunched {
		t.Fatalf("same-daemon scan stole freshly admitted reviewer: replay=%t effect=%#v", replayStarted, committed.State.Effects[receipt.EffectID])
	}
	duplicate := make(chan controlResult, 1)
	go func() { duplicate <- service.perform(t.Context(), request) }()
	service.effects.mu.Unlock()
	locked = false
	select {
	case got := <-result:
		if !got.OK || got.Status != http.StatusAccepted {
			t.Fatalf("original reviewer admission=%#v", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("original reviewer admission did not dispatch after barrier")
	}
	select {
	case <-launch.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("original reviewer did not reach tmux new-session after handoff")
	}
	select {
	case got := <-duplicate:
		var status operatorReceiptStatus
		if !got.OK || got.Status != http.StatusAccepted || json.Unmarshal(got.Data, &status) != nil || status.EffectID != receipt.EffectID {
			t.Fatalf("duplicate RequestID did not reuse original receipt: %#v status=%#v", got, status)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("duplicate RequestID did not resolve after admission handoff")
	}
	stop()
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
}

type admissionReviewerBoundary struct{ entered chan struct{} }

func (b *admissionReviewerBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" {
		return agentruntime.Result{}, fmt.Errorf("unexpected reviewer command %s %s", operation, command.Name)
	}
	if slices.Contains(command.Args, "new-session") {
		close(b.entered)
		<-ctx.Done()
		return agentruntime.Result{}, ctx.Err()
	}
	if slices.Contains(command.Args, "display-message") {
		return agentruntime.Result{Output: "||||||||||"}, nil
	}
	if slices.Contains(command.Args, "has-session") {
		return agentruntime.Result{Exited: true, Code: 1}, errors.New("session not found")
	}
	if slices.Contains(command.Args, "wait-for") || slices.Contains(command.Args, "set-option") {
		return agentruntime.Result{}, nil
	}
	return agentruntime.Result{}, fmt.Errorf("unexpected reviewer command %v", command.Args)
}

func TestPlanReviewCommittedBeforeDispatchReplaysAfterRestart(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 347, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	effect := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	committed := mustOwnerSnapshot(t, owner).State
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, committed); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	replayed := operatorServiceWithCleanup(t, restarted, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })
	replayed.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		close(entered)
		<-release
		return reconciliationV2Batch{}, errors.New("controlled recovery stop before external launch")
	}
	if err := replayed.resumePending(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("committed unstarted review was not replayed after restart")
	}
	state := mustOwnerSnapshot(t, restarted).State
	receipt, ok := operatorReceiptByID(state, fmt.Sprintf("pending-plan-%d", manifest.Issue))
	if !ok || receipt.State != "pending" || state.Effects[effect.ID].ReviewerLaunched || state.Effects[effect.ID].ReviewerRevoked {
		t.Fatalf("restart replay altered unstarted intent before external work: receipt=%#v effect=%#v", receipt, state.Effects[effect.ID])
	}
	once.Do(func() { close(release) })
	select {
	case <-replayed.releaseSignal(effect.ID):
	case <-time.After(5 * time.Second):
		t.Fatal("controlled restart replay did not stop")
	}
}

func TestPlanReviewReservationWaitHonorsCancellationAndShutdown(t *testing.T) {
	owner, _ := operatorTestOwner(t, 346, "active", false)
	service := operatorTestMutationService(t, owner)
	if err := service.reservePlanAdmission(t.Context(), "same-request"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := service.reservePlanAdmission(ctx, "same-request"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled duplicate reservation = %v", err)
	}
	service.release("request:same-request")
	if err := service.reservePlanAdmission(t.Context(), "same-request"); err != nil {
		t.Fatalf("cancelled waiter poisoned reservation: %v", err)
	}
	service.release("request:same-request")
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := service.reservePlanAdmission(t.Context(), "after-shutdown"); !errors.Is(err, context.Canceled) {
		t.Fatalf("shutdown admitted a new reservation: %v", err)
	}
}

type reservationWaitContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (c *reservationWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.waiting) })
	return c.Context.Done()
}

func TestPlanReviewBlockedDuplicateWakesOnShutdown(t *testing.T) {
	owner, _ := operatorTestOwner(t, 345, "active", false)
	service := operatorTestMutationService(t, owner)
	if err := service.reservePlanAdmission(t.Context(), "shutdown-wait"); err != nil {
		t.Fatal(err)
	}
	defer service.release("request:shutdown-wait")
	ctx := &reservationWaitContext{Context: t.Context(), waiting: make(chan struct{})}
	finished := make(chan error, 1)
	go func() { finished <- service.reservePlanAdmission(ctx, "shutdown-wait") }()
	select {
	case <-ctx.waiting:
	case <-time.After(5 * time.Second):
		t.Fatal("duplicate did not reach reservation wait")
	}
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-finished:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("shutdown waiter result = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown left an already-waiting duplicate blocked")
	}
}

func TestAbandonAcceptsAndStopsLiveBoundReviewerThroughService(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 339, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree, additionalPath: filepath.Dir(manifest.LogPath)})
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	boundary := bindLiveReviewerForService(t, owner, reviewer)
	service.reviewer, service.cleanup.reviewer, service.cleanup.owner = boundary, boundary, owner
	service.effects.executor.Cleanup, service.effects.executor.VerifyCleanup = service.cleanup.execute, service.cleanup.verify
	applyReconciliationInput(t, owner, reconciliationInput{Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: manifest.Repository}, Complete: true})
	result := service.perform(t.Context(), operatorRequest("abandon-live-reviewer", "abandon", manifest, true))
	if !result.OK || result.Status != http.StatusAccepted {
		current := mustOwnerSnapshot(t, owner).State
		pending, _ := operatorReceiptByID(current, "abandon-live-reviewer")
		t.Fatalf("Abandon did not admit physical-pending cleanup: result=%#v receipt=%#v effect=%#v", result, pending, current.Effects[pending.EffectID])
	}
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	final := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	receipt, ok := operatorReceiptByID(final, "abandon-live-reviewer")
	effect := final.Effects[receipt.EffectID]
	if !ok || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusOK || final.Tombstones[key].Action != "abandoned" || final.Tombstones[key].CleanupPhase != "completed" || !effect.ReviewerStopped || effect.SupersededReviewerGroupPID < 2 || len(boundary.killed) != 1 || attemptHasReviewerProof(final, manifest.Repository, manifest.Issue, manifest.Attempt) {
		t.Fatalf("Abandon did not complete exact confined reviewer cleanup: receipt=%#v effect=%#v tombstone=%#v killed=%v proofs=%#v", receipt, effect, final.Tombstones[key], boundary.killed, final.ReviewerProofs)
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

func TestPreparingAttemptRejectsNewRecoverAttachmentWithoutChangingPersistedOwner(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 534, "active", false)
	original := operatorRequest("recover-original-preparing", "recover", manifest, false)
	command := operatorCommand(mustOwnerSnapshot(t, owner), original, manifest)
	command.LivenessFailed = true
	command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: "dashboard recovery: runtime liveness mismatch", RequestDigest: strings.Repeat("d", 64)}
	_, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	record := state.Attempts[key]
	record.Manifest.State = "preparing"
	state.Attempts[key] = record
	if err := owner.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, loaded, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	if _, replayed, err := restarted.beginOperatorMutation(t.Context(), command); err != nil || replayed == nil || replayed.ID != effect.ID {
		t.Fatalf("same-ID replay changed: effect=%#v err=%v", replayed, err)
	}
	service := operatorTestMutationService(t, restarted)
	newRequest := operatorRequest("recover-new-preparing", "recover", manifest, false)
	before := mustOwnerSnapshot(t, restarted).State
	attach, ok := service.recoveryAttachCommand(stateOwnerSnapshot{State: before}, newRequest)
	if !ok {
		t.Fatal("fixture lacks a pending Recover attachment")
	}
	if _, _, err := restarted.beginOperatorMutation(t.Context(), attach); !errors.Is(err, errStateConflict) {
		t.Fatalf("owner attachment error=%v", err)
	}
	server := &dashboardServer{ctx: t.Context(), stateRoot: restarted.stateRoot, repository: manifest.Repository, operator: service}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/recover?repository=o%2Fr&issue=534&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
	if response.Code != http.StatusConflict {
		t.Fatalf("new Recover status=%d body=%s", response.Code, response.Body.String())
	}
	after := mustOwnerSnapshot(t, restarted).State
	if after.Revision != before.Revision || !reflect.DeepEqual(after.ControlReceipts, before.ControlReceipts) || !reflect.DeepEqual(after.Effects, before.Effects) {
		t.Fatalf("new Recover changed persisted owner: before=%#v after=%#v", before, after)
	}
	disk, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if disk.Revision != before.Revision || !reflect.DeepEqual(disk.ControlReceipts, before.ControlReceipts) || !reflect.DeepEqual(disk.Effects, before.Effects) {
		t.Fatalf("new Recover changed durable owner: before=%#v disk=%#v", before, disk)
	}
}

func TestOperatorRecoverConvergesWithBackgroundRetry(t *testing.T) {
	for _, backgroundFirst := range []bool{true, false} {
		name := "operator-first"
		if backgroundFirst {
			name = "background-first"
		}
		t.Run(name, func(t *testing.T) {
			root := resolvedTempDir(t)
			manifest := ownerTestManifest(t, root, 349, 1, "failed")
			state := runtimeEffectInitialState(manifest)
			addOperatorObservation(&state, manifest, "failed", false)
			state.Epoch, state.Revision = 1, 1
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			refreshOperatorObservation(t, owner)
			failed := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 349, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}}
			issue := issueFact(349, "recover")
			issue.Attempt, issue.CurrentAttempt, issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, 1, 1, true
			issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
			input := repositoryInput(true, issue)
			input.Attempts = []internalgithub.RecoveryAttemptFact{failed}
			plan := applyAndPlanRecover(t, owner, input, input.Attempts, githubIssueRetry)
			service := operatorTestMutationService(t, owner)
			service.collector.Config.ActorID = 42
			service.collector.Config.RetryCommand = "/agent-symphony retry"
			operatorInput := input
			operatorInput.Scope = issueScope(manifest.Issue)
			service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
				return reconciliationV2Batch{Input: operatorInput}, nil
			}
			service.stopped = true // Admit durably without starting the external worker.
			request := operatorRequest("recover-converged", "recover", manifest, false)

			var effect *runtimeEffectIntent
			if backgroundFirst {
				admitted, err := service.effects.beginReconciliation(t.Context(), plan)
				if err != nil {
					t.Fatal(err)
				}
				stored := mustOwnerSnapshot(t, owner).State.Effects[admitted.Identity.EffectID]
				effect = &stored
			}
			result := service.perform(t.Context(), request)
			if !result.OK || result.Status != http.StatusAccepted {
				t.Fatalf("operator result=%#v", result)
			}
			committedState := mustOwnerSnapshot(t, owner).State
			receipt, ok := operatorReceiptByID(committedState, request.RequestID)
			if !ok || receipt.Phase != operatorPhaseRetryPending || receipt.EffectID == "" {
				t.Fatalf("receipt=%#v", receipt)
			}
			if backgroundFirst {
				if receipt.EffectID != effect.ID {
					t.Fatalf("receipt effect=%s background effect=%s", receipt.EffectID, effect.ID)
				}
			} else {
				fresh := mustOwnerSnapshot(t, owner)
				plans, err := planReconciliationAttemptIssueUpdates(fresh, reconciliationV2Batch{Input: operatorInput}, service.collector.Config)
				if err != nil || len(plans) != 1 {
					t.Fatalf("fresh background plans=%#v err=%v", plans, err)
				}
				admitted, err := service.effects.beginReconciliation(t.Context(), plans[0])
				if err != nil {
					t.Fatal(err)
				}
				if admitted.Identity.EffectID != receipt.EffectID {
					t.Fatalf("background effect=%s operator effect=%s", admitted.Identity.EffectID, receipt.EffectID)
				}
				stored := committedState.Effects[receipt.EffectID]
				effect = cloneEffect(&stored)
			}
			finish := finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*effect), Result: reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueRetry, Observed: true}}}
			if _, err := owner.finishReconciliationEffect(t.Context(), finish); err != nil {
				t.Fatal(err)
			}
			final := mustOwnerSnapshot(t, owner).State
			receipt, _ = operatorReceiptByID(final, request.RequestID)
			if receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || receipt.Result == nil || len(final.Effects) != 1 {
				t.Fatalf("receipt=%#v effects=%#v", receipt, final.Effects)
			}
		})
	}
}

func TestOperatorRecoverDefersOlderCollectionAfterContradictoryNewerCycle(t *testing.T) {
	owner, manifest := operatorNeverLaunchedOwner(t, 460, "failed", "failed", func(runtimeOwnerState) error { return nil })
	failed := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: manifest.Issue, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}}
	issue := issueFact(manifest.Issue, "recover")
	issue.Attempt, issue.CurrentAttempt, issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, 1, 1, true
	issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
	older := reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}
	newer := older
	newer.Issues = slices.Clone(older.Issues)
	newer.Issues[0].Title = "newer GitHub observation"
	service := operatorTestMutationService(t, owner)
	service.collector.Config.ActorID = 42
	service.collector.Config.RetryCommand = "/agent-symphony retry"
	service.stopped = true
	var calls atomic.Int32
	service.collect = func(ctx context.Context, _ stateOwnerSnapshot, _ int) (reconciliationV2Batch, error) {
		if calls.Add(1) == 1 {
			later, err := owner.reconciliationSnapshot(ctx)
			if err != nil {
				return reconciliationV2Batch{}, err
			}
			if _, err := owner.applyReconciliation(ctx, mustCollection(t, later, newer)); err != nil {
				return reconciliationV2Batch{}, err
			}
			return reconciliationV2Batch{Input: older}, nil
		}
		return reconciliationV2Batch{Input: newer}, nil
	}
	request := operatorRequest("recover-out-of-order", "recover", manifest, false)
	first := service.perform(t.Context(), request)
	intermediate := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(intermediate, request.RequestID)
	if !first.OK || first.Status != http.StatusAccepted || !ok || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil || len(intermediate.Effects) != 0 || calls.Load() != 1 {
		t.Fatalf("older collection was not deferred: result=%#v receipt=%#v effects=%#v calls=%d", first, receipt, intermediate.Effects, calls.Load())
	}
	result := service.perform(t.Context(), request)
	committed := mustOwnerSnapshot(t, owner).State
	receipt, ok = operatorReceiptByID(committed, request.RequestID)
	if !result.OK || result.Status != http.StatusAccepted || !ok || receipt.Phase != operatorPhaseRetryPending || receipt.EffectID == "" || calls.Load() != 2 {
		t.Fatalf("fresh later event did not commit Recover: result=%#v receipt=%#v calls=%d", result, receipt, calls.Load())
	}
}

func TestOperatorRecoverRejectsStaleRefreshWithChangedAttemptFacts(t *testing.T) {
	owner, manifest := operatorNeverLaunchedOwner(t, 464, "failed", "failed", func(runtimeOwnerState) error { return nil })
	request := operatorRequest("recover-stale-attempt-facts", "recover", manifest, false)
	reserved, err := owner.reserveOperatorAdmission(t.Context(), reserveOperatorAdmissionCommand{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	admission, ok := operatorReceiptByID(reserved.State, request.RequestID)
	if !ok || admission.Admission == nil {
		t.Fatalf("Recover admission was not reserved: %#v", admission)
	}
	olderCycle, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	newerCycle, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	issue := issueFact(manifest.Issue, "recover")
	issue.Attempt, issue.CurrentAttempt, issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, 1, 1, true
	olderAttempt := internalgithub.RecoveryAttemptFact{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, BaseSHA: manifest.BaseSHA, State: "failed", Diagnostic: "older attempt fact", Checks: []string{}}
	newerAttempt := olderAttempt
	newerAttempt.Diagnostic = "newer attempt fact"
	older := reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{olderAttempt}}
	newer := reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{newerAttempt}}
	if _, err := owner.applyReconciliation(t.Context(), mustCollection(t, newerCycle, newer)); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.refreshOperatorAdmission(t.Context(), request.RequestID, mustCollection(t, olderCycle, older)); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("stale Recover refresh was accepted: %v", err)
	}
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	attempt := state.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)]
	if !ok || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil || receipt.Admission.ObservationCycleID != newerCycle.CycleID || attempt.Fact.Diagnostic != newerAttempt.Diagnostic {
		t.Fatalf("stale refresh changed newer owner state: receipt=%#v attempt=%#v", receipt, attempt)
	}
}

func TestConcurrentRecoverAdmissionsConvergeAfterOneFinishesCollection(t *testing.T) {
	owner, manifest := operatorNeverLaunchedOwner(t, 461, "failed", "failed", func(runtimeOwnerState) error { return nil })
	failed := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: manifest.Issue, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}}
	issue := issueFact(manifest.Issue, "recover")
	issue.Attempt, issue.CurrentAttempt, issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, 1, 1, true
	issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
	input := reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}
	service := operatorTestMutationService(t, owner)
	service.collector.Config.ActorID = 42
	service.collector.Config.RetryCommand = "/agent-symphony retry"
	service.stopped = true
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	service.collect = func(ctx context.Context, _ stateOwnerSnapshot, _ int) (reconciliationV2Batch, error) {
		if calls.Add(1) != 1 {
			return reconciliationV2Batch{}, errors.New("coalesced Recover launched duplicate collection")
		}
		close(entered)
		select {
		case <-release:
			return reconciliationV2Batch{Input: input}, nil
		case <-ctx.Done():
			return reconciliationV2Batch{}, ctx.Err()
		}
	}
	results := make(chan controlResult, 2)
	go func() {
		results <- service.perform(t.Context(), operatorRequest("recover-first", "recover", manifest, false))
	}()
	<-entered
	second := service.perform(t.Context(), operatorRequest("recover-second", "recover", manifest, false))
	if !second.OK || second.Status != http.StatusAccepted {
		close(release)
		t.Fatalf("second Recover result=%#v state=%#v", second, mustOwnerSnapshot(t, owner).State)
	}
	close(release)
	first := <-results
	for _, result := range []controlResult{first, second} {
		if !result.OK || result.Status != http.StatusAccepted {
			t.Fatalf("concurrent Recover result=%#v state=%#v", result, mustOwnerSnapshot(t, owner).State)
		}
	}
	state := mustOwnerSnapshot(t, owner).State
	if len(state.ControlReceipts) != 2 || len(state.Effects) != 1 || calls.Load() != 1 {
		t.Fatalf("state=%#v calls=%d", state, calls.Load())
	}
	var effectID string
	for _, receipt := range state.ControlReceipts {
		if receipt.State != "pending" || receipt.Phase != operatorPhaseRetryPending || receipt.EffectID == "" || effectID != "" && receipt.EffectID != effectID {
			t.Fatalf("Recover receipts did not converge: %#v", state.ControlReceipts)
		}
		effectID = receipt.EffectID
	}
}

func TestRecoverMissingIssueCompletesAdmissionAndReplaysAcrossRestart(t *testing.T) {
	var persisted runtimeOwnerState
	owner, manifest := operatorNeverLaunchedOwner(t, 462, "failed", "failed", func(state runtimeOwnerState) error {
		persisted = cloneRuntimeOwnerState(state)
		return nil
	})
	service := operatorTestMutationService(t, owner)
	service.stopped = true
	var calls atomic.Int32
	service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		calls.Add(1)
		return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true}}, nil
	}
	request := operatorRequest("recover-missing", "recover", manifest, false)
	for range 2 {
		result := service.perform(t.Context(), request)
		if result.OK || result.Status != http.StatusConflict {
			t.Fatalf("missing issue Recover result=%#v", result)
		}
	}
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	if !ok || receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || receipt.Admission != nil || calls.Load() != 1 {
		t.Fatalf("terminal receipt=%#v calls=%d", receipt, calls.Load())
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(state runtimeOwnerState) error {
		persisted = cloneRuntimeOwnerState(state)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	replay := operatorTestMutationService(t, restarted).perform(t.Context(), request)
	if replay.OK || replay.Status != http.StatusConflict || calls.Load() != 1 {
		t.Fatalf("restart replay=%#v calls=%d", replay, calls.Load())
	}
}

func TestLocalDismissUsesHistoricalObservationImmediatelyAfterRestart(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 463, "completed", true)
	before := mustOwnerSnapshot(t, owner)
	issueKey := ownerIssueKey(manifest.Repository, manifest.Issue)
	observationEpoch := before.State.Observations[issueKey].ObservationEpoch
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, before.State, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	current := mustOwnerSnapshot(t, restarted)
	if current.State.Epoch <= observationEpoch || current.State.Observations[issueKey].ObservationEpoch != observationEpoch {
		t.Fatalf("restart did not retain a historical observation: epoch=%d observation=%#v", current.State.Epoch, current.State.Observations[issueKey])
	}
	service := operatorTestMutationService(t, restarted)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	service.stopped = true
	result := service.perform(t.Context(), operatorRequest("dismiss-before-reconcile", "dismiss", manifest, false))
	state := mustOwnerSnapshot(t, restarted).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if !result.OK || result.Status != http.StatusAccepted || state.Tombstones[key].Action != "dismissed" {
		t.Fatalf("historical observation blocked Dismiss: result=%#v tombstone=%#v", result, state.Tombstones[key])
	}
}

func TestUnreservedDismissRejectsHistoricalObservationAfterRestart(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 468, "completed", true)
	before := mustOwnerSnapshot(t, owner)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, before.State, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	command := operatorCommand(mustOwnerSnapshot(t, restarted), operatorRequest("unreserved-historical", "dismiss", manifest, false), manifest)
	if _, _, err := restarted.beginOperatorMutation(t.Context(), command); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("unreserved historical Dismiss err=%v", err)
	}
}

func TestDismissDoesNotOverrideNewerOpenObservationDuringCleanupPreflight(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 467, "completed", true)
	entered, release := make(chan struct{}), make(chan struct{})
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) {
		close(entered)
		<-release
		return true, nil
	}
	service.stopped = true
	result := make(chan controlResult, 1)
	request := operatorRequest("dismiss-reopened", "dismiss", manifest, false)
	go func() { result <- service.perform(t.Context(), request) }()
	<-entered
	reserved, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, request.RequestID)
	if !ok || reserved.Phase != operatorPhaseAdmissionPending || reserved.Admission == nil {
		close(release)
		t.Fatalf("Dismiss close check ran before durable reservation: %#v", reserved)
	}
	snapshot, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	issue := expandIssueFact(snapshot.State.Observations[issueKey].Fact)
	issue.Closed = false
	attempt := expandAttemptFact(snapshot.State.Observations[issueKey].Attempts[attemptKey].Fact)
	input := reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{attempt}}
	if _, err := owner.applyReconciliation(t.Context(), mustCollection(t, snapshot, input)); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	got := <-result
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	if got.OK || got.Status != http.StatusConflict || !ok || receipt.State != "completed" || receipt.Admission != nil || len(state.Tombstones) != 0 || len(state.Effects) != 0 || state.Observations[issueKey].Fact.Closed {
		t.Fatalf("Dismiss overrode reopened issue: result=%#v receipt=%#v state=%#v", got, receipt, state)
	}
}

func TestDismissRetriesAfterNewerAbsentObservationDuringCloseCheck(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 468, "completed", true)
	firstAbsent, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.applyReconciliation(t.Context(), mustCollection(t, firstAbsent, reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true})); err != nil {
		t.Fatal(err)
	}
	issueKey := ownerIssueKey(manifest.Repository, manifest.Issue)
	if mustOwnerSnapshot(t, owner).State.Observations[issueKey].Present {
		t.Fatal("test requires a local orphan with an absent issue observation")
	}

	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return true, nil
	}
	service.stopped = true
	request := operatorRequest("dismiss-absent-cycle", "dismiss", manifest, false)
	result := make(chan controlResult, 1)
	go func() { result <- service.perform(t.Context(), request) }()
	<-entered
	reserved, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, request.RequestID)
	if !ok || reserved.Phase != operatorPhaseAdmissionPending || reserved.Admission == nil || reserved.Admission.RemoteOnly {
		close(release)
		t.Fatalf("local orphan Dismiss was not reserved before close check: %#v", reserved)
	}
	secondAbsent, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.applyReconciliation(t.Context(), mustCollection(t, secondAbsent, reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true})); err != nil {
		close(release)
		t.Fatal(err)
	}
	close(release)
	first := <-result
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, request.RequestID)
	if !first.OK || first.Status != http.StatusAccepted || !ok || receipt.Phase != operatorPhaseAdmissionPending || receipt.Admission == nil || len(state.Tombstones) != 0 || calls.Load() != 1 {
		t.Fatalf("newer absent observation rejected or committed stale Dismiss: result=%#v receipt=%#v tombstones=%#v calls=%d", first, receipt, state.Tombstones, calls.Load())
	}

	second := service.perform(t.Context(), request)
	state = mustOwnerSnapshot(t, owner).State
	if !second.OK || second.Status != http.StatusAccepted || state.Tombstones[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Action != "dismissed" || calls.Load() != 2 {
		t.Fatalf("fresh Dismiss did not commit after absent reconciliation: result=%#v tombstones=%#v calls=%d", second, state.Tombstones, calls.Load())
	}
}

func TestCollectIssueIgnoresStaleDispositionFromDifferentIssue(t *testing.T) {
	root := resolvedTempDir(t)
	first := ownerTestManifest(t, root, 469, 1, "running")
	second := ownerTestManifest(t, root, 470, 1, "running")
	state := runtimeEffectInitialState(first)
	addOperatorObservation(&state, first, "active", false)
	secondIssue, secondAttempt := ownerIssueKey("o/r", second.Issue), ownerAttemptKey("o/r", second.Issue, second.Attempt)
	state.IssueGenerations[secondIssue], state.AttemptGenerations[secondAttempt] = 1, 1
	state.Attempts[secondAttempt] = runtimeAttemptRecord{Generation: 1, Manifest: second}
	addOperatorObservation(&state, second, "active", false)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	service := operatorTestMutationService(t, owner)
	entered, release := make(chan struct{}), make(chan struct{})
	service.collect = func(ctx context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		observation := snapshot.State.Observations[ownerIssueKey("o/r", issue)]
		attempt := observation.Attempts[ownerAttemptKey("o/r", issue, 1)]
		close(entered)
		select {
		case <-release:
			return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(attempt.Fact)}}}, nil
		case <-ctx.Done():
			return reconciliationV2Batch{}, ctx.Err()
		}
	}
	type collected struct {
		snapshot stateOwnerSnapshot
		err      error
	}
	done := make(chan collected, 1)
	go func() {
		snapshot, _, err := service.collectIssue(t.Context(), first.Issue)
		done <- collected{snapshot: snapshot, err: err}
	}()
	<-entered
	staleSnapshot, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	observation := staleSnapshot.State.Observations[secondIssue]
	staleInput := reconciliationInput{Scope: issueScope(second.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{expandIssueFact(observation.Fact)}, Attempts: []internalgithub.RecoveryAttemptFact{expandAttemptFact(observation.Attempts[secondAttempt].Fact)}}
	staleCollection := mustCollection(t, staleSnapshot, staleInput)
	if _, err := owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: second.Issue, ExpectedGeneration: staleSnapshot.State.IssueGenerations[secondIssue]}); err != nil {
		t.Fatal(err)
	}
	beforeStale := mustOwnerSnapshot(t, owner).State.StaleReconciliations
	if _, err := owner.applyReconciliation(t.Context(), staleCollection); err != nil {
		t.Fatal(err)
	}
	if got := mustOwnerSnapshot(t, owner).State.StaleReconciliations; got != beforeStale+1 {
		t.Fatalf("different issue did not record stale collection: before=%d after=%d", beforeStale, got)
	}
	close(release)
	result := <-done
	if result.err != nil || !result.snapshot.State.Observations[ownerIssueKey("o/r", first.Issue)].Present {
		t.Fatalf("different-issue stale disposition rejected valid collect: snapshot=%#v err=%v", result.snapshot, result.err)
	}
}

func TestUnrelatedAttemptMutationDoesNotInvalidateOperatorPreflight(t *testing.T) {
	for _, action := range []string{"cancel", "recover"} {
		t.Run(action, func(t *testing.T) {
			root := resolvedTempDir(t)
			stateName := "running"
			observed := "active"
			if action == "recover" {
				stateName, observed = "failed", "failed"
			}
			primary := ownerTestManifest(t, root, 464, 1, stateName)
			if action == "recover" {
				primary.Version, primary.LaunchToken = agentruntime.ManifestVersion2, strings.Repeat("a", 32)
			}
			other := ownerTestManifest(t, root, 465, 1, "running")
			state := runtimeEffectInitialState(primary)
			addOperatorObservation(&state, primary, observed, false)
			otherIssue, otherAttempt := ownerIssueKey("o/r", other.Issue), ownerAttemptKey("o/r", other.Issue, other.Attempt)
			state.IssueGenerations[otherIssue], state.AttemptGenerations[otherAttempt] = 1, 1
			state.Attempts[otherAttempt] = runtimeAttemptRecord{Generation: 1, Manifest: other}
			addOperatorObservation(&state, other, "active", false)
			state.Epoch, state.Revision = 1, 1
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			refreshOperatorObservation(t, owner)
			service := operatorTestMutationService(t, owner)
			service.stopped = true
			entered, release := make(chan struct{}), make(chan struct{})
			if action == "cancel" {
				service.beforeAdmission = func() { close(entered); <-release }
			} else {
				failed := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: primary.Issue, Attempt: 1, BaseSHA: primary.BaseSHA, State: "failed", Checks: []string{}}
				issue := issueFact(primary.Issue, "recover")
				issue.Attempt, issue.CurrentAttempt, issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, 1, 1, true
				issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
				input := reconciliationInput{Scope: issueScope(primary.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}
				service.collector.Config.ActorID = 42
				service.collector.Config.RetryCommand = "/agent-symphony retry"
				service.collect = func(ctx context.Context, _ stateOwnerSnapshot, _ int) (reconciliationV2Batch, error) {
					close(entered)
					select {
					case <-release:
						return reconciliationV2Batch{Input: input}, nil
					case <-ctx.Done():
						return reconciliationV2Batch{}, ctx.Err()
					}
				}
			}
			result := make(chan controlResult, 1)
			go func() {
				result <- service.perform(t.Context(), operatorRequest(action+"-unrelated", action, primary, false))
			}()
			<-entered
			snapshot, err := owner.reconciliationSnapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			fact := expandIssueFact(snapshot.State.Observations[otherIssue].Fact)
			fact.Title = "unrelated issue changed"
			attempt := expandAttemptFact(snapshot.State.Observations[otherIssue].Attempts[otherAttempt].Fact)
			input := reconciliationInput{Scope: issueScope(other.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{fact}, Attempts: []internalgithub.RecoveryAttemptFact{attempt}}
			beforeRevision := snapshot.State.Revision
			committed, err := owner.applyReconciliation(t.Context(), mustCollection(t, snapshot, input))
			if err != nil || committed.State.Revision <= beforeRevision {
				t.Fatalf("unrelated mutation failed: revision=%d before=%d err=%v", committed.State.Revision, beforeRevision, err)
			}
			close(release)
			got := <-result
			receipt, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, action+"-unrelated")
			if !got.OK || got.Status != http.StatusAccepted || !ok || receipt.EffectID == "" {
				t.Fatalf("unrelated mutation invalidated %s: result=%#v receipt=%#v", action, got, receipt)
			}
		})
	}
}

func TestOperatorAdmissionPersistenceFailureResumesFromDurableReservation(t *testing.T) {
	for _, mode := range []string{"fail", "abort"} {
		t.Run(mode, func(t *testing.T) {
			var persisted runtimeOwnerState
			inject, injected := false, false
			requestID := "recover-persist-" + mode
			persist := func(state runtimeOwnerState) error {
				receipt, exists := operatorReceiptByID(state, requestID)
				terminal := exists && receipt.State == "completed"
				aborted := !exists
				if inject && !injected && (mode == "fail" && terminal || mode == "abort" && aborted) {
					injected = true
					return errors.New("injected admission " + mode + " persistence failure")
				}
				persisted = cloneRuntimeOwnerState(state)
				return nil
			}
			owner, manifest := operatorNeverLaunchedOwner(t, 466, "failed", "failed", persist)
			request := operatorRequest(requestID, "recover", manifest, false)
			service := operatorTestMutationService(t, owner)
			service.stopped = true
			if mode == "fail" {
				service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
					return reconciliationV2Batch{}, errors.New("injected Recover preflight failure")
				}
			} else {
				ctx, cancel := context.WithCancel(t.Context())
				service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
					cancel()
					return reconciliationV2Batch{}, context.Canceled
				}
				inject = true
				result := service.perform(ctx, request)
				if result.OK || result.Status != http.StatusInternalServerError {
					t.Fatalf("abort persistence result=%#v", result)
				}
			}
			if mode == "fail" {
				inject = true
				result := service.perform(t.Context(), request)
				if result.OK || result.Status != http.StatusInternalServerError {
					t.Fatalf("failure persistence result=%#v", result)
				}
			}
			if !injected {
				t.Fatal("persistence failure was not reached")
			}
			current, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, requestID)
			if !ok || current.Phase != operatorPhaseAdmissionPending || current.Admission == nil {
				t.Fatalf("reservation was not retained after persistence failure: %#v", current)
			}
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(state runtimeOwnerState) error {
				persisted = cloneRuntimeOwnerState(state)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.close(context.Background()) })
			restartService := operatorTestMutationService(t, restarted)
			restartService.stopped = true
			if mode == "fail" {
				restartService.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
					return reconciliationV2Batch{}, errors.New("injected Recover preflight failure")
				}
				result := restartService.perform(t.Context(), request)
				if result.OK || result.Status != http.StatusInternalServerError {
					t.Fatalf("resumed terminal failure=%#v", result)
				}
				receipt, _ := operatorReceiptByID(mustOwnerSnapshot(t, restarted).State, requestID)
				if receipt.State != "completed" || receipt.Admission != nil {
					t.Fatalf("failed admission did not terminalize after restart: %#v", receipt)
				}
				return
			}
			failed := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: manifest.Issue, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}}
			issue := issueFact(manifest.Issue, "recover")
			issue.Attempt, issue.CurrentAttempt, issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, 1, 1, true
			issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
			restartService.collector.Config.ActorID = 42
			restartService.collector.Config.RetryCommand = "/agent-symphony retry"
			restartService.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
				return reconciliationV2Batch{Input: reconciliationInput{Scope: issueScope(manifest.Issue), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issue}, Attempts: []internalgithub.RecoveryAttemptFact{failed}}}, nil
			}
			result := restartService.perform(t.Context(), request)
			receipt, _ := operatorReceiptByID(mustOwnerSnapshot(t, restarted).State, requestID)
			if !result.OK || result.Status != http.StatusAccepted || receipt.Phase != operatorPhaseRetryPending || receipt.EffectID == "" {
				t.Fatalf("aborted admission did not resume after restart: result=%#v receipt=%#v", result, receipt)
			}
		})
	}
}

func TestOperatorServiceCancellationAfterOwnerAdmissionReturnsCommittedReceipt(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 344, 1, "running")
	manifest, pane := boundRuntimeEffectTestManifest(t, manifest)
	if pane.PaneID == "" {
		t.Fatal("bound fixture did not return an exact pane identity")
	}
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	entered, release := make(chan struct{}), make(chan struct{})
	writes := 0
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error {
		writes++
		if writes == 3 {
			close(entered)
			<-release
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	service := operatorTestMutationService(t, owner)
	runner := &blockingMissingSessionRunner{entered: make(chan struct{}), release: make(chan struct{})}
	service.effects.executor.Runtime.Runner = runner
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan controlResult, 1)
	go func() {
		result <- service.perform(ctx, operatorRequest("cancel-after-admission", "cancel", manifest, false))
	}()
	<-entered
	cancel()
	select {
	case got := <-result:
		t.Fatalf("operator returned before the admitted owner command completed: %#v", got)
	default:
	}
	close(release)
	got := <-result
	if !got.OK || got.Status != http.StatusAccepted {
		t.Fatalf("result=%#v", got)
	}
	<-runner.entered
	if receipt, ok := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, got.RequestID); !ok || receipt.State != "pending" || receipt.EffectID == "" {
		t.Fatalf("admitted receipt=%#v exists=%v", receipt, ok)
	}
	close(runner.release)
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	final := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(final, got.RequestID)
	effect := final.Effects[receipt.EffectID]
	if !ok || receipt.State != "completed" || receipt.EffectID == "" || effect.State != "completed" || effect.Action != string(agentruntime.EffectStop) || runner.calls.Load() != 2 {
		t.Fatalf("completed receipt=%#v exists=%v effect=%#v boundary_calls=%d", receipt, ok, effect, runner.calls.Load())
	}
}

type blockingMissingSessionRunner struct {
	entered chan struct{}
	release chan struct{}
	calls   atomic.Int32
	once    sync.Once
}

func (r *blockingMissingSessionRunner) Run(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	r.calls.Add(1)
	r.once.Do(func() { close(r.entered) })
	select {
	case <-r.release:
	case <-ctx.Done():
		return agentruntime.Result{}, ctx.Err()
	}
	return missingTmuxSession(command), errors.New("missing session")
}

func TestOperatorWorkerFailureIsClassifiedOnceWithoutRetry(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 345, 1, "completed")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "completed", false)
	state.Epoch, state.Revision = 1, 1
	classified := make(chan struct{})
	var once sync.Once
	owner, err := startTestStateOwner(t, root, state, func(candidate runtimeOwnerState) error {
		for _, effect := range candidate.Effects {
			if effect.Diagnostic != "" {
				once.Do(func() { close(classified) })
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	for _, path := range []string{manifest.Worktree, productionSnapshotRoot(root)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	boundary := &operatorCleanupBoundary{path: manifest.Worktree, err: errors.New("injected cleanup failure")}
	service := operatorServiceWithCleanup(t, owner, t.Context(), boundary)
	result := service.perform(t.Context(), operatorRequest("worker-failure", "archive", manifest, true))
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("result=%#v", result)
	}
	<-classified
	final := mustOwnerSnapshot(t, owner).State
	receipt, _ := operatorReceiptByID(final, result.RequestID)
	effect := final.Effects[receipt.EffectID]
	if boundary.calls.Load() != 1 || receipt.State != "pending" || effect.State != "pending" || !strings.Contains(effect.Diagnostic, "injected cleanup failure") {
		t.Fatalf("calls=%d receipt=%#v effect=%#v", boundary.calls.Load(), receipt, effect)
	}
}

func TestOperatorVerificationForDifferentAttemptsDoesNotSerialize(t *testing.T) {
	root := resolvedTempDir(t)
	manifests := []agentruntime.Manifest{
		ownerTestManifest(t, root, 346, 1, "running"),
		ownerTestManifest(t, root, 347, 1, "running"),
	}
	state := newRuntimeOwnerState("o/r")
	for _, manifest := range manifests {
		issueKey, attemptKey := ownerIssueKey("o/r", manifest.Issue), ownerAttemptKey("o/r", manifest.Issue, 1)
		state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
		state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
		addOperatorObservation(&state, manifest, "active", false)
		if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	runner := &concurrentOwnedRunner{branches: map[string]string{manifests[0].Worktree: manifests[0].Branch, manifests[1].Worktree: manifests[1].Branch}, entered: make(chan struct{}), release: make(chan struct{})}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: runner, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	effects, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	service.effects = effects
	results := make(chan controlResult, 2)
	for index, manifest := range manifests {
		go func(index int, manifest agentruntime.Manifest) {
			results <- service.perform(t.Context(), operatorRequest(fmt.Sprintf("parallel-verify-%d", index), "recover", manifest, false))
		}(index, manifest)
	}
	<-runner.entered
	close(runner.release)
	for range manifests {
		if result := <-results; result.Status != http.StatusConflict && result.Status != http.StatusAccepted {
			t.Fatalf("result=%#v", result)
		}
	}
}

func TestConcurrentConflictingDestructiveActionsCommitOnlyOneTombstone(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 349, "completed", true)
	for _, path := range []string{manifest.Worktree, productionSnapshotRoot(owner.stateRoot)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	service.effects.stopped = true
	start := make(chan struct{})
	results := make(chan controlResult, 2)
	for _, action := range []string{"dismiss", "archive"} {
		go func(action string) {
			<-start
			results <- service.perform(t.Context(), operatorRequest("conflicting-"+action, action, manifest, action == "archive"))
		}(action)
	}
	close(start)
	accepted, conflicts := 0, 0
	for range 2 {
		switch result := <-results; result.Status {
		case http.StatusOK, http.StatusAccepted:
			accepted++
		case http.StatusConflict:
			conflicts++
		default:
			t.Fatalf("unexpected result=%#v", result)
		}
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)
	if accepted != 1 || conflicts != 1 || len(state.Tombstones) != 1 || state.Tombstones[key].Generation != 2 || len(state.ControlReceipts) != 1 {
		t.Fatalf("accepted=%d conflicts=%d state=%#v", accepted, conflicts, state)
	}
}

func TestCleanupInvalidatesBlockedGitHubBindBeforeMutation(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 350, 1, "preparing")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "orphaned", false)
	issueKey := ownerIssueKey("o/r", manifest.Issue)
	observation := state.Observations[issueKey]
	observation.Fact.DispatchAuthorized = true
	observation.Fact.BaseSHA = manifest.BaseSHA
	state.Observations[issueKey] = observation
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	for _, path := range []string{manifest.Worktree, productionSnapshotRoot(root)} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
	plans, err := planReconciliationBinds(mustOwnerSnapshot(t, owner), cfg)
	if err != nil || len(plans) != 1 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	entered := make(chan struct{})
	var requests atomic.Int32
	api := internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		close(entered)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}, Retries: -1}
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.shutdown(ctx); err != nil {
			t.Errorf("join Abandon cleanup before owner teardown: %v", err)
		}
	})
	effects := service.effects
	plan, err := effects.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := effects.executeGitHubBind(t.Context(), api, plan)
		done <- err
	}()
	<-entered
	result := service.perform(t.Context(), operatorRequest("abandon-blocked-bind", "abandon", manifest, true))
	if !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("abandon result=%#v", result)
	}
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("bind err=%v", err)
	}
	if requests.Load() != 1 {
		t.Fatalf("GitHub bind performed %d requests after invalidation", requests.Load())
	}
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	final := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey("o/r", manifest.Issue, 1)
	tombstone := final.Tombstones[key]
	receipt, ok := operatorReceiptByID(final, "abandon-blocked-bind")
	if tombstone.Action != "abandoned" || tombstone.CleanupPhase != "completed" || final.Effects[tombstone.EffectID].State != "completed" || !ok || receipt.State != "completed" || receipt.Result == nil || !receipt.Result.OK {
		t.Fatalf("Abandon did not complete after blocked GitHub bind: tombstone=%#v receipt=%#v found=%t effect=%#v", tombstone, receipt, ok, final.Effects[tombstone.EffectID])
	}
	if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("Abandon left worktree after completed cleanup: %v", err)
	}
}

func TestGitHubInputChangesWhileRecoverEffectIsInFlightRetainsIntentWithoutMutation(t *testing.T) {
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
	var githubRequests atomic.Int32
	service.collector.API = internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		githubRequests.Add(1)
		return nil, fmt.Errorf("unexpected GitHub mutation after changed input: %s", request.URL.String())
	})}, Retries: -1}
	tampered := activeInput
	tampered.Issues = slices.Clone(activeInput.Issues)
	tampered.Issues[0].Body = "changed after durable authorization"
	service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		return reconciliationV2Batch{Input: tampered}, nil
	}
	if err := service.resumeReceipt(t.Context(), request.RequestID); err == nil {
		t.Fatalf("tampered recovery err=%v", err)
	}
	stillPending, _ := operatorReceiptByID(mustOwnerSnapshot(t, restarted).State, request.RequestID)
	if stillPending.State != "pending" || stillPending.Phase != operatorPhaseTerminal {
		t.Fatalf("tampered recovery changed receipt: %#v", stillPending)
	}
	final := mustOwnerSnapshot(t, restarted).State
	retained := final.Effects[stillPending.EffectID]
	if retained.State != "pending" || retained.Diagnostic == "" {
		t.Fatalf("changed external input was not retained as ambiguous: receipt=%#v effect=%#v", stillPending, retained)
	}
	select {
	case <-completed:
		t.Fatal("changed external input completed the old durable intent")
	default:
	}
	if githubRequests.Load() != 0 {
		t.Fatalf("changed GitHub input caused %d external requests", githubRequests.Load())
	}
}

func TestOperatorRecoverResumesAwaitingAndMarkerBeforeLedgerCheckpointsOnce(t *testing.T) {
	for _, test := range []struct {
		name       string
		checkpoint string
		marked     bool
		attached   bool
		posted     bool
	}{
		{name: "terminal-awaiting-single", checkpoint: operatorPhaseTerminalAwait},
		{name: "terminal-awaiting-attached", checkpoint: operatorPhaseTerminalAwait, attached: true},
		{name: "retry-awaiting-single", checkpoint: operatorPhaseRetryAwait},
		{name: "retry-awaiting-attached", checkpoint: operatorPhaseRetryAwait, attached: true},
		{name: "stop-unproved-single", checkpoint: operatorPhaseStopPending},
		{name: "stop-unproved-attached", checkpoint: operatorPhaseStopPending, attached: true},
		{name: "terminal-marker-single", checkpoint: operatorPhaseTerminal, marked: true},
		{name: "terminal-marker-attached", checkpoint: operatorPhaseTerminal, marked: true, attached: true},
		{name: "retry-marker-single", checkpoint: operatorPhaseRetryPending, marked: true},
		{name: "retry-marker-attached", checkpoint: operatorPhaseRetryPending, marked: true, attached: true},
		{name: "retry-posted-unmarked", checkpoint: operatorPhaseRetryPending, posted: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner, manifest := operatorNeverLaunchedOwner(t, 348, "running", "active", func(runtimeOwnerState) error { return nil })
			service := operatorTestMutationService(t, owner)
			request := operatorRequest("recover-checkpoint", "recover", manifest, false)
			command := operatorCommand(mustOwnerSnapshot(t, owner), request, manifest)
			stopRequest, err := service.prepareStop(manifest, "dashboard recovery: runtime liveness mismatch")
			if err != nil {
				t.Fatal(err)
			}
			command.LivenessFailed = true
			command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: stopRequest.Reason, RequestDigest: stopRequest.Identity.RequestDigest}
			_, stopEffect, err := owner.beginOperatorMutation(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			cancelled := manifest
			cancelled.State, cancelled.Diagnostic, cancelled.UpdatedAt = "cancelled", command.Runtime.Reason, time.Unix(80, 0).UTC()
			if test.checkpoint != operatorPhaseStopPending {
				if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stopEffect)), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
					t.Fatal(err)
				}
			}
			active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 348, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
			activeIssue := issueFact(348, "recover")
			activeIssue.Attempt, activeIssue.CurrentAttempt, activeIssue.Active, activeIssue.ActiveAttempt = 1, 1, true, &active
			activeInput := repositoryInput(true, activeIssue)
			activeInput.Attempts = []internalgithub.RecoveryAttemptFact{active}
			failed := active
			failed.State = "failed"
			failedIssue := issueFact(348, "recover")
			failedIssue.Attempt, failedIssue.CurrentAttempt, failedIssue.RecoveryAuthorized = 1, 1, true
			failedIssue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
			failedInput := repositoryInput(true, failedIssue)
			failedInput.Attempts = []internalgithub.RecoveryAttemptFact{failed}

			var terminalEffect, retryEffect *runtimeEffectIntent
			if test.checkpoint != operatorPhaseStopPending && test.checkpoint != operatorPhaseTerminalAwait {
				terminal := applyAndPlanRecover(t, owner, activeInput, activeInput.Attempts, githubIssueTerminalFailure)
				terminalEffect = advanceOperatorRecoverPlan(t, owner, request.RequestID, terminal)
				if test.checkpoint != operatorPhaseTerminal {
					result := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueTerminalFailure, Observed: true}}
					if _, err := owner.finishOperatorReconciliationEffect(t.Context(), finishOperatorReconciliationEffectCommand{Finish: finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*terminalEffect), Result: result}}); err != nil {
						t.Fatal(err)
					}
				}
			}
			if test.checkpoint == operatorPhaseRetryPending {
				retry := applyAndPlanRecover(t, owner, failedInput, failedInput.Attempts, githubIssueRetry)
				retryEffect = advanceOperatorRecoverPlan(t, owner, request.RequestID, retry)
				newAttempt := ownerTestManifest(t, owner.stateRoot, 348, 2, "running")
				beforeBinding := mustOwnerSnapshot(t, owner)
				if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: newAttempt, ExpectedIssueGeneration: beforeBinding.State.IssueGenerations[ownerIssueKey("o/r", 348)]}); err == nil {
					t.Fatal("new attempt bound before pending retry receipt completed")
				}
				afterBinding := mustOwnerSnapshot(t, owner)
				if afterBinding.State.IssueGenerations[ownerIssueKey("o/r", 348)] != beforeBinding.State.IssueGenerations[ownerIssueKey("o/r", 348)] || afterBinding.State.Attempts[ownerAttemptKey("o/r", 348, 2)].Generation != 0 {
					t.Fatal("rejected new attempt binding changed owner state")
				}
			}
			attachedRequest := operatorRequest("recover-checkpoint-attached", "recover", manifest, false)
			if test.attached {
				attach, ok := service.recoveryAttachCommand(mustOwnerSnapshot(t, owner), attachedRequest)
				if !ok {
					t.Fatal("recovery checkpoint did not accept attached receipt")
				}
				if _, _, err := owner.beginOperatorMutation(t.Context(), attach); err != nil {
					t.Fatal(err)
				}
			}
			if test.marked {
				effect := terminalEffect
				kind := githubIssueTerminalFailure
				if retryEffect != nil {
					effect, kind = retryEffect, githubIssueRetry
				}
				result := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: kind, Observed: true}}
				if err := writeReconciliationEffectMarker(owner.stateRoot, ownerReconciliationEffectIdentity(*effect), *effect.Reconciliation, result); err != nil {
					t.Fatal(err)
				}
			}
			var postedComments []map[string]any
			var priorPosts atomic.Int32
			if test.posted {
				failedAt := cancelled.UpdatedAt
				terminalMarker, err := internalgithub.TerminalFailureMarker(348, 1, failedAt)
				if err != nil {
					t.Fatal(err)
				}
				postedComments = []map[string]any{{"id": 1, "body": terminalMarker, "created_at": failedAt, "updated_at": failedAt, "user": map[string]any{"id": 42}}}
				client := &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
					if request.Method != http.MethodPost || !strings.HasSuffix(request.URL.Path, "/issues/348/comments") {
						return nil, fmt.Errorf("unexpected pre-crash request %s %s", request.Method, request.URL)
					}
					var input struct{ Body string }
					if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
						return nil, err
					}
					priorPosts.Add(1)
					postedComments = append(postedComments, map[string]any{"id": 2, "body": input.Body, "created_at": failedAt.Add(time.Second), "updated_at": failedAt.Add(time.Second), "user": map[string]any{"id": 42}})
					return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: request}, nil
				})}
				post, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "https://example.test/repos/o/r/issues/348/comments", strings.NewReader(`{"body":"/agent-symphony retry"}`))
				if err != nil {
					t.Fatal(err)
				}
				response, err := client.Do(post)
				if err != nil {
					t.Fatal(err)
				}
				response.Body.Close()
			}
			before := mustOwnerSnapshot(t, owner)
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			restarted, err := startTestStateOwner(t, owner.stateRoot, before.State, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.close(context.Background()) })
			service = operatorTestMutationService(t, restarted)
			service.collector.Config.ActorID = 42
			service.collector.Config.RetryCommand = "/agent-symphony retry"
			inputs := []reconciliationInput{}
			switch test.checkpoint {
			case operatorPhaseStopPending, operatorPhaseTerminalAwait:
				inputs = []reconciliationInput{activeInput, failedInput}
			case operatorPhaseRetryAwait, operatorPhaseTerminal:
				inputs = []reconciliationInput{failedInput}
			case operatorPhaseRetryPending:
				if test.posted {
					posted := failedInput
					posted.Issues = slices.Clone(failedInput.Issues)
					posted.Issues[0].Retry = true
					inputs = []reconciliationInput{posted}
				}
			}
			var markerFailureAt time.Time
			stopCollections := 0
			service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, _ int) (reconciliationV2Batch, error) {
				if test.checkpoint == operatorPhaseStopPending {
					current := snapshot.State.Attempts[ownerAttemptKey("o/r", 348, 1)].Manifest
					markerFailureAt = current.UpdatedAt
					fact := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 348, Attempt: 1, BaseSHA: current.BaseSHA, State: "active", Checks: []string{}}
					issue := issueFact(348, "recover")
					issue.Attempt, issue.CurrentAttempt = 1, 1
					if stopCollections == 0 {
						issue.Active, issue.ActiveAttempt = true, &fact
					} else {
						fact.State = "failed"
						issue.RecoveryAttempt, issue.RecoveryAuthorized = 1, true
						issue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{fact}
					}
					stopCollections++
					input := repositoryInput(true, issue)
					input.Attempts = []internalgithub.RecoveryAttemptFact{fact}
					return reconciliationV2Batch{Input: input}, nil
				}
				if len(inputs) == 0 {
					return reconciliationV2Batch{}, errors.New("unexpected duplicate recovery collection")
				}
				input := inputs[0]
				inputs = inputs[1:]
				return reconciliationV2Batch{Input: input}, nil
			}
			var githubWrites atomic.Int32
			service.collector.API = internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
				if request.Method != http.MethodGet {
					githubWrites.Add(1)
					return nil, fmt.Errorf("unexpected GitHub mutation while replaying receipt: %s", request.URL.String())
				}
				if !strings.Contains(request.URL.Path, "/comments") {
					return nil, fmt.Errorf("unexpected GitHub request %s", request.URL.String())
				}
				if test.posted {
					encoded, _ := json.Marshal(postedComments)
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(encoded)), Request: request}, nil
				}
				failedAt := cancelled.UpdatedAt
				if !markerFailureAt.IsZero() {
					failedAt = markerFailureAt
				}
				terminalMarker, _ := internalgithub.TerminalFailureMarker(348, 1, failedAt)
				encoded, _ := json.Marshal([]map[string]any{
					{"id": 1, "body": terminalMarker, "created_at": failedAt, "updated_at": failedAt, "user": map[string]any{"id": 42}},
					{"id": 2, "body": "/agent-symphony retry", "created_at": failedAt.Add(time.Second), "updated_at": failedAt.Add(time.Second), "user": map[string]any{"id": 42}},
				})
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(encoded)), Request: request}, nil
			})}, Retries: -1}
			if err := service.resumePending(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := service.shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			final := mustOwnerSnapshot(t, restarted).State
			original, _ := operatorReceiptByID(final, request.RequestID)
			attached, attachedFound := operatorReceiptByID(final, attachedRequest.RequestID)
			if test.checkpoint == operatorPhaseStopPending {
				if original.State != "pending" || original.Phase != operatorPhaseStopPending || attachedFound != test.attached || test.attached && (attached.State != "pending" || attached.Phase != operatorPhaseStopPending || attached.EffectID != original.EffectID) || final.Effects[original.EffectID].State != "pending" || stopCollections != 0 {
					t.Fatalf("unproved Stop advanced: original=%#v attached=%#v effect=%#v collections=%d", original, attached, final.Effects[original.EffectID], stopCollections)
				}
				return
			}
			wantEffects := 2
			remainingInputs := len(inputs)
			if original.State != "completed" || attachedFound != test.attached || test.attached && (attached.State != "completed" || original.EffectID != attached.EffectID) || len(final.Effects) != wantEffects || remainingInputs != 0 {
				t.Fatalf("original=%#v attached=%#v effects=%#v remaining_inputs=%d", original, attached, final.Effects, remainingInputs)
			}
			if test.posted && (priorPosts.Load() != 1 || githubWrites.Load() != 0) {
				t.Fatalf("retry posts before crash=%d after restart=%d", priorPosts.Load(), githubWrites.Load())
			}
		})
	}
}

func TestAwaitingRecoveryFailureIsDurableAndOnlyExplicitReplayRetries(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 350, "active", false)
	request := operatorRequest("recover-awaiting-diagnostic", "recover", manifest, false)
	service := operatorTestMutationService(t, owner)
	stop, err := service.prepareStop(manifest, "dashboard recovery: runtime liveness mismatch")
	if err != nil {
		t.Fatal(err)
	}
	command := operatorCommand(mustOwnerSnapshot(t, owner), request, manifest)
	command.LivenessFailed = true
	command.Runtime = &beginRuntimeEffectCommand{Identity: command.Identity, Action: agentruntime.EffectStop, Manifest: manifest, Reason: stop.Reason, RequestDigest: stop.Identity.RequestDigest}
	_, stopEffect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	cancelled := manifest
	cancelled.State, cancelled.Diagnostic, cancelled.UpdatedAt = "cancelled", stop.Reason, time.Unix(90, 0).UTC()
	if _, err := owner.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*stopEffect)), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
		t.Fatal(err)
	}
	active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 350, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "active", Checks: []string{}}
	activeIssue := issueFact(350, "recover")
	activeIssue.Attempt, activeIssue.CurrentAttempt, activeIssue.Active, activeIssue.ActiveAttempt = 1, 1, true, &active
	activeInput := repositoryInput(true, activeIssue)
	activeInput.Attempts = []internalgithub.RecoveryAttemptFact{active}
	terminal := applyAndPlanRecover(t, owner, activeInput, activeInput.Attempts, githubIssueTerminalFailure)
	terminalEffect := advanceOperatorRecoverPlan(t, owner, request.RequestID, terminal)
	terminalResult := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueTerminalFailure, Observed: true}}
	if _, err := owner.finishOperatorReconciliationEffect(t.Context(), finishOperatorReconciliationEffectCommand{Finish: finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*terminalEffect), Result: terminalResult}}); err != nil {
		t.Fatal(err)
	}

	var collections atomic.Int32
	service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		collections.Add(1)
		return reconciliationV2Batch{}, errors.New("GitHub collection unavailable")
	}
	if err := service.resumePending(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	pending, _ := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, request.RequestID)
	if pending.Phase != operatorPhaseRetryAwait || pending.Diagnostic == "" || collections.Load() != 1 {
		t.Fatalf("pending=%#v collections=%d", pending, collections.Load())
	}

	service = operatorTestMutationService(t, owner)
	service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		collections.Add(1)
		return reconciliationV2Batch{}, errors.New("automatic retry should remain suppressed")
	}
	if err := service.resumePending(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if collections.Load() != 1 {
		t.Fatalf("diagnosed receipt retried automatically: collections=%d", collections.Load())
	}

	failed := active
	failed.State = "failed"
	failedIssue := issueFact(350, "recover")
	failedIssue.Attempt, failedIssue.CurrentAttempt, failedIssue.RecoveryAttempt, failedIssue.RecoveryAuthorized = 1, 1, 1, true
	failedIssue.TerminalAttempts = []internalgithub.RecoveryAttemptFact{failed}
	failedInput := repositoryInput(true, failedIssue)
	failedInput.Attempts = []internalgithub.RecoveryAttemptFact{failed}
	service = operatorTestMutationService(t, owner)
	service.collector.Config.ActorID = 42
	service.collector.Config.RetryCommand = "/agent-symphony retry"
	service.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
		collections.Add(1)
		return reconciliationV2Batch{Input: failedInput}, nil
	}
	terminalMarker, _ := internalgithub.TerminalFailureMarker(350, 1, cancelled.UpdatedAt)
	service.collector.API = issueUpdateAppliedAPI(t, map[int][]map[string]any{350: {
		{"id": 1, "body": terminalMarker, "created_at": cancelled.UpdatedAt, "updated_at": cancelled.UpdatedAt, "user": map[string]any{"id": 42}},
		{"id": 2, "body": "/agent-symphony retry", "created_at": cancelled.UpdatedAt.Add(time.Second), "updated_at": cancelled.UpdatedAt.Add(time.Second), "user": map[string]any{"id": 42}},
	}})
	diagnosticReady, releaseDiagnostic := make(chan struct{}), make(chan struct{})
	diagnosticResult := make(chan error, 1)
	go func(stale controlReceipt) {
		close(diagnosticReady)
		<-releaseDiagnostic
		diagnosticResult <- service.recordAwaitingDiagnostic(stale, errors.New("delayed collection failure"))
	}(pending)
	<-diagnosticReady
	fresh, batch, err := service.collectIssue(t.Context(), request.Issue)
	if err != nil {
		t.Fatal(err)
	}
	command, _, err = service.prepareRecoveryAdmission(fresh, batch, request, githubIssueRetry)
	if err != nil {
		t.Fatal(err)
	}
	advanced, retryEffect, err := owner.advanceOperatorRecovery(t.Context(), advanceOperatorRecoveryCommand{RequestID: request.RequestID, Identity: command.Identity, Reconciliation: *command.Reconciliation})
	if err != nil {
		t.Fatal(err)
	}
	close(releaseDiagnostic)
	if err := <-diagnosticResult; !errors.Is(err, errStaleStateResult) {
		t.Fatalf("delayed diagnostic err=%v", err)
	}
	advancedReceipt, _ := operatorReceiptByID(advanced.State, request.RequestID)
	if advancedReceipt.Phase != operatorPhaseRetryPending || advancedReceipt.EffectID != retryEffect.ID || advancedReceipt.Diagnostic != "" || advancedReceipt.EffectID == pending.EffectID {
		t.Fatalf("delayed diagnostic rolled back receipt: before=%#v after=%#v effect=%#v", pending, advancedReceipt, retryEffect)
	}
	result := service.performSynchronously(t.Context(), request)
	if !result.OK || result.Status != http.StatusOK {
		t.Fatalf("replay result=%#v", result)
	}
	completed, _ := operatorReceiptByID(mustOwnerSnapshot(t, owner).State, request.RequestID)
	if completed.State != "completed" || completed.Diagnostic != "" || collections.Load() != 3 {
		t.Fatalf("completed=%#v collections=%d", completed, collections.Load())
	}
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
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: &barrierEffectRunner{}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	boundary := absentSessionBoundary{}
	cleanup := operatorCleanupExecutor{stateRoot: owner.stateRoot, owner: owner, implementation: boundary, reviewer: boundary, runtime: runtimeState}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState, Cleanup: cleanup.execute, VerifyCleanup: cleanup.verify}, active: map[string]*activeRuntimeEffect{}}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := effects.shutdown(ctx); err != nil {
			t.Errorf("stop operator test effects: %v", err)
		}
	})
	return &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, cleanup: cleanup, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: boundary}
}

func TestOperatorCollectionPersistsConditionalGitHubReadsAcrossCacheReload(t *testing.T) {
	owner, _ := operatorTestOwner(t, 350, "completed", true)
	cachePath := filepath.Join(owner.stateRoot, "github-etag-cache.json")
	var conditional atomic.Int64
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var value any
		switch request.URL.RequestURI() {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1":
			value = []any{}
		case "/repos/o/r":
			value = map[string]any{"default_branch": "main"}
		case "/repos/o/r/branches/main":
			value = map[string]any{"commit": map[string]any{"sha": strings.Repeat("a", 40)}}
		case "/repos/o/r/issues/351":
			value = map[string]any{"number": 351, "state": "closed"}
		default:
			return nil, fmt.Errorf("unexpected GitHub read %s", request.URL.String())
		}
		header := make(http.Header)
		header.Set("ETag", `"operator-unchanged"`)
		if request.Header.Get("If-None-Match") == `"operator-unchanged"` {
			conditional.Add(1)
			return &http.Response{StatusCode: http.StatusNotModified, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
	})}}
	collect := func(cache *internalgithub.ReadCache) {
		service := operatorTestMutationService(t, owner)
		service.collector.API = api
		service.collector.API.Cache = cache
		service.collect = func(ctx context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
			bound := service.collector
			bound.Scope = reconciliationScope{Kind: reconciliationIssueScope, Repository: snapshot.State.Repository, Issue: issue}
			return bound.collect(ctx, snapshot)
		}
		if _, _, err := service.collectIssue(t.Context(), 351); err != nil {
			t.Fatal(err)
		}
		if err := service.shutdown(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	cache, err := internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	collect(cache)
	if conditional.Load() != 0 {
		t.Fatal("first operator collection unexpectedly used a conditional read")
	}
	cache, err = internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatalf("operator-only collection did not persist the cache: %v", err)
	}
	collect(cache)
	if conditional.Load() != 4 {
		t.Fatalf("reloaded operator cache did not reuse all four ETags: conditional reads=%d", conditional.Load())
	}
}

func TestOperatorCollectionCommitsWhenGitHubCacheSaveFails(t *testing.T) {
	owner, _ := operatorTestOwner(t, 352, "completed", true)
	cachePath := filepath.Join(owner.stateRoot, "github-etag-cache.json")
	cache, err := internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(cachePath, 0o700); err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	var warning bytes.Buffer
	service.cacheLog = &warning
	service.collector.API = internalgithub.API{Cache: cache}
	// The cache is dirty only after a successful read; seed it through its API.
	service.collector.API.BaseURL = "https://example.test"
	service.collector.API.HTTP = &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Etag": []string{`"fresh"`}}, Body: io.NopCloser(strings.NewReader(`{"number":353}`)), Request: request}, nil
	})}
	service.collect = func(ctx context.Context, _ stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		var fact struct{ Number int }
		if _, _, err := service.collector.API.Read(ctx, "/repos/o/r/issues/353", "", &fact); err != nil {
			return reconciliationV2Batch{}, err
		}
		return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: "o/r", Issue: issue}, Complete: true}}, nil
	}
	before := mustOwnerSnapshot(t, owner).State.Revision
	if _, _, err := service.collectIssue(t.Context(), 353); err != nil {
		t.Fatalf("cache persistence failure blocked operator collection: %v", err)
	}
	if err := service.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if after := mustOwnerSnapshot(t, owner).State.Revision; after <= before {
		t.Fatalf("cache persistence failure blocked owner commit: before=%d after=%d", before, after)
	}
	if !strings.Contains(warning.String(), "save GitHub cache after operator collection") {
		t.Fatalf("cache persistence failure was not logged: %q", warning.String())
	}
}

func TestOperatorCollectionDoesNotWaitForCacheSaveAndShutdownDrainsNewerReads(t *testing.T) {
	owner, _ := operatorTestOwner(t, 356, "completed", true)
	cachePath := filepath.Join(owner.stateRoot, "github-etag-cache.json")
	cache, err := internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	var conditional, saves atomic.Int64
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, Cache: cache, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		issue := strings.TrimPrefix(request.URL.Path, "/repos/o/r/issues/")
		if issue != "357" && issue != "358" {
			return nil, fmt.Errorf("unexpected GitHub read %s", request.URL.String())
		}
		etag := `"` + issue + `"`
		header := make(http.Header)
		header.Set("ETag", etag)
		if request.Header.Get("If-None-Match") == etag {
			conditional.Add(1)
			return &http.Response{StatusCode: http.StatusNotModified, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`{"number":` + issue + `}`)), Request: request}, nil
	})}}
	service := operatorTestMutationService(t, owner)
	service.collector.API = api
	service.collect = func(ctx context.Context, _ stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		var fact struct{ Number int }
		if _, _, err := service.collector.API.Read(ctx, fmt.Sprintf("/repos/o/r/issues/%d", issue), "", &fact); err != nil || fact.Number != issue {
			return reconciliationV2Batch{}, fmt.Errorf("operator GitHub fact %d: got=%d err=%v", issue, fact.Number, err)
		}
		return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: "o/r", Issue: issue}, Complete: true}}, nil
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	service.cacheSave = func() error {
		call := saves.Add(1)
		err := cache.Save()
		if call == 1 {
			close(entered)
			<-release
		}
		return err
	}
	collect := func(issue int) <-chan error {
		result := make(chan error, 1)
		go func() {
			_, _, err := service.collectIssue(t.Context(), issue)
			result <- err
		}()
		return result
	}
	before := mustOwnerSnapshot(t, owner).State.Revision
	first := collect(357)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first cache save did not start")
	}
	select {
	case err := <-first:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("operator collection waited for blocked cache save")
	}
	second := collect(358)
	select {
	case err := <-second:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("second operator collection waited for blocked cache save")
	}
	if after := mustOwnerSnapshot(t, owner).State.Revision; after <= before {
		t.Fatalf("blocked cache save prevented owner commits: before=%d after=%d", before, after)
	}
	if saves.Load() != 1 {
		t.Fatalf("blocked cache save started extra writers: %d", saves.Load())
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- service.shutdown(t.Context()) }()
	once.Do(func() { close(release) })
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	if saves.Load() < 2 {
		t.Fatalf("second collection was not flushed after blocked save: saves=%d", saves.Load())
	}
	reloaded, err := internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	api.Cache = reloaded
	for _, issue := range []int{357, 358} {
		var fact struct{ Number int }
		if _, changed, err := api.Read(t.Context(), fmt.Sprintf("/repos/o/r/issues/%d", issue), "", &fact); err != nil || changed || fact.Number != issue {
			t.Fatalf("reloaded operator read issue=%d changed=%v fact=%#v err=%v", issue, changed, fact, err)
		}
	}
	if conditional.Load() != 2 {
		t.Fatalf("persisted operator reads did not send two conditional GETs: %d", conditional.Load())
	}
}

func TestOperatorShutdownDrainsCollectionStartedBeforeShutdown(t *testing.T) {
	owner, _ := operatorTestOwner(t, 359, "completed", true)
	cachePath := filepath.Join(owner.stateRoot, "github-etag-cache.json")
	cache, err := internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	var conditional atomic.Bool
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, Cache: cache, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Path != "/repos/o/r/issues/360" {
			return nil, fmt.Errorf("unexpected GitHub read %s", request.URL.String())
		}
		header := make(http.Header)
		header.Set("ETag", `"shutdown"`)
		if request.Header.Get("If-None-Match") == `"shutdown"` {
			conditional.Store(true)
			return &http.Response{StatusCode: http.StatusNotModified, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(`{"number":360}`)), Request: request}, nil
	})}}
	service := operatorTestMutationService(t, owner)
	service.collector.API = api
	service.stopping = make(chan struct{})
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	service.collect = func(ctx context.Context, _ stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		var fact struct{ Number int }
		if _, _, err := api.Read(ctx, "/repos/o/r/issues/360", "", &fact); err != nil || fact.Number != issue {
			return reconciliationV2Batch{}, fmt.Errorf("operator GitHub fact %d: got=%d err=%v", issue, fact.Number, err)
		}
		close(entered)
		<-release
		return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: "o/r", Issue: issue}, Complete: true}}, nil
	}
	collectDone := make(chan error, 1)
	go func() {
		_, _, err := service.collectIssue(t.Context(), 360)
		collectDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("operator collection did not reach GitHub barrier")
	}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- service.shutdown(t.Context()) }()
	select {
	case <-service.stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("operator shutdown did not begin")
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown returned before in-flight collection completed: %v", err)
	case <-time.After(100 * time.Millisecond): // Bounded negative assertion; collection remains held at the barrier.
	}
	once.Do(func() { close(release) })
	if err := <-collectDone; err != nil {
		t.Fatal(err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	reloaded, err := internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatal(err)
	}
	api.Cache = reloaded
	var fact struct{ Number int }
	if _, changed, err := api.Read(t.Context(), "/repos/o/r/issues/360", "", &fact); err != nil || changed || fact.Number != 360 || !conditional.Load() {
		t.Fatalf("shutdown lost in-flight operator ETag: changed=%v fact=%#v conditional=%v err=%v", changed, fact, conditional.Load(), err)
	}
	if _, _, err := service.collectIssue(t.Context(), 360); !errors.Is(err, context.Canceled) {
		t.Fatalf("collection began after shutdown: %v", err)
	}
}

func TestOperatorShutdownDrainsAcceptedRecoveryCollection(t *testing.T) {
	owner, _ := operatorTestOwner(t, 361, "completed", true)
	service := operatorTestMutationService(t, owner)
	service.stopping = make(chan struct{})
	service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: snapshot.State.Repository, Issue: issue}, Complete: true}}, nil
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	result := make(chan error, 1)
	if !service.start("accepted-recovery", func() {
		close(entered)
		<-release
		_, _, err := service.collectIssue(t.Context(), 362)
		result <- err
	}) {
		t.Fatal("recovery worker was not accepted")
	}
	<-entered
	before := mustOwnerSnapshot(t, owner).State.Revision
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- service.shutdown(t.Context()) }()
	select {
	case <-service.stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("operator shutdown did not begin")
	}
	once.Do(func() { close(release) })
	if err := <-result; err != nil {
		t.Fatalf("accepted recovery could not collect during shutdown: %v", err)
	}
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	if after := mustOwnerSnapshot(t, owner).State.Revision; after <= before {
		t.Fatalf("accepted recovery did not commit: before=%d after=%d", before, after)
	}
}

func operatorNeverLaunchedOwner(t *testing.T, issue int, manifestState, observedState string, persist func(runtimeOwnerState) error) (*stateOwner, agentruntime.Manifest) {
	t.Helper()
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, issue, 1, manifestState)
	manifest.Version, manifest.LaunchToken = agentruntime.ManifestVersion2, strings.Repeat("a", 32)
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, observedState, false)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	return owner, manifest
}

func TestV2DashboardConstructionRejectsWrongOwnerBinding(t *testing.T) {
	owner, _ := operatorTestOwner(t, 323, "completed", true)
	service := operatorTestMutationService(t, owner)
	if _, err := newProjectDashboardServerV2(t.Context(), filepath.Join(owner.stateRoot, "wrong"), "o/r", nil, "tmux", service, 1, false, ""); err == nil {
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

func TestV2DashboardProjectionUsesConfiguredCapacity(t *testing.T) {
	owner, _ := operatorTestOwner(t, 324, "completed", true)
	service := operatorTestMutationService(t, owner)
	project, err := newProjectDashboardServerV2(t.Context(), owner.stateRoot, "o/r", nil, "tmux", service, 1, false, "")
	if err != nil {
		t.Fatal(err)
	}
	got, err := project.readStatus()
	if err != nil {
		t.Fatal(err)
	}
	want, err := projectOwnerStatus(mustOwnerSnapshot(t, owner), 1, got.UpdatedAt)
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("dashboard=%#v owner=%#v err=%v", got, want, err)
	}
}

func TestOperatorCleanupExecutesExactArchiveAbandonAndRemovePolicies(t *testing.T) {
	for _, action := range []string{"archive", "abandon", "remove"} {
		t.Run(action, func(t *testing.T) {
			owner, manifest := operatorTestOwner(t, 330, "completed", false)
			if manifest.ReviewSession != "" || manifest.ReviewSnapshot != "" || manifest.ReviewTarget != "" {
				t.Fatal("cleanup fixture must have no reviewer resources")
			}
			snapshotRoot := productionSnapshotRoot(owner.stateRoot)
			if err := os.MkdirAll(snapshotRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			expectedSnapshot, _ := reviewIdentity(operatorEffectAttempt(manifest), snapshotRoot)
			resultRoot := expectedSnapshot + ".result-0123456789abcdef"
			implementation := &operatorBoundaryRecorder{}
			reviewer := &operatorBoundaryRecorder{}
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
			if !slices.Equal(implementation.operations(), []string{"validate-" + wantOperation, wantOperation}) || len(reviewer.operations()) != 0 {
				t.Fatalf("implementation=%v reviewer=%v", implementation.operations(), reviewer.operations())
			}
			if len(reviewer.directories()) != 0 {
				t.Fatalf("cleanup without reviewer resources called reviewer boundary: %v", reviewer.directories())
			}
		})
	}
}

func TestMachineStatusAdmissionRecollectsAfterDestructiveAction(t *testing.T) {
	for _, action := range []string{"archive", "abandon", "dismiss", "remove"} {
		t.Run(action, func(t *testing.T) {
			var owner *stateOwner
			var manifest agentruntime.Manifest
			if action == "dismiss" {
				owner, manifest = operatorTestOwner(t, 410, "completed", true)
			} else {
				owner, manifest = operatorCleanupRestartOwner(t, 410, action)
			}
			service := operatorTestMutationService(t, owner)
			cleanup := service.cleanup
			cleanup.implementation = &operatorBoundaryRecorder{}
			service.cleanup = cleanup
			service.effects.executor.Cleanup, service.effects.executor.VerifyCleanup = cleanup.execute, cleanup.verify
			service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
			snapshot := mustOwnerSnapshot(t, owner)
			issueKey := ownerIssueKey(manifest.Repository, manifest.Issue)
			attemptKey := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
			var err error
			snapshot, err = owner.admitMachineStatus(t.Context(), admitMachineStatusCommand{
				Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt,
				ExpectedIssueGeneration: snapshot.State.IssueGenerations[issueKey], ExpectedAttemptGeneration: snapshot.State.AttemptGenerations[attemptKey],
				ExpectedStatusSequence: snapshot.State.MachineStatuses[issueKey].Sequence, ExpectedCausalityToken: ownerAttemptCausalityToken(snapshot.State, manifest.Repository, manifest.Issue, manifest.Attempt),
				Source: "orchestrator", SourceID: "stale-after-" + action, Status: "needs-attention", Reason: "monitoring: stale plan",
			})
			if err != nil {
				t.Fatal(err)
			}
			plans, err := planMachineStatusUpdates(snapshot, service.collector.Config)
			if err != nil || len(plans) != 1 {
				t.Fatalf("plans=%#v err=%v", plans, err)
			}
			result := service.performSynchronously(t.Context(), operatorRequest("stale-status-"+action, action, manifest, action != "dismiss"))
			if !result.OK || result.Status != http.StatusOK {
				t.Fatalf("destructive result=%#v", result)
			}
			githubCalls := 0
			api := internalgithub.API{HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(*http.Request) (*http.Response, error) {
				githubCalls++
				return nil, errors.New("stale admission reached GitHub")
			})}}
			production := productionReconciliation{effects: service.effects}
			changed, phaseErr := production.runMachineStatusPlan(t.Context(), api, plans[0])
			if changed || !errors.Is(phaseErr, errStaleStateResult) || githubCalls != 0 {
				t.Fatalf("stale admission changed=%t calls=%d err=%v", changed, githubCalls, phaseErr)
			}
			if err := reconciliationCycleDisposition(fmt.Errorf("run machine-status phase: %w", phaseErr), nil); !errors.Is(err, errReconciliationRecollect) {
				t.Fatalf("stale admission disposition=%v", err)
			}
			current := mustOwnerSnapshot(t, owner).State
			if status := current.MachineStatuses[issueKey]; status.Status != "clear" || status.Sequence <= plans[0].Request.GitHubIssueUpdate.StatusSequence {
				t.Fatalf("stale plan replaced destructive status: %#v", status)
			}
			freshPlans, err := planMachineStatusUpdates(stateOwnerSnapshot{State: current}, service.collector.Config)
			if err != nil || len(freshPlans) != 1 || freshPlans[0].Request.GitHubIssueUpdate.Status != "clear" || freshPlans[0].Request.GitHubIssueUpdate.StatusSequence <= plans[0].Request.GitHubIssueUpdate.StatusSequence {
				t.Fatalf("fresh plans=%#v err=%v", freshPlans, err)
			}
			for _, effect := range current.Effects {
				if effect.State == "pending" && effect.Reconciliation != nil && effect.Reconciliation.ExecutionDigest == plans[0].Request.ExecutionDigest {
					t.Fatalf("stale machine-status effect was persisted: %#v", effect)
				}
			}
		})
	}
}

func TestReconciliationCycleErrorDisposition(t *testing.T) {
	conflict := fmt.Errorf("admission: %w", errStateConflict)
	arbitrary := errors.New("GitHub unavailable")
	canceled := context.Canceled
	persistence := errors.New("persist cycle outcome")
	for _, test := range []struct {
		name                 string
		phaseErr, outcomeErr error
		want                 error
	}{
		{name: "success"},
		{name: "machine-status stale", phaseErr: fmt.Errorf("run machine-status phase: %w", errStaleStateResult), want: errReconciliationRecollect},
		{name: "issue-scoped stale", phaseErr: fmt.Errorf("run issue-update phase: %w", errStaleStateResult), want: errReconciliationRecollect},
		{name: "attempt-scoped stale", phaseErr: fmt.Errorf("run monitor phase: %w", errStaleStateResult), want: errReconciliationRecollect},
		{name: "recollect", phaseErr: errReconciliationRecollect, want: errReconciliationRecollect},
		{name: "conflict", phaseErr: conflict, want: conflict},
		{name: "arbitrary", phaseErr: arbitrary, want: arbitrary},
		{name: "canceled", phaseErr: canceled, want: canceled},
		{name: "persistence beats recollect", phaseErr: errReconciliationRecollect, outcomeErr: persistence, want: persistence},
		{name: "conflict beats stale", phaseErr: errStaleStateResult, outcomeErr: conflict, want: conflict},
		{name: "cancellation beats stale", phaseErr: errStaleStateResult, outcomeErr: canceled, want: canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := reconciliationCycleDisposition(test.phaseErr, test.outcomeErr); got != test.want {
				t.Fatalf("disposition=%v want=%v", got, test.want)
			}
		})
	}
}

func TestOperatorCleanupVerifyRequiresExactReviewerProofAndResourcesGone(t *testing.T) {
	for _, test := range []struct {
		name         string
		boundary     string
		nondead      bool
		mutate       func(*testing.T, *stateOwner, *agentruntime.EffectRequest)
		wantComplete bool
	}{
		{name: "authorized", boundary: "absent", wantComplete: true},
		{name: "matching profile without death proof", boundary: "absent", nondead: true},
		{name: "wrong RunID", boundary: "absent", mutate: func(_ *testing.T, _ *stateOwner, request *agentruntime.EffectRequest) {
			request.Manifest.ReviewRunID = digestText("wrong cleanup run")
		}},
		{name: "live exact session", boundary: "live"},
		{name: "ambiguous tmux failure", boundary: "ambiguous"},
		{name: "unknown reserved residue", boundary: "absent", mutate: func(t *testing.T, owner *stateOwner, request *agentruntime.EffectRequest) {
			base, _ := reviewIdentity(request.Attempt, productionSnapshotRoot(owner.stateRoot))
			if err := os.MkdirAll(base+"-unknown-run", 0o700); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			owner, request := operatorDismissCleanupWithRanProof(t, true)
			if test.nondead {
				owner, request = operatorDismissCleanupWithLiveProof(t)
			}
			if test.mutate != nil {
				test.mutate(t, owner, &request)
			}
			runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: &barrierEffectRunner{}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
			executor := operatorCleanupExecutor{stateRoot: owner.stateRoot, owner: owner, implementation: absentSessionBoundary{}, reviewer: cleanupVerifyBoundary{mode: test.boundary}, runtime: runtimeState}
			complete, err := executor.verify(t.Context(), request)
			if err != nil || complete != test.wantComplete {
				t.Fatalf("complete=%t want=%t err=%v", complete, test.wantComplete, err)
			}
		})
	}

}

func operatorDismissCleanupWithLiveProof(t *testing.T) (*stateOwner, agentruntime.EffectRequest) {
	t.Helper()
	review := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	review.Reviewer.Mode = agentruntime.ReviewModeImplementation
	_, owner, snapshot := reconciliationEffectPersistentOwner(t, review)
	review = bindEffectObservation(snapshot, review)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, review), Request: review})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 4321}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(review.Reviewer.Snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	input := reconciliationEffectObservationInput(review, "title")
	input.Issues[0].Closed, input.Issues[0].Active = true, false
	input.Issues[0].ActiveAttempt, input.Issues[0].TerminalAttempts = nil, nil
	input.Issues[0].DispatchAuthorized = false
	applyReconciliationInput(t, owner, input)
	current := mustOwnerSnapshot(t, owner)
	manifest := current.State.Attempts[ownerAttemptKey(review.Repository, review.Issue, review.Attempt)].Manifest
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	request := operatorRequest("dismiss-live-verifier", "dismiss", manifest, false)
	command, work, err := service.prepareAdmission(t.Context(), current, request)
	if err != nil {
		t.Fatal(err)
	}
	_, cleanup, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	bindOperatorWorkIdentity(&work, *cleanup)
	return owner, *work.runtime
}

func TestDismissCleanupStartedWithRanReviewerResumesAcrossRestart(t *testing.T) {
	owner, request := operatorDismissCleanupWithRanProof(t, true)
	for _, directory := range []string{request.Manifest.Worktree, filepath.Dir(request.Manifest.LogPath)} {
		if err := os.MkdirAll(directory, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(request.Manifest.LogPath, []byte("retained implementation log\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(request.Manifest.ReviewSnapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.startOperatorCleanup(t.Context(), startOperatorCleanupCommand{Identity: ownerEffectIdentity(request.Identity)}); err != nil {
		t.Fatal(err)
	}
	before := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(before, "dismiss-verifier")
	if !ok || receipt.State != "pending" || receipt.Phase != operatorPhaseCleanupStarted || !before.ReviewerProofs[reviewerProofKey(request.Manifest.Repository, request.Manifest.Issue, request.Manifest.Attempt, request.Manifest.ReviewMode, request.Manifest.ReviewTarget)].DeadProved {
		t.Fatalf("persisted cleanup-started fixture is incomplete: receipt=%#v proofs=%#v", receipt, before.ReviewerProofs)
	}
	for _, path := range []string{request.Manifest.Worktree, request.Manifest.LogPath, request.Manifest.ReviewSnapshot} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("cleanup-started fixture path %s: %v", path, err)
		}
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, before, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, restarted)
	if err := service.resumeReceipt(t.Context(), receipt.Request.RequestID); err != nil {
		t.Fatal(err)
	}
	settled := mustOwnerSnapshot(t, restarted).State
	receipt, ok = operatorReceiptByID(settled, receipt.Request.RequestID)
	tombstone := settled.Tombstones[ownerAttemptKey(request.Manifest.Repository, request.Manifest.Issue, request.Manifest.Attempt)]
	if !ok || receipt.State != "completed" || receipt.Phase != operatorPhaseCompleted || tombstone.CleanupPhase != "completed" || tombstone.ReviewerLeaseID != "" {
		t.Fatalf("restarted Dismiss cleanup did not settle: receipt=%#v tombstone=%#v", receipt, tombstone)
	}
	for _, proof := range settled.ReviewerProofs {
		if proof.Repository == request.Manifest.Repository && proof.Issue == request.Manifest.Issue && proof.Attempt == request.Manifest.Attempt {
			t.Fatalf("restarted Dismiss retained reviewer proof: %#v", proof)
		}
	}
	if _, err := os.Lstat(request.Manifest.ReviewSnapshot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restarted Dismiss retained reviewer snapshot: %v", err)
	}
	for _, path := range []string{request.Manifest.Worktree, request.Manifest.LogPath} {
		if _, err := os.Lstat(path); err != nil {
			t.Fatalf("restarted Dismiss removed implementation artifact %s: %v", path, err)
		}
	}
	if err := restarted.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	second, err := startTestStateOwner(t, owner.stateRoot, settled, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.close(context.Background()) })
	final := mustOwnerSnapshot(t, second).State
	finalReceipt, _ := operatorReceiptByID(final, receipt.Request.RequestID)
	if finalReceipt.State != "completed" || final.Tombstones[ownerAttemptKey(request.Manifest.Repository, request.Manifest.Issue, request.Manifest.Attempt)].CleanupPhase != "completed" || len(final.ReviewerProofs) != 0 {
		t.Fatalf("second restart changed settled Dismiss cleanup: receipt=%#v tombstones=%#v proofs=%#v", finalReceipt, final.Tombstones, final.ReviewerProofs)
	}
}

func operatorDismissCleanupWithRanProof(t *testing.T, dead bool) (*stateOwner, agentruntime.EffectRequest) {
	t.Helper()
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 399, 1, "completed")
	manifest.ReviewState, manifest.ReviewMode = "clean", agentruntime.ReviewModeImplementation
	manifest.ReviewBase, manifest.ReviewHead = manifest.BaseSHA, strings.Repeat("b", 40)
	manifest.ReviewTarget, manifest.ReviewRunID = manifest.ReviewBase+".."+manifest.ReviewHead, digestText("operator cleanup verifier run")
	manifest.ReviewSnapshot, manifest.ReviewSession = reviewRunIdentity(operatorEffectAttempt(manifest), productionSnapshotRoot(root), manifest.ReviewTarget, manifest.ReviewRunID)
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "completed", true)
	state.Epoch, state.Revision = 1, 1
	proof := reviewerProcessProof{
		Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt,
		Mode: manifest.ReviewMode, Target: manifest.ReviewTarget, RunID: manifest.ReviewRunID,
		EffectID: digestText("operator cleanup proof")[:32], IssueGeneration: 1, AttemptGeneration: 1,
		GroupPID: 4321, DeadProved: dead, ProfileDigest: activeWorkerProfileDigest(state), ConfinementVersion: reviewerConfinementVersion,
	}
	state.ReviewerProofs[reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, proof.Mode, proof.Target)] = proof
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	request := operatorRequest("dismiss-verifier", "dismiss", manifest, false)
	command, work, err := service.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), request)
	if err != nil {
		t.Fatal(err)
	}
	_, effect, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	bindOperatorWorkIdentity(&work, *effect)
	return owner, *work.runtime
}

type cleanupVerifyBoundary struct{ mode string }

func (b cleanupVerifyBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || !slices.Contains(command.Args, "has-session") {
		return agentruntime.Result{}, errors.New("unexpected cleanup verifier boundary operation")
	}
	session := strings.TrimPrefix(command.Args[len(command.Args)-1], "=")
	switch b.mode {
	case "absent":
		return agentruntime.Result{Exited: true, Code: 1, Output: "can't find session: " + session}, errors.New("session absent")
	case "live":
		return agentruntime.Result{}, nil
	case "ambiguous":
		return agentruntime.Result{Exited: true, Code: 1, Output: "permission denied"}, errors.New("tmux denied")
	default:
		return agentruntime.Result{}, errors.New("unknown cleanup verifier boundary mode")
	}
}

func TestOperatorCleanupResumesAfterRestartFromPendingAndStartedPhases(t *testing.T) {
	for actionIndex, action := range []string{"archive", "abandon", "remove"} {
		for _, phase := range []string{operatorPhaseCleanupPending, operatorPhaseCleanupStarted} {
			t.Run(action+"-"+phase, func(t *testing.T) {
				owner, manifest := operatorCleanupRestartOwner(t, 331+actionIndex, action)
				if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
					t.Fatal(err)
				}
				if err := os.MkdirAll(productionSnapshotRoot(owner.stateRoot), 0o700); err != nil {
					t.Fatal(err)
				}
				lifecycle, stop := context.WithCancel(t.Context())
				blocking := &operatorCleanupBoundary{path: manifest.Worktree, entered: make(chan struct{}), exited: make(chan struct{})}
				service := operatorServiceWithCleanup(t, owner, lifecycle, blocking)
				request := operatorRequest(action+"-restart-"+phase, action, manifest, true)
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
				tombstone := final.Tombstones[ownerAttemptKey("o/r", manifest.Issue, 1)]
				if receipt.State != "completed" || tombstone.CleanupPhase != "completed" || final.Effects[tombstone.EffectID].State != "completed" {
					t.Fatalf("receipt=%#v tombstone=%#v effects=%#v", receipt, tombstone, final.Effects)
				}
				if _, err := os.Lstat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("worktree remains after recovery: %v", err)
				}
				for _, path := range []string{manifest.LogPath, filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")} {
					_, err := os.Lstat(path)
					if action == "archive" && err != nil {
						t.Fatalf("archive removed compatibility resource %s: %v", path, err)
					}
					if action != "archive" && !errors.Is(err, os.ErrNotExist) {
						t.Fatalf("%s retained compatibility resource %s: %v", action, path, err)
					}
				}
			})
		}
	}
}

func operatorCleanupRestartOwner(t *testing.T, issue int, action string) (*stateOwner, agentruntime.Manifest) {
	t.Helper()
	switch action {
	case "archive":
		return operatorTestOwner(t, issue, "completed", false)
	case "abandon":
		return operatorTestOwner(t, issue, "orphaned", false)
	case "remove":
		root := resolvedTempDir(t)
		manifest := ownerTestManifest(t, root, issue, 1, "failed")
		newer := ownerTestManifest(t, root, issue, 2, "running")
		state := newRuntimeOwnerState("o/r")
		issueKey := ownerIssueKey("o/r", issue)
		firstKey, newerKey := ownerAttemptKey("o/r", issue, 1), ownerAttemptKey("o/r", issue, 2)
		state.IssueGenerations[issueKey] = 1
		state.AttemptGenerations[firstKey], state.AttemptGenerations[newerKey] = 1, 1
		state.Attempts[firstKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
		state.Attempts[newerKey] = runtimeAttemptRecord{Generation: 1, Manifest: newer}
		addOperatorObservation(&state, newer, "active", false)
		observation := state.Observations[issueKey]
		failed, err := reduceAttemptFact("o/r", internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: issue, Attempt: 1, BaseSHA: manifest.BaseSHA, State: "failed", Checks: []string{}})
		if err != nil {
			t.Fatal(err)
		}
		observation.Fact.TerminalAttempts = []reconciliationAttemptFact{failed}
		observation.Attempts[firstKey] = reconciliationAttemptObservation{Present: true, Generation: 1, OwnerGeneration: 1, SourceIssueGeneration: observation.Generation, ObservationEpoch: 1, LastCycleID: observation.LastCycleID, Fact: failed}
		state.Observations[issueKey] = observation
		state.Epoch, state.Revision = 1, 1
		owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = owner.close(context.Background()) })
		refreshOperatorObservation(t, owner)
		return owner, manifest
	default:
		t.Fatalf("unknown cleanup action %q", action)
		return nil, agentruntime.Manifest{}
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
	attachedRequest := operatorRequest("startup-cleanup-attached", "archive", manifest, true)
	replay, ok := admissionService.tombstoneReplayCommand(committed, attachedRequest)
	if !ok {
		t.Fatal("cleanup tombstone did not accept an attached receipt")
	}
	committed, _, err = owner.beginOperatorMutation(t.Context(), replay)
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
	for range 20 {
		if err := service.resumePending(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if blocking.calls.Load() != 1 {
		t.Fatalf("attached receipts scheduled %d workers for one durable effect", blocking.calls.Load())
	}
	started := mustOwnerSnapshot(t, restarted).State
	receipt, _ := operatorReceiptByID(started, request.RequestID)
	attached, _ := operatorReceiptByID(started, attachedRequest.RequestID)
	if receipt.Phase != operatorPhaseCleanupStarted || receipt.State != "pending" || attached.EffectID != receipt.EffectID || attached.Phase != receipt.Phase {
		t.Fatalf("receipt=%#v attached=%#v", receipt, attached)
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
	before := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(before.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	attempt := expandAttemptFact(before.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/issues/333" {
			t.Errorf("unexpected GitHub read: %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"number":333,"state":"open","body":"body"}`)
	}))
	defer github.Close()
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	beforeManifest, err := os.ReadFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{API: internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
	plan, material, err := service.preparePlanReview(t.Context(), mustOwnerSnapshot(t, owner), manifest)
	if err != nil || material.Issue.Body != "body" || digestText(material.Issue.Body) != plan.Request.BodyDigest {
		t.Fatalf("plan=%#v material=%#v err=%v", plan, material, err)
	}
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

func TestV2PlanReviewSupersedesOnlyCurrentPendingMonitor(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 334, "active", false)
	initial := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	attempt := expandAttemptFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/issues/334" {
			t.Errorf("unexpected GitHub read: %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"number":334,"state":"open","body":"body"}`)
	}))
	defer github.Close()
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	before := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	identity := stateResultIdentity{Epoch: before.Epoch, SourceRevision: before.Revision, IssueGeneration: before.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)], AttemptGeneration: before.AttemptGenerations[key]}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{API: internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
	request := operatorRequest("review-supersedes-monitor", "review-plan", manifest, false)
	snapshot := mustOwnerSnapshot(t, owner)
	command, _, err := service.prepareAdmission(t.Context(), snapshot, request)
	if err != nil {
		t.Fatal(err)
	}
	// The dashboard completed its slow preflight before Monitor reserved the
	// attempt. Monitor changes only global revision, not the bound entity.
	_, monitor, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectMonitor, Manifest: before.Attempts[key].Manifest, RequestDigest: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	committed, review, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if review == nil || review.Reconciliation == nil || review.Reconciliation.Reviewer == nil || review.Reconciliation.Reviewer.Mode != agentruntime.ReviewModePlan || len(committed.State.ControlReceipts) != 1 || committed.State.ControlReceipts[0].EffectID != review.ID {
		t.Fatalf("review effect and receipt not atomic: review=%#v receipts=%#v", review, committed.State.ControlReceipts)
	}
	if _, exists := committed.State.Effects[monitor.ID]; exists {
		t.Fatal("superseded monitor intent survived")
	}
	if err := owner.authorizeRuntimeEffect(t.Context(), authorizeRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*monitor)), Action: agentruntime.EffectMonitor}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("superseded monitor can still execute: %v", err)
	}
}

func TestV2PlanReviewAdmitsAndResumesAfterMonitorOnlyTimestampChange(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 335, "active", false)
	initial := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	attempt := expandAttemptFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	var enteredOnce sync.Once
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/issues/335" {
			t.Errorf("unexpected GitHub read: %s", r.URL.Path)
		}
		enteredOnce.Do(func() { close(entered) })
		<-release
		_, _ = io.WriteString(w, `{"number":335,"state":"open","body":"body"}`)
	}))
	defer github.Close()
	defer once.Do(func() { close(release) })
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{API: internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
	result := make(chan controlResult, 1)
	go func() {
		result <- service.perform(t.Context(), operatorRequest("plan-review-monitor-timestamp", "review-plan", manifest, false))
	}()
	<-entered // The action has captured its owner snapshot and is waiting on GitHub.
	before := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	current := before.Attempts[key].Manifest
	identity := stateResultIdentity{Epoch: before.Epoch, SourceRevision: before.Revision, IssueGeneration: before.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)], AttemptGeneration: before.AttemptGenerations[key]}
	_, monitor, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectMonitor, Manifest: current, RequestDigest: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	updated := cloneManifest(current)
	updated.UpdatedAt = updated.UpdatedAt.Add(time.Second)
	if _, err := owner.finishRuntimeEffect(t.Context(), finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*monitor)), Action: agentruntime.EffectMonitor, Manifest: updated}); err != nil {
		t.Fatal(err)
	}
	after := mustOwnerSnapshot(t, owner).State
	if !reflect.DeepEqual(after.Attempts[key].Manifest, updated) {
		t.Fatalf("monitor changed more than UpdatedAt: before=%#v after=%#v", current, after.Attempts[key].Manifest)
	}
	once.Do(func() { close(release) })
	if got := <-result; got.Status != http.StatusAccepted || !got.OK {
		t.Fatalf("monitor-only timestamp change rejected review: %#v", got)
	}
	committed := mustOwnerSnapshot(t, owner).State
	if len(committed.ControlReceipts) != 1 || committed.ControlReceipts[0].Phase != operatorPhaseReviewPending || committed.Effects[committed.ControlReceipts[0].EffectID].Reconciliation == nil {
		t.Fatalf("review intent not durably admitted after monitor: %#v", committed)
	}
	effect := committed.Effects[committed.ControlReceipts[0].EffectID]
	if err := owner.authorizeReconciliationEffect(t.Context(), authorizeReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(effect), Action: reconciliationReviewer}); err != nil {
		t.Fatalf("monitor timestamp invalidated the admitted review effect: %v", err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(effect)}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: ownerReconciliationEffectIdentity(effect), GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	committed = mustOwnerSnapshot(t, owner).State
	launchPath, terminalPath := reviewerLifecyclePaths(effect.Reconciliation.Reviewer.Snapshot, effect.Reconciliation.Reviewer.Target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	launch := reviewerIdentity(ownerReconciliationEffectIdentity(effect))
	launch.GateProtocol = effect.ReviewerGateProtocol
	launch.SessionRequested = true
	launch.ChildPID = 99999999
	if err := writeReviewerRecord(launchPath, launch); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: launch}); err != nil {
		t.Fatal(err)
	}
	// Crash before the reviewer completion marker, then resume the immutable
	// admitted effect against the newer monitor timestamp.
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, committed); err != nil {
		t.Fatal(err)
	}
	persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	restartRuntime := &agentruntime.Runtime{Root: restarted.attemptRoot, StateRoot: restarted.stateRoot, Runner: operatorOwnedRunner{manifest: updated}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	restartEffects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, executor: agentruntime.EffectExecutor{Runtime: restartRuntime}, active: map[string]*activeRuntimeEffect{}}
	boundary := &completedPlanReviewBoundary{pane: reviewerPaneTestOutput("1|0|||", effect.Reconciliation.Reviewer.Session, "$9", os.Getpid(), "agent-symphony review-pane tmux "+launchPath+" "+terminalPath+" "+reviewerSignal(launch)+" "+launch.RequestDigest)}
	restartService := &operatorMutationService{lifecycle: t.Context(), owner: restarted, effects: restartEffects, collector: service.collector, reviewer: boundary, reviewSource: "source", reviewCommand: []string{"review"}}
	restartService.collect = func(_ context.Context, _ stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		if issue != manifest.Issue {
			return reconciliationV2Batch{}, fmt.Errorf("unexpected issue %d", issue)
		}
		return reconciliationV2Batch{Input: input}, nil
	}
	if err := restartService.resumeReceipt(t.Context(), committed.ControlReceipts[0].Request.RequestID); err != nil {
		t.Fatalf("restart did not resume admitted review: %v", err)
	}
	final := mustOwnerSnapshot(t, restarted).State
	if final.ControlReceipts[0].State != "completed" || final.Effects[effect.ID].State != "completed" || final.Attempts[key].Manifest.ReviewState != "clean" {
		t.Fatalf("restart lost completed review: receipt=%#v effect=%#v manifest=%#v", final.ControlReceipts[0], final.Effects[effect.ID], final.Attempts[key].Manifest)
	}
	if boundary.panes.Load() == 0 || boundary.results.Load() != 1 {
		t.Fatalf("restart did not verify dead pane and exact artifact: panes=%d results=%d", boundary.panes.Load(), boundary.results.Load())
	}
	marker := filepath.Join(owner.stateRoot, "reconciliation-effects", effect.ID+".done")
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review marker %s was not reclaimed", marker)
	}
	durable, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil || durable.Effects[effect.ID].State != "completed" || durable.ControlReceipts[0].State != "completed" {
		t.Fatalf("restart completion was not persisted: receipt=%#v effect=%#v err=%v", durable.ControlReceipts, durable.Effects[effect.ID], err)
	}
}

func TestV2PlanReviewMarkerReplayRejectsChangedGitHubBodyAfterRestart(t *testing.T) {
	for _, entry := range []string{"startup resume", "worker failure classification"} {
		t.Run(entry, func(t *testing.T) {
			owner, manifest := operatorTestOwner(t, 336, "active", false)
			initial := mustOwnerSnapshot(t, owner).State
			issue := expandIssueFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
			issue.Body = "body"
			attempt := expandAttemptFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
			input := repositoryInput(true, issue)
			input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
			applyReconciliationInput(t, owner, input)
			if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
				t.Fatal(err)
			}
			var body atomic.Value
			body.Store("body")
			github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/repos/o/r/issues/336" {
					t.Errorf("unexpected GitHub read: %s", r.URL.Path)
				}
				_, _ = fmt.Fprintf(w, `{"number":336,"state":"open","body":%q}`, body.Load().(string))
			}))
			defer github.Close()
			runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
			effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
			service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{API: internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
			request := operatorRequest("review-marker-body-drift", "review-plan", manifest, false)
			command, _, err := service.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), request)
			if err != nil {
				t.Fatal(err)
			}
			_, effect, err := owner.beginOperatorMutation(t.Context(), command)
			if err != nil {
				t.Fatal(err)
			}
			identity := ownerReconciliationEffectIdentity(*effect)
			if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
				t.Fatal(err)
			}
			reviewer := effect.Reconciliation.Reviewer
			result := reconciliationEffectResult{Action: reconciliationReviewer, Reviewer: &reviewerEffectResult{Phase: reviewer.Phase, Status: "clean", Mode: reviewer.Mode, Target: reviewer.Target, RunID: reviewer.RunID, BaseSHA: reviewer.BaseSHA, HeadSHA: reviewer.HeadSHA, Snapshot: reviewer.Snapshot, Session: reviewer.Session}}
			if err := writeReconciliationEffectMarker(owner.stateRoot, identity, *effect.Reconciliation, result); err != nil {
				t.Fatal(err)
			}
			_, terminalPath := reviewerLifecyclePaths(reviewer.Snapshot, reviewer.Target)
			if err := os.MkdirAll(filepath.Dir(terminalPath), 0o700); err != nil {
				t.Fatal(err)
			}
			if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: reviewerIdentity(identity)}); err != nil {
				t.Fatal(err)
			}
			crashState := mustOwnerSnapshot(t, owner).State
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, crashState); err != nil {
				t.Fatal(err)
			}
			persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
			if err != nil {
				t.Fatal(err)
			}
			restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(state runtimeOwnerState) error {
				return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.close(context.Background()) })
			body.Store("changed while daemon was down")
			changed := issue
			changed.Body = body.Load().(string)
			changedInput := repositoryInput(true, changed)
			changedInput.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
			restartRuntime := &agentruntime.Runtime{Root: restarted.attemptRoot, StateRoot: restarted.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
			restartEffects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, executor: agentruntime.EffectExecutor{Runtime: restartRuntime}, active: map[string]*activeRuntimeEffect{}}
			restartService := &operatorMutationService{lifecycle: t.Context(), owner: restarted, effects: restartEffects, collector: service.collector, reviewer: &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||\n"}}, reviewSource: "source", reviewCommand: []string{"review"}}
			restartService.collect = func(context.Context, stateOwnerSnapshot, int) (reconciliationV2Batch, error) {
				return reconciliationV2Batch{Input: changedInput}, nil
			}
			if entry == "startup resume" {
				if err := restartService.resumePending(t.Context()); err != nil {
					t.Fatal(err)
				}
				if err := restartService.shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			} else {
				restartService.classifyWorkerFailure(request.RequestID, errors.New("worker stopped after writing marker"))
			}
			final := mustOwnerSnapshot(t, restarted).State
			receipt := final.ControlReceipts[0]
			if receipt.State != "pending" || receipt.Result != nil || !final.Effects[effect.ID].ReviewerRevoked || final.Effects[effect.ID].ReconciliationResult != nil || final.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Manifest.ReviewState == "clean" {
				t.Fatalf("stale marker committed review: receipt=%#v effect=%#v", receipt, final.Effects[effect.ID])
			}
		})
	}
}

func TestCancelPreservesExactReviewerStopBindingAcrossRestart(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 337, "active", false)
	initial := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	attempt := expandAttemptFact(initial.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"number":337,"state":"open","body":"body"}`)
	}))
	defer github.Close()
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{API: internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
	command, _, err := service.prepareAdmission(t.Context(), mustOwnerSnapshot(t, owner), operatorRequest("cancel-review-plan", "review-plan", manifest, false))
	if err != nil {
		t.Fatal(err)
	}
	_, reviewer, err := owner.beginOperatorMutation(t.Context(), command)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewer)}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: ownerReconciliationEffectIdentity(*reviewer), GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(reviewer.Reconciliation.Reviewer.Snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	beforeCancel := mustOwnerSnapshot(t, owner)
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if got := beforeCancel.State.Attempts[key].Manifest; got.ReviewState != "" || got.ReviewSession != "" {
		t.Fatalf("in-flight review leaked into manifest: %#v", got)
	}
	cancelRequest := operatorRequest("cancel-after-review-launch", "cancel", manifest, false)
	cancelCommand := operatorCommand(beforeCancel, cancelRequest, beforeCancel.State.Attempts[key].Manifest)
	preparedStop, err := service.prepareStop(cancelCommand.Manifest, "operator cancelled attempt")
	if err != nil {
		t.Fatal(err)
	}
	cancelCommand.Runtime = &beginRuntimeEffectCommand{Identity: cancelCommand.Identity, Action: agentruntime.EffectStop, Manifest: cancelCommand.Manifest, Reason: preparedStop.Reason, RequestDigest: preparedStop.Identity.RequestDigest}
	committed, stop, err := owner.beginOperatorMutation(t.Context(), cancelCommand)
	if err != nil {
		t.Fatalf("Cancel overlapped launched review: %v", err)
	}
	if stop == nil || stop.SupersededReviewerID != reviewer.ID || stop.SupersededReviewerRequestDigest != reviewer.RequestDigest || !stop.SupersededReviewerGateProtocol || !stop.SupersededReviewerSessionRequested || stop.SupersededReviewerGroupPID != 99999999 || committed.State.Effects[reviewer.ID].State == "pending" {
		t.Fatalf("Cancel lost reviewer stop binding: stop=%#v reviewer=%#v", stop, committed.State.Effects[reviewer.ID])
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, committed.State); err != nil {
		t.Fatal(err)
	}
	persisted, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	if stored := persisted.Effects[stop.ID]; stored.SupersededReviewerID != reviewer.ID || stored.SupersededReviewerRequestDigest != reviewer.RequestDigest || !stored.SupersededReviewerSessionRequested || stored.SupersededReviewerGroupPID != 99999999 {
		t.Fatalf("restart lost exact reviewer stop binding: %#v", stored)
	}
	restarted, err := startTestStateOwner(t, owner.stateRoot, persisted, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||\n"}}
	restartRuntime := &agentruntime.Runtime{Root: restarted.attemptRoot, StateRoot: restarted.stateRoot, Runner: &barrierEffectRunner{}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	restartEffects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, executor: agentruntime.EffectExecutor{Runtime: restartRuntime}, active: map[string]*activeRuntimeEffect{}}
	restartService := &operatorMutationService{lifecycle: t.Context(), owner: restarted, effects: restartEffects, collector: service.collector, reviewer: boundary, active: map[string]bool{}, released: map[string]chan struct{}{}}
	if result := restartService.performSynchronously(t.Context(), cancelRequest); !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("restart did not expose physical-pending Cancel: %#v", result)
	}
	session, err := agentruntime.ReviewRunSessionName(manifest.Repository, manifest.Issue, manifest.Attempt, stop.SupersededReviewerTarget, stop.SupersededReviewerRunID)
	if err != nil || len(boundary.killed) != 0 {
		t.Fatalf("restart touched an absent reviewer session: session=%s killed=%v err=%v", session, boundary.killed, err)
	}
	current := mustOwnerSnapshot(t, restarted).State
	currentReceipt, ok := operatorReceiptByID(current, cancelRequest.RequestID)
	_, proofRetained := current.ReviewerProofs[reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, stop.SupersededReviewerMode, stop.SupersededReviewerTarget)]
	if !ok || currentReceipt.State != "pending" || !current.Effects[stop.ID].ReviewerStopped || !proofRetained || !validDigest(current.Effects[stop.ID].ReviewerCleanupDigest) || current.Attempts[key].StopEffectID != stop.ID {
		t.Fatalf("restart did not retain the exact reviewer proof through pending runtime Stop: receipt=%#v effect=%#v attempt=%#v proof_retained=%t", currentReceipt, current.Effects[stop.ID], current.Attempts[key], proofRetained)
	}
	if _, err := os.Lstat(reviewer.Reconciliation.Reviewer.Snapshot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart retained the retired reviewer snapshot: %v", err)
	}
	cancelled := current.Attempts[key].Manifest
	cancelled.State, cancelled.Diagnostic = "cancelled", current.Effects[stop.ID].Reason
	if _, err := restarted.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(current.Effects[stop.ID])), Action: agentruntime.EffectStop, Manifest: cancelled}}); err != nil {
		t.Fatalf("finish exact Stop after restart: %v", err)
	}
	final := mustOwnerSnapshot(t, restarted).State
	if final.Effects[stop.ID].State != "completed" || attemptHasReviewerProof(final, manifest.Repository, manifest.Issue, manifest.Attempt) || final.Attempts[key].StopEffectID != "" {
		t.Fatalf("Stop did not atomically retire proof and binding: effect=%#v attempt=%#v proofs=%#v", final.Effects[stop.ID], final.Attempts[key], final.ReviewerProofs)
	}
}

func TestStopRetainsExactReviewerCleanupCertificatesUntilAtomicFinish(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 399, 1, "running")
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	state := runtimeEffectInitialState(manifest)
	profile := activeWorkerProfileDigest(state)
	attempt := operatorEffectAttempt(manifest)
	snapshotRoot := productionSnapshotRoot(root)
	if err := os.MkdirAll(snapshotRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	targets := []struct {
		mode, target, runID, effectID string
	}{
		{agentruntime.ReviewModePlan, "o/r#399 plan sha256:" + digestText("plan"), digestText("stop proof one"), strings.Repeat("1", 32)},
		{agentruntime.ReviewModeImplementation, manifest.BaseSHA + ".." + strings.Repeat("b", 40), digestText("stop proof two"), strings.Repeat("2", 32)},
	}
	for index, target := range targets {
		snapshot, session := reviewRunIdentity(attempt, snapshotRoot, target.target, target.runID)
		if err := os.MkdirAll(snapshot, 0o700); err != nil {
			t.Fatal(err)
		}
		proof := reviewerProcessProof{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, Mode: target.mode, Target: target.target, RunID: target.runID, EffectID: target.effectID, IssueGeneration: 1, AttemptGeneration: 1, GroupPID: 90000000 + index, DeadProved: true, ProfileDigest: profile, ConfinementVersion: reviewerConfinementVersion}
		state.ReviewerProofs[reviewerProofKey(proof.Repository, proof.Issue, proof.Attempt, proof.Mode, proof.Target)] = proof
		if index == 0 {
			manifest.ReviewState, manifest.ReviewMode, manifest.ReviewTarget = "clean", target.mode, target.target
			manifest.ReviewRunID, manifest.ReviewBase, manifest.ReviewHead = target.runID, manifest.BaseSHA, manifest.BaseSHA
			manifest.ReviewSnapshot, manifest.ReviewSession = snapshot, session
		}
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	record := state.Attempts[key]
	record.Manifest = manifest
	state.Attempts[key] = record
	owner, err := startTestStateOwner(t, root, state, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, productionAttemptRoot(root), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	runtimeState := &agentruntime.Runtime{Root: productionAttemptRoot(root), StateRoot: root, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	request := beginRuntimeTestEffect(t, effects, owner, agentruntime.EffectStop, manifest, "operator cancelled attempt")
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, reviewer: absentSessionBoundary{}}
	if err := service.stopBoundReviewer(t.Context(), request, ""); err != nil {
		t.Fatalf("clean exact reviewer resources: %v", err)
	}
	cleaned := mustOwnerSnapshot(t, owner).State
	proofs := attemptReviewerProofs(cleaned, manifest.Repository, manifest.Issue, manifest.Attempt)
	effect := cleaned.Effects[request.Identity.EffectID]
	if len(proofs) != 2 || effect.ReviewerCleanupDigest != reviewerProofSetDigest(proofs) {
		t.Fatalf("cleanup did not retain and bind the complete proof set: effect=%#v proofs=%#v", effect, proofs)
	}
	for _, target := range targets {
		snapshot, _ := reviewRunIdentity(attempt, snapshotRoot, target.target, target.runID)
		if _, err := os.Lstat(snapshot); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("reviewer snapshot survived certified cleanup: %s: %v", snapshot, err)
		}
	}
	wrong := slices.Clone(proofs)
	wrong[0].RunID = digestText("wrong cleanup set")
	if _, err := owner.markReviewerResourcesCleaned(t.Context(), markReviewerResourcesCleanedCommand{Identity: ownerEffectIdentity(request.Identity), Proofs: wrong}); !errors.Is(err, errStateConflict) {
		t.Fatalf("mismatched proof set was accepted: %v", err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	persisted, err := readRuntimeOwnerState(root, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, persisted, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, productionAttemptRoot(root), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	restartEffects, err := newRuntimeEffectCoordinator(t.Context(), restarted, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	restartService := &operatorMutationService{lifecycle: t.Context(), owner: restarted, effects: restartEffects, reviewer: absentSessionBoundary{}}
	if err := restartService.stopBoundReviewer(t.Context(), request, ""); err != nil {
		t.Fatalf("restart did not replay exact reviewer cleanup: %v", err)
	}
	beforeFinish := mustOwnerSnapshot(t, restarted).State
	if got := len(attemptReviewerProofs(beforeFinish, manifest.Repository, manifest.Issue, manifest.Attempt)); got != 2 {
		t.Fatalf("restart discarded cleanup certificates before Stop finish: %d", got)
	}
	forged := cloneRuntimeOwnerState(beforeFinish)
	third := proofs[0]
	third.Mode, third.Target, third.RunID, third.EffectID = agentruntime.ReviewModeImplementation, manifest.BaseSHA+".."+strings.Repeat("c", 40), digestText("late proof"), strings.Repeat("3", 32)
	forged.ReviewerProofs[reviewerProofKey(third.Repository, third.Issue, third.Attempt, third.Mode, third.Target)] = third
	cancelled := beforeFinish.Attempts[key].Manifest
	cancelled.State, cancelled.Diagnostic = "cancelled", effect.Reason
	finish := finishRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: agentruntime.EffectStop, Manifest: cancelled}
	if err := applyFinishRuntimeEffect(restarted.attemptRoot, restarted.stateRoot, &forged, finish); !errors.Is(err, errStateConflict) {
		t.Fatalf("proof added after physical cleanup was accepted: %v", err)
	}
	if _, err := restarted.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finish}); err != nil {
		t.Fatalf("finish Stop with exact persisted proof set: %v", err)
	}
	if _, err := restarted.finishOperatorRuntimeEffect(t.Context(), finishOperatorRuntimeEffectCommand{Finish: finish}); err != nil {
		t.Fatalf("replay exact committed Stop finish: %v", err)
	}
	final := mustOwnerSnapshot(t, restarted).State
	finalManifest := final.Attempts[key].Manifest
	if attemptHasReviewerProof(final, manifest.Repository, manifest.Issue, manifest.Attempt) || finalManifest.ReviewRunID != "" || finalManifest.ReviewSnapshot != "" || finalManifest.ReviewSession != "" || !finalManifest.ReviewRunCleaned || final.Attempts[key].StopEffectID != "" {
		t.Fatalf("Stop did not atomically clear proofs and physical review identity: attempt=%#v proofs=%#v", final.Attempts[key], final.ReviewerProofs)
	}
	later := cloneRuntimeOwnerState(final)
	newProof := proofs[0]
	newProof.Mode, newProof.Target, newProof.RunID, newProof.EffectID, newProof.AttemptGeneration = agentruntime.ReviewModeImplementation, manifest.BaseSHA+".."+strings.Repeat("d", 40), digestText("later proof"), strings.Repeat("4", 32), final.AttemptGenerations[key]
	newKey := reviewerProofKey(newProof.Repository, newProof.Issue, newProof.Attempt, newProof.Mode, newProof.Target)
	later.ReviewerProofs[newKey] = newProof
	if err := applyFinishRuntimeEffect(restarted.attemptRoot, restarted.stateRoot, &later, finish); err != nil || !reflect.DeepEqual(later.ReviewerProofs[newKey], newProof) {
		t.Fatalf("committed Stop replay touched a later proof: err=%v proofs=%#v", err, later.ReviewerProofs)
	}
	if err := restarted.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	settled, err := readRuntimeOwnerState(root, manifest.Repository)
	if err != nil {
		t.Fatal(err)
	}
	second, err := startTestStateOwner(t, root, settled, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.close(context.Background()) })
	stable := mustOwnerSnapshot(t, second).State
	if stable.Effects[request.Identity.EffectID].State != "completed" || attemptHasReviewerProof(stable, manifest.Repository, manifest.Issue, manifest.Attempt) || stable.Attempts[key].Manifest.ReviewRunID != "" {
		t.Fatalf("second restart changed settled Stop cleanup: effect=%#v attempt=%#v proofs=%#v", stable.Effects[request.Identity.EffectID], stable.Attempts[key], stable.ReviewerProofs)
	}
}

type completedPlanReviewBoundary struct {
	panes   atomic.Int32
	results atomic.Int32
	pane    string
}

func (b *completedPlanReviewBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" && command.Name == "tmux" && slices.Contains(command.Args, "display-message") {
		b.panes.Add(1)
		if command.Args[len(command.Args)-1] == reviewerPaneIdentityFormat {
			return agentruntime.Result{Output: b.pane}, nil
		}
		return agentruntime.Result{Output: "1|0|||"}, nil
	}
	if operation == "review-result" {
		b.results.Add(1)
		return agentruntime.Result{Output: `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`}, nil
	}
	return agentruntime.Result{}, fmt.Errorf("unexpected review boundary %s %s %v", operation, command.Name, command.Args)
}

func TestV2PlanReviewRejectsBodyChangedDuringPreparationBeforeAdmission(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 334, "active", false)
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	body := ""
	github := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r/issues/334" {
			t.Errorf("unexpected GitHub read: %s", r.URL.Path)
		}
		close(entered)
		<-release
		_, _ = fmt.Fprintf(w, `{"number":334,"state":"open","body":%q}`, body)
	}))
	defer github.Close()
	defer once.Do(func() { close(release) })
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: operatorOwnedRunner{manifest: manifest}, Tmux: "tmux", Git: "git", VerifyWorker: func(context.Context) error { return nil }}
	effects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, executor: agentruntime.EffectExecutor{Runtime: runtimeState}, active: map[string]*activeRuntimeEffect{}, stopped: true}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, effects: effects, collector: reconciliationV2Collector{API: internalgithub.API{BaseURL: github.URL, HTTP: github.Client()}, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}, reviewer: absentSessionBoundary{}, reviewSource: "source", reviewCommand: []string{"review"}}
	result := make(chan controlResult, 1)
	go func() {
		result <- service.perform(t.Context(), operatorRequest("plan-review-edited", "review-plan", manifest, false))
	}()
	<-entered
	// The owner remains available while the GitHub read is blocked. Its newer
	// observation must invalidate both the old snapshot and the in-flight body.
	current := mustOwnerSnapshot(t, owner).State
	issue := expandIssueFact(current.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "edited"
	attempt := expandAttemptFact(current.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Fact)
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
	applyReconciliationInput(t, owner, input)
	body = "edited"
	once.Do(func() { close(release) })
	if got := <-result; got.Status != http.StatusConflict || got.OK {
		t.Fatalf("edited issue was admitted: %#v", got)
	}
	state := mustOwnerSnapshot(t, owner).State
	if len(state.ControlReceipts) != 0 || len(state.Effects) != 0 {
		t.Fatalf("stale body committed a pending effect: %#v", state)
	}
}

func TestV2ConcurrentSameAttemptDismissHandlersConverge(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 334, "completed", true)
	service := operatorTestMutationService(t, owner)
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	var reads atomic.Int32
	service.issueClosed = func(context.Context, string, int) (bool, error) {
		reads.Add(1)
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
	close(release)
	for range 2 {
		response := <-responses
		if response.Code != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey("o/r", manifest.Issue, manifest.Attempt)
	if len(state.ControlReceipts) != 2 || len(state.Tombstones) != 1 || state.AttemptGenerations[key] != 2 || len(state.Effects) != 1 {
		t.Fatalf("state=%#v", state)
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("GitHub issue reads=%d, want one coalesced admission read", got)
	}
}

func TestV2SameActionConvergesWhenFirstMutationWinsBeforeSecondReservation(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 335, "completed", true)
	service := operatorTestMutationService(t, owner)
	entered, release := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	service.issueClosed = func(context.Context, string, int) (bool, error) {
		if calls.Add(1) == 1 {
			close(entered)
			<-release
		}
		return true, nil
	}
	slow := make(chan controlResult, 1)
	go func() {
		slow <- service.perform(t.Context(), operatorRequest("dismiss-slow-reservation", "dismiss", manifest, false))
	}()
	<-entered
	if result := service.perform(t.Context(), operatorRequest("dismiss-winner", "dismiss", manifest, false)); !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("winning dismiss=%#v", result)
	}
	close(release)
	if result := <-slow; !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("converged dismiss=%#v", result)
	}
	state := mustOwnerSnapshot(t, owner).State
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if len(state.ControlReceipts) != 2 || len(state.Tombstones) != 1 || state.AttemptGenerations[key] != 2 || len(state.Effects) != 1 {
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
	refreshOperatorObservation(t, owner)
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
		if response := <-responses; response.Code != http.StatusAccepted {
			t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
		}
	}
	committed := mustOwnerSnapshot(t, owner).State
	if len(committed.ControlReceipts) != 2 || len(committed.Tombstones) != 2 {
		t.Fatalf("state=%#v", committed)
	}
}

func TestDashboardAndLocalControlMutationsCommitDuringBlockedReconciliation(t *testing.T) {
	root := resolvedTempDir(t)
	manifests := []agentruntime.Manifest{ownerTestManifest(t, root, 340, 1, "completed"), ownerTestManifest(t, root, 341, 1, "completed")}
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
	refreshOperatorObservation(t, owner)
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	server := &dashboardServer{ctx: t.Context(), stateRoot: root, repository: "o/r", operator: service}

	collectEntered, collectRelease := make(chan struct{}), make(chan struct{})
	reconcileDone := make(chan error, 1)
	runner := reconciliationRunner{owner: owner, collect: func(ctx context.Context, _ stateOwnerSnapshot) (reconciliationInput, error) {
		close(collectEntered)
		select {
		case <-collectRelease:
			return repositoryInput(true), nil
		case <-ctx.Done():
			return reconciliationInput{}, ctx.Err()
		}
	}}
	go func() { _, err := runner.run(t.Context()); reconcileDone <- err }()
	<-collectEntered

	dashboardDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=340&attempt=1", nil)
		request.Host = "localhost"
		request.Header.Set("Origin", "http://localhost")
		response := httptest.NewRecorder()
		server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
		dashboardDone <- response
	}()
	controlDone := make(chan controlResult, 1)
	go func() {
		controlDone <- server.performRecordedControl(t.Context(), operatorRequest("local-dismiss", "dismiss", manifests[1], false))
	}()
	if response := <-dashboardDone; response.Code != http.StatusAccepted {
		t.Fatalf("dashboard status=%d body=%s", response.Code, response.Body.String())
	}
	if result := <-controlDone; !result.OK || result.Status != http.StatusAccepted {
		t.Fatalf("local control result=%#v", result)
	}
	close(collectRelease)
	if err := <-reconcileDone; err != nil && !errors.Is(err, errStaleStateResult) {
		t.Fatal(err)
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
	refreshOperatorObservation(t, owner)
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
				refreshOperatorObservation(t, owner)
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
			t.Cleanup(func() {
				if err := service.shutdown(context.Background()); err != nil {
					t.Error(err)
				}
			})
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
		if writes == 3 {
			return errors.New("injected operator admission persistence failure")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
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
	if after.Revision != 3 || after.AttemptGenerations[key] != 1 || len(after.ControlReceipts) != 0 || len(after.Effects) != 0 || runner.calls.Load() != 0 {
		t.Fatalf("state=%#v calls=%d", after, runner.calls.Load())
	}
}

func TestOperatorMarkerSurvivesFinishPersistenceFailureAndFinalizesAfterRestart(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 341, 1, "running")
	manifest, pane := boundRuntimeEffectTestManifest(t, manifest)
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	writes := 0
	var persisted runtimeOwnerState
	owner, err := startTestStateOwner(t, root, state, func(candidate runtimeOwnerState) error {
		writes++
		if writes == 4 {
			return errors.New("injected operator result persistence failure")
		}
		persisted = cloneRuntimeOwnerState(candidate)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	refreshOperatorObservation(t, owner)
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: root, Runner: &barrierEffectRunner{pane: pane}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
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
	restartRuntime := &agentruntime.Runtime{Root: restarted.attemptRoot, StateRoot: root, Runner: &barrierEffectRunner{pane: pane}, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	restartEffects := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, executor: agentruntime.EffectExecutor{Runtime: restartRuntime}, active: map[string]*activeRuntimeEffect{}}
	restartService := &operatorMutationService{lifecycle: t.Context(), owner: restarted, effects: restartEffects, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}}
	if err := restartService.resumePending(t.Context()); err != nil {
		t.Fatal(err)
	}
	final := mustOwnerSnapshot(t, restarted).State
	receipt, _ = operatorReceiptByID(final, command.Request.RequestID)
	if receipt.State != "completed" || final.Effects[effect.ID].State != "completed" || final.Attempts[ownerAttemptKey("o/r", 341, 1)].Manifest.State != "cancelled" {
		t.Fatalf("state=%#v receipt=%#v", final, receipt)
	}
	if _, err := os.Lstat(marker); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("restart did not reclaim committed marker: %v", err)
	}
}

type operatorOwnedRunner struct{ manifest agentruntime.Manifest }

func (r operatorOwnedRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if command.Name == "git" && slices.Contains(command.Args, "--show-current") {
		return agentruntime.Result{Output: r.manifest.Branch + "\n"}, nil
	}
	if r.manifest.Version == agentruntime.ManifestVersion2 && command.Name == "tmux" && slices.Contains(command.Args, agentruntime.ImplementationPaneFormat) {
		return agentruntime.Result{Output: fmt.Sprintf("%s|$1|%%1|1234|2345|1|%s|%s|bound-test-worker\n", r.manifest.Session, r.manifest.Worktree, r.manifest.LaunchToken)}, nil
	}
	if r.manifest.Version == agentruntime.ManifestVersion2 && command.Name == "tmux" && len(command.Args) > 0 && command.Args[0] == "if-shell" {
		return agentruntime.Result{Output: "0"}, nil
	}
	if command.Name == "tmux" && slices.Contains(command.Args, "has-session") {
		return agentruntime.Result{}, nil
	}
	return agentruntime.Result{}, fmt.Errorf("unexpected ownership command %s %v", command.Name, command.Args)
}

type concurrentOwnedRunner struct {
	branches map[string]string
	entered  chan struct{}
	release  chan struct{}
	calls    atomic.Int32
	once     sync.Once
}

func (r *concurrentOwnedRunner) Run(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if r.calls.Add(1) <= 2 {
		if r.calls.Load() == 2 {
			r.once.Do(func() { close(r.entered) })
		}
		select {
		case <-r.release:
		case <-ctx.Done():
			return agentruntime.Result{}, ctx.Err()
		}
	}
	if command.Name == "git" && slices.Contains(command.Args, "--show-current") {
		path := ""
		if index := slices.Index(command.Args, "-C"); index >= 0 && index+1 < len(command.Args) {
			path = command.Args[index+1]
		}
		return agentruntime.Result{Output: r.branches[path] + "\n"}, nil
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
	additionalPath  string
	entered, exited chan struct{}
	enterOnce       sync.Once
	calls           atomic.Int32
	err             error
}

type operatorAdmissionBarrier struct {
	block func()
}

type cancellableAdmissionBarrier struct {
	entered chan struct{}
	once    sync.Once
}

func (b *cancellableAdmissionBarrier) call(ctx context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if !strings.HasPrefix(operation, "validate-") {
		return agentruntime.Result{}, fmt.Errorf("unexpected cleanup operation %q", operation)
	}
	b.once.Do(func() { close(b.entered) })
	<-ctx.Done()
	return agentruntime.Result{}, ctx.Err()
}

func (b *operatorAdmissionBarrier) call(_ context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if strings.HasPrefix(operation, "validate-") {
		b.block()
		return agentruntime.Result{}, nil
	}
	return agentruntime.Result{}, fmt.Errorf("unexpected cleanup operation %q", operation)
}

func prepareWorkerStatusManifestChange(t *testing.T, owner *stateOwner, repository string, issue, attempt int) finishRuntimeEffectCommand {
	t.Helper()
	snapshot := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey(repository, issue), ownerAttemptKey(repository, issue, attempt)
	manifest := snapshot.State.Attempts[attemptKey].Manifest
	identity := stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[issueKey], AttemptGeneration: snapshot.State.AttemptGenerations[attemptKey]}
	_, effect, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectMonitor, Manifest: manifest, RequestDigest: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	manifest.WorkerStatus, manifest.WorkerStatusReason, manifest.WorkerStatusSeq = "needs-attention", "operator decision required", manifest.WorkerStatusSeq+1
	manifest.UpdatedAt = manifest.UpdatedAt.Add(time.Second)
	return finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectMonitor, Manifest: manifest}
}

func prepareMonitorTimestampChange(t *testing.T, owner *stateOwner, repository string, issue, attempt int) finishRuntimeEffectCommand {
	t.Helper()
	snapshot := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey(repository, issue), ownerAttemptKey(repository, issue, attempt)
	manifest := snapshot.State.Attempts[attemptKey].Manifest
	identity := stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[issueKey], AttemptGeneration: snapshot.State.AttemptGenerations[attemptKey]}
	_, effect, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectMonitor, Manifest: manifest, RequestDigest: digestText(fmt.Sprintf("monitor-timestamp-%d", snapshot.State.Revision))})
	if err != nil {
		t.Fatal(err)
	}
	manifest.UpdatedAt = manifest.UpdatedAt.Add(time.Second)
	return finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectMonitor, Manifest: manifest}
}

func prepareReviewManifestChange(t *testing.T, owner *stateOwner, repository string, issue, attempt int) finishRuntimeEffectCommand {
	t.Helper()
	snapshot := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey(repository, issue), ownerAttemptKey(repository, issue, attempt)
	manifest := snapshot.State.Attempts[attemptKey].Manifest
	identity := stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[issueKey], AttemptGeneration: snapshot.State.AttemptGenerations[attemptKey]}
	head := strings.Repeat("c", 40)
	target := manifest.BaseSHA + ".." + head
	runID := digestText(fmt.Sprintf("preflight-review-%d-%d", issue, attempt))
	review := agentruntime.ReviewTransition{State: "clean", Mode: agentruntime.ReviewModeImplementation, Target: target, RunID: runID, Base: manifest.BaseSHA, Head: head}
	review.Snapshot, review.Session = reviewRunIdentity(operatorEffectAttempt(manifest), productionSnapshotRoot(owner.stateRoot), target, runID)
	_, effect, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{Identity: identity, Action: agentruntime.EffectReview, Manifest: manifest, Review: &review, RequestDigest: strings.Repeat("b", 64)})
	if err != nil {
		t.Fatal(err)
	}
	result, err := agentruntime.ReviewEffectResult(owner.attemptRoot, owner.stateRoot, manifest, review)
	if err != nil {
		t.Fatal(err)
	}
	return finishRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*effect)), Action: agentruntime.EffectReview, Manifest: result}
}

func (b *operatorCleanupBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if strings.HasPrefix(operation, "validate-") {
		return agentruntime.Result{}, nil
	}
	if !slices.Contains([]string{"cleanup", "abandon", "remove"}, operation) {
		return agentruntime.Result{}, fmt.Errorf("unexpected cleanup operation %q", operation)
	}
	b.calls.Add(1)
	if b.err != nil {
		return agentruntime.Result{}, b.err
	}
	if b.entered != nil {
		b.enterOnce.Do(func() { close(b.entered) })
		<-ctx.Done()
		if b.exited != nil {
			close(b.exited)
		}
		return agentruntime.Result{}, ctx.Err()
	}
	if err := os.RemoveAll(b.path); err != nil {
		return agentruntime.Result{}, err
	}
	if b.additionalPath != "" {
		return agentruntime.Result{}, os.RemoveAll(b.additionalPath)
	}
	return agentruntime.Result{}, nil
}

type operatorBoundaryRecorder struct {
	mu          sync.Mutex
	seen        []string
	dirs        []string
	sessionLive bool
}

func (r *operatorBoundaryRecorder) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.seen = append(r.seen, operation)
	r.dirs = append(r.dirs, command.Dir)
	if operation != "run" || len(command.Args) == 0 {
		return agentruntime.Result{}, nil
	}
	switch command.Args[0] {
	case "display-message":
		if command.Args[len(command.Args)-1] == reviewerPaneIdentityFormat {
			if r.sessionLive {
				return agentruntime.Result{Output: "0||||||\n"}, nil
			}
			return agentruntime.Result{Output: "||||||||||\n"}, nil
		}
		if r.sessionLive {
			return agentruntime.Result{Output: "0||||\n"}, nil
		}
		return agentruntime.Result{Output: "||||\n"}, nil
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

func (r *operatorBoundaryRecorder) directories() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.dirs)
}
