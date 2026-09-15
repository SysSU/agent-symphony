package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type blockedOwnerHandoffBoundary struct {
	plan          reconciliationPlannedEffect
	old           agentruntime.ImplementationLaunchBinding
	prepareStart  chan struct{}
	prepareDone   chan struct{}
	cleanupStart  chan struct{}
	cleanupDone   chan struct{}
	cleanupOnce   sync.Once
	cleanupErr    atomic.Bool
	cleanupProofs atomic.Int32
	releases      atomic.Int32
}

func (b *blockedOwnerHandoffBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	switch operation {
	case "verify-handoff":
		return agentruntime.Result{}, nil
	case "prepare-handoff":
		close(b.prepareStart)
		<-b.prepareDone // Simulate host I/O that does not respond to cancellation.
		return agentruntime.Result{Output: b.plan.Identity.EffectID + ":" + b.plan.Request.Handoff.CandidateLaunchToken}, nil
	case "release-handoff":
		b.releases.Add(1)
		return agentruntime.Result{}, errors.New("stale handoff release reached host")
	case "compensate-handoff":
		var request handoffCompensationRequest
		body, err := io.ReadAll(command.Stdin)
		if err != nil || json.Unmarshal(body, &request) != nil {
			return agentruntime.Result{}, errors.New("invalid compensation request")
		}
		b.cleanupOnce.Do(func() { close(b.cleanupStart) })
		<-b.cleanupDone
		if b.cleanupErr.Load() {
			return agentruntime.Result{}, errors.New("simulated host compensation failure")
		}
		disposition := "killed"
		if request.PreserveOld {
			disposition = "marked-old"
		}
		proof := handoffCompensationProof{EffectID: b.plan.Identity.EffectID, Token: b.plan.Request.Handoff.CandidateLaunchToken, OldSessionID: b.old.SessionID, OldPaneID: b.old.PaneID, PreserveOld: request.PreserveOld, Disposition: disposition}
		proofBody, _ := json.Marshal(proof)
		b.cleanupProofs.Add(1)
		return agentruntime.Result{Output: string(proofBody)}, nil
	default:
		return agentruntime.Result{}, errors.New("unexpected boundary operation " + operation)
	}
}

func TestDashboardStopAdmitsWhileBoundHandoffHostIsBlocked(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 352, 1, "running")
	manifest.Version, manifest.LaunchToken, manifest.LaunchID = agentruntime.ManifestVersion2, strings.Repeat("a", 32), strings.Repeat("b", 32)
	manifest.ReviewState, manifest.ReviewHead, manifest.ReviewFindings = "findings-queued", manifest.BaseSHA, []string{"apply review finding"}
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, productionAttemptRoot(root), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	plans, material, err := planReconciliationHandoffs(mustOwnerSnapshot(t, owner), []string{"worker"}, nil)
	if err != nil || len(plans) != 1 {
		t.Fatalf("planned handoff = %#v, %v", plans, err)
	}
	service := operatorTestMutationService(t, owner)
	plan, err := service.effects.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	old := agentruntime.ImplementationLaunchBinding{Version: 1, Role: "unknown", Token: manifest.LaunchToken, EffectID: manifest.LaunchID, ServerPID: 100, ServerStart: 1, SessionName: manifest.Session, SessionID: "$1", PaneID: "%1", PanePID: 101, StartPath: manifest.Worktree, Command: "/bin/sh"}
	if err := os.MkdirAll(filepath.Dir(agentruntime.ImplementationBindingPath(manifest, manifest.LaunchID)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := agentruntime.WriteImplementationBinding(manifest, old); err != nil {
		t.Fatal(err)
	}
	boundary := &blockedOwnerHandoffBoundary{plan: plan, old: old, prepareStart: make(chan struct{}), prepareDone: make(chan struct{}), cleanupStart: make(chan struct{}), cleanupDone: make(chan struct{})}
	service.cleanup.implementation = boundary
	t.Cleanup(func() {
		select {
		case <-boundary.prepareDone:
		default:
			close(boundary.prepareDone)
		}
		select {
		case <-boundary.cleanupDone:
		default:
			close(boundary.cleanupDone)
		}
	})
	workDone := make(chan error, 1)
	go func() {
		_, err := service.effects.executeHandoff(t.Context(), boundary, plan, material[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)])
		workDone <- err
	}()
	<-boundary.prepareStart
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/cancel?repository=o%2Fr&issue=352&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		server.handler(http.NotFoundHandler()).ServeHTTP(recorder, request)
		response <- recorder
	}()
	select {
	case result := <-response:
		if result.Code != http.StatusAccepted {
			t.Fatalf("Stop admission = HTTP %d %s", result.Code, result.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Stop waited for blocked host handoff I/O")
	}
	committed := mustOwnerSnapshot(t, owner).State
	if _, exists := committed.Effects[plan.Identity.EffectID]; exists {
		t.Fatal("old handoff effect survived Stop")
	}
	stopIntent := false
	for _, effect := range committed.Effects {
		if effect.Action == string(agentruntime.EffectStop) && effect.InvalidatedHandoff != nil && effect.InvalidatedHandoff.EffectID == plan.Identity.EffectID {
			stopIntent = true
		}
	}
	if !stopIntent {
		t.Fatalf("Stop did not durably retain candidate cleanup identity: %#v", committed.Effects)
	}
	close(boundary.prepareDone)
	if err := <-workDone; err == nil || boundary.releases.Load() != 0 {
		t.Fatalf("stale handoff was released or completed after Stop: %v, releases=%d", err, boundary.releases.Load())
	}
	select {
	case <-boundary.cleanupStart:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not start candidate compensation")
	}
	close(boundary.cleanupDone)
	service.wg.Wait()
	if boundary.cleanupProofs.Load() == 0 {
		t.Fatal("Stop did not obtain exact candidate compensation proof")
	}
	final := mustOwnerSnapshot(t, owner).State
	if _, exists := final.Effects[plan.Identity.EffectID]; exists {
		t.Fatal("old handoff effect reappeared after Stop")
	}
}

func TestDashboardDismissHidesLegacyAttemptWithoutTouchingLiveUnboundPane(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-v1-dismiss-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 353, 1, "running")
	if output, err := exec.Command(tmux, "new-session", "-d", "-s", manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("create legacy pane: %v: %s", err, output)
	}
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
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=353&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("legacy Dismiss = HTTP %d %s", response.Code, response.Body.String())
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	committed := mustOwnerSnapshot(t, owner).State
	if _, exists := committed.Attempts[key]; exists || committed.Tombstones[key].Action != "dismissed" {
		t.Fatalf("legacy Dismiss did not hide attempt: %#v", committed.Tombstones[key])
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "="+manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("hide-only Dismiss touched unbound live pane: %v: %s", err, output)
	}
}

func TestLegacyCleanupRejectsRenamedLivePane(t *testing.T) {
	tmux, err := exec.LookPath("tmux")
	if err != nil {
		t.Skip("tmux is unavailable")
	}
	socketRoot, err := os.MkdirTemp("/tmp", "as-v1-renamed-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(socketRoot) })
	t.Setenv("TMUX_TMPDIR", socketRoot)
	t.Cleanup(func() { _ = exec.Command(tmux, "kill-server").Run() })
	manifest := ownerTestManifest(t, resolvedTempDir(t), 354, 1, "running")
	if output, err := exec.Command(tmux, "new-session", "-d", "-s", manifest.Session).CombinedOutput(); err != nil {
		t.Fatalf("create legacy pane: %v: %s", err, output)
	}
	if output, err := exec.Command(tmux, "rename-session", "-t", "="+manifest.Session, "renamed-legacy-worker").CombinedOutput(); err != nil {
		t.Fatalf("rename legacy pane: %v: %s", err, output)
	}
	if err := stopAttemptSession(t.Context(), manifest); err == nil || !strings.Contains(err.Error(), "no durable launch identity") {
		t.Fatalf("renamed legacy cleanup was not rejected: %v", err)
	}
	if output, err := exec.Command(tmux, "has-session", "-t", "=renamed-legacy-worker").CombinedOutput(); err != nil {
		t.Fatalf("renamed legacy worker was touched: %v: %s", err, output)
	}
}

func TestDashboardDismissAdmitsWhileBoundHandoffHostIsBlocked(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 351, 1, "running")
	manifest.Version, manifest.LaunchToken, manifest.LaunchID = agentruntime.ManifestVersion2, strings.Repeat("a", 32), strings.Repeat("b", 32)
	manifest.ReviewState, manifest.ReviewHead, manifest.ReviewFindings = "findings-queued", manifest.BaseSHA, []string{"apply review finding"}
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
	snapshot := mustOwnerSnapshot(t, owner)
	plans, material, err := planReconciliationHandoffs(snapshot, []string{"worker"}, nil)
	if err != nil || len(plans) != 1 {
		t.Fatalf("planned handoff = %#v, %v", plans, err)
	}
	service := operatorTestMutationService(t, owner)
	service.issueClosed = func(context.Context, string, int) (bool, error) { return true, nil }
	plan, err := service.effects.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	old := agentruntime.ImplementationLaunchBinding{Version: 1, Role: "unknown", Token: manifest.LaunchToken, EffectID: manifest.LaunchID, ServerPID: 100, ServerStart: 1, SessionName: manifest.Session, SessionID: "$1", PaneID: "%1", PanePID: 101, StartPath: manifest.Worktree, Command: "/bin/sh"}
	if err := os.MkdirAll(filepath.Dir(agentruntime.ImplementationBindingPath(manifest, manifest.LaunchID)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := agentruntime.WriteImplementationBinding(manifest, old); err != nil {
		t.Fatal(err)
	}
	boundary := &blockedOwnerHandoffBoundary{plan: plan, old: old, prepareStart: make(chan struct{}), prepareDone: make(chan struct{}), cleanupStart: make(chan struct{}), cleanupDone: make(chan struct{})}
	boundary.cleanupErr.Store(true)
	service.cleanup.implementation = boundary
	t.Cleanup(func() {
		select {
		case <-boundary.prepareDone:
		default:
			close(boundary.prepareDone)
		}
		select {
		case <-boundary.cleanupDone:
		default:
			close(boundary.cleanupDone)
		}
	})
	workDone := make(chan error, 1)
	go func() {
		_, err := service.effects.executeHandoff(t.Context(), boundary, plan, material[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)])
		workDone <- err
	}()
	<-boundary.prepareStart
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=351&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		recorder := httptest.NewRecorder()
		server.handler(http.NotFoundHandler()).ServeHTTP(recorder, request)
		response <- recorder
	}()
	select {
	case result := <-response:
		var body controlResult
		var status operatorReceiptStatus
		if json.Unmarshal(result.Body.Bytes(), &body) != nil || json.Unmarshal(body.Data, &status) != nil || result.Code != http.StatusAccepted || !body.OK || status.Phase != operatorPhaseHandoffCleanup {
			t.Fatalf("Dismiss admission = HTTP %d %s", result.Code, result.Body.String())
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Dismiss waited for blocked host handoff I/O")
	}
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	committed := mustOwnerSnapshot(t, owner).State
	if _, exists := committed.Attempts[key]; exists || committed.Tombstones[key].InvalidatedHandoff == nil || committed.Tombstones[key].InvalidatedHandoff.EffectID != plan.Identity.EffectID {
		t.Fatalf("Dismiss failed to hide attempt and retain candidate: %#v", committed.Tombstones[key])
	}
	if _, exists := committed.Effects[plan.Identity.EffectID]; exists {
		t.Fatal("old handoff effect survived Dismiss")
	}
	second := httptest.NewRecorder()
	secondRequest := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=351&attempt=1", nil)
	secondRequest.Host = "localhost"
	secondRequest.Header.Set("Origin", "http://localhost")
	server.handler(http.NotFoundHandler()).ServeHTTP(second, secondRequest)
	var secondBody controlResult
	var secondStatus operatorReceiptStatus
	if json.Unmarshal(second.Body.Bytes(), &secondBody) != nil || json.Unmarshal(secondBody.Data, &secondStatus) != nil || second.Code != http.StatusAccepted || !secondBody.OK || secondStatus.Phase != operatorPhaseHandoffCleanup {
		t.Fatalf("second Dismiss during candidate cleanup = HTTP %d %s", second.Code, second.Body.String())
	}
	close(boundary.prepareDone)
	if err := <-workDone; err == nil {
		t.Fatal("stale handoff completed after Dismiss")
	}
	if boundary.releases.Load() != 0 {
		t.Fatal("stale handoff reached release after Dismiss")
	}
	select {
	case <-boundary.cleanupStart:
	case <-time.After(2 * time.Second):
		t.Fatal("durable Dismiss did not dispatch candidate compensation")
	}
	close(boundary.cleanupDone)
	service.wg.Wait()
	failed := mustOwnerSnapshot(t, owner).State
	if failed.Tombstones[key].HandoffCompensated || len(failed.Attempts) != 0 {
		t.Fatalf("failed compensation was falsely completed or restored attempt: %#v", failed.Tombstones[key])
	}
	for _, receipt := range failed.ControlReceipts {
		if receipt.Request.Action == "dismiss" && receipt.Request.Issue == manifest.Issue && (receipt.State != "pending" || receipt.Phase != operatorPhaseHandoffCleanup || !strings.Contains(receipt.Diagnostic, "simulated host compensation failure")) {
			t.Fatalf("failed compensation did not persist pending diagnostic: %#v", receipt)
		}
	}
	if err := owner.close(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil || recovered.Tombstones[key].HandoffCompensated {
		t.Fatalf("restart state lost pending compensation: %#v, %v", recovered.Tombstones[key], err)
	}
	restarted, err := startTestStateOwner(t, root, recovered, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, productionAttemptRoot(root), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	applyReconciliationInput(t, restarted, repositoryInput(true)) // GitHub no longer reports the issue.
	serviceAfterRestart := operatorTestMutationService(t, restarted)
	serviceAfterRestart.cleanup.implementation = boundary
	serverAfterRestart := &dashboardServer{ctx: t.Context(), stateRoot: restarted.stateRoot, repository: manifest.Repository, operator: serviceAfterRestart}
	third := httptest.NewRecorder()
	thirdRequest := httptest.NewRequest(http.MethodPost, "http://localhost/actions/dismiss?repository=o%2Fr&issue=351&attempt=1", nil)
	thirdRequest.Host = "localhost"
	thirdRequest.Header.Set("Origin", "http://localhost")
	serverAfterRestart.handler(http.NotFoundHandler()).ServeHTTP(third, thirdRequest)
	if third.Code != http.StatusAccepted {
		t.Fatalf("post-restart Dismiss replay after absent observation = HTTP %d %s", third.Code, third.Body.String())
	}
	serviceAfterRestart.wg.Wait() // First retry still fails; diagnostic stays durable.
	boundary.cleanupErr.Store(false)
	if err := serviceAfterRestart.resumePending(t.Context()); err != nil {
		t.Fatal(err)
	}
	serviceAfterRestart.wg.Wait()
	finished := mustOwnerSnapshot(t, restarted).State
	if !finished.Tombstones[key].HandoffCompensated || len(finished.Attempts) != 0 {
		t.Fatalf("candidate proof did not finish Dismiss without resurrection: %#v", finished.Tombstones[key])
	}
	completed := 0
	for _, receipt := range finished.ControlReceipts {
		if receipt.Request.Action == "dismiss" && receipt.Request.Issue == manifest.Issue && receipt.Request.Attempt == manifest.Attempt && receipt.State == "completed" {
			completed++
		}
	}
	if completed != 3 {
		t.Fatalf("three Dismiss requests did not share confirmed cleanup: %#v", finished.ControlReceipts)
	}
	persisted, err := readRuntimeOwnerState(restarted.stateRoot, manifest.Repository)
	if err != nil || !persisted.Tombstones[key].HandoffCompensated {
		t.Fatalf("Dismiss proof was not durable: %#v, %v", persisted.Tombstones[key], err)
	}
}
