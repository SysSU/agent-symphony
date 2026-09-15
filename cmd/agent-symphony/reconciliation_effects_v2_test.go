package main

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
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

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestBlockedMachineStatusConvergesAfterDestructiveActionsAndRetry(t *testing.T) {
	for _, status := range []string{"needs-attention", "clear"} {
		for _, action := range []string{"dismiss", "abandon", "remove", "retry"} {
			t.Run(status+"/"+action, func(t *testing.T) { testBlockedMachineStatusConvergence(t, status, action) })
		}
	}
}

func testBlockedMachineStatusConvergence(t *testing.T, desired, action string) {
	owner, manifest := operatorTestOwner(t, 333, "active", false)
	service := operatorTestMutationService(t, owner)
	service.collector.Config.ActorID = 42
	snapshot := mustOwnerSnapshot(t, owner)
	issueKey, attemptKey := ownerIssueKey("o/r", 333), ownerAttemptKey("o/r", 333, 1)
	snapshot, err := owner.admitMachineStatus(t.Context(), admitMachineStatusCommand{Repository: "o/r", Issue: 333, Attempt: 1, ExpectedIssueGeneration: snapshot.State.IssueGenerations[issueKey], ExpectedAttemptGeneration: snapshot.State.AttemptGenerations[attemptKey], Source: "orchestrator", SourceID: "blocked-write", Status: desired, Reason: "monitoring: blocked write"})
	if err != nil {
		t.Fatal(err)
	}
	plans, err := planMachineStatusUpdates(snapshot, service.collector.Config)
	if err != nil || len(plans) != 1 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	plan, err := service.effects.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var comments []map[string]any
	label := desired == "clear"
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		respond := func(status int, value any) *http.Response {
			body, _ := json.Marshal(value)
			return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: request}
		}
		mu.Lock()
		defer mu.Unlock()
		switch {
		case request.Method == http.MethodGet && strings.Contains(request.URL.Path, "/comments"):
			return respond(http.StatusOK, comments), nil
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/issues/333":
			labels := []map[string]string{}
			if label {
				labels = append(labels, map[string]string{"name": internalgithub.NeedsAttentionLabel})
			}
			return respond(http.StatusOK, map[string]any{"labels": labels}), nil
		case request.Method == http.MethodPost && strings.Contains(request.URL.Path, "/comments"):
			var payload struct{ Body string }
			if json.NewDecoder(request.Body).Decode(&payload) != nil {
				t.Fatal("invalid comment payload")
			}
			if len(comments) == 0 {
				close(entered)
				mu.Unlock()
				<-release
				mu.Lock()
			}
			now := time.Unix(int64(len(comments)+1), 0).UTC()
			comments = append(comments, map[string]any{"id": len(comments) + 1, "body": payload.Body, "created_at": now, "updated_at": now, "user": map[string]any{"id": 42}})
			return respond(http.StatusCreated, map[string]any{}), nil
		case request.Method == http.MethodPost && strings.HasSuffix(request.URL.Path, "/labels"):
			label = true
			return respond(http.StatusOK, []any{}), nil
		case request.Method == http.MethodDelete && strings.HasSuffix(request.URL.Path, "/labels/needs-attention"):
			label = false
			return respond(http.StatusNoContent, nil), nil
		default:
			return nil, fmt.Errorf("unexpected request %s %s", request.Method, request.URL.String())
		}
	})}}
	done := make(chan error, 1)
	go func() {
		_, executeErr := service.effects.executeIssueUpdate(t.Context(), api, plan)
		done <- executeErr
	}()
	<-entered
	before := mustOwnerSnapshot(t, owner)
	var after stateOwnerSnapshot
	switch action {
	case "dismiss":
		after, _, err = owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 333, Attempt: 1, ExpectedIssueGeneration: before.State.IssueGenerations[issueKey], ExpectedAttemptGeneration: before.State.AttemptGenerations[attemptKey], Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest})
	case "abandon", "remove":
		published := ""
		tombstoneAction := "abandoned"
		if action == "remove" {
			published = manifest.BaseSHA
			tombstoneAction = "removed"
		}
		policy := agentruntime.EffectCleanupPolicy{Action: action, PublishedHead: published}
		after, _, err = owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 333, Attempt: 1, ExpectedIssueGeneration: before.State.IssueGenerations[issueKey], ExpectedAttemptGeneration: before.State.AttemptGenerations[attemptKey], Action: tombstoneAction, CleanupPhase: "pending", PublishedHead: published, Manifest: &manifest, CleanupPolicy: &policy, EffectAction: string(agentruntime.EffectCleanup), EffectRequestDigest: strings.Repeat("f", 64)})
	case "retry":
		after, err = owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: ownerTestManifest(t, owner.stateRoot, 333, 2, "running")})
	}
	if err != nil {
		t.Fatal(err)
	}
	service.effects.cancelInvalidated(after)
	close(release)
	if err := <-done; err == nil {
		t.Fatal("stale status write completed as current")
	}
	production := productionReconciliation{owner: owner, effects: service.effects, collector: service.collector}
	if changed, err := production.resolveInvalidatedGitHubEffect(t.Context(), api); err != nil || !changed {
		t.Fatalf("compensation changed=%t err=%v", changed, err)
	}
	mu.Lock()
	defer mu.Unlock()
	status := mustOwnerSnapshot(t, owner).State.MachineStatuses[issueKey]
	if label || status.Status != "clear" || status.AppliedSequence != status.Sequence || len(comments) != 2 {
		t.Fatalf("label=%t comments=%#v status=%#v", label, comments, status)
	}
}

func TestEscapedImplementationChildBlocksGitHubPublication(t *testing.T) {
	if os.Getenv("AGENT_SYMPHONY_ESCAPED_IMPLEMENTATION_HELPER") == "1" {
		child := exec.Command("/bin/sleep", "30")
		child.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
		if err := child.Start(); err != nil {
			os.Exit(125)
		}
		_, _ = fmt.Fprintf(os.Stdout, "%d\n", child.Process.Pid)
		os.Exit(0)
	}
	request := reconciliationEffectCaseNamed(t, "github-publish").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	baseline := cloneRuntimeOwnerState(snapshot.State)
	if _, err := applyBeginReconciliationEffect(owner.attemptRoot, owner.stateRoot, &baseline, beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request}); err != nil {
		t.Fatalf("publish fixture is not otherwise admissible: %v", err)
	}
	launcher := exec.Command(os.Args[0], "-test.run=^TestEscapedImplementationChildBlocksGitHubPublication$")
	launcher.Env = append(os.Environ(), "AGENT_SYMPHONY_ESCAPED_IMPLEMENTATION_HELPER=1")
	launcher.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	stdout, err := launcher.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := launcher.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(strings.TrimSpace(line))
	if err != nil || childPID < 2 {
		t.Fatalf("escaped child PID %q: %v", line, err)
	}
	t.Cleanup(func() { _ = syscall.Kill(childPID, syscall.SIGKILL) })
	if err := launcher.Wait(); err != nil {
		t.Fatal(err)
	}
	if group, err := syscall.Getpgid(childPID); err != nil || group == launcher.Process.Pid {
		t.Fatalf("child did not escape original process group: group=%d original=%d err=%v", group, launcher.Process.Pid, err)
	}
	state := cloneRuntimeOwnerState(snapshot.State)
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	record := state.Attempts[key]
	manifest := record.Manifest
	manifest.Version, manifest.LaunchToken, manifest.LaunchID = agentruntime.ManifestVersion2, strings.Repeat("a", 32), strings.Repeat("b", 32)
	record.Manifest = manifest
	state.Attempts[key] = record
	request.Manifest = &manifest
	if err := validateOwnerManifest(state.Repository, owner.attemptRoot, owner.stateRoot, manifest); err != nil {
		t.Fatalf("launched manifest fixture is invalid: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(manifest.LogPath), 0o700); err != nil {
		t.Fatal(err)
	}
	binding := agentruntime.ImplementationLaunchBinding{Version: 1, Role: "interactive", Token: manifest.LaunchToken, EffectID: manifest.LaunchID, ServerPID: os.Getpid(), ServerStart: 1, SessionName: manifest.Session, SessionID: "$1", PaneID: "%1", PanePID: launcher.Process.Pid, StartPath: manifest.Worktree, Command: "bound-worker"}
	if _, err := agentruntime.WriteImplementationGroupStart(manifest, binding, "interactive", launcher.Process.Pid, launcher.Process.Pid); err != nil {
		t.Fatal(err)
	}
	if gone, err := agentruntime.ImplementationWorkerGone(manifest, binding); err == nil || gone {
		t.Fatalf("escaped child was certified absent: gone=%t err=%v", gone, err)
	}
	for _, action := range []reconciliationEffectAction{reconciliationGitHubBind, reconciliationGitHubPublish, reconciliationGitHubIssueUpdate, reconciliationGitHubPRGovernance} {
		if !implementationLeaseBlocksGitHub(state, action, request.Repository, request.Issue) {
			t.Fatalf("%s ignored live implementation lease", action)
		}
	}
	if _, err := applyBeginReconciliationEffect(owner.attemptRoot, owner.stateRoot, &state, beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request}); !errors.Is(err, errStateConflict) {
		t.Fatalf("owner admitted GitHub publication while escaped child may live: %v", err)
	}
	manifest.WorkerGeneration, manifest.WorkerProfileDigest = record.Generation, config.WorkerProfileDigest()
	record.Manifest = manifest
	state.Attempts[key] = record
	request.Manifest = &manifest
	for _, action := range []reconciliationEffectAction{reconciliationGitHubBind, reconciliationGitHubPublish, reconciliationGitHubIssueUpdate, reconciliationGitHubPRGovernance} {
		if implementationLeaseBlocksGitHub(state, action, request.Repository, request.Issue) {
			t.Fatalf("%s retained revoked authority for confined escaped child", action)
		}
	}
}

func TestWorkerStatusOutcomeCannotApplyOutOfOrderOrAfterInvalidation(t *testing.T) {
	root := t.TempDir()
	manifest := ownerTestManifest(t, root, 329, 1, "running")
	manifest.WorkerStatus, manifest.WorkerStatusReason, manifest.WorkerStatusSeq = "needs-attention", "operator decision required", 7
	key, issueKey := ownerAttemptKey("o/r", 329, 1), ownerIssueKey("o/r", 329)
	state := newRuntimeOwnerState("o/r")
	state.IssueGenerations[issueKey], state.AttemptGenerations[key] = 1, 1
	state.Attempts[key] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	state.MachineStatuses[issueKey] = machineStatusRecord{Repository: "o/r", Issue: 329, Attempt: 1, IssueGeneration: 1, AttemptGeneration: 1, Sequence: 2, Source: "worker", SourceID: "worker-7", SourceSequence: 2, Status: "needs-attention", Reason: "operator decision required"}
	request := reconciliationEffectRequest{Action: reconciliationGitHubIssueUpdate, Repository: "o/r", Issue: 329, GitHubIssueUpdate: &githubIssueUpdateEffectRequest{Kind: githubIssueMachineStatus, AttributionAttempt: 1, Status: "needs-attention", StatusReason: "operator decision required", StatusSequence: 2, StatusSource: "worker", StatusSourceSequence: 2}}
	result := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueMachineStatus, Observed: true}}
	if err := applyReconciliationEffectOutcome(&state, request, result); err != nil || state.MachineStatuses[issueKey].AppliedSequence != 2 || state.Attempts[key].Manifest.WorkerStatusApplied != 2 {
		t.Fatalf("current status outcome failed: status=%#v err=%v", state.MachineStatuses[issueKey], err)
	}
	state.MachineStatuses[issueKey] = machineStatusRecord{Repository: "o/r", Issue: 329, Attempt: 1, IssueGeneration: 1, AttemptGeneration: 1, Sequence: 3, AppliedSequence: 2, Source: "orchestrator", SourceID: "proposal-9", SourceSequence: 3, Status: "clear", Reason: "monitoring: recovered"}
	if err := applyReconciliationEffectOutcome(&state, request, result); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("out-of-order status outcome err=%v", err)
	}
}

func TestMachineStatusPlannerRepairsStaleExternalObservationAfterRestart(t *testing.T) {
	root := t.TempDir()
	manifest := ownerTestManifest(t, root, 334, 1, "running")
	state := runtimeEffectInitialState(manifest)
	addOperatorObservation(&state, manifest, "active", false)
	issueKey := ownerIssueKey("o/r", 334)
	observation := state.Observations[issueKey]
	observation.Fact.NeedsAttention = true
	state.Observations[issueKey] = observation
	state.MachineStatuses[issueKey] = machineStatusRecord{Repository: "o/r", Issue: 334, Attempt: 1, IssueGeneration: state.IssueGenerations[issueKey], AttemptGeneration: 1, Sequence: 2, AppliedSequence: 2, Source: "destructive", SourceID: "dismissed-2", SourceSequence: 2, Status: "clear", Reason: "attempt invalidated"}
	plans, err := planMachineStatusUpdates(stateOwnerSnapshot{State: state}, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42})
	if err != nil || len(plans) != 1 || plans[0].Request.GitHubIssueUpdate.StatusSequence != 2 || plans[0].Request.GitHubIssueUpdate.Status != "clear" {
		t.Fatalf("repair plans=%#v err=%v", plans, err)
	}
}

func TestMachineStatusPlannerRequiresExactOwnerMarker(t *testing.T) {
	manifest := ownerTestManifest(t, t.TempDir(), 335, 1, "running")
	base := runtimeEffectInitialState(manifest)
	addOperatorObservation(&base, manifest, "active", false)
	issueKey := ownerIssueKey("o/r", 335)
	base.MachineStatuses[issueKey] = machineStatusRecord{Repository: "o/r", Issue: 335, Attempt: 1, IssueGeneration: base.IssueGenerations[issueKey], AttemptGeneration: 1, Sequence: 2, AppliedSequence: 2, Source: "orchestrator", SourceID: "current", SourceSequence: 2, Status: "clear", Reason: "monitoring: recovered"}
	for _, test := range []struct {
		name string
		edit func(*reconciliationIssueFact)
		want int
	}{
		{"missing", func(*reconciliationIssueFact) {}, 1},
		{"edited", func(f *reconciliationIssueFact) {
			f.MachineStatusProtocol, f.MachineStatusAttempt, f.MachineStatusSequence, f.MachineStatusReason = 2, 1, 2, "edited"
		}, 1},
		{"current", func(f *reconciliationIssueFact) {
			f.MachineStatusProtocol, f.MachineStatusAttempt, f.MachineStatusSequence, f.MachineStatusReason = 2, 1, 2, "monitoring: recovered"
		}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			state := cloneRuntimeOwnerState(base)
			observation := state.Observations[issueKey]
			test.edit(&observation.Fact)
			state.Observations[issueKey] = observation
			plans, err := planMachineStatusUpdates(stateOwnerSnapshot{State: state}, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42})
			if err != nil || len(plans) != test.want {
				t.Fatalf("plans=%d want=%d err=%v", len(plans), test.want, err)
			}
		})
	}
}

func TestDestructiveInvalidationDurablySupersedesPendingControlSnapshot(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 329, "completed", true)
	state := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	body := internalgithub.SnapshotComment(internalgithub.Snapshot{Version: 2})
	proposal := reconciliationIssueUpdateProposal{Repository: manifest.Repository, Issue: manifest.Issue, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: digestText(body)}
	observation := state.Observations[issueKey]
	observation.IssueUpdates = append(observation.IssueUpdates, proposal)
	state.Observations[issueKey] = observation
	request := reconciliationEffectRequest{Action: reconciliationGitHubIssueUpdate, Repository: manifest.Repository, Issue: manifest.Issue, ObservationGeneration: observation.Generation, ObservationCycleID: observation.LastCycleID, BodyDigest: observation.Fact.BodyDigest, ExecutionDigest: strings.Repeat("a", 64), ControlGeneration: 1, GitHubIssueUpdate: &githubIssueUpdateEffectRequest{Kind: githubIssueControlSnapshot, ControlSnapshotDigest: digestText(body), ControlSnapshotBody: body}}
	effect := runtimeEffectIntent{Action: string(request.Action), Repository: request.Repository, Issue: request.Issue, IssueGeneration: state.IssueGenerations[issueKey], IntentEpoch: state.Epoch, IntentRevision: state.Revision, State: "pending", Dispatched: true, RequestDigest: reconciliationEffectDigest(request), Reconciliation: &request}
	effect.ID = runtimeEffectID(effect)
	state.Effects[effect.ID] = effect
	if _, err := applyInvalidateAttempt(owner.attemptRoot, owner.stateRoot, &state, invalidateAttemptCommand{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, ExpectedIssueGeneration: state.IssueGenerations[issueKey], ExpectedAttemptGeneration: state.AttemptGenerations[attemptKey], Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest}); err != nil {
		t.Fatal(err)
	}
	repair := state.ControlRepairs[issueKey]
	parsed, parseErr := internalgithub.ParseSnapshotComment(repair.Body, 1, 1)
	if retained := state.Effects[effect.ID]; retained.State != "invalidated" || repair.Generation != 2 || parsed.OwnerGeneration != 2 || parseErr != nil {
		t.Fatalf("stale control effect was not superseded: repair=%#v parsed=%#v err=%v", repair, parsed, parseErr)
	}
	state.Revision++
	tombstone := state.Tombstones[attemptKey]
	tombstone.Revision = state.Revision
	state.Tombstones[attemptKey] = tombstone
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(owner.stateRoot, manifest.Repository)
	if err != nil || loaded.ControlRepairs[issueKey] != repair {
		t.Fatalf("restart lost control repair: repair=%#v err=%v", loaded.ControlRepairs[issueKey], err)
	}
	if err := applyResolveInvalidatedReconciliationEffect(&loaded, resolveInvalidatedReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(loaded.Effects[effect.ID]), Outcome: invalidatedExternalOutcome{Action: reconciliationGitHubIssueUpdate, Observed: true}}); err != nil {
		t.Fatalf("resolve invalidated control snapshot: %v", err)
	}
	plans, err := planControlSnapshotRepairs(stateOwnerSnapshot{State: loaded}, internalgithub.PRAdapterConfig{Repository: manifest.Repository, ActorID: 42})
	if err != nil || len(plans) != 1 || !plans[0].Request.ControlRepair || plans[0].Request.ControlGeneration != 2 {
		t.Fatalf("restart repair plans=%#v err=%v", plans, err)
	}
}

func TestControlSnapshotRepairDoesNotWaitForUnrelatedAmbiguousGitHubEffect(t *testing.T) {
	state := newRuntimeOwnerState("o/r")
	key := ownerIssueKey("o/r", 330)
	body := internalgithub.SnapshotComment(internalgithub.Snapshot{Version: 2, OwnerGeneration: 2})
	state.ControlGenerations[key] = 2
	state.ControlRepairs[key] = controlSnapshotRepair{Generation: 2, Body: body}
	publish := reconciliationEffectRequest{Action: reconciliationGitHubPublish, Repository: "o/r", Issue: 330, Attempt: 1, GitHubPublish: &githubPublishEffectRequest{}}
	state.Effects["ambiguous-publish"] = runtimeEffectIntent{Repository: "o/r", Issue: 330, Attempt: 1, State: "invalidated", Dispatched: true, Reconciliation: &publish}
	repair := reconciliationEffectRequest{Action: reconciliationGitHubIssueUpdate, Repository: "o/r", Issue: 330, ControlGeneration: 2, ControlRepair: true, GitHubIssueUpdate: &githubIssueUpdateEffectRequest{Kind: githubIssueControlSnapshot, ControlSnapshotBody: body}}
	result := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueControlSnapshot, Observed: true}}

	if err := applyReconciliationEffectOutcome(&state, repair, result); err != nil {
		t.Fatalf("unrelated ambiguous publication blocked control repair: %v", err)
	}
	if _, ok := state.ControlRepairs[key]; ok {
		t.Fatal("completed control repair was retained")
	}
	if effect := state.Effects["ambiguous-publish"]; effect.State != "invalidated" {
		t.Fatalf("ambiguous publication was not retained: %#v", effect)
	}
}

func TestDispatchedGitHubEffectSurvivesDestructiveInvalidationAndRestart(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-bind").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	if err := owner.authorizeReconciliationEffect(t.Context(), authorizeReconciliationEffectCommand{Identity: identity, Action: request.Action}); err != nil {
		t.Fatal(err)
	}
	authorized := mustOwnerSnapshot(t, owner)
	if !authorized.State.Effects[effect.ID].Dispatched {
		t.Fatal("GitHub dispatch admission was not persisted")
	}
	manifest := *request.Manifest
	issueKey, attemptKey := ownerIssueKey(request.Repository, request.Issue), ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	invalidated, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, ExpectedIssueGeneration: authorized.State.IssueGenerations[issueKey], ExpectedAttemptGeneration: authorized.State.AttemptGenerations[attemptKey], Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest})
	if err != nil {
		t.Fatal(err)
	}
	if retained := invalidated.State.Effects[effect.ID]; retained.State != "invalidated" || !retained.Dispatched {
		t.Fatalf("admitted external obligation was lost: %#v", retained)
	}
	loaded, err := readRuntimeOwnerState(root, request.Repository)
	if err != nil || loaded.Effects[effect.ID].State != "invalidated" {
		t.Fatalf("restart lost external obligation: state=%#v err=%v", loaded.Effects[effect.ID], err)
	}
	if err := applyResolveInvalidatedReconciliationEffect(&loaded, resolveInvalidatedReconciliationEffectCommand{Identity: identity, Outcome: invalidatedExternalOutcome{Action: request.Action, Observed: true}}); err != nil {
		t.Fatal(err)
	}
	outcome := loaded.Tombstones[attemptKey].ExternalOutcomes[effect.ID]
	if !outcome.Observed || outcome.Action != request.Action || loaded.Effects[effect.ID].State != "invalidated-resolved" {
		t.Fatalf("external outcome was not durably bound to tombstone: outcome=%#v effect=%#v", outcome, loaded.Effects[effect.ID])
	}
}

func TestReconciliationEffectVariantsAreExactIdempotentAndConflictSafe(t *testing.T) {
	for _, test := range reconciliationEffectCases(t) {
		t.Run(test.name, func(t *testing.T) {
			owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
			identity := reconciliationBeginIdentity(snapshot, request)
			firstState, first, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: request})
			if err != nil || first == nil {
				t.Fatalf("begin effect=%#v err=%v", first, err)
			}
			replayedState, replayed, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: request})
			if err != nil || replayed == nil || replayed.ID != first.ID || replayedState.State.Revision != firstState.State.Revision {
				t.Fatalf("replay state=%#v effect=%#v err=%v", replayedState.State, replayed, err)
			}
			conflict := cloneReconciliationRequest(request)
			conflict.ExecutionDigest = strings.Repeat("b", 64)
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: conflict}); !errors.Is(err, errStateConflict) {
				t.Fatalf("conflicting begin err=%v", err)
			}
			finishIdentity := reconciliationIntentIdentity(*first)
			result := test.result(request)
			if err := owner.authorizeReconciliationEffect(t.Context(), authorizeReconciliationEffectCommand{Identity: finishIdentity, Action: request.Action}); err != nil {
				t.Fatalf("authorize effect: %v", err)
			}
			if request.Action == reconciliationReviewer && request.Reviewer.Phase == "run-observe" {
				if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: finishIdentity}); err != nil {
					t.Fatal(err)
				}
				if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: finishIdentity, GroupPID: 99999999}); err != nil {
					t.Fatal(err)
				}
				sealTestReviewerResult(t, owner, *first, result)
			}
			completed, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: finishIdentity, Result: result})
			if err != nil {
				t.Fatal(err)
			}
			verifyReconciliationEffectOutcome(t, completed.State, request, result)
			replayedCompletion, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: finishIdentity, Result: result})
			if err != nil || replayedCompletion.State.Revision != completed.State.Revision {
				t.Fatalf("completion replay revisions got=%d want=%d err=%v", replayedCompletion.State.Revision, completed.State.Revision, err)
			}
			invalid := reconciliationEffectResult{Action: request.Action}
			if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: finishIdentity, Result: invalid}); !errors.Is(err, errStateConflict) {
				t.Fatalf("conflicting completion err=%v", err)
			}
		})
	}
}

func TestPlanReviewRunningTransitionRequiresExactPendingEffect(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	beforeManifest := snapshot.State.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].Manifest
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	running, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999})
	if err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if got := running.State.Attempts[key].Manifest; !reflect.DeepEqual(got, beforeManifest) || !running.State.Effects[effect.ID].ReviewerLaunched || running.State.Effects[effect.ID].ReviewerGroupPID != 99999999 {
		t.Fatalf("launch changed committed manifest or was not recorded on effect: manifest=%#v effect=%#v", got, running.State.Effects[effect.ID])
	}
	projected, err := projectOwnerStatus(running, 1, time.Unix(1, 0))
	if err != nil || len(projected.Statuses) != 1 || !slices.ContainsFunc(projected.Statuses[0].Sessions, func(session orchestrator.AttemptSession) bool {
		return session.Role == agentruntime.SessionRoleReviewer && session.Name == request.Reviewer.Session && session.State == "running" && session.Current
	}) {
		t.Fatalf("launched effect was not projected as current reviewer: status=%#v err=%v", projected.Statuses, err)
	}
	again, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999})
	if err != nil || again.State.Revision != running.State.Revision {
		t.Fatalf("idempotent running transition revision=%d want=%d err=%v", again.State.Revision, running.State.Revision, err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999998}); !errors.Is(err, errStateConflict) {
		t.Fatalf("different process group replaced committed reviewer binding: %v", err)
	}
	result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(request)
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: result}); !errors.Is(err, errStateConflict) {
		t.Fatalf("launched reviewer finished without owner process-death proof: %v", err)
	}
	if _, err := owner.proveReviewerDead(t.Context(), proveReviewerDeadCommand{Identity: identity, GroupPID: 99999999}); !errors.Is(err, errStateConflict) {
		t.Fatalf("group absence falsely proved descendant death: %v", err)
	}
	launchPath, terminalPath := reviewerLifecyclePaths(request.Reviewer.Snapshot, request.Reviewer.Target)
	pane, err := parseReviewerPaneIdentity(reviewerPaneTestOutput("1|0|||", request.Reviewer.Session, "$9", os.Getpid(), "agent-symphony review-pane tmux "+launchPath+" "+terminalPath+" "+reviewerSignal(reviewerIdentity(identity))+" "+identity.RequestDigest))
	if err != nil {
		t.Fatal(err)
	}
	terminalIdentity := reviewerIdentity(identity)
	terminalIdentity.GateProtocol, terminalIdentity.SessionRequested, terminalIdentity.ChildPID = true, true, 99999999
	if _, err := owner.sealReviewerResult(t.Context(), sealReviewerResultCommand{Identity: identity, Result: result, Pane: pane, Terminal: reviewerTerminalRecord{Identity: terminalIdentity}}); err != nil {
		t.Fatalf("seal exact business result: %v", err)
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: result}); err != nil {
		t.Fatalf("exact running effect did not finish: %v", err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("completed effect could restart reviewer: %v", err)
	}
}

func TestSealedReviewerResultReplaysAfterRestartWithoutDeathProof(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(request)
	sealTestReviewerResult(t, owner, *effect, result)
	if err := writeReconciliationEffectMarker(root, identity, request, result); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, request.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, loaded, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, owner.attemptRoot, state)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted}
	if got, err := coordinator.verifyPendingReconciliation(t.Context(), loaded.Effects[effect.ID]); err != nil || got == nil || !reflect.DeepEqual(*got, result) {
		t.Fatalf("sealed reviewer marker did not replay: result=%#v err=%v", got, err)
	}
	current := mustOwnerSnapshot(t, restarted).State
	proof := current.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)]
	if proof.DeadProved || current.Effects[effect.ID].State != "completed" {
		t.Fatalf("replay falsely certified physical death or lost business result: proof=%#v effect=%#v", proof, current.Effects[effect.ID])
	}
}

func TestGatedReviewerNeverRanProofRequiresNoSessionRequest(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.proveReviewerDead(t.Context(), proveReviewerDeadCommand{Identity: identity, NeverRan: true}); !errors.Is(err, errStateConflict) {
		t.Fatalf("requested session was falsely certified never launched: %v", err)
	}
	current := mustOwnerSnapshot(t, owner)
	proofKey := reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)
	if _, exists := current.State.ReviewerProofs[proofKey]; exists || !current.State.Effects[effect.ID].ReviewerSessionRequested {
		t.Fatalf("rejected no-run proof altered durable owner state: effect=%#v proof=%#v", current.State.Effects[effect.ID], current.State.ReviewerProofs[proofKey])
	}
}

func TestPlanReviewRunningTransitionCannotRestoreInvalidatedAttempt(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*effect)}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: ownerReconciliationEffectIdentity(*effect), GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, ExpectedIssueGeneration: effect.IssueGeneration, ExpectedAttemptGeneration: effect.AttemptGeneration, Action: "dismissed", CleanupPhase: "completed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: ownerReconciliationEffectIdentity(*effect)}); !errors.Is(err, errStaleStateResult) && !errors.Is(err, errAttemptTombstoned) {
		t.Fatalf("invalidated reviewer was restored: %v", err)
	}
	state, err := owner.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := state.State.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]; exists {
		t.Fatal("dismissed attempt was restored by stale reviewer launch")
	}
	proof := state.State.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)]
	if proof.EffectID != effect.ID || proof.DeadProved || proof.GroupPID != 99999999 {
		t.Fatalf("hide-only dismissal invented a process-death certificate or lost exact reviewer binding: %#v", proof)
	}
	result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(request)
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*effect), Result: result}); err == nil {
		t.Fatal("stale reviewer result resurrected a dismissed attempt")
	}
}

func TestIssueGenerationCannotDiscardUnprovedReviewerProcess(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*effect)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	command := advanceIssueGenerationCommand{Repository: request.Repository, Issue: request.Issue, ExpectedGeneration: effect.IssueGeneration}
	if _, err := owner.advanceIssueGeneration(t.Context(), command); !errors.Is(err, errStateConflict) {
		t.Fatalf("issue generation discarded a live reviewer: %v", err)
	}
	current, err := owner.snapshot(t.Context())
	if err != nil || current.State.Effects[effect.ID].State != "pending" || current.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)] != effect.IssueGeneration {
		t.Fatalf("rejected generation change altered durable review: state=%#v err=%v", current.State, err)
	}
	if _, err := owner.proveReviewerDead(t.Context(), proveReviewerDeadCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.advanceIssueGeneration(t.Context(), command); err != nil {
		t.Fatalf("proved-dead reviewer blocked generation change: %v", err)
	}
}

func TestPlanReviewerFailureReceiptAndManifestCommitTogether(t *testing.T) {
	for _, failPersistence := range []bool{false, true} {
		t.Run(fmt.Sprintf("persist-failure-%v", failPersistence), func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
			owner, snapshot, request := reconciliationEffectTestOwner(t, request)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			state, err := owner.snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			initial := state.State
			initial.ControlReceipts = append(initial.ControlReceipts, controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "review-failure-receipt", Repository: request.Repository, Action: "review-plan", Issue: request.Issue, Attempt: request.Attempt}, State: "pending", Phase: operatorPhaseReviewPending, EffectID: effect.ID})
			if err := applyProveReviewerDead(&initial, proveReviewerDeadCommand{Identity: ownerReconciliationEffectIdentity(*effect), NeverRan: true}); err != nil {
				t.Fatal(err)
			}
			var failNow atomic.Bool
			persist := func(runtimeOwnerState) error {
				if failNow.Load() {
					return errors.New("injected persistence failure")
				}
				return nil
			}
			restarted, err := startTestStateOwner(t, owner.stateRoot, initial, persist)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = restarted.close(context.Background()) })
			failNow.Store(failPersistence)
			result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(request)
			result.Reviewer.Status, result.Reviewer.Diagnostic = "failed", "reviewer result artifact was invalid"
			_, finishErr := restarted.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*effect), Result: result})
			if failPersistence && finishErr == nil || !failPersistence && finishErr != nil {
				t.Fatalf("finish err=%v, injected failure=%v", finishErr, failPersistence)
			}
			current, err := restarted.snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			manifest := current.State.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].Manifest
			receipt := current.State.ControlReceipts[0]
			if failPersistence {
				if manifest.ReviewState != "" || receipt.State != "pending" || current.State.Effects[effect.ID].State != "pending" {
					t.Fatalf("failed persistence partially applied reviewer: manifest=%#v receipt=%#v", manifest, receipt)
				}
				return
			}
			if manifest.ReviewState != "failed" || manifest.ReviewDiagnostic != result.Reviewer.Diagnostic || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusInternalServerError || current.State.Effects[effect.ID].State != "completed" {
				t.Fatalf("review failure not durably terminal: manifest=%#v receipt=%#v", manifest, receipt)
			}
		})
	}
}

func TestPlanReviewSupersessionRequiresFreshOwnerInvalidation(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*runtimeOwnerState, string)
	}{
		{"body changed", func(state *runtimeOwnerState, issueKey string) {
			observation := state.Observations[issueKey]
			observation.Generation++
			observation.Fact.BodyDigest = strings.Repeat("e", 64)
			state.Observations[issueKey] = observation
		}},
		{"dispatch revoked", func(state *runtimeOwnerState, issueKey string) {
			observation := state.Observations[issueKey]
			observation.Generation++
			observation.Fact.DispatchAuthorized = false
			state.Observations[issueKey] = observation
		}},
		{"new attempt", func(state *runtimeOwnerState, issueKey string) {
			observation := state.Observations[issueKey]
			observation.Generation++
			observation.Fact.CurrentAttempt++
			state.Observations[issueKey] = observation
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
			owner, snapshot, request := reconciliationEffectTestOwner(t, request)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			current, err := owner.snapshot(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			state := current.State
			state.ControlReceipts = append(state.ControlReceipts, controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "review-superseded", Repository: request.Repository, Action: "review-plan", Issue: request.Issue, Attempt: request.Attempt}, State: "pending", Phase: operatorPhaseReviewPending, EffectID: effect.ID})
			identity := ownerReconciliationEffectIdentity(*effect)
			if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); !errors.Is(err, errStateConflict) {
				t.Fatalf("valid current review was superseded: %v", err)
			}
			if err := applyMarkReviewerSessionRequested(owner.stateRoot, &state, markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
				t.Fatal(err)
			}
			if err := applyMarkPlanReviewRunning(owner.stateRoot, &state, markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999}); err != nil {
				t.Fatal(err)
			}
			issueKey := ownerIssueKey(request.Repository, request.Issue)
			test.change(&state, issueKey)
			if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); !errors.Is(err, errStateConflict) {
				t.Fatalf("launched reviewer was superseded before process death proof: %v", err)
			}
			if err := applyProveReviewerDead(&state, proveReviewerDeadCommand{Identity: identity, GroupPID: 99999999}); err != nil {
				t.Fatal(err)
			}
			if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); err != nil {
				t.Fatal(err)
			}
			receipt := state.ControlReceipts[0]
			manifest := state.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].Manifest
			if receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusConflict || receipt.EffectID != effect.ID || manifest.ReviewState != "failed" || manifest.ReviewDiagnostic == "" {
				t.Fatalf("invalidation was not terminal: receipt=%#v manifest=%#v", receipt, manifest)
			}
			if state.Effects[effect.ID].State != "completed" {
				t.Fatal("stale reviewer effect remained pending")
			}
		})
	}
}

func TestImplementationReviewerCompatibleObservationDriftFinishes(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	request.Reviewer.Mode, request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	identity := ownerReconciliationEffectIdentity(*effect)
	if err := applyMarkReviewerSessionRequested(owner.stateRoot, &state, markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if err := applyMarkPlanReviewRunning(owner.stateRoot, &state, markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	if err := applyProveReviewerDead(&state, proveReviewerDeadCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	observation := state.Observations[issueKey]
	observation.Generation++
	observation.Fact.NeedsAttention = !observation.Fact.NeedsAttention
	state.Observations[issueKey] = observation
	result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(request)
	if err := applyFinishReconciliationEffect(owner.stateRoot, &state, finishReconciliationEffectCommand{Identity: identity, Result: result}); err != nil {
		t.Fatalf("compatible status-only drift stranded implementation review: %v", err)
	}
	if state.Effects[effect.ID].State != "completed" {
		t.Fatal("implementation review remained pending after compatible observation")
	}
}

func TestImplementationReviewerInFlightDriftCancelsOnlyChangedTarget(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	request.Reviewer.Mode, request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	observation := state.Observations[issueKey]
	observation.Generation++
	observation.Fact.NeedsAttention = !observation.Fact.NeedsAttention
	state.Observations[issueKey] = observation
	identity := ownerReconciliationEffectIdentity(*effect)
	if err := applyAuthorizeReconciliationEffect(owner.stateRoot, &state, authorizeReconciliationEffectCommand{Identity: identity, Action: reconciliationReviewer}); err != nil {
		t.Fatalf("compatible drift prevented first reviewer launch: %v", err)
	}
	if err := applyMarkReviewerSessionRequested(owner.stateRoot, &state, markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatalf("compatible drift prevented durable launch request: %v", err)
	}
	runCtx, cancel := context.WithCancel(t.Context())
	defer cancel()
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	coordinator := runtimeEffectCoordinator{owner: owner, active: map[string]*activeRuntimeEffect{key: {issueGeneration: effect.IssueGeneration, attemptGeneration: effect.AttemptGeneration, effectID: effect.ID, ctx: runCtx, cancel: cancel}}}
	coordinator.cancelInvalidated(stateOwnerSnapshot{State: state})
	if runCtx.Err() != nil {
		t.Fatal("status-only observation canceled active implementation reviewer")
	}
	observation.Fact.BodyDigest = strings.Repeat("e", 64)
	observation.Generation++
	state.Observations[issueKey] = observation
	coordinator.cancelInvalidated(stateOwnerSnapshot{State: state})
	if runCtx.Err() == nil {
		t.Fatal("changed reviewer body did not cancel active run")
	}
}

func TestImplementationReviewerLocalHeadPrecedesRemotePublication(t *testing.T) {
	for _, remote := range []string{"active without head", "attempt fact absent", "contradictory head"} {
		t.Run(remote, func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
			request.Reviewer.Mode, request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
			owner, snapshot, request := reconciliationEffectTestOwner(t, request)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			state := mustOwnerSnapshot(t, owner).State
			issueKey := ownerIssueKey(request.Repository, request.Issue)
			observation := state.Observations[issueKey]
			observation.Generation++
			observation.Attempts = nil
			observation.Fact.TerminalAttempts = nil
			switch remote {
			case "active without head":
				observation.Fact.ActiveAttempt = &reconciliationAttemptFact{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, BaseSHA: request.Reviewer.BaseSHA, State: "active"}
			case "attempt fact absent":
				observation.Fact.ActiveAttempt = nil
			case "contradictory head":
				observation.Fact.ActiveAttempt = &reconciliationAttemptFact{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, BaseSHA: request.Reviewer.BaseSHA, HeadSHA: strings.Repeat("f", 40), State: "active"}
			}
			state.Observations[issueKey] = observation
			err = reconciliationEffectFinishCurrent(owner.stateRoot, state, *effect)
			if remote == "contradictory head" && !errors.Is(err, errStaleStateResult) || remote != "contradictory head" && err != nil {
				t.Fatalf("remote %s: compatible local export head was rejected or contradictory head accepted: %v", remote, err)
			}
		})
	}
}

func TestImplementationReviewerChangedBodyRequiresDeadProofBeforeSupersession(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	request.Reviewer.Mode, request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	identity := ownerReconciliationEffectIdentity(*effect)
	if err := applyMarkReviewerSessionRequested(owner.stateRoot, &state, markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if err := applyMarkPlanReviewRunning(owner.stateRoot, &state, markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	observation := state.Observations[issueKey]
	observation.Generation++
	observation.Fact.BodyDigest = digestText("changed body")
	state.Observations[issueKey] = observation
	if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); !errors.Is(err, errStateConflict) {
		t.Fatalf("live implementation reviewer superseded without death proof: %v", err)
	}
	if err := applyProveReviewerDead(&state, proveReviewerDeadCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); err != nil {
		t.Fatalf("proved stopped implementation reviewer stayed pending: %v", err)
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if state.Effects[effect.ID].State != "completed" || state.Attempts[key].Manifest.ReviewState != "failed" || !state.Attempts[key].Manifest.ReviewInvalidated {
		t.Fatalf("implementation supersession did not produce terminal truth: effect=%#v", state.Effects[effect.ID])
	}
	// Certified cleanup detaches the old root before the same local head may be reviewed again.
	record := state.Attempts[key]
	record.Manifest.ReviewSnapshot, record.Manifest.ReviewSession = "", ""
	state.Attempts[key] = record
	issue := issueFact(request.Issue, "title")
	issue.Attempt, issue.Body = request.Attempt, "changed body"
	plans, _, err := planReconciliationReviewers(stateOwnerSnapshot{State: state}, owner.stateRoot, []reviewerExecutionMaterial{{Issue: issue, HeadSHA: request.Reviewer.HeadSHA}})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.HeadSHA != request.Reviewer.HeadSHA {
		t.Fatalf("same-head review did not replan after certified invalidation cleanup: plans=%#v err=%v", plans, err)
	}
}

func TestImplementationReviewerChangedLocalExportHeadSupersedesAfterDeath(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	request.Reviewer.Mode, request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	identity := ownerReconciliationEffectIdentity(*effect)
	if err := applyMarkReviewerSessionRequested(owner.stateRoot, &state, markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if err := applyMarkPlanReviewRunning(owner.stateRoot, &state, markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	newHead := strings.Repeat("f", 40)
	if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity, CurrentHeadSHA: newHead}); !errors.Is(err, errStateConflict) {
		t.Fatalf("unproved H1 reviewer superseded for H2: %v", err)
	}
	if err := applyProveReviewerDead(&state, proveReviewerDeadCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity, CurrentHeadSHA: newHead}); err != nil {
		t.Fatalf("proved H1 reviewer did not supersede for H2: %v", err)
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if state.Effects[effect.ID].State != "completed" || !state.Attempts[key].Manifest.ReviewInvalidated {
		t.Fatal("H1 effect was not terminally invalidated")
	}
	if err := writeRuntimeOwnerState(owner.stateRoot, owner.attemptRoot, state); err != nil {
		t.Fatal(err)
	}
	state, err = readRuntimeOwnerState(owner.stateRoot, request.Repository)
	if err != nil || !state.Attempts[key].Manifest.ReviewInvalidated || !state.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)].DeadProved {
		t.Fatalf("restart lost exact H1 invalidation/death proof: err=%v", err)
	}
	record := state.Attempts[key]
	record.Manifest.ReviewSnapshot, record.Manifest.ReviewSession = "", ""
	state.Attempts[key] = record
	issue := issueFact(request.Issue, "title")
	issue.Attempt = request.Attempt
	plans, _, err := planReconciliationReviewers(stateOwnerSnapshot{State: state}, owner.stateRoot, []reviewerExecutionMaterial{{Issue: issue, HeadSHA: newHead}})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.HeadSHA != newHead {
		t.Fatalf("H2 did not replan after H1 invalidation cleanup: plans=%#v err=%v", plans, err)
	}
}

func TestPrebindingPlanReviewSupersessionRequiresExactStoppedProof(t *testing.T) {
	for _, test := range []struct {
		name        string
		observation reviewerStopObservation
	}{
		{name: "default shell never ran", observation: reviewerStopObservation{NeverRan: true}},
		{name: "child launched before owner binding", observation: reviewerStopObservation{GroupPID: 99999999}},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
			owner, snapshot, request := reconciliationEffectTestOwner(t, request)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			state := mustOwnerSnapshot(t, owner).State
			state.ControlReceipts = append(state.ControlReceipts, controlReceipt{Request: controlRequest{Version: controlVersion, RequestID: "prebind-supersede", Repository: request.Repository, Action: "review-plan", Issue: request.Issue, Attempt: request.Attempt}, State: "pending", EffectID: effect.ID})
			issueKey := ownerIssueKey(request.Repository, request.Issue)
			observation := state.Observations[issueKey]
			observation.Generation++
			observation.Fact.BodyDigest = strings.Repeat("e", 64)
			state.Observations[issueKey] = observation
			identity := ownerReconciliationEffectIdentity(*effect)
			if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); !errors.Is(err, errStateConflict) {
				t.Fatalf("prebind review superseded without child-death or NeverRan proof: %v", err)
			}
			if err := applyProveReviewerDead(&state, proveReviewerDeadCommand{Identity: identity, GroupPID: test.observation.GroupPID, NeverRan: test.observation.NeverRan}); err != nil {
				t.Fatal(err)
			}
			if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); err != nil {
				t.Fatalf("proved stopped prebind review did not supersede: %v", err)
			}
			proof := state.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, request.Reviewer.Mode, request.Reviewer.Target)]
			if !proof.DeadProved || proof.NeverRan != test.observation.NeverRan || proof.GroupPID != test.observation.GroupPID || state.ControlReceipts[0].Result == nil || state.ControlReceipts[0].Result.Status != http.StatusConflict {
				t.Fatalf("supersession lost exact prebind process proof or receipt: proof=%#v receipt=%#v", proof, state.ControlReceipts[0])
			}
		})
	}
}

func verifyReconciliationEffectOutcome(t *testing.T, state runtimeOwnerState, request reconciliationEffectRequest, result reconciliationEffectResult) {
	t.Helper()
	if request.Attempt == 0 {
		return
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	manifest := state.Attempts[key].Manifest
	switch request.Action {
	case reconciliationGitHubPublish:
		recovery, ok := state.Recoveries[key]
		if !ok || recovery.State.Number != result.GitHubPublish.PR || recovery.State.HeadSHA != result.GitHubPublish.HeadSHA || recovery.State.PreparedPublication != nil {
			t.Fatalf("publication recovery=%#v", recovery)
		}
	case reconciliationReviewer:
		if request.Reviewer.Phase == "cleanup" {
			if manifest.ReviewSnapshot != "" || manifest.ReviewSession != "" {
				t.Fatalf("cleanup retained reviewer resources: %#v", manifest)
			}
		} else if manifest.ReviewState != result.Reviewer.Status || manifest.ReviewSnapshot != request.Reviewer.Snapshot || manifest.ReviewSession != request.Reviewer.Session {
			t.Fatalf("review result was not applied: %#v", manifest)
		}
	case reconciliationHandoffDeliver:
		if request.Handoff.Kind == "review-findings" {
			if !manifest.ReviewHandoffQueued || !manifest.ReviewHandoffAck || manifest.State != "running" {
				t.Fatalf("review handoff was not applied: %#v", manifest)
			}
			return
		}
		recovery := state.Recoveries[key].State
		if request.Handoff.Outcome == nil && !recovery.HandoffReceipts[request.Handoff.Key] {
			t.Fatalf("recovery handoff receipt missing: %#v", recovery)
		}
		if request.Handoff.Outcome != nil && (recovery.ValidationInFlightSHA != "" || recovery.ValidationResult != request.Handoff.Outcome.ValidationResult || recovery.ValidationEvidence != request.Handoff.Outcome.ValidationEvidence) {
			t.Fatalf("recovery outcome was not applied: %#v", recovery)
		}
	}
}

func TestReconciliationEffectClonesAndLoadsTypedPayload(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-findings").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	identity := reconciliationBeginIdentity(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: identity, Request: request})
	if err != nil {
		t.Fatal(err)
	}
	request.GitHubIssueUpdate.Findings[0] = "mutated"
	stored, _ := owner.snapshot(t.Context())
	if got := stored.State.Effects[effect.ID].Reconciliation.GitHubIssueUpdate.Findings[0]; got != "finding" {
		t.Fatalf("request aliases caller: %q", got)
	}
	result := reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueFindings, Observed: true}}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: result}); err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, "o/r")
	if err != nil || loaded.Effects[effect.ID].ReconciliationResult.GitHubIssueUpdate.Kind != githubIssueFindings {
		t.Fatalf("loaded effect=%#v err=%v", loaded.Effects[effect.ID], err)
	}
}

func TestReconciliationEffectSurvivesIdenticalCycleAndRejectsChangedObservation(t *testing.T) {
	test := reconciliationEffectCases(t)[1]
	owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identical := reconciliationEffectObservationInput(request, "title")
	applyReconciliationInput(t, owner, identical)
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: test.result(request)}); err != nil {
		t.Fatalf("identical later cycle invalidated effect: %v", err)
	}

	owner, snapshot, request = reconciliationEffectTestOwner(t, test.request)
	_, effect, err = owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	changed := reconciliationEffectObservationInput(request, "changed")
	applyReconciliationInput(t, owner, changed)
	state, _ := owner.snapshot(t.Context())
	retained, exists := state.State.Effects[effect.ID]
	if !exists || retained.State != "pending" {
		t.Fatalf("changed observation lost ambiguous pending effect: %#v", retained)
	}
	production := &productionReconciliation{owner: owner, effects: &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}}
	if resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{}); err != nil || resumed {
		t.Fatalf("changed observation recovery=%v err=%v", resumed, err)
	}
	retained = mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if retained.State != "pending" || retained.Diagnostic == "" {
		t.Fatalf("ambiguous changed-observation effect was not retained and diagnosed: %#v", retained)
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: test.result(request)}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("changed observation finish err=%v", err)
	}
}

func TestReconciliationEffectRejectsUnauthorizedLifecycle(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*runtimeOwnerState, *reconciliationEffectRequest)
	}{
		{"github-bind", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.State = "running" })},
		{"github-publish", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewState = "findings-queued" })},
		{"github-publish-prepared", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			state.Recoveries[effectAttemptKey(*request)] = runtimePRRecovery{}
		}},
		{"issue-terminal-failure", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.State = "running" })},
		{"issue-evidence", removeEffectRemoteAttempt},
		{"issue-findings", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewHandoffAck = true })},
		{"issue-retry", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.State = "running" })},
		{"issue-control-snapshot", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
			observation.Present = false
			state.Observations[ownerIssueKey(request.Repository, request.Issue)] = observation
		}},
		{"issue-dependency-clear", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
			observation.Fact.SatisfiedDependencies = nil
			state.Observations[ownerIssueKey(request.Repository, request.Issue)] = observation
		}},
		{"github-pr-governance", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			recovery := state.Recoveries[effectAttemptKey(*request)]
			recovery.State.HeadSHA = strings.Repeat("c", 40)
			state.Recoveries[effectAttemptKey(*request)] = recovery
		}},
		{"reviewer-run-observe", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewState = "clean" })},
		{"reviewer-cleanup", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewState = "running" })},
		{"handoff-review-findings", mutateEffectManifest(func(manifest *agentruntime.Manifest) { manifest.ReviewHandoffAck = true })},
		{"handoff-recovery", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			recovery := state.Recoveries[effectAttemptKey(*request)]
			recovery.State.ValidationInFlightSHA = ""
			state.Recoveries[effectAttemptKey(*request)] = recovery
		}},
		{"handoff-recovery-outcome", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			recovery := state.Recoveries[effectAttemptKey(*request)]
			delete(recovery.State.HandoffReceipts, request.Handoff.Key)
			state.Recoveries[effectAttemptKey(*request)] = recovery
		}},
		{"retire-completed", func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
			observation := state.Observations[ownerIssueKey(request.Repository, request.Issue)]
			attempt := observation.Attempts[effectAttemptKey(*request)]
			attempt.Fact.State = "active"
			observation.Attempts[effectAttemptKey(*request)] = attempt
			if observation.Fact.ActiveAttempt != nil {
				observation.Fact.ActiveAttempt.State = "active"
			}
			observation.Fact.TerminalAttempts = nil
			state.Observations[ownerIssueKey(request.Repository, request.Issue)] = observation
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			root, owner, snapshot := reconciliationEffectPersistentOwner(t, reconciliationEffectCaseNamed(t, test.name).request)
			request := bindEffectObservation(snapshot, reconciliationEffectCaseNamed(t, test.name).request)
			request = configureEffectFixture(root, request, manifestForRequest(snapshot.State, request), strings.Repeat("b", 40))
			request = bindEffectObservation(snapshot, request)
			state := cloneRuntimeOwnerState(snapshot.State)
			test.mutate(&state, &request)
			if reconciliationObservationCurrent(state, request) && validReconciliationEffectStateBindings(root, state, request) {
				t.Fatal("unauthorized lifecycle remained admissible")
			}
			_ = owner
		})
	}
}

func TestReconciliationEffectRejectsUnobservedIssueUpdateProposal(t *testing.T) {
	for _, name := range []string{"issue-control-snapshot", "issue-dependency-clear"} {
		t.Run(name, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, name)
			owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
			if request.GitHubIssueUpdate.Kind == githubIssueControlSnapshot {
				request.GitHubIssueUpdate.ControlSnapshotDigest = strings.Repeat("d", 64)
			} else {
				request.GitHubIssueUpdate.PullRequest++
			}
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request}); !errors.Is(err, errStaleStateResult) {
				t.Fatalf("unobserved proposal err=%v", err)
			}
		})
	}
}

func effectAttemptKey(request reconciliationEffectRequest) string {
	return ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
}

func manifestForRequest(state runtimeOwnerState, request reconciliationEffectRequest) *agentruntime.Manifest {
	manifest := state.Attempts[effectAttemptKey(request)].Manifest
	return &manifest
}

func mutateEffectManifest(change func(*agentruntime.Manifest)) func(*runtimeOwnerState, *reconciliationEffectRequest) {
	return func(state *runtimeOwnerState, request *reconciliationEffectRequest) {
		key := effectAttemptKey(*request)
		record := state.Attempts[key]
		change(&record.Manifest)
		state.Attempts[key] = record
		manifest := cloneManifest(record.Manifest)
		request.Manifest = &manifest
	}
}

func removeEffectRemoteAttempt(state *runtimeOwnerState, request *reconciliationEffectRequest) {
	key := ownerIssueKey(request.Repository, request.Issue)
	observation := state.Observations[key]
	delete(observation.Attempts, effectAttemptKey(*request))
	observation.Fact.ActiveAttempt, observation.Fact.TerminalAttempts = nil, nil
	state.Observations[key] = observation
}

func TestFirstAttemptReservationInvalidatesIssueScopedEffect(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-control-snapshot").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	// Rebuild this owner without an attempt reservation: issue-scoped updates can precede it.
	root := resolvedTempDir(t)
	owner, err := startTestStateOwner(t, root, newRuntimeOwnerState("o/r"), func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	applyReconciliationInput(t, owner, reconciliationInput{Scope: issueScope(190), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issueFact(190, "title")}, IssueUpdates: []reconciliationIssueUpdateProposal{{Repository: "o/r", Issue: 190, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: strings.Repeat("c", 64)}}})
	snapshot, _ = owner.snapshot(t.Context())
	observation := snapshot.State.Observations[ownerIssueKey("o/r", 190)]
	request.Attempt, request.Manifest = 0, nil
	request.ObservationGeneration, request.ObservationCycleID, request.BodyDigest = observation.Generation, observation.LastCycleID, observation.Fact.BodyDigest
	request.GitHubIssueUpdate = &githubIssueUpdateEffectRequest{Kind: githubIssueControlSnapshot, ControlSnapshotDigest: strings.Repeat("c", 64)}
	request.Action, request.GitHubPRGovernance = reconciliationGitHubIssueUpdate, nil
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey("o/r", 190)]}, Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Result: reconciliationEffectResult{Action: reconciliationGitHubIssueUpdate, GitHubIssueUpdate: &githubIssueUpdateEffectResult{Kind: githubIssueControlSnapshot, Observed: true}}}); err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, 190, 1, "preparing")
	reserved, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest, ExpectedIssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey("o/r", 190)]})
	if _, oldEffectSurvived := reserved.State.Effects[effect.ID]; err != nil || oldEffectSurvived || reserved.State.IssueGenerations[ownerIssueKey("o/r", 190)] != 2 {
		t.Fatalf("reservation=%#v err=%v", reserved.State, err)
	}
}

func TestNewAttemptPrunesCompletedUnboundRetryFromOldIssueGeneration(t *testing.T) {
	caseData := reconciliationEffectCaseNamed(t, "issue-retry")
	owner, snapshot, request := reconciliationEffectTestOwner(t, caseData.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	result := caseData.result(request)
	identity := ownerReconciliationEffectIdentity(*effect)
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: result}); err != nil {
		t.Fatal(err)
	}
	before := mustOwnerSnapshot(t, owner)
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	manifest := ownerTestManifest(t, owner.stateRoot, request.Issue, request.Attempt+1, "preparing")
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest, ExpectedIssueGeneration: before.State.IssueGenerations[issueKey]})
	if err != nil {
		t.Fatalf("new attempt was blocked by completed old retry: %v", err)
	}
	if _, exists := created.State.Effects[effect.ID]; exists {
		t.Fatal("old unbound retry survived issue-generation advance")
	}
	if _, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: result}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("old retry result could reapply after new attempt: %v", err)
	}
}

func TestRuntimePRRecoveryClonesLoadsAndRejectsInvalidState(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 191, 1, "running")
	key := ownerAttemptKey("o/r", 191, 1)
	feedback := internalgithub.Feedback{ID: 9, Source: "issue", ActorID: 1, Body: "fix it", State: internalgithub.FeedbackPending, Execution: internalgithub.FeedbackInFlight, Authorized: true}
	pr := internalgithub.PRState{Repository: "o/r", Number: 8, Issue: 191, Attempt: 1, HeadSHA: strings.Repeat("b", 40), Facts: internalgithub.PRFacts{Feedback: []internalgithub.Feedback{feedback}}, HandoffReceipts: map[string]bool{}}
	handoff := internalgithub.RecoveryHandoff{Repository: "o/r", PR: 8, Issue: 191, Attempt: 1, HeadSHA: pr.HeadSHA, Feedback: []internalgithub.Feedback{feedback}}
	handoff.Key = recoveryHandoffKey(handoff)
	pr.HandoffReceipts[handoff.Key] = true
	state := newRuntimeOwnerState("o/r")
	state.IssueGenerations[ownerIssueKey("o/r", 191)] = 1
	state.AttemptGenerations[key] = 1
	state.Attempts[key] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	state.Recoveries[key] = runtimePRRecovery{IssueGeneration: 1, AttemptGeneration: 1, State: pr}
	owner, err := startTestStateOwner(t, root, state, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
	})
	if err != nil {
		t.Fatal(err)
	}
	pr.Facts.Feedback[0].Body = "caller mutation"
	delete(pr.HandoffReceipts, handoff.Key)
	snapshot, err := owner.snapshot(t.Context())
	if err != nil || snapshot.State.Recoveries[key].State.Facts.Feedback[0].Body != "fix it" || !snapshot.State.Recoveries[key].State.HandoffReceipts[handoff.Key] {
		t.Fatalf("recovery aliases input: %#v err=%v", snapshot.State.Recoveries[key], err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, "o/r")
	if err != nil || loaded.Recoveries[key].State.Facts.Feedback[0].Body != "fix it" || !loaded.Recoveries[key].State.HandoffReceipts[handoff.Key] {
		t.Fatalf("loaded recovery=%#v err=%v", loaded.Recoveries[key], err)
	}

	invalid := []struct {
		name   string
		mutate func(*runtimeOwnerState)
	}{
		{"stale generation", func(candidate *runtimeOwnerState) {
			recovery := candidate.Recoveries[key]
			recovery.AttemptGeneration++
			candidate.Recoveries[key] = recovery
		}},
		{"tombstoned", func(candidate *runtimeOwnerState) { candidate.Tombstones[key] = runtimeTombstone{} }},
		{"wrong key", func(candidate *runtimeOwnerState) {
			recovery := candidate.Recoveries[key]
			delete(candidate.Recoveries, key)
			candidate.Recoveries[ownerAttemptKey("o/r", 191, 2)] = recovery
		}},
		{"oversized feedback", func(candidate *runtimeOwnerState) {
			recovery := candidate.Recoveries[key]
			recovery.State.Facts.Feedback[0].Body = strings.Repeat("x", maxReconciliationBodyBytes+1)
			candidate.Recoveries[key] = recovery
		}},
		{"duplicate PR", func(candidate *runtimeOwnerState) {
			secondKey := ownerAttemptKey("o/r", 191, 2)
			second := ownerTestManifest(t, root, 191, 2, "running")
			candidate.AttemptGenerations[secondKey] = 1
			candidate.Attempts[secondKey] = runtimeAttemptRecord{Generation: 1, Manifest: second}
			recovery := candidate.Recoveries[key]
			recovery.State.Attempt = 2
			candidate.Recoveries[secondKey] = recovery
		}},
	}
	for _, test := range invalid {
		t.Run(test.name, func(t *testing.T) {
			candidate := cloneRuntimeOwnerState(loaded)
			test.mutate(&candidate)
			if validateRuntimePRRecoveries(candidate) == nil {
				t.Fatal("invalid recovery state was accepted")
			}
		})
	}
}

func TestOwnerGenerationChangesDeletePRRecovery(t *testing.T) {
	for _, operation := range []string{"issue", "attempt", "tombstone"} {
		t.Run(operation, func(t *testing.T) {
			_, owner, snapshot := reconciliationEffectPersistentOwner(t, reconciliationEffectCaseNamed(t, "github-pr-governance").request)
			key := ownerAttemptKey("o/r", 190, 1)
			if _, ok := snapshot.State.Recoveries[key]; !ok {
				t.Fatal("fixture lacks PR recovery")
			}
			var (
				result stateOwnerSnapshot
				err    error
			)
			switch operation {
			case "issue":
				result, err = owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: 190, ExpectedGeneration: 1})
			case "attempt":
				result = advanceTestAttemptGeneration(t, owner, snapshot.State.Attempts[key].Manifest)
			case "tombstone":
				result, _, err = owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: 190, Attempt: 1, ExpectedIssueGeneration: 1, ExpectedAttemptGeneration: 1, Action: "dismissed", CleanupPhase: "completed"})
			}
			if err != nil {
				t.Fatal(err)
			}
			if _, ok := result.State.Recoveries[key]; ok {
				t.Fatal("generation change retained stale PR recovery")
			}
		})
	}
}

func TestAcceptedPublishedAttemptHydratesOwnerRecovery(t *testing.T) {
	root := resolvedTempDir(t)
	state := newRuntimeOwnerState("o/r")
	manifest := ownerTestManifest(t, root, 196, 1, "running")
	issueKey, attemptKey := ownerIssueKey("o/r", 196), ownerAttemptKey("o/r", 196, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	issue := issueFact(196, "published")
	issue.Attempt, issue.CurrentAttempt = 1, 1
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 196, Attempt: 1, PR: 9, BaseSHA: manifest.BaseSHA, HeadSHA: strings.Repeat("b", 40), State: "active", PublicationConfirmed: true}
	issue.ActiveAttempt = &remote
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	accepted := applyReconciliationInput(t, owner, input)
	if got := accepted.State.Recoveries[attemptKey]; got.State.Number != 9 || got.State.HeadSHA != remote.HeadSHA || got.IssueGeneration != 1 || got.AttemptGeneration != 1 {
		t.Fatalf("accepted attempt did not hydrate owner recovery: %#v", got)
	}

	nonPublishedOwner := newReconciliationTestOwner(t)
	nonPublished := repositoryInput(true, issueFact(197, "not published"))
	nonPublished.Attempts = []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 197, Attempt: 1, PR: 10, BaseSHA: manifest.BaseSHA, HeadSHA: remote.HeadSHA, State: "active"}}
	result := applyReconciliationInput(t, nonPublishedOwner, nonPublished)
	if len(result.State.Recoveries) != 0 {
		t.Fatalf("unconfirmed attempt hydrated recovery: %#v", result.State.Recoveries)
	}
}

func TestOwnerAttemptRecoveryMutatesOnlyCurrentGovernanceEffect(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	recovery := ownerAttemptRecovery{owner: owner, identity: reconciliationIntentIdentity(*effect)}
	state, err := recovery.PullRequestState(t.Context(), "o/r", 7, request.Issue, request.Attempt, request.GitHubPRGovernance.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	feedback := internalgithub.Feedback{ID: 12, Source: "issue", ActorID: 5, Body: "fix", CreatedAt: time.Unix(10, 0), Authorized: true}
	if claimed, err := recovery.ClaimFeedback(t.Context(), state, feedback); err != nil || !claimed {
		t.Fatalf("claim=%v err=%v", claimed, err)
	}
	if claimed, err := recovery.ClaimFeedback(t.Context(), state, feedback); err != nil || claimed {
		t.Fatalf("duplicate claim=%v err=%v", claimed, err)
	}
	if err := recovery.QueueValidation(t.Context(), state); err != nil {
		t.Fatal(err)
	}
	if err := recovery.QueueValidation(t.Context(), state); err != nil {
		t.Fatalf("duplicate queue: %v", err)
	}
	current, _ := owner.snapshot(t.Context())
	got := current.State.Recoveries[ownerAttemptKey("o/r", request.Issue, request.Attempt)].State
	if got.ValidationGeneration != 1 || got.ValidationQueuedSHA != got.HeadSHA || len(got.Facts.Feedback) != 1 || got.Facts.Feedback[0].Execution != internalgithub.FeedbackClaimed {
		t.Fatalf("owner recovery mutation=%#v", got)
	}
	if _, err := (ownerAttemptRecovery{}).ClaimFeedback(t.Context(), state, feedback); err == nil {
		t.Fatal("zero-value recovery claim panicked or succeeded")
	}
	if err := (ownerAttemptRecovery{}).QueueValidation(t.Context(), state); err == nil {
		t.Fatal("zero-value recovery queue succeeded")
	}
}

func TestGovernancePhaseAdmissionIsDurableGenerationBoundAndInvalidated(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	recovery := ownerAttemptRecovery{owner: owner, identity: ownerReconciliationEffectIdentity(*effect)}
	state, err := recovery.PullRequestState(t.Context(), "o/r", request.GitHubPRGovernance.PR, request.Issue, request.Attempt, request.GitHubPRGovernance.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	state.Facts.HeadSHA = state.HeadSHA
	phase, err := internalgithub.NewGovernancePhase(state, "merge", "squash")
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.AdmitGovernancePhase(t.Context(), phase); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, request.Repository)
	stored := loaded.Effects[effect.ID].GovernancePhases
	if err != nil || len(stored) != 1 || stored[0].State != "admitted" || stored[0].Epoch != effect.IntentEpoch || stored[0].SourceRevision != effect.IntentRevision || stored[0].IssueGeneration != effect.IssueGeneration || stored[0].AttemptGeneration != effect.AttemptGeneration {
		t.Fatalf("durable governance phase=%#v err=%v", stored, err)
	}
	wrong := recovery
	wrong.identity.AttemptGeneration++
	if err := wrong.CompleteGovernancePhase(t.Context(), phase); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("wrong generation completion err=%v", err)
	}
	current := mustOwnerSnapshot(t, owner)
	manifest := *request.Manifest
	_, _, err = owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, ExpectedIssueGeneration: current.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], ExpectedAttemptGeneration: current.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)], Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest})
	if err != nil {
		t.Fatal(err)
	}
	if err := recovery.CompleteGovernancePhase(t.Context(), phase); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("invalidated phase completion err=%v", err)
	}
	invalidated := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if invalidated.State != "invalidated" || len(invalidated.GovernancePhases) != 1 || invalidated.GovernancePhases[0].State != "admitted" {
		t.Fatalf("invalidation lost admitted governance phase: %#v", invalidated)
	}
	loaded, err = readRuntimeOwnerState(root, request.Repository)
	outcome := invalidatedExternalOutcome{Action: reconciliationGitHubPRGovernance, Observed: true, PR: phase.PR, HeadSHA: phase.HeadSHA, Superseded: true}
	if err != nil || applyResolveInvalidatedReconciliationEffect(&loaded, resolveInvalidatedReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(loaded.Effects[effect.ID]), Outcome: outcome}) != nil {
		t.Fatalf("restart did not resolve terminal unmerged governance: %v", err)
	}
	if got := loaded.Tombstones[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].ExternalOutcomes[effect.ID]; got != outcome {
		t.Fatalf("governance outcome=%#v", got)
	}
}

func TestGovernancePhasePersistenceFailureDoesNotAuthorizeMutation(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	fail := false
	_, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, request, func(runtimeOwnerState) error {
		if fail {
			return errors.New("injected persistence failure")
		}
		return nil
	})
	request = bindEffectObservation(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	recovery := ownerAttemptRecovery{owner: owner, identity: ownerReconciliationEffectIdentity(*effect)}
	state, err := recovery.PullRequestState(t.Context(), "o/r", request.GitHubPRGovernance.PR, request.Issue, request.Attempt, request.GitHubPRGovernance.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	state.Facts.HeadSHA = state.HeadSHA
	phase, _ := internalgithub.NewGovernancePhase(state, "policy-status", `{"CheckStatus":"completed","CheckConclusion":"success"}`)
	fail = true
	if err := recovery.AdmitGovernancePhase(t.Context(), phase); err == nil {
		t.Fatal("phase admission survived failed persistence")
	}
	if got := mustOwnerSnapshot(t, owner).State.Effects[effect.ID].GovernancePhases; len(got) != 0 {
		t.Fatalf("failed phase admission leaked into owner: %#v", got)
	}
}

func TestInvalidatedGovernanceRequiresExactMergeProof(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	effect := runtimeEffectIntent{Reconciliation: &request}
	for _, test := range []struct {
		name    string
		outcome invalidatedExternalOutcome
		valid   bool
	}{
		{"exact merged head", invalidatedExternalOutcome{Action: reconciliationGitHubPRGovernance, Observed: true, Merged: true, PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA}, true},
		{"wrong merged head", invalidatedExternalOutcome{Action: reconciliationGitHubPRGovernance, Observed: true, Merged: true, PR: request.GitHubPRGovernance.PR, HeadSHA: strings.Repeat("c", 40)}, false},
		{"weak merged observation", invalidatedExternalOutcome{Action: reconciliationGitHubPRGovernance, Merged: true, PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA}, false},
		{"terminal unmerged", invalidatedExternalOutcome{Action: reconciliationGitHubPRGovernance, Observed: true, Superseded: true, PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA}, true},
		{"ambiguous unmerged", invalidatedExternalOutcome{Action: reconciliationGitHubPRGovernance, Observed: true, PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := validInvalidatedExternalOutcome(effect, test.outcome); got != test.valid {
				t.Fatalf("valid=%v want %v", got, test.valid)
			}
		})
	}
}

func TestEveryGovernancePhaseSurvivesRestart(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	recovery := ownerAttemptRecovery{owner: owner, identity: ownerReconciliationEffectIdentity(*effect)}
	state, err := recovery.PullRequestState(t.Context(), "o/r", request.GitHubPRGovernance.PR, request.Issue, request.Attempt, request.GitHubPRGovernance.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	state.Facts.HeadSHA = state.HeadSHA
	kinds := []string{"review-label", "decision-comment", "feedback-disposition-comment", "feedback-delegation", "validation-queue", "policy-status", "policy-failure-comment", "merge-prepared-comment", "merge-dispatched-comment", "merge-resolved-comment", "merge"}
	for _, kind := range kinds {
		phase, phaseErr := internalgithub.NewGovernancePhase(state, kind, "payload:"+kind)
		if phaseErr != nil {
			t.Fatal(phaseErr)
		}
		if err := recovery.AdmitGovernancePhase(t.Context(), phase); err != nil {
			t.Fatalf("admit %s: %v", kind, err)
		}
	}
	loaded, err := readRuntimeOwnerState(root, request.Repository)
	phases := loaded.Effects[effect.ID].GovernancePhases
	if err != nil || len(phases) != len(kinds) {
		t.Fatalf("restart phases=%#v err=%v", phases, err)
	}
	for i, phase := range phases {
		if phase.Kind != kinds[i] || phase.State != "admitted" || !phase.Valid() {
			t.Fatalf("phase %d=%#v", i, phase)
		}
	}
}

func TestAttemptInvalidationRevokesIssueScopedDependencyClear(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-dependency-clear").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	invalidated, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: request.Issue, Attempt: request.GitHubIssueUpdate.AttributionAttempt, ExpectedIssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey("o/r", request.Issue)], ExpectedAttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey("o/r", request.Issue, request.GitHubIssueUpdate.AttributionAttempt)], Action: "dismissed", CleanupPhase: "completed"})
	if err != nil {
		t.Fatal(err)
	}
	observation := invalidated.State.Observations[ownerIssueKey("o/r", request.Issue)]
	if slices.ContainsFunc(observation.IssueUpdates, func(proposal reconciliationIssueUpdateProposal) bool {
		return proposal.Kind == githubIssueDependencyClear
	}) {
		t.Fatalf("dependency proposal survived attempt invalidation: %#v", observation.IssueUpdates)
	}
	if _, exists := invalidated.State.Effects[effect.ID]; exists {
		t.Fatalf("dependency effect survived attempt invalidation: %#v", invalidated.State.Effects[effect.ID])
	}
	if err := owner.authorizeReconciliationEffect(t.Context(), authorizeReconciliationEffectCommand{Identity: reconciliationIntentIdentity(*effect), Action: request.Action}); !errors.Is(err, errStaleStateResult) {
		t.Fatalf("invalidated dependency effect authorization err=%v", err)
	}
}

func TestProvenRetryCompletionStillRejectsDestructiveInvalidation(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-retry").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	current := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	attemptKey := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	observation := current.Observations[issueKey]
	observation.Generation++
	observation.Fact.Retry, observation.Fact.RecoveryAuthorized = true, true
	current.Observations[issueKey] = observation
	for _, scenario := range []struct {
		name   string
		change func(*runtimeOwnerState)
	}{
		{"issue generation", func(state *runtimeOwnerState) { state.IssueGenerations[issueKey]++ }},
		{"attempt generation", func(state *runtimeOwnerState) { state.AttemptGenerations[attemptKey]++ }},
		{"tombstone", func(state *runtimeOwnerState) { state.Tombstones[attemptKey] = runtimeTombstone{} }},
		{"newer attempt", func(state *runtimeOwnerState) {
			observed := state.Observations[issueKey]
			observed.Fact.CurrentAttempt = request.Attempt + 1
			state.Observations[issueKey] = observed
		}},
		{"cancelled control", func(state *runtimeOwnerState) {
			observed := state.Observations[issueKey]
			observed.Fact.Cancelled = true
			state.Observations[issueKey] = observed
		}},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			invalid := cloneRuntimeOwnerState(current)
			scenario.change(&invalid)
			if err := reconciliationEffectFinishCurrent(owner.stateRoot, invalid, *effect); err == nil {
				t.Fatal("invalidated retry completion was accepted")
			}
		})
	}
}

func TestStaleCollectionCannotRestoreInvalidatedDependencyClear(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-dependency-clear").request
	owner, _, request := reconciliationEffectTestOwner(t, request)
	before, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	stale := mustCollection(t, before, reconciliationEffectObservationInput(request, "title"))
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{
		Repository:                request.Repository,
		Issue:                     request.Issue,
		Attempt:                   request.GitHubIssueUpdate.AttributionAttempt,
		ExpectedIssueGeneration:   before.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)],
		ExpectedAttemptGeneration: before.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.GitHubIssueUpdate.AttributionAttempt)],
		Action:                    "dismissed",
		CleanupPhase:              "completed",
	}); err != nil {
		t.Fatal(err)
	}
	applied := applyCollection(t, owner, stale)
	observation := applied.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	if slices.ContainsFunc(observation.IssueUpdates, func(proposal reconciliationIssueUpdateProposal) bool {
		return proposal.Kind == githubIssueDependencyClear
	}) {
		t.Fatalf("stale collection restored invalidated proposal: %#v", observation.IssueUpdates)
	}
	request = bindEffectObservation(applied, request)
	if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(applied, request), Request: request}); err == nil {
		t.Fatal("effect admission accepted an invalidated dependency proposal")
	}
}

type reconciliationEffectCase struct {
	name    string
	request reconciliationEffectRequest
	result  func(reconciliationEffectRequest) reconciliationEffectResult
}

func reconciliationEffectCaseNamed(t *testing.T, name string) reconciliationEffectCase {
	t.Helper()
	for _, test := range reconciliationEffectCases(t) {
		if test.name == name {
			return test
		}
	}
	t.Fatalf("missing reconciliation effect case %q", name)
	return reconciliationEffectCase{}
}

func reconciliationEffectCases(t *testing.T) []reconciliationEffectCase {
	t.Helper()
	base := strings.Repeat("a", 40)
	digest := strings.Repeat("a", 64)
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 190, Attempt: 1, BaseSHA: base, Branch: "agent/190-1", State: "running", ReviewHead: base}
	common := func(action reconciliationEffectAction) reconciliationEffectRequest {
		copy := manifest
		return reconciliationEffectRequest{Action: action, Repository: "o/r", Issue: 190, Attempt: 1, Manifest: &copy, ExecutionDigest: digest}
	}
	observed := func(action reconciliationEffectAction, value any) func(reconciliationEffectRequest) reconciliationEffectResult {
		return func(request reconciliationEffectRequest) reconciliationEffectResult {
			result := reconciliationEffectResult{Action: action}
			switch action {
			case reconciliationGitHubBind:
				result.GitHubBind = &githubBindEffectResult{Observed: true}
			case reconciliationGitHubIssueUpdate:
				result.GitHubIssueUpdate = &githubIssueUpdateEffectResult{Kind: request.GitHubIssueUpdate.Kind, Observed: true}
			case reconciliationGitHubPRGovernance:
				result.GitHubPRGovernance = &githubGovernanceEffectResult{PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA, Observed: true}
			case reconciliationHandoffDeliver:
				result.Handoff = &handoffEffectResult{Kind: request.Handoff.Kind, Key: request.Handoff.Key, OutcomePath: request.Handoff.OutcomePath, OutcomeToken: request.Handoff.OutcomeToken, Observed: true}
			case reconciliationRetireCompleted:
				result.Retire = &retireCompletedEffectResult{ResourcesGone: true}
			case reconciliationMonitoringCheckIn:
				result.CheckIn = &monitoringCheckInEffectResult{Session: request.CheckIn.Session, Observed: true}
			}
			_ = value
			return result
		}
	}
	bind := common(reconciliationGitHubBind)
	bind.GitHubBind = &githubBindEffectRequest{BaseSHA: base, Branch: manifest.Branch, Detail: "reserved"}
	publish := common(reconciliationGitHubPublish)
	publish.GitHubPublish = &githubPublishEffectRequest{Title: "title", BaseBranch: "main", HeadSHA: base, Validation: "tests", Documentation: "none"}
	publishPrepared := cloneReconciliationRequest(publish)
	publishPrepared.GitHubPublish.Prepared = &internalgithub.PreparedPublication{Handoff: internalgithub.RecoveryHandoff{Repository: "o/r", PR: 7, Issue: 190, Attempt: 1, HeadSHA: strings.Repeat("b", 40)}, HeadSHA: strings.Repeat("c", 40)}
	publishResult := func(request reconciliationEffectRequest) reconciliationEffectResult {
		return reconciliationEffectResult{Action: request.Action, GitHubPublish: &githubPublishEffectResult{PR: 7, HeadSHA: request.GitHubPublish.HeadSHA, BoundBodyDigest: expectedPublishedBodyDigest(request, 7), Evidence: true, PublishedComment: true}}
	}
	update := func(kind githubIssueUpdateKind, payload githubIssueUpdateEffectRequest) reconciliationEffectRequest {
		request := common(reconciliationGitHubIssueUpdate)
		payload.Kind = kind
		request.GitHubIssueUpdate = &payload
		if kind == githubIssueControlSnapshot || kind == githubIssueDependencyClear {
			request.Attempt, request.Manifest = 0, nil
		}
		return request
	}
	governance := common(reconciliationGitHubPRGovernance)
	governance.GitHubPRGovernance = &githubGovernanceEffectRequest{PR: 7, HeadSHA: base, Policy: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 1}}
	reviewer := func(phase string) reconciliationEffectRequest {
		request := common(reconciliationReviewer)
		target := fmt.Sprintf("o/r#190 plan sha256:%s", digest)
		snapshot, session := reviewIdentity(agentruntime.Attempt{Repository: "o/r", Issue: 190, Number: 1}, filepath.Join("/tmp", "reviews"))
		request.Reviewer = &reviewerEffectRequest{Phase: phase, Mode: agentruntime.ReviewModePlan, Target: target, BaseSHA: base, HeadSHA: base, Snapshot: snapshot, Session: session}
		return request
	}
	reviewerResult := func(request reconciliationEffectRequest) reconciliationEffectResult {
		value := request.Reviewer
		status := "clean"
		if value.Phase == "cleanup" {
			status = "cleaned"
		}
		return reconciliationEffectResult{Action: request.Action, Reviewer: &reviewerEffectResult{Phase: value.Phase, Status: status, Mode: value.Mode, Target: value.Target, BaseSHA: value.BaseSHA, HeadSHA: value.HeadSHA, Snapshot: value.Snapshot, Session: value.Session}}
	}
	reviewHandoff := common(reconciliationHandoffDeliver)
	reviewHandoff.Handoff = &handoffEffectRequest{Kind: "review-findings", HeadSHA: base, Key: "review", Findings: []string{"finding"}, OutcomePath: handoffReceiptPath("/tmp/worktree", "review"), OutcomeToken: base}
	reviewHandoff.Manifest.Worktree = "/tmp/worktree"
	recoveryHandoff := common(reconciliationHandoffDeliver)
	recovery := internalgithub.RecoveryHandoff{Key: "recovery", Repository: "o/r", PR: 7, Issue: 190, Attempt: 1, HeadSHA: base, Validation: true}
	token := fmt.Sprintf("%x", sha256Sum("handoff-outcome\x00"+recovery.Key))
	recoveryHandoff.Handoff = &handoffEffectRequest{Kind: "recovery", Key: recovery.Key, Recovery: &recovery, OutcomePath: handoffReceiptPath(recoveryHandoff.Manifest.Worktree, recovery.Key), OutcomeToken: token}
	recoveryOutcome := cloneReconciliationRequest(recoveryHandoff)
	recoveryOutcome.Handoff.Outcome = &internalgithub.HandoffOutcome{Key: recovery.Key, ValidationResult: "passed", ValidationEvidence: "tests passed"}
	retire := common(reconciliationRetireCompleted)
	retire.Retire = &retireCompletedEffectRequest{Mode: "abandon", HeadSHA: base}
	checkIn := common(reconciliationMonitoringCheckIn)
	checkIn.CheckIn = &monitoringCheckInEffectRequest{Binding: digest, Payload: `{\"type\":\"agent-symphony-monitoring-check-in-v1\"}`}
	return []reconciliationEffectCase{
		{"github-bind", bind, observed(reconciliationGitHubBind, nil)},
		{"github-publish", publish, publishResult},
		{"github-publish-prepared", publishPrepared, publishResult},
		{"issue-terminal-failure", update(githubIssueTerminalFailure, githubIssueUpdateEffectRequest{Diagnostic: "failed", FailedAtUnixNano: 1}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-evidence", update(githubIssueEvidence, githubIssueUpdateEffectRequest{HeadSHA: base}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-findings", update(githubIssueFindings, githubIssueUpdateEffectRequest{HeadSHA: base, Findings: []string{"finding"}}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-retry", update(githubIssueRetry, githubIssueUpdateEffectRequest{FailedAtUnixNano: 1}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-control-snapshot", update(githubIssueControlSnapshot, githubIssueUpdateEffectRequest{ControlSnapshotDigest: strings.Repeat("c", 64)}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"issue-dependency-clear", update(githubIssueDependencyClear, githubIssueUpdateEffectRequest{AttributionAttempt: 1, Dependency: 3, PullRequest: 7}), observed(reconciliationGitHubIssueUpdate, nil)},
		{"github-pr-governance", governance, observed(reconciliationGitHubPRGovernance, nil)},
		{"reviewer-run-observe", reviewer("run-observe"), reviewerResult},
		{"reviewer-cleanup", reviewer("cleanup"), reviewerResult},
		{"handoff-review-findings", reviewHandoff, observed(reconciliationHandoffDeliver, nil)},
		{"handoff-recovery", recoveryHandoff, observed(reconciliationHandoffDeliver, nil)},
		{"handoff-recovery-outcome", recoveryOutcome, observed(reconciliationHandoffDeliver, nil)},
		{"retire-completed", retire, observed(reconciliationRetireCompleted, nil)},
		{"monitoring-check-in", checkIn, observed(reconciliationMonitoringCheckIn, nil)},
	}
}

func reconciliationEffectTestOwner(t *testing.T, request reconciliationEffectRequest) (*stateOwner, stateOwnerSnapshot, reconciliationEffectRequest) {
	t.Helper()
	_, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, request, func(runtimeOwnerState) error { return nil })
	return owner, snapshot, bindEffectObservation(snapshot, request)
}

func sealTestReviewerResult(t *testing.T, owner *stateOwner, effect runtimeEffectIntent, result reconciliationEffectResult) {
	t.Helper()
	current := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	identity := ownerReconciliationEffectIdentity(current)
	reviewer := current.Reconciliation.Reviewer
	launchPath, terminalPath := reviewerLifecyclePaths(reviewer.Snapshot, reviewer.Target)
	pane, err := parseReviewerPaneIdentity(reviewerPaneTestOutput("1|0|||", reviewer.Session, "$9", os.Getpid(), "agent-symphony review-pane tmux "+launchPath+" "+terminalPath+" "+reviewerSignal(reviewerIdentity(identity))+" "+identity.RequestDigest))
	if err != nil {
		t.Fatal(err)
	}
	terminalIdentity := reviewerIdentity(identity)
	terminalIdentity.GateProtocol, terminalIdentity.SessionRequested, terminalIdentity.ChildPID = current.ReviewerGateProtocol, current.ReviewerSessionRequested, current.ReviewerGroupPID
	if _, err := owner.sealReviewerResult(t.Context(), sealReviewerResultCommand{Identity: identity, Result: result, Pane: pane, Terminal: reviewerTerminalRecord{Identity: terminalIdentity}}); err != nil {
		t.Fatal(err)
	}
}

func reconciliationEffectPersistentOwner(t *testing.T, request reconciliationEffectRequest) (string, *stateOwner, stateOwnerSnapshot) {
	t.Helper()
	var root string
	root, owner, snapshot := reconciliationEffectPersistentOwnerWithPersist(t, request, nil)
	return root, owner, snapshot
}

func reconciliationEffectPersistentOwnerWithPersist(t *testing.T, request reconciliationEffectRequest, persist func(runtimeOwnerState) error) (string, *stateOwner, stateOwnerSnapshot) {
	t.Helper()
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, request.Issue, max(1, request.Attempt), "running")
	head := strings.Repeat("b", 40)
	request = configureEffectFixture(root, request, &manifest, head)
	state := newRuntimeOwnerState("o/r")
	if request.Attempt > 0 {
		issueKey, attemptKey := ownerIssueKey("o/r", request.Issue), ownerAttemptKey("o/r", request.Issue, request.Attempt)
		state.IssueGenerations[issueKey] = 1
		state.AttemptGenerations[attemptKey] = 1
		state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
		if request.Reviewer != nil && request.Reviewer.Phase == "cleanup" {
			proof := reviewerProcessProof{Repository: "o/r", Issue: request.Issue, Attempt: request.Attempt, Mode: request.Reviewer.Mode, Target: request.Reviewer.Target, EffectID: "1234567890abcdef1234567890abcdef", IssueGeneration: 1, AttemptGeneration: 1, NeverRan: true, DeadProved: true}
			state.ReviewerProofs[reviewerProofKey("o/r", request.Issue, request.Attempt, proof.Mode, proof.Target)] = proof
		}
		if request.Action == reconciliationGitHubPRGovernance || request.Handoff != nil && request.Handoff.Kind == "recovery" || request.GitHubPublish != nil && request.GitHubPublish.Prepared != nil {
			recovery := internalgithub.PRState{Repository: "o/r", Number: 7, Issue: request.Issue, Attempt: request.Attempt, HeadSHA: head, HandoffReceipts: map[string]bool{}}
			if request.Handoff != nil && request.Handoff.Kind == "recovery" {
				recovery.ValidationInFlightSHA, recovery.ValidationGeneration = head, 1
				if request.Handoff.Outcome != nil {
					recovery.HandoffReceipts[request.Handoff.Key] = true
				}
			}
			if request.GitHubPublish != nil && request.GitHubPublish.Prepared != nil {
				recovery.PreparedPublication = clonePreparedPublication(request.GitHubPublish.Prepared)
			}
			state.Recoveries[attemptKey] = runtimePRRecovery{IssueGeneration: 1, AttemptGeneration: 1, State: recovery}
		}
	}
	if persist == nil {
		persist = func(state runtimeOwnerState) error {
			return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), state)
		}
	}
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	input := reconciliationEffectObservationInput(request, "title")
	applyReconciliationInput(t, owner, input)
	snapshot, _ := owner.snapshot(t.Context())
	request.Manifest = nil
	if request.Attempt > 0 {
		stored := snapshot.State.Attempts[ownerAttemptKey("o/r", request.Issue, request.Attempt)].Manifest
		request.Manifest = &stored
	}
	return root, owner, snapshot
}

func configureEffectFixture(root string, request reconciliationEffectRequest, manifest *agentruntime.Manifest, head string) reconciliationEffectRequest {
	manifest.ReviewState, manifest.ReviewMode, manifest.ReviewTarget = "", "", ""
	manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = "", "", "", ""
	manifest.ReviewFindings, manifest.ReviewHandoffQueued, manifest.ReviewHandoffAck = nil, false, false
	switch request.Action {
	case reconciliationGitHubBind:
		manifest.State = "preparing"
	case reconciliationGitHubPublish:
		manifest.State, manifest.ReviewState, manifest.ReviewMode = "completed", "clean", agentruntime.ReviewModeImplementation
		publishedHead := head
		if request.GitHubPublish.Prepared != nil {
			publishedHead = request.GitHubPublish.Prepared.HeadSHA
		}
		manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = manifest.BaseSHA, publishedHead, manifest.BaseSHA+".."+publishedHead
		request.GitHubPublish.HeadSHA = publishedHead
	case reconciliationGitHubPRGovernance:
		request.GitHubPRGovernance.HeadSHA = head
	case reconciliationReviewer:
		snapshot, session := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, productionSnapshotRoot(root))
		request.Reviewer.Snapshot, request.Reviewer.Session = snapshot, session
		if request.Reviewer.Mode == agentruntime.ReviewModeImplementation {
			manifest.State = "completed"
			request.Reviewer.BaseSHA, request.Reviewer.HeadSHA = manifest.BaseSHA, head
			request.Reviewer.Target = manifest.BaseSHA + ".." + head
		} else {
			manifest.State = "running"
			request.Reviewer.BaseSHA, request.Reviewer.HeadSHA = manifest.BaseSHA, manifest.BaseSHA
			request.Reviewer.Target = fmt.Sprintf("%s#%d plan sha256:%x", request.Repository, request.Issue, sha256Sum("body"))
		}
		if request.Reviewer.Phase == "cleanup" {
			manifest.ReviewState, manifest.ReviewMode, manifest.ReviewTarget = "clean", request.Reviewer.Mode, request.Reviewer.Target
			manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = request.Reviewer.BaseSHA, request.Reviewer.HeadSHA, snapshot, session
		}
	case reconciliationHandoffDeliver:
		if request.Handoff.Kind == "review-findings" {
			manifest.State, manifest.ReviewState, manifest.ReviewMode = "completed", "findings-queued", agentruntime.ReviewModeImplementation
			manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = manifest.BaseSHA, head, manifest.BaseSHA+".."+head
			manifest.ReviewFindings = slices.Clone(request.Handoff.Findings)
			request.Handoff.HeadSHA, request.Handoff.Key, request.Handoff.OutcomeToken = head, "independent-review-"+head, head
		} else {
			request.Handoff.Recovery.HeadSHA = head
			request.Handoff.Recovery.ValidationGeneration = 1
			request.Handoff.Recovery.Key = recoveryHandoffKey(*request.Handoff.Recovery)
			request.Handoff.Key = request.Handoff.Recovery.Key
			request.Handoff.OutcomeToken = fmt.Sprintf("%x", sha256Sum("handoff-outcome\x00"+request.Handoff.Key))
			if request.Handoff.Outcome != nil {
				request.Handoff.Outcome.Key = request.Handoff.Key
			}
		}
	case reconciliationRetireCompleted:
		manifest.State, manifest.ReviewHead = "running", head
		request.Retire.HeadSHA = head
	case reconciliationMonitoringCheckIn:
		manifest.State = "running"
		request.CheckIn.Session = manifest.Session
	case reconciliationGitHubIssueUpdate:
		switch request.GitHubIssueUpdate.Kind {
		case githubIssueTerminalFailure, githubIssueRetry:
			manifest.State = "failed"
		case githubIssueEvidence:
			manifest.State, manifest.ReviewHead = "completed", head
			request.GitHubIssueUpdate.HeadSHA = head
		case githubIssueFindings:
			manifest.State, manifest.ReviewState, manifest.ReviewMode = "completed", "findings-queued", agentruntime.ReviewModeImplementation
			manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = manifest.BaseSHA, head, manifest.BaseSHA+".."+head
			manifest.ReviewFindings = slices.Clone(request.GitHubIssueUpdate.Findings)
			request.GitHubIssueUpdate.HeadSHA = head
		}
	}
	request.Manifest = manifest
	return request
}

func reconciliationEffectObservationInput(request reconciliationEffectRequest, title string) reconciliationInput {
	issue := issueFact(request.Issue, title)
	issue.BaseBranch, issue.DispatchAuthorized, issue.Attempt, issue.CurrentAttempt = "main", true, request.Attempt, request.Attempt
	if request.Manifest != nil {
		issue.BaseSHA = request.Manifest.BaseSHA
	}
	if request.GitHubIssueUpdate != nil && request.GitHubIssueUpdate.Kind == githubIssueDependencyClear {
		issue.Dependencies = []int{request.GitHubIssueUpdate.Dependency}
		issue.SatisfiedDependencies = []int{request.GitHubIssueUpdate.Dependency}
		issue.Attempt, issue.CurrentAttempt = 1, 1
		active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: request.Issue, Attempt: 1, State: "active"}
		issue.ActiveAttempt = &active
	}
	input := repositoryInput(true, issue)
	if request.GitHubIssueUpdate != nil && (request.GitHubIssueUpdate.Kind == githubIssueControlSnapshot || request.GitHubIssueUpdate.Kind == githubIssueDependencyClear) {
		input.IssueUpdates = []reconciliationIssueUpdateProposal{{Repository: request.Repository, Issue: request.Issue, Kind: request.GitHubIssueUpdate.Kind, ControlSnapshotDigest: request.GitHubIssueUpdate.ControlSnapshotDigest, AttributionAttempt: request.GitHubIssueUpdate.AttributionAttempt, Dependency: request.GitHubIssueUpdate.Dependency, PullRequest: request.GitHubIssueUpdate.PullRequest}}
		if request.GitHubIssueUpdate.Kind == githubIssueDependencyClear {
			input.Attempts = []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: request.Issue, Attempt: 1, PR: request.GitHubIssueUpdate.PullRequest, State: "active"}}
		}
	}
	if request.Attempt > 0 {
		head := strings.Repeat("b", 40)
		fact := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: request.Issue, Attempt: request.Attempt, PR: 7, BaseSHA: request.Manifest.BaseSHA, HeadSHA: head, State: "completed"}
		if request.Action == reconciliationGitHubPRGovernance || request.Action == reconciliationHandoffDeliver {
			fact.State, fact.PublicationConfirmed = "active", true
		}
		if request.Action == reconciliationGitHubBind || request.Reviewer != nil && request.Reviewer.Mode == agentruntime.ReviewModePlan {
			fact.HeadSHA, fact.State = "", "active"
		}
		if request.Action == reconciliationMonitoringCheckIn {
			issue.NeedsAttention = true
			fact.HeadSHA, fact.State = "", "active"
		}
		if request.GitHubIssueUpdate != nil && (request.GitHubIssueUpdate.Kind == githubIssueTerminalFailure || request.GitHubIssueUpdate.Kind == githubIssueRetry || request.GitHubIssueUpdate.Kind == githubIssueFindings) {
			issue.ActiveAttempt, issue.TerminalAttempts = nil, nil
			input.Issues[0] = issue
			return input
		}
		issue.ActiveAttempt, issue.TerminalAttempts = &fact, []internalgithub.RecoveryAttemptFact{fact}
		input.Issues[0] = issue
		input.Attempts = []internalgithub.RecoveryAttemptFact{fact}
	}
	return input
}

func recoveryHandoffKey(handoff internalgithub.RecoveryHandoff) string {
	identities := make([]string, len(handoff.Feedback))
	for index, feedback := range handoff.Feedback {
		identities[index] = fmt.Sprintf("%s:%d", feedback.Source, feedback.ID)
	}
	slices.Sort(identities)
	return fmt.Sprintf("%x", sha256Sum(fmt.Sprintf("%s\x00%d\x00%d\x00%d\x00%s\x00%d\x00%s", handoff.Repository, handoff.PR, handoff.Issue, handoff.Attempt, handoff.HeadSHA, handoff.ValidationGeneration, strings.Join(identities, ","))))
}

func bindEffectObservation(snapshot stateOwnerSnapshot, request reconciliationEffectRequest) reconciliationEffectRequest {
	observation := snapshot.State.Observations[ownerIssueKey(request.Repository, request.Issue)]
	request.ObservationGeneration, request.ObservationCycleID, request.BodyDigest = observation.Generation, observation.LastCycleID, observation.Fact.BodyDigest
	if request.Manifest != nil {
		stored := snapshot.State.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].Manifest
		request.Manifest = &stored
	}
	if request.GitHubBind != nil {
		request.GitHubBind.BaseSHA, request.GitHubBind.Branch = request.Manifest.BaseSHA, request.Manifest.Branch
	}
	if request.GitHubIssueUpdate != nil && (request.GitHubIssueUpdate.Kind == githubIssueTerminalFailure || request.GitHubIssueUpdate.Kind == githubIssueRetry) {
		request.GitHubIssueUpdate.FailedAtUnixNano = request.Manifest.UpdatedAt.UnixNano()
	}
	if request.Reviewer != nil && request.Reviewer.Mode == agentruntime.ReviewModePlan {
		request.Reviewer.Target = fmt.Sprintf("%s#%d plan sha256:%s", request.Repository, request.Issue, request.BodyDigest)
	}
	if request.Handoff != nil && request.Handoff.Kind == "review-findings" {
		request.Handoff.HeadSHA, request.Handoff.OutcomeToken = request.Manifest.ReviewHead, request.Manifest.ReviewHead
		request.Handoff.OutcomePath = handoffReceiptPath(request.Manifest.Worktree, request.Handoff.Key)
	}
	if request.Handoff != nil && request.Handoff.Kind == "recovery" {
		request.Handoff.OutcomePath = handoffReceiptPath(request.Manifest.Worktree, request.Handoff.Key)
	}
	return request
}

func reconciliationBeginIdentity(snapshot stateOwnerSnapshot, request reconciliationEffectRequest) stateResultIdentity {
	return stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, IssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], AttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)]}
}

func reconciliationIntentIdentity(effect runtimeEffectIntent) stateResultIdentity {
	return stateResultIdentity{Epoch: effect.IntentEpoch, SourceRevision: effect.IntentRevision, IssueGeneration: effect.IssueGeneration, AttemptGeneration: effect.AttemptGeneration, EffectID: effect.ID, RequestDigest: effect.RequestDigest}
}

func sha256Sum(value string) [32]byte { return sha256.Sum256([]byte(value)) }
