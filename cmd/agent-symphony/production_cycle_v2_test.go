package main

import (
	"bytes"
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type fixedGovernanceSource struct{ state internalgithub.PRState }

func (s fixedGovernanceSource) OpenPullRequests(context.Context) ([]int, error) {
	return []int{s.state.Number}, nil
}

func (s fixedGovernanceSource) FreshPullRequest(context.Context, int) (internalgithub.PRState, error) {
	return s.state, nil
}

func (fixedGovernanceSource) FreshFeedback(context.Context, internalgithub.PRState, internalgithub.Feedback) (internalgithub.Feedback, error) {
	return internalgithub.Feedback{}, errors.New("unexpected feedback read")
}

func TestSuccessfulCheckDoesNotCacheCancellation(t *testing.T) {
	var check successfulCheck
	calls := 0
	cancelled, cancel := context.WithCancel(t.Context())
	cancel()
	if err := check.Do(func() error { calls++; return cancelled.Err() }); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled preflight error=%v", err)
	}
	if err := check.Do(func() error { calls++; return nil }); err != nil {
		t.Fatalf("retry after cancellation: %v", err)
	}
	if err := check.Do(func() error { calls++; return errors.New("must not run") }); err != nil || calls != 2 {
		t.Fatalf("successful preflight was not cached: calls=%d err=%v", calls, err)
	}
}

func TestGovernanceMergeObservationRequiresExactIdentityAndHead(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	exact := internalgithub.RecoveryAttemptFact{Repository: request.Repository, PR: request.GitHubPRGovernance.PR, Issue: request.Issue, Attempt: request.Attempt, HeadSHA: request.GitHubPRGovernance.HeadSHA, State: "completed"}
	if !exactGovernanceMergeObserved(request, []internalgithub.RecoveryAttemptFact{exact}) {
		t.Fatal("exact completed merge was not observed")
	}
	for _, mutate := range []func(*internalgithub.RecoveryAttemptFact){
		func(f *internalgithub.RecoveryAttemptFact) { f.Repository = "other/repo" },
		func(f *internalgithub.RecoveryAttemptFact) { f.PR++ },
		func(f *internalgithub.RecoveryAttemptFact) { f.Issue++ },
		func(f *internalgithub.RecoveryAttemptFact) { f.Attempt++ },
		func(f *internalgithub.RecoveryAttemptFact) { f.HeadSHA = strings.Repeat("f", 40) },
		func(f *internalgithub.RecoveryAttemptFact) { f.State = "active" },
	} {
		candidate := exact
		mutate(&candidate)
		if exactGovernanceMergeObserved(request, []internalgithub.RecoveryAttemptFact{candidate}) {
			t.Fatalf("weak merge proof accepted: %#v", candidate)
		}
	}
}

func TestInvalidatedAdmittedMergeStaysUnresolvedAfterUnmergedRead(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	_, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	recovery := ownerAttemptRecovery{owner: owner, identity: ownerReconciliationEffectIdentity(*effect)}
	state, err := recovery.PullRequestState(t.Context(), request.Repository, request.GitHubPRGovernance.PR, request.Issue, request.Attempt, request.GitHubPRGovernance.HeadSHA)
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
	current := mustOwnerSnapshot(t, owner)
	manifest := *request.Manifest
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{
		Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt,
		ExpectedIssueGeneration:   current.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)],
		ExpectedAttemptGeneration: current.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)],
		Action:                    "dismissed", CleanupPhase: "completed", Manifest: &manifest,
	}); err != nil {
		t.Fatal(err)
	}

	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != fmt.Sprintf("/repos/o/r/pulls/%d/merge", request.GitHubPRGovernance.PR) {
			return nil, fmt.Errorf("unexpected request %s %s", r.Method, r.URL.String())
		}
		return &http.Response{StatusCode: http.StatusNotFound, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
	})}}
	production := &productionReconciliation{owner: owner, effects: &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}}}
	invalidated := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	changed, err := production.resolveOneInvalidatedGitHubEffect(t.Context(), api, invalidated)
	if err != nil || changed {
		t.Fatalf("single unmerged read resolved response-lost merge: changed=%v err=%v", changed, err)
	}
	final := mustOwnerSnapshot(t, owner).State
	if final.Effects[effect.ID].State != "invalidated" || len(final.Tombstones[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].ExternalOutcomes) != 0 {
		t.Fatalf("response-lost merge was not retained for convergence: effect=%#v tombstone=%#v", final.Effects[effect.ID], final.Tombstones[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)])
	}
}

func TestInvalidatedMergeConvergesAfterBlockedResponseLostPUTLands(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "github-pr-governance").request
	request.Issue, request.Manifest.Issue = 73, 73
	request.Manifest.Branch = "agent/73-1"
	request.GitHubPRGovernance.HeadSHA = strings.Repeat("b", 40)
	request.GitHubPRGovernance.Policy.ActorID = 42
	_, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	request = *effect.Reconciliation
	recovery := ownerAttemptRecovery{owner: owner, identity: ownerReconciliationEffectIdentity(*effect)}
	state, err := recovery.PullRequestState(t.Context(), request.Repository, request.GitHubPRGovernance.PR, request.Issue, request.Attempt, request.GitHubPRGovernance.HeadSHA)
	if err != nil {
		t.Fatal(err)
	}
	head := state.HeadSHA
	state.CheckHead, state.PolicyStatus, state.MergeAttemptSHA, state.MergePhase = head, "success", head, "prepared"
	state.Facts = internalgithub.PRFacts{IssueOpen: true, IssueEligible: true, AutonomousMerge: true, PRIsOpen: true, Mergeable: true, HeadSHA: head, ValidationSHA: head, DocumentationSHA: head, Approved: true, RequiredChecksPass: true, PolicyCheckRequired: true, MergePermission: true, BranchProtectionAllows: true}
	marker, err := internalgithub.AttemptMarker(request.Issue, request.Attempt, request.Manifest.Branch, head, request.GitHubPRGovernance.PR, "review")
	if err != nil {
		t.Fatal(err)
	}
	fixture := &fullSystemGitHub{
		base: request.Manifest.BaseSHA, labels: map[string]bool{}, includeClosedIssue: true,
		comments: []map[string]any{{"id": 1, "body": marker, "created_at": "2026-09-09T12:00:00Z", "updated_at": "2026-09-09T12:00:00Z", "user": map[string]any{"id": 42}}},
		pr:       map[string]any{"number": request.GitHubPRGovernance.PR, "body": marker, "state": "open", "merged": false, "mergeable": true, "mergeable_state": "clean", "user": map[string]any{"id": 42}, "head": map[string]any{"sha": head, "ref": request.Manifest.Branch}, "base": map[string]any{"sha": request.Manifest.BaseSHA, "ref": "main"}, "labels": []any{}},
	}
	putEntered, releasePUT := make(chan struct{}), make(chan struct{})
	var enterOnce sync.Once
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && r.URL.Path == fmt.Sprintf("/repos/o/r/pulls/%d/merge", request.GitHubPRGovernance.PR) {
			enterOnce.Do(func() { close(putEntered) })
			select {
			case <-releasePUT:
			case <-r.Context().Done():
				return
			}
			fixture.mu.Lock()
			fixture.merged, fixture.closed = true, true
			fixture.pr["state"], fixture.pr["merged"], fixture.pr["merged_at"] = "closed", true, "2026-09-09T12:00:01Z"
			fixture.mu.Unlock()
			http.Error(w, `{"message":"response lost after acceptance"}`, http.StatusInternalServerError)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == fmt.Sprintf("/repos/o/r/pulls/%d/merge", request.GitHubPRGovernance.PR) {
			fixture.mu.Lock()
			merged := fixture.merged
			fixture.mu.Unlock()
			if merged {
				w.WriteHeader(http.StatusNoContent)
			} else {
				http.Error(w, `{"message":"not merged"}`, http.StatusNotFound)
			}
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == fmt.Sprintf("/repos/o/r/pulls/%d", request.GitHubPRGovernance.PR) {
			fixture.mu.Lock()
			defer fixture.mu.Unlock()
			writeFixtureJSON(w, fixture.pr)
			return
		}
		if r.Method == http.MethodGet && (r.URL.Path == fmt.Sprintf("/repos/o/r/issues/%d/comments", request.GitHubPRGovernance.PR) || r.URL.Path == fmt.Sprintf("/repos/o/r/pulls/%d/comments", request.GitHubPRGovernance.PR) || r.URL.Path == fmt.Sprintf("/repos/o/r/pulls/%d/reviews", request.GitHubPRGovernance.PR)) {
			writeFixtureJSON(w, []any{})
			return
		}
		fixture.ServeHTTP(w, r)
	}))
	defer server.Close()
	api := internalgithub.API{BaseURL: server.URL, HTTP: server.Client(), Retries: -1}
	coordinator := internalgithub.PRCoordinator{API: api, Source: fixedGovernanceSource{state: state}, Signals: internalgithub.RecoverySignals{Recovery: recovery}, Phases: recovery, Attempts: map[int]internalgithub.RecoveryAttemptFact{request.GitHubPRGovernance.PR: {Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, PR: request.GitHubPRGovernance.PR}}, MergeMethod: "squash"}
	done := make(chan error, 1)
	go func() { done <- coordinator.Reconcile(t.Context()) }()
	select {
	case <-putEntered:
	case err := <-done:
		t.Fatalf("governance did not reach merge PUT: %v", err)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	}
	current := mustOwnerSnapshot(t, owner)
	admitted := current.State.Effects[effect.ID]
	if !slices.ContainsFunc(admitted.GovernancePhases, func(phase internalgithub.GovernancePhase) bool {
		return phase.Kind == "merge" && phase.State == "admitted"
	}) {
		t.Fatalf("merge PUT started without durable admission: %#v", admitted.GovernancePhases)
	}
	manifest := *request.Manifest
	if _, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: request.Repository, Issue: request.Issue, Attempt: request.Attempt, ExpectedIssueGeneration: current.State.IssueGenerations[ownerIssueKey(request.Repository, request.Issue)], ExpectedAttemptGeneration: current.State.AttemptGenerations[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)], Action: "dismissed", CleanupPhase: "completed", Manifest: &manifest}); err != nil {
		t.Fatal(err)
	}
	production := &productionReconciliation{owner: owner, effects: &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}, collector: reconciliationV2Collector{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}}}
	invalidated := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if changed, err := production.resolveOneInvalidatedGitHubEffect(t.Context(), api, invalidated); err != nil || changed {
		t.Fatalf("blocked response-lost merge resolved before remote acceptance: changed=%v err=%v", changed, err)
	}
	close(releasePUT)
	if err := <-done; err == nil {
		t.Fatal("response-lost merge unexpectedly returned success")
	}
	invalidated = mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if changed, err := production.resolveOneInvalidatedGitHubEffect(t.Context(), api, invalidated); err != nil || !changed {
		facts, factsErr := internalgithub.FetchAttemptFacts(t.Context(), api, request.Repository, 42)
		t.Fatalf("late exact merge did not converge: changed=%v err=%v request=%#v observed=%v facts=%#v facts_err=%v", changed, err, request.GitHubPRGovernance, exactGovernanceMergeObserved(request, facts), facts, factsErr)
	}
	final := mustOwnerSnapshot(t, owner).State
	outcome := final.Tombstones[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].ExternalOutcomes[effect.ID]
	if !outcome.Observed || !outcome.Merged || outcome.Superseded || outcome.HeadSHA != head {
		t.Fatalf("late merge outcome=%#v", outcome)
	}
}

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

func TestLegacyRunningAttemptDoesNotScheduleUnprovableMonitor(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 355, 1, "running") // v1 has no durable pane identity.
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
	snapshot := mustOwnerSnapshot(t, owner)
	issue := expandIssueFact(snapshot.State.Observations[ownerIssueKey(manifest.Repository, manifest.Issue)].Fact)
	issue.Body = "body"
	batch := reconciliationV2Batch{Input: repositoryInput(true, issue)}
	plans, err := planRuntimeLifecycle(snapshot, batch, config.Default(manifest.Repository), time.Now().UTC(), owner.attemptRoot, owner.stateRoot)
	if err != nil || len(plans) != 0 {
		t.Fatalf("legacy monitor was scheduled without pane proof: %#v, %v", plans, err)
	}
	wakes := 0
	reconciler := productionReconciliation{owner: owner, config: config.Default(manifest.Repository), attemptRoot: owner.attemptRoot, stateRoot: owner.stateRoot, wake: func() error { wakes++; return nil }}
	if err := reconciler.runRuntimePhase(t.Context(), batch, "source", agentruntime.EffectMonitor); err != nil || wakes != 0 {
		t.Fatalf("Check now repeated an unprovable legacy monitor: wakes=%d err=%v", wakes, err)
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
	if err := owner.authorizeRuntimeEffect(t.Context(), authorizeRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(*startEffect)), Action: agentruntime.EffectStart, GateNonce: startEffect.StartGateNonce}); err != nil {
		t.Fatal(err)
	}
	running.State = "running"
	running.LaunchID = startEffect.StartGateNonce
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
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	manifest = created.State.Attempts[ownerAttemptKey("o/r", 41, 1)].Manifest
	issue := internalgithub.RecoveryIssueFact{
		Repository: "o/r", Issue: 41, Attempt: 1, Active: true,
		DispatchAuthorized: true, BaseSHA: manifest.BaseSHA, Body: "exact body",
	}
	applyReconciliationInput(t, owner, repositoryInput(true, issue))

	lifecycle, cancel := context.WithCancel(t.Context())
	defer cancel()
	runner := &barrierEffectRunner{entered: make(chan struct{}, 1), release: make(chan struct{}), pane: boundRuntimeEffectTestPane(t, manifest)}
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

func TestPermittedStartWithMissingSessionRemainsQuarantinedAfterRestart(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	manifest := ownerTestManifest(t, owner.stateRoot, 412, 1, "preparing")
	manifest.Version, manifest.LaunchToken = agentruntime.ManifestVersion2, strings.Repeat("a", 32)
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	manifest = created.State.Attempts[ownerAttemptKey("o/r", 412, 1)].Manifest
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 412, Attempt: 1, Active: true, DispatchAuthorized: true, BaseSHA: manifest.BaseSHA, Body: "exact body"}
	input := repositoryInput(true, issue)
	applyReconciliationInput(t, owner, input)
	runner := &barrierEffectRunner{}
	runtimeState := &agentruntime.Runtime{Root: owner.attemptRoot, StateRoot: owner.stateRoot, Runner: runner, Tmux: "tmux", Helper: "agent-symphony-helper", VerifyWorker: func(context.Context) error { return nil }}
	effects, err := newRuntimeEffectCoordinator(t.Context(), owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	batch := reconciliationV2Batch{Input: input}
	snapshot := mustOwnerSnapshot(t, owner)
	observation := snapshot.State.Observations[ownerIssueKey("o/r", 412)]
	accepted := expandIssueFact(observation.Fact)
	accepted.Body, accepted.Attempt, accepted.BaseSHA = issue.Body, manifest.Attempt, manifest.BaseSHA
	attempt, err := runtimeLaunchAttempt(config.Default("o/r"), accepted, manifest, owner.attemptRoot)
	if err != nil {
		t.Fatal(err)
	}
	bound, err := effects.beginWithSource(t.Context(), snapshot, agentruntime.EffectRequest{Action: agentruntime.EffectStart, Attempt: attempt, Manifest: manifest, Eligible: true}, "")
	if err != nil {
		t.Fatal(err)
	}
	before := mustOwnerSnapshot(t, owner)
	var pending runtimeEffectIntent
	for _, effect := range before.State.Effects {
		if effect.Action == string(agentruntime.EffectStart) && effect.State == "pending" {
			pending = effect
		}
	}
	if pending.ID == "" || pending.StartGateNonce == "" {
		t.Fatalf("missing pending Start candidate: %#v", before.State.Effects)
	}
	run, err := effects.acquire(t.Context(), bound)
	if err != nil {
		t.Fatal(err)
	}
	pendingCycle := &productionReconciliation{owner: owner, effects: effects, config: config.Default("o/r"), attemptRoot: owner.attemptRoot, stateRoot: owner.stateRoot}
	if err := pendingCycle.resumePendingRuntime(t.Context(), batch, ""); err != nil {
		t.Fatal(err)
	}
	afterOverlap := mustOwnerSnapshot(t, owner).State.Effects[pending.ID]
	if afterOverlap.StartGateNonce != pending.StartGateNonce || len(afterOverlap.StartCandidates) != 1 {
		t.Fatalf("overlapping reconciliation rotated an active Start: before=%#v after=%#v", pending, afterOverlap)
	}
	effects.release(bound, run)
	if err := owner.authorizeRuntimeEffect(t.Context(), authorizeRuntimeEffectCommand{Identity: ownerEffectIdentity(effectRequestIdentity(pending)), Action: agentruntime.EffectStart, GateNonce: pending.StartGateNonce}); err != nil {
		t.Fatal(err)
	}
	if err := effects.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	restarted := restartOwnerWithInput(t, owner, input)
	recoveredEffects, err := newRuntimeEffectCoordinator(t.Context(), restarted, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	p := &productionReconciliation{owner: restarted, effects: recoveredEffects, config: config.Default("o/r"), attemptRoot: owner.attemptRoot, stateRoot: owner.stateRoot}
	if err := p.resumePendingRuntime(t.Context(), batch, ""); err != nil {
		t.Fatal(err)
	}
	if err := recoveredEffects.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := mustOwnerSnapshot(t, restarted)
	effect := after.State.Effects[pending.ID]
	if effect.State != "pending" || !effect.StartMayRun || effect.StartGateNonce != pending.StartGateNonce || effect.Diagnostic != "pending Start launch identity or worker absence is unproved" || runner.blocked.Load() != 0 {
		t.Fatalf("permitted missing candidate was replayed or lost: effect=%#v external dispatches=%d", effect, runner.blocked.Load())
	}
}

func TestPendingLegacyStartAfterRestartIsQuarantinedWithoutReplaying(t *testing.T) {
	root := resolvedTempDir(t)
	attemptRoot := productionAttemptRoot(root)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := ownerTestManifest(t, root, 410, 1, "preparing")
	persist := func(state runtimeOwnerState) error { return writeRuntimeOwnerState(root, attemptRoot, state) }
	owner, err := startTestStateOwner(t, root, runtimeEffectInitialState(legacy), persist)
	if err != nil {
		t.Fatal(err)
	}
	before := mustOwnerSnapshot(t, owner)
	_, pending, err := owner.beginRuntimeEffect(t.Context(), beginRuntimeEffectCommand{
		Identity: stateResultIdentity{Epoch: before.State.Epoch, SourceRevision: before.State.Revision, IssueGeneration: 1, AttemptGeneration: 1},
		Action:   agentruntime.EffectStart, Manifest: legacy, RequestDigest: strings.Repeat("2", 64),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, "o/r")
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, loaded, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	input := repositoryInput(true, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 410, Attempt: 1, Active: true, DispatchAuthorized: true, BaseSHA: legacy.BaseSHA, Body: "legacy"}, issueFact(411, "unrelated"))
	applyReconciliationInput(t, restarted, input)
	runner := &barrierEffectRunner{}
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: root, Runner: runner, Tmux: "tmux", VerifyWorker: func(context.Context) error { return nil }}
	effects, err := newRuntimeEffectCoordinator(t.Context(), restarted, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		t.Fatal(err)
	}
	p := &productionReconciliation{owner: restarted, effects: effects, config: config.Default("o/r"), attemptRoot: attemptRoot, stateRoot: root}
	if err := p.resumePendingRuntime(t.Context(), reconciliationV2Batch{Input: input}, ""); err != nil {
		t.Fatal(err)
	}
	after := mustOwnerSnapshot(t, restarted)
	if effect := after.State.Effects[pending.ID]; effect.State != "pending" || effect.Diagnostic != "legacy launch identity unproved; manual migration required" {
		t.Fatalf("legacy Start was not durably quarantined: %#v", effect)
	}
	if err := p.resumePendingRuntime(t.Context(), reconciliationV2Batch{Input: input}, ""); err != nil {
		t.Fatal(err)
	}
	if err := effects.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := runner.calls.Load(); got != 0 {
		t.Fatalf("legacy Start replayed %d external calls", got)
	}
	if repeated := mustOwnerSnapshot(t, restarted); repeated.State.Revision != after.State.Revision {
		t.Fatalf("quarantine caused a recurring commit: before=%d after=%d", after.State.Revision, repeated.State.Revision)
	}
	applyReconciliationInput(t, restarted, repositoryInput(true, issueFact(411, "unrelated next cycle")))
	status, err := projectOwnerStatus(mustOwnerSnapshot(t, restarted), 4, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	foundLegacy, foundUnrelated := false, false
	for _, entry := range status.Statuses {
		switch entry.Issue {
		case 410:
			foundLegacy = true
			if entry.Diagnostic != "legacy launch identity unproved; manual migration required" {
				t.Fatalf("legacy diagnostic not visible: %#v", entry)
			}
		case 411:
			foundUnrelated = true
			if entry.Diagnostic == "legacy launch identity unproved; manual migration required" {
				t.Fatalf("legacy quarantine poisoned unrelated issue: %#v", entry)
			}
		}
	}
	if !foundLegacy || !foundUnrelated {
		t.Fatalf("quarantine status missing legacy or unrelated issue: %#v", status.Statuses)
	}
	identityUnproved := mustOwnerSnapshot(t, restarted)
	effect := identityUnproved.State.Effects[pending.ID]
	effect.Diagnostic = "pending Start launch identity or worker absence is unproved"
	identityUnproved.State.Effects[pending.ID] = effect
	status, err = projectOwnerStatus(identityUnproved, 4, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	foundUnproved := false
	for _, entry := range status.Statuses {
		if entry.Issue != 410 {
			continue
		}
		foundUnproved = true
		if entry.Diagnostic != effect.Diagnostic || !entry.NeedsAttention || entry.Action != "inspect the unproved implementation launch before retry" {
			t.Fatalf("pending Start quarantine was not visible: %#v", entry)
		}
	}
	if !foundUnproved {
		t.Fatalf("pending Start quarantine status is missing: %#v", status.Statuses)
	}
	invalidated := mustOwnerSnapshot(t, restarted)
	invalidated.State.AttemptGenerations[ownerAttemptKey(legacy.Repository, legacy.Issue, legacy.Attempt)]++
	status, err = projectOwnerStatus(invalidated, 4, time.Now().UTC())
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range status.Statuses {
		if entry.Issue == 410 && (entry.Diagnostic == effect.Diagnostic || entry.Action == "inspect the unproved implementation launch before retry" || entry.Action == "manually migrate the legacy implementation launch identity") {
			t.Fatalf("stale Start diagnostic survived attempt-generation invalidation: %#v", entry)
		}
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
		base, head, _, exportBoundary := testWorkerExportBoundary(t)
		owner, manifest, issue, snapshot := completedWorkerOwner(t, 44, base, head, true)
		implementation := exportBoundary(manifest.Branch)
		var source string
		_, head, source, snapshot = selectWorkerExportFixture(t, owner, implementation, snapshot, manifest)
		cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
		candidate := publicationExecutionMaterial{Issue: issue, Config: cfg, Root: source, Head: head, Validation: "ok", Documentation: "none"}
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
		production := &productionReconciliation{owner: owner, effects: coordinator, implementation: implementation, collector: reconciliationV2Collector{Config: cfg}, stateRoot: owner.stateRoot}
		batch := reconciliationV2Batch{Input: input}
		if resumed, err := production.resumePendingReconciliation(t.Context(), api, batch); err != nil || !resumed {
			t.Fatalf("resume=%v err=%v", resumed, err)
		}
		if mustOwnerSnapshot(t, owner).State.Effects[plan.Identity.EffectID].State != "completed" {
			t.Fatal("publication intent remained pending")
		}
	})

	t.Run("reviewer-run-observe", func(t *testing.T) {
		base, head, _, exportBoundary := testWorkerExportBoundary(t)
		owner, manifest, issue, snapshot := completedWorkerOwner(t, 45, base, head, false)
		implementation := exportBoundary(manifest.Branch)
		var source string
		_, head, source, snapshot = selectWorkerExportFixture(t, owner, implementation, snapshot, manifest)
		candidate := reviewerExecutionMaterial{Issue: issue, Source: source, HeadSHA: head, Env: []string{"REVIEW=1"}, Command: []string{"reviewer"}}
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
  *'display-message'*) printf %s '{"Output":"||||||||||\n"}' ;;
  *) exit 1 ;;
esac`}}
		production := &productionReconciliation{owner: owner, effects: coordinator, implementation: implementation, reviewer: reviewer, config: cfg, reviewEnv: []string{"REVIEW=1"}, stateRoot: owner.stateRoot}
		if _, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{Input: input}); err != nil {
			t.Fatalf("pre-session gated review resume: %v", err)
		}
		current := mustOwnerSnapshot(t, owner)
		manifest = current.State.Attempts[ownerAttemptKey("o/r", issue.Issue, issue.Attempt)].Manifest
		if effect := current.State.Effects[plan.Identity.EffectID]; effect.State != "completed" || manifest.ReviewState != "failed" || !strings.Contains(manifest.ReviewDiagnostic, "before session creation") {
			t.Fatalf("pre-session gated reviewer did not terminalize truthful failure: effect=%#v manifest=%#v", effect, manifest)
		}
		proof := current.State.ReviewerProofs[reviewerProofKey("o/r", issue.Issue, issue.Attempt, plans[0].Request.Reviewer.Mode, plans[0].Request.Reviewer.Target)]
		if !proof.NeverRan || !proof.DeadProved || proof.EffectID != plan.Identity.EffectID {
			t.Fatalf("pre-session gated reviewer lacks exact no-run certificate: %#v", proof)
		}
	})

	t.Run("reviewer-cleanup", func(t *testing.T) {
		base, head := strings.Repeat("a", 40), strings.Repeat("b", 40)
		owner, _, issue, snapshot := completedWorkerOwner(t, 45, base, head, true)
		manifest := snapshot.State.Attempts[ownerAttemptKey("o/r", 45, 1)].Manifest
		candidate := reviewerExecutionMaterial{Issue: issue, HeadSHA: head}
		plans, _, err := planReconciliationReviewers(snapshot, owner.stateRoot, []reviewerExecutionMaterial{candidate})
		if err != nil || len(plans) != 0 {
			t.Fatalf("proofless legacy cleanup was planned: plans=%#v err=%v", plans, err)
		}
		input := repositoryInput(true, issue)
		owner = restartOwnerWithInput(t, owner, input)
		restarted := mustOwnerSnapshot(t, owner)
		plans, _, err = planReconciliationReviewers(restarted, owner.stateRoot, []reviewerExecutionMaterial{candidate})
		if err != nil || len(plans) != 0 {
			t.Fatalf("proofless legacy cleanup was planned after restart: plans=%#v err=%v", plans, err)
		}
		if current := restarted.State.Attempts[ownerAttemptKey("o/r", 45, 1)].Manifest; current.ReviewSnapshot != manifest.ReviewSnapshot || current.ReviewSession != manifest.ReviewSession || current.ReviewState != "clean" {
			t.Fatalf("legacy review changed without exact death proof: %#v", current)
		}
	})
}

func completedWorkerOwner(t *testing.T, issueNumber int, base, head string, reviewed bool) (*stateOwner, agentruntime.Manifest, internalgithub.RecoveryIssueFact, stateOwnerSnapshot) {
	t.Helper()
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, issueNumber, 1, "completed")
	manifest.BaseSHA = base
	manifest.WorkerGeneration, manifest.WorkerProfileDigest = 1, config.WorkerProfileDigest()
	if reviewed {
		manifest.ReviewState, manifest.ReviewMode = "clean", agentruntime.ReviewModeImplementation
		manifest.ReviewBase, manifest.ReviewHead, manifest.ReviewTarget = base, head, base+".."+head
		manifest.ReviewRunCleaned = true
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

func selectWorkerExportFixture(t *testing.T, owner *stateOwner, implementation workerBoundaryRunner, snapshot stateOwnerSnapshot, manifest agentruntime.Manifest) (workerResult, string, string, stateOwnerSnapshot) {
	t.Helper()
	production := &productionReconciliation{owner: owner, implementation: implementation, stateRoot: owner.stateRoot}
	result, head, root, err := production.selectedWorkerExport(t.Context(), snapshot.State.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)])
	if err != nil {
		t.Fatal(err)
	}
	return result, head, root, mustOwnerSnapshot(t, owner)
}

func testWorkerExportBoundary(t *testing.T) (base, head, checkout string, boundary func(string) workerBoundaryRunner) {
	t.Helper()
	checkout = resolvedTempDir(t)
	runGit(t, checkout, "init")
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "remote", "add", "origin", checkout)
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

func TestProductionCycleCompletesReceiptBoundPlanReviewerAfterFreshExternalAbsence(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 473, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	boundary := bindLiveReviewerForService(t, owner, reviewer)
	service.reviewer = boundary
	reads := 0
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			return nil, fmt.Errorf("unexpected mutation %s", request.URL.String())
		}
		reads++
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
	production := &productionReconciliation{owner: owner, effects: service.effects, operator: service, api: api, config: config.Default("o/r"), collector: reconciliationV2Collector{API: api, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: "o/r"}}, stateRoot: owner.stateRoot, attemptRoot: owner.attemptRoot}
	cycle, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := production.cycleFromSnapshot(t.Context(), cycle); err != nil && !errors.Is(err, errReconciliationRecollect) {
		t.Fatalf("fresh collector cycle failed while quarantining reviewer: %v", err)
	}
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, fmt.Sprintf("pending-plan-%d", manifest.Issue))
	effect := state.Effects[reviewer.ID]
	proof := state.ReviewerProofs[reviewerProofKey(manifest.Repository, manifest.Issue, manifest.Attempt, agentruntime.ReviewModePlan, reviewer.Reconciliation.Reviewer.Target)]
	gone, groupErr := reviewerGroupGone(proof.GroupPID)
	if reads == 0 || !ok || receipt.State != "completed" || receipt.Result == nil || receipt.Result.Status != http.StatusConflict || !effect.ReviewerRevoked || effect.State != "completed" || !proof.DeadProved || !gone || groupErr != nil || len(boundary.killed) != 1 {
		t.Fatalf("cycle did not durably retire stopped reviewer: reads=%d receipt=%#v effect=%#v proof=%#v gone=%v groupErr=%v killed=%v", reads, receipt, effect, proof, gone, groupErr, boundary.killed)
	}
}

func TestUnprovableReviewerDoesNotBlockUnrelatedReconciliation(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 475, "active", false)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	reviewer := admitPendingGatedPlanReviewer(t, owner, service, manifest)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*reviewer)}); err != nil {
		t.Fatal(err)
	}
	service.reviewer = &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||"}}
	applyReconciliationInput(t, owner, repositoryInput(false, issueFact(476, "unrelated")))
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
	production := &productionReconciliation{owner: owner, effects: service.effects, operator: service, api: api, config: config.Default("o/r"), stateRoot: owner.stateRoot, attemptRoot: owner.attemptRoot, checkout: checkout, collector: reconciliationV2Collector{API: api, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: "o/r"}}}
	cycle, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := production.cycleFromSnapshot(t.Context(), cycle); err != nil && !errors.Is(err, errReconciliationRecollect) {
		t.Fatalf("one ambiguous reviewer blocked the global cycle: %v", err)
	}
	state := mustOwnerSnapshot(t, owner).State
	other := state.Observations[ownerIssueKey("o/r", 476)]
	blocked := state.Effects[reviewer.ID]
	if other.Present || other.Generation < 2 || blocked.State != "pending" || !blocked.ReviewerRevoked || !strings.Contains(blocked.Diagnostic, "stop remains pending") {
		t.Fatalf("unrelated issue did not advance or ambiguous review falsely completed: other=%#v reviewer=%#v", other, blocked)
	}
	if superseded, err := production.supersedeInvalidPendingPlanReviewers(t.Context(), mustOwnerSnapshot(t, owner)); err != nil || superseded {
		t.Fatalf("repeated ambiguous stop changed outcome: superseded=%v err=%v", superseded, err)
	}
	if next := mustOwnerSnapshot(t, owner).State.Revision; next != state.Revision {
		t.Fatalf("unchanged diagnostic churned owner revision: before=%d after=%d", state.Revision, next)
	}
}

func TestImplementationReviewerFailureDoesNotBlockOtherEffect(t *testing.T) {
	for _, failure := range []string{"unprovable stop", "unreadable export"} {
		t.Run(failure, func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
			request.Reviewer.Mode, request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
			owner, admitted, request := reconciliationEffectTestOwner(t, request)
			_, blocked, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(admitted, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: ownerReconciliationEffectIdentity(*blocked)}); err != nil {
				t.Fatal(err)
			}
			if failure == "unprovable stop" {
				state := mustOwnerSnapshot(t, owner).State
				changed := expandIssueFact(state.Observations[ownerIssueKey("o/r", request.Issue)].Fact)
				changed.Body = "changed body"
				attempt := expandAttemptFact(state.Observations[ownerIssueKey("o/r", request.Issue)].Attempts[ownerAttemptKey("o/r", request.Issue, request.Attempt)].Fact)
				input := repositoryInput(true, changed)
				input.Scope = issueScope(request.Issue)
				input.Attempts = []internalgithub.RecoveryAttemptFact{attempt}
				applyReconciliationInput(t, owner, input)
			}
			service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: request.Manifest.Worktree})
			service.reviewer = &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||"}}
			production := &productionReconciliation{owner: owner, effects: service.effects, operator: service, implementation: workerBoundaryRunner{}, stateRoot: owner.stateRoot}
			// Join an A-only pass before admitting B. This proves the failed
			// reviewer is nonfatal without depending on hash-derived effect order.
			firstPass := make(chan struct {
				resumed bool
				err     error
			}, 1)
			go func() {
				resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{})
				firstPass <- struct {
					resumed bool
					err     error
				}{resumed: resumed, err: err}
			}()
			first := <-firstPass
			if first.err != nil || first.resumed {
				t.Fatalf("ambiguous issue A aborted or falsely completed: resumed=%v err=%v", first.resumed, first.err)
			}
			wantDiagnostic := "stop remains pending"
			if failure == "unreadable export" {
				wantDiagnostic = "export import failed"
			}
			if current := mustOwnerSnapshot(t, owner).State; current.Effects[blocked.ID].State != "pending" || !strings.Contains(current.Effects[blocked.ID].Diagnostic, wantDiagnostic) {
				t.Fatalf("ambiguous issue A lost its durable pending diagnostic: %#v", current.Effects[blocked.ID])
			}
			other := ownerTestManifest(t, owner.stateRoot, 191, 1, "running")
			if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: other}); err != nil {
				t.Fatal(err)
			}
			otherIssue := issueFact(191, "other")
			otherIssue.Attempt, otherIssue.CurrentAttempt = 1, 1
			otherIssue.NeedsAttention, otherIssue.DispatchAuthorized = true, true
			otherAttempt := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 191, Attempt: 1, BaseSHA: other.BaseSHA, State: "active", Checks: []string{}}
			otherIssue.Active, otherIssue.ActiveAttempt = true, &otherAttempt
			input := repositoryInput(true, otherIssue)
			input.Scope = issueScope(191)
			input.Attempts = []internalgithub.RecoveryAttemptFact{otherAttempt}
			applyReconciliationInput(t, owner, input)
			checkIn := reconciliationEffectCaseNamed(t, "monitoring-check-in")
			otherSnapshot := mustOwnerSnapshot(t, owner)
			planned, err := planMonitoringCheckIn(otherSnapshot, orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 191, Attempt: 1, Action: orchestratoragent.ProposalActionCheckIn, Binding: strings.Repeat("a", 64)})
			if err != nil {
				t.Fatal(err)
			}
			otherRequest := planned.Request
			_, ready, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(otherSnapshot, otherRequest), Request: otherRequest})
			if err != nil {
				t.Fatal(err)
			}
			if err := writeReconciliationEffectMarker(owner.stateRoot, ownerReconciliationEffectIdentity(*ready), *ready.Reconciliation, checkIn.result(*ready.Reconciliation)); err != nil {
				t.Fatal(err)
			}
			resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{})
			if err != nil || !resumed {
				t.Fatalf("ambiguous issue A blocked marked issue B: resumed=%v err=%v", resumed, err)
			}
			final := mustOwnerSnapshot(t, owner).State
			if final.Effects[blocked.ID].State != "pending" || !strings.Contains(final.Effects[blocked.ID].Diagnostic, wantDiagnostic) || final.Effects[ready.ID].State != "completed" {
				t.Fatalf("issue A was falsely certified or issue B did not complete: A=%#v B=%#v", final.Effects[blocked.ID], final.Effects[ready.ID])
			}
		})
	}
}

func TestBadCompletedExportDoesNotBlockOtherCandidate(t *testing.T) {
	base, head, _, exportBoundary := testWorkerExportBoundary(t)
	owner, bad, badIssue, _ := completedWorkerOwner(t, 190, base, head, false)
	good := ownerTestManifest(t, owner.stateRoot, 191, 1, "completed")
	good.BaseSHA = base
	good.WorkerGeneration, good.WorkerProfileDigest = 1, config.WorkerProfileDigest()
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: good}); err != nil {
		t.Fatal(err)
	}
	goodIssue := issueFact(191, "good export")
	goodIssue.Attempt, goodIssue.CurrentAttempt, goodIssue.BaseBranch, goodIssue.BaseSHA = 1, 1, "main", base
	input := repositoryInput(true, goodIssue)
	input.Scope = issueScope(191)
	snapshot := applyReconciliationInput(t, owner, input)
	var log bytes.Buffer
	production := &productionReconciliation{owner: owner, implementation: exportBoundary(good.Branch), log: &log, stateRoot: owner.stateRoot}
	batch := reconciliationV2Batch{Input: reconciliationInput{Issues: []internalgithub.RecoveryIssueFact{badIssue, goodIssue}}}
	reviewers, publications, err := production.executionCandidates(t.Context(), snapshot, batch)
	if err != nil || len(reviewers) != 1 || len(publications) != 1 || reviewers[0].Issue.Issue != good.Issue || publications[0].Issue.Issue != good.Issue {
		t.Fatalf("bad issue blocked good candidate: reviewers=%#v publications=%#v err=%v", reviewers, publications, err)
	}
	snapshot = mustOwnerSnapshot(t, owner)
	plans, _, err := planReconciliationReviewers(snapshot, owner.stateRoot, reviewers)
	if err != nil || len(plans) != 1 || plans[0].Request.Issue != good.Issue {
		t.Fatalf("good issue did not reach reviewer admission: plans=%#v err=%v", plans, err)
	}
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request})
	if err != nil || effect == nil || effect.State != "pending" || effect.Issue != good.Issue {
		t.Fatalf("bad export prevented good issue's durable reviewer intent: effect=%#v err=%v", effect, err)
	}
	if !strings.Contains(log.String(), fmt.Sprintf("attempt #%d/%d", bad.Issue, bad.Attempt)) || !strings.Contains(log.String(), "export import remains unavailable") {
		t.Fatalf("bad attempt lost its bounded operator diagnostic: %q", log.String())
	}
}

func TestChangedExportHeadRejectsOlderReviewerMarker(t *testing.T) {
	base, head, _, exportBoundary := testWorkerExportBoundary(t)
	owner, manifest, issue, snapshot := completedWorkerOwner(t, 193, base, head, false)
	implementation := exportBoundary(manifest.Branch)
	var source string
	_, _, source, snapshot = selectWorkerExportFixture(t, owner, implementation, snapshot, manifest)
	oldHead := strings.Repeat("c", 40)
	plans, _, err := planReconciliationReviewers(snapshot, owner.stateRoot, []reviewerExecutionMaterial{{Issue: issue, Source: source, HeadSHA: oldHead, Command: []string{"reviewer"}}})
	if err != nil || len(plans) != 1 {
		t.Fatalf("admit older reviewer: plans=%#v err=%v", plans, err)
	}
	_, old, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*old)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	const stoppedGroup = 99999999
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: stoppedGroup}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.proveReviewerDead(t.Context(), proveReviewerDeadCommand{Identity: identity, GroupPID: stoppedGroup}); err != nil {
		t.Fatal(err)
	}
	result := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(*old.Reconciliation)
	if err := writeReconciliationEffectMarker(owner.stateRoot, ownerReconciliationEffectIdentity(*old), *old.Reconciliation, result); err != nil {
		t.Fatal(err)
	}
	if current := mustOwnerSnapshot(t, owner).State; reconciliationEffectFinishCurrent(owner.stateRoot, current, current.Effects[old.ID]) != nil || !current.ReviewerProofs[reviewerProofKey(old.Repository, old.Issue, old.Attempt, old.Reconciliation.Reviewer.Mode, old.Reconciliation.Reviewer.Target)].DeadProved {
		t.Fatal("H1 marker lacked an otherwise finishable owner death certificate")
	}
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	service.reviewer = &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||"}}
	production := &productionReconciliation{owner: owner, effects: service.effects, operator: service, implementation: implementation, stateRoot: owner.stateRoot}
	resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{})
	if err != nil || !resumed {
		t.Fatalf("new exported head did not invalidate marked old review: resumed=%v err=%v", resumed, err)
	}
	final := mustOwnerSnapshot(t, owner).State
	if effect := final.Effects[old.ID]; effect.State != "completed" || effect.ReconciliationResult != nil && effect.ReconciliationResult.Reviewer != nil && effect.ReconciliationResult.Reviewer.Status == "clean" || !final.Attempts[ownerAttemptKey("o/r", issue.Issue, issue.Attempt)].Manifest.ReviewInvalidated {
		t.Fatalf("old clean marker was applied despite newer export: effect=%#v manifest=%#v", effect, final.Attempts[ownerAttemptKey("o/r", issue.Issue, issue.Attempt)].Manifest)
	}
}

func TestHealthyPendingReviewerReplayLetsOtherEffectFinish(t *testing.T) {
	base, head, _, exportBoundary := testWorkerExportBoundary(t)
	owner, manifest, issue, snapshot := completedWorkerOwner(t, 194, base, head, false)
	implementation := exportBoundary(manifest.Branch)
	var source string
	_, head, source, snapshot = selectWorkerExportFixture(t, owner, implementation, snapshot, manifest)
	plans, _, err := planReconciliationReviewers(snapshot, owner.stateRoot, []reviewerExecutionMaterial{{Issue: issue, Source: source, HeadSHA: head, Command: []string{"reviewer"}}})
	if err != nil || len(plans) != 1 {
		t.Fatalf("admit reviewer: plans=%#v err=%v", plans, err)
	}
	_, reviewer, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request})
	if err != nil {
		t.Fatal(err)
	}
	live := startUnboundReviewerForService(t, owner, reviewer)
	launchPath, _ := reviewerLifecyclePaths(reviewer.Reconciliation.Reviewer.Snapshot, reviewer.Reconciliation.Reviewer.Target)
	var launch reviewerLaunchIdentity
	if found, err := readReviewerRecord(launchPath, &launch); err != nil || !found {
		t.Fatalf("read unbound reviewer launch: found=%v err=%v", found, err)
	}
	launch.ProfileDigest, launch.ConfinementVersion = reviewer.ReviewerProfileDigest, reviewer.ReviewerConfinementVersion
	if err := os.Remove(launchPath); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(launchPath, launch); err != nil {
		t.Fatal(err)
	}
	parts := strings.SplitN(live.status.Output, "|", 8)
	if len(parts) != 8 {
		t.Fatal("fixture has no exact live wrapper identity")
	}
	encode := func(output string) string {
		body, err := json.Marshal(agentruntime.Result{Output: output})
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	replayBoundary := workerBoundaryRunner{Command: "/bin/sh", Args: []string{"-c", `payload=$(cat)
case "$payload" in
	*'#{session_id}'*) printf %s "$REPLAY_PANE" ;;
  *'#{pane_start_command}'*) printf %s "$REPLAY_START" ;;
  *'#{pane_pid}'*) printf %s "$REPLAY_PID" ;;
  *'#{pane_dead}'*) printf %s "$REPLAY_PANE" ;;
  *'"wait-for"'*) printf %s "$REPLAY_OK" ;;
  *) exit 1 ;;
esac`}, Env: []string{"REPLAY_START=" + encode(parts[7]), "REPLAY_PID=" + encode(parts[6]), "REPLAY_PANE=" + encode(live.status.Output), "REPLAY_OK=" + encode("")}}
	coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	cfg := config.Default("o/r")
	cfg.Commands.Reviewer = []string{"reviewer"}
	production := &productionReconciliation{owner: owner, effects: coordinator, implementation: implementation, reviewer: replayBoundary, config: cfg, stateRoot: owner.stateRoot}
	batch := reconciliationV2Batch{Input: reconciliationInput{Issues: []internalgithub.RecoveryIssueFact{issue}}}
	if resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, batch); err != nil || resumed {
		t.Fatalf("healthy live reviewer forced immediate recollection: resumed=%v err=%v", resumed, err)
	}
	if current := mustOwnerSnapshot(t, owner).State.Effects[reviewer.ID]; current.State != "pending" || current.ReviewerGroupPID < 2 {
		t.Fatalf("healthy reviewer did not remain owner-bound and pending: %#v", current)
	}
	other := ownerTestManifest(t, owner.stateRoot, 195, 1, "running")
	if _, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: other}); err != nil {
		t.Fatal(err)
	}
	otherIssue := issueFact(195, "other")
	otherIssue.Attempt, otherIssue.CurrentAttempt = 1, 1
	otherIssue.NeedsAttention, otherIssue.DispatchAuthorized = true, true
	otherAttempt := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 195, Attempt: 1, BaseSHA: other.BaseSHA, State: "active", Checks: []string{}}
	otherIssue.Active, otherIssue.ActiveAttempt = true, &otherAttempt
	input := repositoryInput(true, otherIssue)
	input.Scope = issueScope(195)
	input.Attempts = []internalgithub.RecoveryAttemptFact{otherAttempt}
	applyReconciliationInput(t, owner, input)
	checkIn := reconciliationEffectCaseNamed(t, "monitoring-check-in")
	otherSnapshot := mustOwnerSnapshot(t, owner)
	planned, err := planMonitoringCheckIn(otherSnapshot, orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 195, Attempt: 1, Action: orchestratoragent.ProposalActionCheckIn, Binding: strings.Repeat("a", 64)})
	if err != nil {
		t.Fatal(err)
	}
	otherRequest := planned.Request
	_, ready, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(otherSnapshot, otherRequest), Request: otherRequest})
	if err != nil {
		t.Fatal(err)
	}
	if err := writeReconciliationEffectMarker(owner.stateRoot, ownerReconciliationEffectIdentity(*ready), *ready.Reconciliation, checkIn.result(*ready.Reconciliation)); err != nil {
		t.Fatal(err)
	}
	if resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, batch); err != nil || !resumed {
		t.Fatalf("healthy pending reviewer blocked other marked effect: resumed=%v err=%v", resumed, err)
	}
	final := mustOwnerSnapshot(t, owner).State
	if final.Effects[reviewer.ID].State != "pending" || final.Effects[ready.ID].State != "completed" {
		t.Fatalf("reviewer or other effect changed incorrectly: reviewer=%#v other=%#v", final.Effects[reviewer.ID], final.Effects[ready.ID])
	}
	if err := live.onKill(); err != nil {
		t.Fatal(err)
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
	root := resolvedTempDir(t)
	restorePinnedWorkerPermissions(t, root)
	cfg := config.Default("o/r")
	profile, err := config.PinWorkerExecutable(t.Context(), root, &cfg.Commands)
	if err != nil {
		t.Fatal(err)
	}
	manifest := ownerTestManifest(t, root, 48, 1, "running")
	manifest.Version, manifest.LaunchToken, manifest.LaunchID = agentruntime.ManifestVersion2, strings.Repeat("a", 32), strings.Repeat("b", 32)
	manifest.WorkerGeneration, manifest.WorkerProfileDigest = 1, profile
	pane := boundRuntimeEffectTestPane(t, manifest)
	body, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	state := runtimeEffectInitialState(manifest)
	state.WorkerProfileDigest = profile
	addOperatorObservation(&state, manifest, "active", false)
	state.Epoch, state.Revision = 1, 1
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	refreshOperatorObservation(t, owner)
	service := operatorServiceWithCleanup(t, owner, t.Context(), &operatorCleanupBoundary{path: manifest.Worktree})
	service.effects.executor.Runtime.Runner = &barrierEffectRunner{pane: pane}
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
	runtime, err := startProductionRuntimeV2(lifecycle, cfg, api, internalgithub.AuthenticatedUser{ID: 42}, owner.stateRoot, filepath.Join(owner.stateRoot, "legacy-pr-state.json"), checkout, io.Discard)
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

func TestProductionRuntimePersistsConditionalGitHubReads(t *testing.T) {
	checkout := gitRepository(t)
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "commit", "--allow-empty", "-m", "base")
	stateRoot := resolvedTempDir(t)
	cfg := productionETagTestConfig(t, stateRoot)
	if err := bindDeployment(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	if err := installDeploymentFence(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	var conditional atomic.Int64
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
			return nil, fmt.Errorf("unexpected GitHub read %s", request.URL.String())
		}
		header := make(http.Header)
		header.Set("ETag", `"unchanged"`)
		if request.Header.Get("If-None-Match") == `"unchanged"` {
			conditional.Add(1)
			return &http.Response{StatusCode: http.StatusNotModified, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
	})}}
	lifecycle, cancel := context.WithCancel(t.Context())
	runtime, err := startProductionRuntimeV2(lifecycle, cfg, api, internalgithub.AuthenticatedUser{ID: 42}, stateRoot, filepath.Join(stateRoot, "legacy-pr-state.json"), checkout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = runtime.shutdown(context.Background())
	}()
	for range 2 {
		if err := runtime.trigger.triggerAndWait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if conditional.Load() == 0 {
		t.Fatal("second production reconciliation did not send a conditional GitHub read")
	}
	beforeRestart := conditional.Load()
	cancel()
	if err := runtime.shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	runtime = nil
	restartedContext, stopRestart := context.WithCancel(t.Context())
	restarted, err := startProductionRuntimeV2(restartedContext, cfg, api, internalgithub.AuthenticatedUser{ID: 42}, stateRoot, filepath.Join(stateRoot, "legacy-pr-state.json"), checkout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopRestart()
		_ = restarted.shutdown(context.Background())
	}()
	if err := restarted.trigger.triggerAndWait(t.Context()); err != nil {
		t.Fatal(err)
	}
	if conditional.Load() <= beforeRestart {
		t.Fatal("restarted production runtime did not reuse persisted ETags")
	}
	cache, err := internalgithub.LoadReadCache(filepath.Join(stateRoot, "github-etag-cache.json"))
	if err != nil {
		t.Fatalf("production cache was not persisted: %v", err)
	}
	var repository struct {
		DefaultBranch string `json:"default_branch"`
	}
	if _, changed, err := (internalgithub.API{BaseURL: api.BaseURL, HTTP: api.HTTP, Cache: cache}).Read(t.Context(), "/repos/o/r", "", &repository); err != nil || changed || repository.DefaultBranch != "main" {
		t.Fatalf("persisted cache did not restore GitHub facts: changed=%v repository=%#v err=%v", changed, repository, err)
	}
}

func productionETagTestConfig(t *testing.T, stateRoot string) config.Config {
	t.Helper()
	restorePinnedWorkerPermissions(t, stateRoot)
	cfg := config.Default("o/r")
	codex := filepath.Join(t.TempDir(), "codex")
	buildNativeCodexFixture(t, codex, `if [ "$1" = --version ]; then printf 'codex-cli 0.153.4\n'; fi`)
	cfg.Commands.Implementation[0], cfg.Commands.Reviewer[0], cfg.Commands.OrchestratorAudit[0] = codex, codex, codex
	return cfg
}

func TestProductionRuntimeRecoversSafeCorruptGitHubCache(t *testing.T) {
	checkout := gitRepository(t)
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "commit", "--allow-empty", "-m", "base")
	stateRoot := resolvedTempDir(t)
	cfg := productionETagTestConfig(t, stateRoot)
	if err := bindDeployment(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	if err := installDeploymentFence(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	cachePath := filepath.Join(stateRoot, "github-etag-cache.json")
	if err := os.WriteFile(cachePath, []byte(`{"version":2,"entries":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		header := make(http.Header)
		header.Set("ETag", `"fresh"`)
		if request.Header.Get("If-None-Match") == `"fresh"` {
			return &http.Response{StatusCode: http.StatusNotModified, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		var value any
		switch request.URL.RequestURI() {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1", "/repos/o/r/issues?state=open&per_page=100&page=1":
			value = []any{}
		case "/repos/o/r":
			value = map[string]any{"default_branch": "main"}
		case "/repos/o/r/branches/main":
			value = map[string]any{"commit": map[string]any{"sha": strings.Repeat("a", 40)}}
		default:
			return nil, fmt.Errorf("unexpected GitHub read %s", request.URL.String())
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(strings.NewReader(string(body))), Request: request}, nil
	})}}
	var log synchronizedBuffer
	lifecycle, cancel := context.WithCancel(t.Context())
	runtime, err := startProductionRuntimeV2(lifecycle, cfg, api, internalgithub.AuthenticatedUser{ID: 42}, stateRoot, filepath.Join(stateRoot, "legacy-pr-state.json"), checkout, &log)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		cancel()
		_ = runtime.shutdown(context.Background())
	}()
	if !strings.Contains(log.String(), "GitHub ETag cache was corrupt") {
		t.Fatalf("cache recovery was not diagnosed: %s", log.String())
	}
	for range 2 {
		if err := runtime.trigger.triggerAndWait(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	cache, err := internalgithub.LoadReadCache(cachePath)
	if err != nil {
		t.Fatalf("fresh reconciliation did not replace corrupt cache: %v", err)
	}
	entry, changed, err := (internalgithub.API{BaseURL: api.BaseURL, HTTP: api.HTTP, Cache: cache}).Read(t.Context(), "/repos/o/r", "", &struct{}{})
	if err != nil || entry != `"fresh"` || changed {
		t.Fatalf("recovered cache did not serve conditional read: etag=%q changed=%v err=%v diagnostic=%q log=%s", entry, changed, err, mustOwnerSnapshot(t, runtime.owner).State.CycleDiagnostic, log.String())
	}
}

func TestProductionRuntimeAdmitsOwnerMutationWhileInitialCollectionIsBlocked(t *testing.T) {
	checkout := gitRepository(t)
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "commit", "--allow-empty", "-m", "base")
	stateRoot := resolvedTempDir(t)
	restorePinnedWorkerPermissions(t, stateRoot)
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

func restorePinnedWorkerPermissions(t *testing.T, stateRoot string) {
	t.Helper()
	t.Cleanup(func() {
		_ = filepath.WalkDir(filepath.Join(stateRoot, "worker-executable"), func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
	})
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

func TestProductionRuntimeSignalDrainsOperatorCollectionAndCacheSave(t *testing.T) {
	checkout := gitRepository(t)
	runGit(t, checkout, "config", "user.email", "test@example.invalid")
	runGit(t, checkout, "config", "user.name", "test")
	runGit(t, checkout, "commit", "--allow-empty", "-m", "base")
	stateRoot := resolvedTempDir(t)
	cfg := productionETagTestConfig(t, stateRoot)
	if err := bindDeployment(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	if err := installDeploymentFence(stateRoot, "o/r"); err != nil {
		t.Fatal(err)
	}
	cycleEntered := make(chan struct{})
	var firstPull, conditional atomic.Bool
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var value any
		switch request.URL.RequestURI() {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1":
			if !firstPull.Swap(true) {
				close(cycleEntered)
				<-request.Context().Done()
				return nil, request.Context().Err()
			}
			value = []any{}
		case "/repos/o/r":
			value = map[string]any{"default_branch": "main"}
		case "/repos/o/r/branches/main":
			value = map[string]any{"commit": map[string]any{"sha": strings.Repeat("a", 40)}}
		case "/repos/o/r/issues?state=open&per_page=100&page=1":
			value = []any{}
		case "/repos/o/r/issues/371":
			value = map[string]any{"number": 371}
		default:
			return nil, fmt.Errorf("unexpected GitHub read %s", request.URL.String())
		}
		header := make(http.Header)
		header.Set("ETag", `"signal"`)
		if request.Header.Get("If-None-Match") == `"signal"` {
			if request.URL.Path == "/repos/o/r/issues/371" {
				conditional.Store(true)
			}
			return &http.Response{StatusCode: http.StatusNotModified, Header: header, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		body, _ := json.Marshal(value)
		return &http.Response{StatusCode: http.StatusOK, Header: header, Body: io.NopCloser(bytes.NewReader(body)), Request: request}, nil
	})}}
	signalCtx, signalStop := context.WithCancel(t.Context())
	runtime, err := startProductionRuntimeV2(signalCtx, cfg, api, internalgithub.AuthenticatedUser{ID: 42}, stateRoot, filepath.Join(stateRoot, "legacy-pr-state.json"), checkout, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	collectEntered, releaseCollect := make(chan struct{}), make(chan struct{})
	saveEntered, releaseSave := make(chan struct{}), make(chan struct{})
	var releaseOnce, saveOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseCollect) })
		saveOnce.Do(func() { close(releaseSave) })
		signalStop()
		_ = runtime.shutdown(context.Background())
	})
	<-cycleEntered
	runtime.operator.collect = func(ctx context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		var fact struct{ Number int }
		if _, _, err := runtime.operator.collector.API.Read(ctx, "/repos/o/r/issues/371", "", &fact); err != nil || fact.Number != issue {
			return reconciliationV2Batch{}, fmt.Errorf("operator GitHub fact %d: got=%d err=%v", issue, fact.Number, err)
		}
		close(collectEntered)
		<-releaseCollect
		return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: snapshot.State.Repository, Issue: issue}, Complete: true}}, nil
	}
	runtime.operator.cacheSave = func() error {
		close(saveEntered)
		<-releaseSave
		return runtime.operator.collector.API.Cache.Save()
	}
	collected := make(chan error, 1)
	go func() {
		_, _, err := runtime.operator.collectIssue(t.Context(), 371)
		collected <- err
	}()
	<-collectEntered
	signalStop()
	if _, err := runtime.owner.snapshot(t.Context()); err != nil {
		t.Fatalf("signal pre-closed owner before graceful drain: %v", err)
	}
	shutdown := make(chan error, 1)
	go func() { shutdown <- runtime.shutdown(t.Context()) }()
	releaseOnce.Do(func() { close(releaseCollect) })
	if err := <-collected; err != nil {
		t.Fatalf("accepted operator collection after signal: %v", err)
	}
	<-saveEntered
	if _, err := runtime.owner.snapshot(t.Context()); err != nil {
		t.Fatalf("owner closed before cache save drained: %v", err)
	}
	saveOnce.Do(func() { close(releaseSave) })
	if err := <-shutdown; err != nil {
		t.Fatal(err)
	}
	cache, err := internalgithub.LoadReadCache(filepath.Join(stateRoot, "github-etag-cache.json"))
	if err != nil {
		t.Fatal(err)
	}
	api.Cache = cache
	var fact struct{ Number int }
	if _, changed, err := api.Read(t.Context(), "/repos/o/r/issues/371", "", &fact); err != nil || changed || fact.Number != 371 || !conditional.Load() {
		t.Fatalf("signal shutdown lost operator cache: changed=%v fact=%#v conditional=%v err=%v", changed, fact, conditional.Load(), err)
	}
}

func TestProductionRuntimeShutdownReportsIncompleteCacheDrain(t *testing.T) {
	owner, _ := operatorTestOwner(t, 372, "completed", true)
	cache, err := internalgithub.LoadReadCache(filepath.Join(owner.stateRoot, "github-etag-cache.json"))
	if err != nil {
		t.Fatal(err)
	}
	service := operatorTestMutationService(t, owner)
	service.collector.API.Cache = cache
	service.stopping = make(chan struct{})
	service.collect = func(_ context.Context, snapshot stateOwnerSnapshot, issue int) (reconciliationV2Batch, error) {
		return reconciliationV2Batch{Input: reconciliationInput{Scope: reconciliationScope{Kind: reconciliationIssueScope, Repository: snapshot.State.Repository, Issue: issue}, Complete: true}}, nil
	}
	saveEntered, releaseSave := make(chan struct{}), make(chan struct{})
	var once sync.Once
	t.Cleanup(func() {
		once.Do(func() { close(releaseSave) })
		_ = service.shutdown(context.Background())
	})
	service.cacheSave = func() error {
		close(saveEntered)
		<-releaseSave
		return nil
	}
	if _, _, err := service.collectIssue(t.Context(), 373); err != nil {
		t.Fatal(err)
	}
	<-saveEntered
	_, cancelLifecycle := context.WithCancel(t.Context())
	runtime := &productionRuntimeV2{cancel: cancelLifecycle, owner: owner, operator: service}
	shutdownCtx, cancelShutdown := context.WithCancel(t.Context())
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.shutdown(shutdownCtx) }()
	<-service.stopping
	cancelShutdown()
	if err := <-shutdownDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("incomplete cache drain was reported as success: %v", err)
	}
	once.Do(func() { close(releaseSave) })
}

func TestProductionRuntimeShutdownDrainsAcceptedDashboardEffect(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 374, "active", false, true)
	service := operatorTestMutationService(t, owner)
	service.stopping = make(chan struct{})
	runner := &barrierEffectRunner{entered: make(chan struct{}, 1), release: make(chan struct{}), cancelled: make(chan struct{}, 1), pane: boundRuntimeEffectTestPane(t, manifest), blockMissingSession: true}
	service.effects.executor.Runtime.Runner = runner
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(runner.release) }) })
	server := &dashboardServer{ctx: t.Context(), stateRoot: owner.stateRoot, repository: manifest.Repository, operator: service}
	request := httptest.NewRequest(http.MethodPost, "http://localhost/actions/cancel?repository=o%2Fr&issue=374&attempt=1", nil)
	request.Host = "localhost"
	request.Header.Set("Origin", "http://localhost")
	response := httptest.NewRecorder()
	server.handler(http.NotFoundHandler()).ServeHTTP(response, request)
	if response.Code != http.StatusAccepted {
		t.Fatalf("dashboard action was not accepted: status=%d body=%s", response.Code, response.Body.String())
	}
	var accepted controlResult
	if err := json.Unmarshal(response.Body.Bytes(), &accepted); err != nil || !accepted.OK || accepted.OwnerRevision == 0 {
		t.Fatalf("dashboard action receipt=%#v err=%v", accepted, err)
	}
	select {
	case <-runner.entered:
	case <-time.After(5 * time.Second):
		state := mustOwnerSnapshot(t, owner).State
		receipt, _ := operatorReceiptByID(state, accepted.RequestID)
		t.Fatalf("accepted effect did not reach the runner: calls=%d receipt=%#v effect=%#v", runner.calls.Load(), receipt, state.Effects[receipt.EffectID])
	}
	runtime := &productionRuntimeV2{operator: service, effects: service.effects}
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- runtime.shutdown(t.Context()) }()
	select {
	case <-service.stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("operator shutdown did not begin")
	}
	select {
	case <-runner.cancelled:
		t.Fatal("shutdown cancelled an admitted dashboard effect")
	default:
	}
	select {
	case err := <-shutdownDone:
		t.Fatalf("shutdown canceled accepted dashboard effect: %v", err)
	default:
	}
	once.Do(func() { close(runner.release) })
	if err := <-shutdownDone; err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, owner).State
	receipt, ok := operatorReceiptByID(state, accepted.RequestID)
	effect := state.Effects[receipt.EffectID]
	if !ok || receipt.State != "completed" || effect.State != "completed" || state.Attempts[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)].Manifest.State != "cancelled" {
		t.Fatalf("accepted dashboard effect was not completed: receipt=%#v effect=%#v", receipt, effect)
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
	if response.Code != http.StatusAccepted {
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
	created, err := owner.upsertAttempt(t.Context(), upsertAttemptCommand{Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	manifest = created.State.Attempts[ownerAttemptKey("o/r", 45, 1)].Manifest
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
	runner := &recordingCheckInRunner{branch: manifest.Branch, pane: boundRuntimeEffectTestPane(t, manifest)}
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
	pane       *agentruntime.ImplementationPane
}

func (r *recordingCheckInRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if command.Name == "git" {
		return agentruntime.Result{Output: r.branch + "\n"}, nil
	}
	if r.pane != nil && len(command.Args) > 0 && command.Args[0] == "display-message" && command.Args[len(command.Args)-1] == agentruntime.ImplementationPaneFormat {
		pane := r.pane
		return agentruntime.Result{Output: fmt.Sprintf("%s|%s|%s|%d|%d|%d|%s|%s|%s\n", pane.SessionName, pane.SessionID, pane.PaneID, pane.PanePID, pane.ServerPID, pane.ServerStart, pane.StartPath, pane.Token, pane.Command)}, nil
	}
	if len(command.Args) > 5 && command.Args[0] == "if-shell" {
		if strings.Contains(command.Args[5], "load-buffer") {
			r.deliveries++
		}
		if strings.Contains(command.Args[5], "display-message") {
			return agentruntime.Result{Output: "0"}, nil
		}
		return agentruntime.Result{}, nil
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
