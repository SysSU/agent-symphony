package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestPlanReconciliationIssueUpdateUsesOnlyAcceptedMaterial(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	body := "<!-- agent-symphony:control-snapshot:v1\n{}\n-->"
	digest := sha256.Sum256([]byte(body))
	proposal := reconciliationIssueUpdateProposal{Repository: "o/r", Issue: 181, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: hex.EncodeToString(digest[:])}
	input := repositoryInput(true, issueFact(181, "proposal"))
	input.IssueUpdates = []reconciliationIssueUpdateProposal{proposal}
	accepted := applyReconciliationInput(t, owner, input)
	batch := reconciliationV2Batch{IssueUpdates: []reconciliationIssueUpdateMaterial{{Proposal: proposal, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, ControlSnapshotBody: body}}}

	plans, err := planReconciliationIssueUpdates(accepted, batch)
	if err != nil || len(plans) != 1 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	plan := plans[0]
	if plan.Identity.SourceRevision != accepted.State.Revision || plan.Identity.IssueGeneration != accepted.State.IssueGenerations[ownerIssueKey("o/r", 181)] || plan.Request.ObservationGeneration != accepted.State.Observations[ownerIssueKey("o/r", 181)].Generation {
		t.Fatalf("plan is not bound to accepted owner state: %#v", plan)
	}
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plan.Identity, Request: plan.Request})
	if err != nil {
		t.Fatalf("owner rejected planned effect: %v", err)
	}

	batch.IssueUpdates[0].ControlSnapshotBody += "tampered"
	if _, err := planReconciliationIssueUpdates(accepted, batch); err == nil {
		t.Fatal("planner accepted material that did not match the accepted proposal digest")
	}
	batch.IssueUpdates[0].ControlSnapshotBody = body
	batch.IssueUpdates[0].Config.ReadyLabel = "changed"
	changedConfig, err := planReconciliationIssueUpdates(accepted, batch)
	if err != nil || len(changedConfig) != 1 || changedConfig[0].Request.ExecutionDigest == plan.Request.ExecutionDigest {
		t.Fatalf("changed execution config did not change durable digest: plans=%#v err=%v", changedConfig, err)
	}
	plan.Identity = reconciliationIntentIdentity(*effect)
	plan.Material.Config.ReadyLabel = "changed"
	requests := 0
	api := internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("unexpected I/O")
	})}}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	if _, err := coordinator.executeIssueUpdate(t.Context(), api, plan); err == nil || requests != 0 {
		t.Fatalf("changed config executed external I/O: requests=%d err=%v", requests, err)
	}
}

type reconciliationRoundTripFunc func(*http.Request) (*http.Response, error)

func (f reconciliationRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDormantV2CollectorUsesReadOnlyBoundariesAndProducesApplicableBatch(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	writes := 0
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			writes++
			return nil, errors.New("unexpected mutation")
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
	snapshot, err := owner.reconciliationSnapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	collector := reconciliationV2Collector{API: api, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: "o/r"}}
	batch, err := collector.collect(t.Context(), snapshot)
	if err != nil || writes != 0 || !batch.Input.Complete || batch.Input.Scope.Kind != reconciliationRepositoryScope {
		t.Fatalf("batch=%#v writes=%d err=%v", batch, writes, err)
	}
	collection, err := collectionFromSnapshot(snapshot, batch.Input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.applyReconciliation(t.Context(), collection); err != nil {
		t.Fatalf("owner rejected collected batch: %v", err)
	}
}

func TestTypedExecutorsRejectChangedMaterialBeforeExternalIO(t *testing.T) {
	apiCalls := 0
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(*http.Request) (*http.Response, error) {
		apiCalls++
		return nil, errors.New("unexpected I/O")
	})}}
	t.Run("publication", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "github-publish")
		root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		request := bindEffectObservation(snapshot, test.request)
		material := publicationExecutionMaterial{Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Root: root, Head: request.GitHubPublish.HeadSHA}
		request.ExecutionDigest = publicationExecutionDigest(request, material)
		material.Head = strings.Repeat("c", 40)
		coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
		if _, err := coordinator.executePublication(t.Context(), api, reconciliationPlannedEffect{Request: request}, material); !errors.Is(err, errStateConflict) || apiCalls != 0 {
			t.Fatalf("changed publication material reached I/O: calls=%d err=%v", apiCalls, err)
		}
	})
	t.Run("governance", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "github-pr-governance")
		_, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		request := bindEffectObservation(snapshot, test.request)
		attempt := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: request.Issue, Attempt: request.Attempt, PR: request.GitHubPRGovernance.PR, HeadSHA: request.GitHubPRGovernance.HeadSHA}
		request.ExecutionDigest = governanceExecutionDigest(request, attempt)
		attempt.HeadSHA = strings.Repeat("c", 40)
		coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
		if _, err := coordinator.executeGovernance(t.Context(), api, reconciliationPlannedEffect{Request: request, Attempt: &attempt}); !errors.Is(err, errStateConflict) || apiCalls != 0 {
			t.Fatalf("changed governance material reached I/O: calls=%d err=%v", apiCalls, err)
		}
	})
}

func TestPlanReconciliationIssueUpdateSkipsSupersededProposal(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	body := "snapshot"
	digest := sha256.Sum256([]byte(body))
	proposal := reconciliationIssueUpdateProposal{Repository: "o/r", Issue: 182, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: hex.EncodeToString(digest[:])}
	input := repositoryInput(true, issueFact(182, "first"))
	input.IssueUpdates = []reconciliationIssueUpdateProposal{proposal}
	accepted := applyReconciliationInput(t, owner, input)
	batch := reconciliationV2Batch{IssueUpdates: []reconciliationIssueUpdateMaterial{{Proposal: proposal, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, ControlSnapshotBody: body}}}

	newer := repositoryInput(true, issueFact(182, "changed"))
	current := applyReconciliationInput(t, owner, newer)
	plans, err := planReconciliationIssueUpdates(current, batch)
	if err != nil || len(plans) != 0 {
		t.Fatalf("superseded proposal planned: %#v err=%v (accepted revision %d)", plans, err, accepted.State.Revision)
	}
}

func TestPlanReconciliationAttemptIssueUpdatesUsesAcceptedLifecycle(t *testing.T) {
	for _, name := range []string{"issue-terminal-failure", "issue-evidence", "issue-findings", "issue-retry"} {
		t.Run(name, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, name)
			_, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
			input := reconciliationEffectObservationInput(test.request, "title")
			if name == "issue-retry" {
				terminal := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: test.request.Issue, Attempt: test.request.Attempt, State: "completed"}
				input.Issues[0].TerminalAttempts = []internalgithub.RecoveryAttemptFact{terminal}
				input.Issues[0].RecoveryAuthorized = true
				input.Attempts = []internalgithub.RecoveryAttemptFact{terminal}
				snapshot = applyReconciliationInput(t, owner, input)
			}
			batch := reconciliationV2Batch{Input: input}
			plans, err := planReconciliationAttemptIssueUpdates(snapshot, batch, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42, RetryCommand: "/agent-symphony retry"})
			if err != nil || len(plans) != 1 || plans[0].Request.GitHubIssueUpdate.Kind != test.request.GitHubIssueUpdate.Kind {
				t.Fatalf("plans=%#v err=%v", plans, err)
			}
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request}); err != nil {
				t.Fatalf("owner rejected planned attempt update: %v", err)
			}
		})
	}
}

func TestExecuteEveryGitHubIssueUpdateVariantFromDurableIntent(t *testing.T) {
	for _, name := range []string{"issue-control-snapshot", "issue-dependency-clear", "issue-terminal-failure", "issue-evidence", "issue-findings", "issue-retry"} {
		t.Run(name, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, name)
			request := cloneReconciliationRequest(test.request)
			cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42, RetryCommand: "/agent-symphony retry"}
			material := reconciliationIssueUpdateMaterial{Config: cfg}
			comments := map[int][]map[string]any{}
			controlSnapshotBody := ""
			if request.GitHubIssueUpdate.Kind == githubIssueControlSnapshot {
				controlSnapshotBody = internalgithub.SnapshotComment(internalgithub.Snapshot{Version: 2})
				request.GitHubIssueUpdate.ControlSnapshotDigest = digestText(controlSnapshotBody)
			}
			_, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
			request = bindEffectObservation(snapshot, request)
			nextComment := func(number int, body string, created time.Time) {
				comments[number] = append(comments[number], map[string]any{"id": len(comments[number]) + 1, "body": body, "created_at": created.UTC(), "updated_at": created.UTC(), "user": map[string]any{"id": 42}})
			}
			now := time.Unix(200, 0)
			switch request.GitHubIssueUpdate.Kind {
			case githubIssueControlSnapshot:
				body := controlSnapshotBody
				material.ControlSnapshotBody = body
				material.Proposal = reconciliationIssueUpdateProposal{Repository: "o/r", Issue: request.Issue, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: digestText(body)}
				nextComment(request.Issue, body, now)
			case githubIssueDependencyClear:
				material.Proposal = reconciliationIssueUpdateProposal{Repository: "o/r", Issue: request.Issue, Kind: githubIssueDependencyClear, AttributionAttempt: request.GitHubIssueUpdate.AttributionAttempt, Dependency: request.GitHubIssueUpdate.Dependency, PullRequest: request.GitHubIssueUpdate.PullRequest}
				nextComment(request.Issue, "/agent-symphony status clear: monitoring: dependency #3 is complete", now)
			case githubIssueTerminalFailure:
				marker, _ := internalgithub.TerminalFailureMarker(request.Issue, request.Attempt, time.Unix(0, request.GitHubIssueUpdate.FailedAtUnixNano))
				nextComment(request.Issue, marker, now)
			case githubIssueEvidence:
				for _, kind := range []string{"validation", "documentation"} {
					body, _ := internalgithub.EvidenceBody(request.Issue, request.Attempt, kind, request.GitHubIssueUpdate.HeadSHA)
					nextComment(request.Issue, body, now)
				}
			case githubIssueFindings:
				body, _ := internalgithub.ReviewFindingsBody(request.Issue, request.Attempt, request.GitHubIssueUpdate.HeadSHA, request.GitHubIssueUpdate.Findings)
				nextComment(request.Issue, body, now)
			case githubIssueRetry:
				failed := time.Unix(0, request.GitHubIssueUpdate.FailedAtUnixNano)
				marker, _ := internalgithub.TerminalFailureMarker(request.Issue, request.Attempt, failed)
				nextComment(request.Issue, marker, failed)
				nextComment(request.Issue, cfg.RetryCommand, failed.Add(time.Second))
			}
			if request.Attempt > 0 {
				issue := issueFact(request.Issue, "title")
				issue.Attempt = request.Attempt
				material.Issue = &issue
				if request.GitHubIssueUpdate.Kind == githubIssueEvidence {
					fact := reconciliationEffectObservationInput(request, "title").Attempts[0]
					material.Attempt = &fact
				}
			}
			request.ExecutionDigest = issueUpdateExecutionDigest(request, material)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			freshInput := reconciliationEffectObservationInput(request, "title")
			freshInput.Issues[0].BaseSHA = snapshot.State.Observations[ownerIssueKey("o/r", request.Issue)].Fact.BaseSHA
			owner = restartOwnerWithInput(t, owner, freshInput)
			api := issueUpdateAppliedAPI(t, comments)
			coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
			batch := reconciliationV2Batch{}
			if request.Attempt == 0 {
				batch.IssueUpdates = []reconciliationIssueUpdateMaterial{material}
			} else {
				batch.Input.Issues = []internalgithub.RecoveryIssueFact{*material.Issue}
				if material.Attempt != nil {
					batch.Input.Attempts = []internalgithub.RecoveryAttemptFact{*material.Attempt}
				}
			}
			production := &productionReconciliation{owner: owner, effects: coordinator, collector: reconciliationV2Collector{Config: cfg}}
			if resumed, err := production.resumePendingReconciliation(t.Context(), api, batch); err != nil || !resumed {
				current := mustOwnerSnapshot(t, owner)
				t.Fatalf("resume %s: resumed=%v err=%v effect=%#v observation=%#v", name, resumed, err, current.State.Effects[effect.ID], current.State.Observations[ownerIssueKey("o/r", request.Issue)])
			}
			current, _ := owner.snapshot(t.Context())
			if current.State.Effects[effect.ID].State != "completed" {
				t.Fatalf("effect was not completed: %#v", current.State.Effects[effect.ID])
			}
		})
	}
}

func issueUpdateAppliedAPI(t *testing.T, comments map[int][]map[string]any) internalgithub.API {
	t.Helper()
	return internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		var body any
		switch {
		case strings.Contains(request.URL.Path, "/comments"):
			var number int
			_, _ = fmt.Sscanf(request.URL.Path, "/repos/o/r/issues/%d/comments", &number)
			body = comments[number]
		case strings.HasSuffix(request.URL.Path, "/issues/190"):
			body = map[string]any{"labels": []any{}}
		default:
			return nil, fmt.Errorf("unexpected GitHub request %s", request.URL.String())
		}
		encoded, _ := json.Marshal(body)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(encoded))), Request: request}, nil
	})}, Retries: -1}
}

func TestPlanReconciliationGovernanceBindsAcceptedAttempt(t *testing.T) {
	root := resolvedTempDir(t)
	state := newRuntimeOwnerState("o/r")
	manifest := ownerTestManifest(t, root, 183, 1, "running")
	issueKey, attemptKey := ownerIssueKey("o/r", 183), ownerAttemptKey("o/r", 183, 1)
	state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: manifest}
	owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(t.Context()) })
	issue := issueFact(183, "govern")
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 183, Attempt: 1, PR: 8, BaseSHA: manifest.BaseSHA, HeadSHA: strings.Repeat("b", 40), State: "active", PublicationConfirmed: true}
	issue.Attempt, issue.CurrentAttempt, issue.ActiveAttempt = 1, 1, &remote
	input := repositoryInput(true, issue)
	input.Attempts = []internalgithub.RecoveryAttemptFact{remote}
	accepted := applyReconciliationInput(t, owner, input)
	plans, err := planReconciliationGovernance(accepted, internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42})
	if err != nil || len(plans) != 1 || plans[0].Attempt == nil || plans[0].Attempt.HeadSHA != remote.HeadSHA {
		t.Fatalf("governance plans=%#v err=%v", plans, err)
	}
	if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request}); err != nil {
		t.Fatalf("owner rejected governance plan: %v", err)
	}
}

func TestGitHubBindPlannerSkipsExactObservedBinding(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "github-bind")
	_, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
	plans, err := planReconciliationBinds(snapshot, cfg)
	if err != nil || len(plans) != 0 {
		t.Fatalf("plans=%#v observation=%#v err=%v", plans, snapshot.State.Observations[ownerIssueKey("o/r", test.request.Issue)], err)
	}
}

func TestReviewerPlannerDoesNotLaunchUnrequestedPlanReview(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	_, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	issue := issueFact(test.request.Issue, "title")
	issue.Attempt = test.request.Attempt
	candidate := reviewerExecutionMaterial{Issue: issue, Source: "/source", HeadSHA: strings.Repeat("b", 40), Env: []string{"GH_TOKEN=one"}, Command: []string{"reviewer"}}
	plans, _, err := planReconciliationReviewers(snapshot, snapshot.State.Repository, []reviewerExecutionMaterial{candidate})
	if err != nil || len(plans) != 0 {
		t.Fatalf("unrequested running review plans=%#v err=%v", plans, err)
	}
}

func TestReviewerPlannerUsesVerifiedCompletedHeadAndFullEnvironmentDigest(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	test.request.Reviewer.Mode = agentruntime.ReviewModeImplementation
	root, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	issue := issueFact(test.request.Issue, "title")
	issue.Attempt = test.request.Attempt
	candidate := reviewerExecutionMaterial{Issue: issue, Source: "/source", HeadSHA: strings.Repeat("b", 40), Env: []string{"GH_TOKEN=one"}, Command: []string{"reviewer"}}
	plans, material, err := planReconciliationReviewers(snapshot, root, []reviewerExecutionMaterial{candidate})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.HeadSHA != candidate.HeadSHA {
		t.Fatalf("completed review plans=%#v err=%v", plans, err)
	}
	changed := material[ownerAttemptKey("o/r", test.request.Issue, test.request.Attempt)]
	changed.Env[0] = "GH_TOKEN=two"
	if reviewerExecutionDigest(plans[0].Request, changed) == plans[0].Request.ExecutionDigest {
		t.Fatal("changed effective environment reused reviewer authorization")
	}
	key := ownerAttemptKey("o/r", test.request.Issue, test.request.Attempt)
	record := snapshot.State.Attempts[key]
	oldHead := strings.Repeat("c", 40)
	record.Manifest.ReviewState, record.Manifest.ReviewMode = "findings-queued", agentruntime.ReviewModeImplementation
	record.Manifest.ReviewBase, record.Manifest.ReviewHead = record.Manifest.BaseSHA, oldHead
	record.Manifest.ReviewTarget = record.Manifest.BaseSHA + ".." + oldHead
	record.Manifest.ReviewHandoffQueued, record.Manifest.ReviewHandoffAck = true, true
	snapshot.State.Attempts[key] = record
	plans, _, err = planReconciliationReviewers(snapshot, root, []reviewerExecutionMaterial{candidate})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "run-observe" || plans[0].Request.Reviewer.HeadSHA != candidate.HeadSHA || !validReconciliationEffectStateBindings(root, snapshot.State, plans[0].Request) {
		t.Fatalf("new worker head after findings handoff plans=%#v err=%v", plans, err)
	}
}

func TestCompletedWorkerCleansPriorReviewBeforeStartingNewImplementationReview(t *testing.T) {
	for _, mode := range []string{agentruntime.ReviewModePlan, agentruntime.ReviewModeImplementation} {
		t.Run(mode, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, "reviewer-cleanup")
			test.request.Reviewer.Mode = mode
			root, owner, before := reconciliationEffectPersistentOwner(t, test.request)
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			state := cloneRuntimeOwnerState(before.State)
			key := ownerAttemptKey(test.request.Repository, test.request.Issue, test.request.Attempt)
			record := state.Attempts[key]
			record.Manifest.State = "completed"
			state.Attempts[key] = record
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			current := refreshOwnerObservation(t, owner, test.request.Issue)
			issue := issueFact(test.request.Issue, "title")
			issue.Attempt = test.request.Attempt
			newHead := strings.Repeat("c", 40)
			candidate := reviewerExecutionMaterial{Issue: issue, Source: "/source", HeadSHA: newHead, Command: []string{"reviewer"}}
			plans, _, err := planReconciliationReviewers(current, root, []reviewerExecutionMaterial{candidate})
			if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "cleanup" || plans[0].Request.Reviewer.Mode != mode {
				t.Fatalf("attached review must be cleaned before new reviewer: plans=%#v err=%v", plans, err)
			}
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request})
			if err != nil {
				t.Fatalf("owner rejected certified cleanup after worker completion: %v", err)
			}
			finished, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*effect), Result: test.result(plans[0].Request)})
			if err != nil {
				t.Fatalf("owner rejected certified cleanup completion: %v", err)
			}
			manifest := finished.State.Attempts[key].Manifest
			if manifest.ReviewSnapshot != "" || manifest.ReviewSession != "" || manifest.ReviewState != "clean" || manifest.ReviewMode != mode {
				t.Fatalf("cleanup did not detach only prior resources: %#v", manifest)
			}
			candidate.HeadSHA = manifest.ReviewHead
			plans, _, err = planReconciliationReviewers(finished, root, []reviewerExecutionMaterial{candidate})
			if err != nil || len(plans) != 0 {
				t.Fatalf("same reviewed head was scheduled again: plans=%#v err=%v", plans, err)
			}
			candidate.HeadSHA = newHead
			plans, _, err = planReconciliationReviewers(finished, root, []reviewerExecutionMaterial{candidate})
			if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "run-observe" || plans[0].Request.Reviewer.Mode != agentruntime.ReviewModeImplementation || plans[0].Request.Reviewer.HeadSHA != newHead {
				t.Fatalf("new worker head did not require implementation review: plans=%#v err=%v", plans, err)
			}
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request}); err != nil {
				t.Fatalf("owner rejected implementation review after certified cleanup: %v", err)
			}
		})
	}
}

func TestFailedImplementationReviewPersistsDiagnosticWithoutAutomaticRetry(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	test.request.Reviewer.Mode = agentruntime.ReviewModeImplementation
	owner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
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
	result := test.result(request)
	result.Reviewer.Status, result.Reviewer.Diagnostic = "failed", "reviewer exited without a valid result"
	launchPath, terminalPath := reviewerLifecyclePaths(request.Reviewer.Snapshot, request.Reviewer.Target)
	pane, err := parseReviewerPaneIdentity(reviewerPaneTestOutput("1|1|||", request.Reviewer.Session, "$9", os.Getpid(), "agent-symphony review-pane tmux "+launchPath+" "+terminalPath+" "+reviewerSignal(reviewerIdentity(identity))+" "+identity.RequestDigest))
	if err != nil {
		t.Fatal(err)
	}
	terminalIdentity := reviewerIdentity(identity)
	terminalIdentity.GateProtocol, terminalIdentity.SessionRequested, terminalIdentity.ChildPID = true, true, 99999999
	if _, err := owner.sealReviewerResult(t.Context(), sealReviewerResultCommand{Identity: identity, Result: result, Pane: pane, Terminal: reviewerTerminalRecord{Identity: terminalIdentity, ExitCode: 1}}); err != nil {
		t.Fatal(err)
	}
	finished, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: result})
	if err != nil {
		t.Fatal(err)
	}
	manifest := finished.State.Attempts[ownerAttemptKey(request.Repository, request.Issue, request.Attempt)].Manifest
	if manifest.ReviewState != "failed" || manifest.ReviewDiagnostic != result.Reviewer.Diagnostic {
		t.Fatalf("failed implementation review was not durable: %#v", manifest)
	}
	issue := issueFact(request.Issue, "title")
	issue.Attempt = request.Attempt
	candidate := reviewerExecutionMaterial{Issue: issue, Source: "/source", HeadSHA: request.Reviewer.HeadSHA, Env: []string{"GH_TOKEN=one"}, Command: []string{"reviewer"}}
	plans, _, err := planReconciliationReviewers(finished, owner.stateRoot, []reviewerExecutionMaterial{candidate})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer == nil || plans[0].Request.Reviewer.Phase != "cleanup" || plans[0].Request.Reviewer.RunID != request.Reviewer.RunID || attemptHasUnprovedReviewer(finished.State, request.Repository, request.Issue, request.Attempt) {
		t.Fatalf("failed reviewer was retried instead of scheduling exact cleanup: plans=%#v err=%v", plans, err)
	}
}

func TestInvalidatedImplementationSameHeadRequiresCertifiedCleanup(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	test.request.Reviewer.Mode, test.request.Reviewer.DigestVersion = agentruntime.ReviewModeImplementation, 1
	oldOwner, snapshot, request := reconciliationEffectTestOwner(t, test.request)
	root := oldOwner.stateRoot
	_, pending, err := oldOwner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	identity := ownerReconciliationEffectIdentity(*pending)
	if _, err := oldOwner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := oldOwner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	state := mustOwnerSnapshot(t, oldOwner).State
	issueKey := ownerIssueKey(request.Repository, request.Issue)
	observation := state.Observations[issueKey]
	observation.Generation++
	observation.Fact.BodyDigest = digestText("changed body")
	for key, attempt := range observation.Attempts {
		attempt.SourceIssueGeneration = observation.Generation
		observation.Attempts[key] = attempt
	}
	state.Observations[issueKey] = observation
	if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); !errors.Is(err, errStateConflict) {
		t.Fatalf("live H1 reviewer superseded without death proof: %v", err)
	}
	sealTestReviewerResult(t, oldOwner, *pending, test.result(request))
	state = mustOwnerSnapshot(t, oldOwner).State
	observation = state.Observations[issueKey]
	observation.Generation++
	observation.Fact.BodyDigest = digestText("changed body")
	for key, attempt := range observation.Attempts {
		attempt.SourceIssueGeneration = observation.Generation
		observation.Attempts[key] = attempt
	}
	state.Observations[issueKey] = observation
	if err := applySupersedePlanReview(&state, supersedePlanReviewCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if err := oldOwner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	key := ownerAttemptKey(request.Repository, request.Issue, request.Attempt)
	if manifest := state.Attempts[key].Manifest; manifest.ReviewState != "failed" || !manifest.ReviewInvalidated || manifest.ReviewSnapshot == "" || manifest.ReviewSession == "" {
		t.Fatalf("superseded H1 review did not retain exact failed resources: %#v", manifest)
	}
	persist := func(value runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), value)
	}
	owner, err := startTestStateOwner(t, root, state, persist)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.close(context.Background()) })
	current := refreshOwnerObservation(t, owner, request.Issue)
	issue := issueFact(request.Issue, "title")
	issue.Attempt, issue.Body = request.Attempt, "changed body"
	candidate := reviewerExecutionMaterial{Issue: issue, Source: "/source", HeadSHA: request.Reviewer.HeadSHA, Command: []string{"reviewer"}}
	plans, _, err := planReconciliationReviewers(current, root, []reviewerExecutionMaterial{candidate})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "cleanup" {
		t.Fatalf("invalidated reviewer resources must be cleaned first: plans=%#v err=%v", plans, err)
	}
	_, cleanup, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request})
	if err != nil {
		t.Fatal(err)
	}
	old := plans[0].Request.Reviewer
	resultRoot := filepath.Dir(reviewResultPath(old.Snapshot, old.Target))
	for _, path := range []string{old.Snapshot, resultRoot} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	proof := current.State.ReviewerProofs[reviewerProofKey(request.Repository, request.Issue, request.Attempt, old.Mode, old.Target)]
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||||||||\n"}}
	attempt := agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt, BaseSHA: old.BaseSHA}
	if err := cleanupBoundReviewResources(t.Context(), boundary, nil, attempt, old.HeadSHA, old.Target, old.RunID, old.Snapshot, old.Session, productionSnapshotRoot(root), activeWorkerProfileDigest(current.State), proof); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{old.Snapshot, resultRoot} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("certified cleanup left old reviewer resource %s: %v", path, err)
		}
	}
	cleaned, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*cleanup), Result: reconciliationEffectCaseNamed(t, "reviewer-cleanup").result(plans[0].Request)})
	if err != nil {
		t.Fatal(err)
	}
	manifest := cleaned.State.Attempts[key].Manifest
	if manifest.ReviewSnapshot != "" || manifest.ReviewSession != "" || !manifest.ReviewInvalidated {
		t.Fatalf("cleanup lost invalidation or retained old resources: %#v", manifest)
	}
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, request.Repository)
	if err != nil || !loaded.Attempts[key].Manifest.ReviewInvalidated || loaded.Attempts[key].Manifest.ReviewSnapshot != "" {
		t.Fatalf("restart lost invalidation or cleanup: err=%v manifest=%#v", err, loaded.Attempts[key].Manifest)
	}
	owner, err = startTestStateOwner(t, root, loaded, persist)
	if err != nil {
		t.Fatal(err)
	}
	plans, _, err = planReconciliationReviewers(refreshOwnerObservation(t, owner, request.Issue), root, []reviewerExecutionMaterial{candidate})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "run-observe" || plans[0].Request.Reviewer.HeadSHA != manifest.ReviewHead {
		t.Fatalf("certified invalidation did not admit same-head review: plans=%#v err=%v", plans, err)
	}
	_, review, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request})
	if err != nil {
		t.Fatalf("owner rejected same-head review after certified cleanup: %v", err)
	}
	identity = ownerReconciliationEffectIdentity(*review)
	if _, err := owner.markReviewerSessionRequested(t.Context(), markReviewerSessionRequestedCommand{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: identity, GroupPID: 99999998}); err != nil {
		t.Fatal(err)
	}
	reviewResult := reconciliationEffectCaseNamed(t, "reviewer-run-observe").result(plans[0].Request)
	sealTestReviewerResult(t, owner, *review, reviewResult)
	finished, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: identity, Result: reviewResult})
	if err != nil {
		t.Fatal(err)
	}
	if manifest := finished.State.Attempts[key].Manifest; manifest.ReviewState != "clean" || manifest.ReviewInvalidated {
		t.Fatalf("completed same-head review retained invalidation: %#v", manifest)
	}
}

func TestProoflessLegacyTerminalReviewerDoesNotPoisonReconciliation(t *testing.T) {
	for _, testCase := range []struct {
		state, diagnostic string
	}{
		{state: "clean"},
		{state: "findings-queued"},
		{state: "failed", diagnostic: "legacy reviewer failed"},
	} {
		t.Run(testCase.state, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, "reviewer-cleanup")
			test.request.Reviewer.Mode = agentruntime.ReviewModeImplementation
			root, oldOwner, before := reconciliationEffectPersistentOwner(t, test.request)
			if err := oldOwner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			state := cloneRuntimeOwnerState(before.State)
			key := ownerAttemptKey(test.request.Repository, test.request.Issue, test.request.Attempt)
			record := state.Attempts[key]
			record.Manifest.ReviewState, record.Manifest.ReviewDiagnostic = testCase.state, testCase.diagnostic
			if testCase.state == "findings-queued" {
				record.Manifest.ReviewFindings = []string{"legacy finding"}
			}
			state.Attempts[key] = record
			proofKey := reviewerProofKey(test.request.Repository, test.request.Issue, test.request.Attempt, record.Manifest.ReviewMode, record.Manifest.ReviewTarget)
			proof := state.ReviewerProofs[proofKey]
			delete(state.ReviewerProofs, proofKey)
			owner, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			current := refreshOwnerObservation(t, owner, test.request.Issue)
			issue := issueFact(test.request.Issue, "title")
			issue.Attempt = test.request.Attempt
			candidate := reviewerExecutionMaterial{Issue: issue, Source: "/source", HeadSHA: strings.Repeat("c", 40), Command: []string{"reviewer"}}
			plans, _, err := planReconciliationReviewers(current, root, []reviewerExecutionMaterial{candidate})
			if err != nil || len(plans) != 0 {
				t.Fatalf("proofless legacy cleanup was planned: plans=%#v err=%v", plans, err)
			}
			withProof := current
			withProof.State = cloneRuntimeOwnerState(current.State)
			withProof.State.ReviewerProofs[proofKey] = proof
			plans, _, err = planReconciliationReviewers(withProof, root, []reviewerExecutionMaterial{candidate})
			if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.Phase != "cleanup" {
				t.Fatalf("certified control plan was unavailable: plans=%#v err=%v", plans, err)
			}
			if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request}); !errors.Is(err, errStaleStateResult) {
				t.Fatalf("owner admitted proofless legacy cleanup: %v", err)
			}
			runner := reconciliationRunner{owner: owner, collect: func(context.Context, stateOwnerSnapshot) (reconciliationInput, error) {
				return reconciliationInput{Scope: issueScope(200), Complete: true, Issues: []internalgithub.RecoveryIssueFact{issueFact(200, "unrelated")}}, nil
			}}
			trigger, err := newReconciliationTriggerRunner(t.Context(), runner)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = trigger.shutdown(context.Background()) }()
			project, err := newProjectDashboardServerV2(t.Context(), root, "o/r", nil, "tmux", operatorTestMutationService(t, owner), 1, false, "")
			if err != nil {
				t.Fatal(err)
			}
			project.reconcile = trigger.triggerAndWait
			request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/reconcile", nil)
			request.Header.Set("Origin", "http://127.0.0.1")
			response := httptest.NewRecorder()
			project.webHandler().ServeHTTP(response, request)
			if response.Code != http.StatusNoContent {
				t.Fatalf("unrelated manual Reconcile HTTP %d: %s", response.Code, response.Body.String())
			}
			after := mustOwnerSnapshot(t, owner)
			manifest := after.State.Attempts[key].Manifest
			if manifest.ReviewState != testCase.state || manifest.ReviewDiagnostic != testCase.diagnostic || manifest.ReviewSnapshot == "" || manifest.ReviewSession == "" {
				t.Fatalf("proofless legacy reviewer was changed or hidden: %#v", manifest)
			}
			for _, effect := range after.State.Effects {
				if effect.State == "pending" && effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer.Phase == "cleanup" {
					t.Fatalf("unfinishable cleanup intent was persisted: %#v", effect)
				}
			}
		})
	}
}

func TestPartialLegacyReviewerBindingDoesNotPoisonSameIssueManualReconcile(t *testing.T) {
	for _, missing := range []string{"snapshot", "session"} {
		t.Run(missing, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, "reviewer-cleanup")
			root, oldOwner, before := reconciliationEffectPersistentOwner(t, test.request)
			if err := oldOwner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			state := cloneRuntimeOwnerState(before.State)
			key := ownerAttemptKey(test.request.Repository, test.request.Issue, test.request.Attempt)
			record := state.Attempts[key]
			if missing == "snapshot" {
				record.Manifest.ReviewSnapshot = ""
			} else {
				record.Manifest.ReviewSession = ""
			}
			state.Attempts[key] = record
			proof := state.ReviewerProofs[reviewerProofKey(test.request.Repository, test.request.Issue, test.request.Attempt, record.Manifest.ReviewMode, record.Manifest.ReviewTarget)]
			if !proof.DeadProved {
				t.Fatal("fixture lacks exact reviewer death proof")
			}
			owner, err := startTestStateOwner(t, root, state, func(value runtimeOwnerState) error {
				return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), value)
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.close(context.Background()) })
			refreshOwnerObservation(t, owner, test.request.Issue)
			if err := owner.close(t.Context()); err != nil {
				t.Fatal(err)
			}
			loaded, err := readRuntimeOwnerState(root, test.request.Repository)
			if err != nil {
				t.Fatal(err)
			}
			owner, err = startTestStateOwner(t, root, loaded, func(value runtimeOwnerState) error {
				return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), value)
			})
			if err != nil {
				t.Fatal(err)
			}
			issue := issueFact(test.request.Issue, "title")
			issue.Attempt = test.request.Attempt
			candidate := reviewerExecutionMaterial{Issue: issue, HeadSHA: strings.Repeat("c", 40), Command: []string{"reviewer"}}
			production := &productionReconciliation{owner: owner, effects: &runtimeEffectCoordinator{owner: owner}, stateRoot: root}
			trigger, err := newProductionReconciliationTriggerRunner(t.Context(), func(ctx context.Context) error {
				return production.runReviewerPhase(ctx, []reviewerExecutionMaterial{candidate})
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = trigger.shutdown(context.Background()) })
			project, err := newProjectDashboardServerV2(t.Context(), root, "o/r", nil, "tmux", operatorTestMutationService(t, owner), 1, false, "")
			if err != nil {
				t.Fatal(err)
			}
			project.reconcile = trigger.triggerAndWait
			request := httptest.NewRequest(http.MethodPost, "http://127.0.0.1/actions/reconcile", nil)
			request.Header.Set("Origin", "http://127.0.0.1")
			response := httptest.NewRecorder()
			project.webHandler().ServeHTTP(response, request)
			if response.Code != http.StatusNoContent {
				t.Fatalf("same-issue manual Reconcile HTTP %d: %s", response.Code, response.Body.String())
			}
			after := mustOwnerSnapshot(t, owner)
			if !reflect.DeepEqual(after.State.Attempts[key].Manifest, record.Manifest) {
				t.Fatalf("partial legacy reviewer binding changed: %#v", after.State.Attempts[key].Manifest)
			}
			for _, effect := range after.State.Effects {
				if effect.State == "pending" && effect.Reconciliation != nil && effect.Reconciliation.Action == reconciliationReviewer && effect.Reconciliation.Reviewer.Phase == "cleanup" {
					t.Fatalf("unfinishable cleanup intent persisted: %#v", effect)
				}
			}
		})
	}
}

type reviewerSessionStillPresentBoundary struct{}

func (reviewerSessionStillPresentBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" {
		return agentruntime.Result{}, errors.New("unexpected boundary operation")
	}
	return agentruntime.Result{Exited: true, Code: 0}, nil
}

func TestPriorPolicyPendingReviewerRestartLaunchesNoExternalWork(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	request := bindEffectObservation(snapshot, test.request)
	material := reviewerExecutionMaterial{Issue: issueFact(request.Issue, "title"), Source: root, HeadSHA: request.Reviewer.HeadSHA, Env: []string{"GH_TOKEN=value"}, Command: []string{"reviewer"}}
	material.Issue.Attempt = request.Attempt
	request.ExecutionDigest = reviewerExecutionDigest(request, material)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	state := cloneRuntimeOwnerState(mustOwnerSnapshot(t, owner).State)
	if err := owner.close(t.Context()); err != nil {
		t.Fatal(err)
	}
	legacy := state.Effects[effect.ID]
	delete(state.Effects, effect.ID)
	legacy.ReviewerConfinementVersion = 0
	legacy.ID = runtimeEffectID(legacy)
	state.Effects[legacy.ID] = legacy
	state.ReviewerPolicyTracked, state.ReviewerPolicyVersion = false, 0
	preflight := cloneRuntimeOwnerState(state)
	migrateReviewerPolicy(&preflight)
	if !validPersistedReconciliationEffect(preflight, preflight.Effects[legacy.ID]) {
		migrated := preflight.Effects[legacy.ID]
		t.Fatalf("migrated prior-policy effect is invalid: quarantine=%q id=%t request_digest=%t request=%t source=%d intent=%d run=%q want_run=%q", preflight.LegacyReviewerQuarantines[ownerIssueKey(request.Repository, request.Issue)], migrated.ID == runtimeEffectID(migrated), migrated.RequestDigest == reconciliationEffectDigest(*migrated.Reconciliation), validReconciliationEffectRequest(preflight.Repository, *migrated.Reconciliation), migrated.ReviewerSourceRevision, migrated.IntentRevision, migrated.Reconciliation.Reviewer.RunID, reviewerRunID(migrated.IntentEpoch, migrated.ReviewerSourceRevision, migrated.IssueGeneration, migrated.AttemptGeneration, migrated.Repository, migrated.Issue, migrated.Attempt, migrated.Reconciliation.Reviewer.Mode, migrated.Reconciliation.Reviewer.Target))
	}
	body, err := json.Marshal(state)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, runtimeOwnerStateFile), body, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := readRuntimeOwnerState(root, state.Repository)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := startTestStateOwner(t, root, loaded, func(next runtimeOwnerState) error {
		return writeRuntimeOwnerState(root, runtimeOwnerAttemptRoot(root), next)
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.close(context.Background()) })
	current := mustOwnerSnapshot(t, restarted).State
	if current.ReviewerPolicyVersion != reviewerConfinementVersion || current.LegacyReviewerQuarantines[ownerIssueKey(request.Repository, request.Issue)] == "" {
		t.Fatalf("prior reviewer policy was not quarantined on restart: version=%d quarantine=%q", current.ReviewerPolicyVersion, current.LegacyReviewerQuarantines[ownerIssueKey(request.Repository, request.Issue)])
	}
	boundary := &operatorBoundaryRecorder{}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: restarted, active: map[string]*activeRuntimeEffect{}}
	plan := reconciliationPlannedEffect{Identity: ownerReconciliationEffectIdentity(current.Effects[legacy.ID]), Request: *current.Effects[legacy.ID].Reconciliation}
	if _, _, err := coordinator.executeReviewer(t.Context(), boundary, plan, material); !errors.Is(err, errStateConflict) {
		t.Fatalf("prior-policy reviewer replay err=%v", err)
	}
	if operations := boundary.operations(); len(operations) != 0 {
		t.Fatalf("prior-policy reviewer crossed the external boundary: %v", operations)
	}
	if _, err := os.Lstat(request.Reviewer.Snapshot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("prior-policy reviewer created a snapshot: %v", err)
	}
}

func TestReviewerCleanupRequiresExactSessionAbsenceBeforeFinish(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-cleanup")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	request := bindEffectObservation(snapshot, test.request)
	material := reviewerExecutionMaterial{Issue: issueFact(request.Issue, "title"), Env: []string{"GH_TOKEN=value"}, Command: []string{"reviewer"}}
	material.Issue.Attempt = request.Attempt
	request.ExecutionDigest = reviewerExecutionDigest(request, material)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	plan := reconciliationPlannedEffect{Identity: ownerReconciliationEffectIdentity(*effect), Request: request}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	if _, _, err := coordinator.executeReviewer(t.Context(), reviewerSessionStillPresentBoundary{}, plan, material); err == nil {
		t.Fatal("reviewer cleanup completed while the exact tmux session remained")
	}
	if _, err := os.Lstat(filepath.Join(root, "reconciliation-effects", effect.ID+".done")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("cleanup marker exists before absence proof: %v", err)
	}
	current, _ := owner.snapshot(t.Context())
	if current.State.Effects[effect.ID].State != "pending" {
		t.Fatalf("cleanup effect was completed: %#v", current.State.Effects[effect.ID])
	}
}

func TestCompletedReviewerCleanupDoesNotReplan(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-cleanup")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	request := bindEffectObservation(snapshot, test.request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	finished, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*effect), Result: test.result(request)})
	if err != nil {
		t.Fatal(err)
	}
	material := reviewerExecutionMaterial{Issue: issueFact(request.Issue, "title"), Source: root, HeadSHA: request.Reviewer.HeadSHA, Command: []string{"reviewer"}}
	material.Issue.Attempt = request.Attempt
	plans, _, err := planReconciliationReviewers(finished, root, []reviewerExecutionMaterial{material})
	if err != nil || len(plans) != 0 {
		t.Fatalf("completed cleanup replanned: plans=%#v err=%v", plans, err)
	}
}

func TestReviewerCleanupPlanningRequiresExactRunGenerationAndProof(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "reviewer-cleanup")
	root, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	proofKey := reviewerProofKey(test.request.Repository, test.request.Issue, test.request.Attempt, test.request.Reviewer.Mode, test.request.Reviewer.Target)
	state := cloneRuntimeOwnerState(snapshot.State)
	proof := state.ReviewerProofs[proofKey]
	proof.NeverRan, proof.DeadProved, proof.GroupPID = false, true, 99999999
	proof.ProfileDigest = activeWorkerProfileDigest(state)
	proof.ConfinementVersion = reviewerConfinementVersion
	proof.IssueGeneration = 1 // The immutable old run may predate the current issue generation.
	issueKey := ownerIssueKey(test.request.Repository, test.request.Issue)
	state.IssueGenerations[issueKey] = 2
	observation := state.Observations[issueKey]
	observation.OwnerGeneration = 2
	state.Observations[issueKey] = observation
	state.ReviewerProofs[proofKey] = proof
	if err := validateRuntimeOwnerState(state, runtimeOwnerAttemptRoot(root), root, false); err != nil {
		t.Fatalf("historical proof fixture is not a valid owner snapshot: %v", err)
	}
	snapshot.State = state
	material := reviewerExecutionMaterial{Issue: issueFact(test.request.Issue, "title"), Source: root, HeadSHA: test.request.Reviewer.HeadSHA, Command: []string{"reviewer"}}
	material.Issue.Attempt = test.request.Attempt
	plans, _, err := planReconciliationReviewers(snapshot, root, []reviewerExecutionMaterial{material})
	if err != nil || len(plans) != 1 || plans[0].Request.Reviewer.RunID != proof.RunID || !validReconciliationEffectStateBindings(root, state, plans[0].Request) {
		t.Fatalf("exact ran reviewer proof did not authorize cleanup: plans=%#v err=%v", plans, err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*reviewerProcessProof)
	}{
		{name: "not dead", mutate: func(value *reviewerProcessProof) { value.DeadProved = false }},
		{name: "stale attempt generation", mutate: func(value *reviewerProcessProof) { value.AttemptGeneration++ }},
		{name: "mismatched run", mutate: func(value *reviewerProcessProof) { value.RunID = digestText("different run") }},
		{name: "unsupported confinement", mutate: func(value *reviewerProcessProof) { value.ConfinementVersion++ }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			changed := cloneRuntimeOwnerState(state)
			value := changed.ReviewerProofs[proofKey]
			tc.mutate(&value)
			changed.ReviewerProofs[proofKey] = value
			changedSnapshot := snapshot
			changedSnapshot.State = changed
			got, _, planErr := planReconciliationReviewers(changedSnapshot, root, []reviewerExecutionMaterial{material})
			if planErr != nil {
				t.Fatal(planErr)
			}
			if len(got) != 0 || validReconciliationEffectStateBindings(root, changed, plans[0].Request) {
				t.Fatalf("invalid proof authorized cleanup: plans=%#v", got)
			}
		})
	}
}

type appliedHandoffBoundary struct{ handoff *handoffEffectRequest }

func (b appliedHandoffBoundary) call(_ context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if operation != "verify-handoff" {
		return agentruntime.Result{}, errors.New("unexpected boundary operation")
	}
	body, _ := json.Marshal(handoffReceipt{Type: "agent-symphony-handoff-executed-v1", Key: b.handoff.Key, OutcomePath: b.handoff.OutcomePath, OutcomeToken: b.handoff.OutcomeToken})
	return agentruntime.Result{Output: string(body)}, nil
}

func workerBoundaryResult(t *testing.T, output string) workerBoundaryRunner {
	t.Helper()
	return workerBoundaryResultValue(t, agentruntime.Result{Output: output})
}

func workerBoundaryResultValue(t *testing.T, result agentruntime.Result) workerBoundaryRunner {
	t.Helper()
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return workerBoundaryRunner{Command: "/bin/sh", Args: []string{"-c", `cat >/dev/null; printf %s "$BOUNDARY_RESULT"`}, Env: []string{"BOUNDARY_RESULT=" + string(encoded)}}
}

func TestHandoffPlannerExecutorCommitsExactReviewAndRecoveryAcknowledgements(t *testing.T) {
	for _, name := range []string{"handoff-review-findings", "handoff-recovery"} {
		t.Run(name, func(t *testing.T) {
			test := reconciliationEffectCaseNamed(t, name)
			_, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
			plans, materials, err := planReconciliationHandoffs(snapshot, []string{"worker"}, []string{"follow instructions"})
			if err != nil || len(plans) != 1 {
				t.Fatalf("plans=%#v err=%v", plans, err)
			}
			coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
			plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
			if err != nil {
				t.Fatal(err)
			}
			owner = restartOwnerWithInput(t, owner, reconciliationEffectObservationInput(plan.Request, "title"))
			coordinator.owner = owner
			cfg := config.Default("o/r")
			cfg.Commands.Implementation = []string{"worker"}
			ack, _ := json.Marshal(handoffReceipt{Type: "agent-symphony-handoff-executed-v1", Key: plan.Request.Handoff.Key, OutcomePath: plan.Request.Handoff.OutcomePath, OutcomeToken: plan.Request.Handoff.OutcomeToken})
			production := &productionReconciliation{owner: owner, effects: coordinator, stateRoot: owner.stateRoot, config: cfg, implementation: workerBoundaryResult(t, string(ack))}
			if resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{}); err != nil || !resumed {
				t.Fatalf("resume handoff=%v err=%v material=%#v", resumed, err, materials)
			}
			key := ownerAttemptKey(plan.Request.Repository, plan.Request.Issue, plan.Request.Attempt)
			current, _ := owner.snapshot(t.Context())
			if current.State.Effects[plan.Identity.EffectID].State != "completed" {
				t.Fatalf("handoff effect pending: %#v", current.State.Effects[plan.Identity.EffectID])
			}
			if name == "handoff-review-findings" && !current.State.Attempts[key].Manifest.ReviewHandoffAck {
				t.Fatal("review handoff acknowledgement was not applied")
			}
			if name == "handoff-recovery" && !current.State.Recoveries[key].State.HandoffReceipts[plan.Request.Handoff.Key] {
				t.Fatal("recovery handoff receipt was not applied")
			}
		})
	}
}

func TestPublicationPlannerBeginsInitialAndCompletedOutcomeTransitions(t *testing.T) {
	t.Run("initial", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "github-publish")
		root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		manifest := snapshot.State.Attempts[ownerAttemptKey("o/r", test.request.Issue, test.request.Attempt)].Manifest
		candidate := publicationExecutionMaterial{Issue: issueFact(test.request.Issue, "title"), Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Root: root, Head: manifest.ReviewHead, Validation: "tests", Documentation: "none"}
		candidate.Issue.Attempt, candidate.Issue.BaseBranch = test.request.Attempt, "main"
		plans, _, err := planReconciliationPublications(snapshot, []publicationExecutionMaterial{candidate})
		if err != nil || len(plans) != 1 || plans[0].Request.GitHubPublish.Prepared != nil {
			t.Fatalf("initial plans=%#v err=%v", plans, err)
		}
		if _, _, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request}); err != nil {
			t.Fatalf("begin initial publication: %v request=%v current=%v binding=%v", err, validReconciliationEffectRequest(snapshot.State.Repository, plans[0].Request), reconciliationObservationCurrent(snapshot.State, plans[0].Request), validReconciliationEffectStateBindings(root, snapshot.State, plans[0].Request))
		}
	})

	t.Run("completed handoff outcome", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "handoff-recovery-outcome")
		root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		request := bindEffectObservation(snapshot, test.request)
		_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
		if err != nil {
			t.Fatal(err)
		}
		finished, err := owner.finishReconciliationEffect(t.Context(), finishReconciliationEffectCommand{Identity: ownerReconciliationEffectIdentity(*effect), Result: test.result(request)})
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.close(context.Background()); err != nil {
			t.Fatal(err)
		}
		state := cloneRuntimeOwnerState(finished.State)
		key := ownerAttemptKey("o/r", request.Issue, request.Attempt)
		record := state.Attempts[key]
		newHead := strings.Repeat("c", 40)
		record.Manifest.State, record.Manifest.ReviewState, record.Manifest.ReviewMode = "completed", "clean", agentruntime.ReviewModeImplementation
		record.Manifest.ReviewBase, record.Manifest.ReviewHead, record.Manifest.ReviewTarget = record.Manifest.BaseSHA, newHead, record.Manifest.BaseSHA+".."+newHead
		record.Manifest.ReviewRunCleaned = true
		state.Attempts[key] = record
		restarted, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = restarted.close(context.Background()) })
		current := refreshOwnerObservation(t, restarted, request.Issue)
		candidate := publicationExecutionMaterial{Issue: issueFact(request.Issue, "title"), Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Root: root, Head: newHead, Validation: "tests", Documentation: "none"}
		candidate.Issue.Attempt, candidate.Issue.BaseBranch = request.Attempt, "main"
		plans, _, err := planReconciliationPublications(current, []publicationExecutionMaterial{candidate})
		if err != nil || len(plans) != 1 || plans[0].Request.GitHubPublish.Prepared == nil || plans[0].Request.GitHubPublish.Prepared.Handoff.Key != request.Handoff.Key {
			t.Fatalf("prepared plans=%#v err=%v", plans, err)
		}
		begun, _, err := restarted.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request})
		if err != nil {
			t.Fatalf("begin prepared publication: %v request=%v current=%v binding=%v", err, validReconciliationEffectRequest(current.State.Repository, plans[0].Request), reconciliationObservationCurrent(current.State, plans[0].Request), validReconciliationEffectStateBindings(root, current.State, plans[0].Request))
		}
		prepared := begun.State.Recoveries[key].State.PreparedPublication
		if prepared == nil || !reflect.DeepEqual(*prepared, *plans[0].Request.GitHubPublish.Prepared) {
			t.Fatalf("prepared transition was not committed: %#v", prepared)
		}
	})

	t.Run("completed delivery receipt", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "handoff-recovery")
		root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		handoffs, materials, err := planReconciliationHandoffs(snapshot, []string{"worker"}, nil)
		if err != nil || len(handoffs) != 1 {
			t.Fatalf("handoff plans=%#v err=%v", handoffs, err)
		}
		coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
		plan, err := coordinator.beginReconciliation(t.Context(), handoffs[0])
		if err != nil {
			t.Fatal(err)
		}
		key := ownerAttemptKey(plan.Request.Repository, plan.Request.Issue, plan.Request.Attempt)
		if _, err := coordinator.executeHandoff(t.Context(), appliedHandoffBoundary{plan.Request.Handoff}, plan, materials[key]); err != nil {
			t.Fatal(err)
		}
		finished, _ := owner.snapshot(t.Context())
		if err := owner.close(context.Background()); err != nil {
			t.Fatal(err)
		}
		state := cloneRuntimeOwnerState(finished.State)
		record := state.Attempts[key]
		newHead := strings.Repeat("c", 40)
		record.Manifest.State, record.Manifest.ReviewState, record.Manifest.ReviewMode = "completed", "clean", agentruntime.ReviewModeImplementation
		record.Manifest.ReviewBase, record.Manifest.ReviewHead, record.Manifest.ReviewTarget = record.Manifest.BaseSHA, newHead, record.Manifest.BaseSHA+".."+newHead
		record.Manifest.ReviewRunCleaned = true
		state.Attempts[key] = record
		restarted, err := startTestStateOwner(t, root, state, func(runtimeOwnerState) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = restarted.close(context.Background()) })
		current := refreshOwnerObservation(t, restarted, plan.Request.Issue)
		candidate := publicationExecutionMaterial{Issue: issueFact(plan.Request.Issue, "title"), Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Root: root, Head: newHead, Validation: "tests", Documentation: "none"}
		candidate.Issue.Attempt, candidate.Issue.BaseBranch = plan.Request.Attempt, "main"
		plans, _, err := planReconciliationPublications(current, []publicationExecutionMaterial{candidate})
		want := completedHandoffPublicationOutcome(*plan.Request.Handoff.Recovery, newHead)
		if err != nil || len(plans) != 1 || plans[0].Request.GitHubPublish.Prepared == nil || !reflect.DeepEqual(plans[0].Request.GitHubPublish.Prepared.Outcome, want) {
			t.Fatalf("receipt publication plans=%#v err=%v", plans, err)
		}
		ambiguous := current
		ambiguous.State = cloneRuntimeOwnerState(current.State)
		for _, effect := range ambiguous.State.Effects {
			duplicate := effect
			duplicate.ID += "-duplicate"
			ambiguous.State.Effects[duplicate.ID] = duplicate
			break
		}
		if _, _, err := planReconciliationPublications(ambiguous, []publicationExecutionMaterial{candidate}); err == nil {
			t.Fatal("publication planner accepted ambiguous completed handoff receipts")
		}
		if _, _, err := restarted.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: plans[0].Identity, Request: plans[0].Request}); err != nil {
			t.Fatalf("begin receipt publication: %v", err)
		}
	})
}

func TestMapBackedReconciliationPlannersReturnStableOrder(t *testing.T) {
	assertOrder := func(t *testing.T, plans []reconciliationPlannedEffect, err error) {
		t.Helper()
		if err != nil || len(plans) != 2 || plans[0].Request.Issue != 189 || plans[1].Request.Issue != 190 {
			t.Fatalf("plans=%#v err=%v", plans, err)
		}
	}
	t.Run("handoffs", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "handoff-review-findings")
		_, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		snapshot = addPlanningTwin(snapshot, 189)
		plans, _, err := planReconciliationHandoffs(snapshot, []string{"worker"}, nil)
		assertOrder(t, plans, err)
	})
	t.Run("reviewers", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "reviewer-run-observe")
		test.request.Reviewer.Mode = agentruntime.ReviewModeImplementation
		root, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		snapshot = addPlanningTwin(snapshot, 189)
		candidates := []reviewerExecutionMaterial{
			{Issue: issueFact(190, "title"), Source: "/source-190", HeadSHA: strings.Repeat("b", 40), Env: []string{"GH_TOKEN=value"}, Command: []string{"reviewer"}},
			{Issue: issueFact(189, "title"), Source: "/source-189", HeadSHA: strings.Repeat("b", 40), Env: []string{"GH_TOKEN=value"}, Command: []string{"reviewer"}},
		}
		for index := range candidates {
			candidates[index].Issue.Attempt = 1
		}
		plans, _, err := planReconciliationReviewers(snapshot, root, candidates)
		assertOrder(t, plans, err)
	})
	t.Run("publications", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "github-publish")
		root, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		snapshot = addPlanningTwin(snapshot, 189)
		var candidates []publicationExecutionMaterial
		for _, issue := range []int{190, 189} {
			manifest := snapshot.State.Attempts[ownerAttemptKey("o/r", issue, 1)].Manifest
			fact := issueFact(issue, "title")
			fact.Attempt, fact.BaseBranch = 1, "main"
			candidates = append(candidates, publicationExecutionMaterial{Issue: fact, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, Root: root, Head: manifest.ReviewHead, Validation: "tests", Documentation: "none"})
		}
		plans, _, err := planReconciliationPublications(snapshot, candidates)
		assertOrder(t, plans, err)
	})
	t.Run("retirements", func(t *testing.T) {
		test := reconciliationEffectCaseNamed(t, "retire-completed")
		_, _, snapshot := reconciliationEffectPersistentOwner(t, test.request)
		snapshot = addPlanningTwin(snapshot, 189)
		assertOrder(t, planReconciliationRetirements(snapshot), nil)
	})
}

func addPlanningTwin(snapshot stateOwnerSnapshot, issue int) stateOwnerSnapshot {
	state := cloneRuntimeOwnerState(snapshot.State)
	var sourceKey string
	var source runtimeAttemptRecord
	for key, record := range state.Attempts {
		sourceKey, source = key, record
		break
	}
	oldIssueKey := ownerIssueKey(source.Manifest.Repository, source.Manifest.Issue)
	oldObservation := state.Observations[oldIssueKey]
	source.Manifest.Issue = issue
	newIssueKey := ownerIssueKey(source.Manifest.Repository, issue)
	newAttemptKey := ownerAttemptKey(source.Manifest.Repository, issue, source.Manifest.Attempt)
	state.IssueGenerations[newIssueKey] = state.IssueGenerations[oldIssueKey]
	state.AttemptGenerations[newAttemptKey] = state.AttemptGenerations[sourceKey]
	state.Attempts[newAttemptKey] = source
	observation := cloneReconciliationObservation(oldObservation)
	observation.Fact.Issue = issue
	if observation.Fact.ActiveAttempt != nil {
		observation.Fact.ActiveAttempt.Issue = issue
	}
	for index := range observation.Fact.TerminalAttempts {
		observation.Fact.TerminalAttempts[index].Issue = issue
	}
	observation.Attempts = map[string]reconciliationAttemptObservation{}
	for _, attempt := range oldObservation.Attempts {
		attempt.Fact.Issue = issue
		observation.Attempts[newAttemptKey] = attempt
	}
	state.Observations[newIssueKey] = observation
	if recovery, ok := state.Recoveries[sourceKey]; ok {
		recovery.State.Issue = issue
		state.Recoveries[newAttemptKey] = recovery
	}
	snapshot.State = state
	return snapshot
}

func TestPushPublishedHeadUsesCapturedObjectWhenHEADDiffers(t *testing.T) {
	repository := resolvedTempDir(t)
	originParent := resolvedTempDir(t)
	origin := filepath.Join(originParent, "origin.git")
	runGit := func(args ...string) string {
		t.Helper()
		output, err := exec.Command("git", args...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, output)
		}
		return strings.TrimSpace(string(output))
	}
	runGit("-C", repository, "init", "-q", "-b", "main")
	runGit("-C", repository, "config", "user.email", "test@example.com")
	runGit("-C", repository, "config", "user.name", "Test")
	if err := os.WriteFile(filepath.Join(repository, "file"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("-C", repository, "add", "file")
	runGit("-C", repository, "commit", "-q", "-m", "base")
	base := runGit("-C", repository, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(repository, "file"), []byte("worker"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit("-C", repository, "commit", "-q", "-am", "worker")
	head := runGit("-C", repository, "rev-parse", "HEAD")
	runGit("-C", repository, "checkout", "-q", base)
	runGit("init", "-q", "--bare", origin)
	runGit("-C", repository, "remote", "add", "origin", origin)
	if err := pushPublishedHead(t.Context(), repository, head, "agent/190-1"); err != nil {
		t.Fatal(err)
	}
	if got := runGit("--git-dir", origin, "rev-parse", "refs/heads/agent/190-1"); got != head {
		t.Fatalf("published=%s want captured=%s (HEAD=%s)", got, head, base)
	}
}

func TestRetirementFinishRemovesLocalAuthorityAndDoesNotReplan(t *testing.T) {
	test := reconciliationEffectCaseNamed(t, "retire-completed")
	root, owner, snapshot := reconciliationEffectPersistentOwner(t, test.request)
	plans := planReconciliationRetirements(snapshot)
	if len(plans) != 1 {
		t.Fatalf("retirement plans=%#v", plans)
	}
	coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	owner = restartOwnerWithInput(t, owner, reconciliationEffectObservationInput(plan.Request, "title"))
	coordinator.owner = owner
	production := &productionReconciliation{owner: owner, effects: coordinator, implementation: workerBoundaryResultValue(t, agentruntime.Result{Exited: true, Code: 1, Output: "can't find session: " + plan.Request.Manifest.Session})}
	if resumed, err := production.resumePendingReconciliation(t.Context(), internalgithub.API{}, reconciliationV2Batch{}); err != nil || !resumed {
		current := mustOwnerSnapshot(t, owner)
		t.Fatalf("resume=%v err=%v effects=%#v proofs=%#v", resumed, err, current.State.Effects, current.State.ReviewerProofs)
	}
	finished, _ := owner.snapshot(t.Context())
	key := ownerAttemptKey(plan.Request.Repository, plan.Request.Issue, plan.Request.Attempt)
	if _, exists := finished.State.Attempts[key]; exists || len(planReconciliationRetirements(finished)) != 0 {
		t.Fatalf("retired local authority survived: %#v", finished.State.Attempts[key])
	}
	status, err := projectOwnerStatus(finished, 1, time.Unix(20, 0))
	if err != nil || !slices.ContainsFunc(status.Statuses, func(value orchestrator.RecoveryStatus) bool {
		return value.Issue == plan.Request.Issue && value.State == "completed"
	}) {
		t.Fatalf("remote completion missing after retirement: %#v err=%v", status.Statuses, err)
	}
	if matches, _ := filepath.Glob(filepath.Join(root, "reconciliation-effects", "*.done")); len(matches) != 0 {
		t.Fatalf("retirement completion marker=%v", matches)
	}
}

type absentSessionBoundary struct{}

func (absentSessionBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" && command.Name == "tmux" && slices.Contains(command.Args, "has-session") {
		target := strings.TrimPrefix(command.Args[len(command.Args)-1], "=")
		return agentruntime.Result{Exited: true, Code: 1, Output: "can't find session: " + target}, errors.New("session not found")
	}
	return agentruntime.Result{}, errors.New("unexpected boundary operation")
}

type retirementDirBoundary struct{ dir string }

func (b *retirementDirBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || !slices.Contains(command.Args, "has-session") {
		return agentruntime.Result{}, errors.New("unexpected boundary operation")
	}
	b.dir = command.Dir
	target := strings.TrimPrefix(command.Args[len(command.Args)-1], "=")
	return agentruntime.Result{Exited: true, Code: 1, Output: "can't find session: " + target}, errors.New("session not found")
}

func TestRetirementSessionProbeUsesImplementationBoundaryRoot(t *testing.T) {
	root := resolvedTempDir(t)
	manifest := ownerTestManifest(t, root, 191, 1, "completed")
	boundary := &retirementDirBoundary{}
	gone, err := retiredResourcesGone(t.Context(), boundary, manifest, root)
	if err != nil || !gone || boundary.dir != productionAttemptRoot(root) {
		t.Fatalf("gone=%v dir=%q want=%q err=%v", gone, boundary.dir, productionAttemptRoot(root), err)
	}
}

func TestIssueUpdateRevalidationCancelsAfterOwnerInvalidationWithoutMutation(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	body := "snapshot"
	digest := sha256.Sum256([]byte(body))
	proposal := reconciliationIssueUpdateProposal{Repository: "o/r", Issue: 184, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: hex.EncodeToString(digest[:])}
	input := repositoryInput(true, issueFact(184, "proposal"))
	input.IssueUpdates = []reconciliationIssueUpdateProposal{proposal}
	accepted := applyReconciliationInput(t, owner, input)
	batch := reconciliationV2Batch{IssueUpdates: []reconciliationIssueUpdateMaterial{{Proposal: proposal, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, ControlSnapshotBody: body}}}
	plans, err := planReconciliationIssueUpdates(accepted, batch)
	if err != nil || len(plans) != 1 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	var mutations atomic.Int32
	api := internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			mutations.Add(1)
		}
		select {
		case <-started:
		default:
			close(started)
		}
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}, Retries: -1}
	done := make(chan error, 1)
	go func() {
		_, err := coordinator.executeIssueUpdate(context.Background(), api, plan)
		done <- err
	}()
	<-started
	invalidated, err := owner.advanceIssueGeneration(t.Context(), advanceIssueGenerationCommand{Repository: "o/r", Issue: 184, ExpectedGeneration: accepted.State.IssueGenerations[ownerIssueKey("o/r", 184)]})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.cancelInvalidated(invalidated)
	if err := <-done; !errors.Is(err, context.Canceled) || mutations.Load() != 0 {
		t.Fatalf("execution err=%v mutations=%d", err, mutations.Load())
	}
}

func TestProvenRetrySurvivesObservationOnlyInvalidation(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-retry").request
	_, owner, snapshot := reconciliationEffectPersistentOwner(t, request)
	request = bindEffectObservation(snapshot, request)
	issue := issueFact(request.Issue, "title")
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42, CancelCommand: "/cancel", RetryCommand: "/retry"}
	material := reconciliationIssueUpdateMaterial{Issue: &issue, Config: cfg}
	request.ExecutionDigest = issueUpdateExecutionDigest(request, material)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	failedAt := time.Unix(0, request.GitHubIssueUpdate.FailedAtUnixNano)
	terminal, err := internalgithub.TerminalFailureMarker(request.Issue, request.Attempt, failedAt)
	if err != nil {
		t.Fatal(err)
	}
	comment := func(id int, body string, at time.Time) map[string]any {
		return map[string]any{"id": id, "body": body, "created_at": at, "updated_at": at, "user": map[string]any{"id": 42}}
	}
	comments := []map[string]any{comment(1, terminal, failedAt), comment(2, cfg.RetryCommand, failedAt.Add(time.Second))}
	var reads atomic.Int32
	proofBlocked, releaseProof := make(chan struct{}), make(chan struct{})
	api := internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != fmt.Sprintf("/repos/o/r/issues/%d/comments", request.Issue) {
			return nil, fmt.Errorf("unexpected GitHub request %s", r.URL)
		}
		if reads.Add(1) == 3 {
			close(proofBlocked)
			<-releaseProof
		}
		if err := r.Context().Err(); err != nil {
			return nil, err
		}
		body, _ := json.Marshal(comments)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(body))), Request: r}, nil
	})}, Retries: -1}
	coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	plan := reconciliationPlannedEffect{Request: request, Identity: ownerReconciliationEffectIdentity(*effect), Material: material}
	done := make(chan error, 1)
	go func() {
		_, err := coordinator.executeOperatorIssueUpdate(api, plan)
		done <- err
	}()
	<-proofBlocked
	newer := reconciliationEffectObservationInput(request, "title")
	newer.Issues[0].RecoveryAuthorized, newer.Issues[0].Retry = true, true
	changed := applyReconciliationInput(t, owner, newer)
	coordinator.cancelInvalidated(changed)
	close(releaseProof)
	if err := <-done; err != nil {
		t.Fatalf("exact retry proof was lost after observation-only drift: %v", err)
	}
	committed := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if committed.State != "completed" || reads.Load() != 4 {
		t.Fatalf("retry effect=%#v GitHub reads=%d", committed, reads.Load())
	}
}

func TestRetryObservationInvalidationBeforeAndAfterPost(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*internalgithub.RecoveryIssueFact)
		posted bool
	}{
		{"revoked", func(f *internalgithub.RecoveryIssueFact) { f.RecoveryAuthorized = false }, false},
		{"closed", func(f *internalgithub.RecoveryIssueFact) { f.Closed = true }, false},
		{"cancelled", func(f *internalgithub.RecoveryIssueFact) { f.Cancelled = true }, false},
		{"posted-then-observed", func(f *internalgithub.RecoveryIssueFact) { f.Retry = true }, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := reconciliationEffectCaseNamed(t, "issue-retry").request
			_, owner, _ := reconciliationEffectPersistentOwner(t, request)
			authorized := reconciliationEffectObservationInput(request, "title")
			authorized.Issues[0].RecoveryAuthorized = true
			snapshot := applyReconciliationInput(t, owner, authorized)
			request = bindEffectObservation(snapshot, request)
			issue := issueFact(request.Issue, "title")
			cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42, CancelCommand: "/cancel", RetryCommand: "/retry"}
			material := reconciliationIssueUpdateMaterial{Issue: &issue, Config: cfg}
			request.ExecutionDigest = issueUpdateExecutionDigest(request, material)
			_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
			if err != nil {
				t.Fatal(err)
			}
			failedAt := time.Unix(0, request.GitHubIssueUpdate.FailedAtUnixNano)
			terminal, err := internalgithub.TerminalFailureMarker(request.Issue, request.Attempt, failedAt)
			if err != nil {
				t.Fatal(err)
			}
			active, err := internalgithub.ActiveAttemptMarker(request.Repository, request.Issue, request.Attempt, request.Manifest.BaseSHA)
			if err != nil {
				t.Fatal(err)
			}
			comment := func(id int, body string, at time.Time) map[string]any {
				return map[string]any{"id": id, "body": body, "created_at": at, "updated_at": at, "user": map[string]any{"id": 42}}
			}
			fixture := &fullSystemGitHub{base: request.Manifest.BaseSHA, historicalIssues: map[int]map[string]any{request.Issue: {"number": request.Issue, "title": "title", "body": issue.Body, "state": "open", "created_at": failedAt.Add(-time.Hour), "updated_at": failedAt, "user": map[string]any{"id": 42}, "labels": []any{}}}, historicalComments: map[int][]map[string]any{request.Issue: {comment(1, active, failedAt.Add(-time.Second)), comment(2, terminal, failedAt)}}}
			postEntered, releasePost := make(chan struct{}), make(chan struct{})
			var posts atomic.Int32
			server := httptest.NewServer(fixture)
			t.Cleanup(server.Close)
			client := &http.Client{Transport: reconciliationRoundTripFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost && r.URL.Path == fmt.Sprintf("/repos/o/r/issues/%d/comments", request.Issue) {
					if test.posted {
						fixture.mu.Lock()
						fixture.historicalComments[request.Issue] = append(fixture.historicalComments[request.Issue], comment(3, cfg.RetryCommand, failedAt.Add(time.Second)))
						fixture.mu.Unlock()
						posts.Add(1)
					}
					close(postEntered)
					<-releasePost
					if r.Context().Err() != nil {
						return nil, r.Context().Err()
					}
					posts.Add(1)
					return &http.Response{StatusCode: http.StatusCreated, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
				}
				return http.DefaultTransport.RoundTrip(r)
			})}
			api := internalgithub.API{BaseURL: server.URL, HTTP: client, Retries: -1}
			coordinator := &runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
			plan := reconciliationPlannedEffect{Request: request, Identity: ownerReconciliationEffectIdentity(*effect), Material: material}
			done := make(chan error, 1)
			go func() {
				_, err := coordinator.executeOperatorIssueUpdate(api, plan)
				done <- err
			}()
			select {
			case <-postEntered:
			case err := <-done:
				t.Fatalf("retry failed before POST: %v", err)
			}
			changed := reconciliationEffectObservationInput(request, "title")
			changed.Issues[0].RecoveryAuthorized = true
			test.change(&changed.Issues[0])
			invalidated := applyReconciliationInput(t, owner, changed)
			coordinator.cancelInvalidated(invalidated)
			close(releasePost)
			err = <-done
			if test.posted {
				if err != nil || posts.Load() != 1 || mustOwnerSnapshot(t, owner).State.Effects[effect.ID].State != "completed" {
					t.Fatalf("posted retry err=%v writes=%d effect=%#v", err, posts.Load(), mustOwnerSnapshot(t, owner).State.Effects[effect.ID])
				}
			} else if err == nil || posts.Load() != 0 {
				t.Fatalf("retry err=%v writes=%d", err, posts.Load())
			}
		})
	}
}

func TestReconciliationCoordinatorShutdownCancelsAndRejectsWork(t *testing.T) {
	owner := newReconciliationTestOwner(t)
	body := "snapshot"
	digest := sha256.Sum256([]byte(body))
	proposal := reconciliationIssueUpdateProposal{Repository: "o/r", Issue: 187, Kind: githubIssueControlSnapshot, ControlSnapshotDigest: hex.EncodeToString(digest[:])}
	input := repositoryInput(true, issueFact(187, "proposal"))
	input.IssueUpdates = []reconciliationIssueUpdateProposal{proposal}
	accepted := applyReconciliationInput(t, owner, input)
	batch := reconciliationV2Batch{IssueUpdates: []reconciliationIssueUpdateMaterial{{Proposal: proposal, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}, ControlSnapshotBody: body}}}
	plans, err := planReconciliationIssueUpdates(accepted, batch)
	if err != nil || len(plans) != 1 {
		t.Fatalf("plans=%#v err=%v", plans, err)
	}
	coordinator := runtimeEffectCoordinator{lifecycle: context.Background(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	plan, err := coordinator.beginReconciliation(t.Context(), plans[0])
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	var mutations atomic.Int32
	api := internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			mutations.Add(1)
		}
		close(started)
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}, Retries: -1}
	executed := make(chan error, 1)
	go func() {
		_, err := coordinator.executeIssueUpdate(context.Background(), api, plan)
		executed <- err
	}()
	<-started
	if err := coordinator.shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := <-executed; !errors.Is(err, context.Canceled) || mutations.Load() != 0 {
		t.Fatalf("execution err=%v mutations=%d", err, mutations.Load())
	}
	if _, err := coordinator.acquireKey(context.Background(), ownerIssueKey("o/r", 188), 1, 0, 1); !errors.Is(err, errStateOwnerStopped) {
		t.Fatalf("post-shutdown admission err=%v", err)
	}
}

func TestDependencyClearRevalidationCancelsWhenAttributionAttemptIsDismissed(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "issue-dependency-clear").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	proposal := reconciliationIssueUpdateProposal{Repository: request.Repository, Issue: request.Issue, Kind: githubIssueDependencyClear, AttributionAttempt: request.GitHubIssueUpdate.AttributionAttempt, Dependency: request.GitHubIssueUpdate.Dependency, PullRequest: request.GitHubIssueUpdate.PullRequest}
	material := reconciliationIssueUpdateMaterial{Proposal: proposal, Config: internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}}
	request.ExecutionDigest = issueUpdateExecutionDigest(request, material)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	plan := reconciliationPlannedEffect{Identity: ownerReconciliationEffectIdentity(*effect), Request: request, Material: material}
	coordinator := runtimeEffectCoordinator{lifecycle: t.Context(), owner: owner, active: map[string]*activeRuntimeEffect{}}
	started := make(chan struct{})
	var mutations atomic.Int32
	api := internalgithub.API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: reconciliationRoundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method != http.MethodGet {
			mutations.Add(1)
		}
		select {
		case <-started:
		default:
			close(started)
		}
		<-request.Context().Done()
		return nil, request.Context().Err()
	})}, Retries: -1}
	done := make(chan error, 1)
	go func() {
		_, err := coordinator.executeIssueUpdate(context.Background(), api, plan)
		done <- err
	}()
	<-started
	attempt := request.GitHubIssueUpdate.AttributionAttempt
	invalidated, _, err := owner.invalidateAttempt(t.Context(), invalidateAttemptCommand{Repository: "o/r", Issue: request.Issue, Attempt: attempt, ExpectedIssueGeneration: snapshot.State.IssueGenerations[ownerIssueKey("o/r", request.Issue)], ExpectedAttemptGeneration: snapshot.State.AttemptGenerations[ownerAttemptKey("o/r", request.Issue, attempt)], Action: "dismissed", CleanupPhase: "completed"})
	if err != nil {
		t.Fatal(err)
	}
	coordinator.cancelInvalidated(invalidated)
	if err := <-done; !errors.Is(err, context.Canceled) || mutations.Load() != 0 {
		t.Fatalf("execution err=%v mutations=%d", err, mutations.Load())
	}
}
