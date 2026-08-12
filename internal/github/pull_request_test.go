package github

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func eligiblePR() PRFacts {
	return PRFacts{
		IssueOpen: true, IssueEligible: true, AutonomousMerge: true, PRIsOpen: true, Mergeable: true,
		HeadSHA: "abc", ValidationSHA: "abc", DocumentationSHA: "abc", Approved: true,
		RequiredChecksPass: true, PolicyCheckRequired: true, MergePermission: true, BranchProtectionAllows: true,
	}
}

func TestEvaluatePRGovernance(t *testing.T) {
	facts := eligiblePR()
	if got := EvaluatePR(facts); !got.Merge || got.CheckConclusion != "success" {
		t.Fatalf("eligible PR: %#v", got)
	}
	facts.NeedsHumanReview = true
	if got := EvaluatePR(facts); got.Merge || got.CheckStatus != "in_progress" || got.CheckConclusion != "" || !slices.Contains(got.Reasons, "human review is required") || !slices.Contains(got.MergeBlockers, "human review is required") {
		t.Fatalf("human review pending: %#v", got)
	}
	tests := []struct {
		name         string
		checkFailure bool
		edit         func(*PRFacts)
	}{
		{"pending feedback", true, func(f *PRFacts) { f.Feedback = []Feedback{{ID: 7, State: FeedbackPending, Authorized: true}} }},
		{"blocked feedback", true, func(f *PRFacts) { f.Feedback = []Feedback{{ID: 8, State: FeedbackBlocked, Authorized: true}} }},
		{"force push", true, func(f *PRFacts) { f.HeadSHA = "new" }},
		{"changed review", true, func(f *PRFacts) { f.ChangesRequested = true }},
		{"failed checks", true, func(f *PRFacts) { f.RequiredChecksPass = false }},
		{"missing docs", true, func(f *PRFacts) { f.DocumentationSHA = "" }},
		{"permission revoked", true, func(f *PRFacts) { f.MergePermission = false }},
		{"merge denial", true, func(f *PRFacts) { f.Mergeable = false }},
		{"ineligible issue", true, func(f *PRFacts) { f.IssueEligible = false }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			facts := eligiblePR()
			test.edit(&facts)
			if got := EvaluatePR(facts); got.Merge || (got.CheckConclusion == "failure") != test.checkFailure || len(got.MergeBlockers) == 0 {
				t.Fatalf("unsafe result: %#v", got)
			}
		})
	}
	facts = eligiblePR()
	facts.Feedback = []Feedback{{ID: 9, State: FeedbackPending}}
	if got := EvaluatePR(facts); !got.Merge {
		t.Fatalf("unauthorized visible feedback blocked merge: %#v", got)
	}
	facts = eligiblePR()
	facts.AutonomousMerge = false
	if got := EvaluatePR(facts); got.Merge || got.CheckConclusion != "success" {
		t.Fatalf("human completion should be ready but not merged: %#v", got)
	}
	facts = eligiblePR()
	facts.PolicyCheckRequired, facts.BranchProtectionAllows = false, false
	if got := EvaluatePR(facts); !got.Merge || got.CheckConclusion != "success" {
		t.Fatalf("optional repository protection blocked merge: %#v", got)
	}
	facts.NeedsHumanReview = true
	if got := EvaluatePR(facts); got.Merge || got.CheckStatus != "in_progress" {
		t.Fatalf("human review did not remain safely pending: %#v", got)
	}
}

func TestActionableFeedbackLifetimeAuthorizationAndState(t *testing.T) {
	now := time.Now()
	feedback := []Feedback{
		{ID: 3, ActorID: 1, Body: "later", CreatedAt: now.Add(time.Hour), Authorized: true},
		{ID: 1, ActorID: 1, Body: "done", CreatedAt: now, State: FeedbackAddressed, Authorized: true},
		{ID: 2, ActorID: 2, Body: "unauthorized", CreatedAt: now},
		{ID: 4, ActorID: 1, Body: "first", CreatedAt: now, Authorized: true},
		{ID: 4, ActorID: 1, Body: "redelivery", CreatedAt: now, Authorized: true},
	}
	got := ActionableFeedback(feedback, func(actor int) bool { return actor == 1 })
	if len(got) != 2 || got[0].ID != 4 || got[1].ID != 3 {
		t.Fatalf("actionable feedback: %#v", got)
	}
	got[0].State = FeedbackBlocked
	body, err := FeedbackDispositionBody(10, 2, got[0], "cannot reproduce")
	if err != nil || !strings.Contains(body, "Feedback inline:4: **blocked**") {
		t.Fatalf("disposition body=%q err=%v", body, err)
	}
}

func TestPullRequestBodyAndMutations(t *testing.T) {
	body, err := PullRequestBody(10, 2, "go test ./... passed", "README updated", "kept API narrow")
	if err != nil || !strings.Contains(body, "Closes #10") || !strings.Contains(body, "agent-symphony:issue:10:attempt:2") {
		t.Fatalf("body=%q err=%v", body, err)
	}
	type request struct{ method, path, body string }
	var requests []request
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		payload, _ := io.ReadAll(r.Body)
		requests = append(requests, request{r.Method, r.URL.Path, string(payload)})
		response := `{"number":9,"html_url":"url"}`
		if strings.HasSuffix(r.URL.Path, "/merge") {
			response = `{"merged":true}`
		}
		return &http.Response{StatusCode: http.StatusCreated, Status: "201 Created", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
	})}}
	pr, err := api.CreatePullRequest(context.Background(), "o/r", "title", "head", "main", body, Mutation{Issue: 10, Attempt: 2})
	if err != nil || pr.Number != 9 {
		t.Fatalf("create: %#v %v", pr, err)
	}
	result := EvaluatePR(eligiblePR())
	if err := api.PublishPolicyStatus(context.Background(), "o/r", "abc", result, Mutation{Issue: 10, Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	if err := api.MergePullRequest(context.Background(), "o/r", 9, "abc", "squash", Mutation{Issue: 10, Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	if len(requests) != 3 || requests[0].method != http.MethodPost || requests[0].path != "/repos/o/r/pulls" || requests[1].path != "/repos/o/r/statuses/abc" || requests[2].method != http.MethodPut || requests[2].path != "/repos/o/r/pulls/9/merge" || !strings.Contains(requests[2].body, `"sha":"abc"`) {
		t.Fatalf("requests: %#v", requests)
	}
}

func TestPolicyStatusPayload(t *testing.T) {
	var body map[string]any
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodPost || r.URL.Path != "/repos/o/r/statuses/abc" {
			t.Fatalf("request %s %s", r.Method, r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return httpResponse(http.StatusOK, `{}`, nil), nil
	})}}
	if err := api.PublishPolicyStatus(context.Background(), "o/r", "abc", EvaluatePR(eligiblePR()), Mutation{Issue: 10, Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	if body["context"] != PolicyCheck || body["state"] != "success" || !strings.Contains(body["description"].(string), "issue #10 attempt 2") {
		t.Fatalf("status schema: %#v", body)
	}
}

func TestHumanReviewCheckPayloadIsPendingWithoutConclusion(t *testing.T) {
	var body map[string]any
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}
	facts := eligiblePR()
	facts.NeedsHumanReview = true
	if err := api.PublishPolicyStatus(context.Background(), "o/r", "abc", EvaluatePR(facts), Mutation{Issue: 10, Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	if body["state"] != "pending" || !strings.Contains(body["description"].(string), "human review is required") {
		t.Fatalf("pending status payload: %#v", body)
	}
}

func TestFailedPolicyCheckSummaryExposesBlockers(t *testing.T) {
	var body map[string]any
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}
	facts := eligiblePR()
	facts.RequiredChecksPass = false
	if err := api.PublishPolicyStatus(context.Background(), "o/r", "abc", EvaluatePR(facts), Mutation{Issue: 10, Attempt: 2}); err != nil {
		t.Fatal(err)
	}
	if body["state"] != "failure" || !strings.Contains(body["description"].(string), "required checks have not passed") {
		t.Fatalf("failed status payload: %#v", body)
	}
}

func TestEnsureEvidenceIsIdempotent(t *testing.T) {
	type comment struct {
		Body string `json:"body"`
		User struct {
			ID int `json:"id"`
		} `json:"user"`
	}
	var comments []comment
	posts := 0
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.Method + " " + r.URL.RequestURI() {
		case "GET /repos/o/r/issues/10/comments?per_page=100&page=1":
			body, _ := json.Marshal(comments)
			return httpResponse(http.StatusOK, string(body), nil), nil
		case "POST /repos/o/r/issues/10/comments":
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil {
				t.Fatal("invalid evidence comment")
			}
			got := comment{Body: payload.Body}
			got.User.ID = 42
			comments, posts = append(comments, got), posts+1
			return httpResponse(http.StatusCreated, `{}`, nil), nil
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
			return nil, nil
		}
	})}}
	for range 2 {
		if err := api.EnsureEvidence(t.Context(), "o/r", 10, 2, "abcdef0", 42); err != nil {
			t.Fatal(err)
		}
	}
	if posts != 2 || !strings.Contains(comments[0].Body, "validation evidence") || !strings.Contains(comments[1].Body, "documentation evidence") {
		t.Fatalf("posts=%d comments=%#v", posts, comments)
	}
}

func TestMergeRejectsSuccessfulHTTPDenialAndAmbiguity(t *testing.T) {
	for _, test := range []struct {
		name      string
		roundTrip roundTripFunc
	}{
		{"denied", func(*http.Request) (*http.Response, error) {
			return httpResponse(http.StatusOK, `{"merged":false}`, nil), nil
		}},
		{"ambiguous", func(*http.Request) (*http.Response, error) { return nil, errors.New("reset") }},
		{"malformed", func(*http.Request) (*http.Response, error) { return httpResponse(http.StatusOK, `{`, nil), nil }},
		{"missing merged", func(*http.Request) (*http.Response, error) { return httpResponse(http.StatusOK, `{}`, nil), nil }},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: test.roundTrip}}
			err := api.MergePullRequest(context.Background(), "o/r", 9, "abc", "squash", Mutation{Issue: 10, Attempt: 2})
			if err == nil || test.name == "malformed" && !IsAmbiguousMutation(err) {
				t.Fatal("unsafe merge result accepted")
			}
		})
	}
}

type prSourceStub struct {
	state  PRState
	states []PRState
	reads  int
}

func (s *prSourceStub) OpenPullRequests(context.Context) ([]int, error) {
	return []int{s.state.Number}, nil
}
func (s *prSourceStub) FreshPullRequest(context.Context, int) (PRState, error) {
	s.reads++
	if len(s.states) > 0 {
		state := s.states[0]
		s.states = s.states[1:]
		return state, nil
	}
	return s.state, nil
}
func (s *prSourceStub) FreshFeedback(_ context.Context, _ PRState, wanted Feedback) (Feedback, error) {
	for _, feedback := range s.state.Facts.Feedback {
		if feedback.identity() == wanted.identity() {
			return feedback, nil
		}
	}
	return Feedback{}, errors.New("missing feedback")
}

type prSignalsStub struct{ feedback, validation int }

func (s *prSignalsStub) DelegateFeedback(context.Context, PRState, Feedback) error {
	s.feedback++
	return nil
}

func TestPRCoordinatorDoesNotMergeHeadChangedAfterPolicy(t *testing.T) {
	firstFacts := eligiblePR()
	first := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", CheckHead: "abc", Facts: firstFacts}
	changed := first
	changed.HeadSHA, changed.Facts.HeadSHA, changed.Facts.ValidationSHA, changed.Facts.DocumentationSHA = "def", "def", "def", "def"
	source := &prSourceStub{state: first, states: []PRState{first, first, changed}}
	signals := &prSignalsStub{}
	var paths []string
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		paths = append(paths, r.URL.Path)
		return httpResponse(http.StatusOK, `{}`, nil), nil
	})}}
	if err := (PRCoordinator{API: api, Source: source, Signals: signals, MergeMethod: "squash"}).Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(paths) != 1 || paths[0] != "/repos/o/r/statuses/abc" {
		t.Fatalf("head race issued unsafe mutation: %#v", paths)
	}
}

func TestPRCoordinatorHonorsRecoveredFeedbackClaim(t *testing.T) {
	facts := eligiblePR()
	facts.AutonomousMerge = false
	facts.Feedback = []Feedback{{ID: 7, ActorID: 1, Body: "fix", Authorized: true, State: FeedbackPending, Delegated: true}}
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", Facts: facts}
	source, signals := &prSourceStub{state: state}, &prSignalsStub{}
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}
	if err := (PRCoordinator{API: api, Source: source, Signals: signals}).Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if signals.feedback != 0 {
		t.Fatal("feedback was delegated again after restart")
	}
}

func TestPRCoordinatorCommentsOnUnresolvedPolicyOnce(t *testing.T) {
	facts := eligiblePR()
	facts.AutonomousMerge, facts.RequiredChecksPass = false, false
	facts.HeadSHA, facts.ValidationSHA, facts.DocumentationSHA = "abcdef0", "abcdef0", "abcdef0"
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", CheckHead: "abcdef0", PolicyStatus: "failure", Facts: facts}
	state.ValidationResult = "blocked"
	var comments []string
	posts := 0
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.Method + " " + r.URL.RequestURI() {
		case "GET /repos/o/r/issues/3/comments?per_page=100&page=1":
			body := make([]map[string]any, len(comments))
			for i, comment := range comments {
				body[i] = map[string]any{"body": comment, "user": map[string]any{"id": 42}}
			}
			encoded, _ := json.Marshal(body)
			return httpResponse(http.StatusOK, string(encoded), nil), nil
		case "POST /repos/o/r/issues/3/comments":
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil {
				t.Fatal("invalid policy comment")
			}
			comments, posts = append(comments, payload.Body), posts+1
			return httpResponse(http.StatusCreated, `{}`, nil), nil
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
			return nil, nil
		}
	})}}
	coordinator := PRCoordinator{API: api, Source: &prSourceStub{state: state}, Signals: &prSignalsStub{}, ActorID: 42}
	for range 2 {
		if err := coordinator.Reconcile(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	if posts != 1 || !strings.Contains(comments[0], "required checks have not passed") || !strings.Contains(comments[0], "ended as **blocked**") {
		t.Fatalf("posts=%d comments=%q", posts, comments)
	}
}

func TestPRCoordinatorPublishesLocalDispositionButWaitsForGitHub(t *testing.T) {
	facts := eligiblePR()
	facts.Feedback = []Feedback{{ID: 7, Source: feedbackInline, ActorID: 1, Body: "fix", Authorized: true, State: FeedbackPending, Execution: FeedbackCompleted, Delegated: true}}
	feedback := facts.Feedback[0]
	feedback.State = FeedbackAddressed
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", Facts: facts, PendingDispositions: []Feedback{feedback}}
	source := &prSourceStub{state: state}
	var bodies []string
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}
	if err := (PRCoordinator{API: api, Source: source, Signals: &prSignalsStub{}}).Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	want, _ := FeedbackDispositionBody(10, 2, feedback, "")
	encodedWant, _ := json.Marshal(map[string]string{"body": want})
	if len(bodies) != 2 || bodies[0] != string(encodedWant) || !strings.Contains(bodies[1], "feedback 7 is pending") {
		t.Fatalf("mutations=%q", bodies)
	}
}

func TestProductionReconcilerRunsRecoveredIssuesThenPullRequests(t *testing.T) {
	facts := eligiblePR()
	facts.AutonomousMerge = false
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", Facts: facts}
	source, signals := &prSourceStub{state: state}, &prSignalsStub{}
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}
	issuesRead := false
	coordinator := &PRCoordinator{API: api, Source: source, Signals: signals}
	if err := (Reconciler{FullRead: func() error { issuesRead = true; return nil }, PullRequests: coordinator}).RunOnce(); err != nil {
		t.Fatal(err)
	}
	if !issuesRead || source.reads != 2 {
		t.Fatalf("issue recovery=%v PR reads=%d", issuesRead, source.reads)
	}
}
func (s *prSignalsStub) RerunValidation(context.Context, PRState) error { s.validation++; return nil }

func TestPRCoordinatorReconstructsLifecycleAndCreatesCheckForNewHead(t *testing.T) {
	facts := eligiblePR()
	facts.AutonomousMerge = false
	facts.HeadSHA, facts.ValidationSHA = "new", "old"
	facts.Feedback = []Feedback{
		{ID: 7, ActorID: 1, Body: "fix", Authorized: true},
		{ID: 7, ActorID: 1, Body: "redelivery", Authorized: true},
		{ID: 8, ActorID: 2, Body: "visible", Authorized: false},
		{ID: 9, ActorID: 1, Body: "done", State: FeedbackAddressed, Authorized: true},
	}
	source := &prSourceStub{state: PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "new", CheckHead: "old", Facts: facts, Decisions: []Decision{{ID: "d1", Body: "keep it small"}}}}
	signals := &prSignalsStub{}
	var requests []struct {
		method, path string
		body         map[string]any
	}
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		requests = append(requests, struct {
			method, path string
			body         map[string]any
		}{r.Method, r.URL.Path, body})
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}
	err := (PRCoordinator{API: api, Source: source, Signals: signals, ReviewLabel: "needs-human-review", MergeMethod: "squash"}).Reconcile(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if source.reads != 4 || signals.feedback != 1 || signals.validation != 1 {
		t.Fatalf("reads=%d feedback=%d validation=%d", source.reads, signals.feedback, signals.validation)
	}
	if len(requests) != 2 || requests[0].path != "/repos/o/r/issues/10/comments" || requests[1].method != http.MethodPost || requests[1].path != "/repos/o/r/statuses/new" || requests[1].body["state"] == nil {
		t.Fatalf("requests %#v", requests)
	}
}

type multiPRSource struct{ prSourceStub }

func (s *multiPRSource) OpenPullRequests(context.Context) ([]int, error) { return []int{1, 2}, nil }
func (s *multiPRSource) FreshPullRequest(_ context.Context, number int) (PRState, error) {
	if number == 1 {
		return PRState{}, errors.New("malformed")
	}
	return s.prSourceStub.FreshPullRequest(context.Background(), number)
}

func TestPRCoordinatorIsolatesAndAggregatesPRErrors(t *testing.T) {
	facts := eligiblePR()
	facts.AutonomousMerge = false
	state := PRState{Repository: "o/r", Number: 2, Issue: 10, Attempt: 1, HeadSHA: "abc", Facts: facts}
	source := &multiPRSource{prSourceStub: prSourceStub{state: state}}
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return httpResponse(http.StatusCreated, `{}`, nil), nil })}}
	err := (PRCoordinator{API: api, Source: source, Signals: &prSignalsStub{}}).Reconcile(context.Background())
	if err == nil || !strings.Contains(err.Error(), "pull request 1") || source.reads != 2 {
		t.Fatalf("err=%v later reads=%d", err, source.reads)
	}
}

type editedFeedbackSource struct{ *prSourceStub }

func (s editedFeedbackSource) FreshFeedback(_ context.Context, _ PRState, feedback Feedback) (Feedback, error) {
	feedback.Body, feedback.Authorized = "edited", true
	return feedback, nil
}

func TestPRCoordinatorBlocksFeedbackIdentityRace(t *testing.T) {
	facts := eligiblePR()
	facts.AutonomousMerge = false
	facts.Feedback = []Feedback{{ID: 7, ActorID: 1, Body: "fix", Authorized: true, State: FeedbackPending}}
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", Facts: facts}
	base, signals := &prSourceStub{state: state}, &prSignalsStub{}
	source := editedFeedbackSource{base}
	source.states = []PRState{state, state}
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return httpResponse(http.StatusCreated, `{}`, nil), nil })}}
	err := (PRCoordinator{API: api, Source: source, Signals: signals}).Reconcile(context.Background())
	if err == nil || signals.feedback != 0 {
		t.Fatalf("err=%v delegated=%d", err, signals.feedback)
	}
}

func TestPRCoordinatorRecoveredDispatchedMergeUsesDedicatedStatus(t *testing.T) {
	for _, test := range []struct {
		name        string
		status      int
		network     bool
		wantComment bool
		wantError   bool
	}{
		{"204 merged", http.StatusNoContent, false, false, false},
		{"404 unmerged", http.StatusNotFound, false, true, false},
		{"500 unknown", http.StatusInternalServerError, false, false, true},
		{"network unknown", 0, true, false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts := eligiblePR()
			state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", MergeAttemptSHA: "abc", MergePhase: "dispatched", Facts: facts}
			var gets, puts, comments int
			api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch {
				case r.Method == http.MethodGet && strings.HasSuffix(r.URL.Path, "/merge"):
					gets++
					if test.network {
						return nil, errors.New("offline")
					}
					return httpResponse(test.status, `{}`, nil), nil
				case r.Method == http.MethodPut && strings.HasSuffix(r.URL.Path, "/merge"):
					puts++
				case strings.HasSuffix(r.URL.Path, "/comments"):
					comments++
				}
				return httpResponse(http.StatusCreated, `{}`, nil), nil
			})}}
			err := (PRCoordinator{API: api, Source: &prSourceStub{state: state}, Signals: &prSignalsStub{}, MergeMethod: "squash"}).Reconcile(context.Background())
			if (err != nil) != test.wantError || gets != 1 || puts != 0 || (comments == 1) != test.wantComment {
				t.Fatalf("err=%v gets=%d puts=%d comments=%d", err, gets, puts, comments)
			}
		})
	}
}

func TestPRCoordinatorRecoveredPreparedMergeDispatchesOnce(t *testing.T) {
	facts := eligiblePR()
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", MergeAttemptSHA: "abc", MergePhase: "prepared", Facts: facts}
	source := &prSourceStub{state: state}
	var comments, merges int
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if strings.HasSuffix(r.URL.Path, "/comments") {
			comments++
		}
		if strings.HasSuffix(r.URL.Path, "/merge") {
			merges++
			return httpResponse(http.StatusOK, `{"merged":true}`, nil), nil
		}
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}
	if err := (PRCoordinator{API: api, Source: source, Signals: &prSignalsStub{}, MergeMethod: "squash"}).Reconcile(context.Background()); err != nil {
		t.Fatal(err)
	}
	if comments != 1 || merges != 1 {
		t.Fatalf("comments=%d merges=%d", comments, merges)
	}
}

func TestMergeSuppressionOnlyForAmbiguousOutcome(t *testing.T) {
	for _, test := range []struct {
		name       string
		mergeReply func() (*http.Response, error)
		comments   int
	}{
		{"definitive denial", func() (*http.Response, error) {
			return httpResponse(http.StatusConflict, `{"message":"not mergeable"}`, nil), nil
		}, 3},
		{"ambiguous transport", func() (*http.Response, error) { return nil, errors.New("connection reset") }, 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts := eligiblePR()
			state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", Facts: facts}
			source := &prSourceStub{state: state}
			comments := 0
			api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				switch {
				case strings.HasSuffix(r.URL.Path, "/merge"):
					return test.mergeReply()
				case strings.HasSuffix(r.URL.Path, "/comments"):
					comments++
				}
				return httpResponse(http.StatusCreated, `{}`, nil), nil
			})}}
			err := (PRCoordinator{API: api, Source: source, Signals: &prSignalsStub{}, MergeMethod: "squash"}).Reconcile(context.Background())
			if err == nil || comments != test.comments {
				t.Fatalf("err=%v comments=%d", err, comments)
			}
		})
	}
}

func TestSyncReviewLabelRemoval404IsIdempotent(t *testing.T) {
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return httpResponse(http.StatusNotFound, `{}`, nil), nil })}}
	if err := api.SyncReviewLabel(context.Background(), "o/r", 3, "review", true, false, Mutation{Issue: 10, Attempt: 2}); err != nil {
		t.Fatal(err)
	}
}
