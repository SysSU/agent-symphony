package github

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestFileRecoveryDurablyClaimsAndCompletesRuntimeHandoffs(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.json")
	recovery := &FileRecovery{Path: path}
	state := PRState{Repository: "owner/repo", Number: 10, Issue: 4, Attempt: 2, HeadSHA: "abcdef1", ValidationQueuedSHA: "abcdef1", Facts: PRFacts{Feedback: []Feedback{{ID: 7, Source: feedbackInline, Execution: FeedbackClaimed, Delegated: true}}}}
	if err := recovery.write([]PRState{state}); err != nil {
		t.Fatal(err)
	}
	got, err := recovery.ClaimHandoffs(context.Background())
	if err != nil || len(got) != 1 || !got[0].Validation || len(got[0].Feedback) != 1 || got[0].Feedback[0].Execution != FeedbackInFlight {
		t.Fatalf("got %#v err=%v", got, err)
	}
	resumed, err := recovery.ClaimHandoffs(context.Background())
	if err != nil || len(resumed) != 1 || !resumed[0].Validation || len(resumed[0].Feedback) != 1 {
		t.Fatalf("restart got %#v err=%v", resumed, err)
	}
	if err := recovery.CompleteHandoff(context.Background(), got[0]); err != nil {
		t.Fatal(err)
	}
	done, err := recovery.ClaimHandoffs(context.Background())
	if err != nil || len(done) != 0 {
		t.Fatalf("completed got %#v err=%v", done, err)
	}
}

func TestOwnedHandoffRedeliverySurvivesClaimAndPasteCrashes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.json")
	r := &FileRecovery{Path: path}
	state := PRState{Repository: "o/r", Number: 9, Issue: 4, Attempt: 2, HeadSHA: "abcdef1", ValidationQueuedSHA: "abcdef1", Facts: PRFacts{Feedback: []Feedback{{ID: 7, Source: feedbackInline, Execution: FeedbackClaimed, Delegated: true}}}}
	if err := r.write([]PRState{state}); err != nil {
		t.Fatal(err)
	}
	owners := map[string]bool{"o/r#4/2": true}
	first, err := r.ClaimHandoffsFor(context.Background(), owners)
	if err != nil || len(first) != 1 {
		t.Fatalf("claim=%#v err=%v", first, err)
	}
	for _, boundary := range []string{"after claim", "after buffer load", "after paste"} {
		again, err := r.ClaimHandoffsFor(context.Background(), owners)
		if err != nil || len(again) != 1 || again[0].Key != first[0].Key {
			t.Fatalf("%s replay=%#v err=%v", boundary, again, err)
		}
	}
	if err := r.ReceiptHandoff(context.Background(), first[0]); err != nil {
		t.Fatal(err)
	}
	received, ok, err := r.ReceivedHandoff(context.Background(), "o/r", 9, 4, 2, "abcdef1")
	if err != nil || !ok || received.Key != first[0].Key || received.Validation != first[0].Validation || !reflect.DeepEqual(received.Feedback, first[0].Feedback) {
		t.Fatalf("received=%#v ok=%v err=%v", received, ok, err)
	}
	if again, err := r.ClaimHandoffsFor(context.Background(), owners); err != nil || len(again) != 0 {
		t.Fatalf("receipt replay=%#v err=%v", again, err)
	}
	if err := r.ReceiptHandoff(context.Background(), first[0]); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffIdentityIncludesSequentialFeedbackAndValidationGeneration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.json")
	r := &FileRecovery{Path: path}
	state := PRState{Repository: "o/r", Number: 9, Issue: 4, Attempt: 2, HeadSHA: "abcdef1", Facts: PRFacts{Feedback: []Feedback{{ID: 7, Source: feedbackInline, Execution: FeedbackClaimed}}}}
	if err := r.write([]PRState{state}); err != nil {
		t.Fatal(err)
	}
	first, _ := r.ClaimHandoffs(context.Background())
	if err := r.CompleteHandoff(context.Background(), first[0]); err != nil {
		t.Fatal(err)
	}
	states, _ := r.read()
	states[0].Facts.Feedback = append(states[0].Facts.Feedback, Feedback{ID: 8, Source: feedbackInline, Execution: FeedbackClaimed})
	if err := r.write(states); err != nil {
		t.Fatal(err)
	}
	second, _ := r.ClaimHandoffs(context.Background())
	if len(second) != 1 || second[0].Key == first[0].Key || len(second[0].Feedback) != 1 || second[0].Feedback[0].ID != 8 {
		t.Fatalf("sequential handoff %#v", second)
	}
	if err := r.CompleteHandoff(context.Background(), second[0]); err != nil {
		t.Fatal(err)
	}
	states, _ = r.read()
	if err := r.QueueValidation(context.Background(), states[0]); err != nil {
		t.Fatal(err)
	}
	validation1, _ := r.ClaimHandoffs(context.Background())
	if err := r.CompleteHandoff(context.Background(), validation1[0]); err != nil {
		t.Fatal(err)
	}
	states, _ = r.read()
	if err := r.QueueValidation(context.Background(), states[0]); err != nil {
		t.Fatal(err)
	}
	validation2, _ := r.ClaimHandoffs(context.Background())
	if validation1[0].Key == validation2[0].Key || validation2[0].ValidationGeneration != validation1[0].ValidationGeneration+1 {
		t.Fatalf("validation generations %#v %#v", validation1, validation2)
	}
}

func TestIdenticalFeedbackOutcomeReplaySucceeds(t *testing.T) {
	path := filepath.Join(t.TempDir(), "recovery.json")
	r := &FileRecovery{Path: path}
	state := PRState{Repository: "o/r", Number: 9, Issue: 4, Attempt: 2, HeadSHA: "abcdef1", Facts: PRFacts{Feedback: []Feedback{{ID: 7, Source: feedbackInline, Execution: FeedbackClaimed, Delegated: true}}}}
	if err := r.write([]PRState{state}); err != nil {
		t.Fatal(err)
	}
	h, _ := r.ClaimHandoffsFor(context.Background(), map[string]bool{"o/r#4/2": true})
	outcome := HandoffOutcome{Key: h[0].Key, Feedback: []FeedbackOutcome{{ID: 7, Source: feedbackInline, State: FeedbackAddressed, Evidence: "fixed"}}}
	if err := r.CompleteHandoffOutcome(context.Background(), h[0], outcome); err != nil {
		t.Fatal(err)
	}
	if err := r.CompleteHandoffOutcome(context.Background(), h[0], outcome); err != nil {
		t.Fatal(err)
	}
}

type recoveryStub struct {
	state  PRState
	claims int
}

func (r *recoveryStub) PullRequestState(context.Context, string, int, int, int, string) (PRState, error) {
	return r.state, nil
}
func (r *recoveryStub) ClaimFeedback(context.Context, PRState, Feedback) (bool, error) {
	r.claims++
	return true, nil
}
func (*recoveryStub) QueueValidation(context.Context, PRState) error { return nil }

func TestGitHubPRSourcePaginatesAndFailsClosed(t *testing.T) {
	comments := make([]map[string]any, 100)
	reviews := make([]map[string]any, 100)
	checks := make([]map[string]any, 100)
	statuses := make([]map[string]any, 100)
	for i := range 100 {
		comments[i] = map[string]any{"body": "noise", "user": map[string]any{"id": 9}}
		reviews[i] = map[string]any{"id": i + 1, "state": "COMMENTED", "user": map[string]any{"id": i + 10}}
		checks[i] = map[string]any{"name": fmt.Sprintf("optional-%d", i), "status": "completed", "conclusion": "success", "app": map[string]any{"id": 1}}
		statuses[i] = map[string]any{"context": fmt.Sprintf("optional-%d", i), "state": "success"}
	}
	responses := map[string]any{
		"/repos/o/r/pulls?state=open&per_page=100&page=1":   []any{map[string]any{"number": 3}},
		"/repos/o/r/pulls/3":                                map[string]any{"number": 3, "state": "open", "body": "<!-- agent-symphony:issue:10:attempt:2 -->", "mergeable": true, "merged": false, "head": map[string]any{"sha": "abcdef0"}, "base": map[string]any{"ref": "main"}},
		"/repos/o/r/issues/10":                              map[string]any{"state": "open", "labels": []any{map[string]any{"name": "ready"}, map[string]any{"name": "auto"}}},
		"/repos/o/r/issues/10/comments?per_page=100&page=1": comments,
		"/repos/o/r/issues/10/comments?per_page=100&page=2": []any{
			map[string]any{"id": 1, "body": "Merge attempt for head `forged00`: **prepared**.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}},
			map[string]any{"id": 2, "body": "Merge attempt for head `badcafe`: **prepared**.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 99}},
			map[string]any{"id": 3, "body": "Merge attempt for head `abcdef0`: **prepared**.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}},
		},
		"/repos/o/r/branches/main/protection": map[string]any{"required_status_checks": map[string]any{"strict": true, "checks": []any{map[string]any{"context": PolicyCheck, "app_id": 7}, map[string]any{"context": "build", "app_id": 8}, map[string]any{"context": "legacy", "app_id": 0}}}, "required_pull_request_reviews": map[string]any{"dismiss_stale_reviews": true, "required_approving_review_count": 2, "require_code_owner_reviews": true, "require_last_push_approval": true}},
		"/graphql":                            map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewDecision": nil}}}},
		"/repos/o/r/rules/branches/main?per_page=100&page=1": []any{map[string]any{"type": "required_status_checks", "parameters": map[string]any{"required_status_checks": []any{map[string]any{"context": "rules-ci", "integration_id": 11}}}}},
		"/repos/o/r/pulls/3/reviews?per_page=100&page=1":     reviews,
		"/repos/o/r/pulls/3/reviews?per_page=100&page=2": []any{
			map[string]any{"id": 101, "state": "APPROVED", "commit_id": "old", "user": map[string]any{"id": 1}},
			map[string]any{"id": 102, "state": "DISMISSED", "commit_id": "abcdef0", "user": map[string]any{"id": 1}},
			map[string]any{"id": 103, "state": "APPROVED", "commit_id": "abcdef0", "user": map[string]any{"id": 2}},
		},
		"/repos/o/r/commits/abcdef0/check-runs?filter=all&per_page=100&page=1": map[string]any{"check_runs": checks},
		"/repos/o/r/commits/abcdef0/check-runs?filter=all&per_page=100&page=2": map[string]any{"check_runs": []any{map[string]any{"name": "build", "status": "completed", "conclusion": "success", "app": map[string]any{"id": 99}}}},
		"/repos/o/r/commits/abcdef0/statuses?per_page=100&page=1":              statuses,
		"/repos/o/r/commits/abcdef0/statuses?per_page=100&page=2":              []any{map[string]any{"context": "legacy", "state": "failure"}},
		"/repos/o/r": map[string]any{"permissions": map[string]any{"push": true}},
		"/repos/o/r/issues/3/comments?per_page=100&page=1": []any{},
		"/repos/o/r/pulls/3/comments?per_page=100&page=1":  []any{map[string]any{"id": 55, "body": "fix", "created_at": "2026-08-02T00:00:00Z", "user": map[string]any{"id": 5}}},
		"/user/5": map[string]any{"login": "owner"}, "/repos/o/r/collaborators/owner/permission": map[string]any{"permission": "write"},
	}
	api := fixtureAPI(t, responses)
	recovery := &recoveryStub{state: PRState{Facts: PRFacts{ValidationSHA: "abcdef0", DocumentationSHA: "abcdef0", Feedback: []Feedback{{ID: 55, State: FeedbackAddressed, Delegated: true}}}}}
	source := GitHubPRSource{API: api, Config: PRAdapterConfig{Repository: "o/r", ReadyLabel: "ready", AutonomousMergeLabel: "auto", HumanReviewLabel: "review", ActorID: 42}, Recovery: recovery}
	state, err := source.FreshPullRequest(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	if state.MergeAttemptSHA != "abcdef0" || state.Facts.RequiredChecksPass || state.Facts.Approved || state.Facts.CodeOwnerApproved || state.Facts.LastPushApproved || state.Facts.Feedback[0].State != FeedbackPending || !state.Facts.Feedback[0].Delegated || len(state.PendingDispositions) != 1 {
		t.Fatalf("unsafe facts: %#v", state)
	}
}

func TestFeedbackDispositionRequiresFreshCanonicalGitHubConfirmation(t *testing.T) {
	feedback := Feedback{ID: 55, Source: feedbackInline, State: FeedbackAddressed, Execution: FeedbackCompleted, Delegated: true}
	body, _ := FeedbackDispositionBody(10, 2, feedback, "")
	for _, test := range []struct {
		name string
		body string
		want bool
	}{
		{"canonical", body, true},
		{"non canonical", "prefix" + body, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			comment := map[string]any{"id": 9, "body": test.body, "user": map[string]any{"id": 42}}
			state := PRState{HeadSHA: "abcdef0", Facts: PRFacts{Feedback: []Feedback{feedback}}}
			source := GitHubPRSource{API: fixtureAPI(t, map[string]any{"/repos/o/r/issues/10/comments?per_page=100&page=1": []any{comment}}), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
			if err := source.readIssueComments(context.Background(), 10, 2, &state); err != nil || (len(state.ConfirmedDispositions) == 1) != test.want {
				t.Fatalf("state=%#v err=%v", state, err)
			}
		})
	}
}

func TestOpenPullRequestsSkipsUnrelatedAndPaginates(t *testing.T) {
	first := make([]map[string]any, 100)
	for i := range first {
		first[i] = map[string]any{"number": i + 1, "body": "ordinary pull request"}
	}
	first[99] = map[string]any{"number": 100, "body": "<!-- agent-symphony:issue:10:attempt:2 -->"}
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/pulls?state=open&per_page=100&page=1": first,
		"/repos/o/r/pulls?state=open&per_page=100&page=2": []any{
			map[string]any{"number": 101, "body": "prefix agent-symphony:issue:10:attempt:2 suffix"},
			map[string]any{"number": 102, "body": "<!-- agent-symphony:issue:11:attempt:3 -->"},
		},
	}), Config: PRAdapterConfig{Repository: "o/r"}}
	got, err := source.OpenPullRequests(context.Background())
	if err != nil || fmt.Sprint(got) != "[100 102]" {
		t.Fatalf("pull requests=%v err=%v", got, err)
	}
}

func TestAuthorizedControlsFailClosedWithoutProductionConfiguration(t *testing.T) {
	facts := PRFacts{IssueOpen: true, IssueEligible: true, AutonomousMerge: true}
	source := GitHubPRSource{}
	if err := source.readAuthorizedControls(context.Background(), 10, &facts); err != nil || facts.IssueEligible || facts.AutonomousMerge || !facts.NeedsHumanReview {
		t.Fatalf("raw labels/local facts survived: %#v err=%v", facts, err)
	}
}

func TestHumanReviewLabelRequiresExactVerifiedCompletionProvenance(t *testing.T) {
	now := time.Now()
	events := []issueTimelineEvent{{ID: 7, Event: "labeled", CreatedAt: now}}
	events[0].Actor.ID = 5
	events[0].Label.Name = "review"
	verified := []Provenance{{Name: "completion", Value: "human-review", Source: "timeline", EventID: 7, ActorID: 5, CreatedAt: now}}
	if !reviewLabelMatches(verified, events, "review") {
		t.Fatal("exact authorized label did not require review")
	}
	for _, provenance := range [][]Provenance{
		{{Name: "completion", Value: "human-review", Source: "creation", ActorID: 9, CreatedAt: now}},
		{{Name: "completion", Value: "human-review", Source: "timeline", EventID: 6, ActorID: 5, CreatedAt: now}},
	} {
		if reviewLabelMatches(provenance, events, "review") {
			t.Fatalf("absent or stale label required review: %#v", provenance)
		}
	}
	events[0].Event = "unlabeled"
	if reviewLabelMatches(verified, events, "review") {
		t.Fatal("verified label removal still required review")
	}
}

func TestIssueEvidenceRequiresCoordinatorAttributionAndAllPages(t *testing.T) {
	first := make([]map[string]any, 100)
	for i := range first {
		first[i] = map[string]any{"id": i + 1, "body": "noise"}
	}
	first[0] = map[string]any{"id": 1, "body": "Agent Symphony validation evidence for head `tampered`.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}}
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/issues/10/comments?per_page=100&page=1": first,
		"/repos/o/r/issues/10/comments?per_page=100&page=2": []any{
			map[string]any{"id": 101, "body": "Agent Symphony validation evidence for head `abcdef0`.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}},
			map[string]any{"id": 102, "body": "Agent Symphony documentation evidence for head `abcdef0`.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}},
		},
	}), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
	state := PRState{HeadSHA: "abcdef0", Facts: PRFacts{ValidationSHA: "local", DocumentationSHA: "local"}}
	if err := source.readIssueComments(context.Background(), 10, 2, &state); err != nil || state.Facts.ValidationSHA != "abcdef0" || state.Facts.DocumentationSHA != "abcdef0" {
		t.Fatalf("facts=%#v err=%v", state.Facts, err)
	}
}

func TestSameUserFeedbackAllowedAndCoordinatorArtifactsFiltered(t *testing.T) {
	artifact, _ := AttributedBody(9, 1, "Attempt published for review.")
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/issues/3/comments?per_page=100&page=1": []any{
			map[string]any{"id": 1, "body": "please fix this", "user": map[string]any{"id": 42}},
			map[string]any{"id": 2, "body": artifact, "user": map[string]any{"id": 42}},
		},
		"/repos/o/r/pulls/3/comments?per_page=100&page=1": []any{},
		"/repos/o/r/pulls/3/reviews?per_page=100&page=1":  []any{},
		"/graphql":                     map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewDecision": nil}}}},
		"/repos/o/r/issues/comments/1": map[string]any{"id": 1, "body": "please fix this", "user": map[string]any{"id": 42}},
		"/repos/o/r/issues/comments/2": map[string]any{"id": 2, "body": artifact, "user": map[string]any{"id": 42}},
		"/user/42":                     map[string]any{"login": "coordinator"},
		"/repos/o/r/collaborators/coordinator/permission": map[string]any{"permission": "admin"},
	}), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
	comments, err := source.readFeedback(context.Background(), 3)
	if err != nil || len(comments) != 1 || comments[0].ID != 1 {
		t.Fatalf("comments=%#v err=%v", comments, err)
	}
	fresh, err := source.FreshFeedback(context.Background(), PRState{Repository: "o/r", Number: 3}, Feedback{ID: 1, Source: feedbackConversation})
	if err != nil || !fresh.Authorized {
		t.Fatalf("fresh=%#v err=%v", fresh, err)
	}
	fresh, err = source.FreshFeedback(context.Background(), PRState{Repository: "o/r", Number: 3}, Feedback{ID: 2, Source: feedbackConversation})
	if err != nil || fresh.Authorized {
		t.Fatalf("artifact=%#v err=%v", fresh, err)
	}
}

func TestUnavailableBranchProtectionContinuesFailClosedAndIdempotent(t *testing.T) {
	unavailable := fixtureHTTP{http.StatusForbidden, `{"message":"Upgrade to GitHub Pro or make this repository public to enable this feature.","documentation_url":"https://docs.github.com/rest/repos/rules#get-rules-for-a-branch","status":"403"}`}
	available := []any{map[string]any{"type": "required_status_checks", "parameters": map[string]any{"required_status_checks": []any{map[string]any{"context": PolicyCheck, "integration_id": 0}}}}}
	for _, test := range []struct {
		name       string
		protection fixtureHTTP
		rules      any
		allows     bool
		policy     bool
	}{
		{"available after classic 404", fixtureHTTP{http.StatusNotFound, `{"message":"Branch not protected"}`}, available, true, true},
		{"available after classic unavailable", fixtureHTTP{http.StatusForbidden, `{"message":"Upgrade to GitHub Pro or make this repository public to enable this feature.","documentation_url":"https://docs.github.com/rest/branches/branch-protection#get-branch-protection","status":"403"}`}, available, false, true},
		{"unavailable after classic 404", fixtureHTTP{http.StatusNotFound, `{"message":"Branch not protected"}`}, unavailable, false, false},
		{"unavailable after classic unavailable", fixtureHTTP{http.StatusForbidden, `{"message":"Upgrade to GitHub Pro or make this repository public to enable this feature.","documentation_url":"https://docs.github.com/rest/branches/branch-protection#get-branch-protection","status":"403"}`}, unavailable, false, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			responses := map[string]any{
				"/repos/o/r/pulls/3":                                               map[string]any{"number": 3, "state": "open", "merged": false, "body": "<!-- agent-symphony:issue:10:attempt:2 -->", "mergeable": true, "head": map[string]any{"sha": "abc"}, "base": map[string]any{"ref": "main"}},
				"/repos/o/r/issues/10":                                             map[string]any{"state": "open", "labels": []any{}},
				"/repos/o/r/issues/10/comments?per_page=100&page=1":                []any{},
				"/repos/o/r/branches/main/protection":                              test.protection,
				"/repos/o/r/rules/branches/main?per_page=100&page=1":               test.rules,
				"/repos/o/r/commits/abc/check-runs?filter=all&per_page=100&page=1": map[string]any{"check_runs": []any{}},
				"/repos/o/r/commits/abc/statuses?per_page=100&page=1":              []any{},
				"/repos/o/r": map[string]any{"permissions": map[string]any{"push": true}},
				"/repos/o/r/issues/3/comments?per_page=100&page=1": []any{},
				"/repos/o/r/pulls/3/comments?per_page=100&page=1":  []any{},
				"/repos/o/r/pulls/3/reviews?per_page=100&page=1":   []any{},
				"/graphql": map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewDecision": nil}}}},
			}
			source := GitHubPRSource{API: fixtureAPI(t, responses), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}, Recovery: &recoveryStub{state: PRState{}}}
			first, err := source.FreshPullRequest(context.Background(), 3)
			second, again := source.FreshPullRequest(context.Background(), 3)
			if err != nil || again != nil || first.Facts.BranchProtectionAllows != test.allows || !reflect.DeepEqual(first, second) {
				t.Fatalf("first=%#v second=%#v err=%v again=%v", first, second, err, again)
			}
			if first.Facts.PolicyCheckRequired != test.policy {
				t.Fatalf("available rules were not evaluated: %#v", first.Facts)
			}
		})
	}
}

func TestClassicProtectionPreservesZeroApprovalCount(t *testing.T) {
	for _, test := range []struct {
		name, setting string
		required      bool
	}{
		{"zero count", "", false},
		{"code owners", "require_code_owner_reviews", true},
		{"last push", "require_last_push_approval", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			reviews := map[string]any{"required_approving_review_count": 0}
			if test.setting != "" {
				reviews[test.setting] = true
			}
			responses := map[string]any{
				"/repos/o/r/pulls/3":                                               map[string]any{"number": 3, "state": "open", "merged": false, "body": "<!-- agent-symphony:issue:10:attempt:2 -->", "mergeable": true, "head": map[string]any{"sha": "abc"}, "base": map[string]any{"ref": "main"}},
				"/repos/o/r/issues/10":                                             map[string]any{"state": "open", "labels": []any{}},
				"/repos/o/r/issues/10/comments?per_page=100&page=1":                []any{},
				"/repos/o/r/branches/main/protection":                              map[string]any{"required_pull_request_reviews": reviews},
				"/repos/o/r/rules/branches/main?per_page=100&page=1":               []any{},
				"/repos/o/r/commits/abc/check-runs?filter=all&per_page=100&page=1": map[string]any{"check_runs": []any{}},
				"/repos/o/r/commits/abc/statuses?per_page=100&page=1":              []any{},
				"/repos/o/r": map[string]any{"permissions": map[string]any{"push": true}},
				"/repos/o/r/issues/3/comments?per_page=100&page=1": []any{},
				"/repos/o/r/pulls/3/comments?per_page=100&page=1":  []any{},
				"/repos/o/r/pulls/3/reviews?per_page=100&page=1":   []any{},
			}
			decision := "APPROVED"
			if !test.required {
				decision = "CHANGES_REQUESTED"
				responses["/repos/o/r/pulls/3/reviews?per_page=100&page=1"] = []any{map[string]any{"id": 1, "state": decision, "user": map[string]any{"id": 5}}}
			}
			responses["/graphql"] = map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewDecision": decision}}}}
			state, err := (&GitHubPRSource{API: fixtureAPI(t, responses), Config: PRAdapterConfig{Repository: "o/r"}, Recovery: &recoveryStub{}}).FreshPullRequest(context.Background(), 3)
			if err != nil || state.Facts.ApprovalRequired != test.required || state.Facts.Approved != test.required || state.Facts.ChangesRequested == test.required {
				t.Fatalf("facts=%#v err=%v", state.Facts, err)
			}
		})
	}
}

func TestClassicProtectionOtherForbiddenResponsesFail(t *testing.T) {
	for _, body := range []string{
		`{"message":"forbidden"}`,
		`{"message":"Upgrade to GitHub Pro or make this repository public to enable this feature.","documentation_url":"https://docs.github.com/rest/branches/branch-protection/other","status":"403"}`,
		`{"message":"Upgrade to GitHub Pro or make this repository public to enable this feature.","documentation_url":"https://docs.github.com/rest/repos/rules#get-rules-for-a-branch","status":"403"}`,
	} {
		responses := map[string]any{
			"/repos/o/r/pulls/3":                                map[string]any{"number": 3, "state": "open", "merged": false, "body": "<!-- agent-symphony:issue:10:attempt:2 -->", "mergeable": true, "head": map[string]any{"sha": "abc"}, "base": map[string]any{"ref": "main"}},
			"/repos/o/r/issues/10":                              map[string]any{"state": "open", "labels": []any{}},
			"/repos/o/r/issues/10/comments?per_page=100&page=1": []any{},
			"/repos/o/r/branches/main/protection":               fixtureHTTP{http.StatusForbidden, body},
		}
		source := GitHubPRSource{API: fixtureAPI(t, responses), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}, Recovery: &recoveryStub{state: PRState{}}}
		if _, err := source.FreshPullRequest(context.Background(), 3); err == nil {
			t.Fatalf("unrelated protection failure was ignored: %s", body)
		}
	}
}

func TestRulesUnavailableRejectsAllOtherFailures(t *testing.T) {
	message := "Upgrade to GitHub Pro or make this repository public to enable this feature."
	documentationURL := "https://docs.github.com/rest/repos/rules#get-rules-for-a-branch"
	body := fmt.Sprintf(`{"message":%q,"documentation_url":%q,"status":"403"}`, message, documentationURL)
	for _, test := range []struct {
		name string
		api  API
	}{
		{"generic 403", fixtureAPI(t, map[string]any{"/repos/o/r/rules/branches/main?per_page=100&page=1": fixtureHTTP{http.StatusForbidden, `{"message":"forbidden"}`}})},
		{"lookalike URL", fixtureAPI(t, map[string]any{"/repos/o/r/rules/branches/main?per_page=100&page=1": fixtureHTTP{http.StatusForbidden, strings.Replace(body, documentationURL, documentationURL+"-lookalike", 1)}})},
		{"classic URL", fixtureAPI(t, map[string]any{"/repos/o/r/rules/branches/main?per_page=100&page=1": fixtureHTTP{http.StatusForbidden, strings.Replace(body, documentationURL, classicProtectionDocumentationURL, 1)}})},
		{"authentication", fixtureAPI(t, map[string]any{"/repos/o/r/rules/branches/main?per_page=100&page=1": fixtureHTTP{http.StatusUnauthorized, body}})},
		{"malformed", fixtureAPI(t, map[string]any{"/repos/o/r/rules/branches/main?per_page=100&page=1": fixtureHTTP{http.StatusForbidden, "not json"}})},
		{"oversized", fixtureAPI(t, map[string]any{"/repos/o/r/rules/branches/main?per_page=100&page=1": fixtureHTTP{http.StatusForbidden, body + strings.Repeat(" ", 4096)}})},
		{"read failure", API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{StatusCode: http.StatusForbidden, Status: "403 Forbidden", Body: io.NopCloser(&readErrorAfterBody{body: body})}, nil
		})}}},
		{"transport failure", API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
			return nil, fmt.Errorf("connection reset")
		})}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			facts := PRFacts{BranchProtectionAllows: true}
			source := GitHubPRSource{API: test.api, Config: PRAdapterConfig{Repository: "o/r"}}
			if err := source.readRules(context.Background(), "main", nil, new(int), new(bool), &facts); err == nil || !facts.BranchProtectionAllows {
				t.Fatalf("failure was ignored or changed protection: facts=%#v err=%v", facts, err)
			}
		})
	}
}

func TestReadRulesAvailablePaginationUnchanged(t *testing.T) {
	first := make([]any, 100)
	for i := range first {
		first[i] = map[string]any{"type": "required_status_checks", "parameters": map[string]any{"required_status_checks": []any{map[string]any{"context": fmt.Sprintf("check-%d", i)}}}}
	}
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/rules/branches/main?per_page=100&page=1": first,
		"/repos/o/r/rules/branches/main?per_page=100&page=2": []any{map[string]any{"type": "pull_request", "parameters": map[string]any{"required_approving_review_count": 2, "dismiss_stale_reviews_on_push": true, "require_code_owner_review": true, "require_last_push_approval": true}}},
	}), Config: PRAdapterConfig{Repository: "o/r"}}
	var required []requiredCheck
	count, dismissStale := 0, false
	facts := PRFacts{BranchProtectionAllows: true}
	if err := source.readRules(context.Background(), "main", &required, &count, &dismissStale, &facts); err != nil || len(required) != 100 || count != 2 || !dismissStale || !facts.CodeOwnerApprovalRequired || !facts.LastPushApprovalRequired || !facts.BranchProtectionAllows {
		t.Fatalf("required=%d count=%d dismiss=%v facts=%#v err=%v", len(required), count, dismissStale, facts, err)
	}
}

func TestPullRequestMergedFieldIsRequired(t *testing.T) {
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/pulls/3": map[string]any{"number": 3, "state": "open", "body": "<!-- agent-symphony:issue:10:attempt:2 -->", "head": map[string]any{"sha": "abc"}},
	}), Config: PRAdapterConfig{Repository: "o/r"}}
	if _, err := source.FreshPullRequest(context.Background(), 3); err == nil {
		t.Fatal("missing merged field was accepted")
	}
}

func TestRunPRReconciliationConstructsAndExecutes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	api := fixtureAPI(t, map[string]any{"/repos/o/r": map[string]any{"full_name": "o/r", "permissions": map[string]any{"pull": true}}, "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1": []any{}, "/repos/o/r/pulls?state=open&per_page=100&page=1": []any{}})
	cfg := productionPRConfig()
	if err := RunPRReconciliation(context.Background(), api, cfg, path); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("initialized state info=%v err=%v", info, err)
	}
	if states, err := (&FileRecovery{Path: path}).read(); err != nil || len(states) != 0 {
		t.Fatalf("initialized state=%#v err=%v", states, err)
	}
	if err := os.WriteFile(path, []byte("not json\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := RunPRReconciliation(context.Background(), api, cfg, path); err == nil {
		t.Fatal("production reconciliation skipped its authoritative recovery-state read")
	}
	if b, err := os.ReadFile(path); err != nil || string(b) != "not json\n" {
		t.Fatalf("invalid state was replaced: %q err=%v", b, err)
	}
	cfg.ApprovalCommand = ""
	if err := RunPRReconciliation(context.Background(), api, cfg, path); err == nil {
		t.Fatal("production reconciliation accepted no GitHub control verifier configuration")
	}
}

func TestRunPRReconciliationHydratesPublishedAttemptAndHandsOffForHumanReview(t *testing.T) {
	for _, precreate := range []bool{false, true} {
		name := "absent state"
		if precreate {
			name = "empty state"
		}
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			if precreate {
				if err := os.WriteFile(path, []byte("[]\n"), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cfg := productionPRConfig()
			now := time.Date(2026, 8, 7, 0, 0, 0, 0, time.UTC)
			body := "## Context\nx\n## Acceptance Criteria\nx\n## Tasks\nx\n## Validation\nx\n## Dependencies\nnone\n"
			controls := NormalizeIssue(IssueInput{Number: 10, State: "open", Body: body, Labels: []string{"ready", "P3"}}, ContractConfig{Ready: "ready", P1: "P1", P2: "P2", P3: "P3", DependencySection: "Dependencies", DefaultCompletion: "human-review", HumanReview: "review", AutonomousMerge: "auto"}, nil).Controls
			provenance := []Provenance{
				{Name: "ready", Value: "true", Source: "timeline", EventID: 20, ActorID: 5, CreatedAt: now},
				{Name: "priority", Value: "3", Source: "timeline", EventID: 21, ActorID: 5, CreatedAt: now.Add(time.Second)},
				{Name: "completion", Value: "human-review", Source: "creation", ActorID: 5, CreatedAt: now},
				{Name: "closed", Value: "false", Source: "creation", ActorID: 5, CreatedAt: now},
				{Name: "cancelled", Value: "false", Source: "creation", ActorID: 5, CreatedAt: now},
				{Name: "retry", Value: "false", Source: "creation", ActorID: 5, CreatedAt: now},
			}
			approval := Approval{CommentID: 50, ActorID: 5, Body: "/approve", CreatedAt: now.Add(time.Minute)}
			snapshot, err := NewSnapshot(controls, body, Anchor{IssueNodeID: "I_10", CreatedAt: now, ChangedAt: now, AuthorID: 5}, approval, provenance, cfg.ApprovalCommand, func(id int) bool { return id == 5 }, timelineFor(provenance))
			if err != nil {
				t.Fatal(err)
			}
			branch, _ := AttemptBranch("o/r", 10, 2)
			marker, _ := AttemptMarker(10, 2, branch, "abcdef0", 3, "review")
			prBody := "Closes #10\n\n<!-- agent-symphony:issue:10:attempt:2 -->\n\n" + marker
			comments := []any{
				map[string]any{"id": 60, "body": SnapshotComment(snapshot), "user": map[string]any{"id": 42}},
				map[string]any{"id": 61, "body": marker, "user": map[string]any{"id": 42}},
			}
			responses := map[string]any{
				"/repos/o/r": map[string]any{"full_name": "o/r", "permissions": map[string]any{"pull": true, "push": true}},
				"/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1": []any{map[string]any{"number": 3, "body": prBody, "state": "open", "head": map[string]any{"sha": "abcdef0", "ref": branch}, "base": map[string]any{"sha": "bbbbbbb"}, "user": map[string]any{"id": 42}}},
				"/repos/o/r/pulls?state=open&per_page=100&page=1":                           []any{map[string]any{"number": 3, "body": prBody}},
				"/repos/o/r/issues/10": map[string]any{"number": 10, "node_id": "I_10", "state": "open", "body": body, "created_at": now, "user": map[string]any{"id": 5}, "labels": []any{map[string]any{"name": "ready"}, map[string]any{"name": "P3"}}},
				"/repos/o/r/issues/10/timeline?per_page=100&page=1": []any{
					map[string]any{"id": 20, "event": "labeled", "label": map[string]any{"name": "ready"}, "created_at": now, "actor": map[string]any{"id": 5}},
					map[string]any{"id": 21, "event": "labeled", "label": map[string]any{"name": "P3"}, "created_at": now.Add(time.Second), "actor": map[string]any{"id": 5}},
				},
				"/repos/o/r/issues/comments/50": map[string]any{"id": 50, "body": "/approve", "created_at": approval.CreatedAt, "updated_at": approval.CreatedAt, "user": map[string]any{"id": 5}},
				"/user/5":                       map[string]any{"login": "owner"},
				"/repos/o/r/collaborators/owner/permission":                               map[string]any{"permission": "maintain"},
				"/repos/o/r/commits/abcdef0/check-runs?filter=latest&per_page=100&page=1": map[string]any{"check_runs": []any{}},
				"/repos/o/r/commits/abcdef0/check-runs?filter=all&per_page=100&page=1":    map[string]any{"check_runs": []any{}},
				"/repos/o/r/commits/abcdef0/status":                                       map[string]any{"statuses": []any{}},
				"/repos/o/r/branches/main/protection":                                     fixtureHTTP{http.StatusNotFound, `{"message":"not protected"}`},
				"/repos/o/r/rules/branches/main?per_page=100&page=1":                      []any{},
				"/repos/o/r/commits/abcdef0/statuses?per_page=100&page=1":                 []any{},
				"/repos/o/r/issues/3/comments?per_page=100&page=1":                        []any{},
				"/repos/o/r/pulls/3/comments?per_page=100&page=1":                         []any{},
				"/repos/o/r/pulls/3/reviews?per_page=100&page=1":                          []any{},
				"/graphql": map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}, "pullRequest": map[string]any{"reviewDecision": nil}}}},
			}
			labelPresent, checkCreated, dropMarkerAfterFetch := false, false, false
			freshMutation, policyComment := "", ""
			commentReads, labelPosts, policyMutations := 0, 0, 0
			api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				switch req.Method + " " + req.URL.RequestURI() {
				case "GET /repos/o/r/pulls/3":
					labels := []any{}
					if labelPresent {
						labels = append(labels, map[string]any{"name": "review"})
					}
					currentBody, headSHA, headRef := prBody, "abcdef0", branch
					switch freshMutation {
					case "marker head":
						currentBody = strings.Replace(prBody, `"head":"abcdef0"`, `"head":"1234567"`, 1)
					case "head SHA":
						headSHA = "1234567"
					case "head ref":
						headRef = "other"
					}
					value := map[string]any{"number": 3, "body": currentBody, "state": "open", "merged": false, "mergeable": true, "head": map[string]any{"sha": headSHA, "ref": headRef}, "base": map[string]any{"ref": "main"}, "labels": labels}
					b, _ := json.Marshal(value)
					return httpResponse(http.StatusOK, string(b), nil), nil
				case "GET /repos/o/r/issues/10/comments?per_page=100&page=1":
					commentReads++
					current := comments
					if dropMarkerAfterFetch && commentReads > 1 {
						current = comments[:1]
					}
					b, _ := json.Marshal(current)
					return httpResponse(http.StatusOK, string(b), nil), nil
				case "GET /repos/o/r/commits/abcdef0/statuses?per_page=100&page=1":
					statuses := []any{}
					if checkCreated {
						statuses = append(statuses, map[string]any{"context": PolicyCheck, "state": "pending", "creator": map[string]any{"id": 42}})
					}
					b, _ := json.Marshal(statuses)
					return httpResponse(http.StatusOK, string(b), nil), nil
				case "POST /repos/o/r/issues/3/labels":
					labelPresent, labelPosts = true, labelPosts+1
					return httpResponse(http.StatusOK, `[]`, nil), nil
				case "POST /repos/o/r/statuses/abcdef0":
					checkCreated, policyMutations = true, policyMutations+1
					return httpResponse(http.StatusCreated, `{}`, nil), nil
				case "GET /repos/o/r/issues/3/comments?per_page=100&page=1":
					if policyComment == "" {
						return httpResponse(http.StatusOK, `[]`, nil), nil
					}
					body, _ := json.Marshal([]any{map[string]any{"body": policyComment, "user": map[string]any{"id": 42}}})
					return httpResponse(http.StatusOK, string(body), nil), nil
				case "POST /repos/o/r/issues/3/comments":
					var payload struct{ Body string }
					if json.NewDecoder(req.Body).Decode(&payload) != nil {
						t.Fatal("invalid policy comment")
					}
					policyComment = payload.Body
					return httpResponse(http.StatusCreated, `{}`, nil), nil
				}
				value, ok := responses[req.URL.RequestURI()]
				if !ok {
					return nil, fmt.Errorf("unexpected fixture request %s %s", req.Method, req.URL.RequestURI())
				}
				if response, ok := value.(fixtureHTTP); ok {
					return httpResponse(response.status, response.body, nil), nil
				}
				b, _ := json.Marshal(value)
				return httpResponse(http.StatusOK, string(b), nil), nil
			})}}
			for range 2 {
				if err := RunPRReconciliation(context.Background(), api, cfg, path); err != nil {
					t.Fatal(err)
				}
			}
			states, err := (&FileRecovery{Path: path}).read()
			if err != nil || len(states) != 1 || states[0].Number != 3 || states[0].Issue != 10 || states[0].Attempt != 2 || states[0].HeadSHA != "abcdef0" || labelPosts != 0 {
				t.Fatalf("states=%#v label posts=%d err=%v", states, labelPosts, err)
			}
			labelPresent, dropMarkerAfterFetch, commentReads = false, true, 0
			beforePolicy := policyMutations
			if err := RunPRReconciliation(context.Background(), api, cfg, path); err == nil || labelPosts != 0 || policyMutations != beforePolicy {
				t.Fatalf("post-fetch App marker removal governed PR: label posts=%d policy mutations=%d err=%v", labelPosts, policyMutations-beforePolicy, err)
			}
			dropMarkerAfterFetch = false
			for _, freshMutation = range []string{"marker head", "head SHA", "head ref"} {
				beforePolicy = policyMutations
				if err := RunPRReconciliation(context.Background(), api, cfg, path); err == nil || labelPosts != 0 || policyMutations != beforePolicy {
					t.Fatalf("%s drift governed PR: label posts=%d policy mutations=%d err=%v", freshMutation, labelPosts, policyMutations-beforePolicy, err)
				}
			}
			unchanged, readErr := (&FileRecovery{Path: path}).read()
			if readErr != nil || !reflect.DeepEqual(states, unchanged) {
				t.Fatalf("authority loss changed durable state: before=%#v after=%#v err=%v", states, unchanged, readErr)
			}
		})
	}
}

func TestRunPRReconciliationSerializesWholeRun(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	var mu sync.Mutex
	inFlight, overlap, pulls := 0, false, 0
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/repos/o/r" {
			return httpResponse(http.StatusOK, `{"full_name":"o/r","permissions":{"pull":true}}`, nil), nil
		}
		mu.Lock()
		inFlight++
		overlap = overlap || inFlight > 1
		pulls++
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		inFlight--
		mu.Unlock()
		return httpResponse(http.StatusOK, `[]`, nil), nil
	})}}
	errCh := make(chan error, 2)
	for range 2 {
		go func() { errCh <- RunPRReconciliation(context.Background(), api, productionPRConfig(), path) }()
	}
	for range 2 {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	if overlap || pulls != 2 {
		t.Fatalf("overlap=%v pull reads=%d", overlap, pulls)
	}
	if states, err := (&FileRecovery{Path: path}).read(); err != nil || len(states) != 0 {
		t.Fatalf("concurrent initial state=%#v err=%v", states, err)
	}
}

func TestRunPRReconciliationIgnoresUnverifiedDurableState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := []PRState{{Repository: "o/r", Number: 3, Issue: 10, Attempt: 1}}
	b, _ := json.Marshal(state)
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	pulls := 0
	api := API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/repos/o/r":
			return httpResponse(http.StatusOK, `{"full_name":"o/r","permissions":{"pull":true}}`, nil), nil
		case "/repos/o/r/issues/10":
			return httpResponse(http.StatusForbidden, `{"message":"denied"}`, nil), nil
		default:
			if r.URL.RawQuery == "state=open&per_page=100&page=1" {
				pulls++
			}
			return httpResponse(http.StatusOK, `[]`, nil), nil
		}
	})}}
	if err := RunPRReconciliation(context.Background(), api, productionPRConfig(), path); err != nil || pulls != 0 {
		t.Fatalf("err=%v pulls=%d", err, pulls)
	}
}

func TestIssueCommentsRecoverExactDecisionAndIgnoreAmbiguousText(t *testing.T) {
	want, _ := AttributedBody(10, 2, "Implementation decision d1\n\nkeep it small")
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{"/repos/o/r/issues/10/comments?per_page=100&page=1": []any{
		map[string]any{"id": 1, "body": "Implementation decision d1 keep it small <!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}},
		map[string]any{"id": 2, "body": want, "user": map[string]any{"id": 42}},
	}}), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
	state := PRState{HeadSHA: "abcdef0", Decisions: []Decision{{ID: "d1", Body: "keep it small"}, {ID: "d2", Body: "keep it small"}}}
	if err := source.readIssueComments(context.Background(), 10, 2, &state); err != nil {
		t.Fatal(err)
	}
	if !state.Decisions[0].Recorded || state.Decisions[1].Recorded {
		t.Fatalf("decisions=%#v", state.Decisions)
	}
}

func productionPRConfig() PRAdapterConfig {
	return PRAdapterConfig{Repository: "o/r", ReadyLabel: "ready", HumanReviewLabel: "review", AutonomousMergeLabel: "auto", MergeMethod: "squash", PriorityP1Label: "P1", PriorityP2Label: "P2", PriorityP3Label: "P3", DependencySection: "Dependencies", DefaultCompletion: "human-review", ApprovalCommand: "/approve", CancelCommand: "/cancel", RetryCommand: "/retry", ActorID: 42}
}

func TestProductionAuthorizedControlsVerifyGitHubSnapshot(t *testing.T) {
	cfg := productionPRConfig()
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	body := "## Context\nx\n## Acceptance Criteria\nx\n## Tasks\nx\n## Validation\nx\n## Dependencies\nnone\n"
	controls := NormalizeIssue(IssueInput{Number: 10, State: "open", Body: body, Labels: []string{"ready", "P1", "auto"}}, ContractConfig{Ready: "ready", P1: "P1", P2: "P2", P3: "P3", DependencySection: "Dependencies", DefaultCompletion: "human-review", HumanReview: "review", AutonomousMerge: "auto"}, nil).Controls
	provenance := provenanceFor(controls, 5)
	for i := range provenance {
		provenance[i].EventID = int64(i + 20)
	}
	anchor := Anchor{IssueNodeID: "I_10", CreatedAt: now, ChangedAt: now, AuthorID: 9}
	approval := Approval{CommentID: 50, ActorID: 5, Body: "/approve", CreatedAt: now.Add(time.Minute)}
	events := []map[string]any{
		{"id": nil, "node_id": nil, "event": "cross-referenced"},
		{"id": int64(20), "event": "labeled", "label": map[string]any{"name": "ready"}, "created_at": now, "actor": map[string]any{"id": 5}},
		{"id": int64(21), "event": "labeled", "label": map[string]any{"name": "P1"}, "created_at": now.Add(time.Second), "actor": map[string]any{"id": 5}},
		{"id": int64(22), "event": "labeled", "label": map[string]any{"name": "auto"}, "created_at": now.Add(2 * time.Second), "actor": map[string]any{"id": 5}},
	}
	provenance = []Provenance{
		{Name: "ready", Value: "true", Source: "timeline", EventID: 20, ActorID: 5, CreatedAt: now},
		{Name: "priority", Value: "1", Source: "timeline", EventID: 21, ActorID: 5, CreatedAt: now.Add(time.Second)},
		{Name: "completion", Value: "autonomous-merge", Source: "timeline", EventID: 22, ActorID: 5, CreatedAt: now.Add(2 * time.Second)},
		{Name: "closed", Value: "false", Source: "creation", ActorID: 9, CreatedAt: now},
		{Name: "cancelled", Value: "false", Source: "creation", ActorID: 9, CreatedAt: now},
		{Name: "retry", Value: "false", Source: "creation", ActorID: 9, CreatedAt: now},
	}
	snapshot, err := NewSnapshot(controls, body, anchor, approval, provenance, cfg.ApprovalCommand, func(id int) bool { return id == 5 }, timelineFor(provenance))
	if err != nil {
		t.Fatal(err)
	}
	responses := func() map[string]any {
		return map[string]any{
			"/graphql":             map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}}}},
			"/repos/o/r/issues/10": map[string]any{"number": 10, "node_id": "I_10", "state": "open", "body": body, "created_at": now, "user": map[string]any{"id": 9}, "labels": []any{map[string]any{"name": "ready"}, map[string]any{"name": "P1"}, map[string]any{"name": "auto"}}},
			"/repos/o/r/issues/10/timeline?per_page=100&page=1": events,
			"/repos/o/r/issues/10/comments?per_page=100&page=1": []any{map[string]any{"id": 60, "body": SnapshotComment(snapshot), "user": map[string]any{"id": 42}}},
			"/repos/o/r/issues/comments/50":                     map[string]any{"id": 50, "body": "/approve", "created_at": now.Add(time.Minute), "updated_at": now.Add(time.Minute), "user": map[string]any{"id": 5}},
			"/user/5":                                           map[string]any{"login": "owner"},
			"/repos/o/r/collaborators/owner/permission":         map[string]any{"permission": "maintain"},
		}
	}
	source := GitHubPRSource{API: fixtureAPI(t, responses()), Config: cfg}
	got, _, _, err := source.authorizedControls(context.Background(), 10)
	if err != nil || !reflect.DeepEqual(got, controls) {
		t.Fatalf("controls=%#v err=%v", got, err)
	}

	for _, test := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"edited approval", func(r map[string]any) {
			r["/repos/o/r/issues/comments/50"].(map[string]any)["body"] = "/approve edited"
		}},
		{"missing provenance", func(r map[string]any) {
			r["/repos/o/r/issues/10/timeline?per_page=100&page=1"] = events[:len(events)-1]
		}},
		{"revoked permission", func(r map[string]any) {
			r["/repos/o/r/collaborators/owner/permission"].(map[string]any)["permission"] = "write"
		}},
		{"edited issue body", func(r map[string]any) { r["/repos/o/r/issues/10"].(map[string]any)["body"] = body + "tampered" }},
		{"foreign coordinator snapshot", func(r map[string]any) {
			r["/repos/o/r/issues/10/comments?per_page=100&page=1"].([]any)[0].(map[string]any)["user"] = map[string]any{"id": 41}
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := responses()
			test.edit(r)
			if _, _, _, err := (&GitHubPRSource{API: fixtureAPI(t, r), Config: cfg}).authorizedControls(context.Background(), 10); err == nil {
				t.Fatal("tampered controls accepted")
			}
		})
	}
}

func TestIssueEvidenceRequiresExactCanonicalBody(t *testing.T) {
	canonical, _ := EvidenceBody(10, 2, "validation", "abcdef0")
	for _, body := range []string{"prefix" + canonical, canonical + "suffix", strings.Replace(canonical, "validation", "validation and documentation", 1)} {
		t.Run(body[:min(12, len(body))], func(t *testing.T) {
			source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
				"/repos/o/r/issues/10/comments?per_page=100&page=1": []any{map[string]any{"id": 1, "body": body, "user": map[string]any{"id": 42}}},
			}), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
			state := PRState{HeadSHA: "abcdef0"}
			if err := source.readIssueComments(context.Background(), 10, 2, &state); err != nil || state.Facts.ValidationSHA != "" || state.Facts.DocumentationSHA != "" {
				t.Fatalf("facts=%#v err=%v", state.Facts, err)
			}
		})
	}
}

func TestFileRecoveryDurablyQueuesFeedbackAndValidation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", Facts: PRFacts{Feedback: []Feedback{{ID: 55, State: FeedbackPending}}}}
	b, _ := json.Marshal([]PRState{state})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	recovery := FileRecovery{Path: path}
	claimed, err := recovery.ClaimFeedback(context.Background(), state, Feedback{ID: 55})
	if err != nil || !claimed {
		t.Fatalf("claimed=%v err=%v", claimed, err)
	}
	recovered, err := recovery.PullRequestState(context.Background(), "o/r", 3, 10, 2, "abcdef0")
	if err != nil || !recovered.Facts.Feedback[0].Delegated || recovered.Facts.Feedback[0].Execution != FeedbackClaimed || recovered.Facts.Feedback[0].ID != 55 {
		t.Fatalf("state=%#v err=%v", recovered, err)
	}
	if claimed, err = recovery.ClaimFeedback(context.Background(), state, Feedback{ID: 55}); err != nil || claimed {
		t.Fatalf("duplicate claimed=%v err=%v", claimed, err)
	}
	if err := recovery.QueueValidation(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	if err := recovery.QueueValidation(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	recovered, err = recovery.PullRequestState(context.Background(), "o/r", 3, 10, 2, "abcdef0")
	if err != nil || recovered.ValidationQueuedSHA != "abcdef0" {
		t.Fatalf("validation queue=%q err=%v", recovered.ValidationQueuedSHA, err)
	}

	for _, disposition := range []FeedbackState{FeedbackAddressed, FeedbackBlocked} {
		recovered.Facts.Feedback[0].State = disposition
		recovered.Facts.Feedback[0].Execution = FeedbackCompleted
		b, _ = json.Marshal([]PRState{recovered})
		if err := os.WriteFile(path, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if claimed, err := recovery.ClaimFeedback(context.Background(), state, Feedback{ID: 55}); err != nil || claimed {
			t.Fatalf("%s feedback requeued: claimed=%v err=%v", disposition, claimed, err)
		}
	}
}

func TestHandoffOutcomeRequiresImmutableKeyAndEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", ValidationQueuedSHA: "abcdef0", Facts: PRFacts{Feedback: []Feedback{{ID: 55, Source: feedbackInline, Execution: FeedbackClaimed}}}}
	b, _ := json.Marshal([]PRState{state})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	r := &FileRecovery{Path: path}
	handoffs, err := r.ClaimHandoffs(context.Background())
	if err != nil || len(handoffs) != 1 || handoffs[0].Key == "" {
		t.Fatalf("handoffs=%#v err=%v", handoffs, err)
	}
	if err := r.CompleteHandoffOutcome(context.Background(), handoffs[0], HandoffOutcome{Key: "wrong"}); err == nil {
		t.Fatal("accepted wrong immutable key")
	}
	if err := r.CompleteHandoffOutcome(context.Background(), handoffs[0], HandoffOutcome{Key: handoffs[0].Key, Retryable: true, ValidationResult: "failed", ValidationEvidence: "outage"}); err != nil {
		t.Fatal(err)
	}
	retry, err := r.ClaimHandoffs(context.Background())
	if err != nil || len(retry) != 1 || !retry[0].Validation {
		t.Fatalf("retry=%#v err=%v", retry, err)
	}
	outcome := HandoffOutcome{Key: handoffs[0].Key, Feedback: []FeedbackOutcome{{ID: 55, Source: feedbackInline, State: FeedbackBlocked, Evidence: "cannot safely apply"}}, ValidationResult: "failed", ValidationEvidence: "go test failed"}
	if err := r.CompleteHandoffOutcome(context.Background(), handoffs[0], outcome); err != nil {
		t.Fatal(err)
	}
	got, err := r.PullRequestState(context.Background(), "o/r", 3, 10, 2, "abcdef0")
	if err != nil || got.Facts.Feedback[0].State != FeedbackBlocked || got.Facts.Feedback[0].Execution != FeedbackCompleted || got.ValidationInFlightSHA != "" {
		t.Fatalf("got=%#v err=%v", got, err)
	}
}

func TestFileRecoveryDetectsRestartedForcePushAndPreservesEvidence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "published1"}
	b, _ := json.Marshal([]PRState{state})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	recovery := FileRecovery{Path: path}
	got, err := recovery.PullRequestState(context.Background(), "o/r", 3, 10, 2, "forced22")
	if err != nil || !got.Facts.BranchModifiedOutsideAttempt || got.HeadSHA != "published1" {
		t.Fatalf("force-pushed state=%#v err=%v", got, err)
	}
	state.Facts.BranchModifiedOutsideAttempt = true
	b, _ = json.Marshal([]PRState{state})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = recovery.PullRequestState(context.Background(), "o/r", 3, 10, 2, "published1")
	if err != nil || !got.Facts.BranchModifiedOutsideAttempt {
		t.Fatalf("stored evidence was overwritten: %#v err=%v", got, err)
	}
}

func TestFileRecoveryHydratesOnlyMissingAuthoritativeAttempts(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	recovery := &FileRecovery{Path: path}
	preserved := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", ValidationGeneration: 4, ValidationResult: "passed", ValidationEvidence: "tests", MergePhase: "prepared", HandoffReceipts: map[string]bool{"receipt": true}}
	if err := recovery.write([]PRState{preserved}); err != nil {
		t.Fatal(err)
	}
	facts := []RecoveryAttemptFact{
		{Repository: "o/r", PR: 3, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", State: "active"},
		{Repository: "o/r", PR: 4, Issue: 11, Attempt: 1, HeadSHA: "1234567", State: "review-ready"},
		{Repository: "o/r", PR: 5, Issue: 12, Attempt: 1, HeadSHA: "7654321", State: "completed"},
	}
	for range 2 {
		if err := recovery.hydrateAttempts("o/r", facts); err != nil {
			t.Fatal(err)
		}
	}
	got, err := recovery.read()
	if err != nil || len(got) != 2 || !reflect.DeepEqual(got[0], preserved) {
		t.Fatalf("hydrated=%#v err=%v", got, err)
	}
	want := PRState{Repository: "o/r", Number: 4, Issue: 11, Attempt: 1, HeadSHA: "1234567"}
	if !reflect.DeepEqual(got[1], want) {
		t.Fatalf("new state=%#v, want %#v", got[1], want)
	}
}

func TestFileRecoveryHydrationRejectsConflictsWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		name string
		fact RecoveryAttemptFact
	}{
		{"changed head", RecoveryAttemptFact{Repository: "o/r", PR: 3, Issue: 10, Attempt: 2, HeadSHA: "1234567", State: "active"}},
		{"rebound PR", RecoveryAttemptFact{Repository: "o/r", PR: 4, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", State: "active"}},
		{"rebound attempt", RecoveryAttemptFact{Repository: "o/r", PR: 3, Issue: 11, Attempt: 1, HeadSHA: "abcdef0", State: "active"}},
		{"foreign repository", RecoveryAttemptFact{Repository: "x/y", PR: 4, Issue: 11, Attempt: 1, HeadSHA: "abcdef0", State: "active"}},
		{"invalid identity", RecoveryAttemptFact{Repository: "o/r", PR: 0, Issue: 11, Attempt: 1, HeadSHA: "abcdef0", State: "active"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "state.json")
			recovery := &FileRecovery{Path: path}
			want := []PRState{{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", ValidationResult: "passed"}}
			if err := recovery.write(want); err != nil {
				t.Fatal(err)
			}
			if err := recovery.hydrateAttempts("o/r", []RecoveryAttemptFact{test.fact}); err == nil {
				t.Fatal("conflicting hydration succeeded")
			}
			got, err := recovery.read()
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("state mutated: %#v err=%v", got, err)
			}
		})
	}
}

func TestFileRecoveryRejectsClaimAfterHeadChangesUnderLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	stored := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "new", Facts: PRFacts{Feedback: []Feedback{{ID: 55}}}}
	b, _ := json.Marshal([]PRState{stored})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	r := FileRecovery{Path: path}
	stale := stored
	stale.HeadSHA = "old"
	if claimed, err := r.ClaimFeedback(context.Background(), stale, stale.Facts.Feedback[0]); err == nil || claimed {
		t.Fatalf("stale head claimed: claimed=%v err=%v", claimed, err)
	}
	got, err := r.PullRequestState(context.Background(), "o/r", 3, 10, 2, "new")
	if err != nil || got.Facts.Feedback[0].Execution != "" || got.Facts.Feedback[0].Delegated {
		t.Fatalf("stale claim mutated state: %#v err=%v", got, err)
	}
}

func TestFileRecoveryCorruptTerminalFeedbackCannotBecomeAuthoritative(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abcdef0", Facts: PRFacts{Feedback: []Feedback{{ID: 55, Source: feedbackInline, State: FeedbackAddressed, Execution: FeedbackCompleted}}}}
	b, _ := json.Marshal([]PRState{state})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := (&FileRecovery{Path: path}).PullRequestState(context.Background(), "o/r", 3, 10, 2, "abcdef0")
	if err != nil || got.Facts.Feedback[0].State != FeedbackAddressed {
		t.Fatalf("recovery must preserve queue data for publication: %#v err=%v", got, err)
	}
	// GitHubPRSource resets this local terminal value to pending unless an exact
	// A coordinator-authored canonical comment confirms it; covered above.
}

func TestFileRecoveryRejectsSymlinkState(t *testing.T) {
	dir := t.TempDir()
	target, link := filepath.Join(dir, "state.json"), filepath.Join(dir, "state-link.json")
	if err := os.WriteFile(target, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := (&FileRecovery{Path: link}).read(); err == nil {
		t.Fatal("symlink recovery state accepted")
	}
	if _, err := (&FileRecovery{Path: target}).read(); err != nil {
		t.Fatalf("regular recovery state rejected: %v", err)
	}
}

func TestFileRecoveryRejectsSymlinkLocks(t *testing.T) {
	dir := t.TempDir()
	path, target := filepath.Join(dir, "state.json"), filepath.Join(dir, "target")
	if err := os.WriteFile(path, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(target, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{".lock", ".governance.lock"} {
		if err := os.Symlink(target, path+suffix); err != nil {
			t.Fatal(err)
		}
	}
	if err := (&FileRecovery{Path: path}).update(func([]PRState) error { return nil }); err == nil {
		t.Fatal("symlink recovery lock accepted")
	}
	if err := RunPRReconciliation(context.Background(), API{}, PRAdapterConfig{}, path); err == nil {
		t.Fatal("symlink governance lock accepted")
	}
}

func TestFileRecoveryDoesNotFollowSubstitutedSymlink(t *testing.T) {
	dir := t.TempDir()
	path, parked, malicious := filepath.Join(dir, "state.json"), filepath.Join(dir, "parked.json"), filepath.Join(dir, "malicious.json")
	if err := os.WriteFile(path, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(malicious, []byte(`[{"repository":"followed"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := openRegular(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := os.Rename(path, parked); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(malicious, path); err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(f)
	if err != nil || string(b) != "[]\n" {
		t.Fatalf("open descriptor followed substituted path: %q err=%v", b, err)
	}
	if states, err := (&FileRecovery{Path: path}).read(); err == nil || len(states) != 0 {
		t.Fatalf("substituted symlink followed: states=%#v err=%v", states, err)
	}
}

func TestReviewDecisionPassBlockUnknown(t *testing.T) {
	for _, test := range []struct{ name, response, want string }{
		{"pass", `{"data":{"repository":{"pullRequest":{"reviewDecision":"APPROVED"}}}}`, "APPROVED"},
		{"block", `{"data":{"repository":{"pullRequest":{"reviewDecision":"CHANGES_REQUESTED"}}}}`, "CHANGES_REQUESTED"},
		{"unknown", `{"data":{"repository":{"pullRequest":{"reviewDecision":null}}}}`, ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := GitHubPRSource{API: API{BaseURL: "https://example.test", HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				return httpResponse(http.StatusOK, test.response, nil), nil
			})}}, Config: PRAdapterConfig{Repository: "o/r"}}
			got, err := source.readReviewDecision(context.Background(), 3)
			if err != nil || got != test.want {
				t.Fatalf("decision=%q err=%v", got, err)
			}
		})
	}
}

func TestGraphQLReviewDecisionAloneAuthorizesApprovals(t *testing.T) {
	for _, test := range []struct {
		name, decision string
		approved       bool
	}{
		{"raw approval with review required", "REVIEW_REQUIRED", false},
		{"raw approval with unknown decision", "SOMETHING_NEW", false},
		{"approved", "APPROVED", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
				"/repos/o/r/pulls/3/reviews?per_page=100&page=1": []any{map[string]any{"id": 1, "state": "APPROVED", "user": map[string]any{"id": 5}}},
				"/graphql": map[string]any{"data": map[string]any{"repository": map[string]any{"pullRequest": map[string]any{"reviewDecision": test.decision}}}},
			}), Config: PRAdapterConfig{Repository: "o/r"}}
			facts := PRFacts{ApprovalRequired: true, CodeOwnerApprovalRequired: true, LastPushApprovalRequired: true}
			if err := source.readReviews(context.Background(), 3, &facts); err != nil {
				t.Fatal(err)
			}
			if err := source.readApprovals(context.Background(), 3, &facts); err != nil || facts.Approved != test.approved || facts.CodeOwnerApproved != test.approved || facts.LastPushApproved != test.approved {
				t.Fatalf("facts=%#v err=%v", facts, err)
			}
		})
	}
}

func TestGraphQLReviewDecisionErrorFailsClosed(t *testing.T) {
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/graphql": fixtureHTTP{http.StatusInternalServerError, `{}`},
	}), Config: PRAdapterConfig{Repository: "o/r"}}
	facts := PRFacts{Approved: true, CodeOwnerApproved: true, LastPushApproved: true}
	if err := source.readApprovals(context.Background(), 3, &facts); err == nil || facts.Approved || facts.CodeOwnerApproved || facts.LastPushApproved {
		t.Fatalf("facts=%#v err=%v", facts, err)
	}
}

func TestLatestAttributedMergeDispositionWins(t *testing.T) {
	comments := []any{
		map[string]any{"id": 2, "body": "Merge attempt for head `abcdef0`: **resolved**.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}},
		map[string]any{"id": 1, "body": "Merge attempt for head `abcdef0`: **prepared**.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}},
	}
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{"/repos/o/r/issues/10/comments?per_page=100&page=1": comments}), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
	state := PRState{HeadSHA: "abcdef0"}
	if err := source.readIssueComments(context.Background(), 10, 2, &state); err != nil || state.MergeAttemptSHA != "" {
		t.Fatalf("state=%#v err=%v", state, err)
	}
	comments = append(comments, map[string]any{"id": 3, "body": "Merge attempt for head `abcdef0`: **prepared**.\n\n<!-- agent-symphony:issue:10:attempt:2 -->", "user": map[string]any{"id": 42}})
	source.API = fixtureAPI(t, map[string]any{"/repos/o/r/issues/10/comments?per_page=100&page=1": comments})
	if err := source.readIssueComments(context.Background(), 10, 2, &state); err != nil || state.MergeAttemptSHA != "abcdef0" {
		t.Fatalf("state=%#v err=%v", state, err)
	}
}

func TestFileRecoveryConcurrentGoroutinesExecuteOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	state := PRState{Repository: "o/r", Number: 3, Issue: 10, Attempt: 2, HeadSHA: "abc", Facts: PRFacts{Feedback: []Feedback{{ID: 55}}}}
	b, _ := json.Marshal([]PRState{state})
	if err := os.WriteFile(path, b, 0o600); err != nil {
		t.Fatal(err)
	}
	r := FileRecovery{Path: path}
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			claimed, err := r.ClaimFeedback(context.Background(), state, state.Facts.Feedback[0])
			if err != nil {
				t.Error(err)
			}
			results <- claimed
		}()
	}
	wg.Wait()
	close(results)
	claims := 0
	for claimed := range results {
		if claimed {
			claims++
		}
	}
	if claims != 1 {
		t.Fatalf("claims=%d", claims)
	}
}

func TestFeedbackSourcesPaginateDoNotCollideAndRefetchExactly(t *testing.T) {
	hundred := make([]map[string]any, 100)
	for i := range hundred {
		hundred[i] = map[string]any{"id": i + 10, "body": "conversation", "user": map[string]any{"id": 5}}
	}
	responses := map[string]any{
		"/repos/o/r/issues/3/comments?per_page=100&page=1": hundred,
		"/repos/o/r/issues/3/comments?per_page=100&page=2": []any{map[string]any{"id": 7, "body": "same", "user": map[string]any{"id": 5}}},
		"/repos/o/r/pulls/3/comments?per_page=100&page=1":  []any{map[string]any{"id": 7, "body": "same", "user": map[string]any{"id": 5}}},
		"/repos/o/r/pulls/3/reviews?per_page=100&page=1":   []any{map[string]any{"id": 7, "body": "same", "user": map[string]any{"id": 5}}},
		"/repos/o/r/issues/comments/7":                     map[string]any{"id": 7, "body": "same", "user": map[string]any{"id": 5}},
		"/repos/o/r/pulls/comments/7":                      map[string]any{"id": 7, "body": "same", "user": map[string]any{"id": 5}},
		"/repos/o/r/pulls/3/reviews/7":                     map[string]any{"id": 7, "body": "same", "user": map[string]any{"id": 5}},
		"/user/5":                                          map[string]any{"login": "owner"},
		"/repos/o/r/collaborators/owner/permission":        map[string]any{"permission": "write"},
	}
	source := GitHubPRSource{API: fixtureAPI(t, responses), Config: PRAdapterConfig{Repository: "o/r"}}
	records, err := source.readFeedback(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	var same []Feedback
	for _, record := range records {
		if record.ID == 7 {
			same = append(same, Feedback{ID: record.ID, Source: record.Source, ActorID: 5, Body: record.Body, Authorized: true})
		}
	}
	if got := ActionableFeedback(same, func(int) bool { return true }); len(got) != 3 {
		t.Fatalf("colliding feedback: %#v", got)
	}
	for _, feedback := range same {
		fresh, err := source.FreshFeedback(context.Background(), PRState{Repository: "o/r", Number: 3}, feedback)
		if err != nil || fresh.identity() != feedback.identity() || !fresh.Authorized {
			t.Fatalf("fresh=%#v err=%v", fresh, err)
		}
	}
}

type fixtureHTTP struct {
	status int
	body   string
}

func fixtureAPI(t *testing.T, responses map[string]any) API {
	t.Helper()
	return API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		value, ok := responses[req.URL.RequestURI()]
		if !ok {
			return nil, fmt.Errorf("unexpected fixture request %s", req.URL.RequestURI())
		}
		if response, ok := value.(fixtureHTTP); ok {
			return httpResponse(response.status, response.body, nil), nil
		}
		b, _ := json.Marshal(value)
		return httpResponse(http.StatusOK, string(b), nil), nil
	})}}
}

func TestRequiredStatusUsesLatestContext(t *testing.T) {
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/commits/abcdef0/check-runs?filter=all&per_page=100&page=1": map[string]any{"check_runs": []any{}},
		"/repos/o/r/commits/abcdef0/statuses?per_page=100&page=1":              []any{map[string]any{"context": "ci", "state": "failure"}, map[string]any{"context": "ci", "state": "success"}},
	}), Config: PRAdapterConfig{Repository: "o/r"}}
	state := PRState{}
	if err := source.readRequiredChecks(context.Background(), "abcdef0", []requiredCheck{{Context: "ci"}}, &state); err != nil || state.Facts.RequiredChecksPass {
		t.Fatalf("facts=%#v err=%v", state.Facts, err)
	}
}

func TestCheckRunsKeepNewestSameContextAndAppAcrossPages(t *testing.T) {
	page := make([]any, 100)
	for i := range page {
		page[i] = map[string]any{"id": i + 1, "name": fmt.Sprintf("noise-%d", i), "status": "completed", "conclusion": "success", "started_at": "2026-08-02T00:00:00Z", "app": map[string]any{"id": 1}}
	}
	page[0] = map[string]any{"id": 200, "name": "ci", "status": "completed", "conclusion": "failure", "completed_at": "2026-08-02T02:00:00Z", "app": map[string]any{"id": 8}}
	page[1] = map[string]any{"id": 300, "name": PolicyCheck, "status": "completed", "conclusion": "failure", "completed_at": "2026-08-02T03:00:00Z", "app": map[string]any{"id": 7}}
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/commits/abc/check-runs?filter=all&per_page=100&page=1": map[string]any{"check_runs": page},
		"/repos/o/r/commits/abc/check-runs?filter=all&per_page=100&page=2": map[string]any{"check_runs": []any{
			map[string]any{"id": 100, "name": "ci", "status": "completed", "conclusion": "success", "completed_at": "2026-08-02T01:00:00Z", "app": map[string]any{"id": 8}},
			map[string]any{"id": 250, "name": PolicyCheck, "status": "completed", "conclusion": "success", "completed_at": "2026-08-02T01:00:00Z", "app": map[string]any{"id": 7}},
		}},
		"/repos/o/r/commits/abc/statuses?per_page=100&page=1": []any{},
	}), Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
	state := PRState{}
	if err := source.readRequiredChecks(context.Background(), "abc", []requiredCheck{{Context: "ci", AppID: 8}}, &state); err != nil || state.Facts.RequiredChecksPass || state.CheckHead != "" {
		t.Fatalf("state=%#v err=%v", state, err)
	}
}

func TestCheckRunsUseImmutableIDAndFailClosedOnDuplicate(t *testing.T) {
	for _, checks := range [][]any{
		{
			map[string]any{"id": 10, "name": "ci", "status": "completed", "conclusion": "success", "completed_at": "2026-08-02T02:00:00Z", "app": map[string]any{"id": 8}},
			map[string]any{"id": 11, "name": "ci", "status": "queued", "app": map[string]any{"id": 8}},
		},
		{
			map[string]any{"id": 11, "name": "ci", "status": "completed", "conclusion": "success", "app": map[string]any{"id": 8}},
			map[string]any{"id": 11, "name": "ci", "status": "queued", "app": map[string]any{"id": 8}},
		},
	} {
		source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
			"/repos/o/r/commits/abc/check-runs?filter=all&per_page=100&page=1": map[string]any{"check_runs": checks},
			"/repos/o/r/commits/abc/statuses?per_page=100&page=1":              []any{},
		}), Config: PRAdapterConfig{Repository: "o/r"}}
		state := PRState{}
		if err := source.readRequiredChecks(context.Background(), "abc", []requiredCheck{{Context: "ci", AppID: 8}}, &state); err != nil || state.Facts.RequiredChecksPass {
			t.Fatalf("unsafe check result: %#v err=%v", state.Facts, err)
		}
	}
}

func TestLatestBodyEditUsesGraphQLUserContentEditShape(t *testing.T) {
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		var payload struct{ Query string }
		if err := json.NewDecoder(req.Body).Decode(&payload); err != nil {
			t.Fatal(err)
		}
		for _, fragment := range []string{"... on Bot{databaseId}", "... on Mannequin{databaseId}", "... on Organization{databaseId}", "... on User{databaseId}"} {
			if !strings.Contains(payload.Query, fragment) {
				t.Fatalf("query lacks %s: %s", fragment, payload.Query)
			}
		}
		if strings.Contains(payload.Query, "... on EnterpriseUserAccount{databaseId}") {
			t.Fatalf("query requests unsupported enterprise databaseId: %s", payload.Query)
		}
		return httpResponse(http.StatusOK, `{"data":{"repository":{"issue":{"userContentEdits":{"nodes":[{"id":"UCE_1","editedAt":"2026-08-02T01:00:00Z","editor":{"__typename":"User","databaseId":5}}]}}}}}`, nil), nil
	})}}
	source := GitHubPRSource{API: api, Config: PRAdapterConfig{Repository: "o/r"}}
	edit, err := source.latestBodyEdit(context.Background(), 10)
	if err != nil || edit == nil || edit.ID != "UCE_1" || edit.Editor.DatabaseID == nil || *edit.Editor.DatabaseID != 5 {
		t.Fatalf("edit=%#v err=%v", edit, err)
	}
}

func TestLatestBodyEditRejectsGraphQLErrorsAndUnsafeEditors(t *testing.T) {
	for name, response := range map[string]any{
		"graphql error": map[string]any{"errors": []any{map[string]any{"message": "bad query"}}},
		"missing id":    map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{map[string]any{"id": "UCE_1", "editedAt": "2026-08-02T01:00:00Z", "editor": map[string]any{"__typename": "User"}}}}}}}},
		"zero id":       map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{map[string]any{"id": "UCE_1", "editedAt": "2026-08-02T01:00:00Z", "editor": map[string]any{"__typename": "User", "databaseId": 0}}}}}}}},
		"unsupported":   map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{map[string]any{"id": "UCE_1", "editedAt": "2026-08-02T01:00:00Z", "editor": map[string]any{"__typename": "EnterpriseUserAccount"}}}}}}}},
	} {
		t.Run(name, func(t *testing.T) {
			source := GitHubPRSource{API: fixtureAPI(t, map[string]any{"/graphql": response}), Config: PRAdapterConfig{Repository: "o/r"}}
			if _, err := source.latestBodyEdit(context.Background(), 10); err == nil {
				t.Fatal("unsafe editor accepted")
			}
		})
	}
}

func TestLatestControlCommandWinsAcrossCancelAndRetry(t *testing.T) {
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	comments := []issueCommentRecord{
		{ID: 9, Body: "/retry", CreatedAt: now, UpdatedAt: now},
		{ID: 10, Body: "/cancel", CreatedAt: now, UpdatedAt: now},
		{ID: 11, Body: "/retry edited", CreatedAt: now.Add(time.Minute), UpdatedAt: now.Add(2 * time.Minute)},
	}
	comment, name := latestControlCommand(comments, "/cancel", "/retry")
	if comment == nil || comment.ID != 10 || name != "cancelled" {
		t.Fatalf("winner=%#v %q", comment, name)
	}
}

func TestControlCommandProvenanceTracksWinningBoundaryForBothValues(t *testing.T) {
	now := time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC)
	for _, test := range []struct {
		name, winner string
		cancelled    bool
	}{
		{"cancel then retry", "retry", false},
		{"retry then cancel", "cancelled", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			provenance := []Provenance{{Name: "cancelled"}, {Name: "retry"}}
			command := issueCommentRecord{ID: 12, CreatedAt: now}
			command.User.ID = 5
			applyControlCommandProvenance(provenance, command, test.winner)
			if provenance[0].Value != strconv.FormatBool(test.cancelled) || provenance[1].Value != strconv.FormatBool(!test.cancelled) {
				t.Fatalf("values=%#v", provenance)
			}
			for _, p := range provenance {
				if p.Source != "comment" || p.EventID != 12 || p.ActorID != 5 || !p.CreatedAt.Equal(now) {
					t.Fatalf("provenance=%#v", provenance)
				}
			}
			controls := Controls{Completion: "human-review", Cancelled: test.cancelled, Retry: !test.cancelled}
			full := []Provenance{
				{Name: "ready", Value: "false", Source: "creation", ActorID: 9, CreatedAt: now},
				{Name: "priority", Value: "0", Source: "creation", ActorID: 9, CreatedAt: now},
				{Name: "completion", Value: "human-review", Source: "creation", ActorID: 9, CreatedAt: now},
				{Name: "closed", Value: "false", Source: "creation", ActorID: 9, CreatedAt: now},
			}
			full = append(full, provenance...)
			anchor := Anchor{IssueNodeID: "I_10", CreatedAt: now, ChangedAt: now, AuthorID: 9}
			approval := Approval{CommentID: 20, ActorID: 5, Body: "/approve", CreatedAt: now.Add(time.Second)}
			if _, err := NewSnapshot(controls, "body", anchor, approval, full, "/approve", func(id int) bool { return id == 5 }, timelineFor(full)); err != nil {
				t.Fatalf("snapshot rejected boundary provenance: %v", err)
			}
		})
	}
}

func TestRawReviewsNeverAuthorizeApproval(t *testing.T) {
	source := GitHubPRSource{API: fixtureAPI(t, map[string]any{
		"/repos/o/r/pulls/3/reviews?per_page=100&page=1": []any{
			map[string]any{"id": 1, "state": "APPROVED", "commit_id": "abc", "submitted_at": "2026-08-02T00:00:00Z", "user": map[string]any{"id": 5}},
			map[string]any{"id": 2, "state": "COMMENTED", "commit_id": "abc", "submitted_at": "2026-08-02T01:00:00Z", "user": map[string]any{"id": 5}},
		},
	}), Config: PRAdapterConfig{Repository: "o/r"}}
	facts := PRFacts{}
	if err := source.readReviews(context.Background(), 3, &facts); err != nil || facts.Approved {
		t.Fatalf("facts=%#v err=%v", facts, err)
	}
}
