package github

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestDirectStatusRequiresTheSmallVocabularyAndReason(t *testing.T) {
	for _, test := range []struct {
		body            string
		wantOK, wantSet bool
		wantReason      string
	}{
		{"/agent-symphony status needs-attention: failing browser validation", true, true, "failing browser validation"},
		{"/agent-symphony status clear: browser validation now passes", true, false, "browser validation now passes"},
		{"/agent-symphony status needs-attention:", false, false, ""},
		{"/agent-symphony status clear", false, false, ""},
		{"/agent-symphony status blocked: reason", false, false, ""},
		{"/agent-symphony status needs-attention: \t" + strings.Repeat("x", 1024) + " \n", true, true, strings.Repeat("x", 1024)},
		{"/agent-symphony status needs-attention: " + strings.Repeat("x", 1025), false, false, ""},
	} {
		got, ok := parseDirectStatus(test.body)
		if ok != test.wantOK || got.NeedsAttention != test.wantSet || got.Reason != test.wantReason {
			t.Fatalf("parseDirectStatus(%q) = %#v, %v", test.body, got, ok)
		}
	}
}

func TestDirectStatusUsesNewestAuthenticatedIssueOrPullRequestComment(t *testing.T) {
	now := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	labelPresent := true
	issueComments := []map[string]any{{"id": 1, "body": "/agent-symphony status needs-attention: implementation needs an operator decision", "created_at": now, "updated_at": now, "user": map[string]any{"id": 42}}}
	pullComments := []map[string]any{
		{"id": 2, "body": "/agent-symphony status clear: foreign clear", "created_at": now.Add(time.Minute), "updated_at": now.Add(time.Minute), "user": map[string]any{"id": 9}},
		{"id": 3, "body": "/agent-symphony status clear: edited clear", "created_at": now.Add(2 * time.Minute), "updated_at": now.Add(3 * time.Minute), "user": map[string]any{"id": 42}},
	}
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var response any
		switch r.URL.RequestURI() {
		case "/repos/o/r/issues/10":
			labels := []any{}
			if labelPresent {
				labels = append(labels, map[string]any{"name": strings.ToUpper(NeedsAttentionLabel)})
			}
			response = map[string]any{"labels": labels}
		case "/repos/o/r/issues/10/comments?per_page=100&page=1":
			response = issueComments
		case "/repos/o/r/issues/11/comments?per_page=100&page=1":
			response = pullComments
		default:
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		body, _ := json.Marshal(response)
		return httpResponse(http.StatusOK, string(body), nil), nil
	})}}
	source := GitHubPRSource{API: api, Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}
	status, err := source.directStatus(t.Context(), 10, 11)
	if err != nil || !status.NeedsAttention || status.Reason != "implementation needs an operator decision" {
		t.Fatalf("set status = %#v, %v", status, err)
	}
	labelPresent = false
	status, err = source.directStatus(t.Context(), 10, 11)
	if err != nil || !status.NeedsAttention || status.Reason != "direct needs-attention status is incomplete: needs-attention label is missing" {
		t.Fatalf("incomplete set status = %#v, %v", status, err)
	}
	labelPresent = true
	pullComments = append(pullComments, map[string]any{"id": 4, "body": "/agent-symphony status clear: review verified the correction", "created_at": now.Add(4 * time.Minute), "updated_at": now.Add(4 * time.Minute), "user": map[string]any{"id": 42}})
	status, err = source.directStatus(t.Context(), 10, 11)
	if err != nil || !status.NeedsAttention || status.Reason != "direct status clear is incomplete: needs-attention label remains" {
		t.Fatalf("incomplete clear status = %#v, %v", status, err)
	}
	labelPresent = false
	status, err = source.directStatus(t.Context(), 10, 11)
	if err != nil || status.NeedsAttention || status.Reason != "review verified the correction" {
		t.Fatalf("clear status = %#v, %v", status, err)
	}
	for i, body := range []string{"/agent-symphony status needs-attention:", "/agent-symphony status blocked: unsupported vocabulary"} {
		createdAt := now.Add(time.Duration(5+i) * time.Minute)
		pullComments = append(pullComments, map[string]any{"id": 5 + i, "body": body, "created_at": createdAt, "updated_at": createdAt, "user": map[string]any{"id": 42}})
		status, err = source.directStatus(t.Context(), 10, 11)
		if err != nil || !status.NeedsAttention || status.Reason != "direct status intent is incomplete: use needs-attention or clear with a nonempty reason" {
			t.Fatalf("malformed status = %#v, %v", status, err)
		}
	}
}

func TestDirectStatusAuthenticationFailureIsNotSuccess(t *testing.T) {
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return httpResponse(http.StatusUnauthorized, `{"message":"Requires authentication"}`, nil), nil
	})}}
	status, err := (&GitHubPRSource{API: api, Config: PRAdapterConfig{Repository: "o/r", ActorID: 42}}).directStatus(t.Context(), 10, 0)
	if err == nil || status.commentID != 0 || !strings.Contains(err.Error(), "GitHub read") {
		t.Fatalf("status=%#v err=%v", status, err)
	}
}

func TestTerminalFailureRequiresStrictCoordinatorAuthorship(t *testing.T) {
	marker, _ := TerminalFailureMarker(4, 2, time.Unix(10, 0))
	api := fixtureAPI(t, map[string]any{"/repos/o/r/issues/4/comments?per_page=100&page=1": []any{
		map[string]any{"body": marker, "user": map[string]any{"id": 42}},
		map[string]any{"body": marker + "tamper", "user": map[string]any{"id": 42}},
		map[string]any{"body": marker, "user": map[string]any{"id": 42}},
	}})
	got, conflicts, err := fetchTerminalFailures(context.Background(), api, PRAdapterConfig{Repository: "o/r", ActorID: 42}, 4)
	if err != nil || !conflicts.Any || len(got) != 1 || got[0].Attempt != 2 {
		t.Fatalf("terminal=%v conflicts=%v err=%v", got, conflicts, err)
	}
}

func TestTerminalFailureRejectsDuplicatePrefix(t *testing.T) {
	marker, _ := TerminalFailureMarker(4, 2, time.Unix(10, 0))
	if _, err := parseTerminalMarker(marker + "\n" + marker); err == nil {
		t.Fatal("duplicate terminal prefix was accepted")
	}
}

func TestEnsureActiveAttemptIsStrictCoordinatorAuthoredAndIdempotent(t *testing.T) {
	marker, err := ActiveAttemptMarker("o/r", 4, 2, "abcdef0")
	if err != nil {
		t.Fatal(err)
	}
	var comments []map[string]any
	posts := 0
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodGet:
			body, _ := json.Marshal(comments)
			return httpResponse(http.StatusOK, string(body), nil), nil
		case http.MethodPost:
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil || !strings.Contains(payload.Body, marker) || !strings.Contains(payload.Body, "Worktree: `/worktree`") {
				t.Fatal("dispatch did not persist the active marker and launch identity")
			}
			comments = append(comments, map[string]any{"body": payload.Body, "user": map[string]any{"id": 42}})
			posts++
			if posts == 1 {
				return nil, io.ErrUnexpectedEOF // GitHub accepted it; the response was lost.
			}
			return httpResponse(http.StatusCreated, `{}`, nil), nil
		default:
			t.Fatalf("unexpected request %s", r.Method)
		}
		return nil, nil
	})}}
	cfg := PRAdapterConfig{Repository: "o/r", ActorID: 42}
	for range 2 {
		if err := EnsureActiveAttempt(context.Background(), api, cfg, 4, 2, "abcdef0", "Implementation session reserved.\n\n- Branch: `branch`\n- Worktree: `/worktree`\n- Session: `session`"); err != nil {
			t.Fatal(err)
		}
	}
	if posts != 1 {
		t.Fatalf("marker posts=%d, want 1", posts)
	}
	comments = append(comments, map[string]any{"body": marker, "user": map[string]any{"id": 9}})
	if err := EnsureActiveAttempt(context.Background(), api, cfg, 4, 2, "abcdef0", "Implementation session reserved."); err == nil || posts != 1 {
		t.Fatalf("foreign marker err=%v posts=%d", err, posts)
	}
}

func TestEnsureActiveAttemptRequiresBoundedPlainDetail(t *testing.T) {
	for _, detail := range []string{"", strings.Repeat("x", 4097), "<!-- agent-symphony:forged -->"} {
		if err := EnsureActiveAttempt(t.Context(), API{}, PRAdapterConfig{Repository: "o/r"}, 4, 2, "abcdef0", detail); err == nil {
			t.Fatalf("accepted detail %q", detail)
		}
	}
}

func TestEnsureRetryCommandIsAuthorizedExactAndIdempotent(t *testing.T) {
	failedAt := time.Unix(10, 0).UTC()
	active, _ := ActiveAttemptMarker("o/r", 4, 2, "abcdef0")
	terminal, _ := TerminalFailureMarker(4, 2, failedAt)
	comments := []map[string]any{
		{"id": 1, "body": active, "created_at": failedAt.Add(-time.Minute), "updated_at": failedAt.Add(-time.Minute), "user": map[string]any{"id": 42}},
		{"id": 2, "body": terminal, "created_at": failedAt, "updated_at": failedAt, "user": map[string]any{"id": 42}},
	}
	posts := 0
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var response any
		switch r.Method + " " + r.URL.RequestURI() {
		case "GET /repos/o/r/issues/4/comments?per_page=100&page=1":
			response = comments
		case "GET /user/42":
			response = map[string]any{"login": "coordinator"}
		case "GET /repos/o/r/collaborators/coordinator/permission":
			response = map[string]any{"permission": "maintain"}
		case "POST /repos/o/r/issues/4/comments":
			if r.Header.Get("X-Agent-Symphony-Issue") != "4" || r.Header.Get("X-Agent-Symphony-Attempt") != "2" {
				t.Fatalf("missing retry attribution: %v", r.Header)
			}
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil || payload.Body != "/retry" {
				t.Fatalf("retry body=%q", payload.Body)
			}
			posts++
			createdAt := failedAt.Add(time.Minute)
			comments = append(comments, map[string]any{"id": 3, "body": payload.Body, "created_at": createdAt, "updated_at": createdAt, "user": map[string]any{"id": 42}})
			return nil, io.ErrUnexpectedEOF // Accepted; response lost.
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		body, _ := json.Marshal(response)
		return httpResponse(http.StatusOK, string(body), nil), nil
	})}}
	cfg := PRAdapterConfig{Repository: "o/r", ActorID: 42, CancelCommand: "/cancel", RetryCommand: "/retry"}
	for range 2 {
		if err := EnsureRetryCommand(t.Context(), api, cfg, 4, 2); err != nil {
			t.Fatal(err)
		}
	}
	if posts != 1 {
		t.Fatalf("retry posts=%d", posts)
	}
}

func TestEnsureRetryCommandRejectsNewerOrUnterminalizedBinding(t *testing.T) {
	failedAt := time.Unix(10, 0).UTC()
	active, _ := ActiveAttemptMarker("o/r", 4, 2, "abcdef0")
	terminal, _ := TerminalFailureMarker(4, 2, failedAt)
	for _, extraAttempt := range []int{1, 3} {
		extra, _ := ActiveAttemptMarker("o/r", 4, extraAttempt, "abcdef0")
		api := fixtureAPI(t, map[string]any{"/repos/o/r/issues/4/comments?per_page=100&page=1": []any{
			map[string]any{"body": active, "user": map[string]any{"id": 42}},
			map[string]any{"body": terminal, "user": map[string]any{"id": 42}},
			map[string]any{"body": extra, "user": map[string]any{"id": 42}},
		}})
		cfg := PRAdapterConfig{Repository: "o/r", ActorID: 42, RetryCommand: "/retry"}
		if err := EnsureRetryCommand(t.Context(), api, cfg, 4, 2); err == nil {
			t.Fatalf("attempt %d binding did not block retry", extraAttempt)
		}
	}
}

func TestActiveAttemptMarkerRejectsHostileAndContradictoryComments(t *testing.T) {
	first, _ := ActiveAttemptMarker("o/r", 4, 2, "abcdef0")
	second, _ := ActiveAttemptMarker("o/r", 4, 2, "abcdef1")
	for _, test := range []struct {
		name     string
		comments []any
	}{
		{"malformed", []any{map[string]any{"body": activeMarkerPrefix + `{}`, "user": map[string]any{"id": 42}}}},
		{"foreign", []any{map[string]any{"body": first, "user": map[string]any{"id": 9}}}},
		{"contradictory", []any{
			map[string]any{"body": first, "user": map[string]any{"id": 42}},
			map[string]any{"body": second, "user": map[string]any{"id": 42}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := fixtureAPI(t, map[string]any{"/repos/o/r/issues/4/comments?per_page=100&page=1": test.comments})
			_, conflicts, err := fetchActiveAttempts(context.Background(), api, PRAdapterConfig{Repository: "o/r", ActorID: 42}, 4)
			if err != nil || !conflicts.Any {
				t.Fatalf("conflicts=%v err=%v", conflicts, err)
			}
		})
	}
}

func TestRetryMustFollowEveryTerminalFailure(t *testing.T) {
	retry := &Provenance{Name: "retry", Value: "true", CreatedAt: time.Unix(20, 0)}
	controls := Controls{Retry: true}
	if retryAuthorizesFailure(controls, retry, terminalMarkerPayload{Attempt: 1, FailedAt: time.Unix(10, 0)}) != true {
		t.Fatal("later retry did not authorize attempt 2")
	}
	if retryAuthorizesFailure(controls, retry, terminalMarkerPayload{Attempt: 2, FailedAt: time.Unix(30, 0)}) != false {
		t.Fatal("timeless retry authorized attempt 3")
	}
	retry.CreatedAt = time.Unix(40, 0)
	if !retryAuthorizesFailure(controls, retry, terminalMarkerPayload{Attempt: 2, FailedAt: time.Unix(30, 0)}) {
		t.Fatal("fresh retry did not authorize attempt 3")
	}
}

func TestFetchIssueFactsCreatesSnapshotThenRereadsEligible(t *testing.T) {
	now := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	body := "##\n## Context\nreason and evidence\n## Acceptance criteria\n- result\n## Checklist\n- [ ] implement\n## Validation\ngo test ./...\n## Dependencies\nNone.\n## ###\n"
	var snapshotBodies []string
	changed, needsAttentionLabel, mentionedPR := false, false, false
	var pullComments []any
	posts := 0
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		comments := []any{map[string]any{"id": 50, "body": "/approve", "created_at": now.Add(time.Minute), "updated_at": now.Add(time.Minute), "user": map[string]any{"id": 5}}}
		for i, snapshotBody := range snapshotBodies {
			createdAt := now.Add(time.Duration(2+i*2) * time.Minute)
			comments = append(comments, map[string]any{"id": 60 + i*20, "body": snapshotBody, "created_at": createdAt, "updated_at": createdAt, "user": map[string]any{"id": 42}})
		}
		var response any
		switch r.Method + " " + r.URL.RequestURI() {
		case "GET /repos/o/r":
			response = map[string]any{"default_branch": "main"}
		case "GET /repos/o/r/branches/main":
			response = map[string]any{"commit": map[string]any{"sha": "abcdef0"}}
		case "GET /repos/o/r/issues?state=open&per_page=100&page=1":
			response = []any{map[string]any{"number": 10, "title": "approved", "body": body, "created_at": now}}
		case "GET /repos/o/r/issues/10":
			priority := "P1"
			if changed {
				priority = "P2"
			}
			labels := []any{map[string]any{"name": "ready"}, map[string]any{"name": priority}}
			if needsAttentionLabel {
				labels = append(labels, map[string]any{"name": strings.ToUpper(NeedsAttentionLabel)})
			}
			response = map[string]any{"number": 10, "node_id": "I_10", "state": "open", "body": body, "created_at": now, "user": map[string]any{"id": 9}, "labels": labels}
		case "GET /repos/o/r/issues/10/timeline?per_page=100&page=1":
			events := []any{
				map[string]any{"id": 20, "event": "labeled", "label": map[string]any{"name": "ready"}, "created_at": now.Add(time.Second), "actor": map[string]any{"id": 5}},
				map[string]any{"id": 21, "event": "labeled", "label": map[string]any{"name": "P1"}, "created_at": now.Add(2 * time.Second), "actor": map[string]any{"id": 5}},
			}
			if changed {
				events = append(events,
					map[string]any{"id": 22, "event": "unlabeled", "label": map[string]any{"name": "P1"}, "created_at": now.Add(3 * time.Minute), "actor": map[string]any{"id": 5}},
					map[string]any{"id": 23, "event": "labeled", "label": map[string]any{"name": "P2"}, "created_at": now.Add(3 * time.Minute), "actor": map[string]any{"id": 5}},
				)
			}
			if mentionedPR {
				events = append(events, map[string]any{"event": "cross-referenced", "source": map[string]any{"issue": map[string]any{"number": 11, "state": "open", "repository_url": "https://api.example.test/repos/o/r", "pull_request": map[string]any{"url": "https://api.example.test/repos/o/r/pulls/11"}}}})
			}
			response = events
		case "GET /repos/o/r/issues/10/comments?per_page=100&page=1":
			response = comments
		case "GET /repos/o/r/issues/11/comments?per_page=100&page=1":
			response = pullComments
		case "POST /graphql":
			response = map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}}}}
		case "GET /repos/o/r/issues/comments/50":
			response = comments[0]
		case "GET /user/5":
			response = map[string]any{"login": "owner"}
		case "GET /repos/o/r/collaborators/owner/permission":
			response = map[string]any{"permission": "maintain"}
		case "POST /repos/o/r/issues/10/comments":
			if r.Header.Get("X-Agent-Symphony-Issue") != "10" || r.Header.Get("X-Agent-Symphony-Attempt") != "0" {
				t.Fatalf("missing pre-attempt attribution headers: %v", r.Header)
			}
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil {
				t.Fatal("invalid snapshot request")
			}
			if _, err := ParseSnapshotComment(payload.Body, 42, 42); err != nil {
				t.Fatalf("non-canonical snapshot: %v", err)
			}
			snapshotBodies, posts = append(snapshotBodies, payload.Body), posts+1
			if posts == 1 {
				return nil, io.ErrUnexpectedEOF // GitHub accepted it, but the response was lost.
			}
			return httpResponse(http.StatusCreated, `{}`, nil), nil
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		encoded, _ := json.Marshal(response)
		return httpResponse(http.StatusOK, string(encoded), nil), nil
	})}}
	cfg := productionPRConfig()
	readOnly, err := FetchIssueFacts(context.Background(), api, cfg, nil, false)
	if err != nil || len(readOnly) != 1 || readOnly[0].Eligible || posts != 0 {
		t.Fatalf("read-only facts=%#v posts=%d err=%v", readOnly, posts, err)
	}
	for range 2 {
		facts, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
		if err != nil || len(facts) != 1 || !facts[0].Eligible || facts[0].Priority != 1 || len(facts[0].Blockers) != 0 {
			t.Fatalf("facts=%#v err=%v", facts, err)
		}
	}
	if posts != 1 {
		t.Fatalf("snapshot posts=%d, want 1", posts)
	}
	changed = true
	changedFacts, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(changedFacts) != 1 || !changedFacts[0].Eligible || changedFacts[0].Priority != 2 || posts != 2 || len(changedFacts[0].Blockers) != 0 {
		t.Fatalf("changed facts=%#v posts=%d err=%v", changedFacts, posts, err)
	}
	readOnly, err = FetchIssueFacts(context.Background(), api, cfg, nil, false)
	if err != nil || len(readOnly) != 1 || !readOnly[0].Eligible || readOnly[0].Priority != 2 || posts != 2 {
		t.Fatalf("changed read-only facts=%#v posts=%d err=%v", readOnly, posts, err)
	}
	for range 2 {
		facts, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
		if err != nil || len(facts) != 1 || !facts[0].Eligible || facts[0].Priority != 2 || len(facts[0].Blockers) != 0 {
			t.Fatalf("changed facts=%#v err=%v", facts, err)
		}
	}
	if posts != 2 {
		t.Fatalf("replacement snapshot posts=%d, want 2 total", posts)
	}
	marker, _ := ActiveAttemptMarker("o/r", 10, 2, "abcdef0")
	snapshotBodies = append(snapshotBodies, marker)
	bound, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(bound) != 1 || bound[0].Eligible || !bound[0].Active || !bound[0].DispatchAuthorized || bound[0].Attempt != 2 || bound[0].ActiveAttempt == nil || bound[0].ActiveAttempt.BaseSHA != "abcdef0" {
		t.Fatalf("bound facts=%#v err=%v", bound, err)
	}
	targeted, err := FetchOperatorIssueFacts(context.Background(), api, cfg, nil, 10)
	if err != nil || len(targeted) != 1 || !targeted[0].Active || !targeted[0].DispatchAuthorized || targeted[0].Attempt != 2 {
		t.Fatalf("targeted facts=%#v err=%v", targeted, err)
	}
	snapshotBodies = append(snapshotBodies, "/agent-symphony status needs-attention: implementation needs an operator decision")
	needsAttentionLabel = true
	attention, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(attention) != 1 || !attention[0].NeedsAttention || attention[0].Eligible || !attention[0].DispatchAuthorized || attention[0].RecoveryAuthorized || !slices.Contains(attention[0].Blockers, "needs attention: implementation needs an operator decision") {
		t.Fatalf("attention facts=%#v err=%v", attention, err)
	}
	snapshotBodies = append(snapshotBodies, "/agent-symphony status clear: operator decision recorded")
	needsAttentionLabel = false
	cleared, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(cleared) != 1 || cleared[0].NeedsAttention || !cleared[0].DispatchAuthorized || len(cleared[0].Blockers) != 0 {
		t.Fatalf("cleared facts=%#v err=%v", cleared, err)
	}
	mentionedPR = true
	reviewSetAt := now.Add(time.Hour)
	pullComments = append(pullComments, map[string]any{"id": 500, "body": "/agent-symphony status needs-attention: review found a blocking regression", "created_at": reviewSetAt, "updated_at": reviewSetAt, "user": map[string]any{"id": 42}})
	foreignAttempt := []RecoveryAttemptFact{{Repository: "o/r", Issue: 9, Attempt: 1, PR: 11, State: "active"}}
	unrelated, err := FetchIssueFacts(context.Background(), api, cfg, foreignAttempt, true)
	if err != nil || len(unrelated) != 1 || unrelated[0].NeedsAttention || !unrelated[0].DispatchAuthorized || len(unrelated[0].Blockers) != 0 {
		t.Fatalf("merely mentioned issue facts=%#v err=%v", unrelated, err)
	}
	needsAttentionLabel = true
	authoritativeAttempt := []RecoveryAttemptFact{{Repository: "o/r", Issue: 10, Attempt: 2, PR: 11, State: "active"}}
	reviewAttention, err := FetchIssueFacts(context.Background(), api, cfg, authoritativeAttempt, true)
	if err != nil || len(reviewAttention) != 1 || reviewAttention[0].Attempt != 2 || reviewAttention[0].CurrentAttempt != 2 || !reviewAttention[0].NeedsAttention || reviewAttention[0].Eligible || !reviewAttention[0].DispatchAuthorized || reviewAttention[0].RecoveryAuthorized || !slices.Contains(reviewAttention[0].Blockers, "needs attention: review found a blocking regression") {
		t.Fatalf("review attention facts=%#v err=%v", reviewAttention, err)
	}
	needsAttentionLabel = false
	reviewClearAt := reviewSetAt.Add(time.Minute)
	pullComments = append(pullComments, map[string]any{"id": 501, "body": "/agent-symphony status clear: review verified the correction", "created_at": reviewClearAt, "updated_at": reviewClearAt, "user": map[string]any{"id": 42}})
	reviewCleared, err := FetchIssueFacts(context.Background(), api, cfg, authoritativeAttempt, true)
	if err != nil || len(reviewCleared) != 1 || reviewCleared[0].Attempt != 2 || reviewCleared[0].CurrentAttempt != 2 || reviewCleared[0].NeedsAttention || len(reviewCleared[0].Blockers) != 0 {
		t.Fatalf("review clear facts=%#v err=%v", reviewCleared, err)
	}
	terminal, _ := TerminalFailureMarker(10, 2, now.Add(20*time.Minute))
	snapshotBodies = append(snapshotBodies, "Attempt failed closed: worker produced no repository changes\n\n"+terminal)
	failed, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(failed) != 1 || failed[0].Active || failed[0].ActiveAttempt != nil || len(failed[0].TerminalAttempts) != 1 || failed[0].TerminalAttempts[0].Attempt != 2 || failed[0].TerminalAttempts[0].BaseSHA != "abcdef0" || failed[0].TerminalAttempts[0].Diagnostic != "worker produced no repository changes" || failed[0].RecoveryAttempt != 2 || !failed[0].RecoveryAuthorized || failed[0].Attempt != 3 || failed[0].CurrentAttempt != 2 || len(failed[0].Blockers) == 0 {
		t.Fatalf("terminal transition facts=%#v err=%v", failed, err)
	}
	third, _ := ActiveAttemptMarker("o/r", 10, 3, "abcdef0")
	thirdTerminal, _ := TerminalFailureMarker(10, 3, now.Add(30*time.Minute))
	snapshotBodies = append(snapshotBodies, third, "Attempt failed closed: newer failure\n\n"+thirdTerminal)
	history, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(history) != 1 || len(history[0].TerminalAttempts) != 2 || history[0].TerminalAttempts[0].Attempt != 2 || history[0].TerminalAttempts[1].Attempt != 3 || history[0].RecoveryAttempt != 3 || !history[0].RecoveryAuthorized || history[0].Attempt != 4 || history[0].CurrentAttempt != 3 {
		t.Fatalf("terminal history=%#v err=%v", history, err)
	}

	legacy, err := ParseSnapshotComment(snapshotBodies[1], 42, 42)
	if err != nil {
		t.Fatal(err)
	}
	body = "unstructured pre-upgrade issue body"
	legacy.BodyHash = bodyHash(body, legacy.Anchor)
	snapshotBodies = []string{SnapshotComment(legacy)}
	incomplete, err := FetchIssueFacts(context.Background(), api, cfg, nil, false)
	if err != nil || len(incomplete) != 1 || incomplete[0].Eligible || incomplete[0].DispatchAuthorized || len(incomplete[0].Blockers) != 1 || !strings.Contains(incomplete[0].Blockers[0], "issue contract is incomplete") {
		t.Fatalf("pre-upgrade incomplete snapshot facts=%#v err=%v", incomplete, err)
	}
}

func TestContradictoryTerminalMarkersDoNotProjectFailedAttempt(t *testing.T) {
	first, _ := TerminalFailureMarker(4, 2, time.Unix(10, 0))
	second, _ := TerminalFailureMarker(4, 2, time.Unix(20, 0))
	api := fixtureAPI(t, map[string]any{"/repos/o/r/issues/4/comments?per_page=100&page=1": []any{
		map[string]any{"body": first, "user": map[string]any{"id": 42}},
		map[string]any{"body": second, "user": map[string]any{"id": 42}},
	}})
	_, conflicts, err := fetchTerminalFailures(context.Background(), api, PRAdapterConfig{Repository: "o/r", ActorID: 42}, 4)
	if err != nil || !conflicts.Any || !conflicts.Attempts[2] {
		t.Fatalf("conflicts=%v err=%v", conflicts, err)
	}
}

func TestFetchIssueFactsAutonomousLabelsAuthorizeWithoutApproval(t *testing.T) {
	now := time.Date(2026, 8, 6, 0, 0, 0, 0, time.UTC)
	body := "## Context\nx\n## Acceptance Criteria\nx\n## Checklist\n- [ ] x\n## Validation\nx\n## Dependencies\nnone\n"
	var snapshots []string
	autonomous, edited, priorityChanged, approved := true, false, false, false
	actor, posts := 5, 0
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		currentBody := body
		if edited {
			currentBody += "later edit\n"
		}
		comments := make([]any, len(snapshots))
		for i, snapshot := range snapshots {
			comments[i] = map[string]any{"id": 60 + i, "body": snapshot, "user": map[string]any{"id": 42}}
		}
		if approved {
			comments = append(comments, map[string]any{"id": 70, "body": "/approve", "created_at": now.Add(time.Minute), "updated_at": now.Add(time.Minute), "user": map[string]any{"id": 5}})
		}
		events := []any{
			map[string]any{"id": 20, "event": "labeled", "label": map[string]any{"name": "ready"}, "created_at": now, "actor": map[string]any{"id": actor}},
			map[string]any{"id": 21, "event": "labeled", "label": map[string]any{"name": "P1"}, "created_at": now, "actor": map[string]any{"id": actor}},
			map[string]any{"id": 22, "event": "labeled", "label": map[string]any{"name": "auto"}, "created_at": now, "actor": map[string]any{"id": actor}},
		}
		labels := []any{map[string]any{"name": "ready"}, map[string]any{"name": "P1"}, map[string]any{"name": "auto"}}
		if priorityChanged {
			events = append(events,
				map[string]any{"id": 24, "event": "unlabeled", "label": map[string]any{"name": "P1"}, "created_at": now.Add(time.Second), "actor": map[string]any{"id": actor}},
				map[string]any{"id": 25, "event": "labeled", "label": map[string]any{"name": "P2"}, "created_at": now.Add(2 * time.Second), "actor": map[string]any{"id": actor}},
			)
			labels[1] = map[string]any{"name": "P2"}
		}
		if !autonomous {
			events = append(events, map[string]any{"id": 23, "event": "unlabeled", "label": map[string]any{"name": "auto"}, "created_at": now.Add(time.Second), "actor": map[string]any{"id": actor}})
			labels = labels[:2]
		}
		var response any
		switch r.Method + " " + r.URL.RequestURI() {
		case "GET /repos/o/r":
			response = map[string]any{"default_branch": "main"}
		case "GET /repos/o/r/branches/main":
			response = map[string]any{"commit": map[string]any{"sha": "abcdef0"}}
		case "GET /repos/o/r/issues?state=open&per_page=100&page=1":
			response = []any{map[string]any{"number": 10, "title": "label-only", "body": currentBody, "created_at": now}}
		case "GET /repos/o/r/issues/10":
			response = map[string]any{"number": 10, "node_id": "I_10", "state": "open", "body": currentBody, "created_at": now, "user": map[string]any{"id": 9}, "labels": labels}
		case "GET /repos/o/r/issues/10/timeline?per_page=100&page=1":
			response = events
		case "GET /repos/o/r/issues/10/comments?per_page=100&page=1":
			response = comments
		case "GET /repos/o/r/issues/comments/70":
			response = comments[len(comments)-1]
		case "POST /graphql":
			nodes := []any{}
			if edited {
				nodes = append(nodes, map[string]any{"id": "UCE_1", "editedAt": now.Add(2 * time.Second), "editor": map[string]any{"__typename": "User", "databaseId": 5}})
			}
			response = map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": nodes}}}}}
		case "GET /user/5", "GET /user/42":
			response = map[string]any{"login": "owner"}
		case "GET /repos/o/r/collaborators/owner/permission":
			response = map[string]any{"permission": "maintain"}
		case "POST /repos/o/r/issues/10/comments":
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil {
				t.Fatal("invalid snapshot request")
			}
			snapshot, err := ParseSnapshotComment(payload.Body, 42, 42)
			if err != nil || !approved && (snapshot.ApprovalID != 0 || snapshot.ApprovalActor != 0) || approved && (snapshot.ApprovalID != 70 || snapshot.ApprovalActor != 5) {
				t.Fatalf("label-only snapshot=%#v err=%v", snapshot, err)
			}
			snapshots, posts = append(snapshots, payload.Body), posts+1
			return httpResponse(http.StatusCreated, `{}`, nil), nil
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		encoded, _ := json.Marshal(response)
		return httpResponse(http.StatusOK, string(encoded), nil), nil
	})}}
	cfg := productionPRConfig()

	facts, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(facts) != 1 || !facts[0].Eligible || !facts[0].DispatchAuthorized || posts != 1 {
		t.Fatalf("autonomous facts=%#v posts=%d err=%v", facts, posts, err)
	}
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || !facts[0].Eligible || posts != 1 {
		t.Fatalf("restored autonomous facts=%#v posts=%d err=%v", facts, posts, err)
	}

	edited = true
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || facts[0].Eligible || posts != 1 || !slices.Contains(facts[0].Blockers, "ready label does not authorize the current issue body") {
		t.Fatalf("edited facts=%#v posts=%d err=%v", facts, posts, err)
	}

	edited, priorityChanged = false, true
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || !facts[0].Eligible || posts != 2 || len(facts[0].Blockers) != 0 {
		t.Fatalf("changed-priority facts=%#v posts=%d err=%v", facts, posts, err)
	}

	approved = true
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || !facts[0].Eligible || posts != 2 {
		t.Fatalf("explicit autonomous approval facts=%#v posts=%d err=%v", facts, posts, err)
	}

	snapshots, approved, priorityChanged, autonomous = nil, false, false, false
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || !facts[0].Eligible || !facts[0].DispatchAuthorized || posts != 3 || len(facts[0].Blockers) != 0 {
		t.Fatalf("human-review facts=%#v posts=%d err=%v", facts, posts, err)
	}

	snapshots, autonomous, actor = nil, true, 42
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || !facts[0].Eligible || posts != 4 {
		t.Fatalf("same-user facts=%#v posts=%d err=%v", facts, posts, err)
	}
}

func TestFetchIssueFactsRequiresConfiguredIssueFilter(t *testing.T) {
	now := time.Date(2026, 8, 30, 0, 0, 0, 0, time.UTC)
	body := "## Context\nx\n## Acceptance criteria\nx\n## Checklist\n- [ ] x\n## Validation\nx\n## Dependencies\nNone.\n"
	filterPresent := false
	var snapshots []string
	posts := 0
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		labels := []any{map[string]any{"name": "ready"}, map[string]any{"name": "P1"}}
		events := []any{
			map[string]any{"id": 20, "event": "labeled", "label": map[string]any{"name": "ready"}, "created_at": now.Add(time.Second), "actor": map[string]any{"id": 5}},
			map[string]any{"id": 21, "event": "labeled", "label": map[string]any{"name": "P1"}, "created_at": now.Add(2 * time.Second), "actor": map[string]any{"id": 5}},
		}
		if filterPresent {
			labels = append(labels, map[string]any{"name": "agent-work"})
			events = append(events, map[string]any{"id": 22, "event": "labeled", "label": map[string]any{"name": "agent-work"}, "created_at": now.Add(3 * time.Second), "actor": map[string]any{"id": 5}})
		}
		comments := make([]any, len(snapshots))
		for i, snapshot := range snapshots {
			comments[i] = map[string]any{"id": 60 + i, "body": snapshot, "user": map[string]any{"id": 42}}
		}
		var response any
		switch r.Method + " " + r.URL.RequestURI() {
		case "GET /repos/o/r":
			response = map[string]any{"default_branch": "main"}
		case "GET /repos/o/r/branches/main":
			response = map[string]any{"commit": map[string]any{"sha": "abcdef0"}}
		case "GET /repos/o/r/issues?state=open&per_page=100&page=1":
			response = []any{map[string]any{"number": 10, "title": "filtered", "body": body, "created_at": now}}
		case "GET /repos/o/r/issues/10":
			response = map[string]any{"number": 10, "node_id": "I_10", "state": "open", "body": body, "created_at": now, "user": map[string]any{"id": 5}, "labels": labels}
		case "GET /repos/o/r/issues/10/timeline?per_page=100&page=1":
			response = events
		case "GET /repos/o/r/issues/10/comments?per_page=100&page=1":
			response = comments
		case "POST /graphql":
			response = map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}}}}
		case "GET /user/5":
			response = map[string]any{"login": "owner"}
		case "GET /repos/o/r/collaborators/owner/permission":
			response = map[string]any{"permission": "maintain"}
		case "POST /repos/o/r/issues/10/comments":
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil {
				t.Fatal("invalid snapshot request")
			}
			snapshots, posts = append(snapshots, payload.Body), posts+1
			return httpResponse(http.StatusCreated, `{}`, nil), nil
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
		body, _ := json.Marshal(response)
		return httpResponse(http.StatusOK, string(body), nil), nil
	})}}
	cfg := productionPRConfig()
	cfg.IssueFilterLabel = "agent-work"

	facts, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(facts) != 1 || facts[0].Eligible || facts[0].DispatchAuthorized || facts[0].RecoveryAuthorized || posts != 0 {
		t.Fatalf("missing-filter facts=%#v posts=%d err=%v", facts, posts, err)
	}

	filterPresent = true
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(facts) != 1 || !facts[0].Eligible || !facts[0].DispatchAuthorized || !facts[0].RecoveryAuthorized || posts != 1 {
		t.Fatalf("applied-filter facts=%#v posts=%d err=%v", facts, posts, err)
	}

	filterPresent, snapshots, cfg.IssueFilterLabel = false, nil, ""
	facts, err = FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(facts) != 1 || !facts[0].Eligible || !facts[0].DispatchAuthorized || !facts[0].RecoveryAuthorized || posts != 2 {
		t.Fatalf("unconfigured facts=%#v posts=%d err=%v", facts, posts, err)
	}
}

func TestFetchAttemptFactsPaginatesMarkersAndChecks(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	marker, _ := AttemptMarker(4, 2, branch, "ccccccc", 9, "review")
	binding, _ := ActiveAttemptMarker("o/r", 4, 2, "aaaaaaa")
	noise := make([]map[string]any, 100)
	checks := make([]map[string]string, 100)
	for i := range noise {
		noise[i] = map[string]any{"body": "noise"}
		checks[i] = map[string]string{"name": "check", "status": "completed", "conclusion": "success"}
	}
	pulls := make([]map[string]any, recoveryPullsPerPage)
	for i := range pulls {
		pulls[i] = map[string]any{"body": "noise"}
	}
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body any
		switch r.URL.Path + "?" + r.URL.RawQuery {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1":
			body = pulls
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=2":
			body = []any{map[string]any{"number": 9, "body": marker, "state": "open", "head": map[string]any{"sha": "ccccccc", "ref": branch}, "base": map[string]any{"sha": "bbbbbbb"}, "user": map[string]any{"id": 42}}}
		case "/repos/o/r/issues/4?":
			body = map[string]any{"state": "open"}
		case "/repos/o/r/issues/4/comments?per_page=100&page=1":
			body = noise
		case "/repos/o/r/issues/4/comments?per_page=100&page=2":
			body = []any{map[string]any{"body": binding, "user": map[string]any{"id": 42}}, map[string]any{"body": marker, "user": map[string]any{"id": 42}}}
		case "/repos/o/r/commits/ccccccc/check-runs?filter=latest&per_page=100&page=1":
			body = map[string]any{"check_runs": checks}
		case "/repos/o/r/commits/ccccccc/check-runs?filter=latest&per_page=100&page=2":
			body = map[string]any{"check_runs": []any{map[string]string{"name": "later", "status": "completed", "conclusion": "success"}}}
		case "/repos/o/r/commits/ccccccc/status?":
			body = map[string]any{"statuses": []any{}}
		default:
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		b, _ := json.Marshal(body)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(b)))}, nil
	})}}
	facts, err := FetchAttemptFacts(context.Background(), api, "o/r", 42)
	if err != nil || len(facts) != 1 || facts[0].BaseSHA != "aaaaaaa" || facts[0].State != "review-ready" || !facts[0].PublicationConfirmed || len(facts[0].Checks) != 101 {
		t.Fatalf("facts=%#v err=%v", facts, err)
	}
}

func TestFetchOperatorAttemptFactsReadsOnlyTheDeterministicPullRequest(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	marker, _ := AttemptMarker(4, 2, branch, "ccccccc", 9, "review")
	binding, _ := ActiveAttemptMarker("o/r", 4, 2, "aaaaaaa")
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body any
		switch {
		case r.URL.Path == "/repos/o/r/pulls" && r.URL.Query().Get("head") == "o:"+branch:
			body = []any{map[string]any{"number": 9, "body": marker, "state": "open", "head": map[string]any{"sha": "ccccccc", "ref": branch}, "base": map[string]any{"sha": "bbbbbbb"}, "user": map[string]any{"id": 42}}}
		case r.URL.RequestURI() == "/repos/o/r/issues/4":
			body = map[string]any{"state": "open"}
		case r.URL.RequestURI() == "/repos/o/r/issues/4/comments?per_page=100&page=1":
			body = []any{map[string]any{"body": binding, "user": map[string]any{"id": 42}}, map[string]any{"body": marker, "user": map[string]any{"id": 42}}}
		default:
			t.Fatalf("unexpected operator-target request %s", r.URL.String())
		}
		encoded, _ := json.Marshal(body)
		return httpResponse(http.StatusOK, string(encoded), nil), nil
	})}}
	facts, err := FetchOperatorAttemptFacts(context.Background(), api, "o/r", 42, 4, 2)
	if err != nil || len(facts) != 1 || facts[0].BaseSHA != "aaaaaaa" || facts[0].State != "active" || len(facts[0].Checks) != 0 {
		t.Fatalf("facts=%#v err=%v", facts, err)
	}
}

func TestFetchAttemptFactsRequiresCoordinatorMarkerAndSafeCompletion(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	marker, _ := AttemptMarker(4, 2, branch, "ccccccc", 9, "review")
	responses := map[string]string{
		"/repos/o/r/pulls": `[
			{"number":8,"body":"agent-symphony:issue:4:attempt:1","state":"open","head":{"sha":"aaaaaaa","ref":"bad"},"base":{"sha":"bbbbbbb"},"user":{"id":9}},
			{"number":9,"body":` + quoteJSON(marker) + `,"state":"open","head":{"sha":"ccccccc","ref":"` + branch + `"},"base":{"sha":"bbbbbbb"},"user":{"id":42}}
		]`,
		"/repos/o/r/issues/4":                   `{"state":"closed"}`,
		"/repos/o/r/issues/4/comments":          `[{"body":` + quoteJSON(marker) + `,"user":{"id":42}}]`,
		"/repos/o/r/commits/ccccccc/check-runs": `{"check_runs":[]}`,
		"/repos/o/r/commits/ccccccc/status":     `{"statuses":[]}`,
	}
	api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, ok := responses[r.URL.Path]
		if !ok {
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	facts, err := FetchAttemptFacts(context.Background(), api, "o/r", 42)
	if err != nil || len(facts) != 1 || facts[0].Attempt != 2 || facts[0].State != "blocked" {
		t.Fatalf("facts=%#v err=%v", facts, err)
	}
}

func TestFindPublishedAttemptRequiresCoordinatorPR(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	pull := map[string]any{"number": 9, "body": "pre-bind", "head": map[string]any{"sha": "ccccccc", "ref": branch}, "user": map[string]any{"id": 42}}
	pulls := make([]any, recoveryPullsPerPage)
	api := fixtureAPI(t, map[string]any{
		"/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1": pulls,
		"/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=2": []any{pull},
	})
	for range 2 {
		pr, body, err := FindPublishedAttempt(context.Background(), api, "o/r", branch, "ccccccc", 42)
		if err != nil || pr.Number != 9 || body != "pre-bind" {
			t.Fatalf("pr=%#v body=%q err=%v", pr, body, err)
		}
	}

	pull["user"] = map[string]any{"id": 9}
	if _, _, err := FindPublishedAttempt(context.Background(), api, "o/r", branch, "ccccccc", 42); err == nil {
		t.Fatal("foreign actor owned deterministic pull request")
	}
	pull["user"] = map[string]any{"id": 42}
	for _, identity := range []map[string]any{{"sha": "ddddddd", "ref": branch}, {"sha": "ccccccc", "ref": "other"}} {
		pull["head"] = identity
		pr, _, err := FindPublishedAttempt(context.Background(), api, "o/r", branch, "ccccccc", 42)
		if err != nil || pr.Number != 0 {
			t.Fatalf("mismatched head was recovered: pr=%#v err=%v", pr, err)
		}
	}
}

func TestFetchAttemptFactsRejectsForeignActors(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	marker, _ := AttemptMarker(4, 2, branch, "ccccccc", 9, "review")
	for _, test := range []struct {
		name         string
		pullActor    int
		commentActor int
	}{
		{name: "foreign PR actor", pullActor: 9, commentActor: 42},
		{name: "foreign comment actor", pullActor: 42, commentActor: 9},
	} {
		t.Run(test.name, func(t *testing.T) {
			responses := map[string]any{
				"/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=25&page=1": []any{map[string]any{"number": 9, "body": marker, "state": "open", "head": map[string]any{"sha": "ccccccc", "ref": branch}, "base": map[string]any{"sha": "bbbbbbb"}, "user": map[string]any{"id": test.pullActor}}},
			}
			if test.pullActor == 42 {
				responses["/repos/o/r/issues/4"] = map[string]any{"state": "open"}
				responses["/repos/o/r/issues/4/comments?per_page=100&page=1"] = []any{map[string]any{"body": marker, "user": map[string]any{"id": test.commentActor}}}
			}
			facts, err := FetchAttemptFacts(context.Background(), fixtureAPI(t, responses), "o/r", 42)
			if err != nil || len(facts) != 0 {
				t.Fatalf("facts=%#v err=%v", facts, err)
			}
		})
	}
}

func TestFetchAttemptFactsRejectsTamperedMarkerBindings(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	valid, _ := AttemptMarker(4, 2, branch, "ccccccc", 9, "review")
	for _, test := range []struct{ name, marker string }{
		{"head", strings.Replace(valid, `"head":"ccccccc"`, `"head":"ddddddd"`, 1)},
		{"pr", strings.Replace(valid, `"pr":9`, `"pr":10`, 1)},
		{"outcome", strings.Replace(valid, `"outcome":"review"`, `"outcome":"failed"`, 1)},
	} {
		t.Run(test.name, func(t *testing.T) {
			responses := map[string]string{
				"/repos/o/r/pulls": `[{"number":9,"body":` + quoteJSON(test.marker) + `,"state":"open","head":{"sha":"ccccccc","ref":"` + branch + `"},"base":{"sha":"bbbbbbb"},"user":{"id":42}}]`,
			}
			api := API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(responses[r.URL.Path]))}, nil
			})}}
			facts, err := FetchAttemptFacts(context.Background(), api, "o/r", 42)
			if err != nil || len(facts) != 0 {
				t.Fatalf("facts=%#v err=%v", facts, err)
			}
		})
	}
}

func quoteJSON(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return `"` + value + `"`
}
