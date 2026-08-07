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

func TestTerminalFailureRequiresStrictAppAuthorship(t *testing.T) {
	marker, _ := TerminalFailureMarker(4, 2, time.Unix(10, 0))
	api := fixtureAPI(t, map[string]any{"/repos/o/r/issues/4/comments?per_page=100&page=1": []any{
		map[string]any{"body": marker, "user": map[string]any{"id": 42}},
		map[string]any{"body": marker + "tamper", "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}},
		map[string]any{"body": marker, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}},
	}})
	got, err := fetchTerminalFailure(context.Background(), api, PRAdapterConfig{Repository: "o/r", AppID: 7, AppActorID: 42}, 4)
	if err != nil || got.Attempt != 2 {
		t.Fatalf("terminal=%v err=%v", got, err)
	}
}

func TestEnsureActiveAttemptIsStrictAppAuthoredAndIdempotent(t *testing.T) {
	marker, err := ActiveAttemptMarker("o/r", 4, 2, "abcdef0")
	if err != nil {
		t.Fatal(err)
	}
	var comments []map[string]any
	posts := 0
	api := API{BaseURL: "https://example.test", Tokens: tokenStub("token"), Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.Method {
		case http.MethodGet:
			body, _ := json.Marshal(comments)
			return httpResponse(http.StatusOK, string(body), nil), nil
		case http.MethodPost:
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil || !strings.Contains(payload.Body, marker) {
				t.Fatal("dispatch did not persist the active marker")
			}
			comments = append(comments, map[string]any{"body": payload.Body, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}})
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
	cfg := PRAdapterConfig{Repository: "o/r", AppID: 7, AppActorID: 42}
	for range 2 {
		if err := EnsureActiveAttempt(context.Background(), api, cfg, 4, 2, "abcdef0"); err != nil {
			t.Fatal(err)
		}
	}
	if posts != 1 {
		t.Fatalf("marker posts=%d, want 1", posts)
	}
	comments = append(comments, map[string]any{"body": marker, "user": map[string]any{"id": 9}, "performed_via_github_app": map[string]any{"id": 7}})
	if err := EnsureActiveAttempt(context.Background(), api, cfg, 4, 2, "abcdef0"); err == nil || posts != 1 {
		t.Fatalf("foreign marker err=%v posts=%d", err, posts)
	}
}

func TestActiveAttemptMarkerRejectsHostileAndContradictoryComments(t *testing.T) {
	first, _ := ActiveAttemptMarker("o/r", 4, 2, "abcdef0")
	second, _ := ActiveAttemptMarker("o/r", 4, 2, "abcdef1")
	for _, test := range []struct {
		name     string
		comments []any
	}{
		{"malformed", []any{map[string]any{"body": activeMarkerPrefix + `{}`, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}}}},
		{"foreign", []any{map[string]any{"body": first, "user": map[string]any{"id": 9}, "performed_via_github_app": map[string]any{"id": 7}}}},
		{"contradictory", []any{
			map[string]any{"body": first, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}},
			map[string]any{"body": second, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			api := fixtureAPI(t, map[string]any{"/repos/o/r/issues/4/comments?per_page=100&page=1": test.comments})
			_, conflict, err := fetchActiveAttempts(context.Background(), api, PRAdapterConfig{Repository: "o/r", AppID: 7, AppActorID: 42}, 4)
			if err != nil || !conflict {
				t.Fatalf("conflict=%v err=%v", conflict, err)
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
	body := "## Context\nx\n## Acceptance Criteria\nx\n## Tasks\nx\n## Validation\nx\n## Dependencies\nnone\n"
	var snapshotBodies []string
	changed, approved := false, false
	posts := 0
	api := API{BaseURL: "https://example.test", Tokens: tokenStub("token"), Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		comments := []any{map[string]any{"id": 50, "body": "/approve", "created_at": now.Add(time.Minute), "updated_at": now.Add(time.Minute), "user": map[string]any{"id": 5}}}
		for i, snapshotBody := range snapshotBodies {
			createdAt := now.Add(time.Duration(2+i*2) * time.Minute)
			comments = append(comments, map[string]any{"id": 60 + i*20, "body": snapshotBody, "created_at": createdAt, "updated_at": createdAt, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}})
		}
		if approved {
			comments = append(comments, map[string]any{"id": 70, "body": "/approve", "created_at": now.Add(4 * time.Minute), "updated_at": now.Add(4 * time.Minute), "user": map[string]any{"id": 5}})
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
			response = map[string]any{"number": 10, "node_id": "I_10", "state": "open", "body": body, "created_at": now, "user": map[string]any{"id": 9}, "labels": []any{map[string]any{"name": "ready"}, map[string]any{"name": priority}}}
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
			response = events
		case "GET /repos/o/r/issues/10/comments?per_page=100&page=1":
			response = comments
		case "POST /graphql":
			response = map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}}}}
		case "GET /repos/o/r/issues/comments/50":
			response = comments[0]
		case "GET /repos/o/r/issues/comments/70":
			response = comments[len(comments)-1]
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
	stale, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(stale) != 1 || stale[0].Eligible || posts != 1 || !slices.Contains(stale[0].Blockers, "fresh exact approval command is missing") {
		t.Fatalf("stale facts=%#v posts=%d err=%v", stale, posts, err)
	}
	approved = true
	readOnly, err = FetchIssueFacts(context.Background(), api, cfg, nil, false)
	if err != nil || len(readOnly) != 1 || readOnly[0].Eligible || posts != 1 {
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
	if err != nil || len(bound) != 1 || bound[0].Eligible || !bound[0].Active || bound[0].Attempt != 2 || bound[0].ActiveAttempt == nil || bound[0].ActiveAttempt.BaseSHA != "abcdef0" {
		t.Fatalf("bound facts=%#v err=%v", bound, err)
	}
	terminal, _ := TerminalFailureMarker(10, 2, now.Add(20*time.Minute))
	snapshotBodies = append(snapshotBodies, terminal)
	failed, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(failed) != 1 || failed[0].Active || failed[0].ActiveAttempt != nil || failed[0].Attempt != 3 || len(failed[0].Blockers) == 0 {
		t.Fatalf("terminal transition facts=%#v err=%v", failed, err)
	}
}

func TestFetchAttemptFactsPaginatesMarkersAndChecks(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	marker, _ := AttemptMarker(4, 2, branch, "ccccccc", 9, "review")
	noise := make([]map[string]any, 100)
	checks := make([]map[string]string, 100)
	for i := range noise {
		noise[i] = map[string]any{"body": "noise"}
		checks[i] = map[string]string{"name": "check", "status": "completed", "conclusion": "success"}
	}
	api := API{BaseURL: "https://example.test", Tokens: tokenStub("token"), Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body any
		switch r.URL.Path + "?" + r.URL.RawQuery {
		case "/repos/o/r/pulls?state=all&sort=updated&direction=desc&per_page=100&page=1":
			body = []any{map[string]any{"number": 9, "body": marker, "state": "open", "head": map[string]any{"sha": "ccccccc", "ref": branch}, "base": map[string]any{"sha": "bbbbbbb"}, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}}}
		case "/repos/o/r/issues/4?":
			body = map[string]any{"state": "open"}
		case "/repos/o/r/issues/4/comments?per_page=100&page=1":
			body = noise
		case "/repos/o/r/issues/4/comments?per_page=100&page=2":
			body = []any{map[string]any{"body": marker, "user": map[string]any{"id": 42}, "performed_via_github_app": map[string]any{"id": 7}}}
		case "/repos/o/r/commits/ccccccc/check-runs?filter=latest&per_page=100&page=1":
			body = map[string]any{"check_runs": checks}
		case "/repos/o/r/commits/ccccccc/check-runs?filter=latest&per_page=100&page=2":
			body = map[string]any{"check_runs": []any{map[string]string{"name": "later", "status": "completed", "conclusion": "success"}}}
		default:
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		b, _ := json.Marshal(body)
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(b)))}, nil
	})}}
	facts, err := FetchAttemptFacts(context.Background(), api, "o/r", 7, 42)
	if err != nil || len(facts) != 1 || facts[0].State != "review-ready" || len(facts[0].Checks) != 101 {
		t.Fatalf("facts=%#v err=%v", facts, err)
	}
}

func TestFetchAttemptFactsRequiresAppMarkerSnapshotAndSafeCompletion(t *testing.T) {
	branch, _ := AttemptBranch("o/r", 4, 2)
	marker, _ := AttemptMarker(4, 2, branch, "ccccccc", 9, "review")
	responses := map[string]string{
		"/repos/o/r/pulls": `[
			{"number":8,"body":"agent-symphony:issue:4:attempt:1","state":"open","head":{"sha":"aaaaaaa","ref":"bad"},"base":{"sha":"bbbbbbb"},"user":{"id":9},"performed_via_github_app":{"id":7}},
			{"number":9,"body":` + quoteJSON(marker) + `,"state":"open","head":{"sha":"ccccccc","ref":"` + branch + `"},"base":{"sha":"bbbbbbb"},"user":{"id":42},"performed_via_github_app":{"id":7}}
		]`,
		"/repos/o/r/issues/4":                   `{"state":"closed"}`,
		"/repos/o/r/issues/4/comments":          `[{"body":` + quoteJSON(marker) + `,"user":{"id":42},"performed_via_github_app":{"id":7}}]`,
		"/repos/o/r/commits/ccccccc/check-runs": `{"check_runs":[]}`,
	}
	api := API{BaseURL: "https://example.test", Tokens: tokenStub("token"), Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, ok := responses[r.URL.Path]
		if !ok {
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
	})}}
	facts, err := FetchAttemptFacts(context.Background(), api, "o/r", 7, 42)
	if err != nil || len(facts) != 1 || facts[0].Attempt != 2 || facts[0].State != "blocked" {
		t.Fatalf("facts=%#v err=%v", facts, err)
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
				"/repos/o/r/pulls": `[{"number":9,"body":` + quoteJSON(test.marker) + `,"state":"open","head":{"sha":"ccccccc","ref":"` + branch + `"},"base":{"sha":"bbbbbbb"},"user":{"id":42},"performed_via_github_app":{"id":7}}]`,
			}
			api := API{BaseURL: "https://example.test", Tokens: tokenStub("token"), Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(responses[r.URL.Path]))}, nil
			})}}
			facts, err := FetchAttemptFacts(context.Background(), api, "o/r", 7, 42)
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
