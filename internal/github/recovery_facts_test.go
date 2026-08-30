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
			if json.NewDecoder(r.Body).Decode(&payload) != nil || !strings.Contains(payload.Body, marker) {
				t.Fatal("dispatch did not persist the active marker")
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
		if err := EnsureActiveAttempt(context.Background(), api, cfg, 4, 2, "abcdef0"); err != nil {
			t.Fatal(err)
		}
	}
	if posts != 1 {
		t.Fatalf("marker posts=%d, want 1", posts)
	}
	comments = append(comments, map[string]any{"body": marker, "user": map[string]any{"id": 9}})
	if err := EnsureActiveAttempt(context.Background(), api, cfg, 4, 2, "abcdef0"); err == nil || posts != 1 {
		t.Fatalf("foreign marker err=%v posts=%d", err, posts)
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
	body := "arbitrary issue body without structured sections"
	var snapshotBodies []string
	changed := false
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
	terminal, _ := TerminalFailureMarker(10, 2, now.Add(20*time.Minute))
	snapshotBodies = append(snapshotBodies, "Attempt failed closed: worker produced no repository changes\n\n"+terminal)
	failed, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(failed) != 1 || failed[0].Active || failed[0].ActiveAttempt != nil || len(failed[0].TerminalAttempts) != 1 || failed[0].TerminalAttempts[0].Attempt != 2 || failed[0].TerminalAttempts[0].BaseSHA != "abcdef0" || failed[0].TerminalAttempts[0].Diagnostic != "worker produced no repository changes" || failed[0].RecoveryAttempt != 2 || !failed[0].RecoveryAuthorized || failed[0].Attempt != 3 || len(failed[0].Blockers) == 0 {
		t.Fatalf("terminal transition facts=%#v err=%v", failed, err)
	}
	third, _ := ActiveAttemptMarker("o/r", 10, 3, "abcdef0")
	thirdTerminal, _ := TerminalFailureMarker(10, 3, now.Add(30*time.Minute))
	snapshotBodies = append(snapshotBodies, third, "Attempt failed closed: newer failure\n\n"+thirdTerminal)
	history, err := FetchIssueFacts(context.Background(), api, cfg, nil, true)
	if err != nil || len(history) != 1 || len(history[0].TerminalAttempts) != 2 || history[0].TerminalAttempts[0].Attempt != 2 || history[0].TerminalAttempts[1].Attempt != 3 || history[0].RecoveryAttempt != 3 || !history[0].RecoveryAuthorized {
		t.Fatalf("terminal history=%#v err=%v", history, err)
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
	body := "## Context\nx\n## Acceptance Criteria\nx\n## Tasks\nx\n## Validation\nx\n## Dependencies\nnone\n"
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
			response = []any{map[string]any{"number": 10, "title": "filtered", "body": "work", "created_at": now}}
		case "GET /repos/o/r/issues/10":
			response = map[string]any{"number": 10, "node_id": "I_10", "state": "open", "body": "work", "created_at": now, "user": map[string]any{"id": 5}, "labels": labels}
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
