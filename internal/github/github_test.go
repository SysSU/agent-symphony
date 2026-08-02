package github

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type jwtStub string

func (j jwtStub) JWT(time.Time) (string, error) { return string(j), nil }

type tokenStub string

func (t tokenStub) Token(context.Context) (InstallationToken, error) {
	return InstallationToken{Value: string(t), ExpiresAt: time.Now().Add(time.Hour)}, nil
}

type permissionStub string

func (p permissionStub) Permission(int) (string, error) { return string(p), nil }

type permissionError struct{}

func (permissionError) Permission(int) (string, error) { return "admin", errors.New("lookup failed") }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func httpResponse(status int, body string, headers http.Header) *http.Response {
	normalized := make(http.Header)
	for name, values := range headers {
		for _, value := range values {
			normalized.Add(name, value)
		}
	}
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: normalized, Body: io.NopCloser(strings.NewReader(body))}
}

func provenanceFor(controls Controls, actor int) []Provenance {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	result := []Provenance{
		{Name: "ready", Value: fmt.Sprint(controls.Ready), Source: "timeline", EventID: 1, ActorID: actor, CreatedAt: now},
		{Name: "priority", Value: fmt.Sprint(controls.Priority), Source: "timeline", EventID: 2, ActorID: actor, CreatedAt: now},
		{Name: "completion", Value: controls.Completion, Source: "timeline", EventID: 3, ActorID: actor, CreatedAt: now},
		{Name: "closed", Value: fmt.Sprint(controls.Closed), Source: "timeline", EventID: 4, ActorID: actor, CreatedAt: now},
		{Name: "cancelled", Value: fmt.Sprint(controls.Cancelled), Source: "comment", EventID: 5, ActorID: actor, CreatedAt: now},
		{Name: "retry", Value: fmt.Sprint(controls.Retry), Source: "comment", EventID: 6, ActorID: actor, CreatedAt: now},
	}
	return result
}

func timelineFor(provenance []Provenance) TimelineVerifier {
	events := make(map[Provenance]bool, len(provenance))
	for _, event := range provenance {
		events[event] = true
	}
	return func(event Provenance) bool { return events[event] }
}

func TestAppJWTAndInstallationExchange(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC)
	token, err := (AppJWT{AppID: "42", Key: key}).JWT(now)
	if err != nil {
		t.Fatal(err)
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		t.Fatalf("invalid JWT %q", token)
	}
	claims, _ := base64.RawURLEncoding.DecodeString(parts[1])
	if !strings.Contains(string(claims), `"iss":"42"`) {
		t.Fatalf("claims %s", claims)
	}

	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/app/installations/7/access_tokens" || r.Header.Get("Authorization") != "Bearer app-jwt" {
			t.Errorf("bad request %#v", r)
		}
		return httpResponse(http.StatusCreated, fmt.Sprintf(`{"token":"installation-canary","expires_at":%q}`, now.Add(time.Hour).Format(time.RFC3339)), nil), nil
	})}
	tokens := &InstallationTokens{BaseURL: "https://api.example.test", InstallationID: 7, JWTs: jwtStub("app-jwt"), HTTP: client, Now: func() time.Time { return now }}
	got, err := tokens.Token(context.Background())
	if err != nil || got.Value != "installation-canary" {
		t.Fatalf("got %#v, %v", got, err)
	}
}

func TestInstallationTokenCacheIsConcurrentAndRefreshesNearExpiry(t *testing.T) {
	var exchanges atomic.Int32
	now := time.Date(2026, 8, 2, 1, 2, 3, 0, time.UTC)
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		n := exchanges.Add(1)
		return httpResponse(http.StatusCreated, fmt.Sprintf(`{"token":"token-%d","expires_at":%q}`, n, now.Add(2*time.Minute).Format(time.RFC3339)), nil), nil
	})}
	tokens := &InstallationTokens{BaseURL: "https://api.example.test", InstallationID: 7, JWTs: jwtStub("jwt"), HTTP: client, Now: func() time.Time { return now }}
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := tokens.Token(context.Background()); err != nil {
				t.Error(err)
			}
		}()
	}
	group.Wait()
	if exchanges.Load() != 1 {
		t.Fatalf("got %d exchanges, want 1", exchanges.Load())
	}
	now = now.Add(70 * time.Second)
	if _, err := tokens.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if exchanges.Load() != 2 {
		t.Fatalf("near-expiry token was not refreshed: %d exchanges", exchanges.Load())
	}
}

func TestDecodeRejectsTrailingAPIAndTokenJSON(t *testing.T) {
	api := API{BaseURL: "https://api.example.test", Tokens: tokenStub("token"), HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return httpResponse(http.StatusOK, `{"ok":true}{"extra":true}`, nil), nil
	})}}
	var result struct {
		OK bool `json:"ok"`
	}
	if _, _, err := api.Read(context.Background(), "/read", "", &result); err == nil {
		t.Fatal("trailing API JSON accepted")
	}
	now := time.Now()
	tokens := &InstallationTokens{BaseURL: "https://api.example.test", InstallationID: 7, JWTs: jwtStub("jwt"), Now: func() time.Time { return now }, HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		body := fmt.Sprintf(`{"token":"value","expires_at":%q}{}`, now.Add(time.Hour).Format(time.RFC3339))
		return httpResponse(http.StatusCreated, body, nil), nil
	})}}
	if _, err := tokens.Token(context.Background()); err == nil {
		t.Fatal("trailing installation-token JSON accepted")
	}
	if err := decodeJSON(strings.NewReader(strings.Repeat(" ", (1<<20)+1)), &result); err == nil {
		t.Fatal("over-limit JSON accepted")
	}
}

func TestWebhookSecurityQueueAndDuplicate(t *testing.T) {
	secret, body := []byte("hook-canary"), []byte(`{"installation":{"id":7},"repository":{"id":9},"issue":{"number":5}}`)
	hints := make(chan Hint, 1)
	handler := Webhook{Secret: secret, RepositoryID: 9, InstallationID: 7, MaxBody: 256, Hints: hints, Deliveries: NewDeliveryCache(2)}
	request := func(delivery, signature string, content []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(content)))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-GitHub-Event", "issues")
		r.Header.Set("X-GitHub-Delivery", delivery)
		r.Header.Set("X-Hub-Signature-256", signature)
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if got := request("one", "sha256=00", body).Code; got != http.StatusUnauthorized {
		t.Fatalf("bad signature: %d", got)
	}
	if got := request("one", SignWebhook(secret, body), body).Code; got != http.StatusAccepted {
		t.Fatalf("valid: %d", got)
	}
	if got := request("one", SignWebhook(secret, body), body).Code; got != http.StatusAccepted {
		t.Fatalf("duplicate: %d", got)
	}
	if len(hints) != 1 {
		t.Fatalf("want one hint, got %d", len(hints))
	}
	if got := request("queue-full", SignWebhook(secret, body), body).Code; got != http.StatusServiceUnavailable {
		t.Fatalf("full queue: %d", got)
	}
	other := []byte(`{"installation":{"id":7},"repository":{"id":8}}`)
	if got := request("two", SignWebhook(secret, other), other).Code; got != http.StatusBadRequest {
		t.Fatalf("target: %d", got)
	}
	large := []byte(strings.Repeat("x", 300))
	if got := request("large", SignWebhook(secret, large), large).Code; got != http.StatusRequestEntityTooLarge {
		t.Fatalf("large: %d", got)
	}
}

func TestWebhookConcurrentDuplicateAdmissionAndFailedOffer(t *testing.T) {
	secret, body := []byte("secret"), []byte(`{"installation":{"id":7},"repository":{"id":9}}`)
	hints := make(chan Hint, 1)
	handler := Webhook{Secret: secret, RepositoryID: 9, InstallationID: 7, Hints: hints, Deliveries: NewDeliveryCache(2)}
	request := func(delivery string) int {
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-GitHub-Event", "issues")
		r.Header.Set("X-GitHub-Delivery", delivery)
		r.Header.Set("X-Hub-Signature-256", SignWebhook(secret, body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w.Code
	}
	var group sync.WaitGroup
	for range 20 {
		group.Add(1)
		go func() {
			defer group.Done()
			if got := request("same"); got != http.StatusAccepted {
				t.Errorf("status %d", got)
			}
		}()
	}
	group.Wait()
	if len(hints) != 1 {
		t.Fatalf("got %d hints, want 1", len(hints))
	}
	if got := request("retry"); got != http.StatusServiceUnavailable {
		t.Fatalf("full queue status %d", got)
	}
	<-hints
	if got := request("retry"); got != http.StatusAccepted {
		t.Fatalf("failed offer was cached: %d", got)
	}
}

func TestWebhookMediaTypeAndRepositoryWideInstallationEvents(t *testing.T) {
	secret := []byte("secret")
	request := func(event, contentType string, body []byte) (int, Hint) {
		hints := make(chan Hint, 1)
		handler := Webhook{Secret: secret, RepositoryID: 9, InstallationID: 7, Hints: hints}
		r := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(body)))
		r.Header.Set("Content-Type", contentType)
		r.Header.Set("X-GitHub-Event", event)
		r.Header.Set("X-GitHub-Delivery", event)
		r.Header.Set("X-Hub-Signature-256", SignWebhook(secret, body))
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		select {
		case hint := <-hints:
			return w.Code, hint
		default:
			return w.Code, Hint{}
		}
	}
	repositoryBody := []byte(`{"installation":{"id":7},"repository":{"id":9}}`)
	if code, _ := request("issues", "application/json-patch+json", repositoryBody); code != http.StatusUnsupportedMediaType {
		t.Fatalf("prefixed media type accepted: %d", code)
	}
	if code, _ := request("issues", "application/json; charset=utf-8", repositoryBody); code != http.StatusAccepted {
		t.Fatalf("valid parameterized media type rejected: %d", code)
	}
	installationBody := []byte(`{"installation":{"id":7}}`)
	for _, event := range []string{"installation", "installation_repositories"} {
		code, hint := request(event, "application/json", installationBody)
		if code != http.StatusAccepted || hint.RepositoryID != 9 {
			t.Fatalf("%s: code=%d hint=%#v", event, code, hint)
		}
	}
	if code, _ := request("issues", "application/json", installationBody); code != http.StatusBadRequest {
		t.Fatalf("repository-less issue accepted: %d", code)
	}
	mismatch := []byte(`{"installation":{"id":7},"repository":{"id":8}}`)
	if code, _ := request("installation", "application/json", mismatch); code != http.StatusBadRequest {
		t.Fatalf("mismatched installation repository accepted: %d", code)
	}
}

func TestAPIReadRetriesMutationDoesNotAndRedacts(t *testing.T) {
	var reads, writes atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("Authorization") != "Bearer token-canary" {
			t.Error("missing auth")
		}
		if r.Method == http.MethodGet {
			if reads.Add(1) == 1 {
				return httpResponse(http.StatusServiceUnavailable, "transient", http.Header{"Retry-After": []string{"0"}}), nil
			}
			return httpResponse(http.StatusOK, `{"ok":true}`, http.Header{"ETag": []string{`"v1"`}}), nil
		}
		writes.Add(1)
		return httpResponse(http.StatusServiceUnavailable, `token=server-canary`, nil), nil
	})}
	api := API{BaseURL: "https://api.example.test", Tokens: tokenStub("token-canary"), HTTP: client, Sleep: func(context.Context, time.Duration) error { return nil }}
	var result struct {
		OK bool `json:"ok"`
	}
	etag, changed, err := api.Read(context.Background(), "/read", "", &result)
	if err != nil || !changed || !result.OK || etag != `"v1"` || reads.Load() != 2 {
		t.Fatalf("read: %q %v %#v %v", etag, changed, result, err)
	}
	update, _ := AttributedBody(5, 1, "done")
	err = api.Mutate(context.Background(), http.MethodPost, "/write", map[string]string{"body": update}, Mutation{Issue: 5, Attempt: 1}, nil)
	if err == nil || writes.Load() != 1 || strings.Contains(err.Error(), "server-canary") {
		t.Fatalf("mutation: %v writes=%d", err, writes.Load())
	}
	if err := api.Mutate(context.Background(), http.MethodPost, "/write", nil, Mutation{}, nil); err == nil {
		t.Fatal("missing attribution accepted")
	}
	if err := api.Mutate(context.Background(), http.MethodPost, "/write", map[string]string{"body": "private headers are insufficient"}, Mutation{Issue: 5, Attempt: 1}, nil); err == nil {
		t.Fatal("mutation without persisted attribution accepted")
	}
	if err := api.Mutate(context.Background(), http.MethodPost, "/write", map[string]string{"note": update, "body": "missing marker"}, Mutation{Issue: 5, Attempt: 1}, nil); err == nil {
		t.Fatal("attribution outside body accepted")
	}
}

func TestIssueControlsApprovalAndCredentialExclusion(t *testing.T) {
	cfg := ContractConfig{Ready: "ready", P1: "P1", P2: "P2", P3: "P3", DependencySection: "Dependencies", HumanReview: "human", AutonomousMerge: "auto"}
	body := "## Context\nfix intake\n## Acceptance Criteria\n- [ ] safe\n## Tasks\n- [ ] implement\n## Validation\ngo test\n## Dependencies\n- #3\n"
	issue := IssueInput{Number: 5, State: "open", Body: body, Labels: []string{"ready", "P1", "auto"}}
	normalized := NormalizeIssue(issue, cfg, map[int]bool{3: true})
	if !normalized.Ready || normalized.Controls.Priority != 1 || normalized.Controls.Completion != "autonomous-merge" {
		t.Fatalf("normalized %#v", normalized)
	}
	issue.Labels = append(issue.Labels, "P2")
	if got := NormalizeIssue(issue, cfg, map[int]bool{3: true}); got.Ready {
		t.Fatalf("conflicting priorities accepted: %#v", got)
	}
	cfg.DefaultCompletion = "autonomous-merge"
	issue.Labels = []string{"ready", "P1", "human"}
	if got := NormalizeIssue(issue, cfg, map[int]bool{3: true}); !got.Ready || got.Controls.Completion != "human-review" {
		t.Fatalf("human override: %#v", got)
	}
	issue.Labels = []string{"ready", "P1", "human", "auto"}
	if got := NormalizeIssue(issue, cfg, map[int]bool{3: true}); got.Ready {
		t.Fatalf("conflicting completion labels accepted: %#v", got)
	}
	issue.Labels = []string{"ready", "P1", "auto"}

	now := time.Now().UTC().Truncate(time.Second)
	anchor := Anchor{IssueNodeID: "I_5", CreatedAt: now, ChangedAt: now, AuthorID: 2}
	approval := Approval{CommentID: 11, ActorID: 4, Body: "/agent-symphony approve", CreatedAt: now.Add(time.Second)}
	provenance := provenanceFor(normalized.Controls, 4)
	authorized := func(id int) bool { return id == 4 }
	timeline := timelineFor(provenance)
	snapshot, err := NewSnapshot(normalized.Controls, issue.Body, anchor, approval, provenance, "/agent-symphony approve", authorized, timeline)
	if err != nil || !snapshot.Valid(normalized.Controls, issue.Body, anchor, approval, provenance, "/agent-symphony approve", authorized, timeline) {
		t.Fatalf("snapshot %v %#v", err, snapshot)
	}
	safetyControls := normalized.Controls
	safetyControls.Closed, safetyControls.Cancelled, safetyControls.Retry = true, true, true
	if _, err := NewSnapshot(safetyControls, issue.Body, anchor, approval, provenance, "/agent-symphony approve", authorized, timeline); err == nil {
		t.Fatal("safety controls without provenance accepted")
	}
	wrongProvenance := provenanceFor(safetyControls, 4)
	wrongProvenance[3].Value = "false"
	if _, err := NewSnapshot(safetyControls, issue.Body, anchor, approval, wrongProvenance, "/agent-symphony approve", authorized, timelineFor(wrongProvenance)); err == nil {
		t.Fatal("mismatched safety provenance accepted")
	}
	if got := NormalizeIssue(IssueInput{Number: 5, State: "open", Body: body, Labels: []string{"ready", "P1"}, Cancelled: true}, cfg, map[int]bool{3: true}); got.Ready {
		t.Fatalf("cancelled issue is ready: %#v", got)
	}
	safetyProvenance := provenanceFor(safetyControls, 4)
	if _, err := NewSnapshot(safetyControls, issue.Body, anchor, approval, safetyProvenance, "/agent-symphony approve", authorized, timelineFor(safetyProvenance)); err != nil {
		t.Fatalf("authorized safety provenance rejected: %v", err)
	}
	approval.Body = "/agent-symphony approve edited"
	if snapshot.Valid(normalized.Controls, issue.Body, anchor, approval, provenance, "/agent-symphony approve", authorized, timeline) {
		t.Fatal("edited command accepted")
	}
	comment := SnapshotComment(snapshot)
	if parsed, err := ParseSnapshotComment(comment, 99, 99); err != nil || parsed.Version != 2 || parsed.ControlsHash != snapshot.ControlsHash || SnapshotComment(parsed) != comment {
		t.Fatalf("parse snapshot: %#v %v", parsed, err)
	}
	legacy := strings.Replace(strings.Replace(comment, "controls:v2", "controls:v1", 1), `"version":2`, `"version":1`, 1)
	if _, err := ParseSnapshotComment(legacy, 99, 99); err == nil {
		t.Fatal("legacy v1 snapshot accepted")
	}
	if _, err := ParseSnapshotComment(comment, 4, 99); err == nil {
		t.Fatal("non-App snapshot accepted")
	}
	trailing := strings.TrimSuffix(comment, "\n-->") + `{}` + "\n-->"
	if _, err := ParseSnapshotComment(trailing, 99, 99); err == nil {
		t.Fatal("snapshot trailing JSON accepted")
	}
	if update, err := AttributedBody(5, 1, "validation passed"); err != nil || !strings.Contains(update, "issue:5:attempt:1") {
		t.Fatalf("attribution %q %v", update, err)
	}
	if ok, err := AuthorizedControlActor(4, 99, permissionStub("maintain")); err != nil || !ok {
		t.Fatalf("authorized actor: %v %v", ok, err)
	}
	if ok, _ := AuthorizedControlActor(99, 99, permissionStub("admin")); ok {
		t.Fatal("App actor authorized its own control")
	}
	if ok, err := AuthorizedControlActor(4, 99, permissionError{}); err == nil || ok {
		t.Fatalf("lookup error authorized actor: %v %v", ok, err)
	}

	env, err := AgentEnvironmentWith([]string{"PATH=/bin", "GITHUB_TOKEN=canary", "SSH_AUTH_SOCK=/tmp/x", "MODEL_API_KEY=allowed", "SURPRISE_CREDENTIAL=blocked"}, "MODEL_API_KEY")
	if err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(env, "\n")
	if strings.Contains(joined, "GITHUB_TOKEN") || strings.Contains(joined, "SSH_AUTH") || strings.Contains(joined, "SURPRISE_CREDENTIAL") || !strings.Contains(joined, "MODEL_API_KEY=allowed") {
		t.Fatalf("environment %s", joined)
	}
	if got := Redact("authorization: Bearer-canary password=hunter2", "Bearer-canary"); strings.Contains(got, "hunter2") || strings.Contains(got, "Bearer-canary") {
		t.Fatalf("redaction %q", got)
	}
}

func TestAgentEnvironmentRejectsReservedExplicitNames(t *testing.T) {
	for _, name := range []string{"GITHUB_TOKEN", "GH_TOKEN", "SSH_AUTH_SOCK", "AWS_ACCESS_KEY_ID", "AZURE_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS", "CLOUDFLARE_API_TOKEN", "GIT_ASKPASS", "GIT_CONFIG_COUNT", "FTP_PROXY", "APP_PEM", "MY_APP_KEY", "WEBHOOK_SECRET", "RANDOM_PASSWORD"} {
		t.Run(name, func(t *testing.T) {
			if _, err := AgentEnvironmentWith([]string{name + "=value"}, name); err == nil {
				t.Fatalf("reserved name %s accepted", name)
			}
		})
	}
	if env, err := AgentEnvironmentWith([]string{"OPENAI_API_KEY=model", "PATH=/bin"}, "OPENAI_API_KEY"); err != nil || !strings.Contains(strings.Join(env, "\n"), "OPENAI_API_KEY=model") {
		t.Fatalf("safe model credential rejected: %v %v", env, err)
	}
}

func TestSnapshotRequiresExactCurrentNonBodyProvenance(t *testing.T) {
	controls := Controls{Ready: true, Priority: 1, Completion: "human-review"} // open, cancellation cleared, retry cleared
	now := time.Now().UTC()
	anchor := Anchor{IssueNodeID: "I_5", CreatedAt: now, ChangedAt: now, AuthorID: 2}
	approval := Approval{CommentID: 7, ActorID: 4, Body: "/approve", CreatedAt: now.Add(time.Second)}
	authorized := func(id int) bool { return id == 4 }
	valid := provenanceFor(controls, 4)
	timeline := timelineFor(valid)
	if _, err := NewSnapshot(controls, "body", anchor, approval, valid, "/approve", authorized, timeline); err != nil {
		t.Fatalf("reopened/cleared controls rejected: %v", err)
	}
	if _, err := NewSnapshot(controls, "body", anchor, approval, valid, "/approve", authorized, nil); err == nil {
		t.Fatal("nil timeline verifier accepted")
	}
	tests := []struct {
		name string
		edit func([]Provenance) []Provenance
	}{
		{"missing", func(p []Provenance) []Provenance { return p[:len(p)-1] }},
		{"duplicate", func(p []Provenance) []Provenance { return append(p, p[0]) }},
		{"extra", func(p []Provenance) []Provenance {
			return append(p, Provenance{Name: "unknown", Value: "x", EventID: 9, ActorID: 4})
		}},
		{"conflicting", func(p []Provenance) []Provenance { p[0].Value = "false"; return p }},
		{"invented-event", func(p []Provenance) []Provenance { p[0].EventID = 999; return p }},
		{"unauthorized", func(p []Provenance) []Provenance { p[0].ActorID = 8; return p }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			candidate := test.edit(slices.Clone(valid))
			if _, err := NewSnapshot(controls, "body", anchor, approval, candidate, "/approve", authorized, timeline); err == nil {
				t.Fatal("invalid provenance accepted")
			}
		})
	}
}

func TestCreationProvenanceAllowsOnlySafeDefaults(t *testing.T) {
	now := time.Now().UTC()
	anchor := Anchor{IssueNodeID: "I_5", CreatedAt: now, ChangedAt: now, AuthorID: 2}
	approval := Approval{CommentID: 7, ActorID: 4, Body: "/approve", CreatedAt: now.Add(time.Second)}
	controls := Controls{Completion: "human-review"}
	provenance := []Provenance{
		{Name: "ready", Value: "false", Source: "creation", ActorID: 2, CreatedAt: now},
		{Name: "priority", Value: "0", Source: "creation", ActorID: 2, CreatedAt: now},
		{Name: "completion", Value: "human-review", Source: "creation", ActorID: 2, CreatedAt: now},
		{Name: "closed", Value: "false", Source: "creation", ActorID: 2, CreatedAt: now},
		{Name: "cancelled", Value: "false", Source: "creation", ActorID: 2, CreatedAt: now},
		{Name: "retry", Value: "false", Source: "creation", ActorID: 2, CreatedAt: now},
	}
	if _, err := NewSnapshot(controls, "body", anchor, approval, provenance, "/approve", func(id int) bool { return id == 4 }, timelineFor(provenance)); err != nil {
		t.Fatal(err)
	}
	controls.Completion, provenance[2].Value = "autonomous-merge", "autonomous-merge"
	if _, err := NewSnapshot(controls, "body", anchor, approval, provenance, "/approve", func(id int) bool { return id == 4 }, timelineFor(provenance)); err == nil {
		t.Fatal("autonomous merge accepted as creation provenance")
	}
}

func TestReconcilerRequiresAuthoritativeRead(t *testing.T) {
	if err := (Reconciler{}).RunOnce(); err == nil {
		t.Fatal("missing read accepted")
	}
	want := errors.New("outage")
	if err := (Reconciler{FullRead: func() error { return want }}).RunOnce(); !errors.Is(err, want) {
		t.Fatalf("got %v", err)
	}
}

func TestReconcilerReadsImmediatelyAndOnHint(t *testing.T) {
	hints := make(chan Hint, 1)
	var reads atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		(Reconciler{Hints: hints, FullRead: func() error { reads.Add(1); return nil }}).Run(ctx, time.Hour, nil)
		close(done)
	}()
	deadline := time.After(time.Second)
	for reads.Load() < 1 {
		select {
		case <-deadline:
			t.Fatal("initial reconciliation did not run")
		default:
		}
	}
	hints <- Hint{Issue: 5}
	for reads.Load() < 2 {
		select {
		case <-deadline:
			t.Fatal("hint reconciliation did not run")
		default:
		}
	}
	cancel()
	<-done
}
