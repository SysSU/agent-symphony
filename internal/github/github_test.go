package github

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type permissionStub string

func (p permissionStub) Permission(int) (string, error) { return string(p), nil }

type permissionError struct{}

func (permissionError) Permission(int) (string, error) { return "admin", errors.New("lookup failed") }

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type readErrorAfterBody struct{ body string }

func (r *readErrorAfterBody) Read(p []byte) (int, error) {
	n := copy(p, r.body)
	r.body = r.body[n:]
	return n, errors.New("response read failed")
}

func TestCLITransportUsesGitHubCLIAuthenticatedSession(t *testing.T) {
	dir := t.TempDir()
	argsPath := filepath.Join(dir, "args")
	ghPath := filepath.Join(dir, "gh")
	script := "#!/bin/sh\nprintf '%s\\n' \"$@\" > \"$GH_ARGS_FILE\"\nprintf 'HTTP/1.1 200 OK\\r\\nContent-Type: application/json\\r\\n\\r\\n{\"id\":42,\"login\":\"coordinator\"}'\n"
	if err := os.WriteFile(ghPath, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GH_ARGS_FILE", argsPath)
	api := API{BaseURL: "https://api.github.com", HTTP: &http.Client{Transport: CLITransport{Path: ghPath}}}
	user, err := api.AuthenticatedUser(context.Background())
	if err != nil || user.ID != 42 || user.Login != "coordinator" {
		t.Fatalf("user=%#v err=%v", user, err)
	}
	args, err := os.ReadFile(argsPath)
	if err != nil {
		t.Fatal(err)
	}
	got := strings.Fields(string(args))
	if len(got) != 9 || !slices.Equal(got[:5], []string{"api", "--include", "--method", "GET", "/user"}) || !slices.Contains(got, "Accept:application/vnd.github+json") || !slices.Contains(got, "X-Github-Api-Version:2022-11-28") {
		t.Fatalf("gh args = %#v", got)
	}
}

func httpResponse(status int, body string, headers http.Header) *http.Response {
	normalized := make(http.Header)
	for name, values := range headers {
		for _, value := range values {
			normalized.Add(name, value)
		}
	}
	return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d %s", status, http.StatusText(status)), Header: normalized, Body: io.NopCloser(strings.NewReader(body))}
}

func TestBranchProtectionUnavailableRequiresExactCompleteBoundedJSON(t *testing.T) {
	message := "Upgrade to GitHub Pro or make this repository public to enable this feature."
	for _, documentationURL := range []string{
		classicProtectionDocumentationURL,
		branchRulesDocumentationURL,
	} {
		body := fmt.Sprintf(`{"message":%q,"documentation_url":%q,"status":"403"}`, message, documentationURL)
		if !isBranchProtectionUnavailable(responseError("GitHub read", httpResponse(http.StatusForbidden, body, nil)), documentationURL) {
			t.Fatalf("exact response was rejected: %s", documentationURL)
		}
		otherDocumentationURL := classicProtectionDocumentationURL
		if documentationURL == classicProtectionDocumentationURL {
			otherDocumentationURL = branchRulesDocumentationURL
		}
		for _, response := range []*http.Response{
			{StatusCode: http.StatusForbidden, Status: "403 Forbidden", Body: io.NopCloser(&readErrorAfterBody{body: body})},
			httpResponse(http.StatusForbidden, body+strings.Repeat(" ", 4096), nil),
			httpResponse(http.StatusForbidden, strings.Replace(body, documentationURL, documentationURL+"-lookalike", 1), nil),
			httpResponse(http.StatusForbidden, strings.Replace(body, documentationURL, otherDocumentationURL, 1), nil),
			httpResponse(http.StatusForbidden, strings.Replace(body, message, "forbidden", 1), nil),
			httpResponse(http.StatusUnauthorized, body, nil),
			httpResponse(http.StatusForbidden, "not json", nil),
		} {
			if isBranchProtectionUnavailable(responseError("GitHub read", response), documentationURL) {
				t.Fatalf("non-exact response was trusted: %s", documentationURL)
			}
		}
	}
	if isBranchProtectionUnavailable(errors.New("connection reset"), branchRulesDocumentationURL) {
		t.Fatal("transport failure was trusted")
	}
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

func TestDecodeRejectsTrailingAndOversizedJSON(t *testing.T) {
	api := API{BaseURL: "https://api.example.test", HTTP: &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return httpResponse(http.StatusOK, `{"ok":true}{"extra":true}`, nil), nil
	})}}
	var result struct {
		OK bool `json:"ok"`
	}
	if _, _, err := api.Read(context.Background(), "/read", "", &result); err == nil {
		t.Fatal("trailing API JSON accepted")
	}
	if err := decodeJSON(strings.NewReader(strings.Repeat(" ", (1<<20)+1)), &result); err == nil {
		t.Fatal("over-limit JSON accepted")
	}
}

func TestAPIReadRetriesMutationDoesNotAndRedacts(t *testing.T) {
	var reads, writes atomic.Int32
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			if reads.Add(1) == 1 {
				return httpResponse(http.StatusServiceUnavailable, "transient", http.Header{"Retry-After": []string{"0"}}), nil
			}
			return httpResponse(http.StatusOK, `{"ok":true}`, http.Header{"ETag": []string{`"v1"`}}), nil
		}
		writes.Add(1)
		return httpResponse(http.StatusServiceUnavailable, `token=server-canary`, nil), nil
	})}
	api := API{BaseURL: "https://api.example.test", HTTP: client, Sleep: func(context.Context, time.Duration) error { return nil }}
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

func TestAPIReadPersistsETagAndBody(t *testing.T) {
	path := filepath.Join(t.TempDir(), "github-etag-cache.json")
	cache, err := LoadReadCache(path)
	if err != nil {
		t.Fatal(err)
	}
	api := API{BaseURL: "https://api.example.test", Cache: cache, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got := r.Header.Get("If-None-Match"); got != "" {
			t.Fatalf("first request sent ETag %q", got)
		}
		return httpResponse(http.StatusOK, `{"ok":true}`, http.Header{"ETag": []string{`"v1"`}}), nil
	})}}
	var first struct {
		OK bool `json:"ok"`
	}
	if etag, changed, err := api.Read(context.Background(), "/read?stable=true", "", &first); err != nil || !changed || !first.OK || etag != `"v1"` {
		t.Fatalf("first read: etag=%q changed=%v result=%#v err=%v", etag, changed, first, err)
	}
	if err := cache.Save(); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Lstat(path); err != nil || info.Mode().Perm() != 0o600 || !info.Mode().IsRegular() {
		t.Fatalf("cache mode: info=%v err=%v", info, err)
	}

	cache, err = LoadReadCache(path)
	if err != nil {
		t.Fatal(err)
	}
	api = API{BaseURL: "https://api.example.test", Cache: cache, HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if got := r.Header.Get("If-None-Match"); got != `"v1"` {
			t.Fatalf("restart request ETag = %q", got)
		}
		return httpResponse(http.StatusNotModified, "", nil), nil
	})}}
	var second struct {
		OK bool `json:"ok"`
	}
	if etag, changed, err := api.Read(context.Background(), "/read?stable=true", "", &second); err != nil || changed || !second.OK || etag != `"v1"` {
		t.Fatalf("restart read: etag=%q changed=%v result=%#v err=%v", etag, changed, second, err)
	}
}

func TestAPIReadSnapshotDeduplicatesUntilMutation(t *testing.T) {
	var reads atomic.Int32
	api := API{BaseURL: "https://api.example.test", HTTP: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodGet {
			reads.Add(1)
			return httpResponse(http.StatusOK, `{"ok":true}`, nil), nil
		}
		return httpResponse(http.StatusCreated, `{}`, nil), nil
	})}}.WithReadSnapshot()
	var result struct {
		OK bool `json:"ok"`
	}
	if _, _, err := api.Read(t.Context(), "/read", "", &result); err != nil {
		t.Fatal(err)
	}
	result.OK = false
	if _, _, err := api.Read(t.Context(), "/read", "", &result); err != nil || !result.OK || reads.Load() != 1 {
		t.Fatalf("snapshot read: result=%#v reads=%d err=%v", result, reads.Load(), err)
	}
	body, _ := AttributedBody(5, 1, "done")
	if err := api.Mutate(t.Context(), http.MethodPost, "/write", map[string]string{"body": body}, Mutation{Issue: 5, Attempt: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, err := api.Read(t.Context(), "/read", "", &result); err != nil || reads.Load() != 2 {
		t.Fatalf("post-mutation read: reads=%d err=%v", reads.Load(), err)
	}
}

func TestReadCacheRejectsUnsafeState(t *testing.T) {
	dir := t.TempDir()
	malformed := filepath.Join(dir, "malformed.json")
	if err := os.WriteFile(malformed, []byte(`{"version":1,"entries":{"/read":{"etag":"bad\nheader","body":{}}}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadReadCache(malformed); err == nil {
		t.Fatal("malformed cache accepted")
	}
	target, link := filepath.Join(dir, "target.json"), filepath.Join(dir, "link.json")
	if err := os.WriteFile(target, []byte(`{"version":1,"entries":{}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadReadCache(link); err == nil {
		t.Fatal("symlink cache accepted")
	}
}

func TestIssueControlsApprovalAndCredentialExclusion(t *testing.T) {
	cfg := ContractConfig{Ready: "ready", P1: "P1", P2: "P2", P3: "P3", DependencySection: "Dependencies", HumanReview: "human", AutonomousMerge: "auto"}
	body := "## Context\nfix intake\n## Acceptance Criteria\n- [ ] safe\n## Checklist\n- [ ] implement\n## Validation\ngo test\n## Dependencies\n- #3\n"
	issue := IssueInput{Number: 5, State: "open", Body: body, Labels: []string{"ready", "P1", "auto"}}
	normalized := NormalizeIssue(issue, cfg, map[int]bool{3: true})
	if !normalized.Ready || normalized.Controls.Priority != 1 || normalized.Controls.Completion != "autonomous-merge" {
		t.Fatalf("normalized %#v", normalized)
	}
	arbitrary := NormalizeIssue(IssueInput{Number: 6, State: "open", Body: "any unstructured text", Labels: []string{"ready", "P1", "auto"}}, cfg, nil)
	if arbitrary.Ready || len(arbitrary.contractBlockers) != 5 || len(arbitrary.Controls.Dependencies) != 0 {
		t.Fatalf("incomplete contract was accepted: %#v", arbitrary)
	}
	paths := IssuePaths("arbitrary text\n\n## Paths\n- `docs/coordination-a.md`\n- README.md\n- README.md\n")
	if !slices.Equal(paths, []string{"README.md", "docs/coordination-a.md"}) || IssuePaths("arbitrary text") != nil {
		t.Fatalf("issue paths=%#v", paths)
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
		t.Fatal("non-coordinator snapshot accepted")
	}
	trailing := strings.TrimSuffix(comment, "\n-->") + `{}` + "\n-->"
	if _, err := ParseSnapshotComment(trailing, 99, 99); err == nil {
		t.Fatal("snapshot trailing JSON accepted")
	}
	if update, err := AttributedBody(5, 1, "validation passed"); err != nil || !strings.Contains(update, "issue:5:attempt:1") {
		t.Fatalf("attribution %q %v", update, err)
	}
	if ok, err := AuthorizedControlActor(4, permissionStub("maintain")); err != nil || !ok {
		t.Fatalf("authorized actor: %v %v", ok, err)
	}
	if ok, err := AuthorizedControlActor(99, permissionStub("admin")); err != nil || !ok {
		t.Fatalf("same-user authorization rejected: %v %v", ok, err)
	}
	if ok, err := AuthorizedControlActor(4, permissionError{}); err == nil || ok {
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

func TestNormalizeIssueOptionalFilter(t *testing.T) {
	cfg := ContractConfig{Ready: "ready", P1: "P1", P2: "P2", P3: "P3", IssueFilter: "agent-work", DependencySection: "Dependencies"}
	body := "## Context\nreason\n## Acceptance criteria\n- works\n## Checklist\n- [ ] implement\n## Validation\ngo test ./...\n## Dependencies\nNone.\n"
	issue := IssueInput{Number: 5, State: "open", Body: body, Labels: []string{"ready", "P1"}}
	if got := NormalizeIssue(issue, cfg, nil); got.Ready || got.Controls.IssueFilter || !slices.Contains(got.Blockers, "issue filter label is missing") {
		t.Fatalf("missing filter = %#v", got)
	}
	issue.Labels = append(issue.Labels, "agent-work")
	if got := NormalizeIssue(issue, cfg, nil); !got.Ready || !got.Controls.IssueFilter {
		t.Fatalf("present filter = %#v", got)
	}
	cfg.IssueFilter = ""
	issue.Labels = issue.Labels[:2]
	if got := NormalizeIssue(issue, cfg, nil); !got.Ready || got.Controls.IssueFilter {
		t.Fatalf("unconfigured filter = %#v", got)
	}
}

func TestNormalizeIssueRequiresCompleteContract(t *testing.T) {
	cfg := ContractConfig{Ready: "ready", P1: "P1", P2: "P2", P3: "P3", DependencySection: "Dependencies"}
	complete := "## Context\n### Evidence\nreason and evidence\n## Acceptance criteria\n- observable result\n## Checklist\n### Backend\n- [ ] implement\n## Validation\ngo test ./...\n## Dependencies\nNone.\n"
	tests := []struct {
		name, body, blocker string
	}{
		{"complete", complete, ""},
		{"missing section", strings.Replace(complete, "## Validation", "## Verification", 1), "required ## Validation section is missing"},
		{"empty section", strings.Replace(complete, "### Evidence\nreason and evidence", "", 1), "required ## Context section is empty"},
		{"malformed checklist", strings.Replace(complete, "- [ ] implement", "implement", 1), "## Checklist must contain a Markdown task"},
		{"malformed dependencies", strings.Replace(complete, "None.", "to be decided", 1), "## Dependencies must contain issue references or None"},
		{"indented-code checklist", strings.Replace(complete, "### Backend\n- [ ] implement", "    - [ ] implement", 1), "required ## Checklist section is empty"},
		{"indented-code dependencies", strings.Replace(complete, "None.", "    #123", 1), "required ## Dependencies section is empty"},
		{"fenced-only checklist", strings.Replace(complete, "### Backend\n- [ ] implement", "Example:\n~~~md\n- [ ] implement\n~~~", 1), "## Checklist must contain a Markdown task"},
		{"nested fenced-only checklist", strings.Replace(complete, "### Backend\n- [ ] implement", "- Example:\n\n    ~~~md\n    - [ ] implement\n    ~~~", 1), "## Checklist must contain a Markdown task"},
		{"fenced-only dependencies", strings.Replace(complete, "None.", "Example:\n```md\n#123\n```", 1), "## Dependencies must contain issue references or None"},
		{"nested fenced-only dependencies", strings.Replace(complete, "None.", "- Example:\n\n    ```md\n    #123\n    ```", 1), "## Dependencies must contain issue references or None"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := NormalizeIssue(IssueInput{Number: 5, State: "open", Body: test.body, Labels: []string{"ready", "P1"}}, cfg, nil)
			if len(got.Controls.Dependencies) != 0 {
				t.Fatalf("contract parsed dependencies from fenced or invalid content: %#v", got)
			}
			if test.blocker == "" {
				if !got.Ready || len(got.contractBlockers) != 0 {
					t.Fatalf("complete contract = %#v", got)
				}
				return
			}
			if got.Ready || !slices.Contains(got.contractBlockers, test.blocker) {
				t.Fatalf("contract = %#v, want blocker %q", got, test.blocker)
			}
		})
	}
	allFenced := NormalizeIssue(IssueInput{Number: 5, State: "open", Body: "```md\n" + complete + "```\n", Labels: []string{"ready", "P1"}}, cfg, nil)
	if allFenced.Ready || len(allFenced.contractBlockers) != 5 || len(allFenced.Controls.Dependencies) != 0 {
		t.Fatalf("all-fenced fake contract = %#v", allFenced)
	}
	for _, marker := range []string{"```", "~~~"} {
		t.Run("nested "+marker, func(t *testing.T) {
			fake := "- Example:\n\n    " + marker + "md\n    " + strings.ReplaceAll(complete+"## Paths\n- internal/fake.go\n", "\n", "\n    ") + marker + "\n"
			got := NormalizeIssue(IssueInput{Number: 5, State: "open", Body: fake, Labels: []string{"ready", "P1"}}, cfg, nil)
			if got.Ready || len(got.contractBlockers) != 5 || len(got.Controls.Dependencies) != 0 || len(IssuePaths(fake)) != 0 {
				t.Fatalf("nested fenced fake contract = %#v, paths=%#v", got, IssuePaths(fake))
			}
		})
	}
	fakeContract := strings.Replace(complete, "None.", "#123", 1) + "## Paths\n- internal/fake.go\n"
	for _, marker := range []string{"```", "~~~"} {
		t.Run("indented pseudo-closer "+marker, func(t *testing.T) {
			fake := marker + "md\n    " + marker + "\n" + fakeContract + marker + "\n"
			got := NormalizeIssue(IssueInput{Number: 5, State: "open", Body: fake, Labels: []string{"ready", "P1"}}, cfg, map[int]bool{123: true})
			if got.Ready || len(got.contractBlockers) != 5 || len(got.Controls.Dependencies) != 0 || len(IssuePaths(fake)) != 0 {
				t.Fatalf("pseudo-closed fake contract = %#v, paths=%#v", got, IssuePaths(fake))
			}
		})
	}
	indented := "    " + strings.ReplaceAll(fakeContract, "\n", "\n    ")
	got := NormalizeIssue(IssueInput{Number: 5, State: "open", Body: indented, Labels: []string{"ready", "P1"}}, cfg, map[int]bool{123: true})
	if got.Ready || len(got.contractBlockers) != 5 || len(got.Controls.Dependencies) != 0 || len(IssuePaths(indented)) != 0 {
		t.Fatalf("indented-code fake contract = %#v, paths=%#v", got, IssuePaths(indented))
	}
	if paths := IssuePaths("## Paths\n    - internal/fake.go\n"); len(paths) != 0 {
		t.Fatalf("indented-code paths = %#v", paths)
	}
}

func TestMarkdownSectionStopsAtSameOrHigherHeading(t *testing.T) {
	body := "## Context\n### Evidence\nproof\n#### Detail\nmore proof\n##not a heading\nstill context\n## Validation\ncommands\n# Appendix\nnot validation\n"
	context, ok := markdownSection(body, "Context")
	if !ok || context != "### Evidence\nproof\n#### Detail\nmore proof\n##not a heading\nstill context" {
		t.Fatalf("context section = %q, found=%v", context, ok)
	}
	validation, ok := markdownSection(body, "Validation")
	if !ok || validation != "commands" {
		t.Fatalf("validation section = %q, found=%v", validation, ok)
	}
}

func TestAgentEnvironmentRejectsReservedExplicitNames(t *testing.T) {
	for _, name := range []string{"GITHUB_TOKEN", "GH_TOKEN", "SSH_AUTH_SOCK", "AWS_ACCESS_KEY_ID", "AZURE_TOKEN", "GOOGLE_APPLICATION_CREDENTIALS", "CLOUDFLARE_API_TOKEN", "GIT_ASKPASS", "GIT_CONFIG_COUNT", "FTP_PROXY", "APP_PEM", "MY_APP_KEY", "RANDOM_PASSWORD"} {
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

func TestAgentEnvironmentCarriesIsolatedCodexHomeButNotCoordinatorHome(t *testing.T) {
	env, err := AgentEnvironmentWith([]string{"HOME=/coordinator", "CODEX_HOME=/runtime/codex", "PATH=/bin"})
	joined := strings.Join(env, "\n")
	if err != nil || strings.Contains(joined, "HOME=/coordinator") || !strings.Contains(joined, "CODEX_HOME=/runtime/codex") {
		t.Fatalf("environment=%v err=%v", env, err)
	}
}

func TestAgentEnvironmentRejectsConfiguredHome(t *testing.T) {
	if _, err := AgentEnvironmentWith(nil, "HOME"); err == nil {
		t.Fatal("configured HOME allowlist was accepted")
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
	changedAfterApproval := slices.Clone(valid)
	changedAfterApproval[0].CreatedAt = approval.CreatedAt.Add(time.Second)
	if _, err := NewSnapshot(controls, "body", anchor, approval, changedAfterApproval, "/approve", authorized, timelineFor(changedAfterApproval)); err == nil {
		t.Fatal("approval predating current controls accepted")
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

func TestReadyLabelAuthorizesCurrentBodyWithoutApproval(t *testing.T) {
	controls := Controls{Ready: true, Priority: 1, Completion: "human-review"}
	now := time.Now().UTC()
	anchor := Anchor{IssueNodeID: "I_5", CreatedAt: now, ChangedAt: now, AuthorID: 2}
	provenance := provenanceFor(controls, 4)
	for i := range provenance {
		provenance[i].CreatedAt = now.Add(time.Second)
	}
	snapshot, err := NewSnapshot(controls, "body", anchor, Approval{}, provenance, "/approve", func(id int) bool { return id == 4 }, timelineFor(provenance))
	if err != nil || snapshot.ApprovalID != 0 || snapshot.ApprovalActor != 0 {
		t.Fatalf("label-only snapshot=%#v err=%v", snapshot, err)
	}
	anchor.ChangedAt = now.Add(2 * time.Second)
	anchor.EditID = "edit"
	if _, err := NewSnapshot(controls, "body", anchor, Approval{}, provenance, "/approve", func(id int) bool { return id == 4 }, timelineFor(provenance)); err == nil || !strings.Contains(err.Error(), "ready label") {
		t.Fatalf("stale ready label err=%v", err)
	}
	controls.Completion = "autonomous-merge"
	provenance[0].CreatedAt = now.Add(3 * time.Second)
	provenance[2].Value = "autonomous-merge"
	provenance[2].CreatedAt = now.Add(time.Second)
	if _, err := NewSnapshot(controls, "body", anchor, Approval{}, provenance, "/approve", func(id int) bool { return id == 4 }, timelineFor(provenance)); err == nil || !strings.Contains(err.Error(), "autonomous label") {
		t.Fatalf("stale autonomous label err=%v", err)
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
