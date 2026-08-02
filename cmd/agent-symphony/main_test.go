package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestWorkerBoundaryStripsCredentialCanaries(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "boundary")
	body := "#!/bin/sh\nprintf '{\"Output\":\"%s|%s|%s\",\"Code\":0,\"Exited\":false}' \"$GITHUB_TOKEN\" \"$GH_TOKEN\" \"$MODEL_TOKEN\"\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITHUB_TOKEN", "github-canary")
	t.Setenv("GH_TOKEN", "gh-canary")
	t.Setenv("MODEL_TOKEN", "model-canary")
	result, err := (workerBoundaryRunner{Command: script}).call(context.Background(), "verify", agentruntime.Command{})
	if err != nil || result.Output != "||" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestWorkerExportRejectsMaliciousOrOversizedBundleBeforeImport(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "boundary")
	export := workerExport{Type: "agent-symphony-export-v1", Repository: "o/r", Branch: "issue-4", BaseSHA: "abcdef1", HeadSHA: "abcdef2", Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "ok", Documentation: "none"}, Bundle: "not-base64", BundleSHA256: "bad"}
	b, _ := json.Marshal(export)
	result, _ := json.Marshal(agentruntime.Result{Output: string(b)})
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '"+string(result)+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, _, _, err := importWorkerExport(context.Background(), workerBoundaryRunner{Command: script}, agentruntime.Manifest{Repository: "o/r", Branch: "issue-4", BaseSHA: "abcdef1"})
	if err == nil || !strings.Contains(err.Error(), "invalid or oversized bundle") {
		t.Fatalf("err=%v", err)
	}
}

func TestBundlePreflightRejectsCompressedSmallExpandedLargeDeletedHistory(t *testing.T) {
	repo := gitRepository(t)
	runGit(t, repo, "config", "user.email", "test@example.test")
	runGit(t, repo, "config", "user.name", "Test")
	large := filepath.Join(repo, "large")
	if err := os.WriteFile(large, make([]byte, 9<<20), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "large")
	runGit(t, repo, "commit", "-m", "large history")
	if err := os.Remove(large); err != nil {
		t.Fatal(err)
	}
	runGit(t, repo, "add", "-u")
	runGit(t, repo, "commit", "-m", "delete large")
	bundlePath := filepath.Join(t.TempDir(), "history.bundle")
	runGit(t, repo, "bundle", "create", bundlePath, "HEAD")
	bundle, err := os.ReadFile(bundlePath)
	if err != nil || len(bundle) >= 1<<20 {
		t.Fatalf("compressed bundle bytes=%d err=%v", len(bundle), err)
	}
	bare := filepath.Join(t.TempDir(), "check.git")
	runGit(t, repo, "init", "--bare", bare)
	if err := preflightBundle(t.Context(), bundle, bundlePath, bare); err == nil || !strings.Contains(err.Error(), "oversized expanded object") {
		t.Fatalf("err=%v", err)
	}
}

func TestBundlePreflightRejectsManySmallObjectsBeforeBufferingOutput(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "git")
	body := "#!/bin/sh\ncase \"$*\" in\n  *index-pack*) exit 0;;\n  *verify-pack*) i=0; while [ $i -le 100000 ]; do printf '%040d blob 1 1 1\\n' $i; i=$((i+1)); done;;\n  *) exit 1;;\nesac\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	repo := filepath.Join(dir, "repo.git")
	if err := os.MkdirAll(filepath.Join(repo, "objects", "pack"), 0o700); err != nil {
		t.Fatal(err)
	}
	err := preflightBundle(t.Context(), []byte("header\nPACKdata"), filepath.Join(dir, "bundle"), repo)
	if err == nil || !strings.Contains(err.Error(), "expanded object count or bytes exceeded") {
		t.Fatalf("err=%v", err)
	}
}

func TestWorkerTreeRejectsSharedSubtreeRecursiveOutputAmplification(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "git")
	body := "#!/bin/sh\ni=0; while [ $i -le 100000 ]; do printf '100644 blob 0000000000000000000000000000000000000000 1\\tshared/file-%s\\n' \"$i\"; i=$((i+1)); done\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	err := validateWorkerTree(t.Context(), dir, strings.Repeat("0", 40))
	if err == nil || !strings.Contains(err.Error(), "tree entry count exceeded") {
		t.Fatalf("err=%v", err)
	}
}

func TestValidateJSONSuccessAndFailure(t *testing.T) {
	root := gitRepository(t)
	path := filepath.Join(root, config.DefaultPath)
	if err := config.Write(path, config.Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"validate", "--config", path, "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	var got envelope
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Version != 1 || !got.OK || got.Command != "validate" {
		t.Fatalf("unexpected envelope: %#v", got)
	}

	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"validate", "--config", path + ".missing", "--json"}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if err := json.Unmarshal(stderr.Bytes(), &got); err != nil || got.OK || got.Error == "" {
		t.Fatalf("unexpected failure envelope: %#v, %v", got, err)
	}
}

func TestQueuedIssueProjectionIsReadOnlyAndAuthoritative(t *testing.T) {
	issues := []internalgithub.RecoveryIssueFact{{Repository: "o/r", Issue: 4, Attempt: 2, Priority: 1, CreatedAt: time.Unix(1, 0), Dependencies: []int{3}, Blockers: []string{"dependency #3 is incomplete"}}}
	statuses, decisions := joinIssueProjection(nil, issues, 1)
	if len(statuses) != 1 || statuses[0].State != string(orchestrator.Blocked) || statuses[0].Priority != 1 || len(statuses[0].Dependencies) != 1 || len(statuses[0].Blockers) != 1 || statuses[0].Action == "" || len(decisions) != 1 {
		t.Fatalf("statuses=%#v decisions=%#v", statuses, decisions)
	}
}

func TestProductionHandoffOutcomeIsCompletedWithoutRedelivery(t *testing.T) {
	dir := t.TempDir()
	statePath := filepath.Join(dir, "state.json")
	state := []internalgithub.PRState{{Repository: "o/r", Number: 3, Issue: 4, Attempt: 2, HeadSHA: "abcdef0", ValidationQueuedSHA: "abcdef0"}}
	b, _ := json.Marshal(state)
	if err := os.WriteFile(statePath, b, 0o600); err != nil {
		t.Fatal(err)
	}
	recovery := &internalgithub.FileRecovery{Path: statePath}
	handoffs, err := recovery.ClaimHandoffsFor(t.Context(), map[string]bool{"o/r#4/2": true})
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("handoffs=%#v err=%v", handoffs, err)
	}
	outcome := internalgithub.HandoffOutcome{Key: handoffs[0].Key, ValidationResult: "passed", ValidationEvidence: "go test ./..."}
	record, _ := json.Marshal(struct {
		Handoff      internalgithub.RecoveryHandoff `json:"handoff"`
		Outcome      internalgithub.HandoffOutcome  `json:"outcome"`
		OutcomeToken string                         `json:"outcome_token"`
	}{handoffs[0], outcome, fmt.Sprintf("%x", sha256.Sum256([]byte("handoff-outcome\x00"+handoffs[0].Key)))})
	outcomeRoot := filepath.Join(dir, "handoff-outcomes")
	if err := os.Mkdir(outcomeRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outcomeRoot, "tampered.json"), record, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := completeHandoffOutcomes(t.Context(), recovery, outcomeRoot); err == nil {
		t.Fatal("accepted outcome from guessed/tampered destination")
	}
	if err := os.Remove(filepath.Join(outcomeRoot, "tampered.json")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outcomeRoot, handoffs[0].Key+".json"), record, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := completeHandoffOutcomes(t.Context(), recovery, outcomeRoot); err != nil {
		t.Fatal(err)
	}
	if again, err := recovery.ClaimHandoffsFor(t.Context(), map[string]bool{"o/r#4/2": true}); err != nil || len(again) != 0 {
		t.Fatalf("redelivered=%#v err=%v", again, err)
	}
}

func TestConfigViewAcceptsConventionalSubcommandFlags(t *testing.T) {
	root := gitRepository(t)
	path := filepath.Join(root, config.DefaultPath)
	if err := config.Write(path, config.Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "view", "--config", path, "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"command":"config view"`) {
		t.Fatalf("unexpected output: %s", stdout.String())
	}
}

func TestInitAndMisuseExitCodes(t *testing.T) {
	root := gitRepository(t)
	var stdout, stderr bytes.Buffer
	if code := run([]string{"init"}, &stdout, &stderr); code != 0 {
		t.Fatalf("init exit %d: %s", code, stderr.String())
	}
	if _, err := config.Load(filepath.Join(root, config.DefaultPath)); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"validate", "extra", "--json"}, &stdout, &stderr); code != 2 {
		t.Fatalf("misuse exit %d, want 2", code)
	}
	var got envelope
	if err := json.Unmarshal(stderr.Bytes(), &got); err != nil || got.OK {
		t.Fatalf("unexpected JSON misuse: %#v, %v", got, err)
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"unknown", "--json"}, &stdout, &stderr); code != 2 {
		t.Fatalf("unknown exit %d, want 2", code)
	}
	if err := json.Unmarshal(stderr.Bytes(), &got); err != nil {
		t.Fatalf("unknown command did not return JSON: %s", stderr.String())
	}
	for _, args := range [][]string{{"validate", "--bad", "--json"}, {"config", "nope", "--json"}, {"help", "extra", "--json"}} {
		stdout.Reset()
		stderr.Reset()
		if code := run(args, &stdout, &stderr); code != 2 {
			t.Fatalf("%v exit %d, want 2", args, code)
		}
		if err := json.Unmarshal(stderr.Bytes(), &got); err != nil {
			t.Fatalf("%v did not return JSON misuse: %s", args, stderr.String())
		}
	}
}

func TestConfigViewDoesNotEchoCredentialArgument(t *testing.T) {
	root := gitRepository(t)
	c := config.Default("owner/repo")
	c.Commands.Implementation = []string{"codex", "GITHUB_TOKEN=canary-value"}
	path := filepath.Join(root, config.DefaultPath)
	if err := config.Write(path, c); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"config", "view", "--config", path}, &stdout, &stderr); code != 1 {
		t.Fatalf("exit %d, want 1", code)
	}
	if strings.Contains(stdout.String()+stderr.String(), "canary-value") {
		t.Fatal("config view echoed credential value")
	}
}

func TestGitHubDiagnosticsUsesInjectedHTTPClient(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/repos/owner/repo" {
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"permissions":{"pull":true}}`)), Header: make(http.Header)}, nil
	})}
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = "https://example.invalid", client
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	got := githubDiagnostics("owner/repo")
	if len(got) != 2 || got[0].Status != "pass" || got[1].Status != "warn" {
		t.Fatalf("unexpected diagnostics: %#v", got)
	}
}

func TestPRGovernanceCommandWiresFakeGitHubAndRecoveryState(t *testing.T) {
	root := gitRepository(t)
	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, config.Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "pr-state.json")
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITHUB_TOKEN", "token-canary")
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_ID", "7")
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID", "42")
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/installation" {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"app_id":7}`)), Header: make(http.Header)}, nil
		}
		if r.URL.Path != "/repos/owner/repo/pulls" || r.Header.Get("Authorization") != "Bearer token-canary" {
			t.Fatalf("unexpected request %s auth=%q", r.URL.String(), r.Header.Get("Authorization"))
		}
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`[]`)), Header: make(http.Header)}, nil
	})}
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = "https://example.invalid", client
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	var stdout, stderr bytes.Buffer
	if code := run([]string{"pr-governance", "--config", configPath, "--state", statePath, "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit %d: %s", code, stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), "token-canary") || !strings.Contains(stdout.String(), `"command":"pr-governance"`) {
		t.Fatalf("unsafe or malformed output: %s%s", stdout.String(), stderr.String())
	}
}

func TestPRGovernanceRejectsSymlinkState(t *testing.T) {
	dir := t.TempDir()
	target, link := filepath.Join(dir, "state.json"), filepath.Join(dir, "state-link.json")
	if err := os.WriteFile(target, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"pr-governance", "--state", link}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "regular recovery file") {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
}

func TestStatusHumanNoColorAndVersionedJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempts.json")
	data := `[{"repository":"owner/repo","issue":4,"attempt":1,"base_sha":"abcdef1","state":"completed","pr":10}]`
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NO_COLOR", "1")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"status", "--attempts", path}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "COMPLETED") || strings.Contains(stdout.String(), "\x1b[") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"inspect", "--issue", "4", "--attempts", path, "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	var got envelope
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil || got.Version != 1 || got.Command != "inspect" || !got.OK {
		t.Fatalf("got %#v err=%v", got, err)
	}
}

func TestStatusRejectsUnknownFieldsAndDoesNotEchoSecret(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempts.json")
	if err := os.WriteFile(path, []byte(`[{"repository":"owner/repo","issue":4,"attempt":1,"base_sha":"abcdef1","state":"active","token":"secret-canary"}]`), 0o600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"status", "--attempts", path, "--json"}, &stdout, &stderr); code != 1 || strings.Contains(stdout.String()+stderr.String(), "secret-canary") {
		t.Fatalf("code=%d output=%q", code, stdout.String()+stderr.String())
	}
}

func TestDaemonLockIsSingleInstanceAndNoFollow(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "daemon.lock")
	first, err := acquireDaemonLock(path)
	if err != nil {
		t.Fatal(err)
	}
	defer releaseDaemonLock(first)
	if second, err := acquireDaemonLock(path); err == nil {
		releaseDaemonLock(second)
		t.Fatal("second instance acquired lock")
	}
	link := filepath.Join(dir, "link.lock")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if lock, err := acquireDaemonLock(link); err == nil {
		releaseDaemonLock(lock)
		t.Fatal("followed symlink lock")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestMountedFilesystemUsesLongestMatchingEntry(t *testing.T) {
	root := t.TempDir()
	mount := filepath.Join(root, "repo mount")
	if err := os.Mkdir(mount, 0o755); err != nil {
		t.Fatal(err)
	}
	mounts := filepath.Join(root, "mounts")
	content := "root / ext4 rw 0 0\nwindows " + strings.ReplaceAll(mount, " ", `\040`) + " 9p rw 0 0\n"
	if err := os.WriteFile(mounts, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	filesystem, err := mountedFilesystem(mount, mounts)
	if err != nil || filesystem != "9p" {
		t.Fatalf("got %q, %v; want 9p", filesystem, err)
	}
}

func TestParseGitHubRemote(t *testing.T) {
	for remote, want := range map[string]string{
		"git@github.com:owner/repo.git":       "owner/repo",
		"https://github.com/owner/repo.git":   "owner/repo",
		"ssh://git@github.com/owner/repo.git": "owner/repo",
	} {
		got, err := parseGitHubRemote(remote)
		if err != nil || got != want {
			t.Fatalf("parse %q = %q, %v", remote, got, err)
		}
	}
	if _, err := parseGitHubRemote("https://example.com/owner/repo.git"); err == nil {
		t.Fatal("accepted non-GitHub remote")
	}
}

func gitRepository(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	if out, err := exec.Command("git", "-C", root, "init", "-q").CombinedOutput(); err != nil {
		t.Fatalf("git init: %s: %v", out, err)
	}
	if out, err := exec.Command("git", "-C", root, "remote", "add", "origin", "https://github.com/owner/repo.git").CombinedOutput(); err != nil {
		t.Fatalf("git remote: %s: %v", out, err)
	}
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(root); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	return root
}

func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
	if err != nil {
		t.Fatalf("git %v: %s: %v", args, out, err)
	}
	return strings.TrimSpace(string(out))
}
