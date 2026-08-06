package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
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

func TestWriteImmutableRecoversAtEveryDurabilityBoundary(t *testing.T) {
	origCreate, origWrite, origFileSync, origInstall, origDirSync := immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync
	t.Cleanup(func() {
		immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync = origCreate, origWrite, origFileSync, origInstall, origDirSync
	})
	body := []byte("complete binding")
	for _, stage := range []string{"create", "write", "file-sync", "install", "dir-sync"} {
		t.Run(stage, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "binding")
			immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync = origCreate, origWrite, origFileSync, origInstall, origDirSync
			switch stage {
			case "create":
				immutableCreate = func(string, string) (*os.File, error) { return nil, errors.New("injected create") }
			case "write":
				immutableWrite = func(f *os.File, b []byte) error { _, _ = f.Write(b[:len(b)/2]); return errors.New("injected write") }
			case "file-sync":
				immutableFileSync = func(*os.File) error { return errors.New("injected file sync") }
			case "install":
				immutableInstall = func(string, string) error { return errors.New("injected install") }
			case "dir-sync":
				immutableDirSync = func(string) error { return errors.New("injected directory sync") }
			}
			if err := writeImmutable(path, body); err == nil {
				t.Fatal("injected failure succeeded")
			}
			if stage != "dir-sync" {
				if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("partial final exposed: %v", err)
				}
			}
			immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync = origCreate, origWrite, origFileSync, origInstall, origDirSync
			if err := writeImmutable(path, body); err != nil {
				t.Fatalf("restart recovery: %v", err)
			}
			got, _ := os.ReadFile(path)
			if !bytes.Equal(got, body) {
				t.Fatalf("final=%q", got)
			}
			if err := writeImmutable(path, []byte("different")); err == nil {
				t.Fatal("mismatched immutable body accepted")
			}
		})
	}
}

func TestReviewFindingsRecoversWorkerReceiptAfterCoordinatorRestart(t *testing.T) {
	state, worktree := t.TempDir(), t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
	head, key := "abcdef1", "independent-review-abcdef1"
	manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, State: "completed", Worktree: worktree, Session: "as-23-1", LogPath: filepath.Join(worktree, "attempt.log"), ReviewState: "findings-queued", ReviewHead: head, ReviewFindings: []string{"fix once"}, ReviewHandoffQueued: true}
	sum := sha256.Sum256([]byte(attempt.Repository))
	manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}

	ack, _ := json.Marshal(struct{ Type, Key, OutcomePath, OutcomeToken string }{"agent-symphony-handoff-executed-v1", key, manifest.LogPath + ".review-outcome", head})
	result, _ := json.Marshal(agentruntime.Result{Output: string(ack)})
	calls, script := filepath.Join(worktree, "accepts"), filepath.Join(t.TempDir(), "boundary")
	scriptBody := fmt.Sprintf("#!/bin/sh\npayload=$(sed -n '1p')\ncase \"$payload\" in *accept-handoff*) printf 'x\\n' >> %q;; esac\nprintf '%%s' '%s'\n", calls, result)
	if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
		t.Fatal(err)
	}
	boundary := workerBoundaryRunner{Command: script}

	// The worker already accepted and executed before the coordinator crashed;
	// the restarted coordinator recovers the bound receipt without rework.
	restarted := &agentruntime.Runtime{StateRoot: state, Runner: boundary, Tmux: "tmux"}
	pending, err := returnReviewFindings(t.Context(), restarted, boundary, attempt, manifest, head, manifest.ReviewFindings, []string{"implementation"})
	if err != nil || pending {
		t.Fatalf("receipt recovery pending=%v err=%v", pending, err)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	_ = json.Unmarshal(storedBody, &stored)
	if !stored.ReviewHandoffAck {
		t.Fatal("worker receipt was not acknowledged")
	}
	accepts, _ := os.ReadFile(calls)
	if strings.Count(string(accepts), "x\n") != 1 {
		t.Fatalf("accept calls=%q", accepts)
	}
}

func TestMonitorRetriesQueuedFindingsReworkAfterRestart(t *testing.T) {
	coordinator, worker := gitRepository(t), gitRepository(t)
	for _, repo := range []string{coordinator, worker} {
		runGit(t, repo, "config", "user.email", "test@example.invalid")
		runGit(t, repo, "config", "user.name", "test")
	}
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "add", "file")
	runGit(t, worker, "commit", "-m", "base")
	base := runGit(t, worker, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("head"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "commit", "-am", "head")
	head := runGit(t, worker, "rev-parse", "HEAD")
	bundlePath := filepath.Join(t.TempDir(), "attempt.bundle")
	runGit(t, worker, "bundle", "create", bundlePath, "--all")
	bundle, _ := os.ReadFile(bundlePath)
	realGit, _ := exec.LookPath("git")
	verifyRepo := filepath.Join(t.TempDir(), "verify.git")
	runGit(t, worker, "init", "--bare", verifyRepo)
	gitWrapperDir := t.TempDir()
	gitWrapper := filepath.Join(gitWrapperDir, "git")
	gitWrapperBody := fmt.Sprintf("#!/bin/sh\ncase \" $* \" in\n  *\" bundle verify \"*) for last do :; done; exec %q --git-dir=%q bundle verify \"$last\";;\nesac\nexec %q \"$@\"\n", realGit, verifyRepo, realGit)
	if err := os.WriteFile(gitWrapper, []byte(gitWrapperBody), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", gitWrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	exported := workerExport{Type: "agent-symphony-export-v1", Repository: "o/r", Branch: "issue-23", BaseSHA: base, HeadSHA: head, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "ok", Documentation: "none"}, Bundle: base64.StdEncoding.EncodeToString(bundle)}
	exportedJSON, _ := json.Marshal(exported)
	exportResult, _ := json.Marshal(agentruntime.Result{Output: string(exportedJSON)})

	state, worktree := t.TempDir(), t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
	manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Branch: "issue-23", BaseSHA: base, State: "completed", Worktree: worktree, Session: "as-23-1", LogPath: filepath.Join(worktree, "attempt.log"), ReviewState: "findings-queued", ReviewHead: head, ReviewFindings: []string{"retry me"}, ReviewHandoffQueued: true}
	sum := sha256.Sum256([]byte(attempt.Repository))
	manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	ack, _ := json.Marshal(struct{ Type, Key, OutcomePath, OutcomeToken string }{"agent-symphony-handoff-executed-v1", "independent-review-" + head, manifest.LogPath + ".review-outcome", head})
	ackResult, _ := json.Marshal(agentruntime.Result{Output: string(ack)})
	boundaryLog, script := filepath.Join(worktree, "boundary.log"), filepath.Join(t.TempDir(), "boundary")
	scriptBody := fmt.Sprintf("#!/bin/sh\npayload=$(sed -n '1p')\nprintf '%%s\\n' \"$payload\" >> %q\ncase \"$payload\" in\n  *operation*export*) printf '%%s' '%s';;\n  *) printf '%%s' '%s';;\nesac\n", boundaryLog, exportResult, ackResult)
	if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", script)
	old, _ := os.Getwd()
	if err := os.Chdir(coordinator); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	boundary := workerBoundaryRunner{Command: script}
	runtimeState := &agentruntime.Runtime{StateRoot: state, Runner: boundary, Tmux: "tmux"}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1, Eligible: true}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, runtimeState, config.Config{Commands: config.Commands{Implementation: []string{"implementation"}}}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{manifest}, nil, state); err != nil {
		log, _ := os.ReadFile(boundaryLog)
		t.Fatalf("transient rework reached GitHub failure: %v\n%s", err, log)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	_ = json.Unmarshal(storedBody, &stored)
	if !stored.ReviewHandoffAck {
		t.Fatal("monitor did not recover worker receipt")
	}
	restarted := &agentruntime.Runtime{StateRoot: state, Runner: boundary, Tmux: "tmux"}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, restarted, config.Config{Commands: config.Commands{Implementation: []string{"implementation"}}}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{stored}, nil, state); err != nil {
		t.Fatalf("retry reached GitHub failure: %v", err)
	}
	storedBody, _ = os.ReadFile(manifestPath)
	_ = json.Unmarshal(storedBody, &stored)
	log, _ := os.ReadFile(boundaryLog)
	if strings.Count(string(log), "accept-handoff") != 1 {
		t.Fatalf("handoff redelivered: %s", log)
	}
}

func TestIndependentReviewUsesReviewerBoundaryAndReadOnlySnapshot(t *testing.T) {
	source := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", source, "add", ".").CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	if out, err := exec.Command("git", "-C", source, "commit", "-m", "base").CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	head := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", source, "rev-parse", "HEAD"))))
	imported := filepath.Join(t.TempDir(), "coordinator.git")
	if out, err := exec.Command("git", "init", "--bare", imported).CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	if out, err := exec.Command("git", "-C", imported, "fetch", "--no-tags", source, head).CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	if out, _ := exec.Command("git", "-C", imported, "show-ref").Output(); len(bytes.TrimSpace(out)) != 0 {
		t.Fatalf("attested object unexpectedly advertised: %s", out)
	}
	source = imported
	script := filepath.Join(t.TempDir(), "review-boundary")
	boundaryLog := filepath.Join(t.TempDir(), "boundary.log")
	body := fmt.Sprintf("#!/bin/sh\ntee -a %q >/dev/null\nprintf '{\"Output\":\"1 0\",\"Code\":0,\"Exited\":false}'\n", boundaryLog)
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	old := reviewSnapshotRoot
	reviewSnapshotRoot = t.TempDir()
	t.Cleanup(func() { reviewSnapshotRoot = old })
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1}
	t.Setenv("OPENAI_API_KEY", "model-canary")
	t.Setenv("GITHUB_TOKEN", "github-canary")
	t.Setenv("SSH_AUTH_SOCK", "ssh-canary")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "cloud-canary")
	t.Setenv("HTTPS_PROXY", "proxy-canary")
	t.Setenv("APP_SECRET", "app-canary")
	t.Setenv("HOME", "/coordinator-home")
	env, err := configuredAgentEnvironment([]string{"OPENAI_API_KEY"})
	if err != nil {
		t.Fatal(err)
	}
	result, pending, err := runIndependentReview(t.Context(), nil, agentruntime.Attempt{Repository: issue.Repository, Issue: issue.Issue, Number: issue.Attempt}, workerBoundaryRunner{Command: script}, env, []string{"reviewer"}, issue, agentruntime.Manifest{}, source, head, reviewSnapshotRoot)
	if err != nil || !pending {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = filepath.WalkDir(result.Snapshot, func(path string, entry os.DirEntry, err error) error {
			if err == nil && entry.IsDir() {
				_ = os.Chmod(path, 0o700)
			}
			return nil
		})
		_ = os.RemoveAll(result.Snapshot)
	})
	got := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", result.Snapshot, "rev-parse", "HEAD"))))
	if got != head {
		t.Fatalf("snapshot head=%s want=%s", got, head)
	}
	log, err := os.ReadFile(boundaryLog)
	if err != nil || !strings.Contains(string(log), `"args":["send-keys","-t","=as-review-23-1:0.0","C-d"]`) {
		t.Fatalf("review stdin was not submitted like runtime stdin: %s err=%v", log, err)
	}
	if !strings.Contains(string(log), `OPENAI_API_KEY=model-canary`) || slices.ContainsFunc([]string{"github-canary", "ssh-canary", "cloud-canary", "proxy-canary", "app-canary", "/coordinator-home"}, func(secret string) bool { return strings.Contains(string(log), secret) }) {
		t.Fatalf("review boundary environment was not safely filtered: %s", log)
	}
}

func mustOutput(t *testing.T, cmd *exec.Cmd) []byte {
	t.Helper()
	out, err := cmd.Output()
	if err != nil {
		t.Fatal(err)
	}
	return out
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

func TestStructuredReviewResultIsBoundedAndFindingsBlockClean(t *testing.T) {
	clean, err := parseIndependentReview(`noise
{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`)
	if err != nil || clean.Status != "clean" {
		t.Fatalf("clean=%#v err=%v", clean, err)
	}
	findings, err := parseIndependentReview(`{"type":"agent-symphony-review-v1","status":"findings","findings":["fix boundary"]}`)
	if err != nil || len(findings.Findings) != 1 {
		t.Fatalf("findings=%#v err=%v", findings, err)
	}
	if _, err := parseIndependentReview(`{"type":"agent-symphony-review-v1","status":"clean","findings":["hidden"]}`); err == nil {
		t.Fatal("clean result carried findings")
	}
	if _, err := parseIndependentReview("{\"type\":\"agent-symphony-review-v1\",\"status\":\"findings\",\"findings\":[\"fix\"]}\n{\"type\":\"agent-symphony-review-v1\",\"status\":\"clean\",\"findings\":[]}"); err == nil {
		t.Fatal("accepted multiple structured results")
	}
}

func TestReviewCleanupSurvivesRestartUntilSessionIsGone(t *testing.T) {
	for _, test := range []struct {
		name, result string
		transient    bool
	}{
		{"kill success", `{"Code":0,"Exited":false}`, false},
		{"confirmed absence", `{"Code":1,"Exited":true}`, false},
		{"transient failure", "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, base := t.TempDir(), t.TempDir()
			oldRoot := reviewSnapshotRoot
			reviewSnapshotRoot = base
			t.Cleanup(func() { reviewSnapshotRoot = oldRoot })
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
			snapshot, session := reviewIdentity(attempt, reviewSnapshotRoot)
			if err := os.Mkdir(snapshot, 0o550); err != nil {
				t.Fatal(err)
			}
			manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, State: "completed", ReviewState: "clean", ReviewHead: "head", ReviewSnapshot: snapshot, ReviewSession: session}
			sum := sha256.Sum256([]byte(attempt.Repository))
			manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
			if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(manifest)
			if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
				t.Fatal(err)
			}
			script := filepath.Join(t.TempDir(), "boundary")
			scriptBody := "#!/bin/sh\nexit 1\n"
			if !test.transient {
				scriptBody = "#!/bin/sh\nprintf '%s' '" + test.result + "'\n"
			}
			if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
				t.Fatal(err)
			}

			runtimeState := &agentruntime.Runtime{StateRoot: state}
			_, err := cleanupReviewOutcome(t.Context(), runtimeState, attempt, workerBoundaryRunner{Command: script}, nil, manifest, reviewSnapshotRoot)
			var stored agentruntime.Manifest
			storedBody, readErr := os.ReadFile(manifestPath)
			if readErr != nil || json.Unmarshal(storedBody, &stored) != nil {
				t.Fatal(readErr)
			}
			if test.transient {
				if err == nil || stored.ReviewSession == "" {
					t.Fatalf("cleanup err=%v stored=%#v", err, stored)
				}
				if _, statErr := os.Stat(snapshot); statErr != nil {
					t.Fatalf("snapshot removed after ambiguous kill: %v", statErr)
				}
				if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '{\"Code\":0}'\n"), 0o700); err != nil {
					t.Fatal(err)
				}
				_, err = cleanupReviewOutcome(t.Context(), &agentruntime.Runtime{StateRoot: state}, attempt, workerBoundaryRunner{Command: script}, nil, stored, reviewSnapshotRoot)
			}
			if err != nil {
				t.Fatal(err)
			}
			storedBody, _ = os.ReadFile(manifestPath)
			stored = agentruntime.Manifest{}
			_ = json.Unmarshal(storedBody, &stored)
			if stored.ReviewSession != "" || stored.ReviewSnapshot != "" {
				t.Fatalf("cleanup state retained after authoritative outcome: %#v", stored)
			}
			if _, err := os.Stat(snapshot); !os.IsNotExist(err) {
				t.Fatalf("snapshot remains after cleanup: %v", err)
			}
		})
	}
}

type blockingReviewBoundary struct {
	block bool
	done  chan struct{}
	err   error
}

type countingReviewBoundary int

func (b *countingReviewBoundary) call(context.Context, string, agentruntime.Command) (agentruntime.Result, error) {
	*b++
	return agentruntime.Result{}, nil
}

type recoveringReviewBoundary struct {
	displayErr error
	started    int
}

func (b *recoveringReviewBoundary) call(_ context.Context, _ string, command agentruntime.Command) (agentruntime.Result, error) {
	if len(command.Args) > 0 && command.Args[0] == "display-message" && b.displayErr != nil {
		err := b.displayErr
		b.displayErr = nil
		return agentruntime.Result{}, err
	}
	if len(command.Args) > 0 && command.Args[0] == "new-session" {
		b.started++
	}
	return agentruntime.Result{}, nil
}

func TestRunningReviewUnobservableRebuildsWithoutDurableFailure(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
	}{
		{"absent tmux session", errors.New("tmux session absent")},
		{"transient display failure", errors.New("temporary host restart")},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, source, base := t.TempDir(), t.TempDir(), t.TempDir()
			reviewSnapshotRoot = base
			t.Cleanup(func() {
				reviewSnapshotRoot = ""
				_ = filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
					if err == nil && entry.IsDir() {
						_ = os.Chmod(path, 0o700)
					}
					return nil
				})
			})
			runGit(t, source, "init")
			runGit(t, source, "config", "user.email", "test@example.invalid")
			runGit(t, source, "config", "user.name", "test")
			runGit(t, source, "commit", "--allow-empty", "-m", "head")
			head := runGit(t, source, "rev-parse", "HEAD")
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
			snapshot, session := reviewIdentity(attempt, reviewSnapshotRoot)
			if err := os.Mkdir(snapshot, 0o700); err != nil {
				t.Fatal(err)
			}
			manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, State: "completed", ReviewState: "running", ReviewHead: head, ReviewSnapshot: snapshot, ReviewSession: session}
			sum := sha256.Sum256([]byte(attempt.Repository))
			manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
			if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(manifest)
			if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
				t.Fatal(err)
			}
			boundary := &recoveringReviewBoundary{displayErr: test.err}
			_, pending, err := runIndependentReview(t.Context(), &agentruntime.Runtime{StateRoot: state}, attempt, boundary, nil, []string{"review"}, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1}, manifest, source, head, reviewSnapshotRoot)
			if err != nil || !pending || boundary.started != 1 {
				t.Fatalf("pending=%v starts=%d err=%v", pending, boundary.started, err)
			}
			storedBody, _ := os.ReadFile(manifestPath)
			var stored agentruntime.Manifest
			_ = json.Unmarshal(storedBody, &stored)
			if stored.ReviewState != "running" || stored.ReviewHead != head || stored.Diagnostic != "" {
				t.Fatalf("review was terminalized: %#v", stored)
			}
		})
	}
}

func TestReviewCleanupRejectsForeignOutsideAndSymlinkIdentity(t *testing.T) {
	oldRoot := reviewSnapshotRoot
	reviewSnapshotRoot = t.TempDir()
	t.Cleanup(func() { reviewSnapshotRoot = oldRoot })
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
	snapshot, session := reviewIdentity(attempt, reviewSnapshotRoot)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.Mkdir(outside, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(reviewSnapshotRoot, filepath.Base(snapshot))
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct{ name, snapshot, session string }{
		{"foreign session", "", "as-review-99-1"},
		{"outside root", outside, session},
		{"symlink", link, session},
	} {
		t.Run(test.name, func(t *testing.T) {
			var boundary countingReviewBoundary
			if err := cleanupReviewResources(t.Context(), &boundary, nil, attempt, test.snapshot, test.session, reviewSnapshotRoot); err == nil {
				t.Fatal("unsafe cleanup identity accepted")
			}
			if boundary != 0 {
				t.Fatal("cleanup boundary reached before validation")
			}
			if _, err := os.Stat(outside); err != nil {
				t.Fatalf("outside path mutated: %v", err)
			}
		})
	}
}

func (b *blockingReviewBoundary) call(ctx context.Context, _ string, command agentruntime.Command) (agentruntime.Result, error) {
	if slices.Contains(command.Args, "display-message") {
		return agentruntime.Result{Output: "1 0"}, nil
	}
	if slices.Contains(command.Args, "capture-pane") {
		return agentruntime.Result{Output: `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`}, nil
	}
	if !b.block {
		return agentruntime.Result{}, nil
	}
	if b.err != nil {
		return agentruntime.Result{}, b.err
	}
	<-ctx.Done()
	close(b.done)
	return agentruntime.Result{}, ctx.Err()
}

func TestPreparingReviewCleanupRetainsStateAndRetries(t *testing.T) {
	for _, test := range []struct {
		name      string
		preparing bool
		transient bool
	}{
		{"fresh start cancellation", false, false},
		{"preparing recovery ambiguity", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			state, source, base := t.TempDir(), t.TempDir(), t.TempDir()
			reviewSnapshotRoot = base
			t.Cleanup(func() {
				reviewSnapshotRoot = ""
				_ = filepath.WalkDir(base, func(path string, entry os.DirEntry, err error) error {
					if err == nil && entry.IsDir() {
						_ = os.Chmod(path, 0o700)
					}
					return nil
				})
			})
			runGit(t, source, "init")
			runGit(t, source, "config", "user.email", "test@example.invalid")
			runGit(t, source, "config", "user.name", "test")
			if err := os.WriteFile(filepath.Join(source, "file"), []byte("content"), 0o600); err != nil {
				t.Fatal(err)
			}
			runGit(t, source, "add", "file")
			runGit(t, source, "commit", "-m", "head")
			head := runGit(t, source, "rev-parse", "HEAD")
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
			snapshot, session := filepath.Join(base, "o-r-23-1"), "as-review-23-1"
			manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, State: "completed"}
			if test.preparing {
				manifest.ReviewState, manifest.ReviewHead, manifest.ReviewSnapshot, manifest.ReviewSession = "preparing", head, snapshot, session
			}
			sum := sha256.Sum256([]byte(attempt.Repository))
			manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
			if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(manifest)
			if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(snapshot, 0o700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(snapshot, "retained")
			if err := os.WriteFile(marker, nil, 0o600); err != nil {
				t.Fatal(err)
			}

			boundary := &blockingReviewBoundary{block: true, done: make(chan struct{})}
			ctx := t.Context()
			if test.transient {
				boundary.err = errors.New("transient boundary failure")
			} else {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, 50*time.Millisecond)
				defer cancel()
			}
			_, pending, err := runIndependentReview(ctx, &agentruntime.Runtime{StateRoot: state}, attempt, boundary, nil, []string{"review"}, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1}, manifest, source, head, reviewSnapshotRoot)
			if err != nil || !pending {
				t.Fatalf("pending=%v err=%v", pending, err)
			}
			storedBody, err := os.ReadFile(manifestPath)
			if err != nil {
				t.Fatal(err)
			}
			var stored agentruntime.Manifest
			if json.Unmarshal(storedBody, &stored) != nil || stored.ReviewState != "preparing" || stored.ReviewSnapshot != snapshot || stored.ReviewSession != session {
				t.Fatalf("preparing state not retained: %#v", stored)
			}
			if _, err := os.Stat(marker); err != nil {
				t.Fatalf("snapshot removed after ambiguous cleanup: %v", err)
			}

			boundary.block, boundary.err = false, nil
			_, pending, err = runIndependentReview(t.Context(), &agentruntime.Runtime{StateRoot: state}, attempt, boundary, nil, []string{"review"}, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1}, stored, source, head, reviewSnapshotRoot)
			if err != nil || !pending {
				t.Fatalf("retry pending=%v err=%v", pending, err)
			}
			storedBody, _ = os.ReadFile(manifestPath)
			stored = agentruntime.Manifest{}
			_ = json.Unmarshal(storedBody, &stored)
			if stored.ReviewState != "running" || stored.ReviewSnapshot != snapshot || stored.ReviewSession != session {
				t.Fatalf("retry did not start reviewer: %#v", stored)
			}
			if _, err := os.Stat(marker); !os.IsNotExist(err) {
				t.Fatalf("retry did not replace retained snapshot: %v", err)
			}
		})
	}
}

func TestReviewCleanupCancellationRetainsStateAndRetries(t *testing.T) {
	state, base := t.TempDir(), t.TempDir()
	reviewSnapshotRoot = base
	t.Cleanup(func() { reviewSnapshotRoot = "" })
	snapshot := filepath.Join(base, "o-r-23-1")
	if err := os.Mkdir(snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
	manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, State: "completed", ReviewState: "running", ReviewHead: "head", ReviewSnapshot: snapshot, ReviewSession: "as-review-23-1"}
	sum := sha256.Sum256([]byte(attempt.Repository))
	manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}

	boundary := &blockingReviewBoundary{block: true, done: make(chan struct{})}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	started := time.Now()
	result, pending, err := runIndependentReview(ctx, &agentruntime.Runtime{StateRoot: state}, attempt, boundary, nil, []string{"review"}, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1}, manifest, "", "head", reviewSnapshotRoot)
	if err != nil || !pending || result.Status != "clean" || time.Since(started) > time.Second {
		t.Fatalf("result=%#v pending=%v err=%v elapsed=%v", result, pending, err, time.Since(started))
	}
	select {
	case <-boundary.done:
	default:
		t.Fatal("boundary outlived cleanup cancellation")
	}
	storedBody, err := os.ReadFile(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	var stored agentruntime.Manifest
	if err := json.Unmarshal(storedBody, &stored); err != nil {
		t.Fatal(err)
	}
	if stored.ReviewState != "clean" || stored.ReviewSession == "" || stored.ReviewSnapshot == "" {
		t.Fatalf("cleanup discarded retry state: %#v", stored)
	}
	if _, err := os.Stat(snapshot); err != nil {
		t.Fatalf("cleanup removed retained snapshot: %v", err)
	}

	boundary.block = false
	stored, err = cleanupReviewOutcome(t.Context(), &agentruntime.Runtime{StateRoot: state}, attempt, boundary, nil, stored, reviewSnapshotRoot)
	if err != nil || stored.ReviewSession != "" || stored.ReviewSnapshot != "" {
		t.Fatalf("retry stored=%#v err=%v", stored, err)
	}
	if _, err := os.Stat(snapshot); !os.IsNotExist(err) {
		t.Fatalf("retry retained snapshot: %v", err)
	}
}

func TestMonitorRetriesRetainedReviewCleanupBeforePublication(t *testing.T) {
	coordinator, worker, remote := t.TempDir(), t.TempDir(), filepath.Join(t.TempDir(), "remote.git")
	for _, repo := range []string{coordinator, worker} {
		runGit(t, repo, "init")
		runGit(t, repo, "config", "user.email", "test@example.invalid")
		runGit(t, repo, "config", "user.name", "test")
	}
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "add", "file")
	runGit(t, worker, "commit", "-m", "base")
	base := runGit(t, worker, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("head"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "commit", "-am", "head")
	head := runGit(t, worker, "rev-parse", "HEAD")
	bundlePath := filepath.Join(t.TempDir(), "attempt.bundle")
	runGit(t, worker, "bundle", "create", bundlePath, "--all")
	bundle, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	realGit, err := exec.LookPath("git")
	if err != nil {
		t.Fatal(err)
	}
	verifyRepo := filepath.Join(t.TempDir(), "verify.git")
	runGit(t, worker, "init", "--bare", verifyRepo)
	gitWrapperDir := t.TempDir()
	gitWrapper := filepath.Join(gitWrapperDir, "git")
	gitWrapperBody := fmt.Sprintf("#!/bin/sh\ncase \" $* \" in\n  *\" bundle verify \"*) for last do :; done; exec %q --git-dir=%q bundle verify \"$last\";;\nesac\nexec %q \"$@\"\n", realGit, verifyRepo, realGit)
	if err := os.WriteFile(gitWrapper, []byte(gitWrapperBody), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", gitWrapperDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	runGit(t, coordinator, "init", "--bare", remote)
	runGit(t, coordinator, "remote", "add", "origin", remote)
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(coordinator); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })

	exported := workerExport{Type: "agent-symphony-export-v1", Repository: "o/r", Branch: "issue-23", BaseSHA: base, HeadSHA: head, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "ok", Documentation: "none"}, Bundle: base64.StdEncoding.EncodeToString(bundle)}
	exportedJSON, _ := json.Marshal(exported)
	boundaryResult, _ := json.Marshal(agentruntime.Result{Output: string(exportedJSON)})
	implementation := filepath.Join(t.TempDir(), "implementation-boundary")
	if err := os.WriteFile(implementation, []byte("#!/bin/sh\nprintf '%s' '"+string(boundaryResult)+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	review := filepath.Join(t.TempDir(), "review-boundary")
	if err := os.WriteFile(review, []byte("#!/bin/sh\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", implementation)
	t.Setenv("AGENT_SYMPHONY_REVIEW_BOUNDARY", review)

	state, snapshotBase := t.TempDir(), t.TempDir()
	oldReviewRoot := reviewSnapshotRoot
	reviewSnapshotRoot = snapshotBase
	t.Cleanup(func() { reviewSnapshotRoot = oldReviewRoot })
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
	snapshot, session := reviewIdentity(attempt, reviewSnapshotRoot)
	if err := os.Mkdir(snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Branch: "issue-23", BaseSHA: base, State: "completed", ReviewState: "clean", ReviewHead: head, ReviewSnapshot: snapshot, ReviewSession: session, UpdatedAt: time.Now().UTC()}
	sum := sha256.Sum256([]byte(attempt.Repository))
	manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	runtimeState := &agentruntime.Runtime{StateRoot: state}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1, Eligible: true, BaseBranch: "main"}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, runtimeState, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{manifest}, nil, state); err != nil {
		t.Fatal(err)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	_ = json.Unmarshal(storedBody, &stored)
	if stored.ReviewSession == "" {
		t.Fatal("ambiguous cleanup discarded retry state")
	}
	if refs, _ := exec.Command("git", "-C", remote, "show-ref").Output(); len(bytes.TrimSpace(refs)) != 0 {
		t.Fatalf("publication ran before authoritative cleanup: %s", refs)
	}

	if err := os.WriteFile(review, []byte("#!/bin/sh\nprintf '{\"Code\":0}'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	err = monitorQueuedAttempts(t.Context(), internalgithub.API{}, runtimeState, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{stored}, nil, state)
	if err == nil || !strings.Contains(err.Error(), "AGENT_SYMPHONY_GITHUB_APP_ID") {
		t.Fatalf("publication did not continue after cleanup: %v", err)
	}
	storedBody, _ = os.ReadFile(manifestPath)
	stored = agentruntime.Manifest{}
	_ = json.Unmarshal(storedBody, &stored)
	if stored.ReviewSession != "" || stored.ReviewSnapshot != "" {
		t.Fatalf("authoritative cleanup retained state: %#v", stored)
	}
	if refs := runGit(t, remote, "show-ref"); !strings.Contains(refs, "refs/heads/issue-23") {
		t.Fatalf("publication did not proceed after cleanup: %s", refs)
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
	t.Setenv("GITHUB_TOKEN", "token-canary")
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_ID", "7")
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_ACTOR_ID", "42")
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path == "/installation/repositories" {
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"repositories":[{"full_name":"owner/repo"}]}`)), Header: make(http.Header)}, nil
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
	if info, err := os.Lstat(statePath); err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("initialized state info=%v err=%v", info, err)
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

func TestConcurrentOneShotReconcileRunsOneMutation(t *testing.T) {
	runtimeState := t.TempDir()
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_ID", "7")
	t.Setenv("GITHUB_TOKEN", "token")
	original := reconcileGitHubRun
	t.Cleanup(func() { reconcileGitHubRun = original })
	started, release := make(chan struct{}, 2), make(chan struct{})
	var snapshots, dispatches atomic.Int32
	reconcileGitHubRun = func(context.Context, string, string, string, bool, internalgithub.TokenSource) ([]orchestrator.RecoveryStatus, error) {
		snapshots.Add(1)
		dispatches.Add(1)
		started <- struct{}{}
		<-release
		return nil, nil
	}
	runOnce := func() int {
		return run([]string{"reconcile", "--state", filepath.Join(runtimeState, "pr.json"), "--runtime-state", runtimeState}, io.Discard, io.Discard)
	}
	first := make(chan int, 1)
	go func() { first <- runOnce() }()
	<-started
	second := make(chan int, 1)
	go func() { second <- runOnce() }()
	select {
	case <-started:
		close(release)
		<-first
		<-second
		t.Fatal("concurrent reconcile entered the snapshot and dispatch transition")
	case code := <-second:
		if code != 1 {
			close(release)
			<-first
			t.Fatalf("concurrent reconcile exit=%d, want 1", code)
		}
	case <-time.After(time.Second):
		close(release)
		<-first
		t.Fatal("concurrent reconcile did not fail fast on daemon lock")
	}
	close(release)
	if code := <-first; code != 0 || snapshots.Load() != 1 || dispatches.Load() != 1 {
		t.Fatalf("first exit=%d snapshots=%d dispatches=%d", code, snapshots.Load(), dispatches.Load())
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

func TestWSLPreflightRejectsEveryWindowsMountedPathBeforeMountProbe(t *testing.T) {
	for _, test := range []struct{ name, root, worktree, state string }{
		{"repository", "/mnt/c/repo", "/tmp/worktrees", "/tmp/state"},
		{"worktree", "/tmp/repo", "/mnt/c/worktrees", "/tmp/state"},
		{"state", "/tmp/repo", "/tmp/worktrees", "/mnt/c/state"},
	} {
		t.Run(test.name, func(t *testing.T) {
			if err := validateWSLFilesystem(test.root, test.worktree, test.state, filepath.Join(t.TempDir(), "missing-mounts")); err == nil || !strings.Contains(err.Error(), "Move all three paths") && !strings.Contains(err.Error(), "move all three paths") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestServeRejectsWSLStateBeforeCreatingDaemonLock(t *testing.T) {
	root := gitRepository(t)
	configPath := filepath.Join(root, ".agent-symphony.yaml")
	if err := config.Write(configPath, config.Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	oldWSL := runningOnWSL
	runningOnWSL = func() bool { return true }
	t.Cleanup(func() { runningOnWSL = oldWSL })
	state := fmt.Sprintf("/mnt/agent-symphony-preflight-%d", time.Now().UnixNano())
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve", "--config", configPath, "--state", filepath.Join(root, "state.json"), "--runtime-state", state}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "move all three paths") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Lstat(state); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("serve mutated rejected runtime state: %v", err)
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

func TestGithubTokenSourceSelectsAppJWTOrStaticToken(t *testing.T) {
	t.Run("neither configured requires GITHUB_TOKEN", func(t *testing.T) {
		if _, err := githubTokenSource(42); err == nil {
			t.Fatal("expected error without GITHUB_TOKEN or App credentials")
		}
	})
	t.Run("static GITHUB_TOKEN", func(t *testing.T) {
		t.Setenv("GITHUB_TOKEN", "static-canary")
		tokens, err := githubTokenSource(42)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := tokens.(environmentToken); !ok {
			t.Fatalf("expected environmentToken, got %T", tokens)
		}
	})
	t.Run("only private key path set is an error", func(t *testing.T) {
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH", "/nonexistent.pem")
		if _, err := githubTokenSource(42); err == nil {
			t.Fatal("expected error with only the private key path set")
		}
	})
	t.Run("only installation ID set is an error", func(t *testing.T) {
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID", "7")
		if _, err := githubTokenSource(42); err == nil {
			t.Fatal("expected error with only the installation ID set")
		}
	})
	t.Run("non-numeric installation ID is an error", func(t *testing.T) {
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH", "/nonexistent.pem")
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID", "not-a-number")
		if _, err := githubTokenSource(42); err == nil {
			t.Fatal("expected error with non-numeric installation ID")
		}
	})
	t.Run("unreadable private key path is an error", func(t *testing.T) {
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH", filepath.Join(t.TempDir(), "missing.pem"))
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID", "7")
		if _, err := githubTokenSource(42); err == nil {
			t.Fatal("expected error for unreadable private key")
		}
	})
	t.Run("valid PEM and installation ID build an auto-refreshing source", func(t *testing.T) {
		key, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			t.Fatal(err)
		}
		pemPath := filepath.Join(t.TempDir(), "app.pem")
		pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
		if err := os.WriteFile(pemPath, pemBytes, 0o600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH", pemPath)
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID", "7")
		tokens, err := githubTokenSource(42)
		if err != nil {
			t.Fatal(err)
		}
		installTokens, ok := tokens.(*internalgithub.InstallationTokens)
		if !ok {
			t.Fatalf("expected *internalgithub.InstallationTokens, got %T", tokens)
		}
		if installTokens.InstallationID != 7 {
			t.Fatalf("installation ID = %d, want 7", installTokens.InstallationID)
		}
		jwtSource, ok := installTokens.JWTs.(internalgithub.AppJWT)
		if !ok || jwtSource.AppID != "42" || jwtSource.Key.N.Cmp(key.N) != 0 {
			t.Fatalf("unexpected JWT source %#v", installTokens.JWTs)
		}
	})
}

func TestGithubTokenSourceEndToEndVerificationUsesSupportedEndpoint(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	pemPath := filepath.Join(t.TempDir(), "app.pem")
	pemBytes := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
	if err := os.WriteFile(pemPath, pemBytes, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_PRIVATE_KEY_PATH", pemPath)
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID", "7")

	exchanges, verifications := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/app/installations/7/access_tokens":
			if auth := r.Header.Get("Authorization"); !strings.HasPrefix(auth, "Bearer ey") {
				t.Errorf("missing signed App JWT: %s", auth)
			}
			exchanges++
			w.WriteHeader(http.StatusCreated)
			fmt.Fprintf(w, `{"token":"minted-canary-%d","expires_at":%q}`, exchanges, time.Now().Add(time.Hour).Format(time.RFC3339))
		case r.Method == http.MethodGet && r.URL.Path == "/installation/repositories":
			if auth := r.Header.Get("Authorization"); auth != "Bearer minted-canary-1" {
				t.Errorf("installation verification auth = %q", auth)
			}
			verifications++
			fmt.Fprint(w, `{"repositories":[{"full_name":"owner/repo"}]}`)
		default:
			t.Errorf("unexpected request %s %s", r.Method, r.URL.Path)
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = server.URL, server.Client()
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })

	tokens, err := githubTokenSource(42)
	if err != nil {
		t.Fatal(err)
	}
	api := internalgithub.API{BaseURL: githubAPI, Tokens: tokens, HTTP: githubClient}
	if err := api.VerifyInstallation(context.Background(), 42, "owner/repo"); err != nil {
		t.Fatal(err)
	}
	if err := api.VerifyInstallation(context.Background(), 42, "owner/repo"); err != nil {
		t.Fatal(err)
	}
	if exchanges != 1 {
		t.Fatalf("token exchanges = %d, want 1", exchanges)
	}
	if verifications != 2 {
		t.Fatalf("installation verifications = %d, want 2", verifications)
	}
}

func TestStartWebhookListenerValidatesConfig(t *testing.T) {
	t.Run("neither set is a no-op", func(t *testing.T) {
		wake, shutdown, err := startWebhookListener(context.Background(), internalgithub.API{}, "o/r", io.Discard)
		if err != nil || wake != nil || shutdown == nil {
			t.Fatalf("wake=%v shutdownIsNil=%v err=%v", wake, shutdown == nil, err)
		}
		if err := shutdown(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("only addr set is an error", func(t *testing.T) {
		t.Setenv("AGENT_SYMPHONY_WEBHOOK_ADDR", "127.0.0.1:0")
		if _, _, err := startWebhookListener(context.Background(), internalgithub.API{}, "o/r", io.Discard); err == nil {
			t.Fatal("expected error with only addr set")
		}
	})
	t.Run("only secret set is an error", func(t *testing.T) {
		t.Setenv("AGENT_SYMPHONY_WEBHOOK_SECRET", "secret-canary")
		if _, _, err := startWebhookListener(context.Background(), internalgithub.API{}, "o/r", io.Discard); err == nil {
			t.Fatal("expected error with only secret set")
		}
	})
	t.Run("missing installation ID is an error", func(t *testing.T) {
		t.Setenv("AGENT_SYMPHONY_WEBHOOK_ADDR", "127.0.0.1:0")
		t.Setenv("AGENT_SYMPHONY_WEBHOOK_SECRET", "secret-canary")
		if _, _, err := startWebhookListener(context.Background(), internalgithub.API{}, "o/r", io.Discard); err == nil {
			t.Fatal("expected error without installation ID")
		}
	})
	t.Run("unbindable address is an error", func(t *testing.T) {
		reserve, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer reserve.Close()
		t.Setenv("AGENT_SYMPHONY_WEBHOOK_ADDR", reserve.Addr().String())
		t.Setenv("AGENT_SYMPHONY_WEBHOOK_SECRET", "secret-canary")
		t.Setenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID", "7")
		if _, _, err := startWebhookListener(context.Background(), internalgithub.API{}, "o/r", io.Discard); err == nil {
			t.Fatal("expected error binding an already-held address")
		}
	})
}

func TestStartWebhookListenerDeliversWakeOnValidSignedWebhook(t *testing.T) {
	reserve, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := reserve.Addr().String()
	reserve.Close()

	fakeGitHub := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/repos/o/r" {
			t.Errorf("unexpected repository lookup path %s", r.URL.Path)
		}
		fmt.Fprint(w, `{"id":9999}`)
	}))
	defer fakeGitHub.Close()

	t.Setenv("AGENT_SYMPHONY_WEBHOOK_ADDR", addr)
	t.Setenv("AGENT_SYMPHONY_WEBHOOK_SECRET", "secret-canary")
	t.Setenv("AGENT_SYMPHONY_GITHUB_APP_INSTALLATION_ID", "7")

	api := internalgithub.API{BaseURL: fakeGitHub.URL, Tokens: environmentToken("test-token"), HTTP: fakeGitHub.Client()}
	wake, shutdown, err := startWebhookListener(context.Background(), api, "o/r", io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if wake == nil {
		t.Fatal("expected a non-nil wake channel")
	}
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := shutdown(shutdownCtx); err != nil {
			t.Error(err)
		}
	})

	body := []byte(`{"installation":{"id":7},"repository":{"id":9999},"issue":{"number":42}}`)
	mac := hmac.New(sha256.New, []byte("secret-canary"))
	mac.Write(body)
	signature := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-GitHub-Event", "issues")
	req.Header.Set("X-GitHub-Delivery", "delivery-1")
	req.Header.Set("X-Hub-Signature-256", signature)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("webhook POST status = %d, want 202", resp.StatusCode)
	}

	select {
	case <-wake:
	case <-time.After(2 * time.Second):
		t.Fatal("wake channel never received a signal from the valid webhook")
	}

	// A second delivery with a bad signature must be rejected and must not wake again.
	badReq, _ := http.NewRequest(http.MethodPost, "http://"+addr+"/", bytes.NewReader(body))
	badReq.Header.Set("Content-Type", "application/json")
	badReq.Header.Set("X-GitHub-Event", "issues")
	badReq.Header.Set("X-GitHub-Delivery", "delivery-2")
	badReq.Header.Set("X-Hub-Signature-256", "sha256=0000000000000000000000000000000000000000000000000000000000000000")
	badResp, err := http.DefaultClient.Do(badReq)
	if err != nil {
		t.Fatal(err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad signature status = %d, want 401", badResp.StatusCode)
	}
	select {
	case <-wake:
		t.Fatal("wake fired for an invalid signature")
	case <-time.After(100 * time.Millisecond):
	}
}
