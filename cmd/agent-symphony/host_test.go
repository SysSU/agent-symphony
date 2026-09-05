package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type agentHostRuntimeRunner struct{}

func (agentHostRuntimeRunner) Run(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: command.Name, Args: command.Args, Env: command.Env, Dir: command.Dir}})
	var output bytes.Buffer
	if err := agentHost(ctx, "implementation", bytes.NewReader(payload), &output); err != nil {
		return agentruntime.Result{}, err
	}
	var result agentruntime.Result
	if err := json.Unmarshal(output.Bytes(), &result); err != nil {
		return agentruntime.Result{}, err
	}
	if result.Code != 0 {
		return result, fmt.Errorf("agent-host command exited %d", result.Code)
	}
	return result, nil
}

func TestHostOrchestratorProposalWritesOnlyTheFixedValidatedArtifact(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "orchestrator-test")
	if err := os.Mkdir(workspace, 0o750); err != nil {
		t.Fatal(err)
	}
	proposalPath := filepath.Join(workspace, orchestratoragent.MessageProposalFile)
	if err := os.WriteFile(proposalPath, nil, 0o620); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(proposalPath, 0o620); err != nil {
		t.Fatal(err)
	}
	oldGetwd := hostGetwd
	hostGetwd = func() (string, error) { return workspace, nil }
	t.Cleanup(func() { hostGetwd = oldGetwd })
	proposal := `{"version":1,"repository":"o/r","issue":131,"attempt":3,"message":"Run the focused test."}`
	var output bytes.Buffer
	if err := writeHostOrchestratorProposal(root, strings.NewReader(proposal), &output); err != nil {
		t.Fatal(err)
	}
	written, err := os.ReadFile(proposalPath)
	if err != nil || strings.TrimSpace(string(written)) != proposal || !strings.Contains(output.String(), `"state":"submitted"`) {
		t.Fatalf("output=%q artifact=%q err=%v", output.String(), written, err)
	}
	output.Reset()
	if err := writeHostOrchestratorProposal(root, strings.NewReader(`{"version":1,"repository":"o/r","issue":131,"attempt":3,"message":"changed","command":"tmux"}`), &output); err == nil {
		t.Fatal("arbitrary proposal field accepted")
	}
	if output.Len() != 0 {
		t.Fatalf("invalid proposal emitted output: %q", output.String())
	}
	output.Reset()
	if err := reportHostOrchestratorProposalStatus(root, strings.NewReader(proposal), &output); err != nil || !strings.Contains(output.String(), `"state":"unknown"`) {
		t.Fatalf("uncaptured proposal status=%q err=%v", output.String(), err)
	}
	if entries, err := os.ReadDir(workspace); err != nil || len(entries) != 1 || entries[0].Name() != orchestratoragent.MessageProposalFile {
		t.Fatalf("read-only status changed workspace: entries=%#v err=%v", entries, err)
	}
}

func TestHostTransitionRetryProposalReportsCoordinatorResolution(t *testing.T) {
	root := t.TempDir()
	workspace := filepath.Join(root, "orchestrator-test")
	if err := os.Mkdir(workspace, 0o750); err != nil {
		t.Fatal(err)
	}
	oldGetwd := hostGetwd
	hostGetwd = func() (string, error) { return workspace, nil }
	t.Cleanup(func() { hostGetwd = oldGetwd })
	submitted := `{"version":1,"repository":"o/r","issue":161,"attempt":1,"action":"retry_transition","request_id":"retry-161-1"}`
	proposal, _, err := parseHostOrchestratorProposal(strings.NewReader(submitted))
	if err != nil || proposal.Action != orchestratoragent.ProposalActionRetry {
		t.Fatalf("proposal=%#v err=%v", proposal, err)
	}
	status, _ := json.Marshal(orchestratoragent.MessageProposalStatus{Version: 1, UpdatedAt: time.Now().UTC(), ConsumedBinding: proposal.Binding, ResolvedBinding: proposal.Binding, Resolution: "refused", Detail: "target is stale"})
	if err := os.WriteFile(filepath.Join(workspace, orchestratoragent.MessageProposalStatusFile), status, 0o440); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err := reportHostOrchestratorProposalStatus(root, strings.NewReader(submitted), &output); err != nil || !strings.Contains(output.String(), `"state":"refused"`) || !strings.Contains(output.String(), `"detail":"target is stale"`) {
		t.Fatalf("resolved proposal status=%q err=%v", output.String(), err)
	}
}

func TestHostAttentionActionsRequireExactBoundedHandoff(t *testing.T) {
	handoff := strings.Repeat("a", 64)
	for _, submitted := range []string{
		`{"version":1,"repository":"o/r","issue":187,"attempt":1,"action":"recover_attempt","request_id":"recover-187-1","handoff_id":"` + handoff + `"}`,
		`{"version":1,"repository":"o/r","issue":187,"attempt":1,"action":"human_attention","request_id":"attention-187-1","handoff_id":"` + handoff + `","detail":"operator repair is required"}`,
	} {
		proposal, _, err := parseHostOrchestratorProposal(strings.NewReader(submitted))
		if err != nil || proposal.HandoffID != handoff {
			t.Fatalf("proposal=%#v err=%v", proposal, err)
		}
	}
	for _, submitted := range []string{
		`{"version":1,"repository":"o/r","issue":187,"attempt":1,"action":"recover_attempt","request_id":"recover-187-1"}`,
		`{"version":1,"repository":"o/r","issue":187,"attempt":1,"action":"human_attention","request_id":"attention-187-1","handoff_id":"` + handoff + `"}`,
		`{"version":1,"repository":"o/r","issue":187,"attempt":1,"action":"shell","request_id":"unsafe","handoff_id":"` + handoff + `"}`,
	} {
		if _, _, err := parseHostOrchestratorProposal(strings.NewReader(submitted)); err == nil {
			t.Fatalf("unsafe proposal accepted: %s", submitted)
		}
	}
}

func fakeHostIdentity(t *testing.T, uid, gid int) {
	t.Helper()
	oldEUID, oldEGID, oldUser, oldGroup, oldOutput := hostEUID, hostEGID, hostLookupUser, hostLookupGroup, hostOutput
	hostEUID, hostEGID = func() int { return uid }, func() int { return gid }
	hostLookupUser = func(name string) (*user.User, error) {
		return &user.User{Username: name, Uid: "1234", Gid: "5678", HomeDir: "/var/lib/" + name}, nil
	}
	hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: "5678"}, nil }
	hostOutput = func(name string, args ...string) ([]byte, error) {
		if name == "getent" {
			if args[0] == "group" {
				return []byte(args[len(args)-1] + ":x:5678:\n"), nil
			}
			account := args[len(args)-1]
			return []byte(account + ":x:1234:5678::/var/lib/" + account + ":/usr/sbin/nologin\n"), nil
		}
		return []byte("5678\n"), nil
	}
	t.Cleanup(func() {
		hostEUID, hostEGID, hostLookupUser, hostLookupGroup, hostOutput = oldEUID, oldEGID, oldUser, oldGroup, oldOutput
	})
}

func TestAgentHostRunsBoundedCommandWithFilteredEnvironment(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldGOOS, oldRoot, oldExec := hostGOOS, hostRoot, hostExecRunner
	hostGOOS = "linux"
	hostRoot = t.TempDir()
	t.Cleanup(func() { hostGOOS, hostRoot, hostExecRunner = oldGOOS, oldRoot, oldExec })
	var launched agentruntime.Command
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		launched = command
		return agentruntime.Result{Output: "model-canary github-canary"}, nil
	}
	for mode, spec := range map[string]struct{ root, home string }{
		"implementation": {"/var/lib/agent-symphony/attempts", "/var/lib/agent-symphony-worker"},
		"review":         {"/var/lib/agent-symphony/snapshots", "/var/lib/agent-symphony-reviewer"},
	} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("GH_TOKEN", "github-canary")
			dir := filepath.Join(nativeRoot(spec.root), "fake-test")
			if err := os.MkdirAll(dir, 0o700); err != nil {
				t.Skipf("cannot create fake boundary root: %v", err)
			}
			payload, _ := json.Marshal(struct {
				Operation string          `json:"operation"`
				Command   boundaryCommand `json:"command"`
			}{"run", boundaryCommand{Name: "git", Args: []string{"-C", dir, "rev-parse", "HEAD"}, Dir: dir, Env: []string{"MODEL_API_KEY=model-canary", "GH_TOKEN=github-canary", "PATH=/bin"}}})
			var out bytes.Buffer
			if err := agentHost(t.Context(), mode, bytes.NewReader(payload), &out); err != nil {
				t.Fatal(err)
			}
			var result agentruntime.Result
			if err := json.Unmarshal(out.Bytes(), &result); err != nil || strings.Contains(result.Output, "model-canary") || strings.Contains(result.Output, "github-canary") || !slices.Contains(launched.Env, "MODEL_API_KEY=model-canary") || !slices.Contains(launched.Env, "GH_TOKEN=github-canary") || !slices.Contains(launched.Env, "HOME="+spec.home) || slices.Contains(launched.Env, "HOME="+os.Getenv("HOME")) {
				t.Fatal("host boundary did not deliver and redact its filtered credential environment")
			}
		})
	}
}

func TestAgentHostRedactsCredentialFromReturnedBoundaryError(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldGOOS, oldRoot, oldExec := hostGOOS, hostRoot, hostExecRunner
	hostGOOS, hostRoot = "linux", t.TempDir()
	t.Cleanup(func() { hostGOOS, hostRoot, hostExecRunner = oldGOOS, oldRoot, oldExec })
	canary := "host-error-auth-canary"
	hostExecRunner = func(context.Context, agentruntime.Command) (agentruntime.Result, error) {
		return agentruntime.Result{}, errors.New("boundary failure " + canary)
	}
	dir := filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "fake-test")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "git", Args: []string{"-C", dir, "rev-parse", "HEAD"}, Dir: dir, Env: []string{"GH_TOKEN=" + canary, "PATH=/bin"}}})
	err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &bytes.Buffer{})
	if err == nil || strings.Contains(err.Error(), canary) || !strings.Contains(err.Error(), "boundary failure") {
		t.Fatal("host boundary returned an unredacted or unclear error")
	}
}

func TestAgentHostAllowsWorkerRuntimeHistoryLimitCommand(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldGOOS, oldRoot, oldExec := hostGOOS, hostRoot, hostExecRunner
	hostGOOS, hostRoot = "linux", t.TempDir()
	t.Cleanup(func() { hostGOOS, hostRoot, hostExecRunner = oldGOOS, oldRoot, oldExec })
	root := nativeRoot("/var/lib/agent-symphony/attempts")
	if err := os.MkdirAll(root, 0o750); err != nil {
		t.Fatal(err)
	}
	want := []string{"set-option", "-w", "-t", "=as-o-r-131-3:0.0", "history-limit", "5000"}
	var launched agentruntime.Command
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		launched = command
		return agentruntime.Result{}, nil
	}
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "tmux", Args: want, Dir: root, Env: []string{"PATH=/bin"}}})
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if launched.Name != "tmux" || !slices.Equal(launched.Args, want) || launched.Dir != "/tmp" {
		t.Fatalf("runtime command changed at boundary: %#v", launched)
	}
	canary := "host-boundary-auth-canary"
	t.Setenv("TMUX_TMPDIR", "/tmp/denied-tmux-canary")
	t.Setenv("UNRELATED_SECRET", "denied-secret-canary")
	sessionEnv := []string{"PATH=/bin", "GH_TOKEN=" + canary}
	newSession := agentruntime.TmuxNewSessionArgs("as-o-r-131-3", root, sessionEnv)
	payload, _ = json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "tmux", Args: newSession, Dir: root, Env: sessionEnv}})
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(launched.Args, "HOME=/var/lib/agent-symphony-worker") || launched.Dir != "/tmp" || !slices.Contains(launched.Env, "GH_TOKEN="+canary) || strings.Contains(strings.Join(launched.Args, " "), canary) || slices.ContainsFunc(launched.Env, func(value string) bool {
		return strings.HasPrefix(value, "TMUX_TMPDIR=") || strings.HasPrefix(value, "UNRELATED_SECRET=")
	}) {
		t.Fatal("new tmux session did not receive a safely transported worker environment")
	}
	legacy := []string{"new-session", "-d", "-s", "as-o-r-131-3", "-c", root, "-e", "GH_TOKEN=" + canary}
	payload, _ = json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "tmux", Args: legacy, Dir: root, Env: sessionEnv}})
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &bytes.Buffer{}); err == nil {
		t.Fatal("credential-bearing tmux argv crossed the host boundary")
	}
	want[5] = "5001"
	payload, _ = json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "tmux", Args: want, Dir: root, Env: []string{"PATH=/bin"}}})
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &bytes.Buffer{}); err == nil {
		t.Fatal("unapproved history limit crossed the boundary")
	}
}

func TestWorkerBoundaryAllowsOnlyExactAncestryCheck(t *testing.T) {
	root := t.TempDir()
	worktree := filepath.Join(root, "attempt")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	base, head := strings.Repeat("a", 40), strings.Repeat("b", 40)
	command := func(args ...string) boundaryCommand {
		return boundaryCommand{Name: "git", Args: append([]string{"-C", worktree}, args...)}
	}
	if err := validateBoundaryCommand(command("merge-base", "--is-ancestor", base, head), root); err != nil {
		t.Fatalf("exact ancestry check rejected: %v", err)
	}
	for name, args := range map[string][]string{
		"short object": {"merge-base", "--is-ancestor", "abcdef1", head},
		"symbolic ref": {"merge-base", "--is-ancestor", base, "HEAD"},
		"wrong mode":   {"merge-base", "--octopus", base, head},
		"extra arg":    {"merge-base", "--is-ancestor", base, head, "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateBoundaryCommand(command(args...), root); err == nil {
				t.Fatal("broader ancestry command accepted")
			}
		})
	}
}

func TestReviewerBoundaryAllowsOnlyExactOrchestratorTmuxLaunch(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldGOOS, oldRoot, oldExec := hostGOOS, hostRoot, hostExecRunner
	hostGOOS, hostRoot = "linux", t.TempDir()
	t.Cleanup(func() { hostGOOS, hostRoot, hostExecRunner = oldGOOS, oldRoot, oldExec })
	root := nativeRoot("/var/lib/agent-symphony/snapshots")
	dir := filepath.Join(root, "orchestrator-owner-repo")
	if err := os.MkdirAll(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	var launched agentruntime.Command
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		launched = command
		return agentruntime.Result{}, nil
	}
	request := func(args, env []string) error {
		payload, _ := json.Marshal(struct {
			Operation string          `json:"operation"`
			Command   boundaryCommand `json:"command"`
		}{"run", boundaryCommand{Name: "tmux", Args: args, Dir: dir, Env: env}})
		return agentHost(t.Context(), "review", bytes.NewReader(payload), &bytes.Buffer{})
	}
	respawn := func(target string) []string {
		return []string{"respawn-pane", "-k", "-t", target, "--", "operator-agent", "sanitized context"}
	}
	if err := request(respawn("=as-o-owner-repo:0.0"), []string{"MODEL_API_KEY=model-canary", "PATH=/bin"}); err != nil {
		t.Fatal(err)
	}
	joined := strings.Join(launched.Env, "|")
	if !strings.Contains(joined, "HOME=/var/lib/agent-symphony-reviewer") {
		t.Fatalf("unsafe orchestrator environment: %s", joined)
	}
	for _, args := range [][]string{
		{"split-window", "-d", "-t", "=as-o-owner-repo:0.0", "-c", dir, "--", "operator-agent", "sanitized context"},
		{"kill-pane", "-t", "=as-o-owner-repo:0.0"},
	} {
		if err := request(args, []string{"PATH=/bin"}); err != nil {
			t.Fatalf("exact pane replacement rejected: %v", err)
		}
	}
	for _, target := range []string{"as-o-owner-repo:0.0", "=as-o-owner-repo:1.0", "=../../foreign:0.0"} {
		if err := request(respawn(target), []string{"PATH=/bin"}); err == nil {
			t.Fatalf("unsafe orchestrator target accepted: %q", target)
		}
	}
	if err := request([]string{"split-window", "-d", "-t", "=as-o-owner-repo:0.0", "-c", filepath.Dir(root), "--", "operator-agent"}, []string{"PATH=/bin"}); err == nil {
		t.Fatal("orchestrator pane replacement escaped the snapshot root")
	}
	if err := request(respawn("=as-o-owner-repo:0.0"), []string{"GH_TOKEN=secret"}); err != nil {
		t.Fatal("orchestrator launch rejected GitHub CLI authentication")
	}
	if err := request(respawn("=as-o-owner-repo:0.0"), []string{"SSH_AUTH_SOCK=/tmp/agent"}); err == nil {
		t.Fatal("orchestrator launch accepted SSH credentials")
	}
}

func TestHostOrchestratorLaunchContractIsReadOnlyAndCredentialFiltered(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "orchestrator-owner-repo")
	if err := os.Mkdir(dir, 0o750); err != nil {
		t.Fatal(err)
	}
	launch, _ := json.Marshal(struct {
		Version int      `json:"version"`
		Command []string `json:"command"`
		Context string   `json:"context"`
	}{1, []string{"operator-agent", "--read-only"}, "sanitized context"})
	path := filepath.Join(dir, orchestratorLaunchFile)
	if err := os.WriteFile(path, launch, 0o440); err != nil {
		t.Fatal(err)
	}
	oldGetwd, oldRun, oldEGID := hostGetwd, hostOrchestratorRun, hostEGID
	hostGetwd = func() (string, error) { return dir, nil }
	t.Cleanup(func() { hostGetwd, hostOrchestratorRun, hostEGID = oldGetwd, oldRun, oldEGID })
	t.Setenv("GH_TOKEN", "github-canary")
	var got agentruntime.Command
	hostOrchestratorRun = func(_ context.Context, command agentruntime.Command) error { got = command; return nil }
	if err := runHostOrchestrator(t.Context(), root, "/reviewer-home", false); err != nil {
		t.Fatal(err)
	}
	if got.Name != "operator-agent" || !slices.Equal(got.Args, []string{"--read-only", "sanitized context"}) || !slices.Contains(got.Env, "HOME=/reviewer-home") || !slices.Contains(got.Env, "GH_TOKEN=github-canary") {
		t.Fatalf("unsafe launch: %#v", got)
	}
	oneShot, _ := json.Marshal(struct {
		Version int      `json:"version"`
		Command []string `json:"command"`
		Context string   `json:"context"`
		OneShot bool     `json:"one_shot"`
		Timeout int      `json:"timeout_seconds"`
	}{1, []string{"operator-agent", "--read-only", "-"}, "audit prompt", true, 120})
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, oneShot, 0o440); err != nil {
		t.Fatal(err)
	}
	var stdin string
	hostOrchestratorRun = func(_ context.Context, command agentruntime.Command) error {
		got = command
		body, _ := io.ReadAll(command.Stdin)
		stdin = string(body)
		return nil
	}
	if err := runHostOrchestrator(t.Context(), root, "/reviewer-home", false); err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(got.Args, []string{"--read-only", "-"}) || stdin != "audit prompt" {
		t.Fatalf("one-shot launch args=%q stdin=%q", got.Args, stdin)
	}
	hostEGID = func() int { return oldEGID() + 1 }
	if err := runHostOrchestrator(t.Context(), root, "/reviewer-home", false); err == nil {
		t.Fatal("launch contract outside the reviewer snapshot group was accepted")
	}
	hostEGID = oldEGID
	if err := os.Chmod(path, 0o660); err != nil {
		t.Fatal(err)
	}
	if err := runHostOrchestrator(t.Context(), root, "/reviewer-home", false); err == nil {
		t.Fatal("writable launch contract accepted")
	}
}

func TestReviewResultArtifactFailsClosed(t *testing.T) {
	const valid = `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`
	request := reviewResultRequest{Repository: "o/r", Issue: 23, Attempt: 1, Mode: agentruntime.ReviewModeImplementation, Target: strings.Repeat("b", 40) + ".." + strings.Repeat("a", 40), Head: strings.Repeat("a", 40)}
	requestBody, _ := json.Marshal(request)

	for _, test := range []struct {
		name  string
		setup func(*testing.T, string, string)
		valid bool
	}{
		{"valid", func(t *testing.T, path, _ string) { mustWriteFile(t, path, valid) }, true},
		{"missing", func(*testing.T, string, string) {}, false},
		{"malformed", func(t *testing.T, path, _ string) { mustWriteFile(t, path, `{`) }, false},
		{"unknown field", func(t *testing.T, path, _ string) {
			mustWriteFile(t, path, `{"type":"agent-symphony-review-v1","status":"clean","findings":[],"unknown":true}`)
		}, false},
		{"oversized", func(t *testing.T, path, _ string) { mustWriteFile(t, path, strings.Repeat("x", maxReviewResultSize+1)) }, false},
		{"symlink", func(t *testing.T, path, root string) {
			target := filepath.Join(root, "external-result")
			mustWriteFile(t, target, valid)
			if err := os.MkdirAll(filepath.Dir(path), 0o770); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(target, path); err != nil {
				t.Fatal(err)
			}
		}, false},
		{"stale head", func(t *testing.T, _ string, root string) {
			snapshot, _ := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, root)
			mustWriteFile(t, reviewResultPath(snapshot, strings.Repeat("b", 40)), valid)
		}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			snapshot, _ := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, root)
			path := reviewResultPath(snapshot, request.Target)
			test.setup(t, path, root)
			output, err := readReviewResult(requestBody, root)
			if err == nil {
				_, err = parseIndependentReview(output)
			}
			if test.valid && err != nil {
				t.Fatalf("valid result rejected: %v", err)
			}
			if !test.valid && err == nil {
				t.Fatal("unsafe result accepted")
			}
		})
	}

	for _, test := range []struct {
		name   string
		mutate func(*reviewResultRequest)
	}{
		{"unknown mode", func(request *reviewResultRequest) { request.Mode = "ui-review" }},
		{"head outside target", func(request *reviewResultRequest) { request.Head = strings.Repeat("c", 40) }},
		{"plan target for wrong issue", func(request *reviewResultRequest) {
			request.Mode = agentruntime.ReviewModePlan
			request.Target = "o/r#24 plan sha256:" + strings.Repeat("a", 64)
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			invalid := request
			test.mutate(&invalid)
			body, _ := json.Marshal(invalid)
			if _, err := readReviewResult(body, t.TempDir()); err == nil {
				t.Fatal("invalid mode or exact target reached the review artifact")
			}
		})
	}

	t.Run("substitution", func(t *testing.T) {
		root := t.TempDir()
		snapshot, _ := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, root)
		path := reviewResultPath(snapshot, request.Target)
		mustWriteFile(t, path, valid)
		oldOpen := hostReviewResultOpen
		hostReviewResultOpen = func(candidate string, flags int, mode uint32) (int, error) {
			if err := os.Rename(candidate, candidate+".replaced"); err != nil {
				return -1, err
			}
			mustWriteFile(t, candidate, valid)
			return syscall.Open(candidate, flags, mode)
		}
		t.Cleanup(func() { hostReviewResultOpen = oldOpen })
		if _, err := readReviewResult(requestBody, root); err == nil {
			t.Fatal("substituted result accepted")
		}
	})

	root := t.TempDir()
	snapshot, _ := reviewIdentity(agentruntime.Attempt{Repository: request.Repository, Issue: request.Issue, Number: request.Attempt}, root)
	mustWriteFile(t, reviewResultPath(snapshot, request.Target), valid)
	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
	operation, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"review-result", boundaryCommand{Input: requestBody}})
	for _, mode := range []string{"review", "implementation"} {
		var output bytes.Buffer
		err := agentHost(t.Context(), mode, bytes.NewReader(operation), &output)
		if mode == "review" && err != nil {
			t.Fatalf("review boundary rejected result: %v", err)
		}
		if mode == "implementation" && err == nil {
			t.Fatal("implementation boundary read review result")
		}
	}

	missingRequest := request
	missingRequest.Head = strings.Repeat("b", 40)
	missingBody, _ := json.Marshal(missingRequest)
	invalidOperation, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"review-result", boundaryCommand{Input: missingBody}})
	var output bytes.Buffer
	if err := agentHost(t.Context(), "review", bytes.NewReader(invalidOperation), &output); err != nil {
		t.Fatalf("invalid artifact was not encoded: %v", err)
	}
	var result agentruntime.Result
	if err := json.Unmarshal(output.Bytes(), &result); err != nil || !result.Exited || result.Code != reviewResultInvalidCode || result.Output != "review result artifact is invalid" {
		t.Fatalf("invalid artifact result=%#v err=%v", result, err)
	}
}

func mustWriteFile(t *testing.T, path, body string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o770); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o660); err != nil {
		t.Fatal(err)
	}
}

func TestHandoffPersistenceAndExportStayBounded(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is unavailable")
	}
	tmuxTmp, err := os.MkdirTemp("/tmp", "as-tmux-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmuxTmp) })
	t.Setenv("TMUX_TMPDIR", tmuxTmp)

	t.Run("findings persist without a separate signal", func(t *testing.T) {
		oldExec := hostExecRunner
		calls := 0
		recipient := ""
		prompt := ""
		var submission agentruntime.Command
		hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
			calls++
			if command.Args[0] == "load-buffer" {
				body, _ := io.ReadAll(command.Stdin)
				prompt = string(body)
			}
			if slices.Contains(command.Args, "show-options") {
				return agentruntime.Result{Output: recipient}, nil
			}
			if command.Args[0] == "respawn-pane" {
				submission = command
				var err error
				recipient, err = acknowledgeHandoffLaunch(command)
				return agentruntime.Result{}, err
			}
			if command.Args[0] == "set-option" {
				recipient = command.Args[len(command.Args)-1]
			}
			return agentruntime.Result{}, nil
		}
		t.Cleanup(func() { hostExecRunner = oldExec })
		root := t.TempDir()
		worktree := filepath.Join(root, "attempt")
		if err := os.Mkdir(worktree, 0o700); err != nil {
			t.Fatal(err)
		}
		handoff := []byte(`{"type":"agent-symphony-handoff-v1","key":"pane-test","human_instructions":["keep the YAML ignored"]}`)
		request, _ := json.Marshal(struct {
			Manifest     agentruntime.Manifest `json:"manifest"`
			Handoff      json.RawMessage       `json:"handoff"`
			OutcomePath  string                `json:"outcome_path"`
			OutcomeToken string                `json:"outcome_token"`
			Command      []string              `json:"command"`
		}{agentruntime.Manifest{Worktree: worktree, Session: "as-23-1", LogPath: filepath.Join(worktree, "attempt.log")}, handoff, handoffReceiptPath(worktree, "pane-test"), "token", []string{"implementation"}})
		if _, err := acceptHandoff(t.Context(), request, root); err != nil {
			t.Fatal(err)
		}
		if _, err := acceptHandoff(t.Context(), request, root); err != nil {
			t.Fatal(err)
		}
		if calls != 4 {
			t.Fatalf("worker made %d tmux calls, want lookup, buffer load, launch, and ready binding", calls)
		}
		if !slices.Contains(submission.Args, "worker-capture-handoff-ready") || !slices.Contains(submission.Args, "implementation") || slices.Contains(submission.Args, "paste-buffer") || slices.Contains(submission.Args, "send-keys") {
			t.Fatalf("handoff did not use the stdin capture helper: %#v", submission.Args)
		}
		if !strings.Contains(prompt, humanInstructionPrecedence) || !strings.Contains(prompt, "keep the YAML ignored") {
			t.Fatalf("handoff omitted human instruction precedence: %q", prompt)
		}
		if _, err := os.ReadFile(filepath.Join(worktree, ".agent-symphony", "handoffs", "pane-test.json")); err != nil {
			t.Fatalf("durable handoff: %v", err)
		}
	})

	t.Run("export", func(t *testing.T) {
		root := t.TempDir()
		primary := filepath.Join(root, "primary")
		if err := os.Mkdir(primary, 0o700); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}} {
			if out, err := exec.Command("git", append([]string{"-C", primary}, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}
		if err := os.WriteFile(filepath.Join(primary, "README.md"), []byte("base\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		for _, args := range [][]string{{"add", "README.md"}, {"commit", "-m", "base"}} {
			if out, err := exec.Command("git", append([]string{"-C", primary}, args...)...).CombinedOutput(); err != nil {
				t.Fatalf("git %v: %v: %s", args, err, out)
			}
		}
		base := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", primary, "rev-parse", "HEAD"))))
		identity, err := agentruntime.AttemptIdentity(root, agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: base})
		if err != nil {
			t.Fatal(err)
		}
		if out, err := exec.Command("git", "-C", primary, "worktree", "add", "-b", identity.Branch, identity.Worktree).CombinedOutput(); err != nil {
			t.Fatalf("git worktree: %v: %s", err, out)
		}
		if err := os.WriteFile(filepath.Join(identity.Worktree, "README.md"), []byte("base\nchanged\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		mustWriteFile(t, filepath.Join(identity.Worktree, ".agent-symphony", "handoffs", "receipt.json"), "metadata")
		validation := "pane-zero-" + strings.Repeat("v", 240)
		const credential = "implementation-result-auth-canary"
		good := fmt.Sprintf(`{"type":"agent-symphony-result-v1","validation":%q,"documentation":"[REDACTED]"}`, validation)
		bad := `{"type":"agent-symphony-result-v1","validation":"wrong-window","documentation":"wrong"}`
		resultPath := agentruntime.ResultPath(identity.Worktree)
		prompt, captureTmux := filepath.Join(t.TempDir(), "prompt"), filepath.Join(t.TempDir(), "tmux")
		if err := os.WriteFile(prompt, []byte("prompt"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(captureTmux, []byte("#!/bin/sh\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer) :;; *) exit 2;; esac\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		t.Setenv("FAKE_PROMPT", prompt)
		t.Setenv("GH_TOKEN", credential)
		t.Setenv("EXPECTED_TOKEN", credential)
		if code, err := agentruntime.CaptureWorker(t.Context(), captureTmux, "buffer", resultPath, []string{"sh", "-c", `test "$GH_TOKEN" = "$EXPECTED_TOKEN" && printf '{"type":"agent-symphony-result-v1","validation":"%s","documentation":"%s"}' "$1" "$GH_TOKEN"`, "worker", validation}, io.Discard, io.Discard); err != nil || code != 0 {
			t.Fatalf("capture implementation result: code=%d err=%v", code, err)
		}
		t.Setenv("GH_TOKEN", "")
		tmux(t, "new-session", "-d", "-x", "80", "-s", identity.Session, "-c", identity.Worktree, "sh", "-c", "printf 'prompt echo\\n%s\\n%s\\n' '"+good+"' '"+good+"' >&2; sleep 30")
		t.Cleanup(func() { _ = exec.Command("tmux", "kill-session", "-t", "="+identity.Session).Run() })
		for deadline := time.Now().Add(2 * time.Second); ; time.Sleep(10 * time.Millisecond) {
			out, _ := exec.Command("tmux", "capture-pane", "-p", "-J", "-t", agentruntime.PaneTarget(identity.Session), "-S", "-200").CombinedOutput()
			if strings.Contains(string(out), good) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("pane zero did not emit result: %s", out)
			}
		}
		tmux(t, "new-window", "-d", "-t", "="+identity.Session, "sh", "-c", "printf '%s\\n' '"+bad+"'; sleep 30")
		tmux(t, "select-window", "-t", "="+identity.Session+":1")
		manifest := identity
		manifest.State = "completed"
		t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
		call := func(mode string, candidate agentruntime.Manifest) (agentruntime.Result, error) {
			body, _ := json.Marshal(candidate)
			request, _ := json.Marshal(struct {
				Operation string          `json:"operation"`
				Command   boundaryCommand `json:"command"`
			}{"export", boundaryCommand{Input: body}})
			var boundary bytes.Buffer
			err := agentHost(t.Context(), mode, bytes.NewReader(request), &boundary)
			var result agentruntime.Result
			if err == nil {
				err = json.Unmarshal(boundary.Bytes(), &result)
			}
			return result, err
		}
		for name, edit := range map[string]func(*agentruntime.Manifest){
			"sibling worktree": func(m *agentruntime.Manifest) { m.Worktree = primary },
			"wrong session":    func(m *agentruntime.Manifest) { m.Session = "as-sibling" },
			"invalid base":     func(m *agentruntime.Manifest) { m.BaseSHA = "not-a-commit" },
		} {
			t.Run(name, func(t *testing.T) {
				candidate := manifest
				edit(&candidate)
				if _, err := call("implementation", candidate); err == nil {
					t.Fatal("forged manifest was accepted")
				}
			})
		}
		badTMP := filepath.Join(t.TempDir(), "missing")
		t.Setenv("TMPDIR", badTMP)
		if _, err := call("implementation", manifest); err == nil {
			t.Fatal("post-commit export failure was not injected")
		}
		if _, err := os.Lstat(resultPath); err != nil {
			t.Fatalf("result was consumed after failed export: %v", err)
		}
		committedHead := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", identity.Worktree, "rev-parse", "HEAD"))))
		if committedHead == base {
			t.Fatal("injected failure happened before the worker commit")
		}
		if status := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", identity.Worktree, "status", "--porcelain", "--", ".", ":(exclude).agent-symphony")))); status != "" {
			t.Fatalf("worktree remained dirty after commit: %q", status)
		}

		t.Setenv("TMPDIR", t.TempDir())
		result, err := call("implementation", manifest)
		if err != nil {
			t.Fatal(err)
		}
		var exported workerExport
		if err := json.Unmarshal([]byte(result.Output), &exported); err != nil || exported.Result.Validation != validation || exported.Result.Documentation != "[REDACTED]" || strings.Contains(result.Output, credential) || exported.HeadSHA == base {
			t.Fatalf("export=%#v err=%v", exported, err)
		}
		pullBody, err := internalgithub.PullRequestBody(23, 1, exported.Result.Validation, exported.Result.Documentation, exported.Result.Decisions)
		if err != nil || strings.Contains(pullBody, credential) || !strings.Contains(pullBody, "[REDACTED]") {
			t.Fatalf("unsafe generated pull request body: %q err=%v", pullBody, err)
		}
		second, err := call("implementation", manifest)
		if err != nil {
			t.Fatal(err)
		}
		var repeated workerExport
		if err := json.Unmarshal([]byte(second.Output), &repeated); err != nil {
			t.Fatal(err)
		}
		if repeated.HeadSHA != exported.HeadSHA || repeated.BundleSHA256 != exported.BundleSHA256 || repeated.Result != exported.Result {
			t.Fatalf("retry changed export metadata: first=%#v repeated=%#v", exported, repeated)
		}
		if got, err := os.ReadFile(resultPath); err != nil || string(got) != good {
			t.Fatalf("retained result = %q, %v", got, err)
		}
		if _, err := call("review", manifest); err == nil {
			t.Fatal("review boundary accepted implementation export")
		}
		if status := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", identity.Worktree, "status", "--porcelain", "--", ".", ":(exclude).agent-symphony")))); status != "" {
			t.Fatalf("committed worktree remains dirty: %q", status)
		}
		if got := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", identity.Worktree, "ls-tree", "-r", "--name-only", "HEAD", "--", ".agent-symphony")))); got != "" {
			t.Fatalf("handoff metadata committed: %q", got)
		}
		if got := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", identity.Worktree, "show", "HEAD:README.md")))); got != "base\nchanged" {
			t.Fatalf("committed content = %q", got)
		}
		if got := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", primary, "rev-parse", "HEAD")))); got != base {
			t.Fatalf("primary worktree moved from %s to %s", base, got)
		}
	})
}

func TestWorkerResultArtifactFailsClosed(t *testing.T) {
	valid := `{"type":"agent-symphony-result-v1","validation":"go test ./... passed","documentation":"none"}`
	malformed := `{"type":"agent-symphony-result-v1","validation":`
	unknown := `{"type":"agent-symphony-result-v1","validation":"ok","documentation":"none","extra":true}`
	oversized := `{"type":"agent-symphony-result-v1","validation":"` + strings.Repeat("x", agentruntime.WorkerResultMaxBytes) + `","documentation":"none"}`
	for _, test := range []struct {
		name, body string
	}{
		{"missing", ""},
		{"malformed", malformed},
		{"malformed then valid", malformed + "\n" + valid},
		{"unknown field", unknown},
		{"empty validation", `{"type":"agent-symphony-result-v1","validation":" ","documentation":"none"}`},
		{"empty documentation", `{"type":"agent-symphony-result-v1","validation":"ok","documentation":" "}`},
		{"multiple objects", valid + "\n" + valid},
		{"capture ceiling truncated", strings.Repeat("x", agentruntime.WorkerResultMaxBytes)},
		{"oversized", oversized},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "attempt.result.json")
			if test.body != "" {
				if err := os.WriteFile(path, []byte(test.body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			if result, err := readWorkerResult(path); err == nil {
				t.Fatalf("result=%#v", result)
			}
		})
	}
	t.Run("symlink", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "attempt.result.json")
		target := filepath.Join(t.TempDir(), "result")
		if err := os.WriteFile(target, []byte(valid), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, path); err != nil {
			t.Fatal(err)
		}
		if _, err := readWorkerResult(path); err == nil {
			t.Fatal("symlink result was accepted")
		}
	})
	t.Run("world readable", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "attempt.result.json")
		if err := os.WriteFile(path, []byte(valid), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := readWorkerResult(path); err == nil {
			t.Fatal("world-readable result was accepted")
		}
	})
}

func TestPendingHandoffRetriesWithoutDuplicateExecution(t *testing.T) {
	for _, test := range []struct {
		name, failure, kind string
		accept              func(context.Context, []byte, string) (string, error)
	}{
		{"load-buffer", "load-buffer", "", acceptHandoff},
		{"submission", "submission", "", acceptHandoff},
		{"respawn-side-effect", "respawn-side-effect", "", acceptHandoff},
		{"receipt", "receipt", "", acceptHandoff},
		{"operator-respawn-side-effect", "respawn-side-effect", "operator-message", acceptOperatorHandoff},
	} {
		t.Run(test.name, func(t *testing.T) {
			oldExec, oldDirSync := hostExecRunner, immutableDirSync
			t.Cleanup(func() { hostExecRunner, immutableDirSync = oldExec, oldDirSync })
			root := t.TempDir()
			worktree := filepath.Join(root, "attempt")
			if err := os.Mkdir(worktree, 0o700); err != nil {
				t.Fatal(err)
			}
			resultPath := agentruntime.ResultPath(worktree)
			previousResult := `{"type":"agent-symphony-result-v1","validation":"previous turn passed","documentation":"none"}`
			if err := os.WriteFile(resultPath, []byte(previousResult), 0o600); err != nil {
				t.Fatal(err)
			}
			handoff, _ := json.Marshal(struct{ Type, Key, Kind string }{"agent-symphony-handoff-v1", "retry-key", test.kind})
			request, _ := json.Marshal(struct {
				Manifest     agentruntime.Manifest `json:"manifest"`
				Handoff      json.RawMessage       `json:"handoff"`
				OutcomePath  string                `json:"outcome_path"`
				OutcomeToken string                `json:"outcome_token"`
				Command      []string              `json:"command"`
			}{agentruntime.Manifest{Worktree: worktree, Session: "as-retry", LogPath: filepath.Join(worktree, "attempt.log")}, handoff, handoffReceiptPath(worktree, "retry-key"), "token", []string{"implementation"}})
			recipient, deliveries, injected := "", 0, false
			hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
				if command.Args[0] == "set-option" {
					if len(command.Args) == 5 {
						recipient = ""
					} else {
						recipient = command.Args[len(command.Args)-1]
					}
					return agentruntime.Result{}, nil
				}
				if slices.Contains(command.Args, "show-options") {
					return agentruntime.Result{Output: recipient}, nil
				}
				if slices.Contains(command.Args, "display-message") {
					return agentruntime.Result{Output: "1"}, nil
				}
				if !injected && test.failure == "load-buffer" && slices.Contains(command.Args, "load-buffer") {
					injected = true
					return agentruntime.Result{}, errors.New("injected load failure")
				}
				if command.Args[0] == "respawn-pane" {
					if !injected && test.failure == "submission" {
						injected = true
						return agentruntime.Result{}, errors.New("injected submission failure")
					}
					launchedRecipient, launchErr := acknowledgeHandoffLaunch(command)
					if launchErr != nil {
						return agentruntime.Result{}, launchErr
					}
					if !injected && test.failure == "respawn-side-effect" {
						injected = true
						deliveries++
						recipient = launchedRecipient
						return agentruntime.Result{}, errors.New("injected failure after respawn")
					}
					recipient, deliveries = launchedRecipient, deliveries+1
				}
				return agentruntime.Result{}, nil
			}
			immutableDirSync = func(dir string) error {
				if !injected && test.failure == "receipt" && dir == filepath.Join(worktree, ".agent-symphony", "handoffs") && recipient != "" {
					injected = true
					return errors.New("injected receipt sync failure")
				}
				return oldDirSync(dir)
			}
			if _, err := test.accept(t.Context(), request, root); err == nil {
				t.Fatal("injected failure succeeded")
			}
			if retained, err := os.ReadFile(resultPath); err != nil || string(retained) != previousResult {
				t.Fatalf("previous result was not retained across %s failure: %q err=%v", test.failure, retained, err)
			}
			if _, err := os.Stat(filepath.Join(worktree, ".agent-symphony", "handoffs", "retry-key.json")); err != nil {
				t.Fatalf("pending state lost: %v", err)
			}
			if test.failure == "respawn-side-effect" {
				if _, err := os.Stat(filepath.Join(worktree, ".agent-symphony", "handoffs", "retry-key.launching")); err != nil {
					t.Fatalf("durable launch state lost: %v", err)
				}
			}
			if _, err := test.accept(t.Context(), request, root); err != nil {
				t.Fatalf("restart retry: %v", err)
			}
			if deliveries != 1 {
				t.Fatalf("deliveries=%d, want 1", deliveries)
			}
		})
	}
}

func TestOperatorHandoffCommandDriftPreservesEveryLaunchBoundary(t *testing.T) {
	for _, stage := range []string{"binding", "launching", "launched", "receipt"} {
		t.Run(stage, func(t *testing.T) {
			oldExec := hostExecRunner
			hostExecRunner = func(context.Context, agentruntime.Command) (agentruntime.Result, error) {
				return agentruntime.Result{}, errors.New("tmux must not be reached after command drift")
			}
			t.Cleanup(func() { hostExecRunner = oldExec })

			root := t.TempDir()
			worktree := filepath.Join(root, "attempt")
			if err := os.Mkdir(worktree, 0o700); err != nil {
				t.Fatal(err)
			}
			handoff := json.RawMessage(`{"type":"agent-symphony-handoff-v1","key":"operator-message-stable","kind":"operator-message"}`)
			requestBody := func(command []string) []byte {
				body, _ := json.Marshal(handoffRequest{
					Manifest:     agentruntime.Manifest{Worktree: worktree, Session: "as-command-drift", LogPath: filepath.Join(worktree, "attempt.log")},
					Handoff:      handoff,
					OutcomePath:  handoffReceiptPath(worktree, "operator-message-stable"),
					OutcomeToken: "stable-token",
					Command:      command,
				})
				return body
			}
			originalBody := requestBody([]string{"implementation-v1"})
			original, _, err := decodeHandoffRequest(originalBody, root)
			if err != nil {
				t.Fatal(err)
			}
			binding, recipient := handoffBinding(original)
			inbox := filepath.Join(worktree, ".agent-symphony", "handoffs")
			if err := os.MkdirAll(inbox, 0o700); err != nil {
				t.Fatal(err)
			}
			files := map[string][]byte{".json": binding}
			if stage == "launching" || stage == "launched" || stage == "receipt" {
				files[".launching"] = []byte(recipient)
			}
			if stage == "launched" || stage == "receipt" {
				files[".launched"] = []byte(recipient)
			}
			if stage == "receipt" {
				ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", "operator-message-stable", original.OutcomePath, original.OutcomeToken})
				files[".receipt"] = ack
			}
			for suffix, body := range files {
				if err := writeImmutable(filepath.Join(inbox, "operator-message-stable"+suffix), body); err != nil {
					t.Fatal(err)
				}
			}

			changedBody := requestBody([]string{"implementation-v2"})
			if _, err := verifyHandoff(t.Context(), changedBody, root); err == nil || !strings.Contains(err.Error(), "binding mismatch") {
				t.Fatalf("verify command drift = %v", err)
			}
			if _, err := acceptOperatorHandoff(t.Context(), changedBody, root); err == nil || !strings.Contains(err.Error(), "binding changed") {
				t.Fatalf("accept command drift = %v", err)
			}
			for suffix, want := range files {
				got, err := os.ReadFile(filepath.Join(inbox, "operator-message-stable"+suffix))
				if err != nil || !bytes.Equal(got, want) {
					t.Fatalf("%s evidence changed: %q err=%v", suffix, got, err)
				}
			}
		})
	}
}

func TestMissingOperatorHandoffBindingCannotReuseTmuxLaunchOption(t *testing.T) {
	oldExec := hostExecRunner
	t.Cleanup(func() { hostExecRunner = oldExec })

	root := t.TempDir()
	worktree := filepath.Join(root, "attempt")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	handoff := json.RawMessage(`{"type":"agent-symphony-handoff-v1","key":"operator-message-forged","kind":"operator-message"}`)
	request := handoffRequest{
		Manifest:     agentruntime.Manifest{Worktree: worktree, Session: "as-forged", LogPath: filepath.Join(worktree, "attempt.log")},
		Handoff:      handoff,
		OutcomePath:  handoffReceiptPath(worktree, "operator-message-forged"),
		OutcomeToken: "token",
		Command:      []string{"implementation"},
	}
	body, _ := json.Marshal(request)
	_, recipient := handoffBinding(request)
	option, launches, calls, launchDir := recipient, 0, 0, ""
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		calls++
		switch command.Args[0] {
		case "show-options":
			return agentruntime.Result{Output: option}, nil
		case "set-option":
			option = ""
			if len(command.Args) == 6 {
				option = command.Args[5]
			}
		case "respawn-pane":
			if _, err := acknowledgeHandoffLaunch(command); err != nil {
				return agentruntime.Result{}, err
			}
			launches++
			if index := slices.Index(command.Args, "-c"); index >= 0 && index+1 < len(command.Args) {
				launchDir = command.Args[index+1]
			}
		}
		return agentruntime.Result{}, nil
	}

	if observed, err := verifyHandoff(t.Context(), body, root); err != nil || observed != "" {
		t.Fatalf("missing binding verification = %q, %v", observed, err)
	}
	if calls != 0 {
		t.Fatalf("missing binding reached tmux verification: %d calls", calls)
	}
	if _, err := acceptOperatorHandoff(t.Context(), body, root); err != nil {
		t.Fatal(err)
	}
	if launches != 1 || launchDir != worktree {
		t.Fatalf("fresh handoff launches=%d dir=%q", launches, launchDir)
	}
}

func TestOperatorHandoffRetriesAfterUnacknowledgedWorkerStartup(t *testing.T) {
	oldExec := hostExecRunner
	t.Cleanup(func() { hostExecRunner = oldExec })
	root := t.TempDir()
	worktree := filepath.Join(root, "attempt")
	if err := os.Mkdir(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	handoff := json.RawMessage(`{"type":"agent-symphony-handoff-v1","key":"operator-message-retry","kind":"operator-message"}`)
	request := handoffRequest{
		Manifest:     agentruntime.Manifest{Worktree: worktree, Session: "as-retry", LogPath: filepath.Join(worktree, "attempt.log")},
		Handoff:      handoff,
		OutcomePath:  handoffReceiptPath(worktree, "operator-message-retry"),
		OutcomeToken: "token",
		Command:      []string{"implementation"},
	}
	binding, recipient := handoffBinding(request)
	inbox := filepath.Join(worktree, ".agent-symphony", "handoffs")
	if err := os.MkdirAll(inbox, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeImmutable(filepath.Join(inbox, "operator-message-retry.json"), binding); err != nil {
		t.Fatal(err)
	}
	if err := writeImmutable(filepath.Join(inbox, "operator-message-retry.launching"), []byte(recipient)); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(request)
	respawns := 0
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		switch command.Args[0] {
		case "show-options":
			return agentruntime.Result{}, nil
		case "display-message":
			if slices.Contains(command.Args, "#{pane_start_command}") {
				return agentruntime.Result{Output: "/bin/zsh"}, nil
			}
			return agentruntime.Result{Output: "0"}, nil
		case "respawn-pane":
			respawns++
			if _, err := acknowledgeHandoffLaunch(command); err != nil {
				return agentruntime.Result{}, err
			}
		}
		return agentruntime.Result{}, nil
	}
	if _, err := acceptOperatorHandoff(t.Context(), body, root); err != nil {
		t.Fatal(err)
	}
	if respawns != 1 {
		t.Fatalf("replacement worker respawns=%d", respawns)
	}
	if body, err := os.ReadFile(filepath.Join(inbox, "operator-message-retry.launched")); err != nil || string(body) != recipient {
		t.Fatalf("replacement launch was not acknowledged: %q %v", body, err)
	}
}

func tmux(t *testing.T, args ...string) {
	t.Helper()
	if out, err := exec.Command("tmux", args...).CombinedOutput(); err != nil {
		t.Fatalf("tmux %v: %v: %s", args, err, out)
	}
}

func cleanupTestManifest(t *testing.T, root string) agentruntime.Manifest {
	t.Helper()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	manifest, err := agentruntime.AttemptIdentity(root, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{{"init", "-b", manifest.Branch}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"commit", "--allow-empty", "-m", "attempt"}} {
		if out, err := exec.Command("git", append([]string{"-C", manifest.Worktree}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	manifest.State = "completed"
	manifest.ReviewHead = runGit(t, manifest.Worktree, "rev-parse", "HEAD")
	return manifest
}

func TestCleanupAttemptRemovesOnlyVerifiedRuntimeResources(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := cleanupTestManifest(t, root)
	resultPath := agentruntime.ResultPath(manifest.Worktree)
	mustWriteFile(t, resultPath, `{"type":"agent-symphony-result-v1"}`)
	logPath := filepath.Join(t.TempDir(), "agent.log")
	mustWriteFile(t, logPath, "retained diagnostics")
	manifest.LogPath = logPath
	body, _ := json.Marshal(manifest)

	oldExec := hostExecRunner
	live, kills := true, 0
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		switch command.Args[0] {
		case "has-session":
			if live {
				return agentruntime.Result{}, nil
			}
			return agentruntime.Result{Code: 1, Exited: true}, errors.New("missing session")
		case "kill-session":
			live, kills = false, kills+1
			return agentruntime.Result{}, nil
		default:
			return agentruntime.Result{}, fmt.Errorf("unexpected tmux command %v", command.Args)
		}
	}
	t.Cleanup(func() { hostExecRunner = oldExec })

	for range 2 {
		if err := cleanupAttempt(t.Context(), body, root); err != nil {
			t.Fatal(err)
		}
	}
	if kills != 1 {
		t.Fatalf("tmux kills = %d, want 1", kills)
	}
	for _, path := range []string{manifest.Worktree, resultPath} {
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("cleanup retained %s: %v", path, err)
		}
	}
	if got, err := os.ReadFile(logPath); err != nil || string(got) != "retained diagnostics" {
		t.Fatalf("diagnostics = %q, %v", got, err)
	}
}

func TestAbandonAttemptAcceptsExactFailedWorktreeWithoutWeakeningCleanup(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	manifest := cleanupTestManifest(t, root)
	manifest.State, manifest.ReviewHead = "failed", ""
	runGit(t, manifest.Worktree, "switch", "-c", "partially-prepared")
	body, _ := json.Marshal(manifest)
	if err := cleanupAttempt(t.Context(), body, root); err == nil {
		t.Fatal("completed cleanup accepted a failed manifest")
	}
	oldExec := hostExecRunner
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		if len(command.Args) > 0 && command.Args[0] == "has-session" {
			return agentruntime.Result{Code: 1, Exited: true}, errors.New("missing session")
		}
		return agentruntime.Result{}, fmt.Errorf("unexpected tmux command %v", command.Args)
	}
	t.Cleanup(func() { hostExecRunner = oldExec })
	if err := abandonAttempt(t.Context(), body, root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(manifest.Worktree); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("abandon retained worktree: %v", err)
	}
}

func TestCleanupAttemptRejectsSubstitutedResources(t *testing.T) {
	oldExec := hostExecRunner
	hostExecRunner = func(context.Context, agentruntime.Command) (agentruntime.Result, error) {
		return agentruntime.Result{}, errors.New("tmux must not be reached")
	}
	t.Cleanup(func() { hostExecRunner = oldExec })

	t.Run("worktree symlink", func(t *testing.T) {
		root, _ := filepath.EvalSymlinks(t.TempDir())
		attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: strings.Repeat("a", 40)}
		manifest, _ := agentruntime.AttemptIdentity(root, attempt)
		manifest.State, manifest.ReviewHead = "completed", strings.Repeat("b", 40)
		external := t.TempDir()
		canary := filepath.Join(external, "canary")
		mustWriteFile(t, canary, "unchanged")
		if err := os.Symlink(external, manifest.Worktree); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(manifest)
		if err := cleanupAttempt(t.Context(), body, root); err == nil || !strings.Contains(err.Error(), "non-symlink") {
			t.Fatalf("substituted worktree cleanup = %v", err)
		}
		if got, err := os.ReadFile(canary); err != nil || string(got) != "unchanged" {
			t.Fatalf("outside canary = %q, %v", got, err)
		}
	})

	t.Run("worktree file", func(t *testing.T) {
		root, _ := filepath.EvalSymlinks(t.TempDir())
		attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: strings.Repeat("a", 40)}
		manifest, _ := agentruntime.AttemptIdentity(root, attempt)
		manifest.State, manifest.ReviewHead = "completed", strings.Repeat("b", 40)
		mustWriteFile(t, manifest.Worktree, "not a repository")
		body, _ := json.Marshal(manifest)
		if err := cleanupAttempt(t.Context(), body, root); err == nil || !strings.Contains(err.Error(), "non-symlink directory") {
			t.Fatalf("non-directory worktree cleanup = %v", err)
		}
		if got, err := os.ReadFile(manifest.Worktree); err != nil || string(got) != "not a repository" {
			t.Fatalf("worktree file = %q, %v", got, err)
		}
	})

	t.Run("head mismatch", func(t *testing.T) {
		root, _ := filepath.EvalSymlinks(t.TempDir())
		manifest := cleanupTestManifest(t, root)
		manifest.ReviewHead = strings.Repeat("b", 40)
		body, _ := json.Marshal(manifest)
		if err := cleanupAttempt(t.Context(), body, root); err == nil || !strings.Contains(err.Error(), "identity changed") {
			t.Fatalf("mismatched head cleanup = %v", err)
		}
		if _, err := os.Stat(manifest.Worktree); err != nil {
			t.Fatalf("mismatched worktree removed: %v", err)
		}
	})

	t.Run("result symlink", func(t *testing.T) {
		root, _ := filepath.EvalSymlinks(t.TempDir())
		manifest := cleanupTestManifest(t, root)
		canary := filepath.Join(t.TempDir(), "canary")
		mustWriteFile(t, canary, "unchanged")
		if err := os.Symlink(canary, agentruntime.ResultPath(manifest.Worktree)); err != nil {
			t.Fatal(err)
		}
		body, _ := json.Marshal(manifest)
		if err := cleanupAttempt(t.Context(), body, root); err == nil || !strings.Contains(err.Error(), "cleanup result") {
			t.Fatalf("substituted result cleanup = %v", err)
		}
		if got, err := os.ReadFile(canary); err != nil || string(got) != "unchanged" {
			t.Fatalf("result canary = %q, %v", got, err)
		}
	})
}

func TestProductionSeedClonesThroughAgentHostBoundary(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldGOOS, oldRoot, oldExec := hostGOOS, hostRoot, hostExecRunner
	hostGOOS, hostRoot, hostExecRunner = "linux", t.TempDir(), (agentruntime.ExecRunner{}).Run
	t.Cleanup(func() { hostGOOS, hostRoot, hostExecRunner = oldGOOS, oldRoot, oldExec })
	source := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"commit", "--allow-empty", "-m", "base"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	root := nativeRoot("/var/lib/agent-symphony/attempts")
	bundle, err := seedAttemptSource(t.Context(), source, "o/r", root, "", "")
	if err != nil {
		t.Fatal(err)
	}
	otherBundle, err := seedAttemptSource(t.Context(), source, "other/r", root, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if bundle == otherBundle {
		t.Fatal("different repositories shared a source bundle")
	}
	if _, err := os.Stat(bundle); err != nil {
		t.Fatalf("first repository bundle was replaced: %v", err)
	}
	info, err := os.Stat(bundle)
	if err != nil || info.Mode().Perm() != 0o640 {
		t.Fatalf("seed info=%v err=%v", info, err)
	}
	destination := filepath.Join(root, "production-attempt")
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "git", Args: []string{"clone", "--no-local", "--no-checkout", bundle, destination}, Dir: root, Env: DefaultEnvironmentForTest()}})
	var out bytes.Buffer
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &out); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(destination, ".git")); err != nil {
		t.Fatal(err)
	}
}

func TestResumeHandoffFetchesThroughAgentHostBoundary(t *testing.T) {
	oldExec := hostExecRunner
	execRunner := agentruntime.ExecRunner{}
	hostExecRunner = func(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		if command.Name == "tmux" && slices.Contains(command.Args, "has-session") {
			return agentruntime.Result{}, nil
		}
		return execRunner.Run(ctx, command)
	}
	t.Cleanup(func() { hostExecRunner = oldExec })

	stateRoot := t.TempDir()
	stateRoot, _ = filepath.EvalSymlinks(stateRoot)
	root := filepath.Join(stateRoot, "worktrees")
	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
	source := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"commit", "--allow-empty", "-m", "base"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	base := runGit(t, source, "rev-parse", "HEAD")
	bundle, err := seedAttemptSource(t.Context(), source, "o/r", root, "", "")
	if err != nil {
		t.Fatal(err)
	}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 4, Number: 2, BaseSHA: base}
	manifest, err := agentruntime.AttemptIdentity(root, attempt)
	if err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "clone", "--no-local", "--no-checkout", bundle, manifest.Worktree).CombinedOutput(); err != nil {
		t.Fatalf("clone attempt: %v: %s", err, out)
	}
	runGit(t, manifest.Worktree, "checkout", "--detach", base)
	runGit(t, manifest.Worktree, "switch", "-c", manifest.Branch)
	runGit(t, manifest.Worktree, "remote", "remove", "origin")

	runGit(t, source, "commit", "--allow-empty", "-m", "handoff source")
	want := runGit(t, source, "rev-parse", "HEAD")
	branch := runGit(t, source, "branch", "--show-current")
	if _, err := seedAttemptSource(t.Context(), source, "o/r", root, "", ""); err != nil {
		t.Fatal(err)
	}

	manifest.State = "completed"
	manifest.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository), "4-2", "agent.log")
	manifest.CreatedAt, manifest.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	manifestPath := filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}

	runtimeState := agentruntime.Runtime{Root: root, StateRoot: stateRoot, Source: bundle, Runner: agentHostRuntimeRunner{}}
	resumed, err := runtimeState.ResumeHandoff(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	if got := runGit(t, manifest.Worktree, "rev-parse", "refs/remotes/agent-symphony/"+branch); resumed.State != "running" || got != want {
		t.Fatalf("resumed=%#v fetched=%s want=%s", resumed, got, want)
	}
	if validGitBoundaryArgs([]string{"-C", manifest.Worktree, "fetch", "--no-tags", filepath.Join(t.TempDir(), "outside.source.bundle"), "+refs/heads/*:refs/remotes/agent-symphony/*"}, root, root) ||
		validGitBoundaryArgs([]string{"-C", manifest.Worktree, "fetch", "--no-tags", bundle, "+refs/heads/*:refs/remotes/origin/*"}, root, root) {
		t.Fatal("handoff fetch allowlist accepted a source escape or alternate refspec")
	}
}

func TestProductionSeedFetchesSelectedBaseWithoutMovingCheckout(t *testing.T) {
	fakeNoHostIsolation(t)
	run := func(dir string, args ...string) string {
		t.Helper()
		out, err := exec.Command("git", append([]string{"-C", dir}, args...)...).CombinedOutput()
		if err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
		return strings.TrimSpace(string(out))
	}
	remote, coordinator, publisher := t.TempDir(), t.TempDir(), t.TempDir()
	run(remote, "init", "--bare")
	run(coordinator, "init")
	run(coordinator, "checkout", "-b", "main")
	run(coordinator, "config", "user.email", "test@example.invalid")
	run(coordinator, "config", "user.name", "test")
	run(coordinator, "commit", "--allow-empty", "-m", "base")
	run(coordinator, "remote", "add", "origin", remote)
	run(coordinator, "push", "-u", "origin", "main")
	coordinatorHead := run(coordinator, "rev-parse", "HEAD")
	if out, err := exec.Command("git", "clone", "--branch", "main", remote, publisher).CombinedOutput(); err != nil {
		t.Fatalf("clone publisher: %v: %s", err, out)
	}
	run(publisher, "config", "user.email", "test@example.invalid")
	run(publisher, "config", "user.name", "test")
	run(publisher, "commit", "--allow-empty", "-m", "advanced base")
	run(publisher, "push", "origin", "main")
	selectedBase := run(publisher, "rev-parse", "HEAD")

	bundle, err := seedAttemptSource(t.Context(), coordinator, "o/r", t.TempDir(), "main", selectedBase)
	if err != nil {
		t.Fatal(err)
	}
	if got := run(coordinator, "rev-parse", "HEAD"); got != coordinatorHead {
		t.Fatalf("coordinator checkout moved from %s to %s", coordinatorHead, got)
	}
	clone := filepath.Join(t.TempDir(), "worker")
	if out, err := exec.Command("git", "clone", "--no-checkout", bundle, clone).CombinedOutput(); err != nil {
		t.Fatalf("clone seed: %v: %s", err, out)
	}
	run(clone, "cat-file", "-e", selectedBase+"^{commit}")
}

func TestLocalSeedAndDiscoveryUseSeparateRoots(t *testing.T) {
	fakeNoHostIsolation(t)
	source := t.TempDir()
	for _, args := range [][]string{{"init"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"commit", "--allow-empty", "-m", "base"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	stateRoot := t.TempDir()
	worktreeRoot := productionAttemptRoot(stateRoot)
	bundle, err := seedAttemptSource(t.Context(), source, "o/r", worktreeRoot, "", "")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(bundle) != filepath.Join(stateRoot, "worktrees") {
		t.Fatalf("bundle shares recovery root: %s", bundle)
	}
	attempts := filepath.Join(stateRoot, "attempts")
	if err := os.Mkdir(attempts, 0o700); err != nil {
		t.Fatal(err)
	}
	legacy := filepath.Join(attempts, "source.bundle")
	if err := os.WriteFile(legacy, []byte("legacy bundle"), 0o640); err != nil {
		t.Fatal(err)
	}
	runtime := agentruntime.Runtime{Root: worktreeRoot, StateRoot: stateRoot}
	if manifests, err := runtime.Discover(); err != nil || len(manifests) != 0 {
		t.Fatalf("discover seeded layout = %#v, %v", manifests, err)
	}
	if err := os.WriteFile(filepath.Join(attempts, "unexpected"), []byte("bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Discover(); err == nil || !strings.Contains(err.Error(), "non-symlink directory") {
		t.Fatalf("unexpected state file accepted: %v", err)
	}
	if err := os.Remove(filepath.Join(attempts, "unexpected")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(legacy); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(bundle, legacy); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Discover(); err == nil || !strings.Contains(err.Error(), "non-symlink directory") {
		t.Fatalf("legacy bundle symlink accepted: %v", err)
	}
}

func DefaultEnvironmentForTest() []string { return []string{"PATH=" + os.Getenv("PATH"), "LANG=C"} }

func TestAgentHostRejectsWrongIdentityCredentialAndEscapingCommand(t *testing.T) {
	fakeHostIdentity(t, 1, 2)
	if err := agentHost(t.Context(), "implementation", strings.NewReader(`{"operation":"verify","command":{}}`), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "must run as") {
		t.Fatalf("err=%v", err)
	}
	fakeHostIdentity(t, 1234, 5678)
	for _, command := range []boundaryCommand{
		{Name: "sh"},
		{Name: filepath.Join(t.TempDir(), "git")},
		{Name: "git", Dir: "/tmp"},
		{Name: "git", Args: []string{"-C", "/tmp"}},
		{Name: "git", Args: []string{"-C", "../../outside", "rev-parse", "HEAD"}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "git", Args: []string{"clone", "--no-local", "--no-checkout", "../../outside", "clone"}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "tmux", Args: []string{"-S", "../../outside/socket", "has-session", "-t", "=x"}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "tmux", Args: []string{"-f", "../../outside.conf", "new-session", "-d", "-s", "x", "-c", "."}, Dir: filepath.Join(nativeRoot("/var/lib/agent-symphony/attempts"), "inside")},
		{Name: "git", Env: []string{"RANDOM_TOKEN=secret-canary"}},
		{Name: "git", Env: []string{"HTTPS_PROXY=https://secret-canary@example.invalid"}},
		{Name: "git", Env: []string{"HOME=/coordinator-home"}},
	} {
		payload, _ := json.Marshal(struct {
			Operation string          `json:"operation"`
			Command   boundaryCommand `json:"command"`
		}{"run", command})
		if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &bytes.Buffer{}); err == nil {
			t.Fatalf("accepted %#v", command)
		}
	}
}

func TestBoundaryRejectsSymlinkEscapeForMissingTarget(t *testing.T) {
	root, outside := t.TempDir(), t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "link")); err != nil {
		t.Fatal(err)
	}
	if belowRoot(filepath.Join(root, "link", "missing"), root) {
		t.Fatal("accepted path through symlinked ancestor")
	}
}

func TestAgentHostRejectsEveryCLIFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"agent-host", "implementation", "--offline"}, &stdout, &stderr); code != 2 || !strings.Contains(stderr.String(), "usage:") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestProvisionIdentitiesReusesNativeCommandsAndRejectsConflict(t *testing.T) {
	oldGOOS, oldRun, oldUser, oldGroup, oldOutput := hostGOOS, hostRun, hostLookupUser, hostLookupGroup, hostOutput
	t.Cleanup(func() {
		hostGOOS, hostRun, hostLookupUser, hostLookupGroup, hostOutput = oldGOOS, oldRun, oldUser, oldGroup, oldOutput
	})
	hostGOOS = "linux"
	hostLookupUser = func(name string) (*user.User, error) {
		return &user.User{Username: name, Uid: "100", Gid: "200", HomeDir: "/var/lib/" + name}, nil
	}
	hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: "200"}, nil }
	hostOutput = func(name string, args ...string) ([]byte, error) {
		if name == "getent" {
			if args[0] == "group" {
				return []byte(args[len(args)-1] + ":x:200:\n"), nil
			}
			account := args[len(args)-1]
			return []byte(account + ":x:100:200::/var/lib/" + account + ":/usr/sbin/nologin\n"), nil
		}
		return []byte("200\n"), nil
	}
	var calls []string
	hostRun = func(name string, args ...string) error {
		calls = append(calls, strings.Join(append([]string{name}, args...), " "))
		return nil
	}
	if err := provisionIdentities("coordinator"); err != nil {
		t.Fatal(err)
	}
	if len(calls) != 1 || calls[0] != "usermod --append --groups agent-symphony-attempt,agent-symphony-snapshot coordinator" {
		t.Fatalf("calls=%v", calls)
	}
	hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: "999"}, nil }
	if err := provisionIdentities("coordinator"); err == nil || !strings.Contains(err.Error(), "conflicting") {
		t.Fatalf("err=%v", err)
	}
}

func TestProvisionedIdentitiesRejectUIDAndGIDCollisions(t *testing.T) {
	oldUser, oldGroup := hostLookupUser, hostLookupGroup
	t.Cleanup(func() { hostLookupUser, hostLookupGroup = oldUser, oldGroup })

	for _, test := range []struct {
		name     string
		userIDs  map[string]string
		groupIDs map[string]string
	}{
		{"worker reviewer UID", map[string]string{"coordinator": "1000", workerUser: "1001", reviewerUser: "1001"}, map[string]string{attemptGroup: "2001", snapshotGroup: "2002"}},
		{"attempt snapshot GID", map[string]string{"coordinator": "1000", workerUser: "1001", reviewerUser: "1002"}, map[string]string{attemptGroup: "2001", snapshotGroup: "2001"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			hostLookupUser = func(name string) (*user.User, error) { return &user.User{Username: name, Uid: test.userIDs[name]}, nil }
			hostLookupGroup = func(name string) (*user.Group, error) { return &user.Group{Name: name, Gid: test.groupIDs[name]}, nil }
			if err := validateProvisionedIdentitySeparation("coordinator"); err == nil || !strings.Contains(err.Error(), "share") {
				t.Fatalf("err=%v", err)
			}
		})
	}
}

func TestHostRootCreationRetriesAfterEveryMutationBoundary(t *testing.T) {
	for _, failAt := range []int{1, 2, 3} {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			oldRun, oldUser, oldGroup := hostRun, hostLookupUser, hostLookupGroup
			t.Cleanup(func() { hostRun, hostLookupUser, hostLookupGroup = oldRun, oldUser, oldGroup })
			uid, gid := strconv.Itoa(os.Getuid()), strconv.Itoa(os.Getgid())
			hostLookupUser = func(string) (*user.User, error) { return &user.User{Uid: uid}, nil }
			hostLookupGroup = func(string) (*user.Group, error) { return &user.Group{Gid: gid}, nil }
			calls := 0
			hostRun = func(name string, args ...string) error {
				calls++
				if calls == failAt {
					return os.ErrPermission
				}
				if name == "chmod" {
					mode, _ := strconv.ParseUint(args[0], 8, 32)
					return os.Chmod(args[1], os.FileMode(mode))
				}
				return nil
			}
			path := filepath.Join(t.TempDir(), "attempts")
			if err := ensureHostRoot(path, "worker", "attempt", "2770"); err == nil {
				t.Fatal("injected failure passed")
			}
			hostRun = func(name string, args ...string) error {
				if name == "chmod" {
					mode, _ := strconv.ParseUint(args[0], 8, 32)
					return os.Chmod(args[1], os.FileMode(mode))
				}
				return nil
			}
			if err := ensureHostRoot(path, "worker", "attempt", "2770"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDarwinIdentityRecordRetriesAfterEveryDSCLMutation(t *testing.T) {
	properties := [][2]string{{"UniqueID", "401"}, {"PrimaryGroupID", "402"}, {"NFSHomeDirectory", "/var/db/test"}, {"UserShell", "/usr/bin/false"}, {"IsHidden", "1"}}
	for failAt := 1; failAt <= len(properties)+2; failAt++ {
		t.Run(strconv.Itoa(failAt), func(t *testing.T) {
			oldRun := hostRun
			t.Cleanup(func() { hostRun = oldRun })
			calls := 0
			hostRun = func(string, ...string) error {
				calls++
				if calls == failAt {
					return os.ErrPermission
				}
				return nil
			}
			if err := ensureDarwinRecord("/Users/test", properties); err == nil {
				t.Fatal("injected failure passed")
			}
			hostRun = func(string, ...string) error { return nil }
			if err := ensureDarwinRecord("/Users/test", properties); err != nil {
				t.Fatal(err)
			}
		})
	}
	for failAt := 1; failAt <= 4; failAt++ {
		t.Run("group-"+strconv.Itoa(failAt), func(t *testing.T) {
			oldRun := hostRun
			t.Cleanup(func() { hostRun = oldRun })
			calls := 0
			hostRun = func(string, ...string) error {
				calls++
				if calls == failAt {
					return os.ErrPermission
				}
				return nil
			}
			if err := ensureDarwinRecord("/Groups/test", [][2]string{{"PrimaryGroupID", "402"}, {"Password", "*"}}); err == nil {
				t.Fatal("injected failure passed")
			}
			hostRun = func(string, ...string) error { return nil }
			if err := ensureDarwinRecord("/Groups/test", [][2]string{{"PrimaryGroupID", "402"}, {"Password", "*"}}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBroadSudoAuthorityRejectsAnythingButManagedTuples(t *testing.T) {
	binary := "/usr/local/libexec/agent-symphony/1/agent-symphony"
	managed := []byte("    (agent-symphony-worker : agent-symphony-attempt) NOPASSWD: " + binary + " agent-host implementation\n    (agent-symphony-reviewer : agent-symphony-snapshot) NOPASSWD: " + binary + " agent-host review\n    (agent-symphony-reviewer : agent-symphony-snapshot) NOPASSWD: " + binary + " agent-host orchestrator\n")
	if !exactSudoAuthority(managed, binary) {
		t.Fatal("managed tuple rejected")
	}
	previous := []byte(strings.ReplaceAll(string(managed), "NOPASSWD:", "NOPASSWD:SETENV:"))
	if exactSudoAuthority(previous, binary) || !installableSudoAuthority(previous, binary) {
		t.Fatal("safe previous rules cannot be upgraded")
	}
	legacy := []byte("    (agent-symphony-worker : agent-symphony-attempt) NOPASSWD: " + binary + " agent-host implementation\n    (agent-symphony-reviewer : agent-symphony-snapshot) NOPASSWD: " + binary + " agent-host review\n")
	if exactSudoAuthority(legacy, binary) || !installableSudoAuthority(legacy, binary) {
		t.Fatal("safe pre-orchestrator rules cannot be upgraded")
	}
	for _, rule := range []string{
		strings.Replace(string(managed), binary, "/old/agent-symphony", 1),
		strings.Replace(string(managed), "agent-symphony-worker : agent-symphony-attempt", "root : agent-symphony-attempt", 1),
		string(managed) + "(root) NOPASSWD: /usr/bin/id\n",
	} {
		if exactSudoAuthority([]byte(rule), binary) {
			t.Fatalf("broad rule accepted: %s", rule)
		}
	}
}

func TestSudoPolicyPreservesOnlyBoundedGitHubEnvironment(t *testing.T) {
	policy := sudoersPolicy("coordinator", "/usr/local/libexec/agent-symphony/1/agent-symphony")
	if strings.Contains(policy, "SETENV") {
		t.Fatal("sudo policy grants unbounded environment authority")
	}
	for _, name := range internalgithub.GitHubCLIEnvironmentNames() {
		if !strings.Contains(policy, name) {
			t.Fatalf("sudo policy omitted GitHub CLI variable %s", name)
		}
	}
	for _, denied := range []string{"AGENT_SYMPHONY_LOCAL_ROOT", "TMUX_TMPDIR", "UNRELATED_SECRET"} {
		if strings.Contains(policy, denied) {
			t.Fatalf("sudo policy preserves denied variable %s", denied)
		}
	}
}

func TestBinarySubstitutionRollbackRemovesNewAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent-symphony")
	if err := os.WriteFile(path, []byte("new privileged tuple"), 0o440); err != nil {
		t.Fatal(err)
	}
	if err := rollbackSudoers(path, nil, false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("new sudo authority survived rollback: %v", err)
	}
}

// fakeNoHostIsolation simulates a host where install-host has never been run,
// which is the sole signal that selects the zero-admin default (local) boundary.
func fakeNoHostIsolation(t *testing.T) {
	t.Helper()
	old := hostLookupUser
	hostLookupUser = func(name string) (*user.User, error) {
		if name == workerUser || name == reviewerUser {
			return nil, errors.New("user: unknown user " + name)
		}
		return old(name)
	}
	t.Cleanup(func() { hostLookupUser = old })
}

func TestHostIsolationInstalledReflectsWorkerIdentityLookup(t *testing.T) {
	fakeNoHostIsolation(t)
	if hostIsolationInstalled() {
		t.Fatal("expected host isolation to be reported as not installed")
	}
}

func TestBoundarySelectionUsesLocalModeWhenHostIsolationIsNotInstalled(t *testing.T) {
	fakeNoHostIsolation(t)
	stateRoot := t.TempDir()
	implementation := implementationBoundary(stateRoot)
	if implementation.Command == "sudo" {
		t.Fatalf("expected local boundary, got sudo: %#v", implementation)
	}
	if len(implementation.Args) != 2 || implementation.Args[0] != "agent-host" || implementation.Args[1] != "implementation" {
		t.Fatalf("unexpected local implementation args: %#v", implementation.Args)
	}
	if want := "AGENT_SYMPHONY_LOCAL_ROOT=" + localAttemptRoot(stateRoot); !slices.Contains(implementation.Env, want) {
		t.Fatalf("expected local root env %q, got %#v", want, implementation.Env)
	}
	if want := "TMUX_TMPDIR=" + projectTmuxRoot(stateRoot); !slices.Contains(implementation.Env, want) {
		t.Fatalf("expected project tmux env %q, got %#v", want, implementation.Env)
	}
	review := reviewBoundary(stateRoot)
	if review.Command == "sudo" {
		t.Fatalf("expected local review boundary, got sudo: %#v", review)
	}
	if want := "AGENT_SYMPHONY_LOCAL_ROOT=" + localSnapshotRoot(stateRoot); !slices.Contains(review.Env, want) {
		t.Fatalf("expected local snapshot root env %q, got %#v", want, review.Env)
	}
	if want := "TMUX_TMPDIR=" + projectTmuxRoot(stateRoot); !slices.Contains(review.Env, want) {
		t.Fatalf("expected project tmux env %q, got %#v", want, review.Env)
	}
}

func TestAdvancedBoundaryUsesExactSudoCommandWithoutSetenvArguments(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	stateRoot := t.TempDir()
	for role, boundary := range map[string]workerBoundaryRunner{"implementation": implementationBoundary(stateRoot), "review": reviewBoundary(stateRoot)} {
		if boundary.Command != "sudo" || len(boundary.Env) != 0 || slices.ContainsFunc(boundary.Args, func(arg string) bool {
			return strings.Contains(arg, "preserve-env") || strings.Contains(arg, "SETENV") || strings.Contains(arg, "=")
		}) {
			t.Fatalf("unsafe advanced %s boundary: %#v", role, boundary)
		}
	}
}

func TestAdvancedAgentHostRejectsLocalRootSeamBeforeExecution(t *testing.T) {
	fakeHostIdentity(t, 1234, 5678)
	oldExec := hostExecRunner
	executed := false
	hostExecRunner = func(context.Context, agentruntime.Command) (agentruntime.Result, error) {
		executed = true
		return agentruntime.Result{}, nil
	}
	t.Cleanup(func() { hostExecRunner = oldExec })
	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", t.TempDir())
	for _, mode := range []string{"implementation", "review", "orchestrator"} {
		if err := agentHost(t.Context(), mode, strings.NewReader(`{}`), &bytes.Buffer{}); err == nil || !strings.Contains(err.Error(), "disabled when host isolation is installed") {
			t.Fatalf("advanced %s local-root seam did not fail closed: %v", mode, err)
		}
	}
	if executed {
		t.Fatal("advanced local-root seam reached command execution")
	}
}

func TestAgentHostLocalModesSkipIdentityCheckAndUseLocalRoot(t *testing.T) {
	oldExec := hostExecRunner
	t.Cleanup(func() { hostExecRunner = oldExec })
	var launched agentruntime.Command
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		launched = command
		return agentruntime.Result{Output: "model-canary github-canary"}, nil
	}
	root := t.TempDir()
	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
	t.Setenv("GITHUB_TOKEN", "github-canary")
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"run", boundaryCommand{Name: "git", Args: []string{"-C", root, "rev-parse", "HEAD"}, Dir: root, Env: []string{"MODEL_API_KEY=model-canary", "GITHUB_TOKEN=github-canary", "PATH=/bin"}}})
	current, err := user.Current()
	if err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"implementation", "review"} {
		t.Run(mode, func(t *testing.T) {
			var out bytes.Buffer
			if err := agentHost(t.Context(), mode, bytes.NewReader(payload), &out); err != nil {
				t.Fatal(err)
			}
			var result agentruntime.Result
			if err := json.Unmarshal(out.Bytes(), &result); err != nil || strings.Contains(result.Output, "model-canary") || strings.Contains(result.Output, "github-canary") || !slices.Contains(launched.Env, "MODEL_API_KEY=model-canary") || !slices.Contains(launched.Env, "GITHUB_TOKEN=github-canary") || !slices.Contains(launched.Env, "HOME="+current.HomeDir) {
				t.Fatal("local host boundary did not deliver and redact its filtered credential environment")
			}
		})
	}
}

func TestAgentHostLocalModeVerifyProvisionsPrivateRoot(t *testing.T) {
	root := filepath.Join(t.TempDir(), "attempts")
	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
	payload, _ := json.Marshal(struct {
		Operation string          `json:"operation"`
		Command   boundaryCommand `json:"command"`
	}{"verify", boundaryCommand{}})
	var out bytes.Buffer
	if err := agentHost(t.Context(), "implementation", bytes.NewReader(payload), &out); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(root)
	if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("local root not provisioned: %v %#v", err, info)
	}
}

func TestHostDiagnosticFallsBackToLocalModeWhenNotInstalled(t *testing.T) {
	fakeNoHostIsolation(t)
	stateRoot := t.TempDir()
	d := hostDiagnostic(stateRoot)
	if d.Status != "pass" || !strings.Contains(d.Message, "zero-admin") {
		t.Fatalf("diagnostic=%#v", d)
	}
	for _, root := range []string{localAttemptRoot(stateRoot), localSnapshotRoot(stateRoot)} {
		info, err := os.Stat(root)
		if err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
			t.Fatalf("local root %s not provisioned: %v", root, err)
		}
	}
}

func TestLocalHostDiagnosticRequiresRuntimeStateRoot(t *testing.T) {
	fakeNoHostIsolation(t)
	if d := hostDiagnostic(""); d.Status != "fail" {
		t.Fatalf("expected fail without a runtime state root, got %#v", d)
	}
}

func TestEnsureLocalRootRejectsGroupOrWorldAccessibleDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attempts")
	if err := os.Mkdir(path, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := ensureLocalRoot(path); err == nil {
		t.Fatal("expected rejection of group-accessible local root")
	}
}

func TestEnsureLocalRootRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "real")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := ensureLocalRoot(link); err == nil {
		t.Fatal("expected rejection of symlinked local root")
	}
}
