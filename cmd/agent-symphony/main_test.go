package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func writeStatusSnapshot(stateRoot string, statuses []orchestrator.RecoveryStatus) error {
	return writeDashboardStatusSnapshot(stateRoot, dashboardStatusSnapshot{UpdatedAt: time.Now().UTC(), Statuses: statuses})
}

func recordReconcileFailure(stateRoot, repository string, cause error) error {
	server := dashboardServer{stateRoot: stateRoot, repository: repository}
	snapshot, err := server.readStatus()
	if errors.Is(err, os.ErrNotExist) {
		snapshot = dashboardStatusSnapshot{}
	} else if err != nil {
		return err
	}
	snapshot.ReconciliationError = internalgithub.Redact(cause.Error())
	snapshot.ReconciliationErrorAt = time.Now().UTC()
	return writeDashboardStatusSnapshot(stateRoot, snapshot)
}

func writeProjectStatusSnapshot(stateRoot, repository string, statuses []orchestrator.RecoveryStatus) error {
	if slices.ContainsFunc(statuses, func(status orchestrator.RecoveryStatus) bool { return status.Repository != repository }) {
		return errors.New("refusing to write a cross-project status projection")
	}
	return writeStatusSnapshot(stateRoot, statuses)
}

const completeImplementationIssueBody = "## Context\nreason and evidence\n## Acceptance criteria\n- result\n## Checklist\n- [ ] implement\n## Validation\ngo test ./...\n## Dependencies\nNone.\n"

func TestEffectiveServeInterval(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want time.Duration
	}{
		{"configured", nil, 20 * time.Second},
		{"override", []string{"--interval=7s"}, 7 * time.Second},
		{"explicit default override", []string{"--interval=60s"}, 60 * time.Second},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := flag.NewFlagSet("serve", flag.ContinueOnError)
			override := fs.Duration("interval", 60*time.Second, "")
			if err := fs.Parse(test.args); err != nil {
				t.Fatal(err)
			}
			if got := effectiveServeInterval(fs, 20, *override); got != test.want {
				t.Fatalf("interval = %s, want %s", got, test.want)
			}
		})
	}
}

func TestRuntimeStateBindsExactlyOneProject(t *testing.T) {
	root := filepath.Join(t.TempDir(), "state")
	if err := bindDeployment(root, "owner/first"); err != nil {
		t.Fatal(err)
	}
	if err := bindDeployment(root, "owner/first"); err != nil {
		t.Fatalf("same project binding failed: %v", err)
	}
	if err := bindDeployment(root, "owner/second"); err == nil || !strings.Contains(err.Error(), "bound to project owner/first") {
		t.Fatalf("cross-project binding error = %v", err)
	}
	info, err := os.Stat(filepath.Join(root, "deployment.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("deployment identity mode=%v", info.Mode().Perm())
	}
}

func TestRuntimeStateRefusesForeignExistingProjectBeforeBinding(t *testing.T) {
	root := t.TempDir()
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{{Repository: "owner/first", Issue: 1, Attempt: 1}}); err != nil {
		t.Fatal(err)
	}
	if err := bindDeployment(root, "owner/second"); err == nil || !strings.Contains(err.Error(), "another project") {
		t.Fatalf("foreign existing status binding error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "deployment.json")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign state was bound: %v", err)
	}
}

func TestDeploymentBindingRecoversFromInterruptedImmutableWriteAndInstall(t *testing.T) {
	origCreate, origWrite, origFileSync, origInstall, origDirSync := immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync
	t.Cleanup(func() {
		immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync = origCreate, origWrite, origFileSync, origInstall, origDirSync
	})
	for _, stage := range []string{"write", "install"} {
		t.Run(stage, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "state")
			immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync = origCreate, origWrite, origFileSync, origInstall, origDirSync
			if stage == "write" {
				immutableWrite = func(f *os.File, body []byte) error {
					_, _ = f.Write(body[:len(body)/2])
					return errors.New("injected interrupted write")
				}
			} else {
				immutableInstall = func(string, string) error { return errors.New("injected interrupted install") }
			}
			if err := bindDeployment(root, "owner/repo"); err == nil {
				t.Fatal("interrupted deployment binding succeeded")
			}
			if _, err := os.Lstat(filepath.Join(root, "deployment.json")); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("partial deployment identity exposed: %v", err)
			}
			immutableCreate, immutableWrite, immutableFileSync, immutableInstall, immutableDirSync = origCreate, origWrite, origFileSync, origInstall, origDirSync
			if err := bindDeployment(root, "owner/repo"); err != nil {
				t.Fatalf("restart recovery: %v", err)
			}
		})
	}
}

func TestServeRepositoryVerificationFailureDoesNotBindDeployment(t *testing.T) {
	root := gitRepository(t)
	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, config.Default("owner/missing")); err != nil {
		t.Fatal(err)
	}
	oldAPI, oldClient := githubAPI, githubClient
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	githubAPI = "https://example.invalid"
	githubClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		status, response := http.StatusOK, any(map[string]any{"id": 42, "login": "coordinator"})
		if r.URL.Path == "/repos/owner/missing" {
			status, response = http.StatusNotFound, map[string]any{"message": "not found"}
		} else if r.URL.Path != "/user" {
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		body, _ := json.Marshal(response)
		return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}
	stateRoot := filepath.Join(root, "runtime-state")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"serve", "--config", configPath, "--state", filepath.Join(root, "state.json"), "--runtime-state", stateRoot}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "verify GitHub repository") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if _, err := os.Lstat(stateRoot); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed repository verification mutated runtime state: %v", err)
	}
}

func TestForeignBoundDeploymentRemainsUnchanged(t *testing.T) {
	root := gitRepository(t)
	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, config.Default("owner/second")); err != nil {
		t.Fatal(err)
	}
	stateRoot := filepath.Join(root, "runtime-state")
	if err := bindDeployment(stateRoot, "owner/first"); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(filepath.Join(stateRoot, "deployment.json"))
	if err != nil {
		t.Fatal(err)
	}
	oldAPI, oldClient := githubAPI, githubClient
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	githubAPI = "https://example.invalid"
	githubClient = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var response any
		switch r.URL.Path {
		case "/user":
			response = map[string]any{"id": 42, "login": "coordinator"}
		case "/repos/owner/second":
			response = map[string]any{"full_name": "owner/second", "permissions": map[string]any{"pull": true}}
		default:
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		body, _ := json.Marshal(response)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}
	var stdout, stderr bytes.Buffer
	if code := run([]string{"status", "--config", configPath, "--state", filepath.Join(root, "state.json"), "--runtime-state", stateRoot}, &stdout, &stderr); code != 1 || !strings.Contains(stderr.String(), "runtime owner deployment is unavailable") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	after, err := os.ReadFile(filepath.Join(stateRoot, "deployment.json"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(after, before) || len(entries) != 1 || entries[0].Name() != "deployment.json" {
		t.Fatalf("foreign deployment changed: entries=%v identity=%q", entries, after)
	}
}

type cleanupBoundary struct {
	manifests  []agentruntime.Manifest
	operations []string
	err        error
}

func TestAuthorizedHumanInstructionsAmendIndependentReviewContractInOrder(t *testing.T) {
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 186, Attempt: 1, Body: "Commit the repository YAML."}
	now := time.Date(2026, 8, 31, 6, 0, 0, 0, time.UTC)
	feedback := []internalgithub.Feedback{
		{ID: 11, Source: "issue", Body: "Issue comment instruction.", CreatedAt: now, Authorized: true},
		{ID: 12, Source: "conversation", Body: "PR conversation instruction.", CreatedAt: now.Add(time.Minute), Authorized: true},
		{ID: 13, Source: "inline", Body: "Unauthorized.", CreatedAt: now.Add(4 * time.Minute)},
	}

	amended, instructions := amendIssueWithHumanInstructions(issue, feedback)
	if len(instructions) != 2 || !strings.Contains(instructions[0], feedback[0].Body) || !strings.Contains(instructions[1], feedback[1].Body) {
		t.Fatalf("instructions=%#v", instructions)
	}
	prompt, err := reviewPrompt(agentruntime.ReviewModeImplementation, strings.Repeat("a", 40)+".."+strings.Repeat("b", 40), amended)
	if err != nil {
		t.Fatal(err)
	}
	original, precedence, firstFeedback, laterFeedback := strings.Index(prompt, issue.Body), strings.Index(prompt, humanInstructionPrecedence), strings.Index(prompt, feedback[0].Body), strings.Index(prompt, feedback[1].Body)
	if original < 0 || precedence <= original || firstFeedback <= precedence || laterFeedback <= firstFeedback || strings.Contains(prompt, feedback[2].Body) {
		t.Fatalf("review prompt did not preserve human instruction precedence: %q", prompt)
	}
}

func TestMissingTmuxReviewerPaneIsTheExactPrelaunchStatus(t *testing.T) {
	if !missingTmuxPaneStatus(agentruntime.Result{Output: "|||\n"}) {
		t.Fatal("tmux missing-target output was not recognized")
	}
	for _, result := range []agentruntime.Result{{Output: "0|||\n"}, {Output: "1|0||\n"}, {Output: "|||\n", Exited: true, Code: 1}} {
		if missingTmuxPaneStatus(result) {
			t.Fatalf("live, dead, or failed tmux result was treated as missing: %#v", result)
		}
	}
}

func acknowledgeHandoffLaunch(command agentruntime.Command) (string, error) {
	index := slices.Index(command.Args, "worker-capture-handoff-ready")
	if index < 0 || index+5 >= len(command.Args) {
		return "", nil
	}
	recipient := command.Args[index+5]
	return recipient, writeImmutable(command.Args[index+4], []byte(recipient))
}

func TestHelpListsUserFacingCommandsAndFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--help"}, &stdout, &stderr); code != 0 || stderr.Len() != 0 {
		t.Fatalf("help code=%d stderr=%q", code, stderr.String())
	}
	for _, want := range []string{
		"install-host", "agent-host", "chat", "control", "init", "validate", "config view", "serve", "status", "list", "inspect", "reconcile", "doctor", "diagnostics", "pr-governance", "help",
		"--config", "--state", "--runtime-state", "--attempts", "--issue", "--attempt", "--repository", "--role", "--action", "--confirm", "--request-id", "--timeout", "--interval", "--dashboard-address", "--allow-unsafe-dashboard-network", "--dashboard-password-file", "--offline", "--coordinator", "--json", "--help", "--version",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("help is missing %q", want)
		}
	}
}

func TestCoordinatorCLICommandContractParsesWithExactPlaceholders(t *testing.T) {
	commands := orchestratoragent.CoordinatorCLICommands("o/r", filepath.Join(t.TempDir(), "missing-runtime"))
	if len(commands) != 13 {
		t.Fatalf("commands=%d", len(commands))
	}
	for _, command := range commands {
		parsed := slices.Clone(command[1:])
		for index := range parsed {
			switch parsed[index] {
			case "<issue>":
				parsed[index] = "23"
			case "<attempt>":
				parsed[index] = "4"
			case "<request-id>":
				parsed[index] = "coordinator-test"
			}
		}
		var stdout, stderr bytes.Buffer
		if code := run(parsed, &stdout, &stderr); code == 2 {
			t.Errorf("fixed command does not parse: %q stderr=%q", command, stderr.String())
		}
	}
}

func TestImplementationChatTargetRequiresOneRunningCurrentSession(t *testing.T) {
	running, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 214, 2)
	completed, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 214, 1)
	history := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 214, Attempt: 1, State: "active", CurrentPhase: "validation", Action: "validate the completed implementation result", Session: completed, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: completed, State: "completed"}}}
	active := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 214, Attempt: 2, State: "active", CurrentPhase: "implementation", Action: "monitor the implementation session", Session: running, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: running, State: "running", Current: true}}}

	status, session, err := implementationChatTarget([]orchestrator.RecoveryStatus{history, active}, 214)
	if err != nil || status.Attempt != 2 || session.Name != running {
		t.Fatalf("status=%#v session=%#v err=%v", status, session, err)
	}
	for name, statuses := range map[string][]orchestrator.RecoveryStatus{
		"missing":  nil,
		"inactive": {history},
		"ambiguous": {active, func() orchestrator.RecoveryStatus {
			other := active
			other.Attempt = 3
			other.Session, _ = agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 214, 3)
			other.Sessions[0].Name = other.Session
			return other
		}()},
	} {
		t.Run(name, func(t *testing.T) {
			_, _, err := implementationChatTarget(statuses, 214)
			if err == nil || name == "inactive" && (!strings.Contains(err.Error(), completed) || !strings.Contains(err.Error(), "completed") || !strings.Contains(err.Error(), history.Action)) {
				t.Fatalf("error=%v", err)
			}
		})
	}
}

func TestChatIssueExchangesInputWithExactTmuxSessionAndReportsMissingRuntime(t *testing.T) {
	root := t.TempDir()
	if err := bindDeployment(root, "o/r"); err != nil {
		t.Fatal(err)
	}
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 214, 2)
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 214, Attempt: 2, State: "active", CurrentPhase: "implementation", Action: "monitor the implementation session", Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "running", Current: true}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	tmux := filepath.Join(bin, "tmux")
	script := "#!/bin/sh\ncase $1 in\n" +
		"display-message) test \"$2\" = -p && test \"$3\" = -t && test \"$4\" = \"=$EXPECTED_SESSION:0.0\" && test \"$5\" = '#{pane_dead}' && printf '0\\n';;\n" +
		"attach-session) test \"$2\" = -t && test \"$3\" = \"=$EXPECTED_SESSION\" || exit 2; IFS= read -r input; printf 'agent:%s\\n' \"$input\";;\n" +
		"*) exit 2;;\nesac\n"
	if err := os.WriteFile(tmux, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("EXPECTED_SESSION", session)
	t.Setenv("TMUX_TMPDIR", "")
	var stdout, stderr bytes.Buffer
	if err := chatIssue(root, 214, strings.NewReader("hello agent\n"), &stdout, &stderr); err != nil {
		t.Fatal(err)
	}
	if stdout.String() != "agent:hello agent\n" || !strings.Contains(stderr.String(), session) || !strings.Contains(stderr.String(), "Ctrl-b d") {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
	if got := os.Getenv("TMUX_TMPDIR"); got != projectTmuxRoot(root) {
		t.Fatalf("TMUX_TMPDIR=%q, want %q", got, projectTmuxRoot(root))
	}

	if err := os.WriteFile(tmux, []byte("#!/bin/sh\nif test \"$1\" = display-message; then exit 1; fi\nprintf attached >\"$ATTACHED\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	attached := filepath.Join(t.TempDir(), "attached")
	t.Setenv("ATTACHED", attached)
	if err := chatIssue(root, 214, strings.NewReader("must not be sent\n"), io.Discard, io.Discard); err == nil || !strings.Contains(err.Error(), session) || !strings.Contains(err.Error(), "reconcile project runtime state") {
		t.Fatalf("missing runtime error=%v", err)
	}
	if _, err := os.Stat(attached); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing session reached attach: %v", err)
	}
}

func TestChatIssueRejectsStatusFromForeignRepositoryBeforeTmux(t *testing.T) {
	root := t.TempDir()
	if err := bindDeployment(root, "owner/local"); err != nil {
		t.Fatal(err)
	}
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "owner/foreign", 214, 1)
	status := orchestrator.RecoveryStatus{Repository: "owner/foreign", Issue: 214, Attempt: 1, State: "active", CurrentPhase: "implementation", Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "running", Current: true}}}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	bin := t.TempDir()
	called := filepath.Join(t.TempDir(), "tmux-called")
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte("#!/bin/sh\ntouch \"$TMUX_CALLED\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", bin+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TMUX_CALLED", called)

	err := chatIssue(root, 214, strings.NewReader("must not be sent\n"), io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "status snapshot contains another project") {
		t.Fatalf("foreign repository error=%v", err)
	}
	if _, err := os.Stat(called); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("foreign repository reached tmux: %v", err)
	}
}

func TestTransitionRetryOnlyAcceptsExactUnblockedCompletedPublication(t *testing.T) {
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 131, Attempt: 3, Action: orchestratoragent.ProposalActionRetry, RequestID: "retry-1"}
	valid := orchestrator.RecoveryStatus{
		Repository:         proposal.Repository,
		Issue:              proposal.Issue,
		Attempt:            proposal.Attempt,
		State:              "active",
		CurrentPhase:       "publication",
		DispatchAuthorized: true,
		Sessions:           []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, State: "completed"}},
	}
	if err := validateTransitionRetry(proposal, []orchestrator.RecoveryStatus{valid}); err != nil {
		t.Fatalf("valid transition retry rejected: %v", err)
	}
	for name, mutate := range map[string]func(*orchestrator.RecoveryStatus){
		"stale attempt":         func(status *orchestrator.RecoveryStatus) { status.Attempt++ },
		"authorization revoked": func(status *orchestrator.RecoveryStatus) { status.DispatchAuthorized = false },
		"blocked":               func(status *orchestrator.RecoveryStatus) { status.Blockers = []string{"authorization revoked"} },
		"running worker":        func(status *orchestrator.RecoveryStatus) { status.Sessions[0].State = "running" },
		"unrelated transition":  func(status *orchestrator.RecoveryStatus) { status.CurrentPhase = "review" },
		"terminal":              func(status *orchestrator.RecoveryStatus) { status.State = "completed" },
	} {
		t.Run(name, func(t *testing.T) {
			status := valid
			status.Sessions = slices.Clone(valid.Sessions)
			mutate(&status)
			if err := validateTransitionRetry(proposal, []orchestrator.RecoveryStatus{status}); err == nil {
				t.Fatal("unsafe transition retry accepted")
			}
		})
	}
}

func (b *cleanupBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "cleanup" && operation != "abandon" {
		return agentruntime.Result{}, fmt.Errorf("unexpected operation %q", operation)
	}
	var manifest agentruntime.Manifest
	if err := json.NewDecoder(command.Stdin).Decode(&manifest); err != nil {
		return agentruntime.Result{}, err
	}
	b.manifests = append(b.manifests, manifest)
	b.operations = append(b.operations, operation)
	return agentruntime.Result{}, b.err
}

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestImplementationPromptDefinesAcceptedResultAndPreservesIssue(t *testing.T) {
	issue := internalgithub.RecoveryIssueFact{Repository: "owner/repo", Issue: 56, Attempt: 3, BaseBranch: "main", Body: "## Context\nunique issue contract\n\n## Validation\ngo test ./..."}
	identity := agentruntime.Manifest{Branch: "agent-symphony/owner-repo/56-3", Worktree: "/attempts/owner-repo-56-3", Session: "as-owner-repo-56-3"}
	for _, interactive := range []bool{false, true} {
		prompt := implementationPrompt(issue, identity, interactive)
		for _, want := range []string{"Project: owner/repo", "Issue: #56", "Attempt: 3", "Base branch: main", "Branch: " + identity.Branch, "Worktree: " + identity.Worktree, "Session: " + identity.Session, issue.Body, "exactly one JSON line", "at most 64 KiB", "nonempty validation and documentation", "installed gh CLI", "/agent-symphony status needs-attention: REASON", "/agent-symphony status clear: REASON", "`needs-attention` label", "Re-read both the comment and label", "updated directly with gh", "partial-update errors are failures, never success"} {
			if !strings.Contains(prompt, want) {
				t.Fatalf("interactive=%v prompt omitted %q: %s", interactive, want, prompt)
			}
		}
		if interactive && (!strings.Contains(prompt, agentruntime.WorkerResultEnvironment) || !strings.Contains(prompt, "operator-visible conversation") || strings.Contains(prompt, "Make stdout")) {
			t.Fatalf("interactive completion contract is not direct-chat safe: %s", prompt)
		}
		if !interactive && (!strings.Contains(prompt, "Make stdout") || !strings.Contains(prompt, "outside the worktree")) {
			t.Fatalf("captured completion contract changed: %s", prompt)
		}
		if strings.Count(prompt, issue.Body) != 1 || strings.Contains(prompt, "GITHUB_TOKEN") || strings.Contains(prompt, "PRIVATE_KEY") {
			t.Fatalf("prompt changed issue content or named a credential: %s", prompt)
		}
		line := prompt[strings.LastIndex(prompt, "\n")+1:]
		result, err := parseWorkerResult([]byte(line))
		if err != nil || result.Validation == "" || result.Documentation == "" {
			t.Fatalf("documented result was rejected: %#v, %v", result, err)
		}
	}
}

func TestReviewPromptExposesTheSameDirectStatusContract(t *testing.T) {
	prompt, err := reviewPrompt(agentruntime.ReviewModeImplementation, strings.Repeat("a", 40)+".."+strings.Repeat("b", 40), internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 56, Attempt: 3, Body: "review contract"})
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"/agent-symphony status needs-attention: REASON", "/agent-symphony status clear: REASON", "`needs-attention` label", "nonempty reason", "fresh re-read", "partial-update errors are failures, never success"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("review prompt omitted %q: %s", want, prompt)
		}
	}
}

func TestIssueContractControlsDispatch(t *testing.T) {
	now := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	cfg := internalgithub.ContractConfig{Ready: "ready", P1: "P1", P2: "P2", P3: "P3", DependencySection: "Dependencies"}
	for _, test := range []struct {
		name, body string
		state      orchestrator.State
	}{
		{"incomplete issue is deferred", "implement from chat", orchestrator.Blocked},
		{"complete issue is eligible", completeImplementationIssueBody, orchestrator.Runnable},
	} {
		t.Run(test.name, func(t *testing.T) {
			normalized := internalgithub.NormalizeIssue(internalgithub.IssueInput{Number: 211, State: "open", Body: test.body, Labels: []string{"ready", "P1"}}, cfg, nil)
			issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 211, Priority: normalized.Controls.Priority, CreatedAt: now, Blockers: normalized.Blockers, Eligible: normalized.Ready}
			_, decisions := joinIssueProjection(nil, []internalgithub.RecoveryIssueFact{issue}, 1)
			if len(decisions) != 1 || decisions[0].State != test.state {
				t.Fatalf("decisions = %#v, want %s", decisions, test.state)
			}
		})
	}
}

func TestWorkerCaptureInternalCLIAndHandoffPreStartRecovery(t *testing.T) {
	dir := t.TempDir()
	prompt := filepath.Join(dir, "prompt")
	if err := os.WriteFile(prompt, []byte("issue prompt"), 0o600); err != nil {
		t.Fatal(err)
	}
	tmux := filepath.Join(dir, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\ntest \"$PWD\" = /tmp || exit 3\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer|set-option) exit 0;; wait-for) test -z \"$FAKE_SIGNAL_FAILURE\" || exit 1; printf %s \"$3\" >\"$FAKE_SIGNAL\";; *) exit 2;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_PROMPT", prompt)
	t.Setenv("TMUX_PANE", "%7")
	signalPath := filepath.Join(dir, "launch-signal")
	t.Setenv("FAKE_SIGNAL", signalPath)
	t.Setenv("TMPDIR", filepath.Join(dir, "inaccessible"))
	resultPath := filepath.Join(dir, "attempt.result.json")
	result := `{"type":"agent-symphony-result-v1","validation":"ok","documentation":"none"}`
	var stdout, stderr bytes.Buffer
	code := run([]string{"worker-capture", tmux, "prompt-buffer", resultPath, "--", "sh", "-c", `test "$(cat)" = "$1" && test "$TMPDIR" = /tmp && printf %s "$2"`, "consumer", "issue prompt", result}, &stdout, &stderr)
	if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got, err := os.ReadFile(resultPath); err != nil || string(got) != result {
		t.Fatalf("result=%q err=%v", got, err)
	}
	replacement := `{"type":"agent-symphony-result-v1","validation":"follow-up passed","documentation":"none"}`
	code = run([]string{"worker-capture-replace", tmux, "prompt-buffer", resultPath, "--", "sh", "-c", `printf %s "$1"`, "consumer", replacement}, &stdout, &stderr)
	if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("replacement code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got, err := os.ReadFile(resultPath); err != nil || string(got) != replacement {
		t.Fatalf("replacement result=%q err=%v", got, err)
	}
	launchedPath := filepath.Join(dir, "handoff.launched")
	code = run([]string{"worker-capture-handoff", tmux, "prompt-buffer", resultPath, launchedPath, "recipient", "signal-name", "--", "agent-symphony-missing-worker-command"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("pre-start handoff failure succeeded")
	}
	for _, path := range []string{launchedPath, signalPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("pre-start failure acknowledged launch at %s: %v", path, err)
		}
	}
	code = run([]string{"worker-capture-handoff-ready", tmux, "prompt-buffer", resultPath, launchedPath, "recipient", "signal-name", "--", "agent-symphony-missing-worker-command"}, &stdout, &stderr)
	if code == 0 {
		t.Fatal("ready-protocol pre-start failure succeeded")
	}
	if _, err := os.Stat(launchedPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ready-protocol failure acknowledged launch: %v", err)
	}
	if got, err := os.ReadFile(signalPath); err != nil || string(got) != "signal-name" {
		t.Fatalf("ready-protocol failure did not wake coordinator: %q %v", got, err)
	}
	if err := os.Remove(signalPath); err != nil {
		t.Fatal(err)
	}
	stderr.Reset()
	t.Setenv("FAKE_SIGNAL_FAILURE", "1")
	code = run([]string{"worker-capture-handoff", tmux, "prompt-buffer", resultPath, launchedPath, "recipient", "signal-name", "--", "sh", "-c", `printf %s "$1"`, "consumer", result}, &stdout, &stderr)
	if code == 0 || !strings.Contains(stderr.String(), "launch signal") {
		t.Fatalf("signal failure code=%d stderr=%q", code, stderr.String())
	}
	for _, path := range []string{launchedPath, signalPath} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("signal failure retained launch handshake at %s: %v", path, err)
		}
	}
	if got, err := os.ReadFile(resultPath); err != nil || string(got) != replacement {
		t.Fatalf("signal failure replaced prior result=%q err=%v", got, err)
	}
	t.Setenv("FAKE_SIGNAL_FAILURE", "")
	stderr.Reset()
	code = run([]string{"worker-capture-handoff", tmux, "prompt-buffer", resultPath, launchedPath, "recipient", "signal-name", "--", "sh", "-c", `printf %s "$1"`, "consumer", result}, &stdout, &stderr)
	if code != 0 || stdout.Len() != 0 || stderr.Len() != 0 {
		t.Fatalf("handoff code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if got, err := os.ReadFile(launchedPath); err != nil || string(got) != "recipient" {
		t.Fatalf("launch marker=%q err=%v", got, err)
	}
	if got, err := os.ReadFile(signalPath); err != nil || string(got) != "signal-name" {
		t.Fatalf("launch signal=%q err=%v", got, err)
	}
}

func TestConfigureProjectTmuxUsesPrivateProjectNamespace(t *testing.T) {
	stateRoot := t.TempDir()
	t.Setenv("TMUX_TMPDIR", "previous")
	if err := configureProjectTmux(stateRoot); err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(stateRoot, "tmux")
	if got := os.Getenv("TMUX_TMPDIR"); got != want {
		t.Fatalf("TMUX_TMPDIR=%q, want %q", got, want)
	}
	if info, err := os.Lstat(want); err != nil || !info.IsDir() || info.Mode().Perm() != 0o700 {
		t.Fatalf("project tmux root=%v err=%v", info, err)
	}
}

func TestConfigureAgentCodexHomeLinksCapabilitiesAndIsolatesRuntimeState(t *testing.T) {
	source, stateRoot := t.TempDir(), t.TempDir()
	for _, name := range agentCodexAssets {
		path := filepath.Join(source, name)
		if name == "skills" || name == "plugins" || name == "cache" || name == "rules" {
			if err := os.Mkdir(path, 0o700); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := os.WriteFile(path, []byte(name), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("CODEX_HOME", source)
	if err := configureAgentCodexHome(stateRoot); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(stateRoot, "codex-home")
	if got := os.Getenv("CODEX_HOME"); got != target {
		t.Fatalf("CODEX_HOME=%q, want %q", got, target)
	}
	for _, name := range agentCodexAssets {
		if got, err := os.Readlink(filepath.Join(target, name)); err != nil || got != filepath.Join(source, name) {
			t.Fatalf("capability %s link=%q err=%v", name, got, err)
		}
	}
	if _, err := os.Lstat(filepath.Join(target, "thread-writer-locks")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mutable Codex runtime state was shared: %v", err)
	}
}

func TestWorkerBoundaryCarriesGitHubCredentialsOnlyInBoundedInput(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "boundary")
	body := "#!/bin/sh\ntest -z \"$GITHUB_TOKEN$GH_TOKEN$MODEL_TOKEN\" || exit 9\npayload=$(dd bs=1048576 count=1 2>/dev/null)\ncase \"$payload\" in *GITHUB_TOKEN=*GH_TOKEN=*) printf '{\"Output\":\"bounded\",\"Code\":0,\"Exited\":false}' ;; *) exit 8 ;; esac\n"
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GITHUB_TOKEN", "github-canary")
	t.Setenv("GH_TOKEN", "gh-canary")
	t.Setenv("MODEL_TOKEN", "model-canary")
	result, err := (workerBoundaryRunner{Command: script}).call(context.Background(), "run", agentruntime.Command{Env: []string{"GITHUB_TOKEN=github-canary", "GH_TOKEN=gh-canary"}})
	if err != nil || result.Output != "bounded" {
		t.Fatalf("result=%#v err=%v", result, err)
	}
}

func TestWorkerBoundaryReturnsBoundedRedactedDiagnostic(t *testing.T) {
	dir := t.TempDir()
	script := filepath.Join(dir, "boundary")
	if err := os.WriteFile(script, []byte("#!/bin/sh\ncat >/dev/null\ndd if=/dev/zero bs=1024 count=60 2>/dev/null | tr '\\000' x >&2\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	_, err := (workerBoundaryRunner{Command: script}).call(t.Context(), "run", agentruntime.Command{Env: []string{"GITHUB_TOKEN=x"}})
	if err == nil || !strings.Contains(err.Error(), "[REDACTED]") || strings.Contains(err.Error(), strings.Repeat("x", 32)) || len(err.Error()) > workerBoundaryDiagnosticLimit+256 {
		t.Fatalf("boundary diagnostic = %q", err)
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

func TestProjectRecoveryStatusesUsesCurrentManifestAndFreshRemoteFacts(t *testing.T) {
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 23, 1)
	head := strings.Repeat("b", 40)
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: strings.Repeat("a", 40), ReviewHead: head, State: "completed", ReviewState: "clean", Session: implementation}
	fact := orchestrator.AttemptFact{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: manifest.BaseSHA, HeadSHA: head, PR: 7, State: "active", Checks: []string{"ci:success"}}
	statuses, _ := projectRecoveryStatuses(t.Context(), []orchestrator.AttemptFact{fact}, []internalgithub.RecoveryIssueFact{{Repository: "o/r", Issue: 23, Active: true}}, []agentruntime.Manifest{manifest}, 1, nil)
	if len(statuses) != 1 || statuses[0].CurrentPhase != "publication" || statuses[0].PR != 7 || !slices.Equal(statuses[0].Checks, []string{"ci:success"}) || len(statuses[0].Sessions) != 1 {
		t.Fatalf("fresh projection=%#v", statuses)
	}
}

func TestPublishedAttemptSuppressesLocalTerminalIssueBlockerOnlyOnExactIdentity(t *testing.T) {
	baseIssue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1, Eligible: true}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", State: "completed", ReviewState: "clean", ReviewHead: "bbbbbbb"}
	exact := orchestrator.AttemptFact{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", PR: 7, State: "review-ready"}
	for _, test := range []struct {
		name    string
		state   string
		base    string
		head    string
		pr      int
		blocked bool
	}{
		{name: "published active", state: "active", base: "aaaaaaa", head: "bbbbbbb", pr: 7},
		{name: "published review ready", state: "review-ready", base: "aaaaaaa", head: "bbbbbbb", pr: 7},
		{name: "mismatched base", state: "review-ready", base: "wrong00", head: "bbbbbbb", pr: 7, blocked: true},
		{name: "force pushed head", state: "review-ready", base: "aaaaaaa", head: "force00", pr: 7, blocked: true},
		{name: "unbound", state: "review-ready", base: "aaaaaaa", head: "bbbbbbb", blocked: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			issues := []internalgithub.RecoveryIssueFact{baseIssue}
			fact := exact
			fact.State, fact.BaseSHA, fact.HeadSHA, fact.PR = test.state, test.base, test.head, test.pr
			addTerminalAttemptBlockers(issues, []agentruntime.Manifest{manifest}, []orchestrator.AttemptFact{fact})
			if got := slices.Contains(issues[0].Blockers, "local terminal attempt awaits or has durable GitHub outcome"); got != test.blocked || issues[0].Eligible == test.blocked {
				t.Fatalf("issue=%#v", issues[0])
			}
		})
	}
	paired := []internalgithub.RecoveryIssueFact{{Repository: "o/r", Issue: 23, Attempt: 2, Eligible: true, RecoveryAuthorized: true, RecoveryAttempt: 1, TerminalAttempts: []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", State: "failed"}}}}
	addTerminalAttemptBlockers(paired, []agentruntime.Manifest{manifest}, []orchestrator.AttemptFact{{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", State: "failed"}})
	if len(paired[0].Blockers) != 0 || !paired[0].RecoveryAuthorized {
		t.Fatalf("paired terminal was blocked: %#v", paired[0])
	}
}

func TestCompletedAttemptCleanupRequiresExactPublishedIdentity(t *testing.T) {
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", State: "completed", ReviewHead: "bbbbbbb"}
	exact := orchestrator.AttemptFact{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", PR: 7, State: "completed"}
	boundary := &cleanupBoundary{}
	facts := []orchestrator.AttemptFact{
		{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", PR: 7, State: "review-ready"},
		{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", HeadSHA: "wrong00", PR: 7, State: "completed"},
		exact,
	}
	if err := cleanupCompletedAttempts(t.Context(), boundary, facts, []agentruntime.Manifest{manifest}); err != nil {
		t.Fatal(err)
	}
	if len(boundary.manifests) != 1 || !reflect.DeepEqual(boundary.manifests[0], manifest) {
		t.Fatalf("cleaned manifests = %#v", boundary.manifests)
	}
	if !slices.Equal(boundary.operations, []string{"cleanup"}) {
		t.Fatalf("cleanup operations = %v", boundary.operations)
	}

	boundary.err = errors.New("injected cleanup failure")
	if err := cleanupCompletedAttempts(t.Context(), boundary, []orchestrator.AttemptFact{exact}, []agentruntime.Manifest{manifest}); err == nil || !strings.Contains(err.Error(), "o/r#23 attempt 1") {
		t.Fatalf("cleanup failure = %v", err)
	}
}

func TestReviewModesBindExactTargetsAndStatusPermissions(t *testing.T) {
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 2, Body: "## Plan\nShip the bounded change."}
	base, head := strings.Repeat("a", 40), strings.Repeat("b", 40)
	plan, err := reviewTarget(agentruntime.ReviewModePlan, issue, base, base)
	if err != nil {
		t.Fatal(err)
	}
	changed := issue
	changed.Body += "\nNew constraint."
	changedPlan, err := reviewTarget(agentruntime.ReviewModePlan, changed, base, base)
	if err != nil || changedPlan == plan || !validReviewTarget(agentruntime.ReviewModePlan, plan, issue.Repository, issue.Issue, base) {
		t.Fatalf("plan targets were not exact: %q %q err=%v", plan, changedPlan, err)
	}
	implementation, err := reviewTarget(agentruntime.ReviewModeImplementation, issue, base, head)
	if err != nil || implementation != base+".."+head || !validReviewTarget(agentruntime.ReviewModeImplementation, implementation, issue.Repository, issue.Issue, head) {
		t.Fatalf("implementation target=%q err=%v", implementation, err)
	}
	for _, test := range []struct{ mode, target string }{{agentruntime.ReviewModePlan, plan}, {agentruntime.ReviewModeImplementation, implementation}} {
		prompt, err := reviewPrompt(test.mode, test.target, issue)
		if err != nil || !strings.Contains(prompt, "Review mode: "+test.mode) || !strings.Contains(prompt, test.target) || !strings.Contains(prompt, "Use the installed gh CLI") || !strings.Contains(prompt, "/agent-symphony status needs-attention: REASON") || !strings.Contains(prompt, "needs-attention` label") || strings.Contains(prompt, "ui-review") {
			t.Fatalf("%s prompt did not preserve its exact target and permissions: %q err=%v", test.mode, prompt, err)
		}
	}
	if _, err := reviewTarget("ui-review", issue, base, head); err == nil {
		t.Fatal("specialized reviewer mode was accepted")
	}
	if _, err := reviewTarget(agentruntime.ReviewModeImplementation, issue, head, head); err == nil {
		t.Fatal("empty implementation range was accepted")
	}
}

func TestDefaultReviewerProductionShapeUsesExactDiffAndRejectsProse(t *testing.T) {
	dir := t.TempDir()
	codex := filepath.Join(dir, "codex")
	const script = `#!/bin/sh
test "$#" -eq 5 && test "$1" = -c && test "$2" = "projects={$FAKE_REVIEW_WORKSPACE={trust_level=\"trusted\"}}" && test "$3" = --dangerously-bypass-approvals-and-sandbox && test "$4" = --no-alt-screen || exit 20
prompt=$5
printf '%s' "$prompt" | grep -F "$FAKE_REVIEW_BASE..$FAKE_REVIEW_HEAD" >/dev/null || exit 22
diff=$(git -C "$FAKE_REVIEW_REPO" diff --no-ext-diff "$FAKE_REVIEW_BASE" HEAD) || exit 23
printf '%s' "$diff" | grep -F '+first implementation commit' >/dev/null || exit 24
printf '%s' "$diff" | grep -F '+second implementation commit' >/dev/null || exit 25
read operator && test "$operator" = 'inspect the edge' || exit 26
printf x >>"$FAKE_REVIEW_COUNT"
printf '%s' "$FAKE_REVIEW_OUTPUT" >"$AGENT_SYMPHONY_REVIEW_RESULT.tmp"
mv "$AGENT_SYMPHONY_REVIEW_RESULT.tmp" "$AGENT_SYMPHONY_REVIEW_RESULT"
printf 'received:%s\n' "$operator"`
	if err := os.WriteFile(codex, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	source := t.TempDir()
	runGit(t, source, "init")
	runGit(t, source, "config", "user.email", "test@example.invalid")
	runGit(t, source, "config", "user.name", "test")
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("before\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, source, "add", "file")
	runGit(t, source, "commit", "-m", "base")
	base := runGit(t, source, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(source, "first"), []byte("first implementation commit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, source, "add", "first")
	runGit(t, source, "commit", "-m", "first implementation commit")
	if err := os.WriteFile(filepath.Join(source, "second"), []byte("second implementation commit\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, source, "add", "second")
	runGit(t, source, "commit", "-m", "second implementation commit")
	head := runGit(t, source, "rev-parse", "HEAD")
	t.Setenv("FAKE_REVIEW_REPO", source)
	encodedSource, _ := json.Marshal(source)
	t.Setenv("FAKE_REVIEW_WORKSPACE", string(encodedSource))
	t.Setenv("FAKE_REVIEW_BASE", base)
	t.Setenv("FAKE_REVIEW_HEAD", head)
	defaultReviewer, err := config.ExpandManagedWorkspace(config.Default("o/r").Commands.Reviewer, source)
	if err != nil {
		t.Fatal(err)
	}

	for i, test := range []struct {
		name, output string
		wantErr      bool
	}{
		{"strict result", `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`, false},
		{"prose", `No findings.`, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			count := filepath.Join(dir, fmt.Sprintf("count-%d", i))
			t.Setenv("FAKE_REVIEW_COUNT", count)
			t.Setenv("FAKE_REVIEW_OUTPUT", test.output)
			prompt, err := reviewPrompt(agentruntime.ReviewModeImplementation, base+".."+head, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23 + i, Attempt: 1, Body: "Review the change."})
			if err != nil {
				t.Fatal(err)
			}
			resultRoot := filepath.Join(dir, fmt.Sprintf("result-%d", i))
			if err := os.Mkdir(resultRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			resultPath := filepath.Join(resultRoot, "result.json")
			command := exec.Command(defaultReviewer[0], append(defaultReviewer[1:], prompt)...)
			command.Dir, command.Stdin = source, strings.NewReader("inspect the edge\n")
			command.Env = append(os.Environ(), "AGENT_SYMPHONY_REVIEW_RESULT="+resultPath)
			output, err := command.CombinedOutput()
			if err != nil || !strings.Contains(string(output), "received:inspect the edge") {
				t.Fatalf("interactive reviewer output=%q err=%v", output, err)
			}
			body, err := os.ReadFile(resultPath)
			_, parseErr := parseIndependentReview(string(body))
			if err != nil || (parseErr != nil) != test.wantErr {
				t.Fatalf("result=%q read=%v parse=%v", body, err, parseErr)
			}
			if body, err := os.ReadFile(count); err != nil || string(body) != "x" {
				t.Fatalf("default reviewer runs=%q err=%v", body, err)
			}
		})
	}
}

func TestManagedCodexLaunchesCarryExactTrustAndApprovalWithoutPriorState(t *testing.T) {
	dir := t.TempDir()
	fake := filepath.Join(dir, "codex")
	const script = `#!/bin/sh
test "$CODEX_HOME" = "$EXPECTED_CODEX_HOME" || exit 10
trusted=0
bypass=0
never=0
previous=
for argument do
  test "$argument" != "$EXPECTED_TRUST" || trusted=1
  test "$argument" != --dangerously-bypass-approvals-and-sandbox || bypass=1
  if test "$previous" = --ask-for-approval && test "$argument" = never; then never=1; fi
  previous=$argument
done
test "$trusted" -eq 1 || { printf 'Do you trust the contents of this directory?'; exit 20; }
case "$ROLE" in
implementation|review) test "$bypass" -eq 1 || exit 21;;
orchestrator|heartbeat) test "$never" -eq 1 || exit 22;;
*) exit 23;;
esac
printf started`
	if err := os.WriteFile(fake, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	freshHome := filepath.Join(dir, "fresh codex home")
	if err := os.Mkdir(freshHome, 0o700); err != nil {
		t.Fatal(err)
	}
	defaults := config.Default("o/r").Commands
	implementation, interactive := interactiveImplementationCommand(defaults.Implementation)
	if !interactive {
		t.Fatal("default implementation command did not become an interactive session")
	}
	roles := []struct {
		name      string
		command   []string
		workspace string
		promptArg bool
	}{
		{"implementation", implementation, filepath.Join(dir, `implementation path [x] "quoted"`), true},
		{"review", defaults.Reviewer, filepath.Join(dir, `review path [x] "quoted"`), true},
		{"orchestrator", defaults.Orchestrator, filepath.Join(dir, `orchestrator path [x] "quoted"`), true},
		{"heartbeat", defaults.OrchestratorAudit, filepath.Join(dir, `heartbeat path [x] "quoted"`), false},
	}
	for _, role := range roles {
		t.Run(role.name, func(t *testing.T) {
			if err := os.Mkdir(role.workspace, 0o700); err != nil {
				t.Fatal(err)
			}
			command, err := config.ExpandManagedWorkspace(role.command, role.workspace)
			if err != nil {
				t.Fatal(err)
			}
			for index := range command {
				command[index] = strings.ReplaceAll(command[index], "{orchestrator_result}", filepath.Join(role.workspace, "result"))
			}
			command[0] = fake
			if role.promptArg {
				command = append(command, "task")
			}
			encoded, _ := json.Marshal(role.workspace)
			process := exec.Command(command[0], command[1:]...)
			process.Dir = role.workspace
			process.Env = []string{"CODEX_HOME=" + freshHome, "EXPECTED_CODEX_HOME=" + freshHome, "EXPECTED_TRUST=projects={" + string(encoded) + `={trust_level="trusted"}}`, "ROLE=" + role.name}
			process.Stdin = strings.NewReader("task")
			if output, err := process.CombinedOutput(); err != nil || string(output) != "started" {
				t.Fatalf("managed %s startup output=%q err=%v command=%q", role.name, output, err, command)
			}
		})
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

func TestWorkerExportVerifiesRealBundleInIsolatedRepository(t *testing.T) {
	coordinator, worker := t.TempDir(), t.TempDir()
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
	intermediate := runGit(t, worker, "rev-parse", "HEAD")
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("tip"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "commit", "-am", "tip")
	head := runGit(t, worker, "rev-parse", "HEAD")
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(coordinator); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })

	manifest := agentruntime.Manifest{Repository: "o/r", Branch: "issue-61", BaseSHA: base}
	importBundle := func(claimedHead string, revisions ...string) (workerResult, string, string, error) {
		bundlePath := filepath.Join(t.TempDir(), "attempt.bundle")
		runGit(t, worker, append([]string{"bundle", "create", bundlePath}, revisions...)...)
		bundle, err := os.ReadFile(bundlePath)
		if err != nil {
			t.Fatal(err)
		}
		exported := workerExport{Type: "agent-symphony-export-v1", Repository: manifest.Repository, Branch: manifest.Branch, BaseSHA: base, HeadSHA: claimedHead, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "ok", Documentation: "none"}, Bundle: base64.StdEncoding.EncodeToString(bundle)}
		exportedJSON, _ := json.Marshal(exported)
		boundaryJSON, _ := json.Marshal(agentruntime.Result{Output: string(exportedJSON)})
		script := filepath.Join(t.TempDir(), "boundary")
		if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '"+string(boundaryJSON)+"'\n"), 0o700); err != nil {
			t.Fatal(err)
		}
		return importWorkerExport(t.Context(), workerBoundaryRunner{Command: script}, manifest)
	}

	if _, _, _, err := importBundle(head, "HEAD", "^"+base); err == nil || !strings.Contains(err.Error(), "worker bundle verification failed") {
		t.Fatalf("prerequisite-dependent bundle err=%v", err)
	}
	if err := exec.Command("git", "-C", coordinator, "cat-file", "-e", head).Run(); err == nil {
		t.Fatal("failed import changed the configured repository")
	}
	if _, _, _, err := importBundle(intermediate, "HEAD"); err == nil || !strings.Contains(err.Error(), "worker head is not advertised by bundle") {
		t.Fatalf("unadvertised intermediate head err=%v", err)
	}
	if err := exec.Command("git", "-C", coordinator, "cat-file", "-e", intermediate).Run(); err == nil {
		t.Fatal("unadvertised head changed the configured repository")
	}
	runGit(t, worker, "branch", "unchanged", base)
	if _, _, _, err := importBundle(base, "unchanged"); err == nil || !strings.Contains(err.Error(), "worker produced no repository changes") {
		t.Fatalf("unchanged worker head err=%v", err)
	}
	result, importedHead, root, err := importBundle(head, "HEAD")
	resolvedCoordinator, resolveErr := filepath.EvalSymlinks(coordinator)
	if err != nil || resolveErr != nil || result.Validation != "ok" || importedHead != head || root != resolvedCoordinator {
		t.Fatalf("result=%#v head=%q root=%q err=%v", result, importedHead, root, err)
	}
	if err := exec.Command("git", "-C", coordinator, "cat-file", "-e", head).Run(); err != nil {
		t.Fatalf("verified head was not imported: %v", err)
	}
}

func TestStructuredReviewResultIsBoundedAndFindingsBlockClean(t *testing.T) {
	clean, err := parseIndependentReview(`{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`)
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
	if _, err := parseIndependentReview(`{"type":"agent-symphony-review-v1","status":"clean","findings":[],"extra":true}`); err == nil {
		t.Fatal("accepted unknown field")
	}
	if _, err := parseIndependentReview(strings.Repeat("x", maxReviewResultSize+1)); err == nil {
		t.Fatal("accepted oversized result")
	}
}

type countingReviewBoundary int

func (b *countingReviewBoundary) call(context.Context, string, agentruntime.Command) (agentruntime.Result, error) {
	*b++
	return agentruntime.Result{}, nil
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
		{"foreign session", "", "as-r-foreign-99-1"},
		{"outside root", outside, session},
		{"symlink", link, session},
	} {
		t.Run(test.name, func(t *testing.T) {
			var boundary countingReviewBoundary
			if err := cleanupReviewResources(t.Context(), &boundary, nil, attempt, strings.Repeat("a", 40), "", test.snapshot, test.session, reviewSnapshotRoot); err == nil {
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

func TestReviewIdentitySeparatesRepositories(t *testing.T) {
	root := t.TempDir()
	first := agentruntime.Attempt{Repository: "a-b/c", Issue: 23, Number: 1}
	second := agentruntime.Attempt{Repository: "a/b-c", Issue: 23, Number: 1}
	firstSnapshot, firstSession := reviewIdentity(first, root)
	secondSnapshot, secondSession := reviewIdentity(second, root)
	if firstSnapshot == secondSnapshot || firstSession == secondSession {
		t.Fatalf("review identities collide: %q %q", firstSnapshot, firstSession)
	}
	largest := first
	largest.Issue, largest.Number = int(^uint(0)>>1), int(^uint(0)>>1)
	_, largestSession := reviewIdentity(largest, root)
	if len(largestSession) > 64 {
		t.Fatalf("review session exceeds runtime limit: %q", largestSession)
	}
	var boundary countingReviewBoundary
	if err := cleanupReviewResources(t.Context(), &boundary, nil, first, strings.Repeat("a", 40), "", secondSnapshot, secondSession, root); err == nil {
		t.Fatal("cleanup accepted another repository identity")
	}
	if boundary != 0 {
		t.Fatal("cleanup boundary reached for another repository")
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

func TestAttentionProjectionAppliesOnlyToTheCurrentAttempt(t *testing.T) {
	statuses := []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 218, Attempt: 1, State: "failed", Blockers: []string{"attempt 1 failed before retry"}},
		{Repository: "o/r", Issue: 218, Attempt: 2, State: "review-ready"},
	}
	issue := internalgithub.RecoveryIssueFact{
		Repository: "o/r", Issue: 218, Attempt: 2, DispatchAuthorized: true, NeedsAttention: true,
		Blockers: []string{"needs attention: review found a blocking regression"},
	}

	got, _ := joinIssueProjection(statuses, []internalgithub.RecoveryIssueFact{issue}, 1)
	if got[0].NeedsAttention || len(got[0].Blockers) != 1 || got[0].Blockers[0] != "attempt 1 failed before retry" || got[0].State != "failed" {
		t.Fatalf("historical attempt=%#v", got[0])
	}
	if !got[1].NeedsAttention || len(got[1].Blockers) != 1 || got[1].Blockers[0] != "needs attention: review found a blocking regression" || got[1].State != "review-ready" {
		t.Fatalf("current attempt=%#v", got[1])
	}
}

func TestIssueProjectionMarksEveryRetainedAttemptWhenIssueIsClosed(t *testing.T) {
	statuses := []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 218, Attempt: 1, State: "failed"},
		{Repository: "o/r", Issue: 218, Attempt: 2, State: "completed"},
	}
	got, _ := joinIssueProjection(statuses, []internalgithub.RecoveryIssueFact{{Repository: "o/r", Issue: 218, Attempt: 2, Closed: true}}, 1)
	if !got[0].IssueClosed || !got[1].IssueClosed {
		t.Fatalf("closed issue state was not projected to every attempt: %#v", got)
	}
}

func TestClosedIssueProjectionReadsUnmatchedRetainedAttemptOnce(t *testing.T) {
	reads := 0
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		reads++
		if request.URL.Path != "/repos/o/r/issues/218" {
			t.Fatalf("unexpected issue read %s", request.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"number":218,"state":"closed"}`))}, nil
	})}}
	statuses := []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 218, Attempt: 1, State: "orphaned"},
		{Repository: "o/r", Issue: 218, Attempt: 2, State: "failed"},
		{Repository: "o/r", Issue: 219, Attempt: 1, State: "active"},
	}
	if err := addClosedIssueProjection(t.Context(), api, "o/r", statuses, nil); err != nil || reads != 1 || !statuses[0].IssueClosed || !statuses[1].IssueClosed || statuses[2].IssueClosed {
		t.Fatalf("statuses=%#v reads=%d err=%v", statuses, reads, err)
	}
}

func TestIssueProjectionAllowsOnlyLatestUnblockedTerminalRecovery(t *testing.T) {
	statuses := []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 4, Attempt: 1, State: "failed", Retryable: true},
		{Repository: "o/r", Issue: 4, Attempt: 2, State: "failed", Retryable: true},
	}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 4, RecoveryAuthorized: true, RecoveryAttempt: 2, TerminalAttempts: []internalgithub.RecoveryAttemptFact{{Attempt: 1}, {Attempt: 2}}}
	got, _ := joinIssueProjection(statuses, []internalgithub.RecoveryIssueFact{issue}, 1)
	if got[0].Retryable || !got[1].Retryable {
		t.Fatalf("retry projection=%#v", got)
	}
	issue.RecoveryAuthorized = false
	issue.Blockers = []string{"dependency #3 is incomplete"}
	got, _ = joinIssueProjection(statuses, []internalgithub.RecoveryIssueFact{issue}, 1)
	if got[0].Retryable || got[1].Retryable {
		t.Fatalf("blocked retry projection=%#v", got)
	}
}

func TestIssueProjectionCarriesDeclaredPaths(t *testing.T) {
	now := time.Unix(1, 0)
	issues := []internalgithub.RecoveryIssueFact{
		{Repository: "o/r", Issue: 11, Attempt: 1, Priority: 3, CreatedAt: now, Paths: []string{"docs/low.md"}, Eligible: true},
		{Repository: "o/r", Issue: 12, Attempt: 1, Priority: 2, CreatedAt: now.Add(time.Second), Paths: []string{"docs/shared.md"}, Eligible: true},
		{Repository: "o/r", Issue: 13, Attempt: 1, Priority: 1, CreatedAt: now.Add(2 * time.Second), Paths: []string{"docs/shared.md"}, Eligible: true},
		{Repository: "o/r", Issue: 14, Attempt: 1, Priority: 2, CreatedAt: now.Add(3 * time.Second), Paths: []string{"docs/disjoint.md"}, Eligible: true},
	}
	statuses, _ := joinIssueProjection(nil, issues, 2)
	byIssue := map[int]orchestrator.RecoveryStatus{}
	for _, status := range statuses {
		byIssue[status.Issue] = status
	}
	if byIssue[13].State != string(orchestrator.Runnable) || byIssue[14].State != string(orchestrator.Runnable) ||
		byIssue[12].State != string(orchestrator.Queued) || !strings.Contains(byIssue[12].Action, "#13") ||
		byIssue[11].State != string(orchestrator.Queued) || !strings.Contains(byIssue[11].Action, "capacity") {
		t.Fatalf("initial scheduling=%#v", statuses)
	}

	issues[2].Active = true
	issues[3].Completed = true
	statuses, _ = joinIssueProjection(nil, issues, 2)
	byIssue = map[int]orchestrator.RecoveryStatus{}
	for _, status := range statuses {
		byIssue[status.Issue] = status
	}
	if byIssue[12].State != string(orchestrator.Queued) || !strings.Contains(byIssue[12].Action, "#13") || byIssue[11].State != string(orchestrator.Runnable) {
		t.Fatalf("released slot scheduling=%#v", statuses)
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
	initialized, err := config.Load(filepath.Join(root, config.DefaultPath))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(initialized.Commands.Implementation, config.Default("owner/repo").Commands.Implementation) {
		t.Fatalf("unexpected initialized implementation command: %q", initialized.Commands.Implementation)
	}
	if !slices.Equal(initialized.Commands.Reviewer, config.Default("owner/repo").Commands.Reviewer) {
		t.Fatalf("unexpected initialized reviewer command: %q", initialized.Commands.Reviewer)
	}
	wantOrchestrator := []string{"codex", "-c", `projects={"{orchestrator_workspace}"={trust_level="trusted"}}`, "--sandbox", "danger-full-access", "--ask-for-approval", "never", "--no-alt-screen"}
	if !slices.Equal(initialized.Commands.Orchestrator, wantOrchestrator) {
		t.Fatalf("initialized orchestrator=%#v", initialized.Commands.Orchestrator)
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

func TestGitHubDiagnosticsVerifiesCLIIdentityAndRepository(t *testing.T) {
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	if err := os.WriteFile(gh, []byte("#!/bin/sh\necho 'gh version 2.0.0'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/user":
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"id":42,"login":"coordinator"}`)), Header: make(http.Header)}, nil
		case "/repos/owner/repo":
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"full_name":"owner/repo","permissions":{"pull":true,"push":true}}`)), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected request %s", r.URL.Path)
			return nil, nil
		}
	})}
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = "https://example.invalid", client
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	got := githubDiagnostics("owner/repo")
	if len(got) != 3 || got[0].Status != "pass" || got[1].Status != "pass" || got[2].Status != "pass" || !strings.Contains(got[0].Message, "authenticated as coordinator") {
		t.Fatalf("unexpected diagnostics: %#v", got)
	}
}

func TestDaemonGitHubAuthenticationBoundary(t *testing.T) {
	dir := t.TempDir()
	gh := filepath.Join(dir, "gh")
	script := `#!/bin/sh
if [ "$1" = "--version" ]; then echo 'gh version 2.0.0'; exit 0; fi
case "$GH_TOKEN" in
valid)
  case "$*" in
    *'/user'*) body='{"id":42,"login":"coordinator"}' ;;
    *) body='{"full_name":"owner/repo","permissions":{"pull":true,"push":true}}' ;;
  esac
  printf 'HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n%s' "$body" ;;
'') echo 'GitHub CLI authentication is missing' >&2; exit 4 ;;
*) echo "GitHub CLI authentication token=$GH_TOKEN is invalid" >&2; exit 5 ;;
esac
`
	if err := os.WriteFile(gh, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = "https://api.github.com", &http.Client{Transport: internalgithub.CLITransport{Path: gh}}
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	for _, test := range []struct {
		name, token string
		ok          bool
	}{{"authenticated", "valid", true}, {"missing", "", false}, {"invalid", "daemon-invalid-canary", false}} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("GH_TOKEN", test.token)
			got := githubDiagnostics("owner/repo")
			if test.ok {
				if len(got) != 3 || got[0].Status != "pass" || got[1].Status != "pass" || got[2].Status != "pass" {
					t.Fatal("authenticated daemon diagnostics did not pass")
				}
				return
			}
			if len(got) != 1 || got[0].Status != "fail" || !strings.Contains(got[0].Message, "authenticate GitHub CLI") || test.token != "" && strings.Contains(got[0].Message, test.token) {
				t.Fatal("daemon authentication failure was unclear or exposed its credential")
			}
		})
	}
}

func TestStatusHumanNoColorAndVersionedJSON(t *testing.T) {
	owner, manifest := operatorTestOwner(t, 4, "completed", false)
	runtimeState := owner.stateRoot
	identity, _ := json.Marshal(deploymentIdentity{Version: deploymentIdentityVersion, Repository: manifest.Repository})
	if err := writeImmutable(filepath.Join(runtimeState, "deployment.json"), append(identity, '\n')); err != nil {
		t.Fatal(err)
	}
	snapshot, err := owner.snapshot(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := writeRuntimeOwnerState(runtimeState, owner.attemptRoot, snapshot.State); err != nil {
		t.Fatal(err)
	}
	repositoryRoot := gitRepository(t)
	t.Chdir(repositoryRoot)
	cfg := config.Default(manifest.Repository)
	configPath := filepath.Join(repositoryRoot, config.DefaultPath)
	if err := config.Write(configPath, cfg); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NO_COLOR", "1")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"status", "--config", configPath, "--state", "state", "--runtime-state", runtimeState}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "COMPLETED") || !strings.Contains(stdout.String(), "phase: completed") || !strings.Contains(stdout.String(), "session: implementation completed "+manifest.Session) || strings.Contains(stdout.String(), "\x1b[") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"inspect", "--config", configPath, "--issue", "4", "--state", "state", "--runtime-state", runtimeState, "--json"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	var got envelope
	if err := json.Unmarshal(stdout.Bytes(), &got); err != nil || got.Version != 1 || got.Command != "inspect" || !got.OK || !strings.Contains(stdout.String(), `"current_phase":"completed"`) || !strings.Contains(stdout.String(), `"role":"implementation"`) {
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

func TestWriteStatusSnapshotPersistsBlockers(t *testing.T) {
	root := t.TempDir()
	want := []orchestrator.RecoveryStatus{{Repository: "o/r", Issue: 9, Attempt: 1, State: "blocked", Blockers: []string{"exactly one priority label is required"}}}
	if err := writeStatusSnapshot(root, want); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "status.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var got struct {
		UpdatedAt time.Time                     `json:"updated_at"`
		Statuses  []orchestrator.RecoveryStatus `json:"statuses"`
	}
	if json.Unmarshal(body, &got) != nil || got.UpdatedAt.IsZero() || !reflect.DeepEqual(got.Statuses, want) {
		t.Fatalf("status snapshot=%#v", got)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("status mode=%v", info.Mode())
	}
}

func TestReconcileFailurePreservesLastGoodProjectionUntilRecovery(t *testing.T) {
	root := t.TempDir()
	want := []orchestrator.RecoveryStatus{{Repository: "o/r", Issue: 9, Attempt: 1, State: "active"}}
	if err := writeProjectStatusSnapshot(root, "o/r", want); err != nil {
		t.Fatal(err)
	}
	before, err := (&dashboardServer{stateRoot: root, repository: "o/r"}).readStatus()
	if err != nil {
		t.Fatal(err)
	}
	if err := recordReconcileFailure(root, "o/r", errors.New("GitHub unavailable token=secret-canary")); err != nil {
		t.Fatal(err)
	}
	failed, err := (&dashboardServer{stateRoot: root, repository: "o/r"}).readStatus()
	if err != nil || !reflect.DeepEqual(failed.Statuses, want) || failed.UpdatedAt != before.UpdatedAt || failed.ReconciliationErrorAt.IsZero() || failed.ReconciliationError == "" || strings.Contains(failed.ReconciliationError, "secret-canary") {
		t.Fatalf("failed projection=%#v err=%v", failed, err)
	}
	if err := writeProjectStatusSnapshot(root, "o/r", want); err != nil {
		t.Fatal(err)
	}
	recovered, err := (&dashboardServer{stateRoot: root, repository: "o/r"}).readStatus()
	if err != nil || recovered.ReconciliationError != "" || !recovered.ReconciliationErrorAt.IsZero() || recovered.UpdatedAt.Before(failed.UpdatedAt) {
		t.Fatalf("recovered projection=%#v err=%v", recovered, err)
	}
}

func TestRestartReplacesPersistedDependencyBlockerWithFreshProjection(t *testing.T) {
	stateRoot := t.TempDir()
	repository := "o/r"
	blockedIssue := internalgithub.RecoveryIssueFact{
		Repository: repository, Issue: 20, Attempt: 1, Priority: 1, Dependencies: []int{19},
		CreatedAt: time.Unix(1, 0), Blockers: []string{"dependency #19 is incomplete"},
	}
	blocked, _ := projectRecoveryStatuses(t.Context(), nil, []internalgithub.RecoveryIssueFact{blockedIssue}, nil, 1, nil)
	if err := writeProjectStatusSnapshot(stateRoot, repository, blocked); err != nil {
		t.Fatal(err)
	}
	before, err := (&dashboardServer{stateRoot: stateRoot, repository: repository}).readStatus()
	if err != nil || len(before.Statuses) != 1 || !slices.Equal(before.Statuses[0].Blockers, []string{"dependency #19 is incomplete"}) {
		t.Fatalf("blocked dashboard projection=%#v err=%v", before, err)
	}

	restarted := &agentruntime.Runtime{Root: filepath.Join(stateRoot, "attempts"), StateRoot: stateRoot}
	manifests, err := restarted.Discover()
	if err != nil || len(manifests) != 0 {
		t.Fatalf("reconstructed manifests=%#v err=%v", manifests, err)
	}
	freshIssue := blockedIssue
	freshIssue.Blockers = nil
	freshIssue.Eligible = true
	freshIssue.SatisfiedDependencies = []int{19}
	fresh, _ := projectRecoveryStatuses(t.Context(), nil, []internalgithub.RecoveryIssueFact{freshIssue}, manifests, 1, nil)
	if err := writeProjectStatusSnapshot(stateRoot, repository, fresh); err != nil {
		t.Fatal(err)
	}
	after, err := (&dashboardServer{stateRoot: stateRoot, repository: repository}).readStatus()
	if err != nil || len(after.Statuses) != 1 || after.Statuses[0].State != "runnable" || len(after.Statuses[0].Blockers) != 0 {
		t.Fatalf("post-restart dashboard projection=%#v err=%v", after, err)
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
