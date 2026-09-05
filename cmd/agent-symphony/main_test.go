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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestrator"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

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

type revokedAttemptRunner struct {
	live, interrupted bool
	starts            int
}

type cleanupBoundary struct {
	manifests  []agentruntime.Manifest
	operations []string
	err        error
}

type directHandoffBoundary struct{ root string }

type refusingHandoffBoundary struct{ calls int }

type crashingHandoffBoundary struct {
	direct               directHandoffBoundary
	before, after, calls int
}

type operatorMessageRunner struct {
	dead, interrupted     bool
	missing               bool
	branch, head, started string
}

type operatorMessageStore struct {
	comments        []map[string]any
	outcomeFailures int
	outcomePosts    int
}

func TestAuthorizedHumanInstructionsAmendIndependentReviewContractInOrder(t *testing.T) {
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 186, Attempt: 1, Body: "Commit the repository YAML."}
	now := time.Date(2026, 8, 31, 6, 0, 0, 0, time.UTC)
	first, _ := internalgithub.PrepareOperatorMessage("o/r", 186, 1, "Keep the YAML ignored.")
	first.State, first.UpdatedAt = "delivered", now
	later, _ := internalgithub.PrepareOperatorMessage("o/r", 186, 1, "Keep the machine-local config out of the PR.")
	later.State, later.UpdatedAt = "delivered", now.Add(3*time.Minute)
	queued, _ := internalgithub.PrepareOperatorMessage("o/r", 186, 1, "Not delivered.")
	wrongAttempt, _ := internalgithub.PrepareOperatorMessage("o/r", 186, 2, "Wrong attempt.")
	wrongAttempt.State = "delivered"
	forged := first
	forged.ID = strings.Repeat("f", 64)
	feedback := []internalgithub.Feedback{
		{ID: 11, Source: "issue", Body: "Issue comment instruction.", CreatedAt: now.Add(time.Minute), Authorized: true},
		{ID: 12, Source: "conversation", Body: "PR conversation instruction.", CreatedAt: now.Add(2 * time.Minute), Authorized: true},
		{ID: 13, Source: "inline", Body: "Unauthorized.", CreatedAt: now.Add(4 * time.Minute)},
	}

	amended, instructions, delivered := amendIssueWithHumanInstructions(issue, []internalgithub.OperatorMessage{first, later, queued, wrongAttempt, forged}, feedback)
	if !delivered || len(instructions) != 4 || !strings.Contains(instructions[0], first.Message) || !strings.Contains(instructions[1], feedback[0].Body) || !strings.Contains(instructions[2], feedback[1].Body) || !strings.Contains(instructions[3], later.Message) {
		t.Fatalf("instructions=%#v", instructions)
	}
	prompt := reviewPrompt(amended, strings.Repeat("a", 40))
	original, precedence, firstMessage, laterMessage := strings.Index(prompt, issue.Body), strings.Index(prompt, humanInstructionPrecedence), strings.Index(prompt, first.Message), strings.Index(prompt, later.Message)
	if original < 0 || precedence <= original || firstMessage <= precedence || laterMessage <= firstMessage || strings.Contains(prompt, queued.Message) || strings.Contains(prompt, wrongAttempt.Message) {
		t.Fatalf("review prompt did not preserve human instruction precedence: %q", prompt)
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

func (b *refusingHandoffBoundary) call(_ context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if operation == "verify-handoff" {
		return agentruntime.Result{}, nil
	}
	b.calls++
	return agentruntime.Result{}, errors.New("handoff must remain queued")
}

func (b *crashingHandoffBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	b.calls++
	if b.before > 0 {
		b.before--
		return agentruntime.Result{}, errors.New("injected crash after resume")
	}
	result, err := b.direct.call(ctx, operation, command)
	if err == nil && b.after > 0 && operation == "accept-operator-handoff" {
		b.after--
		return agentruntime.Result{}, errors.New("injected crash after worker receipt")
	}
	return result, err
}

func (r *operatorMessageRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if len(command.Args) == 0 {
		return agentruntime.Result{}, nil
	}
	if command.Name == "git" {
		switch command.Args[len(command.Args)-1] {
		case "--show-current":
			return agentruntime.Result{Output: r.branch}, nil
		case "HEAD":
			return agentruntime.Result{Output: r.head}, nil
		}
		return agentruntime.Result{}, nil
	}
	if command.Name != "tmux" {
		return agentruntime.Result{}, nil
	}
	switch command.Args[0] {
	case "has-session":
		if r.missing {
			return agentruntime.Result{Code: 1, Exited: true}, errors.New("missing session")
		}
		return agentruntime.Result{}, nil
	case "display-message":
		if slices.Contains(command.Args, "#{pane_start_command}") {
			return agentruntime.Result{Output: r.started}, nil
		}
		if r.dead || r.interrupted {
			if slices.Contains(command.Args, "#{pane_dead}") && !slices.Contains(command.Args, "#{pane_dead} #{pane_dead_status}") {
				return agentruntime.Result{Output: "1"}, nil
			}
			return agentruntime.Result{Output: "1 0"}, nil
		}
		return agentruntime.Result{Output: "0"}, nil
	case "send-keys":
		r.interrupted = true
		return agentruntime.Result{}, nil
	case "capture-pane":
		return agentruntime.Result{Output: "completed"}, nil
	default:
		return agentruntime.Result{}, nil
	}
}

func (s *operatorMessageStore) roundTrip(t *testing.T) http.RoundTripper {
	t.Helper()
	return roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			body, _ := json.Marshal(s.comments)
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
		}
		var input struct {
			Body string `json:"body"`
		}
		body, _ := io.ReadAll(request.Body)
		if json.Unmarshal(body, &input) != nil {
			t.Fatal("invalid comment mutation")
		}
		if strings.Contains(input.Body, "agent-symphony:operator-message-outcome:v1") {
			if s.outcomeFailures > 0 {
				s.outcomeFailures--
				return nil, errors.New("injected crash before delivered outcome")
			}
			s.outcomePosts++
		}
		s.comments = append(s.comments, map[string]any{"body": input.Body, "created_at": time.Now().UTC(), "user": map[string]any{"id": 42}})
		return &http.Response{StatusCode: http.StatusCreated, Status: "201 Created", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
}

func TestHelpListsUserFacingCommandsAndFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--help"}, &stdout, &stderr); code != 0 || stderr.Len() != 0 {
		t.Fatalf("help code=%d stderr=%q", code, stderr.String())
	}
	for _, want := range []string{
		"install-host", "agent-host", "init", "validate", "config view", "serve", "status", "list", "inspect", "reconcile", "doctor", "diagnostics", "pr-governance", "help",
		"--config", "--state", "--runtime-state", "--attempts", "--issue", "--interval", "--dashboard-address", "--allow-unsafe-dashboard-network", "--dashboard-password-file", "--offline", "--coordinator", "--json", "--help", "--version",
	} {
		if !strings.Contains(stdout.String(), want) {
			t.Errorf("help is missing %q", want)
		}
	}
}

func TestOperatorMessageQueuesBehindRunningTurnWithoutInterruptingIt(t *testing.T) {
	message, _ := internalgithub.PrepareOperatorMessage("o/r", 131, 3, "Run the focused test.")
	key := "o/r#131/3"
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	bound := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 131, Attempt: 3, State: "active"}
	issues := []internalgithub.RecoveryIssueFact{{Repository: "o/r", Issue: 131, Attempt: 3, DispatchAuthorized: true, ActiveAttempt: &bound}}
	manifests := []agentruntime.Manifest{{Repository: "o/r", Issue: 131, Attempt: 3, State: "running", Session: "live"}}
	boundary := &refusingHandoffBoundary{}
	if err := resumeOperatorMessages(t.Context(), internalgithub.API{}, internalgithub.PRAdapterConfig{}, nil, boundary, issues, nil, manifests, messages, []string{"implementation"}, func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }, nil, nil); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != 0 || messages[key][0].State != "queued" {
		t.Fatalf("running turn was interrupted: calls=%d message=%#v", boundary.calls, messages[key][0])
	}
	statuses := []orchestrator.RecoveryStatus{{Repository: "o/r", Issue: 131, Attempt: 3}}
	attachOperatorMessageStatuses(statuses, messages)
	body, _ := json.Marshal(statuses)
	if bytes.Contains(body, []byte(message.Message)) || !bytes.Contains(body, []byte(message.ID)) || !bytes.Contains(body, []byte(`"state":"queued"`)) {
		t.Fatalf("dashboard status exposed content or omitted outcome: %s", body)
	}
}

func TestPublishedOperatorMessageWithAdvancedHeadStaysQueuedBehindRunningTurn(t *testing.T) {
	runtimeState, attempt, _, runner := operatorMessageRuntime(t)
	manifest, err := runtimeState.ResumeHandoff(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ReviewHead = strings.Repeat("b", 40)
	runner.head = strings.Repeat("c", 40)
	message, _ := internalgithub.PrepareOperatorMessage(attempt.Repository, attempt.Issue, attempt.Number, "Wait for the active follow-up.")
	key := fmt.Sprintf("%s#%d/%d", attempt.Repository, attempt.Issue, attempt.Number)
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true}
	published := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, PR: 99, BaseSHA: attempt.BaseSHA, HeadSHA: manifest.ReviewHead, State: "review-ready"}
	boundary := &refusingHandoffBoundary{}
	if err := resumeOperatorMessages(t.Context(), internalgithub.API{}, internalgithub.PRAdapterConfig{}, runtimeState, boundary, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{published}, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error {
		return errors.New("worktree HEAD does not match GitHub")
	}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != 0 || messages[key][0].State != "queued" {
		t.Fatalf("busy published follow-up was not retained: calls=%d message=%#v", boundary.calls, messages[key][0])
	}
}

func TestQueuedOperatorMessageRetiresLegacyResumeBeforeFreshDelivery(t *testing.T) {
	runtimeState, attempt, _, runner := operatorMessageRuntime(t)
	manifest, err := runtimeState.ResumeHandoff(t.Context(), attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.UpdatedAt = time.Now().Add(-3 * time.Minute)
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	runner.started = "agent-symphony worker-capture-handoff tmux buffer result launch token launched -- codex exec resume --last --dangerously-bypass-approvals-and-sandbox -"
	store := &operatorMessageStore{}
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: store.roundTrip(t)}}
	cfg := internalgithub.PRAdapterConfig{Repository: attempt.Repository, ActorID: 42}
	message, _ := internalgithub.PrepareOperatorMessage(attempt.Repository, attempt.Issue, attempt.Number, "Resume after the prior pane exited.")
	message, err = internalgithub.RecordOperatorMessage(t.Context(), api, cfg, message)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%s#%d/%d", attempt.Repository, attempt.Issue, attempt.Number)
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	bound := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "active"}
	issues := []internalgithub.RecoveryIssueFact{{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true, ActiveAttempt: &bound}}
	remote := []internalgithub.RecoveryAttemptFact{bound}
	refresh := func(context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
		current, discoverErr := runtimeState.Discover()
		return issues, remote, current, discoverErr
	}
	oldExec := hostExecRunner
	recipient, launched := "", ""
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		if len(command.Args) > 0 && command.Args[0] == "show-options" {
			return agentruntime.Result{Output: recipient}, nil
		}
		if len(command.Args) > 0 && command.Args[0] == "respawn-pane" {
			var err error
			recipient, err = acknowledgeHandoffLaunch(command)
			if err != nil {
				return agentruntime.Result{}, err
			}
			launched = strings.Join(command.Args, " ")
		}
		return agentruntime.Result{}, nil
	}
	t.Cleanup(func() { hostExecRunner = oldExec })
	defaultCommand := []string{"codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"}
	if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, &directHandoffBoundary{root: runtimeState.Root}, issues, remote, []agentruntime.Manifest{manifest}, messages, defaultCommand, func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if messages[key][0].State != "delivered" || !runner.interrupted || recipient == "" || !strings.Contains(launched, strings.Join(defaultCommand, " ")) || strings.Contains(launched, " resume ") {
		t.Fatalf("message=%#v interrupted=%v recipient=%q launch=%q", messages[key][0], runner.interrupted, recipient, launched)
	}
}

func TestPublishedOperatorMessageStaysQueuedWhileCompletedFollowUpReviewRuns(t *testing.T) {
	message, _ := internalgithub.PrepareOperatorMessage("o/r", 131, 3, "Wait for the follow-up review.")
	key := "o/r#131/3"
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 131, Attempt: 3, DispatchAuthorized: true}
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 131, Attempt: 3, PR: 99, BaseSHA: strings.Repeat("a", 40), HeadSHA: strings.Repeat("b", 40), State: "review-ready"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 131, Attempt: 3, BaseSHA: remote.BaseSHA, State: "completed", ReviewState: "running", ReviewHead: strings.Repeat("c", 40), Session: "reviewing"}
	boundary := &refusingHandoffBoundary{}
	if err := resumeOperatorMessages(t.Context(), internalgithub.API{}, internalgithub.PRAdapterConfig{}, nil, boundary, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{remote}, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != 0 || messages[key][0].State != "queued" {
		t.Fatalf("review-in-progress message was not retained: calls=%d message=%#v", boundary.calls, messages[key][0])
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

func TestTransitionRetryRunsOnceAndRecordsTerminalOutcome(t *testing.T) {
	root := t.TempDir()
	agent := &orchestratoragent.Supervisor{
		Root:                  root,
		Workspace:             filepath.Join(root, "orchestrator-o-r"),
		Repository:            "o/r",
		Command:               []string{"agent"},
		ProposalCommand:       []string{"proposal"},
		ProposalStatusCommand: []string{"proposal-status"},
		Runner:                &orchestratorTestRunner{},
	}
	if _, err := agent.Observe(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 131, Attempt: 3, Action: orchestratoragent.ProposalActionRetry, RequestID: "retry-once"}
	body, _ := json.Marshal(proposal)
	if err := os.WriteFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalFile), body, 0o620); err != nil {
		t.Fatal(err)
	}
	valid := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 131, Attempt: 3, State: "active", CurrentPhase: "publication", DispatchAuthorized: true, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, State: "completed"}}}
	previous := reconcileRetryRun
	calls := 0
	reconcileRetryRun = func(ctx context.Context, _, _, _ string, options reconcileOptions) ([]orchestrator.RecoveryStatus, error) {
		calls++
		if !options.transition || options.intake || options.timeout != 10*time.Minute {
			t.Fatalf("retry options=%+v", options)
		}
		if err := options.authorize([]orchestrator.RecoveryStatus{valid}); err != nil {
			return nil, err
		}
		return []orchestrator.RecoveryStatus{valid}, nil
	}
	t.Cleanup(func() { reconcileRetryRun = previous })
	operationMu := &sync.Mutex{}
	operationMu.Lock()
	processOrchestratorProposal(t.Context(), agent, operationMu, &operationCancellation{}, "config", "state", "runtime", io.Discard)
	operationMu.Unlock()
	pendingBody, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalStatusFile))
	var pending orchestratoragent.MessageProposalStatus
	if err != nil || json.Unmarshal(pendingBody, &pending) != nil || calls != 0 || pending.PendingBinding == "" {
		t.Fatalf("busy calls=%d status=%s err=%v", calls, pendingBody, err)
	}
	processOrchestratorProposal(t.Context(), agent, operationMu, &operationCancellation{}, "config", "state", "runtime", io.Discard)
	statusBody, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalStatusFile))
	var status orchestratoragent.MessageProposalStatus
	if err != nil || json.Unmarshal(statusBody, &status) != nil || calls != 1 || status.Resolution != "succeeded" || status.PendingBinding != "" {
		t.Fatalf("calls=%d status=%s err=%v", calls, statusBody, err)
	}

	proposal.RequestID = "retry-fails"
	body, _ = json.Marshal(proposal)
	if err := os.WriteFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalFile), body, 0o620); err != nil {
		t.Fatal(err)
	}
	reconcileRetryRun = func(_ context.Context, _, _, _ string, options reconcileOptions) ([]orchestrator.RecoveryStatus, error) {
		calls++
		if err := options.authorize([]orchestrator.RecoveryStatus{valid}); err != nil {
			return nil, err
		}
		return nil, errors.New("publish stage failed")
	}
	processOrchestratorProposal(t.Context(), agent, &sync.Mutex{}, &operationCancellation{}, "config", "state", "runtime", io.Discard)
	statusBody, err = os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalStatusFile))
	if err != nil || json.Unmarshal(statusBody, &status) != nil || calls != 2 || status.Resolution != "failed" || !strings.Contains(status.Detail, "publish stage failed") {
		t.Fatalf("calls=%d status=%s err=%v", calls, statusBody, err)
	}
}

func TestAttentionProposalRecordsDurableHumanOutcomeWithoutMutation(t *testing.T) {
	root := t.TempDir()
	agent := &orchestratoragent.Supervisor{
		Root:                  root,
		Workspace:             filepath.Join(root, "orchestrator-o-r"),
		Repository:            "o/r",
		Command:               []string{"agent"},
		ProposalCommand:       []string{"proposal"},
		ProposalStatusCommand: []string{"proposal-status"},
		Runner:                &orchestratorTestRunner{},
	}
	blocked := []orchestrator.RecoveryStatus{{Repository: "o/r", Issue: 193, Attempt: 1, State: "blocked", CurrentPhase: "blocked", Blockers: []string{"unsafe worktree"}}}
	if _, err := agent.Observe(t.Context(), blocked); err != nil {
		t.Fatal(err)
	}
	var handoff struct {
		ID string `json:"id"`
	}
	handoffBody, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.AttentionHandoffFile))
	if err != nil || json.Unmarshal(handoffBody, &handoff) != nil || handoff.ID == "" {
		t.Fatalf("handoff=%s err=%v", handoffBody, err)
	}
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 193, Attempt: 1, Action: orchestratoragent.ProposalActionAttention, RequestID: "attention-193-1", HandoffID: handoff.ID, Detail: "unsafe worktree requires operator repair"}
	body, _ := json.Marshal(proposal)
	if err := os.WriteFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalFile), body, 0o620); err != nil {
		t.Fatal(err)
	}
	previous := reconcileGitHubRun
	reconcileGitHubRun = func(context.Context, string, string, string, bool) ([]orchestrator.RecoveryStatus, error) {
		return blocked, nil
	}
	t.Cleanup(func() { reconcileGitHubRun = previous })
	processOrchestratorProposal(t.Context(), agent, &sync.Mutex{}, &operationCancellation{}, "config", "state", "runtime", io.Discard)
	statusBody, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalStatusFile))
	var status orchestratoragent.MessageProposalStatus
	if err != nil || json.Unmarshal(statusBody, &status) != nil || status.Resolution != "succeeded" || status.Detail != proposal.Detail {
		t.Fatalf("proposal status=%s err=%v", statusBody, err)
	}
	var outcome struct {
		State  string `json:"state"`
		Detail string `json:"detail"`
	}
	handoffBody, err = os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.AttentionHandoffFile))
	if err != nil || json.Unmarshal(handoffBody, &outcome) != nil || outcome.State != "human-attention" || outcome.Detail != proposal.Detail {
		t.Fatalf("attention outcome=%s err=%v", handoffBody, err)
	}
}

func TestOperationCancellationInterruptsTheCurrentCoordinatorPass(t *testing.T) {
	operations := &operationCancellation{}
	ctx, finish := operations.begin(t.Context())
	operations.interrupt()
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		t.Fatal("coordinator pass was not interrupted")
	}
	finish()
}

func TestWriteOperatorMessageStatusProjectsQueuedAndTerminalStateImmediately(t *testing.T) {
	root := t.TempDir()
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 4, Attempt: 2, State: "active"}
	if err := writeStatusSnapshot(root, []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	message, err := internalgithub.PrepareOperatorMessage("o/r", 4, 2, "Continue the exact attempt.")
	if err != nil {
		t.Fatal(err)
	}
	message.State = "claimed"
	message.UpdatedAt = time.Unix(10, 0).UTC()
	if err := writeOperatorMessageStatus(root, message); err != nil {
		t.Fatal(err)
	}
	message.State, message.Diagnostic = "delivered", ""
	message.UpdatedAt = time.Unix(20, 0).UTC()
	if err := writeOperatorMessageStatus(root, message); err != nil {
		t.Fatal(err)
	}
	body, err := readDashboardStatus(filepath.Join(root, "status.json"))
	if err != nil {
		t.Fatal(err)
	}
	var snapshot struct {
		Statuses []orchestrator.RecoveryStatus `json:"statuses"`
	}
	if json.Unmarshal(body, &snapshot) != nil || len(snapshot.Statuses) != 1 || len(snapshot.Statuses[0].OperatorMessages) != 1 || snapshot.Statuses[0].OperatorMessages[0].State != "delivered" || !snapshot.Statuses[0].OperatorMessages[0].UpdatedAt.Equal(message.UpdatedAt) {
		t.Fatalf("snapshot=%s", body)
	}
}

func TestStartupMessageRecoveryAcceptsCurrentStatusSchemaWithoutPendingWork(t *testing.T) {
	root := t.TempDir()
	statuses := []orchestrator.RecoveryStatus{{Issue: 161, OperatorMessages: []orchestrator.OperatorMessageStatus{{ID: strings.Repeat("a", 64), State: "delivered"}}}}
	if err := writeStatusSnapshot(root, statuses); err != nil {
		t.Fatal(err)
	}
	if err := retryProjectedOperatorMessages(t.Context(), filepath.Join(root, "missing-config"), root); err != nil {
		t.Fatal(err)
	}
}

func TestOperatorMessageTargetValidationRejectsStaleAndTerminalAttempts(t *testing.T) {
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 131, Attempt: 3, Message: "message"}
	bound := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 131, Attempt: 3, BaseSHA: "aaaaaaa", State: "active"}
	activeIssue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 131, Attempt: 3, DispatchAuthorized: true, ActiveAttempt: &bound}
	running := agentruntime.Manifest{Repository: "o/r", Issue: 131, Attempt: 3, BaseSHA: "aaaaaaa", State: "running", Session: "live"}
	check := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
	if err := validateOperatorMessageTarget(t.Context(), proposal, []internalgithub.RecoveryIssueFact{activeIssue}, nil, []agentruntime.Manifest{running}, check); err != nil {
		t.Fatalf("active target rejected: %v", err)
	}
	unpublished := running
	unpublished.State = "completed"
	if err := validateOperatorMessageTarget(t.Context(), proposal, []internalgithub.RecoveryIssueFact{activeIssue}, nil, []agentruntime.Manifest{unpublished}, check); err != nil {
		t.Fatalf("completed unpublished target rejected: %v", err)
	}
	publishedIssue := activeIssue
	publishedIssue.ActiveAttempt = nil
	published := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 131, Attempt: 3, PR: 7, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", State: "review-ready"}
	completed := agentruntime.Manifest{Repository: "o/r", Issue: 131, Attempt: 3, BaseSHA: "aaaaaaa", State: "completed", ReviewHead: "bbbbbbb", Session: "retained"}
	if err := validateOperatorMessageTarget(t.Context(), proposal, []internalgithub.RecoveryIssueFact{publishedIssue}, []internalgithub.RecoveryAttemptFact{published}, []agentruntime.Manifest{completed}, check); err != nil {
		t.Fatalf("published target rejected: %v", err)
	}
	cancelled := activeIssue
	cancelled.Cancelled, cancelled.DispatchAuthorized = true, false
	wrong := bound
	wrong.Attempt = 2
	mismatched := activeIssue
	mismatched.ActiveAttempt = &wrong
	wrongBase := bound
	wrongBase.BaseSHA = "ccccccc"
	baseMismatch := activeIssue
	baseMismatch.ActiveAttempt = &wrongBase
	headMismatch := published
	headMismatch.HeadSHA = "ccccccc"
	for name, input := range map[string]struct {
		issues    []internalgithub.RecoveryIssueFact
		remote    []internalgithub.RecoveryAttemptFact
		manifests []agentruntime.Manifest
	}{
		"missing":                   {nil, nil, nil},
		"competing active binding":  {[]internalgithub.RecoveryIssueFact{mismatched}, []internalgithub.RecoveryAttemptFact{published}, []agentruntime.Manifest{completed}},
		"mismatched base":           {[]internalgithub.RecoveryIssueFact{baseMismatch}, nil, []agentruntime.Manifest{running}},
		"mismatched published head": {[]internalgithub.RecoveryIssueFact{publishedIssue}, []internalgithub.RecoveryAttemptFact{headMismatch}, []agentruntime.Manifest{completed}},
		"cancelled":                 {[]internalgithub.RecoveryIssueFact{cancelled}, nil, []agentruntime.Manifest{running}},
		"merged":                    {[]internalgithub.RecoveryIssueFact{activeIssue}, []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 131, Attempt: 3, BaseSHA: "aaaaaaa", State: "completed"}}, []agentruntime.Manifest{running}},
		"terminal":                  {[]internalgithub.RecoveryIssueFact{activeIssue}, nil, []agentruntime.Manifest{{Repository: "o/r", Issue: 131, Attempt: 3, BaseSHA: "aaaaaaa", State: "failed"}}},
	} {
		t.Run(name, func(t *testing.T) {
			if err := validateOperatorMessageTarget(t.Context(), proposal, input.issues, input.remote, input.manifests, check); err == nil {
				t.Fatal("unsafe target accepted")
			}
		})
	}
}

func TestOperatorMessageTargetValidationVerifiesCompletedRuntimeResources(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(agentruntime.Manifest, *operatorMessageRunner)
	}{
		{"missing worktree", func(manifest agentruntime.Manifest, _ *operatorMessageRunner) { _ = os.RemoveAll(manifest.Worktree) }},
		{"mismatched branch", func(_ agentruntime.Manifest, runner *operatorMessageRunner) { runner.branch = "wrong-branch" }},
		{"missing result", func(manifest agentruntime.Manifest, _ *operatorMessageRunner) {
			_ = os.Remove(agentruntime.ResultPath(manifest.Worktree))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeState, attempt, manifest, runner := operatorMessageRuntime(t)
			test.mutate(manifest, runner)
			bound := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "active"}
			issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true, ActiveAttempt: &bound}
			proposal := orchestratoragent.MessageProposal{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Message: "message"}
			err := validateOperatorMessageTarget(t.Context(), proposal, []internalgithub.RecoveryIssueFact{issue}, nil, []agentruntime.Manifest{manifest}, func(ctx context.Context, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
				return verifyOperatorMessageRuntime(ctx, runtimeState, manifest, fact)
			})
			if err == nil || !strings.Contains(err.Error(), "verified exact runtime owner") {
				t.Fatalf("completed target with invalid runtime accepted: %v", err)
			}
		})
	}
}

func TestOperatorMessageAcceptanceBindingRejectsRemoteRaces(t *testing.T) {
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 131, Attempt: 3, Message: "message"}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 131, Attempt: 3, DispatchAuthorized: true}
	remote := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 131, Attempt: 3, PR: 7, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", State: "review-ready"}
	expected, err := operatorMessageBinding(proposal, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{remote})
	if err != nil {
		t.Fatal(err)
	}
	if !matchesOperatorMessageBinding(proposal, expected, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{remote}) {
		t.Fatal("unchanged remote binding was rejected at delivery acceptance")
	}
	for name, mutate := range map[string]func(*internalgithub.RecoveryAttemptFact){
		"force-pushed head": func(fact *internalgithub.RecoveryAttemptFact) { fact.HeadSHA = "ccccccc" },
		"rebound base":      func(fact *internalgithub.RecoveryAttemptFact) { fact.BaseSHA = "ddddddd" },
		"closed pull request": func(fact *internalgithub.RecoveryAttemptFact) {
			fact.State = "blocked"
		},
	} {
		t.Run(name, func(t *testing.T) {
			current := remote
			mutate(&current)
			if matchesOperatorMessageBinding(proposal, expected, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{current}) {
				t.Fatal("changed remote binding remained eligible at delivery acceptance")
			}
		})
	}
}

func TestOperatorMessageConfirmationAcceptsOwnedRunningAdvancedHead(t *testing.T) {
	stateRoot := t.TempDir()
	snapshotRoot := filepath.Join(t.TempDir(), "snapshots")
	oldSnapshotRoot := reviewSnapshotRoot
	reviewSnapshotRoot = snapshotRoot
	t.Cleanup(func() { reviewSnapshotRoot = oldSnapshotRoot })
	if err := os.MkdirAll(filepath.Dir(snapshotRoot), 0o700); err != nil {
		t.Fatal(err)
	}

	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 131, Number: 3, BaseSHA: strings.Repeat("a", 40)}
	runtimeState := newOperatorMessageRuntime(stateRoot)
	if err := os.MkdirAll(runtimeState.Root, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest, err := agentruntime.AttemptIdentity(runtimeState.Root, attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "running"
	manifest.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository), "131-3", "agent.log")
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}

	logPath, script := filepath.Join(t.TempDir(), "boundary.log"), filepath.Join(t.TempDir(), "boundary")
	scriptBody := fmt.Sprintf(`#!/bin/sh
payload=$(sed -n '1p')
printf '%%s\n' "$payload" >> %q
case "$payload" in
  *--show-current*) output=%q ;;
  *'rev-parse","HEAD'*) output=%q ;;
  *) output='' ;;
esac
printf '{"Output":"%%s","Code":0,"Exited":false}' "$output"
`, logPath, manifest.Branch, strings.Repeat("c", 40))
	if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", script)
	runtimeState = newOperatorMessageRuntime(stateRoot)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Message: "Queue behind the running turn."}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true}
	remote := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, PR: 7, BaseSHA: attempt.BaseSHA, HeadSHA: strings.Repeat("b", 40), State: "review-ready"}
	if err := validateOperatorMessageTarget(t.Context(), proposal, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{remote}, []agentruntime.Manifest{manifest}, func(ctx context.Context, manifest agentruntime.Manifest, fact orchestrator.AttemptFact) error {
		return verifyOperatorMessageRuntime(ctx, &runtimeState, manifest, fact)
	}); err != nil {
		t.Fatalf("confirmation rejected the owned advanced-head running target: %v", err)
	}
	logBody, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(logBody, []byte(`"operation":"verify"`)) || !bytes.Contains(logBody, []byte(`"operation":"run"`)) {
		t.Fatalf("confirmation runtime did not use both boundary hooks: %s", logBody)
	}
	if bytes.Contains(logBody, []byte(`rev-parse`)) {
		t.Fatalf("busy confirmation compared mutable HEAD: %s", logBody)
	}
}

func TestOperatorMessageAdmissionRequiresOnlyTheExactLocalRuntimeOwner(t *testing.T) {
	runtimeState, attempt, manifest, runner := operatorMessageRuntime(t)
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Message: "Queue before remote delivery validation."}
	if err := validateOperatorMessageAdmission(t.Context(), proposal, runtimeState, []agentruntime.Manifest{manifest}); err != nil {
		t.Fatalf("exact local owner was rejected: %v", err)
	}
	runner.missing = true
	if err := validateOperatorMessageAdmission(t.Context(), proposal, runtimeState, []agentruntime.Manifest{manifest}); err != nil {
		t.Fatalf("completed retained owner required its old session: %v", err)
	}
	manifest.State = "running"
	if err := validateOperatorMessageAdmission(t.Context(), proposal, runtimeState, []agentruntime.Manifest{manifest}); err != nil {
		t.Fatalf("interrupted pre-launch owner was rejected: %v", err)
	}
	manifest.State = "completed"
	if err := os.Remove(agentruntime.ResultPath(manifest.Worktree)); err != nil {
		t.Fatal(err)
	}
	if err := validateOperatorMessageAdmission(t.Context(), proposal, runtimeState, []agentruntime.Manifest{manifest}); err == nil {
		t.Fatal("completed owner without a retained result was admitted")
	}
	if err := validateOperatorMessageAdmission(t.Context(), proposal, runtimeState, []agentruntime.Manifest{manifest, manifest}); err == nil {
		t.Fatal("ambiguous local owner was admitted")
	}
}

func TestClaimedOperatorMessageDoesNotInterruptCompetingHandoff(t *testing.T) {
	runtimeState, attempt, manifest, runner := operatorMessageRuntime(t)
	store := &operatorMessageStore{}
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: store.roundTrip(t)}}
	cfg := internalgithub.PRAdapterConfig{Repository: attempt.Repository, ActorID: 42}
	message, _ := internalgithub.PrepareOperatorMessage(attempt.Repository, attempt.Issue, attempt.Number, "Wait for the current handoff.")
	message, err := internalgithub.RecordOperatorMessage(t.Context(), api, cfg, message)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%s#%d/%d", attempt.Repository, attempt.Issue, attempt.Number)
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	bound := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "active"}
	issues := []internalgithub.RecoveryIssueFact{{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true, ActiveAttempt: &bound}}
	remote := []internalgithub.RecoveryAttemptFact{bound}
	check := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
	refresh := func(context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
		current, discoverErr := runtimeState.Discover()
		return issues, remote, current, discoverErr
	}
	boundary := &crashingHandoffBoundary{direct: directHandoffBoundary{root: runtimeState.Root}, before: 1}
	if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, boundary, issues, remote, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, check, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
		return true, nil
	}); err == nil || !strings.Contains(err.Error(), "after resume") {
		t.Fatalf("missing injected restart: %v", err)
	}
	recovered, err := internalgithub.FetchOperatorMessages(t.Context(), api, cfg, attempt.Issue)
	if err != nil || len(recovered) != 1 || recovered[0].State != "claimed" {
		t.Fatalf("durable claim=%#v err=%v", recovered, err)
	}
	current, err := runtimeState.Discover()
	if err != nil || len(current) != 1 || current[0].State != "running" {
		t.Fatalf("resumed manifest=%#v err=%v", current, err)
	}

	oldExec := hostExecRunner
	respawns := 0
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		if len(command.Args) > 0 && command.Args[0] == "show-options" {
			return agentruntime.Result{Output: "different handoff"}, nil
		}
		if len(command.Args) > 0 && command.Args[0] == "respawn-pane" {
			respawns++
			if _, err := acknowledgeHandoffLaunch(command); err != nil {
				return agentruntime.Result{}, err
			}
		}
		return agentruntime.Result{}, nil
	}
	t.Cleanup(func() { hostExecRunner = oldExec })
	competingKey := "independent-review-competing"
	competingPayload := json.RawMessage(`{"type":"agent-symphony-handoff-v1","key":"independent-review-competing"}`)
	competingPath := handoffReceiptPath(manifest.Worktree, competingKey)
	request, _ := json.Marshal(struct {
		Manifest     agentruntime.Manifest `json:"manifest"`
		Handoff      json.RawMessage       `json:"handoff"`
		OutcomePath  string                `json:"outcome_path"`
		OutcomeToken string                `json:"outcome_token"`
		Command      []string              `json:"command"`
	}{current[0], competingPayload, competingPath, "competing-token", []string{"implementation"}})
	if _, err := boundary.direct.call(t.Context(), "accept-handoff", agentruntime.Command{Stdin: bytes.NewReader(request)}); err != nil {
		t.Fatal(err)
	}
	messages[key] = recovered
	calls := boundary.calls
	if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, boundary, issues, remote, current, messages, []string{"implementation"}, check, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != calls+1 || respawns != 1 || messages[key][0].State != "claimed" || runner.dead {
		t.Fatalf("competing live turn was interrupted: calls=%d respawns=%d message=%#v", boundary.calls, respawns, messages[key][0])
	}
	statuses := []orchestrator.RecoveryStatus{{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number}}
	attachOperatorMessageStatuses(statuses, messages)
	if len(statuses[0].OperatorMessages) != 1 || statuses[0].OperatorMessages[0].State != "queued" {
		t.Fatalf("restart claim leaked internal dashboard state: %#v", statuses[0].OperatorMessages)
	}
}

func TestFirstOperatorMessageDeliveryRecoversExactlyOnceAtCrashBoundaries(t *testing.T) {
	for _, stage := range []string{"after resume", "after worker receipt", "before delivered outcome"} {
		t.Run(stage, func(t *testing.T) {
			runtimeState, attempt, manifest, runner := operatorMessageRuntime(t)
			store := &operatorMessageStore{}
			api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: store.roundTrip(t)}}
			cfg := internalgithub.PRAdapterConfig{Repository: attempt.Repository, ActorID: 42}
			message, _ := internalgithub.PrepareOperatorMessage(attempt.Repository, attempt.Issue, attempt.Number, "Recover this delivery exactly once.")
			message, err := internalgithub.RecordOperatorMessage(t.Context(), api, cfg, message)
			if err != nil {
				t.Fatal(err)
			}
			key := fmt.Sprintf("%s#%d/%d", attempt.Repository, attempt.Issue, attempt.Number)
			messages := map[string][]internalgithub.OperatorMessage{key: {message}}
			bound := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "active"}
			issues := []internalgithub.RecoveryIssueFact{{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true, ActiveAttempt: &bound}}
			remote := []internalgithub.RecoveryAttemptFact{bound}
			check := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
			refresh := func(context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
				current, discoverErr := runtimeState.Discover()
				return issues, remote, current, discoverErr
			}
			boundary := &crashingHandoffBoundary{direct: directHandoffBoundary{root: runtimeState.Root}}
			switch stage {
			case "after resume":
				boundary.before = 1
			case "after worker receipt":
				boundary.after = 1
			case "before delivered outcome":
				store.outcomeFailures = 1
			}
			oldExec := hostExecRunner
			respawns, recipient := 0, ""
			hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
				if len(command.Args) > 0 && command.Args[0] == "show-options" {
					if recipient == "" {
						return agentruntime.Result{}, nil
					}
					return agentruntime.Result{Output: recipient}, nil
				}
				if len(command.Args) > 0 && command.Args[0] == "respawn-pane" {
					respawns++
					var err error
					recipient, err = acknowledgeHandoffLaunch(command)
					if err != nil {
						return agentruntime.Result{}, err
					}
				}
				return agentruntime.Result{}, nil
			}
			t.Cleanup(func() { hostExecRunner = oldExec })

			if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, boundary, issues, remote, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, check, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
				return true, nil
			}); err == nil {
				t.Fatal("injected crash did not stop delivery")
			}
			recovered, err := internalgithub.FetchOperatorMessages(t.Context(), api, cfg, attempt.Issue)
			if err != nil || len(recovered) != 1 || recovered[0].State != "claimed" {
				t.Fatalf("durable claim=%#v err=%v", recovered, err)
			}
			messages[key] = recovered
			current, err := runtimeState.Discover()
			if err != nil || len(current) != 1 || current[0].State != "running" {
				t.Fatalf("resumed manifest=%#v err=%v", current, err)
			}
			if stage == "after resume" {
				calls := boundary.calls
				if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, boundary, issues, remote, current, messages, []string{"implementation"}, check, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
					return true, nil
				}); err != nil {
					t.Fatal(err)
				}
				if boundary.calls != calls+1 || respawns != 0 || messages[key][0].State != "claimed" {
					t.Fatalf("unowned running pane was reused: calls=%d respawns=%d message=%#v", boundary.calls, respawns, messages[key][0])
				}
				runner.dead = true
				if _, err := runtimeState.Monitor(t.Context(), attempt); err != nil {
					t.Fatal(err)
				}
				current, err = runtimeState.Discover()
				if err != nil || len(current) != 1 || current[0].State != "completed" {
					t.Fatalf("completed transition=%#v err=%v", current, err)
				}
			}
			if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, boundary, issues, remote, current, messages, []string{"implementation"}, check, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			delivered, err := internalgithub.FetchOperatorMessages(t.Context(), api, cfg, attempt.Issue)
			if err != nil || len(delivered) != 1 || delivered[0].State != "delivered" {
				t.Fatalf("durable delivery=%#v err=%v", delivered, err)
			}
			messages[key] = delivered
			if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, boundary, issues, remote, current, messages, []string{"implementation"}, check, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
				return true, nil
			}); err != nil {
				t.Fatal(err)
			}
			if respawns != 1 || store.outcomePosts != 1 {
				t.Fatalf("delivery repeated: respawns=%d delivered outcomes=%d", respawns, store.outcomePosts)
			}
		})
	}
}

func TestMergedAttemptRejectsPendingOperatorMessageBeforeDelivery(t *testing.T) {
	comments := []map[string]any{}
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			body, _ := json.Marshal(comments)
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
		}
		var input struct {
			Body string `json:"body"`
		}
		body, _ := io.ReadAll(request.Body)
		if json.Unmarshal(body, &input) != nil {
			t.Fatal("invalid comment mutation")
		}
		comments = append(comments, map[string]any{"body": input.Body, "created_at": time.Now().UTC(), "user": map[string]any{"id": 42}})
		return &http.Response{StatusCode: http.StatusCreated, Status: "201 Created", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: transport}}
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
	message, _ := internalgithub.PrepareOperatorMessage("o/r", 131, 3, "Do not outlive the merge.")
	message, err := internalgithub.RecordOperatorMessage(t.Context(), api, cfg, message)
	if err != nil {
		t.Fatal(err)
	}
	key := "o/r#131/3"
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	remote := []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 131, Attempt: 3, State: "completed", PR: 99}}
	boundary := &refusingHandoffBoundary{}
	if err := resumeOperatorMessages(t.Context(), api, cfg, nil, boundary, nil, remote, nil, messages, []string{"implementation"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != 0 || messages[key][0].State != "rejected" {
		t.Fatalf("merged message delivered: calls=%d message=%#v", boundary.calls, messages[key][0])
	}
	recovered, err := internalgithub.FetchOperatorMessages(t.Context(), api, cfg, 131)
	if err != nil || len(recovered) != 1 || recovered[0].State != "rejected" {
		t.Fatalf("durable rejection=%#v err=%v", recovered, err)
	}
}

func TestOperatorMessageOutcomeFailureAfterCleanupIsRecoveredOnRestart(t *testing.T) {
	store := &operatorMessageStore{}
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: store.roundTrip(t)}}
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
	message, _ := internalgithub.PrepareOperatorMessage("o/r", 131, 3, "Reject this after the merged runtime is cleaned up.")
	if _, err := internalgithub.RecordOperatorMessage(t.Context(), api, cfg, message); err != nil {
		t.Fatal(err)
	}
	terminal := []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 131, Attempt: 3, State: "completed", PR: 99}}
	messages, err := fetchOperatorMessages(t.Context(), api, cfg, nil, terminal, nil)
	if err != nil || len(messages["o/r#131/3"]) != 1 {
		t.Fatalf("message was not rediscovered after cleanup: messages=%#v err=%v", messages, err)
	}
	store.outcomeFailures = 1
	if err := resumeOperatorMessages(t.Context(), api, cfg, nil, &refusingHandoffBoundary{}, nil, terminal, nil, messages, []string{"implementation"}, nil, nil, nil); err == nil {
		t.Fatal("injected outcome failure did not stop reconciliation")
	}
	messages, err = fetchOperatorMessages(t.Context(), api, cfg, nil, terminal, nil)
	if err != nil || len(messages["o/r#131/3"]) != 1 || messages["o/r#131/3"][0].State != "queued" {
		t.Fatalf("restart did not recover unresolved message: messages=%#v err=%v", messages, err)
	}
	if err := resumeOperatorMessages(t.Context(), api, cfg, nil, &refusingHandoffBoundary{}, nil, terminal, nil, messages, []string{"implementation"}, nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	if messages["o/r#131/3"][0].State != "rejected" || store.outcomePosts != 1 {
		t.Fatalf("recovered outcome=%#v posts=%d", messages["o/r#131/3"][0], store.outcomePosts)
	}
}

func TestOperatorMessageTerminalStateWinsAfterDurableClaimBeforeAcceptance(t *testing.T) {
	comments := []map[string]any{}
	transport := roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.Method == http.MethodGet {
			body, _ := json.Marshal(comments)
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
		}
		var input struct {
			Body string `json:"body"`
		}
		body, _ := io.ReadAll(request.Body)
		if json.Unmarshal(body, &input) != nil {
			t.Fatal("invalid comment mutation")
		}
		comments = append(comments, map[string]any{"body": input.Body, "created_at": time.Now().UTC(), "user": map[string]any{"id": 42}})
		return &http.Response{StatusCode: http.StatusCreated, Status: "201 Created", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: transport}}
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ActorID: 42}
	message, _ := internalgithub.PrepareOperatorMessage("o/r", 131, 3, "Stop if the attempt merges during delivery.")
	message, err := internalgithub.RecordOperatorMessage(t.Context(), api, cfg, message)
	if err != nil {
		t.Fatal(err)
	}
	key := "o/r#131/3"
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 131, Attempt: 3, DispatchAuthorized: true}
	active := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 131, Attempt: 3, PR: 99, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", State: "review-ready"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 131, Attempt: 3, BaseSHA: "aaaaaaa", State: "completed", ReviewHead: "bbbbbbb", Session: "retained"}
	boundary := &refusingHandoffBoundary{}
	crashAfterClaim := func(ctx context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
		recovered, err := internalgithub.FetchOperatorMessages(ctx, api, cfg, 131)
		if err != nil || len(recovered) != 1 || recovered[0].State != "claimed" {
			t.Fatalf("delivery was not durably claimed before refresh: %#v err=%v", recovered, err)
		}
		return nil, nil, nil, errors.New("injected restart after claim")
	}
	if err := resumeOperatorMessages(t.Context(), api, cfg, nil, boundary, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{active}, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, nil, crashAfterClaim, nil); err == nil || !strings.Contains(err.Error(), "injected restart") {
		t.Fatalf("claim did not survive injected restart: %v", err)
	}
	recovered, err := internalgithub.FetchOperatorMessages(t.Context(), api, cfg, 131)
	if err != nil || len(recovered) != 1 || recovered[0].State != "claimed" {
		t.Fatalf("restart did not recover claim=%#v err=%v", recovered, err)
	}
	messages[key] = recovered
	manifest.State = "running" // ResumeHandoff may have committed before a crash.
	refreshTerminal := func(context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
		terminal := active
		terminal.State = "completed"
		return []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{terminal}, []agentruntime.Manifest{manifest}, nil
	}
	check := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
	if err := resumeOperatorMessages(t.Context(), api, cfg, nil, boundary, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{active}, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, check, refreshTerminal, nil); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != 0 || messages[key][0].State != "claimed" {
		t.Fatalf("unverified running pane was reused: calls=%d message=%#v", boundary.calls, messages[key][0])
	}
	terminal := active
	terminal.State = "completed"
	if err := resumeOperatorMessages(t.Context(), api, cfg, nil, boundary, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{terminal}, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, check, refreshTerminal, nil); err != nil {
		t.Fatal(err)
	}
	if boundary.calls != 0 || messages[key][0].State != "rejected" {
		t.Fatalf("terminal race reached acceptance: calls=%d message=%#v", boundary.calls, messages[key][0])
	}
	recovered, err = internalgithub.FetchOperatorMessages(t.Context(), api, cfg, 131)
	if err != nil || len(recovered) != 1 || recovered[0].State != "rejected" {
		t.Fatalf("restart-safe terminal outcome=%#v err=%v", recovered, err)
	}
}

func TestOperatorMessageTerminalStateWinsAtAcceptanceBoundary(t *testing.T) {
	runtimeState, attempt, manifest, _ := operatorMessageRuntime(t)
	store := &operatorMessageStore{}
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: store.roundTrip(t)}}
	cfg := internalgithub.PRAdapterConfig{Repository: attempt.Repository, ActorID: 42}
	message, _ := internalgithub.PrepareOperatorMessage(attempt.Repository, attempt.Issue, attempt.Number, "Do not start after merge.")
	message, err := internalgithub.RecordOperatorMessage(t.Context(), api, cfg, message)
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("%s#%d/%d", attempt.Repository, attempt.Issue, attempt.Number)
	messages := map[string][]internalgithub.OperatorMessage{key: {message}}
	active := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "active"}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true, ActiveAttempt: &active}
	refreshes := 0
	refresh := func(context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
		refreshes++
		current, discoverErr := runtimeState.Discover()
		return []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{active}, current, discoverErr
	}
	acceptances := 0
	accept := func(_ context.Context, proposal orchestratoragent.MessageProposal, expected operatorAttemptBinding) (bool, error) {
		acceptances++
		terminal := active
		terminal.State = "completed"
		return matchesOperatorMessageBinding(proposal, expected, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{terminal}), nil
	}
	boundary := &refusingHandoffBoundary{}
	check := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
	if err := resumeOperatorMessages(t.Context(), api, cfg, runtimeState, boundary, []internalgithub.RecoveryIssueFact{issue}, []internalgithub.RecoveryAttemptFact{active}, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, check, refresh, accept); err != nil {
		t.Fatal(err)
	}
	if refreshes != 1 || acceptances != 1 || boundary.calls != 0 || messages[key][0].State != "rejected" {
		t.Fatalf("terminal acceptance race was not excluded: refreshes=%d acceptances=%d calls=%d message=%#v", refreshes, acceptances, boundary.calls, messages[key][0])
	}
	current, err := runtimeState.Discover()
	if err != nil || len(current) != 1 || current[0].State != "completed" {
		t.Fatalf("terminal race changed the retained runtime: manifests=%#v err=%v", current, err)
	}
}

func (b *directHandoffBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	body, err := io.ReadAll(command.Stdin)
	if err != nil {
		return agentruntime.Result{}, err
	}
	var out string
	switch operation {
	case "accept-handoff":
		out, err = acceptHandoff(ctx, body, b.root)
	case "accept-operator-handoff":
		out, err = acceptOperatorHandoff(ctx, body, b.root)
	case "verify-handoff":
		out, err = verifyHandoff(ctx, body, b.root)
	default:
		return agentruntime.Result{}, fmt.Errorf("unexpected operation %q", operation)
	}
	return agentruntime.Result{Output: out}, err
}

func operatorMessageRuntime(t *testing.T) (*agentruntime.Runtime, agentruntime.Attempt, agentruntime.Manifest, *operatorMessageRunner) {
	t.Helper()
	root, stateRoot := t.TempDir(), t.TempDir()
	stateRoot, _ = filepath.EvalSymlinks(stateRoot)
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 131, Number: 3, BaseSHA: strings.Repeat("a", 40)}
	manifest, err := agentruntime.AttemptIdentity(root, attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "completed"
	manifest.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository), "131-3", "agent.log")
	manifest.CreatedAt, manifest.UpdatedAt = time.Now().UTC(), time.Now().UTC()
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(agentruntime.ResultPath(manifest.Worktree), []byte(`{"type":"agent-symphony-result-v1","validation":"retained","documentation":"none"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(manifest.LogPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &operatorMessageRunner{branch: manifest.Branch, head: attempt.BaseSHA}
	runtimeState := &agentruntime.Runtime{Root: root, StateRoot: stateRoot, Source: "source.bundle", Runner: runner, VerifyWorker: func(context.Context) error { return nil }}
	return runtimeState, attempt, manifest, runner
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

func (r *revokedAttemptRunner) Run(ctx context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	if len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("missing tmux operation")
	}
	if command.Name == "git" {
		return agentruntime.ExecRunner{}.Run(ctx, command)
	}
	switch command.Args[0] {
	case "has-session":
		if r.live {
			return agentruntime.Result{}, nil
		}
		return agentruntime.Result{Code: 1, Exited: true}, errors.New("missing session")
	case "send-keys":
		r.interrupted = true
		return agentruntime.Result{}, nil
	case "display-message":
		if !r.interrupted {
			return agentruntime.Result{Output: "0"}, nil
		}
		return agentruntime.Result{Output: "1"}, nil
	case "kill-session":
		r.live = false
		return agentruntime.Result{}, nil
	case "set-option":
		if slices.Contains(command.Args, "new-session") {
			r.live = true
		}
		return agentruntime.Result{}, nil
	case "load-buffer":
		return agentruntime.Result{}, nil
	case "respawn-pane":
		r.starts++
		return agentruntime.Result{}, nil
	default:
		return agentruntime.Result{}, fmt.Errorf("unexpected tmux operation %q", command.Args[0])
	}
}

func TestImplementationPromptDefinesAcceptedResultAndPreservesIssue(t *testing.T) {
	issue := internalgithub.RecoveryIssueFact{Repository: "owner/repo", Issue: 56, Attempt: 3, Body: "## Context\nunique issue contract\n\n## Validation\ngo test ./..."}
	prompt := implementationPrompt(issue)
	for _, want := range []string{"Repository: owner/repo", "Issue: #56", "Attempt: 3", issue.Body, "exactly one JSON line", "at most 64 KiB", "nonempty validation and documentation", "stdout", "stderr", "outside the worktree", "/agent-symphony status needs-attention: REASON", "/agent-symphony status clear: REASON", "`needs-attention` label", "Re-read both the comment and label", "partial-update errors are failures, never success"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("prompt omitted %q: %s", want, prompt)
		}
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

func TestReviewPromptExposesTheSameDirectStatusContract(t *testing.T) {
	prompt := reviewPrompt(internalgithub.RecoveryIssueFact{Issue: 56, Attempt: 3, Body: "review contract"}, strings.Repeat("a", 40))
	for _, want := range []string{"/agent-symphony status needs-attention: REASON", "/agent-symphony status clear: REASON", "`needs-attention` label", "nonempty reason", "fresh re-read", "partial-update errors are failures, never success"} {
		if !strings.Contains(prompt, want) {
			t.Fatalf("review prompt omitted %q: %s", want, prompt)
		}
	}
}

func TestDirectAttentionSetReconcileClearPreservesRunningAttemptAndBlocksQueue(t *testing.T) {
	source := gitRepository(t)
	for _, args := range [][]string{{"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"switch", "-c", "main"}} {
		runGit(t, source, args...)
	}
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, source, "add", "file")
	runGit(t, source, "commit", "-m", "base")
	base := runGit(t, source, "rev-parse", "HEAD")

	now := time.Date(2026, 9, 4, 0, 0, 0, 0, time.UTC)
	activeMarker, _ := internalgithub.ActiveAttemptMarker("o/r", 218, 1, base)
	var snapshots []string
	set, clear := false, false
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		comments := make([]any, len(snapshots))
		for i, snapshot := range snapshots {
			comments[i] = map[string]any{"id": 60 + i, "body": snapshot, "created_at": now.Add(time.Minute), "updated_at": now.Add(time.Minute), "user": map[string]any{"id": 42}}
		}
		comments = append(comments, map[string]any{"id": 65, "body": activeMarker, "created_at": now.Add(time.Minute), "updated_at": now.Add(time.Minute), "user": map[string]any{"id": 42}})
		if set {
			comments = append(comments, map[string]any{"id": 70, "body": "/agent-symphony status needs-attention: operator decision required", "created_at": now.Add(2 * time.Minute), "updated_at": now.Add(2 * time.Minute), "user": map[string]any{"id": 42}})
		}
		if clear {
			comments = append(comments, map[string]any{"id": 71, "body": "/agent-symphony status clear: operator decision recorded", "created_at": now.Add(3 * time.Minute), "updated_at": now.Add(3 * time.Minute), "user": map[string]any{"id": 42}})
		}
		labels := []any{map[string]any{"name": "ready"}, map[string]any{"name": "P1"}}
		if set && !clear {
			labels = append(labels, map[string]any{"name": "needs-attention"})
		}
		var response any
		switch request.Method + " " + request.URL.RequestURI() {
		case "GET /repos/o/r":
			response = map[string]any{"default_branch": "main"}
		case "GET /repos/o/r/branches/main":
			response = map[string]any{"commit": map[string]any{"sha": base}}
		case "GET /repos/o/r/issues?state=open&per_page=100&page=1":
			response = []any{map[string]any{"number": 218, "title": "Direct status", "body": completeImplementationIssueBody, "created_at": now}}
		case "GET /repos/o/r/issues/218":
			response = map[string]any{"number": 218, "node_id": "I_218", "state": "open", "title": "Direct status", "body": completeImplementationIssueBody, "created_at": now, "user": map[string]any{"id": 5}, "labels": labels}
		case "GET /repos/o/r/issues/218/timeline?per_page=100&page=1":
			response = []any{
				map[string]any{"id": 20, "event": "labeled", "label": map[string]any{"name": "ready"}, "created_at": now.Add(time.Second), "actor": map[string]any{"id": 5}},
				map[string]any{"id": 21, "event": "labeled", "label": map[string]any{"name": "P1"}, "created_at": now.Add(2 * time.Second), "actor": map[string]any{"id": 5}},
			}
		case "GET /repos/o/r/issues/218/comments?per_page=100&page=1":
			response = comments
		case "POST /graphql":
			response = map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}}}}
		case "GET /user/5":
			response = map[string]any{"login": "maintainer"}
		case "GET /repos/o/r/collaborators/maintainer/permission":
			response = map[string]any{"permission": "maintain"}
		case "POST /repos/o/r/issues/218/comments":
			var input struct{ Body string }
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			snapshots = append(snapshots, input.Body)
			return &http.Response{StatusCode: http.StatusCreated, Status: "201 Created", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
		default:
			t.Fatalf("unexpected GitHub request %s", request.URL.String())
		}
		body, _ := json.Marshal(response)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}}
	cfg := internalgithub.PRAdapterConfig{Repository: "o/r", ReadyLabel: "ready", HumanReviewLabel: "review", AutonomousMergeLabel: "auto", PriorityP1Label: "P1", PriorityP2Label: "P2", PriorityP3Label: "P3", DependencySection: "Dependencies", DefaultCompletion: "human-review", ApprovalCommand: "/approve", CancelCommand: "/cancel", RetryCommand: "/retry", ActorID: 42}
	facts, err := internalgithub.FetchIssueFacts(t.Context(), api, cfg, nil, true)
	if err != nil || len(facts) != 1 || facts[0].Eligible || !facts[0].Active || !facts[0].DispatchAuthorized {
		t.Fatalf("initial facts=%#v err=%v", facts, err)
	}

	stateRoot, _ := filepath.EvalSymlinks(t.TempDir())
	attemptRoot, _ := filepath.EvalSymlinks(t.TempDir())
	runner := &revokedAttemptRunner{}
	runtimeState := &agentruntime.Runtime{Root: attemptRoot, StateRoot: stateRoot, Source: source, Tmux: "tmux", Runner: runner, VerifyWorker: func(context.Context) error { return nil }}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 218, Number: 1, BaseSHA: base, Command: []string{"agent"}}
	manifest, err := runtimeState.PrepareAndStart(t.Context(), attempt)
	if err != nil || manifest.State != "running" || !runner.live || runner.starts != 1 {
		t.Fatalf("start manifest=%#v live=%v starts=%d err=%v", manifest, runner.live, runner.starts, err)
	}

	set = true
	facts, err = internalgithub.FetchIssueFacts(t.Context(), api, cfg, nil, false)
	if err != nil || len(facts) != 1 || !facts[0].NeedsAttention || facts[0].Eligible || !facts[0].DispatchAuthorized {
		t.Fatalf("set facts=%#v err=%v", facts, err)
	}
	queued := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 219, Attempt: 1, Priority: 1, CreatedAt: now.Add(time.Minute), Eligible: false, DispatchAuthorized: true, NeedsAttention: true, Blockers: []string{"needs attention: queued operator decision required"}}
	issues := append(facts, queued)
	manifests, _ := runtimeState.Discover()
	_, attemptFacts := recoveryAttemptFacts(nil, facts)
	live := func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }
	statuses, decisions := projectRecoveryStatuses(t.Context(), attemptFacts, issues, manifests, 2, live)
	if err := monitorAttempts(t.Context(), runtimeState, statuses, manifests, issues); err != nil {
		t.Fatal(err)
	}
	active := slices.IndexFunc(statuses, func(status orchestrator.RecoveryStatus) bool { return status.Issue == 218 })
	queuedDecision := slices.IndexFunc(decisions, func(decision orchestrator.Decision) bool { return decision.Number == 219 })
	stored, _ := runtimeState.Discover()
	if active < 0 || statuses[active].State != "active" || !statuses[active].NeedsAttention || queuedDecision < 0 || decisions[queuedDecision].State != orchestrator.Blocked || len(stored) != 1 || stored[0].State != "running" || stored[0].Session != manifest.Session || !runner.live || runner.starts != 1 {
		t.Fatalf("set reconcile statuses=%#v decisions=%#v manifests=%#v live=%v starts=%d", statuses, decisions, stored, runner.live, runner.starts)
	}

	clear = true
	facts, err = internalgithub.FetchIssueFacts(t.Context(), api, cfg, nil, false)
	if err != nil || len(facts) != 1 || facts[0].NeedsAttention || !facts[0].DispatchAuthorized {
		t.Fatalf("clear facts=%#v err=%v", facts, err)
	}
	issues = append(facts, queued)
	manifests, _ = runtimeState.Discover()
	_, attemptFacts = recoveryAttemptFacts(nil, facts)
	statuses, decisions = projectRecoveryStatuses(t.Context(), attemptFacts, issues, manifests, 2, live)
	if err := monitorAttempts(t.Context(), runtimeState, statuses, manifests, issues); err != nil {
		t.Fatal(err)
	}
	active = slices.IndexFunc(statuses, func(status orchestrator.RecoveryStatus) bool { return status.Issue == 218 })
	queuedDecision = slices.IndexFunc(decisions, func(decision orchestrator.Decision) bool { return decision.Number == 219 })
	stored, _ = runtimeState.Discover()
	if active < 0 || statuses[active].State != "active" || statuses[active].NeedsAttention || queuedDecision < 0 || decisions[queuedDecision].State != orchestrator.Blocked || len(stored) != 1 || stored[0].State != "running" || stored[0].Session != manifest.Session || !runner.live || runner.starts != 1 {
		t.Fatalf("clear reconcile statuses=%#v decisions=%#v manifests=%#v live=%v starts=%d", statuses, decisions, stored, runner.live, runner.starts)
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
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\ntest \"$PWD\" = /tmp || exit 3\ncase $1 in save-buffer) cat \"$FAKE_PROMPT\";; delete-buffer) exit 0;; wait-for) test -z \"$FAKE_SIGNAL_FAILURE\" || exit 1; printf %s \"$3\" >\"$FAKE_SIGNAL\";; *) exit 2;; esac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("FAKE_PROMPT", prompt)
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

	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", key, handoffReceiptPath(manifest.Worktree, key), head})
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
	pending, err := returnReviewFindings(t.Context(), restarted, boundary, attempt, manifest, head, manifest.ReviewFindings, nil, []string{"implementation"})
	if err != nil || pending {
		t.Fatalf("receipt recovery pending=%v err=%v", pending, err)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	_ = json.Unmarshal(storedBody, &stored)
	if !stored.ReviewHandoffAck {
		t.Fatal("worker receipt was not acknowledged")
	}
	if stored.State != "running" {
		t.Fatalf("acknowledged handoff state=%q, want running", stored.State)
	}
	accepts, _ := os.ReadFile(calls)
	if strings.Count(string(accepts), "x\n") != 1 {
		t.Fatalf("accept calls=%q", accepts)
	}
}

func TestReviewFindingsRejectsMismatchedWorkerReceipt(t *testing.T) {
	worktree, head := t.TempDir(), "abcdef1"
	manifest := agentruntime.Manifest{Worktree: worktree, Session: "as-23-1"}
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", "independent-review-" + head, handoffReceiptPath(worktree, "independent-review-"+head), "wrong-head"})
	result, _ := json.Marshal(agentruntime.Result{Output: string(ack)})
	script := filepath.Join(t.TempDir(), "boundary")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' '"+string(result)+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	boundary := workerBoundaryRunner{Command: script}
	_, err := returnReviewFindings(t.Context(), &agentruntime.Runtime{}, boundary, agentruntime.Attempt{}, manifest, head, []string{"fix"}, nil, []string{"implementation"})
	if err == nil || !strings.Contains(err.Error(), "acceptance binding mismatch") {
		t.Fatalf("mismatched receipt err=%v", err)
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
	key := "independent-review-" + head
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", key, handoffReceiptPath(manifest.Worktree, key), head})
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
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1, Eligible: true, DispatchAuthorized: true}
	bound := []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: base, State: "active"}}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, runtimeState, config.Config{Commands: config.Commands{Implementation: []string{"implementation"}}}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{manifest}, bound, nil, "", state, nil); err != nil {
		log, _ := os.ReadFile(boundaryLog)
		t.Fatalf("transient rework reached GitHub failure: %v\n%s", err, log)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	_ = json.Unmarshal(storedBody, &stored)
	if !stored.ReviewHandoffAck {
		t.Fatal("monitor did not recover worker receipt")
	}
	if stored.State != "running" {
		t.Fatalf("acknowledged handoff state=%q, want running", stored.State)
	}
	restarted := &agentruntime.Runtime{StateRoot: state, Runner: boundary, Tmux: "tmux"}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, restarted, config.Config{Commands: config.Commands{Implementation: []string{"implementation"}}}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{stored}, bound, nil, "", state, nil); err != nil {
		t.Fatalf("retry reached GitHub failure: %v", err)
	}
	storedBody, _ = os.ReadFile(manifestPath)
	_ = json.Unmarshal(storedBody, &stored)
	log, _ := os.ReadFile(boundaryLog)
	if strings.Count(string(log), "accept-handoff") != 1 {
		t.Fatalf("handoff redelivered: %s", log)
	}
	if strings.Count(string(log), `"operation":"export"`) != 1 {
		t.Fatalf("live follow-up was exported: %s", log)
	}
}

func TestCompletedTurnContractFailureIsPreservedBeforeFollowUp(t *testing.T) {
	for _, test := range []struct {
		name, result, export string
	}{
		{name: "missing", export: ""},
		{name: "malformed", result: "not-json", export: "{"},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtimeState, attempt, manifest, _ := operatorMessageRuntime(t)
			resultPath := agentruntime.ResultPath(manifest.Worktree)
			if err := os.Remove(resultPath); err != nil {
				t.Fatal(err)
			}
			if test.result != "" {
				if err := os.WriteFile(resultPath, []byte(test.result), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			boundaryResult, _ := json.Marshal(agentruntime.Result{Output: test.export})
			boundary := filepath.Join(t.TempDir(), "boundary")
			if err := os.WriteFile(boundary, []byte("#!/bin/sh\nprintf '%s' '"+string(boundaryResult)+"'\n"), 0o700); err != nil {
				t.Fatal(err)
			}
			t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", boundary)

			api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
				status, body := http.StatusOK, []byte(`[]`)
				switch {
				case request.Method == http.MethodGet && request.URL.Path == "/user":
					body = []byte(`{"id":42,"login":"coordinator"}`)
				case request.Method == http.MethodPost && request.URL.Path == "/repos/o/r/issues/131/comments":
					status, body = http.StatusCreated, []byte(`{}`)
				case request.Method != http.MethodGet || request.URL.Path != "/repos/o/r/issues/131/comments":
					t.Fatalf("unexpected GitHub request %s %s", request.Method, request.URL.String())
				}
				return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
			})}}
			boundFact := internalgithub.RecoveryAttemptFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "active"}
			bound := []internalgithub.RecoveryAttemptFact{boundFact}
			issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, DispatchAuthorized: true, ActiveAttempt: &boundFact}
			err := monitorQueuedAttempts(t.Context(), api, runtimeState, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{manifest}, bound, nil, "", runtimeState.StateRoot, nil)
			message, _ := internalgithub.PrepareOperatorMessage(attempt.Repository, attempt.Issue, attempt.Number, "Do not mask the preceding result.")
			message.State = "claimed"
			messages := map[string][]internalgithub.OperatorMessage{fmt.Sprintf("%s#%d/%d", attempt.Repository, attempt.Issue, attempt.Number): {message}}
			followUp := &refusingHandoffBoundary{}
			if err == nil {
				refresh := func(context.Context) ([]internalgithub.RecoveryIssueFact, []internalgithub.RecoveryAttemptFact, []agentruntime.Manifest, error) {
					current, discoverErr := runtimeState.Discover()
					return []internalgithub.RecoveryIssueFact{issue}, bound, current, discoverErr
				}
				err = resumeOperatorMessages(t.Context(), api, internalgithub.PRAdapterConfig{}, runtimeState, followUp, []internalgithub.RecoveryIssueFact{issue}, bound, []agentruntime.Manifest{manifest}, messages, []string{"implementation"}, func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil }, refresh, func(context.Context, orchestratoragent.MessageProposal, operatorAttemptBinding) (bool, error) {
					return true, nil
				})
			}
			if err == nil || !strings.Contains(err.Error(), "invalid attested metadata") {
				t.Fatalf("invalid completed result did not stop reconciliation: %v", err)
			}
			if followUp.calls != 0 {
				t.Fatal("queued operator message replaced the invalid completed result")
			}
			body, readErr := os.ReadFile(resultPath)
			if test.result == "" {
				if !errors.Is(readErr, os.ErrNotExist) {
					t.Fatalf("missing result was replaced: %v", readErr)
				}
			} else if readErr != nil || string(body) != test.result {
				t.Fatalf("malformed result was not preserved: %q %v", body, readErr)
			}
		})
	}
}

func TestPublishedPROperatorMessageFollowUpIsPublishedWithoutPRFeedback(t *testing.T) {
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
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("operator follow-up"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "commit", "-am", "operator follow-up")
	head := runGit(t, worker, "rev-parse", "HEAD")
	branch, _ := internalgithub.AttemptBranch("o/r", 23, 1)
	runGit(t, coordinator, "init", "--bare", remote)
	runGit(t, worker, "push", remote, base+":refs/heads/"+branch)
	runGit(t, coordinator, "remote", "add", "origin", remote)

	bundlePath := filepath.Join(t.TempDir(), "attempt.bundle")
	runGit(t, worker, "bundle", "create", bundlePath, "--all")
	bundle, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	exported := workerExport{Type: "agent-symphony-export-v1", Repository: "o/r", Branch: branch, BaseSHA: base, HeadSHA: head, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "stale diagnostic probe", Documentation: "none"}, Bundle: base64.StdEncoding.EncodeToString(bundle)}
	exportedJSON, _ := json.Marshal(exported)
	boundaryResult, _ := json.Marshal(agentruntime.Result{Output: string(exportedJSON)})
	implementation := filepath.Join(t.TempDir(), "implementation-boundary")
	if err := os.WriteFile(implementation, []byte("#!/bin/sh\nprintf '%s' '"+string(boundaryResult)+"'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", implementation)

	message, _ := internalgithub.PrepareOperatorMessage("o/r", 23, 1, "Apply the follow-up.")
	key := "operator-message-" + message.ID
	receiptPath := handoffReceiptPath(worker, key)
	if err := os.MkdirAll(filepath.Dir(receiptPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(receiptPath, []byte("malformed worker receipt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(filepath.Dir(receiptPath), "operator-message-forged.receipt"), []byte(`{"type":"agent-symphony-handoff-executed-v1"}`), 0o600); err != nil {
		t.Fatal(err)
	}

	recoveryPath := filepath.Join(t.TempDir(), "pr.json")
	recoveryBody, _ := json.Marshal([]internalgithub.PRState{{Repository: "o/r", Number: 7, Issue: 23, Attempt: 1, HeadSHA: base}})
	if err := os.WriteFile(recoveryPath, recoveryBody, 0o600); err != nil {
		t.Fatal(err)
	}
	var pullBody string
	var comments []map[string]any
	api := internalgithub.API{BaseURL: "https://example.test", Retries: -1, HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusOK
		var body []byte
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/user":
			body = []byte(`{"id":42,"login":"coordinator"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/pulls":
			body, _ = json.Marshal([]map[string]any{{"number": 7, "body": pullBody, "user": map[string]any{"id": 42}, "head": map[string]any{"sha": head, "ref": branch}}})
		case request.Method == http.MethodPatch && request.URL.Path == "/repos/o/r/pulls/7":
			var input struct {
				Body string `json:"body"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			pullBody = input.Body
			body = []byte(`{}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/issues/23/comments":
			body, _ = json.Marshal(comments)
		case request.Method == http.MethodPost && request.URL.Path == "/repos/o/r/issues/23/comments":
			var input struct {
				Body string `json:"body"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			comments = append(comments, map[string]any{"body": input.Body, "user": map[string]any{"id": 42}})
			status, body = http.StatusCreated, []byte(`{}`)
		default:
			t.Fatalf("unexpected GitHub request %s %s", request.Method, request.URL.String())
		}
		return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}}

	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(coordinator); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	manifest := agentruntime.Manifest{Version: 1, Repository: "o/r", Issue: 23, Attempt: 1, Branch: branch, Worktree: worker, BaseSHA: base, State: "completed", ReviewState: "clean", ReviewBase: base, ReviewHead: head, UpdatedAt: time.Now().UTC()}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 2, Title: "Operator follow-up", BaseBranch: "main", DispatchAuthorized: true}
	bound := []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 23, Attempt: 1, PR: 7, BaseSHA: base, HeadSHA: base, State: "review-ready"}}
	message.State = "delivered"
	operatorMessages := map[string][]internalgithub.OperatorMessage{"o/r#23/1": {message}}
	if err := monitorQueuedAttempts(t.Context(), api, &agentruntime.Runtime{}, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{manifest}, bound, operatorMessages, recoveryPath, t.TempDir(), nil); err != nil {
		t.Fatal(err)
	}
	if got := runGit(t, remote, "rev-parse", "refs/heads/"+branch); got != head {
		t.Fatalf("published head=%s want %s; body=%q comments=%#v", got, head, pullBody, comments)
	}
	if !strings.Contains(pullBody, head) || !strings.Contains(pullBody, "Attempt: 1") || strings.Contains(pullBody, "Attempt: 2") || len(comments) != 3 {
		t.Fatalf("pull request was not fully updated: body=%q comments=%#v", pullBody, comments)
	}
	if !strings.Contains(pullBody, "Independent review passed for exact head `"+head+"`.") || strings.Contains(pullBody, "stale diagnostic probe") {
		t.Fatalf("pull request did not publish exact-head review evidence: body=%q", pullBody)
	}
	recovered, err := (&internalgithub.FileRecovery{Path: recoveryPath}).PullRequestState(t.Context(), "o/r", 7, 23, 1, head)
	if err != nil || recovered.HeadSHA != head {
		t.Fatalf("published follow-up did not advance recovery: state=%#v err=%v", recovered, err)
	}
}

func TestCompletedCleanAttemptQueuesRequiredBaseDriftForImplementation(t *testing.T) {
	coordinator, worker, remote := gitRepository(t), t.TempDir(), filepath.Join(t.TempDir(), "remote.git")
	runGit(t, worker, "init")
	runGit(t, worker, "config", "user.email", "test@example.invalid")
	runGit(t, worker, "config", "user.name", "test")
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
	branch, _ := internalgithub.AttemptBranch("o/r", 23, 1)
	runGit(t, coordinator, "init", "--bare", remote)
	runGit(t, coordinator, "remote", "set-url", "origin", remote)
	runGit(t, worker, "push", remote, base+":refs/heads/"+branch)

	bundlePath := filepath.Join(t.TempDir(), "attempt.bundle")
	runGit(t, worker, "bundle", "create", bundlePath, "--all")
	bundle, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	exported := workerExport{Type: "agent-symphony-export-v1", Repository: "o/r", Branch: branch, BaseSHA: base, HeadSHA: head, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "old validation", Documentation: "none"}, Bundle: base64.StdEncoding.EncodeToString(bundle)}
	exportedJSON, _ := json.Marshal(exported)
	exportResult, _ := json.Marshal(agentruntime.Result{Output: string(exportedJSON)})

	stateRoot, worktree := t.TempDir(), t.TempDir()
	manifest := agentruntime.Manifest{Version: 1, Repository: "o/r", Issue: 23, Attempt: 1, Branch: branch, Worktree: worktree, Session: "as-23-1", BaseSHA: base, LogPath: filepath.Join(worktree, "attempt.log"), State: "completed", ReviewState: "clean", ReviewBase: base, ReviewHead: head, CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC()}
	manifestPath := filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier("o/r"), "23-1", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	manifestBody, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, manifestBody, 0o600); err != nil {
		t.Fatal(err)
	}
	key := "independent-review-" + head
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", key, handoffReceiptPath(worktree, key), head})
	ackResult, _ := json.Marshal(agentruntime.Result{Output: string(ack)})
	script := filepath.Join(t.TempDir(), "boundary")
	scriptBody := fmt.Sprintf("#!/bin/sh\npayload=$(sed -n '1p')\ncase \"$payload\" in\n  *'\"operation\":\"export\"'*) printf '%%s' '%s';;\n  *) printf '%%s' '%s';;\nesac\n", exportResult, ackResult)
	if err := os.WriteFile(script, []byte(scriptBody), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", script)

	currentBase := strings.Repeat("b", 40)
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1, BaseBranch: "main", BaseSHA: currentBase, DispatchAuthorized: true}
	pending, err := publishWorkerResult(t.Context(), internalgithub.API{}, &agentruntime.Runtime{StateRoot: stateRoot}, config.Config{Commands: config.Commands{Implementation: []string{"implementation"}}}, issue, manifest, nil, stateRoot, nil, nil)
	if err != nil || pending {
		t.Fatalf("base refresh handoff pending=%v err=%v", pending, err)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	_ = json.Unmarshal(storedBody, &stored)
	if stored.State != "running" || !stored.ReviewHandoffAck || len(stored.ReviewFindings) != 1 || !strings.Contains(stored.ReviewFindings[0], currentBase) {
		t.Fatalf("base drift was not durably handed back: %#v", stored)
	}
	if got := runGit(t, remote, "rev-parse", "refs/heads/"+branch); got != base {
		t.Fatalf("stale head was published: got %s want %s", got, base)
	}
}

func TestReconcileMonitorsAndPublishesCompletedAttemptBeforePRGovernanceFailure(t *testing.T) {
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
	runGit(t, coordinator, "init", "--bare", remote)
	runGit(t, worker, "push", remote, base+":refs/heads/main")
	runGit(t, coordinator, "remote", "add", "origin", remote)
	if err := os.WriteFile(filepath.Join(worker, "file"), []byte("completed"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, worker, "commit", "-am", "completed")
	head := runGit(t, worker, "rev-parse", "HEAD")
	branch, _ := internalgithub.AttemptBranch("o/r", 23, 1)

	bundlePath := filepath.Join(t.TempDir(), "attempt.bundle")
	runGit(t, worker, "bundle", "create", bundlePath, "--all")
	bundle, err := os.ReadFile(bundlePath)
	if err != nil {
		t.Fatal(err)
	}
	exported := workerExport{Type: "agent-symphony-export-v1", Repository: "o/r", Branch: branch, BaseSHA: base, HeadSHA: head, BundleSHA256: fmt.Sprintf("%x", sha256.Sum256(bundle)), Clean: true, Result: workerResult{Type: "agent-symphony-result-v1", Validation: "focused tests passed", Documentation: "none"}, Bundle: base64.StdEncoding.EncodeToString(bundle)}
	exportedJSON, _ := json.Marshal(exported)
	boundaryResult, _ := json.Marshal(agentruntime.Result{Output: string(exportedJSON)})
	implementation := filepath.Join(t.TempDir(), "implementation-boundary")
	boundaryScript := fmt.Sprintf(`#!/bin/sh
payload=$(sed -n '1p')
case "$payload" in
  *'"operation":"verify"'*) printf '{"Code":0}';;
  *'"branch","--show-current"'*) printf '{"Output":"%s"}';;
  *'"rev-parse","HEAD"'*) printf '{"Output":"%s"}';;
  *'"display-message"'*) printf '{"Output":"1 0"}';;
  *'"capture-pane"'*) printf '{"Output":"completed"}';;
  *'"operation":"export"'*) printf '%%s' '%s';;
  *) printf '{"Code":0}';;
esac
`, branch, head, string(boundaryResult))
	if err := os.WriteFile(implementation, []byte(boundaryScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", implementation)

	issueCreated := time.Date(2026, 8, 19, 1, 0, 0, 0, time.UTC)
	activeMarker, err := internalgithub.ActiveAttemptMarker("o/r", 23, 1, base)
	if err != nil {
		t.Fatal(err)
	}
	var pullBody string
	created, governanceFailed := false, false
	comments := []map[string]any{{"id": 1, "body": activeMarker, "created_at": issueCreated, "updated_at": issueCreated, "user": map[string]any{"id": 42}}}
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusOK
		var body []byte
		switch {
		case request.Method == http.MethodGet && request.URL.Path == "/user":
			body = []byte(`{"id":42,"login":"coordinator"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/user/42":
			body = []byte(`{"login":"coordinator"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r":
			body = []byte(`{"full_name":"o/r","default_branch":"main","permissions":{"pull":true}}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/branches/main":
			body, _ = json.Marshal(map[string]any{"commit": map[string]any{"sha": base}})
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/issues":
			body, _ = json.Marshal([]map[string]any{{"number": 23, "title": "Completed attempt", "body": completeImplementationIssueBody, "created_at": issueCreated}})
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/issues/23":
			body, _ = json.Marshal(map[string]any{"number": 23, "node_id": "issue-node", "state": "open", "body": completeImplementationIssueBody, "created_at": issueCreated, "user": map[string]any{"id": 42}, "labels": []map[string]any{{"name": "agent-ready"}, {"name": "priority:P1"}}})
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/issues/23/timeline":
			body, _ = json.Marshal([]map[string]any{
				{"id": 11, "event": "labeled", "created_at": issueCreated.Add(time.Minute), "actor": map[string]any{"id": 42}, "label": map[string]any{"name": "agent-ready"}},
				{"id": 12, "event": "labeled", "created_at": issueCreated.Add(2 * time.Minute), "actor": map[string]any{"id": 42}, "label": map[string]any{"name": "priority:P1"}},
			})
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/collaborators/coordinator/permission":
			body = []byte(`{"permission":"admin"}`)
		case request.Method == http.MethodPost && request.URL.Path == "/graphql":
			body = []byte(`{"data":{"repository":{"issue":{"userContentEdits":{"nodes":[]}}}}}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/pulls":
			var pulls []map[string]any
			if created {
				pulls = append(pulls, map[string]any{"number": 7, "body": pullBody, "state": "open", "user": map[string]any{"id": 42}, "head": map[string]any{"sha": head, "ref": branch}, "base": map[string]any{"sha": base}})
			}
			body, _ = json.Marshal(pulls)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/pulls/7":
			governanceFailed = true
			status, body = http.StatusInternalServerError, []byte(`{"message":"forced governance failure"}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/commits/"+head+"/check-runs":
			body = []byte(`{"check_runs":[]}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/commits/"+head+"/status":
			body = []byte(`{"statuses":[]}`)
		case request.Method == http.MethodPost && request.URL.Path == "/repos/o/r/pulls":
			var input struct {
				Body string `json:"body"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			created, pullBody = true, input.Body
			status, body = http.StatusCreated, []byte(`{"number":7}`)
		case request.Method == http.MethodPatch && request.URL.Path == "/repos/o/r/pulls/7":
			var input struct {
				Body string `json:"body"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			pullBody, body = input.Body, []byte(`{}`)
		case request.Method == http.MethodGet && request.URL.Path == "/repos/o/r/issues/23/comments":
			body, _ = json.Marshal(comments)
		case request.Method == http.MethodPost && request.URL.Path == "/repos/o/r/issues/23/comments":
			var input struct {
				Body string `json:"body"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			createdAt := issueCreated.Add(time.Duration(len(comments)) * time.Minute)
			comments = append(comments, map[string]any{"id": len(comments) + 1, "body": input.Body, "created_at": createdAt, "updated_at": createdAt, "user": map[string]any{"id": 42}})
			status, body = http.StatusCreated, []byte(`{}`)
		default:
			t.Fatalf("unexpected GitHub request %s %s", request.Method, request.URL.String())
		}
		return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}

	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(coordinator); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	configPath := filepath.Join(coordinator, config.DefaultPath)
	if err := config.Write(configPath, config.Default("o/r")); err != nil {
		t.Fatal(err)
	}
	stateRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshotRoot := reviewSnapshotRoot
	reviewSnapshotRoot = filepath.Join(t.TempDir(), "snapshots")
	t.Cleanup(func() { reviewSnapshotRoot = oldSnapshotRoot })
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: base}
	attemptRoot := productionAttemptRoot(stateRoot)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest, err := agentruntime.AttemptIdentity(attemptRoot, attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.LogPath = filepath.Join(stateRoot, "attempts", internalgithub.RepositoryIdentifier("o/r"), "23-1", "agent.log")
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest.State, manifest.ReviewState, manifest.ReviewBase, manifest.ReviewHead = "running", "clean", base, head
	manifest.ReviewSession, _ = agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, manifest.Repository, manifest.Issue, manifest.Attempt)
	manifest.CreatedAt, manifest.UpdatedAt = issueCreated, issueCreated
	manifestPath := filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	manifestBody, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, manifestBody, 0o600); err != nil {
		t.Fatal(err)
	}
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = "https://example.test", client
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	t.Setenv("AGENT_SYMPHONY_REVIEW_BOUNDARY", implementation)
	statePath := filepath.Join(stateRoot, "pr.json")
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	statuses, err := reconcileGitHub(t.Context(), configPath, statePath, stateRoot, true)
	if err == nil || !governanceFailed {
		t.Fatalf("pull-request governance did not fail after publication: err=%v failed=%v", err, governanceFailed)
	}
	if !created {
		t.Fatalf("completed unpublished attempt was not published: statuses=%#v comments=%#v", statuses, comments)
	}
	if got := runGit(t, remote, "rev-parse", "refs/heads/"+branch); got != head {
		t.Fatalf("published head=%s want %s", got, head)
	}
	if !created || !strings.Contains(pullBody, head) || len(comments) != 5 {
		t.Fatalf("publication incomplete: created=%v body=%q comments=%#v", created, pullBody, comments)
	}
	projected, err := (&dashboardServer{stateRoot: stateRoot}).projectedStatus(23, 1)
	if err != nil || projected.CurrentPhase != "publication" || slices.ContainsFunc(projected.Sessions, func(session orchestrator.AttemptSession) bool {
		return session.Role == agentruntime.SessionRoleReviewer
	}) {
		t.Fatalf("post-review projection=%#v err=%v", projected, err)
	}
}

func TestReconcileProjectsDispatchedSessionBeforeFailedRemoteRefresh(t *testing.T) {
	root := gitRepository(t)
	for _, args := range [][]string{{"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}, {"switch", "-c", "main"}} {
		runGit(t, root, args...)
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("base"), 0o600); err != nil {
		t.Fatal(err)
	}
	runGit(t, root, "add", "file")
	runGit(t, root, "commit", "-m", "base")
	base := runGit(t, root, "rev-parse", "HEAD")
	runGit(t, root, "update-ref", "refs/remotes/origin/main", base)

	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, config.Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	stateRoot, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	oldSnapshotRoot := reviewSnapshotRoot
	reviewSnapshotRoot = filepath.Join(t.TempDir(), "snapshots")
	t.Cleanup(func() { reviewSnapshotRoot = oldSnapshotRoot })
	attemptRoot := productionAttemptRoot(stateRoot)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	attempt := agentruntime.Attempt{Repository: "owner/repo", Issue: 184, Number: 1, BaseSHA: base}
	manifest, err := agentruntime.AttemptIdentity(attemptRoot, attempt)
	if err != nil {
		t.Fatal(err)
	}
	source := filepath.Join(attemptRoot, internalgithub.RepositoryIdentifier(attempt.Repository)+".source.bundle")
	boundary := filepath.Join(t.TempDir(), "implementation-boundary")
	sessionState := filepath.Join(t.TempDir(), "session")
	boundaryScript := fmt.Sprintf(`#!/bin/sh
payload=$(sed -n '1p')
case "$payload" in
  *'"clone","--no-local","--no-checkout"'*) git clone --no-local --no-checkout %q %q >/dev/null 2>&1;;
  *'"checkout","--detach"'*) git -C %q checkout --detach %q >/dev/null 2>&1;;
  *'"switch","-c"'*) git -C %q switch -c %q >/dev/null 2>&1;;
  *'"remote","remove","origin"'*) git -C %q remote remove origin;;
  *'"config","--local","credential.helper",""'*) git -C %q config --local credential.helper '';;
  *'"branch","--show-current"'*) printf '{"Output":"%s"}'; exit 0;;
  *'"rev-parse","HEAD"'*) printf '{"Output":"%s"}'; exit 0;;
  *'"has-session"'*) if test -f %q; then printf '{"Code":0}'; else printf '{"Code":1,"Exited":true}'; fi; exit 0;;
  *'"new-session"'*) touch %q;;
  *) printf '{"Code":0}'; exit 0;;
esac
if test $? -eq 0; then printf '{"Code":0}'; else printf '{"Code":1,"Exited":true}'; fi
`, source, manifest.Worktree, manifest.Worktree, base, manifest.Worktree, manifest.Branch, manifest.Worktree, manifest.Worktree, manifest.Branch, base, sessionState, sessionState)
	if err := os.WriteFile(boundary, []byte(boundaryScript), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_WORKER_BOUNDARY", boundary)

	createdAt := time.Date(2026, 8, 30, 1, 0, 0, 0, time.UTC)
	var comments []map[string]any
	dispatched := false
	client := &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		status := http.StatusOK
		var response any
		switch request.Method + " " + request.URL.Path {
		case "GET /user":
			response = map[string]any{"id": 42, "login": "coordinator"}
		case "GET /user/42":
			response = map[string]any{"login": "coordinator"}
		case "GET /repos/owner/repo":
			response = map[string]any{"full_name": "owner/repo", "default_branch": "main", "permissions": map[string]any{"pull": true}}
		case "GET /repos/owner/repo/branches/main":
			response = map[string]any{"commit": map[string]any{"sha": base}}
		case "GET /repos/owner/repo/issues":
			response = []map[string]any{{"number": 184, "title": "Project dispatched session", "body": completeImplementationIssueBody, "created_at": createdAt}}
		case "GET /repos/owner/repo/issues/184":
			response = map[string]any{"number": 184, "node_id": "issue-node", "state": "open", "title": "Project dispatched session", "body": completeImplementationIssueBody, "created_at": createdAt, "user": map[string]any{"id": 42}, "labels": []map[string]any{{"name": "agent-ready"}, {"name": "priority:P1"}}}
		case "GET /repos/owner/repo/issues/184/timeline":
			response = []map[string]any{
				{"id": 11, "event": "labeled", "created_at": createdAt.Add(time.Minute), "actor": map[string]any{"id": 42}, "label": map[string]any{"name": "agent-ready"}},
				{"id": 12, "event": "labeled", "created_at": createdAt.Add(2 * time.Minute), "actor": map[string]any{"id": 42}, "label": map[string]any{"name": "priority:P1"}},
			}
		case "GET /repos/owner/repo/issues/184/comments":
			response = comments
		case "GET /repos/owner/repo/collaborators/coordinator/permission":
			response = map[string]any{"permission": "admin"}
		case "GET /repos/owner/repo/pulls":
			if dispatched {
				status, response = http.StatusBadRequest, map[string]any{"message": "forced post-dispatch read failure"}
			} else {
				response = []any{}
			}
		case "POST /graphql":
			response = map[string]any{"data": map[string]any{"repository": map[string]any{"issue": map[string]any{"userContentEdits": map[string]any{"nodes": []any{}}}}}}
		case "POST /repos/owner/repo/issues/184/comments":
			var input struct {
				Body string `json:"body"`
			}
			if err := json.NewDecoder(request.Body).Decode(&input); err != nil {
				t.Fatal(err)
			}
			comments = append(comments, map[string]any{"id": len(comments) + 1, "body": input.Body, "created_at": createdAt.Add(time.Duration(len(comments)+1) * time.Minute), "updated_at": createdAt.Add(time.Duration(len(comments)+1) * time.Minute), "user": map[string]any{"id": 42}})
			dispatched = dispatched || strings.Contains(input.Body, "agent-symphony:active-attempt:v1")
			status, response = http.StatusCreated, map[string]any{}
		default:
			t.Fatalf("unexpected GitHub request %s", request.URL.String())
		}
		body, _ := json.Marshal(response)
		return &http.Response{StatusCode: status, Status: fmt.Sprintf("%d", status), Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = "https://example.test", client
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })

	statePath := filepath.Join(stateRoot, "pr.json")
	if err := os.WriteFile(statePath, []byte("[]\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = reconcileGitHub(t.Context(), configPath, statePath, stateRoot, true)
	if err == nil || !strings.Contains(err.Error(), "refresh dispatched pull request attempts") || !dispatched {
		t.Fatalf("post-dispatch failure err=%v dispatched=%v", err, dispatched)
	}
	projected, err := (&dashboardServer{stateRoot: stateRoot}).projectedStatus(184, 1)
	if err != nil || projected.State != "active" || projected.CurrentPhase != "implementation" || projected.Session != manifest.Session || len(projected.Sessions) != 1 || projected.Sessions[0].Name != manifest.Session {
		t.Fatalf("local dispatched projection=%#v err=%v", projected, err)
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

func TestBoundAttemptSuppressesRestartDispatchAcrossWorkerAndReviewStates(t *testing.T) {
	binding := internalgithub.RecoveryAttemptFact{Repository: "o/r", Issue: 23, Attempt: 4, BaseSHA: "abcdef0", State: "active"}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 4, Priority: 1, CreatedAt: time.Unix(1, 0), Active: true, DispatchAuthorized: true, ActiveAttempt: &binding}
	fact := orchestrator.AttemptFact{Repository: binding.Repository, Issue: binding.Issue, Attempt: binding.Attempt, BaseSHA: binding.BaseSHA, State: binding.State}
	for _, test := range []struct {
		manifest agentruntime.Manifest
		blocked  bool
	}{
		{manifest: agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 4, BaseSHA: "abcdef0", State: "preparing", Session: "pending"}, blocked: true},
		{manifest: agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 4, BaseSHA: "abcdef0", State: "running", Session: "live"}},
		{manifest: agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 4, BaseSHA: "abcdef0", State: "completed"}},
		{manifest: agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 4, BaseSHA: "abcdef0", State: "completed", ReviewState: "running", ReviewHead: "head"}},
		{manifest: agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 4, BaseSHA: "abcdef0", State: "completed", ReviewState: "clean", ReviewHead: "head"}},
	} {
		manifests := []agentruntime.Manifest{test.manifest}
		for range 2 {
			var err error
			manifests, err = resumeBoundAttempts(t.Context(), nil, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, manifests, nil)
			if err != nil || len(manifests) != 1 {
				t.Fatalf("state=%s review=%s manifests=%#v err=%v", test.manifest.State, test.manifest.ReviewState, manifests, err)
			}
		}
		statuses := orchestrator.RecoverChecked(t.Context(), []orchestrator.AttemptFact{fact}, manifests, func(context.Context, agentruntime.Manifest, orchestrator.AttemptFact) error { return nil })
		_, decisions := joinIssueProjection(statuses, []internalgithub.RecoveryIssueFact{issue}, 1)
		if len(decisions) != 1 || decisions[0].State != orchestrator.Active {
			t.Fatalf("state=%s review=%s statuses=%#v decisions=%#v", test.manifest.State, test.manifest.ReviewState, statuses, decisions)
		}
		if test.blocked && (len(statuses) != 1 || statuses[0].State != "blocked" || !slices.Contains(statuses[0].Blockers, "runtime liveness mismatch")) {
			t.Fatalf("preparing attempt did not fail closed: %#v", statuses)
		}
	}
	conflictingRemote := []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 23, Attempt: 3, PR: 31, State: "active"}}
	if manifests, err := resumeBoundAttempts(t.Context(), nil, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, nil, conflictingRemote); err != nil || len(manifests) != 0 {
		t.Fatalf("contradictory remote attempt was resumed: manifests=%#v err=%v", manifests, err)
	}
	orphan := agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 3, BaseSHA: "aaaaaaa", State: "completed"}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, nil, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{orphan}, nil, nil, "", "", nil); err != nil {
		t.Fatalf("genuine orphan was not preserved: %v", err)
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

func TestMergedAttemptCleansOperatorMessageRaceStates(t *testing.T) {
	fact := orchestrator.AttemptFact{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", PR: 7, State: "completed"}
	for _, test := range []struct {
		name, state, operation string
	}{
		{"message queued", "completed", "cleanup"},
		{"message claimed after resume", "running", "abandon"},
		{"follow-up active", "running", "abandon"},
	} {
		t.Run(test.name, func(t *testing.T) {
			manifest := agentruntime.Manifest{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: "aaaaaaa", State: test.state, ReviewHead: "bbbbbbb", Session: "operator-follow-up"}
			boundary := &cleanupBoundary{}
			if err := cleanupCompletedAttempts(t.Context(), boundary, []orchestrator.AttemptFact{fact}, []agentruntime.Manifest{manifest}); err != nil {
				t.Fatal(err)
			}
			if !slices.Equal(boundary.operations, []string{test.operation}) || len(boundary.manifests) != 1 || !reflect.DeepEqual(boundary.manifests[0], manifest) {
				t.Fatalf("merged %s cleanup operations=%v manifests=%#v", test.name, boundary.operations, boundary.manifests)
			}
		})
	}
}

func TestRevokedBoundAttemptCancelsWorkerAndSuppressesPublication(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	state, _ = filepath.EvalSymlinks(state)
	root, _ = filepath.EvalSymlinks(root)
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 4, BaseSHA: "abcdef0"}
	manifest, err := agentruntime.AttemptIdentity(root, attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "running"
	sum := sha256.Sum256([]byte(attempt.Repository))
	manifest.LogPath = filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-4", "agent.log")
	manifestPath := filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &revokedAttemptRunner{live: true}
	runtimeState := &agentruntime.Runtime{Root: root, StateRoot: state, Tmux: "tmux", Runner: runner, StopWait: time.Millisecond}
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 23, Attempt: 4, State: "active", Action: "monitor the implementation session"}
	authorized := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 4, DispatchAuthorized: true}
	if err := monitorAttempts(t.Context(), runtimeState, []orchestrator.RecoveryStatus{status}, []agentruntime.Manifest{manifest}, []internalgithub.RecoveryIssueFact{authorized}); err != nil || !runner.live {
		t.Fatalf("authorized worker did not continue: live=%v err=%v", runner.live, err)
	}
	revoked := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 4}
	if err := monitorAttempts(t.Context(), runtimeState, []orchestrator.RecoveryStatus{status}, []agentruntime.Manifest{manifest}, []internalgithub.RecoveryIssueFact{revoked}); err != nil {
		t.Fatal(err)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	if json.Unmarshal(storedBody, &stored) != nil || stored.State != "cancelled" || runner.live {
		t.Fatalf("revoked worker continued: manifest=%#v live=%v", stored, runner.live)
	}
	completed := manifest
	completed.State = "completed"
	bound := []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 23, Attempt: 4, BaseSHA: attempt.BaseSHA, State: "active"}}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, nil, config.Config{}, []internalgithub.RecoveryIssueFact{revoked}, []agentruntime.Manifest{completed}, bound, nil, "", "", nil); err != nil {
		t.Fatalf("revoked completed attempt reached publication: %v", err)
	}
}

func TestMonitorConsumesDeadAttemptFromLivenessMismatchProjection(t *testing.T) {
	state, root := t.TempDir(), t.TempDir()
	state, _ = filepath.EvalSymlinks(state)
	root, _ = filepath.EvalSymlinks(root)
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 4, BaseSHA: "abcdef0"}
	manifest, err := agentruntime.AttemptIdentity(root, attempt)
	if err != nil {
		t.Fatal(err)
	}
	manifest.State = "running"
	manifest.LogPath = filepath.Join(state, "attempts", internalgithub.RepositoryIdentifier(attempt.Repository), "23-4", "agent.log")
	manifestPath := filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	runner := &operatorMessageRunner{dead: true}
	runtimeState := &agentruntime.Runtime{Root: root, StateRoot: state, Tmux: "tmux", Runner: runner}
	status := orchestrator.RecoveryStatus{Repository: "o/r", Issue: 23, Attempt: 4, State: "blocked", Blockers: []string{"runtime liveness mismatch"}, Diagnostic: "worktree HEAD does not match GitHub"}
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 4, DispatchAuthorized: true}
	if err := monitorAttempts(t.Context(), runtimeState, []orchestrator.RecoveryStatus{status}, []agentruntime.Manifest{manifest}, []internalgithub.RecoveryIssueFact{issue}); err != nil {
		t.Fatal(err)
	}
	storedBody, _ := os.ReadFile(manifestPath)
	var stored agentruntime.Manifest
	log, _ := os.ReadFile(manifest.LogPath)
	if json.Unmarshal(storedBody, &stored) != nil || stored.State != "completed" || string(log) != "completed" {
		t.Fatalf("dead liveness-mismatch attempt was not captured: manifest=%#v log=%q", stored, log)
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
	base := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", source, "rev-parse", "HEAD"))))
	if err := os.WriteFile(filepath.Join(source, "upstream"), []byte("current base"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", source, "add", "upstream").CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	if out, err := exec.Command("git", "-C", source, "commit", "-m", "current base").CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	reviewBase := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", source, "rev-parse", "HEAD"))))
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("changed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", source, "commit", "-am", "change").CombinedOutput(); err != nil {
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
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1, BaseSHA: reviewBase}
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
	result, pending, err := runIndependentReview(t.Context(), nil, agentruntime.Attempt{Repository: issue.Repository, Issue: issue.Issue, Number: issue.Attempt, BaseSHA: base}, workerBoundaryRunner{Command: script}, env, []string{"reviewer", "--custom"}, issue, agentruntime.Manifest{}, source, head, reviewSnapshotRoot)
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
	if info, err := os.Stat(result.Snapshot); err != nil || info.Mode().Perm() != 0o550 {
		t.Fatalf("snapshot mode=%v err=%v", info.Mode().Perm(), err)
	}
	if info, err := os.Stat(filepath.Join(result.Snapshot, "file")); err != nil || info.Mode().Perm() != 0o440 {
		t.Fatalf("snapshot file mode=%v err=%v", info.Mode().Perm(), err)
	}
	resultPath := reviewResultPath(result.Snapshot, head)
	if info, err := os.Stat(filepath.Dir(resultPath)); err != nil || info.Mode().Perm() != 0o770 {
		t.Fatalf("review result directory=%v err=%v", info, err)
	}
	if _, err := os.Stat(resultPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("review result existed before capture: %v", err)
	}
	log, err := os.ReadFile(boundaryLog)
	load, start := strings.Index(string(log), `"args":["load-buffer"`), strings.Index(string(log), `"args":["respawn-pane"`)
	if err != nil || load < 0 || start < load || !strings.Contains(string(log), "worker-capture") {
		t.Fatalf("review stdin was not loaded before reviewer start: %s err=%v", log, err)
	}
	if !strings.Contains(string(log), `OPENAI_API_KEY=model-canary`) || !strings.Contains(string(log), `GITHUB_TOKEN=github-canary`) || !strings.Contains(string(log), `GH_REPO=o/r`) || slices.ContainsFunc([]string{"ssh-canary", "cloud-canary", "proxy-canary", "app-canary", "/coordinator-home"}, func(secret string) bool { return strings.Contains(string(log), secret) }) {
		t.Fatalf("review boundary environment was not safely filtered: %s", log)
	}
	if !strings.Contains(string(log), `"reviewer","--custom"`) || strings.Contains(string(log), "--output-last-message") {
		t.Fatalf("custom reviewer command or result environment changed: %s", log)
	}
	var prompt string
	decoder := json.NewDecoder(bytes.NewReader(log))
	for {
		var request struct {
			Command boundaryCommand `json:"command"`
		}
		if decoder.Decode(&request) != nil {
			break
		}
		if slices.Contains(request.Command.Args, "load-buffer") {
			prompt = string(request.Command.Input)
		}
	}
	if !strings.Contains(prompt, reviewBase+"..HEAD") || strings.Contains(prompt, base+"..HEAD") {
		t.Fatalf("review prompt base is not current: %q", prompt)
	}
}

type artifactReviewBoundary struct {
	root          string
	terminal      string
	paneOutput    string
	captureCalls  int
	respawn       []string
	respawns      int
	failReads     int
	invalidReads  int
	resultContent string
}

type authenticationReviewBoundary struct {
	valid string
	saw   bool
}

func (b *authenticationReviewBoundary) call(_ context.Context, _ string, command agentruntime.Command) (agentruntime.Result, error) {
	if !slices.Contains(command.Args, "new-session") {
		return agentruntime.Result{}, nil
	}
	b.saw = true
	token := ""
	for _, entry := range command.Env {
		if name, value, ok := strings.Cut(entry, "="); ok && name == "GH_TOKEN" {
			token = value
		}
	}
	if token != "" && strings.Contains(strings.Join(command.Args, " "), token) {
		return agentruntime.Result{}, errors.New("review credential reached tmux argv")
	}
	if token == "" {
		return agentruntime.Result{}, errors.New("review GitHub CLI authentication is missing")
	}
	if token != b.valid {
		return agentruntime.Result{}, errors.New("review GitHub CLI authentication is invalid")
	}
	return agentruntime.Result{}, nil
}

func (b *artifactReviewBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation == "review-result" {
		if b.invalidReads > 0 {
			b.invalidReads--
			return agentruntime.Result{Output: "review result artifact is invalid", Code: reviewResultInvalidCode, Exited: true}, errors.New("review result artifact invalid")
		}
		if b.failReads > 0 {
			b.failReads--
			return agentruntime.Result{}, errors.New("transient review result read")
		}
		input, _ := io.ReadAll(command.Stdin)
		output, err := readReviewResult(input, b.root)
		return agentruntime.Result{Output: output}, err
	}
	if slices.Contains(command.Args, "display-message") {
		if b.paneOutput != "" {
			return agentruntime.Result{Output: b.paneOutput}, nil
		}
		return agentruntime.Result{Output: "1 0"}, nil
	}
	if slices.Contains(command.Args, "capture-pane") {
		b.captureCalls++
		return agentruntime.Result{Output: b.terminal}, nil
	}
	if slices.Contains(command.Args, "respawn-pane") {
		b.respawn = slices.Clone(command.Args)
		b.respawns++
		for _, path := range command.Args {
			if strings.HasPrefix(path, b.root+string(filepath.Separator)) && strings.Contains(filepath.Base(filepath.Dir(path)), ".result-") {
				if err := os.WriteFile(path, []byte(b.resultContent), 0o660); err != nil {
					return agentruntime.Result{}, err
				}
			}
		}
	}
	return agentruntime.Result{}, nil
}

func TestReviewAuthenticationCrossesIndependentReviewBoundary(t *testing.T) {
	source := t.TempDir()
	for _, args := range [][]string{{"init", "-q"}, {"config", "user.email", "test@example.invalid"}, {"config", "user.name", "test"}} {
		if out, err := exec.Command("git", append([]string{"-C", source}, args...)...).CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("content"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", source, "add", "file").CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	if out, err := exec.Command("git", "-C", source, "commit", "-q", "-m", "base").CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	base := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", source, "rev-parse", "HEAD"))))
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("reviewed change"), 0o600); err != nil {
		t.Fatal(err)
	}
	if out, err := exec.Command("git", "-C", source, "commit", "-q", "-am", "change").CombinedOutput(); err != nil {
		t.Fatal(string(out))
	}
	head := strings.TrimSpace(string(mustOutput(t, exec.Command("git", "-C", source, "rev-parse", "HEAD"))))
	for _, test := range []struct {
		name, token string
		ok          bool
	}{{"authenticated", "review-auth-canary", true}, {"missing", "", false}, {"invalid", "review-invalid-canary", false}} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			boundary := &authenticationReviewBoundary{valid: "review-auth-canary"}
			env := []string{"PATH=/bin"}
			if test.token != "" {
				env = append(env, "GH_TOKEN="+test.token)
			}
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 217, Number: 1, BaseSHA: base}
			issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base}
			snapshot, _ := reviewIdentity(attempt, root)
			_, pending, err := runIndependentReview(t.Context(), nil, attempt, boundary, env, []string{"reviewer"}, issue, agentruntime.Manifest{}, source, head, root)
			if test.ok {
				if err != nil || !pending || !boundary.saw {
					t.Fatal("authenticated review did not cross its launch boundary")
				}
			} else if err == nil || pending || !strings.Contains(err.Error(), "GitHub CLI authentication") || test.token != "" && strings.Contains(err.Error(), test.token) {
				t.Fatal("review authentication failure was unclear or exposed its credential")
			}
			t.Cleanup(func() {
				_ = filepath.WalkDir(snapshot, func(path string, entry os.DirEntry, err error) error {
					if err == nil && entry.IsDir() {
						_ = os.Chmod(path, 0o700)
					}
					return nil
				})
			})
		})
	}
}

func TestRunningReviewAcceptsBlankLivePaneStatus(t *testing.T) {
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	head, root := strings.Repeat("b", 40), t.TempDir()
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{ReviewState: "running", ReviewBase: attempt.BaseSHA, ReviewHead: head, ReviewSnapshot: snapshot, ReviewSession: session}
	boundary := &artifactReviewBoundary{paneOutput: "0 \n"}

	result, pending, err := runIndependentReview(t.Context(), nil, attempt, boundary, nil, []string{"reviewer"}, internalgithub.RecoveryIssueFact{}, manifest, "", head, root)
	if err != nil || !pending || result.Snapshot != snapshot || result.Session != session || boundary.respawns != 0 {
		t.Fatalf("result=%#v pending=%v respawns=%d err=%v", result, pending, boundary.respawns, err)
	}
}

func TestRunningReviewWithDifferentBaseIsNotReused(t *testing.T) {
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	head, root := strings.Repeat("b", 40), t.TempDir()
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{ReviewState: "running", ReviewBase: attempt.BaseSHA, ReviewHead: head, ReviewSnapshot: snapshot, ReviewSession: session}
	issue := internalgithub.RecoveryIssueFact{BaseSHA: strings.Repeat("c", 40)}
	if _, pending, err := runIndependentReview(t.Context(), nil, attempt, &artifactReviewBoundary{paneOutput: "0"}, nil, []string{"reviewer"}, issue, manifest, "", head, root); err == nil || pending {
		t.Fatalf("mismatched-base review was reused: pending=%v err=%v", pending, err)
	}
}

func TestIndependentReviewIgnoresEchoedAndDuplicatedTerminalResult(t *testing.T) {
	source, root := t.TempDir(), t.TempDir()
	runGit(t, source, "init")
	runGit(t, source, "config", "user.email", "test@example.invalid")
	runGit(t, source, "config", "user.name", "test")
	runGit(t, source, "commit", "--allow-empty", "-m", "base")
	base := runGit(t, source, "rev-parse", "HEAD")
	runGit(t, source, "commit", "--allow-empty", "-m", "head")
	head := runGit(t, source, "rev-parse", "HEAD")
	const clean = `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`
	boundary := &artifactReviewBoundary{
		root:          root,
		resultContent: `{`,
		terminal:      "prompt example: " + clean + "\n" + clean + "\n" + clean,
	}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: base}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Body: "review this"}
	defaultReviewer := config.Default(attempt.Repository).Commands.Reviewer
	started, pending, err := runIndependentReview(t.Context(), nil, attempt, boundary, nil, defaultReviewer, issue, agentruntime.Manifest{}, source, head, root)
	if err != nil || !pending {
		t.Fatalf("start pending=%v err=%v", pending, err)
	}
	if slices.Contains(boundary.respawn, "review") || slices.Contains(boundary.respawn, "--output-last-message") || !slices.Contains(boundary.respawn, reviewResultPath(started.Snapshot, head)) || !slices.Contains(boundary.respawn, "--dangerously-bypass-approvals-and-sandbox") || !slices.Contains(boundary.respawn, "-") {
		t.Fatalf("default reviewer did not receive result artifact: %q", boundary.respawn)
	}
	manifest := agentruntime.Manifest{ReviewState: "running", ReviewBase: attempt.BaseSHA, ReviewHead: head, ReviewSnapshot: started.Snapshot, ReviewSession: started.Session}
	if _, _, err := runIndependentReview(t.Context(), nil, attempt, boundary, nil, defaultReviewer, issue, manifest, source, head, root); err == nil {
		t.Fatal("malformed artifact was accepted")
	}
	resultPath := reviewResultPath(started.Snapshot, head)
	if _, err := os.Stat(resultPath); err != nil {
		t.Fatalf("artifact was not preserved for retry: %v", err)
	}
	if err := os.WriteFile(resultPath, []byte(clean), 0o660); err != nil {
		t.Fatal(err)
	}
	result, pending, err := runIndependentReview(t.Context(), nil, attempt, boundary, nil, defaultReviewer, issue, manifest, source, head, root)
	if err != nil || pending || result.Status != "clean" {
		t.Fatalf("result=%#v pending=%v err=%v", result, pending, err)
	}
	if boundary.captureCalls != 0 {
		t.Fatalf("tmux transcript was captured %d times", boundary.captureCalls)
	}
}

func TestReviewResultReadFailureRetriesWithoutRespawningReviewer(t *testing.T) {
	source, root := t.TempDir(), t.TempDir()
	runGit(t, source, "init")
	runGit(t, source, "config", "user.email", "test@example.invalid")
	runGit(t, source, "config", "user.name", "test")
	runGit(t, source, "commit", "--allow-empty", "-m", "base")
	base := runGit(t, source, "rev-parse", "HEAD")
	runGit(t, source, "commit", "--allow-empty", "-m", "head")
	head := runGit(t, source, "rev-parse", "HEAD")
	boundary := &artifactReviewBoundary{root: root, failReads: 1, resultContent: `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: base}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number}
	command := config.Default(attempt.Repository).Commands.Reviewer
	started, pending, err := runIndependentReview(t.Context(), nil, attempt, boundary, nil, command, issue, agentruntime.Manifest{}, source, head, root)
	if err != nil || !pending {
		t.Fatalf("start pending=%v err=%v", pending, err)
	}
	defer func() {
		_ = cleanupReviewResources(t.Context(), boundary, nil, attempt, head, started.Snapshot, started.Session, root)
	}()
	manifest := agentruntime.Manifest{ReviewState: "running", ReviewBase: attempt.BaseSHA, ReviewHead: head, ReviewSnapshot: started.Snapshot, ReviewSession: started.Session}
	if _, pending, err = runIndependentReview(t.Context(), nil, attempt, boundary, nil, command, issue, manifest, source, head, root); err != nil || !pending {
		t.Fatalf("transient read pending=%v err=%v", pending, err)
	}
	result, pending, err := runIndependentReview(t.Context(), nil, attempt, boundary, nil, command, issue, manifest, source, head, root)
	if err != nil || pending || result.Status != "clean" || boundary.respawns != 1 {
		t.Fatalf("result=%#v pending=%v respawns=%d err=%v", result, pending, boundary.respawns, err)
	}
}

func TestInvalidReviewArtifactFailsWithoutRespawningReviewer(t *testing.T) {
	source, root := t.TempDir(), t.TempDir()
	runGit(t, source, "init")
	runGit(t, source, "config", "user.email", "test@example.invalid")
	runGit(t, source, "config", "user.name", "test")
	runGit(t, source, "commit", "--allow-empty", "-m", "base")
	base := runGit(t, source, "rev-parse", "HEAD")
	runGit(t, source, "commit", "--allow-empty", "-m", "head")
	head := runGit(t, source, "rev-parse", "HEAD")
	boundary := &artifactReviewBoundary{root: root, invalidReads: 1, resultContent: `{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`}
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 24, Number: 1, BaseSHA: base}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number}
	command := config.Default(attempt.Repository).Commands.Reviewer
	started, pending, err := runIndependentReview(t.Context(), nil, attempt, boundary, nil, command, issue, agentruntime.Manifest{}, source, head, root)
	if err != nil || !pending {
		t.Fatalf("start pending=%v err=%v", pending, err)
	}
	defer func() {
		_ = cleanupReviewResources(t.Context(), boundary, nil, attempt, head, started.Snapshot, started.Session, root)
	}()
	manifest := agentruntime.Manifest{ReviewState: "running", ReviewBase: attempt.BaseSHA, ReviewHead: head, ReviewSnapshot: started.Snapshot, ReviewSession: started.Session}
	if _, pending, err = runIndependentReview(t.Context(), nil, attempt, boundary, nil, command, issue, manifest, source, head, root); err == nil || pending || boundary.respawns != 1 {
		t.Fatalf("invalid artifact pending=%v respawns=%d err=%v", pending, boundary.respawns, err)
	}
}

func TestReviewCaptureRedactsResultBeforeFindingsHandoff(t *testing.T) {
	const credential = "review-result-auth-canary"
	dir := t.TempDir()
	tmux := filepath.Join(dir, "tmux")
	if err := os.WriteFile(tmux, []byte("#!/bin/sh\ncase \"$1\" in\nsave-buffer) printf 'review prompt';;\ndelete-buffer) :;;\n*) exit 1;;\nesac\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	resultRoot := filepath.Join(dir, "result")
	if err := os.Mkdir(resultRoot, 0o770); err != nil {
		t.Fatal(err)
	}
	resultPath := filepath.Join(resultRoot, "result.json")
	var stdout, stderr bytes.Buffer
	t.Setenv("GH_TOKEN", credential)
	t.Setenv("EXPECTED_TOKEN", credential)
	code, err := agentruntime.CaptureWorker(t.Context(), tmux, "buffer", resultPath, []string{"sh", "-c", `test "$(cat)" = "review prompt" && test "$GH_TOKEN" = "$EXPECTED_TOKEN" || exit 9; printf '{"type":"agent-symphony-review-v1","status":"findings","findings":["%s"]}' "$GH_TOKEN"; printf 'diagnostic' >&2`}, &stdout, &stderr)
	if err != nil || code != 0 {
		t.Fatalf("code=%d err=%v", code, err)
	}
	body, err := os.ReadFile(resultPath)
	t.Setenv("GH_TOKEN", "")
	result, parseErr := parseIndependentReview(string(body))
	if err != nil || bytes.Contains(body, []byte(credential)) || result.Status != "findings" || !slices.Equal(result.Findings, []string{"[REDACTED]"}) || parseErr != nil || stdout.Len() != 0 || stderr.String() != "diagnostic" {
		t.Fatalf("artifact=%q stdout=%q stderr=%q err=%v", body, stdout.String(), stderr.String(), err)
	}

	state, worktree := t.TempDir(), t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
	head, key := "abcdef1", "independent-review-abcdef1"
	manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, State: "completed", Worktree: worktree, Session: "as-23-1", LogPath: filepath.Join(worktree, "attempt.log"), ReviewState: "findings-queued", ReviewHead: head, ReviewFindings: result.Findings}
	sum := sha256.Sum256([]byte(attempt.Repository))
	manifestPath := filepath.Join(state, "attempts", fmt.Sprintf("o-r-%x", sum[:6]), "23-1", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	manifestBody, _ := json.Marshal(manifest)
	if err := os.WriteFile(manifestPath, manifestBody, 0o600); err != nil {
		t.Fatal(err)
	}
	ack, _ := json.Marshal(handoffReceipt{"agent-symphony-handoff-executed-v1", key, handoffReceiptPath(worktree, key), head})
	boundaryResult, _ := json.Marshal(agentruntime.Result{Output: string(ack)})
	boundaryLog, boundary := filepath.Join(worktree, "boundary.log"), filepath.Join(t.TempDir(), "boundary")
	script := fmt.Sprintf("#!/bin/sh\npayload=$(cat)\nprintf '%%s' \"$payload\" > %q\nprintf '%%s' '%s'\n", boundaryLog, boundaryResult)
	if err := os.WriteFile(boundary, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	pending, err := returnReviewFindings(t.Context(), &agentruntime.Runtime{StateRoot: state}, workerBoundaryRunner{Command: boundary}, attempt, manifest, head, result.Findings, nil, []string{"implementation"})
	stored, storedErr := os.ReadFile(manifestPath)
	handoff, handoffErr := os.ReadFile(boundaryLog)
	var request struct {
		Command boundaryCommand `json:"command"`
	}
	decodeErr := json.Unmarshal(handoff, &request)
	if err != nil || pending || storedErr != nil || handoffErr != nil || decodeErr != nil || bytes.Contains(stored, []byte(credential)) || bytes.Contains(request.Command.Input, []byte(credential)) || !bytes.Contains(stored, []byte("[REDACTED]")) || !bytes.Contains(request.Command.Input, []byte("[REDACTED]")) {
		t.Fatalf("unsafe review propagation: pending=%v err=%v stored=%q storedErr=%v handoff=%q handoffErr=%v decodeErr=%v", pending, err, stored, storedErr, request.Command.Input, handoffErr, decodeErr)
	}
}

func TestDefaultReviewerProductionShapeUsesExactDiffAndRejectsProse(t *testing.T) {
	if _, err := exec.LookPath("tmux"); err != nil {
		t.Skip("tmux is unavailable")
	}
	dir := t.TempDir()
	codex := filepath.Join(dir, "codex")
	const script = `#!/bin/sh
test "$#" -eq 3 && test "$1" = exec && test "$2" = --dangerously-bypass-approvals-and-sandbox && test "$3" = - || exit 20
prompt=$(cat) || exit 21
printf '%s' "$prompt" | grep -F "$FAKE_REVIEW_BASE..HEAD" >/dev/null || exit 22
diff=$(git -C "$FAKE_REVIEW_REPO" diff --no-ext-diff "$FAKE_REVIEW_BASE" HEAD) || exit 23
printf '%s' "$diff" | grep -F '+first implementation commit' >/dev/null || exit 24
printf '%s' "$diff" | grep -F '+second implementation commit' >/dev/null || exit 25
printf x >>"$FAKE_REVIEW_COUNT"
printf '%s' "$FAKE_REVIEW_OUTPUT"
printf 'review diagnostic\n' >&2`
	if err := os.WriteFile(codex, []byte(script), 0o700); err != nil {
		t.Fatal(err)
	}
	tmuxRoot, err := os.MkdirTemp("/tmp", "agent-symphony-review-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmuxRoot) })
	t.Setenv("PATH", dir+":"+os.Getenv("PATH"))
	t.Setenv("TMUX", "")
	t.Setenv("TMUX_PANE", "")
	t.Setenv("TMUX_TMPDIR", tmuxRoot)
	bootstrap := exec.Command("tmux", "-f", "/dev/null", "new-session", "-d", "-s", "agent-symphony-test-bootstrap")
	if output, err := bootstrap.CombinedOutput(); err != nil {
		t.Fatalf("start isolated tmux server: %v: %s", err, output)
	}
	t.Cleanup(func() {
		command := exec.Command("tmux", "kill-session", "-t", "=agent-symphony-test-bootstrap")
		_ = command.Run()
	})
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
	t.Setenv("FAKE_REVIEW_REPO", source)
	t.Setenv("FAKE_REVIEW_BASE", base)
	defaultReviewer := config.Default("o/r").Commands.Reviewer

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
			buffer := fmt.Sprintf("review-%d", i)
			load := exec.Command("tmux", "load-buffer", "-b", buffer, "-")
			load.Stdin = strings.NewReader(reviewPrompt(internalgithub.RecoveryIssueFact{Issue: 23 + i, Attempt: 1, Body: "Review the change."}, base))
			if output, err := load.CombinedOutput(); err != nil {
				t.Fatalf("load real tmux prompt: %v: %s", err, output)
			}
			resultRoot := filepath.Join(dir, fmt.Sprintf("result-%d", i))
			if err := os.Mkdir(resultRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			resultPath := filepath.Join(resultRoot, "result.json")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			code, err := agentruntime.CaptureWorker(ctx, "tmux", buffer, resultPath, defaultReviewer, io.Discard, io.Discard)
			if err != nil || code != 0 {
				t.Fatalf("default reviewer capture code=%d err=%v", code, err)
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

func TestIndependentReviewRequiresAvailableApprovedBase(t *testing.T) {
	source := t.TempDir()
	runGit(t, source, "init")
	runGit(t, source, "config", "user.email", "test@example.invalid")
	runGit(t, source, "config", "user.name", "test")
	runGit(t, source, "commit", "--allow-empty", "-m", "head")
	head := runGit(t, source, "rev-parse", "HEAD")
	for i, test := range []struct {
		base          string
		boundaryCalls countingReviewBoundary
	}{{"", 0}, {strings.Repeat("b", 40), 1}} {
		var boundary countingReviewBoundary
		_, pending, err := runIndependentReview(t.Context(), nil, agentruntime.Attempt{Repository: "o/r", Issue: 30 + i, Number: 1, BaseSHA: test.base}, &boundary, nil, []string{"reviewer"}, internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 30 + i, Attempt: 1}, agentruntime.Manifest{}, source, head, t.TempDir())
		if err == nil || pending || boundary != test.boundaryCalls {
			t.Fatalf("base=%q pending=%v boundary=%d err=%v", test.base, pending, boundary, err)
		}
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
	if slices.Contains(command.Args, "new-session") {
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
			runGit(t, source, "commit", "--allow-empty", "-m", "base")
			baseSHA := runGit(t, source, "rev-parse", "HEAD")
			runGit(t, source, "commit", "--allow-empty", "-m", "head")
			head := runGit(t, source, "rev-parse", "HEAD")
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: baseSHA}
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
		{"foreign session", "", "as-r-foreign-99-1"},
		{"outside root", outside, session},
		{"symlink", link, session},
	} {
		t.Run(test.name, func(t *testing.T) {
			var boundary countingReviewBoundary
			if err := cleanupReviewResources(t.Context(), &boundary, nil, attempt, strings.Repeat("a", 40), test.snapshot, test.session, reviewSnapshotRoot); err == nil {
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
	if err := cleanupReviewResources(t.Context(), &boundary, nil, first, strings.Repeat("a", 40), secondSnapshot, secondSession, root); err == nil {
		t.Fatal("cleanup accepted another repository identity")
	}
	if boundary != 0 {
		t.Fatal("cleanup boundary reached for another repository")
	}
}

func (b *blockingReviewBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if slices.Contains(command.Args, "display-message") {
		return agentruntime.Result{Output: "1 0"}, nil
	}
	if operation == "review-result" {
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
			runGit(t, source, "commit", "--allow-empty", "-m", "base")
			baseSHA := runGit(t, source, "rev-parse", "HEAD")
			if err := os.WriteFile(filepath.Join(source, "file"), []byte("content"), 0o600); err != nil {
				t.Fatal(err)
			}
			runGit(t, source, "add", "file")
			runGit(t, source, "commit", "-m", "head")
			head := runGit(t, source, "rev-parse", "HEAD")
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: baseSHA}
			snapshot, session := reviewIdentity(attempt, base)
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
			if json.Unmarshal(storedBody, &stored) != nil || stored.ReviewState != "preparing" || stored.ReviewBase != baseSHA || stored.ReviewSnapshot != snapshot || stored.ReviewSession != session {
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
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	snapshot, session := reviewIdentity(attempt, base)
	if err := os.Mkdir(snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	resultPath := reviewResultPath(snapshot, "head")
	if err := os.MkdirAll(filepath.Dir(resultPath), 0o770); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(resultPath, []byte(`{"type":"agent-symphony-review-v1","status":"clean","findings":[]}`), 0o660); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, State: "completed", ReviewState: "running", ReviewBase: attempt.BaseSHA, ReviewHead: "head", ReviewSnapshot: snapshot, ReviewSession: session}
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
	if stored.ReviewState != "clean" || stored.ReviewBase != attempt.BaseSHA || stored.ReviewSession == "" || stored.ReviewSnapshot == "" {
		t.Fatalf("cleanup discarded retry state: %#v", stored)
	}
	if _, err := os.Stat(snapshot); err != nil {
		t.Fatalf("cleanup removed retained snapshot: %v", err)
	}
	if _, err := os.Stat(resultPath); err != nil {
		t.Fatalf("cleanup removed result before durable acknowledgment: %v", err)
	}

	boundary.block = false
	stored, err = cleanupReviewOutcome(t.Context(), &agentruntime.Runtime{StateRoot: state}, attempt, boundary, nil, stored, reviewSnapshotRoot)
	if err != nil || stored.ReviewSession != "" || stored.ReviewSnapshot != "" {
		t.Fatalf("retry stored=%#v err=%v", stored, err)
	}
	if _, err := os.Stat(snapshot); !os.IsNotExist(err) {
		t.Fatalf("retry retained snapshot: %v", err)
	}
	if _, err := os.Stat(resultPath); !os.IsNotExist(err) {
		t.Fatalf("retry retained result artifact: %v", err)
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
	recoveryPath := filepath.Join(t.TempDir(), "pr.json")
	recoveryState := []internalgithub.PRState{{Repository: "o/r", Number: 7, Issue: 23, Attempt: 1, HeadSHA: base, Facts: internalgithub.PRFacts{Feedback: []internalgithub.Feedback{{ID: 55, Source: "conversation", Execution: internalgithub.FeedbackClaimed}}}}}
	recoveryBody, _ := json.Marshal(recoveryState)
	if err := os.WriteFile(recoveryPath, recoveryBody, 0o600); err != nil {
		t.Fatal(err)
	}
	recovery := &internalgithub.FileRecovery{Path: recoveryPath}
	handoffs, err := recovery.ClaimHandoffsFor(t.Context(), map[string]bool{"o/r#23/1": true})
	if err != nil || len(handoffs) != 1 {
		t.Fatalf("handoffs=%#v err=%v", handoffs, err)
	}
	if err := recovery.ReceiptHandoff(t.Context(), handoffs[0]); err != nil {
		t.Fatal(err)
	}
	oldReviewRoot := reviewSnapshotRoot
	reviewSnapshotRoot = snapshotBase
	t.Cleanup(func() { reviewSnapshotRoot = oldReviewRoot })
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 23, Number: 1}
	snapshot, session := reviewIdentity(attempt, reviewSnapshotRoot)
	if err := os.Mkdir(snapshot, 0o700); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Version: 1, Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, Branch: "issue-23", BaseSHA: base, State: "completed", ReviewState: "clean", ReviewBase: base, ReviewHead: head, ReviewSnapshot: snapshot, ReviewSession: session, UpdatedAt: time.Now().UTC()}
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
	issue := internalgithub.RecoveryIssueFact{Repository: "o/r", Issue: 23, Attempt: 1, Eligible: true, DispatchAuthorized: true, BaseBranch: "main"}
	bound := []internalgithub.RecoveryAttemptFact{{Repository: "o/r", Issue: 23, Attempt: 1, PR: 7, BaseSHA: base, HeadSHA: base, State: "active"}}
	if err := monitorQueuedAttempts(t.Context(), internalgithub.API{}, runtimeState, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{manifest}, bound, nil, recoveryPath, state, nil); err != nil {
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
	_, facts := recoveryAttemptFacts(bound, []internalgithub.RecoveryIssueFact{issue})
	statuses, _ := projectRecoveryStatuses(t.Context(), facts, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{stored}, 1, nil)
	if len(statuses) != 1 || !slices.ContainsFunc(statuses[0].Sessions, func(session orchestrator.AttemptSession) bool {
		return session.Role == agentruntime.SessionRoleReviewer
	}) {
		t.Fatalf("fixture did not project the retained reviewer: %#v", statuses)
	}
	if err := writeStatusSnapshot(state, statuses); err != nil {
		t.Fatal(err)
	}

	if err := os.WriteFile(review, []byte("#!/bin/sh\nprintf '{\"Code\":0}'\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	stopAfterProjection := errors.New("stop after projection")
	err = monitorQueuedAttempts(t.Context(), internalgithub.API{}, runtimeState, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{stored}, bound, nil, recoveryPath, state, func() error {
		body, err := os.ReadFile(manifestPath)
		if err != nil {
			return err
		}
		var current agentruntime.Manifest
		if err := json.Unmarshal(body, &current); err != nil {
			return err
		}
		statuses, _ := projectRecoveryStatuses(t.Context(), facts, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{current}, 1, nil)
		if err := writeStatusSnapshot(state, statuses); err != nil {
			return err
		}
		body, err = readDashboardStatus(filepath.Join(state, "status.json"))
		if err != nil {
			return err
		}
		var snapshot struct {
			Statuses []orchestrator.RecoveryStatus `json:"statuses"`
		}
		if json.Unmarshal(body, &snapshot) != nil || len(snapshot.Statuses) != 1 || slices.ContainsFunc(snapshot.Statuses[0].Sessions, func(session orchestrator.AttemptSession) bool {
			return session.Role == agentruntime.SessionRoleReviewer
		}) {
			return errors.New("status projection retained the cleaned reviewer")
		}
		return stopAfterProjection
	})
	if !errors.Is(err, stopAfterProjection) {
		t.Fatalf("first post-cleanup projection err=%v", err)
	}
	if refs, _ := exec.Command("git", "-C", remote, "show-ref").Output(); len(bytes.TrimSpace(refs)) != 0 {
		t.Fatalf("publication ran before the post-cleanup projection: %s", refs)
	}
	storedBody, _ = os.ReadFile(manifestPath)
	stored = agentruntime.Manifest{}
	_ = json.Unmarshal(storedBody, &stored)
	err = monitorQueuedAttempts(t.Context(), internalgithub.API{}, runtimeState, config.Config{}, []internalgithub.RecoveryIssueFact{issue}, []agentruntime.Manifest{stored}, bound, nil, recoveryPath, state, nil)
	if err == nil || !strings.Contains(err.Error(), "authenticate GitHub CLI") {
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
	prepared, ok, err := recovery.PreparedHandoffPublication(t.Context(), "o/r", 23, 1)
	if err != nil || !ok || prepared.HeadSHA != head {
		t.Fatalf("publication was not durably prepared: prepared=%#v ok=%v err=%v", prepared, ok, err)
	}
	if err := recovery.CompleteHandoffPublication(t.Context(), prepared); err != nil {
		t.Fatal(err)
	}
	completed, err := recovery.PullRequestState(t.Context(), "o/r", 7, 23, 1, head)
	if err != nil || completed.HeadSHA != head || completed.Facts.Feedback[0].Execution != internalgithub.FeedbackCompleted || completed.Facts.Feedback[0].State != internalgithub.FeedbackAddressed {
		t.Fatalf("completed publication=%#v err=%v", completed, err)
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

func TestResumeHandoffsDeliversConfiguredImplementationCommand(t *testing.T) {
	root, stateRoot := t.TempDir(), t.TempDir()
	worktree := filepath.Join(root, "attempt")
	if err := os.MkdirAll(worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(stateRoot, "state.json")
	state := []internalgithub.PRState{{Repository: "o/r", Number: 3, Issue: 4, Attempt: 2, HeadSHA: "abcdef0", Facts: internalgithub.PRFacts{Feedback: []internalgithub.Feedback{{ID: 55, Source: "conversation", Execution: internalgithub.FeedbackClaimed}}}}}
	body, _ := json.Marshal(state)
	if err := os.WriteFile(statePath, body, 0o600); err != nil {
		t.Fatal(err)
	}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, State: "completed", Worktree: worktree, Session: "as-4-2", LogPath: filepath.Join(worktree, "attempt.log")}
	statuses := []orchestrator.RecoveryStatus{{Repository: "o/r", Issue: 4, Attempt: 2, State: "review-ready", DispatchAuthorized: true, Action: "publish the independently reviewed implementation"}}

	oldExec := hostExecRunner
	var respawn []string
	var prompt string
	hostExecRunner = func(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
		if slices.Contains(command.Args, "show-options") {
			return agentruntime.Result{}, errors.New("not delivered")
		}
		if slices.Contains(command.Args, "respawn-pane") {
			respawn = slices.Clone(command.Args)
			if _, err := acknowledgeHandoffLaunch(command); err != nil {
				return agentruntime.Result{}, err
			}
		}
		if slices.Contains(command.Args, "load-buffer") {
			body, _ := io.ReadAll(command.Stdin)
			prompt = string(body)
		}
		return agentruntime.Result{}, nil
	}
	t.Cleanup(func() { hostExecRunner = oldExec })

	command := []string{"implementation", "--flag"}
	boundary := &directHandoffBoundary{root: root}
	if err := resumeHandoffs(t.Context(), nil, boundary, statePath, stateRoot, statuses, []agentruntime.Manifest{manifest}, command); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(respawn, command[0]) || !slices.Contains(respawn, command[1]) {
		t.Fatalf("respawn omitted implementation command: %q", respawn)
	}
	if !slices.Contains(respawn, agentruntime.ResultPath(worktree)) || !strings.Contains(prompt, "agent-symphony-result-v1") || !strings.Contains(prompt, "refs/remotes/agent-symphony/") {
		t.Fatalf("handoff omitted capture contract: respawn=%q prompt=%q", respawn, prompt)
	}
	entries, err := os.ReadDir(filepath.Join(worktree, ".agent-symphony", "handoffs"))
	if err != nil || !slices.ContainsFunc(entries, func(entry os.DirEntry) bool { return strings.HasSuffix(entry.Name(), ".receipt") }) {
		t.Fatalf("handoff receipt missing: entries=%v err=%v", entries, err)
	}
	if again, err := (&internalgithub.FileRecovery{Path: statePath}).ClaimHandoffsFor(t.Context(), map[string]bool{"o/r#4/2": true}); err != nil || len(again) != 0 {
		t.Fatalf("handoff redelivered: handoffs=%#v err=%v", again, err)
	}
}

func TestResumeHandoffsLeavesUnauthorizedWorkQueued(t *testing.T) {
	for _, status := range []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 4, Attempt: 2, State: "review-ready", Action: "monitor the matching published pull request"},
		{Repository: "o/r", Issue: 4, Attempt: 2, State: "review-ready", DispatchAuthorized: true, Blockers: []string{"review required"}, Action: "monitor the matching published pull request"},
	} {
		stateRoot := t.TempDir()
		statePath := filepath.Join(stateRoot, "state.json")
		state := []internalgithub.PRState{{Repository: "o/r", Number: 3, Issue: 4, Attempt: 2, HeadSHA: "abcdef0", Facts: internalgithub.PRFacts{Feedback: []internalgithub.Feedback{{ID: 55, Source: "conversation", Execution: internalgithub.FeedbackClaimed}}}}}
		body, _ := json.Marshal(state)
		if err := os.WriteFile(statePath, body, 0o600); err != nil {
			t.Fatal(err)
		}
		manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, State: "completed"}
		boundary := &refusingHandoffBoundary{}
		if err := resumeHandoffs(t.Context(), nil, boundary, statePath, stateRoot, []orchestrator.RecoveryStatus{status}, []agentruntime.Manifest{manifest}, []string{"implementation"}); err != nil {
			t.Fatal(err)
		}
		handoffs, err := (&internalgithub.FileRecovery{Path: statePath}).ClaimHandoffsFor(t.Context(), map[string]bool{"o/r#4/2": true})
		if err != nil || len(handoffs) != 1 || boundary.calls != 0 {
			t.Fatalf("unauthorized handoff was consumed: handoffs=%#v calls=%d err=%v", handoffs, boundary.calls, err)
		}
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
	if !slices.Equal(initialized.Commands.Implementation, []string{"codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"}) {
		t.Fatalf("unexpected initialized implementation command: %q", initialized.Commands.Implementation)
	}
	if !slices.Equal(initialized.Commands.Reviewer, []string{"codex", "exec", "--dangerously-bypass-approvals-and-sandbox", "-"}) {
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

func TestPRGovernanceCommandWiresFakeGitHubAndRecoveryState(t *testing.T) {
	root := gitRepository(t)
	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, config.Default("owner/repo")); err != nil {
		t.Fatal(err)
	}
	statePath := filepath.Join(root, "pr-state.json")
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		switch r.URL.Path {
		case "/user":
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"id":42,"login":"coordinator"}`)), Header: make(http.Header)}, nil
		case "/repos/owner/repo":
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`{"full_name":"owner/repo","permissions":{"pull":true}}`)), Header: make(http.Header)}, nil
		case "/repos/owner/repo/pulls":
			return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Body: io.NopCloser(strings.NewReader(`[]`)), Header: make(http.Header)}, nil
		default:
			t.Fatalf("unexpected request %s", r.URL.String())
			return nil, nil
		}
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
	session, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "owner/repo", 4, 1)
	runtimeState := t.TempDir()
	status := orchestrator.RecoveryStatus{Repository: "owner/repo", Issue: 4, Attempt: 1, State: "completed", CurrentPhase: "completed", PR: 10, Session: session, Sessions: []orchestrator.AttemptSession{{Role: agentruntime.SessionRoleImplementation, Name: session, State: "completed"}}}
	previous := reconcileGitHubRun
	reconcileGitHubRun = func(context.Context, string, string, string, bool) ([]orchestrator.RecoveryStatus, error) {
		return []orchestrator.RecoveryStatus{status}, nil
	}
	t.Cleanup(func() { reconcileGitHubRun = previous })
	t.Setenv("NO_COLOR", "1")
	var stdout, stderr bytes.Buffer
	if code := run([]string{"status", "--state", "state", "--runtime-state", runtimeState}, &stdout, &stderr); code != 0 || !strings.Contains(stdout.String(), "COMPLETED") || !strings.Contains(stdout.String(), "phase: completed") || !strings.Contains(stdout.String(), "session: implementation completed "+session) || strings.Contains(stdout.String(), "\x1b[") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	stdout.Reset()
	stderr.Reset()
	if code := run([]string{"inspect", "--issue", "4", "--state", "state", "--runtime-state", runtimeState, "--json"}, &stdout, &stderr); code != 0 {
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
	original := reconcileGitHubRun
	t.Cleanup(func() { reconcileGitHubRun = original })
	started, release := make(chan struct{}, 2), make(chan struct{})
	var snapshots, dispatches atomic.Int32
	reconcileGitHubRun = func(context.Context, string, string, string, bool) ([]orchestrator.RecoveryStatus, error) {
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

func TestRecoverProposalAndDashboardShareOneIdempotentGuardedPath(t *testing.T) {
	root := gitRepository(t)
	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, config.Default("o/r")); err != nil {
		t.Fatal(err)
	}
	stateRoot := filepath.Join(root, "state")
	if err := os.MkdirAll(stateRoot, 0o700); err != nil {
		t.Fatal(err)
	}
	oldReviewRoot := reviewSnapshotRoot
	reviewSnapshotRoot = filepath.Join(stateRoot, "snapshots")
	t.Cleanup(func() { reviewSnapshotRoot = oldReviewRoot })
	manifest := writeDashboardManifest(t, stateRoot, 24, 2, "failed")
	want, err := agentruntime.AttemptIdentity(productionAttemptRoot(stateRoot), agentruntime.Attempt{Repository: manifest.Repository, Issue: manifest.Issue, Number: manifest.Attempt, BaseSHA: manifest.BaseSHA})
	if err != nil {
		t.Fatal(err)
	}
	resolvedStateRoot, err := filepath.EvalSymlinks(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	want.LogPath = filepath.Join(resolvedStateRoot, "attempts", internalgithub.RepositoryIdentifier(manifest.Repository), fmt.Sprintf("%d-%d", manifest.Issue, manifest.Attempt), "agent.log")
	want.State, want.Diagnostic = manifest.State, manifest.Diagnostic
	want.CreatedAt, want.UpdatedAt = manifest.CreatedAt, manifest.UpdatedAt
	manifest = want
	body, _ := json.Marshal(manifest)
	if err := os.WriteFile(filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json"), body, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(manifest.Worktree, 0o700); err != nil {
		t.Fatal(err)
	}
	status := orchestrator.RecoveryStatus{Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, State: "failed", Branch: manifest.Branch, Worktree: manifest.Worktree, Session: manifest.Session, Retryable: true}
	oldReconcile := reconcileGitHubRun
	reconcileGitHubRun = func(context.Context, string, string, string, bool) ([]orchestrator.RecoveryStatus, error) {
		return []orchestrator.RecoveryStatus{status}, nil
	}
	t.Cleanup(func() { reconcileGitHubRun = oldReconcile })

	failedAt := time.Unix(10, 0).UTC()
	active, _ := internalgithub.ActiveAttemptMarker(manifest.Repository, manifest.Issue, manifest.Attempt, manifest.BaseSHA)
	terminal, _ := internalgithub.TerminalFailureMarker(manifest.Issue, manifest.Attempt, failedAt)
	comments := []map[string]any{
		{"id": 1, "body": active, "created_at": failedAt.Add(-time.Minute), "updated_at": failedAt.Add(-time.Minute), "user": map[string]any{"id": 42}},
		{"id": 2, "body": terminal, "created_at": failedAt, "updated_at": failedAt, "user": map[string]any{"id": 42}},
	}
	posts := 0
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var response any
		switch r.Method + " " + r.URL.RequestURI() {
		case "GET /user":
			response = map[string]any{"id": 42, "login": "coordinator"}
		case "GET /repos/o/r/issues/24/comments?per_page=100&page=1":
			response = comments
		case "GET /user/42":
			response = map[string]any{"login": "coordinator"}
		case "GET /repos/o/r/collaborators/coordinator/permission":
			response = map[string]any{"permission": "admin"}
		case "POST /repos/o/r/issues/24/comments":
			var payload struct{ Body string }
			if json.NewDecoder(r.Body).Decode(&payload) != nil || payload.Body != "/agent-symphony retry" {
				t.Fatalf("retry body=%q", payload.Body)
			}
			posts++
			createdAt := failedAt.Add(time.Minute)
			comments = append(comments, map[string]any{"id": 3, "body": payload.Body, "created_at": createdAt, "updated_at": createdAt, "user": map[string]any{"id": 42}})
			response = map[string]any{}
		default:
			t.Fatalf("unexpected request %s", r.URL.String())
		}
		body, _ := json.Marshal(response)
		return &http.Response{StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, nil
	})}
	oldAPI, oldClient := githubAPI, githubClient
	githubAPI, githubClient = "https://example.invalid", client
	t.Cleanup(func() { githubAPI, githubClient = oldAPI, oldClient })
	agentRoot := filepath.Join(root, "orchestrator-state")
	agent := &orchestratoragent.Supervisor{Root: agentRoot, Workspace: filepath.Join(agentRoot, "orchestrator-o-r"), Repository: "o/r", Command: []string{"agent"}, ProposalCommand: []string{"proposal"}, ProposalStatusCommand: []string{"proposal-status"}, Runner: &orchestratorTestRunner{}}
	if _, err := agent.Observe(t.Context(), []orchestrator.RecoveryStatus{status}); err != nil {
		t.Fatal(err)
	}
	var handoff struct {
		ID string `json:"id"`
	}
	handoffBody, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.AttentionHandoffFile))
	if err != nil || json.Unmarshal(handoffBody, &handoff) != nil || handoff.ID == "" {
		t.Fatalf("handoff=%s err=%v", handoffBody, err)
	}
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 24, Attempt: 2, Action: orchestratoragent.ProposalActionRecover, RequestID: "recover-24-2", HandoffID: handoff.ID}
	proposalBody, _ := json.Marshal(proposal)
	if err := os.WriteFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalFile), proposalBody, 0o620); err != nil {
		t.Fatal(err)
	}
	processOrchestratorProposal(t.Context(), agent, &sync.Mutex{}, &operationCancellation{}, configPath, filepath.Join(stateRoot, "pr.json"), stateRoot, io.Discard)
	var proposalStatus orchestratoragent.MessageProposalStatus
	proposalStatusBody, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalStatusFile))
	if err != nil || json.Unmarshal(proposalStatusBody, &proposalStatus) != nil || proposalStatus.Resolution != "succeeded" {
		t.Fatalf("proposal status=%s err=%v", proposalStatusBody, err)
	}
	if err := recoverDashboardAttempt(t.Context(), configPath, filepath.Join(stateRoot, "pr.json"), stateRoot, 24, 2); err != nil {
		t.Fatal(err)
	}
	if posts != 1 {
		t.Fatalf("retry posts=%d", posts)
	}
	for _, path := range []string{manifest.Worktree, manifest.LogPath, filepath.Join(filepath.Dir(manifest.LogPath), "manifest.json")} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("recovery removed %s: %v", path, err)
		}
	}
}

func TestRecoverDashboardRejectsUnsafeOrStaleStatusBeforeMutation(t *testing.T) {
	root := gitRepository(t)
	configPath := filepath.Join(root, config.DefaultPath)
	if err := config.Write(configPath, config.Default("o/r")); err != nil {
		t.Fatal(err)
	}
	oldReconcile, oldAPI, oldClient := reconcileGitHubRun, githubAPI, githubClient
	t.Cleanup(func() { reconcileGitHubRun, githubAPI, githubClient = oldReconcile, oldAPI, oldClient })
	requests := 0
	githubAPI = "https://example.invalid"
	githubClient = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		requests++
		return nil, errors.New("unexpected mutation path")
	})}
	for _, status := range []orchestrator.RecoveryStatus{
		{Repository: "o/r", Issue: 24, Attempt: 2, State: "blocked", Blockers: []string{"runtime worktree unsafe"}},
		{Repository: "o/r", Issue: 24, Attempt: 2, State: "failed"},
	} {
		reconcileGitHubRun = func(context.Context, string, string, string, bool) ([]orchestrator.RecoveryStatus, error) {
			return []orchestrator.RecoveryStatus{status}, nil
		}
		if err := recoverDashboardAttempt(t.Context(), configPath, filepath.Join(root, "pr.json"), filepath.Join(root, "state"), 24, 2); err == nil {
			t.Fatalf("unsafe/stale status was recoverable: %#v", status)
		}
	}
	if requests != 0 {
		t.Fatalf("unsafe/stale recovery made %d GitHub requests", requests)
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
