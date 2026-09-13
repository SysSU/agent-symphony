package main

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type forbiddenReviewerBoundary struct{ calls int }

func (b *forbiddenReviewerBoundary) call(context.Context, string, agentruntime.Command) (agentruntime.Result, error) {
	b.calls++
	return agentruntime.Result{}, errors.New("replayed reviewer attempted boundary work")
}

type prelaunchFailureBoundary struct {
	created         bool
	killed          bool
	newSessionError bool
	beforeCreate    bool
}

type ambiguousRespawnBoundary struct {
	identity reviewerLaunchIdentity
	launch   string
	terminal string
	after    bool
	marker   bool
	dead     bool
	created  bool
	killed   bool
	probes   int
}

func (b *ambiguousRespawnBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary command")
	}
	switch command.Args[0] {
	case "set-option":
		if len(command.Args) > 5 && command.Args[4] == ";" && command.Args[5] == "new-session" {
			b.created = true
		}
		return agentruntime.Result{}, nil
	case "display-message":
		if command.Args[len(command.Args)-1] == "#{pane_start_command}" {
			b.probes++
			if b.after && b.probes > 1 {
				return agentruntime.Result{Output: "agent-symphony review-pane tmux " + b.launch + " " + b.terminal + " " + reviewerSignal(b.identity) + " " + b.identity.RequestDigest}, nil
			}
			return agentruntime.Result{Output: ""}, nil // tmux reports an empty start command for its default shell.
		}
		if b.created && !b.killed {
			if b.dead {
				return agentruntime.Result{Output: "1|||0|"}, nil
			}
			return agentruntime.Result{Output: "0||||"}, nil
		}
		return agentruntime.Result{Output: "||||"}, nil
	case "respawn-pane":
		if b.after && b.marker {
			if err := writeReviewerRecord(b.launch, b.identity); err != nil {
				return agentruntime.Result{}, err
			}
		}
		return agentruntime.Result{}, errors.New("respawn boundary response lost")
	case "kill-session":
		b.killed = true
		return agentruntime.Result{}, nil
	case "wait-for":
		return agentruntime.Result{}, nil
	default:
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary command")
	}
}

func (b *prelaunchFailureBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary command")
	}
	switch command.Args[0] {
	case "display-message":
		if b.created && !b.killed {
			return agentruntime.Result{Output: "0||||"}, nil
		}
		return agentruntime.Result{Output: "||||"}, nil
	case "set-option":
		if len(command.Args) > 5 && command.Args[4] == ";" && command.Args[5] == "new-session" {
			if !b.beforeCreate {
				b.created = true
			}
			if b.newSessionError {
				return agentruntime.Result{}, errors.New("tmux new-session response failed")
			}
			return agentruntime.Result{}, nil
		}
		return agentruntime.Result{}, errors.New("tmux set-option denied")
	case "kill-session":
		b.killed = true
		return agentruntime.Result{}, nil
	default:
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary command")
	}
}

type reviewerSessionStopBoundary struct {
	status agentruntime.Result
	err    error
	killed []string
}

func (b *reviewerSessionStopBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) < 2 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
	}
	switch command.Args[0] {
	case "display-message":
		return b.status, b.err
	case "kill-session":
		b.killed = append(b.killed, command.Args[len(command.Args)-1])
		b.status = agentruntime.Result{Output: "||||\n"}
		return agentruntime.Result{}, nil
	case "wait-for":
		return agentruntime.Result{}, nil
	default:
		return agentruntime.Result{}, errors.New("unexpected tmux command")
	}
}

func TestReviewerSessionStopRequiresExactPaneEvidence(t *testing.T) {
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		status    agentruntime.Result
		err       error
		wantKill  bool
		wantError bool
	}{
		{name: "exact missing pane", status: agentruntime.Result{Output: "||||\n"}},
		{name: "unverified live pane", status: agentruntime.Result{Output: "0||||\n"}, wantError: true},
		{name: "ambiguous tmux exit one", status: agentruntime.Result{Exited: true, Code: 1, Output: "permission denied"}, err: errors.New("tmux failed"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			boundary := &reviewerSessionStopBoundary{status: test.status, err: test.err}
			service := &operatorMutationService{reviewer: boundary, lifecycle: t.Context()}
			err := service.stopReviewerSession(t.Context(), session, strings.Repeat("a", 32), 99999999)
			if (err != nil) != test.wantError || (len(boundary.killed) != 0) != test.wantKill {
				t.Fatalf("err=%v killed=%v", err, boundary.killed)
			}
			if test.wantKill && boundary.killed[0] != "="+session {
				t.Fatalf("killed wrong session: %v", boundary.killed)
			}
		})
	}
}

type forgedTerminalStopBoundary struct {
	parentPID int
	reviewerID string
	kills      int
}

func (b *forgedTerminalStopBoundary) call(_ context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation != "run" || command.Name != "tmux" || len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
	}
	switch command.Args[0] {
	case "display-message":
		switch command.Args[4] {
		case agentruntime.PaneStatusFormat:
			return agentruntime.Result{Output: "0||||\n"}, nil
		case "#{pane_start_command}":
			return agentruntime.Result{Output: "agent-symphony review-pane tmux /tmp/launch /tmp/terminal review-" + b.reviewerID}, nil
		case "#{pane_pid}":
			return agentruntime.Result{Output: strconv.Itoa(b.parentPID)}, nil
		}
	case "kill-session":
		b.kills++ // The fake tmux ACK deliberately does not terminate the child.
		return agentruntime.Result{}, nil
	case "wait-for":
		return agentruntime.Result{}, nil
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer tmux command")
}

func TestReviewerStopDoesNotTrustTmuxKillAck(t *testing.T) {
	root := t.TempDir()
	reviewerID := strings.Repeat("a", 32)
	identity := reviewerLaunchIdentity{EffectID: reviewerID, IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	terminal := filepath.Join(root, "terminal.json")
	if err := writeReviewerRecord(terminal, reviewerTerminalRecord{Identity: identity, ExitCode: 0}); err != nil {
		t.Fatal(err)
	}
	readyReader, readyWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer readyReader.Close()
	defer readyWriter.Close()
	holdReader, holdWriter, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	defer holdReader.Close()
	defer holdWriter.Close()
	child := exec.Command("/bin/sh", "-c", `printf ready >&3; IFS= read -r _`)
	child.Stdin = holdReader
	child.ExtraFiles = []*os.File{readyWriter}
	child.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = syscall.Kill(-child.Process.Pid, syscall.SIGKILL); _ = child.Wait() }()
	ready := make(chan string, 1)
	go func() {
		buffer := make([]byte, 5)
		n, _ := readyReader.Read(buffer)
		ready <- string(buffer[:n])
	}()
	select {
	case value := <-ready:
		if value != "ready" {
			t.Fatalf("child readiness = %q", value)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("child did not start")
	}
	boundary := &forgedTerminalStopBoundary{parentPID: os.Getpid(), reviewerID: reviewerID}
	service := &operatorMutationService{lifecycle: t.Context(), reviewer: boundary}
	session, err := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 73, 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.stopReviewerSession(t.Context(), session, reviewerID, child.Process.Pid); err == nil || boundary.kills != 1 {
		t.Fatalf("forged terminal or kill ACK completed cancellation: err=%v kills=%d", err, boundary.kills)
	}
	if err := syscall.Kill(-child.Process.Pid, 0); err != nil {
		t.Fatalf("fake did not preserve live reviewer group: %v", err)
	}
	if content, err := os.ReadFile(terminal); err != nil || !strings.Contains(string(content), reviewerID) {
		t.Fatalf("forged terminal fixture missing: %q %v", content, err)
	}
}

type liveReviewerArtifactBoundary struct{ artifactCalls int }

func (b *liveReviewerArtifactBoundary) call(_ context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" {
		return agentruntime.Result{Output: "0||||\n"}, nil
	}
	if operation == "review-result" {
		b.artifactCalls++
		return agentruntime.Result{Exited: true, Code: reviewResultInvalidCode}, errors.New("forged clean result")
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
}

func TestPlanReviewerForgedTerminalCannotFinishLivePane(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	target := "o/r#73 plan sha256:" + digestText(issue.Body)
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running", ReviewState: "running", ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target, ReviewBase: attempt.BaseSHA, ReviewHead: attempt.BaseSHA, ReviewSnapshot: snapshot, ReviewSession: session}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	launchPath, terminalPath := reviewerLifecyclePaths(snapshot, target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(launchPath, identity); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	boundary := &liveReviewerArtifactBoundary{}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if !pending || err != nil || boundary.artifactCalls != 0 {
		t.Fatalf("forged terminal overrode live reviewer: pending=%v err=%v artifact_calls=%d", pending, err, boundary.artifactCalls)
	}
}

func TestReviewCleanupFailsClosedOnAmbiguousTmuxFailure(t *testing.T) {
	for _, test := range []struct {
		name      string
		status    agentruntime.Result
		err       error
		wantError bool
		wantKill  bool
	}{
		{name: "exact missing", status: agentruntime.Result{Output: "||||\n"}},
		{name: "live", status: agentruntime.Result{Output: "0||||\n"}, wantKill: true},
		{name: "exit one is ambiguous", status: agentruntime.Result{Exited: true, Code: 1, Output: "permission denied"}, err: errors.New("tmux unavailable"), wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			root := t.TempDir()
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: strings.Repeat("a", 40)}
			snapshot, session := reviewIdentity(attempt, root)
			target := "o/r#73 plan sha256:" + digestText("body")
			resultRoot := filepath.Dir(reviewResultPath(snapshot, target))
			for _, path := range []string{snapshot, resultRoot} {
				if err := os.MkdirAll(path, 0o700); err != nil {
					t.Fatal(err)
				}
			}
			boundary := &reviewerSessionStopBoundary{status: test.status, err: test.err}
			err := cleanupReviewResources(t.Context(), boundary, nil, attempt, attempt.BaseSHA, target, snapshot, session, root)
			if (err != nil) != test.wantError || (len(boundary.killed) != 0) != test.wantKill {
				t.Fatalf("cleanup err=%v killed=%v", err, boundary.killed)
			}
			for _, path := range []string{snapshot, resultRoot} {
				_, statErr := os.Lstat(path)
				if test.wantError && statErr != nil || !test.wantError && !errors.Is(statErr, os.ErrNotExist) {
					t.Fatalf("cleanup path %s stat=%v", path, statErr)
				}
			}
		})
	}
}

type blockedReviewerWatchBoundary struct {
	entered chan struct{}
	exited  chan struct{}
	once    sync.Once
}

func (b *blockedReviewerWatchBoundary) call(ctx context.Context, operation string, command agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" && command.Name == "tmux" && len(command.Args) >= 2 && command.Args[0] == "wait-for" {
		if command.Args[1] == "-U" {
			return agentruntime.Result{}, nil
		}
		if command.Args[1] == "-L" {
			b.once.Do(func() { close(b.entered) })
			<-ctx.Done()
			close(b.exited)
			return agentruntime.Result{}, ctx.Err()
		}
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
}

func TestInvalidatedReviewerWatcherExitsWithoutPaneSignal(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	boundary := &blockedReviewerWatchBoundary{entered: make(chan struct{}), exited: make(chan struct{})}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, reviewer: boundary, active: map[string]bool{}, released: map[string]chan struct{}{}}
	service.watchPlanReviewer("watch-cancel-test", effect.ID)
	<-boundary.entered
	service.cancelPlanWatcher(effect.ID)
	<-boundary.exited
}

func TestAmbiguousReviewerPaneProbeKeepsPendingIntentWithDiagnostic(t *testing.T) {
	request := reconciliationEffectCaseNamed(t, "reviewer-run-observe").request
	owner, snapshot, request := reconciliationEffectTestOwner(t, request)
	_, effect, err := owner.beginReconciliationEffect(t.Context(), beginReconciliationEffectCommand{Identity: reconciliationBeginIdentity(snapshot, request), Request: request})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := owner.markPlanReviewRunning(t.Context(), markPlanReviewRunningCommand{Identity: ownerReconciliationEffectIdentity(*effect), GroupPID: 99999999}); err != nil {
		t.Fatal(err)
	}
	current := mustOwnerSnapshot(t, owner)
	current.State.ControlReceipts = append(current.State.ControlReceipts, controlReceipt{Request: operatorRequest("probe-reviewer", "review-plan", *request.Manifest, false), State: "pending", Phase: operatorPhaseReviewPending, EffectID: effect.ID})
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Exited: true, Code: 1, Output: "permission denied"}, err: errors.New("tmux socket unavailable")}
	service := &operatorMutationService{lifecycle: t.Context(), owner: owner, reviewer: boundary}
	service.scanPendingPlanReviewers(t.Context(), current)
	after := mustOwnerSnapshot(t, owner).State.Effects[effect.ID]
	if after.State != "pending" || !strings.Contains(after.Diagnostic, "reviewer pane probe unavailable") || boundary.killed != nil {
		t.Fatalf("ambiguous probe falsely terminalized reviewer: %#v boundary=%#v", after, boundary)
	}
}

type invalidReviewArtifactBoundary struct{ calls int }

func (b *invalidReviewArtifactBoundary) call(_ context.Context, operation string, _ agentruntime.Command) (agentruntime.Result, error) {
	if operation == "run" {
		return agentruntime.Result{Output: "1|0|||\n"}, nil
	}
	if operation == "review-result" {
		b.calls++
		return agentruntime.Result{Exited: true, Code: reviewResultInvalidCode}, errors.New("invalid reviewer result")
	}
	return agentruntime.Result{}, errors.New("unexpected reviewer boundary call")
}

func TestReviewerPaneRecordsExactLaunchAndTerminalIdentity(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "tmux"), []byte("#!/bin/sh\nexit 0\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", root+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Setenv("TMUX_PANE", "%123")
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 2, AttemptGeneration: 4, RequestDigest: strings.Repeat("b", 64)}
	launch, terminal := filepath.Join(root, "launch.json"), filepath.Join(root, "terminal.json")
	encoded := `{"effect_id":"` + identity.EffectID + `","issue_generation":2,"attempt_generation":4,"request_digest":"` + identity.RequestDigest + `"}`
	args := []string{"tmux", launch, terminal, reviewerSignal(identity), reviewerStartSignal(identity), encoded, "--", "sh", "-c", "exit 0"}
	code, signal, err := runReviewerPane(args, io.Discard, io.Discard)
	if err != nil || code != 0 || signal != 0 {
		t.Fatalf("reviewer wrapper result code=%d signal=%d err=%v", code, signal, err)
	}
	record, err := readReviewerTerminal(launch, terminal, identity)
	if err != nil || record == nil || record.ExitCode != 0 || record.Signal != 0 {
		t.Fatalf("exact terminal record=%#v err=%v", record, err)
	}
	changed := identity
	changed.AttemptGeneration++
	if _, err := readReviewerTerminal(launch, terminal, changed); err == nil {
		t.Fatal("different attempt generation accepted prior reviewer launch")
	}
}

func TestPlanReviewerReplayFailsClosedWithoutExactLaunch(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 72, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	target := "o/r#72 plan sha256:" + digestText(issue.Body)
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running", ReviewState: "running", ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target, ReviewBase: attempt.BaseSHA, ReviewHead: attempt.BaseSHA, ReviewSnapshot: snapshot, ReviewSession: session}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	boundary := &reviewerSessionStopBoundary{status: agentruntime.Result{Output: "||||"}}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if pending || !errors.Is(err, errReviewerTerminal) || len(boundary.killed) != 0 {
		t.Fatalf("missing launch replay pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
	launchPath, terminalPath := reviewerLifecyclePaths(snapshot, target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	wrong := identity
	wrong.AttemptGeneration++
	if err := writeReviewerRecord(launchPath, wrong); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: wrong}); err != nil {
		t.Fatal(err)
	}
	_, pending, err = runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if pending || !errors.Is(err, errReviewerTerminal) || len(boundary.killed) != 0 {
		t.Fatalf("mismatched launch replay pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
}

func TestPlanReviewerCleanupFailureDoesNotWaitForAnUnlaunchedPane(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 74, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running"}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	boundary := &forbiddenReviewerBoundary{}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, false)
	if pending || !errors.Is(err, errReviewerTerminal) || boundary.calls != 1 {
		t.Fatalf("failed prelaunch cleanup may not leave a pending reviewer: pending=%v err=%v calls=%d", pending, err, boundary.calls)
	}
}

func TestPlanReviewerSetupFailureKillsDefaultShellAndTerminalizes(t *testing.T) {
	base, _, source, _ := testWorkerExportBoundary(t)
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 75, Number: 1, BaseSHA: base}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, Body: "plan"}
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, State: "running"}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
	boundary := &prelaunchFailureBoundary{}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, source, base, root, agentruntime.ReviewModePlan, &identity, false)
	if pending || !errors.Is(err, errReviewerTerminal) || !boundary.created || !boundary.killed {
		t.Fatalf("setup failure left default reviewer shell or pending intent: pending=%v err=%v boundary=%#v", pending, err, boundary)
	}
}

func TestPlanReviewerNewSessionErrorCleansExactShellBeforeTerminal(t *testing.T) {
	for _, test := range []struct {
		name         string
		beforeCreate bool
	}{
		{name: "error before session creation", beforeCreate: true},
		{name: "error after session creation"},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, _, source, _ := testWorkerExportBoundary(t)
			root := t.TempDir()
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 77, Number: 1, BaseSHA: base}
			issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, Body: "plan"}
			manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, State: "running"}
			identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
			boundary := &prelaunchFailureBoundary{newSessionError: true, beforeCreate: test.beforeCreate}
			_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, source, base, root, agentruntime.ReviewModePlan, &identity, false)
			if pending || !errors.Is(err, errReviewerTerminal) || boundary.killed == test.beforeCreate {
				t.Fatalf("new-session ambiguity left shell/receipt pending: pending=%v err=%v boundary=%#v", pending, err, boundary)
			}
		})
	}
}

func TestPlanReviewerAmbiguousRespawnProbesExactPaneBeforeTerminalizing(t *testing.T) {
	for _, test := range []struct {
		name   string
		after  bool
		marker bool
		dead   bool
	}{
		{name: "before spawn"},
		{name: "after spawn with marker", after: true, marker: true},
		{name: "after spawn before marker", after: true},
		{name: "after spawn crashes before marker", after: true, dead: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			base, _, source, _ := testWorkerExportBoundary(t)
			root := t.TempDir()
			attempt := agentruntime.Attempt{Repository: "o/r", Issue: 76, Number: 1, BaseSHA: base}
			issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, Body: "plan"}
			manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: base, State: "running"}
			identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64)}
			target, err := reviewTarget(agentruntime.ReviewModePlan, issue, base, base)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, session := reviewIdentity(attempt, root)
			launch, terminal := reviewerLifecyclePaths(snapshot, target)
			boundary := &ambiguousRespawnBoundary{identity: identity, launch: launch, terminal: terminal, after: test.after, marker: test.marker, dead: test.dead}
			_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, source, base, root, agentruntime.ReviewModePlan, &identity, false)
			if !test.after {
				if pending || !errors.Is(err, errReviewerTerminal) || !boundary.killed || boundary.probes != 2 {
					t.Fatalf("unstarted reviewer not terminalized: pending=%v err=%v boundary=%#v", pending, err, boundary)
				}
				return
			}
			if !pending || err == nil || boundary.killed {
				t.Fatalf("possibly started reviewer was terminalized: pending=%v err=%v boundary=%#v", pending, err, boundary)
			}
			if !test.marker {
				_, pending, err = runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, source, base, root, agentruntime.ReviewModePlan, &identity, true)
				if test.dead {
					if pending || !errors.Is(err, errReviewerTerminal) || !boundary.killed {
						t.Fatalf("dead unmarked reviewer did not terminalize: pending=%v err=%v boundary=%#v", pending, err, boundary)
					}
					return
				}
				if !pending || err != nil || boundary.killed {
					t.Fatalf("restart before launch marker lost live reviewer: pending=%v err=%v boundary=%#v", pending, err, boundary)
				}
			}
			if err := cleanupReviewResources(t.Context(), boundary, nil, attempt, base, target, snapshot, session, root); err != nil {
				t.Fatalf("test reviewer cleanup: %v", err)
			}
		})
	}
}

func TestPlanReviewerInvalidArtifactIsTerminalFailure(t *testing.T) {
	root := t.TempDir()
	attempt := agentruntime.Attempt{Repository: "o/r", Issue: 73, Number: 1, BaseSHA: strings.Repeat("a", 40)}
	issue := internalgithub.RecoveryIssueFact{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, Body: "plan"}
	target := "o/r#73 plan sha256:" + digestText(issue.Body)
	snapshot, session := reviewIdentity(attempt, root)
	manifest := agentruntime.Manifest{Repository: attempt.Repository, Issue: attempt.Issue, Attempt: attempt.Number, BaseSHA: attempt.BaseSHA, State: "running", ReviewState: "running", ReviewMode: agentruntime.ReviewModePlan, ReviewTarget: target, ReviewBase: attempt.BaseSHA, ReviewHead: attempt.BaseSHA, ReviewSnapshot: snapshot, ReviewSession: session}
	identity := reviewerLaunchIdentity{EffectID: strings.Repeat("a", 32), IssueGeneration: 1, AttemptGeneration: 1, RequestDigest: strings.Repeat("b", 64), ChildPID: 99999999}
	launchPath, terminalPath := reviewerLifecyclePaths(snapshot, target)
	if err := os.MkdirAll(filepath.Dir(launchPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(launchPath, identity); err != nil {
		t.Fatal(err)
	}
	if err := writeReviewerRecord(terminalPath, reviewerTerminalRecord{Identity: identity}); err != nil {
		t.Fatal(err)
	}
	boundary := &invalidReviewArtifactBoundary{}
	_, pending, err := runIndependentReviewCore(t.Context(), attempt, boundary, nil, []string{"review"}, issue, manifest, "", attempt.BaseSHA, root, agentruntime.ReviewModePlan, &identity, true)
	if pending || !errors.Is(err, errReviewerTerminal) || boundary.calls != 1 {
		t.Fatalf("invalid artifact replay pending=%v err=%v boundary_calls=%d", pending, err, boundary.calls)
	}
}
