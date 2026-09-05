package orchestrator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

func TestRecoverRestartDuplicateStaleAndOrphans(t *testing.T) {
	facts := []AttemptFact{
		{Repository: "owner/repo", Issue: 1, Attempt: 1, BaseSHA: "aaaaaaa", State: "active"},
		{Repository: "owner/repo", Issue: 2, Attempt: 1, BaseSHA: "bbbbbbb", State: "completed", PR: 9},
		{Repository: "owner/repo", Issue: 3, Attempt: 2, BaseSHA: "ccccccc", State: "active"},
	}
	local := []agentruntime.Manifest{
		{Repository: "owner/repo", Issue: 1, Attempt: 1, BaseSHA: "aaaaaaa", State: "running", Session: "one"},
		{Repository: "owner/repo", Issue: 2, Attempt: 1, BaseSHA: "oldold1", State: "running", Session: "two"},
		{Repository: "owner/repo", Issue: 4, Attempt: 1, BaseSHA: "ddddddd", State: "failed", Session: "four"},
	}
	got := RecoverChecked(context.Background(), facts, local, func(context.Context, agentruntime.Manifest, AttemptFact) error { return nil })
	want := []string{"active", "completed", "blocked", "orphaned"}
	if len(got) != len(want) {
		t.Fatalf("got %#v", got)
	}
	for i := range want {
		if got[i].State != want[i] {
			t.Fatalf("[%d] = %#v", i, got[i])
		}
	}
	if got[0].Action == "" || got[1].Action == "" || got[2].Action == "" {
		t.Fatal("missing corrective action")
	}
}

func TestRecoverConflictingFactsFailClosed(t *testing.T) {
	facts := []AttemptFact{{Repository: "owner/repo", Issue: 1, Attempt: 1, BaseSHA: "aaaaaaa", State: "active"}, {Repository: "owner/repo", Issue: 1, Attempt: 1, BaseSHA: "bbbbbbb", State: "completed"}}
	got := Recover(facts, nil)
	if len(got) != 1 || got[0].State != "conflicting" {
		t.Fatalf("got %#v", got)
	}
}

func TestReconcileLoopRunsAtStartupAndRecoversAfterTransientOutage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := ReconcileLoop(ctx, time.Millisecond, func(context.Context) error {
		calls++
		if calls == 3 {
			cancel()
		}
		if calls < 3 {
			return errors.New("outage")
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestReconcileLoopWaitsAfterSlowCycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const interval = 20 * time.Millisecond
	started := make(chan struct{})
	release := make(chan struct{})
	done := make(chan error, 1)
	var completed time.Time
	var wait time.Duration
	calls := 0
	go func() {
		done <- ReconcileLoop(ctx, interval, func(context.Context) error {
			calls++
			switch calls {
			case 1:
				close(started)
				<-release
				completed = time.Now()
			case 2:
				wait = time.Since(completed)
				cancel()
			}
			return nil
		})
	}()
	<-started
	time.Sleep(2 * interval)
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) || calls != 2 || wait < interval {
		t.Fatalf("calls=%d wait=%s err=%v", calls, wait, err)
	}
}

func TestRecoverBlocksIssueLevelDuplicateAndPreservesCompletedAuthority(t *testing.T) {
	facts := []AttemptFact{{Repository: "o/r", Issue: 4, Attempt: 1, BaseSHA: "aaaaaaa", State: "completed"}, {Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "bbbbbbb", State: "active"}}
	local := []agentruntime.Manifest{{Repository: "o/r", Issue: 4, Attempt: 1, BaseSHA: "wrong00", State: "failed"}}
	got := Recover(facts, local)
	if len(got) != 2 || got[0].State != "completed" || got[1].State != "blocked" || len(got[1].Blockers) == 0 {
		t.Fatalf("got %#v", got)
	}
}

func TestRecoverBlocksFailedExactLivenessCheck(t *testing.T) {
	facts := []AttemptFact{{Repository: "o/r", Issue: 4, Attempt: 1, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", State: "active"}}
	local := []agentruntime.Manifest{{Repository: "o/r", Issue: 4, Attempt: 1, BaseSHA: "aaaaaaa", State: "running", Worktree: "/missing", Branch: "wrong", Session: "dead"}}
	calls := 0
	got := RecoverChecked(context.Background(), facts, local, func(context.Context, agentruntime.Manifest, AttemptFact) error {
		calls++
		return errors.New("exact tmux session is not live")
	})
	if len(got) != 1 || got[0].State != "blocked" || got[0].Diagnostic != "exact tmux session is not live" || !got[0].Retryable || got[0].Action == "" || calls != 1 {
		t.Fatalf("got %#v", got)
	}
}

func TestRecoverUnsafeWorktreeChecksAreDistinctAndNotRetryable(t *testing.T) {
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 1, BaseSHA: "aaaaaaa", State: "active"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 1, BaseSHA: "aaaaaaa", State: "running", Session: "named"}
	for _, test := range []struct {
		err     error
		blocker string
	}{
		{agentruntime.ErrWorktreeMissing, "runtime worktree missing"},
		{agentruntime.ErrWorktreeUnsafe, "runtime worktree unsafe"},
		{agentruntime.ErrWorktreeNonCanonical, "runtime worktree noncanonical"},
	} {
		got := RecoverChecked(t.Context(), []AttemptFact{fact}, []agentruntime.Manifest{manifest}, func(context.Context, agentruntime.Manifest, AttemptFact) error { return test.err })
		if len(got) != 1 || got[0].Retryable || !slices.Equal(got[0].Blockers, []string{test.blocker}) {
			t.Fatalf("err=%v got=%#v", test.err, got)
		}
	}
}

func TestRecoverTerminalFactJoinsRetainedFailure(t *testing.T) {
	fact := AttemptFact{Repository: "o/r", Issue: 13, Attempt: 1, BaseSHA: "aaaaaaa", State: "failed"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 13, Attempt: 1, BaseSHA: "aaaaaaa", State: "failed", Diagnostic: "worker produced no repository changes"}
	got := Recover([]AttemptFact{fact}, []agentruntime.Manifest{manifest})
	if len(got) != 1 || got[0].State != "failed" || got[0].Diagnostic != manifest.Diagnostic || !got[0].Retryable {
		t.Fatalf("got %#v", got)
	}
}

func TestRecoverTerminalFactDoesNotRetryNonterminalLocalRuntime(t *testing.T) {
	fact := AttemptFact{Repository: "o/r", Issue: 13, Attempt: 1, BaseSHA: "aaaaaaa", State: "failed"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 13, Attempt: 1, BaseSHA: "aaaaaaa", State: "running"}
	got := Recover([]AttemptFact{fact}, []agentruntime.Manifest{manifest})
	if len(got) != 1 || got[0].Retryable || got[0].Action != "terminalize the matching local runtime before requesting a retry" {
		t.Fatalf("got %#v", got)
	}
}

func TestRecoverOnlyLatestUncontestedFailureIsRetryable(t *testing.T) {
	facts := []AttemptFact{
		{Repository: "o/r", Issue: 13, Attempt: 1, BaseSHA: "aaaaaaa", State: "failed"},
		{Repository: "o/r", Issue: 13, Attempt: 2, BaseSHA: "bbbbbbb", State: "failed"},
	}
	local := []agentruntime.Manifest{
		{Repository: "o/r", Issue: 13, Attempt: 1, BaseSHA: "aaaaaaa", State: "failed"},
		{Repository: "o/r", Issue: 13, Attempt: 2, BaseSHA: "bbbbbbb", State: "failed"},
	}
	got := Recover(facts, local)
	if len(got) != 2 || got[0].State != "failed" || got[0].Retryable || !got[1].Retryable {
		t.Fatalf("got %#v", got)
	}
	facts = append(facts, AttemptFact{Repository: "o/r", Issue: 13, Attempt: 3, BaseSHA: "ccccccc", State: "active"})
	got = Recover(facts, local)
	if got[0].Retryable || got[1].Retryable {
		t.Fatalf("newer authority left failure retryable: %#v", got)
	}
}

func TestRecoverActiveBindingRetainsCompletedAttemptForPublication(t *testing.T) {
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "active"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "completed", Session: "as-o-r-4-2", ReviewState: "running"}
	got := RecoverChecked(context.Background(), []AttemptFact{fact}, []agentruntime.Manifest{manifest}, func(context.Context, agentruntime.Manifest, AttemptFact) error { return nil })
	if len(got) != 1 || got[0].State != "active" || got[0].CurrentPhase != "review" || got[0].Action != "restore or restart the exact reviewer session" {
		t.Fatalf("got %#v", got)
	}
}

func TestRecoverProjectsBoundedAttemptSessionsAndPhases(t *testing.T) {
	created := time.Date(2026, 8, 20, 1, 2, 3, 0, time.UTC)
	updated := created.Add(time.Minute)
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 4, 2)
	reviewer, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 4, 2)
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "active"}
	base := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: fact.BaseSHA, State: "completed", Session: implementation, CreatedAt: created, UpdatedAt: updated}

	for _, test := range []struct {
		name           string
		manifest       agentruntime.Manifest
		phase          string
		currentRole    string
		sessionCount   int
		actionContains string
	}{
		{"implementation", func() agentruntime.Manifest { m := base; m.State = "running"; return m }(), "implementation", "implementation", 1, "implementation session"},
		{"validation", base, "validation", "", 1, "validate"},
		{"review running", func() agentruntime.Manifest {
			m := base
			m.ReviewState, m.ReviewSession = "running", reviewer
			m.ReviewMode, m.ReviewTarget = agentruntime.ReviewModeImplementation, "aaaaaaa..bbbbbbb"
			m.ReviewBase, m.ReviewHead = "aaaaaaa", "bbbbbbb"
			return m
		}(), "review", "reviewer", 2, "reviewer session"},
		{"review session missing", func() agentruntime.Manifest { m := base; m.ReviewState = "running"; return m }(), "review", "", 1, "restore"},
		{"findings handoff", func() agentruntime.Manifest {
			m := base
			m.ReviewState, m.ReviewHandoffQueued = "findings-queued", true
			return m
		}(), "findings-handoff", "implementation", 1, "deliver review findings"},
		{"follow-up implementation", func() agentruntime.Manifest {
			m := base
			m.State, m.ReviewState, m.ReviewHandoffQueued, m.ReviewHandoffAck = "running", "findings-queued", true, true
			return m
		}(), "findings-handoff", "implementation", 1, "follow-up implementation"},
		{"publication after reviewer cleanup", func() agentruntime.Manifest { m := base; m.ReviewState = "clean"; return m }(), "publication", "", 1, "publish"},
	} {
		t.Run(test.name, func(t *testing.T) {
			got := RecoverChecked(t.Context(), []AttemptFact{fact}, []agentruntime.Manifest{test.manifest}, func(context.Context, agentruntime.Manifest, AttemptFact) error { return nil })
			if len(got) != 1 || got[0].CurrentPhase != test.phase || len(got[0].Sessions) != test.sessionCount || !strings.Contains(got[0].Action, test.actionContains) {
				t.Fatalf("got %#v", got)
			}
			current := ""
			for _, session := range got[0].Sessions {
				if session.Current {
					current = session.Role
				}
				if session.Name == implementation && (session.CreatedAt != created || session.UpdatedAt != updated) {
					t.Fatalf("implementation timestamps = %#v", session)
				}
				if session.Name == reviewer && test.name == "review running" && (session.Mode != agentruntime.ReviewModeImplementation || session.Target != "aaaaaaa..bbbbbbb") {
					t.Fatalf("review metadata = %#v", session)
				}
			}
			if current != test.currentRole {
				t.Fatalf("current role = %q, want %q: %#v", current, test.currentRole, got[0].Sessions)
			}
		})
	}
}

func TestRecoverOmitsUnknownSessionIdentityAndKeepsTerminalHistory(t *testing.T) {
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 4, 2)
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "failed"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: fact.BaseSHA, State: "cancelled", Session: implementation, ReviewState: "running", ReviewSession: "as-r-forged"}
	got := Recover([]AttemptFact{fact}, []agentruntime.Manifest{manifest})
	if len(got) != 1 || got[0].CurrentPhase != "failed" || len(got[0].Sessions) != 1 || got[0].Sessions[0].Name != implementation || got[0].Sessions[0].State != "cancelled" {
		t.Fatalf("got %#v", got)
	}
}

func TestRecoverOmitsMalformedReviewTarget(t *testing.T) {
	implementation, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleImplementation, "o/r", 4, 2)
	reviewer, _ := agentruntime.AttemptSessionName(agentruntime.SessionRoleReviewer, "o/r", 4, 2)
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "active"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: fact.BaseSHA, State: "running", Session: implementation, ReviewState: "running", ReviewMode: agentruntime.ReviewModeImplementation, ReviewTarget: "garbage", ReviewSession: reviewer}
	got := Recover([]AttemptFact{fact}, []agentruntime.Manifest{manifest})
	if len(got) != 1 || slices.ContainsFunc(got[0].Sessions, func(session AttemptSession) bool { return session.Role == agentruntime.SessionRoleReviewer }) {
		t.Fatalf("malformed reviewer was projected: %#v", got)
	}
}

func TestRecoverPublishedAttemptUsesDurablePRInsteadOfWorkerLiveness(t *testing.T) {
	for _, state := range []string{"active", "review-ready"} {
		t.Run(state, func(t *testing.T) {
			fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", PR: 9, State: state}
			manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "completed", ReviewState: "clean", ReviewHead: "bbbbbbb", Session: "dead"}
			checked := false
			got := RecoverChecked(t.Context(), []AttemptFact{fact}, []agentruntime.Manifest{manifest}, func(context.Context, agentruntime.Manifest, AttemptFact) error {
				checked = true
				return errors.New("dead")
			})
			if checked || len(got) != 1 || got[0].State != state || got[0].PR != 9 || got[0].Action != "monitor the matching published pull request" || len(got[0].Blockers) != 0 {
				t.Fatalf("checked=%v got=%#v", checked, got)
			}
		})
	}
}

func TestRecoverPublishedAttemptIdentityMismatchFailsClosed(t *testing.T) {
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", PR: 9, State: "active"}
	for _, manifest := range []agentruntime.Manifest{
		{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "wrong00", State: "completed", ReviewHead: "bbbbbbb", Session: "dead"},
		{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "completed", ReviewHead: "force00", Session: "dead"},
	} {
		got := RecoverChecked(t.Context(), []AttemptFact{fact}, []agentruntime.Manifest{manifest}, func(context.Context, agentruntime.Manifest, AttemptFact) error { return errors.New("dead") })
		if len(got) != 1 || got[0].State != "blocked" || len(got[0].Blockers) == 0 {
			t.Fatalf("manifest=%#v got=%#v", manifest, got)
		}
	}
}

func TestRecoverRunningPublishedAttemptStillRequiresLiveness(t *testing.T) {
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", HeadSHA: "bbbbbbb", PR: 9, State: "active"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "running", Session: "live"}
	checked := false
	got := RecoverChecked(t.Context(), []AttemptFact{fact}, []agentruntime.Manifest{manifest}, func(context.Context, agentruntime.Manifest, AttemptFact) error {
		checked = true
		return nil
	})
	if !checked || len(got) != 1 || got[0].State != "active" || got[0].Action != "monitor the implementation session" {
		t.Fatalf("checked=%v got=%#v", checked, got)
	}
}

func TestReconcileLoopRejectsSlowInterval(t *testing.T) {
	if err := ReconcileLoop(context.Background(), 61*time.Second, func(context.Context) error { return nil }); err == nil {
		t.Fatal("accepted interval over 60 seconds")
	}
}
