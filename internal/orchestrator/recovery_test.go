package orchestrator

import (
	"context"
	"errors"
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
	err := ReconcileLoop(ctx, time.Millisecond, nil, func(context.Context) error {
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

func TestReconcileLoopWakeTriggersExtraReconcileBetweenTicks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wake := make(chan struct{}, 1)
	calls := make(chan int, 8)
	count := 0
	done := make(chan error, 1)
	go func() {
		done <- ReconcileLoop(ctx, MaxReconcileInterval, wake, func(context.Context) error {
			count++
			calls <- count
			return nil
		})
	}()
	if got := <-calls; got != 1 {
		t.Fatalf("startup call = %d, want 1", got)
	}
	wake <- struct{}{}
	if got := <-calls; got != 2 {
		t.Fatalf("wake-triggered call = %d, want 2 (interval is the 60s max, so only a wake could cause this within the test)", got)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
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
	got := RecoverChecked(context.Background(), facts, local, func(context.Context, agentruntime.Manifest, AttemptFact) error { return errors.New("dead") })
	if len(got) != 1 || got[0].State != "blocked" || got[0].Action == "" {
		t.Fatalf("got %#v", got)
	}
}

func TestRecoverActiveBindingRetainsCompletedAttemptForPublication(t *testing.T) {
	fact := AttemptFact{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "active"}
	manifest := agentruntime.Manifest{Repository: "o/r", Issue: 4, Attempt: 2, BaseSHA: "aaaaaaa", State: "completed", Session: "as-o-r-4-2", ReviewState: "running"}
	got := RecoverChecked(context.Background(), []AttemptFact{fact}, []agentruntime.Manifest{manifest}, func(context.Context, agentruntime.Manifest, AttemptFact) error { return nil })
	if len(got) != 1 || got[0].State != "active" || got[0].Action != "resume publication of the matching completed attempt" {
		t.Fatalf("got %#v", got)
	}
}

func TestReconcileLoopRejectsSlowInterval(t *testing.T) {
	if err := ReconcileLoop(context.Background(), 61*time.Second, nil, func(context.Context) error { return nil }); err == nil {
		t.Fatal("accepted interval over 60 seconds")
	}
}
