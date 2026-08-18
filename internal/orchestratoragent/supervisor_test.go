package orchestratoragent

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/orchestrator"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type fakeRunner struct {
	live       bool
	failStarts int
	starts     int
	notices    []string
	commands   []agentruntime.Command
}

func (f *fakeRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	f.commands = append(f.commands, command)
	if command.Name != "tmux" || len(command.Args) == 0 {
		return agentruntime.Result{}, errors.New("unexpected command")
	}
	switch command.Args[0] {
	case "display-message":
		if !f.live {
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("missing")
		}
		return agentruntime.Result{Output: "0\n"}, nil
	case "new-session":
		f.starts++
		if f.failStarts > 0 {
			f.failStarts--
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("launch failed token=secret-value")
		}
		f.live = true
	case "respawn-pane":
		f.live = true
	case "kill-session":
		if !f.live {
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("missing")
		}
		f.live = false
	case "load-buffer":
		body, _ := io.ReadAll(command.Stdin)
		f.notices = append(f.notices, string(body))
	}
	return agentruntime.Result{}, nil
}

func newTestSupervisor(t *testing.T, runner *fakeRunner, now *time.Time) *Supervisor {
	t.Helper()
	root := t.TempDir()
	return &Supervisor{Root: root, Workspace: filepath.Join(root, "workspace"), Repository: "SysSU/example", Command: []string{"agent", "--read-only"}, Runner: runner, Now: func() time.Time { return *now }}
}

func TestDisabledSupervisorNeverLaunches(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{}
	agent := newTestSupervisor(t, runner, &now)
	agent.Command = nil
	status, err := agent.Observe(context.Background(), nil)
	if err != nil || status.Enabled || status.State != "disabled" || runner.starts != 0 {
		t.Fatalf("disabled status = %#v, starts=%d, err=%v", status, runner.starts, err)
	}
}

func TestRemovingCommandStopsExactSessionAndPersistsDisabled(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{}
	agent := newTestSupervisor(t, runner, &now)
	if _, err := agent.Observe(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	agent.Command = nil
	status, err := agent.Observe(context.Background(), nil)
	if err != nil || status.Enabled || status.State != "disabled" || runner.live {
		t.Fatalf("disabled status=%#v live=%v err=%v", status, runner.live, err)
	}
	last := runner.commands[len(runner.commands)-1]
	if !slices.Equal(last.Args, []string{"kill-session", "-t", "=" + Session(agent.Repository)}) {
		t.Fatalf("exact persistent session was not stopped: %#v", runner.commands)
	}
	restarted := newTestSupervisor(t, runner, &now)
	restarted.Root, restarted.Workspace, restarted.Command = agent.Root, agent.Workspace, nil
	persistedStatus, err := restarted.Status(context.Background())
	if err != nil || persistedStatus.Enabled || persistedStatus.State != "disabled" {
		t.Fatalf("persisted disabled status=%#v err=%v", persistedStatus, err)
	}
}

func TestLifecycleAdoptsRecreatesClearsAndRebuilds(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{}
	agent := newTestSupervisor(t, runner, &now)
	projection := []orchestrator.RecoveryStatus{{Repository: agent.Repository, Issue: 5, Attempt: 1, State: "blocked", Diagnostic: "runtime mismatch", Action: "retry"}}

	first, err := agent.Observe(context.Background(), projection)
	if err != nil || first.State != "running" || first.Generation != 1 || first.PendingAttention != 1 || runner.starts != 1 || len(runner.notices) != 1 {
		t.Fatalf("first observe = %#v, starts=%d notices=%d err=%v", first, runner.starts, len(runner.notices), err)
	}
	second, err := agent.Observe(context.Background(), projection)
	if err != nil || second.Generation != 1 || runner.starts != 1 || len(runner.notices) != 1 {
		t.Fatalf("adoption/dedupe failed: %#v starts=%d notices=%d err=%v", second, runner.starts, len(runner.notices), err)
	}

	runner.live = false
	now = now.Add(time.Minute)
	recreated, err := agent.Observe(context.Background(), projection)
	if err != nil || recreated.Generation != 2 || runner.starts != 2 {
		t.Fatalf("recreated = %#v starts=%d err=%v", recreated, runner.starts, err)
	}

	now = now.Add(time.Minute)
	cleared, err := agent.Clear(context.Background())
	if err != nil || cleared.ContextMode != "clear" || cleared.Generation != 3 {
		t.Fatalf("clear = %#v err=%v", cleared, err)
	}
	contextBody, _ := os.ReadFile(filepath.Join(agent.Root, "orchestrator-context.md"))
	if strings.Contains(string(contextBody), "Sanitized current projection") {
		t.Fatalf("clear retained projection: %s", contextBody)
	}
	notices := len(runner.notices)
	if _, err := agent.Observe(context.Background(), projection); err != nil || len(runner.notices) != notices {
		t.Fatalf("clear immediately replayed attention: notices=%d err=%v", len(runner.notices), err)
	}

	now = now.Add(time.Minute)
	rebuilt, err := agent.Rebuild(context.Background())
	if err != nil || rebuilt.ContextMode != "rebuild" || rebuilt.Generation != 4 || rebuilt.RebuiltAt != now {
		t.Fatalf("rebuild = %#v err=%v", rebuilt, err)
	}
	contextBody, _ = os.ReadFile(filepath.Join(agent.Root, "orchestrator-context.md"))
	if !strings.Contains(string(contextBody), "Sanitized current projection") || !strings.Contains(string(contextBody), `"issue": 5`) {
		t.Fatalf("rebuilt context lacks projection: %s", contextBody)
	}
	if info, err := os.Stat(filepath.Join(agent.Root, "orchestrator-agent.json")); err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %v, err=%v", info.Mode(), err)
	}
}

func TestOutageRecoveryReusesDurableContext(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{}
	agent := newTestSupervisor(t, runner, &now)
	projection := []orchestrator.RecoveryStatus{{Repository: agent.Repository, Issue: 5, Attempt: 1, State: "blocked", Diagnostic: "durable diagnostic"}}
	if _, err := agent.Observe(context.Background(), projection); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(agent.Root, "orchestrator-context.md")
	want, err := os.ReadFile(path)
	if err != nil || !strings.Contains(string(want), "durable diagnostic") {
		t.Fatalf("durable context=%q err=%v", want, err)
	}
	runner.live = false
	now = now.Add(time.Minute)
	restarted := newTestSupervisor(t, runner, &now)
	restarted.Root, restarted.Workspace = agent.Root, agent.Workspace
	status, err := restarted.Recover(context.Background())
	got, readErr := os.ReadFile(path)
	if err != nil || readErr != nil || status.Generation != 2 || !bytes.Equal(got, want) {
		t.Fatalf("recovered=%#v context=%q err=%v read=%v", status, got, err, readErr)
	}
}

func TestFailureBackoffAndRedaction(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{failStarts: 2}
	agent := newTestSupervisor(t, runner, &now)
	agent.projectionKnown = true
	status, err := agent.Recover(context.Background())
	if err == nil || status.State != "degraded" || status.RetryAt != now.Add(time.Minute) || strings.Contains(status.Diagnostic, "secret-value") || runner.starts != 1 {
		t.Fatalf("first failure = %#v starts=%d err=%v", status, runner.starts, err)
	}
	status, err = agent.Recover(context.Background())
	if err != nil || status.State != "degraded" || runner.starts != 1 {
		t.Fatalf("backoff retry = %#v starts=%d err=%v", status, runner.starts, err)
	}
	now = now.Add(time.Minute)
	status, err = agent.Recover(context.Background())
	if err == nil || status.RetryAt != now.Add(2*time.Minute) || runner.starts != 2 {
		t.Fatalf("second failure = %#v starts=%d err=%v", status, runner.starts, err)
	}
}

func TestLaunchContractUsesFixedBoundaryCommand(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{}
	agent := newTestSupervisor(t, runner, &now)
	agent.Command = []string{"agent", "--workspace", "{orchestrator_workspace}"}
	agent.Launcher = []string{"agent-symphony", "agent-host", "orchestrator"}
	agent.projectionKnown = true
	if _, err := agent.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(runner.commands, func(command agentruntime.Command) bool {
		return len(command.Args) > 0 && command.Args[0] == "respawn-pane"
	})
	if index < 0 || !slices.Equal(runner.commands[index].Args[len(runner.commands[index].Args)-3:], agent.Launcher) {
		t.Fatalf("pane did not use fixed launcher: %#v", runner.commands)
	}
	remainOptions := 0
	for _, command := range runner.commands {
		if slices.Contains(command.Args, "remain-on-exit") {
			remainOptions++
		}
	}
	if remainOptions != 1 {
		t.Fatalf("remain-on-exit configured %d times: %#v", remainOptions, runner.commands)
	}
	path := filepath.Join(agent.Workspace, "orchestrator-launch.json")
	body, err := os.ReadFile(path)
	info, statErr := os.Stat(path)
	if err != nil || statErr != nil || info.Mode().Perm() != 0o440 || !strings.Contains(string(body), `"command": [`) || !strings.Contains(string(body), agent.Workspace) || strings.Contains(string(body), "{orchestrator_workspace}") || !strings.Contains(string(body), `"context":`) {
		t.Fatalf("launch contract body=%q mode=%v read=%v stat=%v", body, info.Mode(), err, statErr)
	}
}

func TestLaunchExpandsOrchestratorWorkspaceWithoutChangingConfiguredCommand(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{}
	agent := newTestSupervisor(t, runner, &now)
	configured := `projects={"{orchestrator_workspace}"={trust_level="trusted"}}`
	agent.Command = []string{"codex", "-c", configured}
	agent.projectionKnown = true
	if _, err := agent.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	index := slices.IndexFunc(runner.commands, func(command agentruntime.Command) bool {
		return len(command.Args) > 0 && command.Args[0] == "respawn-pane"
	})
	if index < 0 {
		t.Fatalf("missing respawn command: %#v", runner.commands)
	}
	want := `projects={"` + agent.Workspace + `"={trust_level="trusted"}}`
	if !slices.Contains(runner.commands[index].Args, want) {
		t.Fatalf("workspace override not expanded: %#v", runner.commands[index])
	}
	if agent.Command[2] != configured {
		t.Fatalf("configured command mutated: %#v", agent.Command)
	}
}

func TestProjectionIsSanitizedBoundedAndInvestigateIsExact(t *testing.T) {
	now := time.Date(2026, 8, 15, 1, 2, 3, 0, time.UTC)
	runner := &fakeRunner{}
	agent := newTestSupervisor(t, runner, &now)
	projection := []orchestrator.RecoveryStatus{
		{Repository: agent.Repository, Issue: 5, Attempt: 1, State: "failed", Title: "untrusted title", Diagnostic: "token=abc123\x00", Action: strings.Repeat("x", 700)},
		{Repository: "Other/repo", Issue: 9, Attempt: 1, State: "failed"},
	}
	if _, err := agent.Observe(context.Background(), projection); err != nil {
		t.Fatal(err)
	}
	contextBody, _ := os.ReadFile(filepath.Join(agent.Root, "orchestrator-context.md"))
	if strings.Contains(string(contextBody), "untrusted title") || strings.Contains(string(contextBody), "abc123") || len(contextBody) > maxContextBytes {
		t.Fatalf("unsafe context: %s", contextBody)
	}
	aggregateNotices := len(runner.notices)
	if _, err := agent.Investigate(context.Background(), 5, 1); err != nil || len(runner.notices) != aggregateNotices+1 {
		t.Fatal(err)
	}
	notices := len(runner.notices)
	if _, err := agent.Investigate(context.Background(), 5, 1); err != nil || len(runner.notices) != notices {
		t.Fatalf("investigate was not deduplicated: notices=%d err=%v", len(runner.notices), err)
	}
	if _, err := agent.Investigate(context.Background(), 5, 2); err == nil {
		t.Fatal("investigate accepted an absent attempt")
	}
	last := runner.commands[len(runner.commands)-1]
	if slices.ContainsFunc(last.Env, func(value string) bool { return strings.HasPrefix(value, "GH_TOKEN=") }) {
		t.Fatalf("credential reached runner: %v", last.Env)
	}
}
