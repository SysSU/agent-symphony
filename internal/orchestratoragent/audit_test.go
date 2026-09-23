package orchestratoragent

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/SysSU/agent-symphony/internal/orchestrator"
)

func auditJSONL(text string) string {
	message, _ := json.Marshal(map[string]any{"type": "item.completed", "item": map[string]string{"type": "agent_message", "text": text}})
	return "{\"type\":\"turn.started\"}\n" + string(message) + "\n{\"type\":\"turn.completed\"}\n"
}

func TestAuditJSONLRequiresCompleteBoundedSuccessfulResult(t *testing.T) {
	valid := auditJSONL("final report")
	for _, test := range []struct {
		name, output, want string
	}{
		{"success", valid, "final report"},
		{"progress", "{\"type\":\"thread.started\",\"thread_id\":\"id\"}\n" + strings.Replace(valid, "{\"type\":\"turn.completed\"}", "{\"type\":\"item.completed\",\"item\":{\"type\":\"reasoning\",\"text\":\"not the report\"}}\n{\"type\":\"turn.completed\"}", 1), "final report"},
		{"last message", strings.Replace(valid, "{\"type\":\"turn.started\"}\n", "{\"type\":\"turn.started\"}\n{\"type\":\"item.completed\",\"item\":{\"type\":\"agent_message\",\"text\":\"progress\"}}\n", 1), "final report"},
		{"empty", "", ""},
		{"malformed prefix", "garbage\n" + valid, ""},
		{"error prefix", "{\"type\":\"error\",\"message\":\"failure\"}\n" + valid, ""},
		{"failed", strings.Replace(valid, "turn.completed", "turn.failed", 1), ""},
		{"failed item", strings.Replace(valid, "agent_message", "error", 1), ""},
		{"cancelled turn", strings.Replace(valid, "turn.completed", "turn.cancelled", 1), ""},
		{"repeated turn", "{\"type\":\"turn.started\"}\n" + valid, ""},
		{"truncated", strings.TrimSuffix(valid, "\n"), ""},
		{"missing completion", strings.Replace(valid, "{\"type\":\"turn.completed\"}\n", "", 1), ""},
		{"missing message", "{\"type\":\"turn.completed\"}\n", ""},
		{"message after completion", "{\"type\":\"turn.completed\"}\n" + valid, ""},
		{"trailing error", valid + "{\"type\":\"error\"}\n", ""},
		{"trailing malformed", valid + "{\n", ""},
		{"oversized report", auditJSONL(strings.Repeat("x", maxAuditReportBytes+1)), ""},
		{"maximum report", auditJSONL(strings.Repeat("x", maxAuditReportBytes)), strings.Repeat("x", maxAuditReportBytes)},
		{"empty report", auditJSONL(" \n"), ""},
		{"oversized stream", strings.Repeat(" ", AuditOutputMaxBytes) + valid, ""},
		{"invalid UTF8", "\xff" + valid, ""},
		{"wrong message type", strings.Replace(valid, "agent_message", "reasoning", 1), ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := parseAuditOutput(test.output)
			if got != test.want || (err != nil) != (test.want == "") {
				t.Fatalf("report length=%d want=%d error=%v", len(got), len(test.want), err)
			}
		})
	}
}

func TestAuditRestartRetainsLegacyChildrenAndRejectsLateOldCompletion(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	oldRunner := &fakeRunner{firstAuditEntered: make(chan struct{}), firstAuditGate: make(chan struct{}), firstAuditOutput: "stale old report"}
	old := newTestSupervisor(t, oldRunner, &now)
	old.AuditCommand = []string{"audit", "--json"}
	if err := os.MkdirAll(old.AuditWorkspace, 0o750); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"orchestrator-audit-legacy", "unrelated"} {
		path := filepath.Join(old.AuditWorkspace, name)
		if err := os.Mkdir(path, 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(path, "retain"), []byte(name), 0o640); err != nil {
			t.Fatal(err)
		}
	}
	projection := []orchestrator.RecoveryStatus{{Repository: old.Repository, Issue: 302, Attempt: 1, State: "active"}}
	if _, err := old.Observe(t.Context(), projection); err != nil {
		t.Fatal(err)
	}
	<-oldRunner.firstAuditEntered
	// End the old coordinator's ownership while its external launcher ignores
	// cancellation. A crash cannot execute any of that coordinator's Go code.
	stopped := make(chan error, 1)
	go func() { stopped <- old.Shutdown(t.Context()) }()
	_, _, oldCtx := oldRunner.auditPair()
	<-oldCtx.Done()
	waitHeartbeatReport(t, old.Workspace, "failed")
	released := false
	defer func() {
		if !released {
			close(oldRunner.firstAuditGate)
		}
		<-stopped
	}()
	fresh := newTestSupervisor(t, &fakeRunner{auditOutput: "fresh report"}, &now)
	fresh.Root, fresh.Workspace, fresh.AuditWorkspace = old.Root, old.Workspace, old.AuditWorkspace
	fresh.AuditCommand = []string{"audit", "--json"}
	if _, err := fresh.Observe(t.Context(), projection); err != nil {
		t.Fatal(err)
	}
	if _, err := fresh.Investigate(t.Context(), 302, 1); err != nil {
		t.Fatal(err)
	}
	fresh.wg.Wait()
	if report := waitHeartbeatReport(t, fresh.Workspace, "completed"); report.Report != "fresh report" {
		t.Fatalf("new coordinator report=%#v", report)
	}
	close(oldRunner.firstAuditGate)
	released = true
	old.wg.Wait()
	if report := waitHeartbeatReport(t, fresh.Workspace, "completed"); report.Report != "fresh report" {
		t.Fatalf("old completion replaced new report=%#v", report)
	}
	assertAuditDirectoryUnchanged(t, fresh.AuditWorkspace, []string{"orchestrator-audit-legacy", "unrelated"})
	for _, name := range []string{"orchestrator-audit-legacy", "unrelated"} {
		body, err := os.ReadFile(filepath.Join(fresh.AuditWorkspace, name, "retain"))
		if err != nil || string(body) != name {
			t.Fatalf("legacy/unrelated artifact changed: %q %v", body, err)
		}
	}
}

func TestAuditFailureAndCancellationCreateNoArtifacts(t *testing.T) {
	for _, mode := range []string{"failure", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
			runner := &fakeRunner{rawAuditOutput: true, auditOutput: "malformed"}
			if mode == "cancel" {
				runner.auditGate = make(chan struct{})
				runner.auditEntered = make(chan struct{}, 1)
			}
			agent := newTestSupervisor(t, runner, &now)
			agent.AuditCommand = []string{"audit"}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if err := agent.BindLifecycle(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := agent.Observe(t.Context(), []orchestrator.RecoveryStatus{{Repository: agent.Repository, Issue: 302, Attempt: 1, State: "active"}}); err != nil {
				t.Fatal(err)
			}
			if mode == "cancel" {
				<-runner.auditEntered
				cancel()
				if err := agent.Shutdown(t.Context()); err != nil {
					t.Fatal(err)
				}
			}
			agent.wg.Wait()
			assertAuditDirectoryUnchanged(t, agent.AuditWorkspace, nil)
			if report := waitHeartbeatReport(t, agent.Workspace, "failed"); report.Report != "" {
				t.Fatalf("failure published report: %#v", report)
			}
		})
	}
}

func TestAuditCannotPublishSuccessAfterItsContextEnds(t *testing.T) {
	now := time.Date(2026, 9, 22, 12, 0, 0, 0, time.UTC)
	agent := newTestSupervisor(t, &fakeRunner{auditOutput: "late success"}, &now)
	agent.AuditCommand = []string{"audit"}
	launch, err := agent.prepareAudit("bounded prompt", now, "digest", "")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	agent.auditGeneration, agent.auditRunning = 1, true
	agent.wg.Add(1)
	agent.runAudit(ctx, cancel, launch, 1, 0, now, "digest", "")
	if report := waitHeartbeatReport(t, agent.Workspace, "failed"); report.Report != "" || !strings.Contains(report.Diagnostic, "context canceled") {
		t.Fatalf("canceled audit published late success: %#v", report)
	}
	assertAuditDirectoryUnchanged(t, agent.AuditWorkspace, nil)
}
