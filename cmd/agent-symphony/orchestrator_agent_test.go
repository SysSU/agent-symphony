package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type orchestratorTestRunner struct {
	live bool
	pane string
}

func TestDashboardMessageConfirmationSchedulesOnlyDurablyAcceptedMessages(t *testing.T) {
	proposal := orchestratoragent.MessageProposal{Version: 1, Repository: "o/r", Issue: 4, Attempt: 2, Message: "Continue."}
	var scheduled internalgithub.OperatorMessage
	service := dashboardOrchestratorService{
		confirm: func(context.Context, orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error) {
			return internalgithub.PrepareOperatorMessage("o/r", 4, 2, "Continue.")
		},
		accepted: func(message internalgithub.OperatorMessage) { scheduled = message },
	}
	message, err := service.ConfirmMessage(t.Context(), proposal)
	if err != nil || scheduled.ID == "" || scheduled.ID != message.ID {
		t.Fatalf("message=%#v scheduled=%#v err=%v", message, scheduled, err)
	}
	service.confirm = func(context.Context, orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error) {
		return internalgithub.OperatorMessage{}, errors.New("not recorded")
	}
	scheduled = internalgithub.OperatorMessage{}
	if _, err := service.ConfirmMessage(t.Context(), proposal); err == nil || scheduled.ID != "" {
		t.Fatalf("failed confirmation scheduled=%#v err=%v", scheduled, err)
	}
}

func fakeAdvancedOrchestratorHost(t *testing.T) int {
	t.Helper()
	snapshotRoot := t.TempDir()
	info, err := os.Stat(snapshotRoot)
	if err != nil {
		t.Fatal(err)
	}
	gid := fileGID(info)
	oldUser, oldGroup, oldSnapshotRoot := hostLookupUser, hostLookupGroup, reviewSnapshotRoot
	hostLookupUser = func(name string) (*user.User, error) {
		return &user.User{Username: name, Uid: strconv.Itoa(os.Geteuid()), Gid: strconv.Itoa(gid)}, nil
	}
	hostLookupGroup = func(name string) (*user.Group, error) {
		return &user.Group{Name: name, Gid: strconv.Itoa(gid)}, nil
	}
	reviewSnapshotRoot = snapshotRoot
	t.Cleanup(func() {
		hostLookupUser, hostLookupGroup, reviewSnapshotRoot = oldUser, oldGroup, oldSnapshotRoot
	})
	return gid
}

func (r *orchestratorTestRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	switch command.Args[0] {
	case "display-message":
		if !r.live {
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("missing")
		}
		return agentruntime.Result{Output: "0\n"}, nil
	case "new-session", "split-window":
		r.live = true
	case "capture-pane":
		return agentruntime.Result{Output: r.pane}, nil
	case "kill-session":
		if !r.live {
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("missing")
		}
		r.live = false
	}
	return agentruntime.Result{}, nil
}

func TestConfiguredFullAccessOrchestratorProposesThroughDurableArtifact(t *testing.T) {
	fakeAdvancedOrchestratorHost(t)
	oldPrepare := orchestratorWorkspacePrepare
	orchestratorWorkspacePrepare = func(path string, _ int) error { return os.MkdirAll(path, 0o750) }
	t.Cleanup(func() { orchestratorWorkspacePrepare = oldPrepare })
	cfg := config.Default("SysSU/example")
	cfg.Commands.Orchestrator = []string{"codex", "--sandbox", "danger-full-access", "--ask-for-approval", "never", "--no-alt-screen"}
	stateRoot := t.TempDir()
	agent, err := newOrchestratorAgent(cfg, stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	runner := &orchestratorTestRunner{}
	agent.Runner = runner
	if _, err := agent.Observe(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	proposal := struct {
		Version    int    `json:"version"`
		Repository string `json:"repository"`
		Issue      int    `json:"issue"`
		Attempt    int    `json:"attempt"`
		Message    string `json:"message"`
	}{1, cfg.Repository, 131, 3, "Run the focused test."}
	body, _ := json.Marshal(proposal)
	oldGetwd := hostGetwd
	hostGetwd = func() (string, error) { return agent.Workspace, nil }
	t.Cleanup(func() { hostGetwd = oldGetwd })
	var pane bytes.Buffer
	if err := writeHostOrchestratorProposal(productionSnapshotRoot(stateRoot), bytes.NewReader(body), &pane); err != nil {
		t.Fatal(err)
	}
	t.Setenv("AGENT_SYMPHONY_ORCHESTRATOR_ROOT", productionSnapshotRoot(stateRoot))
	queryStatus := func(input []byte) string {
		t.Helper()
		var output bytes.Buffer
		if err := agentHost(t.Context(), "orchestrator-proposal-status", bytes.NewReader(input), &output); err != nil {
			t.Fatal(err)
		}
		var status struct {
			State string `json:"state"`
		}
		if err := json.Unmarshal(output.Bytes(), &status); err != nil {
			t.Fatal(err)
		}
		return status.State
	}
	if state := queryStatus(body); state != "unknown" {
		t.Fatalf("emitted-only proposal state=%q", state)
	}
	got, err := agent.MessageProposal(t.Context())
	if err != nil || got.Binding == "" || got.Message != proposal.Message {
		t.Fatalf("proposal=%#v err=%v", got, err)
	}
	if state := queryStatus(body); state != "pending" {
		t.Fatalf("captured proposal state=%q", state)
	}
	previous := slices.Clone(body)
	proposal.Message = strings.Repeat("<", internalgithub.OperatorMessageMaxBytes)
	body, _ = json.Marshal(proposal)
	if len(body) <= 16<<10 {
		t.Fatalf("maximum escaped proposal did not exercise the transport bound: %d bytes", len(body))
	}
	pane.Reset()
	if err := writeHostOrchestratorProposal(productionSnapshotRoot(stateRoot), bytes.NewReader(body), &pane); err != nil {
		t.Fatal(err)
	}
	got, err = agent.MessageProposal(t.Context())
	if err != nil || got.Message != proposal.Message {
		t.Fatalf("maximum escaped proposal length=%d err=%v", len(got.Message), err)
	}
	if state := queryStatus(previous); state != "replaced" {
		t.Fatalf("replaced proposal state=%q", state)
	}
	if state := queryStatus(body); state != "pending" {
		t.Fatalf("replacement proposal state=%q", state)
	}
	if err := agent.ConsumeMessageProposal(t.Context(), got.Binding); err != nil {
		t.Fatal(err)
	}
	if state := queryStatus(body); state != "consumed" {
		t.Fatalf("consumed proposal state=%q", state)
	}
	launch, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratorLaunchFile))
	if err != nil || !strings.Contains(string(launch), `"--sandbox"`) || !strings.Contains(string(launch), `"danger-full-access"`) || !strings.Contains(string(launch), "orchestrator-proposal-status") || !strings.Contains(string(launch), "successful command durably submits") {
		t.Fatalf("full-access launch=%s err=%v", launch, err)
	}
	entries, err := os.ReadDir(agent.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Name() == orchestratoragent.MessageProposalFile {
			info, statErr := entry.Info()
			if statErr != nil {
				t.Fatal(statErr)
			}
			if info.Mode().Perm() != 0o620 {
				t.Fatalf("unsafe proposal artifact mode=%v", info.Mode())
			}
			continue
		}
		if entry.Name() == orchestratoragent.MessageProposalStatusFile {
			path := filepath.Join(agent.Workspace, entry.Name())
			status, readErr := os.ReadFile(path)
			info, statErr := os.Stat(path)
			if readErr != nil || statErr != nil {
				t.Fatalf("read proposal status: read=%v stat=%v", readErr, statErr)
			}
			if info.Mode().Perm() != 0o440 || strings.Contains(string(status), proposal.Message) {
				t.Fatalf("unsafe proposal status=%q mode=%v", status, info.Mode())
			}
			continue
		}
		if strings.Contains(entry.Name(), "proposal") {
			t.Fatalf("unexpected proposal artifact: %s", entry.Name())
		}
	}
}

func TestConfiguredOrchestratorUsesZeroAdminBoundary(t *testing.T) {
	fakeNoHostIsolation(t)
	stateRoot := t.TempDir()
	coordinatorHome := t.TempDir()
	t.Setenv("HOME", coordinatorHome)
	t.Setenv("GH_TOKEN", "coordinator-canary")
	t.Setenv("TMUX", "/private/coordinator-tmux-canary")
	cfg := config.Default("SysSU/example")
	cfg.Commands.Orchestrator = []string{"operator-agent"}
	agent, err := newOrchestratorAgent(cfg, stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	binary, _ := os.Executable()
	if !slices.Equal(agent.Launcher, []string{binary, "agent-host", "orchestrator"}) {
		t.Fatalf("zero-admin launcher=%#v", agent.Launcher)
	}
	root := localSnapshotRoot(stateRoot)
	if !slices.Contains(agent.Env, "AGENT_SYMPHONY_LOCAL_ROOT="+root) || !slices.Contains(agent.Env, "GH_REPO="+cfg.Repository) || !slices.Equal(agent.ProposalCommand, []string{binary, "agent-host", "orchestrator-proposal"}) || !slices.Equal(agent.ProposalStatusCommand, []string{binary, "agent-host", "orchestrator-proposal-status"}) || agent.AuditWorkspace != filepath.Join(root, "orchestrator-audit-"+internalgithub.RepositoryIdentifier(cfg.Repository)) || len(agent.AuditCommand) == 0 {
		t.Fatalf("zero-admin environment=%#v proposal=%#v status=%#v", agent.Env, agent.ProposalCommand, agent.ProposalStatusCommand)
	}
	agent.Runner = &orchestratorTestRunner{}
	if _, err := agent.Observe(t.Context(), nil); err != nil {
		t.Fatal(err)
	}

	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", root)
	oldGetwd, oldRun := hostGetwd, hostOrchestratorRun
	hostGetwd = func() (string, error) { return agent.Workspace, nil }
	var launched agentruntime.Command
	hostOrchestratorRun = func(_ context.Context, command agentruntime.Command) error { launched = command; return nil }
	t.Cleanup(func() { hostGetwd, hostOrchestratorRun = oldGetwd, oldRun })
	if err := agentHost(t.Context(), "orchestrator", strings.NewReader(""), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	current, err := hostCurrentUser()
	if err != nil {
		t.Fatal(err)
	}
	if launched.Name != "operator-agent" || launched.Dir != agent.Workspace || !slices.Contains(launched.Env, "HOME="+current.HomeDir) || slices.Contains(launched.Env, "HOME="+coordinatorHome) || !slices.Contains(launched.Env, "AGENT_SYMPHONY_ORCHESTRATOR_ROOT="+root) || !slices.Contains(launched.Env, "GH_TOKEN=coordinator-canary") || slices.ContainsFunc(launched.Env, func(value string) bool {
		return strings.HasPrefix(value, "TMUX=")
	}) {
		t.Fatalf("unsafe zero-admin launch: %#v", launched)
	}

	t.Setenv("AGENT_SYMPHONY_LOCAL_ROOT", "")
	t.Setenv("AGENT_SYMPHONY_ORCHESTRATOR_ROOT", root)
	proposal := `{"version":1,"repository":"SysSU/example","issue":159,"attempt":1,"message":"Run the focused test."}`
	var output bytes.Buffer
	if err := agentHost(t.Context(), "orchestrator-proposal", strings.NewReader(proposal), &output); err != nil || !strings.Contains(output.String(), `"state":"submitted"`) {
		t.Fatalf("zero-admin proposal=%q err=%v", output.String(), err)
	}
}

func TestAdvancedOrchestratorLaunchContractUsesSnapshotGroup(t *testing.T) {
	gid := fakeAdvancedOrchestratorHost(t)

	cfg := config.Default("SysSU/example")
	cfg.Commands.Orchestrator = []string{"operator-agent", "--read-only"}
	agent, err := newOrchestratorAgent(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(agent.Launcher[:7], []string{"sudo", githubCLIPreserveEnv, "-n", "-u", reviewerUser, "-g", snapshotGroup}) {
		t.Fatalf("orchestrator was not pinned to the reviewer identity: %#v", agent.Launcher)
	}
	agent.Runner = &orchestratorTestRunner{}
	if _, err := agent.Observe(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	workspace, err := os.Stat(agent.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	if workspace.Mode()&(os.ModePerm|os.ModeSetgid) != os.ModeSetgid|0o750 || fileGID(workspace) != gid {
		t.Fatalf("workspace mode=%v gid=%d", workspace.Mode(), fileGID(workspace))
	}
	auditWorkspace, err := os.Stat(agent.AuditWorkspace)
	if err != nil {
		t.Fatal(err)
	}
	if auditWorkspace.Mode()&(os.ModePerm|os.ModeSetgid) != os.ModeSetgid|0o750 || fileGID(auditWorkspace) != gid {
		t.Fatalf("audit workspace mode=%v gid=%d", auditWorkspace.Mode(), fileGID(auditWorkspace))
	}
	contract, err := os.Stat(filepath.Join(agent.Workspace, orchestratorLaunchFile))
	if err != nil {
		t.Fatal(err)
	}
	if contract.Mode().Perm() != 0o440 || fileGID(contract) != gid {
		t.Fatalf("launch contract mode=%v gid=%d", contract.Mode(), fileGID(contract))
	}
	proposal, err := os.Stat(filepath.Join(agent.Workspace, orchestratoragent.MessageProposalFile))
	if err != nil {
		t.Fatal(err)
	}
	if proposal.Mode().Perm() != 0o620 || fileGID(proposal) != gid {
		t.Fatalf("proposal artifact mode=%v gid=%d err=%v", proposal.Mode(), fileGID(proposal), err)
	}
}
