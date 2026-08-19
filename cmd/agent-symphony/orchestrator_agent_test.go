package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type orchestratorTestRunner struct {
	live bool
	pane string
}

func (r *orchestratorTestRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	switch command.Args[0] {
	case "display-message":
		if !r.live {
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("missing")
		}
		return agentruntime.Result{Output: "0\n"}, nil
	case "new-session", "respawn-pane":
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

func TestConfiguredReadOnlyOrchestratorProposesThroughCapturedOutput(t *testing.T) {
	cfg := config.Default("SysSU/example")
	cfg.Commands.Orchestrator = []string{"codex", "--sandbox", "read-only", "--ask-for-approval", "never", "--no-alt-screen"}
	stateRoot := t.TempDir()
	agent, err := newOrchestratorAgent(cfg, stateRoot)
	if err != nil {
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
	runner := &orchestratorTestRunner{pane: pane.String()}
	agent.Runner = runner
	if _, err := agent.Observe(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	got, err := agent.MessageProposal(t.Context())
	if err != nil || got.Binding == "" || got.Message != proposal.Message {
		t.Fatalf("proposal=%#v err=%v", got, err)
	}
	proposal.Message = strings.Repeat("<", internalgithub.OperatorMessageMaxBytes)
	body, _ = json.Marshal(proposal)
	if len(body) <= 16<<10 {
		t.Fatalf("maximum escaped proposal did not exercise the transport bound: %d bytes", len(body))
	}
	pane.Reset()
	if err := writeHostOrchestratorProposal(productionSnapshotRoot(stateRoot), bytes.NewReader(body), &pane); err != nil {
		t.Fatal(err)
	}
	runner.pane = pane.String()
	got, err = agent.MessageProposal(t.Context())
	if err != nil || got.Message != proposal.Message {
		t.Fatalf("maximum escaped proposal length=%d err=%v", len(got.Message), err)
	}
	launch, err := os.ReadFile(filepath.Join(agent.Workspace, orchestratorLaunchFile))
	if err != nil || !strings.Contains(string(launch), `"--sandbox"`) || !strings.Contains(string(launch), `"read-only"`) {
		t.Fatalf("read-only launch=%s err=%v", launch, err)
	}
	entries, err := os.ReadDir(agent.Workspace)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.Contains(entry.Name(), "proposal") {
			t.Fatalf("read-only proposal required a writable file: %s", entry.Name())
		}
	}
}

func TestAdvancedOrchestratorLaunchContractUsesSnapshotGroup(t *testing.T) {
	snapshotRoot := t.TempDir()
	gid := os.Getegid()
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

	cfg := config.Default("SysSU/example")
	cfg.Commands.Orchestrator = []string{"operator-agent", "--read-only"}
	agent, err := newOrchestratorAgent(cfg, t.TempDir())
	if err != nil {
		t.Fatal(err)
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
	contract, err := os.Stat(filepath.Join(agent.Workspace, orchestratorLaunchFile))
	if err != nil {
		t.Fatal(err)
	}
	if contract.Mode().Perm() != 0o440 || fileGID(contract) != gid {
		t.Fatalf("launch contract mode=%v gid=%d", contract.Mode(), fileGID(contract))
	}
}
