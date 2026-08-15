package main

import (
	"context"
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/SysSU/agent-symphony/internal/config"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type orchestratorTestRunner struct{ live bool }

func (r *orchestratorTestRunner) Run(_ context.Context, command agentruntime.Command) (agentruntime.Result, error) {
	switch command.Args[0] {
	case "display-message":
		if !r.live {
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("missing")
		}
		return agentruntime.Result{Output: "0\n"}, nil
	case "new-session", "respawn-pane":
		r.live = true
	case "kill-session":
		if !r.live {
			return agentruntime.Result{Exited: true, Code: 1}, errors.New("missing")
		}
		r.live = false
	}
	return agentruntime.Result{}, nil
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
