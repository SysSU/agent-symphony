package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

type dashboardOrchestratorService struct {
	orchestratoragent.Service
	supervisor *orchestratoragent.Supervisor
	confirm    func(context.Context, orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error)
}

var orchestratorWorkspacePrepare = prepareOrchestratorWorkspace

func (s dashboardOrchestratorService) MessageProposal(ctx context.Context) (orchestratoragent.MessageProposal, error) {
	return s.supervisor.MessageProposal(ctx)
}

func (s dashboardOrchestratorService) ConsumeMessageProposal(ctx context.Context, binding string) error {
	return s.supervisor.ConsumeMessageProposal(ctx, binding)
}

func (s dashboardOrchestratorService) ConfirmMessage(ctx context.Context, proposal orchestratoragent.MessageProposal) (internalgithub.OperatorMessage, error) {
	if s.confirm == nil {
		return internalgithub.OperatorMessage{}, fmt.Errorf("operator message confirmation is unavailable")
	}
	return s.confirm(ctx, proposal)
}

func newOrchestratorAgent(cfg config.Config, stateRoot string) (*orchestratoragent.Supervisor, error) {
	workspace := filepath.Join(productionSnapshotRoot(stateRoot), "orchestrator-"+internalgithub.RepositoryIdentifier(cfg.Repository))
	auditWorkspace := filepath.Join(productionSnapshotRoot(stateRoot), "orchestrator-audit-"+internalgithub.RepositoryIdentifier(cfg.Repository))
	agent := &orchestratoragent.Supervisor{
		Root:                  stateRoot,
		Workspace:             workspace,
		AuditWorkspace:        auditWorkspace,
		Repository:            cfg.Repository,
		Command:               cfg.Commands.Orchestrator,
		AuditCommand:          cfg.Commands.OrchestratorAudit,
		Launcher:              orchestratorBoundaryCommand(),
		ProposalCommand:       orchestratorProposalCommand(),
		ProposalStatusCommand: orchestratorProposalStatusCommand(),
		Runner:                agentruntime.ExecRunner{},
	}
	if cfg.Commands.Orchestrator == nil {
		return agent, nil
	}
	workspaces := []string{workspace}
	if cfg.Commands.OrchestratorAudit != nil {
		workspaces = append(workspaces, auditWorkspace)
	}
	env, err := configuredAgentEnvironment(cfg.Commands.Environment)
	if err != nil {
		return nil, err
	}
	agent.Env = env
	if !hostIsolationInstalled() {
		agent.Env = append(agent.Env, "AGENT_SYMPHONY_LOCAL_ROOT="+productionSnapshotRoot(stateRoot))
		for _, path := range workspaces {
			if err := os.MkdirAll(path, 0o750); err != nil {
				return nil, fmt.Errorf("prepare orchestrator workspace: %w", err)
			}
		}
		return agent, nil
	}
	group, err := hostLookupGroup(snapshotGroup)
	if err != nil {
		return nil, fmt.Errorf("resolve orchestrator boundary group: %w", err)
	}
	gid, err := strconv.Atoi(group.Gid)
	if err != nil {
		return nil, fmt.Errorf("prepare orchestrator reviewer boundary")
	}
	for _, path := range workspaces {
		if orchestratorWorkspacePrepare(path, gid) != nil {
			return nil, fmt.Errorf("prepare orchestrator reviewer boundary")
		}
	}
	return agent, nil
}

func orchestratorProposalCommand() []string {
	binary, _ := os.Executable()
	return []string{binary, "agent-host", "orchestrator-proposal"}
}

func orchestratorProposalStatusCommand() []string {
	binary, _ := os.Executable()
	return []string{binary, "agent-host", "orchestrator-proposal-status"}
}

func prepareOrchestratorWorkspace(path string, gid int) error {
	if err := os.MkdirAll(path, 0o750); err != nil {
		return err
	}
	listed, err := os.Lstat(path)
	if err != nil || !listed.IsDir() || listed.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("orchestrator workspace is unsafe")
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	opened, err := dir.Stat()
	if err != nil || !os.SameFile(listed, opened) {
		return fmt.Errorf("orchestrator workspace changed while opening")
	}
	if fileGID(opened) != gid {
		if err := dir.Chown(-1, gid); err != nil {
			return err
		}
	}
	if err := dir.Chmod(os.ModeSetgid | 0o750); err != nil {
		return err
	}
	current, err := os.Lstat(path)
	if err != nil || !os.SameFile(opened, current) || current.Mode()&(os.ModePerm|os.ModeSetgid) != os.ModeSetgid|0o750 || fileGID(current) != gid {
		return fmt.Errorf("orchestrator workspace ownership or mode is unsafe")
	}
	return nil
}

func orchestratorBoundaryCommand() []string {
	binary, _ := os.Executable()
	if !hostIsolationInstalled() {
		return []string{binary, "agent-host", "orchestrator"}
	}
	return []string{"sudo", "-n", "-u", reviewerUser, "-g", snapshotGroup, binary, "agent-host", "orchestrator"}
}
