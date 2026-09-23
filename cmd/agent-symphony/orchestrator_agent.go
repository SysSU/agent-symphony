package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

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
		AuditLauncher:         orchestratorAuditBoundaryCommand(),
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
	agent.Env = append(env, "GH_REPO="+cfg.Repository, "TMUX_TMPDIR="+projectTmuxRoot(stateRoot))
	agent.Env = append(agent.Env, "AGENT_SYMPHONY_LOCAL_ROOT="+productionSnapshotRoot(stateRoot))
	if cfg.Commands.OrchestratorAudit != nil {
		auditEnv, err := configuredWorkerEnvironment(cfg.Commands.Environment, stateRoot)
		if err != nil {
			return nil, err
		}
		agent.AuditEnv = append(auditEnv, "TMUX_TMPDIR="+projectTmuxRoot(stateRoot), "AGENT_SYMPHONY_LOCAL_ROOT="+productionSnapshotRoot(stateRoot))
	}
	for _, path := range workspaces {
		if err := os.MkdirAll(path, 0o750); err != nil {
			return nil, fmt.Errorf("prepare orchestrator workspace: %w", err)
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

func orchestratorBoundaryCommand() []string {
	binary, _ := os.Executable()
	return []string{binary, "agent-host", "orchestrator"}
}

func orchestratorAuditBoundaryCommand() []string {
	binary, _ := os.Executable()
	return []string{binary, "agent-host", "orchestrator-audit"}
}
