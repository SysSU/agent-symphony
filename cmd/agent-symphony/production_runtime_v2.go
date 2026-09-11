package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/SysSU/agent-symphony/internal/config"
	internalgithub "github.com/SysSU/agent-symphony/internal/github"
	"github.com/SysSU/agent-symphony/internal/orchestratoragent"
	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// productionRuntimeV2 owns every goroutine that can observe or mutate the v2
// runtime ledger. Callers hold the daemon lock for its entire lifetime.
type productionRuntimeV2 struct {
	cancel   context.CancelFunc
	owner    *stateOwner
	effects  *runtimeEffectCoordinator
	operator *operatorMutationService
	status   *ownerStatusReplica
	cycle    *productionReconciliation
	trigger  *reconciliationTriggerRunner
	agent    *orchestratoragent.Supervisor
	proposal *supervisorProposalRunnerV2
}

func startProductionRuntimeV2(parent context.Context, cfg config.Config, api internalgithub.API, user internalgithub.AuthenticatedUser, stateRoot, legacyRecoveryPath, checkout string, log io.Writer) (*productionRuntimeV2, error) {
	if parent == nil || api.HTTP == nil || user.ID < 1 || cfg.Repository == "" || stateRoot == "" || legacyRecoveryPath == "" || checkout == "" || log == nil {
		return nil, errors.New("production v2 runtime is incomplete")
	}
	identity, err := readDeploymentIdentity(stateRoot)
	if err != nil || identity.Version != deploymentIdentityVersion || identity.Repository != cfg.Repository {
		return nil, errors.New("production v2 deployment fence is not installed")
	}
	attemptRoot := productionAttemptRoot(stateRoot)
	mode := os.FileMode(0o770)
	if !hostIsolationInstalled() {
		mode = 0o700
	}
	if err := os.MkdirAll(attemptRoot, mode); err != nil {
		return nil, fmt.Errorf("prepare attempt root: %w", err)
	}
	if err := prepareProductionMarkerDirectories(stateRoot); err != nil {
		return nil, err
	}
	initial, _, err := loadOrMigrateRuntimeOwnerState(stateRoot, legacyRecoveryPath, cfg.Repository)
	if err != nil {
		return nil, err
	}
	lifecycle, cancel := context.WithCancel(parent)
	runtime := &productionRuntimeV2{cancel: cancel}
	fail := func(err error) (*productionRuntimeV2, error) {
		_ = runtime.shutdown(context.Background())
		return nil, err
	}
	owner, err := startStateOwner(lifecycle, stateRoot, attemptRoot, initial, func(state runtimeOwnerState) error {
		return writeRuntimeOwnerState(stateRoot, attemptRoot, state)
	})
	if err != nil {
		cancel()
		return nil, err
	}
	runtime.owner = owner
	agent, err := newOrchestratorAgent(cfg, stateRoot)
	if err != nil {
		return fail(err)
	}
	if err := agent.BindLifecycle(lifecycle); err != nil {
		return fail(err)
	}
	runtime.agent = agent

	source, err := seedImmutableAttemptSource(lifecycle, checkout, cfg.Repository, attemptRoot, "", "")
	if err != nil {
		return fail(err)
	}
	binary, err := os.Executable()
	if err != nil {
		return fail(err)
	}
	implementation := implementationBoundary(stateRoot)
	reviewer := reviewBoundary(stateRoot)
	runtimeState := &agentruntime.Runtime{
		Root: attemptRoot, StateRoot: stateRoot, Source: source, Git: "git", Tmux: "tmux", Helper: binary,
		Runner: implementation, AllowEnv: cfg.Commands.Environment,
		VerifyWorker: func(ctx context.Context) error {
			_, err := implementation.call(ctx, "verify", agentruntime.Command{})
			return err
		},
	}
	effects, err := newRuntimeEffectCoordinator(lifecycle, owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		return fail(err)
	}
	runtime.effects = effects
	prConfig := githubPRConfig(cfg, user.ID)
	collector := reconciliationV2Collector{API: api, Config: prConfig, Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: cfg.Repository}}
	reviewEnvironment, err := configuredAgentEnvironment(cfg.Commands.Environment)
	if err != nil {
		return fail(err)
	}
	cleanup := operatorCleanupExecutor{stateRoot: stateRoot, implementation: implementation, reviewer: reviewer, runtime: runtimeState}
	operator, err := newOperatorMutationService(lifecycle, owner, effects, cleanup, collector, reviewer, source, reviewEnvironment, cfg.Commands.Reviewer)
	if err != nil {
		return fail(err)
	}
	runtime.operator = operator
	cycle := &productionReconciliation{
		owner: owner, effects: effects, collector: collector, config: cfg, api: api,
		stateRoot: stateRoot, attemptRoot: attemptRoot, checkout: checkout,
		implementation: implementation, reviewer: reviewer, reviewEnv: reviewEnvironment,
		supervisor: agent, capacity: cfg.Concurrency, log: log,
	}
	runtime.cycle = cycle
	status, err := startOwnerStatusReplica(lifecycle, owner, cfg.Concurrency, log)
	if err != nil {
		return fail(err)
	}
	runtime.status = status
	if err := cycle.sweepPendingMarkers(lifecycle); err != nil {
		return fail(err)
	}
	if err := operator.resumePending(lifecycle); err != nil {
		return fail(err)
	}
	trigger, err := newProductionReconciliationTriggerRunner(lifecycle, cycle.runCycle)
	if err != nil {
		return fail(err)
	}
	runtime.trigger = trigger
	cycle.wake = trigger.trigger
	if err := trigger.trigger(); err != nil {
		return fail(err)
	}
	proposal, err := startSupervisorProposalRunnerV2(lifecycle, &supervisorProposalServiceV2{agent: agent, owner: owner, effects: effects, operator: operator, trigger: trigger, capacity: cfg.Concurrency}, log)
	if err != nil {
		return fail(err)
	}
	runtime.proposal = proposal
	return runtime, nil
}

func prepareProductionMarkerDirectories(stateRoot string) error {
	if _, err := reconciliationMarkerDirectory(stateRoot, true); err != nil {
		return err
	}
	directory := filepath.Join(stateRoot, "runtime-effects")
	created := false
	if err := os.Mkdir(directory, 0o700); err != nil {
		if !errors.Is(err, os.ErrExist) {
			return err
		}
	} else {
		created = true
	}
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o700 || !ownedByCurrentUser(info) {
		return errors.New("runtime effect marker directory is unsafe")
	}
	if created {
		return immutableDirSync(stateRoot)
	}
	return nil
}

func (r *productionRuntimeV2) shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	if r.cancel != nil {
		r.cancel()
	}
	joins := []func() error{}
	if r.proposal != nil {
		joins = append(joins, func() error { return r.proposal.shutdown(ctx) })
	}
	if r.trigger != nil {
		joins = append(joins, func() error { return r.trigger.shutdown(ctx) })
	}
	if r.operator != nil {
		joins = append(joins, func() error { return r.operator.shutdown(ctx) })
	}
	if r.effects != nil {
		joins = append(joins, func() error { return r.effects.shutdown(ctx) })
	}
	if r.status != nil {
		joins = append(joins, func() error { return r.status.wait(ctx) })
	}
	if r.agent != nil {
		joins = append(joins, func() error { return r.agent.Shutdown(ctx) })
	}
	results := make(chan error, len(joins))
	for _, join := range joins {
		go func() { results <- join() }()
	}
	var result error
	for range joins {
		result = errors.Join(result, <-results)
	}
	if r.owner != nil {
		result = errors.Join(result, r.owner.close(ctx))
	}
	return result
}
