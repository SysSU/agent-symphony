package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

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
	if err := validateProductionStateRoot(stateRoot); err != nil {
		return nil, err
	}
	identity, err := readDeploymentIdentity(stateRoot)
	if err != nil || identity.Version != deploymentIdentityVersion || identity.Repository != cfg.Repository {
		return nil, errors.New("production v2 deployment fence is not installed")
	}
	cache, err := internalgithub.LoadReadCache(filepath.Join(stateRoot, "github-etag-cache.json"))
	if errors.Is(err, internalgithub.ErrReadCacheCorrupt) {
		_, _ = fmt.Fprintln(log, "GitHub ETag cache was corrupt; rebuilding from fresh reads: "+internalgithub.Redact(err.Error()))
	} else if err != nil {
		return nil, fmt.Errorf("load GitHub cache: %w", err)
	}
	api.Cache = cache
	attemptRoot := productionAttemptRoot(stateRoot)
	if err := os.MkdirAll(attemptRoot, 0o700); err != nil {
		return nil, fmt.Errorf("prepare attempt root: %w", err)
	}
	if err := prepareProductionMarkerDirectories(stateRoot); err != nil {
		return nil, err
	}
	workerProfileDigest, err := config.PinWorkerExecutable(parent, stateRoot, &cfg.Commands)
	if err != nil {
		return nil, err
	}
	initial, _, err := loadOrMigrateRuntimeOwnerState(stateRoot, legacyRecoveryPath, cfg.Repository)
	if err != nil {
		return nil, err
	}
	initial.WorkerProfileDigest = workerProfileDigest
	// A signal requests graceful shutdown; it must not pre-close the owner or
	// cancel persistence before accepted operator work has drained.
	lifecycle, cancel := context.WithCancel(context.WithoutCancel(parent))
	runtime := &productionRuntimeV2{cancel: cancel}
	fail := func(err error) (*productionRuntimeV2, error) {
		cancel() // Startup did not reach the graceful serve drain.
		_ = runtime.shutdown(context.Background())
		return nil, err
	}
	owner, err := startStateOwner(lifecycle, stateRoot, attemptRoot, initial, func(ctx context.Context, state runtimeOwnerState) error {
		return writeRuntimeOwnerStateContext(ctx, stateRoot, attemptRoot, state)
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

	source, err := seedImmutableAttemptSource(parent, checkout, cfg.Repository, attemptRoot, "", "")
	if err != nil {
		return fail(err)
	}
	binary, err := os.Executable()
	if err != nil {
		return fail(err)
	}
	implementation := implementationBoundary(stateRoot)
	reviewer := reviewBoundary(stateRoot)
	for _, boundary := range []*workerBoundaryRunner{&implementation, &reviewer} {
		boundary.Env = append(boundary.Env, "AGENT_SYMPHONY_CODEX_EXECUTABLE="+cfg.Commands.Implementation[0], "AGENT_SYMPHONY_WORKER_PROFILE_DIGEST="+workerProfileDigest)
	}
	var preflight successfulCheck
	runtimeState := &agentruntime.Runtime{
		Root: attemptRoot, StateRoot: stateRoot, Source: source, Git: "git", Tmux: "tmux", Helper: binary,
		Runner: implementation, AllowEnv: cfg.Commands.Environment, WorkerHome: workerCodexHome(stateRoot),
		WorkerProfileDigest: workerProfileDigest,
		VerifyWorker: func(ctx context.Context) error {
			if err := config.VerifyWorkerExecutable(ctx, cfg.Commands.Implementation[0], workerProfileDigest); err != nil {
				return err
			}
			return preflight.Do(func() error {
				if _, err := implementation.call(ctx, "verify", agentruntime.Command{}); err != nil {
					return err
				}
				_, err := verifyRootlessCodex(ctx, attemptRoot, workerCodexHome(stateRoot), cfg.Commands.Implementation[0])
				return err
			})
		},
	}
	effects, err := newRuntimeEffectCoordinator(lifecycle, owner, agentruntime.EffectExecutor{Runtime: runtimeState})
	if err != nil {
		return fail(err)
	}
	runtime.effects = effects
	prConfig := githubPRConfig(cfg, user.ID)
	collector := reconciliationV2Collector{API: api, Config: prConfig, Scope: reconciliationScope{Kind: reconciliationRepositoryScope, Repository: cfg.Repository}}
	reviewEnvironment, err := configuredWorkerEnvironment(cfg.Commands.Environment, stateRoot)
	if err != nil {
		return fail(err)
	}
	cleanup := operatorCleanupExecutor{stateRoot: stateRoot, implementation: implementation, reviewer: reviewer, runtime: runtimeState}
	operator, err := newOperatorMutationService(lifecycle, owner, effects, cleanup, collector, reviewer, source, reviewEnvironment, cfg.Commands.Reviewer)
	if err != nil {
		return fail(err)
	}
	runtime.operator = operator
	operator.cacheLog = log
	cycle := &productionReconciliation{
		owner: owner, effects: effects, collector: collector, config: cfg, api: api,
		stateRoot: stateRoot, attemptRoot: attemptRoot, checkout: checkout,
		implementation: implementation, reviewer: reviewer, operator: operator, reviewEnv: reviewEnvironment,
		supervisor: agent, capacity: cfg.Concurrency, log: log,
		workerProfileDigest: workerProfileDigest,
	}
	runtime.cycle = cycle
	status, err := startOwnerStatusReplica(lifecycle, owner, cfg.Concurrency, log)
	if err != nil {
		return fail(err)
	}
	runtime.status = status
	if err := cycle.sweepPendingMarkers(parent); err != nil {
		return fail(err)
	}
	if err := operator.resumePending(parent); err != nil {
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
	if err := parent.Err(); err != nil {
		return fail(err)
	}
	return runtime, nil
}

type successfulCheck struct {
	mu   sync.Mutex
	done bool
}

func (c *successfulCheck) Do(check func() error) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.done {
		return nil
	}
	if err := check(); err != nil {
		return err
	}
	c.done = true
	return nil
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
	var proposalDone, triggerDone <-chan error
	if r.proposal != nil {
		done := make(chan error, 1)
		proposalDone = done
		go func() { done <- r.proposal.shutdown(ctx) }()
	}
	if r.trigger != nil {
		done := make(chan error, 1)
		triggerDone = done
		go func() { done <- r.trigger.shutdown(ctx) }()
	}
	var result error
	if r.operator != nil {
		result = errors.Join(result, r.operator.shutdown(ctx))
	}
	if ctx.Err() != nil && r.cancel != nil {
		r.cancel() // Timed-out graceful drain becomes a forced stop.
	}
	if r.effects != nil {
		result = errors.Join(result, r.effects.shutdown(ctx))
	}
	if proposalDone != nil {
		result = errors.Join(result, <-proposalDone)
	}
	if triggerDone != nil {
		result = errors.Join(result, <-triggerDone)
	}
	if r.cancel != nil {
		r.cancel()
	}
	if r.status != nil {
		result = errors.Join(result, r.status.wait(ctx))
	}
	if r.agent != nil {
		result = errors.Join(result, r.agent.Shutdown(ctx))
	}
	if r.owner != nil {
		result = errors.Join(result, r.owner.close(ctx))
	}
	return result
}
