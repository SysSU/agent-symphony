package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

// runtimeEffectCoordinator is the dormant v2 bridge between durable owner
// intents and slow runtime I/O. Production activation is deferred to #277.
type runtimeEffectCoordinator struct {
	lifecycle context.Context
	owner     *stateOwner
	executor  agentruntime.EffectExecutor
	mu        sync.Mutex
	active    map[string]*activeRuntimeEffect
	stopped   bool
}

type activeRuntimeEffect struct {
	issueGeneration, attemptGeneration, observationGeneration uint64
	ctx                                                       context.Context
	cancel                                                    context.CancelFunc
	done                                                      chan struct{}
}

func newRuntimeEffectCoordinator(lifecycle context.Context, owner *stateOwner, executor agentruntime.EffectExecutor) (*runtimeEffectCoordinator, error) {
	if lifecycle == nil || owner == nil || executor.Runtime == nil {
		return nil, errors.New("runtime effect coordinator is incomplete")
	}
	return &runtimeEffectCoordinator{lifecycle: lifecycle, owner: owner, executor: executor, active: map[string]*activeRuntimeEffect{}}, nil
}

// begin commits the exact effect intent before any external work is admitted.
func (c *runtimeEffectCoordinator) begin(ctx context.Context, snapshot stateOwnerSnapshot, request agentruntime.EffectRequest) (agentruntime.EffectRequest, error) {
	if snapshot.CycleID != 0 || snapshot.State.Epoch == 0 || snapshot.State.Revision == 0 {
		return agentruntime.EffectRequest{}, errStaleStateResult
	}
	request, err := c.executor.BindRequest(request)
	if err != nil {
		return agentruntime.EffectRequest{}, err
	}
	if err := c.executor.ValidateRequest(request); err != nil {
		return agentruntime.EffectRequest{}, err
	}
	manifest := request.Manifest
	issueGeneration := snapshot.State.IssueGenerations[ownerIssueKey(manifest.Repository, manifest.Issue)]
	attemptGeneration := snapshot.State.AttemptGenerations[ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)]
	identity := stateResultIdentity{
		Epoch:             snapshot.State.Epoch,
		SourceRevision:    snapshot.State.Revision,
		IssueGeneration:   issueGeneration,
		AttemptGeneration: attemptGeneration,
	}
	digest, err := agentruntime.EffectRequestDigest(request)
	if err != nil {
		return agentruntime.EffectRequest{}, err
	}
	var review *agentruntime.ReviewTransition
	if request.Action == agentruntime.EffectReview {
		review = cloneReviewTransition(&request.Review)
	}
	_, effect, err := c.owner.beginRuntimeEffect(ctx, beginRuntimeEffectCommand{Identity: identity, Action: request.Action, Manifest: manifest, Reason: request.Reason, RequestDigest: digest, Review: review})
	if err != nil {
		return agentruntime.EffectRequest{}, err
	}
	request.Identity = effectRequestIdentity(*effect)
	if request.Action == agentruntime.EffectStop {
		c.cancelOlder(manifest, effect.AttemptGeneration)
	}
	return request, nil
}

// execute runs outside the owner, one effect at a time for an attempt. A
// successful result and intent completion are committed atomically afterward.
func (c *runtimeEffectCoordinator) execute(_ context.Context, request agentruntime.EffectRequest) (agentruntime.EffectResult, error) {
	run, err := c.acquire(c.lifecycle, request)
	if err != nil {
		return agentruntime.EffectResult{}, err
	}
	defer c.release(request, run)
	if err := c.owner.authorizeRuntimeEffect(c.lifecycle, authorizeRuntimeEffectCommand{Identity: ownerEffectIdentity(request.Identity), Action: request.Action}); err != nil {
		return agentruntime.EffectResult{}, err
	}
	if err := run.ctx.Err(); err != nil {
		return agentruntime.EffectResult{}, err
	}
	result, err := c.executor.Execute(run.ctx, request)
	if result.Disposition != agentruntime.EffectResultReady {
		return result, err
	}
	if _, finishErr := c.owner.finishRuntimeEffect(c.lifecycle, finishRuntimeEffectCommand{
		Identity: ownerEffectIdentity(result.Identity),
		Action:   result.Action,
		Manifest: result.Manifest,
	}); finishErr != nil {
		return result, errors.Join(err, finishErr)
	}
	return result, err
}

// verifyPending checks external state after restart and commits only a proven,
// generation-current outcome. Retry and ambiguous review/monitor work stay pending.
func (c *runtimeEffectCoordinator) verifyPending(ctx context.Context, snapshot stateOwnerSnapshot, effect runtimeEffectIntent, request agentruntime.EffectRequest) (agentruntime.EffectVerification, error) {
	if snapshot.CycleID != 0 || effect.State != "pending" {
		return agentruntime.EffectVerification{}, errStateConflict
	}
	action := agentruntime.EffectAction(effect.Action)
	if !validRuntimeEffectAction(action) {
		return agentruntime.EffectVerification{}, errStateConflict
	}
	request.Action = action
	request.Identity = effectRequestIdentity(effect)
	verification, err := c.executor.VerifyPending(ctx, request)
	if err != nil || verification.Disposition != agentruntime.EffectVerified || verification.Result == nil {
		return verification, err
	}
	result := verification.Result
	if _, err := c.owner.finishRuntimeEffect(ctx, finishRuntimeEffectCommand{
		Identity: ownerEffectIdentity(result.Identity),
		Action:   result.Action,
		Manifest: result.Manifest,
	}); err != nil {
		return agentruntime.EffectVerification{}, err
	}
	return verification, nil
}

func (c *runtimeEffectCoordinator) acquire(ctx context.Context, request agentruntime.EffectRequest) (*activeRuntimeEffect, error) {
	key := ownerAttemptKey(request.Manifest.Repository, request.Manifest.Issue, request.Manifest.Attempt)
	return c.acquireKey(ctx, key, request.Identity.IssueGeneration, request.Identity.AttemptGeneration, 0)
}

func (c *runtimeEffectCoordinator) acquireKey(ctx context.Context, key string, issueGeneration, attemptGeneration, observationGeneration uint64) (*activeRuntimeEffect, error) {
	for {
		c.mu.Lock()
		if c.stopped {
			c.mu.Unlock()
			return nil, errStateOwnerStopped
		}
		if err := c.lifecycle.Err(); err != nil {
			c.mu.Unlock()
			return nil, err
		}
		previous := c.active[key]
		if previous == nil {
			runCtx, cancel := context.WithCancel(c.lifecycle)
			run := &activeRuntimeEffect{issueGeneration: issueGeneration, attemptGeneration: attemptGeneration, observationGeneration: observationGeneration, ctx: runCtx, cancel: cancel, done: make(chan struct{})}
			c.active[key] = run
			c.mu.Unlock()
			return run, nil
		}
		c.mu.Unlock()
		select {
		case <-previous.done:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

func (c *runtimeEffectCoordinator) shutdown(ctx context.Context) error {
	if c == nil {
		return nil
	}
	c.mu.Lock()
	c.stopped = true
	done := make([]<-chan struct{}, 0, len(c.active))
	for _, run := range c.active {
		run.cancel()
		done = append(done, run.done)
	}
	c.mu.Unlock()
	for _, finished := range done {
		select {
		case <-finished:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (c *runtimeEffectCoordinator) release(request agentruntime.EffectRequest, run *activeRuntimeEffect) {
	key := ownerAttemptKey(request.Manifest.Repository, request.Manifest.Issue, request.Manifest.Attempt)
	c.releaseKey(key, run)
}

func (c *runtimeEffectCoordinator) releaseKey(key string, run *activeRuntimeEffect) {
	c.mu.Lock()
	if c.active[key] == run {
		delete(c.active, key)
		close(run.done)
	}
	c.mu.Unlock()
	run.cancel()
}

func (c *runtimeEffectCoordinator) cancelOlder(manifest agentruntime.Manifest, generation uint64) {
	key := ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	c.mu.Lock()
	if run := c.active[key]; run != nil && run.attemptGeneration < generation {
		run.cancel()
	}
	c.mu.Unlock()
}

func (c *runtimeEffectCoordinator) cancelInvalidated(snapshot stateOwnerSnapshot) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for key, run := range c.active {
		repository, rest, ok := strings.Cut(key, "#")
		if !ok {
			run.cancel()
			continue
		}
		issueText, attemptText, attemptScoped := strings.Cut(rest, "/")
		issue, err := strconv.Atoi(issueText)
		if err != nil || snapshot.State.IssueGenerations[ownerIssueKey(repository, issue)] != run.issueGeneration {
			run.cancel()
			continue
		}
		if run.observationGeneration != 0 {
			observation, ok := snapshot.State.Observations[ownerIssueKey(repository, issue)]
			if !ok || observation.Generation != run.observationGeneration {
				run.cancel()
				continue
			}
		}
		if attemptScoped {
			attempt, err := strconv.Atoi(attemptText)
			if err != nil || snapshot.State.AttemptGenerations[ownerAttemptKey(repository, issue, attempt)] != run.attemptGeneration {
				run.cancel()
			}
		}
	}
}

func effectRequestIdentity(effect runtimeEffectIntent) agentruntime.EffectIdentity {
	return agentruntime.EffectIdentity{
		Repository:        effect.Repository,
		Issue:             effect.Issue,
		Attempt:           effect.Attempt,
		Epoch:             effect.IntentEpoch,
		SourceRevision:    effect.IntentRevision,
		IssueGeneration:   effect.IssueGeneration,
		AttemptGeneration: effect.AttemptGeneration,
		EffectID:          effect.ID,
		RequestDigest:     effect.RequestDigest,
	}
}

func ownerEffectIdentity(identity agentruntime.EffectIdentity) stateResultIdentity {
	return stateResultIdentity{
		Epoch:             identity.Epoch,
		SourceRevision:    identity.SourceRevision,
		IssueGeneration:   identity.IssueGeneration,
		AttemptGeneration: identity.AttemptGeneration,
		EffectID:          identity.EffectID,
		RequestDigest:     identity.RequestDigest,
	}
}
