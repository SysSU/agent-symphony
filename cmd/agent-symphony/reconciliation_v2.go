package main

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"reflect"
	"slices"
	"strings"
	"sync"

	internalgithub "github.com/SysSU/agent-symphony/internal/github"
)

const (
	maxReconciliationIssueCount    = 1024
	maxReconciliationAttemptCount  = 4096
	maxReconciliationFactBytes     = 4 << 20
	maxReconciliationBodyBytes     = 64 << 10
	maxReconciliationStringBytes   = 4096
	maxReconciliationPathBytes     = 64 << 10
	maxReconciliationRelationCount = 1000
	maxReconciliationPathCount     = 10000
	maxReconciliationCheckCount    = 4096
	maxReconciliationProposalCount = 2048
)

type reconciliationScopeKind string

const (
	reconciliationRepositoryScope reconciliationScopeKind = "repository"
	reconciliationIssueScope      reconciliationScopeKind = "issue"
)

type reconciliationScope struct {
	Kind       reconciliationScopeKind `json:"kind"`
	Repository string                  `json:"repository"`
	Issue      int                     `json:"issue,omitempty"`
}

type reconciliationAttemptFact struct {
	Repository           string   `json:"repository"`
	Issue                int      `json:"issue"`
	Attempt              int      `json:"attempt"`
	PR                   int      `json:"pr,omitempty"`
	BaseSHA              string   `json:"base_sha,omitempty"`
	HeadSHA              string   `json:"head_sha,omitempty"`
	State                string   `json:"state,omitempty"`
	PublicationConfirmed bool     `json:"publication_confirmed,omitempty"`
	Diagnostic           string   `json:"diagnostic,omitempty"`
	Checks               []string `json:"checks"`
}

type reconciliationIssueFact struct {
	Repository            string                      `json:"repository"`
	Title                 string                      `json:"title,omitempty"`
	BodyDigest            string                      `json:"body_digest"`
	BaseSHA               string                      `json:"base_sha,omitempty"`
	BaseBranch            string                      `json:"base_branch,omitempty"`
	Issue                 int                         `json:"issue"`
	Attempt               int                         `json:"attempt,omitempty"`
	CurrentAttempt        int                         `json:"current_attempt,omitempty"`
	Priority              int                         `json:"priority,omitempty"`
	CreatedAtUnixNano     int64                       `json:"created_at_unix_nano,omitempty"`
	Dependencies          []int                       `json:"dependencies"`
	SatisfiedDependencies []int                       `json:"satisfied_dependencies"`
	Paths                 []string                    `json:"paths"`
	Blockers              []string                    `json:"blockers"`
	Eligible              bool                        `json:"eligible,omitempty"`
	Active                bool                        `json:"active,omitempty"`
	Completed             bool                        `json:"completed,omitempty"`
	Retry                 bool                        `json:"retry,omitempty"`
	Cancelled             bool                        `json:"cancelled,omitempty"`
	Closed                bool                        `json:"closed,omitempty"`
	DispatchAuthorized    bool                        `json:"dispatch_authorized,omitempty"`
	RecoveryAuthorized    bool                        `json:"recovery_authorized,omitempty"`
	RecoveryAttempt       int                         `json:"recovery_attempt,omitempty"`
	NeedsAttention        bool                        `json:"needs_attention,omitempty"`
	ActiveAttempt         *reconciliationAttemptFact  `json:"active_attempt,omitempty"`
	TerminalAttempts      []reconciliationAttemptFact `json:"terminal_attempts"`
}

type reconciliationAttemptObservation struct {
	Present               bool                      `json:"present"`
	Generation            uint64                    `json:"generation"`
	OwnerGeneration       uint64                    `json:"owner_generation"`
	SourceIssueGeneration uint64                    `json:"source_issue_generation"`
	ObservationEpoch      uint64                    `json:"observation_epoch"`
	LastCycleID           uint64                    `json:"last_cycle_id"`
	Fact                  reconciliationAttemptFact `json:"fact"`
}

type reconciliationObservation struct {
	Present          bool                                        `json:"present"`
	Generation       uint64                                      `json:"generation"`
	OwnerGeneration  uint64                                      `json:"owner_generation"`
	ObservationEpoch uint64                                      `json:"observation_epoch"`
	LastCycleID      uint64                                      `json:"last_cycle_id"`
	InputDigest      string                                      `json:"input_digest"`
	Fact             reconciliationIssueFact                     `json:"fact"`
	IssueUpdates     []reconciliationIssueUpdateProposal         `json:"issue_updates"`
	Attempts         map[string]reconciliationAttemptObservation `json:"attempts"`
}

type reconciliationIssueGroup struct {
	Fact         reconciliationIssueFact
	IssueUpdates []reconciliationIssueUpdateProposal
	Attempts     []reconciliationAttemptFact
}

type reconciliationIssueUpdateProposal struct {
	Repository            string                `json:"repository"`
	Issue                 int                   `json:"issue"`
	Kind                  githubIssueUpdateKind `json:"kind"`
	ControlSnapshotDigest string                `json:"control_snapshot_digest,omitempty"`
	AttributionAttempt    int                   `json:"attribution_attempt,omitempty"`
	Dependency            int                   `json:"dependency,omitempty"`
	PullRequest           int                   `json:"pull_request,omitempty"`
}

type reconciliationCollection struct {
	Identity           stateResultIdentity
	Scope              reconciliationScope
	Complete           bool
	IssueGenerations   map[string]uint64
	AttemptGenerations map[string]uint64
	Issues             []reconciliationIssueGroup
}

type reconciliationInput struct {
	Scope        reconciliationScope
	Complete     bool
	Issues       []internalgithub.RecoveryIssueFact
	IssueUpdates []reconciliationIssueUpdateProposal
	Attempts     []internalgithub.RecoveryAttemptFact
}

type reconciliationCollectFunc func(context.Context, stateOwnerSnapshot) (reconciliationInput, error)

type reconciliationRunner struct {
	owner   *stateOwner
	collect reconciliationCollectFunc
}

type reconciliationTriggerRunner struct {
	mu        sync.Mutex
	run       func(context.Context) error
	ctx       context.Context
	cancel    context.CancelFunc
	wake      chan uint64
	done      chan struct{}
	stopped   bool
	running   bool
	runs      uint64
	lastErr   error
	nextID    uint64
	queuedID  uint64
	runningID uint64
	waiters   map[uint64][]chan error
	beforeRun func()
}

type applyReconciliationCommand struct{ Collection reconciliationCollection }

func (r reconciliationRunner) run(ctx context.Context) (stateOwnerSnapshot, error) {
	if r.owner == nil || r.collect == nil {
		return stateOwnerSnapshot{}, errors.New("reconciliation runner is incomplete")
	}
	snapshot, err := r.owner.reconciliationSnapshot(ctx)
	if err != nil {
		return stateOwnerSnapshot{}, err
	}
	input, err := r.collect(ctx, stateOwnerSnapshot{State: cloneRuntimeOwnerState(snapshot.State), CycleID: snapshot.CycleID})
	if err != nil {
		return stateOwnerSnapshot{}, err
	}
	collection, err := collectionFromSnapshot(snapshot, input)
	if err != nil {
		return stateOwnerSnapshot{}, err
	}
	return r.owner.applyReconciliation(ctx, collection)
}

func newReconciliationTriggerRunner(ctx context.Context, runner reconciliationRunner) (*reconciliationTriggerRunner, error) {
	if ctx == nil || runner.owner == nil || runner.collect == nil {
		return nil, errors.New("reconciliation trigger runner is incomplete")
	}
	return newReconciliationTrigger(ctx, func(ctx context.Context) error {
		_, err := runner.run(ctx)
		return err
	})
}

func newProductionReconciliationTriggerRunner(ctx context.Context, cycle func(context.Context) error) (*reconciliationTriggerRunner, error) {
	if ctx == nil || cycle == nil {
		return nil, errors.New("production reconciliation trigger is incomplete")
	}
	return newReconciliationTrigger(ctx, cycle)
}

func newReconciliationTrigger(ctx context.Context, run func(context.Context) error) (*reconciliationTriggerRunner, error) {
	lifecycle, cancel := context.WithCancel(ctx)
	r := &reconciliationTriggerRunner{run: run, ctx: lifecycle, cancel: cancel, wake: make(chan uint64, 1), done: make(chan struct{}), waiters: map[uint64][]chan error{}}
	go r.loop()
	return r, nil
}

func (r *reconciliationTriggerRunner) triggerAndWait(ctx context.Context) error {
	wait := make(chan error, 1)
	if err := r.request(wait); err != nil {
		return err
	}
	select {
	case err := <-wait:
		return err
	case <-r.done:
		return errStateOwnerStopped
	case <-ctx.Done():
		r.removeWaiter(wait)
		return ctx.Err()
	}
}

func (r *reconciliationTriggerRunner) removeWaiter(wait chan error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, waits := range r.waiters {
		for index := range waits {
			if waits[index] == wait {
				r.waiters[id] = slices.Delete(waits, index, index+1)
				if len(r.waiters[id]) == 0 {
					delete(r.waiters, id)
				}
				return
			}
		}
	}
}

func (r *reconciliationTriggerRunner) trigger() error {
	return r.request(nil)
}

func (r *reconciliationTriggerRunner) request(wait chan error) error {
	if r == nil {
		return errors.New("reconciliation trigger runner is nil")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.stopped {
		return errStateOwnerStopped
	}
	id := r.queuedID
	if id == 0 {
		r.nextID++
		id, r.queuedID = r.nextID, r.nextID
		r.wake <- id
	}
	if wait != nil {
		r.waiters[id] = append(r.waiters[id], wait)
	}
	return nil
}

func (r *reconciliationTriggerRunner) shutdown(ctx context.Context) error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	if !r.stopped {
		r.stopped = true
		r.cancel()
	}
	r.mu.Unlock()
	select {
	case <-r.done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *reconciliationTriggerRunner) loop() {
	defer func() {
		r.mu.Lock()
		for _, waits := range r.waiters {
			for _, wait := range waits {
				wait <- errStateOwnerStopped
			}
		}
		r.waiters = nil
		r.mu.Unlock()
		close(r.done)
	}()
	for {
		select {
		case <-r.ctx.Done():
			return
		case id := <-r.wake:
			if r.beforeRun != nil {
				r.beforeRun()
			}
			r.mu.Lock()
			r.queuedID = 0
			r.runningID = id
			r.running = true
			r.mu.Unlock()
			err := r.run(r.ctx)
			recollect := errors.Is(err, errReconciliationRecollect)
			if recollect {
				err = nil
			}
			r.mu.Lock()
			r.runs++
			r.lastErr = err
			if recollect && r.ctx.Err() == nil {
				target := r.queuedID
				if target == 0 {
					r.nextID++
					target, r.queuedID = r.nextID, r.nextID
					r.wake <- target
				}
				r.waiters[target] = append(r.waiters[target], r.waiters[id]...)
			} else {
				for _, wait := range r.waiters[id] {
					wait <- err
				}
			}
			delete(r.waiters, id)
			r.runningID = 0
			r.running = false
			r.mu.Unlock()
			if r.ctx.Err() != nil {
				return
			}
		}
	}
}

func (o *stateOwner) applyReconciliation(ctx context.Context, collection reconciliationCollection) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerApplyReconciliation, reconcile: applyReconciliationCommand{Collection: cloneReconciliationCollection(collection)}})
	return result.snapshot, err
}

func (o *stateOwner) recordCycleOutcome(ctx context.Context, command recordCycleOutcomeCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerRecordCycleOutcome, cycleOutcome: command})
	return result.snapshot, err
}

func collectionFromSnapshot(snapshot stateOwnerSnapshot, input reconciliationInput) (reconciliationCollection, error) {
	if len(input.Issues) > maxReconciliationIssueCount || len(input.IssueUpdates) > maxReconciliationProposalCount || len(input.Attempts) > maxReconciliationAttemptCount {
		return reconciliationCollection{}, errors.New("reconciliation observation exceeds durable bounds")
	}
	collection := reconciliationCollection{
		Identity: stateResultIdentity{Epoch: snapshot.State.Epoch, SourceRevision: snapshot.State.Revision, CycleID: snapshot.CycleID},
		Scope:    input.Scope, Complete: input.Complete,
	}
	groups := make(map[string]*reconciliationIssueGroup, len(input.Issues))
	for _, raw := range input.Issues {
		fact, err := reduceIssueFact(snapshot.State.Repository, raw)
		if err != nil {
			return reconciliationCollection{}, err
		}
		key := ownerIssueKey(fact.Repository, fact.Issue)
		if _, exists := groups[key]; exists {
			return reconciliationCollection{}, errStateConflict
		}
		groups[key] = &reconciliationIssueGroup{Fact: fact}
	}
	for _, raw := range input.Attempts {
		fact, err := reduceAttemptFact(snapshot.State.Repository, raw)
		if err != nil {
			return reconciliationCollection{}, err
		}
		group := groups[ownerIssueKey(fact.Repository, fact.Issue)]
		if group == nil {
			continue
		}
		group.Attempts = append(group.Attempts, fact)
	}
	for _, proposal := range input.IssueUpdates {
		group := groups[ownerIssueKey(proposal.Repository, proposal.Issue)]
		if group == nil || !validReconciliationIssueUpdateProposal(snapshot.State.Repository, proposal) {
			return reconciliationCollection{}, errStateConflict
		}
		group.IssueUpdates = append(group.IssueUpdates, proposal)
	}
	for _, group := range groups {
		collection.Issues = append(collection.Issues, *group)
	}
	normalizeReconciliationCollection(&collection)
	collection.IssueGenerations, collection.AttemptGenerations = captureReconciliationGenerations(snapshot.State, collection.Scope, collection.Complete, collection.Issues)
	if err := validateReconciliationCollection(snapshot.State.Repository, collection); err != nil {
		return reconciliationCollection{}, err
	}
	return cloneReconciliationCollection(collection), nil
}

func applyReconciliation(state *runtimeOwnerState, command applyReconciliationCommand, appliedCycles map[string]appliedReconciliationCycle) error {
	collection := cloneReconciliationCollection(command.Collection)
	normalizeReconciliationCollection(&collection)
	if err := validateReconciliationCollection(state.Repository, collection); err != nil {
		return err
	}
	identity := collection.Identity
	if identity.Epoch != state.Epoch || identity.SourceRevision == 0 || identity.SourceRevision > state.Revision {
		return errStaleStateResult
	}
	seen := make(map[string]bool, len(collection.Issues))
	for _, group := range collection.Issues {
		key := ownerIssueKey(group.Fact.Repository, group.Fact.Issue)
		seen[key] = true
		if !reconciliationIssueGenerationMatches(*state, key, collection.IssueGenerations[key]) {
			if err := countStaleReconciliation(state); err != nil {
				return err
			}
			continue
		}
		group = prepareReconciliationIssueGroup(*state, collection, group)
		digest := reconciliationInputDigest(collection.Scope, collection.Complete, key, &group)
		if applied, ok := appliedCycles[key]; ok && identity.CycleID <= applied.cycle {
			if identity.CycleID == applied.cycle && digest != applied.digest {
				return errStateConflict
			}
			if identity.CycleID < applied.cycle {
				if err := countStaleReconciliation(state); err != nil {
					return err
				}
			}
			continue
		}
		if err := applyReconciliationIssue(state, collection, group); err != nil {
			return err
		}
	}
	if !collection.Complete {
		return nil
	}
	keys := make([]string, 0, len(state.Observations)+1)
	addKey := func(key string) {
		if reconciliationScopeContains(collection.Scope, key) && !seen[key] && !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	for key := range state.Observations {
		addKey(key)
	}
	for _, record := range state.Attempts {
		addKey(ownerIssueKey(record.Manifest.Repository, record.Manifest.Issue))
	}
	if collection.Scope.Kind == reconciliationIssueScope {
		key := ownerIssueKey(collection.Scope.Repository, collection.Scope.Issue)
		if !seen[key] && !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	for _, key := range keys {
		if !reconciliationIssueGenerationMatches(*state, key, collection.IssueGenerations[key]) {
			if err := countStaleReconciliation(state); err != nil {
				return err
			}
			continue
		}
		digest := reconciliationInputDigest(collection.Scope, collection.Complete, key, nil)
		if applied, ok := appliedCycles[key]; ok && identity.CycleID <= applied.cycle {
			if identity.CycleID == applied.cycle && digest != applied.digest {
				return errStateConflict
			}
			if identity.CycleID < applied.cycle {
				if err := countStaleReconciliation(state); err != nil {
					return err
				}
			}
			continue
		}
		previous, exists := state.Observations[key]
		if exists && previous.ObservationEpoch == identity.Epoch && identity.CycleID <= previous.LastCycleID {
			if identity.CycleID == previous.LastCycleID && previous.InputDigest != digest {
				return errStateConflict
			}
			if identity.CycleID < previous.LastCycleID {
				if err := countStaleReconciliation(state); err != nil {
					return err
				}
			}
			continue
		}
		issue := issueFromOwnerKey(key)
		if issue < 1 {
			return errStateConflict
		}
		generation := uint64(1)
		if exists {
			generation = previous.Generation
			if previous.Present {
				if previous.Generation == ^uint64(0) {
					return errors.New("issue observation generation overflow")
				}
				generation++
			}
		}
		next := reconciliationObservation{Generation: generation, OwnerGeneration: state.IssueGenerations[key], ObservationEpoch: identity.Epoch, LastCycleID: identity.CycleID, InputDigest: digest, Attempts: map[string]reconciliationAttemptObservation{}}
		if exists && reconciliationObservationContentEqual(previous, next) {
			continue
		}
		state.Observations[key] = next
	}
	return nil
}

func countStaleReconciliation(state *runtimeOwnerState) error {
	if state.StaleReconciliations == ^uint64(0) {
		return errors.New("stale reconciliation count overflow")
	}
	state.StaleReconciliations++
	return nil
}

func recordAppliedReconciliationCycles(applied map[string]appliedReconciliationCycle, command stateOwnerCommand, state runtimeOwnerState) {
	if command.kind != stateOwnerApplyReconciliation {
		return
	}
	collection := command.reconcile.Collection
	seen := make(map[string]bool, len(collection.Issues))
	for _, group := range collection.Issues {
		key := ownerIssueKey(group.Fact.Repository, group.Fact.Issue)
		seen[key] = true
		if !reconciliationIssueGenerationMatches(state, key, collection.IssueGenerations[key]) {
			continue
		}
		group = prepareReconciliationIssueGroup(state, collection, group)
		recordAppliedReconciliationCycle(applied, key, collection.Identity.CycleID, reconciliationInputDigest(collection.Scope, collection.Complete, key, &group))
	}
	if !collection.Complete {
		return
	}
	keys := make([]string, 0, len(state.Observations)+1)
	addKey := func(key string) {
		if reconciliationScopeContains(collection.Scope, key) && !seen[key] && !slices.Contains(keys, key) {
			keys = append(keys, key)
		}
	}
	for key := range state.Observations {
		addKey(key)
	}
	for _, record := range state.Attempts {
		addKey(ownerIssueKey(record.Manifest.Repository, record.Manifest.Issue))
	}
	if collection.Scope.Kind == reconciliationIssueScope {
		addKey(ownerIssueKey(collection.Scope.Repository, collection.Scope.Issue))
	}
	for _, key := range keys {
		if reconciliationIssueGenerationMatches(state, key, collection.IssueGenerations[key]) {
			recordAppliedReconciliationCycle(applied, key, collection.Identity.CycleID, reconciliationInputDigest(collection.Scope, collection.Complete, key, nil))
		}
	}
}

func recordAppliedReconciliationCycle(applied map[string]appliedReconciliationCycle, key string, cycle uint64, digest string) {
	if current, ok := applied[key]; !ok || cycle > current.cycle {
		applied[key] = appliedReconciliationCycle{cycle: cycle, digest: digest}
	}
}

func reconciliationIssueGenerationMatches(state runtimeOwnerState, key string, captured uint64) bool {
	current := state.IssueGenerations[key]
	if current == captured {
		return true
	}
	observation, observed := state.Observations[key]
	return captured == 0 && current == 1 && observed && observation.OwnerGeneration == current
}

func applyReconciliationIssue(state *runtimeOwnerState, collection reconciliationCollection, group reconciliationIssueGroup) error {
	identity, key := collection.Identity, ownerIssueKey(group.Fact.Repository, group.Fact.Issue)
	group = prepareReconciliationIssueGroup(*state, collection, group)
	digest := reconciliationInputDigest(collection.Scope, collection.Complete, key, &group)
	previous, exists := state.Observations[key]
	if exists && previous.ObservationEpoch == identity.Epoch && identity.CycleID <= previous.LastCycleID {
		if identity.CycleID == previous.LastCycleID && previous.InputDigest != digest {
			return errStateConflict
		}
		return nil
	}
	ownerIssueGeneration := state.IssueGenerations[key]
	if ownerIssueGeneration == 0 {
		ownerIssueGeneration = 1
		state.IssueGenerations[key] = ownerIssueGeneration
		deleteIssueScopedEffects(state, group.Fact.Repository, group.Fact.Issue)
	}
	issueGeneration := uint64(1)
	attempts := map[string]reconciliationAttemptObservation{}
	issueChanged := exists && (!previous.Present || !reflect.DeepEqual(previous.Fact, group.Fact) || !reflect.DeepEqual(previous.IssueUpdates, group.IssueUpdates))
	if exists {
		issueGeneration = previous.Generation
	}
	if issueChanged {
		if issueGeneration == ^uint64(0) {
			return errors.New("issue observation generation overflow")
		}
		issueGeneration++
	} else if exists {
		attempts = cloneAttemptObservations(previous.Attempts)
	}
	seenAttempts := make(map[string]bool, len(group.Attempts))
	for _, fact := range group.Attempts {
		attemptKey := ownerAttemptKey(fact.Repository, fact.Issue, fact.Attempt)
		if seenAttempts[attemptKey] {
			return errStateConflict
		}
		seenAttempts[attemptKey] = true
		if _, tombstoned := state.Tombstones[attemptKey]; tombstoned || state.AttemptGenerations[attemptKey] != collection.AttemptGenerations[attemptKey] {
			continue
		}
		ownerGeneration := state.AttemptGenerations[attemptKey]
		generation := uint64(1)
		old, oldExists := attempts[attemptKey]
		if oldExists {
			generation = old.Generation
		}
		if oldExists && (!old.Present || !reflect.DeepEqual(old.Fact, fact)) {
			if generation == ^uint64(0) {
				return errors.New("attempt observation generation overflow")
			}
			generation++
		}
		attempts[attemptKey] = reconciliationAttemptObservation{Present: true, Generation: generation, OwnerGeneration: ownerGeneration, SourceIssueGeneration: issueGeneration, ObservationEpoch: identity.Epoch, LastCycleID: identity.CycleID, Fact: cloneReconciliationAttemptFact(fact)}
	}
	if collection.Complete {
		for attemptKey, old := range attempts {
			if seenAttempts[attemptKey] || old.SourceIssueGeneration != issueGeneration || old.ObservationEpoch == identity.Epoch && identity.CycleID <= old.LastCycleID || state.AttemptGenerations[attemptKey] != collection.AttemptGenerations[attemptKey] {
				continue
			}
			generation := old.Generation
			if old.Present {
				if generation == ^uint64(0) {
					return errors.New("attempt observation generation overflow")
				}
				generation++
			}
			attempts[attemptKey] = reconciliationAttemptObservation{Generation: generation, OwnerGeneration: state.AttemptGenerations[attemptKey], SourceIssueGeneration: issueGeneration, ObservationEpoch: identity.Epoch, LastCycleID: identity.CycleID}
		}
	}
	next := reconciliationObservation{Present: true, Generation: issueGeneration, OwnerGeneration: ownerIssueGeneration, ObservationEpoch: identity.Epoch, LastCycleID: identity.CycleID, InputDigest: digest, Fact: cloneReconciliationIssueFact(group.Fact), IssueUpdates: slices.Clone(group.IssueUpdates), Attempts: attempts}
	if exists && previous.ObservationEpoch == identity.Epoch && reconciliationObservationContentEqual(previous, next) {
		return nil
	}
	state.Observations[key] = next
	for _, fact := range group.Attempts {
		attemptKey := ownerAttemptKey(fact.Repository, fact.Issue, fact.Attempt)
		observation, accepted := attempts[attemptKey]
		record, owned := state.Attempts[attemptKey]
		if !accepted || !observation.Present || !owned || (fact.State != "active" && fact.State != "review-ready") || !fact.PublicationConfirmed || fact.PR < 1 || fact.HeadSHA == "" || fact.BaseSHA != record.Manifest.BaseSHA {
			continue
		}
		recovery := state.Recoveries[attemptKey]
		pr := clonePRState(recovery.State)
		if err := internalgithub.HydrateRecoveryState(&pr, internalgithub.RecoveryAttemptFact{Repository: fact.Repository, Issue: fact.Issue, Attempt: fact.Attempt, PR: fact.PR, BaseSHA: fact.BaseSHA, HeadSHA: fact.HeadSHA, State: fact.State, PublicationConfirmed: fact.PublicationConfirmed}); err != nil {
			return errStateConflict
		}
		state.Recoveries[attemptKey] = runtimePRRecovery{IssueGeneration: ownerIssueGeneration, AttemptGeneration: observation.OwnerGeneration, State: pr}
	}
	return nil
}

func prepareReconciliationIssueGroup(state runtimeOwnerState, collection reconciliationCollection, group reconciliationIssueGroup) reconciliationIssueGroup {
	group.Fact = maskStaleIssueAttemptFacts(state, collection, group.Fact)
	group.IssueUpdates = slices.DeleteFunc(slices.Clone(group.IssueUpdates), func(proposal reconciliationIssueUpdateProposal) bool {
		if proposal.Kind != githubIssueDependencyClear {
			return false
		}
		attemptKey := ownerAttemptKey(proposal.Repository, proposal.Issue, proposal.AttributionAttempt)
		captured, capturedOK := collection.AttemptGenerations[attemptKey]
		current, ownerKnown := state.AttemptGenerations[attemptKey]
		_, tombstoned := state.Tombstones[attemptKey]
		return tombstoned || capturedOK && captured != current || ownerKnown && !capturedOK
	})
	return group
}

func reconciliationObservationContentEqual(left, right reconciliationObservation) bool {
	left.Attempts = cloneAttemptObservations(left.Attempts)
	right.Attempts = cloneAttemptObservations(right.Attempts)
	left.ObservationEpoch, left.LastCycleID, left.InputDigest = 0, 0, ""
	right.ObservationEpoch, right.LastCycleID, right.InputDigest = 0, 0, ""
	for key, attempt := range left.Attempts {
		attempt.ObservationEpoch, attempt.LastCycleID = 0, 0
		left.Attempts[key] = attempt
	}
	for key, attempt := range right.Attempts {
		attempt.ObservationEpoch, attempt.LastCycleID = 0, 0
		right.Attempts[key] = attempt
	}
	return reflect.DeepEqual(left, right)
}

func deleteAttemptObservation(state *runtimeOwnerState, repository string, issue, attempt int) error {
	key := ownerIssueKey(repository, issue)
	observation, exists := state.Observations[key]
	if !exists {
		return nil
	}
	delete(observation.Attempts, ownerAttemptKey(repository, issue, attempt))
	if observation.Fact.ActiveAttempt != nil && observation.Fact.ActiveAttempt.Attempt == attempt {
		observation.Fact.ActiveAttempt = nil
	}
	observation.Fact.TerminalAttempts = slices.DeleteFunc(observation.Fact.TerminalAttempts, func(fact reconciliationAttemptFact) bool { return fact.Attempt == attempt })
	updates := slices.DeleteFunc(slices.Clone(observation.IssueUpdates), func(proposal reconciliationIssueUpdateProposal) bool {
		return proposal.Kind == githubIssueDependencyClear && proposal.AttributionAttempt == attempt
	})
	if len(updates) != len(observation.IssueUpdates) {
		if observation.Generation == ^uint64(0) {
			return errors.New("issue observation generation overflow")
		}
		observation.Generation++
		observation.IssueUpdates = updates
		observation.Attempts = map[string]reconciliationAttemptObservation{}
		body, _ := json.Marshal(struct {
			Fact    reconciliationIssueFact
			Updates []reconciliationIssueUpdateProposal
		}{observation.Fact, updates})
		digest := sha256.Sum256(body)
		observation.InputDigest = hex.EncodeToString(digest[:])
		for id, effect := range state.Effects {
			request := effect.Reconciliation
			if effect.State == "pending" && request != nil && request.GitHubIssueUpdate != nil && request.Repository == repository && request.Issue == issue && request.GitHubIssueUpdate.Kind == githubIssueDependencyClear && request.GitHubIssueUpdate.AttributionAttempt == attempt {
				delete(state.Effects, id)
			}
		}
	}
	state.Observations[key] = observation
	return nil
}

func maskStaleIssueAttemptFacts(state runtimeOwnerState, collection reconciliationCollection, fact reconciliationIssueFact) reconciliationIssueFact {
	fact = cloneReconciliationIssueFact(fact)
	current := func(attempt reconciliationAttemptFact) bool {
		key := ownerAttemptKey(attempt.Repository, attempt.Issue, attempt.Attempt)
		captured, ok := collection.AttemptGenerations[key]
		_, tombstoned := state.Tombstones[key]
		return ok && !tombstoned && captured == state.AttemptGenerations[key]
	}
	if fact.ActiveAttempt != nil {
		if !current(*fact.ActiveAttempt) {
			fact.ActiveAttempt = nil
		}
	}
	fact.TerminalAttempts = slices.DeleteFunc(fact.TerminalAttempts, func(attempt reconciliationAttemptFact) bool {
		return !current(attempt)
	})
	return fact
}

func validateReconciliationCollection(repository string, collection reconciliationCollection) error {
	identity := collection.Identity
	if identity.Epoch == 0 || identity.SourceRevision == 0 || identity.CycleID == 0 || identity.EffectID != "" || identity.RequestDigest != "" || !validReconciliationScope(repository, collection.Scope) || len(collection.Issues) > maxReconciliationIssueCount || len(collection.IssueGenerations) > maxReconciliationIssueCount || len(collection.AttemptGenerations) > maxReconciliationAttemptCount || collection.IssueGenerations == nil || collection.AttemptGenerations == nil {
		return errStateConflict
	}
	seen, attempts, proposals := map[string]bool{}, 0, 0
	for _, group := range collection.Issues {
		key := ownerIssueKey(group.Fact.Repository, group.Fact.Issue)
		if seen[key] || !reconciliationScopeContains(collection.Scope, key) || validateReconciliationIssueFact(repository, group.Fact) != nil {
			return errStateConflict
		}
		seen[key] = true
		if !validIssueUpdateProposals(repository, group.Fact, group.Attempts, group.IssueUpdates) {
			return errStateConflict
		}
		proposals += len(group.IssueUpdates)
		attemptSeen := map[string]bool{}
		for _, fact := range group.Attempts {
			attemptKey := ownerAttemptKey(fact.Repository, fact.Issue, fact.Attempt)
			if attemptSeen[attemptKey] || fact.Issue != group.Fact.Issue || validateReconciliationAttemptFact(repository, fact) != nil {
				return errStateConflict
			}
			attemptSeen[attemptKey] = true
			attempts++
		}
	}
	if attempts > maxReconciliationAttemptCount || proposals > maxReconciliationProposalCount {
		return errors.New("reconciliation observation exceeds durable bounds")
	}
	body, err := json.Marshal(collection.Issues)
	if err != nil || len(body) > maxReconciliationFactBytes {
		return errors.New("reconciliation observation exceeds durable bounds")
	}
	return validateGenerationSnapshot(repository, collection.IssueGenerations, collection.AttemptGenerations)
}

func validIssueUpdateProposals(repository string, fact reconciliationIssueFact, attempts []reconciliationAttemptFact, proposals []reconciliationIssueUpdateProposal) bool {
	seen := map[githubIssueUpdateKind]bool{}
	for _, proposal := range proposals {
		if seen[proposal.Kind] || proposal.Repository != repository || proposal.Issue != fact.Issue || !validReconciliationIssueUpdateProposal(repository, proposal) {
			return false
		}
		if proposal.Kind == githubIssueDependencyClear {
			if proposal.AttributionAttempt != max(1, fact.CurrentAttempt) {
				return false
			}
			if !slices.Contains(fact.Dependencies, proposal.Dependency) || !slices.Contains(fact.SatisfiedDependencies, proposal.Dependency) {
				return false
			}
			boundPR, currentAttempt := 0, fact.CurrentAttempt
			if currentAttempt == 0 {
				currentAttempt = fact.Attempt
			}
			foundCurrent := false
			if fact.ActiveAttempt != nil && (fact.ActiveAttempt.Attempt == fact.Attempt || fact.ActiveAttempt.Attempt == fact.CurrentAttempt) {
				boundPR, foundCurrent = fact.ActiveAttempt.PR, true
			}
			for _, attempt := range fact.TerminalAttempts {
				if attempt.Attempt == currentAttempt {
					foundCurrent = true
					if attempt.PR > 0 {
						boundPR = attempt.PR
					}
				}
			}
			for _, attempt := range attempts {
				if attempt.Attempt == currentAttempt {
					foundCurrent = true
					if attempt.PR > 0 {
						if boundPR > 0 && boundPR != attempt.PR {
							return false
						}
						boundPR = attempt.PR
					}
				}
			}
			if foundCurrent && proposal.PullRequest != boundPR {
				return false
			}
		}
		seen[proposal.Kind] = true
	}
	return len(proposals) <= 2
}

func validReconciliationIssueUpdateProposal(repository string, proposal reconciliationIssueUpdateProposal) bool {
	if proposal.Repository != repository || proposal.Issue < 1 {
		return false
	}
	switch proposal.Kind {
	case githubIssueControlSnapshot:
		return validDigest(proposal.ControlSnapshotDigest) && proposal.AttributionAttempt == 0 && proposal.Dependency == 0 && proposal.PullRequest == 0
	case githubIssueDependencyClear:
		return proposal.ControlSnapshotDigest == "" && proposal.AttributionAttempt > 0 && proposal.Dependency > 0 && proposal.PullRequest >= 0
	default:
		return false
	}
}

func validateReconciliationObservations(state runtimeOwnerState) error {
	if len(state.Observations) > maxReconciliationIssueCount {
		return errors.New("runtime owner observations exceed durable bounds")
	}
	body, err := json.Marshal(state.Observations)
	if err != nil || len(body) > maxReconciliationFactBytes {
		return errors.New("runtime owner observations exceed durable bounds")
	}
	attemptCount := 0
	for key, observation := range state.Observations {
		if key != ownerIssueKey(state.Repository, issueFromOwnerKey(key)) || observation.Generation == 0 || state.IssueGenerations[key] != observation.OwnerGeneration || observation.ObservationEpoch == 0 || observation.ObservationEpoch > state.Epoch || observation.LastCycleID == 0 || !validDigest(observation.InputDigest) || observation.Attempts == nil {
			return errors.New("runtime owner issue observation is invalid")
		}
		if !observation.Present {
			if !reflect.DeepEqual(observation.Fact, reconciliationIssueFact{}) || len(observation.IssueUpdates) != 0 || len(observation.Attempts) != 0 {
				return errors.New("runtime owner absent issue observation is invalid")
			}
			continue
		}
		if validateReconciliationIssueFact(state.Repository, observation.Fact) != nil || key != ownerIssueKey(observation.Fact.Repository, observation.Fact.Issue) {
			return errors.New("runtime owner issue observation is invalid")
		}
		attemptFacts := make([]reconciliationAttemptFact, 0, len(observation.Attempts))
		for _, attempt := range observation.Attempts {
			if attempt.Present && attempt.SourceIssueGeneration == observation.Generation {
				attemptFacts = append(attemptFacts, attempt.Fact)
			}
		}
		if !validIssueUpdateProposals(state.Repository, observation.Fact, attemptFacts, observation.IssueUpdates) {
			return errors.New("runtime owner issue update proposals are invalid")
		}
		if observation.Fact.ActiveAttempt != nil {
			if _, tombstoned := state.Tombstones[ownerAttemptKey(state.Repository, observation.Fact.Issue, observation.Fact.ActiveAttempt.Attempt)]; tombstoned {
				return errors.New("runtime owner issue observation conflicts with tombstone")
			}
		}
		for _, terminal := range observation.Fact.TerminalAttempts {
			if _, tombstoned := state.Tombstones[ownerAttemptKey(state.Repository, observation.Fact.Issue, terminal.Attempt)]; tombstoned {
				return errors.New("runtime owner issue observation conflicts with tombstone")
			}
		}
		for attemptKey, attempt := range observation.Attempts {
			attemptCount++
			repository, issue, number, ok := parseOwnerAttemptKey(attemptKey)
			if !ok || repository != state.Repository || issue != observation.Fact.Issue || number < 1 || attempt.Generation == 0 || state.AttemptGenerations[attemptKey] != attempt.OwnerGeneration || attempt.SourceIssueGeneration != observation.Generation || attempt.ObservationEpoch == 0 || attempt.ObservationEpoch > state.Epoch || attempt.LastCycleID == 0 {
				return errors.New("runtime owner attempt observation is invalid")
			}
			if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
				return errors.New("runtime owner attempt observation conflicts with tombstone")
			}
			if attempt.Present && validateReconciliationAttemptFact(state.Repository, attempt.Fact) != nil || !attempt.Present && !reflect.DeepEqual(attempt.Fact, reconciliationAttemptFact{}) {
				return errors.New("runtime owner attempt observation is invalid")
			}
		}
	}
	if attemptCount > maxReconciliationAttemptCount {
		return errors.New("runtime owner observations exceed durable bounds")
	}
	return nil
}

func validReconciliationScope(repository string, scope reconciliationScope) bool {
	if scope.Repository != repository {
		return false
	}
	return scope.Kind == reconciliationRepositoryScope && scope.Issue == 0 || scope.Kind == reconciliationIssueScope && scope.Issue > 0
}

func reconciliationScopeContains(scope reconciliationScope, issueKey string) bool {
	issue := issueFromOwnerKey(issueKey)
	return issue > 0 && (scope.Kind == reconciliationRepositoryScope || scope.Kind == reconciliationIssueScope && issue == scope.Issue)
}

func validateGenerationSnapshot(repository string, issues, attempts map[string]uint64) error {
	for key, generation := range issues {
		if generation == 0 || key != ownerIssueKey(repository, issueFromOwnerKey(key)) {
			return errStateConflict
		}
	}
	for key, generation := range attempts {
		repo, issue, attempt, ok := parseOwnerAttemptKey(key)
		if !ok || repo != repository || issue < 1 || attempt < 1 || generation == 0 {
			return errStateConflict
		}
	}
	return nil
}

func reduceIssueFact(repository string, raw internalgithub.RecoveryIssueFact) (reconciliationIssueFact, error) {
	if len(raw.Body) > maxReconciliationBodyBytes || strings.ContainsRune(raw.Body, 0) {
		return reconciliationIssueFact{}, errors.New("reconciliation issue body exceeds durable bounds")
	}
	digest := sha256.Sum256([]byte(raw.Body))
	fact := reconciliationIssueFact{
		Repository: raw.Repository, Title: raw.Title, BodyDigest: hex.EncodeToString(digest[:]), BaseSHA: raw.BaseSHA, BaseBranch: raw.BaseBranch,
		Issue: raw.Issue, Attempt: raw.Attempt, CurrentAttempt: raw.CurrentAttempt, Priority: raw.Priority, CreatedAtUnixNano: raw.CreatedAt.UnixNano(),
		Dependencies: slices.Clone(raw.Dependencies), SatisfiedDependencies: slices.Clone(raw.SatisfiedDependencies), Paths: slices.Clone(raw.Paths), Blockers: slices.Clone(raw.Blockers),
		Eligible: raw.Eligible, Active: raw.Active, Completed: raw.Completed, Retry: raw.Retry, Cancelled: raw.Cancelled, Closed: raw.Closed,
		DispatchAuthorized: raw.DispatchAuthorized, RecoveryAuthorized: raw.RecoveryAuthorized, RecoveryAttempt: raw.RecoveryAttempt, NeedsAttention: raw.NeedsAttention,
	}
	if raw.CreatedAt.IsZero() {
		fact.CreatedAtUnixNano = 0
	}
	if raw.ActiveAttempt != nil {
		active, err := reduceAttemptFact(repository, *raw.ActiveAttempt)
		if err != nil {
			return reconciliationIssueFact{}, err
		}
		fact.ActiveAttempt = &active
	}
	for _, rawAttempt := range raw.TerminalAttempts {
		attempt, err := reduceAttemptFact(repository, rawAttempt)
		if err != nil {
			return reconciliationIssueFact{}, err
		}
		fact.TerminalAttempts = append(fact.TerminalAttempts, attempt)
	}
	if err := validateReconciliationIssueFact(repository, fact); err != nil {
		return reconciliationIssueFact{}, err
	}
	return fact, nil
}

func reduceAttemptFact(repository string, raw internalgithub.RecoveryAttemptFact) (reconciliationAttemptFact, error) {
	fact := reconciliationAttemptFact{Repository: raw.Repository, Issue: raw.Issue, Attempt: raw.Attempt, PR: raw.PR, BaseSHA: raw.BaseSHA, HeadSHA: raw.HeadSHA, State: raw.State, PublicationConfirmed: raw.PublicationConfirmed, Diagnostic: raw.Diagnostic, Checks: slices.Clone(raw.Checks)}
	if err := validateReconciliationAttemptFact(repository, fact); err != nil {
		return reconciliationAttemptFact{}, err
	}
	return fact, nil
}

func validateReconciliationIssueFact(repository string, fact reconciliationIssueFact) error {
	if fact.Repository != repository || fact.Issue < 1 || fact.Attempt < 0 || fact.CurrentAttempt < 0 || fact.RecoveryAttempt < 0 || len(fact.Title) > maxReconciliationStringBytes || len(fact.BaseBranch) > maxReconciliationStringBytes || strings.ContainsRune(fact.Title, 0) || strings.ContainsRune(fact.BaseBranch, 0) || !validDigest(fact.BodyDigest) || !validOptionalObjectID(fact.BaseSHA) || !validIntList(fact.Dependencies) || !validIntList(fact.SatisfiedDependencies) || !validStringList(fact.Paths, maxReconciliationPathCount, maxReconciliationPathBytes) || !validStringList(fact.Blockers, maxReconciliationRelationCount, maxReconciliationStringBytes) || len(fact.TerminalAttempts) > maxReconciliationRelationCount {
		return errStateConflict
	}
	if fact.ActiveAttempt != nil && (fact.ActiveAttempt.Issue != fact.Issue || validateReconciliationAttemptFact(repository, *fact.ActiveAttempt) != nil) {
		return errStateConflict
	}
	for _, attempt := range fact.TerminalAttempts {
		if attempt.Issue != fact.Issue || validateReconciliationAttemptFact(repository, attempt) != nil {
			return errStateConflict
		}
	}
	return nil
}

func validateReconciliationAttemptFact(repository string, fact reconciliationAttemptFact) error {
	if fact.Repository != repository || fact.Issue < 1 || fact.Attempt < 1 || fact.PR < 0 || !validOptionalObjectID(fact.BaseSHA) || !validOptionalObjectID(fact.HeadSHA) || len(fact.State) > 64 || len(fact.Diagnostic) > maxReconciliationStringBytes || strings.ContainsRune(fact.State, 0) || strings.ContainsRune(fact.Diagnostic, 0) || !validStringList(fact.Checks, maxReconciliationCheckCount, maxReconciliationStringBytes) {
		return errStateConflict
	}
	return nil
}

func validOptionalObjectID(value string) bool {
	return value == "" || preflightObjectID.MatchString(value)
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size
}

func validIntList(values []int) bool {
	if len(values) > maxReconciliationRelationCount {
		return false
	}
	for _, value := range values {
		if value < 1 {
			return false
		}
	}
	return true
}

func captureReconciliationGenerations(state runtimeOwnerState, scope reconciliationScope, complete bool, groups []reconciliationIssueGroup) (map[string]uint64, map[string]uint64) {
	issues, attempts := map[string]uint64{}, map[string]uint64{}
	addIssue := func(key string) {
		if generation, exists := state.IssueGenerations[key]; exists {
			issues[key] = generation
		}
	}
	addAttempt := func(key string) {
		if generation, exists := state.AttemptGenerations[key]; exists {
			attempts[key] = generation
		}
	}
	if scope.Kind == reconciliationIssueScope {
		addIssue(ownerIssueKey(scope.Repository, scope.Issue))
	}
	if complete {
		for key := range state.Attempts {
			repository, issue, _, ok := parseOwnerAttemptKey(key)
			if ok && reconciliationScopeContains(scope, ownerIssueKey(repository, issue)) {
				addIssue(ownerIssueKey(repository, issue))
				addAttempt(key)
			}
		}
	}
	for key, observation := range state.Observations {
		if !reconciliationScopeContains(scope, key) {
			continue
		}
		addIssue(key)
		for attemptKey := range observation.Attempts {
			addAttempt(attemptKey)
		}
	}
	for _, group := range groups {
		addIssue(ownerIssueKey(group.Fact.Repository, group.Fact.Issue))
		if group.Fact.ActiveAttempt != nil {
			addAttempt(ownerAttemptKey(group.Fact.ActiveAttempt.Repository, group.Fact.ActiveAttempt.Issue, group.Fact.ActiveAttempt.Attempt))
		}
		for _, attempt := range group.Fact.TerminalAttempts {
			addAttempt(ownerAttemptKey(attempt.Repository, attempt.Issue, attempt.Attempt))
		}
		for _, attempt := range group.Attempts {
			addAttempt(ownerAttemptKey(attempt.Repository, attempt.Issue, attempt.Attempt))
		}
	}
	return issues, attempts
}

func validStringList(values []string, count, size int) bool {
	if len(values) > count {
		return false
	}
	for _, value := range values {
		if len(value) > size || strings.ContainsRune(value, 0) {
			return false
		}
	}
	return true
}

func reconciliationInputDigest(scope reconciliationScope, complete bool, key string, group *reconciliationIssueGroup) string {
	body, _ := json.Marshal(struct {
		Scope    reconciliationScope
		Complete bool
		Key      string
		Group    *reconciliationIssueGroup
	}{scope, complete, key, group})
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:])
}

func cloneReconciliationCollection(collection reconciliationCollection) reconciliationCollection {
	collection.IssueGenerations = cloneMap(collection.IssueGenerations)
	collection.AttemptGenerations = cloneMap(collection.AttemptGenerations)
	issues := make([]reconciliationIssueGroup, len(collection.Issues))
	for index, group := range collection.Issues {
		issues[index] = reconciliationIssueGroup{Fact: cloneReconciliationIssueFact(group.Fact), IssueUpdates: slices.Clone(group.IssueUpdates), Attempts: cloneReconciliationAttemptFacts(group.Attempts)}
	}
	collection.Issues = issues
	return collection
}

func normalizeReconciliationCollection(collection *reconciliationCollection) {
	slices.SortFunc(collection.Issues, func(left, right reconciliationIssueGroup) int { return cmp.Compare(left.Fact.Issue, right.Fact.Issue) })
	for index := range collection.Issues {
		slices.SortFunc(collection.Issues[index].IssueUpdates, func(left, right reconciliationIssueUpdateProposal) int {
			if ordered := cmp.Compare(left.Kind, right.Kind); ordered != 0 {
				return ordered
			}
			if ordered := cmp.Compare(left.Dependency, right.Dependency); ordered != 0 {
				return ordered
			}
			if ordered := cmp.Compare(left.AttributionAttempt, right.AttributionAttempt); ordered != 0 {
				return ordered
			}
			return cmp.Compare(left.PullRequest, right.PullRequest)
		})
		slices.SortFunc(collection.Issues[index].Attempts, func(left, right reconciliationAttemptFact) int { return cmp.Compare(left.Attempt, right.Attempt) })
	}
}

func cloneReconciliationObservation(observation reconciliationObservation) reconciliationObservation {
	observation.Fact = cloneReconciliationIssueFact(observation.Fact)
	observation.IssueUpdates = slices.Clone(observation.IssueUpdates)
	observation.Attempts = cloneAttemptObservations(observation.Attempts)
	return observation
}

func cloneAttemptObservations(attempts map[string]reconciliationAttemptObservation) map[string]reconciliationAttemptObservation {
	clone := make(map[string]reconciliationAttemptObservation, len(attempts))
	for key, attempt := range attempts {
		attempt.Fact = cloneReconciliationAttemptFact(attempt.Fact)
		clone[key] = attempt
	}
	return clone
}

func cloneReconciliationIssueFact(fact reconciliationIssueFact) reconciliationIssueFact {
	fact.Dependencies = slices.Clone(fact.Dependencies)
	fact.SatisfiedDependencies = slices.Clone(fact.SatisfiedDependencies)
	fact.Paths = slices.Clone(fact.Paths)
	fact.Blockers = slices.Clone(fact.Blockers)
	if fact.ActiveAttempt != nil {
		attempt := cloneReconciliationAttemptFact(*fact.ActiveAttempt)
		fact.ActiveAttempt = &attempt
	}
	fact.TerminalAttempts = cloneReconciliationAttemptFacts(fact.TerminalAttempts)
	return fact
}

func cloneReconciliationAttemptFacts(facts []reconciliationAttemptFact) []reconciliationAttemptFact {
	if facts == nil {
		return nil
	}
	clone := make([]reconciliationAttemptFact, len(facts))
	for index, fact := range facts {
		clone[index] = cloneReconciliationAttemptFact(fact)
	}
	return clone
}

func cloneReconciliationAttemptFact(fact reconciliationAttemptFact) reconciliationAttemptFact {
	fact.Checks = slices.Clone(fact.Checks)
	return fact
}
