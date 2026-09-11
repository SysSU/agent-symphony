package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"

	agentruntime "github.com/SysSU/agent-symphony/internal/runtime"
)

const (
	runtimeOwnerStateVersion = 2
	runtimeOwnerStateFile    = "runtime-state.json"
	maxRuntimeOwnerState     = 16 << 20
	stateOwnerQueueSize      = 64
)

var (
	errStateOwnerStopped = errors.New("state owner is stopped")
	errStaleStateResult  = errors.New("state result is stale")
	errAttemptTombstoned = errors.New("attempt is tombstoned")
	errStateConflict     = errors.New("state transition conflicts with committed state")
)

type runtimeOwnerState struct {
	Version            int                             `json:"version"`
	Repository         string                          `json:"repository"`
	Epoch              uint64                          `json:"epoch"`
	Revision           uint64                          `json:"revision"`
	IssueGenerations   map[string]uint64               `json:"issue_generations"`
	AttemptGenerations map[string]uint64               `json:"attempt_generations"`
	Attempts           map[string]runtimeAttemptRecord `json:"attempts"`
	Tombstones         map[string]runtimeTombstone     `json:"tombstones"`
	Effects            map[string]runtimeEffectIntent  `json:"effects"`
	ControlReceipts    []controlReceipt                `json:"control_receipts"`
}

type runtimeAttemptRecord struct {
	Generation       uint64                `json:"generation"`
	ObservationEpoch uint64                `json:"observation_epoch,omitempty"`
	LastCycleID      uint64                `json:"last_cycle_id,omitempty"`
	Manifest         agentruntime.Manifest `json:"manifest"`
}

type runtimeTombstone struct {
	Repository            string                 `json:"repository"`
	Issue                 int                    `json:"issue"`
	Attempt               int                    `json:"attempt"`
	Action                string                 `json:"action"`
	InvalidatedGeneration uint64                 `json:"invalidated_generation"`
	Generation            uint64                 `json:"generation"`
	Revision              uint64                 `json:"revision"`
	CleanupPhase          string                 `json:"cleanup_phase"`
	PublishedHead         string                 `json:"published_head,omitempty"`
	Manifest              *agentruntime.Manifest `json:"manifest,omitempty"`
	EffectID              string                 `json:"effect_id,omitempty"`
	Diagnostic            string                 `json:"diagnostic,omitempty"`
}

type runtimeEffectIntent struct {
	ID                string `json:"id"`
	Action            string `json:"action"`
	Repository        string `json:"repository"`
	Issue             int    `json:"issue"`
	Attempt           int    `json:"attempt"`
	IssueGeneration   uint64 `json:"issue_generation"`
	AttemptGeneration uint64 `json:"attempt_generation"`
	IntentRevision    uint64 `json:"intent_revision"`
	State             string `json:"state"`
	Diagnostic        string `json:"diagnostic,omitempty"`
}

type stateResultIdentity struct {
	Epoch             uint64
	SourceRevision    uint64
	CycleID           uint64
	IssueGeneration   uint64
	AttemptGeneration uint64
	EffectID          string
}

type stateOwnerSnapshot struct {
	State   runtimeOwnerState
	CycleID uint64
}

type upsertAttemptCommand struct {
	Manifest                  agentruntime.Manifest
	ExpectedIssueGeneration   uint64
	ExpectedAttemptGeneration uint64
}

type advanceIssueGenerationCommand struct {
	Repository         string
	Issue              int
	ExpectedGeneration uint64
}

type invalidateAttemptCommand struct {
	Repository                string
	Issue                     int
	Attempt                   int
	ExpectedIssueGeneration   uint64
	ExpectedAttemptGeneration uint64
	Action                    string
	CleanupPhase              string
	PublishedHead             string
	Manifest                  *agentruntime.Manifest
	Diagnostic                string
	EffectAction              string
}

type applyAttemptResultCommand struct {
	Identity stateResultIdentity
	Manifest agentruntime.Manifest
	Global   bool
}

type recordEffectCommand struct {
	Identity stateResultIdentity
	Action   string
	Manifest agentruntime.Manifest
}

type completeEffectCommand struct {
	Identity   stateResultIdentity
	Diagnostic string
}

type recordControlReceiptCommand struct {
	Receipt controlReceipt
}

type stateOwnerCommandKind uint8

const (
	stateOwnerStart stateOwnerCommandKind = iota + 1
	stateOwnerAdvanceIssueGeneration
	stateOwnerUpsertAttempt
	stateOwnerInvalidateAttempt
	stateOwnerApplyAttemptResult
	stateOwnerRecordEffect
	stateOwnerCompleteEffect
	stateOwnerRecordControlReceipt
)

type stateOwnerCommand struct {
	kind       stateOwnerCommandKind
	issue      advanceIssueGenerationCommand
	upsert     upsertAttemptCommand
	invalidate invalidateAttemptCommand
	apply      applyAttemptResultCommand
	record     recordEffectCommand
	complete   completeEffectCommand
	receipt    recordControlReceiptCommand
	reply      chan stateOwnerResult
}

type stateOwnerResult struct {
	snapshot stateOwnerSnapshot
	effect   *runtimeEffectIntent
	err      error
}

type stateOwnerSnapshotRequest struct {
	cycle bool
	reply chan stateOwnerResult
}

type stateOwnerStopRequest struct{ reply chan error }

type statePersistenceRequest struct {
	state runtimeOwnerState
	reply chan error
}

type pendingStateCommit struct {
	command   stateOwnerCommand
	candidate runtimeOwnerState
	effect    *runtimeEffectIntent
}

// stateOwner is the sole in-process commit authority for a v2 runtime ledger.
// Production activation is intentionally deferred until every v1 writer is removed.
type stateOwner struct {
	stateRoot string
	commands  chan stateOwnerCommand
	snapshots chan stateOwnerSnapshotRequest
	stop      chan stateOwnerStopRequest
	done      chan struct{}
}

func startStateOwner(ctx context.Context, stateRoot string, initial runtimeOwnerState, persist func(runtimeOwnerState) error) (*stateOwner, error) {
	if persist == nil {
		return nil, errors.New("state persistence is required")
	}
	root, err := filepath.EvalSymlinks(stateRoot)
	if err != nil || root != filepath.Clean(stateRoot) {
		return nil, errors.New("runtime state root is unsafe")
	}
	if err := validateRuntimeOwnerState(initial, root, false); err != nil {
		return nil, err
	}
	owner := &stateOwner{
		stateRoot: root,
		commands:  make(chan stateOwnerCommand),
		snapshots: make(chan stateOwnerSnapshotRequest),
		stop:      make(chan stateOwnerStopRequest),
		done:      make(chan struct{}),
	}
	persistRequests := make(chan statePersistenceRequest, 1)
	persistDone := make(chan struct{})
	go runStatePersistence(persistRequests, persistDone, persist)
	go owner.run(initial, persistRequests, persistDone)
	if _, err := owner.submit(ctx, stateOwnerCommand{kind: stateOwnerStart}); err != nil {
		_ = owner.close(context.Background())
		return nil, err
	}
	return owner, nil
}

func runStatePersistence(requests <-chan statePersistenceRequest, done chan<- struct{}, persist func(runtimeOwnerState) error) {
	defer close(done)
	for request := range requests {
		request.reply <- persist(request.state)
	}
}

func (o *stateOwner) run(initial runtimeOwnerState, persistence chan statePersistenceRequest, persistenceDone <-chan struct{}) {
	defer close(o.done)
	committed := cloneRuntimeOwnerState(initial)
	var queue []stateOwnerCommand
	var inFlight *pendingStateCommit
	var persistenceResult <-chan error
	var stopReply chan error
	var cycleID uint64

	startNext := func() {
		for inFlight == nil && len(queue) > 0 {
			command := queue[0]
			queue = queue[1:]
			candidate, effect, err := applyStateOwnerCommand(o.stateRoot, committed, command)
			if err != nil {
				command.reply <- stateOwnerResult{err: err}
				continue
			}
			if reflect.DeepEqual(candidate, committed) {
				command.reply <- stateOwnerResult{snapshot: stateOwnerSnapshot{State: cloneRuntimeOwnerState(committed)}, effect: cloneEffect(effect)}
				continue
			}
			reply := make(chan error, 1)
			persistence <- statePersistenceRequest{state: candidate, reply: reply}
			inFlight = &pendingStateCommit{command: command, candidate: candidate, effect: effect}
			persistenceResult = reply
		}
	}

	finish := func() bool {
		if stopReply == nil || inFlight != nil {
			return false
		}
		close(persistence)
		<-persistenceDone
		stopReply <- nil
		return true
	}

	for {
		startNext()
		if finish() {
			return
		}
		commandInput := (<-chan stateOwnerCommand)(o.commands)
		if len(queue) == stateOwnerQueueSize {
			commandInput = nil
		}
		select {
		case command := <-commandInput:
			if stopReply != nil {
				command.reply <- stateOwnerResult{err: errStateOwnerStopped}
			} else {
				queue = append(queue, command)
			}
		case request := <-o.snapshots:
			if stopReply != nil {
				request.reply <- stateOwnerResult{err: errStateOwnerStopped}
				continue
			}
			if request.cycle {
				cycleID++
			}
			request.reply <- stateOwnerResult{snapshot: stateOwnerSnapshot{State: cloneRuntimeOwnerState(committed), CycleID: cycleID}}
		case err := <-persistenceResult:
			if err == nil {
				committed = inFlight.candidate
				inFlight.command.reply <- stateOwnerResult{snapshot: stateOwnerSnapshot{State: cloneRuntimeOwnerState(committed)}, effect: cloneEffect(inFlight.effect)}
			} else {
				inFlight.command.reply <- stateOwnerResult{err: err}
			}
			inFlight, persistenceResult = nil, nil
		case request := <-o.stop:
			if stopReply != nil {
				request.reply <- errStateOwnerStopped
				continue
			}
			stopReply = request.reply
			for _, command := range queue {
				command.reply <- stateOwnerResult{err: errStateOwnerStopped}
			}
			queue = nil
		}
	}
}

func (o *stateOwner) close(ctx context.Context) error {
	reply := make(chan error, 1)
	select {
	case <-o.done:
		return nil
	case o.stop <- stateOwnerStopRequest{reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (o *stateOwner) snapshot(ctx context.Context) (stateOwnerSnapshot, error) {
	return o.requestSnapshot(ctx, false)
}

func (o *stateOwner) reconciliationSnapshot(ctx context.Context) (stateOwnerSnapshot, error) {
	return o.requestSnapshot(ctx, true)
}

func (o *stateOwner) requestSnapshot(ctx context.Context, cycle bool) (stateOwnerSnapshot, error) {
	reply := make(chan stateOwnerResult, 1)
	select {
	case <-o.done:
		return stateOwnerSnapshot{}, errStateOwnerStopped
	case o.snapshots <- stateOwnerSnapshotRequest{cycle: cycle, reply: reply}:
	case <-ctx.Done():
		return stateOwnerSnapshot{}, ctx.Err()
	}
	select {
	case result := <-reply:
		return result.snapshot, result.err
	case <-o.done:
		return stateOwnerSnapshot{}, errStateOwnerStopped
	}
}

func (o *stateOwner) submit(ctx context.Context, command stateOwnerCommand) (stateOwnerResult, error) {
	command.reply = make(chan stateOwnerResult, 1)
	select {
	case <-o.done:
		return stateOwnerResult{}, errStateOwnerStopped
	case o.commands <- command:
	case <-ctx.Done():
		return stateOwnerResult{}, ctx.Err()
	}
	select {
	case result := <-command.reply:
		return result, result.err
	case <-o.done:
		return stateOwnerResult{}, errStateOwnerStopped
	}
}

func (o *stateOwner) upsertAttempt(ctx context.Context, command upsertAttemptCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerUpsertAttempt, upsert: command})
	return result.snapshot, err
}

func (o *stateOwner) advanceIssueGeneration(ctx context.Context, command advanceIssueGenerationCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerAdvanceIssueGeneration, issue: command})
	return result.snapshot, err
}

func (o *stateOwner) invalidateAttempt(ctx context.Context, command invalidateAttemptCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerInvalidateAttempt, invalidate: command})
	return result.snapshot, result.effect, err
}

func (o *stateOwner) applyAttemptResult(ctx context.Context, command applyAttemptResultCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerApplyAttemptResult, apply: command})
	return result.snapshot, err
}

func (o *stateOwner) recordEffect(ctx context.Context, command recordEffectCommand) (stateOwnerSnapshot, *runtimeEffectIntent, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerRecordEffect, record: command})
	return result.snapshot, result.effect, err
}

func (o *stateOwner) completeEffect(ctx context.Context, command completeEffectCommand) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerCompleteEffect, complete: command})
	return result.snapshot, err
}

func (o *stateOwner) recordControlReceipt(ctx context.Context, receipt controlReceipt) (stateOwnerSnapshot, error) {
	result, err := o.submit(ctx, stateOwnerCommand{kind: stateOwnerRecordControlReceipt, receipt: recordControlReceiptCommand{Receipt: receipt}})
	return result.snapshot, err
}

func applyStateOwnerCommand(stateRoot string, committed runtimeOwnerState, command stateOwnerCommand) (runtimeOwnerState, *runtimeEffectIntent, error) {
	candidate := cloneRuntimeOwnerState(committed)
	if command.kind == stateOwnerStart {
		if candidate.Epoch == ^uint64(0) {
			return runtimeOwnerState{}, nil, errors.New("runtime epoch overflow")
		}
		candidate.Epoch++
		return finishRuntimeOwnerTransition(stateRoot, candidate, nil)
	}
	switch command.kind {
	case stateOwnerAdvanceIssueGeneration:
		if err := applyAdvanceIssueGeneration(&candidate, command.issue); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerUpsertAttempt:
		if err := applyUpsertAttempt(stateRoot, &candidate, command.upsert); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerInvalidateAttempt:
		effect, err := applyInvalidateAttempt(stateRoot, &candidate, command.invalidate)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		if reflect.DeepEqual(candidate, committed) {
			return candidate, effect, nil
		}
		return finishRuntimeOwnerTransition(stateRoot, candidate, effect)
	case stateOwnerApplyAttemptResult:
		if err := applyAttemptResult(stateRoot, &candidate, command.apply); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerRecordEffect:
		effect, err := applyRecordEffect(&candidate, command.record)
		if err != nil {
			return runtimeOwnerState{}, nil, err
		}
		return finishRuntimeOwnerTransition(stateRoot, candidate, effect)
	case stateOwnerCompleteEffect:
		if err := applyCompleteEffect(&candidate, command.complete); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	case stateOwnerRecordControlReceipt:
		if err := applyControlReceipt(&candidate, command.receipt.Receipt); err != nil {
			return runtimeOwnerState{}, nil, err
		}
	default:
		return runtimeOwnerState{}, nil, errors.New("unknown state owner command")
	}
	if reflect.DeepEqual(candidate, committed) {
		return candidate, nil, nil
	}
	return finishRuntimeOwnerTransition(stateRoot, candidate, nil)
}

func applyAdvanceIssueGeneration(state *runtimeOwnerState, command advanceIssueGenerationCommand) error {
	if command.Repository != state.Repository || command.Issue < 1 {
		return errStateConflict
	}
	key := ownerIssueKey(command.Repository, command.Issue)
	generation := state.IssueGenerations[key]
	if generation != command.ExpectedGeneration {
		return errStaleStateResult
	}
	if generation == ^uint64(0) {
		return errors.New("issue generation overflow")
	}
	state.IssueGenerations[key] = generation + 1
	for id, effect := range state.Effects {
		if effect.Repository == command.Repository && effect.Issue == command.Issue && !effectAuthorizedByTombstone(*state, effect) {
			delete(state.Effects, id)
		}
	}
	return nil
}

func finishRuntimeOwnerTransition(stateRoot string, candidate runtimeOwnerState, effect *runtimeEffectIntent) (runtimeOwnerState, *runtimeEffectIntent, error) {
	if candidate.Revision == ^uint64(0) {
		return runtimeOwnerState{}, nil, errors.New("runtime revision overflow")
	}
	candidate.Revision++
	if effect != nil {
		effect.IntentRevision = candidate.Revision
		effect.ID = runtimeEffectID(*effect)
		candidate.Effects[effect.ID] = *effect
		attemptKey := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
		if tombstone, ok := candidate.Tombstones[attemptKey]; ok && tombstone.Revision == 0 {
			tombstone.EffectID = effect.ID
			candidate.Tombstones[attemptKey] = tombstone
		}
	}
	for key, tombstone := range candidate.Tombstones {
		if tombstone.Revision == 0 {
			tombstone.Revision = candidate.Revision
			candidate.Tombstones[key] = tombstone
		}
	}
	if err := validateRuntimeOwnerState(candidate, stateRoot, true); err != nil {
		return runtimeOwnerState{}, nil, err
	}
	return candidate, cloneEffect(effect), nil
}

func applyUpsertAttempt(stateRoot string, state *runtimeOwnerState, command upsertAttemptCommand) error {
	manifest := cloneManifest(command.Manifest)
	if err := validateOwnerManifest(state.Repository, stateRoot, manifest); err != nil {
		return err
	}
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	issueGeneration := state.IssueGenerations[issueKey]
	if issueGeneration != command.ExpectedIssueGeneration {
		return errStaleStateResult
	}
	if issueGeneration == 0 {
		issueGeneration = 1
		state.IssueGenerations[issueKey] = issueGeneration
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return errAttemptTombstoned
	}
	generation := state.AttemptGenerations[attemptKey]
	if generation != command.ExpectedAttemptGeneration {
		return errStaleStateResult
	}
	if generation == 0 {
		generation = 1
	} else {
		if generation == ^uint64(0) {
			return errors.New("attempt generation overflow")
		}
		generation++
		deleteAttemptEffects(state, manifest.Repository, manifest.Issue, manifest.Attempt)
	}
	state.AttemptGenerations[attemptKey] = generation
	previous := state.Attempts[attemptKey]
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: generation, ObservationEpoch: previous.ObservationEpoch, LastCycleID: previous.LastCycleID, Manifest: manifest}
	return nil
}

func applyInvalidateAttempt(stateRoot string, state *runtimeOwnerState, command invalidateAttemptCommand) (*runtimeEffectIntent, error) {
	if command.Repository != state.Repository || command.Issue < 1 || command.Attempt < 1 || !validTombstoneAction(command.Action) || !validCleanupPhase(command.CleanupPhase) {
		return nil, errStateConflict
	}
	issueKey, attemptKey := ownerIssueKey(command.Repository, command.Issue), ownerAttemptKey(command.Repository, command.Issue, command.Attempt)
	if state.IssueGenerations[issueKey] != command.ExpectedIssueGeneration {
		return nil, errStaleStateResult
	}
	if existing, ok := state.Tombstones[attemptKey]; ok {
		if existing.InvalidatedGeneration != command.ExpectedAttemptGeneration || existing.Action != command.Action || existing.CleanupPhase != command.CleanupPhase || existing.PublishedHead != command.PublishedHead || existing.Diagnostic != command.Diagnostic || !sameOptionalAttemptIdentity(existing.Manifest, command.Manifest) {
			return nil, errStateConflict
		}
		if command.EffectAction == "" && existing.EffectID == "" {
			return nil, nil
		}
		effect, ok := state.Effects[existing.EffectID]
		if !ok || effect.Action != command.EffectAction || effect.Repository != command.Repository || effect.Issue != command.Issue || effect.Attempt != command.Attempt {
			return nil, errStateConflict
		}
		return cloneEffect(&effect), nil
	}
	if state.AttemptGenerations[attemptKey] != command.ExpectedAttemptGeneration {
		return nil, errStaleStateResult
	}
	if command.Manifest != nil {
		manifest := cloneManifest(*command.Manifest)
		if err := validateOwnerManifest(state.Repository, stateRoot, manifest); err != nil || manifest.Issue != command.Issue || manifest.Attempt != command.Attempt {
			return nil, errStateConflict
		}
		command.Manifest = &manifest
	}
	invalidated := command.ExpectedAttemptGeneration
	if invalidated == ^uint64(0) {
		return nil, errors.New("attempt generation overflow")
	}
	generation := invalidated + 1
	if generation == 0 {
		generation = 1
	}
	state.AttemptGenerations[attemptKey] = generation
	delete(state.Attempts, attemptKey)
	deleteAttemptEffects(state, command.Repository, command.Issue, command.Attempt)
	state.Tombstones[attemptKey] = runtimeTombstone{
		Repository: command.Repository, Issue: command.Issue, Attempt: command.Attempt, Action: command.Action,
		InvalidatedGeneration: invalidated, Generation: generation, CleanupPhase: command.CleanupPhase,
		PublishedHead: command.PublishedHead, Manifest: command.Manifest, Diagnostic: command.Diagnostic,
	}
	if command.EffectAction == "" {
		return nil, nil
	}
	return &runtimeEffectIntent{Action: command.EffectAction, Repository: command.Repository, Issue: command.Issue, Attempt: command.Attempt, IssueGeneration: command.ExpectedIssueGeneration, AttemptGeneration: generation, State: "pending"}, nil
}

func applyAttemptResult(stateRoot string, state *runtimeOwnerState, command applyAttemptResultCommand) error {
	identity, manifest := command.Identity, cloneManifest(command.Manifest)
	if err := validateOwnerManifest(state.Repository, stateRoot, manifest); err != nil || identity.Epoch != state.Epoch || identity.SourceRevision == 0 || identity.SourceRevision > state.Revision || command.Global && identity.SourceRevision != state.Revision || identity.CycleID == 0 {
		return errStaleStateResult
	}
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if state.IssueGenerations[issueKey] != identity.IssueGeneration || state.AttemptGenerations[attemptKey] != identity.AttemptGeneration {
		return errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return errAttemptTombstoned
	}
	if identity.EffectID != "" {
		effect, ok := state.Effects[identity.EffectID]
		if !ok || effect.State != "pending" || effect.IssueGeneration != identity.IssueGeneration || effect.AttemptGeneration != identity.AttemptGeneration {
			return errStaleStateResult
		}
	}
	record := state.Attempts[attemptKey]
	if record.ObservationEpoch == identity.Epoch && identity.CycleID < record.LastCycleID {
		return errStaleStateResult
	}
	if record.ObservationEpoch == identity.Epoch && identity.CycleID == record.LastCycleID {
		if reflect.DeepEqual(record.Manifest, manifest) {
			return nil
		}
		return errStateConflict
	}
	state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: identity.AttemptGeneration, ObservationEpoch: identity.Epoch, LastCycleID: identity.CycleID, Manifest: manifest}
	return nil
}

func applyRecordEffect(state *runtimeOwnerState, command recordEffectCommand) (*runtimeEffectIntent, error) {
	manifest, identity := command.Manifest, command.Identity
	if command.Action == "" || manifest.Repository != state.Repository || manifest.Issue < 1 || manifest.Attempt < 1 || identity.Epoch != state.Epoch || identity.SourceRevision != state.Revision {
		return nil, errStaleStateResult
	}
	issueKey, attemptKey := ownerIssueKey(manifest.Repository, manifest.Issue), ownerAttemptKey(manifest.Repository, manifest.Issue, manifest.Attempt)
	if state.IssueGenerations[issueKey] != identity.IssueGeneration || state.AttemptGenerations[attemptKey] != identity.AttemptGeneration {
		return nil, errStaleStateResult
	}
	if _, tombstoned := state.Tombstones[attemptKey]; tombstoned {
		return nil, errAttemptTombstoned
	}
	return &runtimeEffectIntent{Action: command.Action, Repository: manifest.Repository, Issue: manifest.Issue, Attempt: manifest.Attempt, IssueGeneration: identity.IssueGeneration, AttemptGeneration: identity.AttemptGeneration, State: "pending"}, nil
}

func applyCompleteEffect(state *runtimeOwnerState, command completeEffectCommand) error {
	identity := command.Identity
	if identity.Epoch != state.Epoch || identity.EffectID == "" {
		return errStaleStateResult
	}
	effect, ok := state.Effects[identity.EffectID]
	if !ok || effect.IssueGeneration != identity.IssueGeneration || effect.AttemptGeneration != identity.AttemptGeneration || state.IssueGenerations[ownerIssueKey(effect.Repository, effect.Issue)] != identity.IssueGeneration && !effectAuthorizedByTombstone(*state, effect) {
		return errStaleStateResult
	}
	if effect.State == "completed" {
		if effect.Diagnostic == command.Diagnostic {
			return nil
		}
		return errStateConflict
	}
	if effect.State != "pending" {
		return errStaleStateResult
	}
	attemptKey := ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
	if state.AttemptGenerations[attemptKey] != identity.AttemptGeneration {
		return errStaleStateResult
	}
	effect.State, effect.Diagnostic = "completed", command.Diagnostic
	state.Effects[effect.ID] = effect
	if tombstone, ok := state.Tombstones[attemptKey]; ok && tombstone.EffectID == effect.ID {
		tombstone.CleanupPhase = "completed"
		state.Tombstones[attemptKey] = tombstone
	}
	return nil
}

func applyControlReceipt(state *runtimeOwnerState, receipt controlReceipt) error {
	if !validControlRequest(receipt.Request, state.Repository) || receipt.State != "pending" && receipt.State != "completed" || receipt.State == "pending" && receipt.Result != nil || receipt.State == "completed" && (receipt.Result == nil || !validRecordedControlResult(*receipt.Result, receipt.Request)) {
		return errStateConflict
	}
	for index, existing := range state.ControlReceipts {
		if existing.Request.RequestID != receipt.Request.RequestID {
			continue
		}
		if existing.Request != receipt.Request || existing.State == "completed" && !reflect.DeepEqual(existing, receipt) {
			return errStateConflict
		}
		state.ControlReceipts[index] = cloneControlReceipt(receipt)
		return nil
	}
	if len(state.ControlReceipts) == maxControlReceipts {
		completed := slices.IndexFunc(state.ControlReceipts, func(existing controlReceipt) bool { return existing.State == "completed" })
		if completed < 0 {
			return errStateConflict
		}
		state.ControlReceipts = slices.Delete(state.ControlReceipts, completed, completed+1)
	}
	state.ControlReceipts = append(state.ControlReceipts, cloneControlReceipt(receipt))
	return nil
}

func loadOrMigrateRuntimeOwnerState(stateRoot, repository string) (runtimeOwnerState, bool, error) {
	state, err := readRuntimeOwnerState(stateRoot, repository)
	if err == nil {
		return state, false, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return runtimeOwnerState{}, false, err
	}
	state, err = migrateLegacyRuntimeState(stateRoot, repository)
	return state, true, err
}

func readRuntimeOwnerState(stateRoot, repository string) (runtimeOwnerState, error) {
	path := filepath.Join(stateRoot, runtimeOwnerStateFile)
	info, err := os.Lstat(path)
	if err != nil {
		return runtimeOwnerState{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info) || info.Size() > maxRuntimeOwnerState {
		return runtimeOwnerState{}, errors.New("runtime owner ledger is unsafe")
	}
	body, err := readDashboardFile(path, maxRuntimeOwnerState)
	if err != nil {
		return runtimeOwnerState{}, err
	}
	var state runtimeOwnerState
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&state) != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return runtimeOwnerState{}, errors.New("runtime owner ledger is invalid")
	}
	if err := validateRuntimeOwnerState(state, stateRoot, true); err != nil || state.Repository != repository {
		if err == nil {
			err = fmt.Errorf("runtime state is bound to project %s, not %s", state.Repository, repository)
		}
		return runtimeOwnerState{}, err
	}
	return state, nil
}

func migrateLegacyRuntimeState(stateRoot, repository string) (runtimeOwnerState, error) {
	identity, err := readDeploymentIdentity(stateRoot)
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("read deployment identity: %w", err)
	}
	if identity.Repository != repository {
		return runtimeOwnerState{}, fmt.Errorf("runtime state is bound to project %s, not %s", identity.Repository, repository)
	}
	state := newRuntimeOwnerState(repository)
	server := dashboardServer{stateRoot: stateRoot, repository: repository}
	manifests, err := (&agentruntime.Runtime{Root: productionAttemptRoot(stateRoot), StateRoot: stateRoot}).Discover()
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("migrate attempt manifests: %w", err)
	}
	for _, manifest := range manifests {
		issueKey, attemptKey := ownerIssueKey(repository, manifest.Issue), ownerAttemptKey(repository, manifest.Issue, manifest.Attempt)
		state.IssueGenerations[issueKey], state.AttemptGenerations[attemptKey] = 1, 1
		state.Attempts[attemptKey] = runtimeAttemptRecord{Generation: 1, Manifest: cloneManifest(manifest)}
	}
	dashboard, err := server.readState()
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("migrate dashboard state: %w", err)
	}
	for _, hidden := range dashboard.Hidden {
		if err := migrateLegacyTombstone(&state, hidden.Repository, hidden.Issue, hidden.Attempt, hidden.Reason, "completed", "", nil); err != nil {
			return runtimeOwnerState{}, err
		}
	}
	removals, err := server.readRemovalState()
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("migrate permanent removal state: %w", err)
	}
	for _, removal := range removals.Intents {
		phase := "pending"
		if removal.CleanupStarted {
			phase = "cleanup-started"
		}
		manifest := cloneManifest(removal.Manifest)
		if err := migrateLegacyTombstone(&state, manifest.Repository, manifest.Issue, manifest.Attempt, "removed", phase, removal.PublishedHead, &manifest); err != nil {
			return runtimeOwnerState{}, err
		}
		attemptKey := ownerAttemptKey(repository, manifest.Issue, manifest.Attempt)
		tombstone := state.Tombstones[attemptKey]
		effect := runtimeEffectIntent{Action: "remove", Repository: repository, Issue: manifest.Issue, Attempt: manifest.Attempt, IssueGeneration: state.IssueGenerations[ownerIssueKey(repository, manifest.Issue)], AttemptGeneration: tombstone.Generation, IntentRevision: 1, State: "pending"}
		effect.ID = runtimeEffectID(effect)
		state.Effects[effect.ID] = effect
		tombstone.EffectID = effect.ID
		state.Tombstones[attemptKey] = tombstone
	}
	receipts, err := server.readControlReceipts()
	if err != nil {
		return runtimeOwnerState{}, fmt.Errorf("migrate control receipts: %w", err)
	}
	state.ControlReceipts = make([]controlReceipt, len(receipts.Receipts))
	for index, receipt := range receipts.Receipts {
		state.ControlReceipts[index] = cloneControlReceipt(receipt)
	}
	if err := validateRuntimeOwnerState(state, stateRoot, false); err != nil {
		return runtimeOwnerState{}, err
	}
	return state, nil
}

func migrateLegacyTombstone(state *runtimeOwnerState, repository string, issue, attempt int, action, phase, publishedHead string, manifest *agentruntime.Manifest) error {
	if repository != state.Repository || issue < 1 || attempt < 1 || !validTombstoneAction(action) || !validCleanupPhase(phase) {
		return errors.New("legacy tombstone is invalid")
	}
	issueKey, attemptKey := ownerIssueKey(repository, issue), ownerAttemptKey(repository, issue, attempt)
	state.IssueGenerations[issueKey] = max(1, state.IssueGenerations[issueKey])
	invalidated := state.AttemptGenerations[attemptKey]
	if invalidated == 0 {
		invalidated = 1
	}
	generation := invalidated + 1
	existing, exists := state.Tombstones[attemptKey]
	if exists && action != "removed" {
		if existing.Action == action {
			return nil
		}
		return fmt.Errorf("legacy attempt %s has conflicting lifecycle state", attemptKey)
	}
	if action == "removed" && manifest == nil {
		return errors.New("legacy permanent removal is missing its manifest identity")
	}
	if manifest == nil {
		if record, ok := state.Attempts[attemptKey]; ok {
			copy := cloneManifest(record.Manifest)
			manifest = &copy
		}
	} else if existing.Manifest != nil && !sameAttemptIdentity(*existing.Manifest, *manifest) {
		return fmt.Errorf("legacy attempt %s has conflicting resource identity", attemptKey)
	}
	delete(state.Attempts, attemptKey)
	state.AttemptGenerations[attemptKey] = generation
	state.Tombstones[attemptKey] = runtimeTombstone{Repository: repository, Issue: issue, Attempt: attempt, Action: action, InvalidatedGeneration: invalidated, Generation: generation, CleanupPhase: phase, PublishedHead: publishedHead, Manifest: manifest}
	return nil
}

func writeRuntimeOwnerState(stateRoot string, state runtimeOwnerState) error {
	if err := validateRuntimeOwnerState(state, stateRoot, true); err != nil {
		return err
	}
	body, err := json.MarshalIndent(state, "", "  ")
	if err != nil || len(body)+1 > maxRuntimeOwnerState {
		return errors.New("runtime owner ledger is too large")
	}
	root, err := filepath.EvalSymlinks(stateRoot)
	if err != nil || root != filepath.Clean(stateRoot) {
		return errors.New("runtime state root is unsafe")
	}
	path := filepath.Join(root, runtimeOwnerStateFile)
	if info, statErr := os.Lstat(path); statErr == nil && (!info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm() != 0o600 || !ownedByCurrentUser(info)) {
		return errors.New("runtime owner ledger is unsafe")
	} else if statErr != nil && !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	temporary, err := os.CreateTemp(root, ".runtime-state-*")
	if err != nil {
		return err
	}
	name := temporary.Name()
	defer os.Remove(name)
	if err := temporary.Chmod(0o600); err != nil {
		temporary.Close()
		return err
	}
	if _, err := temporary.Write(append(body, '\n')); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(name, path); err != nil {
		return err
	}
	directory, err := os.Open(root)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func validateRuntimeOwnerState(state runtimeOwnerState, stateRoot string, persisted bool) error {
	if state.Version != runtimeOwnerStateVersion || strings.TrimSpace(state.Repository) == "" || state.IssueGenerations == nil || state.AttemptGenerations == nil || state.Attempts == nil || state.Tombstones == nil || state.Effects == nil || state.ControlReceipts == nil || persisted && (state.Epoch == 0 || state.Revision == 0) {
		return errors.New("runtime owner ledger is invalid")
	}
	for key, generation := range state.IssueGenerations {
		issue := issueFromOwnerKey(key)
		if generation == 0 || issue < 1 || key != ownerIssueKey(state.Repository, issue) {
			return errors.New("runtime owner issue generation is invalid")
		}
	}
	for key, generation := range state.AttemptGenerations {
		repository, issue, attempt, ok := parseOwnerAttemptKey(key)
		if !ok || repository != state.Repository || issue < 1 || attempt < 1 || generation == 0 {
			return errors.New("runtime owner attempt generation is invalid")
		}
	}
	for key, record := range state.Attempts {
		if state.AttemptGenerations[key] != record.Generation || record.Generation == 0 || record.ObservationEpoch > state.Epoch || record.ObservationEpoch == 0 && record.LastCycleID != 0 || record.ObservationEpoch != 0 && record.LastCycleID == 0 || key != ownerAttemptKey(record.Manifest.Repository, record.Manifest.Issue, record.Manifest.Attempt) {
			return errors.New("runtime owner attempt record is invalid")
		}
		if err := validateOwnerManifest(state.Repository, stateRoot, record.Manifest); err != nil {
			return err
		}
	}
	for key, tombstone := range state.Tombstones {
		if key != ownerAttemptKey(tombstone.Repository, tombstone.Issue, tombstone.Attempt) || tombstone.Repository != state.Repository || !validTombstoneAction(tombstone.Action) || !validCleanupPhase(tombstone.CleanupPhase) || tombstone.Generation == 0 || tombstone.InvalidatedGeneration >= tombstone.Generation || state.AttemptGenerations[key] != tombstone.Generation || tombstone.Revision > state.Revision || persisted && tombstone.Revision == 0 {
			return errors.New("runtime owner tombstone is invalid")
		}
		if _, exists := state.Attempts[key]; exists {
			return errors.New("runtime owner tombstone conflicts with an attempt")
		}
		if tombstone.Manifest != nil {
			if err := validateOwnerManifest(state.Repository, stateRoot, *tombstone.Manifest); err != nil || tombstone.Manifest.Issue != tombstone.Issue || tombstone.Manifest.Attempt != tombstone.Attempt {
				return errors.New("runtime owner tombstone manifest is invalid")
			}
		}
		if tombstone.CleanupPhase != "completed" && tombstone.Manifest == nil {
			return errors.New("pending tombstone lacks resource identity")
		}
		if tombstone.Action == "removed" && tombstone.CleanupPhase != "completed" && !preflightObjectID.MatchString(tombstone.PublishedHead) {
			return errors.New("pending permanent removal has an invalid published head")
		}
		if tombstone.EffectID != "" {
			effect, ok := state.Effects[tombstone.EffectID]
			if !ok || effect.Repository != tombstone.Repository || effect.Issue != tombstone.Issue || effect.Attempt != tombstone.Attempt || effect.AttemptGeneration != tombstone.Generation || tombstone.CleanupPhase == "completed" && effect.State != "completed" || tombstone.CleanupPhase != "completed" && effect.State != "pending" {
				return errors.New("runtime owner tombstone effect is invalid")
			}
		} else if tombstone.CleanupPhase != "completed" {
			return errors.New("pending tombstone lacks its cleanup effect")
		}
	}
	for key, effect := range state.Effects {
		maxRevision := state.Revision
		if !persisted {
			maxRevision++
		}
		issueKey, attemptKey := ownerIssueKey(effect.Repository, effect.Issue), ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)
		issueCurrent := state.IssueGenerations[issueKey] == effect.IssueGeneration || effectAuthorizedByTombstone(state, effect)
		if key != effect.ID || effect.ID != runtimeEffectID(effect) || effect.Repository != state.Repository || effect.Issue < 1 || effect.Attempt < 1 || effect.Action == "" || effect.IssueGeneration == 0 || effect.AttemptGeneration == 0 || effect.IntentRevision == 0 || effect.IntentRevision > maxRevision || effect.State != "pending" && effect.State != "completed" || !issueCurrent || state.AttemptGenerations[attemptKey] != effect.AttemptGeneration {
			return errors.New("runtime owner effect intent is invalid")
		}
	}
	if len(state.ControlReceipts) > maxControlReceipts {
		return errors.New("runtime owner control receipts are invalid")
	}
	seenReceipts := map[string]bool{}
	for _, receipt := range state.ControlReceipts {
		if seenReceipts[receipt.Request.RequestID] || !validControlRequest(receipt.Request, state.Repository) || receipt.State != "pending" && receipt.State != "completed" || receipt.State == "pending" && receipt.Result != nil || receipt.State == "completed" && (receipt.Result == nil || !validRecordedControlResult(*receipt.Result, receipt.Request)) {
			return errors.New("runtime owner control receipt is invalid")
		}
		seenReceipts[receipt.Request.RequestID] = true
	}
	return nil
}

func validateOwnerManifest(repository, stateRoot string, manifest agentruntime.Manifest) error {
	if manifest.Repository != repository || manifest.Issue < 1 || manifest.Attempt < 1 {
		return errors.New("runtime owner manifest is invalid")
	}
	if !filepath.IsAbs(stateRoot) {
		return errors.New("runtime owner state root is invalid")
	}
	return agentruntime.ValidateManifest(productionAttemptRoot(stateRoot), stateRoot, manifest)
}

func newRuntimeOwnerState(repository string) runtimeOwnerState {
	return runtimeOwnerState{Version: runtimeOwnerStateVersion, Repository: repository, IssueGenerations: map[string]uint64{}, AttemptGenerations: map[string]uint64{}, Attempts: map[string]runtimeAttemptRecord{}, Tombstones: map[string]runtimeTombstone{}, Effects: map[string]runtimeEffectIntent{}, ControlReceipts: []controlReceipt{}}
}

func cloneRuntimeOwnerState(state runtimeOwnerState) runtimeOwnerState {
	clone := state
	clone.IssueGenerations = cloneMap(state.IssueGenerations)
	clone.AttemptGenerations = cloneMap(state.AttemptGenerations)
	clone.Attempts = make(map[string]runtimeAttemptRecord, len(state.Attempts))
	for key, record := range state.Attempts {
		record.Manifest = cloneManifest(record.Manifest)
		clone.Attempts[key] = record
	}
	clone.Tombstones = make(map[string]runtimeTombstone, len(state.Tombstones))
	for key, tombstone := range state.Tombstones {
		if tombstone.Manifest != nil {
			manifest := cloneManifest(*tombstone.Manifest)
			tombstone.Manifest = &manifest
		}
		clone.Tombstones[key] = tombstone
	}
	clone.Effects = make(map[string]runtimeEffectIntent, len(state.Effects))
	for key, effect := range state.Effects {
		clone.Effects[key] = effect
	}
	clone.ControlReceipts = make([]controlReceipt, len(state.ControlReceipts))
	for index, receipt := range state.ControlReceipts {
		clone.ControlReceipts[index] = cloneControlReceipt(receipt)
	}
	return clone
}

func cloneMap(source map[string]uint64) map[string]uint64 {
	clone := make(map[string]uint64, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

func cloneManifest(manifest agentruntime.Manifest) agentruntime.Manifest {
	manifest.ReviewFindings = slices.Clone(manifest.ReviewFindings)
	return manifest
}

func cloneControlReceipt(receipt controlReceipt) controlReceipt {
	if receipt.Result != nil {
		result := *receipt.Result
		result.Data = slices.Clone(result.Data)
		receipt.Result = &result
	}
	return receipt
}

func cloneEffect(effect *runtimeEffectIntent) *runtimeEffectIntent {
	if effect == nil {
		return nil
	}
	clone := *effect
	return &clone
}

func ownerIssueKey(repository string, issue int) string {
	return repository + "#" + strconv.Itoa(issue)
}

func ownerAttemptKey(repository string, issue, attempt int) string {
	return ownerIssueKey(repository, issue) + "/" + strconv.Itoa(attempt)
}

func issueFromOwnerKey(key string) int {
	_, value, ok := strings.Cut(key, "#")
	if !ok {
		return 0
	}
	issue, _ := strconv.Atoi(value)
	return issue
}

func parseOwnerAttemptKey(key string) (string, int, int, bool) {
	repository, rest, ok := strings.Cut(key, "#")
	if !ok {
		return "", 0, 0, false
	}
	issueText, attemptText, ok := strings.Cut(rest, "/")
	issue, issueErr := strconv.Atoi(issueText)
	attempt, attemptErr := strconv.Atoi(attemptText)
	return repository, issue, attempt, ok && issueErr == nil && attemptErr == nil
}

func validTombstoneAction(action string) bool {
	return slices.Contains([]string{"archived", "abandoned", "dismissed", "removed"}, action)
}

func validCleanupPhase(phase string) bool {
	return slices.Contains([]string{"pending", "cleanup-started", "completed"}, phase)
}

func runtimeEffectID(effect runtimeEffectIntent) string {
	material := fmt.Sprintf("%s\x00%s\x00%d\x00%d\x00%d\x00%d\x00%d", effect.Action, effect.Repository, effect.Issue, effect.Attempt, effect.IssueGeneration, effect.AttemptGeneration, effect.IntentRevision)
	digest := sha256.Sum256([]byte(material))
	return hex.EncodeToString(digest[:16])
}

func sameAttemptIdentity(left, right agentruntime.Manifest) bool {
	return left.Repository == right.Repository && left.Issue == right.Issue && left.Attempt == right.Attempt && left.Branch == right.Branch && left.Worktree == right.Worktree && left.Session == right.Session && left.BaseSHA == right.BaseSHA && left.LogPath == right.LogPath
}

func sameOptionalAttemptIdentity(left, right *agentruntime.Manifest) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return sameAttemptIdentity(*left, *right)
}

func deleteAttemptEffects(state *runtimeOwnerState, repository string, issue, attempt int) {
	for id, effect := range state.Effects {
		if effect.Repository == repository && effect.Issue == issue && effect.Attempt == attempt {
			delete(state.Effects, id)
		}
	}
}

func effectAuthorizedByTombstone(state runtimeOwnerState, effect runtimeEffectIntent) bool {
	tombstone, ok := state.Tombstones[ownerAttemptKey(effect.Repository, effect.Issue, effect.Attempt)]
	return ok && tombstone.EffectID == effect.ID && tombstone.Generation == effect.AttemptGeneration
}
